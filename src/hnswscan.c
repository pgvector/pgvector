#include "postgres.h"

#include "access/relscan.h"
#include "hnsw.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"

/*
 * Algorithm 5 from paper
 */
static List *
GetScanItems(IndexScanDesc scan, Datum q)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	FmgrInfo   *procinfo = so->procinfo;
	Oid			collation = so->collation;
	List	   *ep;
	List	   *w;
	int			m;
	HnswElement entryPoint;
	char	   *base = NULL;

	/* Get m and entry point */
	HnswGetMetaPageInfo(index, &m, &entryPoint);

	so->q = q;
	so->m = m;

	if (entryPoint == NULL)
		return NIL;

	ep = list_make1(HnswEntryCandidate(base, entryPoint, q, index, procinfo, collation, false));

	for (int lc = entryPoint->level; lc >= 1; lc--)
	{
		w = HnswSearchLayer(base, q, ep, 1, lc, index, procinfo, collation, m, false, NULL, NULL, NULL, true);
		ep = w;
	}

	return HnswSearchLayer(base, q, ep, hnsw_ef_search, 0, index, procinfo, collation, m, false, NULL, &so->v, &so->discarded, true);
}

/*
 * Resume scan at ground level with discarded candidates
 */
static List *
ResumeScanItems(IndexScanDesc scan)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	FmgrInfo   *procinfo = so->procinfo;
	Oid			collation = so->collation;
	List	   *ep;
	char	   *base = NULL;

	if (list_length(so->discarded) == 0)
		return NIL;

	ep = so->discarded;
	so->discarded = NIL;
	return HnswSearchLayer(base, so->q, ep, hnsw_ef_search, 0, index, procinfo, collation, so->m, false, NULL, &so->v, &so->discarded, false);
}

/*
 * Get scan value
 */
static Datum
GetScanValue(IndexScanDesc scan)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	Datum		value;

	if (scan->orderByData->sk_flags & SK_ISNULL)
		value = PointerGetDatum(NULL);
	else
	{
		value = scan->orderByData->sk_argument;

		/* Value should not be compressed or toasted */
		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		/* Normalize if needed */
		if (so->normprocinfo != NULL)
			value = HnswNormValue(so->typeInfo, so->collation, value);
	}

	return value;
}

/*
 * Prepare for an index scan
 */
IndexScanDesc
hnswbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	HnswScanOpaque so;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	so = (HnswScanOpaque) palloc(sizeof(HnswScanOpaqueData));
	so->typeInfo = HnswGetTypeInfo(index);
	so->first = true;
	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Hnsw scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	/* Set support functions */
	so->procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
	so->normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
	so->collation = index->rd_indcollation[0];

	scan->opaque = so;

	return scan;
}

/*
 * Start or restart an index scan
 */
void
hnswrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;

	if (!so->first)
		tidhash_reset(so->v.tids);
	so->first = true;
	so->discarded = NIL;
	so->tuples = 0;
	MemoryContextReset(so->tmpCtx);

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
}

/*
 * Compare search candidate distances
 */
static int
CompareSearchCandidateDistances(const ListCell *a, const ListCell *b)
{
	HnswSearchCandidate *hca = lfirst(a);
	HnswSearchCandidate *hcb = lfirst(b);

	if (hca->distance < hcb->distance)
		return 1;

	if (hca->distance > hcb->distance)
		return -1;

	return 0;
}

/*
 * Fetch the next tuple in the given scan
 */
bool
hnswgettuple(IndexScanDesc scan, ScanDirection dir)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	/*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
	Assert(ScanDirectionIsForward(dir));

	if (so->first)
	{
		Datum		value;

		/* Count index scan for stats */
		pgstat_count_index_scan(scan->indexRelation);

		/* Safety check */
		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan hnsw index without order");

		/* Requires MVCC-compliant snapshot as not able to maintain a pin */
		/* https://www.postgresql.org/docs/current/index-locking.html */
		if (!IsMVCCSnapshot(scan->xs_snapshot))
			elog(ERROR, "non-MVCC snapshots are not supported with hnsw");

		/* Get scan value */
		value = GetScanValue(scan);

		/*
		 * Get a shared lock. This allows vacuum to ensure no in-flight scans
		 * before marking tuples as deleted.
		 */
		LockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

		HnswBench("scan iteration", so->w = GetScanItems(scan, value));

		/* Release shared lock */
		UnlockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

		so->first = false;

#if defined(HNSW_MEMORY)
		elog(INFO, "memory: %zu KB", MemoryContextMemAllocated(so->tmpCtx, false) / 1024);
#endif
	}

	for (;;)
	{
		char	   *base = NULL;
		HnswSearchCandidate *hc;
		HnswElement element;
		ItemPointer heaptid;

		if (list_length(so->w) == 0)
		{
			if (!hnsw_streaming)
				break;

			if (MemoryContextMemAllocated(so->tmpCtx, false) > (work_mem * 1024L))
			{
				if (list_length(so->discarded) == 0)
				{
					ereport(NOTICE,
							(errmsg("hnsw iterative search exceeded work_mem after " INT64_FORMAT " tuples", so->tuples),
							 errhint("Increase work_mem to scan more tuples.")));

					break;
				}

				/* Return remaining tuples */
				so->w = so->discarded;
				so->discarded = NIL;

				/* Sort in reverse order since results are removed from end */
				list_sort(so->w, CompareSearchCandidateDistances);
			}
			else
			{
				/*
				 * Ensure vacuum does not mark tuples as deleted during an
				 * iteration
				 */
				LockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

				HnswBench("scan iteration", so->w = ResumeScanItems(scan));

				UnlockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);
			}

#if defined(HNSW_MEMORY)
			elog(INFO, "memory: %zu KB", MemoryContextMemAllocated(so->tmpCtx, false) / 1024);
#endif

			if (list_length(so->w) == 0)
				break;
		}

		hc = llast(so->w);
		element = HnswPtrAccess(base, hc->element);

		/* Move to next element if no valid heap TIDs */
		if (element->heaptidsLength == 0)
		{
			so->w = list_delete_last(so->w);

			/* Mark memory as free for next iteration */
			if (hnsw_streaming)
			{
				pfree(element);
				pfree(hc);
			}

			continue;
		}

		so->tuples++;

		heaptid = &element->heaptids[--element->heaptidsLength];

		MemoryContextSwitchTo(oldCtx);

		scan->xs_heaptid = *heaptid;
		scan->xs_recheck = false;
		scan->xs_recheckorderby = false;
		return true;
	}

	MemoryContextSwitchTo(oldCtx);
	return false;
}

/*
 * End a scan and release resources
 */
void
hnswendscan(IndexScanDesc scan)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}
