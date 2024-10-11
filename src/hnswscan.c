#include "postgres.h"

#include "access/relscan.h"
#include "hnsw.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/float.h"
#include "utils/memutils.h"

/*
 * Algorithm 5 from paper
 */
static List *
GetScanItems(IndexScanDesc scan, Datum value)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	HnswSupport *support = &so->support;
	List	   *ep;
	List	   *w;
	int			m;
	HnswElement entryPoint;
	char	   *base = NULL;
	HnswQuery  *q = &so->q;

	/* Get m and entry point */
	HnswGetMetaPageInfo(index, &m, &entryPoint);

	q->value = value;
	so->m = m;

	if (entryPoint == NULL)
		return NIL;

	ep = list_make1(HnswEntryCandidate(base, entryPoint, q, index, support, false));

	for (int lc = entryPoint->level; lc >= 1; lc--)
	{
		w = HnswSearchLayer(base, q, ep, 1, lc, index, support, m, false, NULL, NULL, NULL, true, NULL);
		ep = w;
	}

	return HnswSearchLayer(base, q, ep, hnsw_ef_search, 0, index, support, m, false, NULL, &so->v, hnsw_iterative_search != HNSW_ITERATIVE_SEARCH_OFF ? &so->discarded : NULL, true, &so->tuples);
}

/*
 * Resume scan at ground level with discarded candidates
 */
static List *
ResumeScanItems(IndexScanDesc scan)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	List	   *ep = NIL;
	char	   *base = NULL;
	int			batch_size = hnsw_ef_search;

	if (pairingheap_is_empty(so->discarded))
		return NIL;

	/* Get next batch of candidates */
	for (int i = 0; i < batch_size; i++)
	{
		HnswSearchCandidate *sc;

		if (pairingheap_is_empty(so->discarded))
			break;

		sc = HnswGetSearchCandidate(w_node, pairingheap_remove_first(so->discarded));

		ep = lappend(ep, sc);
	}

	return HnswSearchLayer(base, &so->q, ep, batch_size, 0, index, &so->support, so->m, false, NULL, &so->v, &so->discarded, false, &so->tuples);
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
		if (so->support.normprocinfo != NULL)
			value = HnswNormValue(so->typeInfo, so->support.collation, value);
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
	so->v.tids = NULL;
	so->discarded = NULL;
	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Hnsw scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	/* Set support functions */
	HnswInitSupport(&so->support, index);

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

	if (so->v.tids != NULL)
		tidhash_reset(so->v.tids);

	if (so->discarded != NULL)
		pairingheap_reset(so->discarded);

	so->first = true;
	so->tuples = 0;
	so->previousDistance = -get_float8_infinity();
	MemoryContextReset(so->tmpCtx);

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
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

		so->w = GetScanItems(scan, value);

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
		HnswSearchCandidate *sc;
		HnswElement element;
		ItemPointer heaptid;

		if (list_length(so->w) == 0)
		{
			if (hnsw_iterative_search == HNSW_ITERATIVE_SEARCH_OFF)
				break;

			/* Empty index */
			if (so->discarded == NULL)
				break;

			/* Reached max number of additional tuples */
			if (hnsw_iterative_search_max_tuples != -1 && so->tuples >= hnsw_iterative_search_max_tuples)
			{
				if (pairingheap_is_empty(so->discarded))
					break;

				/* Return remaining tuples */
				so->w = lappend(so->w, HnswGetSearchCandidate(w_node, pairingheap_remove_first(so->discarded)));
			}
			/* Prevent scans from consuming too much memory */
			else if (MemoryContextMemAllocated(so->tmpCtx, false) > (Size) work_mem * 1024L)
			{
				if (pairingheap_is_empty(so->discarded))
				{
					ereport(DEBUG1,
							(errmsg("hnsw index scan exceeded work_mem after " INT64_FORMAT " tuples", so->tuples),
							 errhint("Increase work_mem to scan more tuples.")));

					break;
				}

				/* Return remaining tuples */
				so->w = lappend(so->w, HnswGetSearchCandidate(w_node, pairingheap_remove_first(so->discarded)));
			}
			else
			{
				/*
				 * Locking ensures when neighbors are read, the elements they
				 * reference will not be deleted (and replaced) during the
				 * iteration.
				 *
				 * Elements loaded into memory on previous iterations may have
				 * been deleted (and replaced), so when reading neighbors, the
				 * element version must be checked.
				 */
				LockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

				so->w = ResumeScanItems(scan);

				UnlockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

#if defined(HNSW_MEMORY)
				elog(INFO, "memory: %zu KB", MemoryContextMemAllocated(so->tmpCtx, false) / 1024);
#endif
			}

			if (list_length(so->w) == 0)
				break;
		}

		sc = llast(so->w);
		element = HnswPtrAccess(base, sc->element);

		/* Move to next element if no valid heap TIDs */
		if (element->heaptidsLength == 0)
		{
			so->w = list_delete_last(so->w);

			/* Mark memory as free for next iteration */
			if (hnsw_iterative_search != HNSW_ITERATIVE_SEARCH_OFF)
			{
				pfree(element);
				pfree(sc);
			}

			continue;
		}

		heaptid = &element->heaptids[--element->heaptidsLength];

		if (hnsw_iterative_search == HNSW_ITERATIVE_SEARCH_STRICT)
		{
			if (sc->distance < so->previousDistance)
				continue;

			so->previousDistance = sc->distance;
		}

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
