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

	/* Get m and entry point */
	HnswGetMetaPageInfo(index, &m, &entryPoint);

	if (entryPoint == NULL)
		return NIL;

	ep = list_make1(HnswEntryCandidate(entryPoint, q, index, procinfo, collation, false));

	for (int lc = entryPoint->level; lc >= 1; lc--)
	{
		w = HnswSearchLayer(q, ep, 1, lc, index, procinfo, collation, m, false, NULL);
		ep = w;
	}

	return HnswSearchLayer(q, ep, so->ef_search, 0, index, procinfo, collation, m, false, NULL);
}

/*
 * Get dimensions from metapage
 */
static int
GetDimensions(Relation index)
{
	Buffer		buf;
	Page		page;
	HnswMetaPage metap;
	int			dimensions;

	buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = HnswPageGetMeta(page);

	dimensions = metap->dimensions;

	UnlockReleaseBuffer(buf);

	return dimensions;
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
		value = PointerGetDatum(InitVector(GetDimensions(scan->indexRelation)));
	else
	{
		value = scan->orderByData->sk_argument;

		/* Value should not be compressed or toasted */
		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		/* Fine if normalization fails */
		if (so->normprocinfo != NULL)
			HnswNormValue(so->normprocinfo, so->collation, &value, NULL);
	}

	return value;
}


static void
FilterResults(List* items, ItemPointer results, size_t n_results)
{
	ListCell *c1, *c2;
	foreach (c1, items)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(c1);
		foreach (c2, hc->element->heaptids)
		{
			ItemPointer heaptid = (ItemPointer) lfirst(c2);
			if (bsearch(heaptid, results, n_results, sizeof(ItemPointerData), (int (*)(const void *, const void *))ItemPointerCompare))
			{
				hc->element->heaptids = foreach_delete_current(hc->element->heaptids, c2);
			}
		}
	}
}

static size_t
CountResults(List* items)
{
	ListCell *c1;
	size_t count = 0;
	foreach (c1, items)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(c1);
		count += list_length(hc->element->heaptids);
	}
	return count;
}


static void
ExtractResults(List* items, ItemPointer results)
{
	ListCell *c1, *c2;
	foreach (c1, items)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(c1);
		foreach (c2, hc->element->heaptids)
		{
			ItemPointer heaptid = (ItemPointer) lfirst(c2);
			*results++ = *heaptid;
		}
	}
}

static void
FreeItems(List* items)
{
	ListCell *c1;
	foreach (c1, items)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(c1);
		list_free(hc->element->heaptids);
	}
	list_free(items);
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
	so->first = true;
	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Hnsw scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	/* Set support functions */
	so->procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
	so->normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
	so->collation = index->rd_indcollation[0];

	so->results = NULL;
	so->n_results = 0;
	so->ef_search = hnsw_ef_search;
	so->has_more_results = false;

	scan->opaque = so;

	/*
	 * Get a shared lock. This allows vacuum to ensure no in-flight scans
	 * before marking tuples as deleted.
	 */
	LockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

	return scan;
}

/*
 * Start or restart an index scan
 */
void
hnswrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;

	so->first = true;
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
	Datum		value;
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	/*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
	Assert(ScanDirectionIsForward(dir));

	if (so->first)
	{
		/* Count index scan for stats */
		pgstat_count_index_scan(scan->indexRelation);

		/* Safety check */
		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan hnsw index without order");

		so->first = false;

	  SearchMore:
		/* Get scan value */
		value = GetScanValue(scan);

		so->w = GetScanItems(scan, value);
		so->has_more_results = list_length(so->w) >= so->ef_search;
		if (so->results)
		{
			/* Sort for binary search */
			pg_qsort(so->results, so->n_results, sizeof(ItemPointerData), (int (*)(const void *, const void *))ItemPointerCompare);
			FilterResults(so->w, so->results, so->n_results);
		}
		if (so->has_more_results)
		{
			size_t more_results = CountResults(so->w);
			so->results = so->results
				? repalloc(so->results, (so->n_results + more_results) * sizeof(ItemPointerData))
				: palloc(more_results * sizeof(ItemPointerData));
			ExtractResults(so->w, so->results + so->n_results);
			so->n_results += more_results;
		}
	}

	while (list_length(so->w) > 0)
	{
		HnswCandidate *hc = llast(so->w);
		ItemPointer heaptid;

		/* Move to next element if no valid heap TIDs */
		if (list_length(hc->element->heaptids) == 0)
		{
			so->w = list_delete_last(so->w);
			continue;
		}

		heaptid = llast(hc->element->heaptids);

		hc->element->heaptids = list_delete_last(hc->element->heaptids);

		MemoryContextSwitchTo(oldCtx);

#if PG_VERSION_NUM >= 120000
		scan->xs_heaptid = *heaptid;
#else
		scan->xs_ctup.t_self = *heaptid;
#endif

		/*
		 * Typically, an index scan must maintain a pin on the index page
		 * holding the item last returned by amgettuple. However, this is not
		 * needed with the current vacuum strategy, which ensures scans do not
		 * visit tuples in danger of being marked as deleted.
		 *
		 * https://www.postgresql.org/docs/current/index-locking.html
		 */

		scan->xs_recheckorderby = false;
		return true;
	}

	/* Try to search more condidates */
	if (so->has_more_results)
	{
		so->ef_search *= 2;
		FreeItems(so->w);
		goto SearchMore;
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

	/* Release shared lock */
	UnlockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}
