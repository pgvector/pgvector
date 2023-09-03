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
	HnswElement entryPoint = HnswGetEntryPoint(index);

	if (entryPoint == NULL)
		return NIL;

	ep = list_make1(HnswEntryCandidate(entryPoint, q, index, procinfo, collation, false));

	for (int lc = entryPoint->level; lc >= 1; lc--)
	{
		w = HnswSearchLayer(q, ep, 1, lc, index, procinfo, collation, false, NULL);
		ep = w;
	}

	return HnswSearchLayer(q, ep, hnsw_ef_search, 0, index, procinfo, collation, false, NULL);
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
 * Remove deleted heap TID
 */
static void
RemoveHeapTid(IndexScanDesc scan)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	Buffer		buf = so->buf;
	Page		page;
	GenericXLogState *state;
	ItemId		itemid;
	HnswElementTuple etup;
	Size		etupSize;
	int			idx = -1;

	if (!BufferIsValid(buf) || !OffsetNumberIsValid(so->offno) || !ItemPointerIsValid(&so->heaptid))
		return;

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	itemid = PageGetItemId(page, so->offno);
	etup = (HnswElementTuple) PageGetItem(page, itemid);
	etupSize = ItemIdGetLength(itemid);

	Assert(HnswIsElementTuple(etup));

	/* Find index */
	for (int i = 0; i < HNSW_HEAPTIDS; i++)
	{
		if (!ItemPointerIsValid(&etup->heaptids[i]))
			break;

		if (ItemPointerEquals(&etup->heaptids[i], &so->heaptid))
		{
			idx = i;
			break;
		}
	}

	if (idx == -1)
		GenericXLogAbort(state);
	else
	{
		/* Move pointers forward */
		for (int i = idx; i < HNSW_HEAPTIDS; i++)
		{
			if (i + 1 == HNSW_HEAPTIDS || !ItemPointerIsValid(&etup->heaptids[i + 1]))
				ItemPointerSetInvalid(&etup->heaptids[i]);
			else
				ItemPointerCopy(&etup->heaptids[i + 1], &etup->heaptids[i]);
		}

		/* Overwrite tuple */
		if (!PageIndexTupleOverwrite(page, so->offno, (Item) etup, etupSize))
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		/* Commit */
		MarkBufferDirty(buf);
		GenericXLogFinish(state);
	}

	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
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
	so->buf = InvalidBuffer;
	ItemPointerSetInvalid(&so->heaptid);
	so->offno = InvalidOffsetNumber;
	so->removedCount = 0;
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

	so->first = true;
	ItemPointerSetInvalid(&so->heaptid);
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

		/*
		 * Get a shared lock. This allows vacuum to ensure no in-flight scans
		 * before marking tuples as deleted.
		 */
		LockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

		so->w = GetScanItems(scan, value);

		/* Release shared lock */
		UnlockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

		so->first = false;
	}
	else
	{
		/*
		 * Remove dead tuples. kill_prior_tuple will only be true if not in
		 * recovery. Limit the number removed per scan for performance.
		 */
		if (scan->kill_prior_tuple && so->removedCount < 3)
		{
			RemoveHeapTid(scan);
			so->removedCount++;
		}
	}

	while (list_length(so->w) > 0)
	{
		HnswCandidate *hc = llast(so->w);
		ItemPointer tid;
		BlockNumber indexblkno;
		OffsetNumber indexoffno;

		/* Move to next element if no valid heap tids */
		if (list_length(hc->element->heaptids) == 0)
		{
			so->w = list_delete_last(so->w);
			continue;
		}

		tid = llast(hc->element->heaptids);
		indexblkno = hc->element->blkno;
		indexoffno = hc->element->offno;

		hc->element->heaptids = list_delete_last(hc->element->heaptids);

		MemoryContextSwitchTo(oldCtx);

#if PG_VERSION_NUM >= 120000
		scan->xs_heaptid = *tid;
#else
		scan->xs_ctup.t_self = *tid;
#endif

		/* Keep track of info needed to remove dead tuples */
		so->heaptid = *tid;
		so->offno = indexoffno;

		/* Unpin buffer */
		if (BufferIsValid(so->buf))
			ReleaseBuffer(so->buf);

		/*
		 * An index scan must maintain a pin on the index page holding the
		 * item last returned by amgettuple
		 *
		 * https://www.postgresql.org/docs/current/index-locking.html
		 */
		so->buf = ReadBuffer(scan->indexRelation, indexblkno);

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

	/* Release pin */
	if (BufferIsValid(so->buf))
		ReleaseBuffer(so->buf);

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}
