#include "postgres.h"

#include <float.h>

#include "access/relscan.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"

#if PG_VERSION_NUM >= 110000
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#else
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#endif

/*
 * Compare list distances
 */
static int
CompareLists(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (((const IvfflatScanList *) a)->distance > ((const IvfflatScanList *) b)->distance)
		return 1;

	if (((const IvfflatScanList *) a)->distance < ((const IvfflatScanList *) b)->distance)
		return -1;

	return 0;
}

/*
 * Compare item distances
 */
static int
CompareItems(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (((const IvfflatScanItem *) a)->distance > ((const IvfflatScanItem *) b)->distance)
		return 1;

	if (((const IvfflatScanItem *) a)->distance < ((const IvfflatScanItem *) b)->distance)
		return -1;

	return ItemPointerCompare(&((IvfflatScanItem *) a)->tid, &((IvfflatScanItem *) b)->tid);
}

/*
 * Get lists and sort by distance
 */
static void
GetScanLists(IndexScanDesc scan, Datum value)
{
	Buffer		cbuf;
	Page		cpage;
	IvfflatList list;
	OffsetNumber offno;
	OffsetNumber maxoffno;
	BlockNumber nextblkno = IVFFLAT_HEAD_BLKNO;
	int			listCount = 0;
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	int			i;
	double		distance;
	IvfflatScanList *scanlist;
	double		maxDistance = DBL_MAX;

	/* Search all list pages */
	while (BlockNumberIsValid(nextblkno))
	{
		cbuf = ReadBuffer(scan->indexRelation, nextblkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		maxoffno = PageGetMaxOffsetNumber(cpage);

		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			list = (IvfflatList) PageGetItem(cpage, PageGetItemId(cpage, offno));

			/* Use procinfo from the index instead of scan key for performance */
			distance = DatumGetFloat8(FunctionCall2Coll(so->procinfo, so->collation, PointerGetDatum(&list->center), value));

			if (listCount < so->probes)
			{
				scanlist = &so->lists[listCount];
				scanlist->startPage = list->startPage;
				scanlist->distance = distance;
				listCount++;

				/* Add to heap */
				pairingheap_add(so->listQueue, &scanlist->ph_node);

				/* Calculate max distance */
				if (listCount == so->probes)
					maxDistance = ((IvfflatScanList *) pairingheap_first(so->listQueue))->distance;
			}
			else if (distance < maxDistance)
			{
				/* Remove */
				scanlist = (IvfflatScanList *) pairingheap_remove_first(so->listQueue);

				/* Reuse */
				scanlist->startPage = list->startPage;
				scanlist->distance = distance;
				pairingheap_add(so->listQueue, &scanlist->ph_node);

				/* Update max distance */
				maxDistance = ((IvfflatScanList *) pairingheap_first(so->listQueue))->distance;
			}
		}

		nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

		UnlockReleaseBuffer(cbuf);
	}

	for (i = 0; i < so->probes; i++)
		so->sortedLists[i] = (IvfflatScanList *) pairingheap_remove_first(so->listQueue);

	Assert(pairingheap_is_empty(so->listQueue));
}

/*
 * Get items
 */
static void
GetScanItemsQuick(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	Buffer		buf;
	Page		page;
	IndexTuple	itup;
	BlockNumber searchPage;
	OffsetNumber offno;
	OffsetNumber maxoffno;
	Datum		datum;
	bool		isnull;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	int			i;
	double		distance;
	IvfflatScanItem *scanitem;
	double		maxDistance = DBL_MAX;

	/*
	 * Reuse same set of shared buffers for scan
	 *
	 * See postgres/src/backend/storage/buffer/README for description
	 */
	BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

	/* Search closest probes lists */
	for (i = 0; i < so->probes; i++)
	{
		/* Read closest lists first for performance */
		searchPage = so->sortedLists[i]->startPage;

		/* Search all entry pages for list */
		while (BlockNumberIsValid(searchPage))
		{
			buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			maxoffno = PageGetMaxOffsetNumber(page);

			for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
			{
				itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offno));
				datum = index_getattr(itup, 1, tupdesc, &isnull);
				distance = DatumGetFloat8(FunctionCall2Coll(so->procinfo, so->collation, datum, value));

				if (so->itemCount < so->maxItems)
				{
					scanitem = &so->items[so->itemCount];
					scanitem->searchPage = searchPage;
					scanitem->tid = itup->t_tid;
					scanitem->distance = distance;
					so->itemCount++;

					/* Add to heap */
					pairingheap_add(so->itemQueue, &scanitem->ph_node);

					/* Calculate max distance */
					if (so->itemCount == so->maxItems)
					{
						maxDistance = ((IvfflatScanItem *) pairingheap_first(so->itemQueue))->distance;
						scanitem = &so->items[so->itemCount];
					}
				}
				else if (distance <= maxDistance)
				{
					/* Reuse */
					scanitem->searchPage = searchPage;
					scanitem->tid = itup->t_tid;
					scanitem->distance = distance;
					pairingheap_add(so->itemQueue, &scanitem->ph_node);

					/* Remove */
					scanitem = (IvfflatScanItem *) pairingheap_remove_first(so->itemQueue);

					/* Update max distance */
					maxDistance = ((IvfflatScanItem *) pairingheap_first(so->itemQueue))->distance;
				}
			}

			searchPage = IvfflatPageGetOpaque(page)->nextblkno;

			UnlockReleaseBuffer(buf);
		}
	}

	for (i = 0; i < so->itemCount; i++)
		so->sortedItems[i] = (IvfflatScanItem *) pairingheap_remove_first(so->itemQueue);

	Assert(pairingheap_is_empty(so->itemQueue));
}

/*
 * Initialize sort
 */
static void
InitSort(IvfflatScanOpaque so)
{
	AttrNumber	attNums[] = {1, 2};
	Oid			sortOperators[] = {Float8LessOperator, TIDLessOperator};
	Oid			sortCollations[] = {InvalidOid, InvalidOid};
	bool		nullsFirstFlags[] = {false, false};

	/* Create tuple description for sorting */
#if PG_VERSION_NUM >= 120000
	so->tupdesc = CreateTemplateTupleDesc(3);
#else
	so->tupdesc = CreateTemplateTupleDesc(3, false);
#endif
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "tid", TIDOID, -1, 0);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 3, "indexblkno", INT4OID, -1, 0);

	/* Prep sort */
#if PG_VERSION_NUM >= 110000
	so->sortstate = tuplesort_begin_heap(so->tupdesc, sizeof(attNums) / sizeof(attNums[0]), attNums, sortOperators, sortCollations, nullsFirstFlags, work_mem, NULL, false);
#else
	so->sortstate = tuplesort_begin_heap(so->tupdesc, sizeof(attNums) / sizeof(attNums[0]), attNums, sortOperators, sortCollations, nullsFirstFlags, work_mem, false);
#endif

#if PG_VERSION_NUM >= 120000
	so->slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);
#else
	so->slot = MakeSingleTupleTableSlot(so->tupdesc);
#endif
}

/*
 * Get items
 */
static void
GetScanItems(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	Buffer		buf;
	Page		page;
	IndexTuple	itup;
	BlockNumber searchPage;
	OffsetNumber offno;
	OffsetNumber maxoffno;
	Datum		datum;
	bool		isnull;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	int			i;

#if PG_VERSION_NUM >= 120000
	TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
#else
	TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc);
#endif

	/*
	 * Reuse same set of shared buffers for scan
	 *
	 * See postgres/src/backend/storage/buffer/README for description
	 */
	BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

	/* Search closest probes lists */
	for (i = 0; i < so->probes; i++)
	{
		searchPage = so->sortedLists[i]->startPage;

		/* Search all entry pages for list */
		while (BlockNumberIsValid(searchPage))
		{
			buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			maxoffno = PageGetMaxOffsetNumber(page);

			for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
			{
				itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offno));
				datum = index_getattr(itup, 1, tupdesc, &isnull);

				/*
				 * Add virtual tuple
				 *
				 * Use procinfo from the index instead of scan key for
				 * performance
				 */
				ExecClearTuple(slot);
				slot->tts_values[0] = FunctionCall2Coll(so->procinfo, so->collation, datum, value);
				slot->tts_isnull[0] = false;
				slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
				slot->tts_isnull[1] = false;
				slot->tts_values[2] = Int32GetDatum((int) searchPage);
				slot->tts_isnull[2] = false;
				ExecStoreVirtualTuple(slot);

				tuplesort_puttupleslot(so->sortstate, slot);
			}

			searchPage = IvfflatPageGetOpaque(page)->nextblkno;

			UnlockReleaseBuffer(buf);
		}
	}

	tuplesort_performsort(so->sortstate);
	tuplesort_skiptuples(so->sortstate, so->maxItems, true);
}

/*
 * Prepare for an index scan
 */
IndexScanDesc
ivfflatbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(index, nkeys, norderbys);
	int			lists = IvfflatGetLists(scan->indexRelation);
	int			probes = ivfflat_probes;
	IvfflatScanOpaque so;

	if (probes > lists)
		probes = lists;

	so = (IvfflatScanOpaque) palloc(offsetof(IvfflatScanOpaqueData, lists) + probes * sizeof(IvfflatScanList));
	so->buf = InvalidBuffer;
	so->stage = 0;
	so->probes = probes;

	/* Set support functions */
	so->procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
	so->normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	so->collation = index->rd_indcollation[0];

	so->listQueue = pairingheap_allocate(CompareLists, scan);
	so->sortedLists = palloc(sizeof(IvfflatScanItem *) * probes);

	so->maxItems = 1024;
	so->itemCount = 0;
	so->itemQueue = pairingheap_allocate(CompareItems, scan);
	so->items = palloc(sizeof(IvfflatScanItem) * so->maxItems + 1);
	so->sortedItems = palloc(sizeof(IvfflatScanItem *) * so->maxItems);

	so->sortstate = NULL;

	scan->opaque = so;

	return scan;
}

/*
 * Start or restart an index scan
 */
void
ivfflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;

#if PG_VERSION_NUM >= 130000
	if (so->sortstate != NULL)
		tuplesort_reset(so->sortstate);
#endif

	so->stage = 0;
	pairingheap_reset(so->listQueue);
	pairingheap_reset(so->itemQueue);
	so->itemCount = 0;

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
}

/*
 * Fetch the next tuple in the given scan
 */
bool
ivfflatgettuple(IndexScanDesc scan, ScanDirection dir)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;

	/*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
	Assert(ScanDirectionIsForward(dir));

	if (so->stage == 0)
	{
		Datum		value;

		/* Safety check */
		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan ivfflat index without order");

		/* No items will match if null */
		if (scan->orderByData->sk_flags & SK_ISNULL)
			return false;

		value = scan->orderByData->sk_argument;

		if (so->normprocinfo != NULL)
		{
			/* No items will match if normalization fails */
			if (!IvfflatNormValue(so->normprocinfo, so->collation, &value, NULL))
				return false;
		}

		IvfflatBench("GetScanLists", GetScanLists(scan, value));
		IvfflatBench("GetScanItemsQuick", GetScanItemsQuick(scan, value));
		so->heapFull = so->itemCount == so->maxItems;
		so->stage++;

		/* Clean up if we allocated a new value */
		if (value != scan->orderByData->sk_argument)
			pfree(DatumGetPointer(value));
	}

	if (so->stage == 1)
	{
		if (so->itemCount > 0)
		{
			IvfflatScanItem *scanitem;

			so->itemCount--;

			scanitem = so->sortedItems[so->itemCount];

#if PG_VERSION_NUM >= 120000
			scan->xs_heaptid = scanitem->tid;
#else
			scan->xs_ctup.t_sef = scanitem->tid;
#endif

			if (BufferIsValid(so->buf))
				ReleaseBuffer(so->buf);

			/*
			 * An index scan must maintain a pin on the index page holding the
			 * item last returned by amgettuple
			 *
			 * https://www.postgresql.org/docs/current/index-locking.html
			 */
			so->buf = ReadBuffer(scan->indexRelation, scanitem->searchPage);

			scan->xs_recheckorderby = false;
			return true;
		}
		else if (so->heapFull)
		{
			Datum value = scan->orderByData->sk_argument;

			if (so->normprocinfo != NULL)
			{
				/* No items will match if normalization fails */
				if (!IvfflatNormValue(so->normprocinfo, so->collation, &value, NULL))
					return false;
			}

			if (so->sortstate == NULL)
				InitSort(so);

			IvfflatBench("GetScanItems", GetScanItems(scan, value));
			so->stage++;

			/* Clean up if we allocated a new value */
			if (value != scan->orderByData->sk_argument)
				pfree(DatumGetPointer(value));
		}
		else
			so->stage = 3;
	}

	if (so->stage == 2)
	{
#if PG_VERSION_NUM >= 100000
		if (tuplesort_gettupleslot(so->sortstate, true, false, so->slot, NULL))
#else
		if (tuplesort_gettupleslot(so->sortstate, true, so->slot, NULL))
#endif
		{
			ItemPointer tid = (ItemPointer) DatumGetPointer(slot_getattr(so->slot, 2, &so->isnull));
			BlockNumber indexblkno = DatumGetInt32(slot_getattr(so->slot, 3, &so->isnull));

#if PG_VERSION_NUM >= 120000
			scan->xs_heaptid = *tid;
#else
			scan->xs_ctup.t_self = *tid;
#endif

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
	}

	return false;
}

/*
 * End a scan and release resources
 */
void
ivfflatendscan(IndexScanDesc scan)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;

	/* Release pin */
	if (BufferIsValid(so->buf))
		ReleaseBuffer(so->buf);

	pairingheap_free(so->listQueue);
	pfree(so->sortedLists);

	if (so->sortstate != NULL)
		tuplesort_end(so->sortstate);

	pairingheap_free(so->itemQueue);
	pfree(so->items);
	pfree(so->sortedItems);

	pfree(so);
	scan->opaque = NULL;
}
