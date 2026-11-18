/*
 * Binary heap based on Postgres implementation
 */

#include "postgres.h"

#include <float.h>

#include "access/genam.h"
#include "access/itup.h"
#include "access/relscan.h"
#include "access/tupdesc.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "lib/pairingheap.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"

#ifdef IVFFLAT_USE_READ_STREAM
#include "storage/read_stream.h"
#endif

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#define IVFFLAT_SORT_MEMORY (64 * 1024)

#define GetScanList(ptr) pairingheap_container(IvfflatScanList, ph_node, ptr)
#define GetScanListConst(ptr) pairingheap_const_container(IvfflatScanList, ph_node, ptr)

/*
 * Compare list distances
 */
static int
CompareLists(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (GetScanListConst(a)->distance > GetScanListConst(b)->distance)
		return 1;

	if (GetScanListConst(a)->distance < GetScanListConst(b)->distance)
		return -1;

	return 0;
}

/*
 * Compare list distances for sort
 */
static int
CompareListsSort(const void *a, const void *b)
{
	if (((const IvfflatScanList *) a)->distance < ((const IvfflatScanList *) b)->distance)
		return -1;

	if (((const IvfflatScanList *) a)->distance > ((const IvfflatScanList *) b)->distance)
		return 1;

	return 0;
}

/*
 * Initialize binary heap
 */
static void
BinaryHeapInit(IvfflatBinaryHeap * heap, int capacity)
{
	heap->nodes = palloc_array_checked(IvfflatBinaryNode, capacity);
	heap->length = 0;
	heap->capacity = capacity;
}

/*
 * Add unordered - O(1)
 */
static void
BinaryHeapAddUnordered(IvfflatBinaryHeap * heap, IvfflatBinaryNode node)
{
	/* Safety check */
	if (heap->length >= heap->capacity)
		elog(ERROR, "heap full");

	heap->nodes[heap->length++] = node;
}

/*
 * Grow binary heap
 */
static void
BinaryHeapGrow(IvfflatBinaryHeap * heap, int newCapacity)
{
	if (newCapacity > heap->capacity)
	{
		heap->nodes = repalloc_array_checked(heap->nodes, IvfflatBinaryNode, newCapacity);
		heap->capacity = newCapacity;
	}
}

/*
 * Sift down - O(log n)
 */
static void
BinaryHeapSiftDown(IvfflatBinaryHeap * heap, int offset)
{
	IvfflatBinaryNode node = heap->nodes[offset];

	for (;;)
	{
		int			left = 2 * offset + 1;
		int			right = 2 * offset + 2;
		int			swap = left;

		if (right < heap->length && heap->nodes[left].distance >= heap->nodes[right].distance)
			swap = right;

		if (left >= heap->length || node.distance < heap->nodes[swap].distance)
			break;

		heap->nodes[offset] = heap->nodes[swap];
		offset = swap;
	}

	heap->nodes[offset] = node;
}

/*
 * Build binary heap - O(n)
 */
static void
BinaryHeapBuild(IvfflatBinaryHeap * heap)
{
	for (int i = heap->length / 2 - 1; i >= 0; i--)
		BinaryHeapSiftDown(heap, i);
}

/*
 * Remove first from binary heap - O(log n)
 */
static IvfflatBinaryNode
BinaryHeapRemoveFirst(IvfflatBinaryHeap * heap)
{
	IvfflatBinaryNode node;

	/* Safety check */
	if (heap->length < 1)
		elog(ERROR, "heap empty");

	node = heap->nodes[0];
	heap->nodes[0] = heap->nodes[--heap->length];
	BinaryHeapSiftDown(heap, 0);
	return node;
}

/*
 * Reset binary heap
 */
static void
BinaryHeapReset(IvfflatBinaryHeap * heap)
{
	heap->length = 0;
}

/*
 * Get lists and sort by distance
 */
static void
GetScanLists(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	BlockNumber nextblkno = IVFFLAT_HEAD_BLKNO;
	double		maxDistance = DBL_MAX;
	IvfflatScanList *prevscanlist = NULL;

	so->listsCount = 0;

	/* Search all list pages */
	while (BlockNumberIsValid(nextblkno))
	{
		Buffer		cbuf;
		Page		cpage;
		OffsetNumber maxoffno;

		cbuf = ReadBuffer(scan->indexRelation, nextblkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		maxoffno = PageGetMaxOffsetNumber(cpage);

		for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			IvfflatList list = (IvfflatList) PageGetItem(cpage, PageGetItemId(cpage, offno));
			IvfflatScanList *scanlist = NULL;
			double		distance;

			/* Use procinfo from the index instead of scan key for performance */
			distance = DatumGetFloat8(so->distfunc(so->procinfo, so->collation, PointerGetDatum(&list->center), value));

			if (prevscanlist != NULL)
				prevscanlist->lastContiguousPage = list->startPage - 1;

			if (so->listsCount < so->maxProbes)
			{
				scanlist = &so->lists[so->listsCount++];
				scanlist->startPage = list->startPage;
				scanlist->lastContiguousPage = list->startPage;
				scanlist->distance = distance;

				/* Add to heap */
				pairingheap_add(so->listsHeap, &scanlist->ph_node);

				/* Calculate max distance */
				if (so->listsCount == so->maxProbes)
					maxDistance = GetScanList(pairingheap_first(so->listsHeap))->distance;
			}
			else if (distance < maxDistance)
			{
				/* Remove */
				scanlist = GetScanList(pairingheap_remove_first(so->listsHeap));

				/* Reuse */
				scanlist->startPage = list->startPage;
				scanlist->lastContiguousPage = list->startPage;
				scanlist->distance = distance;
				pairingheap_add(so->listsHeap, &scanlist->ph_node);

				/* Update max distance */
				maxDistance = GetScanList(pairingheap_first(so->listsHeap))->distance;
			}

			prevscanlist = scanlist;
		}

		nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

		UnlockReleaseBuffer(cbuf);
	}

	Assert(so->listsCount == so->maxProbes);

	qsort(so->lists, so->listsCount, sizeof(IvfflatScanList), CompareListsSort);
}

/*
 * Initialize scan sort state
 */
static void
InitScanSortState(IvfflatScanOpaque so)
{
	AttrNumber	attNums[] = {1};
	Oid			sortOperators[] = {Float8LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};
	MemoryContext oldCtx;

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	/* Create tuple description for sorting */
	so->tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "heaptid", TIDOID, -1, 0);
#if PG_VERSION_NUM >= 190000
	TupleDescFinalize(so->tupdesc);
#endif

	/* Prep sort */
	so->sortstate = tuplesort_begin_heap(so->tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, IVFFLAT_SORT_MEMORY / 1024, NULL, TUPLESORT_NONE);

	/* Need separate slots for puttuple and gettuple */
	so->vslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
	so->mslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);

	MemoryContextSwitchTo(oldCtx);
}

/*
 * Add sort item
 */
static inline void
AddSortItem(IvfflatScanOpaque so, double distance, ItemPointer heaptid)
{
	TupleTableSlot *slot = so->vslot;

	/* Add virtual tuple */
	ExecClearTuple(slot);
	slot->tts_values[0] = Float8GetDatum(distance);
	slot->tts_isnull[0] = false;
	slot->tts_values[1] = PointerGetDatum(heaptid);
	slot->tts_isnull[1] = false;
	ExecStoreVirtualTuple(slot);

	tuplesort_puttupleslot(so->sortstate, slot);
}

/*
 * Add scan item
 */
static void
AddScanItem(IvfflatScanOpaque so, double distance, ItemPointer heaptid)
{
	IvfflatBinaryHeap *heap = &so->itemsHeap;

	if (so->useHeap)
	{
		if (heap->length < heap->capacity)
		{
			IvfflatBinaryNode node = {*heaptid, distance};

			BinaryHeapAddUnordered(heap, node);
		}
		else if (heap->capacity < so->maxHeapItems)
		{
			IvfflatBinaryNode node = {*heaptid, distance};

			/* Avoid overflow */
			int			newCapacity = heap->capacity < so->maxHeapItems / 2 ? heap->capacity * 2 : so->maxHeapItems;

			BinaryHeapGrow(heap, newCapacity);
			BinaryHeapAddUnordered(heap, node);
		}
		else
		{
			if (so->sortstate == NULL)
				InitScanSortState(so);

			/* Move items to sort */
			for (int i = 0; i < heap->length; i++)
				AddSortItem(so, heap->nodes[i].distance, &heap->nodes[i].heaptid);

			AddSortItem(so, distance, heaptid);
			so->useHeap = false;

			elog(DEBUG1, "ivfflat scan no longer fits into work_mem after %d tuples", heap->length);
		}
	}
	else
		AddSortItem(so, distance, heaptid);
}

/*
 * Get page items
 */
static void
GetScanPageItems(IndexScanDesc scan, Datum value, Page page)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	OffsetNumber maxoffno;

	maxoffno = PageGetMaxOffsetNumber(page);

	for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
	{
		IndexTuple	itup;
		Datum		datum;
		bool		isnull;
		double		distance;

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offno));
		datum = index_getattr(itup, 1, tupdesc, &isnull);

		/* Use procinfo from the index instead of scan key for performance */
		distance = DatumGetFloat8(so->distfunc(so->procinfo, so->collation, datum, value));

		AddScanItem(so, distance, &itup->t_tid);
	}
}

/*
 * Get items
 */
static void
GetScanItems(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	int			batchProbes = 0;

	so->useHeap = true;
	BinaryHeapReset(&so->itemsHeap);
	if (so->sortstate != NULL)
		tuplesort_reset(so->sortstate);

	/* Search closest probes lists */
	while (so->listsVisited < so->listsCount && batchProbes++ < so->probes)
	{
		IvfflatScanList *scanlist = &so->lists[so->listsVisited++];
		BlockNumber searchPage;

#ifdef IVFFLAT_USE_READ_STREAM
		searchPage = InvalidBlockNumber;
		so->searchPage = scanlist->startPage;
		so->lastContiguousPage = scanlist->lastContiguousPage;
#if PG_VERSION_NUM >= 190000
		read_stream_resume(so->stream);
#else
		read_stream_reset(so->stream);
#endif

		/* Search contiguous entry pages for list */
		for (;;)
		{
			Buffer		buf;
			Page		page;

			buf = read_stream_next_buffer(so->stream, NULL);
			if (!BufferIsValid(buf))
				break;

			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			GetScanPageItems(scan, value, page);

			if (BufferGetBlockNumber(buf) == scanlist->lastContiguousPage)
				searchPage = IvfflatPageGetOpaque(page)->nextblkno;

			UnlockReleaseBuffer(buf);
		}
#else
		searchPage = scanlist->startPage;
#endif

		/* Search remaining entry pages for list */
		while (BlockNumberIsValid(searchPage))
		{
			Buffer		buf;
			Page		page;

			buf = ReadBuffer(scan->indexRelation, searchPage);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			GetScanPageItems(scan, value, page);

			searchPage = IvfflatPageGetOpaque(page)->nextblkno;

			UnlockReleaseBuffer(buf);
		}
	}

	if (so->useHeap)
		BinaryHeapBuild(&so->itemsHeap);
	else
		tuplesort_performsort(so->sortstate);

#if defined(IVFFLAT_MEMORY)
	elog(INFO, "memory: %zu MB", MemoryContextMemAllocated(CurrentMemoryContext, true) / (1024 * 1024));
#endif
}

/*
 * Zero distance
 */
static Datum
ZeroDistance(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
{
	return Float8GetDatum(0.0);
}

/*
 * Get scan value
 */
static Datum
GetScanValue(IndexScanDesc scan)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	Datum		value;

	if (scan->orderByData->sk_flags & SK_ISNULL)
	{
		value = PointerGetDatum(NULL);
		so->distfunc = ZeroDistance;
	}
	else
	{
		value = scan->orderByData->sk_argument;
		so->distfunc = FunctionCall2Coll;

		/* Value should not be compressed or toasted */
		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		/* Normalize if needed */
		if (so->normprocinfo != NULL)
		{
			MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

			value = IvfflatNormValue(so->typeInfo, so->collation, value);

			MemoryContextSwitchTo(oldCtx);
		}

		/* Check dimensions match index */
		IvfflatCheckDim(so->dimensions, so->typeInfo, so->collation, value);
	}

	return value;
}

/*
 * Compute max heap items
 */
static int
ComputeMaxHeapItems(Size memory)
{
	Size		maxItems;

	if (memory < IVFFLAT_SORT_MEMORY)
		return 1;

	maxItems = Min(memory - IVFFLAT_SORT_MEMORY, MaxAllocSize) / sizeof(IvfflatBinaryNode);

	if (maxItems < 1)
		return 1;

	return Min(maxItems, INT_MAX);
}

#ifdef IVFFLAT_USE_READ_STREAM
/*
 * Get next block number for read stream
 */
static BlockNumber
IvfflatReadStreamNextBlock(ReadStream *stream, void *callback_private_data, void *per_buffer_data)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) callback_private_data;

	if (so->searchPage > so->lastContiguousPage)
	{
#if PG_VERSION_NUM >= 190000
		return read_stream_pause(stream);
#else
		return InvalidBlockNumber;
#endif
	}

	return so->searchPage++;
}
#endif

/*
 * Get next scan item
 */
static inline bool
IvfflatNextScanItem(IvfflatScanOpaque so, ItemPointerData *heaptid)
{
	if (so->useHeap)
	{
		IvfflatBinaryHeap *heap = &so->itemsHeap;

		if (heap->length < 1)
			return false;

		*heaptid = BinaryHeapRemoveFirst(heap).heaptid;
		return true;
	}
	else
	{
		bool		isnull;

		if (!tuplesort_gettupleslot(so->sortstate, true, false, so->mslot, NULL))
			return false;

		*heaptid = *((ItemPointer) DatumGetPointer(slot_getattr(so->mslot, 2, &isnull)));
		return true;
	}
}

/*
 * Prepare for an index scan
 */
IndexScanDesc
ivfflatbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	IvfflatScanOpaque so;
	int			lists;
	int			dimensions;
	int			probes = ivfflat_probes;
	int			maxProbes;
	MemoryContext oldCtx;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	/* Get lists and dimensions from metapage */
	IvfflatGetMetaPageInfo(index, &lists, &dimensions);

	if (ivfflat_iterative_scan != IVFFLAT_ITERATIVE_SCAN_OFF)
		maxProbes = Max(ivfflat_max_probes, probes);
	else
		maxProbes = probes;

	if (probes > lists)
		probes = lists;

	if (maxProbes > lists)
		maxProbes = lists;

	so = palloc_object(IvfflatScanOpaqueData);
	so->typeInfo = IvfflatGetTypeInfo(index);
	so->first = true;
	so->probes = probes;
	so->maxProbes = maxProbes;
	so->dimensions = dimensions;
	so->value = PointerGetDatum(NULL);

	/* Set support functions */
	so->procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
	so->normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	so->collation = index->rd_indcollation[0];

	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Ivfflat scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	so->lists = palloc_array_checked(IvfflatScanList, maxProbes);
	so->listsCount = 0;
	so->listsVisited = 0;
	so->listsHeap = pairingheap_allocate(CompareLists, NULL);

	so->useHeap = true;
	so->maxHeapItems = ComputeMaxHeapItems(mul_size(work_mem, 1024));
	BinaryHeapInit(&so->itemsHeap, Min(32 * 1024, so->maxHeapItems));
	so->sortstate = NULL;

#ifdef IVFFLAT_USE_READ_STREAM
	so->stream = read_stream_begin_relation(READ_STREAM_DEFAULT, NULL, index, MAIN_FORKNUM, IvfflatReadStreamNextBlock, so, 0);
#endif

	MemoryContextSwitchTo(oldCtx);

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

	so->first = true;
	pairingheap_reset(so->listsHeap);
	so->listsCount = 0;
	so->listsVisited = 0;

	if (so->normprocinfo != NULL && DatumGetPointer(so->value) != NULL)
	{
		pfree(DatumGetPointer(so->value));
		so->value = PointerGetDatum(NULL);
	}

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

	if (so->first)
	{
		Datum		value;

		/* Count index scan for stats */
		pgstat_count_index_scan(scan->indexRelation);
#if PG_VERSION_NUM >= 180000
		if (scan->instrument)
			scan->instrument->nsearches++;
#endif

		/* Safety check */
		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan ivfflat index without order");

		/* Requires MVCC-compliant snapshot as not able to pin during sorting */
		/* https://www.postgresql.org/docs/current/index-locking.html */
		if (!IsMVCCSnapshot(scan->xs_snapshot))
			elog(ERROR, "non-MVCC snapshots are not supported with ivfflat");

		value = GetScanValue(scan);
		IvfflatBench("GetScanLists", GetScanLists(scan, value));
		IvfflatBench("GetScanItems", GetScanItems(scan, value));
		so->first = false;
		so->value = value;
	}

	while (!IvfflatNextScanItem(so, &scan->xs_heaptid))
	{
		if (so->listsVisited == so->listsCount)
			return false;

		IvfflatBench("GetScanItems", GetScanItems(scan, so->value));
	}

	scan->xs_recheck = false;
	scan->xs_recheckorderby = false;
	return true;
}

/*
 * End a scan and release resources
 */
void
ivfflatendscan(IndexScanDesc scan)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;

	/* Free any temporary files */
	if (so->sortstate != NULL)
		tuplesort_end(so->sortstate);

#ifdef IVFFLAT_USE_READ_STREAM
	read_stream_end(so->stream);
#endif

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}
