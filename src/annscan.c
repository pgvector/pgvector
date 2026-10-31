#include "postgres.h"

#include "ann.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"

typedef struct AnnScanOpaqueData
{
	bool		first;
	FmgrInfo   *procinfo;
	TupleDesc	tupdesc;
	TupleTableSlot *input;
	TupleTableSlot *output;
	Tuplesortstate *sortstate;
	MemoryContext tmpCtx;
} AnnScanOpaqueData;

typedef AnnScanOpaqueData *AnnScanOpaque;

static void
GetScanItems(IndexScanDesc scan)
{
	AnnScanOpaque so = scan->opaque;
	Relation	index = scan->indexRelation;
	BlockNumber blocks;
	Buffer		buf;
	Datum		query = scan->orderByData[0].sk_argument;
	bool		queryIsNull = (scan->orderByData[0].sk_flags & SK_ISNULL) != 0;

	buf = ReadBuffer(index, ANN_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	AnnCheckMetaPage(BufferGetPage(buf));
	UnlockReleaseBuffer(buf);
	blocks = RelationGetNumberOfBlocks(index);
	for (BlockNumber blkno = 1; blkno < blocks; blkno++)
	{
		Page		page;
		OffsetNumber maxoff;

		CHECK_FOR_INTERRUPTS();
		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		/* An aborted extension can leave an unpublished, empty physical page. */
		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}
		AnnCheckPage(page);
		maxoff = PageGetMaxOffsetNumber(page);
		for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoff; offno++)
		{
			ItemId		itemid = PageGetItemId(page, offno);
			IndexTuple	tuple;
			bool		isnull;
			Datum		value;
			MemoryContext oldCtx;

			if (!ItemIdIsNormal(itemid))
				continue;
			tuple = (IndexTuple) PageGetItem(page, itemid);
			oldCtx = MemoryContextSwitchTo(so->tmpCtx);
			value = index_getattr(tuple, 1, RelationGetDescr(index), &isnull);
			ExecClearTuple(so->input);
			so->input->tts_isnull[0] = isnull || queryIsNull;
			so->input->tts_values[0] = (isnull || queryIsNull) ? (Datum) 0 :
				FunctionCall2Coll(so->procinfo, index->rd_indcollation[0],
								  PointerGetDatum(DatumGetVector(value)), query);
			so->input->tts_isnull[1] = false;
			so->input->tts_values[1] = PointerGetDatum(&tuple->t_tid);
			ExecStoreVirtualTuple(so->input);
			MemoryContextSwitchTo(oldCtx);
			tuplesort_puttupleslot(so->sortstate, so->input);
			ExecClearTuple(so->input);
			MemoryContextReset(so->tmpCtx);
		}
		UnlockReleaseBuffer(buf);
	}
	tuplesort_performsort(so->sortstate);
}

IndexScanDesc
annbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(index, nkeys, norderbys);
	AnnScanOpaque so = palloc0_object(AnnScanOpaqueData);
	AttrNumber	attNums[] = {1};
	Oid			operators[] = {Float8LessOperator};
	Oid			collations[] = {InvalidOid};
	bool		nullsFirst[] = {false};

	so->first = true;
	so->procinfo = index_getprocinfo(index, 1, ANN_DISTANCE_PROC);
	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext, "Ann scan temporary context", ALLOCSET_DEFAULT_SIZES);
	so->tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(so->tupdesc, 1, "distance", FLOAT8OID, -1, 0);
	TupleDescInitEntry(so->tupdesc, 2, "heaptid", TIDOID, -1, 0);
#if PG_VERSION_NUM >= 190000
	TupleDescFinalize(so->tupdesc);
#endif
	so->input = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
	so->output = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);
	so->sortstate = tuplesort_begin_heap(so->tupdesc, 1, attNums, operators, collations,
										 nullsFirst, work_mem, NULL, false);
	scan->opaque = so;
	return scan;
}

void
annrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	AnnScanOpaque so = scan->opaque;

	so->first = true;
	ExecClearTuple(so->output);
	tuplesort_reset(so->sortstate);
	MemoryContextReset(so->tmpCtx);
	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));
	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
}

bool
anngettuple(IndexScanDesc scan, ScanDirection dir)
{
	AnnScanOpaque so = scan->opaque;
	Datum		value;
	bool		isnull;

	Assert(ScanDirectionIsForward(dir));
	if (so->first)
	{
		if (scan->numberOfOrderBys != 1 || !scan->orderByData)
			elog(ERROR, "cannot scan ann index without one distance order");
		if (!IsMVCCSnapshot(scan->xs_snapshot))
			elog(ERROR, "non-MVCC snapshots are not supported with ann");
		pgstat_count_index_scan(scan->indexRelation);
		GetScanItems(scan);
		so->first = false;
	}
	if (!tuplesort_gettupleslot(so->sortstate, true, false, so->output, NULL))
		return false;
	value = slot_getattr(so->output, 2, &isnull);
	Assert(!isnull);
	scan->xs_heaptid = *DatumGetItemPointer(value);
	scan->xs_recheck = false;
	scan->xs_recheckorderby = false;
	return true;
}

void
annendscan(IndexScanDesc scan)
{
	AnnScanOpaque so = scan->opaque;

	ExecDropSingleTupleTableSlot(so->input);
	ExecDropSingleTupleTableSlot(so->output);
	tuplesort_end(so->sortstate);
	FreeTupleDesc(so->tupdesc);
	MemoryContextDelete(so->tmpCtx);
	pfree(so);
	scan->opaque = NULL;
}
