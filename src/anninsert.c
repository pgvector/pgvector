#include "postgres.h"

#include "access/generic_xlog.h"
#include "ann.h"
#include "miscadmin.h"
#include "storage/freespace.h"
#include "utils/memutils.h"

/*
 * The metapage serializes insertion. VACUUM only locks data pages, so it never
 * reverses this lock order. Free-space-map entries are hints, not authority.
 */
void
AnnInsertTuple(Relation index, Datum *values, bool *isnull, ItemPointer tid, bool building)
{
	Buffer		metaBuf = ReadBuffer(index, ANN_METAPAGE_BLKNO);
	Buffer		buf;
	Page		metaPage;
	Page		page;
	BlockNumber target;
	GenericXLogState *state = NULL;
	IndexTuple	tuple;
	Size		size;
	Size		freeSpace;
	Datum		tupleValues[1];
	bool		newPage = false;

	LockBuffer(metaBuf, BUFFER_LOCK_EXCLUSIVE);
	metaPage = BufferGetPage(metaBuf);
	AnnCheckMetaPage(metaPage);
	tupleValues[0] = isnull[0] ? (Datum) 0 : values[0];
	if (!isnull[0])
	{
		Vector	   *vector = DatumGetVector(values[0]);

		if (vector->dim != (int) AnnPageGetMeta(metaPage)->dimensions)
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("different vector dimensions")));
		tupleValues[0] = PointerGetDatum(vector);
	}
	tuple = index_form_tuple(RelationGetDescr(index), tupleValues, isnull);
	tuple->t_tid = *tid;
	size = IndexTupleSize(tuple);
	if (size > BLCKSZ - SizeOfPageHeaderData - MAXALIGN(sizeof(AnnPageOpaqueData)) - sizeof(ItemIdData))
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("ann index tuple is too large")));
	target = AnnPageGetMeta(metaPage)->insertPage;
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		if (!BlockNumberIsValid(target))
		{
			buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			newPage = true;
			break;
		}
		buf = ReadBuffer(index, target);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		if (PageIsNew(page))
		{
			newPage = true;
			break;
		}
		AnnCheckPage(page);
		freeSpace = PageGetFreeSpace(page);
		if (freeSpace >= size)
			break;
		UnlockReleaseBuffer(buf);
		target = RecordAndGetPageWithFreeSpace(index, target, freeSpace, size);
	}
	if (!building)
	{
		state = GenericXLogStart(index);
		metaPage = GenericXLogRegisterBuffer(state, metaBuf, 0);
		page = GenericXLogRegisterBuffer(state, buf, newPage ? GENERIC_XLOG_FULL_IMAGE : 0);
	}
	else
		page = BufferGetPage(buf);
	if (newPage)
	{
		PageInit(page, BufferGetPageSize(buf), sizeof(AnnPageOpaqueData));
		AnnPageGetOpaque(page)->page_id = ANN_PAGE_ID;
	}
	if (PageAddItem(page, (char *) tuple, size, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add ann index tuple");
	AnnPageGetMeta(metaPage)->insertPage = BufferGetBlockNumber(buf);
	freeSpace = PageGetFreeSpace(page);
	target = BufferGetBlockNumber(buf);
	if (building)
	{
		MarkBufferDirty(buf);
		MarkBufferDirty(metaBuf);
	}
	else
		GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
	UnlockReleaseBuffer(metaBuf);
	RecordPageWithFreeSpace(index, target, freeSpace);
	pfree(tuple);
}

bool
anninsert(Relation index, Datum *values, bool *isnull, ItemPointer tid,
		  Relation heap, IndexUniqueCheck checkUnique, bool indexUnchanged, IndexInfo *indexInfo)
{
	MemoryContext ctx = AllocSetContextCreate(CurrentMemoryContext, "Ann insert temporary context", ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldCtx = MemoryContextSwitchTo(ctx);

	AnnInsertTuple(index, values, isnull, tid, false);
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(ctx);
	return false;
}
