#include "postgres.h"

#include <float.h>

#include "ivfflat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"

/*
 * Find the list that minimizes the distance function
 */
static void
FindInsertPage(Relation rel, Datum *values, BlockNumber *insertPage, ListInfo * listInfo)
{
	Buffer		cbuf;
	Page		cpage;
	IvfflatList list;
	double		distance;
	double		minDistance = DBL_MAX;
	BlockNumber nextblkno = IVFFLAT_HEAD_BLKNO;
	FmgrInfo   *procinfo;
	Oid			collation;
	OffsetNumber offno;
	OffsetNumber maxoffno;

	/* Avoid compiler warning */
	listInfo->blkno = nextblkno;
	listInfo->offno = FirstOffsetNumber;

	procinfo = index_getprocinfo(rel, 1, IVFFLAT_DISTANCE_PROC);
	collation = rel->rd_indcollation[0];

	/* Search all list pages */
	while (BlockNumberIsValid(nextblkno))
	{
		cbuf = ReadBuffer(rel, nextblkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);
		maxoffno = PageGetMaxOffsetNumber(cpage);

		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			list = (IvfflatList) PageGetItem(cpage, PageGetItemId(cpage, offno));
			distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, values[0], PointerGetDatum(&list->center)));

			if (distance < minDistance || !BlockNumberIsValid(*insertPage))
			{
				*insertPage = list->insertPage;
				listInfo->blkno = nextblkno;
				listInfo->offno = offno;
				minDistance = distance;
			}
		}

		nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

		UnlockReleaseBuffer(cbuf);
	}
}

/*
 * Insert a tuple into the index
 */
static void
InsertTuple(Relation rel, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel)
{
	IndexTuple	itup;
	Datum		value;
	FmgrInfo   *normprocinfo;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		itemsz;
	BlockNumber insertPage = InvalidBlockNumber;
	ListInfo	listInfo;
	BlockNumber originalInsertPage;

	/* Detoast once for all calls */
	value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

	/* Normalize if needed */
	normprocinfo = IvfflatOptionalProcInfo(rel, IVFFLAT_NORM_PROC);
	if (normprocinfo != NULL)
	{
		if (!IvfflatNormValue(normprocinfo, rel->rd_indcollation[0], &value, NULL))
			return;
	}

	/* Find the insert page - sets the page and list info */
	FindInsertPage(rel, values, &insertPage, &listInfo);
	Assert(BlockNumberIsValid(insertPage));
	originalInsertPage = insertPage;

	/* Form tuple */
	itup = index_form_tuple(RelationGetDescr(rel), &value, isnull);
	itup->t_tid = *heap_tid;

	/* Get tuple size */
	itemsz = MAXALIGN(IndexTupleSize(itup));
	Assert(itemsz <= BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(IvfflatPageOpaqueData)));

	/* Find a page to insert the item */
	for (;;)
	{
		buf = ReadBuffer(rel, insertPage);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		state = GenericXLogStart(rel);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		if (PageGetFreeSpace(page) >= itemsz)
			break;

		insertPage = IvfflatPageGetOpaque(page)->nextblkno;

		if (BlockNumberIsValid(insertPage))
		{
			/* Move to next page */
			GenericXLogAbort(state);
			UnlockReleaseBuffer(buf);
		}
		else
		{
			Buffer		newbuf;
			Page		newpage;

			/* Add a new page */
			LockRelationForExtension(rel, ExclusiveLock);
			newbuf = IvfflatNewBuffer(rel, MAIN_FORKNUM);
			UnlockRelationForExtension(rel, ExclusiveLock);

			/* Init new page */
			newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);
			IvfflatInitPage(newbuf, newpage);

			/* Update insert page */
			insertPage = BufferGetBlockNumber(newbuf);

			/* Update previous buffer */
			IvfflatPageGetOpaque(page)->nextblkno = insertPage;

			/* Commit */
			MarkBufferDirty(newbuf);
			MarkBufferDirty(buf);
			GenericXLogFinish(state);

			/* Unlock previous buffer */
			UnlockReleaseBuffer(buf);

			/* Prepare new buffer */
			state = GenericXLogStart(rel);
			buf = newbuf;
			page = GenericXLogRegisterBuffer(state, buf, 0);
			break;
		}
	}

	/* Add to next offset */
	if (PageAddItem(page, (Item) itup, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(rel));

	IvfflatCommitBuffer(buf, state);

	/* Update the insert page */
	if (insertPage != originalInsertPage)
		IvfflatUpdateList(rel, listInfo, insertPage, originalInsertPage, InvalidBlockNumber, MAIN_FORKNUM);
}

/*
 * Insert a tuple into the index
 */
bool
ivfflatinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
			  Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
			  ,bool indexUnchanged
#endif
			  ,IndexInfo *indexInfo
)
{
	MemoryContext oldCtx;
	MemoryContext insertCtx;

	/* Skip nulls */
	if (isnull[0])
		return false;

	/*
	 * Use memory context since detoast, IvfflatNormValue, and
	 * index_form_tuple can allocate
	 */
	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Ivfflat insert temporary context",
									  ALLOCSET_DEFAULT_SIZES);
	oldCtx = MemoryContextSwitchTo(insertCtx);

	/* Insert tuple */
	InsertTuple(index, values, isnull, heap_tid, heap);

	/* Delete memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	return false;
}
