#include "postgres.h"

#include <float.h>

#include "ivfflat.h"
#include "storage/bufmgr.h"

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

			if (distance < minDistance)
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
 * Prepare to insert an index tuple
 */
static void
LoadInsertPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, BlockNumber insertPage)
{
	*buf = ReadBuffer(index, insertPage);
	LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, 0);
}

/*
 * Insert a tuple into the index
 */
static void
InsertTuple(Relation rel, IndexTuple itup, Relation heapRel, Datum *values)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		itemsz;
	BlockNumber insertPage = InvalidBlockNumber;
	ListInfo	listInfo;
	BlockNumber originalInsertPage;

	/* Find the insert page - sets the page and list info */
	FindInsertPage(rel, values, &insertPage, &listInfo);
	Assert(BlockNumberIsValid(insertPage));
	originalInsertPage = insertPage;

	itemsz = MAXALIGN(IndexTupleSize(itup));
	Assert(itemsz <= BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(IvfflatPageOpaqueData)));

	LoadInsertPage(rel, &buf, &page, &state, insertPage);

	/* Find a page to insert the item */
	while (PageGetFreeSpace(page) < itemsz)
	{
		insertPage = IvfflatPageGetOpaque(page)->nextblkno;

		if (BlockNumberIsValid(insertPage))
		{
			/* Move to next page */
			GenericXLogAbort(state);
			UnlockReleaseBuffer(buf);

			LoadInsertPage(rel, &buf, &page, &state, insertPage);
		}
		else
		{
			/* Add a new page */
			IvfflatAppendPage(rel, &buf, &page, &state, MAIN_FORKNUM);

			insertPage = BufferGetBlockNumber(buf);
		}
	}

	/* Add to next offset */
	if (PageAddItem(page, (Item) itup, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(rel));

	IvfflatCommitBuffer(buf, state);

	/* Update the insert page */
	if (insertPage != originalInsertPage)
		IvfflatUpdateList(rel, state, listInfo, insertPage, originalInsertPage, InvalidBlockNumber, MAIN_FORKNUM);
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
#if PG_VERSION_NUM >= 100000
			  ,IndexInfo *indexInfo
#endif
)
{
	IndexTuple	itup;
	Datum		value;
	FmgrInfo   *normprocinfo;

	if (isnull[0])
		return false;

	value = values[0];

	/* Normalize if needed */
	normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	if (normprocinfo != NULL)
	{
		if (!IvfflatNormValue(normprocinfo, index->rd_indcollation[0], &value, NULL))
			return false;
	}

	itup = index_form_tuple(RelationGetDescr(index), &value, isnull);
	itup->t_tid = *heap_tid;
	InsertTuple(index, itup, heap, &value);
	pfree(itup);

	/* Clean up if we allocated a new value */
	if (value != values[0])
		pfree(DatumGetPointer(value));

	return false;
}
