#include "postgres.h"

#include <math.h>

#include "hnsw.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"

/*
 * Get the insert page
 */
static BlockNumber
GetInsertPage(Relation index)
{
	Buffer		buf;
	Page		page;
	HnswMetaPage metap;
	BlockNumber insertPage;

	buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = HnswPageGetMeta(page);

	insertPage = metap->insertPage;

	UnlockReleaseBuffer(buf);

	return insertPage;
}

/*
 * Check for a free offset
 */
static bool
HnswFreeOffset(Page page, OffsetNumber *freeOffno, BlockNumber *neighborPage)
{
	OffsetNumber offno;
	OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);

	for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
	{
		HnswElementTuple item = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, offno));

		if (item->deleted)
		{
			*freeOffno = offno;
			*neighborPage = item->neighborPage;
			return true;
		}
	}

	return false;
}

/*
 * Add to element and neighbor pages
 */
static void
WriteNewElementPages(Relation index, HnswElement e, int m)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		esize;
	HnswElementTuple etup;
	BlockNumber insertPage = GetInsertPage(index);
	BlockNumber originalInsertPage = insertPage;
	int			dimensions = e->vec->dim;
	Size		nsize = MAXALIGN(sizeof(HnswNeighborTupleData));
	HnswNeighborTuple ntup = palloc0(nsize);
	Buffer		nbuf;
	Page		npage;
	OffsetNumber freeOffno = InvalidOffsetNumber;
	BlockNumber neighborPage = InvalidBlockNumber;

	/* Get tuple size */
	esize = MAXALIGN(HNSW_ELEMENT_TUPLE_SIZE(dimensions));

	/* Prepare tuple */
	etup = palloc0(esize);
	etup->heaptids[0] = *((ItemPointer) linitial(e->heaptids));
	for (int i = 1; i < HNSW_HEAPTIDS; i++)
		ItemPointerSetInvalid(&etup->heaptids[i]);
	etup->level = e->level;
	etup->deleted = 0;
	memcpy(&etup->vec, e->vec, VECTOR_SIZE(dimensions));

	/* Find a page to insert the item */
	for (;;)
	{
		buf = ReadBuffer(index, insertPage);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		if (HnswFreeOffset(page, &freeOffno, &neighborPage) || PageGetFreeSpace(page) >= esize)
			break;

		insertPage = HnswPageGetOpaque(page)->nextblkno;

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

			/*
			 * From ReadBufferExtended: Caller is responsible for ensuring
			 * that only one backend tries to extend a relation at the same
			 * time!
			 */
			LockRelationForExtension(index, ExclusiveLock);

			/* Add a new page */
			newbuf = HnswNewBuffer(index, MAIN_FORKNUM);

			/* Unlock extend relation lock as early as possible */
			UnlockRelationForExtension(index, ExclusiveLock);

			/* Init new page */
			newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);
			HnswInitPage(newbuf, newpage);

			/* Update insert page */
			insertPage = BufferGetBlockNumber(newbuf);

			/* Update previous buffer */
			HnswPageGetOpaque(page)->nextblkno = insertPage;

			/* Commit */
			MarkBufferDirty(newbuf);
			MarkBufferDirty(buf);
			GenericXLogFinish(state);

			/* Unlock previous buffer */
			UnlockReleaseBuffer(buf);

			/* Prepare new buffer */
			state = GenericXLogStart(index);
			buf = newbuf;
			page = GenericXLogRegisterBuffer(state, buf, 0);
			break;
		}
	}

	if (OffsetNumberIsValid(freeOffno))
	{
		/* Reuse existing page */
		nbuf = ReadBuffer(index, neighborPage);
		LockBuffer(nbuf, BUFFER_LOCK_EXCLUSIVE);
	}
	else
	{
		/* Add new page */
		LockRelationForExtension(index, ExclusiveLock);
		nbuf = HnswNewBuffer(index, MAIN_FORKNUM);
		UnlockRelationForExtension(index, ExclusiveLock);
	}

	npage = GenericXLogRegisterBuffer(state, nbuf, GENERIC_XLOG_FULL_IMAGE);

	/* Overwrites existing page via InitPage */
	HnswInitPage(nbuf, npage);

	/* Update neighbors */
	AddNeighborsToPage(index, npage, e, ntup, nsize, m);

	e->blkno = BufferGetBlockNumber(buf);
	e->neighborPage = BufferGetBlockNumber(nbuf);

	/* Set neighbor page for element */
	etup->neighborPage = e->neighborPage;

	/* Add to next offset */
	if (OffsetNumberIsValid(freeOffno))
	{
		e->offno = freeOffno;
		if (!PageIndexTupleOverwrite(page, freeOffno, (Item) etup, esize))
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
	}
	else
	{
		e->offno = PageAddItem(page, (Item) etup, esize, InvalidOffsetNumber, false, false);
		if (e->offno == InvalidOffsetNumber)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
	}

	/* Commit */
	MarkBufferDirty(buf);
	MarkBufferDirty(nbuf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
	UnlockReleaseBuffer(nbuf);

	/* Update the insert page */
	if (insertPage != originalInsertPage)
		UpdateMetaPage(index, false, NULL, insertPage, MAIN_FORKNUM);
}

/*
 * Calculate offset number for update
 */
static OffsetNumber
HnswGetOffsetNumber(HnswUpdate * update, int m)
{
	return FirstOffsetNumber + (update->hc.element->level - update->level) * m + update->index;
}

/*
 * Update neighbors
 */
static void
UpdateNeighborPages(Relation index, HnswElement e, int m, List *updates)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	ListCell   *lc;
	OffsetNumber offno;
	Size		neighborsz = MAXALIGN(sizeof(HnswNeighborTupleData));
	HnswNeighborTuple neighbor = palloc0(neighborsz);

	/* Could update multiple at once for same element */
	/* but should only happen a low percent of time, so keep simple for now */
	foreach(lc, updates)
	{
		HnswUpdate *update = lfirst(lc);

		/* Register page */
		buf = ReadBuffer(index, update->hc.element->neighborPage);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);
		offno = HnswGetOffsetNumber(update, m);

		/* Make robust against issues */
		if (offno <= PageGetMaxOffsetNumber(page))
		{
			/* Set item data */
			ItemPointerSet(&neighbor->indextid, e->blkno, e->offno);
			neighbor->distance = update->hc.distance;

			/* Update connections */
			if (!PageIndexTupleOverwrite(page, offno, (Item) neighbor, neighborsz))
				elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

			/* Commit */
			MarkBufferDirty(buf);
			GenericXLogFinish(state);
		}
		else
			GenericXLogAbort(state);

		UnlockReleaseBuffer(buf);
	}
}

/*
 * Add a heap tid to an existing element
 */
static bool
HnswAddDuplicate(Relation index, HnswElement element, HnswElement dup)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		esize = MAXALIGN(HNSW_ELEMENT_TUPLE_SIZE(dup->vec->dim));
	HnswElementTuple etup;
	int			i;

	/* Read page */
	buf = ReadBuffer(index, dup->blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	/* Find space */
	etup = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, dup->offno));
	for (i = 0; i < HNSW_HEAPTIDS; i++)
	{
		if (!ItemPointerIsValid(&etup->heaptids[i]))
			break;
	}

	/* Either being deleted or we lost our chance to another backend */
	if (i == 0 || i == HNSW_HEAPTIDS)
	{
		GenericXLogAbort(state);
		UnlockReleaseBuffer(buf);
		return false;
	}

	/* Add heap tid */
	etup->heaptids[i] = *((ItemPointer) linitial(element->heaptids));

	/* Update index tuple */
	if (!PageIndexTupleOverwrite(page, dup->offno, (Item) etup, esize))
		elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

	/* Commit */
	MarkBufferDirty(buf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	return true;
}

/*
 * Write changes to disk
 */
static void
WriteElement(Relation index, HnswElement element, int m, List *updates, HnswElement dup, HnswElement entryPoint)
{
	/* Try to add to existing page */
	if (dup != NULL)
	{
		if (HnswAddDuplicate(index, element, dup))
			return;
	}

	/* If fails, take this path */
	WriteNewElementPages(index, element, m);
	UpdateNeighborPages(index, element, m, updates);

	/* Update metapage if needed */
	if (entryPoint == NULL || element->level > entryPoint->level)
		UpdateMetaPage(index, true, element, InvalidBlockNumber, MAIN_FORKNUM);
}

/*
 * Insert a tuple into the index
 */
bool
HnswInsertTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel)
{
	Datum		value;
	FmgrInfo   *normprocinfo;
	HnswElement entryPoint;
	HnswElement element;
	int			m = HnswGetM(index);
	int			efConstruction = HnswGetEfConstruction(index);
	double		ml = HnswGetMl(m);
	FmgrInfo   *procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
	Oid			collation = index->rd_indcollation[0];
	List	   *updates = NIL;
	HnswElement dup;

	/* Detoast once for all calls */
	value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

	/* Normalize if needed */
	normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
	if (normprocinfo != NULL)
	{
		if (!HnswNormValue(normprocinfo, collation, &value, NULL))
			return false;
	}

	/* Create an element */
	element = HnswInitElement(heap_tid, m, ml, HnswGetMaxLevel(m));
	element->vec = DatumGetVector(value);

	/* Get entry point */
	entryPoint = GetEntryPoint(index);

	/* Insert element in graph */
	dup = HnswInsertElement(element, entryPoint, index, procinfo, collation, m, efConstruction, &updates, false);

	/* Write to disk */
	WriteElement(index, element, m, updates, dup, entryPoint);

	return true;
}

/*
 * Insert a tuple into the index
 */
bool
hnswinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
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

	/* Create memory context */
	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Hnsw insert temporary context",
									  ALLOCSET_DEFAULT_SIZES);
	oldCtx = MemoryContextSwitchTo(insertCtx);

	/* Insert tuple */
	HnswInsertTuple(index, values, isnull, heap_tid, heap);

	/* Delete memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	return false;
}
