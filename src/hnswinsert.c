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
HnswFreeOffset(Relation index, Buffer buf, Page page, HnswElement element, Size ntupSize, Buffer *nbuf, Page *npage, OffsetNumber *freeOffno, OffsetNumber *freeNeighborOffno, BlockNumber *newInsertPage)
{
	OffsetNumber offno;
	OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);

	for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
	{
		HnswElementTuple etup = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, offno));

		/* Skip neighbor tuples */
		if (!HnswIsElementTuple(etup))
			continue;

		if (etup->deleted)
		{
			BlockNumber elementPage = BufferGetBlockNumber(buf);
			BlockNumber neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
			OffsetNumber neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);
			ItemId		itemid;

			if (!BlockNumberIsValid(*newInsertPage))
				*newInsertPage = elementPage;

			if (neighborPage == elementPage)
			{
				*nbuf = buf;
				*npage = page;
			}
			else
			{
				*nbuf = ReadBuffer(index, neighborPage);
				LockBuffer(*nbuf, BUFFER_LOCK_EXCLUSIVE);

				/* Skip WAL for now */
				*npage = BufferGetPage(*nbuf);
			}

			itemid = PageGetItemId(*npage, neighborOffno);

			/* Check for space on neighbor tuple page */
			if (PageGetFreeSpace(*npage) + ItemIdGetLength(itemid) - sizeof(ItemIdData) >= ntupSize)
			{
				*freeOffno = offno;
				*freeNeighborOffno = neighborOffno;
				return true;
			}
			else if (*nbuf != buf)
				UnlockReleaseBuffer(*nbuf);
		}
	}

	return false;
}

/*
 * Add a new page
 */
static void
HnswInsertAppendPage(Relation index, Buffer *nbuf, Page *npage, GenericXLogState *state, Page page)
{
	/* Add a new page */
	LockRelationForExtension(index, ExclusiveLock);
	*nbuf = HnswNewBuffer(index, MAIN_FORKNUM);
	UnlockRelationForExtension(index, ExclusiveLock);

	/* Init new page */
	*npage = GenericXLogRegisterBuffer(state, *nbuf, GENERIC_XLOG_FULL_IMAGE);
	HnswInitPage(*nbuf, *npage);

	/* Update previous buffer */
	HnswPageGetOpaque(page)->nextblkno = BufferGetBlockNumber(*nbuf);
}

/*
 * Add to element and neighbor pages
 */
static void
WriteNewElementPages(Relation index, HnswElement e, int m, BlockNumber insertPage, BlockNumber *updatedInsertPage)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		etupSize;
	Size		ntupSize;
	Size		combinedSize;
	Size		maxSize;
	Size		minCombinedSize;
	HnswElementTuple etup;
	BlockNumber currentPage = insertPage;
	int			dimensions = e->vec->dim;
	HnswNeighborTuple ntup;
	Buffer		nbuf;
	Page		npage;
	OffsetNumber freeOffno = InvalidOffsetNumber;
	OffsetNumber freeNeighborOffno = InvalidOffsetNumber;
	BlockNumber newInsertPage = InvalidBlockNumber;

	/* Calculate sizes */
	etupSize = HNSW_ELEMENT_TUPLE_SIZE(dimensions);
	ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(e->level, m);
	combinedSize = etupSize + ntupSize + sizeof(ItemIdData);
	maxSize = BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(HnswPageOpaqueData));
	minCombinedSize = etupSize + HNSW_NEIGHBOR_TUPLE_SIZE(0, m) + sizeof(ItemIdData);

	/* Prepare element tuple */
	etup = palloc0(etupSize);
	HnswSetElementTuple(etup, e);

	/* Prepare neighbor tuple */
	ntup = palloc0(ntupSize);
	HnswSetNeighborTuple(ntup, e, m);

	/* Find a page (or two if needed) to insert the tuples */
	for (;;)
	{
		buf = ReadBuffer(index, currentPage);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		/* Keep track of first page where element at level 0 can fit */
		if (!BlockNumberIsValid(newInsertPage) && PageGetFreeSpace(page) >= minCombinedSize)
			newInsertPage = currentPage;

		/* First, try the fastest path */
		/* Space for both tuples on the current page */
		/* This can split existing tuples in rare cases */
		if (PageGetFreeSpace(page) >= combinedSize)
		{
			nbuf = buf;
			npage = page;
			break;
		}

		/* Next, try space from a deleted element */
		if (HnswFreeOffset(index, buf, page, e, ntupSize, &nbuf, &npage, &freeOffno, &freeNeighborOffno, &newInsertPage))
		{
			if (nbuf != buf)
				npage = GenericXLogRegisterBuffer(state, nbuf, 0);

			break;
		}

		/* Finally, try space for element only if last page */
		/* Skip if both tuples can fit on the same page */
		if (combinedSize > maxSize && PageGetFreeSpace(page) >= etupSize && !BlockNumberIsValid(HnswPageGetOpaque(page)->nextblkno))
		{
			HnswInsertAppendPage(index, &nbuf, &npage, state, page);
			break;
		}

		currentPage = HnswPageGetOpaque(page)->nextblkno;

		if (BlockNumberIsValid(currentPage))
		{
			/* Move to next page */
			GenericXLogAbort(state);
			UnlockReleaseBuffer(buf);
		}
		else
		{
			Buffer		newbuf;
			Page		newpage;

			HnswInsertAppendPage(index, &newbuf, &newpage, state, page);

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

			/* Create new page for neighbors if needed */
			if (PageGetFreeSpace(page) < combinedSize)
				HnswInsertAppendPage(index, &nbuf, &npage, state, page);
			else
			{
				nbuf = buf;
				npage = page;
			}

			break;
		}
	}

	e->blkno = BufferGetBlockNumber(buf);
	e->neighborPage = BufferGetBlockNumber(nbuf);

	/* Added tuple to new page if newInsertPage is not set */
	/* So can set to neighbor page instead of element page */
	if (!BlockNumberIsValid(newInsertPage))
		newInsertPage = e->neighborPage;

	if (OffsetNumberIsValid(freeOffno))
	{
		e->offno = freeOffno;
		e->neighborOffno = freeNeighborOffno;
	}
	else
	{
		e->offno = OffsetNumberNext(PageGetMaxOffsetNumber(page));
		if (nbuf == buf)
			e->neighborOffno = OffsetNumberNext(e->offno);
		else
			e->neighborOffno = FirstOffsetNumber;
	}

	ItemPointerSet(&etup->neighbortid, e->neighborPage, e->neighborOffno);

	/* Add element and neighbors */
	if (OffsetNumberIsValid(freeOffno))
	{
		if (!PageIndexTupleOverwrite(page, e->offno, (Item) etup, etupSize))
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		if (!PageIndexTupleOverwrite(npage, e->neighborOffno, (Item) ntup, ntupSize))
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
	}
	else
	{
		if (PageAddItem(page, (Item) etup, etupSize, InvalidOffsetNumber, false, false) != e->offno)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		if (PageAddItem(npage, (Item) ntup, ntupSize, InvalidOffsetNumber, false, false) != e->neighborOffno)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
	}

	/* Commit */
	MarkBufferDirty(buf);
	if (nbuf != buf)
		MarkBufferDirty(nbuf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
	if (nbuf != buf)
		UnlockReleaseBuffer(nbuf);

	/* Update the insert page */
	if (BlockNumberIsValid(newInsertPage) && newInsertPage != insertPage)
		*updatedInsertPage = newInsertPage;
}

/*
 * Check if connection already exists
 */
static bool
ConnectionExists(HnswElement e, HnswNeighborTuple ntup, int startIdx, int lm)
{
	for (int i = 0; i < lm; i++)
	{
		ItemPointer indextid = &ntup->indextids[startIdx + i];

		if (!ItemPointerIsValid(indextid))
			break;

		if (ItemPointerGetBlockNumber(indextid) == e->blkno && ItemPointerGetOffsetNumber(indextid) == e->offno)
			return true;
	}

	return false;
}

/*
 * Update neighbors
 */
void
HnswUpdateNeighborPages(Relation index, FmgrInfo *procinfo, Oid collation, HnswElement e, int m, bool checkExisting)
{
	for (int lc = e->level; lc >= 0; lc--)
	{
		int			lm = HnswGetLayerM(m, lc);
		HnswNeighborArray *neighbors = &e->neighbors[lc];

		for (int i = 0; i < neighbors->length; i++)
		{
			HnswCandidate *hc = &neighbors->items[i];
			Buffer		buf;
			Page		page;
			GenericXLogState *state;
			ItemId		itemid;
			HnswNeighborTuple ntup;
			Size		ntupSize;
			int			idx = -1;
			int			startIdx;
			OffsetNumber offno = hc->element->neighborOffno;

			/* Get latest neighbors since they may have changed */
			/* Do not lock yet since selecting neighbors can take time */
			HnswLoadNeighbors(hc->element, index);

			/*
			 * Could improve performance for vacuuming by checking neighbors
			 * against list of elements being deleted to find index. It's
			 * important to exclude already deleted elements for this since
			 * they can be replaced at any time.
			 */

			/* Select neighbors */
			HnswUpdateConnection(e, hc, lm, lc, &idx, index, procinfo, collation);

			/* New element was not selected as a neighbor */
			if (idx == -1)
				continue;

			/* Register page */
			buf = ReadBuffer(index, hc->element->neighborPage);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buf, 0);

			/* Get tuple */
			itemid = PageGetItemId(page, offno);
			ntup = (HnswNeighborTuple) PageGetItem(page, itemid);
			ntupSize = ItemIdGetLength(itemid);

			/* Calculate index for update */
			startIdx = (hc->element->level - lc) * m;

			/* Check for existing connection */
			if (checkExisting && ConnectionExists(e, ntup, startIdx, lm))
				idx = -1;
			else if (idx == -2)
			{
				/* Find free offset if still exists */
				/* TODO Retry updating connections if not */
				for (int j = 0; j < lm; j++)
				{
					if (!ItemPointerIsValid(&ntup->indextids[startIdx + j]))
					{
						idx = startIdx + j;
						break;
					}
				}
			}
			else
				idx += startIdx;

			/* Make robust to issues */
			if (idx >= 0 && idx < ntup->count)
			{
				ItemPointer indextid = &ntup->indextids[idx];

				/* Update neighbor */
				ItemPointerSet(indextid, e->blkno, e->offno);

				/* Overwrite tuple */
				if (!PageIndexTupleOverwrite(page, offno, (Item) ntup, ntupSize))
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
}

/*
 * Add a heap TID to an existing element
 */
static bool
HnswAddDuplicate(Relation index, HnswElement element, HnswElement dup)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		etupSize = HNSW_ELEMENT_TUPLE_SIZE(dup->vec->dim);
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

	/* Add heap TID */
	etup->heaptids[i] = *((ItemPointer) linitial(element->heaptids));

	/* Overwrite tuple */
	if (!PageIndexTupleOverwrite(page, dup->offno, (Item) etup, etupSize))
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
WriteElement(Relation index, FmgrInfo *procinfo, Oid collation, HnswElement element, int m, int efConstruction, HnswElement dup, HnswElement entryPoint)
{
	BlockNumber newInsertPage = InvalidBlockNumber;

	/* Try to add to existing page */
	if (dup != NULL)
	{
		if (HnswAddDuplicate(index, element, dup))
			return;
	}

	/* Write element and neighbor tuples */
	WriteNewElementPages(index, element, m, GetInsertPage(index), &newInsertPage);

	/* Update insert page if needed */
	if (BlockNumberIsValid(newInsertPage))
		HnswUpdateMetaPage(index, 0, NULL, newInsertPage, MAIN_FORKNUM);

	/* Update neighbors */
	HnswUpdateNeighborPages(index, procinfo, collation, element, m, false);

	/* Update metapage if needed */
	if (entryPoint == NULL || element->level > entryPoint->level)
		HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_GREATER, element, InvalidBlockNumber, MAIN_FORKNUM);
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
	HnswElement dup;
	LOCKMODE	lockmode = ShareLock;

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

	/*
	 * Get a shared lock. This allows vacuum to ensure no in-flight inserts
	 * before repairing graph. Use a page lock so it does not interfere with
	 * buffer lock (or reads when vacuuming).
	 */
	LockPage(index, HNSW_UPDATE_LOCK, lockmode);

	/* Get entry point */
	entryPoint = HnswGetEntryPoint(index);

	/* Prevent concurrent inserts when likely updating entry point */
	if (entryPoint == NULL || element->level > entryPoint->level)
	{
		/* Release shared lock */
		UnlockPage(index, HNSW_UPDATE_LOCK, lockmode);

		/* Get exclusive lock */
		lockmode = ExclusiveLock;
		LockPage(index, HNSW_UPDATE_LOCK, lockmode);

		/* Get latest entry point after lock is acquired */
		entryPoint = HnswGetEntryPoint(index);
	}

	/* Insert element in graph */
	HnswInsertElement(element, entryPoint, index, procinfo, collation, m, efConstruction, false);

	/* Look for duplicate */
	dup = HnswFindDuplicate(element);

	/* Write to disk */
	WriteElement(index, procinfo, collation, element, m, efConstruction, dup, entryPoint);

	/* Release lock */
	UnlockPage(index, HNSW_UPDATE_LOCK, lockmode);

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
