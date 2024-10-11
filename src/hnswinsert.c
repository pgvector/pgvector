#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "hnsw.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/datum.h"
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
HnswFreeOffset(Relation index, Buffer buf, Page page, HnswElement element, Size etupSize, Size ntupSize, Buffer *nbuf, Page *npage, OffsetNumber *freeOffno, OffsetNumber *freeNeighborOffno, BlockNumber *newInsertPage, uint8 *tupleVersion)
{
	OffsetNumber offno;
	OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);

	for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
	{
		ItemId		eitemid = PageGetItemId(page, offno);
		HnswElementTuple etup = (HnswElementTuple) PageGetItem(page, eitemid);

		/* Skip neighbor tuples */
		if (!HnswIsElementTuple(etup))
			continue;

		if (etup->deleted)
		{
			BlockNumber elementPage = BufferGetBlockNumber(buf);
			BlockNumber neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
			OffsetNumber neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);
			ItemId		nitemid;
			Size		pageFree;
			Size		npageFree;

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

			nitemid = PageGetItemId(*npage, neighborOffno);

			/* Ensure aligned for space check */
			Assert(etupSize == MAXALIGN(etupSize));
			Assert(ntupSize == MAXALIGN(ntupSize));

			/*
			 * Calculate free space individually since tuples are overwritten
			 * individually (in separate calls to PageIndexTupleOverwrite)
			 */
			pageFree = ItemIdGetLength(eitemid) + PageGetExactFreeSpace(page);
			npageFree = ItemIdGetLength(nitemid);
			if (neighborPage != elementPage)
				npageFree += PageGetExactFreeSpace(*npage);
			else if (pageFree >= etupSize)
				npageFree += pageFree - etupSize;

			/* Check for space */
			if (pageFree >= etupSize && npageFree >= ntupSize)
			{
				*freeOffno = offno;
				*freeNeighborOffno = neighborOffno;
				*tupleVersion = etup->version;
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
HnswInsertAppendPage(Relation index, Buffer *nbuf, Page *npage, GenericXLogState *state, Page page, bool building)
{
	/* Add a new page */
	LockRelationForExtension(index, ExclusiveLock);
	*nbuf = HnswNewBuffer(index, MAIN_FORKNUM);
	UnlockRelationForExtension(index, ExclusiveLock);

	/* Init new page */
	if (building)
		*npage = BufferGetPage(*nbuf);
	else
		*npage = GenericXLogRegisterBuffer(state, *nbuf, GENERIC_XLOG_FULL_IMAGE);

	HnswInitPage(*nbuf, *npage);

	/* Update previous buffer */
	HnswPageGetOpaque(page)->nextblkno = BufferGetBlockNumber(*nbuf);
}

/*
 * Add to element and neighbor pages
 */
static void
AddElementOnDisk(Relation index, HnswElement e, int m, BlockNumber insertPage, BlockNumber *updatedInsertPage, bool building)
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
	HnswNeighborTuple ntup;
	Buffer		nbuf;
	Page		npage;
	OffsetNumber freeOffno = InvalidOffsetNumber;
	OffsetNumber freeNeighborOffno = InvalidOffsetNumber;
	BlockNumber newInsertPage = InvalidBlockNumber;
	uint8		tupleVersion;
	char	   *base = NULL;

	/* Calculate sizes */
	etupSize = HNSW_ELEMENT_TUPLE_SIZE(VARSIZE_ANY(HnswPtrAccess(base, e->value)));
	ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(e->level, m);
	combinedSize = etupSize + ntupSize + sizeof(ItemIdData);
	maxSize = HNSW_MAX_SIZE;
	minCombinedSize = etupSize + HNSW_NEIGHBOR_TUPLE_SIZE(0, m) + sizeof(ItemIdData);

	/* Prepare element tuple */
	etup = palloc0(etupSize);
	HnswSetElementTuple(base, etup, e);

	/* Prepare neighbor tuple */
	ntup = palloc0(ntupSize);
	HnswSetNeighborTuple(base, ntup, e, m);

	/* Find a page (or two if needed) to insert the tuples */
	for (;;)
	{
		buf = ReadBuffer(index, currentPage);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		if (building)
		{
			state = NULL;
			page = BufferGetPage(buf);
		}
		else
		{
			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buf, 0);
		}

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
		if (HnswFreeOffset(index, buf, page, e, etupSize, ntupSize, &nbuf, &npage, &freeOffno, &freeNeighborOffno, &newInsertPage, &tupleVersion))
		{
			if (nbuf != buf)
			{
				if (building)
					npage = BufferGetPage(nbuf);
				else
					npage = GenericXLogRegisterBuffer(state, nbuf, 0);
			}

			/* Set tuple version */
			etup->version = tupleVersion;
			ntup->version = tupleVersion;

			break;
		}

		/* Finally, try space for element only if last page */
		/* Skip if both tuples can fit on the same page */
		if (combinedSize > maxSize && PageGetFreeSpace(page) >= etupSize && !BlockNumberIsValid(HnswPageGetOpaque(page)->nextblkno))
		{
			HnswInsertAppendPage(index, &nbuf, &npage, state, page, building);
			break;
		}

		currentPage = HnswPageGetOpaque(page)->nextblkno;

		if (BlockNumberIsValid(currentPage))
		{
			/* Move to next page */
			if (!building)
				GenericXLogAbort(state);
			UnlockReleaseBuffer(buf);
		}
		else
		{
			Buffer		newbuf;
			Page		newpage;

			HnswInsertAppendPage(index, &newbuf, &newpage, state, page, building);

			/* Commit */
			if (building)
				MarkBufferDirty(buf);
			else
				GenericXLogFinish(state);

			/* Unlock previous buffer */
			UnlockReleaseBuffer(buf);

			/* Prepare new buffer */
			buf = newbuf;
			if (building)
			{
				state = NULL;
				page = BufferGetPage(buf);
			}
			else
			{
				state = GenericXLogStart(index);
				page = GenericXLogRegisterBuffer(state, buf, 0);
			}

			/* Create new page for neighbors if needed */
			if (PageGetFreeSpace(page) < combinedSize)
				HnswInsertAppendPage(index, &nbuf, &npage, state, page, building);
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
	if (building)
	{
		MarkBufferDirty(buf);
		if (nbuf != buf)
			MarkBufferDirty(nbuf);
	}
	else
		GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
	if (nbuf != buf)
		UnlockReleaseBuffer(nbuf);

	/* Update the insert page */
	if (BlockNumberIsValid(newInsertPage) && newInsertPage != insertPage)
		*updatedInsertPage = newInsertPage;
}

/*
 * Load neighbors
 */
static HnswNeighborArray *
HnswLoadNeighbors(HnswElement element, Relation index, int m, int lm, int lc)
{
	char	   *base = NULL;
	HnswNeighborArray *neighbors = HnswInitNeighborArray(lm, NULL);
	ItemPointerData indextids[HNSW_MAX_M * 2];

	if (!HnswLoadNeighborTids(element, indextids, index, m, lm, lc))
		return neighbors;

	for (int i = 0; i < lm; i++)
	{
		ItemPointer indextid = &indextids[i];
		HnswElement e;
		HnswCandidate *hc;

		if (!ItemPointerIsValid(indextid))
			break;

		e = HnswInitElementFromBlock(ItemPointerGetBlockNumber(indextid), ItemPointerGetOffsetNumber(indextid));
		hc = &neighbors->items[neighbors->length++];
		HnswPtrStore(base, hc->element, e);
	}

	return neighbors;
}

/*
 * Load elements for insert
 */
static void
LoadElementsForInsert(HnswNeighborArray * neighbors, HnswQuery * q, int *idx, Relation index, HnswSupport * support)
{
	char	   *base = NULL;

	for (int i = 0; i < neighbors->length; i++)
	{
		HnswCandidate *hc = &neighbors->items[i];
		HnswElement element = HnswPtrAccess(base, hc->element);
		double		distance;

		HnswLoadElement(element, &distance, q, index, support, true, NULL);
		hc->distance = distance;

		/* Prune element if being deleted */
		if (element->heaptidsLength == 0)
		{
			*idx = i;
			break;
		}
	}
}

/*
 * Get update index
 */
static int
GetUpdateIndex(HnswElement element, HnswElement newElement, float distance, int m, int lm, int lc, Relation index, HnswSupport * support, MemoryContext updateCtx)
{
	char	   *base = NULL;
	int			idx = -1;
	HnswNeighborArray *neighbors;
	MemoryContext oldCtx = MemoryContextSwitchTo(updateCtx);

	/*
	 * Get latest neighbors since they may have changed. Do not lock yet since
	 * selecting neighbors can take time. Could use optimistic locking to
	 * retry if another update occurs before getting exclusive lock.
	 */
	neighbors = HnswLoadNeighbors(element, index, m, lm, lc);

	/*
	 * Could improve performance for vacuuming by checking neighbors against
	 * list of elements being deleted to find index. It's important to exclude
	 * already deleted elements for this since they can be replaced at any
	 * time.
	 */

	if (neighbors->length < lm)
		idx = -2;
	else
	{
		HnswQuery	q;

		q.value = HnswGetValue(base, element);

		LoadElementsForInsert(neighbors, &q, &idx, index, support);

		if (idx == -1)
			HnswUpdateConnection(base, neighbors, newElement, distance, lm, &idx, index, support);
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(updateCtx);

	return idx;
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
 * Update neighbor
 */
static void
UpdateNeighborOnDisk(HnswElement element, HnswElement newElement, int idx, int m, int lm, int lc, Relation index, bool checkExisting, bool building)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	HnswNeighborTuple ntup;
	int			startIdx;
	OffsetNumber offno = element->neighborOffno;

	/* Register page */
	buf = ReadBuffer(index, element->neighborPage);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	if (building)
	{
		state = NULL;
		page = BufferGetPage(buf);
	}
	else
	{
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);
	}

	/* Get tuple */
	ntup = (HnswNeighborTuple) PageGetItem(page, PageGetItemId(page, offno));

	/* Calculate index for update */
	startIdx = (element->level - lc) * m;

	/* Check for existing connection */
	if (checkExisting && ConnectionExists(newElement, ntup, startIdx, lm))
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

		/* Update neighbor on the buffer */
		ItemPointerSet(indextid, newElement->blkno, newElement->offno);

		/* Commit */
		if (building)
			MarkBufferDirty(buf);
		else
			GenericXLogFinish(state);
	}
	else if (!building)
		GenericXLogAbort(state);

	UnlockReleaseBuffer(buf);
}

/*
 * Update neighbors
 */
void
HnswUpdateNeighborsOnDisk(Relation index, HnswSupport * support, HnswElement e, int m, bool checkExisting, bool building)
{
	char	   *base = NULL;

	/* Use separate memory context to improve performance for larger vectors */
	MemoryContext updateCtx = GenerationContextCreate(CurrentMemoryContext,
													  "Hnsw insert update context",
#if PG_VERSION_NUM >= 150000
													  128 * 1024, 128 * 1024,
#endif
													  128 * 1024);

	for (int lc = e->level; lc >= 0; lc--)
	{
		int			lm = HnswGetLayerM(m, lc);
		HnswNeighborArray *neighbors = HnswGetNeighbors(base, e, lc);

		for (int i = 0; i < neighbors->length; i++)
		{
			HnswCandidate *hc = &neighbors->items[i];
			HnswElement neighborElement = HnswPtrAccess(base, hc->element);
			int			idx;

			idx = GetUpdateIndex(neighborElement, e, hc->distance, m, lm, lc, index, support, updateCtx);

			/* New element was not selected as a neighbor */
			if (idx == -1)
				continue;

			UpdateNeighborOnDisk(neighborElement, e, idx, m, lm, lc, index, checkExisting, building);
		}
	}

	MemoryContextDelete(updateCtx);
}

/*
 * Add a heap TID to an existing element
 */
static bool
AddDuplicateOnDisk(Relation index, HnswElement element, HnswElement dup, bool building)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	HnswElementTuple etup;
	int			i;

	/* Read page */
	buf = ReadBuffer(index, dup->blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	if (building)
	{
		state = NULL;
		page = BufferGetPage(buf);
	}
	else
	{
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);
	}

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
		if (!building)
			GenericXLogAbort(state);
		UnlockReleaseBuffer(buf);
		return false;
	}

	/* Add heap TID, modifying the tuple on the page directly */
	etup->heaptids[i] = element->heaptids[0];

	/* Commit */
	if (building)
		MarkBufferDirty(buf);
	else
		GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	return true;
}

/*
 * Find duplicate element
 */
static bool
FindDuplicateOnDisk(Relation index, HnswElement element, bool building)
{
	char	   *base = NULL;
	HnswNeighborArray *neighbors = HnswGetNeighbors(base, element, 0);
	Datum		value = HnswGetValue(base, element);

	for (int i = 0; i < neighbors->length; i++)
	{
		HnswCandidate *neighbor = &neighbors->items[i];
		HnswElement neighborElement = HnswPtrAccess(base, neighbor->element);
		Datum		neighborValue = HnswGetValue(base, neighborElement);

		/* Exit early since ordered by distance */
		if (!datumIsEqual(value, neighborValue, false, -1))
			return false;

		if (AddDuplicateOnDisk(index, element, neighborElement, building))
			return true;
	}

	return false;
}

/*
 * Update graph on disk
 */
static void
UpdateGraphOnDisk(Relation index, HnswSupport * support, HnswElement element, int m, int efConstruction, HnswElement entryPoint, bool building)
{
	BlockNumber newInsertPage = InvalidBlockNumber;

	/* Look for duplicate */
	if (FindDuplicateOnDisk(index, element, building))
		return;

	/* Add element */
	AddElementOnDisk(index, element, m, GetInsertPage(index), &newInsertPage, building);

	/* Update insert page if needed */
	if (BlockNumberIsValid(newInsertPage))
		HnswUpdateMetaPage(index, 0, NULL, newInsertPage, MAIN_FORKNUM, building);

	/* Update neighbors */
	HnswUpdateNeighborsOnDisk(index, support, element, m, false, building);

	/* Update entry point if needed */
	if (entryPoint == NULL || element->level > entryPoint->level)
		HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_GREATER, element, InvalidBlockNumber, MAIN_FORKNUM, building);
}

/*
 * Insert a tuple into the index
 */
bool
HnswInsertTupleOnDisk(Relation index, HnswSupport * support, Datum value, ItemPointer heaptid, bool building)
{
	HnswElement entryPoint;
	HnswElement element;
	int			m;
	int			efConstruction = HnswGetEfConstruction(index);
	LOCKMODE	lockmode = ShareLock;
	char	   *base = NULL;

	/*
	 * Get a shared lock. This allows vacuum to ensure no in-flight inserts
	 * before repairing graph. Use a page lock so it does not interfere with
	 * buffer lock (or reads when vacuuming).
	 */
	LockPage(index, HNSW_UPDATE_LOCK, lockmode);

	/* Get m and entry point */
	HnswGetMetaPageInfo(index, &m, &entryPoint);

	/* Create an element */
	element = HnswInitElement(base, heaptid, m, HnswGetMl(m), HnswGetMaxLevel(m), NULL);
	HnswPtrStore(base, element->value, DatumGetPointer(value));

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

	/* Find neighbors for element */
	HnswFindElementNeighbors(base, element, entryPoint, index, support, m, efConstruction, false);

	/* Update graph on disk */
	UpdateGraphOnDisk(index, support, element, m, efConstruction, entryPoint, building);

	/* Release lock */
	UnlockPage(index, HNSW_UPDATE_LOCK, lockmode);

	return true;
}

/*
 * Insert a tuple into the index
 */
static void
HnswInsertTuple(Relation index, Datum *values, bool *isnull, ItemPointer heaptid)
{
	Datum		value;
	const		HnswTypeInfo *typeInfo = HnswGetTypeInfo(index);
	HnswSupport support;

	HnswInitSupport(&support, index);

	/* Form index value */
	if (!HnswFormIndexValue(&value, values, isnull, typeInfo, &support))
		return;

	HnswInsertTupleOnDisk(index, &support, value, heaptid, false);
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
	HnswInsertTuple(index, values, isnull, heap_tid);

	/* Delete memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	return false;
}
