#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "tqhnsw.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

/*
 * Quantize a heap value into a fresh in-memory element (codes + rhat), drawing a
 * random level.  Mirrors TqhnswBuildCallback's encode block exactly (detoast ->
 * TqExtractForEncode -> TqEncode -> TqhnswReconstructHalf, which reconstructs
 * rhat with cosine unit-normalization when applicable) so on-disk inserts are
 * numerically identical to build-time nodes.
 *
 * Allocated in the current (per-insert) memory context.  Neighbor arrays are
 * sized per layer (level 0 doubled) and zero-initialized; forward links are
 * filled by TqhnswInsertElement.
 */
static TqhnswElement *
TqhnswQuantizeElement(Relation index, const TqModel *model, TqMetric metric,
					  int m, int dimCodes, int codesBytes, ItemPointer heaptid,
					  Datum value)
{
	char	   *base = NULL;
	TqhnswElement *element;
	int			level;
	int			lc;
	Size		scratchSize = TqEntrySize(dimCodes, model->bits, false);
	TqEntry    *scratch = (TqEntry *) palloc(scratchSize);
	const TqTypeInfo *ti = TqGetTypeInfo(index, TQHNSW_TYPE_INFO_PROC);
	float	   *vscratch = palloc(sizeof(float) * model->dim);	/* freed with the
																 * caller's short-lived
																 * context (insertCtx /
																 * build tmpCtx reset) */
	const float *fv;
	char	   *codes;
	half	   *rhat;
	float	   *rhatScratch;

	value = PointerGetDatum(PG_DETOAST_DATUM(value));

	fv = TqExtractForEncode(ti, value, metric, vscratch, model->dim);

	memset(scratch, 0, scratchSize);
	TqEncode(model, fv, scratch);

	level = TqhnswRandomLevel(TqhnswGetMl(m), TqhnswGetMaxLevel(m));

	element = palloc0(sizeof(TqhnswElement));
	element->heaptid = *heaptid;
	element->level = (uint8) level;
	element->norm = scratch->norm;
	element->scale = scratch->scale;
	codes = (char *) palloc0(codesBytes);
	memcpy(codes, scratch->data, codesBytes);
	TqhnswPtrStore(base, element->codes, codes);
	rhat = (half *) palloc(sizeof(half) * dimCodes);
	rhatScratch = (float *) palloc(sizeof(float) * dimCodes);
	TqhnswPtrStore(base, element->rhat, rhat);

	{
		TqhnswNeighborArrayPtr *neighbors = palloc(sizeof(TqhnswNeighborArrayPtr) * (level + 1));

		TqhnswPtrStore(base, element->neighbors, neighbors);
		for (lc = 0; lc <= level; lc++)
		{
			int			lm = TqhnswGetLayerM(m, lc);
			TqhnswNeighborArray *na = palloc(TQHNSW_NEIGHBOR_ARRAY_SIZE(lm));

			na->count = 0;
			TqhnswPtrStore(base, neighbors[lc], na);
		}
	}

	element->blkno = InvalidBlockNumber;
	element->offno = InvalidOffsetNumber;
	element->neighborPage = InvalidBlockNumber;
	element->neighborOffno = InvalidOffsetNumber;
	TqhnswPtrStore(base, element->next, (TqhnswElement *) NULL);
	LWLockInitialize(&element->lock, tqhnsw_lock_tranche_id);

	TqhnswReconstructHalf(model, codes, element->norm, element->scale,
						  metric == TQ_METRIC_COSINE, rhatScratch, rhat);

	pfree(scratch);
	return element;
}

/*
 * Fill an element tuple from an in-memory element (mirrors the etup field writes
 * in TqhnswFlushGraph).  version comes from e->version (0 for a fresh insert;
 * carried from the reused slot for a vacuum-repaired element), unlike the build
 * path which always writes 0.  neighbortid is set by the caller after page
 * placement.
 */
static void
TqhnswSetElementTuple(TqhnswElementTuple etup, TqhnswElement *e, int codesBytes)
{
	char	   *base = NULL;
	char	   *codes = TqhnswPtrAccess(base, e->codes);

	Assert(codes != NULL);

	etup->type = TQHNSW_ELEMENT_TUPLE_TYPE;
	etup->level = e->level;
	etup->deleted = 0;
	etup->version = e->version;
	etup->heaptid = e->heaptid;
	etup->norm = e->norm;
	etup->scale = e->scale;
	memcpy(etup->codes, codes, codesBytes);
}

/*
 * Fill a neighbor tuple from an in-memory element's forward links (mirrors the
 * pass-2 fill in TqhnswFlushGraph).  version comes from e->version, kept in sync
 * with the element tuple (the build path always writes 0).  Slots beyond the
 * selected count are set invalid; reciprocal edges are written separately by
 * UpdateNeighborsOnDisk.
 */
void
TqhnswSetNeighborTuple(TqhnswNeighborTuple ntup, TqhnswElement *e, int m)
{
	char	   *base = NULL;
	int			idx = 0;
	int			lc;

	ntup->type = TQHNSW_NEIGHBOR_TUPLE_TYPE;
	ntup->version = e->version;

	for (lc = e->level; lc >= 0; lc--)
	{
		int			lm = TqhnswGetLayerM(m, lc);
		TqhnswNeighborArray *na = TqhnswGetNeighbors(NULL, e, lc);
		int			i;

		for (i = 0; i < lm; i++)
		{
			ItemPointer indextid = &ntup->indextids[idx++];

			if (i < na->count)
			{
				TqhnswElement *ne = TqhnswPtrAccess(base, na->items[i].element);

				ItemPointerSet(indextid, ne->blkno, ne->offno);
			}
			else
				ItemPointerSetInvalid(indextid);
		}
	}
	ntup->count = (uint16) idx;
}

/*
 * Look for a deleted element tuple on `page` whose slot (and its paired neighbor
 * slot) can be reused for element e.  Mirrors hnswinsert.c HnswFreeOffset.  On
 * success sets *freeOffno / *freeNeighborOffno, fills *nbuf / *npage for the neighbor
 * page (which may differ from buf), carries the deleted tuple's version into
 * *tupleVersion, and returns true.  Element tuples are fixed-size in tqhnsw so
 * the element-slot check is a simple length comparison.
 *
 * Neighbor-slot check is deliberately slot-only (conservative): we require the
 * deleted neighbor slot alone — ItemIdGetLength(nitemid) >= ntupSize — to fit the
 * new neighbor tuple.  HnswFreeOffset is more permissive, adding
 * PageGetExactFreeSpace so it can reuse a slot that grows into adjacent free
 * space; tqhnsw omits that because the neighbor tuple size varies with the new
 * element's level (a higher level than the deleted occupant needs a larger tuple,
 * which the extra free-space accounting would be required to safely reclaim).
 * Skipping it is safe — we may leave some hnsw-reusable slots un-reused, but
 * never overwrite a slot that is too small.
 *
 * The neighbor page (when different from buf) is locked EXCLUSIVE and returned
 * unlocked-on-failure / locked-on-success; the caller registers it into the
 * GenericXLogState and releases it.  No GenericXLogState is needed here — mirrors
 * HnswFreeOffset which also avoids xlog in the probe phase.
 */
static bool
TqhnswFreeOffset(Relation index, Buffer buf, Page page, TqhnswElement *e,
				 Size etupSize, Size ntupSize,
				 Buffer *nbuf, Page *npage,
				 OffsetNumber *freeOffno, OffsetNumber *freeNeighborOffno,
				 uint8 *tupleVersion)
{
	OffsetNumber offno;
	OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);

	/*
	 * Sizes must be aligned for the ItemIdGetLength >= size comparisons to be
	 * exact.
	 */
	Assert(etupSize == MAXALIGN(etupSize));
	Assert(ntupSize == MAXALIGN(ntupSize));

	for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
	{
		ItemId		eitemid = PageGetItemId(page, offno);
		TqhnswElementTuple etup = (TqhnswElementTuple) PageGetItem(page, eitemid);
		BlockNumber neighborPage;
		OffsetNumber neighborOffno;
		ItemId		nitemid;

		/* Skip neighbor tuples and live element tuples. */
		if (etup->type != TQHNSW_ELEMENT_TUPLE_TYPE || !etup->deleted)
			continue;

		/* Reused element slot must be large enough for the new tuple. */
		if (ItemIdGetLength(eitemid) < etupSize)
			continue;

		neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
		neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);

		/* Resolve the neighbor page. */
		if (neighborPage == BufferGetBlockNumber(buf))
		{
			*nbuf = buf;
			*npage = page;
		}
		else
		{
			*nbuf = ReadBuffer(index, neighborPage);
			LockBuffer(*nbuf, BUFFER_LOCK_EXCLUSIVE);
			*npage = BufferGetPage(*nbuf);
		}

		nitemid = PageGetItemId(*npage, neighborOffno);

		/*
		 * Deleted neighbor slot alone must fit the new neighbor tuple
		 * (slot-only / conservative check — see function comment above for
		 * why we do not add PageGetExactFreeSpace as HnswFreeOffset does).
		 */
		if (ItemIdGetLength(nitemid) < ntupSize)
		{
			if (*nbuf != buf)
				UnlockReleaseBuffer(*nbuf);
			*nbuf = InvalidBuffer;
			continue;
		}

		*freeOffno = offno;
		*freeNeighborOffno = neighborOffno;
		*tupleVersion = etup->version;	/* carry version for iterative-scan
										 * correctness */
		return true;
	}
	return false;
}

/*
 * Append a new page after the current one within a GenericXLog transaction
 * (mirrors hnswinsert.c HnswInsertAppendPage; on-disk path only).
 */
static void
TqhnswAppendInsertPage(Relation index, Buffer *nbuf, Page *npage,
					   GenericXLogState *state, Page page)
{
	LockRelationForExtension(index, ExclusiveLock);
	*nbuf = TqNewBuffer(index, MAIN_FORKNUM);
	UnlockRelationForExtension(index, ExclusiveLock);

	*npage = GenericXLogRegisterBuffer(state, *nbuf, GENERIC_XLOG_FULL_IMAGE);
	TqInitPage(*nbuf, *npage, TQHNSW_PAGE_ID);

	/* Link the previous page to the new one. */
	TqhnswPageGetOpaque(page)->nextblkno = BufferGetBlockNumber(*nbuf);
}

/*
 * Add an element + its neighbor tuple to disk (mirrors hnswinsert.c
 * AddElementOnDisk).  Walks the element-page chain from insertPage looking first
 * for a deleted-slot reuse opportunity (TqhnswFreeOffset), then for free space to
 * append, adding new pages as needed.  Assigns e->blkno/offno/neighborPage/
 * neighborOffno.  Returns the (possibly updated) insert-page hint in
 * *updatedInsertPage.
 *
 * Deleted-slot reuse (TqhnswFreeOffset) only fires once vacuum has tombstoned a
 * tuple (deleted=1); on a never-vacuumed index no slot qualifies and it is a
 * no-op.
 */
static void
TqhnswAddElementOnDisk(Relation index, TqhnswElement *e, int m, int codesBytes,
					   BlockNumber insertPage, BlockNumber firstElementPage,
					   BlockNumber *updatedInsertPage)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		etupSize = TQHNSW_ELEMENT_TUPLE_SIZE(codesBytes);
	Size		ntupSize = TQHNSW_NEIGHBOR_TUPLE_SIZE(e->level, m);
	Size		combinedSize = etupSize + ntupSize + sizeof(ItemIdData);
	Size		maxSize = TqPageCapacity();
	Size		minCombinedSize = etupSize + TQHNSW_NEIGHBOR_TUPLE_SIZE(0, m) + sizeof(ItemIdData);
	TqhnswElementTuple etup;
	TqhnswNeighborTuple ntup;
	Buffer		nbuf;
	Page		npage;
	BlockNumber currentPage = insertPage;
	BlockNumber newInsertPage = InvalidBlockNumber;
	OffsetNumber freeOffno = InvalidOffsetNumber;
	OffsetNumber freeNeighborOffno = InvalidOffsetNumber;
	uint8		tupleVersion = 0;

	/* Prepare tuples. */
	etup = palloc0(etupSize);
	TqhnswSetElementTuple(etup, e, codesBytes);
	ntup = palloc0(ntupSize);
	TqhnswSetNeighborTuple(ntup, e, m);

	if (!BlockNumberIsValid(currentPage))
		currentPage = firstElementPage; /* may still be Invalid for an empty
										 * index */

	if (!BlockNumberIsValid(currentPage))
	{
		/*
		 * Empty index: allocate the first element page.  Never reuse block 1
		 * (the codebook page).  Record the new block so the caller can
		 * persist firstElementPage and insertPage in the meta.
		 */
		LockRelationForExtension(index, ExclusiveLock);
		buf = TqNewBuffer(index, MAIN_FORKNUM);
		UnlockRelationForExtension(index, ExclusiveLock);
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
		TqInitPage(buf, page, TQHNSW_PAGE_ID);
		currentPage = BufferGetBlockNumber(buf);
		*updatedInsertPage = currentPage;

		/*
		 * A freshly-initialized page is always empty; run the same size
		 * checks as the loop body below (handles the rare oversized-element
		 * case where etup+ntup don't share a page).
		 */
		if (PageGetFreeSpace(page) >= minCombinedSize)
			newInsertPage = currentPage;

		if (PageGetFreeSpace(page) >= combinedSize)
		{
			nbuf = buf;
			npage = page;
		}
		else if (combinedSize > maxSize && PageGetFreeSpace(page) >= etupSize)
		{
			TqhnswAppendInsertPage(index, &nbuf, &npage, state, page);
		}
		else
		{
			/*
			 * Should never happen: a fresh page should always fit at least
			 * etupSize.  Fail loudly rather than looping.
			 */
			GenericXLogAbort(state);
			UnlockReleaseBuffer(buf);
			elog(ERROR, "tqhnsw element tuple too large for a fresh page in \"%s\"",
				 RelationGetRelationName(index));
		}
	}
	else
	{
		/* Find a page (or two if needed) to insert the tuples. */
		for (;;)
		{
			buf = ReadBuffer(index, currentPage);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buf, 0);

			/* First page where a level-0 element can fit -> next insert hint. */
			if (!BlockNumberIsValid(newInsertPage) && PageGetFreeSpace(page) >= minCombinedSize)
				newInsertPage = currentPage;

			/* Fast path: both tuples fit on the current page. */
			if (PageGetFreeSpace(page) >= combinedSize)
			{
				nbuf = buf;
				npage = page;
				break;
			}

			/*
			 * Try to reuse a deleted element slot on this page (returns false
			 * unless vacuum has tombstoned a tuple here).
			 */
			if (TqhnswFreeOffset(index, buf, page, e, etupSize, ntupSize,
								 &nbuf, &npage,
								 &freeOffno, &freeNeighborOffno, &tupleVersion))
			{
				/*
				 * nbuf is locked; register it in the current state so the
				 * GenericXLogFinish below covers both pages.
				 */
				if (nbuf != buf)
					npage = GenericXLogRegisterBuffer(state, nbuf, 0);
				break;
			}

			/*
			 * Element only, if both can't share a page and this is the last
			 * page.
			 */
			if (combinedSize > maxSize && PageGetFreeSpace(page) >= etupSize &&
				!BlockNumberIsValid(TqhnswPageGetOpaque(page)->nextblkno))
			{
				TqhnswAppendInsertPage(index, &nbuf, &npage, state, page);
				break;
			}

			currentPage = TqhnswPageGetOpaque(page)->nextblkno;

			if (BlockNumberIsValid(currentPage))
			{
				/* Move to the next page. */
				GenericXLogAbort(state);
				UnlockReleaseBuffer(buf);
			}
			else
			{
				Buffer		newbuf;
				Page		newpage;

				/* Append a fresh page after the last and commit the link. */
				TqhnswAppendInsertPage(index, &newbuf, &newpage, state, page);
				GenericXLogFinish(state);
				UnlockReleaseBuffer(buf);

				/* Continue on the new page. */
				buf = newbuf;
				state = GenericXLogStart(index);
				page = GenericXLogRegisterBuffer(state, buf, 0);

				/* Add a second page for the neighbor tuple if needed. */
				if (PageGetFreeSpace(page) < combinedSize)
					TqhnswAppendInsertPage(index, &nbuf, &npage, state, page);
				else
				{
					nbuf = buf;
					npage = page;
				}

				break;
			}
		}
	}							/* end if/else empty-index vs normal page walk */

	e->blkno = BufferGetBlockNumber(buf);
	e->neighborPage = BufferGetBlockNumber(nbuf);

	/* If we only allocated new pages, hint at the neighbor page next time. */
	if (!BlockNumberIsValid(newInsertPage))
		newInsertPage = e->neighborPage;

	if (OffsetNumberIsValid(freeOffno))
	{
		/* Reuse path: overwrite the deleted slot and its neighbor slot. */
		e->offno = freeOffno;
		e->neighborOffno = freeNeighborOffno;

		/*
		 * Carry the deleted tuple's version so iterative-scan cursors stay
		 * valid.
		 */
		etup->version = tupleVersion;
		ntup->version = tupleVersion;
	}
	else
	{
		/* Append path: assign the next offset on each page. */
		e->offno = OffsetNumberNext(PageGetMaxOffsetNumber(page));
		if (nbuf == buf)
			e->neighborOffno = OffsetNumberNext(e->offno);
		else
			e->neighborOffno = FirstOffsetNumber;
	}

	ItemPointerSet(&etup->neighbortid, e->neighborPage, e->neighborOffno);

	if (OffsetNumberIsValid(freeOffno))
	{
		/* Overwrite the previously-deleted slots. */
		if (!PageIndexTupleOverwrite(page, e->offno, (Pointer) etup, etupSize))
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		if (!PageIndexTupleOverwrite(npage, e->neighborOffno, (Pointer) ntup, ntupSize))
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
	}
	else
	{
		if (PageAddItem(page, (Pointer) etup, etupSize, InvalidOffsetNumber, false, false) != e->offno)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		if (PageAddItem(npage, (Pointer) ntup, ntupSize, InvalidOffsetNumber, false, false) != e->neighborOffno)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
	}

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
	if (nbuf != buf)
		UnlockReleaseBuffer(nbuf);

	pfree(etup);
	pfree(ntup);

	if (BlockNumberIsValid(newInsertPage) && newInsertPage != insertPage)
		*updatedInsertPage = newInsertPage;
}

/*
 * Check whether an edge to newElement already exists in the layer-lc slice
 * [startIdx, startIdx+lm) of ntup (mirrors hnswinsert.c ConnectionExists).
 */
static bool
ConnectionExists(TqhnswElement *newElement, TqhnswNeighborTuple ntup, int startIdx, int lm)
{
	for (int i = 0; i < lm; i++)
	{
		ItemPointer indextid = &ntup->indextids[startIdx + i];

		if (!ItemPointerIsValid(indextid))
			break;

		if (ItemPointerGetBlockNumber(indextid) == newElement->blkno &&
			ItemPointerGetOffsetNumber(indextid) == newElement->offno)
			return true;
	}
	return false;
}

/*
 * Compute the single slice slot (relative to the layer-lc slice) that
 * newElement should occupy in neighborElement's neighbor tuple (mirrors
 * hnswinsert.c GetUpdateIndex).  Done UNLOCKED (SHARE via TqhnswLoadNeighbors)
 * since selecting neighbors can take time; the chosen slot is re-validated
 * under the EXCLUSIVE lock in TqhnswUpdateNeighborOnDisk.
 *
 * Returns:
 *   -1  newElement should NOT be added (it loses the prune against the
 *       neighbor's current edges).
 *   -2  the slice has a free/invalid slot; the writer fills the first one.
 *   >=0 the (slice-relative) index whose current occupant is pruned OUT in
 *       favor of newElement.
 *
 * Crucially this never mutates neighborElement's cached neighbor array: the
 * prune is computed into a LOCAL scratch element (palloc'd in updateCtx) so the
 * shared cached element is left untouched (FIX 2).
 */
static int
GetUpdateIndex(Relation index, const TqModel *model, TqMetric metric, HTAB *cache,
			   MemoryContext cacheCtx, MemoryContext updateCtx,
			   TqhnswElement *neighborElement,
			   TqhnswElement *newElement, double distance, int m, int dc, int lc)
{
	char	   *base = NULL;
	int			lm = TqhnswGetLayerM(m, lc);
	int			idx = -1;
	TqhnswNeighborArray *na;
	TqhnswNeighborArray *savedNeighbors;
	MemoryContext oldCtx = MemoryContextSwitchTo(updateCtx);

	/*
	 * Load the neighbor's current layer-lc edges (SHARE). TqhnswLoadNeighbors
	 * assigns the freshly loaded array (in updateCtx) to
	 * neighborElement->neighbors[lc]; since this is a cache-shared element
	 * and updateCtx is reset between neighbors, save and restore the original
	 * pointer so we never leave a dangling array on, or otherwise mutate, the
	 * cached element's authoritative neighbor state (FIX 2).
	 */
	savedNeighbors = TqhnswGetNeighbors(NULL, neighborElement, lc);
	na = TqhnswLoadNeighbors(index, model, metric, neighborElement, lc, m, updateCtx);

	if (na->count < lm)
	{
		/* Free slot exists; writer will pick the first invalid one. */
		idx = -2;
	}
	else
	{
		/*
		 * Full: run the prune in a LOCAL scratch element so neighborElement's
		 * cached neighbor array is never mutated (FIX 2).  The scratch
		 * element shares neighborElement's rhat (used only for distance) and
		 * carries a private neighbor slice seeded from the loaded edges.
		 */
		TqhnswElement scratch;
		TqhnswNeighborArray *sna;
		int			i;

		TqhnswPtrStore(base, scratch.rhat, TqhnswPtrAccess(base, neighborElement->rhat));
		scratch.level = (uint8) lc; /* neighbors[] sized [0..lc] */
		LWLockInitialize(&scratch.lock, tqhnsw_lock_tranche_id);
		sna = palloc(TQHNSW_NEIGHBOR_ARRAY_SIZE(lm));
		sna->count = na->count;
		{
			TqhnswNeighborArrayPtr *snl = palloc0(sizeof(TqhnswNeighborArrayPtr) * (lc + 1));

			TqhnswPtrStore(base, scratch.neighbors, snl);
			TqhnswPtrStore(base, snl[lc], sna);
		}

		/* Resolve each loaded TID to an element so rhat is available. */
		for (i = 0; i < na->count; i++)
		{
			TqhnswElement *ne = TqhnswLoadElement(index, model, metric,
												  &na->items[i].tid, cacheCtx, cache);

			TqhnswPtrStore(base, sna->items[i].element, ne);
			sna->items[i].distance = TqhnswBuildDistance(TqhnswPtrAccess(base, neighborElement->rhat),
														 TqhnswPtrAccess(base, ne->rhat), dc, metric);
		}

		/*
		 * Re-select with newElement competing.  TqhnswUpdateConnection
		 * replaces the pruned-out item IN PLACE and sets idx to its index in
		 * the loaded (on-disk slot order) slice -- the index the single-slot
		 * disk write below needs.  If newElement loses the prune, idx stays
		 * -1.
		 */
		TqhnswUpdateConnection(NULL, &scratch, newElement, distance, lm, lc,
							   dc, metric, &idx);
	}

	/* Restore the cached element's neighbor pointer (FIX 2). */
	{
		TqhnswNeighborArrayPtr *nl = TqhnswPtrAccess(base, neighborElement->neighbors);

		TqhnswPtrStore(base, nl[lc], savedNeighbors);
	}

	MemoryContextSwitchTo(oldCtx);
	return idx;
}

/*
 * Persist the single reciprocal edge neighborElement -> newElement at layer lc
 * (mirrors hnswinsert.c UpdateNeighborOnDisk).  idx is the slice-relative slot
 * computed (unlocked) by GetUpdateIndex.
 *
 * Under the EXCLUSIVE lock we RE-READ the live neighbor tuple, re-check
 * ConnectionExists, resolve a -2 ("any free slot") request against the live
 * slice, and write a SINGLE ItemPointer at slot startIdx + idx -- leaving every
 * other slot (and count) untouched (FIX 1).  This preserves any edge a
 * concurrent insert added to a different slot between the two phases; the
 * residual race of two inserts choosing the same idx is the same one HNSW
 * tolerates.
 */
static void
TqhnswUpdateNeighborOnDisk(Relation index, TqhnswElement *neighborElement,
						   TqhnswElement *newElement, int idx, int m, int lc)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	TqhnswNeighborTuple ntup;
	int			lm = TqhnswGetLayerM(m, lc);
	int			startIdx = (neighborElement->level - lc) * m;

	buf = ReadBuffer(index, neighborElement->neighborPage);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	ntup = (TqhnswNeighborTuple) PageGetItem(page,
											 PageGetItemId(page, neighborElement->neighborOffno));

	/* Already connected (e.g. by a concurrent backend). */
	if (ConnectionExists(newElement, ntup, startIdx, lm))
		idx = -1;
	else if (idx == -2)
	{
		/* Re-resolve the free slot against the live tuple. */
		idx = -1;
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

	/* Write exactly one slot; leave all others (and count) untouched. */
	if (idx >= 0 && idx < ntup->count)
	{
		ItemPointer indextid = &ntup->indextids[idx];

		ItemPointerSet(indextid, newElement->blkno, newElement->offno);
		GenericXLogFinish(state);
	}
	else
		GenericXLogAbort(state);

	UnlockReleaseBuffer(buf);
}

/*
 * For each layer, persist the reciprocal edges from newElement's selected
 * neighbors back to newElement (mirrors hnswinsert.c HnswUpdateNeighborsOnDisk).
 * newElement->neighbors[lc] holds the forward links chosen by
 * TqhnswInsertElement; each target's authoritative on-disk neighbor tuple is
 * re-read under EXCLUSIVE lock here.
 */
void
TqhnswUpdateNeighborsOnDisk(Relation index, const TqModel *model, TqMetric metric,
							HTAB *cache, MemoryContext ctx, TqhnswElement *newElement,
							int m, int dc)
{
	char	   *base = NULL;

	/* Throwaway context for the per-neighbor unlocked prune (mirrors HNSW). */
	MemoryContext updateCtx = AllocSetContextCreate(ctx,
													"tqhnsw insert update context",
													ALLOCSET_DEFAULT_SIZES);

	for (int lc = newElement->level; lc >= 0; lc--)
	{
		TqhnswNeighborArray *na = TqhnswGetNeighbors(NULL, newElement, lc);

		for (int i = 0; i < na->count; i++)
		{
			TqhnswElement *neighborElement = TqhnswPtrAccess(base, na->items[i].element);
			int			idx;

			/* Unlocked: compute the single slot newElement should occupy. */
			idx = GetUpdateIndex(index, model, metric, cache, ctx, updateCtx,
								 neighborElement, newElement,
								 na->items[i].distance, m, dc, lc);

			/* newElement was not selected as a neighbor. */
			if (idx == -1)
			{
				MemoryContextReset(updateCtx);
				continue;
			}

			/* Locked: re-read + single-slot write. */
			TqhnswUpdateNeighborOnDisk(index, neighborElement, newElement, idx, m, lc);

			MemoryContextReset(updateCtx);
		}
	}

	MemoryContextDelete(updateCtx);
}

/*
 * Set the meta-page entry point + insertPage hint + bump nVectors, under an
 * EXCLUSIVE lock on the meta page.  entryUpdate controls whether/how the entry
 * point is updated; newInsertPage (if valid) advances the insert-page hint.
 * nVectors is incremented only when newElement is non-NULL (i.e. a real node
 * is being added).
 */
void
TqhnswUpdateMetaPage(Relation index, TqhnswElement *newElement,
					 TqhnswEntryUpdate entryUpdate, BlockNumber newInsertPage)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	TqhnswMetaPage metap;

	buf = ReadBuffer(index, TQHNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	metap = TqhnswPageGetMeta(page);

	if (entryUpdate == TQHNSW_ENTRY_ALWAYS)
	{
		if (newElement != NULL)
		{
			metap->entryBlkno = newElement->blkno;
			metap->entryOffno = newElement->offno;
			metap->entryLevel = (int16) newElement->level;
		}
		else
		{
			metap->entryBlkno = InvalidBlockNumber;
			metap->entryOffno = InvalidOffsetNumber;
			metap->entryLevel = -1;
		}
	}
	else if (entryUpdate == TQHNSW_ENTRY_GREATER && newElement != NULL &&
			 (metap->entryLevel < 0 || newElement->level > metap->entryLevel))
	{
		metap->entryBlkno = newElement->blkno;
		metap->entryOffno = newElement->offno;
		metap->entryLevel = (int16) newElement->level;
	}

	/* Record the element-page chain head the first time one exists. */
	if (newElement != NULL && !BlockNumberIsValid(metap->firstElementPage))
		metap->firstElementPage = newElement->blkno;

	if (BlockNumberIsValid(newInsertPage))
		metap->insertPage = newInsertPage;

	if (newElement != NULL)
		metap->nVectors += 1;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Read efConstruction and the current entry point from the meta page (SHARE).
 * Also reads insertPage (advisory hint) and firstElementPage (chain head).
 */
static void
TqhnswGetInsertMeta(Relation index, int *m, int *efConstruction,
					BlockNumber *entryBlkno, OffsetNumber *entryOffno, int *entryLevel,
					BlockNumber *insertPage, BlockNumber *firstElementPage)
{
	Buffer		buf;
	Page		page;
	TqhnswMetaPage metap;

	buf = ReadBuffer(index, TQHNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqhnswPageGetMeta(page);

	if (unlikely(metap->magicNumber != TQHNSW_MAGIC_NUMBER))
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqhnsw index is not valid");
	}

	if (unlikely(metap->version != TQHNSW_VERSION))
	{
		uint32		v = metap->version;

		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqhnsw index version %u is unsupported; REINDEX required", v);
	}

	*m = metap->m;
	*efConstruction = metap->efConstruction;
	*entryBlkno = metap->entryBlkno;
	*entryOffno = metap->entryOffno;
	*entryLevel = metap->entryLevel;
	*insertPage = metap->insertPage;
	*firstElementPage = metap->firstElementPage;

	UnlockReleaseBuffer(buf);
}

/*
 * Insert a single quantized tuple into the on-disk graph.
 */
void
TqhnswInsertTupleOnDisk(Relation index, const TqModel *model, TqMetric metric,
						Datum value, ItemPointer heaptid, MemoryContext insertCtx)
{
	int			m;
	int			efConstruction;
	BlockNumber entryBlkno;
	OffsetNumber entryOffno;
	int			entryLevel;
	BlockNumber insertPage;
	BlockNumber firstElementPage;
	int			dimCodes = model->dimCodes;
	int			codesBytes = TQ_CODES_BYTES(dimCodes, model->bits);
	HTAB	   *cache;
	TqhnswElement *newElement;
	BlockNumber updatedInsertPage = InvalidBlockNumber;
	LOCKMODE	lockmode = ShareLock;

	/*
	 * Skip zero-norm vectors under cosine (the operator returns NaN for
	 * them), mirroring hnsw/ivfflat.  The build callback already skips them
	 * before routing here; this guards aminsert.  The scratch lives in the
	 * caller's short-lived context (insertCtx / build tmpCtx reset).
	 */
	if (metric == TQ_METRIC_COSINE)
	{
		const TqTypeInfo *ti = TqGetTypeInfo(index, TQHNSW_TYPE_INFO_PROC);
		float	   *vscratch = palloc(sizeof(float) * model->dim);
		const float *fv;

		value = PointerGetDatum(PG_DETOAST_DATUM(value));
		fv = ti->toFloat(value, vscratch, model->dim);
		if (!TqCheckNorm(fv, model->dim))
			return;
	}

	/*
	 * Take a shared page lock for the whole insert.  This lets a future
	 * graph-mutating vacuum (#2) drain in-flight inserts before repairing the
	 * graph.  A page lock is used so it does not interfere with buffer locks
	 * (or reads while vacuuming).  Upgraded to ExclusiveLock below when this
	 * insert may change the entry point.  Mirrors hnswinsert.c
	 * HnswInsertTupleOnDisk.
	 */
	LockPage(index, TQHNSW_UPDATE_LOCK, lockmode);

	TqhnswGetInsertMeta(index, &m, &efConstruction, &entryBlkno, &entryOffno, &entryLevel,
						&insertPage, &firstElementPage);

	cache = TqhnswCreateElementCache(insertCtx);
	newElement = TqhnswQuantizeElement(index, model, metric, m, dimCodes,
									   codesBytes, heaptid, value);

	/*
	 * Prevent concurrent inserts when this element may change the entry point
	 * (empty index, or its level exceeds the current entry level) -- the same
	 * condition HNSW uses.  Re-read the entry point after the upgrade since
	 * it may have advanced while we held only the shared lock.
	 */
	if (!BlockNumberIsValid(entryBlkno) || entryLevel < 0 ||
		newElement->level > entryLevel)
	{
		UnlockPage(index, TQHNSW_UPDATE_LOCK, lockmode);
		lockmode = ExclusiveLock;
		LockPage(index, TQHNSW_UPDATE_LOCK, lockmode);

		TqhnswGetInsertMeta(index, &m, &efConstruction, &entryBlkno, &entryOffno, &entryLevel,
							&insertPage, &firstElementPage);
	}

	if (!BlockNumberIsValid(entryBlkno) || entryLevel < 0)
	{
		/*
		 * Empty index: the new element becomes the sole node and the entry
		 * point. Its neighbor tuple is all-invalid (no forward links to
		 * write).
		 */
		TqhnswAddElementOnDisk(index, newElement, m, codesBytes,
							   insertPage, firstElementPage, &updatedInsertPage);
		TqhnswUpdateMetaPage(index, newElement, TQHNSW_ENTRY_ALWAYS, updatedInsertPage);

		UnlockPage(index, TQHNSW_UPDATE_LOCK, lockmode);
		return;
	}

	/*
	 * Non-empty: materialize the entry point and run Alg 1 over the disk
	 * graph.
	 */
	{
		ItemPointerData entryTid;
		TqhnswElement *entryPoint;

		ItemPointerSet(&entryTid, entryBlkno, entryOffno);
		entryPoint = TqhnswLoadElement(index, model, metric, &entryTid,
									   insertCtx, cache);

		TqhnswInsertElement(NULL /* base */ , index, model, cache, insertCtx,
							newElement, entryPoint, m, efConstruction, dimCodes,
							metric, false /* existing */ );
	}

	/* Write the new element (forward links from newElement->neighbors). */
	TqhnswAddElementOnDisk(index, newElement, m, codesBytes,
						   insertPage, firstElementPage, &updatedInsertPage);

	/* Persist reciprocal edges under per-neighbor EXCLUSIVE locks. */
	TqhnswUpdateNeighborsOnDisk(index, model, metric, cache, insertCtx, newElement, m, dimCodes);

	/* Entry-point update (if higher) + insert-page hint + nVectors bump. */
	TqhnswUpdateMetaPage(index, newElement,
						 newElement->level > entryLevel ? TQHNSW_ENTRY_GREATER : TQHNSW_ENTRY_NO_UPDATE,
						 updatedInsertPage);

	/* Release the page-level update lock. */
	UnlockPage(index, TQHNSW_UPDATE_LOCK, lockmode);
}

bool
tqhnswinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
			 Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
			 ,bool indexUnchanged
#endif
			 ,struct IndexInfo *indexInfo
)
{
	MemoryContext oldCtx;
	MemoryContext insertCtx;
	TqModel    *model;
	TqMetric	metric;

	/* Skip nulls. */
	if (isnull[0])
		return false;

	/*
	 * Unlike hnsw, the on-disk insert path takes element LWLocks, so the
	 * tranche must be valid here too.  Under shared_preload_libraries
	 * _PG_init skips registration; do it lazily on first use (0 = never
	 * assigned).
	 */
	if (tqhnsw_lock_tranche_id == 0)
		TqhnswInitLockTranche();

	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "tqhnsw insert temporary context",
									  ALLOCSET_DEFAULT_SIZES);
	oldCtx = MemoryContextSwitchTo(insertCtx);

	model = TqhnswGetCachedModel(index);
	metric = model->metric;

	TqhnswInsertTupleOnDisk(index, model, metric, values[0], heap_tid, insertCtx);

	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	/* tqhnsw is never a unique index. */
	return false;
}
