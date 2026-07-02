#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "nodes/pg_list.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "tqhnsw.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#if PG_VERSION_NUM >= 180000
#define vacuum_delay_point() vacuum_delay_point(false)
#endif

/* ------------------------------------------------------------------------- *
 * Deleted-set: a dynahash set of dead element TIDs (key = ItemPointerData).  *
 * Populated by RemoveHeapTids; consulted via DeletedContains by NeedsUpdated, *
 * the entry-point repair, and ConfirmRepaired to find edges pointing at       *
 * soon-to-be-removed nodes.                                                   *
 * ------------------------------------------------------------------------- */

static HTAB *
TqhnswCreateDeletedSet(MemoryContext ctx)
{
	HASHCTL		hashctl;

	memset(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize = sizeof(ItemPointerData);
	hashctl.entrysize = sizeof(ItemPointerData);
	hashctl.hcxt = ctx;
	return hash_create("tqhnsw vacuum deleted set", 256, &hashctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static bool
DeletedContains(HTAB *deleted, ItemPointer tid)
{
	bool		found;

	(void) hash_search(deleted, tid, HASH_FIND, &found);
	return found;
}

static void
DeletedAdd(HTAB *deleted, BlockNumber blkno, OffsetNumber offno)
{
	ItemPointerData ip;
	bool		found;

	ItemPointerSet(&ip, blkno, offno);
	(void) hash_search(deleted, &ip, HASH_ENTER, &found);
}

/*
 * Initialize the vacuum state from the index meta page.
 */
static void
InitVacuumState(TqhnswVacuumState *vs, IndexVacuumInfo *info,
				IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
				void *callback_state)
{
	Relation	index = info->index;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/* Recount from scratch: a caller may pass back a prior call's stats. */
	stats->num_index_tuples = 0;

	vs->index = index;
	vs->stats = stats;
	vs->callback = callback;
	vs->callback_state = callback_state;
	vs->model = TqhnswGetCachedModel(index);
	vs->metric = vs->model->metric;
	vs->dimCodes = vs->model->dimCodes;
	vs->bas = GetAccessStrategy(BAS_BULKREAD);
	vs->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "tqhnsw vacuum temporary context",
									   ALLOCSET_DEFAULT_SIZES);
	vs->deleted = TqhnswCreateDeletedSet(CurrentMemoryContext);
	vs->highestBlkno = InvalidBlockNumber;
	vs->highestOffno = InvalidOffsetNumber;
	vs->highestLevel = -1;
	vs->fallbackBlkno = InvalidBlockNumber;
	vs->fallbackOffno = InvalidOffsetNumber;
	vs->fallbackLevel = -1;

	/* m, efConstruction, firstElementPage from the meta page. */
	TqhnswGetMetaInfo(index, NULL, NULL, &vs->m, NULL, NULL, NULL,
					  &vs->efConstruction, &vs->firstElementPage);
}

static void
FreeVacuumState(TqhnswVacuumState *vs)
{
	hash_destroy(vs->deleted);
	FreeAccessStrategy(vs->bas);
	MemoryContextDelete(vs->tmpCtx);
}

/* ------------------------------------------------------------------------- *
 * Pass 1: RemoveHeapTids                                                     *
 * Walk the element-page chain; for each live element tuple, ask the vacuum   *
 * callback whether its heap TID is dead.  If so, invalidate the heaptid and  *
 * record the element in the deleted set.  Track the highest-level surviving  *
 * element (and a fallback) for later entry-point replacement.                *
 * Single heaptid -> no array compaction (unlike HNSW's heaptids[]).          *
 * ------------------------------------------------------------------------- */

static void
RemoveHeapTids(TqhnswVacuumState *vs)
{
	Relation	index = vs->index;
	BufferAccessStrategy bas = vs->bas;
	IndexBulkDeleteResult *stats = vs->stats;
	BlockNumber blkno = vs->firstElementPage;
	int			codesBytes = TQ_CODES_BYTES(vs->dimCodes, vs->model->bits);
	Size		etupSize = TQHNSW_ELEMENT_TUPLE_SIZE(codesBytes);

	while (BlockNumberIsValid(blkno))
	{
		Buffer		buf;
		Page		page;
		GenericXLogState *state;
		OffsetNumber offno;
		OffsetNumber maxoffno;
		bool		updated = false;

		vacuum_delay_point();

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);
		maxoffno = PageGetMaxOffsetNumber(page);

		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			ItemId		itemid = PageGetItemId(page, offno);
			TqhnswElementTuple etup;

			if (!ItemIdIsUsed(itemid))
				continue;

			etup = (TqhnswElementTuple) PageGetItem(page, itemid);

			/* Skip neighbor tuples and side-page chunks. */
			if (etup->type != TQHNSW_ELEMENT_TUPLE_TYPE ||
				ItemIdGetLength(itemid) != etupSize)
				continue;

			/* Skip tuples already marked deleted by a prior vacuum. */
			if (etup->deleted)
				continue;

			if (ItemPointerIsValid(&etup->heaptid))
			{
				if (vs->callback(&etup->heaptid, vs->callback_state))
				{
					ItemPointerSetInvalid(&etup->heaptid);
					stats->tuples_removed++;
					updated = true;
				}
				else
					stats->num_index_tuples++;
			}

			if (!ItemPointerIsValid(&etup->heaptid))
			{
				/* Dead: record for graph repair. */
				DeletedAdd(vs->deleted, blkno, offno);
			}
			else if (etup->level > vs->highestLevel)
			{
				/*
				 * Track the highest-level surviving element unconditionally
				 * (the entry-point check is deferred to RepairGraphEntryPoint,
				 * under the update lock, since the entry point can change
				 * concurrently).  Demote the previous highest to the fallback
				 * so a replacement is available if the highest later proves to
				 * be the entry point itself.
				 */
				if (BlockNumberIsValid(vs->highestBlkno))
				{
					vs->fallbackBlkno = vs->highestBlkno;
					vs->fallbackOffno = vs->highestOffno;
					vs->fallbackLevel = vs->highestLevel;
				}

				vs->highestBlkno = blkno;
				vs->highestOffno = offno;
				vs->highestLevel = etup->level;
			}
			else if (etup->level > vs->fallbackLevel)
			{
				/* Track the second-highest surviving element. */
				vs->fallbackBlkno = blkno;
				vs->fallbackOffno = offno;
				vs->fallbackLevel = etup->level;
			}
		}

		blkno = TqhnswPageGetOpaque(page)->nextblkno;

		if (updated)
			GenericXLogFinish(state);
		else
			GenericXLogAbort(state);

		UnlockReleaseBuffer(buf);
	}
}

/* ------------------------------------------------------------------------- *
 * Pass 2: RepairGraph                                                        *
 * For every surviving element whose neighbor tuple references a dead node    *
 * (or is under-full), re-select its neighbors over the surviving graph and   *
 * rewrite its forward + reciprocal edges.  Mirrors hnswvacuum.c.             *
 * ------------------------------------------------------------------------- */

/*
 * Whether element's on-disk neighbor tuple references a dead node, or its
 * level-0 slice is not full.  Mirrors hnswvacuum.c NeedsUpdated.
 */
static bool
NeedsUpdated(TqhnswVacuumState *vs, TqhnswElement *element)
{
	Relation	index = vs->index;
	BufferAccessStrategy bas = vs->bas;
	Buffer		buf;
	Page		page;
	TqhnswNeighborTuple ntup;
	bool		needsUpdated = false;

	buf = ReadBufferExtended(index, MAIN_FORKNUM, element->neighborPage, RBM_NORMAL, bas);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	ntup = (TqhnswNeighborTuple) PageGetItem(page,
											 PageGetItemId(page, element->neighborOffno));

	Assert(ntup->type == TQHNSW_NEIGHBOR_TUPLE_TYPE);

	for (int i = 0; i < ntup->count; i++)
	{
		ItemPointer indextid = &ntup->indextids[i];

		if (!ItemPointerIsValid(indextid))
			continue;

		if (DeletedContains(vs->deleted, indextid))
		{
			needsUpdated = true;
			break;
		}
	}

	/*
	 * Also repair if the layer-0 slice (the last slots of the tuple) is not
	 * full -- this can happen when too many candidates were deleted during a
	 * prior insert.  Mirrors HNSW's check on the final slot.  count should
	 * always be > 0, but guard it.
	 */
	if (!needsUpdated && ntup->count > 0)
		needsUpdated = !ItemPointerIsValid(&ntup->indextids[ntup->count - 1]);

	UnlockReleaseBuffer(buf);

	return needsUpdated;
}

/*
 * Re-init an element's in-memory neighbor arrays (empty, per-layer sized),
 * mirroring TqhnswQuantizeElement's allocation so TqhnswInsertElement can
 * refill them.
 */
static void
TqhnswResetNeighbors(TqhnswElement *element, int m)
{
	char	   *base = NULL;
	int			lc;

	{
		TqhnswNeighborArrayPtr *neighbors = palloc(sizeof(TqhnswNeighborArrayPtr) * (element->level + 1));

		TqhnswPtrStore(base, element->neighbors, neighbors);
		for (lc = 0; lc <= element->level; lc++)
		{
			int			lm = TqhnswGetLayerM(m, lc);
			TqhnswNeighborArray *na = palloc(TQHNSW_NEIGHBOR_ARRAY_SIZE(lm));

			na->count = 0;
			TqhnswPtrStore(base, neighbors[lc], na);
		}
	}
}

/*
 * Repair the graph for a single element: re-select its neighbors over the
 * surviving graph and rewrite both its forward neighbor tuple (full overwrite)
 * and the reciprocal edges back to it.  Mirrors hnswvacuum.c RepairGraphElement.
 *
 * The caller passes a fresh per-element cache rooted in ctx, with the element
 * and entry point already loaded into ctx via TqhnswLoadElement (so
 * element->rhat exists).  The cache must be rooted in a context that outlives
 * the cache itself (else a later cache hit can return freed memory).
 *
 * entryPoint may be NULL: TqhnswInsertElement then leaves the element with an
 * empty neighbor list and an all-invalid neighbor tuple is written.
 */
static void
RepairGraphElement(TqhnswVacuumState *vs, TqhnswElement *element,
				   TqhnswElement *entryPoint, HTAB *cache, MemoryContext ctx)
{
	Relation	index = vs->index;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	int			m = vs->m;
	Size		ntupSize = TQHNSW_NEIGHBOR_TUPLE_SIZE(element->level, m);
	TqhnswNeighborTuple ntup;

	/* Skip if element is the entry point. */
	if (entryPoint != NULL &&
		element->blkno == entryPoint->blkno && element->offno == entryPoint->offno)
		return;

	/* Reset neighbor arrays so the re-search starts clean. */
	TqhnswResetNeighbors(element, m);

	/*
	 * Re-search the graph for this already-present element (existing=true:
	 * skip self, exclude dead nodes from the beam, fill only forward
	 * neighbors).
	 */
	TqhnswInsertElement(NULL /* base */ , index, vs->model, cache, ctx,
						element, entryPoint, vs->m, vs->efConstruction,
						vs->dimCodes, vs->metric, true /* existing */ );

	/* Build the new neighbor tuple and FULL-overwrite the on-disk tuple. */
	ntup = palloc0(ntupSize);
	TqhnswSetNeighborTuple(ntup, element, vs->m);

	buf = ReadBufferExtended(index, MAIN_FORKNUM, element->neighborPage, RBM_NORMAL, vs->bas);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	if (!PageIndexTupleOverwrite(page, element->neighborOffno, (Pointer) ntup, ntupSize))
		elog(ERROR, "failed to overwrite neighbor tuple in \"%s\"",
			 RelationGetRelationName(index));

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	/* Persist the reciprocal edges back to element. */
	TqhnswUpdateNeighborsOnDisk(index, vs->model, vs->metric, cache, ctx,
								element, vs->m, vs->dimCodes);
}

/*
 * Load an element by (blkno, offno) into ctx, registered in cache.
 * Returns NULL when the TID is invalid.
 */
static TqhnswElement *
LoadByTid(TqhnswVacuumState *vs, BlockNumber blkno, OffsetNumber offno,
		  HTAB *cache, MemoryContext ctx)
{
	ItemPointerData tid;

	if (!BlockNumberIsValid(blkno) || !OffsetNumberIsValid(offno))
		return NULL;

	ItemPointerSet(&tid, blkno, offno);
	return TqhnswLoadElement(vs->index, vs->model, vs->metric, &tid, ctx, cache);
}

/*
 * Repair the graph entry point.  Mirrors hnswvacuum.c RepairGraphEntryPoint:
 * first repair the highest surviving point under a SHARE lock (it may be stale
 * due to concurrent inserts), then under an EXCLUSIVE lock either replace the
 * entry point (if it is being deleted) or repair it.
 */
static void
RepairGraphEntryPoint(TqhnswVacuumState *vs)
{
	char	   *base = NULL;
	Relation	index = vs->index;
	BlockNumber entryBlkno;
	OffsetNumber entryOffno;
	int			entryLevel;
	HTAB	   *cache;
	TqhnswElement *highestPoint;
	MemoryContext oldCtx = MemoryContextSwitchTo(vs->tmpCtx);

	cache = TqhnswCreateElementCache(vs->tmpCtx);

	/*
	 * Repair the highest surviving element.  It may be outdated due to inserts
	 * during/after RemoveHeapTids; the SHARE lock blocks new inserts from
	 * changing the entry point while we work.
	 */
	highestPoint = LoadByTid(vs, vs->highestBlkno, vs->highestOffno, cache, vs->tmpCtx);
	if (highestPoint != NULL)
	{
		LockPage(index, TQHNSW_UPDATE_LOCK, ShareLock);

		/* Get the latest entry point. */
		TqhnswGetMetaInfo(index, NULL, NULL, NULL, &entryBlkno, &entryOffno,
						  &entryLevel, NULL, NULL);

		/*
		 * If the highest surviving element is itself the current entry point
		 * (a concurrent insert may have promoted it after RemoveHeapTids), it
		 * cannot serve as the repair entry for its own neighborhood -- fall
		 * back to the second-highest surviving element (or NULL if none).
		 */
		if (BlockNumberIsValid(entryBlkno) &&
			entryBlkno == vs->highestBlkno && entryOffno == vs->highestOffno)
		{
			if (BlockNumberIsValid(vs->fallbackBlkno))
				highestPoint = LoadByTid(vs, vs->fallbackBlkno, vs->fallbackOffno, cache, vs->tmpCtx);
			else
				highestPoint = NULL;
		}

		if (highestPoint != NULL && NeedsUpdated(vs, highestPoint))
		{
			TqhnswElement *ep = LoadByTid(vs, entryBlkno, entryOffno, cache, vs->tmpCtx);

			RepairGraphElement(vs, highestPoint, ep, cache, vs->tmpCtx);
		}

		UnlockPage(index, TQHNSW_UPDATE_LOCK, ShareLock);
	}

	/* Prevent concurrent inserts while we may change the entry point. */
	LockPage(index, TQHNSW_UPDATE_LOCK, ExclusiveLock);

	/* Get the latest entry point. */
	TqhnswGetMetaInfo(index, NULL, NULL, NULL, &entryBlkno, &entryOffno,
					  &entryLevel, NULL, NULL);

	if (BlockNumberIsValid(entryBlkno) && entryLevel >= 0)
	{
		ItemPointerData epData;

		ItemPointerSet(&epData, entryBlkno, entryOffno);

		if (DeletedContains(vs->deleted, &epData))
		{
			/*
			 * The entry point is being deleted.  Replace it with the highest
			 * surviving point (may be NULL -> empties the entry point until
			 * an element is repaired).
			 */
			TqhnswUpdateMetaPage(index, highestPoint, TQHNSW_ENTRY_ALWAYS,
								 InvalidBlockNumber);
		}
		else
		{
			TqhnswElement *entryPoint = LoadByTid(vs, entryBlkno, entryOffno, cache, vs->tmpCtx);

			if (NeedsUpdated(vs, entryPoint))
			{
				/*
				 * If we repaired highestPoint above, its in-memory neighbor
				 * arrays now hold the freshly-selected (possibly partial)
				 * edges. Reset every layer slot to NULL so the search below
				 * reloads the authoritative edges from disk (mirrors
				 * hnswvacuum.c's HnswPtrStore reset before reusing
				 * highestPoint as the entry point).  The pointer array itself
				 * stays allocated so TqhnswSearchLayer's per-layer NULL check
				 * is valid.
				 */
				if (highestPoint != NULL && !TqhnswPtrIsNull(base, highestPoint->neighbors))
				{
					TqhnswNeighborArrayPtr *nl = TqhnswPtrAccess(base, highestPoint->neighbors);
					int			lc;

					for (lc = 0; lc <= highestPoint->level; lc++)
						TqhnswPtrStore(base, nl[lc], (TqhnswNeighborArray *) NULL);
				}

				RepairGraphElement(vs, entryPoint, highestPoint, cache, vs->tmpCtx);
			}
		}
	}

	UnlockPage(index, TQHNSW_UPDATE_LOCK, ExclusiveLock);

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(vs->tmpCtx);
}

/*
 * A surviving element to (potentially) repair, recorded by TID so the page can
 * be released before the per-element repair.
 */
typedef struct TqhnswRepairItem
{
	BlockNumber blkno;
	OffsetNumber offno;
} TqhnswRepairItem;

/*
 * Repair the graph for all surviving elements.  Mirrors hnswvacuum.c RepairGraph:
 * drain in-flight inserts, repair the entry point, then sweep the element pages,
 * repairing each element whose neighbor tuple references a dead node.
 */
static void
RepairGraph(TqhnswVacuumState *vs)
{
	Relation	index = vs->index;
	BufferAccessStrategy bas = vs->bas;
	BlockNumber blkno = vs->firstElementPage;
	int			codesBytes = TQ_CODES_BYTES(vs->dimCodes, vs->model->bits);
	Size		etupSize = TQHNSW_ELEMENT_TUPLE_SIZE(codesBytes);

	/*
	 * Wait for inserts to complete.  Inserts before this point may have
	 * neighbors about to be deleted; inserts after this point will not.
	 */
	LockPage(index, TQHNSW_UPDATE_LOCK, ExclusiveLock);
	UnlockPage(index, TQHNSW_UPDATE_LOCK, ExclusiveLock);

	/* Repair entry point first. */
	RepairGraphEntryPoint(vs);

	while (BlockNumberIsValid(blkno))
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offno;
		OffsetNumber maxoffno;
		List	   *items = NIL;
		ListCell   *lc2;
		MemoryContext oldCtx;
		MemoryContext repairCtx;
		BlockNumber nextblkno;

		vacuum_delay_point();

		oldCtx = MemoryContextSwitchTo(vs->tmpCtx);

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		maxoffno = PageGetMaxOffsetNumber(page);

		/* Record live element TIDs, releasing the page lock before repairing. */
		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			ItemId		itemid = PageGetItemId(page, offno);
			TqhnswElementTuple etup;
			TqhnswRepairItem *item;

			if (!ItemIdIsUsed(itemid))
				continue;

			etup = (TqhnswElementTuple) PageGetItem(page, itemid);

			/* Skip neighbor tuples and side-page chunks. */
			if (etup->type != TQHNSW_ELEMENT_TUPLE_TYPE ||
				ItemIdGetLength(itemid) != etupSize)
				continue;

			/* Skip elements being deleted. */
			if (!ItemPointerIsValid(&etup->heaptid))
				continue;

			item = palloc(sizeof(TqhnswRepairItem));
			item->blkno = blkno;
			item->offno = offno;
			items = lappend(items, item);
		}

		nextblkno = TqhnswPageGetOpaque(page)->nextblkno;

		UnlockReleaseBuffer(buf);

		/*
		 * Per-element repair context, child of tmpCtx, reset every iteration
		 * so a page's element caches and their loaded neighborhoods do not
		 * pile up within a single page sweep.  The `items` list and its
		 * TqhnswRepairItem structs stay in vs->tmpCtx (page-level) so they
		 * survive these resets.
		 */
		repairCtx = AllocSetContextCreate(vs->tmpCtx,
										  "tqhnsw vacuum repair element context",
										  ALLOCSET_DEFAULT_SIZES);

		/* Repair each recorded element. */
		foreach(lc2, items)
		{
			TqhnswRepairItem *item = (TqhnswRepairItem *) lfirst(lc2);
			HTAB	   *cache;
			TqhnswElement *element;
			TqhnswElement *entryPoint;
			BlockNumber entryBlkno;
			OffsetNumber entryOffno;
			int			entryLevel;
			LOCKMODE	lockmode = ShareLock;

			/*
			 * Reset the per-element context up front (rather than at the end)
			 * so every exit path -- including the !NeedsUpdated `continue`
			 * below -- reclaims the prior iteration's cache and loads.  The
			 * cache and the elements loaded through it both live in
			 * repairCtx, which outlives the cache for the whole iteration and
			 * is only reset here, before the next iteration begins.
			 */
			MemoryContextReset(repairCtx);

			/* Fresh cache rooted in repairCtx; loads outlive the cache. */
			cache = TqhnswCreateElementCache(repairCtx);
			element = LoadByTid(vs, item->blkno, item->offno, cache, repairCtx);
			Assert(element != NULL);

			/* Check if any neighbors point to deleted values. */
			if (!NeedsUpdated(vs, element))
				continue;

			LockPage(index, TQHNSW_UPDATE_LOCK, lockmode);

			TqhnswGetMetaInfo(index, NULL, NULL, NULL, &entryBlkno, &entryOffno,
							  &entryLevel, NULL, NULL);

			/*
			 * Prevent concurrent inserts when likely updating the entry
			 * point.
			 */
			if (!BlockNumberIsValid(entryBlkno) || entryLevel < 0 ||
				element->level > entryLevel)
			{
				UnlockPage(index, TQHNSW_UPDATE_LOCK, lockmode);
				lockmode = ExclusiveLock;
				LockPage(index, TQHNSW_UPDATE_LOCK, lockmode);

				/* Re-read the entry point after the lock upgrade. */
				TqhnswGetMetaInfo(index, NULL, NULL, NULL, &entryBlkno, &entryOffno,
								  &entryLevel, NULL, NULL);
			}

			entryPoint = LoadByTid(vs, entryBlkno, entryOffno, cache, repairCtx);

			RepairGraphElement(vs, element, entryPoint, cache, repairCtx);

			/*
			 * Update the metapage if needed.  Should only happen if the entry
			 * point was replaced and the highest point was outdated.
			 */
			if (entryPoint == NULL || element->level > entryLevel)
				TqhnswUpdateMetaPage(index, element, TQHNSW_ENTRY_GREATER,
									 InvalidBlockNumber);

			UnlockPage(index, TQHNSW_UPDATE_LOCK, lockmode);
		}

		MemoryContextDelete(repairCtx);

		blkno = nextblkno;

		MemoryContextSwitchTo(oldCtx);
		MemoryContextReset(vs->tmpCtx);
	}
}

/* ------------------------------------------------------------------------- *
 * Pass 3: ConfirmRepaired                                                    *
 * Walk every surviving element and verify none still references a            *
 * to-be-deleted element in its neighbor list.  If one does, the graph was    *
 * not fully repaired and proceeding to MarkDeleted would corrupt the index,  *
 * so error out first.  Mirrors hnswvacuum.c ConfirmRepaired.                 *
 * ------------------------------------------------------------------------- */

static void
ConfirmRepaired(TqhnswVacuumState *vs)
{
	Relation	index = vs->index;
	BufferAccessStrategy bas = vs->bas;
	BlockNumber blkno = vs->firstElementPage;
	int			codesBytes = TQ_CODES_BYTES(vs->dimCodes, vs->model->bits);
	Size		etupSize = TQHNSW_ELEMENT_TUPLE_SIZE(codesBytes);

	while (BlockNumberIsValid(blkno))
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offno;
		OffsetNumber maxoffno;

		vacuum_delay_point();

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		maxoffno = PageGetMaxOffsetNumber(page);

		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			ItemId		itemid = PageGetItemId(page, offno);
			TqhnswElementTuple etup;
			TqhnswNeighborTuple ntup;
			Buffer		nbuf;
			Page		npage;
			BlockNumber neighborPage;
			OffsetNumber neighborOffno;

			if (!ItemIdIsUsed(itemid))
				continue;

			etup = (TqhnswElementTuple) PageGetItem(page, itemid);

			/* Skip neighbor tuples and side-page chunks. */
			if (etup->type != TQHNSW_ELEMENT_TUPLE_TYPE ||
				ItemIdGetLength(itemid) != etupSize)
				continue;

			/* Only surviving elements must have a fully repaired neighborhood. */
			if (etup->deleted || !ItemPointerIsValid(&etup->heaptid))
				continue;

			/* Resolve the neighbor page (may be the same buffer). */
			neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
			neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);

			if (neighborPage == blkno)
			{
				nbuf = buf;
				npage = page;
			}
			else
			{
				nbuf = ReadBufferExtended(index, MAIN_FORKNUM, neighborPage, RBM_NORMAL, bas);
				LockBuffer(nbuf, BUFFER_LOCK_SHARE);
				npage = BufferGetPage(nbuf);
			}

			ntup = (TqhnswNeighborTuple) PageGetItem(npage,
													 PageGetItemId(npage, neighborOffno));

			for (int i = 0; i < ntup->count; i++)
			{
				ItemPointer indextid = &ntup->indextids[i];

				if (!ItemPointerIsValid(indextid))
					continue;

				if (DeletedContains(vs->deleted, indextid))
					elog(ERROR, "tqhnsw graph not repaired");
			}

			if (nbuf != buf)
				UnlockReleaseBuffer(nbuf);
		}

		blkno = TqhnswPageGetOpaque(page)->nextblkno;

		UnlockReleaseBuffer(buf);
	}
}

/* ------------------------------------------------------------------------- *
 * Pass 4: MarkDeleted                                                        *
 * Drain in-flight scans, then mark dead element tuples deleted=1, zero their  *
 * codes, invalidate their neighbor tuples, and bump versions.  Publishes the  *
 * first reclaimable page as the insert-page hint.  Mirrors hnswvacuum.c.      *
 * ------------------------------------------------------------------------- */

static void
MarkDeleted(TqhnswVacuumState *vs)
{
	Relation	index = vs->index;
	BufferAccessStrategy bas = vs->bas;
	BlockNumber blkno = vs->firstElementPage;
	BlockNumber insertPage = InvalidBlockNumber;
	int			codesBytes = TQ_CODES_BYTES(vs->dimCodes, vs->model->bits);
	Size		etupSize = TQHNSW_ELEMENT_TUPLE_SIZE(codesBytes);

	/*
	 * Wait for index scans to complete.  Scans before this point may
	 * reference tuples about to be deleted; scans after this point will not,
	 * since the graph has been repaired.
	 */
	LockPage(index, TQHNSW_SCAN_LOCK, ExclusiveLock);
	UnlockPage(index, TQHNSW_SCAN_LOCK, ExclusiveLock);

	while (BlockNumberIsValid(blkno))
	{
		Buffer		buf;
		Page		page;
		GenericXLogState *state;
		OffsetNumber offno;
		OffsetNumber maxoffno;

		vacuum_delay_point();

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);

		/*
		 * ambulkdelete cannot delete entries from pages pinned by other
		 * backends.
		 * https://www.postgresql.org/docs/current/index-locking.html
		 */
		LockBufferForCleanup(buf);

		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);
		maxoffno = PageGetMaxOffsetNumber(page);

		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			ItemId		itemid = PageGetItemId(page, offno);
			TqhnswElementTuple etup;
			TqhnswNeighborTuple ntup;
			Buffer		nbuf;
			Page		npage;
			BlockNumber neighborPage;
			OffsetNumber neighborOffno;

			if (!ItemIdIsUsed(itemid))
				continue;

			etup = (TqhnswElementTuple) PageGetItem(page, itemid);

			/* Skip neighbor tuples and side-page chunks. */
			if (etup->type != TQHNSW_ELEMENT_TUPLE_TYPE ||
				ItemIdGetLength(itemid) != etupSize)
				continue;

			/* Already deleted: this slot is reclaimable. */
			if (etup->deleted)
			{
				if (!BlockNumberIsValid(insertPage))
					insertPage = blkno;
				continue;
			}

			/* Skip live tuples. */
			if (ItemPointerIsValid(&etup->heaptid))
				continue;

			/* Resolve the neighbor page (may be the same buffer). */
			neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
			neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);

			if (neighborPage == blkno)
			{
				nbuf = buf;
				npage = page;
			}
			else
			{
				nbuf = ReadBufferExtended(index, MAIN_FORKNUM, neighborPage, RBM_NORMAL, bas);
				LockBuffer(nbuf, BUFFER_LOCK_EXCLUSIVE);
				npage = GenericXLogRegisterBuffer(state, nbuf, 0);
			}

			ntup = (TqhnswNeighborTuple) PageGetItem(npage,
													 PageGetItemId(npage, neighborOffno));

			/* Mark element deleted and zero its codes. */
			etup->deleted = 1;
			memset(etup->codes, 0, codesBytes);

			/* Invalidate all neighbor slots. */
			for (int i = 0; i < ntup->count; i++)
				ItemPointerSetInvalid(&ntup->indextids[i]);

			/*
			 * Bump version on both tuples to avoid incorrect reads for
			 * iterative scans.  Reserve some bits for future use.
			 */
			etup->version++;
			if (etup->version > 15)
				etup->version = 1;
			ntup->version = etup->version;

			/*
			 * Tuples are modified in place (no size change), so no
			 * PageIndexTupleOverwrite is needed.
			 */

			GenericXLogFinish(state);
			if (nbuf != buf)
				UnlockReleaseBuffer(nbuf);

			/* First reclaimable page becomes the insert-page hint. */
			if (!BlockNumberIsValid(insertPage))
				insertPage = blkno;

			/* Start a fresh xlog state for the next tuple. */
			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buf, 0);
		}

		blkno = TqhnswPageGetOpaque(page)->nextblkno;

		/* The trailing state is unmodified. */
		GenericXLogAbort(state);
		UnlockReleaseBuffer(buf);
	}

	/* Publish the insert-page hint last, after all marking is done. */
	TqhnswUpdateMetaPage(index, NULL, TQHNSW_ENTRY_NO_UPDATE, insertPage);
}

/*
 * Bulk delete tuples from the index (four passes: remove heap TIDs, repair the
 * graph, confirm the repair, then tombstone dead element tuples).
 */
IndexBulkDeleteResult *
tqhnswbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				 IndexBulkDeleteCallback callback, void *callback_state)
{
	TqhnswVacuumState vs;
	IndexBulkDeleteResult *result;

	InitVacuumState(&vs, info, stats, callback, callback_state);

	/* Pass 1: remove dead heap TIDs. */
	RemoveHeapTids(&vs);

	/* Pass 2: repair the graph around removed nodes. */
	RepairGraph(&vs);

	/* Pass 3: confirm no surviving element still points at a dead node. */
	ConfirmRepaired(&vs);

	/* Pass 4: mark dead element tuples deleted and reclaim slots. */
	MarkDeleted(&vs);

	result = vs.stats;
	FreeVacuumState(&vs);
	return result;
}

/*
 * Clean up after a VACUUM operation.
 */
IndexBulkDeleteResult *
tqhnswvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	rel = info->index;

	if (info->analyze_only)
		return stats;

	/* stats is NULL if ambulkdelete was not called. */
	if (stats == NULL)
		return NULL;

	stats->num_pages = RelationGetNumberOfBlocks(rel);
	return stats;
}
