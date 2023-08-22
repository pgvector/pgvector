#include "postgres.h"

#include <math.h>

#include "commands/vacuum.h"
#include "hnsw.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"

/*
 * Check if deleted list contains an index TID
 */
static bool
DeletedContains(HTAB *deleted, ItemPointer indextid)
{
	bool		found;

	hash_search(deleted, indextid, HASH_FIND, &found);
	return found;
}

/*
 * Remove deleted heap TIDs
 *
 * OK to remove for entry point, since always considered for searches and inserts
 */
static void
RemoveHeapTids(HnswVacuumState * vacuumstate)
{
	BlockNumber blkno = HNSW_HEAD_BLKNO;
	HnswElement highestPoint = &vacuumstate->highestPoint;
	Relation	index = vacuumstate->index;
	BufferAccessStrategy bas = vacuumstate->bas;
	HnswElement entryPoint = HnswGetEntryPoint(vacuumstate->index);
	IndexBulkDeleteResult *stats = vacuumstate->stats;

	/* Store separately since highestPoint.level is uint8 */
	int			highestLevel = -1;

	/* Initialize highest point */
	highestPoint->blkno = InvalidBlockNumber;
	highestPoint->offno = InvalidOffsetNumber;

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

		/* Iterate over nodes */
		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			HnswElementTuple etup = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, offno));
			int			idx = 0;
			bool		itemUpdated = false;

			/* Skip neighbor tuples */
			if (!HnswIsElementTuple(etup))
				continue;

			if (ItemPointerIsValid(&etup->heaptids[0]))
			{
				for (int i = 0; i < HNSW_HEAPTIDS; i++)
				{
					/* Stop at first unused */
					if (!ItemPointerIsValid(&etup->heaptids[i]))
						break;

					if (vacuumstate->callback(&etup->heaptids[i], vacuumstate->callback_state))
					{
						itemUpdated = true;
						stats->tuples_removed++;
					}
					else
					{
						/* Move to front of list */
						etup->heaptids[idx++] = etup->heaptids[i];
						stats->num_index_tuples++;
					}
				}

				if (itemUpdated)
				{
					Size		etupSize = HNSW_ELEMENT_TUPLE_SIZE(etup->vec.dim);

					/* Mark rest as invalid */
					for (int i = idx; i < HNSW_HEAPTIDS; i++)
						ItemPointerSetInvalid(&etup->heaptids[i]);

					if (!PageIndexTupleOverwrite(page, offno, (Item) etup, etupSize))
						elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

					updated = true;
				}
			}

			if (!ItemPointerIsValid(&etup->heaptids[0]))
			{
				ItemPointerData ip;

				/* Add to deleted list */
				ItemPointerSet(&ip, blkno, offno);

				(void) hash_search(vacuumstate->deleted, &ip, HASH_ENTER, NULL);
			}
			else if (etup->level > highestLevel && !(entryPoint != NULL && blkno == entryPoint->blkno && offno == entryPoint->offno))
			{
				/* Keep track of highest non-entry point */
				highestPoint->blkno = blkno;
				highestPoint->offno = offno;
				highestPoint->level = etup->level;
				highestLevel = etup->level;
			}
		}

		blkno = HnswPageGetOpaque(page)->nextblkno;

		if (updated)
		{
			MarkBufferDirty(buf);
			GenericXLogFinish(state);
		}
		else
			GenericXLogAbort(state);

		UnlockReleaseBuffer(buf);
	}
}

/*
 * Check for deleted neighbors
 */
static bool
NeedsUpdated(HnswVacuumState * vacuumstate, HnswElement element)
{
	Relation	index = vacuumstate->index;
	BufferAccessStrategy bas = vacuumstate->bas;
	Buffer		buf;
	Page		page;
	HnswNeighborTuple ntup;
	bool		needsUpdated = false;

	buf = ReadBufferExtended(index, MAIN_FORKNUM, element->neighborPage, RBM_NORMAL, bas);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	ntup = (HnswNeighborTuple) PageGetItem(page, PageGetItemId(page, element->neighborOffno));

	Assert(HnswIsNeighborTuple(ntup));

	/* Check neighbors */
	for (int i = 0; i < ntup->count; i++)
	{
		ItemPointer indextid = &ntup->indextids[i];

		if (!ItemPointerIsValid(indextid))
			continue;

		/* Check if in deleted list */
		if (DeletedContains(vacuumstate->deleted, indextid))
		{
			needsUpdated = true;
			break;
		}
	}

	/* Also update if layer 0 is not full */
	/* This could indicate too many candidates being deleted during insert */
	if (!needsUpdated)
		needsUpdated = !ItemPointerIsValid(&ntup->indextids[ntup->count - 1]);

	UnlockReleaseBuffer(buf);

	return needsUpdated;
}

/*
 * Repair graph for a single element
 */
static void
RepairGraphElement(HnswVacuumState * vacuumstate, HnswElement element, HnswElement entryPoint)
{
	Relation	index = vacuumstate->index;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	int			m = vacuumstate->m;
	int			efConstruction = vacuumstate->efConstruction;
	FmgrInfo   *procinfo = vacuumstate->procinfo;
	Oid			collation = vacuumstate->collation;
	BufferAccessStrategy bas = vacuumstate->bas;
	HnswNeighborTuple ntup = vacuumstate->ntup;
	Size		ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(element->level, m);

	/* Skip if element is entry point */
	if (entryPoint != NULL && element->blkno == entryPoint->blkno && element->offno == entryPoint->offno)
		return;

	/* Init fields */
	HnswInitNeighbors(element, m);
	element->heaptids = NIL;

	/* Add element to graph, skipping itself */
	HnswInsertElement(element, entryPoint, index, procinfo, collation, m, efConstruction, true);

	/* Update neighbor tuple */
	/* Do this before getting page to minimize locking */
	HnswSetNeighborTuple(ntup, element, m);

	/* Get neighbor page */
	buf = ReadBufferExtended(index, MAIN_FORKNUM, element->neighborPage, RBM_NORMAL, bas);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	/* Overwrite tuple */
	if (!PageIndexTupleOverwrite(page, element->neighborOffno, (Item) ntup, ntupSize))
		elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

	/* Commit */
	MarkBufferDirty(buf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	/* Update neighbors */
	HnswUpdateNeighborPages(index, procinfo, collation, element, m, true);
}

/*
 * Repair graph entry point
 */
static void
RepairGraphEntryPoint(HnswVacuumState * vacuumstate)
{
	Relation	index = vacuumstate->index;
	HnswElement highestPoint = &vacuumstate->highestPoint;
	HnswElement entryPoint;
	MemoryContext oldCtx = MemoryContextSwitchTo(vacuumstate->tmpCtx);

	if (!BlockNumberIsValid(highestPoint->blkno))
		highestPoint = NULL;

	/*
	 * Repair graph for highest non-entry point. Highest point may be outdated
	 * due to inserts that happen during and after RemoveHeapTids.
	 */
	if (highestPoint != NULL)
	{
		/* Get a shared lock */
		LockPage(index, HNSW_UPDATE_LOCK, ShareLock);

		/* Load element */
		HnswLoadElement(highestPoint, NULL, NULL, index, vacuumstate->procinfo, vacuumstate->collation, true);

		/* Repair if needed */
		if (NeedsUpdated(vacuumstate, highestPoint))
			RepairGraphElement(vacuumstate, highestPoint, HnswGetEntryPoint(index));

		/* Release lock */
		UnlockPage(index, HNSW_UPDATE_LOCK, ShareLock);
	}

	/* Prevent concurrent inserts when possibly updating entry point */
	LockPage(index, HNSW_UPDATE_LOCK, ExclusiveLock);

	/* Get latest entry point */
	entryPoint = HnswGetEntryPoint(index);

	if (entryPoint != NULL)
	{
		ItemPointerData epData;

		ItemPointerSet(&epData, entryPoint->blkno, entryPoint->offno);

		if (DeletedContains(vacuumstate->deleted, &epData))
		{
			/*
			 * Replace the entry point with the highest point. If highest
			 * point is outdated and empty, the entry point will be empty
			 * until an element is repaired.
			 */
			HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_ALWAYS, highestPoint, InvalidBlockNumber, MAIN_FORKNUM);
		}
		else
		{
			/*
			 * Repair the entry point with the highest point. If highest point
			 * is outdated, this can remove connections at higher levels in
			 * the graph until they are repaired, but this should be fine.
			 */
			HnswLoadElement(entryPoint, NULL, NULL, index, vacuumstate->procinfo, vacuumstate->collation, true);

			if (NeedsUpdated(vacuumstate, entryPoint))
			{
				/* Reset neighbors from previous update */
				if (highestPoint != NULL)
					highestPoint->neighbors = NULL;

				RepairGraphElement(vacuumstate, entryPoint, highestPoint);
			}
		}
	}

	/* Release lock */
	UnlockPage(index, HNSW_UPDATE_LOCK, ExclusiveLock);

	/* Reset memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(vacuumstate->tmpCtx);
}

/*
 * Repair graph for all elements
 */
static void
RepairGraph(HnswVacuumState * vacuumstate)
{
	Relation	index = vacuumstate->index;
	BufferAccessStrategy bas = vacuumstate->bas;
	BlockNumber blkno = HNSW_HEAD_BLKNO;

	/* Wait for inserts to complete */
	LockPage(index, HNSW_UPDATE_LOCK, ExclusiveLock);
	UnlockPage(index, HNSW_UPDATE_LOCK, ExclusiveLock);

	/* Repair entry point first */
	RepairGraphEntryPoint(vacuumstate);

	while (BlockNumberIsValid(blkno))
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offno;
		OffsetNumber maxoffno;
		List	   *elements = NIL;
		ListCell   *lc2;
		MemoryContext oldCtx;

		vacuum_delay_point();

		oldCtx = MemoryContextSwitchTo(vacuumstate->tmpCtx);

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		maxoffno = PageGetMaxOffsetNumber(page);

		/* Load items into memory to minimize locking */
		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			HnswElementTuple etup = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, offno));
			HnswElement element;

			/* Skip neighbor tuples */
			if (!HnswIsElementTuple(etup))
				continue;

			/* Skip updating neighbors if being deleted */
			if (!ItemPointerIsValid(&etup->heaptids[0]))
				continue;

			/* Create an element */
			element = HnswInitElementFromBlock(blkno, offno);
			HnswLoadElementFromTuple(element, etup, false, true);

			elements = lappend(elements, element);
		}

		blkno = HnswPageGetOpaque(page)->nextblkno;

		UnlockReleaseBuffer(buf);

		/* Update neighbor pages */
		foreach(lc2, elements)
		{
			HnswElement element = (HnswElement) lfirst(lc2);
			HnswElement entryPoint;
			LOCKMODE	lockmode = ShareLock;

			/* Check if any neighbors point to deleted values */
			if (!NeedsUpdated(vacuumstate, element))
				continue;

			/* Get a shared lock */
			LockPage(index, HNSW_UPDATE_LOCK, lockmode);

			/* Refresh entry point for each element */
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

			/* Repair connections */
			RepairGraphElement(vacuumstate, element, entryPoint);

			/*
			 * Update metapage if needed. Should only happen if entry point
			 * was replaced and highest point was outdated.
			 */
			if (entryPoint == NULL || element->level > entryPoint->level)
				HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_GREATER, element, InvalidBlockNumber, MAIN_FORKNUM);

			/* Release lock */
			UnlockPage(index, HNSW_UPDATE_LOCK, lockmode);
		}

		/* Reset memory context */
		MemoryContextSwitchTo(oldCtx);
		MemoryContextReset(vacuumstate->tmpCtx);
	}
}

/*
 * Mark items as deleted
 */
static void
MarkDeleted(HnswVacuumState * vacuumstate)
{
	BlockNumber blkno = HNSW_HEAD_BLKNO;
	BlockNumber insertPage = InvalidBlockNumber;
	Relation	index = vacuumstate->index;
	BufferAccessStrategy bas = vacuumstate->bas;

	/* Wait for selects to complete */
	LockPage(index, HNSW_SCAN_LOCK, ExclusiveLock);
	UnlockPage(index, HNSW_SCAN_LOCK, ExclusiveLock);

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
		 * ambulkdelete cannot delete entries from pages that are pinned by
		 * other backends
		 *
		 * https://www.postgresql.org/docs/current/index-locking.html
		 */
		LockBufferForCleanup(buf);

		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);
		maxoffno = PageGetMaxOffsetNumber(page);

		/* Update element and neighbors together */
		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			HnswElementTuple etup = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, offno));
			HnswNeighborTuple ntup;
			Size		etupSize;
			Size		ntupSize;
			Buffer		nbuf;
			Page		npage;
			BlockNumber neighborPage;
			OffsetNumber neighborOffno;

			/* Skip neighbor tuples */
			if (!HnswIsElementTuple(etup))
				continue;

			/* Skip deleted tuples */
			if (etup->deleted)
			{
				/* Set to first free page */
				if (!BlockNumberIsValid(insertPage))
					insertPage = blkno;

				continue;
			}

			/* Skip live tuples */
			if (ItemPointerIsValid(&etup->heaptids[0]))
				continue;

			/* Calculate sizes */
			etupSize = HNSW_ELEMENT_TUPLE_SIZE(etup->vec.dim);
			ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(etup->level, vacuumstate->m);

			/* Get neighbor page */
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

			ntup = (HnswNeighborTuple) PageGetItem(npage, PageGetItemId(npage, neighborOffno));

			/* Overwrite element */
			etup->deleted = 1;
			MemSet(&etup->vec.x, 0, etup->vec.dim * sizeof(float));

			/* Overwrite neighbors */
			for (int i = 0; i < ntup->count; i++)
				ItemPointerSetInvalid(&ntup->indextids[i]);

			/* Overwrite element tuple */
			if (!PageIndexTupleOverwrite(page, offno, (Item) etup, etupSize))
				elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

			/* Overwrite neighbor tuple */
			if (!PageIndexTupleOverwrite(npage, neighborOffno, (Item) ntup, ntupSize))
				elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

			/* Commit */
			MarkBufferDirty(buf);
			if (nbuf != buf)
				MarkBufferDirty(nbuf);
			GenericXLogFinish(state);
			if (nbuf != buf)
				UnlockReleaseBuffer(nbuf);

			/* Set to first free page */
			if (!BlockNumberIsValid(insertPage))
				insertPage = blkno;

			/* Prepare new xlog */
			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buf, 0);
		}

		blkno = HnswPageGetOpaque(page)->nextblkno;

		GenericXLogAbort(state);
		UnlockReleaseBuffer(buf);
	}

	/* Update insert page last, after everything has been marked as deleted */
	HnswUpdateMetaPage(index, 0, NULL, insertPage, MAIN_FORKNUM);
}

/*
 * Initialize the vacuum state
 */
static void
InitVacuumState(HnswVacuumState * vacuumstate, IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;
	HASHCTL		hash_ctl;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	vacuumstate->index = index;
	vacuumstate->stats = stats;
	vacuumstate->callback = callback;
	vacuumstate->callback_state = callback_state;
	vacuumstate->m = HnswGetM(index);
	vacuumstate->efConstruction = HnswGetEfConstruction(index);
	vacuumstate->bas = GetAccessStrategy(BAS_BULKREAD);
	vacuumstate->procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
	vacuumstate->collation = index->rd_indcollation[0];
	vacuumstate->ntup = palloc0(BLCKSZ);
	vacuumstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
												"Hnsw vacuum temporary context",
												ALLOCSET_DEFAULT_SIZES);

	/* Create hash table */
	hash_ctl.keysize = sizeof(ItemPointerData);
	hash_ctl.entrysize = sizeof(ItemPointerData);
	hash_ctl.hcxt = CurrentMemoryContext;
	vacuumstate->deleted = hash_create("hnswbulkdelete indextids", 256, &hash_ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Free resources
 */
static void
FreeVacuumState(HnswVacuumState * vacuumstate)
{
	hash_destroy(vacuumstate->deleted);
	FreeAccessStrategy(vacuumstate->bas);
	pfree(vacuumstate->ntup);
	MemoryContextDelete(vacuumstate->tmpCtx);
}

/*
 * Bulk delete tuples from the index
 */
IndexBulkDeleteResult *
hnswbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state)
{
	HnswVacuumState vacuumstate;

	InitVacuumState(&vacuumstate, info, stats, callback, callback_state);

	/* Pass 1: Remove heap TIDs */
	RemoveHeapTids(&vacuumstate);

	/* Pass 2: Repair graph */
	RepairGraph(&vacuumstate);

	/* Pass 3: Mark as deleted */
	MarkDeleted(&vacuumstate);

	FreeVacuumState(&vacuumstate);

	return vacuumstate.stats;
}

/*
 * Clean up after a VACUUM operation
 */
IndexBulkDeleteResult *
hnswvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	rel = info->index;

	if (info->analyze_only)
		return stats;

	/* stats is NULL if ambulkdelete not called */
	/* OK to return NULL if index not changed */
	if (stats == NULL)
		return NULL;

	stats->num_pages = RelationGetNumberOfBlocks(rel);

	return stats;
}
