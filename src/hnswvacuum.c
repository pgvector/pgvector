#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "hnsw.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"

/*
 * Check if deleted list contains an index TID
 */
static bool
DeletedContains(tidhash_hash * deleted, ItemPointer indextid)
{
	return tidhash_lookup(deleted, *indextid) != NULL;
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
					/* Mark rest as invalid */
					for (int i = idx; i < HNSW_HEAPTIDS; i++)
						ItemPointerSetInvalid(&etup->heaptids[i]);

					updated = true;
				}
			}

			if (!ItemPointerIsValid(&etup->heaptids[0]))
			{
				ItemPointerData ip;
				bool		found;

				/* Add to deleted list */
				ItemPointerSet(&ip, blkno, offno);

				tidhash_insert(vacuumstate->deleted, ip, &found);
				Assert(!found);
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
			GenericXLogFinish(state);
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
	HnswSupport *support = &vacuumstate->support;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	int			m = vacuumstate->m;
	int			efConstruction = vacuumstate->efConstruction;
	BufferAccessStrategy bas = vacuumstate->bas;
	HnswNeighborTuple ntup = vacuumstate->ntup;
	Size		ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(element->level, m);
	char	   *base = NULL;

	/* Skip if element is entry point */
	if (entryPoint != NULL && element->blkno == entryPoint->blkno && element->offno == entryPoint->offno)
		return;

	/* Init fields */
	HnswInitNeighbors(base, element, m, NULL);
	element->heaptidsLength = 0;

	/* Find neighbors for element, skipping itself */
	HnswFindElementNeighbors(base, element, entryPoint, index, support, m, efConstruction, true);

	/* Zero memory for each element */
	MemSet(ntup, 0, HNSW_TUPLE_ALLOC_SIZE);

	/* Update neighbor tuple */
	/* Do this before getting page to minimize locking */
	HnswSetNeighborTuple(base, ntup, element, m);

	/* Get neighbor page */
	buf = ReadBufferExtended(index, MAIN_FORKNUM, element->neighborPage, RBM_NORMAL, bas);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	/* Overwrite tuple */
	if (!PageIndexTupleOverwrite(page, element->neighborOffno, (Item) ntup, ntupSize))
		elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

	/* Commit */
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	/* Update neighbors */
	HnswUpdateNeighborsOnDisk(index, support, element, m, true, false);
}

/*
 * Repair graph entry point
 */
static void
RepairGraphEntryPoint(HnswVacuumState * vacuumstate)
{
	Relation	index = vacuumstate->index;
	HnswSupport *support = &vacuumstate->support;
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
		HnswLoadElement(highestPoint, NULL, NULL, index, support, true, NULL);

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
			HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_ALWAYS, highestPoint, InvalidBlockNumber, MAIN_FORKNUM, false);
		}
		else
		{
			/*
			 * Repair the entry point with the highest point. If highest point
			 * is outdated, this can remove connections at higher levels in
			 * the graph until they are repaired, but this should be fine.
			 */
			HnswLoadElement(entryPoint, NULL, NULL, index, support, true, NULL);

			if (NeedsUpdated(vacuumstate, entryPoint))
			{
				/* Reset neighbors from previous update */
				if (highestPoint != NULL)
					HnswPtrStore((char *) NULL, highestPoint->neighbors, (HnswNeighborArrayPtr *) NULL);

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

	/*
	 * Wait for inserts to complete. Inserts before this point may have
	 * neighbors about to be deleted. Inserts after this point will not.
	 */
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
				HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_GREATER, element, InvalidBlockNumber, MAIN_FORKNUM, false);

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

	/*
	 * Wait for index scans to complete. Scans before this point may contain
	 * tuples about to be deleted. Scans after this point will not, since the
	 * graph has been repaired.
	 */
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
			MemSet(&etup->data, 0, VARSIZE_ANY(&etup->data));

			/* Overwrite neighbors */
			for (int i = 0; i < ntup->count; i++)
				ItemPointerSetInvalid(&ntup->indextids[i]);

			/* Increment version */
			/* This is used to avoid incorrect reads for iterative scans */
			/* Reserve some bits for future use */
			etup->version++;
			if (etup->version > 15)
				etup->version = 1;
			ntup->version = etup->version;

			/*
			 * We modified the tuples in place, no need to call
			 * PageIndexTupleOverwrite
			 */

			/* Commit */
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
	HnswUpdateMetaPage(index, 0, NULL, insertPage, MAIN_FORKNUM, false);
}

/*
 * Initialize the vacuum state
 */
static void
InitVacuumState(HnswVacuumState * vacuumstate, IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	vacuumstate->index = index;
	vacuumstate->stats = stats;
	vacuumstate->callback = callback;
	vacuumstate->callback_state = callback_state;
	vacuumstate->efConstruction = HnswGetEfConstruction(index);
	vacuumstate->bas = GetAccessStrategy(BAS_BULKREAD);
	vacuumstate->ntup = palloc0(HNSW_TUPLE_ALLOC_SIZE);
	vacuumstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
												"Hnsw vacuum temporary context",
												ALLOCSET_DEFAULT_SIZES);

	HnswInitSupport(&vacuumstate->support, index);

	/* Get m from metapage */
	HnswGetMetaPageInfo(index, &vacuumstate->m, NULL);

	/* Create hash table */
	vacuumstate->deleted = tidhash_create(CurrentMemoryContext, 256, NULL);
}

/*
 * Free resources
 */
static void
FreeVacuumState(HnswVacuumState * vacuumstate)
{
	tidhash_destroy(vacuumstate->deleted);
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
