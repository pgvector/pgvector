#include "postgres.h"

#include "commands/vacuum.h"
#include "hnsw.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

/*
 * Check if deleted list contains an index tid
 */
static bool
DeletedContains(HTAB *deleted, ItemPointer indextid)
{
	bool		found;

	hash_search(deleted, indextid, HASH_FIND, &found);
	return found;
}

/*
 * Remove deleted heap tids
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
	HnswElement entryPoint = GetEntryPoint(vacuumstate->index);

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
			HnswElementTuple item = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, offno));
			int			idx = 0;
			bool		itemUpdated = false;

			if (ItemPointerIsValid(&item->heaptids[0]))
			{
				for (int i = 0; i < HNSW_HEAPTIDS; i++)
				{
					/* Stop at first unused */
					if (!ItemPointerIsValid(&item->heaptids[i]))
						break;

					if (vacuumstate->callback(&item->heaptids[i], vacuumstate->callback_state))
						itemUpdated = true;
					else
					{
						/* Move to front of list */
						item->heaptids[idx++] = item->heaptids[i];
					}
				}

				if (itemUpdated)
				{
					Size		itemsz = MAXALIGN(HNSW_ELEMENT_TUPLE_SIZE(item->vec.dim));

					/* Mark rest as invalid */
					for (int i = idx; i < HNSW_HEAPTIDS; i++)
						ItemPointerSetInvalid(&item->heaptids[i]);

					if (!PageIndexTupleOverwrite(page, offno, (Item) item, itemsz))
						elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

					updated = true;
				}
			}

			if (!ItemPointerIsValid(&item->heaptids[0]))
			{
				ItemPointerData ip;

				/* Add to deleted list */
				ItemPointerSet(&ip, blkno, offno);

				(void) hash_search(vacuumstate->deleted, &ip, HASH_ENTER, NULL);
			}
			else if (item->level > highestLevel && !(highestPoint->blkno == entryPoint->blkno && highestPoint->offno == entryPoint->offno))
			{
				/* Keep track of highest non-entry point */
				/* TODO Keep track of closest one to entry point? */
				highestPoint->blkno = blkno;
				highestPoint->offno = offno;
				highestLevel = item->level;
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
	OffsetNumber offno;
	OffsetNumber maxoffno;
	bool		needsUpdated = false;

	buf = ReadBufferExtended(index, MAIN_FORKNUM, element->neighborPage, RBM_NORMAL, bas);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	maxoffno = PageGetMaxOffsetNumber(page);

	/* Check neighbors */
	for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
	{
		HnswNeighborTuple ntup = (HnswNeighborTuple) PageGetItem(page, PageGetItemId(page, offno));

		if (!ItemPointerIsValid(&ntup->indextid))
			continue;

		/* Check if in deleted list */
		if (DeletedContains(vacuumstate->deleted, &ntup->indextid))
		{
			needsUpdated = true;
			break;
		}
	}

	UnlockReleaseBuffer(buf);

	return needsUpdated;
}

/*
 * Repair graph for a single element
 */
static void
RepairGraphElement(HnswVacuumState * vacuumstate, HnswElement element)
{
	Relation	index = vacuumstate->index;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	int			m = vacuumstate->m;
	int			efConstruction = vacuumstate->efConstruction;
	FmgrInfo   *procinfo = vacuumstate->procinfo;
	Oid			collation = vacuumstate->collation;
	HnswElement entryPoint;
	BufferAccessStrategy bas = vacuumstate->bas;
	HnswNeighborTuple ntup = vacuumstate->ntup;
	Size		nsize = vacuumstate->nsize;

	/* Check if any neighbors point to deleted values */
	if (!NeedsUpdated(vacuumstate, element))
		return;

	/* Refresh entry point for each element */
	entryPoint = GetEntryPoint(index);

	/* Special case for entry point */
	if (element->blkno == entryPoint->blkno && element->offno == entryPoint->offno)
	{
		if (BlockNumberIsValid(vacuumstate->highestPoint.blkno))
		{
			/* Already updated */
			if (vacuumstate->highestPoint.blkno == element->blkno && vacuumstate->highestPoint.offno == element->offno)
				return;

			entryPoint = &vacuumstate->highestPoint;
		}
		else
			entryPoint = NULL;
	}

	HnswInitNeighbors(element, m);
	element->heaptids = NIL;

	HnswInsertElement(element, entryPoint, index, procinfo, collation, m, efConstruction, NULL, true);

	/* Write out new neighbors on page */
	buf = ReadBufferExtended(index, MAIN_FORKNUM, element->neighborPage, RBM_NORMAL, bas);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

	/* Overwrites existing page via InitPage */
	HnswInitPage(buf, page);

	/* Update neighbors */
	AddNeighborsToPage(index, page, element, ntup, nsize, m);

	/* Commit */
	MarkBufferDirty(buf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
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

	/* Repair graph for highest non-entry point */
	/* This may not be the highest with new inserts, but should be fine */
	if (BlockNumberIsValid(highestPoint->blkno))
	{
		HnswLoadElement(highestPoint, NULL, NULL, index, vacuumstate->procinfo, vacuumstate->collation, true);
		RepairGraphElement(vacuumstate, highestPoint);
	}

	entryPoint = GetEntryPoint(index);
	if (entryPoint != NULL)
	{
		ItemPointerData epData;

		ItemPointerSet(&epData, entryPoint->blkno, entryPoint->offno);

		if (DeletedContains(vacuumstate->deleted, &epData))
			UpdateMetaPage(index, true, highestPoint, InvalidBlockNumber, MAIN_FORKNUM);
		else
		{
			/* Highest point will be used to repair */
			HnswLoadElement(entryPoint, NULL, NULL, index, vacuumstate->procinfo, vacuumstate->collation, true);
			RepairGraphElement(vacuumstate, entryPoint);
		}
	}

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
			HnswElementTuple item = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, offno));
			HnswElement element;

			/* Skip updating neighbors if being deleted */
			if (!ItemPointerIsValid(&item->heaptids[0]))
				continue;

			/* Create an element */
			element = palloc(sizeof(HnswElementData));
			element->neighborPage = item->neighborPage;
			element->level = item->level;
			element->blkno = blkno;
			element->offno = offno;
			element->vec = palloc(VECTOR_SIZE(item->vec.dim));
			memcpy(element->vec, &item->vec, VECTOR_SIZE(item->vec.dim));

			elements = lappend(elements, element);
		}

		blkno = HnswPageGetOpaque(page)->nextblkno;

		UnlockReleaseBuffer(buf);

		/* Update neighbor pages */
		foreach(lc2, elements)
			RepairGraphElement(vacuumstate, (HnswElement) lfirst(lc2));

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
			HnswElementTuple item = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, offno));
			Size		itemsz;
			Buffer		nbuf;
			Page		npage;

			if (ItemPointerIsValid(&item->heaptids[0]))
				continue;

			/* Overwrite element */
			/* TODO Increment version? */
			item->deleted = 1;
			MemSet(&item->vec.x, 0, item->vec.dim * sizeof(float));

			itemsz = MAXALIGN(HNSW_ELEMENT_TUPLE_SIZE(item->vec.dim));
			if (!PageIndexTupleOverwrite(page, offno, (Item) item, itemsz))
				elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

			/* Overwrite neighbors */
			nbuf = ReadBufferExtended(index, MAIN_FORKNUM, item->neighborPage, RBM_NORMAL, bas);
			LockBuffer(nbuf, BUFFER_LOCK_EXCLUSIVE);
			npage = GenericXLogRegisterBuffer(state, nbuf, GENERIC_XLOG_FULL_IMAGE);
			HnswInitPage(nbuf, npage);

			/* Commit */
			MarkBufferDirty(buf);
			MarkBufferDirty(nbuf);
			GenericXLogFinish(state);
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

	UpdateMetaPage(index, false, NULL, insertPage, MAIN_FORKNUM);
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
	vacuumstate->nsize = MAXALIGN(sizeof(HnswNeighborTupleData));
	vacuumstate->ntup = palloc0(vacuumstate->nsize);
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

	/* Pass 1: Remove heap tids */
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
