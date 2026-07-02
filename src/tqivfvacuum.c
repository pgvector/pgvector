#include "postgres.h"

#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "tqivf.h"

#if PG_VERSION_NUM >= 180000
#define vacuum_delay_point() vacuum_delay_point(false)
#endif

/*
 * tqivfbulkdelete -- tombstone-based bulk delete for a tqivf index.
 *
 * Design notes (mirrors tqvacuum.c):
 *
 * Outer loop: walk the list-directory chain (from listStart), collecting each
 * list's sideStart and tailStart.  Release the directory page before processing
 * each list's chains (mirrors ivfvacuum's pattern of collecting startPages first
 * then processing).
 *
 * Per-list side chain: for each TqBlockSideRec item, check each lane j in
 * [0, nvecs).  If the lane is not already tombstoned and the callback says it is
 * dead, set deletedMask |= (1u << j) and mark the page dirty.  Write modified
 * pages via GenericXLog (in-place item update, exactly as tqvacuum does).
 *
 * Per-list tail chain: for each TqEntry item, if !entry->deleted and the
 * callback says it is dead, set entry->deleted = 1 and write via GenericXLog.
 *
 * Tombstone only: no page compaction, no chain/count modification.
 * VACUUM FULL rebuilds via tqivfbuild.
 */
IndexBulkDeleteResult *
tqivfbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;
	BlockNumber listBlkno;
	BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/* Recount from scratch: a caller may pass back a prior call's stats. */
	stats->num_index_tuples = 0;

	/* Read listStart from the meta page. */
	TqivfGetMetaInfo(index, NULL, NULL, NULL, &listBlkno);

	/*
	 * Walk the list-directory chain.  Mirror ivfvacuum: read each directory
	 * page under a share lock, collect all list chain heads on that page,
	 * then release the directory page before processing the per-list chains
	 * to avoid holding the directory lock while doing potentially-blocking
	 * cleanup work.
	 */
	while (BlockNumberIsValid(listBlkno))
	{
		Buffer		cbuf;
		Page		cpage;
		OffsetNumber cmaxoffno;
		OffsetNumber coffno;

		/*
		 * Collect up to MaxOffsetNumber list chain head pairs from this
		 * directory page.
		 */
		BlockNumber sideStarts[MaxOffsetNumber];
		BlockNumber tailStarts[MaxOffsetNumber];
		int			nLists = 0;
		BlockNumber nextListBlkno;

		cbuf = ReadBuffer(index, listBlkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);
		cmaxoffno = PageGetMaxOffsetNumber(cpage);

		for (coffno = FirstOffsetNumber; coffno <= cmaxoffno; coffno = OffsetNumberNext(coffno))
		{
			TqivfList	list = (TqivfList) PageGetItem(cpage, PageGetItemId(cpage, coffno));

			sideStarts[nLists] = list->sideStart;
			tailStarts[nLists] = list->tailStart;
			nLists++;
		}

		nextListBlkno = TqPageGetOpaque(cpage)->nextblkno;
		UnlockReleaseBuffer(cbuf);

		/* Now process each list's side chain then tail chain. */
		for (int li = 0; li < nLists; li++)
		{
			BlockNumber blkno;

			/*
			 * --- Side chain: tombstone lanes via deletedMask ---
			 */
			blkno = sideStarts[li];
			while (BlockNumberIsValid(blkno))
			{
				Buffer		buf;
				Page		page;
				GenericXLogState *state;
				OffsetNumber offno;
				OffsetNumber maxoffno;
				bool		modified = false;
				BlockNumber nextblkno;

				vacuum_delay_point();

				buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);

				/*
				 * ambulkdelete cannot delete entries from pages that are
				 * pinned by other backends.  LockBufferForCleanup waits until
				 * no other backend holds a pin on this buffer (mirrors
				 * ivfvacuum.c).
				 */
				LockBufferForCleanup(buf);

				state = GenericXLogStart(index);
				page = GenericXLogRegisterBuffer(state, buf, 0);

				maxoffno = PageGetMaxOffsetNumber(page);

				for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
				{
					TqBlockSideRec *srec = (TqBlockSideRec *) PageGetItem(page, PageGetItemId(page, offno));
					int			j;

					for (j = 0; j < srec->nvecs; j++)
					{
						/*
						 * Skip already-tombstoned lanes (avoids
						 * double-counting).
						 */
						if (srec->deletedMask & (1u << j))
							continue;

						if (callback(&srec->side[j].heaptid, callback_state))
						{
							srec->deletedMask |= (1u << j);
							modified = true;
							stats->tuples_removed++;
						}
						else
							stats->num_index_tuples++;
					}
				}

				/* Capture nextblkno before we finish/abort the xlog state. */
				nextblkno = TqPageGetOpaque(page)->nextblkno;

				if (modified)
					GenericXLogFinish(state);
				else
					GenericXLogAbort(state);

				UnlockReleaseBuffer(buf);

				blkno = nextblkno;
			}

			/*
			 * --- Tail chain: tombstone row-major TqEntry items ---
			 *
			 * Rows inserted after the build live in the row-major tail chain
			 * as TqEntry items; tombstone them via entry->deleted (the scan
			 * skips those).  tailStart is Invalid until the first post-build
			 * insert, so this loop is a no-op for lists with no tail inserts.
			 */
			blkno = tailStarts[li];
			while (BlockNumberIsValid(blkno))
			{
				Buffer		buf;
				Page		page;
				GenericXLogState *state;
				OffsetNumber offno;
				OffsetNumber maxoffno;
				bool		modified = false;
				BlockNumber nextblkno;

				vacuum_delay_point();

				buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);

				/*
				 * ambulkdelete cannot delete entries from pages that are
				 * pinned by other backends.
				 */
				LockBufferForCleanup(buf);

				state = GenericXLogStart(index);
				page = GenericXLogRegisterBuffer(state, buf, 0);

				maxoffno = PageGetMaxOffsetNumber(page);

				for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
				{
					TqEntry    *entry = (TqEntry *) PageGetItem(page, PageGetItemId(page, offno));

					/*
					 * Skip already-tombstoned entries (avoids
					 * double-counting).
					 */
					if (entry->deleted)
						continue;

					if (callback(&entry->heaptid, callback_state))
					{
						entry->deleted = 1;
						modified = true;
						stats->tuples_removed++;
					}
					else
						stats->num_index_tuples++;
				}

				/* Capture nextblkno before we finish/abort the xlog state. */
				nextblkno = TqPageGetOpaque(page)->nextblkno;

				if (modified)
					GenericXLogFinish(state);
				else
					GenericXLogAbort(state);

				UnlockReleaseBuffer(buf);

				blkno = nextblkno;
			}
		}

		listBlkno = nextListBlkno;
	}

	FreeAccessStrategy(bas);

	return stats;
}

/*
 * tqivfvacuumcleanup -- post-VACUUM cleanup for a tqivf index.
 *
 * Mirrors tqvacuumcleanup / ivfflatvacuumcleanup: if ambulkdelete was not
 * called (analyze-only path), stats is NULL and we return NULL.  Otherwise
 * fill in num_pages.
 *
 * No physical compaction in v1 (tombstone-only).  VACUUM FULL rebuilds via
 * tqivfbuild.
 */
IndexBulkDeleteResult *
tqivfvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	rel = info->index;

	if (info->analyze_only)
		return stats;

	/* stats is NULL if ambulkdelete was not called; OK to return NULL. */
	if (stats == NULL)
		return NULL;

	stats->num_pages = RelationGetNumberOfBlocks(rel);

	return stats;
}
