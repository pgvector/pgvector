#include "postgres.h"

#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "tq.h"

#if PG_VERSION_NUM >= 180000
#define vacuum_delay_point() vacuum_delay_point(false)
#endif

/*
 * tqbulkdelete -- tombstone-based bulk delete for a tqflat index.
 *
 * Design notes:
 *
 * nVectors semantics: nVectors in the meta page is the PHYSICAL count of
 * entries ever written (live + tombstoned).  We do NOT decrement it on
 * delete.  The scan already skips entries with deleted != 0, so correctness
 * does not require nVectors to track live count.  Keeping nVectors as a
 * physical count avoids a meta-page write per vacuum and keeps the insert
 * tail-walk and tqflat_test_meta semantics simple and stable.
 *
 * num_index_tuples in stats reflects live (non-deleted) entries at the end of
 * the bulk-delete pass, computed by counting non-deleted entries as we scan.
 *
 * Space reclamation is tombstone-only.  Deleted entries remain on their
 * pages; space is NOT reclaimed until a full REINDEX or a future compaction
 * pass.  VACUUM FULL rebuilds the index from scratch via ambuild, which
 * does reclaim space.
 */
IndexBulkDeleteResult *
tqbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			 IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;
	BlockNumber blkno;
	BlockNumber sideStart;
	BlockNumber tailStart;
	BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/* Recount from scratch: a caller may pass back a prior call's stats. */
	stats->num_index_tuples = 0;

	/* Read the side-chain and tail-chain heads from the meta page. */
	{
		Buffer		metabuf;
		Page		metapage;
		TqMetaPage	metap;

		metabuf = ReadBuffer(index, TQ_METAPAGE_BLKNO);
		LockBuffer(metabuf, BUFFER_LOCK_SHARE);
		metapage = BufferGetPage(metabuf);
		metap = TqPageGetMeta(metapage);
		sideStart = metap->sideStart;
		tailStart = metap->tailStart;
		UnlockReleaseBuffer(metabuf);
	}

	/*
	 * --- Side chain: tombstone lanes via deletedMask ---
	 *
	 * The built rows live in the blocked code-plane; their heap TIDs and
	 * tombstone bits are carried in the parallel side chain (one
	 * TqBlockSideRec per block).  We set deletedMask bit j for any lane whose
	 * heap TID the callback wants removed; the scan skips those lanes.
	 */
	blkno = sideStart;
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
		 * ambulkdelete cannot delete entries from pages that are pinned by
		 * other backends.  LockBufferForCleanup waits until no other backend
		 * holds a pin on this buffer (mirrors ivfvacuum.c).
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
				/* Skip already-tombstoned lanes (avoids double-counting). */
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
	 * Rows inserted after the build live in the row-major tail chain as
	 * TqEntry items; tombstone them via entry->deleted (the scan skips
	 * those).
	 */
	blkno = tailStart;
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
		 * ambulkdelete cannot delete entries from pages that are pinned by
		 * other backends.  LockBufferForCleanup waits until no other backend
		 * holds a pin on this buffer (mirrors ivfvacuum.c).
		 */
		LockBufferForCleanup(buf);

		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		maxoffno = PageGetMaxOffsetNumber(page);

		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			TqEntry    *entry = (TqEntry *) PageGetItem(page, PageGetItemId(page, offno));

			/* Skip already-tombstoned entries (avoids double-counting). */
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

	FreeAccessStrategy(bas);

	return stats;
}

/*
 * tqvacuumcleanup -- post-VACUUM cleanup for a tqflat index.
 *
 * On the analyze-only path we return stats unchanged.  Otherwise, if
 * ambulkdelete was not called stats is NULL and we return NULL to indicate no
 * changes (mirrors ivfflatvacuumcleanup).
 *
 * When stats is provided, fill in num_pages and leave num_index_tuples as set
 * by tqbulkdelete.  We do not re-scan the index here; the live count from the
 * bulk-delete pass is sufficient.
 *
 * No physical compaction is performed in v1 (tombstone-only).  Space occupied
 * by deleted entries is not reclaimed; a REINDEX or VACUUM FULL is needed to
 * recover that space.
 */
IndexBulkDeleteResult *
tqvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
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
