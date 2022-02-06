#include "postgres.h"

#include "commands/vacuum.h"
#include "ivfflat.h"
#include "storage/bufmgr.h"

/*
 * Bulk delete tuples from the index
 */
IndexBulkDeleteResult *
ivfflatbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				  IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;
	Buffer		cbuf;
	Page		cpage;
	Buffer		buf;
	Page		page;
	IvfflatList list;
	IndexTuple	itup;
	ItemPointer htup;
	OffsetNumber deletable[MaxOffsetNumber];
	int			ndeletable;
	BlockNumber startPages[MaxOffsetNumber];
	BlockNumber nextblkno = IVFFLAT_HEAD_BLKNO;
	BlockNumber searchPage;
	BlockNumber insertPage;
	GenericXLogState *state;
	OffsetNumber coffno;
	OffsetNumber cmaxoffno;
	OffsetNumber offno;
	OffsetNumber maxoffno;
	ListInfo	listInfo;
	BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/* Iterate over list pages */
	while (BlockNumberIsValid(nextblkno))
	{
		cbuf = ReadBuffer(index, nextblkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		cmaxoffno = PageGetMaxOffsetNumber(cpage);

		/* Iterate over lists */
		for (coffno = FirstOffsetNumber; coffno <= cmaxoffno; coffno = OffsetNumberNext(coffno))
		{
			list = (IvfflatList) PageGetItem(cpage, PageGetItemId(cpage, coffno));
			startPages[coffno - FirstOffsetNumber] = list->startPage;
		}

		listInfo.blkno = nextblkno;
		nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

		UnlockReleaseBuffer(cbuf);

		for (coffno = FirstOffsetNumber; coffno <= cmaxoffno; coffno = OffsetNumberNext(coffno))
		{
			searchPage = startPages[coffno - FirstOffsetNumber];
			insertPage = InvalidBlockNumber;

			/* Iterate over entry pages */
			while (BlockNumberIsValid(searchPage))
			{
				vacuum_delay_point();

				buf = ReadBufferExtended(index, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);

				/*
				 * ambulkdelete cannot delete entries from pages that are
				 * pinned by other backends
				 *
				 * https://www.postgresql.org/docs/current/index-locking.html
				 */
				LockBufferForCleanup(buf);

				state = GenericXLogStart(index);
				page = GenericXLogRegisterBuffer(state, buf, 0);

				maxoffno = PageGetMaxOffsetNumber(page);
				ndeletable = 0;

				/* Find deleted tuples */
				for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
				{
					itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offno));
					htup = &(itup->t_tid);

					if (callback(htup, callback_state))
					{
						deletable[ndeletable++] = offno;
						stats->tuples_removed++;
					}
					else
						stats->num_index_tuples++;
				}

				/* Set to first free page */
				/* Must be set before searchPage is updated */
				if (!BlockNumberIsValid(insertPage) && ndeletable > 0)
					insertPage = searchPage;

				searchPage = IvfflatPageGetOpaque(page)->nextblkno;

				if (ndeletable > 0)
				{
					/* Delete tuples */
					PageIndexMultiDelete(page, deletable, ndeletable);
					MarkBufferDirty(buf);
					GenericXLogFinish(state);
				}
				else
					GenericXLogAbort(state);

				UnlockReleaseBuffer(buf);
			}

			/*
			 * Update after all tuples deleted.
			 *
			 * We don't add or delete items from lists pages, so offset won't
			 * change.
			 */
			if (BlockNumberIsValid(insertPage))
			{
				listInfo.offno = coffno;
				IvfflatUpdateList(index, state, listInfo, insertPage, InvalidBlockNumber, InvalidBlockNumber, MAIN_FORKNUM);
			}
		}
	}

	return stats;
}

/*
 * Clean up after a VACUUM operation
 */
IndexBulkDeleteResult *
ivfflatvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	rel = info->index;

	if (stats == NULL)
		return NULL;

	stats->num_pages = RelationGetNumberOfBlocks(rel);

	return stats;
}
