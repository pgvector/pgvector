#include "postgres.h"

#include "access/generic_xlog.h"
#include "ann.h"
#include "commands/vacuum.h"
#include "storage/freespace.h"

#if PG_VERSION_NUM >= 180000
#define vacuum_delay_point() vacuum_delay_point(false)
#endif

IndexBulkDeleteResult *
annbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			  IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;
	BlockNumber blocks = RelationGetNumberOfBlocks(index);

	if (!stats)
		stats = palloc0_object(IndexBulkDeleteResult);
	/* A later bulk-delete cycle rescans survivors; do not count them twice. */
	stats->num_index_tuples = 0;
	for (BlockNumber blkno = 1; blkno < blocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		GenericXLogState *state;
		OffsetNumber deletable[MaxOffsetNumber];
		OffsetNumber maxoff;
		int			count = 0;
		Size		freeSpace;

		vacuum_delay_point();
		buf = ReadBuffer(index, blkno);
		LockBufferForCleanup(buf);
		page = BufferGetPage(buf);
		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buf);
			RecordPageWithFreeSpace(index, blkno, BLCKSZ - SizeOfPageHeaderData -
									MAXALIGN(sizeof(AnnPageOpaqueData)) - sizeof(ItemIdData));
			continue;
		}
		AnnCheckPage(page);
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);
		maxoff = PageGetMaxOffsetNumber(page);
		for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoff; offno++)
		{
			IndexTuple	tuple = (IndexTuple) PageGetItem(page, PageGetItemId(page, offno));

			if (callback(&tuple->t_tid, callback_state))
			{
				deletable[count++] = offno;
				stats->tuples_removed++;
			}
			else
				stats->num_index_tuples++;
		}
		if (count)
		{
			PageIndexMultiDelete(page, deletable, count);
			freeSpace = PageGetFreeSpace(page);
			GenericXLogFinish(state);
		}
		else
		{
			freeSpace = PageGetFreeSpace(page);
			GenericXLogAbort(state);
		}
		UnlockReleaseBuffer(buf);
		RecordPageWithFreeSpace(index, blkno, freeSpace);
	}
	FreeSpaceMapVacuum(index);
	return stats;
}

IndexBulkDeleteResult *
annvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	if (info->analyze_only || !stats)
		return stats;
	stats->num_pages = RelationGetNumberOfBlocks(info->index);
	return stats;
}
