#include "metadata.h"

#include "postgres.h"

#include "access/generic_xlog.h"
#include "c.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "storage/block.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/item.h"
#include "storage/itemid.h"
#include "storage/itemptr.h"
#include "storage/off.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "utils/relcache.h"

typedef struct MetadataPageOpaqueData
{
	BlockNumber next_blkno;
	uint16_t unused;
	uint16_t page_id; /* for identification of metadata pages */
} MetadataPageOpaqueData;

typedef MetadataPageOpaqueData *MetadataPageOpaque;

/*
 * Metadata chunk is stored as `varlena` with the following format:
 * ----------------------------
 * | VAR_HEADER | NEXT | DATA |
 * ----------------------------
 */
typedef struct MetadataChunkHeader
{
	char vl_len_[4];
	ItemPointerData next;
} MetadataChunkHeader;

#define kMetadataPageId 0xBEEF

#define kChunkHeaderSize sizeof(MetadataChunkHeader)

#define kMaxChunkDataSize 1024

/* The max number of metadata chunks in a page. */
#define kMaxMetadataChunksPerPage                                         \
	(int)((BLCKSZ - MAXALIGN(SizeOfPageHeaderData +                       \
							 MAXALIGN(sizeof(MetadataPageOpaqueData)))) / \
		  (sizeof(ItemIdData) + MAXALIGN(kMaxChunkDataSize + kChunkHeaderSize)))

/* The max number of pages a metadata is allowed to span. */
#define kMaxMetadataPages 16

/* The maximum metadata size supported. */
#define kMaxMetadataSizeAllowed \
	(kMaxMetadataPages * kMaxMetadataChunksPerPage * kMaxChunkDataSize)

#define MetadataPageGetOpaque(page) \
	((MetadataPageOpaque)PageGetSpecialPointer((page)))

/*
 * Aligned metadata chunk, used as a temporary buffer allocated on the stack
 * from which page item is copied.
 */
typedef union AlignedMetadataChunk
{
	MetadataChunkHeader header;
	char data[kMaxChunkDataSize + kChunkHeaderSize];
	double force_align_d;
	int64_t force_align_i64;
} AlignedMetadataChunk;

/* Returns the pointer to the chunk start (after `MetadataChunkHeader`). */
#define CHUNK_DATA(ptr) ((char *)VARDATA_ANY((ptr)) + sizeof(ItemPointerData))

/* Returns the size of the chunk (excluding `MetadataChunkHeader`). */
#define CHUNK_SIZE_EXHDR(ptr) \
	(VARSIZE_ANY_EXHDR((ptr)) - sizeof(ItemPointerData))

/*
 * Initialize the metadata page.
 */
static void MetadataInitPage(Buffer buf, Page page)
{
	PageInit(page, BufferGetPageSize(buf), sizeof(MetadataPageOpaqueData));
	MetadataPageGetOpaque(page)->next_blkno = InvalidBlockNumber;
	MetadataPageGetOpaque(page)->page_id = kMetadataPageId;
}

/*
 * Initialize the metadata page and register it to xlog for modifications.
 */
static void MetadataInitRegisterPage(Relation rel, Buffer *buf, Page *page,
									 GenericXLogState **state)
{
	*state = GenericXLogStart(rel);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	MetadataInitPage(*buf, *page);
}

/*
 * Allocate and append a new page after the page in `buf`. Note that all pending
 * writes on `buf` will be committed and a new xlog will be open for the new
 * page. `buf` gets modified to point to the newly allocated buffer.
 */
static void MetadataAppendPage(Relation rel, Buffer *buf, Page *page,
							   GenericXLogState **state, ForkNumber fork_num)
{
	/* Allocate a new buffer. */
	Page newpage;
	Buffer newbuf = ReadBufferExtended(rel, fork_num, P_NEW, RBM_NORMAL, NULL);

	LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);
	newpage = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);

	/* Update the previous buffer. */
	MetadataPageGetOpaque(*page)->next_blkno = BufferGetBlockNumber(newbuf);

	/* Initialize the new page. */
	MetadataInitPage(newbuf, newpage);

	/* Commit all pending modifications on the previous page. */
	GenericXLogFinish(*state);
	UnlockReleaseBuffer(*buf);

	/* Register the new page to xlog for modifications. */
	*state = GenericXLogStart(rel);
	*page = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);
	*buf = newbuf;
}

static OffsetNumber WriteChunk(Relation rel, Buffer buf, Page page, char *data,
							   size_t size)
{
	OffsetNumber offno;
	AlignedMetadataChunk chunk;
	ItemPointerSet(&chunk.header.next, InvalidBlockNumber, InvalidOffsetNumber);

	SET_VARSIZE(&chunk, kChunkHeaderSize + size);
	memcpy(CHUNK_DATA(&chunk), data, size);

	if ((offno = PageAddItem(page, (Item)&chunk, MAXALIGN(VARSIZE_ANY(&chunk)),
							 InvalidOffsetNumber,
							 /*overwrite=*/false, /*is_heap=*/false)) ==
		InvalidOffsetNumber)
		elog(ERROR, "WriteChunk: failed to add a metadata chunk to \"%s\"",
			 RelationGetRelationName(rel));

	return offno;
}

/*
 * Update the `next` pointer for the previous chunk to point to the current
 * chunk. This function should be called after the current chunk is added to a
 * page slot (so that its location is known). The current chunk is written to
 * `curr_blkno` at offset `curr_offno` and similarly, the previous chunk is in
 * page `prev_blkno` at `prev_offno`.
 * When current and previous chunks are on the same page, i.e. `prev_blkno ==
 * curr_blkno`, the update is made directly on `curr_page` image, which should
 * be X-locked by the caller. Otherwise, the `prev_blkno` needs to be read and
 * X-locked.
 */
static void LinkToPrevChunk(Relation rel, BlockNumber prev_blkno,
							OffsetNumber prev_offno, BlockNumber curr_blkno,
							OffsetNumber curr_offno, Page curr_page,
							ForkNumber fork_num)
{
	Page prev_page;
	Buffer prev_buf;
	GenericXLogState *state;
	MetadataChunkHeader *header;

	if (prev_blkno == curr_blkno)
	{
		/* Two chunks landed on the same page. */
		header = (MetadataChunkHeader *)PageGetItem(
			curr_page, PageGetItemId(curr_page, prev_offno));
		ItemPointerSet(&header->next, curr_blkno, curr_offno);
		return;
	}

	prev_buf = ReadBufferExtended(rel, fork_num, prev_blkno, RBM_NORMAL,
								  /*strategy=*/NULL);
	LockBuffer(prev_buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(rel);
	prev_page = GenericXLogRegisterBuffer(state, prev_buf, /*flags=*/0);

	header = (MetadataChunkHeader *)PageGetItem(
		prev_page, PageGetItemId(prev_page, prev_offno));
	ItemPointerSet(&header->next, curr_blkno, curr_offno);

	GenericXLogFinish(state);
	UnlockReleaseBuffer(prev_buf);
}

Metadata *WriteMetadata(Relation rel, const Metadata *metadata,
						ForkNumber fork_num)
{
	char *ptr = VARDATA_ANY(metadata);
	size_t size_remaining = VARSIZE_ANY_EXHDR(metadata);
	Buffer buf;
	Page curr_page;
	GenericXLogState *x_log_state;
	ExternalMetadata *external;
	BlockNumber prev_blkno, curr_blkno;
	OffsetNumber prev_offno, curr_offno;
	int page_allocated = 1;
	int size_to_write;
	Size item_size;

	if (size_remaining == 0 || size_remaining > kMaxMetadataSizeAllowed)
		return NULL;

	external = (ExternalMetadata *)palloc0(EXTERNAL_METADATA_SIZE);
	SET_VARTAG_EXTERNAL(external, /*tag=*/0);
	external->length = size_remaining;

	/*
	 * Always allocate a new page. Note that the caller needs to be responsible
	 * for ensuring only one backend is extending the relation.
	 */
	buf = ReadBufferExtended(rel, fork_num, P_NEW, RBM_NORMAL,
							 /*strategy=*/NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	/* Initialize the first page and update `loc`. */
	MetadataInitRegisterPage(rel, &buf, &curr_page, &x_log_state);
	curr_blkno = BufferGetBlockNumber(buf);
	ItemPointerSetBlockNumber(&external->loc, curr_blkno);

	prev_offno = InvalidOffsetNumber;
	while (size_remaining > 0)
	{
		CHECK_FOR_INTERRUPTS();

		size_to_write = Min(kMaxChunkDataSize, size_remaining);
		item_size = MAXALIGN(size_to_write + kChunkHeaderSize);
		if (PageGetFreeSpace(curr_page) < item_size)
		{
			if (page_allocated >= kMaxMetadataPages)
			{
				elog(WARNING,
					 "WriteMetadata: max number of pages exceeded, size remaining: "
					 "%lu, page allocated: %d, max pages allocated: %d",
					 size_remaining, page_allocated, kMaxMetadataPages);
				UnlockReleaseBuffer(buf);
				pfree(external);
				return NULL;
			}

			/* Run out of space in the current page, allocate a new page. */
			MetadataAppendPage(rel, &buf, &curr_page, &x_log_state, fork_num);
			curr_blkno = BufferGetBlockNumber(buf);
			page_allocated++;
		}

		/* Write the current chunk. */
		curr_offno = WriteChunk(rel, buf, curr_page, ptr, size_to_write);

		if (likely(prev_offno != InvalidOffsetNumber))
			/* Go back to the previous chunk and update the `next` link. */
			LinkToPrevChunk(rel, prev_blkno, prev_offno, curr_blkno, curr_offno,
							curr_page, fork_num);
		else
			/* This is the first chunk. */
			ItemPointerSetOffsetNumber(&external->loc, curr_offno);

		prev_blkno = curr_blkno;
		prev_offno = curr_offno;

		/* Advance the position in the buffer. */
		ptr += size_to_write;
		size_remaining -= size_to_write;
	}

	/* Commit the last buffer. */
	GenericXLogFinish(x_log_state);
	UnlockReleaseBuffer(buf);

	return (Metadata *)external;
}

bool ReadMetadata(Relation rel, ItemPointer loc, const Metadata *metadata,
				  size_t max_bytes, ForkNumber fork_num)
{
	Buffer buf = InvalidBuffer;
	Page page = NULL;
	OffsetNumber max_offno;
	BlockNumber blkno, next_blkno;
	OffsetNumber offno;

	char *ptr = VARDATA_ANY(metadata);
	MetadataChunkHeader *chunk;
	size_t chunk_size;
	size_t bytes_read = 0;

	blkno = InvalidBlockNumber;
	next_blkno = ItemPointerGetBlockNumber(loc);
	offno = ItemPointerGetOffsetNumber(loc);

	while (next_blkno != InvalidBlockNumber && bytes_read < max_bytes)
	{
		Assert(offno != InvalidOffsetNumber);
		if (blkno != next_blkno)
		{
			/* A new page needs to be read. */
			if (buf != InvalidBuffer)
				UnlockReleaseBuffer(buf);
			buf = ReadBufferExtended(rel, fork_num, next_blkno, RBM_NORMAL,
									 /*strategy=*/NULL);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			blkno = next_blkno;
		}

		max_offno = PageGetMaxOffsetNumber(page);
		if (offno > max_offno)
		{
			/* Confidence check, this is unexpected. */
			if (buf != InvalidBuffer)
				UnlockReleaseBuffer(buf);
			return false;
		}

		/* Copy the current chunk into the buffer and advance the buffer position. */
		chunk = (MetadataChunkHeader *)PageGetItem(page, PageGetItemId(page, offno));
		chunk_size = CHUNK_SIZE_EXHDR(chunk);
		memcpy(ptr, CHUNK_DATA(chunk), chunk_size);
		ptr += chunk_size;
		bytes_read += chunk_size;

		// Get the location for the next chunk.
		next_blkno = ItemPointerGetBlockNumberNoCheck(&chunk->next);
		offno = ItemPointerGetOffsetNumberNoCheck(&chunk->next);
	}

	if (buf != InvalidBuffer)
		UnlockReleaseBuffer(buf);
	SET_VARSIZE(metadata, bytes_read + VARHDRSZ);

	return true;
}

Metadata *FlattenMetadata(Relation rel, Metadata *metadata,
						  ForkNumber fork_num)
{
	ExternalMetadata *external_metadata;
	Metadata *flattened;

	if (!VARATT_IS_EXTERNAL(metadata))
		return metadata;

	external_metadata = (ExternalMetadata *)(metadata);
	flattened = (Metadata *)palloc0(external_metadata->length + VARHDRSZ);
	if (ReadMetadata(rel, &external_metadata->loc, flattened,
					 external_metadata->length, fork_num))
		return flattened;

	pfree(flattened);
	return NULL;
}
