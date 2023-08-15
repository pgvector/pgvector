#ifndef PGVECTOR_SRC_COMMON_METADATA_H_
#define PGVECTOR_SRC_COMMON_METADATA_H_

#include "postgres.h"

#include "c.h"
#include "common/relpath.h"
#include "storage/item.h"
#include "storage/itemptr.h"
#include "utils/relcache.h"

/*
 * ********************************************************************
 *                   METADATA READ & WRITE
 * ********************************************************************
 *
 * I. DATA FORMAT
 * ==============
 * `Metadata` is defined as a variable-length byte array `varlena` with a 4-byte
 * header and body.
 *
 * II. WRITE
 * ==============
 * We always allocate a new page to persist metadata on write. Metadata is
 * divided into chunks and each chunk (bounded by `EXTERN_TUPLE_MAX_SIZE`) is
 * stored in a page slot. Multiple ordered chunks form a linked list by storing
 * the "next" chunk location in the chunk header (see `MetadataChunkHeader`).
 * New pages are allocated if the size of the metadata exceeds the page
 * capacity. To visualize the storage:
 *
 *               PAGE ID: 1
 * -------------------------------------
 * | 0: chunk[0]: next(1:1), data(xxx) |
 * -------------------------------------
 * | 1: chunk[1]: next(2:0), data(xxx) |
 * -------------------------------------
 *
 *               PAGE ID: 2
 * -------------------------------------
 * | 0: chunk[2]: next(2:1), data(xxx) |
 * -------------------------------------
 * | 1: chunk[3]: next(end), data(xxx) |
 * -------------------------------------
 *
 * *** NOTES ***:
 *
 * 1. Since write allocates new page(s), the caller is responsible for ensuring
 *    that there is only one backend calls this API per relation at a time.
 * 2. Each buffer page is registered to a separate xlog, and the previous
 *    xlog is always committed before allowing the next page to be modified.
 *    This means pages will be "leaked" if the process crashes in between. Since
 *    pages are registered to the tablespace of some relation, they will be
 *    recycled only when the tablespace is dropped.
 * 3. There are various bounds to ensure that a single write does not saturate
 *    the tablespace. We only allow a single write to span across
 *    `kMaxMetadataPages` pages. As a result, metadata size is also bounded
 *    by 'kMaxMetadataSizeAllowed'.
 *
 * III. READ
 * ==============
 * Chunks are combined and copied into the user provided `varlena` buffer.
 */

typedef struct varlena Metadata;

/*
 * External metadata is an extension of `varattrib_1b_e` to be compatible with
 * `varlena` macros.
 */
typedef struct ExternalMetadata
{
	uint8_t header;
	uint8_t unused;
	uint32_t length;	 /* Externally stored metadata size (i.e., size to read). */
	ItemPointerData loc; /* Location of the externally stored data. */
} ExternalMetadata;

#define EXTERNAL_METADATA_SIZE sizeof(ExternalMetadata)

/*
 * Read the metadata stored at location `loc` in relation `rel`. This function
 * guarantees that the **effective** max bytes read does not exceed `max_bytes`,
 * which should usually be the size of metadata (excluding the header). The
 * caller is responsible for allocating a buffer `metadata` of proper size and
 * the result is written into `metadata` (including the `varlena` header).
 *
 * RETURNS `true` on success or `false` otherwise.
 */
bool ReadMetadata(Relation rel, ItemPointer loc, const Metadata *metadata,
				  size_t max_bytes, ForkNumber fork_num);

/*
 * Flatten the `metadata`. The input `metadata` can be either inline or
 * out-of-line (`ExternalMetadata`).
 *
 * RETURNS:
 *  The flattened metadata, when `metadata` is `ExternalMetadata`.
 *  otherwise, `metadata` itself.
 */
Metadata *FlattenMetadata(Relation rel, Metadata *metadata,
						  ForkNumber fork_num);

/*
 * Write the `metadata` into pages allocated in relation `rel`. The location of
 * the first chunk is stored in `loc`. NOTE: refer to the top level comments for
 * how metadata are externalized.
 * RETURNS externalized metadata on success or `nullptr` otherwise.
 */
Metadata *WriteMetadata(Relation rel, const Metadata *metadata,
						ForkNumber fork_num);

#endif /* PGVECTOR_SRC_COMMON_METADATA_H_ */
