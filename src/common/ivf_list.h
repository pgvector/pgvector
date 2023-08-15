#ifndef PGVECTOR_SRC_COMMON_IVF_LIST_H_
#define PGVECTOR_SRC_COMMON_IVF_LIST_H_

#include "postgres.h"

#include "storage/block.h"
#include "common/relpath.h"
#include "utils/relcache.h"

#include "metadata.h"

/*
 * Partition ("list") metadata. It stores the location of its leaf pages
 * (`start_page`), the location to insert new data into leaf pages
 * (`insert_page`), together with some user defined metadata.
 *
 * STORAGE FORMAT
 * --------------------------------------------------------
 * | start_page(4) | insert_page(4) | metadata (varlena) |
 * --------------------------------------------------------
 */
typedef struct IvfListDataV1
{
	BlockNumber startPage;
	BlockNumber insertPage;
	/* Inline or externalized storage. */
	Metadata metadata;
} IvfListDataV1;

typedef IvfListDataV1 *IvfListV1;

/*
 * Similar to the above but added 8-bytes `unused` space for future use.
 * Note that the newly built indexes are forced to use this format.
 */
typedef struct IvfListDataV2
{
	BlockNumber startPage;
	BlockNumber insertPage;
	uint64_t unused;
	// Inline or externalized storage.
	Metadata metadata;
} IvfListDataV2;

typedef IvfListDataV2 *IvfListV2;

#define IVF_LIST_GET_START_PAGE(item, version)         \
	(((version) == 1) ? ((IvfListV1)(item))->startPage \
					  : ((IvfListV2)(item))->startPage)

#define IVF_LIST_SET_START_PAGE(item, version, startPage) \
	do                                                    \
	{                                                     \
		if ((version) == 1)                               \
			((IvfListV1)(item))->startPage = startPage;   \
		else                                              \
			((IvfListV2)(item))->startPage = startPage;   \
	} while (0);

#define IVF_LIST_SET_INSERT_PAGE(item, version, insertPage) \
	do                                                      \
	{                                                       \
		if ((version) == 1)                                 \
			((IvfListV1)(item))->insertPage = insertPage;   \
		else                                                \
			((IvfListV2)(item))->insertPage = insertPage;   \
	} while (0);

#define IVF_LIST_GET_INSERT_PAGE(item, version)         \
	(((version) == 1) ? ((IvfListV1)(item))->insertPage \
					  : ((IvfListV2)(item))->insertPage)

#define IVF_LIST_GET_METADATA(item, version)           \
	(((version) == 1) ? &((IvfListV1)(item))->metadata \
					  : &((IvfListV2)(item))->metadata)

/*
 * Creates `IvfListV2` structure from `metadata`. The input `metadata` should be inlined.
 * When `external` is `true`, it writes the `metadata` into external pages and an
 * `ExternalMetadata` is written into the structure, otherwise, the `metadata` is copied
 * into the result.
 *   RETURNS: `IvfListV2` structure on the heap with its size `list_size`.
 */
IvfListV2 CreateIvfListV2(Relation rel, Metadata *metadata, bool external,
						  ForkNumber fork_num, size_t *list_size);

#endif /* PGVECTOR_SRC_COMMON_IVF_LIST_H_ */
