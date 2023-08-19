#include "ivf_list.h"

#include "metadata.h"

/* PG C headers */
#include "c.h"
#include "common/relpath.h"
#include "pg_config.h"
#include "utils/relcache.h"

IvfListV2 CreateIvfListV2(Relation rel, Metadata *metadata, bool external,
						  ForkNumber fork_num, size_t *list_size)
{
	IvfListV2 list;
	Metadata *list_metadata = metadata;
	size_t metadata_size =
		external ? EXTERNAL_METADATA_SIZE : VARSIZE_ANY(metadata);

	Assert(!VARATT_IS_EXTERNAL(metadata));
	*list_size = offsetof(IvfListDataV2, metadata) + metadata_size;

	list = (IvfListV2)palloc0(*list_size);
	list->startPage = InvalidBlockNumber;
	list->insertPage = InvalidBlockNumber;
	list->unused = 0UL;
	if (external)
	{
		list_metadata = WriteMetadata(rel, metadata, fork_num);
		Assert(VARATT_IS_EXTERNAL(list_metadata));
		Assert(((ExternalMetadata *)list_metadata)->length ==
			   (VARSIZE_ANY_EXHDR(metadata)));
	}

	memcpy(&list->metadata, list_metadata, metadata_size);
	return list;
}
