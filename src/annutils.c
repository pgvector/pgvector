#include "postgres.h"

#include "access/generic_xlog.h"
#include "ann.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * Resolve the extension type in its actual schema, not the session search path.
 */
Oid
AnnVectorTypeOid(void)
{
	Oid			namespace = get_extension_schema(get_extension_oid("vector", false));

	return GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
						   CStringGetDatum("vector"), ObjectIdGetDatum(namespace));
}

void
AnnCheckIndex(Relation index)
{
	Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(index), 0);

	if (attr->atttypid != AnnVectorTypeOid())
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("ann indexes require vector values")));
	if (attr->atttypmod < 1)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("column does not have dimensions")));
	if (attr->atttypmod > ANN_MAX_DIM)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("column cannot have more than %d dimensions for ann index", ANN_MAX_DIM)));
}

void
AnnCheckPage(Page page)
{
	if (PageIsNew(page) || PageGetSpecialSize(page) != MAXALIGN(sizeof(AnnPageOpaqueData)) ||
		AnnPageGetOpaque(page)->page_id != ANN_PAGE_ID)
		ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("invalid ann index page")));
}

void
AnnCheckMetaPage(Page page)
{
	AnnMetaPageData *meta;

	AnnCheckPage(page);
	meta = AnnPageGetMeta(page);
	if (meta->magicNumber != ANN_MAGIC_NUMBER || meta->version != ANN_VERSION ||
		meta->dimensions < 1 || meta->dimensions > ANN_MAX_DIM)
		ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("invalid ann index metapage")));
}

void
AnnInitMetaPage(Relation index, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	AnnMetaPageData *meta;

	AnnCheckIndex(index);
	buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	if (BufferGetBlockNumber(buf) != ANN_METAPAGE_BLKNO)
		elog(ERROR, "ann index is not empty");
	page = BufferGetPage(buf);
	PageInit(page, BufferGetPageSize(buf), sizeof(AnnPageOpaqueData));
	AnnPageGetOpaque(page)->page_id = ANN_PAGE_ID;
	meta = AnnPageGetMeta(page);
	meta->magicNumber = ANN_MAGIC_NUMBER;
	meta->version = ANN_VERSION;
	meta->dimensions = TupleDescAttr(RelationGetDescr(index), 0)->atttypmod;
	meta->insertPage = InvalidBlockNumber;
	((PageHeader) page)->pd_lower = SizeOfPageHeaderData + sizeof(*meta);
	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
}
