#include "ivf_sq.h"

#include "../common/metadata.h"
#include "scalar_quantizer.h"
#include "../vector.h"
#include "access/generic_xlog.h"
#include "c.h"
#include "fmgr.h"
#include "common/relpath.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/itemptr.h"
#include "utils/relcache.h"

void UpdateMetaPageWithMultipliers(Relation index, const Vector *multipliers,
								   ForkNumber fork_num)
{
	Buffer buf;
	Page page;
	GenericXLogState *state;
	IvfsqMetaPage ptr;
	Metadata *metadata;

	buf = ReadBufferExtended(index, fork_num, IVFFLAT_METAPAGE_BLKNO, RBM_NORMAL,
							 /*strategy=*/NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	ptr = IvfsqPageGetMeta(page);
	metadata = WriteMetadata(index, (Metadata *)multipliers, fork_num);
	Assert(VARATT_IS_EXTERNAL(metadata));
	memcpy(&ptr->multipliers_loc, &((ExternalMetadata *)metadata)->loc,
		   sizeof(ItemPointerData));

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

Vector *IvfsqGetMultipliers(Relation index, ForkNumber fork_num)
{
	Buffer buf;
	Page page;
	size_t max_bytes;
	IvfsqMetaPage meta_page;
	Vector *inv_multipliers;

	buf = ReadBufferExtended(index, fork_num, IVFFLAT_METAPAGE_BLKNO, RBM_NORMAL,
							 /*strategy=*/NULL);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buf);
	meta_page = IvfsqPageGetMeta(page);

	max_bytes = VECTOR_SIZE(meta_page->base.dimensions);
	inv_multipliers = InitVector(meta_page->base.dimensions);
	/* Read the metadata and perform some basic dimension check to verify the integrity. */
	if (!ReadMetadata(index, &meta_page->multipliers_loc,
					  (Metadata *)(inv_multipliers), max_bytes, fork_num) ||
		inv_multipliers->dim != meta_page->base.dimensions)
	{
		elog(WARNING,
			 "cannot read metadata from index or dimension mismatch: read: %d vs. "
			 "meta page dim: %d",
			 inv_multipliers->dim, meta_page->base.dimensions);
		pfree(inv_multipliers);
		inv_multipliers = NULL;
	}

	UnlockReleaseBuffer(buf);
	return inv_multipliers;
}

static Vector *ProprocessQuery(const Vector *query,
							   const Vector *inv_multipliers)
{
	Vector *preprocessed_query = InitVector(query->dim);
	for (int i = 0; i < preprocessed_query->dim; ++i)
	{
		/* We ensure multipliers are non-zero. */
		Assert(inv_multipliers->x[i] != 0.0);
		preprocessed_query->x[i] = query->x[i] * inv_multipliers->x[i];
	}

	return preprocessed_query;
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(inner_product_int8_float_batched);
Datum inner_product_int8_float_batched(PG_FUNCTION_ARGS)
{
	Vector *query = PG_GETARG_VECTOR_P(0);
	Vector *inv_multipliers = PG_GETARG_VECTOR_P(1);
	Page page = PG_GETARG_POINTER(2);
	Vector *preprocessed_query = ProprocessQuery(query, inv_multipliers);
	Vector *result = InitVector(PageGetMaxOffsetNumber(page));
	TupleDesc tupdesc;
#if PG_VERSION_NUM >= 120000
	tupdesc = CreateTemplateTupleDesc(1);
#else
	tupdesc = CreateTemplateTupleDesc(1, false);
#endif
	TupleDescInitEntry(tupdesc, (AttrNumber)1, "quantized_vector", BYTEAOID, -1, 0);

	ComputeOneToManyDotProductDistance(preprocessed_query, tupdesc, page, result);

	pfree(preprocessed_query);

	PG_RETURN_VECTOR_P(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(squared_l2_distance_int8_float_batched);
Datum squared_l2_distance_int8_float_batched(PG_FUNCTION_ARGS)
{
	Vector *query = PG_GETARG_VECTOR_P(0);
	Vector *inv_multipliers = PG_GETARG_VECTOR_P(1);
	Page page = PG_GETARG_POINTER(2);
	Vector *result = InitVector(PageGetMaxOffsetNumber(page));

	TupleDesc tupdesc;
#if PG_VERSION_NUM >= 120000
	tupdesc = CreateTemplateTupleDesc(1);
#else
	tupdesc = CreateTemplateTupleDesc(1, false);
#endif
	TupleDescInitEntry(tupdesc, (AttrNumber)1, "quantized_vector", BYTEAOID, -1, 0);

	ComputeOneToManySquaredL2Distance(query, inv_multipliers, tupdesc, page,
									  result);
	PG_RETURN_VECTOR_P(result);
}
