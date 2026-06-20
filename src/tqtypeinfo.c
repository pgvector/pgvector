#include "postgres.h"

#include <math.h>
#include <string.h>

#include "halfutils.h"
#include "halfvec.h"
#include "sparsevec.h"
#include "tq.h"
#include "vector.h"

/*
 * Ensure the value's dimensions match the index's.  The opclass distance
 * functions never run on this path (the LUT is built from raw floats), so the
 * converters themselves must reject mismatched dimensions -- otherwise a
 * mismatched query or inserted value reads (or, for sparsevec, writes) past
 * the scratch buffer.
 */
static inline void
TqCheckDim(int valueDim, int dim)
{
	if (valueDim != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different vector dimensions %d and %d", valueDim, dim)));
}

/* vector: zero-copy passthrough of the detoasted ->x (scratch unused). */
const float *
TqVectorToFloat(Datum value, float *scratch, int dim)
{
	Vector	   *v = DatumGetVector(value);

	(void) scratch;
	TqCheckDim(v->dim, dim);
	return v->x;
}

/* halfvec: convert each fp16 coord to fp32 into scratch. */
const float *
TqHalfvecToFloat(Datum value, float *scratch, int dim)
{
	HalfVector *h = DatumGetHalfVector(value);

	TqCheckDim(h->dim, dim);
	for (int i = 0; i < dim; i++)
		scratch[i] = HalfToFloat4(h->x[i]);
	return scratch;
}

/* sparsevec: zero scratch then scatter nonzeros by 0-based index. */
const float *
TqSparsevecToFloat(Datum value, float *scratch, int dim)
{
	SparseVector *s = DatumGetSparseVector(value);
	float	   *vals = SPARSEVEC_VALUES(s);

	TqCheckDim(s->dim, dim);
	memset(scratch, 0, sizeof(float) * dim);
	for (int j = 0; j < s->nnz; j++)
		scratch[s->indices[j]] = vals[j];
	return scratch;
}

/*
 * Resolve the opclass type-info vtable. `procnum` is the AM's type-info support
 * slot (TQ_TYPE_INFO_PROC / TQIVF_TYPE_INFO_PROC / TQHNSW_TYPE_INFO_PROC). Falls
 * back to vector/L2 when absent.
 */
const TqTypeInfo *
TqGetTypeInfo(Relation index, int procnum)
{
	static const TqTypeInfo defaultInfo = {
		.metric = TQ_METRIC_L2,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqVectorToFloat,
	};

	if (OidIsValid(index_getprocid(index, 1, procnum)))
	{
		FmgrInfo   *proc = index_getprocinfo(index, 1, procnum);

		return (const TqTypeInfo *) DatumGetPointer(FunctionCall0Coll(proc, InvalidOid));
	}
	return &defaultInfo;
}

/*
 * Extract a dense float array ready for TqEncode. For cosine the result is
 * unit-normalized at float level, mirroring the pre-encode l2_normalize the
 * vector path has always applied (so the stored entry->norm is ~1 for cosine).
 * The normalize runs on `scratch`, never on the source ->x, which may alias a
 * non-toasted heap tuple; matches l2_normalize's zero-vector semantics.
 */
const float *
TqExtractForEncode(const TqTypeInfo *ti, Datum value, TqMetric metric,
				   float *scratch, int dim)
{
	const float *fv = ti->toFloat(value, scratch, dim);

	if (metric == TQ_METRIC_COSINE)
	{
		if (fv != scratch)
			memcpy(scratch, fv, sizeof(float) * dim);
		TqL2NormalizeFloat(scratch, dim);
		fv = scratch;
	}
	return fv;
}
