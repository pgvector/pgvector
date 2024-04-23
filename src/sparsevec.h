#ifndef SPARSEVEC_H
#define SPARSEVEC_H

#include "fmgr.h"

#define SPARSEVEC_MAX_DIM 100000
#define SPARSEVEC_MAX_NNZ 16000

typedef struct SparseVector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		dim;			/* number of dimensions */
	int32		nnz;			/* number of non-zero dimensions */
	int32		unused;			/* reserved for future use, always zero */

	/*
	 * indices contains nnz entries, one for each non-zero dimension. Each
	 * element is the dimension stored a that index. Sorted by dimension.
	 */
	int32		indices[FLEXIBLE_ARRAY_MEMBER];

	/*
	 * The values are stored after the indices:
	 *
	 * float4   values[FLEXIBLE_ARRAY_MEMBER];
	 */
}			SparseVector;

/* accessor for the 'values' field that is logically part of the struct */
static inline float *
SPARSEVEC_VALUES(SparseVector *x)
{
	return (float *) &x->indices[x->nnz];
}

/* size in bytes of a SparseVector */
static inline size_t
SPARSEVEC_SIZE(int32 nnz)
{
	/*
	 * Note: No padding is needed, because SparseVector only contains 'int32'
	 * and 'float4' fields, and the have the same alignment.
	 */
	return offsetof(SparseVector, indices) +
		nnz * sizeof(int32) +		/* indices array */
		nnz * sizeof(float);		/* values array */
}

#define DatumGetSparseVector(x)		((SparseVector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_SPARSEVEC_P(x)	DatumGetSparseVector(PG_GETARG_DATUM(x))
#define PG_RETURN_SPARSEVEC_P(x)	PG_RETURN_POINTER(x)

SparseVector *InitSparseVector(int dim, int nnz);

#endif
