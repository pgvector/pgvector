#ifndef SPARSEVEC_H
#define SPARSEVEC_H

#define SPARSEVEC_MAX_DIM 1000000000
#define SPARSEVEC_MAX_NNZ 16000

#define DatumGetSparseVector(x)		((SparseVector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_SPARSEVEC_P(x)	DatumGetSparseVector(PG_GETARG_DATUM(x))
#define PG_RETURN_SPARSEVEC_P(x)	PG_RETURN_POINTER(x)

/*
 * Indices use 0-based numbering for the on-disk (and binary) format (consistent with C)
 * and are always sorted. Values come after indices.
 */
typedef struct SparseVector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		dim;			/* number of dimensions */
	int32		nnz;			/* number of non-zero elements */
	int32		unused;			/* reserved for future use, always zero */
	int32		indices[FLEXIBLE_ARRAY_MEMBER];
}			SparseVector;

/* Use functions instead of macros to avoid double evaluation */

static inline Size
SPARSEVEC_SIZE(int nnz)
{
	return offsetof(SparseVector, indices) + (nnz * sizeof(int32)) + (nnz * sizeof(float));
}

static inline float *
SPARSEVEC_VALUES(SparseVector * x)
{
	return (float *) (((char *) x) + offsetof(SparseVector, indices) + (x->nnz * sizeof(int32)));
}

SparseVector *InitSparseVector(int dim, int nnz);

#endif
