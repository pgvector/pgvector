#ifndef SPARSEVEC_H
#define SPARSEVEC_H

#include "fmgr.h"

#define SPARSEVEC_MAX_DIM 1000000
#define SPARSEVEC_MAX_NNZ 16000

/* Ensure values are aligned */
#define SPARSEVEC_SIZE(_nnz)		(offsetof(SparseVector, indices) + MAXALIGN((_nnz) * sizeof(int32)) + (_nnz * sizeof(float)))
#define SPARSEVEC_VALUES(x)		((float *) (((char *) (x)) + offsetof(SparseVector, indices) + MAXALIGN((x)->nnz * sizeof(int32))))
#define DatumGetSparseVector(x)		((SparseVector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_SPARSEVEC_P(x)	DatumGetSparseVector(PG_GETARG_DATUM(x))
#define PG_RETURN_SPARSEVEC_P(x)	PG_RETURN_POINTER(x)

typedef struct SparseVector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		dim;			/* number of dimensions */
	int32		nnz;
	int32		unused;
	int32		indices[FLEXIBLE_ARRAY_MEMBER];
}			SparseVector;

SparseVector *InitSparseVector(int dim, int nnz);

#endif
