#ifndef SVECTOR_H
#define SVECTOR_H

#define SVECTOR_MAX_DIM 100000

#define SVECTOR_SIZE(_nnz)		(offsetof(SVector, indices) + (_nnz) * sizeof(int32) + (_nnz * sizeof(float)))
#define SVECTOR_VALUES(x)		((float *) (((char *) (x)) + offsetof(SVector, indices) + (x)->nnz * sizeof(int32)))
#define DatumGetSVector(x)		((SVector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_SVECTOR_P(x)	DatumGetSVector(PG_GETARG_DATUM(x))
#define PG_RETURN_SVECTOR_P(x)	PG_RETURN_POINTER(x)

typedef struct SVector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		dim;			/* number of dimensions */
	int32		nnz;
	int32		unused;
	int32		indices[FLEXIBLE_ARRAY_MEMBER];
}			SVector;

SVector    *InitSVector(int dim, int nnz);

#endif
