#ifndef INTVEC_H
#define INTVEC_H

#include "vector.h"

#define INTVEC_MAX_DIM VECTOR_MAX_DIM

#define INTVEC_SIZE(_dim)		(offsetof(IntVector, x) + sizeof(int8)*(_dim))
#define DatumGetIntVector(x)	((IntVector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_INTVEC_P(x)	DatumGetIntVector(PG_GETARG_DATUM(x))
#define PG_RETURN_INTVEC_P(x)	PG_RETURN_POINTER(x)

typedef struct IntVector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int16		dim;			/* number of dimensions */
	int16		unused;
	int8		x[FLEXIBLE_ARRAY_MEMBER];
}			IntVector;

IntVector  *InitIntVector(int dim);

#endif
