#ifndef INT8VEC_H
#define INT8VEC_H

#include "vector.h"

#define INT8VEC_MAX_DIM 16000

#define INT8VEC_SIZE(_dim)		(offsetof(Int8Vector, x) + sizeof(int8)*(_dim))
#define DatumGetInt8Vector(x)	((Int8Vector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_INT8VEC_P(x)	DatumGetInt8Vector(PG_GETARG_DATUM(x))
#define PG_RETURN_INT8VEC_P(x)	PG_RETURN_POINTER(x)

typedef struct Int8Vector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int16		dim;			/* number of dimensions */
	int16		unused;			/* reserved for future use, always zero */
	int8		x[FLEXIBLE_ARRAY_MEMBER];
}			Int8Vector;

Int8Vector *InitInt8Vector(int dim);

#endif
