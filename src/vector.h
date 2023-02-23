#ifndef VECTOR_H
#define VECTOR_H

#include "postgres.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#define VECTOR_MAX_DIM 16000

#define VECTOR_SIZE(_dim)		(offsetof(Vector, x) + sizeof(float)*(_dim))
#define DatumGetVector(x)		((Vector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_VECTOR_P(x)	DatumGetVector(PG_GETARG_DATUM(x))
#define PG_RETURN_VECTOR_P(x)	PG_RETURN_POINTER(x)

typedef struct Vector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int16		dim;			/* number of dimensions */
	int16		unused;
	float		x[FLEXIBLE_ARRAY_MEMBER];
}			Vector;

void		PrintVector(char *msg, Vector * vector);
int			vector_cmp_internal(Vector * a, Vector * b);

/*
 * Allocate and initialize a new vector
 */
static inline Vector *
InitVector(int dim)
{
	Vector	   *result;
	int			size;

	size = VECTOR_SIZE(dim);
	result = (Vector *) palloc0(size);
	SET_VARSIZE(result, size);
	result->dim = dim;

	return result;
}

#endif
