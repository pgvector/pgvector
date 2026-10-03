#ifndef VECTOR_H
#define VECTOR_H

#include "fmgr.h"
#include "utils/palloc.h"

#if PG_VERSION_NUM < 190000
#include "storage/shmem.h"		/* for add_size()/mul_size() in some versions */
#endif

#define VECTOR_MAX_DIM 16000

#define VECTOR_SIZE(_dim)		add_size(offsetof(Vector, x), mul_size(sizeof(float), _dim))
#define VECTOR_EXPAND(x)		x
#define _DATUM_GET_VECTOR_1(x)	((Vector *) PG_DETOAST_DATUM(x))
#define _DATUM_GET_VECTOR_2(x, len)				DatumGetVectorPrefix(x, len)
#define _DATUM_GET_VECTOR_3(x, start, count)	DatumGetVectorSlice(x, start, count)
#define _DATUM_GET_VECTOR_SELECT(_1, _2, _3, NAME, ...)	NAME
#define DatumGetVector(...) \
	VECTOR_EXPAND(_DATUM_GET_VECTOR_SELECT(__VA_ARGS__, _DATUM_GET_VECTOR_3, _DATUM_GET_VECTOR_2, _DATUM_GET_VECTOR_1, 0)(__VA_ARGS__))
#define _PG_GETARG_VECTOR_P_1(n)				_DATUM_GET_VECTOR_1(PG_GETARG_DATUM(n))
#define _PG_GETARG_VECTOR_P_2(n, len)			_DATUM_GET_VECTOR_2(PG_GETARG_DATUM(n), len)
#define _PG_GETARG_VECTOR_P_3(n, start, count)	_DATUM_GET_VECTOR_3(PG_GETARG_DATUM(n), start, count)
#define PG_GETARG_VECTOR_P(...) \
	VECTOR_EXPAND(_DATUM_GET_VECTOR_SELECT(__VA_ARGS__, _PG_GETARG_VECTOR_P_3, _PG_GETARG_VECTOR_P_2, _PG_GETARG_VECTOR_P_1, 0)(__VA_ARGS__))
#define PG_RETURN_VECTOR_P(x)	PG_RETURN_POINTER(x)

typedef struct Vector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int16		dim;			/* number of dimensions */
	int16		unused;			/* reserved for future use, always zero */
	float		x[FLEXIBLE_ARRAY_MEMBER];
}			Vector;

Vector	   *InitVector(int dim);
Vector	   *DatumGetVectorPrefix(Datum x, int32 len);
Vector	   *DatumGetVectorSlice(Datum x, int32 start, int32 count);
void		PrintVector(char *msg, Vector * vector);
int			vector_cmp_internal(Vector * a, Vector * b);

/* TODO Move to better place */
#if PG_VERSION_NUM >= 160000
#define FUNCTION_PREFIX
#else
#define FUNCTION_PREFIX PGDLLEXPORT
#endif

#endif
