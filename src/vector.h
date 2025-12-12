#ifndef VECTOR_H
#define VECTOR_H

#include <stdalign.h>

#define VECTOR_MAX_DIM 16000

#define VECTOR_SIZE(_dim)		(offsetof(Vector, x) + sizeof(float)*(_dim))
#define DatumGetVector(x)		((Vector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_VECTOR_P(x)	DatumGetVector(PG_GETARG_DATUM(x))
#define PG_RETURN_VECTOR_P(x)	PG_RETURN_POINTER(x)

typedef struct Vector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int16		dim;			/* number of dimensions */
	int16		unused;			/* reserved for future use, always zero */
	float		x[FLEXIBLE_ARRAY_MEMBER];
}			Vector;

Vector	   *InitVector(int dim);
void		PrintVector(char *msg, Vector * vector);
int			vector_cmp_internal(Vector * a, Vector * b);

#define VECTOR_SIZE_I16(_dim)		(offsetof(VectorI16, x) + sizeof(int16) * (_dim))
#define DatumGetVectorI16(x)		((VectorI16 *) PG_DETOAST_DATUM(x))
#define PG_GETARG_VECTORI16_P(x)	DatumGetVectorI16(PG_GETARG_DATUM(x))
#define PG_RETURN_VECTORI16_P(x)	PG_RETURN_POINTER(x)

typedef struct VectorI16
{
	int32				vl_len_;
	int16				dim;
	int16				unused;
	double				dot_product;
	alignas(64) int16	x[FLEXIBLE_ARRAY_MEMBER];
} VectorI16;

VectorI16	*InitVectorI16(int dim);

/* TODO Move to better place */
#if PG_VERSION_NUM >= 160000
#define FUNCTION_PREFIX
#else
#define FUNCTION_PREFIX PGDLLEXPORT
#endif

#endif
