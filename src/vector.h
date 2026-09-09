#ifndef VECTOR_H
#define VECTOR_H

#include "fmgr.h"
#include "utils/palloc.h"

#if PG_VERSION_NUM < 190000
#include "storage/shmem.h"		/* for add_size()/mul_size() in some versions */
#endif

#define VECTOR_MAX_DIM 16000

#define VECTOR_SIZE(_dim)		add_size(offsetof(Vector, x), mul_size(sizeof(float), (Size) (_dim)))
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

/*
 * Task 2 O1: C-level distance kernels exported for the Ivfflat scan fast
 * path (src/ivfscan.c).
 *
 * They take (dim, ax, bx) instead of (Vector *, Vector *) because index
 * tuples store small vectors with a 1-byte short varlena header, so the
 * 4-byte-header Vector layout cannot be assumed for index entries. The
 * caller resolves the parts with IvfflatVectorParts() and guarantees equal
 * dimensions (enforced by the index typmod at insert time; the scan falls
 * back to the fmgr path if dimensions ever disagree).
 *
 * IvfflatFastL2SquaredDistance returns the SQUARED L2 distance: the opclass
 * distance proc is vector_l2_squared_distance and sqrt() is monotonic, so
 * the ordering is identical while one sqrt per tuple is saved.
 */
extern double	IvfflatFastL2SquaredDistance(int dim, const float *ax, const float *bx);
extern double	IvfflatFastNegInnerProduct(int dim, const float *ax, const float *bx);

/* TODO Move to better place */
#if PG_VERSION_NUM >= 160000
#define FUNCTION_PREFIX
#else
#define FUNCTION_PREFIX PGDLLEXPORT
#endif

#endif
