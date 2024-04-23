#ifndef HALFVEC_H
#define HALFVEC_H

#define __STDC_WANT_IEC_60559_TYPES_EXT__

#include <float.h>

#include "bitutils.h"
#include "fmgr.h"
#include "vector.h"

#if defined(USE_DISPATCH)
#define HALFVEC_DISPATCH
#endif

/* F16C has better performance than _Float16 (on x86-64) */
#if defined(__F16C__)
#define F16C_SUPPORT
#elif defined(__FLT16_MAX__) && !defined(HALFVEC_DISPATCH)
#define FLT16_SUPPORT
#endif

#ifdef FLT16_SUPPORT
#define half _Float16
#define HALF_MAX FLT16_MAX
#else
#define half uint16
#define HALF_MAX 65504
#endif

#define HALFVEC_MAX_DIM VECTOR_MAX_DIM

#define HALFVEC_SIZE(_dim)		(offsetof(HalfVector, x) + sizeof(half)*(_dim))
#define DatumGetHalfVector(x)	((HalfVector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_HALFVEC_P(x)	DatumGetHalfVector(PG_GETARG_DATUM(x))
#define PG_RETURN_HALFVEC_P(x)	PG_RETURN_POINTER(x)

typedef struct HalfVector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int16		dim;			/* number of dimensions */
	int16		unused;
	half		x[FLEXIBLE_ARRAY_MEMBER];
}			HalfVector;

HalfVector *InitHalfVector(int dim);
int			halfvec_cmp_internal(HalfVector * a, HalfVector * b);

#endif
