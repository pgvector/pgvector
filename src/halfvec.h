#ifndef HALFVEC_H
#define HALFVEC_H

#define __STDC_WANT_IEC_60559_TYPES_EXT__

#include <float.h>

/* We use two types of dispatching: intrinsics and target_clones */
/* TODO Move to better place */
#ifndef DISABLE_DISPATCH
/* Only enable for more recent compilers to keep build process simple */
#if defined(__x86_64__) && defined(__GNUC__) && __GNUC__ >= 9
#define USE_DISPATCH
#elif defined(__x86_64__) && defined(__clang_major__) && __clang_major__ >= 7
#define USE_DISPATCH
#elif defined(_M_AMD64) && defined(_MSC_VER) && _MSC_VER >= 1920
#define USE_DISPATCH
#endif
#endif

/* target_clones requires glibc */
#if defined(USE_DISPATCH) && defined(__gnu_linux__) && defined(__has_attribute)
/* Use separate line for portability */
#if __has_attribute(target_clones)
#define USE_TARGET_CLONES
#endif
#endif

/* Apple clang check needed for universal binaries on Mac */
#if defined(USE_DISPATCH) && (defined(HAVE__GET_CPUID) || defined(__apple_build_version__))
#define USE__GET_CPUID
#endif

#if defined(USE_DISPATCH)
#define HALFVEC_DISPATCH
#endif

/* F16C has better performance than _Float16 (on x86-64) */
#if defined(__F16C__)
#define F16C_SUPPORT
#elif defined(__FLT16_MAX__) && !defined(HALFVEC_DISPATCH) && !defined(__FreeBSD__) && (!defined(__i386__) || defined(__SSE2__))
#define FLT16_SUPPORT
#endif

#ifdef FLT16_SUPPORT
#define half _Float16
#define HALF_MAX FLT16_MAX
#else
#define half uint16
#define HALF_MAX 65504
#endif

#define HALFVEC_MAX_DIM 16000

#define HALFVEC_SIZE(_dim)		(offsetof(HalfVector, x) + sizeof(half)*(_dim))
#define DatumGetHalfVector(x)	((HalfVector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_HALFVEC_P(x)	DatumGetHalfVector(PG_GETARG_DATUM(x))
#define PG_RETURN_HALFVEC_P(x)	PG_RETURN_POINTER(x)

typedef struct HalfVector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int16		dim;			/* number of dimensions */
	int16		unused;			/* reserved for future use, always zero */
	half		x[FLEXIBLE_ARRAY_MEMBER];
}			HalfVector;

HalfVector *InitHalfVector(int dim);

#endif
