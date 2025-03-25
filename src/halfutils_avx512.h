#ifndef HALFUTILS_AVX512_H
#define HALFUTILS_AVX512_H

#ifdef USE_AVX512
#include "postgres.h"
#include "halfvec.h"
#include "vector.h"

#if (defined(__GNUC__) && (__GNUC__ >= 12)) || \
	(defined(__clang__) && (__clang_major__ >= 16)) || \
	(defined __AVX512FP16__)
#define HAVE_AVX512FP16
#endif

#ifdef HAVE_AVX512FP16
extern float HalfvecL2SquaredDistanceAvx512(int dim, half * ax, half * bx);
extern float HalfvecInnerProductAvx512(int dim, half * ax, half * bx);
extern double HalfvecCosineSimilarityAvx512(int dim, half * ax, half * bx);
extern float HalfvecL1DistanceAvx512(int dim, half * ax, half * bx);
extern void Float4ToHalfVectorAvx512(Vector * vec, HalfVector * result);

extern bool SupportsAvx512Fp16(void);
#endif
#endif
#endif
