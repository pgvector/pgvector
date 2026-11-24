#include "postgres.h"

#include <math.h>

#include "halfvec.h"			/* for USE_TARGET_CLONES */
#include "vecutils.h"

#if defined(USE_TARGET_CLONES) && defined(__x86_64__)
#if (defined(__GNUC__) && __GNUC__ >= 12) || (defined(__clang_major__) && __clang_major__ >= 18)
#define VECTOR_TARGET_CLONES __attribute__((target_clones("default", "arch=x86-64-v3", "arch=x86-64-v4")))
#else
#define VECTOR_TARGET_CLONES __attribute__((target_clones("default", "fma", "avx512f")))
#endif
#else
#define VECTOR_TARGET_CLONES
#endif

VECTOR_TARGET_CLONES float
VectorL2SquaredDistance(int dim, float *ax, float *bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		float		diff = ax[i] - bx[i];

		distance += diff * diff;
	}

	return distance;
}

VECTOR_TARGET_CLONES float
VectorInnerProduct(int dim, float *ax, float *bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += ax[i] * bx[i];

	return distance;
}

VECTOR_TARGET_CLONES double
VectorCosineSimilarity(int dim, float *ax, float *bx)
{
	float		similarity = 0.0;
	float		norma = 0.0;
	float		normb = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		similarity += ax[i] * bx[i];
		norma += ax[i] * ax[i];
		normb += bx[i] * bx[i];
	}

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
}

/* Does not require FMA, but keep logic simple */
VECTOR_TARGET_CLONES float
VectorL1Distance(int dim, float *ax, float *bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += fabsf(ax[i] - bx[i]);

	return distance;
}
