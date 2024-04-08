#include "postgres.h"

#include "halfutils.h"
#include "halfvec.h"

#ifdef F16C_SUPPORT
#include <immintrin.h>
#endif

/*
 * Get the L2 squared distance between half vectors
 */
double
HalfvecL2DistanceSquared(HalfVector * a, HalfVector * b)
{
	half	   *ax = a->x;
	half	   *bx = b->x;
	float		distance = 0.0;

#if defined(F16C_SUPPORT) && defined(__FMA__)
	int			i;
	float		s[8];
	int			count = (a->dim / 8) * 8;
	__m256		dist = _mm256_setzero_ps();

	for (i = 0; i < count; i += 8)
	{
		__m128i		axi = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		bxi = _mm_loadu_si128((__m128i *) (bx + i));
		__m256		axs = _mm256_cvtph_ps(axi);
		__m256		bxs = _mm256_cvtph_ps(bxi);
		__m256		diff = _mm256_sub_ps(axs, bxs);

		dist = _mm256_fmadd_ps(diff, diff, dist);
	}

	_mm256_storeu_ps(s, dist);

	distance = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	for (; i < a->dim; i++)
	{
		float		diff = HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]);

		distance += diff * diff;
	}
#else
	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
	{
		float		diff = HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]);

		distance += diff * diff;
	}
#endif

	return (double) distance;
}

/*
 * Get the inner product of two half vectors
 */
double
HalfvecInnerProduct(HalfVector * a, HalfVector * b)
{
	half	   *ax = a->x;
	half	   *bx = b->x;
	float		distance = 0.0;

#if defined(F16C_SUPPORT) && defined(__FMA__)
	int			i;
	float		s[8];
	int			count = (a->dim / 8) * 8;
	__m256		dist = _mm256_setzero_ps();

	for (i = 0; i < count; i += 8)
	{
		__m128i		axi = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		bxi = _mm_loadu_si128((__m128i *) (bx + i));
		__m256		axs = _mm256_cvtph_ps(axi);
		__m256		bxs = _mm256_cvtph_ps(bxi);

		dist = _mm256_fmadd_ps(axs, bxs, dist);
	}

	_mm256_storeu_ps(s, dist);

	distance = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	for (; i < a->dim; i++)
		distance += HalfToFloat4(ax[i]) * HalfToFloat4(bx[i]);
#else
	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
		distance += HalfToFloat4(ax[i]) * HalfToFloat4(bx[i]);
#endif

	return (double) distance;
}
