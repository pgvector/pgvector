#include "postgres.h"

#include <math.h>

#include "halfutils.h"
#include "halfvec.h"

#ifdef HALFVEC_DISPATCH
#include <immintrin.h>

#if defined(USE__GET_CPUID)
#include <cpuid.h>
#else
#include <intrin.h>
#endif

#ifdef _MSC_VER
#define TARGET_F16C
#else
#define TARGET_F16C __attribute__((target("avx,f16c,fma")))
#endif
#endif

float		(*HalfvecL2SquaredDistance) (int dim, half * ax, half * bx);
float		(*HalfvecInnerProduct) (int dim, half * ax, half * bx);
double		(*HalfvecCosineSimilarity) (int dim, half * ax, half * bx);
float		(*HalfvecL1Distance) (int dim, half * ax, half * bx);

static float
HalfvecL2SquaredDistanceDefault(int dim, half * ax, half * bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		float		diff = HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]);

		distance += diff * diff;
	}

	return distance;
}

#ifdef HALFVEC_DISPATCH
TARGET_F16C static float
HalfvecL2SquaredDistanceF16c(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	float		s[8];
	int			count = (dim / 16) * 16;
	__m256		dist0 = _mm256_setzero_ps();
	__m256		dist1 = _mm256_setzero_ps();

	for (i = 0; i < count; i += 16)
	{
		__m128i		axi0 = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		axi1 = _mm_loadu_si128((__m128i *) (ax + i + 8));
		__m128i		bxi0 = _mm_loadu_si128((__m128i *) (bx + i));
		__m128i		bxi1 = _mm_loadu_si128((__m128i *) (bx + i + 8));

		__m256		diff0 = _mm256_sub_ps(_mm256_cvtph_ps(axi0), _mm256_cvtph_ps(bxi0));
		__m256		diff1 = _mm256_sub_ps(_mm256_cvtph_ps(axi1), _mm256_cvtph_ps(bxi1));

		dist0 = _mm256_fmadd_ps(diff0, diff0, dist0);
		dist1 = _mm256_fmadd_ps(diff1, diff1, dist1);
	}

	_mm256_storeu_ps(s, _mm256_add_ps(dist0, dist1));

	distance = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	for (; i < dim; i++)
	{
		float		diff = HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]);

		distance += diff * diff;
	}

	return distance;
}
#endif

static float
HalfvecInnerProductDefault(int dim, half * ax, half * bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += HalfToFloat4(ax[i]) * HalfToFloat4(bx[i]);

	return distance;
}

#ifdef HALFVEC_DISPATCH
TARGET_F16C static float
HalfvecInnerProductF16c(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	float		s[8];
	int			count = (dim / 32) * 32;
	__m256		dist0 = _mm256_setzero_ps();
	__m256		dist1 = _mm256_setzero_ps();
	__m256		dist2 = _mm256_setzero_ps();
	__m256		dist3 = _mm256_setzero_ps();

	for (i = 0; i < count; i += 32)
	{
		__m128i		axi0 = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		axi1 = _mm_loadu_si128((__m128i *) (ax + i + 8));
		__m128i		axi2 = _mm_loadu_si128((__m128i *) (ax + i + 16));
		__m128i		axi3 = _mm_loadu_si128((__m128i *) (ax + i + 24));
		__m128i		bxi0 = _mm_loadu_si128((__m128i *) (bx + i));
		__m128i		bxi1 = _mm_loadu_si128((__m128i *) (bx + i + 8));
		__m128i		bxi2 = _mm_loadu_si128((__m128i *) (bx + i + 16));
		__m128i		bxi3 = _mm_loadu_si128((__m128i *) (bx + i + 24));

		dist0 = _mm256_fmadd_ps(_mm256_cvtph_ps(axi0), _mm256_cvtph_ps(bxi0), dist0);
		dist1 = _mm256_fmadd_ps(_mm256_cvtph_ps(axi1), _mm256_cvtph_ps(bxi1), dist1);
		dist2 = _mm256_fmadd_ps(_mm256_cvtph_ps(axi2), _mm256_cvtph_ps(bxi2), dist2);
		dist3 = _mm256_fmadd_ps(_mm256_cvtph_ps(axi3), _mm256_cvtph_ps(bxi3), dist3);
	}

	_mm256_storeu_ps(s, _mm256_add_ps(_mm256_add_ps(dist0, dist1), _mm256_add_ps(dist2, dist3)));

	distance = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	for (; i < dim; i++)
		distance += HalfToFloat4(ax[i]) * HalfToFloat4(bx[i]);

	return distance;
}
#endif

static double
HalfvecCosineSimilarityDefault(int dim, half * ax, half * bx)
{
	float		similarity = 0.0;
	float		norma = 0.0;
	float		normb = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		float		axi = HalfToFloat4(ax[i]);
		float		bxi = HalfToFloat4(bx[i]);

		similarity += axi * bxi;
		norma += axi * axi;
		normb += bxi * bxi;
	}

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
}

#ifdef HALFVEC_DISPATCH
TARGET_F16C static double
HalfvecCosineSimilarityF16c(int dim, half * ax, half * bx)
{
	float		similarity;
	float		norma;
	float		normb;
	int			i;
	float		s[8];
	int			count = (dim / 16) * 16;
	__m256		sim0 = _mm256_setzero_ps();
	__m256		sim1 = _mm256_setzero_ps();
	__m256		na0 = _mm256_setzero_ps();
	__m256		na1 = _mm256_setzero_ps();
	__m256		nb0 = _mm256_setzero_ps();
	__m256		nb1 = _mm256_setzero_ps();

	for (i = 0; i < count; i += 16)
	{
		__m128i		axi0 = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		axi1 = _mm_loadu_si128((__m128i *) (ax + i + 8));
		__m128i		bxi0 = _mm_loadu_si128((__m128i *) (bx + i));
		__m128i		bxi1 = _mm_loadu_si128((__m128i *) (bx + i + 8));

		__m256		axs0 = _mm256_cvtph_ps(axi0);
		__m256		bxs0 = _mm256_cvtph_ps(bxi0);
		__m256		axs1 = _mm256_cvtph_ps(axi1);
		__m256		bxs1 = _mm256_cvtph_ps(bxi1);

		sim0 = _mm256_fmadd_ps(axs0, bxs0, sim0);
		na0 = _mm256_fmadd_ps(axs0, axs0, na0);
		nb0 = _mm256_fmadd_ps(bxs0, bxs0, nb0);

		sim1 = _mm256_fmadd_ps(axs1, bxs1, sim1);
		na1 = _mm256_fmadd_ps(axs1, axs1, na1);
		nb1 = _mm256_fmadd_ps(bxs1, bxs1, nb1);
	}

	_mm256_storeu_ps(s, _mm256_add_ps(sim0, sim1));
	similarity = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	_mm256_storeu_ps(s, _mm256_add_ps(na0, na1));
	norma = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	_mm256_storeu_ps(s, _mm256_add_ps(nb0, nb1));
	normb = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	/* Auto-vectorized */
	for (; i < dim; i++)
	{
		float		axi = HalfToFloat4(ax[i]);
		float		bxi = HalfToFloat4(bx[i]);

		similarity += axi * bxi;
		norma += axi * axi;
		normb += bxi * bxi;
	}

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
}
#endif

static float
HalfvecL1DistanceDefault(int dim, half * ax, half * bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += fabsf(HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]));

	return distance;
}

#ifdef HALFVEC_DISPATCH
/* Does not require FMA, but keep logic simple */
TARGET_F16C static float
HalfvecL1DistanceF16c(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	float		s[8];
	int			count = (dim / 32) * 32;
	__m256		dist0 = _mm256_setzero_ps();
	__m256		dist1 = _mm256_setzero_ps();
	__m256		dist2 = _mm256_setzero_ps();
	__m256		dist3 = _mm256_setzero_ps();
	__m256		sign = _mm256_set1_ps(-0.0);

	for (i = 0; i < count; i += 32)
	{
		__m128i		axi0 = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		axi1 = _mm_loadu_si128((__m128i *) (ax + i + 8));
		__m128i		axi2 = _mm_loadu_si128((__m128i *) (ax + i + 16));
		__m128i		axi3 = _mm_loadu_si128((__m128i *) (ax + i + 24));
		__m128i		bxi0 = _mm_loadu_si128((__m128i *) (bx + i));
		__m128i		bxi1 = _mm_loadu_si128((__m128i *) (bx + i + 8));
		__m128i		bxi2 = _mm_loadu_si128((__m128i *) (bx + i + 16));
		__m128i		bxi3 = _mm_loadu_si128((__m128i *) (bx + i + 24));

		dist0 = _mm256_add_ps(dist0, _mm256_andnot_ps(sign, _mm256_sub_ps(_mm256_cvtph_ps(axi0), _mm256_cvtph_ps(bxi0))));
		dist1 = _mm256_add_ps(dist1, _mm256_andnot_ps(sign, _mm256_sub_ps(_mm256_cvtph_ps(axi1), _mm256_cvtph_ps(bxi1))));
		dist2 = _mm256_add_ps(dist2, _mm256_andnot_ps(sign, _mm256_sub_ps(_mm256_cvtph_ps(axi2), _mm256_cvtph_ps(bxi2))));
		dist3 = _mm256_add_ps(dist3, _mm256_andnot_ps(sign, _mm256_sub_ps(_mm256_cvtph_ps(axi3), _mm256_cvtph_ps(bxi3))));
	}

	_mm256_storeu_ps(s, _mm256_add_ps(_mm256_add_ps(dist0, dist1), _mm256_add_ps(dist2, dist3)));

	distance = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	for (; i < dim; i++)
		distance += fabsf(HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]));

	return distance;
}
#endif

#ifdef HALFVEC_DISPATCH
#define CPU_FEATURE_FMA     (1 << 12)
#define CPU_FEATURE_OSXSAVE (1 << 27)
#define CPU_FEATURE_AVX     (1 << 28)
#define CPU_FEATURE_F16C    (1 << 29)

#ifdef _MSC_VER
#define TARGET_XSAVE
#else
#define TARGET_XSAVE __attribute__((target("xsave")))
#endif

TARGET_XSAVE static bool
SupportsCpuFeature(unsigned int feature)
{
	unsigned int exx[4] = {0, 0, 0, 0};

#if defined(USE__GET_CPUID)
	__get_cpuid(1, &exx[0], &exx[1], &exx[2], &exx[3]);
#else
	__cpuid(exx, 1);
#endif

	/* Check OS supports XSAVE */
	if ((exx[2] & CPU_FEATURE_OSXSAVE) != CPU_FEATURE_OSXSAVE)
		return false;

	/* Check XMM and YMM registers are enabled */
	if ((_xgetbv(0) & 6) != 6)
		return false;

	/* Now check features */
	return (exx[2] & feature) == feature;
}
#endif

void
HalfvecInit(void)
{
	/*
	 * Could skip pointer when single function, but no difference in
	 * performance
	 */
	HalfvecL2SquaredDistance = HalfvecL2SquaredDistanceDefault;
	HalfvecInnerProduct = HalfvecInnerProductDefault;
	HalfvecCosineSimilarity = HalfvecCosineSimilarityDefault;
	HalfvecL1Distance = HalfvecL1DistanceDefault;

#ifdef HALFVEC_DISPATCH
	if (SupportsCpuFeature(CPU_FEATURE_AVX | CPU_FEATURE_F16C | CPU_FEATURE_FMA))
	{
		HalfvecL2SquaredDistance = HalfvecL2SquaredDistanceF16c;
		HalfvecInnerProduct = HalfvecInnerProductF16c;
		HalfvecCosineSimilarity = HalfvecCosineSimilarityF16c;
		/* Does not require FMA, but keep logic simple */
		HalfvecL1Distance = HalfvecL1DistanceF16c;
	}
#endif
}
