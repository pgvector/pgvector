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
#elif defined(USE_DISPATCH) && defined(USE_SVE) && (defined(USE_GETAUXVAL) || defined(USE_ELF_AUX_INFO))
#include <arm_sve.h>
#include <sys/auxv.h>

#if defined(AT_HWCAP2) && defined(HWCAP2_SVE2)
#define HALFVEC_SVE_DISPATCH
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

static float
HalfvecInnerProductDefault(int dim, half * ax, half * bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += HalfToFloat4(ax[i]) * HalfToFloat4(bx[i]);

	return distance;
}

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
#ifdef _MSC_VER
#define TARGET_F16C
#define TARGET_AVX512
#else
#define TARGET_F16C __attribute__((target("avx,f16c,fma")))
#define TARGET_AVX512 __attribute__((target("avx512f,avx512dq")))
#endif

TARGET_F16C static float
HalfvecL2SquaredDistanceF16c(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	float		s[8];
	int			count = (dim / 32) * 32;
	int			count8 = (dim / 8) * 8;
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

		__m256		diff0 = _mm256_sub_ps(_mm256_cvtph_ps(axi0), _mm256_cvtph_ps(bxi0));
		__m256		diff1 = _mm256_sub_ps(_mm256_cvtph_ps(axi1), _mm256_cvtph_ps(bxi1));
		__m256		diff2 = _mm256_sub_ps(_mm256_cvtph_ps(axi2), _mm256_cvtph_ps(bxi2));
		__m256		diff3 = _mm256_sub_ps(_mm256_cvtph_ps(axi3), _mm256_cvtph_ps(bxi3));

		dist0 = _mm256_fmadd_ps(diff0, diff0, dist0);
		dist1 = _mm256_fmadd_ps(diff1, diff1, dist1);
		dist2 = _mm256_fmadd_ps(diff2, diff2, dist2);
		dist3 = _mm256_fmadd_ps(diff3, diff3, dist3);
	}

	for (; i < count8; i += 8)
	{
		__m128i		axi0 = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		bxi0 = _mm_loadu_si128((__m128i *) (bx + i));

		__m256		diff0 = _mm256_sub_ps(_mm256_cvtph_ps(axi0), _mm256_cvtph_ps(bxi0));

		dist0 = _mm256_fmadd_ps(diff0, diff0, dist0);
	}

	_mm256_storeu_ps(s, _mm256_add_ps(_mm256_add_ps(dist0, dist1), _mm256_add_ps(dist2, dist3)));

	distance = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	for (; i < dim; i++)
	{
		float		diff = HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]);

		distance += diff * diff;
	}

	return distance;
}

TARGET_F16C static float
HalfvecInnerProductF16c(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	float		s[8];
	int			count = (dim / 32) * 32;
	int			count8 = (dim / 8) * 8;
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

	for (; i < count8; i += 8)
	{
		__m128i		axi0 = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		bxi0 = _mm_loadu_si128((__m128i *) (bx + i));

		dist0 = _mm256_fmadd_ps(_mm256_cvtph_ps(axi0), _mm256_cvtph_ps(bxi0), dist0);
	}

	_mm256_storeu_ps(s, _mm256_add_ps(_mm256_add_ps(dist0, dist1), _mm256_add_ps(dist2, dist3)));

	distance = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	for (; i < dim; i++)
		distance += HalfToFloat4(ax[i]) * HalfToFloat4(bx[i]);

	return distance;
}

TARGET_F16C static double
HalfvecCosineSimilarityF16c(int dim, half * ax, half * bx)
{
	float		similarity;
	float		norma;
	float		normb;
	int			i;
	float		s0[8];
	float		s1[8];
	float		s2[8];
	int			count = (dim / 16) * 16;
	int			count8 = (dim / 8) * 8;
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
		__m256		axs1 = _mm256_cvtph_ps(axi1);
		__m256		bxs0 = _mm256_cvtph_ps(bxi0);
		__m256		bxs1 = _mm256_cvtph_ps(bxi1);

		sim0 = _mm256_fmadd_ps(axs0, bxs0, sim0);
		sim1 = _mm256_fmadd_ps(axs1, bxs1, sim1);
		na0 = _mm256_fmadd_ps(axs0, axs0, na0);
		na1 = _mm256_fmadd_ps(axs1, axs1, na1);
		nb0 = _mm256_fmadd_ps(bxs0, bxs0, nb0);
		nb1 = _mm256_fmadd_ps(bxs1, bxs1, nb1);
	}

	for (; i < count8; i += 8)
	{
		__m128i		axi0 = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		bxi0 = _mm_loadu_si128((__m128i *) (bx + i));

		__m256		axs0 = _mm256_cvtph_ps(axi0);
		__m256		bxs0 = _mm256_cvtph_ps(bxi0);

		sim0 = _mm256_fmadd_ps(axs0, bxs0, sim0);
		na0 = _mm256_fmadd_ps(axs0, axs0, na0);
		nb0 = _mm256_fmadd_ps(bxs0, bxs0, nb0);
	}

	_mm256_storeu_ps(s0, _mm256_add_ps(sim0, sim1));
	similarity = s0[0] + s0[1] + s0[2] + s0[3] + s0[4] + s0[5] + s0[6] + s0[7];

	_mm256_storeu_ps(s1, _mm256_add_ps(na0, na1));
	norma = s1[0] + s1[1] + s1[2] + s1[3] + s1[4] + s1[5] + s1[6] + s1[7];

	_mm256_storeu_ps(s2, _mm256_add_ps(nb0, nb1));
	normb = s2[0] + s2[1] + s2[2] + s2[3] + s2[4] + s2[5] + s2[6] + s2[7];

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

/* Does not require FMA, but keep logic simple */
TARGET_F16C static float
HalfvecL1DistanceF16c(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	float		s[8];
	int			count = (dim / 8) * 8;
	__m256		dist = _mm256_setzero_ps();
	__m256		sign = _mm256_set1_ps(-0.0);

	for (i = 0; i < count; i += 8)
	{
		__m128i		axi = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		bxi = _mm_loadu_si128((__m128i *) (bx + i));
		__m256		axs = _mm256_cvtph_ps(axi);
		__m256		bxs = _mm256_cvtph_ps(bxi);

		dist = _mm256_add_ps(dist, _mm256_andnot_ps(sign, _mm256_sub_ps(axs, bxs)));
	}

	_mm256_storeu_ps(s, dist);

	distance = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	for (; i < dim; i++)
		distance += fabsf(HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]));

	return distance;
}

TARGET_AVX512 static float
HalfvecL2SquaredDistanceAvx512(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	int			count = (dim / 64) * 64;
	__m512		dist0 = _mm512_setzero_ps();
	__m512		dist1 = _mm512_setzero_ps();
	__m512		dist2 = _mm512_setzero_ps();
	__m512		dist3 = _mm512_setzero_ps();

	for (i = 0; i < count; i += 64)
	{
		__m256i		axi0 = _mm256_loadu_si256((__m256i *) (ax + i));
		__m256i		axi1 = _mm256_loadu_si256((__m256i *) (ax + i + 16));
		__m256i		axi2 = _mm256_loadu_si256((__m256i *) (ax + i + 32));
		__m256i		axi3 = _mm256_loadu_si256((__m256i *) (ax + i + 48));
		__m256i		bxi0 = _mm256_loadu_si256((__m256i *) (bx + i));
		__m256i		bxi1 = _mm256_loadu_si256((__m256i *) (bx + i + 16));
		__m256i		bxi2 = _mm256_loadu_si256((__m256i *) (bx + i + 32));
		__m256i		bxi3 = _mm256_loadu_si256((__m256i *) (bx + i + 48));

		__m512		diff0 = _mm512_sub_ps(_mm512_cvtph_ps(axi0), _mm512_cvtph_ps(bxi0));
		__m512		diff1 = _mm512_sub_ps(_mm512_cvtph_ps(axi1), _mm512_cvtph_ps(bxi1));
		__m512		diff2 = _mm512_sub_ps(_mm512_cvtph_ps(axi2), _mm512_cvtph_ps(bxi2));
		__m512		diff3 = _mm512_sub_ps(_mm512_cvtph_ps(axi3), _mm512_cvtph_ps(bxi3));

		dist0 = _mm512_fmadd_ps(diff0, diff0, dist0);
		dist1 = _mm512_fmadd_ps(diff1, diff1, dist1);
		dist2 = _mm512_fmadd_ps(diff2, diff2, dist2);
		dist3 = _mm512_fmadd_ps(diff3, diff3, dist3);
	}

	distance = _mm512_reduce_add_ps(_mm512_add_ps(_mm512_add_ps(dist0, dist1), _mm512_add_ps(dist2, dist3)));

	for (; i < dim; i++)
	{
		float		diff = HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]);

		distance += diff * diff;
	}

	return distance;
}

TARGET_AVX512 static float
HalfvecInnerProductAvx512(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	int			count = (dim / 64) * 64;
	__m512		dist0 = _mm512_setzero_ps();
	__m512		dist1 = _mm512_setzero_ps();
	__m512		dist2 = _mm512_setzero_ps();
	__m512		dist3 = _mm512_setzero_ps();

	for (i = 0; i < count; i += 64)
	{
		__m256i		axi0 = _mm256_loadu_si256((__m256i *) (ax + i));
		__m256i		axi1 = _mm256_loadu_si256((__m256i *) (ax + i + 16));
		__m256i		axi2 = _mm256_loadu_si256((__m256i *) (ax + i + 32));
		__m256i		axi3 = _mm256_loadu_si256((__m256i *) (ax + i + 48));
		__m256i		bxi0 = _mm256_loadu_si256((__m256i *) (bx + i));
		__m256i		bxi1 = _mm256_loadu_si256((__m256i *) (bx + i + 16));
		__m256i		bxi2 = _mm256_loadu_si256((__m256i *) (bx + i + 32));
		__m256i		bxi3 = _mm256_loadu_si256((__m256i *) (bx + i + 48));

		dist0 = _mm512_fmadd_ps(_mm512_cvtph_ps(axi0), _mm512_cvtph_ps(bxi0), dist0);
		dist1 = _mm512_fmadd_ps(_mm512_cvtph_ps(axi1), _mm512_cvtph_ps(bxi1), dist1);
		dist2 = _mm512_fmadd_ps(_mm512_cvtph_ps(axi2), _mm512_cvtph_ps(bxi2), dist2);
		dist3 = _mm512_fmadd_ps(_mm512_cvtph_ps(axi3), _mm512_cvtph_ps(bxi3), dist3);
	}

	distance = _mm512_reduce_add_ps(_mm512_add_ps(_mm512_add_ps(dist0, dist1), _mm512_add_ps(dist2, dist3)));

	for (; i < dim; i++)
		distance += HalfToFloat4(ax[i]) * HalfToFloat4(bx[i]);

	return distance;
}

TARGET_AVX512 static double
HalfvecCosineSimilarityAvx512(int dim, half * ax, half * bx)
{
	float		similarity;
	float		norma;
	float		normb;
	int			i;
	int			count = (dim / 32) * 32;
	__m512		sim0 = _mm512_setzero_ps();
	__m512		sim1 = _mm512_setzero_ps();
	__m512		na0 = _mm512_setzero_ps();
	__m512		na1 = _mm512_setzero_ps();
	__m512		nb0 = _mm512_setzero_ps();
	__m512		nb1 = _mm512_setzero_ps();

	for (i = 0; i < count; i += 32)
	{
		__m256i		axi0 = _mm256_loadu_si256((__m256i *) (ax + i));
		__m256i		axi1 = _mm256_loadu_si256((__m256i *) (ax + i + 16));
		__m256i		bxi0 = _mm256_loadu_si256((__m256i *) (bx + i));
		__m256i		bxi1 = _mm256_loadu_si256((__m256i *) (bx + i + 16));

		__m512		axs0 = _mm512_cvtph_ps(axi0);
		__m512		axs1 = _mm512_cvtph_ps(axi1);
		__m512		bxs0 = _mm512_cvtph_ps(bxi0);
		__m512		bxs1 = _mm512_cvtph_ps(bxi1);

		sim0 = _mm512_fmadd_ps(axs0, bxs0, sim0);
		sim1 = _mm512_fmadd_ps(axs1, bxs1, sim1);
		na0 = _mm512_fmadd_ps(axs0, axs0, na0);
		na1 = _mm512_fmadd_ps(axs1, axs1, na1);
		nb0 = _mm512_fmadd_ps(bxs0, bxs0, nb0);
		nb1 = _mm512_fmadd_ps(bxs1, bxs1, nb1);
	}

	similarity = _mm512_reduce_add_ps(_mm512_add_ps(sim0, sim1));
	norma = _mm512_reduce_add_ps(_mm512_add_ps(na0, na1));
	normb = _mm512_reduce_add_ps(_mm512_add_ps(nb0, nb1));

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

TARGET_AVX512 static float
HalfvecL1DistanceAvx512(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	int			count = (dim / 32) * 32;
	__m512		dist = _mm512_setzero_ps();
	__m512		sign = _mm512_set1_ps(-0.0);

	for (i = 0; i < count; i += 32)
	{
		__m256i		axi = _mm256_loadu_si256((__m256i *) (ax + i));
		__m256i		bxi = _mm256_loadu_si256((__m256i *) (bx + i));
		__m512		axs = _mm512_cvtph_ps(axi);
		__m512		bxs = _mm512_cvtph_ps(bxi);

		dist = _mm512_add_ps(dist, _mm512_andnot_ps(sign, _mm512_sub_ps(axs, bxs)));
	}

	distance = _mm512_reduce_add_ps(dist);

	for (; i < dim; i++)
		distance += fabsf(HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]));

	return distance;
}

#define CPU_FEATURE_FMA     (1 << 12)
#define CPU_FEATURE_OSXSAVE (1 << 27)
#define CPU_FEATURE_AVX     (1 << 28)
#define CPU_FEATURE_F16C    (1 << 29)

#define CPU_FEATURE_AVX512F  (1 << 16)	/* F7,0 EBX */
#define CPU_FEATURE_AVX512DQ (1 << 17)	/* F7,0 EBX */

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

TARGET_XSAVE static bool
SupportsAvx512(void)
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

	/* Check XMM, YMM, and ZMM registers are enabled */
	if ((_xgetbv(0) & 0xe6) != 0xe6)
		return false;

#if defined(USE__GET_CPUID)
	__get_cpuid_count(7, 0, &exx[0], &exx[1], &exx[2], &exx[3]);
#else
	__cpuidex(exx, 7, 0);
#endif

	/* Check AVX512F */
	if ((exx[1] & CPU_FEATURE_AVX512F) != CPU_FEATURE_AVX512F)
		return false;

	/* Check AVX512VDQ */
	return (exx[1] & CPU_FEATURE_AVX512DQ) == CPU_FEATURE_AVX512DQ;
}
#elif defined(HALFVEC_SVE_DISPATCH)
#define TARGET_SVE2 __attribute__((target("+sve2")))

#define SVE_SUM(a, b, c, d, pt) svaddv_f32(pt, svadd_f32_x(pt, svadd_f32_x(pt, a, b), svadd_f32_x(pt, c, d)))

TARGET_SVE2 static float
HalfvecL2SquaredDistanceSve2(int dim, half * ax, half * bx)
{
	int			width = svcnth();
	int			width2 = width * 2;
	int			i;
	int			count = (dim / width2) * width2;
	svfloat32_t dist0 = svdup_n_f32(0.0);
	svfloat32_t dist1 = svdup_n_f32(0.0);
	svfloat32_t dist2 = svdup_n_f32(0.0);
	svfloat32_t dist3 = svdup_n_f32(0.0);
	svfloat32_t zero = svdup_n_f32(0.0);
	svfloat16_t one = svdup_n_f16(1.0);
	svbool_t	pt16 = svptrue_b16();
	svbool_t	pt32 = svptrue_b32();

	for (i = 0; i < count; i += width2)
	{
		svfloat16_t axi0 = svld1_f16(pt16, (float16_t *) (ax + i));
		svfloat16_t axi1 = svld1_f16(pt16, (float16_t *) (ax + i + width));
		svfloat16_t bxi0 = svld1_f16(pt16, (float16_t *) (bx + i));
		svfloat16_t bxi1 = svld1_f16(pt16, (float16_t *) (bx + i + width));

		svfloat32_t diff0 = svmlslb_f32(svmlalb_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff1 = svmlslt_f32(svmlalt_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff2 = svmlslb_f32(svmlalb_f32(zero, axi1, one), bxi1, one);
		svfloat32_t diff3 = svmlslt_f32(svmlalt_f32(zero, axi1, one), bxi1, one);

		dist0 = svmla_f32_x(pt32, dist0, diff0, diff0);
		dist1 = svmla_f32_x(pt32, dist1, diff1, diff1);
		dist2 = svmla_f32_x(pt32, dist2, diff2, diff2);
		dist3 = svmla_f32_x(pt32, dist3, diff3, diff3);
	}

	for (; i < dim; i += width)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t axi0 = svld1_f16(pg, (float16_t *) (ax + i));
		svfloat16_t bxi0 = svld1_f16(pg, (float16_t *) (bx + i));

		svfloat32_t diff0 = svmlslb_f32(svmlalb_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff1 = svmlslt_f32(svmlalt_f32(zero, axi0, one), bxi0, one);

		dist0 = svmla_f32_x(pt32, dist0, diff0, diff0);
		dist1 = svmla_f32_x(pt32, dist1, diff1, diff1);
	}

	return SVE_SUM(dist0, dist1, dist2, dist3, pt32);
}

TARGET_SVE2 static float
HalfvecInnerProductSve2(int dim, half * ax, half * bx)
{
	int			width = svcnth();
	int			width2 = width * 2;
	int			i;
	int			count = (dim / width2) * width2;
	svfloat32_t dist0 = svdup_n_f32(0.0);
	svfloat32_t dist1 = svdup_n_f32(0.0);
	svfloat32_t dist2 = svdup_n_f32(0.0);
	svfloat32_t dist3 = svdup_n_f32(0.0);
	svbool_t	pt16 = svptrue_b16();
	svbool_t	pt32 = svptrue_b32();

	for (i = 0; i < count; i += width2)
	{
		svfloat16_t axi0 = svld1_f16(pt16, (float16_t *) (ax + i));
		svfloat16_t axi1 = svld1_f16(pt16, (float16_t *) (ax + i + width));
		svfloat16_t bxi0 = svld1_f16(pt16, (float16_t *) (bx + i));
		svfloat16_t bxi1 = svld1_f16(pt16, (float16_t *) (bx + i + width));

		dist0 = svmlalb_f32(dist0, axi0, bxi0);
		dist1 = svmlalt_f32(dist1, axi0, bxi0);
		dist2 = svmlalb_f32(dist2, axi1, bxi1);
		dist3 = svmlalt_f32(dist3, axi1, bxi1);
	}

	for (; i < dim; i += width)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t axi0 = svld1_f16(pg, (float16_t *) (ax + i));
		svfloat16_t bxi0 = svld1_f16(pg, (float16_t *) (bx + i));

		dist0 = svmlalb_f32(dist0, axi0, bxi0);
		dist1 = svmlalt_f32(dist1, axi0, bxi0);
	}

	return SVE_SUM(dist0, dist1, dist2, dist3, pt32);
}

TARGET_SVE2 static double
HalfvecCosineSimilaritySve2(int dim, half * ax, half * bx)
{
	float		similarity;
	float		norma;
	float		normb;
	int			width = svcnth();
	int			width2 = width * 2;
	int			i;
	int			count = (dim / width2) * width2;
	svfloat32_t sim0 = svdup_n_f32(0.0);
	svfloat32_t sim1 = svdup_n_f32(0.0);
	svfloat32_t sim2 = svdup_n_f32(0.0);
	svfloat32_t sim3 = svdup_n_f32(0.0);
	svfloat32_t na0 = svdup_n_f32(0.0);
	svfloat32_t na1 = svdup_n_f32(0.0);
	svfloat32_t na2 = svdup_n_f32(0.0);
	svfloat32_t na3 = svdup_n_f32(0.0);
	svfloat32_t nb0 = svdup_n_f32(0.0);
	svfloat32_t nb1 = svdup_n_f32(0.0);
	svfloat32_t nb2 = svdup_n_f32(0.0);
	svfloat32_t nb3 = svdup_n_f32(0.0);
	svbool_t	pt16 = svptrue_b16();
	svbool_t	pt32 = svptrue_b32();

	for (i = 0; i < count; i += width2)
	{
		svfloat16_t axi0 = svld1_f16(pt16, (float16_t *) (ax + i));
		svfloat16_t axi1 = svld1_f16(pt16, (float16_t *) (ax + i + width));
		svfloat16_t bxi0 = svld1_f16(pt16, (float16_t *) (bx + i));
		svfloat16_t bxi1 = svld1_f16(pt16, (float16_t *) (bx + i + width));

		sim0 = svmlalb_f32(sim0, axi0, bxi0);
		sim1 = svmlalt_f32(sim1, axi0, bxi0);
		sim2 = svmlalb_f32(sim2, axi1, bxi1);
		sim3 = svmlalt_f32(sim3, axi1, bxi1);
		na0 = svmlalb_f32(na0, axi0, axi0);
		na1 = svmlalt_f32(na1, axi0, axi0);
		na2 = svmlalb_f32(na2, axi1, axi1);
		na3 = svmlalt_f32(na3, axi1, axi1);
		nb0 = svmlalb_f32(nb0, bxi0, bxi0);
		nb1 = svmlalt_f32(nb1, bxi0, bxi0);
		nb2 = svmlalb_f32(nb2, bxi1, bxi1);
		nb3 = svmlalt_f32(nb3, bxi1, bxi1);
	}

	for (; i < dim; i += width)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t axi0 = svld1_f16(pg, (float16_t *) (ax + i));
		svfloat16_t bxi0 = svld1_f16(pg, (float16_t *) (bx + i));

		sim0 = svmlalb_f32(sim0, axi0, bxi0);
		sim1 = svmlalt_f32(sim1, axi0, bxi0);
		na0 = svmlalb_f32(na0, axi0, axi0);
		na1 = svmlalt_f32(na1, axi0, axi0);
		nb0 = svmlalb_f32(nb0, bxi0, bxi0);
		nb1 = svmlalt_f32(nb1, bxi0, bxi0);
	}

	similarity = SVE_SUM(sim0, sim1, sim2, sim3, pt32);
	norma = SVE_SUM(na0, na1, na2, na3, pt32);
	normb = SVE_SUM(nb0, nb1, nb2, nb3, pt32);

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
}

TARGET_SVE2 static float
HalfvecL1DistanceSve2(int dim, half * ax, half * bx)
{
	int			width = svcnth();
	int			width2 = width * 2;
	int			i;
	int			count = (dim / width2) * width2;
	svfloat32_t dist0 = svdup_n_f32(0.0);
	svfloat32_t dist1 = svdup_n_f32(0.0);
	svfloat32_t dist2 = svdup_n_f32(0.0);
	svfloat32_t dist3 = svdup_n_f32(0.0);
	svfloat32_t zero = svdup_n_f32(0.0);
	svfloat16_t one = svdup_n_f16(1.0);
	svbool_t	pt16 = svptrue_b16();
	svbool_t	pt32 = svptrue_b32();

	for (i = 0; i < count; i += width2)
	{
		svfloat16_t axi0 = svld1_f16(pt16, (float16_t *) (ax + i));
		svfloat16_t axi1 = svld1_f16(pt16, (float16_t *) (ax + i + width));
		svfloat16_t bxi0 = svld1_f16(pt16, (float16_t *) (bx + i));
		svfloat16_t bxi1 = svld1_f16(pt16, (float16_t *) (bx + i + width));

		svfloat32_t diff0 = svmlslb_f32(svmlalb_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff1 = svmlslt_f32(svmlalt_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff2 = svmlslb_f32(svmlalb_f32(zero, axi1, one), bxi1, one);
		svfloat32_t diff3 = svmlslt_f32(svmlalt_f32(zero, axi1, one), bxi1, one);

		dist0 = svadd_f32_x(pt32, dist0, svabs_f32_x(pt32, diff0));
		dist1 = svadd_f32_x(pt32, dist1, svabs_f32_x(pt32, diff1));
		dist2 = svadd_f32_x(pt32, dist2, svabs_f32_x(pt32, diff2));
		dist3 = svadd_f32_x(pt32, dist3, svabs_f32_x(pt32, diff3));
	}

	for (; i < dim; i += width)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t axi0 = svld1_f16(pg, (float16_t *) (ax + i));
		svfloat16_t bxi0 = svld1_f16(pg, (float16_t *) (bx + i));

		svfloat32_t diff0 = svmlslb_f32(svmlalb_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff1 = svmlslt_f32(svmlalt_f32(zero, axi0, one), bxi0, one);

		dist0 = svadd_f32_x(pt32, dist0, svabs_f32_x(pt32, diff0));
		dist1 = svadd_f32_x(pt32, dist1, svabs_f32_x(pt32, diff1));
	}

	return SVE_SUM(dist0, dist1, dist2, dist3, pt32);
}

static bool
SupportsSve2(void)
{
#if defined(USE_GETAUXVAL)
	return (getauxval(AT_HWCAP2) & HWCAP2_SVE2) != 0;
#else
	unsigned long hwcap2;

	if (elf_aux_info(AT_HWCAP2, &hwcap2, sizeof(hwcap2)) != 0)
		return false;

	return (hwcap2 & HWCAP2_SVE2) != 0;
#endif
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
	if (SupportsAvx512())
	{
		HalfvecL2SquaredDistance = HalfvecL2SquaredDistanceAvx512;
		HalfvecInnerProduct = HalfvecInnerProductAvx512;
		HalfvecCosineSimilarity = HalfvecCosineSimilarityAvx512;
		HalfvecL1Distance = HalfvecL1DistanceAvx512;
	}
	else if (SupportsCpuFeature(CPU_FEATURE_AVX | CPU_FEATURE_F16C | CPU_FEATURE_FMA))
	{
		HalfvecL2SquaredDistance = HalfvecL2SquaredDistanceF16c;
		HalfvecInnerProduct = HalfvecInnerProductF16c;
		HalfvecCosineSimilarity = HalfvecCosineSimilarityF16c;
		/* Does not require FMA, but keep logic simple */
		HalfvecL1Distance = HalfvecL1DistanceF16c;
	}
#elif defined(HALFVEC_SVE_DISPATCH)
	if (SupportsSve2())
	{
		HalfvecL2SquaredDistance = HalfvecL2SquaredDistanceSve2;
		HalfvecInnerProduct = HalfvecInnerProductSve2;
		HalfvecCosineSimilarity = HalfvecCosineSimilaritySve2;
		HalfvecL1Distance = HalfvecL1DistanceSve2;
	}
#endif
}
