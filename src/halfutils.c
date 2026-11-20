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
#elif defined(HALFVEC_SVE_DISPATCH)
#include <arm_sve.h>
#include <asm/hwcap.h>
#include <sys/auxv.h>

#define TARGET_SVE2 __attribute__((target("+sve2")))

#define SVE_SUM(a, b, c, d) svaddv_f32(svptrue_b32(), svadd_f32_x(svptrue_b32(), svadd_f32_x(svptrue_b32(), a, b), svadd_f32_x(svptrue_b32(), c, d)));
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
	int			count = (dim / 8) * 8;
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

	for (; i < dim; i++)
	{
		float		diff = HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]);

		distance += diff * diff;
	}

	return distance;
}
#elif defined(HALFVEC_SVE_DISPATCH)
TARGET_SVE2 static float
HalfvecL2SquaredDistanceSve2(int dim, half * ax, half * bx)
{
	int32_t		width = svcnth();
	int32_t		width2 = width * 2;
	int32_t		i;
	int32_t		count = (dim / width2) * width2;
	svfloat32_t dist0 = svdup_n_f32(0.0);
	svfloat32_t dist1 = svdup_n_f32(0.0);
	svfloat32_t dist2 = svdup_n_f32(0.0);
	svfloat32_t dist3 = svdup_n_f32(0.0);
	svfloat32_t zero = svdup_n_f32(0.0);
	svfloat16_t one = svdup_n_f16(1.0);

	for (i = 0; i < count; i += width2)
	{
		svfloat16_t axi0 = svld1_f16(svptrue_b16(), (float16_t *) (ax + i));
		svfloat16_t axi1 = svld1_f16(svptrue_b16(), (float16_t *) (ax + i + width));
		svfloat16_t bxi0 = svld1_f16(svptrue_b16(), (float16_t *) (bx + i));
		svfloat16_t bxi1 = svld1_f16(svptrue_b16(), (float16_t *) (bx + i + width));

		svfloat32_t diff0 = svmlslb_f32(svmlalb_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff1 = svmlslt_f32(svmlalt_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff2 = svmlslb_f32(svmlalb_f32(zero, axi1, one), bxi1, one);
		svfloat32_t diff3 = svmlslt_f32(svmlalt_f32(zero, axi1, one), bxi1, one);

		/* Use separate accumulators for performance */
		dist0 = svmla_f32_x(svptrue_b32(), dist0, diff0, diff0);
		dist1 = svmla_f32_x(svptrue_b32(), dist1, diff1, diff1);
		dist2 = svmla_f32_x(svptrue_b32(), dist2, diff2, diff2);
		dist3 = svmla_f32_x(svptrue_b32(), dist3, diff3, diff3);
	}

	for (; i < dim; i += width)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t axi0 = svld1_f16(pg, (float16_t *) (ax + i));
		svfloat16_t bxi0 = svld1_f16(pg, (float16_t *) (bx + i));

		svfloat32_t diff0 = svmlslb_f32(svmlalb_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff1 = svmlslt_f32(svmlalt_f32(zero, axi0, one), bxi0, one);

		/* Use separate accumulators for performance */
		dist0 = svmla_f32_x(svptrue_b32(), dist0, diff0, diff0);
		dist1 = svmla_f32_x(svptrue_b32(), dist1, diff1, diff1);
	}

	return SVE_SUM(dist0, dist1, dist2, dist3);
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
	int			count = (dim / 8) * 8;
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

	for (; i < dim; i++)
		distance += HalfToFloat4(ax[i]) * HalfToFloat4(bx[i]);

	return distance;
}
#elif defined(HALFVEC_SVE_DISPATCH)
TARGET_SVE2 static float
HalfvecInnerProductSve2(int dim, half * ax, half * bx)
{
	int32_t		width = svcnth();
	int32_t		width2 = width * 2;
	int32_t		i;
	int32_t		count = (dim / width2) * width2;
	svfloat32_t dist0 = svdup_n_f32(0.0);
	svfloat32_t dist1 = svdup_n_f32(0.0);
	svfloat32_t dist2 = svdup_n_f32(0.0);
	svfloat32_t dist3 = svdup_n_f32(0.0);

	for (i = 0; i < count; i += width2)
	{
		svfloat16_t axi0 = svld1_f16(svptrue_b16(), (float16_t *) (ax + i));
		svfloat16_t axi1 = svld1_f16(svptrue_b16(), (float16_t *) (ax + i + width));
		svfloat16_t bxi0 = svld1_f16(svptrue_b16(), (float16_t *) (bx + i));
		svfloat16_t bxi1 = svld1_f16(svptrue_b16(), (float16_t *) (bx + i + width));

		/* Use separate accumulators for performance */
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

		/* Use separate accumulators for performance */
		dist0 = svmlalb_f32(dist0, axi0, bxi0);
		dist1 = svmlalt_f32(dist1, axi0, bxi0);
	}

	return SVE_SUM(dist0, dist1, dist2, dist3);
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
	int			count = (dim / 8) * 8;
	__m256		sim = _mm256_setzero_ps();
	__m256		na = _mm256_setzero_ps();
	__m256		nb = _mm256_setzero_ps();

	for (i = 0; i < count; i += 8)
	{
		__m128i		axi = _mm_loadu_si128((__m128i *) (ax + i));
		__m128i		bxi = _mm_loadu_si128((__m128i *) (bx + i));
		__m256		axs = _mm256_cvtph_ps(axi);
		__m256		bxs = _mm256_cvtph_ps(bxi);

		sim = _mm256_fmadd_ps(axs, bxs, sim);
		na = _mm256_fmadd_ps(axs, axs, na);
		nb = _mm256_fmadd_ps(bxs, bxs, nb);
	}

	_mm256_storeu_ps(s, sim);
	similarity = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	_mm256_storeu_ps(s, na);
	norma = s[0] + s[1] + s[2] + s[3] + s[4] + s[5] + s[6] + s[7];

	_mm256_storeu_ps(s, nb);
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
#elif defined(HALFVEC_SVE_DISPATCH)
TARGET_SVE2 static double
HalfvecCosineSimilaritySve2(int dim, half * ax, half * bx)
{
	float		similarity;
	float		norma;
	float		normb;
	int32_t		width = svcnth();
	int32_t		width2 = width * 2;
	int32_t		i;
	int32_t		count = (dim / width2) * width2;
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

	for (i = 0; i < count; i += width2)
	{
		svfloat16_t axi0 = svld1_f16(svptrue_b16(), (float16_t *) (ax + i));
		svfloat16_t axi1 = svld1_f16(svptrue_b16(), (float16_t *) (ax + i + width));
		svfloat16_t bxi0 = svld1_f16(svptrue_b16(), (float16_t *) (bx + i));
		svfloat16_t bxi1 = svld1_f16(svptrue_b16(), (float16_t *) (bx + i + width));

		/* Use separate accumulators for performance */
		sim0 = svmlalb_f32(sim0, axi0, bxi0);
		sim1 = svmlalt_f32(sim1, axi0, bxi0);
		na0 = svmlalb_f32(na0, axi0, axi0);
		na1 = svmlalt_f32(na1, axi0, axi0);
		nb0 = svmlalb_f32(nb0, bxi0, bxi0);
		nb1 = svmlalt_f32(nb1, bxi0, bxi0);

		sim2 = svmlalb_f32(sim2, axi1, bxi1);
		sim3 = svmlalt_f32(sim3, axi1, bxi1);
		na2 = svmlalb_f32(na2, axi1, axi1);
		na3 = svmlalt_f32(na3, axi1, axi1);
		nb2 = svmlalb_f32(nb2, bxi1, bxi1);
		nb3 = svmlalt_f32(nb3, bxi1, bxi1);
	}

	for (; i < dim; i += width)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t axi0 = svld1_f16(pg, (float16_t *) (ax + i));
		svfloat16_t bxi0 = svld1_f16(pg, (float16_t *) (bx + i));

		/* Use separate accumulators for performance */
		sim0 = svmlalb_f32(sim0, axi0, bxi0);
		sim1 = svmlalt_f32(sim1, axi0, bxi0);
		na0 = svmlalb_f32(na0, axi0, axi0);
		na1 = svmlalt_f32(na1, axi0, axi0);
		nb0 = svmlalb_f32(nb0, bxi0, bxi0);
		nb1 = svmlalt_f32(nb1, bxi0, bxi0);
	}

	similarity = SVE_SUM(sim0, sim1, sim2, sim3);
	norma = SVE_SUM(na0, na1, na2, na3);
	normb = SVE_SUM(nb0, nb1, nb2, nb3);

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
#elif defined(HALFVEC_SVE_DISPATCH)
TARGET_SVE2 static float
HalfvecL1DistanceSve2(int dim, half * ax, half * bx)
{
	int32_t		width = svcnth();
	int32_t		width2 = width * 2;
	int32_t		i;
	int32_t		count = (dim / width2) * width2;
	svfloat32_t dist0 = svdup_n_f32(0.0);
	svfloat32_t dist1 = svdup_n_f32(0.0);
	svfloat32_t dist2 = svdup_n_f32(0.0);
	svfloat32_t dist3 = svdup_n_f32(0.0);
	svfloat32_t zero = svdup_n_f32(0.0);
	svfloat16_t one = svdup_n_f16(1.0);

	for (i = 0; i < count; i += width2)
	{
		svfloat16_t axi0 = svld1_f16(svptrue_b16(), (float16_t *) (ax + i));
		svfloat16_t axi1 = svld1_f16(svptrue_b16(), (float16_t *) (ax + i + width));
		svfloat16_t bxi0 = svld1_f16(svptrue_b16(), (float16_t *) (bx + i));
		svfloat16_t bxi1 = svld1_f16(svptrue_b16(), (float16_t *) (bx + i + width));

		svfloat32_t diff0 = svmlslb_f32(svmlalb_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff1 = svmlslt_f32(svmlalt_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff2 = svmlslb_f32(svmlalb_f32(zero, axi1, one), bxi1, one);
		svfloat32_t diff3 = svmlslt_f32(svmlalt_f32(zero, axi1, one), bxi1, one);

		/* Use separate accumulators for performance */
		dist0 = svadd_f32_x(svptrue_b32(), dist0, svabs_f32_x(svptrue_b32(), diff0));
		dist1 = svadd_f32_x(svptrue_b32(), dist1, svabs_f32_x(svptrue_b32(), diff1));
		dist2 = svadd_f32_x(svptrue_b32(), dist2, svabs_f32_x(svptrue_b32(), diff2));
		dist3 = svadd_f32_x(svptrue_b32(), dist3, svabs_f32_x(svptrue_b32(), diff3));
	}

	for (; i < dim; i += width)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t axi0 = svld1_f16(pg, (float16_t *) (ax + i));
		svfloat16_t bxi0 = svld1_f16(pg, (float16_t *) (bx + i));

		svfloat32_t diff0 = svmlslb_f32(svmlalb_f32(zero, axi0, one), bxi0, one);
		svfloat32_t diff1 = svmlslt_f32(svmlalt_f32(zero, axi0, one), bxi0, one);

		/* Use separate accumulators for performance */
		dist0 = svadd_f32_x(svptrue_b32(), dist0, svabs_f32_x(svptrue_b32(), diff0));
		dist1 = svadd_f32_x(svptrue_b32(), dist1, svabs_f32_x(svptrue_b32(), diff1));
	}

	return SVE_SUM(dist0, dist1, dist2, dist3);
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
#elif defined(HALFVEC_SVE_DISPATCH)
	if ((getauxval(AT_HWCAP2) & HWCAP2_SVE2) == HWCAP2_SVE2)
	{
		HalfvecL2SquaredDistance = HalfvecL2SquaredDistanceSve2;
		HalfvecInnerProduct = HalfvecInnerProductSve2;
		HalfvecCosineSimilarity = HalfvecCosineSimilaritySve2;
		HalfvecL1Distance = HalfvecL1DistanceSve2;
	}
#endif
}
