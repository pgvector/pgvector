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
#endif

#ifdef USE_SVE
#include <arm_sve.h>

#define SVE_HSUM4(pg, a0, a1, a2, a3) \
	svaddv_f32((pg), svadd_f32_x((pg), svadd_f32_x((pg), (a0), (a1)), svadd_f32_x((pg), (a2), (a3))))

#define SVE2_MIN_DIM 32

#define SVE2_DIFF_BOT(z, one, a, b) svmlslb_f32(svmlalb_f32((z), (a), (one)), (b), (one))
#define SVE2_DIFF_TOP(z, one, a, b) svmlslt_f32(svmlalt_f32((z), (a), (one)), (b), (one))

TARGET_SVE2 static float
HalfvecL2SquaredDistanceSve2(int dim, half * ax, half * bx)
{
	const float16_t *a = (const float16_t *) ax;
	const float16_t *b = (const float16_t *) bx;
	const svbool_t all16 = svptrue_b16();
	const svbool_t all32 = svptrue_b32();
	const svfloat16_t one = svdup_n_f16(1.0);
	const svfloat32_t z = svdup_n_f32(0.0f);
	const int	vl = (int) svcnth();
	int			i = 0;
	svfloat32_t acc0 = z;
	svfloat32_t acc1 = z;
	svfloat32_t acc2 = z;
	svfloat32_t acc3 = z;

	if (dim < SVE2_MIN_DIM)
		return HalfvecL2SquaredDistanceDefault(dim, ax, bx);

	for (; i + 2 * vl <= dim; i += 2 * vl)
	{
		svfloat16_t a0 = svld1_f16(all16, a + i);
		svfloat16_t b0 = svld1_f16(all16, b + i);
		svfloat16_t a1 = svld1_f16(all16, a + i + vl);
		svfloat16_t b1 = svld1_f16(all16, b + i + vl);
		svfloat32_t d0 = SVE2_DIFF_BOT(z, one, a0, b0);
		svfloat32_t d1 = SVE2_DIFF_TOP(z, one, a0, b0);
		svfloat32_t d2 = SVE2_DIFF_BOT(z, one, a1, b1);
		svfloat32_t d3 = SVE2_DIFF_TOP(z, one, a1, b1);

		acc0 = svmla_f32_x(all32, acc0, d0, d0);
		acc1 = svmla_f32_x(all32, acc1, d1, d1);
		acc2 = svmla_f32_x(all32, acc2, d2, d2);
		acc3 = svmla_f32_x(all32, acc3, d3, d3);
	}

	for (; i < dim; i += vl)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t a0 = svld1_f16(pg, a + i);
		svfloat16_t b0 = svld1_f16(pg, b + i);
		svfloat32_t d0 = SVE2_DIFF_BOT(z, one, a0, b0);
		svfloat32_t d1 = SVE2_DIFF_TOP(z, one, a0, b0);

		acc0 = svmla_f32_x(all32, acc0, d0, d0);
		acc1 = svmla_f32_x(all32, acc1, d1, d1);
	}

	return SVE_HSUM4(all32, acc0, acc1, acc2, acc3);
}

TARGET_SVE2 static float
HalfvecInnerProductSve2(int dim, half * ax, half * bx)
{
	const float16_t *a = (const float16_t *) ax;
	const float16_t *b = (const float16_t *) bx;
	const svbool_t all16 = svptrue_b16();
	const svbool_t all32 = svptrue_b32();
	const int	vl = (int) svcnth();
	int			i = 0;
	svfloat32_t z = svdup_n_f32(0.0f);
	svfloat32_t acc0 = z, acc1 = z, acc2 = z, acc3 = z;
	svfloat32_t acc4 = z, acc5 = z, acc6 = z, acc7 = z;

	if (dim < SVE2_MIN_DIM)
		return HalfvecInnerProductDefault(dim, ax, bx);

	for (; i + 4 * vl <= dim; i += 4 * vl)
	{
		svfloat16_t a0 = svld1_f16(all16, a + i);
		svfloat16_t b0 = svld1_f16(all16, b + i);
		svfloat16_t a1 = svld1_f16(all16, a + i + vl);
		svfloat16_t b1 = svld1_f16(all16, b + i + vl);
		svfloat16_t a2 = svld1_f16(all16, a + i + 2 * vl);
		svfloat16_t b2 = svld1_f16(all16, b + i + 2 * vl);
		svfloat16_t a3 = svld1_f16(all16, a + i + 3 * vl);
		svfloat16_t b3 = svld1_f16(all16, b + i + 3 * vl);

		acc0 = svmlalb_f32(acc0, a0, b0);
		acc1 = svmlalt_f32(acc1, a0, b0);
		acc2 = svmlalb_f32(acc2, a1, b1);
		acc3 = svmlalt_f32(acc3, a1, b1);
		acc4 = svmlalb_f32(acc4, a2, b2);
		acc5 = svmlalt_f32(acc5, a2, b2);
		acc6 = svmlalb_f32(acc6, a3, b3);
		acc7 = svmlalt_f32(acc7, a3, b3);
	}

	for (; i < dim; i += vl)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t a0 = svld1_f16(pg, a + i);
		svfloat16_t b0 = svld1_f16(pg, b + i);

		acc0 = svmlalb_f32(acc0, a0, b0);
		acc1 = svmlalt_f32(acc1, a0, b0);
	}

	acc0 = svadd_f32_x(all32, acc0, acc4);
	acc1 = svadd_f32_x(all32, acc1, acc5);
	acc2 = svadd_f32_x(all32, acc2, acc6);
	acc3 = svadd_f32_x(all32, acc3, acc7);
	return SVE_HSUM4(all32, acc0, acc1, acc2, acc3);
}

TARGET_SVE2 static double
HalfvecCosineSimilaritySve2(int dim, half * ax, half * bx)
{
	const float16_t *a = (const float16_t *) ax;
	const float16_t *b = (const float16_t *) bx;
	const svbool_t all16 = svptrue_b16();
	const svbool_t all32 = svptrue_b32();
	const int	vl = (int) svcnth();
	int			i = 0;
	svfloat32_t zero = svdup_n_f32(0.0f);
	svfloat32_t s0 = zero, s1 = zero, s2 = zero, s3 = zero;
	svfloat32_t na0 = zero, na1 = zero, na2 = zero, na3 = zero;
	svfloat32_t nb0 = zero, nb1 = zero, nb2 = zero, nb3 = zero;
	float		similarity;
	float		norma;
	float		normb;

	if (dim < SVE2_MIN_DIM)
		return HalfvecCosineSimilarityDefault(dim, ax, bx);

	for (; i + 2 * vl <= dim; i += 2 * vl)
	{
		svfloat16_t a0 = svld1_f16(all16, a + i);
		svfloat16_t b0 = svld1_f16(all16, b + i);
		svfloat16_t a1 = svld1_f16(all16, a + i + vl);
		svfloat16_t b1 = svld1_f16(all16, b + i + vl);

		s0 = svmlalb_f32(s0, a0, b0);
		s1 = svmlalt_f32(s1, a0, b0);
		s2 = svmlalb_f32(s2, a1, b1);
		s3 = svmlalt_f32(s3, a1, b1);
		na0 = svmlalb_f32(na0, a0, a0);
		na1 = svmlalt_f32(na1, a0, a0);
		na2 = svmlalb_f32(na2, a1, a1);
		na3 = svmlalt_f32(na3, a1, a1);
		nb0 = svmlalb_f32(nb0, b0, b0);
		nb1 = svmlalt_f32(nb1, b0, b0);
		nb2 = svmlalb_f32(nb2, b1, b1);
		nb3 = svmlalt_f32(nb3, b1, b1);
	}

	for (; i < dim; i += vl)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t a0 = svld1_f16(pg, a + i);
		svfloat16_t b0 = svld1_f16(pg, b + i);

		s0 = svmlalb_f32(s0, a0, b0);
		s1 = svmlalt_f32(s1, a0, b0);
		na0 = svmlalb_f32(na0, a0, a0);
		na1 = svmlalt_f32(na1, a0, a0);
		nb0 = svmlalb_f32(nb0, b0, b0);
		nb1 = svmlalt_f32(nb1, b0, b0);
	}

	similarity = SVE_HSUM4(all32, s0, s1, s2, s3);
	norma = SVE_HSUM4(all32, na0, na1, na2, na3);
	normb = SVE_HSUM4(all32, nb0, nb1, nb2, nb3);

	return (double) similarity / sqrt((double) norma * (double) normb);
}

TARGET_SVE2 static float
HalfvecL1DistanceSve2(int dim, half * ax, half * bx)
{
	const float16_t *a = (const float16_t *) ax;
	const float16_t *b = (const float16_t *) bx;
	const svbool_t all16 = svptrue_b16();
	const svbool_t all32 = svptrue_b32();
	const svfloat16_t one = svdup_n_f16(1.0);
	const svfloat32_t z = svdup_n_f32(0.0f);
	const int	vl = (int) svcnth();
	int			i = 0;
	svfloat32_t acc0 = z;
	svfloat32_t acc1 = z;
	svfloat32_t acc2 = z;
	svfloat32_t acc3 = z;

	if (dim < SVE2_MIN_DIM)
		return HalfvecL1DistanceDefault(dim, ax, bx);

	for (; i + 2 * vl <= dim; i += 2 * vl)
	{
		svfloat16_t a0 = svld1_f16(all16, a + i);
		svfloat16_t b0 = svld1_f16(all16, b + i);
		svfloat16_t a1 = svld1_f16(all16, a + i + vl);
		svfloat16_t b1 = svld1_f16(all16, b + i + vl);

		acc0 = svadd_f32_x(all32, acc0, svabs_f32_x(all32, SVE2_DIFF_BOT(z, one, a0, b0)));
		acc1 = svadd_f32_x(all32, acc1, svabs_f32_x(all32, SVE2_DIFF_TOP(z, one, a0, b0)));
		acc2 = svadd_f32_x(all32, acc2, svabs_f32_x(all32, SVE2_DIFF_BOT(z, one, a1, b1)));
		acc3 = svadd_f32_x(all32, acc3, svabs_f32_x(all32, SVE2_DIFF_TOP(z, one, a1, b1)));
	}

	for (; i < dim; i += vl)
	{
		svbool_t	pg = svwhilelt_b16_s32(i, dim);
		svfloat16_t a0 = svld1_f16(pg, a + i);
		svfloat16_t b0 = svld1_f16(pg, b + i);

		acc0 = svadd_f32_x(all32, acc0, svabs_f32_x(all32, SVE2_DIFF_BOT(z, one, a0, b0)));
		acc1 = svadd_f32_x(all32, acc1, svabs_f32_x(all32, SVE2_DIFF_TOP(z, one, a0, b0)));
	}

	return SVE_HSUM4(all32, acc0, acc1, acc2, acc3);
}
#endif							/* USE_SVE */

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

#ifdef USE_SVE
	if (SVE2_RUNTIME_SUPPORTED())
	{
		HalfvecL2SquaredDistance = HalfvecL2SquaredDistanceSve2;
		HalfvecInnerProduct = HalfvecInnerProductSve2;
		HalfvecCosineSimilarity = HalfvecCosineSimilaritySve2;
		HalfvecL1Distance = HalfvecL1DistanceSve2;
	}
#endif
}