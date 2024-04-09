#include "postgres.h"

#include "halfutils.h"
#include "halfvec.h"
#include "utils/guc.h"

#ifdef HALFVEC_DISPATCH
#include <immintrin.h>

#if defined(USE__GET_CPUID)
#include <cpuid.h>
#else
#include <intrin.h>
#endif

#if (defined(__GNUC__) && (__GNUC__ >= 12)) || \
	(defined(__clang__) && (__clang_major__ >= 16)) || \
	(defined __AVX512FP16__)
#define HAVE_AVX512FP16
#endif

#ifdef _MSC_VER
#define TARGET_F16C
#define TARGET_AVX512FP16
#else
#define TARGET_F16C __attribute__((target("avx,f16c,fma")))
#define TARGET_AVX512FP16 __attribute__((target("avx512fp16,avx512f,avx512vl,avx512bw")))
#endif
#endif

bool halfvec_use_fp16_compute;

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

#ifdef HAVE_AVX512FP16
TARGET_AVX512FP16 static float
HalfvecL2SquaredDistanceAvx512Fp16(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	int			count = (dim / 32) * 32;
	unsigned long mask;
	__m512h		axi;
	__m512h		bxi;
	__m512h		diff;
	__m512h		dist = _mm512_setzero_ph();

	for (i = 0; i < count; i += 32)
	{
		axi = _mm512_loadu_ph(ax+i);
		bxi = _mm512_loadu_ph(bx+i);
		diff = _mm512_sub_ph(axi, bxi);
		dist = _mm512_fmadd_ph(diff, diff, dist);
	}

	mask = (1 << (dim - i)) - 1;
	axi = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, ax + i));
	bxi = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, bx + i));
	diff = _mm512_sub_ph(axi, bxi);
	dist = _mm512_fmadd_ph(diff, diff, dist);

	distance = (float)_mm512_reduce_add_ph(dist);

	return distance;
}

TARGET_AVX512FP16 static float
HalfvecL2SquaredDistanceAvx512Fp32(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	int			count = (dim / 16) * 16;
	unsigned long mask;
	__m256h		axi;
	__m256h		bxi;
	__m512		axs;
	__m512		bxs;
	__m512		diff;
	__m512		dist = _mm512_setzero_ps();

	for (i = 0; i < count; i += 16)
	{
		axi = _mm256_loadu_ph(ax+i);
		bxi = _mm256_loadu_ph(bx+i);
		axs = _mm512_cvtxph_ps(axi);
		bxs = _mm512_cvtxph_ps(bxi);
		diff = _mm512_sub_ps(axs, bxs);
		dist = _mm512_fmadd_ps(diff, diff, dist);
	}
	
	mask = (1 << (dim - i)) - 1;
	axi = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, ax + i));
	bxi = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, bx + i));
	axs = _mm512_cvtxph_ps(axi);
	bxs = _mm512_cvtxph_ps(bxi);
	diff = _mm512_sub_ps(axs, bxs);
	dist = _mm512_fmadd_ps(diff, diff, dist);

	distance = (float)_mm512_reduce_add_ps(dist);

	return distance;
}

static float
HalfvecL2SquaredDistanceAvx512(int dim, half * ax, half * bx)
{
	if (halfvec_use_fp16_compute)
		return HalfvecL2SquaredDistanceAvx512Fp16(dim, ax, bx);
	else
		return HalfvecL2SquaredDistanceAvx512Fp32(dim, ax, bx);
}
#endif
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

#ifdef HAVE_AVX512FP16
TARGET_AVX512FP16 static float
HalfvecInnerProductAvx512Fp16(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	int			count = (dim / 32) * 32;
	unsigned long mask;
	__m512h		axi;
	__m512h		bxi;
	__m512h		dist = _mm512_setzero_ph();

	for (i = 0; i < count; i += 32)
	{
		axi = _mm512_loadu_ph(ax+i);
		bxi = _mm512_loadu_ph(bx+i);
		dist = _mm512_fmadd_ph(axi, bxi, dist);
	}

	mask = (1 << (dim - i)) - 1;
	axi = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, ax + i));
	bxi = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, bx + i));
	dist = _mm512_fmadd_ph(axi, bxi, dist);

	distance = (float)_mm512_reduce_add_ph(dist);

	return distance;
}

TARGET_AVX512FP16 static float
HalfvecInnerProductAvx512Fp32(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	int			count = (dim / 16) * 16;
	unsigned long mask;
	__m256h		axi;
	__m256h		bxi;
	__m512		axs;
	__m512		bxs;
	__m512		dist = _mm512_setzero_ps();

	for (i = 0; i < count; i += 16)
	{
		axi = _mm256_loadu_ph(ax+i);
		bxi = _mm256_loadu_ph(bx+i);
		axs = _mm512_cvtxph_ps(axi);
		bxs = _mm512_cvtxph_ps(bxi);
		dist = _mm512_fmadd_ps(axs, bxs, dist);
	}

	mask = (1 << (dim - i)) - 1;
	axi = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, ax + i));
	bxi = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, bx + i));
	axs = _mm512_cvtxph_ps(axi);
	bxs = _mm512_cvtxph_ps(bxi);
	dist = _mm512_fmadd_ps(axs, bxs, dist);

	distance = (float)_mm512_reduce_add_ps(dist);

	return distance;
}

static float
HalfvecInnerProductAvx512(int dim, half * ax, half * bx)
{
	if (halfvec_use_fp16_compute)
		return HalfvecInnerProductAvx512Fp16(dim, ax, bx);
	else
		return HalfvecInnerProductAvx512Fp32(dim, ax, bx);
}
#endif
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

#ifdef HAVE_AVX512FP16
TARGET_AVX512FP16 static double
HalfvecCosineSimilarityAvx512Fp16(int dim, half * ax, half * bx)
{
	float		similarity;
	float		norma;
	float		normb;
	int			i;
	int			count = (dim / 32) * 32;
	unsigned long mask;
	__m512h		axi;
	__m512h		bxi;
	__m512h		sim = _mm512_setzero_ph();
	__m512h		na = _mm512_setzero_ph();
	__m512h		nb = _mm512_setzero_ph();

	for (i = 0; i < count; i += 32)
	{
		axi = _mm512_loadu_ph(ax+i);
		bxi = _mm512_loadu_ph(bx+i);
		sim = _mm512_fmadd_ph(axi, bxi, sim);
		na = _mm512_fmadd_ph(axi, axi, na);
		nb = _mm512_fmadd_ph(bxi, bxi, nb);
	}

	mask = (1 << (dim - i)) - 1;
	axi = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, ax + i));
	bxi = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, bx + i));
	sim = _mm512_fmadd_ph(axi, bxi, sim);
	na = _mm512_fmadd_ph(axi, axi, na);
	nb = _mm512_fmadd_ph(bxi, bxi, nb);

	similarity = (float)_mm512_reduce_add_ph(sim);
	norma = (float)_mm512_reduce_add_ph(na);
	normb = (float)_mm512_reduce_add_ph(nb);

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
}

TARGET_AVX512FP16 static double
HalfvecCosineSimilarityAvx512Fp32(int dim, half * ax, half * bx)
{
	float		similarity;
	float		norma;
	float		normb;
	int			i;
	int			count = (dim / 16) * 16;
	unsigned long mask;
	__m256h		axi;
	__m256h 	bxi;
	__m512		axs;
	__m512		bxs;
	__m512		sim = _mm512_setzero_ps();
	__m512		na = _mm512_setzero_ps();
	__m512		nb = _mm512_setzero_ps();

	for (i = 0; i < count; i += 16)
	{
		axi = _mm256_loadu_ph(ax+i);
		bxi = _mm256_loadu_ph(bx+i);
		axs = _mm512_cvtxph_ps(axi);
		bxs = _mm512_cvtxph_ps(bxi);
		sim = _mm512_fmadd_ps(axs, bxs, sim);
		na = _mm512_fmadd_ps(axs, axs, na);
		nb = _mm512_fmadd_ps(bxs, bxs, nb);
	}

	mask = (1 << (dim - i)) - 1;
	axi = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, ax + i));
	bxi = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, bx + i));
	axs = _mm512_cvtxph_ps(axi);
	bxs = _mm512_cvtxph_ps(bxi);
	sim = _mm512_fmadd_ps(axs, bxs, sim);
	na = _mm512_fmadd_ps(axs, axs, na);
	nb = _mm512_fmadd_ps(bxs, bxs, nb);

	similarity = (float)_mm512_reduce_add_ps(sim);
	norma = (float)_mm512_reduce_add_ps(na);
	normb = (float)_mm512_reduce_add_ps(nb);

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
}

static double
HalfvecCosineSimilarityAvx512(int dim, half * ax, half * bx)
{
	if (halfvec_use_fp16_compute)
		return HalfvecCosineSimilarityAvx512Fp16(dim, ax, bx);
	else
		return HalfvecCosineSimilarityAvx512Fp32(dim, ax, bx);
}
#endif
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

#ifdef HAVE_AVX512FP16
TARGET_AVX512FP16 static float
HalfvecL1DistanceAvx512Fp16(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	int			count = (dim / 32) * 32;
	unsigned long mask;
	__m512h		axi;
	__m512h		bxi;
	__m512h		dist = _mm512_setzero_ph();

	for (i = 0; i < count; i += 32)
	{
		axi = _mm512_loadu_ph(ax+i);
		bxi = _mm512_loadu_ph(bx+i);
		dist = _mm512_add_ph(dist, _mm512_abs_ph(_mm512_sub_ph(axi, bxi)));
	}

	mask = (1 << (dim - i)) - 1;
	axi = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, ax + i));
	bxi = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, bx + i));
	dist = _mm512_add_ph(dist, _mm512_abs_ph(_mm512_sub_ph(axi, bxi)));

	distance = (float)_mm512_reduce_add_ph(dist);

	return distance;
}

TARGET_AVX512FP16 static float
HalfvecL1DistanceAvx512Fp32(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	int			count = (dim / 16) * 16;
	unsigned long mask;
	__m256h		axi;
	__m256h		bxi;
	__m512		axs;
	__m512		bxs;
	__m512		dist = _mm512_setzero_ps();

	for (i = 0; i < count; i += 16)
	{
		axi = _mm256_loadu_ph(ax+i);
		bxi = _mm256_loadu_ph(bx+i);
		axs = _mm512_cvtxph_ps(axi);
		bxs = _mm512_cvtxph_ps(bxi);
		dist = _mm512_add_ps(dist, _mm512_abs_ps(_mm512_sub_ps(axs, bxs)));
	}

	mask = (1 << (dim - i)) - 1;
	axi = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, ax + i));
	bxi = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, bx + i));
	axs = _mm512_cvtxph_ps(axi);
	bxs = _mm512_cvtxph_ps(bxi);
	dist = _mm512_add_ps(dist, _mm512_abs_ps(_mm512_sub_ps(axs, bxs)));

	distance = (float)_mm512_reduce_add_ps(dist);

	return distance;
}

static float
HalfvecL1DistanceAvx512(int dim, half * ax, half * bx)
{
	if (halfvec_use_fp16_compute)
		return HalfvecL1DistanceAvx512Fp16(dim, ax, bx);
	else
		return HalfvecL1DistanceAvx512Fp32(dim, ax, bx);
}
#endif
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

#ifdef HAVE_AVX512FP16
TARGET_XSAVE static bool
SupportsOsXsave()
{
	unsigned int exx[4] = {0, 0, 0, 0};

#if defined(HAVE__GET_CPUID)
	__get_cpuid(1, &exx[0], &exx[1], &exx[2], &exx[3]);
#else
	__cpuid(exx, 1);
#endif

	return (exx[2] & CPU_FEATURE_OSXSAVE) == CPU_FEATURE_OSXSAVE;
}

#define CPU_FEATURE_AVX512F         (1 << 16)
#define CPU_FEATURE_AVX512_FP16     (1 << 23)
#define CPU_FEATURE_AVX512_BW       (1 << 30)
#define CPU_FEATURE_AVX512VL     	(1 << 31)

TARGET_XSAVE static bool
SupportsAvx512Fp16()
{
	unsigned int exx[4] = {0, 0, 0, 0};

	/* Check OS supports XSAVE */
	if (!SupportsOsXsave())
		return false;

	/* Check XMM, YMM, and ZMM registers are enabled */
	if ((_xgetbv(0) & 0xe6) != 0xe6)
		return false;
		
#if defined(HAVE__GET_CPUID)
	__get_cpuid_count(7, 0, &exx[0], &exx[1], &exx[2], &exx[3]);
#elif defined(HAVE__CPUID)
	__cpuid(exx, 7, 0);
#endif

	/* Required by AVX512 sub/fma/add instructions */
	if ((exx[1] & CPU_FEATURE_AVX512F) != CPU_FEATURE_AVX512F)
		return false;

	/* Required by _mm256_loadu_ph */
    if ((exx[1] & CPU_FEATURE_AVX512VL) != CPU_FEATURE_AVX512VL)
	    return false;

	/* Required by masked loads in remainder loops */
	if ((exx[1] & CPU_FEATURE_AVX512_BW) != CPU_FEATURE_AVX512_BW)
		return false;

	return (exx[3] & CPU_FEATURE_AVX512_FP16) == CPU_FEATURE_AVX512_FP16;
}
#endif
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

#ifdef HAVE_AVX512FP16
	if (SupportsAvx512Fp16())
	{
		HalfvecL2SquaredDistance = HalfvecL2SquaredDistanceAvx512;
		HalfvecInnerProduct = HalfvecInnerProductAvx512;
		HalfvecCosineSimilarity = HalfvecCosineSimilarityAvx512;
		HalfvecL1Distance = HalfvecL1DistanceAvx512;
	}
#endif
#endif

	DefineCustomBoolVariable("halfvec.use_fp16_compute", "Use FP16 computation for distance calculations",
							"If true, distance calculations are executed in FP16. If false, distance calculations are executed in FP32",
							&halfvec_use_fp16_compute, false, PGC_USERSET, 0, NULL, NULL, NULL);
}
