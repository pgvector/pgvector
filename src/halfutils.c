#include "postgres.h"

#include "halfutils.h"
#include "halfvec.h"

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
#define TARGET_AVX512FP16 __attribute__((target("avx512fp16,avx512f,avx512dq,avx512vl,avx512bw")))
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

#ifdef HAVE_AVX512FP16
TARGET_AVX512FP16 static inline bool
HasInfinity(__m512h val) {
	/* Test for positive and negative infinity */
	__mmask32 mask = _mm512_fpclass_ph_mask(val, 0x08 + 0x10);
	return mask != 0;
}

TARGET_AVX512FP16 static inline __m512
ConvertToFp32Sum(__m512h val) {
	__m256h val_lower = _mm256_castsi256_ph(_mm512_extracti32x8_epi32(_mm512_castph_si512(val), 0));
	__m256h val_upper = _mm256_castsi256_ph(_mm512_extracti32x8_epi32(_mm512_castph_si512(val), 1));
	return _mm512_add_ps(_mm512_cvtxph_ps(val_lower), _mm512_cvtxph_ps(val_upper));
}

TARGET_AVX512FP16 static float
HalfvecL2SquaredDistanceAvx512(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	unsigned long mask;

	/* For FP16 computation */
	__m512h		axi_512h;
	__m512h		bxi_512h;
	__m512h		diff_512h;
	__m512h		dist_512h = _mm512_setzero_ph();
	__m512h		dist_512h_temp;

	/* For FP32 computation */
	__m256h		axi_256h;
	__m256h		bxi_256h;
	__m512		axi_512;
	__m512		bxi_512;
	__m512		diff_512;
	__m512		dist_512;

	/* FP16 computation */
	for (i = 0; i < dim; i += 32)
	{
		if (dim - i < 32)
		{
			mask = (1 << (dim - i)) - 1;
			axi_512h = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, ax + i));
			bxi_512h = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, bx + i));
		}
		else
		{
			axi_512h = _mm512_loadu_ph(ax + i);
			bxi_512h = _mm512_loadu_ph(bx + i);
		}
		diff_512h = _mm512_sub_ph(axi_512h, bxi_512h);
		dist_512h_temp = _mm512_fmadd_ph(diff_512h, diff_512h, dist_512h);

		/* if overflow, continue with FP32 */
		if (HasInfinity(dist_512h_temp))
			break;
		else
			dist_512h = dist_512h_temp;
	}
	dist_512 = ConvertToFp32Sum(dist_512h);

	/* FP32 computation */
	for (; i < dim; i += 16)
	{
		if (dim - i < 16)
		{
			mask = (1 << (dim - i)) - 1;
			axi_256h = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, ax + i));
			bxi_256h = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, bx + i));
		}
		else
		{
			axi_256h = _mm256_loadu_ph(ax + i);
			bxi_256h = _mm256_loadu_ph(bx + i);
		}
		axi_512 = _mm512_cvtxph_ps(axi_256h);
		bxi_512 = _mm512_cvtxph_ps(bxi_256h);
		diff_512 = _mm512_sub_ps(axi_512, bxi_512);
		dist_512 = _mm512_fmadd_ps(diff_512, diff_512, dist_512);
	}

	distance = _mm512_reduce_add_ps(dist_512);
	return distance;
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
HalfvecInnerProductAvx512(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	unsigned int mask;

	/* For FP16 computation */
	__m512h		axi_512h;
	__m512h		bxi_512h;
	__m512h		dist_512h = _mm512_setzero_ph();
	__m512h		dist_512h_temp;

	/* For FP32 computation */
	__m256h		axi_256h;
	__m256h		bxi_256h;
	__m512		axi_512;
	__m512		bxi_512;
	__m512		dist_512;

	/* FP16 computation */
	for (i = 0; i < dim; i += 32)
	{
		if (dim - i < 32)
		{
			mask = (1 << (dim - i)) - 1;
			axi_512h = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, ax + i));
			bxi_512h = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, bx + i));
		}
		else
		{
			axi_512h = _mm512_loadu_ph(ax + i);
			bxi_512h = _mm512_loadu_ph(bx + i);
		}
		dist_512h_temp = _mm512_fmadd_ph(axi_512h, bxi_512h, dist_512h);

		/* if overflow, continue with FP32 */
		if (HasInfinity(dist_512h_temp))
			break;
		else
			dist_512h = dist_512h_temp;
	}
	dist_512 = ConvertToFp32Sum(dist_512h);

	/* FP32 computation */
	for (; i < dim; i += 16)
	{
		if (dim - i < 16)
		{
			mask = (1 << (dim - i)) - 1;
			axi_256h = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, ax + i));
			bxi_256h = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, bx + i));
		}
		else
		{
			axi_256h = _mm256_loadu_ph(ax + i);
			bxi_256h = _mm256_loadu_ph(bx + i);
		}
		axi_512 = _mm512_cvtxph_ps(axi_256h);
		bxi_512 = _mm512_cvtxph_ps(bxi_256h);
		dist_512 = _mm512_fmadd_ps(axi_512, bxi_512, dist_512);
	}

	distance = _mm512_reduce_add_ps(dist_512);
	return distance;
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
HalfvecCosineSimilarityAvx512(int dim, half * ax, half * bx)
{
	float		similarity;
	float		norma;
	float		normb;
	int			i;
	unsigned int mask;

	/* For FP16 computation */
	__m512h		axi_512h;
	__m512h		bxi_512h;
	__m512h		sim_512h = _mm512_setzero_ph();
	__m512h		na_512h = _mm512_setzero_ph();
	__m512h		nb_512h = _mm512_setzero_ph();
	__m512h		sim_512h_temp;
	__m512h		na_512h_temp;
	__m512h		nb_512h_temp;

	/* For FP32 computation */
	__m256h		axi_256h;
	__m256h 	bxi_256h;
	__m512		axi_512;
	__m512		bxi_512;
	__m512		sim_512;
	__m512		na_512;
	__m512		nb_512;

	/* FP16 computation */
	for (i = 0; i < dim; i += 32)
	{
		if (dim - i < 32) {
			mask = (1 << (dim - i)) - 1;
			axi_512h = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, ax + i));
			bxi_512h = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, bx + i));
		}
		else {
			axi_512h = _mm512_loadu_ph(ax + i);
			bxi_512h = _mm512_loadu_ph(bx + i);
		}
		sim_512h_temp = _mm512_fmadd_ph(axi_512h, bxi_512h, sim_512h);
		na_512h_temp = _mm512_fmadd_ph(axi_512h, axi_512h, na_512h);
		nb_512h_temp = _mm512_fmadd_ph(bxi_512h, bxi_512h, nb_512h);

		/* if overflow, continue with FP32 */
		if (HasInfinity(sim_512h_temp) ||
			HasInfinity(na_512h_temp) ||
			HasInfinity(nb_512h_temp))
			break;
		else
		{
			sim_512h = sim_512h_temp;
			na_512h = na_512h_temp;
			nb_512h = nb_512h_temp;
		}
	}
	sim_512 = ConvertToFp32Sum(sim_512h);
	na_512 = ConvertToFp32Sum(na_512h);
	nb_512 = ConvertToFp32Sum(nb_512h);

	/* FP32 computation */
	for (; i < dim; i += 16)
	{
		if (dim - i < 16)
		{
			mask = (1 << (dim - i)) - 1;
			axi_256h = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, ax + i));
			bxi_256h = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, bx + i));
		}
		else
		{
			axi_256h = _mm256_loadu_ph(ax + i);
			bxi_256h = _mm256_loadu_ph(bx + i);
		}
		axi_512 = _mm512_cvtxph_ps(axi_256h);
		bxi_512 = _mm512_cvtxph_ps(bxi_256h);
		sim_512 = _mm512_fmadd_ps(axi_512, bxi_512, sim_512);
		na_512 = _mm512_fmadd_ps(axi_512, axi_512, na_512);
		nb_512 = _mm512_fmadd_ps(bxi_512, bxi_512, nb_512);
	}

	similarity = _mm512_reduce_add_ps(sim_512);
	norma = _mm512_reduce_add_ps(na_512);
	normb = _mm512_reduce_add_ps(nb_512);

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
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
HalfvecL1DistanceAvx512(int dim, half * ax, half * bx)
{
	float		distance;
	int			i;
	unsigned long mask;

	/* For FP16 computation */
	__m512h		axi_512h;
	__m512h		bxi_512h;
	__m512h		dist_512h = _mm512_setzero_ph();
	__m512h		dist_512h_temp;

	/* For FP32 computation */
	__m256h		axi_256h;
	__m256h		bxi_256h;
	__m512		axi_512;
	__m512		bxi_512;
	__m512		dist_512;

	/* FP16 computation */
	for (i = 0; i < dim; i += 32)
	{
		if (dim - i < 32)
		{
			mask = (1 << (dim - i)) - 1;
			axi_512h = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, ax + i));
			bxi_512h = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, bx + i));
		}
		else
		{
			axi_512h = _mm512_loadu_ph(ax + i);
			bxi_512h = _mm512_loadu_ph(bx + i);
		}
		dist_512h_temp = _mm512_add_ph(dist_512h, _mm512_abs_ph(_mm512_sub_ph(axi_512h, bxi_512h)));

		/* if overflow, continue with FP32 */
		if (HasInfinity(dist_512h_temp))
			break;
		else
			dist_512h = dist_512h_temp;
	}
	dist_512 = ConvertToFp32Sum(dist_512h);

	/* FP32 computation */
	for (; i < dim; i += 16)
	{
		if (dim - i < 16)
		{
			mask = (1 << (dim - i)) - 1;
			axi_256h = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, ax + i));
			bxi_256h = _mm256_castsi256_ph(_mm256_maskz_loadu_epi16(mask, bx + i));
		}
		else
		{
			axi_256h = _mm256_loadu_ph(ax + i);
			bxi_256h = _mm256_loadu_ph(bx + i);
		}
		axi_512 = _mm512_cvtxph_ps(axi_256h);
		bxi_512 = _mm512_cvtxph_ps(bxi_256h);
		dist_512 = _mm512_add_ps(dist_512, _mm512_abs_ps(_mm512_sub_ps(axi_512, bxi_512)));
	}

	distance = _mm512_reduce_add_ps(dist_512);

	return distance;
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

#define CPU_FEATURE_AVX512F     (1 << 16)
#define CPU_FEATURE_AVX512DQ    (1 << 17)
#define CPU_FEATURE_AVX512_FP16 (1 << 23)
#define CPU_FEATURE_AVX512BW    (1 << 30)
#define CPU_FEATURE_AVX512VL    (1 << 31)

TARGET_XSAVE static bool
SupportsAvx512Fp16()
{
	unsigned int exx[4] = {0, 0, 0, 0};

	/* AVX512 features required:
	 *  AVX512F : sub/fma/add instructions
	 *  AVX512DQ: _mm512_extracti32x8_epi32
	 *  AVX512VL: _mm256_loadu_ph
	 *  AVX512BW: masked loads
	 */
	unsigned int features = CPU_FEATURE_AVX512F |
	                        CPU_FEATURE_AVX512DQ |
	                        CPU_FEATURE_AVX512VL |
	                        CPU_FEATURE_AVX512BW;

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

	if ((exx[1] & features) != features)
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
}
