#ifdef USE_AVX512
#include "halfutils_avx512.h"

#ifdef HAVE_AVX512FP16
#include "common/shortest_dec.h"

#include <immintrin.h>
#include <math.h>

#if defined(USE__GET_CPUID)
#include <cpuid.h>
#else
#include <intrin.h>
#endif

#ifdef _MSC_VER
#define TARGET_AVX512FP16
#else
#define TARGET_AVX512FP16 __attribute__((target("avx512fp16,avx512f,avx512dq,avx512vl,avx512bw")))
#endif

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

TARGET_AVX512FP16 float
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

TARGET_AVX512FP16 float
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

TARGET_AVX512FP16 double
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

TARGET_AVX512FP16 float
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

TARGET_AVX512FP16 void
Float4ToHalfVectorAvx512(Vector * vec, HalfVector * result)
{
	unsigned long mask;
	__m512		vec_512;
	__m256h		vec_256h;
	__mmask16 	vec_512_inf;
	__mmask16 	vec_256h_inf;

	for (int i = 0; i < vec->dim; i += 16)
	{
		if (vec->dim - i < 16)
		{
			mask = (1 << (vec->dim - i)) - 1;
			vec_512 = _mm512_maskz_loadu_ps(mask, vec->x + i);
			vec_256h = _mm512_cvtxps_ph(vec_512);
			_mm256_mask_storeu_epi16(result->x + i, mask, _mm256_castph_si256(vec_256h));
		}
		else
		{
			vec_512 = _mm512_loadu_ps(vec->x + i);
			vec_256h = _mm512_cvtxps_ph(vec_512);
			_mm256_storeu_ph(result->x + i, vec_256h);
		}

		/* Test for positive and negative infinity */
		vec_512_inf = _mm512_fpclass_ps_mask(vec_512, 0x08 + 0x10);
		vec_256h_inf = _mm256_fpclass_ph_mask(vec_256h, 0x08 + 0x10);
		if (unlikely(vec_512_inf != vec_256h_inf))
		{
			float num;
			char* buf;

			__mmask16 diff = _kxor_mask16(vec_512_inf, vec_256h_inf);
			/* Find first element in vector to overflow after conversion (first bit set) */
			int count = 0;
			while (diff % 2 == 0) {
				diff >>= 1;
				count++;
			}
			num = vec->x[i + count];

			/* TODO Avoid duplicate code in Float4ToHalf */
			buf = palloc(FLOAT_SHORTEST_DECIMAL_LEN);

			float_to_shortest_decimal_buf(num, buf);

			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("\"%s\" is out of range for type halfvec", buf)));
		}
	}
}

#define CPU_FEATURE_OSXSAVE     (1 << 27)
#define CPU_FEATURE_AVX512F     (1 << 16)
#define CPU_FEATURE_AVX512DQ    (1 << 17)
#define CPU_FEATURE_AVX512_FP16 (1 << 23)
#define CPU_FEATURE_AVX512BW    (1 << 30)
#define CPU_FEATURE_AVX512VL    (1 << 31)

#ifdef _MSC_VER
#define TARGET_XSAVE
#else
#define TARGET_XSAVE __attribute__((target("xsave")))
#endif

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

TARGET_XSAVE bool
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
