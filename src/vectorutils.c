#include "postgres.h"

#include "vectorutils.h"
#include "halfvec.h"			/* for USE_DISPATCH and USE_TARGET_CLONES */

#if defined(USE_DISPATCH)
#define VECTOR_DISPATCH
#endif

#ifdef VECTOR_DISPATCH
#include <immintrin.h>

#if defined(USE__GET_CPUID)
#include <cpuid.h>
#else
#include <intrin.h>
#endif

#ifdef _MSC_VER
#define TARGET_AVX512
#define TARGET_AVX512_GFNI
#else
#define TARGET_AVX512 __attribute__((target("avx512f")))
#define TARGET_AVX512_GFNI __attribute__((target("avx512f,avx512vl,gfni")))
#endif
#endif

void		(*BinaryQuantize) (int dim, float *ax, unsigned char *rx);

static void
BinaryQuantizeDefault(int dim, float *ax, unsigned char *rx) {
	int			i;
	int			count = (dim / 8) * 8;
	unsigned char result_byte;

	for (i = 0; i < count; i += 8)
	{
		result_byte = 0;
		for (int j = 0; j < 8; j++)
			result_byte |= (ax[i + j] > 0) << (7 - j);
		rx[i / 8] = result_byte;
	}
	for (; i < dim; i++)
		rx[i / 8] |= (ax[i] > 0) << (7 - (i % 8));
}

#ifdef VECTOR_DISPATCH
TARGET_AVX512 static inline void
BinaryQuantizeAvx512Compare(int dim, float *ax, unsigned char *rx) {
	int				rx_bytes = 0;
	unsigned long	mask;
	__m512			axi_512;
	__m512			zero_512 = _mm512_setzero_ps();
	__mmask16		cmp;

	for (int i = 0; i < dim; i += 16)
	{
		if (dim - i < 16)
		{
			mask = (1 << (dim - i)) - 1;
			axi_512 = _mm512_maskz_loadu_ps(mask, ax + i);
			cmp = _mm512_cmp_ps_mask(axi_512, zero_512, _CMP_GT_OQ);
			if (dim - i > 8)
				*((uint16_t*)(rx + rx_bytes)) = cmp;
			else {
				*((uint8_t*)(rx + rx_bytes)) = (uint8_t)(cmp & 0xFF);
			}
		}
		else
		{
			axi_512 = _mm512_loadu_ps(ax + i);
			cmp = _mm512_cmp_ps_mask(axi_512, zero_512, _CMP_GT_OQ);
			*((uint16_t*)(rx + rx_bytes)) = cmp;
			rx_bytes += 2;
		}
	}
}

static const uint8_t bit_invert_lookup[16] = {
	0x0, 0x8, 0x4, 0xC, 0x2, 0xA, 0x6, 0xE,
	0x1, 0x9, 0x5, 0xD, 0x3, 0xB, 0x7, 0xF
};

TARGET_AVX512 static void
BinaryQuantizeAvx512(int dim, float *ax, unsigned char *rx) {
	int	rx_bytes = 0;

	BinaryQuantizeAvx512Compare(dim, ax, rx);

	rx_bytes = dim / 8;
	if (dim % 8 > 0)
		rx_bytes++;
	for (int i = 0; i < rx_bytes; i++)
		rx[i] = (bit_invert_lookup[rx[i] & 0b1111] << 4) | bit_invert_lookup[rx[i] >> 4];
}

/* For GFNI instructions to invert bit order refer to
 * Galois Field New Instructions (GFNI) Technology Guide
 * https://builders.intel.com/docs/networkbuilders/galois-field-new-instructions-gfni-technology-guide-1-1639042826.pdf
 */
#define GFNI_REVBIT		0x8040201008040201

TARGET_AVX512_GFNI static void
BinaryQuantizeAvx512Gfni(int dim, float *ax, unsigned char *rx) {
	int			rx_bytes = 0;
	__m128i		revbit = _mm_set1_epi64x(GFNI_REVBIT);
	__m128i		rxi;
	__m128i		rxirev;
	int			count;
	int			i = 0;

	BinaryQuantizeAvx512Compare(dim, ax, rx);

	rx_bytes = dim / 8;
	if (dim % 8 > 0)
		rx_bytes++;
	count = (rx_bytes/16)*16;
	for (; i < count; i += 16)
	{
		rxi = _mm_loadu_epi64(rx + i);
		rxirev = _mm_gf2p8affine_epi64_epi8(rxi, revbit, 0);
		_mm_storeu_epi64(rx + i, rxirev);
	}
	for (; i < rx_bytes; i++)
		rx[i] =(bit_invert_lookup[rx[i] & 0b1111] << 4) | bit_invert_lookup[rx[i] >> 4];
}

#define CPU_FEATURE_OSXSAVE  (1 << 27)
#define CPU_FEATURE_AVX512F  (1 << 16)
#define CPU_FEATURE_AVX512VL (1 << 31)
#define CPU_FEATURE_GFNI     (1 << 8)

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

TARGET_XSAVE static bool
SupportsAvx512(unsigned int feature)
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

	return (exx[1] & feature) == feature;
}

TARGET_XSAVE static bool
SupportsGfni()
{
	unsigned int exx[4] = {0, 0, 0, 0};

#if defined(HAVE__GET_CPUID)
	__get_cpuid_count(7, 0, &exx[0], &exx[1], &exx[2], &exx[3]);
#elif defined(HAVE__CPUID)
	__cpuid(exx, 7, 0);
#endif

	return (exx[2] & CPU_FEATURE_GFNI) == CPU_FEATURE_GFNI;
}
#endif

void
VectorInit(void)
{
	BinaryQuantize = BinaryQuantizeDefault;

#ifdef VECTOR_DISPATCH
	if (SupportsAvx512(CPU_FEATURE_AVX512F | CPU_FEATURE_AVX512VL) && SupportsGfni())
		BinaryQuantize = BinaryQuantizeAvx512Gfni;
	else if (SupportsAvx512(CPU_FEATURE_AVX512F))
		BinaryQuantize = BinaryQuantizeAvx512;
#endif
}
