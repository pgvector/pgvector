#include "postgres.h"

#include "bitutils.h"
#include "halfvec.h"			/* for USE_DISPATCH and USE_TARGET_CLONES */
#include "port/pg_bitutils.h"

#if defined(USE_DISPATCH)
#define BIT_DISPATCH
#endif

#ifdef BIT_DISPATCH
#include <immintrin.h>

#if defined(USE__GET_CPUID)
#include <cpuid.h>
#else
#include <intrin.h>
#endif

#ifdef _MSC_VER
#define TARGET_AVX512_POPCOUNT
#else
#define TARGET_AVX512_POPCOUNT __attribute__((target("avx512f,avx512vpopcntdq")))
#endif
#endif

/* Disable for LLVM due to crash with bitcode generation */
#if defined(USE_TARGET_CLONES) && !defined(__POPCNT__) && !defined(__llvm__)
#define BIT_TARGET_CLONES __attribute__((target_clones("default", "popcnt")))
#else
#define BIT_TARGET_CLONES
#endif

/* Use built-ins when possible for inlining */
#if defined(HAVE__BUILTIN_POPCOUNT) && defined(HAVE_LONG_INT_64)
#define popcount64(x) __builtin_popcountl(x)
#elif defined(HAVE__BUILTIN_POPCOUNT) && defined(HAVE_LONG_LONG_INT_64)
#define popcount64(x) __builtin_popcountll(x)
#elif !defined(_MSC_VER)
/* Fails to resolve with MSVC */
#define popcount64(x) pg_popcount64(x)
#endif

uint64		(*BitHammingDistance) (uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 distance);
double		(*BitJaccardDistance) (uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 ab, uint64 aa, uint64 bb);

BIT_TARGET_CLONES static uint64
BitHammingDistanceDefault(uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 distance)
{
#ifdef popcount64
	for (; bytes >= sizeof(uint64); bytes -= sizeof(uint64))
	{
		uint64		axs;
		uint64		bxs;

		/* Ensure aligned */
		memcpy(&axs, ax, sizeof(uint64));
		memcpy(&bxs, bx, sizeof(uint64));

		distance += popcount64(axs ^ bxs);

		ax += sizeof(uint64);
		bx += sizeof(uint64);
	}
#endif

	for (uint32 i = 0; i < bytes; i++)
		distance += pg_number_of_ones[ax[i] ^ bx[i]];

	return distance;
}

#ifdef BIT_DISPATCH
TARGET_AVX512_POPCOUNT static uint64
BitHammingDistanceAvx512Popcount(uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 distance)
{
	__m512i		dist = _mm512_setzero_si512();

	for (; bytes >= sizeof(__m512i); bytes -= sizeof(__m512i))
	{
		__m512i		axs = _mm512_loadu_si512((const __m512i *) ax);
		__m512i		bxs = _mm512_loadu_si512((const __m512i *) bx);

		dist = _mm512_add_epi64(dist, _mm512_popcnt_epi64(_mm512_xor_si512(axs, bxs)));

		ax += sizeof(__m512i);
		bx += sizeof(__m512i);
	}

	distance += _mm512_reduce_add_epi64(dist);

	return BitHammingDistanceDefault(bytes, ax, bx, distance);
}
#endif

BIT_TARGET_CLONES static double
BitJaccardDistanceDefault(uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 ab, uint64 aa, uint64 bb)
{
#ifdef popcount64
	for (; bytes >= sizeof(uint64); bytes -= sizeof(uint64))
	{
		uint64		axs;
		uint64		bxs;

		/* Ensure aligned */
		memcpy(&axs, ax, sizeof(uint64));
		memcpy(&bxs, bx, sizeof(uint64));

		ab += popcount64(axs & bxs);
		aa += popcount64(axs);
		bb += popcount64(bxs);

		ax += sizeof(uint64);
		bx += sizeof(uint64);
	}
#endif

	for (uint32 i = 0; i < bytes; i++)
	{
		ab += pg_number_of_ones[ax[i] & bx[i]];
		aa += pg_number_of_ones[ax[i]];
		bb += pg_number_of_ones[bx[i]];
	}

	if (ab == 0)
		return 1;
	else
		return 1 - (ab / ((double) (aa + bb - ab)));
}

#ifdef BIT_DISPATCH
TARGET_AVX512_POPCOUNT static double
BitJaccardDistanceAvx512Popcount(uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 ab, uint64 aa, uint64 bb)
{
	__m512i		abx = _mm512_setzero_si512();
	__m512i		aax = _mm512_setzero_si512();
	__m512i		bbx = _mm512_setzero_si512();

	for (; bytes >= sizeof(__m512i); bytes -= sizeof(__m512i))
	{
		__m512i		axs = _mm512_loadu_si512((const __m512i *) ax);
		__m512i		bxs = _mm512_loadu_si512((const __m512i *) bx);

		abx = _mm512_add_epi64(abx, _mm512_popcnt_epi64(_mm512_and_si512(axs, bxs)));
		aax = _mm512_add_epi64(aax, _mm512_popcnt_epi64(axs));
		bbx = _mm512_add_epi64(bbx, _mm512_popcnt_epi64(bxs));

		ax += sizeof(__m512i);
		bx += sizeof(__m512i);
	}

	ab += _mm512_reduce_add_epi64(abx);
	aa += _mm512_reduce_add_epi64(aax);
	bb += _mm512_reduce_add_epi64(bbx);

	return BitJaccardDistanceDefault(bytes, ax, bx, ab, aa, bb);
}
#endif

#ifdef BIT_DISPATCH
#define CPU_FEATURE_OSXSAVE         (1 << 27)	/* F1 ECX */
#define CPU_FEATURE_AVX512F         (1 << 16)	/* F7,0 EBX */
#define CPU_FEATURE_AVX512VPOPCNTDQ (1 << 14)	/* F7,0 ECX */

#ifdef _MSC_VER
#define TARGET_XSAVE
#else
#define TARGET_XSAVE __attribute__((target("xsave")))
#endif

TARGET_XSAVE static bool
SupportsAvx512Popcount()
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

	/* Check AVX512VPOPCNTDQ */
	return (exx[2] & CPU_FEATURE_AVX512VPOPCNTDQ) == CPU_FEATURE_AVX512VPOPCNTDQ;
}
#endif

void
BitvecInit(void)
{
	/*
	 * Could skip pointer when single function, but no difference in
	 * performance
	 */
	BitHammingDistance = BitHammingDistanceDefault;
	BitJaccardDistance = BitJaccardDistanceDefault;

#ifdef BIT_DISPATCH
	if (SupportsAvx512Popcount())
	{
		BitHammingDistance = BitHammingDistanceAvx512Popcount;
		BitJaccardDistance = BitJaccardDistanceAvx512Popcount;
	}
#endif
}
