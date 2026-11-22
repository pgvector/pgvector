#include "postgres.h"

#include "bitutils.h"
#include "halfvec.h"			/* for USE_DISPATCH and USE_TARGET_CLONES */
#include "port/pg_bitutils.h"

#if defined(USE_DISPATCH) && (defined(__x86_64__) || defined(_M_AMD64))
#define BIT_DISPATCH
#elif defined(USE_DISPATCH) && defined(__aarch64__)
#define BIT_SVE_DISPATCH
#endif

#ifdef BIT_DISPATCH
#include <immintrin.h>

#if defined(USE__GET_CPUID)
#include <cpuid.h>
#else
#include <intrin.h>
#endif
#elif defined(BIT_SVE_DISPATCH)
#include <arm_sve.h>
#include <sys/auxv.h>
#endif

/* Disable for LLVM due to crash with bitcode generation */
#if defined(USE_TARGET_CLONES) && defined(__x86_64__) && !defined(__POPCNT__) && !defined(__llvm__)
#define BIT_TARGET_CLONES __attribute__((target_clones("default", "popcnt")))
#else
#define BIT_TARGET_CLONES
#endif

/* Use built-ins when possible for Postgres < 19 for inlining */
#if PG_VERSION_NUM >= 190000
#define popcount64(x) pg_popcount64(x)
#elif defined(HAVE__BUILTIN_POPCOUNT) && (defined(HAVE_LONG_INT_64) || SIZEOF_LONG == 8)
#define popcount64(x) __builtin_popcountl(x)
#elif defined(HAVE__BUILTIN_POPCOUNT) && (defined(HAVE_LONG_LONG_INT_64) || SIZEOF_LONG_LONG == 8)
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
#ifdef _MSC_VER
#define TARGET_AVX512_POPCOUNT
#else
#define TARGET_AVX512_POPCOUNT __attribute__((target("avx512f,avx512vpopcntdq")))
#endif

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

#define CPU_FEATURE_OSXSAVE         (1 << 27)	/* F1 ECX */
#define CPU_FEATURE_AVX512F         (1 << 16)	/* F7,0 EBX */
#define CPU_FEATURE_AVX512VPOPCNTDQ (1 << 14)	/* F7,0 ECX */

#ifdef _MSC_VER
#define TARGET_XSAVE
#else
#define TARGET_XSAVE __attribute__((target("xsave")))
#endif

TARGET_XSAVE static bool
SupportsAvx512Popcount(void)
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
#elif defined(BIT_SVE_DISPATCH)
#define TARGET_SVE __attribute__((target("+sve")))

TARGET_SVE static uint64
BitHammingDistanceSve(uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 distance)
{
	uint32		width = svcntb();
	uint32		width2 = width * 2;
	svbool_t	pt64 = svptrue_b64();
	svuint64_t	dist0 = svdup_n_u64(0);
	svuint64_t	dist1 = svdup_n_u64(0);

	for (; bytes >= width2; bytes -= width2)
	{
		svuint64_t	axs0 = svld1_u64(pt64, (const uint64_t *) ax);
		svuint64_t	axs1 = svld1_u64(pt64, (const uint64_t *) (ax + width));
		svuint64_t	bxs0 = svld1_u64(pt64, (const uint64_t *) bx);
		svuint64_t	bxs1 = svld1_u64(pt64, (const uint64_t *) (bx + width));

		dist0 = svadd_u64_x(pt64, dist0, svcnt_u64_x(pt64, sveor_u64_x(pt64, axs0, bxs0)));
		dist1 = svadd_u64_x(pt64, dist1, svcnt_u64_x(pt64, sveor_u64_x(pt64, axs1, bxs1)));

		ax += width2;
		bx += width2;
	}

	distance += svaddv_u64(pt64, svadd_u64_x(pt64, dist0, dist1));

	return BitHammingDistanceDefault(bytes, ax, bx, distance);
}

TARGET_SVE static double
BitJaccardDistanceSve(uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 ab, uint64 aa, uint64 bb)
{
	uint32		width = svcntb();
	uint32		width2 = width * 2;
	svbool_t	pt64 = svptrue_b64();
	svuint64_t	abx0 = svdup_n_u64(0);
	svuint64_t	abx1 = svdup_n_u64(0);
	svuint64_t	aax0 = svdup_n_u64(0);
	svuint64_t	aax1 = svdup_n_u64(0);
	svuint64_t	bbx0 = svdup_n_u64(0);
	svuint64_t	bbx1 = svdup_n_u64(0);

	for (; bytes >= width2; bytes -= width2)
	{
		svuint64_t	axs0 = svld1_u64(pt64, (const uint64_t *) ax);
		svuint64_t	axs1 = svld1_u64(pt64, (const uint64_t *) (ax + width));
		svuint64_t	bxs0 = svld1_u64(pt64, (const uint64_t *) bx);
		svuint64_t	bxs1 = svld1_u64(pt64, (const uint64_t *) (bx + width));

		abx0 = svadd_u64_x(pt64, abx0, svcnt_u64_x(pt64, svand_u64_x(pt64, axs0, bxs0)));
		abx1 = svadd_u64_x(pt64, abx1, svcnt_u64_x(pt64, svand_u64_x(pt64, axs1, bxs1)));
		aax0 = svadd_u64_x(pt64, aax0, svcnt_u64_x(pt64, axs0));
		aax1 = svadd_u64_x(pt64, aax1, svcnt_u64_x(pt64, axs1));
		bbx0 = svadd_u64_x(pt64, bbx0, svcnt_u64_x(pt64, bxs0));
		bbx1 = svadd_u64_x(pt64, bbx1, svcnt_u64_x(pt64, bxs1));

		ax += width2;
		bx += width2;
	}

	ab += svaddv_u64(pt64, svadd_u64_x(pt64, abx0, abx1));
	aa += svaddv_u64(pt64, svadd_u64_x(pt64, aax0, aax1));
	bb += svaddv_u64(pt64, svadd_u64_x(pt64, bbx0, bbx1));

	return BitJaccardDistanceDefault(bytes, ax, bx, ab, aa, bb);
}

static bool
SupportsSve(void)
{
	return (getauxval(AT_HWCAP) & HWCAP_SVE) == HWCAP_SVE;
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
#elif defined(BIT_SVE_DISPATCH)
	if (SupportsSve())
	{
		BitHammingDistance = BitHammingDistanceSve;
		BitJaccardDistance = BitJaccardDistanceSve;
	}
#endif
}
