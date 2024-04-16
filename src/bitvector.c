#include "postgres.h"

#include "bitvector.h"
#include "port/pg_bitutils.h"
#include "utils/varbit.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

/* Use built-ins when possible for inlining */
#if defined(HAVE__BUILTIN_POPCOUNT) && defined(HAVE_LONG_INT_64)
#define popcount64(x) __builtin_popcountl(x)
#elif defined(HAVE__BUILTIN_POPCOUNT) && defined(HAVE_LONG_LONG_INT_64)
#define popcount64(x) __builtin_popcountll(x)
#elif defined(_MSC_VER)
#define popcount64(x) __popcnt64(x)
#else
#define popcount64(x) pg_popcount64(x)
#endif

/* target_clones requires glibc */
#if defined(__x86_64__) && defined(__gnu_linux__) && defined(__has_attribute) && __has_attribute(target_clones) && !defined(__POPCNT__)
#define BIT_DISPATCH __attribute__((target_clones("default", "popcnt")))
#else
#define BIT_DISPATCH
#endif

/*
 * Allocate and initialize a new bit vector
 */
VarBit *
InitBitVector(int dim)
{
	VarBit	   *result;
	int			size;

	size = VARBITTOTALLEN(dim);
	result = (VarBit *) palloc0(size);
	SET_VARSIZE(result, size);
	VARBITLEN(result) = dim;

	return result;
}

/*
 * Ensure same dimensions
 */
static inline void
CheckDims(VarBit *a, VarBit *b)
{
	if (VARBITLEN(a) != VARBITLEN(b))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different bit lengths %u and %u", VARBITLEN(a), VARBITLEN(b))));
}

BIT_DISPATCH static uint64
BitHammingDistance(uint32 bytes, unsigned char *ax, unsigned char *bx)
{
	uint64		distance = 0;
	uint32		i;
	uint32		count = (bytes / 8) * 8;

	for (i = 0; i < count; i += 8)
	{
		uint64		axs;
		uint64		bxs;

		memcpy(&axs, ax + i, sizeof(uint64));
		memcpy(&bxs, bx + i, sizeof(uint64));

		distance += popcount64(axs ^ bxs);
	}

	for (; i < bytes; i++)
		distance += pg_number_of_ones[ax[i] ^ bx[i]];

	return distance;
}

/*
 * Get the Hamming distance between two bit vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(hamming_distance);
Datum
hamming_distance(PG_FUNCTION_ARGS)
{
	VarBit	   *a = PG_GETARG_VARBIT_P(0);
	VarBit	   *b = PG_GETARG_VARBIT_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) BitHammingDistance(VARBITBYTES(a), VARBITS(a), VARBITS(b)));
}

BIT_DISPATCH static double
BitJaccardDistance(uint32 bytes, unsigned char *ax, unsigned char *bx)
{
	uint64		ab = 0;
	uint64		aa = 0;
	uint64		bb = 0;
	uint32		i;
	uint32		count = (bytes / 8) * 8;

	for (i = 0; i < count; i += 8)
	{
		uint64		axs;
		uint64		bxs;

		memcpy(&axs, ax + i, sizeof(uint64));
		memcpy(&bxs, bx + i, sizeof(uint64));

		ab += popcount64(axs & bxs);
		aa += popcount64(axs);
		bb += popcount64(bxs);
	}

	for (; i < bytes; i++)
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

/*
 * Get the Jaccard distance between two bit vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(jaccard_distance);
Datum
jaccard_distance(PG_FUNCTION_ARGS)
{
	VarBit	   *a = PG_GETARG_VARBIT_P(0);
	VarBit	   *b = PG_GETARG_VARBIT_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8(BitJaccardDistance(VARBITBYTES(a), VARBITS(a), VARBITS(b)));
}
