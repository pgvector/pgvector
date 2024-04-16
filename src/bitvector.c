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

/*
 * Get the Hamming distance between two bit vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(hamming_distance);
Datum
hamming_distance(PG_FUNCTION_ARGS)
{
	VarBit	   *a = PG_GETARG_VARBIT_P(0);
	VarBit	   *b = PG_GETARG_VARBIT_P(1);
	unsigned char *ax = VARBITS(a);
	unsigned char *bx = VARBITS(b);
	uint64		distance = 0;
	uint32		i;
	uint32		count = (VARBITBYTES(a) / 8) * 8;

	CheckDims(a, b);

	for (i = 0; i < count; i += 8)
	{
		uint64		axs;
		uint64		bxs;

		memcpy(&axs, ax + i, sizeof(uint64));
		memcpy(&bxs, bx + i, sizeof(uint64));

		distance += popcount64(axs ^ bxs);
	}

	for (; i < VARBITBYTES(a); i++)
		distance += pg_number_of_ones[ax[i] ^ bx[i]];

	PG_RETURN_FLOAT8((double) distance);
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
	unsigned char *ax = VARBITS(a);
	unsigned char *bx = VARBITS(b);
	uint64		ab = 0;
	uint64		aa;
	uint64		bb;
	uint32		i;
	uint32		count = (VARBITBYTES(a) / 8) * 8;

	CheckDims(a, b);

	for (i = 0; i < count; i += 8)
	{
		uint64		axs;
		uint64		bxs;

		memcpy(&axs, ax + i, sizeof(uint64));
		memcpy(&bxs, bx + i, sizeof(uint64));

		ab += popcount64(axs & bxs);
	}

	for (; i < VARBITBYTES(a); i++)
		ab += pg_number_of_ones[ax[i] & bx[i]];

	if (ab == 0)
		PG_RETURN_FLOAT8(1);

	aa = pg_popcount((char *) ax, VARBITBYTES(a));
	bb = pg_popcount((char *) bx, VARBITBYTES(b));

	PG_RETURN_FLOAT8(1 - (ab / ((double) (aa + bb - ab))));
}
