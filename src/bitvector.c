#include "postgres.h"

#include "bitvector.h"
#include "port/pg_bitutils.h"
#include "utils/varbit.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
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
 * Ensure same number of bits
 */
static inline void
CheckBitLengths(uint32 aLen, uint32 bLen)
{
	if (aLen != bLen)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different bit lengths %u and %u", aLen, bLen)));
}

/*
 * Get the Hamming distance between two bit strings
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

	CheckBitLengths(VARBITLEN(a), VARBITLEN(b));

	/* TODO Improve performance */
	for (uint32 i = 0; i < VARBITBYTES(a); i++)
		distance += pg_number_of_ones[ax[i] ^ bx[i]];

	PG_RETURN_FLOAT8((double) distance);
}
