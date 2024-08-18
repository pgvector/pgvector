#include "postgres.h"

#include "bitutils.h"
#include "bitvec.h"
#include "utils/varbit.h"
#include "vector.h"

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
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(hamming_distance);
Datum
hamming_distance(PG_FUNCTION_ARGS)
{
	VarBit	   *a = PG_GETARG_VARBIT_P(0);
	VarBit	   *b = PG_GETARG_VARBIT_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) BitHammingDistance(VARBITBYTES(a), VARBITS(a), VARBITS(b), 0));
}

/*
 * Get the Jaccard distance between two bit vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(jaccard_distance);
Datum
jaccard_distance(PG_FUNCTION_ARGS)
{
	VarBit	   *a = PG_GETARG_VARBIT_P(0);
	VarBit	   *b = PG_GETARG_VARBIT_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8(BitJaccardDistance(VARBITBYTES(a), VARBITS(a), VARBITS(b), 0, 0, 0));
}
