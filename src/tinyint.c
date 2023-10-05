#include "postgres.h"

#include <stdint.h>

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "tinyint.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/numeric.h"

/*
 * Check if array is a vector
 */
static bool
ArrayIsVector(ArrayType *a)
{
	return ARR_NDIM(a) == 1 && !array_contains_nulls(a);
}

/*
 * Check if dimensions are the same
 */
static int
CheckDims(ArrayType *a, ArrayType *b)
{
	int			dima;
	int			dimb;

	if (!ArrayIsVector(a) || !ArrayIsVector(b))
		return 0;

	dima = ARR_DIMS(a)[0];
	dimb = ARR_DIMS(b)[0];

	if (dima != dimb)
		return 0;

	return dima;
}

/*
 * Check range
 */
static void
CheckRange(long i)
{
	if (i < INT8_MIN || i > INT8_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%ld\" is out of range for type tinyint", i)));
}

/*
 * Convert textual representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(tinyint_in);
Datum
tinyint_in(PG_FUNCTION_ARGS)
{
	char	   *s = PG_GETARG_CSTRING(0);
	const char *ptr = s;
	long		i;
	char	   *end;

	/* skip leading spaces */
	while (*ptr != '\0' && isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type tinyint: \"%s\"", s)));

	i = strtol(ptr, &end, 10);
	ptr = end;

	if (i < INT8_MIN || i > INT8_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%s\" is out of range for type tinyint", s)));

	/* allow trailing whitespace, but not other trailing chars */
	while (*ptr != '\0' && isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type tinyint: \"%s\"", s)));

	PG_RETURN_INT8(i);
}

/*
 * Convert internal representation to textual representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(tinyint_out);
Datum
tinyint_out(PG_FUNCTION_ARGS)
{
	int8		num = PG_GETARG_INT8(0);
	char	   *result = (char *) palloc(5);	/* sign, 3 digits, '\0' */

	pg_ltoa((int32) num, result);
	PG_RETURN_CSTRING(result);
}

/*
 * Convert external binary representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(tinyint_recv);
Datum
tinyint_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_INT8((int8) pq_getmsgint(buf, sizeof(int8)));
}

/*
 * Convert internal representation to the external binary representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(tinyint_send);
Datum
tinyint_send(PG_FUNCTION_ARGS)
{
	int8		arg1 = PG_GETARG_INT8(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint8(&buf, arg1);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert integer to tinyint
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(integer_to_tinyint);
Datum
integer_to_tinyint(PG_FUNCTION_ARGS)
{
	int32		i = PG_GETARG_INT32(0);

	CheckRange(i);

	PG_RETURN_INT8(i);
}

/*
 * Convert numeric to tinyint
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(numeric_to_tinyint);
Datum
numeric_to_tinyint(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	int32		i = numeric_int4_opt_error(num, NULL);

	CheckRange(i);

	PG_RETURN_INT8(i);
}

/*
 * Get the L2 distance between tinyint arrays
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(tinyint_l2_distance);
Datum
tinyint_l2_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int8    *ax = (int8 *) ARR_DATA_PTR(a);
	int8    *bx = (int8 *) ARR_DATA_PTR(b);
	double		distance = 0.0;
	int			dim = CheckDims(a, b);

	/* TODO Decide on error or NULL */
	if (!dim)
		PG_RETURN_NULL();

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		double		diff = ax[i] - bx[i];

		distance += diff * diff;
	}

	PG_RETURN_FLOAT8(sqrt(distance));
}

/*
 * Get the inner product of two tinyint arrays
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(tinyint_inner_product);
Datum
tinyint_inner_product(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int8	   *ax = (int8 *) ARR_DATA_PTR(a);
	int8	   *bx = (int8 *) ARR_DATA_PTR(b);
	double		distance = 0.0;
	int			dim = CheckDims(a, b);

	/* TODO Decide on error or NULL */
	if (!dim)
		PG_RETURN_NULL();

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += ax[i] * bx[i];

	PG_RETURN_FLOAT8(distance);
}

/*
 * Get the negative inner product of two tinyint arrays
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(tinyint_negative_inner_product);
Datum
tinyint_negative_inner_product(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int8	   *ax = (int8 *) ARR_DATA_PTR(a);
	int8	   *bx = (int8 *) ARR_DATA_PTR(b);
	double		distance = 0.0;
	int			dim = CheckDims(a, b);

	/* TODO Decide on error or NULL */
	if (!dim)
		PG_RETURN_NULL();

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += ax[i] * bx[i];

	PG_RETURN_FLOAT8(distance * -1);
}

/*
 * Get the cosine distance between two float2 arrays
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(tinyint_cosine_distance);
Datum
tinyint_cosine_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int8   *ax = (int8 *) ARR_DATA_PTR(a);
	int8   *bx = (int8 *) ARR_DATA_PTR(b);
	double		distance = 0.0;
	double		norma = 0.0;
	double		normb = 0.0;
	double		similarity;
	int			dim = CheckDims(a, b);

	/* TODO Decide on error or NULL */
	if (!dim)
		PG_RETURN_NULL();

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		float		axi = ax[i];
		float		bxi = bx[i];

		distance += axi * bxi;
		norma += axi * axi;
		normb += bxi * bxi;
	}

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	similarity = distance / sqrt(norma * normb);

#ifdef _MSC_VER
	/* /fp:fast may not propagate NaN */
	if (isnan(similarity))
		PG_RETURN_FLOAT8(NAN);
#endif

	/* Keep in range */
	if (similarity > 1)
		similarity = 1;
	else if (similarity < -1)
		similarity = -1;

	PG_RETURN_FLOAT8(1 - similarity);
}
