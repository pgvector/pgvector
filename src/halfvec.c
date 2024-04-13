#include "postgres.h"

#include <math.h>

#include "bitvector.h"
#include "catalog/pg_type.h"
#include "common/shortest_dec.h"
#include "fmgr.h"
#include "halfutils.h"
#include "halfvec.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "port.h"				/* for strtof() */
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "vector.h"

#if PG_VERSION_NUM < 130000
#define TYPALIGN_DOUBLE 'd'
#define TYPALIGN_INT 'i'
#endif

/*
 * Get a half from a message buffer
 */
static half
pq_getmsghalf(StringInfo msg)
{
	union
	{
		half		h;
		uint16		i;
	}			swap;

	swap.i = pq_getmsgint(msg, 2);
	return swap.h;
}

/*
 * Append a half to a StringInfo buffer
 */
static void
pq_sendhalf(StringInfo buf, half h)
{
	union
	{
		half		h;
		uint16		i;
	}			swap;

	swap.h = h;
	pq_sendint16(buf, swap.i);
}

/*
 * Ensure same dimensions
 */
static inline void
CheckDims(HalfVector * a, HalfVector * b)
{
	if (a->dim != b->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different halfvec dimensions %d and %d", a->dim, b->dim)));
}

/*
 * Ensure expected dimensions
 */
static inline void
CheckExpectedDim(int32 typmod, int dim)
{
	if (typmod != -1 && typmod != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("expected %d dimensions, not %d", typmod, dim)));
}

/*
 * Ensure valid dimensions
 */
static inline void
CheckDim(int dim)
{
	if (dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("halfvec must have at least 1 dimension")));

	if (dim > HALFVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("halfvec cannot have more than %d dimensions", HALFVEC_MAX_DIM)));
}

/*
 * Ensure finite element
 */
static inline void
CheckElement(half value)
{
	if (HalfIsNan(value))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("NaN not allowed in halfvec")));

	if (HalfIsInf(value))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("infinite value not allowed in halfvec")));
}

/*
 * Allocate and initialize a new half vector
 */
HalfVector *
InitHalfVector(int dim)
{
	HalfVector *result;
	int			size;

	size = HALFVEC_SIZE(dim);
	result = (HalfVector *) palloc0(size);
	SET_VARSIZE(result, size);
	result->dim = dim;

	return result;
}

/*
 * Check for whitespace, since array_isspace() is static
 */
static inline bool
halfvec_isspace(char ch)
{
	if (ch == ' ' ||
		ch == '\t' ||
		ch == '\n' ||
		ch == '\r' ||
		ch == '\v' ||
		ch == '\f')
		return true;
	return false;
}

/*
 * Convert textual representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_in);
Datum
halfvec_in(PG_FUNCTION_ARGS)
{
	char	   *lit = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	half		x[HALFVEC_MAX_DIM];
	int			dim = 0;
	char	   *pt;
	char	   *stringEnd;
	HalfVector *result;
	char	   *litcopy = pstrdup(lit);
	char	   *str = litcopy;

	while (halfvec_isspace(*str))
		str++;

	if (*str != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed halfvec literal: \"%s\"", lit),
				 errdetail("Vector contents must start with \"[\".")));

	str++;

	while (halfvec_isspace(*str))
		str++;

	pt = strtok(str, ",");
	stringEnd = pt;

	while (pt != NULL && *stringEnd != ']')
	{
		float		val;

		if (dim == HALFVEC_MAX_DIM)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("halfvec cannot have more than %d dimensions", HALFVEC_MAX_DIM)));

		while (halfvec_isspace(*pt))
			pt++;

		/* Check for empty string like float4in */
		if (*pt == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type halfvec: \"%s\"", lit)));

		/* Use strtof like float4in to avoid a double-rounding problem */
		errno = 0;
		val = strtof(pt, &stringEnd);

		if (stringEnd == pt)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type halfvec: \"%s\"", lit)));

		x[dim] = Float4ToHalfUnchecked(val);

		if ((errno == ERANGE && isinf(val)) || (HalfIsInf(x[dim]) && !isinf(val)))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("\"%s\" is out of range for type halfvec", pt)));

		CheckElement(x[dim]);
		dim++;

		while (halfvec_isspace(*stringEnd))
			stringEnd++;

		if (*stringEnd != '\0' && *stringEnd != ']')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type halfvec: \"%s\"", lit)));

		pt = strtok(NULL, ",");
	}

	if (stringEnd == NULL || *stringEnd != ']')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed halfvec literal: \"%s\"", lit),
				 errdetail("Unexpected end of input.")));

	stringEnd++;

	/* Only whitespace is allowed after the closing brace */
	while (halfvec_isspace(*stringEnd))
		stringEnd++;

	if (*stringEnd != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed halfvec literal: \"%s\"", lit),
				 errdetail("Junk after closing right brace.")));

	/* Ensure no consecutive delimiters since strtok skips */
	for (pt = lit + 1; *pt != '\0'; pt++)
	{
		if (pt[-1] == ',' && *pt == ',')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed halfvec literal: \"%s\"", lit)));
	}

	if (dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("halfvec must have at least 1 dimension")));

	pfree(litcopy);

	CheckExpectedDim(typmod, dim);

	result = InitHalfVector(dim);
	for (int i = 0; i < dim; i++)
		result->x[i] = x[i];

	PG_RETURN_POINTER(result);
}

#define AppendChar(ptr, c) (*(ptr)++ = (c))
#define AppendFloat(ptr, f) ((ptr) += float_to_shortest_decimal_bufn((f), (ptr)))

/*
 * Convert internal representation to textual representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_out);
Datum
halfvec_out(PG_FUNCTION_ARGS)
{
	HalfVector *vector = PG_GETARG_HALFVEC_P(0);
	int			dim = vector->dim;
	char	   *buf;
	char	   *ptr;

	/*
	 * Need:
	 *
	 * dim * (FLOAT_SHORTEST_DECIMAL_LEN - 1) bytes for
	 * float_to_shortest_decimal_bufn
	 *
	 * dim - 1 bytes for separator
	 *
	 * 3 bytes for [, ], and \0
	 */
	buf = (char *) palloc(FLOAT_SHORTEST_DECIMAL_LEN * dim + 2);
	ptr = buf;

	AppendChar(ptr, '[');

	for (int i = 0; i < dim; i++)
	{
		if (i > 0)
			AppendChar(ptr, ',');

		AppendFloat(ptr, HalfToFloat4(vector->x[i]));
	}

	AppendChar(ptr, ']');
	*ptr = '\0';

	PG_FREE_IF_COPY(vector, 0);
	PG_RETURN_CSTRING(buf);
}

/*
 * Convert type modifier
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_typmod_in);
Datum
halfvec_typmod_in(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *tl;
	int			n;

	tl = ArrayGetIntegerTypmods(ta, &n);

	if (n != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid type modifier")));

	if (*tl < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type halfvec must be at least 1")));

	if (*tl > HALFVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type halfvec cannot exceed %d", HALFVEC_MAX_DIM)));

	PG_RETURN_INT32(*tl);
}

/*
 * Convert external binary representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_recv);
Datum
halfvec_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	HalfVector *result;
	int16		dim;
	int16		unused;

	dim = pq_getmsgint(buf, sizeof(int16));
	unused = pq_getmsgint(buf, sizeof(int16));

	CheckDim(dim);
	CheckExpectedDim(typmod, dim);

	if (unused != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("expected unused to be 0, not %d", unused)));

	result = InitHalfVector(dim);
	for (int i = 0; i < dim; i++)
	{
		result->x[i] = pq_getmsghalf(buf);
		CheckElement(result->x[i]);
	}

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to the external binary representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_send);
Datum
halfvec_send(PG_FUNCTION_ARGS)
{
	HalfVector *vec = PG_GETARG_HALFVEC_P(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint(&buf, vec->dim, sizeof(int16));
	pq_sendint(&buf, vec->unused, sizeof(int16));
	for (int i = 0; i < vec->dim; i++)
		pq_sendhalf(&buf, vec->x[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert half vector to half vector
 * This is needed to check the type modifier
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec);
Datum
halfvec(PG_FUNCTION_ARGS)
{
	HalfVector *vec = PG_GETARG_HALFVEC_P(0);
	int32		typmod = PG_GETARG_INT32(1);

	CheckExpectedDim(typmod, vec->dim);

	PG_RETURN_POINTER(vec);
}

/*
 * Convert array to half vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(array_to_halfvec);
Datum
array_to_halfvec(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	HalfVector *result;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum	   *elemsp;
	int			nelemsp;

	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("array must be 1-D")));

	if (ARR_HASNULL(array) && array_contains_nulls(array))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("array must not contain nulls")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
	deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, NULL, &nelemsp);

	CheckDim(nelemsp);
	CheckExpectedDim(typmod, nelemsp);

	result = InitHalfVector(nelemsp);

	if (ARR_ELEMTYPE(array) == INT4OID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = Float4ToHalf(DatumGetInt32(elemsp[i]));
	}
	else if (ARR_ELEMTYPE(array) == FLOAT8OID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = Float4ToHalf(DatumGetFloat8(elemsp[i]));
	}
	else if (ARR_ELEMTYPE(array) == FLOAT4OID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = Float4ToHalf(DatumGetFloat4(elemsp[i]));
	}
	else if (ARR_ELEMTYPE(array) == NUMERICOID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = Float4ToHalf(DatumGetFloat4(DirectFunctionCall1(numeric_float4, elemsp[i])));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("unsupported array type")));
	}

	/*
	 * Free allocation from deconstruct_array. Do not free individual elements
	 * when pass-by-reference since they point to original array.
	 */
	pfree(elemsp);

	/* Check elements */
	for (int i = 0; i < result->dim; i++)
		CheckElement(result->x[i]);

	PG_RETURN_POINTER(result);
}

/*
 * Convert half vector to float4[]
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_to_float4);
Datum
halfvec_to_float4(PG_FUNCTION_ARGS)
{
	HalfVector *vec = PG_GETARG_HALFVEC_P(0);
	Datum	   *datums;
	ArrayType  *result;

	datums = (Datum *) palloc(sizeof(Datum) * vec->dim);

	for (int i = 0; i < vec->dim; i++)
		datums[i] = Float4GetDatum(HalfToFloat4(vec->x[i]));

	/* Use TYPALIGN_INT for float4 */
	result = construct_array(datums, vec->dim, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);

	pfree(datums);

	PG_RETURN_POINTER(result);
}

/*
 * Convert vector to half vec
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_to_halfvec);
Datum
vector_to_halfvec(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	HalfVector *result;

	CheckDim(vec->dim);
	CheckExpectedDim(typmod, vec->dim);

	result = InitHalfVector(vec->dim);

	for (int i = 0; i < vec->dim; i++)
		result->x[i] = Float4ToHalf(vec->x[i]);

	PG_RETURN_POINTER(result);
}

/*
 * Get the L2 distance between half vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_l2_distance);
Datum
halfvec_l2_distance(PG_FUNCTION_ARGS)
{
	HalfVector *a = PG_GETARG_HALFVEC_P(0);
	HalfVector *b = PG_GETARG_HALFVEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8(sqrt((double) HalfvecL2SquaredDistance(a->dim, a->x, b->x)));
}

/*
 * Get the L2 squared distance between half vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_l2_squared_distance);
Datum
halfvec_l2_squared_distance(PG_FUNCTION_ARGS)
{
	HalfVector *a = PG_GETARG_HALFVEC_P(0);
	HalfVector *b = PG_GETARG_HALFVEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) HalfvecL2SquaredDistance(a->dim, a->x, b->x));
}

/*
 * Get the inner product of two half vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_inner_product);
Datum
halfvec_inner_product(PG_FUNCTION_ARGS)
{
	HalfVector *a = PG_GETARG_HALFVEC_P(0);
	HalfVector *b = PG_GETARG_HALFVEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) HalfvecInnerProduct(a->dim, a->x, b->x));
}

/*
 * Get the negative inner product of two half vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_negative_inner_product);
Datum
halfvec_negative_inner_product(PG_FUNCTION_ARGS)
{
	HalfVector *a = PG_GETARG_HALFVEC_P(0);
	HalfVector *b = PG_GETARG_HALFVEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) -HalfvecInnerProduct(a->dim, a->x, b->x));
}

/*
 * Get the cosine distance between two half vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_cosine_distance);
Datum
halfvec_cosine_distance(PG_FUNCTION_ARGS)
{
	HalfVector *a = PG_GETARG_HALFVEC_P(0);
	HalfVector *b = PG_GETARG_HALFVEC_P(1);
	half	   *ax = a->x;
	half	   *bx = b->x;
	float		distance = 0.0;
	float		norma = 0.0;
	float		normb = 0.0;
	double		similarity;

	CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
	{
		float		axi = HalfToFloat4(ax[i]);
		float		bxi = HalfToFloat4(bx[i]);

		distance += axi * bxi;
		norma += axi * axi;
		normb += bxi * bxi;
	}

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	similarity = (double) distance / sqrt((double) norma * (double) normb);

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

/*
 * Get the distance for spherical k-means
 * Currently uses angular distance since needs to satisfy triangle inequality
 * Assumes inputs are unit vectors (skips norm)
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_spherical_distance);
Datum
halfvec_spherical_distance(PG_FUNCTION_ARGS)
{
	HalfVector *a = PG_GETARG_HALFVEC_P(0);
	HalfVector *b = PG_GETARG_HALFVEC_P(1);
	double		distance;

	CheckDims(a, b);

	distance = (double) HalfvecInnerProduct(a->dim, a->x, b->x);

	/* Prevent NaN with acos with loss of precision */
	if (distance > 1)
		distance = 1;
	else if (distance < -1)
		distance = -1;

	PG_RETURN_FLOAT8(acos(distance) / M_PI);
}

/*
 * Get the L1 distance between two half vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_l1_distance);
Datum
halfvec_l1_distance(PG_FUNCTION_ARGS)
{
	HalfVector *a = PG_GETARG_HALFVEC_P(0);
	HalfVector *b = PG_GETARG_HALFVEC_P(1);
	half	   *ax = a->x;
	half	   *bx = b->x;
	float		distance = 0.0;

	CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
		distance += fabsf(HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]));

	PG_RETURN_FLOAT8((double) distance);
}

/*
 * Get the L2 norm of a half vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_norm);
Datum
halfvec_norm(PG_FUNCTION_ARGS)
{
	HalfVector *a = PG_GETARG_HALFVEC_P(0);
	half	   *ax = a->x;
	double		norm = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
	{
		double		axi = (double) HalfToFloat4(ax[i]);

		norm += axi * axi;
	}

	PG_RETURN_FLOAT8(sqrt(norm));
}

/*
 * Quantize a half vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_quantize_binary);
Datum
halfvec_quantize_binary(PG_FUNCTION_ARGS)
{
	HalfVector *a = PG_GETARG_HALFVEC_P(0);
	half	   *ax = a->x;
	VarBit	   *result = InitBitVector(a->dim);
	unsigned char *rx = VARBITS(result);

	for (int i = 0; i < a->dim; i++)
		rx[i / 8] |= (HalfToFloat4(ax[i]) > 0) << (7 - (i % 8));

	PG_RETURN_VARBIT_P(result);
}

/*
 * Get a subvector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_subvector);
Datum
halfvec_subvector(PG_FUNCTION_ARGS)
{
	HalfVector *a = PG_GETARG_HALFVEC_P(0);
	int32		start = PG_GETARG_INT32(1);
	int32		count = PG_GETARG_INT32(2);
	int32		end = start + count;
	half	   *ax = a->x;
	HalfVector *result;
	int			dim;

	/* Indexing starts at 1, like substring */
	if (start < 1)
		start = 1;

	if (end > a->dim)
		end = a->dim + 1;

	dim = end - start;
	CheckDim(dim);
	result = InitHalfVector(dim);

	for (int i = 0; i < dim; i++)
		result->x[i] = ax[start - 1 + i];

	PG_RETURN_POINTER(result);
}

/*
 * Internal helper to compare half vectors
 */
int
halfvec_cmp_internal(HalfVector * a, HalfVector * b)
{
	int			dim = Min(a->dim, b->dim);

	/* Check values before dimensions to be consistent with Postgres arrays */
	for (int i = 0; i < dim; i++)
	{
		if (HalfToFloat4(a->x[i]) < HalfToFloat4(b->x[i]))
			return -1;

		if (HalfToFloat4(a->x[i]) > HalfToFloat4(b->x[i]))
			return 1;
	}

	if (a->dim < b->dim)
		return -1;

	if (a->dim > b->dim)
		return 1;

	return 0;
}
