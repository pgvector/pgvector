#include "postgres.h"

#include <math.h>

#include "bitvec.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "halfutils.h"
#include "halfvec.h"
#include "int8utils.h"
#include "int8vec.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "sparsevec.h"
#include "utils/array.h"
#include "utils/float.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/varbit.h"
#include "vector.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#if PG_VERSION_NUM >= 170000
#include "parser/scansup.h"
#endif

#define STATE_DIMS(x) (ARR_DIMS(x)[0] - 1)
#define CreateStateDatums(dim) palloc(sizeof(Datum) * (dim + 1))

/*
 * Ensure same dimensions
 */
static inline void
CheckDims(Int8Vector * a, Int8Vector * b)
{
	if (a->dim != b->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different int8vec dimensions %d and %d", a->dim, b->dim)));
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
				 errmsg("int8vec must have at least 1 dimension")));

	if (dim > INT8VEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("int8vec cannot have more than %d dimensions", INT8VEC_MAX_DIM)));
}

/*
 * Allocate and initialize a new int8 vector
 */
Int8Vector *
InitInt8Vector(int dim)
{
	Int8Vector *result;
	int			size;

	size = INT8VEC_SIZE(dim);
	result = (Int8Vector *) palloc0(size);
	SET_VARSIZE(result, size);
	result->dim = dim;

	return result;
}

#if PG_VERSION_NUM >= 170000
#define int8vec_isspace(ch) scanner_isspace(ch)
#else
static inline bool
int8vec_isspace(char ch)
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
#endif

/*
 * Check state array
 */
static float8 *
CheckStateArray(ArrayType *statearray, const char *caller)
{
	if (ARR_NDIM(statearray) != 1 ||
		ARR_DIMS(statearray)[0] < 1 ||
		ARR_HASNULL(statearray) ||
		ARR_ELEMTYPE(statearray) != FLOAT8OID)
		elog(ERROR, "%s: expected state array", caller);
	return (float8 *) ARR_DATA_PTR(statearray);
}

/*
 * Convert textual representation to internal representation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_in);
Datum
int8vec_in(PG_FUNCTION_ARGS)
{
	char	   *lit = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	int8		x[INT8VEC_MAX_DIM];
	int			dim = 0;
	char	   *pt = lit;
	Int8Vector *result;

	while (int8vec_isspace(*pt))
		pt++;

	if (*pt != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type int8vec: \"%s\"", lit),
				 errdetail("Vector contents must start with \"[\".")));

	pt++;

	while (int8vec_isspace(*pt))
		pt++;

	if (*pt == ']')
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("int8vec must have at least 1 dimension")));

	for (;;)
	{
		long		val;
		char	   *stringEnd;

		if (dim == INT8VEC_MAX_DIM)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("int8vec cannot have more than %d dimensions", INT8VEC_MAX_DIM)));

		while (int8vec_isspace(*pt))
			pt++;

		/* Check for empty string */
		if (*pt == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type int8vec: \"%s\"", lit)));

		errno = 0;
		val = strtol(pt, &stringEnd, 10);

		if (stringEnd == pt)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type int8vec: \"%s\"", lit)));

		/* Reject non-integer input (no decimal point, no 'e') */
		if (*stringEnd == '.' || *stringEnd == 'e' || *stringEnd == 'E')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type int8vec: \"%s\"", lit),
					 errdetail("int8vec values must be integers.")));

		if (val < -128 || val > 127)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("\"%ld\" is out of range for type int8vec", val),
					 errdetail("int8vec values must be between -128 and 127.")));

		x[dim] = (int8) val;
		dim++;

		pt = stringEnd;

		while (int8vec_isspace(*pt))
			pt++;

		if (*pt == ',')
			pt++;
		else if (*pt == ']')
		{
			pt++;
			break;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type int8vec: \"%s\"", lit)));
	}

	/* Only whitespace is allowed after the closing brace */
	while (int8vec_isspace(*pt))
		pt++;

	if (*pt != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type int8vec: \"%s\"", lit),
				 errdetail("Junk after closing right brace.")));

	CheckDim(dim);
	CheckExpectedDim(typmod, dim);

	result = InitInt8Vector(dim);
	for (int i = 0; i < dim; i++)
		result->x[i] = x[i];

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to textual representation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_out);
Datum
int8vec_out(PG_FUNCTION_ARGS)
{
	Int8Vector *vector = PG_GETARG_INT8VEC_P(0);
	int			dim = vector->dim;
	char	   *buf;
	char	   *ptr;

	/*
	 * Need:
	 *
	 * dim * 4 bytes max for "-128"
	 *
	 * dim - 1 bytes for separator
	 *
	 * 3 bytes for [, ], and \0
	 */
	buf = (char *) palloc(5 * dim + 2);
	ptr = buf;

	*ptr++ = '[';

	for (int i = 0; i < dim; i++)
	{
		if (i > 0)
			*ptr++ = ',';

		ptr += sprintf(ptr, "%d", (int) vector->x[i]);
	}

	*ptr++ = ']';
	*ptr = '\0';

	PG_FREE_IF_COPY(vector, 0);
	PG_RETURN_CSTRING(buf);
}

/*
 * Convert type modifier
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_typmod_in);
Datum
int8vec_typmod_in(PG_FUNCTION_ARGS)
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
				 errmsg("dimensions for type int8vec must be at least 1")));

	if (*tl > INT8VEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type int8vec cannot exceed %d", INT8VEC_MAX_DIM)));

	PG_RETURN_INT32(*tl);
}

/*
 * Convert external binary representation to internal representation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_recv);
Datum
int8vec_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	Int8Vector *result;
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

	result = InitInt8Vector(dim);
	for (int i = 0; i < dim; i++)
		result->x[i] = (int8) pq_getmsgint(buf, sizeof(int8));

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to the external binary representation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_send);
Datum
int8vec_send(PG_FUNCTION_ARGS)
{
	Int8Vector *vec = PG_GETARG_INT8VEC_P(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint(&buf, vec->dim, sizeof(int16));
	pq_sendint(&buf, vec->unused, sizeof(int16));
	for (int i = 0; i < vec->dim; i++)
		pq_sendint(&buf, (uint8) vec->x[i], sizeof(int8));

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert int8vec to int8vec
 * This is needed to check the type modifier
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec);
Datum
int8vec(PG_FUNCTION_ARGS)
{
	Int8Vector *vec = PG_GETARG_INT8VEC_P(0);
	int32		typmod = PG_GETARG_INT32(1);

	CheckExpectedDim(typmod, vec->dim);

	PG_RETURN_POINTER(vec);
}

/*
 * Convert array to int8vec
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(array_to_int8vec);
Datum
array_to_int8vec(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	Int8Vector *result;
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

	result = InitInt8Vector(nelemsp);

	if (ARR_ELEMTYPE(array) == INT4OID)
	{
		for (int i = 0; i < nelemsp; i++)
		{
			int32		val = DatumGetInt32(elemsp[i]);

			if (val < -128 || val > 127)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("\"%d\" is out of range for type int8vec", val)));
			result->x[i] = (int8) val;
		}
	}
	else if (ARR_ELEMTYPE(array) == FLOAT8OID)
	{
		for (int i = 0; i < nelemsp; i++)
		{
			float8		val = DatumGetFloat8(elemsp[i]);
			int32		ival = (int32) round(val);

			if (ival < -128 || ival > 127)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value out of range for type int8vec")));
			result->x[i] = (int8) ival;
		}
	}
	else if (ARR_ELEMTYPE(array) == FLOAT4OID)
	{
		for (int i = 0; i < nelemsp; i++)
		{
			float4		val = DatumGetFloat4(elemsp[i]);
			int32		ival = (int32) roundf(val);

			if (ival < -128 || ival > 127)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value out of range for type int8vec")));
			result->x[i] = (int8) ival;
		}
	}
	else if (ARR_ELEMTYPE(array) == NUMERICOID)
	{
		for (int i = 0; i < nelemsp; i++)
		{
			float4		val = DatumGetFloat4(DirectFunctionCall1(numeric_float4, elemsp[i]));
			int32		ival = (int32) roundf(val);

			if (ival < -128 || ival > 127)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value out of range for type int8vec")));
			result->x[i] = (int8) ival;
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("unsupported array type")));
	}

	pfree(elemsp);

	PG_RETURN_POINTER(result);
}

/*
 * Convert int8vec to float4[]
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_to_float4);
Datum
int8vec_to_float4(PG_FUNCTION_ARGS)
{
	Int8Vector *vec = PG_GETARG_INT8VEC_P(0);
	Datum	   *datums;
	ArrayType  *result;

	datums = (Datum *) palloc(sizeof(Datum) * vec->dim);

	for (int i = 0; i < vec->dim; i++)
		datums[i] = Float4GetDatum((float4) vec->x[i]);

	/* Use TYPALIGN_INT for float4 */
	result = construct_array(datums, vec->dim, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);

	pfree(datums);

	PG_RETURN_POINTER(result);
}

/*
 * Convert vector to int8vec
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_to_int8vec);
Datum
vector_to_int8vec(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	Int8Vector *result;

	CheckDim(vec->dim);
	CheckExpectedDim(typmod, vec->dim);

	result = InitInt8Vector(vec->dim);

	for (int i = 0; i < vec->dim; i++)
	{
		int32		val = (int32) roundf(vec->x[i]);

		if (val < -128 || val > 127)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value out of range for type int8vec")));
		result->x[i] = (int8) val;
	}

	PG_RETURN_POINTER(result);
}

/*
 * Convert halfvec to int8vec
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(halfvec_to_int8vec);
Datum
halfvec_to_int8vec(PG_FUNCTION_ARGS)
{
	HalfVector *vec = PG_GETARG_HALFVEC_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	Int8Vector *result;

	CheckDim(vec->dim);
	CheckExpectedDim(typmod, vec->dim);

	result = InitInt8Vector(vec->dim);

	for (int i = 0; i < vec->dim; i++)
	{
		float		val = HalfToFloat4(vec->x[i]);
		int32		ival = (int32) roundf(val);

		if (ival < -128 || ival > 127)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value out of range for type int8vec")));
		result->x[i] = (int8) ival;
	}

	PG_RETURN_POINTER(result);
}

/*
 * Get the L2 distance between int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_l2_distance);
Datum
int8vec_l2_distance(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8(sqrt((double) Int8vecL2SquaredDistance(a->dim, a->x, b->x)));
}

/*
 * Get the L2 squared distance between int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_l2_squared_distance);
Datum
int8vec_l2_squared_distance(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) Int8vecL2SquaredDistance(a->dim, a->x, b->x));
}

/*
 * Get the inner product of two int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_inner_product);
Datum
int8vec_inner_product(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) Int8vecInnerProduct(a->dim, a->x, b->x));
}

/*
 * Get the negative inner product of two int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_negative_inner_product);
Datum
int8vec_negative_inner_product(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) -Int8vecInnerProduct(a->dim, a->x, b->x));
}

/*
 * Get the cosine distance between two int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_cosine_distance);
Datum
int8vec_cosine_distance(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);
	double		similarity;

	CheckDims(a, b);

	similarity = Int8vecCosineSimilarity(a->dim, a->x, b->x);

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
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_spherical_distance);
Datum
int8vec_spherical_distance(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);
	double		distance;

	CheckDims(a, b);

	distance = (double) Int8vecInnerProduct(a->dim, a->x, b->x);

	/* Prevent NaN with acos with loss of precision */
	if (distance > 1)
		distance = 1;
	else if (distance < -1)
		distance = -1;

	PG_RETURN_FLOAT8(acos(distance) / M_PI);
}

/*
 * Get the L1 distance between two int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_l1_distance);
Datum
int8vec_l1_distance(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) Int8vecL1Distance(a->dim, a->x, b->x));
}

/*
 * Get the dimensions of an int8 vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_vector_dims);
Datum
int8vec_vector_dims(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);

	PG_RETURN_INT32(a->dim);
}

/*
 * Get the L2 norm of an int8 vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_l2_norm);
Datum
int8vec_l2_norm(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	int8	   *ax = a->x;
	double		norm = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
	{
		double		axi = (double) ax[i];

		norm += axi * axi;
	}

	PG_RETURN_FLOAT8(sqrt(norm));
}

/*
 * Normalize an int8 vector with the L2 norm
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_l2_normalize);
Datum
int8vec_l2_normalize(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	int8	   *ax = a->x;
	double		norm = 0;
	Int8Vector *result;
	int8	   *rx;

	result = InitInt8Vector(a->dim);
	rx = result->x;

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
		norm += (double) ax[i] * (double) ax[i];

	norm = sqrt(norm);

	/* Return zero vector for zero norm */
	if (norm > 0)
	{
		for (int i = 0; i < a->dim; i++)
		{
			double		val = ((double) ax[i] / norm) * 127.0;

			/* Round to nearest int8 */
			if (val >= 0)
				rx[i] = (int8) (val + 0.5 > 127.0 ? 127 : (int32) (val + 0.5));
			else
				rx[i] = (int8) (val - 0.5 < -128.0 ? -128 : (int32) (val - 0.5));
		}
	}

	PG_RETURN_POINTER(result);
}

/*
 * Add int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_add);
Datum
int8vec_add(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);
	int8	   *ax = a->x;
	int8	   *bx = b->x;
	Int8Vector *result;
	int8	   *rx;

	CheckDims(a, b);

	result = InitInt8Vector(a->dim);
	rx = result->x;

	for (int i = 0, imax = a->dim; i < imax; i++)
	{
		int32		val = (int32) ax[i] + (int32) bx[i];

		if (val < -128 || val > 127)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value out of range for type int8vec")));
		rx[i] = (int8) val;
	}

	PG_RETURN_POINTER(result);
}

/*
 * Subtract int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_sub);
Datum
int8vec_sub(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);
	int8	   *ax = a->x;
	int8	   *bx = b->x;
	Int8Vector *result;
	int8	   *rx;

	CheckDims(a, b);

	result = InitInt8Vector(a->dim);
	rx = result->x;

	for (int i = 0, imax = a->dim; i < imax; i++)
	{
		int32		val = (int32) ax[i] - (int32) bx[i];

		if (val < -128 || val > 127)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value out of range for type int8vec")));
		rx[i] = (int8) val;
	}

	PG_RETURN_POINTER(result);
}

/*
 * Multiply int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_mul);
Datum
int8vec_mul(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);
	int8	   *ax = a->x;
	int8	   *bx = b->x;
	Int8Vector *result;
	int8	   *rx;

	CheckDims(a, b);

	result = InitInt8Vector(a->dim);
	rx = result->x;

	for (int i = 0, imax = a->dim; i < imax; i++)
	{
		int32		val = (int32) ax[i] * (int32) bx[i];

		if (val < -128 || val > 127)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value out of range for type int8vec")));
		rx[i] = (int8) val;
	}

	PG_RETURN_POINTER(result);
}

/*
 * Concatenate int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_concat);
Datum
int8vec_concat(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);
	Int8Vector *result;
	int			dim = a->dim + b->dim;

	CheckDim(dim);
	result = InitInt8Vector(dim);

	for (int i = 0; i < a->dim; i++)
		result->x[i] = a->x[i];

	for (int i = 0; i < b->dim; i++)
		result->x[i + a->dim] = b->x[i];

	PG_RETURN_POINTER(result);
}

/*
 * Quantize an int8 vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_binary_quantize);
Datum
int8vec_binary_quantize(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	int8	   *ax = a->x;
	VarBit	   *result = InitBitVector(a->dim);
	unsigned char *rx = VARBITS(result);
	int			i = 0;
	int			count = (a->dim / 8) * 8;

	/* Auto-vectorized on aarch64 */
	for (; i < count; i += 8)
	{
		unsigned char result_byte = 0;

		for (int j = 0; j < 8; j++)
			result_byte |= (ax[i + j] > 0) << (7 - j);

		rx[i / 8] = result_byte;
	}

	for (; i < a->dim; i++)
		rx[i / 8] |= (ax[i] > 0) << (7 - (i % 8));

	PG_RETURN_VARBIT_P(result);
}

/*
 * Get a subvector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_subvector);
Datum
int8vec_subvector(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	int32		start = PG_GETARG_INT32(1);
	int32		count = PG_GETARG_INT32(2);
	int32		end;
	int8	   *ax = a->x;
	Int8Vector *result;
	int32		dim;

	if (count < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("int8vec must have at least 1 dimension")));

	/*
	 * Check if (start + count > a->dim), avoiding integer overflow. a->dim
	 * and count are both positive, so a->dim - count won't overflow.
	 */
	if (start > a->dim - count)
		end = a->dim + 1;
	else
		end = start + count;

	/* Indexing starts at 1, like substring */
	if (start < 1)
		start = 1;
	else if (start > a->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("int8vec must have at least 1 dimension")));

	dim = end - start;
	CheckDim(dim);
	result = InitInt8Vector(dim);

	for (int i = 0; i < dim; i++)
		result->x[i] = ax[start - 1 + i];

	PG_RETURN_POINTER(result);
}

/*
 * Internal helper to compare int8 vectors
 */
static int
int8vec_cmp_internal(Int8Vector * a, Int8Vector * b)
{
	int			dim = Min(a->dim, b->dim);

	/* Check values before dimensions to be consistent with Postgres arrays */
	for (int i = 0; i < dim; i++)
	{
		if (a->x[i] < b->x[i])
			return -1;

		if (a->x[i] > b->x[i])
			return 1;
	}

	if (a->dim < b->dim)
		return -1;

	if (a->dim > b->dim)
		return 1;

	return 0;
}

/*
 * Less than
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_lt);
Datum
int8vec_lt(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	PG_RETURN_BOOL(int8vec_cmp_internal(a, b) < 0);
}

/*
 * Less than or equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_le);
Datum
int8vec_le(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	PG_RETURN_BOOL(int8vec_cmp_internal(a, b) <= 0);
}

/*
 * Equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_eq);
Datum
int8vec_eq(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	PG_RETURN_BOOL(int8vec_cmp_internal(a, b) == 0);
}

/*
 * Not equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_ne);
Datum
int8vec_ne(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	PG_RETURN_BOOL(int8vec_cmp_internal(a, b) != 0);
}

/*
 * Greater than or equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_ge);
Datum
int8vec_ge(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	PG_RETURN_BOOL(int8vec_cmp_internal(a, b) >= 0);
}

/*
 * Greater than
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_gt);
Datum
int8vec_gt(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	PG_RETURN_BOOL(int8vec_cmp_internal(a, b) > 0);
}

/*
 * Compare int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_cmp);
Datum
int8vec_cmp(PG_FUNCTION_ARGS)
{
	Int8Vector *a = PG_GETARG_INT8VEC_P(0);
	Int8Vector *b = PG_GETARG_INT8VEC_P(1);

	PG_RETURN_INT32(int8vec_cmp_internal(a, b));
}

/*
 * Accumulate int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_accum);
Datum
int8vec_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *statearray = PG_GETARG_ARRAYTYPE_P(0);
	Int8Vector *newval = PG_GETARG_INT8VEC_P(1);
	float8	   *statevalues;
	int16		dim;
	bool		newarr;
	float8		n;
	Datum	   *statedatums;
	int8	   *x = newval->x;
	ArrayType  *result;

	/* Check array before using */
	statevalues = CheckStateArray(statearray, "int8vec_accum");
	dim = STATE_DIMS(statearray);
	newarr = dim == 0;

	if (newarr)
		dim = newval->dim;
	else
		CheckExpectedDim(dim, newval->dim);

	n = statevalues[0] + 1.0;

	statedatums = CreateStateDatums(dim);
	statedatums[0] = Float8GetDatum(n);

	if (newarr)
	{
		for (int i = 0; i < dim; i++)
			statedatums[i + 1] = Float8GetDatum((double) x[i]);
	}
	else
	{
		for (int i = 0; i < dim; i++)
		{
			double		v = statevalues[i + 1] + (double) x[i];

			/* Check for overflow */
			if (isinf(v))
				float_overflow_error();

			statedatums[i + 1] = Float8GetDatum(v);
		}
	}

	/* Use float8 array like float4_accum */
	result = construct_array(statedatums, dim + 1,
							 FLOAT8OID,
							 sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

	pfree(statedatums);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * Average int8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(int8vec_avg);
Datum
int8vec_avg(PG_FUNCTION_ARGS)
{
	ArrayType  *statearray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *statevalues;
	float8		n;
	uint16		dim;
	Int8Vector *result;

	/* Check array before using */
	statevalues = CheckStateArray(statearray, "int8vec_avg");
	n = statevalues[0];

	/* SQL defines AVG of no values to be NULL */
	if (n == 0.0)
		PG_RETURN_NULL();

	/* Create int8 vector */
	dim = STATE_DIMS(statearray);
	CheckDim(dim);
	result = InitInt8Vector(dim);
	for (int i = 0; i < dim; i++)
	{
		double		val = statevalues[i + 1] / n;
		int32		ival = (int32) round(val);

		if (ival < -128)
			ival = -128;
		else if (ival > 127)
			ival = 127;

		result->x[i] = (int8) ival;
	}

	PG_RETURN_POINTER(result);
}

/*
 * Convert sparse vector to int8 vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(sparsevec_to_int8vec);
Datum
sparsevec_to_int8vec(PG_FUNCTION_ARGS)
{
	SparseVector *svec = PG_GETARG_SPARSEVEC_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	Int8Vector *result;
	int			dim = svec->dim;
	float	   *values = SPARSEVEC_VALUES(svec);

	CheckDim(dim);
	CheckExpectedDim(typmod, dim);

	result = InitInt8Vector(dim);
	for (int i = 0; i < svec->nnz; i++)
	{
		int32		val = (int32) roundf(values[i]);

		if (val < -128 || val > 127)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value out of range for type int8vec")));
		result->x[svec->indices[i]] = (int8) val;
	}

	PG_RETURN_POINTER(result);
}
