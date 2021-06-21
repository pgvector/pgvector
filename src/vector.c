#include "postgres.h"

#include <math.h>

#include "vector.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

#if PG_VERSION_NUM >= 120000
#include "utils/float.h"
#endif

#if PG_VERSION_NUM < 130000
#define TYPALIGN_INT 'i'
#endif

PG_MODULE_MAGIC;

/*
 * Ensure same dimensions
 */
static inline void
CheckDims(Vector * a, Vector * b)
{
	if (a->dim != b->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different vector dimensions %d and %d", a->dim, b->dim)));
}

/*
 * Ensure expected dimension
 */
static inline void
CheckExpectedDim(int32 typmod, int dim)
{
	if (typmod != -1 && typmod != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("expected %d dimensions, not %d", typmod, dim)));
}


static inline void
CheckDim(int dim)
{
	if (dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector must have at least 1 dimension")));

	if (dim > VECTOR_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("vector cannot have more than %d dimensions", VECTOR_MAX_DIM)));
}

/*
 * Ensure finite elements
 */
static inline void
CheckElement(float value)
{
	if (isnan(value))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("NaN not allowed in vector")));


	if (isinf(value))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("infinite value not allowed in vector")));
}

/*
 * Print vector - useful for debugging
 */
void
PrintVector(char *msg, Vector * vector)
{
	StringInfoData buf;
	int			dim = vector->dim;
	int			i;

	initStringInfo(&buf);

	appendStringInfoChar(&buf, '[');
	for (i = 0; i < dim; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, ",");
		appendStringInfoString(&buf, float8out_internal(vector->x[i]));
	}
	appendStringInfoChar(&buf, ']');

	elog(INFO, "%s = %s", msg, buf.data);
}

/*
 * Convert textual representation to internal representation
 */
PG_FUNCTION_INFO_V1(vector_in);
Datum
vector_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	int			i;
	double		x[VECTOR_MAX_DIM];
	int			dim = 0;
	char	   *pt;
	char	   *stringEnd;
	Vector	   *result;

	if (*str != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed vector literal: \"%s\"", str),
				 errdetail("Vector contents must start with \"[\".")));

	str++;
	pt = strtok(str, ",");
	stringEnd = pt;

	while (pt != NULL && *stringEnd != ']')
	{
		if (dim == VECTOR_MAX_DIM)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("vector cannot have more than %d dimensions", VECTOR_MAX_DIM)));

		x[dim] = strtod(pt, &stringEnd);
		CheckElement(x[dim]);
		dim++;

		if (stringEnd == pt)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type vector: \"%s\"", pt)));

		if (*stringEnd != '\0' && *stringEnd != ']')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type vector: \"%s\"", pt)));

		pt = strtok(NULL, ",");
	}

	if (*stringEnd != ']')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed vector literal"),
				 errdetail("Unexpected end of input.")));

	if (stringEnd[1] != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed vector literal"),
				 errdetail("Junk after closing right brace.")));

	if (dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector must have at least 1 dimension")));

	CheckExpectedDim(typmod, dim);

	result = InitVector(dim);
	for (i = 0; i < dim; i++)
		result->x[i] = x[i];

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to textual representation
 */
PG_FUNCTION_INFO_V1(vector_out);
Datum
vector_out(PG_FUNCTION_ARGS)
{
	Vector	   *vector = PG_GETARG_VECTOR_P(0);
	StringInfoData buf;
	int			dim = vector->dim;
	int			i;

	initStringInfo(&buf);

	appendStringInfoChar(&buf, '[');
	for (i = 0; i < dim; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, ",");

		appendStringInfoString(&buf, float8out_internal(vector->x[i]));
	}
	appendStringInfoChar(&buf, ']');

	PG_FREE_IF_COPY(vector, 0);
	PG_RETURN_CSTRING(buf.data);
}

/*
 * Convert type modifier
 */
PG_FUNCTION_INFO_V1(vector_typmod_in);
Datum
vector_typmod_in(PG_FUNCTION_ARGS)
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
				 errmsg("dimensions for type vector must be at least 1")));

	if (*tl > VECTOR_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type vector cannot exceed %d", VECTOR_MAX_DIM)));

	PG_RETURN_INT32(*tl);
}

/*
 * Convert external binary representation to internal representation
 */
PG_FUNCTION_INFO_V1(vector_recv);
Datum
vector_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	Vector	   *result;
	int16		dim;
	int16		unused;
	int			i;

	dim = pq_getmsgint(buf, sizeof(int16));
	unused = pq_getmsgint(buf, sizeof(int16));

	CheckDim(dim);
	CheckExpectedDim(typmod, dim);

	if (unused != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("expected unused to be 0, not %d", unused)));

	result = InitVector(dim);
	for (i = 0; i < dim; i++)
		result->x[i] = pq_getmsgfloat4(buf);

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to the external binary representation
 */
PG_FUNCTION_INFO_V1(vector_send);
Datum
vector_send(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	StringInfoData buf;
	int			i;

	pq_begintypsend(&buf);
	pq_sendint(&buf, vec->dim, sizeof(int16));
	pq_sendint(&buf, vec->unused, sizeof(int16));
	for (i = 0; i < vec->dim; i++)
		pq_sendfloat4(&buf, vec->x[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert vector to vector
 */
PG_FUNCTION_INFO_V1(vector);
Datum
vector(PG_FUNCTION_ARGS)
{
	Vector	   *arg = PG_GETARG_VECTOR_P(0);
	int32		typmod = PG_GETARG_INT32(1);

	CheckExpectedDim(typmod, arg->dim);

	PG_RETURN_POINTER(arg);
}

/*
 * Convert array to vector
 */
PG_FUNCTION_INFO_V1(array_to_vector);
Datum
array_to_vector(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	int			i;
	Vector	   *result;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum	   *elemsp;
	bool	   *nullsp;
	int			nelemsp;

	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("array must be 1-D")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
	deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, &nullsp, &nelemsp);

	if (typmod == -1)
		CheckDim(nelemsp);
	else
		CheckExpectedDim(typmod, nelemsp);

	result = InitVector(nelemsp);
	for (i = 0; i < nelemsp; i++)
	{
		if (nullsp[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("array must not containing NULLs")));

		if (ARR_ELEMTYPE(array) == INT4OID)
			result->x[i] = DatumGetInt32(elemsp[i]);
		else if (ARR_ELEMTYPE(array) == FLOAT8OID)
			result->x[i] = DatumGetFloat8(elemsp[i]);
		else if (ARR_ELEMTYPE(array) == FLOAT4OID)
			result->x[i] = DatumGetFloat4(elemsp[i]);
		else if (ARR_ELEMTYPE(array) == NUMERICOID)
			result->x[i] = DatumGetFloat4(DirectFunctionCall1(numeric_float4, NumericGetDatum(elemsp[i])));
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("unsupported array type")));

		CheckElement(result->x[i]);
	}

	PG_RETURN_POINTER(result);
}

/*
 * Convert vector to float4[]
 */
PG_FUNCTION_INFO_V1(vector_to_float4);
Datum
vector_to_float4(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	Datum	   *d;
	ArrayType  *result;
	int			i;

	d = (Datum *) palloc(sizeof(Datum) * vec->dim);

	for (i = 0; i < vec->dim; i++)
		d[i] = Float4GetDatum(vec->x[i]);

	/* Use TYPALIGN_INT for float4 */
	result = construct_array(d, vec->dim, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);

	PG_RETURN_POINTER(result);
}

/*
 * Get the L2 distance between vectors
 */
PG_FUNCTION_INFO_V1(l2_distance);
Datum
l2_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	double		distance = 0.0;
	double		diff;

	CheckDims(a, b);

	for (int i = 0; i < a->dim; i++)
	{
		diff = a->x[i] - b->x[i];
		distance += diff * diff;
	}

	PG_RETURN_FLOAT8(sqrt(distance));
}

/*
 * Get the L2 squared distance between vectors
 * This saves a sqrt calculation
 */
PG_FUNCTION_INFO_V1(vector_l2_squared_distance);
Datum
vector_l2_squared_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	double		distance = 0.0;
	double		diff;

	CheckDims(a, b);

	for (int i = 0; i < a->dim; i++)
	{
		diff = a->x[i] - b->x[i];
		distance += diff * diff;
	}

	PG_RETURN_FLOAT8(distance);
}

/*
 * Get the inner product of two vectors
 */
PG_FUNCTION_INFO_V1(inner_product);
Datum
inner_product(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	double		distance = 0.0;

	CheckDims(a, b);

	for (int i = 0; i < a->dim; i++)
		distance += a->x[i] * b->x[i];

	PG_RETURN_FLOAT8(distance);
}

/*
 * Get the negative inner product of two vectors
 */
PG_FUNCTION_INFO_V1(vector_negative_inner_product);
Datum
vector_negative_inner_product(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	double		distance = 0.0;

	CheckDims(a, b);

	for (int i = 0; i < a->dim; i++)
		distance += a->x[i] * b->x[i];

	PG_RETURN_FLOAT8(distance * -1);
}

/*
 * Get the cosine distance between two vectors
 */
PG_FUNCTION_INFO_V1(cosine_distance);
Datum
cosine_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	double		distance = 0.0;
	double		norma = 0.0;
	double		normb = 0.0;

	CheckDims(a, b);

	for (int i = 0; i < a->dim; i++)
	{
		distance += a->x[i] * b->x[i];
		norma += a->x[i] * a->x[i];
		normb += b->x[i] * b->x[i];
	}

	PG_RETURN_FLOAT8(1 - (distance / (sqrt(norma) * sqrt(normb))));
}

/*
 * Get the distance for spherical k-means
 * Currently uses angular distance since needs to satisfy triangle inequality
 * Assumes inputs are unit vectors (skips norm)
 */
PG_FUNCTION_INFO_V1(vector_spherical_distance);
Datum
vector_spherical_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	double		distance = 0.0;

	CheckDims(a, b);

	for (int i = 0; i < a->dim; i++)
		distance += a->x[i] * b->x[i];

	/* Prevent NaN with acos with loss of precision */
	if (distance > 1)
		distance = 1;
	else if (distance < -1)
		distance = -1;

	PG_RETURN_FLOAT8(acos(distance) / M_PI);
}

/*
 * Get the dimensions of a vector
 */
PG_FUNCTION_INFO_V1(vector_dims);
Datum
vector_dims(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);

	PG_RETURN_INT32(a->dim);
}

/*
 * Get the L2 norm of a vector
 */
PG_FUNCTION_INFO_V1(vector_norm);
Datum
vector_norm(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	double		norm = 0.0;

	for (int i = 0; i < a->dim; i++)
		norm += a->x[i] * a->x[i];

	PG_RETURN_FLOAT8(sqrt(norm));
}

/*
 * Add vectors
 */
PG_FUNCTION_INFO_V1(vector_add);
Datum
vector_add(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	Vector	   *result;
	int			i;

	CheckDims(a, b);

	result = InitVector(a->dim);
	for (i = 0; i < a->dim; i++)
		result->x[i] = a->x[i] + b->x[i];

	PG_RETURN_POINTER(result);
}

/*
 * Subtract vectors
 */
PG_FUNCTION_INFO_V1(vector_sub);
Datum
vector_sub(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	Vector	   *result;
	int			i;

	CheckDims(a, b);

	result = InitVector(a->dim);
	for (i = 0; i < a->dim; i++)
		result->x[i] = a->x[i] - b->x[i];

	PG_RETURN_POINTER(result);
}

/*
 * Internal helper to compare vectors
 */
int
vector_cmp_internal(Vector * a, Vector * b)
{
	int			i;

	CheckDims(a, b);

	for (i = 0; i < a->dim; i++)
	{
		if (a->x[i] < b->x[i])
			return -1;

		if (a->x[i] > b->x[i])
			return 1;
	}
	return 0;
}

/*
 * Less than
 */
PG_FUNCTION_INFO_V1(vector_lt);
Datum
vector_lt(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) < 0);
}

/*
 * Less than or equal
 */
PG_FUNCTION_INFO_V1(vector_le);
Datum
vector_le(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) <= 0);
}

/*
 * Equal
 */
PG_FUNCTION_INFO_V1(vector_eq);
Datum
vector_eq(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) == 0);
}

/*
 * Not equal
 */
PG_FUNCTION_INFO_V1(vector_ne);
Datum
vector_ne(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) != 0);
}

/*
 * Greater than or equal
 */
PG_FUNCTION_INFO_V1(vector_ge);
Datum
vector_ge(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) >= 0);
}

/*
 * Greater than
 */
PG_FUNCTION_INFO_V1(vector_gt);
Datum
vector_gt(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) > 0);
}

/*
 * Compare vectors
 */
PG_FUNCTION_INFO_V1(vector_cmp);
Datum
vector_cmp(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_INT32(vector_cmp_internal(a, b));
}
