#include "postgres.h"

#include <math.h>

#include "bitutils.h"
#include "bitvec.h"
#include "catalog/pg_type.h"
#include "common/shortest_dec.h"
#include "fmgr.h"
#include "halfutils.h"
#include "halfvec.h"
#include "hnsw.h"
#include "ivfflat.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "port.h"				/* for strtof() */
#include "sparsevec.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "vector.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#if PG_VERSION_NUM < 130000
#define TYPALIGN_DOUBLE 'd'
#define TYPALIGN_INT 'i'
#endif

#define STATE_DIMS(x) (ARR_DIMS(x)[0] - 1)
#define CreateStateDatums(dim) palloc(sizeof(Datum) * (dim + 1))

#if defined(USE_TARGET_CLONES) && !defined(__FMA__)
#define VECTOR_TARGET_CLONES __attribute__((target_clones("default", "fma")))
#else
#define VECTOR_TARGET_CLONES
#endif

PG_MODULE_MAGIC;

/*
 * Initialize index options and variables
 */
PGDLLEXPORT void _PG_init(void);
void
_PG_init(void)
{
	BitvecInit();
	HalfvecInit();
	HnswInit();
	IvfflatInit();
}

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
				 errmsg("vector must have at least 1 dimension")));

	if (dim > VECTOR_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("vector cannot have more than %d dimensions", VECTOR_MAX_DIM)));
}

/*
 * Ensure finite element
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
 * Allocate and initialize a new vector
 */
Vector *
InitVector(int dim)
{
	Vector	   *result;
	int			size;

	size = VECTOR_SIZE(dim);
	result = (Vector *) palloc0(size);
	SET_VARSIZE(result, size);
	result->dim = dim;

	return result;
}

/*
 * Check for whitespace, since array_isspace() is static
 */
static inline bool
vector_isspace(char ch)
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

#if PG_VERSION_NUM < 120003
static pg_noinline void
float_overflow_error(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value out of range: overflow")));
}

static pg_noinline void
float_underflow_error(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value out of range: underflow")));
}
#endif

/*
 * Convert textual representation to internal representation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_in);
Datum
vector_in(PG_FUNCTION_ARGS)
{
	char	   *lit = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	float		x[VECTOR_MAX_DIM];
	int			dim = 0;
	char	   *pt = lit;
	Vector	   *result;

	while (vector_isspace(*pt))
		pt++;

	if (*pt != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type vector: \"%s\"", lit),
				 errdetail("Vector contents must start with \"[\".")));

	pt++;

	while (vector_isspace(*pt))
		pt++;

	if (*pt == ']')
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector must have at least 1 dimension")));

	for (;;)
	{
		float		val;
		char	   *stringEnd;

		if (dim == VECTOR_MAX_DIM)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("vector cannot have more than %d dimensions", VECTOR_MAX_DIM)));

		while (vector_isspace(*pt))
			pt++;

		/* Check for empty string like float4in */
		if (*pt == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type vector: \"%s\"", lit)));

		errno = 0;

		/* Use strtof like float4in to avoid a double-rounding problem */
		/* Postgres sets LC_NUMERIC to C on startup */
		val = strtof(pt, &stringEnd);

		if (stringEnd == pt)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type vector: \"%s\"", lit)));

		/* Check for range error like float4in */
		if (errno == ERANGE && isinf(val))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("\"%s\" is out of range for type vector", pnstrdup(pt, stringEnd - pt))));

		CheckElement(val);
		x[dim++] = val;

		pt = stringEnd;

		while (vector_isspace(*pt))
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
					 errmsg("invalid input syntax for type vector: \"%s\"", lit)));
	}

	/* Only whitespace is allowed after the closing brace */
	while (vector_isspace(*pt))
		pt++;

	if (*pt != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type vector: \"%s\"", lit),
				 errdetail("Junk after closing right brace.")));

	CheckDim(dim);
	CheckExpectedDim(typmod, dim);

	result = InitVector(dim);
	for (int i = 0; i < dim; i++)
		result->x[i] = x[i];

	PG_RETURN_POINTER(result);
}

#define AppendChar(ptr, c) (*(ptr)++ = (c))
#define AppendFloat(ptr, f) ((ptr) += float_to_shortest_decimal_bufn((f), (ptr)))

/*
 * Convert internal representation to textual representation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_out);
Datum
vector_out(PG_FUNCTION_ARGS)
{
	Vector	   *vector = PG_GETARG_VECTOR_P(0);
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

		AppendFloat(ptr, vector->x[i]);
	}

	AppendChar(ptr, ']');
	*ptr = '\0';

	PG_FREE_IF_COPY(vector, 0);
	PG_RETURN_CSTRING(buf);
}

/*
 * Print vector - useful for debugging
 */
void
PrintVector(char *msg, Vector * vector)
{
	char	   *out = DatumGetPointer(DirectFunctionCall1(vector_out, PointerGetDatum(vector)));

	elog(INFO, "%s = %s", msg, out);
	pfree(out);
}

/*
 * Convert type modifier
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_typmod_in);
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
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_recv);
Datum
vector_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	Vector	   *result;
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

	result = InitVector(dim);
	for (int i = 0; i < dim; i++)
	{
		result->x[i] = pq_getmsgfloat4(buf);
		CheckElement(result->x[i]);
	}

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to the external binary representation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_send);
Datum
vector_send(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint(&buf, vec->dim, sizeof(int16));
	pq_sendint(&buf, vec->unused, sizeof(int16));
	for (int i = 0; i < vec->dim; i++)
		pq_sendfloat4(&buf, vec->x[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert vector to vector
 * This is needed to check the type modifier
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector);
Datum
vector(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	int32		typmod = PG_GETARG_INT32(1);

	CheckExpectedDim(typmod, vec->dim);

	PG_RETURN_POINTER(vec);
}

/*
 * Convert array to vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(array_to_vector);
Datum
array_to_vector(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	Vector	   *result;
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

	result = InitVector(nelemsp);

	if (ARR_ELEMTYPE(array) == INT4OID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = DatumGetInt32(elemsp[i]);
	}
	else if (ARR_ELEMTYPE(array) == FLOAT8OID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = DatumGetFloat8(elemsp[i]);
	}
	else if (ARR_ELEMTYPE(array) == FLOAT4OID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = DatumGetFloat4(elemsp[i]);
	}
	else if (ARR_ELEMTYPE(array) == NUMERICOID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = DatumGetFloat4(DirectFunctionCall1(numeric_float4, elemsp[i]));
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
 * Convert vector to float4[]
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_to_float4);
Datum
vector_to_float4(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	Datum	   *datums;
	ArrayType  *result;

	datums = (Datum *) palloc(sizeof(Datum) * vec->dim);

	for (int i = 0; i < vec->dim; i++)
		datums[i] = Float4GetDatum(vec->x[i]);

	/* Use TYPALIGN_INT for float4 */
	result = construct_array(datums, vec->dim, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);

	pfree(datums);

	PG_RETURN_POINTER(result);
}

/*
 * Convert half vector to vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(halfvec_to_vector);
Datum
halfvec_to_vector(PG_FUNCTION_ARGS)
{
	HalfVector *vec = PG_GETARG_HALFVEC_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	Vector	   *result;

	CheckDim(vec->dim);
	CheckExpectedDim(typmod, vec->dim);

	result = InitVector(vec->dim);

	for (int i = 0; i < vec->dim; i++)
		result->x[i] = HalfToFloat4(vec->x[i]);

	PG_RETURN_POINTER(result);
}

VECTOR_TARGET_CLONES static float
VectorL2SquaredDistance(int dim, float *ax, float *bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		float		diff = ax[i] - bx[i];

		distance += diff * diff;
	}

	return distance;
}

/*
 * Get the L2 distance between vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(l2_distance);
Datum
l2_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8(sqrt((double) VectorL2SquaredDistance(a->dim, a->x, b->x)));
}

/*
 * Get the L2 squared distance between vectors
 * This saves a sqrt calculation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_l2_squared_distance);
Datum
vector_l2_squared_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) VectorL2SquaredDistance(a->dim, a->x, b->x));
}

VECTOR_TARGET_CLONES static float
VectorInnerProduct(int dim, float *ax, float *bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += ax[i] * bx[i];

	return distance;
}

/*
 * Get the inner product of two vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(inner_product);
Datum
inner_product(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) VectorInnerProduct(a->dim, a->x, b->x));
}

/*
 * Get the negative inner product of two vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_negative_inner_product);
Datum
vector_negative_inner_product(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) -VectorInnerProduct(a->dim, a->x, b->x));
}

VECTOR_TARGET_CLONES static double
VectorCosineSimilarity(int dim, float *ax, float *bx)
{
	float		similarity = 0.0;
	float		norma = 0.0;
	float		normb = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		similarity += ax[i] * bx[i];
		norma += ax[i] * ax[i];
		normb += bx[i] * bx[i];
	}

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
}

/*
 * Get the cosine distance between two vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(cosine_distance);
Datum
cosine_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	double		similarity;

	CheckDims(a, b);

	similarity = VectorCosineSimilarity(a->dim, a->x, b->x);

#ifdef _MSC_VER
	/* /fp:fast may not propagate NaN */
	if (isnan(similarity))
		PG_RETURN_FLOAT8(NAN);
#endif

	/* Keep in range */
	if (similarity > 1)
		similarity = 1.0;
	else if (similarity < -1)
		similarity = -1.0;

	PG_RETURN_FLOAT8(1.0 - similarity);
}

/*
 * Get the distance for spherical k-means
 * Currently uses angular distance since needs to satisfy triangle inequality
 * Assumes inputs are unit vectors (skips norm)
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_spherical_distance);
Datum
vector_spherical_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	double		distance;

	CheckDims(a, b);

	distance = (double) VectorInnerProduct(a->dim, a->x, b->x);

	/* Prevent NaN with acos with loss of precision */
	if (distance > 1)
		distance = 1;
	else if (distance < -1)
		distance = -1;

	PG_RETURN_FLOAT8(acos(distance) / M_PI);
}

/* Does not require FMA, but keep logic simple */
VECTOR_TARGET_CLONES static float
VectorL1Distance(int dim, float *ax, float *bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += fabsf(ax[i] - bx[i]);

	return distance;
}

/*
 * Get the L1 distance between two vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(l1_distance);
Datum
l1_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) VectorL1Distance(a->dim, a->x, b->x));
}

/*
 * Get the dimensions of a vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_dims);
Datum
vector_dims(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);

	PG_RETURN_INT32(a->dim);
}

/*
 * Get the L2 norm of a vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_norm);
Datum
vector_norm(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	float	   *ax = a->x;
	double		norm = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
		norm += (double) ax[i] * (double) ax[i];

	PG_RETURN_FLOAT8(sqrt(norm));
}

/*
 * Normalize a vector with the L2 norm
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(l2_normalize);
Datum
l2_normalize(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	float	   *ax = a->x;
	double		norm = 0;
	Vector	   *result;
	float	   *rx;

	result = InitVector(a->dim);
	rx = result->x;

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
		norm += (double) ax[i] * (double) ax[i];

	norm = sqrt(norm);

	/* Return zero vector for zero norm */
	if (norm > 0)
	{
		for (int i = 0; i < a->dim; i++)
			rx[i] = ax[i] / norm;

		/* Check for overflow */
		for (int i = 0; i < a->dim; i++)
		{
			if (isinf(rx[i]))
				float_overflow_error();
		}
	}

	PG_RETURN_POINTER(result);
}

/*
 * Add vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_add);
Datum
vector_add(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	float	   *ax = a->x;
	float	   *bx = b->x;
	Vector	   *result;
	float	   *rx;

	CheckDims(a, b);

	result = InitVector(a->dim);
	rx = result->x;

	/* Auto-vectorized */
	for (int i = 0, imax = a->dim; i < imax; i++)
		rx[i] = ax[i] + bx[i];

	/* Check for overflow */
	for (int i = 0, imax = a->dim; i < imax; i++)
	{
		if (isinf(rx[i]))
			float_overflow_error();
	}

	PG_RETURN_POINTER(result);
}

/*
 * Subtract vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_sub);
Datum
vector_sub(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	float	   *ax = a->x;
	float	   *bx = b->x;
	Vector	   *result;
	float	   *rx;

	CheckDims(a, b);

	result = InitVector(a->dim);
	rx = result->x;

	/* Auto-vectorized */
	for (int i = 0, imax = a->dim; i < imax; i++)
		rx[i] = ax[i] - bx[i];

	/* Check for overflow */
	for (int i = 0, imax = a->dim; i < imax; i++)
	{
		if (isinf(rx[i]))
			float_overflow_error();
	}

	PG_RETURN_POINTER(result);
}

/*
 * Multiply vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_mul);
Datum
vector_mul(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	float	   *ax = a->x;
	float	   *bx = b->x;
	Vector	   *result;
	float	   *rx;

	CheckDims(a, b);

	result = InitVector(a->dim);
	rx = result->x;

	/* Auto-vectorized */
	for (int i = 0, imax = a->dim; i < imax; i++)
		rx[i] = ax[i] * bx[i];

	/* Check for overflow and underflow */
	for (int i = 0, imax = a->dim; i < imax; i++)
	{
		if (isinf(rx[i]))
			float_overflow_error();

		if (rx[i] == 0 && !(ax[i] == 0 || bx[i] == 0))
			float_underflow_error();
	}

	PG_RETURN_POINTER(result);
}

/*
 * Concatenate vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_concat);
Datum
vector_concat(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	Vector	   *result;
	int			dim = a->dim + b->dim;

	CheckDim(dim);
	result = InitVector(dim);

	for (int i = 0; i < a->dim; i++)
		result->x[i] = a->x[i];

	for (int i = 0; i < b->dim; i++)
		result->x[i + a->dim] = b->x[i];

	PG_RETURN_POINTER(result);
}

/*
 * Quantize a vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(binary_quantize);
Datum
binary_quantize(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	float	   *ax = a->x;
	VarBit	   *result = InitBitVector(a->dim);
	unsigned char *rx = VARBITS(result);

	for (int i = 0; i < a->dim; i++)
		rx[i / 8] |= (ax[i] > 0) << (7 - (i % 8));

	PG_RETURN_VARBIT_P(result);
}

/*
 * Get a subvector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(subvector);
Datum
subvector(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	int32		start = PG_GETARG_INT32(1);
	int32		count = PG_GETARG_INT32(2);
	int32		end;
	float	   *ax = a->x;
	Vector	   *result;
	int			dim;

	if (count < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector must have at least 1 dimension")));

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
				 errmsg("vector must have at least 1 dimension")));

	dim = end - start;
	CheckDim(dim);
	result = InitVector(dim);

	for (int i = 0; i < dim; i++)
		result->x[i] = ax[start - 1 + i];

	PG_RETURN_POINTER(result);
}

/*
 * Internal helper to compare vectors
 */
int
vector_cmp_internal(Vector * a, Vector * b)
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
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_lt);
Datum
vector_lt(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) < 0);
}

/*
 * Less than or equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_le);
Datum
vector_le(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) <= 0);
}

/*
 * Equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_eq);
Datum
vector_eq(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) == 0);
}

/*
 * Not equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_ne);
Datum
vector_ne(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) != 0);
}

/*
 * Greater than or equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_ge);
Datum
vector_ge(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) >= 0);
}

/*
 * Greater than
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_gt);
Datum
vector_gt(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) > 0);
}

/*
 * Compare vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_cmp);
Datum
vector_cmp(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_INT32(vector_cmp_internal(a, b));
}

/*
 * Accumulate vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_accum);
Datum
vector_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *statearray = PG_GETARG_ARRAYTYPE_P(0);
	Vector	   *newval = PG_GETARG_VECTOR_P(1);
	float8	   *statevalues;
	int16		dim;
	bool		newarr;
	float8		n;
	Datum	   *statedatums;
	float	   *x = newval->x;
	ArrayType  *result;

	/* Check array before using */
	statevalues = CheckStateArray(statearray, "vector_accum");
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
			double		v = statevalues[i + 1] + x[i];

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
 * Combine vectors or half vectors (also used for halfvec_combine)
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_combine);
Datum
vector_combine(PG_FUNCTION_ARGS)
{
	/* Must also update parameters of halfvec_combine if modifying */
	ArrayType  *statearray1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *statearray2 = PG_GETARG_ARRAYTYPE_P(1);
	float8	   *statevalues1;
	float8	   *statevalues2;
	float8		n;
	float8		n1;
	float8		n2;
	int16		dim;
	Datum	   *statedatums;
	ArrayType  *result;

	/* Check arrays before using */
	statevalues1 = CheckStateArray(statearray1, "vector_combine");
	statevalues2 = CheckStateArray(statearray2, "vector_combine");

	n1 = statevalues1[0];
	n2 = statevalues2[0];

	if (n1 == 0.0)
	{
		n = n2;
		dim = STATE_DIMS(statearray2);
		statedatums = CreateStateDatums(dim);
		for (int i = 1; i <= dim; i++)
			statedatums[i] = Float8GetDatum(statevalues2[i]);
	}
	else if (n2 == 0.0)
	{
		n = n1;
		dim = STATE_DIMS(statearray1);
		statedatums = CreateStateDatums(dim);
		for (int i = 1; i <= dim; i++)
			statedatums[i] = Float8GetDatum(statevalues1[i]);
	}
	else
	{
		n = n1 + n2;
		dim = STATE_DIMS(statearray1);
		CheckExpectedDim(dim, STATE_DIMS(statearray2));
		statedatums = CreateStateDatums(dim);
		for (int i = 1; i <= dim; i++)
		{
			double		v = statevalues1[i] + statevalues2[i];

			/* Check for overflow */
			if (isinf(v))
				float_overflow_error();

			statedatums[i] = Float8GetDatum(v);
		}
	}

	statedatums[0] = Float8GetDatum(n);

	result = construct_array(statedatums, dim + 1,
							 FLOAT8OID,
							 sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

	pfree(statedatums);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * Average vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_avg);
Datum
vector_avg(PG_FUNCTION_ARGS)
{
	ArrayType  *statearray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *statevalues;
	float8		n;
	uint16		dim;
	Vector	   *result;

	/* Check array before using */
	statevalues = CheckStateArray(statearray, "vector_avg");
	n = statevalues[0];

	/* SQL defines AVG of no values to be NULL */
	if (n == 0.0)
		PG_RETURN_NULL();

	/* Create vector */
	dim = STATE_DIMS(statearray);
	CheckDim(dim);
	result = InitVector(dim);
	for (int i = 0; i < dim; i++)
	{
		result->x[i] = statevalues[i + 1] / n;
		CheckElement(result->x[i]);
	}

	PG_RETURN_POINTER(result);
}

/*
 * Convert sparse vector to dense vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(sparsevec_to_vector);
Datum
sparsevec_to_vector(PG_FUNCTION_ARGS)
{
	SparseVector *svec = PG_GETARG_SPARSEVEC_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	Vector	   *result;
	int			dim = svec->dim;
	float	   *values = SPARSEVEC_VALUES(svec);

	CheckDim(dim);
	CheckExpectedDim(typmod, dim);

	result = InitVector(dim);
	for (int i = 0; i < svec->nnz; i++)
		result->x[svec->indices[i]] = values[i];

	PG_RETURN_POINTER(result);
}
