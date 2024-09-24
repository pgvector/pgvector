#include "postgres.h"

#include <math.h>

#include "bitvec.h"
#include "catalog/pg_type.h"
#include "common/shortest_dec.h"
#include "fmgr.h"
#include "minivec.h"
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

/*
 * Ensure same dimensions
 */
static inline void
CheckDims(MiniVector * a, MiniVector * b)
{
	if (a->dim != b->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different minivec dimensions %d and %d", a->dim, b->dim)));
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
				 errmsg("minivec must have at least 1 dimension")));

	if (dim > MINIVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("minivec cannot have more than %d dimensions", MINIVEC_MAX_DIM)));
}

/*
 * Ensure finite element
 */
static inline void
CheckElement(fp8 value)
{
	if (Fp8IsNan(value))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("NaN not allowed in minivec")));
}

/*
 * Allocate and initialize a new fp8 vector
 */
MiniVector *
InitMiniVector(int dim)
{
	MiniVector *result;
	int			size;

	size = MINIVEC_SIZE(dim);
	result = (MiniVector *) palloc0(size);
	SET_VARSIZE(result, size);
	result->dim = dim;

	return result;
}

/*
 * Check for whitespace, since array_isspace() is static
 */
static inline bool
minivec_isspace(char ch)
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
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_in);
Datum
minivec_in(PG_FUNCTION_ARGS)
{
	char	   *lit = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	fp8			x[MINIVEC_MAX_DIM];
	int			dim = 0;
	char	   *pt = lit;
	MiniVector *result;

	while (minivec_isspace(*pt))
		pt++;

	if (*pt != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type minivec: \"%s\"", lit),
				 errdetail("Vector contents must start with \"[\".")));

	pt++;

	while (minivec_isspace(*pt))
		pt++;

	if (*pt == ']')
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("minivec must have at least 1 dimension")));

	for (;;)
	{
		float		val;
		char	   *stringEnd;

		if (dim == MINIVEC_MAX_DIM)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("minivec cannot have more than %d dimensions", MINIVEC_MAX_DIM)));

		while (minivec_isspace(*pt))
			pt++;

		/* Check for empty string like float4in */
		if (*pt == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type minivec: \"%s\"", lit)));

		errno = 0;

		/* Postgres sets LC_NUMERIC to C on startup */
		val = strtof(pt, &stringEnd);

		if (stringEnd == pt)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type minivec: \"%s\"", lit)));

		x[dim] = Float4ToFp8Unchecked(val);

		/* Check for range error like float4in */
		if ((errno == ERANGE && isinf(val)) || (Fp8IsNan(x[dim]) && !isnan(val)))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("\"%s\" is out of range for type minivec", pnstrdup(pt, stringEnd - pt))));

		CheckElement(x[dim]);
		dim++;

		pt = stringEnd;

		while (minivec_isspace(*pt))
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
					 errmsg("invalid input syntax for type minivec: \"%s\"", lit)));
	}

	/* Only whitespace is allowed after the closing brace */
	while (minivec_isspace(*pt))
		pt++;

	if (*pt != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type minivec: \"%s\"", lit),
				 errdetail("Junk after closing right brace.")));

	CheckDim(dim);
	CheckExpectedDim(typmod, dim);

	result = InitMiniVector(dim);
	for (int i = 0; i < dim; i++)
		result->x[i] = x[i];

	PG_RETURN_POINTER(result);
}

#define AppendChar(ptr, c) (*(ptr)++ = (c))
#define AppendFloat(ptr, f) ((ptr) += float_to_shortest_decimal_bufn((f), (ptr)))

/*
 * Convert internal representation to textual representation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_out);
Datum
minivec_out(PG_FUNCTION_ARGS)
{
	MiniVector *vector = PG_GETARG_MINIVEC_P(0);
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

		/*
		 * Use shortest decimal representation of single-precision float for
		 * simplicity
		 */
		AppendFloat(ptr, Fp8ToFloat4(vector->x[i]));
	}

	AppendChar(ptr, ']');
	*ptr = '\0';

	PG_FREE_IF_COPY(vector, 0);
	PG_RETURN_CSTRING(buf);
}

/*
 * Convert type modifier
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_typmod_in);
Datum
minivec_typmod_in(PG_FUNCTION_ARGS)
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
				 errmsg("dimensions for type minivec must be at least 1")));

	if (*tl > MINIVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type minivec cannot exceed %d", MINIVEC_MAX_DIM)));

	PG_RETURN_INT32(*tl);
}

/*
 * Convert external binary representation to internal representation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_recv);
Datum
minivec_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	MiniVector *result;
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

	result = InitMiniVector(dim);
	for (int i = 0; i < dim; i++)
	{
		result->x[i] = pq_getmsgint(buf, sizeof(uint8));
		CheckElement(result->x[i]);
	}

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to the external binary representation
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_send);
Datum
minivec_send(PG_FUNCTION_ARGS)
{
	MiniVector *vec = PG_GETARG_MINIVEC_P(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint(&buf, vec->dim, sizeof(int16));
	pq_sendint(&buf, vec->unused, sizeof(int16));
	for (int i = 0; i < vec->dim; i++)
		pq_sendint8(&buf, vec->x[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert fp8 vector to fp8 vector
 * This is needed to check the type modifier
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec);
Datum
minivec(PG_FUNCTION_ARGS)
{
	MiniVector *vec = PG_GETARG_MINIVEC_P(0);
	int32		typmod = PG_GETARG_INT32(1);

	CheckExpectedDim(typmod, vec->dim);

	PG_RETURN_POINTER(vec);
}

/*
 * Convert array to fp8 vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(array_to_minivec);
Datum
array_to_minivec(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	MiniVector *result;
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

	result = InitMiniVector(nelemsp);

	if (ARR_ELEMTYPE(array) == INT4OID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = Float4ToFp8(DatumGetInt32(elemsp[i]));
	}
	else if (ARR_ELEMTYPE(array) == FLOAT8OID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = Float4ToFp8(DatumGetFloat8(elemsp[i]));
	}
	else if (ARR_ELEMTYPE(array) == FLOAT4OID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = Float4ToFp8(DatumGetFloat4(elemsp[i]));
	}
	else if (ARR_ELEMTYPE(array) == NUMERICOID)
	{
		for (int i = 0; i < nelemsp; i++)
			result->x[i] = Float4ToFp8(DatumGetFloat4(DirectFunctionCall1(numeric_float4, elemsp[i])));
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
 * Convert fp8 vector to float4[]
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_to_float4);
Datum
minivec_to_float4(PG_FUNCTION_ARGS)
{
	MiniVector *vec = PG_GETARG_MINIVEC_P(0);
	Datum	   *datums;
	ArrayType  *result;

	datums = (Datum *) palloc(sizeof(Datum) * vec->dim);

	for (int i = 0; i < vec->dim; i++)
		datums[i] = Float4GetDatum(Fp8ToFloat4(vec->x[i]));

	/* Use TYPALIGN_INT for float4 */
	result = construct_array(datums, vec->dim, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);

	pfree(datums);

	PG_RETURN_POINTER(result);
}

/*
 * Convert vector to fp8 vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(vector_to_minivec);
Datum
vector_to_minivec(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	MiniVector *result;

	CheckDim(vec->dim);
	CheckExpectedDim(typmod, vec->dim);

	result = InitMiniVector(vec->dim);

	for (int i = 0; i < vec->dim; i++)
		result->x[i] = Float4ToFp8(vec->x[i]);

	PG_RETURN_POINTER(result);
}

static float
MinivecL2SquaredDistance(int dim, fp8 * ax, fp8 * bx)
{
	float		distance = 0.0;

	for (int i = 0; i < dim; i++)
	{
		float		diff = Fp8ToFloat4(ax[i]) - Fp8ToFloat4(bx[i]);

		distance += diff * diff;
	}

	return distance;
}

/*
 * Get the L2 distance between fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_l2_distance);
Datum
minivec_l2_distance(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8(sqrt((double) MinivecL2SquaredDistance(a->dim, a->x, b->x)));
}

/*
 * Get the L2 squared distance between fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_l2_squared_distance);
Datum
minivec_l2_squared_distance(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) MinivecL2SquaredDistance(a->dim, a->x, b->x));
}

static float
MinivecInnerProduct(int dim, fp8 * ax, fp8 * bx)
{
	float		distance = 0.0;

	for (int i = 0; i < dim; i++)
		distance += Fp8ToFloat4(ax[i]) * Fp8ToFloat4(bx[i]);

	return distance;
}

/*
 * Get the inner product of two fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_inner_product);
Datum
minivec_inner_product(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) MinivecInnerProduct(a->dim, a->x, b->x));
}

/*
 * Get the negative inner product of two fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_negative_inner_product);
Datum
minivec_negative_inner_product(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) -MinivecInnerProduct(a->dim, a->x, b->x));
}

static double
MinivecCosineSimilarity(int dim, fp8 * ax, fp8 * bx)
{
	float		similarity = 0.0;
	float		norma = 0.0;
	float		normb = 0.0;

	for (int i = 0; i < dim; i++)
	{
		float		axi = Fp8ToFloat4(ax[i]);
		float		bxi = Fp8ToFloat4(bx[i]);

		similarity += axi * bxi;
		norma += axi * axi;
		normb += bxi * bxi;
	}

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
}

/*
 * Get the cosine distance between two fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_cosine_distance);
Datum
minivec_cosine_distance(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);
	double		similarity;

	CheckDims(a, b);

	similarity = MinivecCosineSimilarity(a->dim, a->x, b->x);

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
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_spherical_distance);
Datum
minivec_spherical_distance(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);
	double		distance;

	CheckDims(a, b);

	distance = (double) MinivecInnerProduct(a->dim, a->x, b->x);

	/* Prevent NaN with acos with loss of precision */
	if (distance > 1)
		distance = 1;
	else if (distance < -1)
		distance = -1;

	PG_RETURN_FLOAT8(acos(distance) / M_PI);
}

static float
MinivecL1Distance(int dim, fp8 * ax, fp8 * bx)
{
	float		distance = 0.0;

	for (int i = 0; i < dim; i++)
		distance += fabsf(Fp8ToFloat4(ax[i]) - Fp8ToFloat4(bx[i]));

	return distance;
}

/*
 * Get the L1 distance between two fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_l1_distance);
Datum
minivec_l1_distance(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8((double) MinivecL1Distance(a->dim, a->x, b->x));
}

/*
 * Get the dimensions of a fp8 vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_vector_dims);
Datum
minivec_vector_dims(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);

	PG_RETURN_INT32(a->dim);
}

/*
 * Get the L2 norm of a fp8 vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_l2_norm);
Datum
minivec_l2_norm(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	fp8		   *ax = a->x;
	double		norm = 0.0;

	for (int i = 0; i < a->dim; i++)
	{
		double		axi = (double) Fp8ToFloat4(ax[i]);

		norm += axi * axi;
	}

	PG_RETURN_FLOAT8(sqrt(norm));
}

/*
 * Normalize a fp8 vector with the L2 norm
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_l2_normalize);
Datum
minivec_l2_normalize(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	fp8		   *ax = a->x;
	double		norm = 0;
	MiniVector *result;
	fp8		   *rx;

	result = InitMiniVector(a->dim);
	rx = result->x;

	for (int i = 0; i < a->dim; i++)
		norm += (double) Fp8ToFloat4(ax[i]) * (double) Fp8ToFloat4(ax[i]);

	norm = sqrt(norm);

	/* Return zero vector for zero norm */
	if (norm > 0)
	{
		for (int i = 0; i < a->dim; i++)
			rx[i] = Float4ToFp8Unchecked(Fp8ToFloat4(ax[i]) / norm);

		/* Check for overflow */
		for (int i = 0; i < a->dim; i++)
		{
			if (Fp8IsNan(rx[i]))
				float_overflow_error();
		}
	}

	PG_RETURN_POINTER(result);
}

/*
 * Add fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_add);
Datum
minivec_add(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);
	fp8		   *ax = a->x;
	fp8		   *bx = b->x;
	MiniVector *result;
	fp8		   *rx;

	CheckDims(a, b);

	result = InitMiniVector(a->dim);
	rx = result->x;

	for (int i = 0, imax = a->dim; i < imax; i++)
		rx[i] = Float4ToFp8Unchecked(Fp8ToFloat4(ax[i]) + Fp8ToFloat4(bx[i]));

	/* Check for overflow */
	for (int i = 0, imax = a->dim; i < imax; i++)
	{
		if (Fp8IsNan(rx[i]))
			float_overflow_error();
	}

	PG_RETURN_POINTER(result);
}

/*
 * Subtract fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_sub);
Datum
minivec_sub(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);
	fp8		   *ax = a->x;
	fp8		   *bx = b->x;
	MiniVector *result;
	fp8		   *rx;

	CheckDims(a, b);

	result = InitMiniVector(a->dim);
	rx = result->x;

	for (int i = 0, imax = a->dim; i < imax; i++)
		rx[i] = Float4ToFp8Unchecked(Fp8ToFloat4(ax[i]) - Fp8ToFloat4(bx[i]));

	/* Check for overflow */
	for (int i = 0, imax = a->dim; i < imax; i++)
	{
		if (Fp8IsNan(rx[i]))
			float_overflow_error();
	}

	PG_RETURN_POINTER(result);
}

/*
 * Multiply fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_mul);
Datum
minivec_mul(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);
	fp8		   *ax = a->x;
	fp8		   *bx = b->x;
	MiniVector *result;
	fp8		   *rx;

	CheckDims(a, b);

	result = InitMiniVector(a->dim);
	rx = result->x;

	for (int i = 0, imax = a->dim; i < imax; i++)
		rx[i] = Float4ToFp8Unchecked(Fp8ToFloat4(ax[i]) * Fp8ToFloat4(bx[i]));

	/* Check for overflow and underflow */
	for (int i = 0, imax = a->dim; i < imax; i++)
	{
		if (Fp8IsNan(rx[i]))
			float_overflow_error();

		if (Fp8IsZero(rx[i]) && !(Fp8IsZero(ax[i]) || Fp8IsZero(bx[i])))
			float_underflow_error();
	}

	PG_RETURN_POINTER(result);
}

/*
 * Concatenate fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_concat);
Datum
minivec_concat(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);
	MiniVector *result;
	int			dim = a->dim + b->dim;

	CheckDim(dim);
	result = InitMiniVector(dim);

	for (int i = 0; i < a->dim; i++)
		result->x[i] = a->x[i];

	for (int i = 0; i < b->dim; i++)
		result->x[i + a->dim] = b->x[i];

	PG_RETURN_POINTER(result);
}

/*
 * Quantize a fp8 vector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_binary_quantize);
Datum
minivec_binary_quantize(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	fp8		   *ax = a->x;
	VarBit	   *result = InitBitVector(a->dim);
	unsigned char *rx = VARBITS(result);

	for (int i = 0; i < a->dim; i++)
		rx[i / 8] |= (Fp8ToFloat4(ax[i]) > 0) << (7 - (i % 8));

	PG_RETURN_VARBIT_P(result);
}

/*
 * Get a subvector
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_subvector);
Datum
minivec_subvector(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	int32		start = PG_GETARG_INT32(1);
	int32		count = PG_GETARG_INT32(2);
	int32		end;
	fp8		   *ax = a->x;
	MiniVector *result;
	int32		dim;

	if (count < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("minivec must have at least 1 dimension")));

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
				 errmsg("minivec must have at least 1 dimension")));

	dim = end - start;
	CheckDim(dim);
	result = InitMiniVector(dim);

	for (int i = 0; i < dim; i++)
		result->x[i] = ax[start - 1 + i];

	PG_RETURN_POINTER(result);
}

/*
 * Internal helper to compare fp8 vectors
 */
static int
minivec_cmp_internal(MiniVector * a, MiniVector * b)
{
	int			dim = Min(a->dim, b->dim);

	/* Check values before dimensions to be consistent with Postgres arrays */
	for (int i = 0; i < dim; i++)
	{
		if (Fp8ToFloat4(a->x[i]) < Fp8ToFloat4(b->x[i]))
			return -1;

		if (Fp8ToFloat4(a->x[i]) > Fp8ToFloat4(b->x[i]))
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
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_lt);
Datum
minivec_lt(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	PG_RETURN_BOOL(minivec_cmp_internal(a, b) < 0);
}

/*
 * Less than or equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_le);
Datum
minivec_le(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	PG_RETURN_BOOL(minivec_cmp_internal(a, b) <= 0);
}

/*
 * Equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_eq);
Datum
minivec_eq(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	PG_RETURN_BOOL(minivec_cmp_internal(a, b) == 0);
}

/*
 * Not equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_ne);
Datum
minivec_ne(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	PG_RETURN_BOOL(minivec_cmp_internal(a, b) != 0);
}

/*
 * Greater than or equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_ge);
Datum
minivec_ge(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	PG_RETURN_BOOL(minivec_cmp_internal(a, b) >= 0);
}

/*
 * Greater than
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_gt);
Datum
minivec_gt(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	PG_RETURN_BOOL(minivec_cmp_internal(a, b) > 0);
}

/*
 * Compare fp8 vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(minivec_cmp);
Datum
minivec_cmp(PG_FUNCTION_ARGS)
{
	MiniVector *a = PG_GETARG_MINIVEC_P(0);
	MiniVector *b = PG_GETARG_MINIVEC_P(1);

	PG_RETURN_INT32(minivec_cmp_internal(a, b));
}
