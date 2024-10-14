#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "catalog/pg_type.h"
#include "fmgr.h"
#include "intvec.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/*
 * Ensure same dimensions
 */
static inline void
CheckDims(IntVector * a, IntVector * b)
{
	if (a->dim != b->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different intvec dimensions %d and %d", a->dim, b->dim)));
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
				 errmsg("intvec must have at least 1 dimension")));

	if (dim > INTVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("intvec cannot have more than %d dimensions", INTVEC_MAX_DIM)));
}

/*
 * Ensure element in range
 */
static inline void
CheckElement(long value)
{
	if (value < SCHAR_MIN || value > SCHAR_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%ld\" is out of range for type intvec", value)));
}

/*
 * Allocate and initialize a new int vector
 */
IntVector *
InitIntVector(int dim)
{
	IntVector  *result;
	int			size;

	size = INTVEC_SIZE(dim);
	result = (IntVector *) palloc0(size);
	SET_VARSIZE(result, size);
	result->dim = dim;

	return result;
}

/*
 * Check for whitespace, since array_isspace() is static
 */
static inline bool
intvec_isspace(char ch)
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
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_in);
Datum
intvec_in(PG_FUNCTION_ARGS)
{
	char	   *lit = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	int8		x[INTVEC_MAX_DIM];
	int			dim = 0;
	char	   *pt = lit;
	IntVector  *result;

	while (intvec_isspace(*pt))
		pt++;

	if (*pt != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type intvec: \"%s\"", lit),
				 errdetail("Vector contents must start with \"[\".")));

	pt++;

	while (intvec_isspace(*pt))
		pt++;

	if (*pt == ']')
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("intvec must have at least 1 dimension")));

	for (;;)
	{
		long		val;
		char	   *stringEnd;

		if (dim == INTVEC_MAX_DIM)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("intvec cannot have more than %d dimensions", VECTOR_MAX_DIM)));

		while (intvec_isspace(*pt))
			pt++;

		/* Check for empty string like float4in */
		if (*pt == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type intvec: \"%s\"", lit)));

		errno = 0;

		/* Use similar logic as int2vectorin */
		val = strtol(pt, &stringEnd, 10);

		if (stringEnd == pt)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type intvec: \"%s\"", lit)));

		/* Check for range error like float4in */
		if (errno == ERANGE || val < SCHAR_MIN || val > SCHAR_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("\"%s\" is out of range for type intvec", pnstrdup(pt, stringEnd - pt))));

		CheckElement(val);
		x[dim++] = val;

		pt = stringEnd;

		while (intvec_isspace(*pt))
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
					 errmsg("invalid input syntax for type intvec: \"%s\"", lit)));
	}

	/* Only whitespace is allowed after the closing brace */
	while (intvec_isspace(*pt))
		pt++;

	if (*pt != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type intvec: \"%s\"", lit),
				 errdetail("Junk after closing right brace.")));

	CheckDim(dim);
	CheckExpectedDim(typmod, dim);

	result = InitIntVector(dim);
	for (int i = 0; i < dim; i++)
		result->x[i] = x[i];

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to textual representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_out);
Datum
intvec_out(PG_FUNCTION_ARGS)
{
	IntVector  *vector = PG_GETARG_INTVEC_P(0);
	int			dim = vector->dim;
	char	   *buf;
	char	   *ptr;

	/*
	 * Need:
	 *
	 * dim * 4 bytes for elements (-128 to 127)
	 *
	 * dim - 1 bytes for separator
	 *
	 * 3 bytes for [, ], and \0
	 */
	buf = (char *) palloc(5 * dim + 2);
	ptr = buf;

	*ptr = '[';
	ptr++;
	for (int i = 0; i < dim; i++)
	{
		if (i > 0)
		{
			*ptr = ',';
			ptr++;
		}

#if PG_VERSION_NUM >= 140000
		ptr += pg_ltoa(vector->x[i], ptr);
#else
		pg_ltoa(vector->x[i], ptr);
		while (*ptr != '\0')
			ptr++;
#endif
	}
	*ptr = ']';
	ptr++;
	*ptr = '\0';

	PG_FREE_IF_COPY(vector, 0);
	PG_RETURN_CSTRING(buf);
}

/*
 * Convert type modifier
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_typmod_in);
Datum
intvec_typmod_in(PG_FUNCTION_ARGS)
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
				 errmsg("dimensions for type intvec must be at least 1")));

	if (*tl > INTVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type intvec cannot exceed %d", INTVEC_MAX_DIM)));

	PG_RETURN_INT32(*tl);
}

/*
 * Convert external binary representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_recv);
Datum
intvec_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	IntVector  *result;
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

	result = InitIntVector(dim);
	for (int i = 0; i < dim; i++)
		result->x[i] = pq_getmsgint(buf, sizeof(int8));

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to the external binary representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_send);
Datum
intvec_send(PG_FUNCTION_ARGS)
{
	IntVector  *vec = PG_GETARG_INTVEC_P(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint(&buf, vec->dim, sizeof(int16));
	pq_sendint(&buf, vec->unused, sizeof(int16));
	for (int i = 0; i < vec->dim; i++)
		pq_sendint8(&buf, vec->x[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert int vector to int vector
 * This is needed to check the type modifier
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec);
Datum
intvec(PG_FUNCTION_ARGS)
{
	IntVector  *vec = PG_GETARG_INTVEC_P(0);
	int32		typmod = PG_GETARG_INT32(1);

	CheckExpectedDim(typmod, vec->dim);

	PG_RETURN_POINTER(vec);
}

/*
 * Convert array to intvec vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(array_to_intvec);
Datum
array_to_intvec(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	IntVector  *result;
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

	result = InitIntVector(nelemsp);

	if (ARR_ELEMTYPE(array) == INT4OID)
	{
		for (int i = 0; i < nelemsp; i++)
		{
			long		l = DatumGetInt32(elemsp[i]);

			CheckElement(l);

			result->x[i] = l;
		}
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

	PG_RETURN_POINTER(result);
}

/*
 * Get the L2 distance between int vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_l2_distance);
Datum
intvec_l2_distance(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);
	int8	   *ax = a->x;
	int8	   *bx = b->x;
	int			distance = 0;

	CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
	{
		int			diff = ax[i] - bx[i];

		distance += diff * diff;
	}

	PG_RETURN_FLOAT8(sqrt((double) distance));
}

/*
 * Get the L2 squared distance between int vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_l2_squared_distance);
Datum
intvec_l2_squared_distance(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);
	int8	   *ax = a->x;
	int8	   *bx = b->x;
	int			distance = 0;

	CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
	{
		int			diff = ax[i] - bx[i];

		distance += diff * diff;
	}

	PG_RETURN_FLOAT8((double) distance);
}

/*
 * Get the inner product of two int vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_inner_product);
Datum
intvec_inner_product(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);
	int8	   *ax = a->x;
	int8	   *bx = b->x;
	int			distance = 0;

	CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
		distance += ax[i] * bx[i];

	PG_RETURN_FLOAT8((double) distance);
}

/*
 * Get the negative inner product of two int vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_negative_inner_product);
Datum
intvec_negative_inner_product(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);
	int8	   *ax = a->x;
	int8	   *bx = b->x;
	int			distance = 0;

	CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
		distance += ax[i] * bx[i];

	PG_RETURN_FLOAT8((double) -distance);
}

/*
 * Get the cosine distance between two int vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_cosine_distance);
Datum
intvec_cosine_distance(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);
	int8	   *ax = a->x;
	int8	   *bx = b->x;
	int			distance = 0;
	int			norma = 0;
	int			normb = 0;
	double		similarity;

	CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
	{
		int8		axi = ax[i];
		int8		bxi = bx[i];

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
 * Get the L1 distance between two int vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_l1_distance);
Datum
intvec_l1_distance(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);
	int8	   *ax = a->x;
	int8	   *bx = b->x;
	int			distance = 0;

	CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
		distance += abs(ax[i] - bx[i]);

	PG_RETURN_FLOAT8((double) distance);
}

/*
 * Get the L2 norm of an int vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(intvec_l2_norm);
Datum
intvec_l2_norm(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	int8	   *ax = a->x;
	int			norm = 0;

	/* Auto-vectorized */
	for (int i = 0; i < a->dim; i++)
		norm += ax[i] * ax[i];

	PG_RETURN_FLOAT8(sqrt((double) norm));
}

/*
 * Internal helper to compare int vectors
 */
static int
intvec_cmp_internal(IntVector * a, IntVector * b)
{
	int			dim = Min(a->dim, b->dim);

	/* Check values before dimensions to be consistent with Postgres arrays */
	for (int i = 0; i < dim; i++)
	{
		if ((int) a->x[i] < (int) b->x[i])
			return -1;

		if ((int) a->x[i] > (int) b->x[i])
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
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(intvec_lt);
Datum
intvec_lt(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);

	PG_RETURN_BOOL(intvec_cmp_internal(a, b) < 0);
}

/*
 * Less than or equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(intvec_le);
Datum
intvec_le(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);

	PG_RETURN_BOOL(intvec_cmp_internal(a, b) <= 0);
}

/*
 * Equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(intvec_eq);
Datum
intvec_eq(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);

	PG_RETURN_BOOL(intvec_cmp_internal(a, b) == 0);
}

/*
 * Not equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(intvec_ne);
Datum
intvec_ne(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);

	PG_RETURN_BOOL(intvec_cmp_internal(a, b) != 0);
}

/*
 * Greater than or equal
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(intvec_ge);
Datum
intvec_ge(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);

	PG_RETURN_BOOL(intvec_cmp_internal(a, b) >= 0);
}

/*
 * Greater than
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(intvec_gt);
Datum
intvec_gt(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);

	PG_RETURN_BOOL(intvec_cmp_internal(a, b) > 0);
}

/*
 * Compare int vectors
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(intvec_cmp);
Datum
intvec_cmp(PG_FUNCTION_ARGS)
{
	IntVector  *a = PG_GETARG_INTVEC_P(0);
	IntVector  *b = PG_GETARG_INTVEC_P(1);

	PG_RETURN_INT32(intvec_cmp_internal(a, b));
}
