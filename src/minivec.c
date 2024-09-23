#include "postgres.h"

#include <math.h>

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
 * Allocate and initialize a new half vector
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

static float
MinivecL2SquaredDistance(int dim, fp8 * ax, fp8 * bx)
{
	float		distance = 0.0;

	/* Auto-vectorized */
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
