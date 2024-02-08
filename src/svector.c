#include "postgres.h"

#include <float.h>
#include <math.h>

#include "catalog/pg_type.h"
#include "fmgr.h"
#include "shnsw.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "port.h"				/* for strtof() */
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "svector.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "common/shortest_dec.h"
#include "utils/float.h"
#endif

#if PG_VERSION_NUM < 130000
#define TYPALIGN_DOUBLE 'd'
#define TYPALIGN_INT 'i'
#endif

// First state elem is n, second state elem is dim
#define STATE_ELEM_INDEX(i) i * 2 + 2
#define STATE_ELEM_VALUE(i) i * 2 + 3
#define STATE_N_ELEM(x) (ARR_DIMS(x)[0] - 2) / 2 // In pairs: index, value
#define CreateStateDatums(n_elem) palloc(sizeof(Datum) * (n_elem * 2 + 1))

PG_MODULE_MAGIC;

/*
 * Initialize index options and variables
 */
PGDLLEXPORT void _PG_init(void);
void
_PG_init(void)
{
	HnswInit();
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
				 errmsg("different svector dimensions %d and %d", a->dim, b->dim)));
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
 * Ensure expected non-zero elements
 */
static inline void
CheckExpectedNElem(int32 typmod, int n_elem)
{
	if (typmod != -1 && typmod != n_elem)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("expected %d non-zero elements, not %d", typmod, n_elem)));
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
				 errmsg("svector must have at least 1 dimension")));
}

/*
 * Ensure valid n elements
 */
static inline void
CheckNElem(int n_elem)
{
	if (n_elem > VECTOR_MAX_N)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("vector cannot have more than %d non-zero elements", VECTOR_MAX_N)));
}

/*
 * Ensure finite elements
 */
static inline void
CheckElement(VecEl element)
{
	if (isnan(element.value))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("NaN not allowed in svector")));

	if (isinf(element.value))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("infinite value not allowed in svector")));
}

/*
 * Allocate and initialize a new svector
 */
Vector *
InitVector(int32 n_elem, int dim)
{
	Vector	   *result;
	int			size;

	size = VECTOR_SIZE(n_elem);
	result = (Vector *) palloc0(size);
	SET_VARSIZE(result, size);
	result->n_elem = n_elem;
	result->dim = dim;

	return result;
}

/*
 * Check for whitespace, since array_isspace() is static
 */
static inline bool
svectorisspace(char ch)
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
		ARR_DIMS(statearray)[0] < 2 || // First elem is n, second elem is dim
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
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_in);
Datum
svector_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	VecEl		x[VECTOR_MAX_N];
	int			n_elem = 0;
	int 		dim = 0;
	char	   *pt;
	char	   *stringEnd;
	Vector	   *result;
	char	   *lit = pstrdup(str);

	while (svectorisspace(*str))
		str++;

	if (*str != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed svector literal: \"%s\"", lit),
				 errdetail("Vector contents must start with \"[\".")));

	str++;
	pt = strtok(str, ",");
	stringEnd = pt;

	while (pt != NULL && *stringEnd != ']')
	{
		while (svectorisspace(*pt))
			pt++;

		/* Check for empty string like float4in */
		if (*pt == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type svector: \"%s\"", lit)));

		float value = strtof(pt, &stringEnd);
		if (value != 0) {
			if (n_elem == VECTOR_MAX_N)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("svector cannot have more than %d non-zero elements", VECTOR_MAX_N)));

			/* Use strtof like float4in to avoid a double-rounding problem */
			x[n_elem].index = dim;
			x[n_elem].value = strtof(pt, &stringEnd);
			CheckElement(x[n_elem]);
			n_elem++;
		}

		if (stringEnd == pt)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type svector: \"%s\"", lit)));

		while (svectorisspace(*stringEnd))
			stringEnd++;

		if (*stringEnd != '\0' && *stringEnd != ']')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type svector: \"%s\"", lit)));

		pt = strtok(NULL, ",");
		dim++;
	}

	if (stringEnd == NULL || *stringEnd != ']')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed svector literal: \"%s\"", lit),
				 errdetail("Unexpected end of input.")));

	stringEnd++;

	/* Only whitespace is allowed after the closing brace */
	while (svectorisspace(*stringEnd))
		stringEnd++;

	if (*stringEnd != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed svector literal: \"%s\"", lit),
				 errdetail("Junk after closing right brace.")));

	/* Ensure no consecutive delimiters since strtok skips */
	for (pt = lit + 1; *pt != '\0'; pt++)
	{
		if (pt[-1] == ',' && *pt == ',')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed svector literal: \"%s\"", lit)));
	}

	if (dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("svector must have at least 1 dimension")));

	pfree(lit);

	CheckExpectedDim(typmod, dim);

	result = InitVector(n_elem, dim);
	for (int i = 0; i < n_elem; i++) {
		result->x[i].value = x[i].value;
		result->x[i].index = x[i].index;
	}

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to textual representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_out);
Datum
svector_out(PG_FUNCTION_ARGS)
{
	Vector	   *svector = PG_GETARG_VECTOR_P(0);
	int32       n_elem = svector->n_elem;
	int32		dim = svector->dim;
	char	   *buf;
	char	   *ptr;
	int			n;

#if PG_VERSION_NUM < 120000
	int			ndig = FLT_DIG + extra_float_digits;

	if (ndig < 1)
		ndig = 1;

#define FLOAT_SHORTEST_DECIMAL_LEN (ndig + 10)
#endif

	/*
	 * Need:
	 *
	 * n_elem * (FLOAT_SHORTEST_DECIMAL_LEN - 1) bytes for
	 * float_to_shortest_decimal_bufn
	 *
	 * dim - n_elem bytes for 0 entries
	 *
	 * dim - 1 bytes for separator
	 *
	 * 3 bytes for [, ], and \0
	 */
	buf = (char *) palloc(n_elem * (FLOAT_SHORTEST_DECIMAL_LEN - 1) + dim - n_elem + dim - 1 + 3);
	ptr = buf;

	*ptr = '[';
	ptr++;
	int32 curr_x_index = 0;
	for (int i = 0; i < dim; i++)
	{
		if (i > 0)
		{
			*ptr = ',';
			ptr++;
		}

		float value = 0;
		if (curr_x_index < n_elem && svector->x[curr_x_index].index == i) {
			value = svector->x[curr_x_index].value;
			curr_x_index++;
		}
#if PG_VERSION_NUM >= 120000
		n = float_to_shortest_decimal_bufn(value, ptr);
#else
		n = sprintf(ptr, "%.*g", ndig, value);
#endif
		ptr += n;
	}
	*ptr = ']';
	ptr++;
	*ptr = '\0';

	PG_FREE_IF_COPY(svector, 0);
	PG_RETURN_CSTRING(buf);
}

/*
 * Print svector - useful for debugging
 */
void
PrintVector(char *msg, Vector * svector)
{
	char	   *out = DatumGetPointer(DirectFunctionCall1(svector_out, PointerGetDatum(svector)));

	elog(INFO, "%s = %s", msg, out);
	pfree(out);
}

/*
 * Convert type modifier
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_typmod_in);
Datum
svector_typmod_in(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *tl;
	int			n;

	tl = ArrayGetIntegerTypmods(ta, &n);

	if (n != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid type modifier")));

	PG_RETURN_INT32(*tl);
}

/*
 * Convert external binary representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_recv);
Datum
svector_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	Vector	   *result;
	int16		n_elem;
	int16		dim;
	int16		unused;

	n_elem = pq_getmsgint(buf, sizeof(int16));
	dim = pq_getmsgint(buf, sizeof(int16));
	unused = pq_getmsgint(buf, sizeof(int16));

	CheckDim(dim);
	CheckNElem(n_elem);
	CheckExpectedDim(typmod, dim);

	if (unused != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("expected unused to be 0, not %d", unused)));

	result = InitVector(n_elem, dim);
	for (int i = 0; i < n_elem; i++)
	{
		result->x[i].index = pq_getmsgint(buf, sizeof(int32));
		result->x[i].value = pq_getmsgfloat4(buf);
		CheckElement(result->x[i]);
	}

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to the external binary representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_send);
Datum
svector_send(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint(&buf, vec->n_elem, sizeof(int16));
	pq_sendint(&buf, vec->dim, sizeof(int16));
	pq_sendint(&buf, vec->unused, sizeof(int16));
	for (int i = 0; i < vec->n_elem; i++) {
		pq_sendint(&buf, vec->x[i].index, sizeof(int32));
		pq_sendfloat4(&buf, vec->x[i].value);
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert svector to svector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector);
Datum
svector(PG_FUNCTION_ARGS)
{
	Vector	   *arg = PG_GETARG_VECTOR_P(0);
	int32		typmod = PG_GETARG_INT32(1);

	CheckExpectedDim(typmod, arg->dim);

	PG_RETURN_POINTER(arg);
}

/*
 * Convert array to svector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(array_to_svector);
Datum
array_to_svector(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32		typmod = PG_GETARG_INT32(1);
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

	if (ARR_HASNULL(array) && array_contains_nulls(array))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("array must not contain nulls")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
	deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, &nullsp, &nelemsp);

	CheckDim(nelemsp);
	CheckExpectedDim(typmod, nelemsp);
	int32 n_elem = 0;
	if (ARR_ELEMTYPE(array) == INT4OID)
	{
		for (int i = 0; i < nelemsp; i++) {
			if (elemsp[i] != 0)
				n_elem++;
		}
	}
	else if (ARR_ELEMTYPE(array) == FLOAT8OID || ARR_ELEMTYPE(array) == FLOAT4OID)
	{
		for (int i = 0; i < nelemsp; i++) {
			if (fabs(elemsp[i]) >= FLT_EPSILON)
				n_elem++;
		}
	}
	else if (ARR_ELEMTYPE(array) == NUMERICOID)
	{
		for (int i = 0; i < nelemsp; i++) {
			if (fabs(DirectFunctionCall1(numeric_float4, elemsp[i])) >= FLT_EPSILON)
				n_elem++;
		}
	}

	result = InitVector(n_elem, nelemsp);

	if (ARR_ELEMTYPE(array) == INT4OID)
	{
		int curr_x_index = 0;
		for (int i = 0; i < nelemsp; i++) {
			if (elemsp[i] == 0)
				continue;
			result->x[curr_x_index].index = i;
			result->x[curr_x_index].value = DatumGetInt32(elemsp[i]);
			curr_x_index++;
		}
	}
	else if (ARR_ELEMTYPE(array) == FLOAT8OID)
	{
		int curr_x_index = 0;
		for (int i = 0; i < nelemsp; i++) {
			if (fabs(elemsp[i]) < FLT_EPSILON)
				continue;
			result->x[curr_x_index].index = i;
			result->x[curr_x_index].value = DatumGetFloat8(elemsp[i]);
			curr_x_index++;
		}
	}
	else if (ARR_ELEMTYPE(array) == FLOAT4OID)
	{
		int curr_x_index = 0;
		for (int i = 0; i < nelemsp; i++) {
			if (fabs(elemsp[i]) < FLT_EPSILON)
				continue;
			result->x[curr_x_index].index = i;
			result->x[curr_x_index].value = DatumGetFloat4(elemsp[i]);
			curr_x_index++;
		}
	}
	else if (ARR_ELEMTYPE(array) == NUMERICOID)
	{
		int curr_x_index = 0;
		for (int i = 0; i < nelemsp; i++) {
			if (fabs(DirectFunctionCall1(numeric_float4, elemsp[i])) < FLT_EPSILON)
				continue;
			result->x[curr_x_index].index = i;
			result->x[curr_x_index].value = DatumGetFloat4(DirectFunctionCall1(numeric_float4, elemsp[i]));
			curr_x_index++;
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("unsupported array type")));
	}

	/* Check elements */
	for (int i = 0; i < result->n_elem; i++)
		CheckElement(result->x[i]);

	PG_RETURN_POINTER(result);
}

/*
 * Convert svector to float4[]
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_to_float4);
Datum
svector_to_float4(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	Datum	   *datums;
	ArrayType  *result;

	datums = (Datum *) palloc(sizeof(Datum) * vec->dim);

	int32 curr_x_index = 0;
	for (int i = 0; i < vec->dim; i++) {
		if (curr_x_index < vec->n_elem && i == vec->x[curr_x_index].index) {
			datums[i] = Float4GetDatum(vec->x[i].value);
			curr_x_index++;
		} else {
			datums[i] = Float4GetDatum(0);
		}
	}

	/* Use TYPALIGN_INT for float4 */
	result = construct_array(datums, vec->dim, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);

	pfree(datums);

	PG_RETURN_POINTER(result);
}

/*
 * Get the L2 distance between vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(l2_distance);
Datum
l2_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	VecEl	   *ax = a->x;
	VecEl	   *bx = b->x;
	float		distance = 0.0;

	CheckDims(a, b);

	/* Auto-vectorized */
	int a_i = 0;
	int b_i = 0;
	float a_value = 0.0;
	float b_value = 0.0;
	while (a_i < a->n_elem || b_i < b->n_elem) {
		if (a_i >= a->n_elem) {
			a_value = 0;
			b_value = bx[b_i].value;
			b_i++;
		} else if (b_i >= b->n_elem) {
			a_value = ax[a_i].value;
			b_value = 0;
			a_i++;
		} else {
			if (ax[a_i].index == bx[b_i].index) {
				a_value = ax[a_i].value;
				b_value = bx[b_i].value;
				a_i++;
				b_i++;
			} else if (ax[a_i].index < bx[b_i].index) {
				a_value = ax[a_i].value;
				b_value = 0;
				a_i++;
			} else if (bx[b_i].index < ax[a_i].index) {
				a_value = 0;
				b_value = bx[b_i].value;
				b_i++;
			}
		}
		distance += (a_value - b_value) * (a_value - b_value);
	}

	PG_RETURN_FLOAT8(sqrt((double) distance));
}

/*
 * Get the L2 squared distance between vectors
 * This saves a sqrt calculation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_l2_squared_distance);
Datum
svector_l2_squared_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	VecEl	   *ax = a->x;
	VecEl	   *bx = b->x;
	float		distance = 0.0;

	CheckDims(a, b);

	int a_i = 0;
	int b_i = 0;
	float a_value = 0.0;
	float b_value = 0.0;
	while (a_i < a->n_elem || b_i < b->n_elem) {
		if (a_i >= a->n_elem) {
			a_value = 0;
			b_value = bx[b_i].value;
			b_i++;
		} else if (b_i >= b->n_elem) {
			a_value = ax[a_i].value;
			b_value = 0;
			a_i++;
		} else {
			if (ax[a_i].index == bx[b_i].index) {
				a_value = ax[a_i].value;
				b_value = bx[b_i].value;
				a_i++;
				b_i++;
			} else if (ax[a_i].index < bx[b_i].index) {
				a_value = ax[a_i].value;
				b_value = 0;
				a_i++;
			} else if (bx[b_i].index < ax[a_i].index) {
				a_value = 0;
				b_value = bx[b_i].value;
				b_i++;
			}
		}
		distance += (a_value - b_value) * (a_value - b_value);
	}

	PG_RETURN_FLOAT8((double) distance);
}

/*
 * Get the inner product of two vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(inner_product);
Datum
inner_product(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	VecEl	   *ax = a->x;
	VecEl	   *bx = b->x;
	float		distance = 0.0;

	CheckDims(a, b);

	int a_i = 0;
	int b_i = 0;
	while (a_i < a->n_elem && b_i < b->n_elem) {
		if (ax[a_i].index == bx[b_i].index) {
			distance += ax[a_i].value * bx[b_i].value;
			a_i++;
			b_i++;
		} else if (ax[a_i].index < bx[b_i].index) {
			a_i++;
		} else if (bx[b_i].index < ax[a_i].index) {
			b_i++;
		}
	}

	PG_RETURN_FLOAT8((double) distance);
}

/*
 * Get the negative inner product of two vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_negative_inner_product);
Datum
svector_negative_inner_product(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	VecEl	   *ax = a->x;
	VecEl	   *bx = b->x;
	float		distance = 0.0;

	CheckDims(a, b);

	int a_i = 0;
	int b_i = 0;
	while (a_i < a->n_elem && b_i < b->n_elem) {
		if (ax[a_i].index == bx[b_i].index) {
			distance += ax[a_i].value * bx[b_i].value;
			a_i++;
			b_i++;
		} else if (ax[a_i].index < bx[b_i].index) {
			a_i++;
		} else if (bx[b_i].index < ax[a_i].index) {
			b_i++;
		}
	}

	PG_RETURN_FLOAT8((double) distance * -1);
}

/*
 * Get the cosine distance between two vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(cosine_distance);
Datum
cosine_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	VecEl	   *ax = a->x;
	VecEl	   *bx = b->x;
	float		distance = 0.0;
	float		norma = 0.0;
	float		normb = 0.0;
	double		similarity;

	CheckDims(a, b);

	int a_i = 0;
	int b_i = 0;
	while (a_i < a->n_elem || b_i < b->n_elem) {
		if (a_i >= a->n_elem) {
			normb += bx[b_i].value * bx[b_i].value;
			b_i++;
		} else if (b_i >= b->n_elem) {
			norma += ax[a_i].value * ax[a_i].value;
			a_i++;
		} else {
			if (ax[a_i].index == bx[b_i].index) {
				distance += ax[a_i].value * bx[b_i].value;
				norma += ax[a_i].value * ax[a_i].value;
				normb += bx[b_i].value * bx[b_i].value;
				a_i++;
				b_i++;
			} else if (ax[a_i].index < bx[b_i].index) {
				norma += ax[a_i].value * ax[a_i].value;
				a_i++;
			} else if (bx[b_i].index < ax[a_i].index) {
				normb += bx[b_i].value * bx[b_i].value;
				b_i++;
			}
		}
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
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_spherical_distance);
Datum
svector_spherical_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	VecEl	   *ax = a->x;
	VecEl	   *bx = b->x;
	float		dp = 0.0;
	double		distance;

	CheckDims(a, b);

	int a_i = 0;
	int b_i = 0;
	while (a_i < a->n_elem && b_i < b->n_elem) {
		if (ax[a_i].index == bx[b_i].index) {
			dp += ax[a_i].value * bx[b_i].value;
			a_i++;
			b_i++;
		} else if (ax[a_i].index < bx[b_i].index) {
			a_i++;
		} else if (bx[b_i].index < ax[a_i].index) {
			b_i++;
		}
	}

	distance = (double) dp;

	/* Prevent NaN with acos with loss of precision */
	if (distance > 1)
		distance = 1;
	else if (distance < -1)
		distance = -1;

	PG_RETURN_FLOAT8(acos(distance) / M_PI);
}

/*
 * Get the L1 distance between vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(l1_distance);
Datum
l1_distance(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	VecEl	   *ax = a->x;
	VecEl	   *bx = b->x;
	float		distance = 0.0;

	CheckDims(a, b);

	int a_i = 0;
	int b_i = 0;
	float a_value = 0.0;
	float b_value = 0.0;
	while (a_i < a->n_elem || b_i < b->n_elem) {
		if (a_i >= a->n_elem) {
			a_value = 0;
			b_value = bx[b_i].value;
			b_i++;
		} else if (b_i >= b->n_elem) {
			a_value = ax[a_i].value;
			b_value = 0;
			a_i++;
		} else {
			if (ax[a_i].index == bx[b_i].index) {
				a_value = ax[a_i].value;
				b_value = bx[b_i].value;
				a_i++;
				b_i++;
			} else if (ax[a_i].index < bx[b_i].index) {
				a_value = ax[a_i].value;
				b_value = 0;
				a_i++;
			} else if (bx[b_i].index < ax[a_i].index) {
				a_value = 0;
				b_value = bx[b_i].value;
				b_i++;
			}
		}
		distance += fabsf(a_value - b_value);
	}

	PG_RETURN_FLOAT8((double) distance);
}

// TODO: the above distance algorithms can probably have the a/b looping abstracted out

/*
 * Get the dimensions of a svector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_dims);
Datum
svector_dims(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);

	PG_RETURN_INT32(a->dim);
}

/*
 * Get the number of non-zero elements of the svector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svectorn_elem);
Datum
svectorn_elem(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);

	PG_RETURN_INT32(a->n_elem);
}

/*
 * Get the L2 norm of a svector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_norm);
Datum
svector_norm(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	VecEl	   *ax = a->x;
	double		norm = 0.0;

	/* Auto-vectorized */
	for (int i = 0; i < a->n_elem; i++)
		norm += (double) ax[i].value * (double) ax[i].value;

	PG_RETURN_FLOAT8(sqrt(norm));
}

/*
 * Add vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_add);
Datum
svector_add(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	VecEl	   *ax = a->x;
	VecEl	   *bx = b->x;
	Vector	   *result;
	VecEl	   *rx;

	CheckDims(a, b);

	// TODO: this might not be the most time-efficient, although it's just O(n)
	int32 result_n_elem = 0;
	int a_i = 0;
	int b_i = 0;
	while (a_i < a->n_elem || b_i < b->n_elem) {
		if (a_i >= a->n_elem) {
			b_i++;
		} else if (b_i >= b->n_elem) {
			a_i++;
		} else {
			if (ax[a_i].index == bx[b_i].index && ax[a_i].value + bx[b_i].value != 0) {
				a_i++;
				b_i++;
			} else if (ax[a_i].index < bx[b_i].index) {
				a_i++;
			} else if (bx[b_i].index < ax[a_i].index) {
				b_i++;
			}
		}
		result_n_elem++;
	}

	result = InitVector(result_n_elem, a->dim);
	rx = result->x;

	int i = 0;
	a_i = 0;
	b_i = 0;
	float a_value = 0.0;
	float b_value = 0.0;
	int sum_index = 0;
	while (a_i < a->n_elem || b_i < b->n_elem) {
		if (a_i >= a->n_elem) {
			a_value = 0;
			b_value = bx[b_i].value;
			sum_index = bx[b_i].index;
			b_i++;
		} else if (b_i >= b->n_elem) {
			a_value = ax[a_i].value;
			b_value = 0;
			sum_index = ax[a_i].index;
			a_i++;
		} else {
			if (ax[a_i].index == bx[b_i].index && ax[a_i].value + bx[b_i].value != 0) {
				a_value = ax[a_i].value;
				b_value = bx[b_i].value;
				sum_index = ax[a_i].index;
				a_i++;
				b_i++;
			} else if (ax[a_i].index < bx[b_i].index) {
				a_value = ax[a_i].value;
				b_value = 0;
				sum_index = ax[a_i].index;
				a_i++;
			} else if (bx[b_i].index < ax[a_i].index) {
				a_value = 0;
				b_value = bx[b_i].value;
				sum_index = bx[b_i].index;
				b_i++;
			}
		}
		rx[i].value = a_value + b_value;
		rx[i].index = sum_index;
		i++;
	}

	/* Check for overflow */
	for (int i = 0, imax = result->n_elem; i < imax; i++)
	{
		if (isinf(rx[i].value))
			float_overflow_error();
	}

	PG_RETURN_POINTER(result);
}

/*
 * Subtract vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_sub);
Datum
svector_sub(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	VecEl	   *ax = a->x;
	VecEl	   *bx = b->x;
	Vector	   *result;
	VecEl	   *rx;

	CheckDims(a, b);

	// TODO: this might not be the most time-efficient, although it's just O(n)
	int32 result_n_elem = 0;
	int a_i = 0;
	int b_i = 0;
	while (a_i < a->n_elem || b_i < b->n_elem) {
		if (a_i >= a->n_elem) {
			b_i++;
		} else if (b_i >= b->n_elem) {
			a_i++;
		} else {
			if (ax[a_i].index == bx[b_i].index && ax[a_i].value - bx[b_i].value != 0) {
				a_i++;
				b_i++;
			} else if (ax[a_i].index < bx[b_i].index) {
				a_i++;
			} else if (bx[b_i].index < ax[a_i].index) {
				b_i++;
			}
		}
		result_n_elem++;
	}

	result = InitVector(result_n_elem, a->dim);
	rx = result->x;

	int i = 0;
	a_i = 0;
	b_i = 0;
	float a_value = 0.0;
	float b_value = 0.0;
	int diff_index = 0;
	while (a_i < a->n_elem || b_i < b->n_elem) {
		if (a_i >= a->n_elem) {
			a_value = 0;
			b_value = bx[b_i].value;
			diff_index = bx[b_i].index;
			b_i++;
		} else if (b_i >= b->n_elem) {
			a_value = ax[a_i].value;
			b_value = 0;
			diff_index = ax[a_i].index;
			a_i++;
		} else {
			if (ax[a_i].index == bx[b_i].index && ax[a_i].value - bx[b_i].value != 0) {
				a_value = ax[a_i].value;
				b_value = bx[b_i].value;
				diff_index = ax[a_i].index;
				a_i++;
				b_i++;
			} else if (ax[a_i].index < bx[b_i].index) {
				a_value = ax[a_i].value;
				b_value = 0;
				diff_index = ax[a_i].index;
				a_i++;
			} else if (bx[b_i].index < ax[a_i].index) {
				a_value = 0;
				b_value = bx[b_i].value;
				diff_index = bx[b_i].index;
				b_i++;
			}
		}
		rx[i].value = a_value - b_value;
		rx[i].index = diff_index;
		i++;
	}

	/* Check for overflow */
	for (int i = 0, imax = result->n_elem; i < imax; i++)
	{
		if (isinf(rx[i].value))
			float_overflow_error();
	}

	PG_RETURN_POINTER(result);
}

/*
 * Multiply vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_mul);
Datum
svector_mul(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	VecEl	   *ax = a->x;
	VecEl	   *bx = b->x;
	Vector	   *result;
	VecEl	   *rx;

	CheckDims(a, b);

	// TODO: this might not be the most time-efficient, although it's just O(n)
	int32 result_n_elem = 0;
	int a_i = 0;
	int b_i = 0;
	while (a_i < a->n_elem && b_i < b->n_elem) {
		if (ax[a_i].index == bx[b_i].index && ax[a_i].value * bx[b_i].value != 0) {
			result_n_elem++;
			a_i++;
			b_i++;
		} else if (ax[a_i].index < bx[b_i].index) {
			a_i++;
		} else if (bx[b_i].index < ax[a_i].index) {
			b_i++;
		}
	}

	result = InitVector(result_n_elem, a->dim);
	rx = result->x;

	int i = 0;
	a_i = 0;
	b_i = 0;
	while (a_i < a->n_elem && b_i < b->n_elem) {
		if (ax[a_i].index == bx[b_i].index && ax[a_i].value * bx[b_i].value != 0) {
			rx[i].index = ax[a_i].index;
			rx[i].value = ax[a_i].value * bx[b_i].value;
			i++;
			a_i++;
			b_i++;
		} else if (ax[a_i].index < bx[b_i].index) {
			a_i++;
		} else if (bx[b_i].index < ax[a_i].index) {
			b_i++;
		}
	}

	/* Check for overflow and underflow */
	for (int i = 0, imax = result->n_elem; i < imax; i++)
	{
		if (isinf(rx[i].value))
			float_overflow_error();

		// There should be no zeros because the first check wouldn't allow any
		if (rx[i].value == 0)
			float_underflow_error();
	}

	PG_RETURN_POINTER(result);
}

/*
 * Internal helper to compare vectors
 */
int
svector_cmp_internal(Vector * a, Vector * b)
{
	CheckDims(a, b);

	VecEl* ax = a->x;
	VecEl* bx = b->x;

	int a_i = 0;
	int b_i = 0;
	while (a_i < a->n_elem && b_i < b->n_elem) {
		if (ax[a_i].index == bx[b_i].index) {
			if (ax[a_i].value < bx[b_i].value)
				return -1;
			if (ax[a_i].value > bx[b_i].value)
				return 1;
			a_i++;
			b_i++;
		}
		if (ax[a_i].index < bx[b_i].index) {
			// the "current" b element is 0 and a has value
			if (ax[a_i].value < 0) {
				return -1;
			} else if (ax[a_i].value > 0) {
				return 1;
			}
			a_i++;
		}
		if (bx[b_i].index < ax[a_i].index) {
			// the "current" a element is 0 and b has value
			if (bx[b_i].value < 0) {
				return 1;
			} else if (bx[b_i].value > 0) {
				return -1;
			}
			b_i++;
		}
	}

	return 0;
}

/*
 * Less than
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_lt);
Datum
svector_lt(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(svector_cmp_internal(a, b) < 0);
}

/*
 * Less than or equal
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_le);
Datum
svector_le(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(svector_cmp_internal(a, b) <= 0);
}

/*
 * Equal
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_eq);
Datum
svector_eq(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(svector_cmp_internal(a, b) == 0);
}

/*
 * Not equal
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_ne);
Datum
svector_ne(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(svector_cmp_internal(a, b) != 0);
}

/*
 * Greater than or equal
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_ge);
Datum
svector_ge(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(svector_cmp_internal(a, b) >= 0);
}

/*
 * Greater than
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_gt);
Datum
svector_gt(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(svector_cmp_internal(a, b) > 0);
}

/*
 * Compare vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_cmp);
Datum
svector_cmp(PG_FUNCTION_ARGS)
{
	Vector	   *a = (Vector *) PG_GETARG_VECTOR_P(0);
	Vector	   *b = (Vector *) PG_GETARG_VECTOR_P(1);

	PG_RETURN_INT32(svector_cmp_internal(a, b));
}
