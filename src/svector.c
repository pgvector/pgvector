#include "postgres.h"

#include <math.h>

#include "fmgr.h"
#include "libpq/pqformat.h"
#include "svector.h"
#include "utils/array.h"
#include "vector.h"

#if PG_VERSION_NUM >= 120000
#include "common/shortest_dec.h"
#include "utils/float.h"
#else
#include <float.h>
#include "utils/builtins.h"
#endif

/*
 * Ensure same dimensions
 */
static inline void
CheckDims(SVector * a, SVector * b)
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
 * Ensure valid dimensions
 */
static inline void
CheckDim(int dim)
{
	if (dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("svector must have at least 1 dimension")));

	if (dim > SVECTOR_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("svector cannot have more than %d dimensions", SVECTOR_MAX_DIM)));
}

/*
 * Ensure valid nnz
 */
static inline void
CheckNnz(int nnz, int dim)
{
	if (nnz < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("svector must have at least one element")));

	if (nnz > dim)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("svector cannot have more elements than dimensions")));
}

/*
 * Ensure valid index
 */
static inline void
CheckIndex(int32 *indices, int i, int dim)
{
	int32		index = indices[i];

	if (index < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("index must not be negative")));

	if (index >= dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("index must be less than dimensions")));

	if (i > 0)
	{
		if (index < indices[i - 1])
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("indexes must be in ascending order")));

		if (index == indices[i - 1])
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("indexes must not contain duplicates")));
	}
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
				 errmsg("NaN not allowed in svector")));

	if (isinf(value))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("infinite value not allowed in svector")));
}

/*
 * Allocate and initialize a new sparse vector
 */
SVector *
InitSVector(int dim, int nnz)
{
	SVector    *result;
	int			size;

	size = SVECTOR_SIZE(nnz);
	result = (SVector *) palloc0(size);
	SET_VARSIZE(result, size);
	result->dim = dim;
	result->nnz = nnz;

	return result;
}

/*
 * Convert textual representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_in);
Datum
svector_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	int			dim;
	char	   *pt;
	SVector    *result;
	float	   *rvalues;
	char	   *lit = pstrdup(str);
	int			n;
	int32	   *indices;
	float	   *values;
	int			index;
	float		value;
	int			maxNnz;
	int			nnz = 0;

	/* TODO Improve code and checks after deciding on format */

	maxNnz = 1;
	pt = str;
	while (*pt != '\0')
	{
		if (*pt == ',')
			maxNnz++;

		pt++;
	}
	maxNnz /= 2;

	indices = palloc(maxNnz * sizeof(int32));
	values = palloc(maxNnz * sizeof(float));

	while (sscanf(str, "(%d,%f)%n", &index, &value, &n) == 2)
	{
		/* TODO Better error */
		if (nnz == maxNnz)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("ran out of buffer: \"%s\"", lit)));

		/* TODO Decide whether to store zero values */
		indices[nnz] = index;
		values[nnz] = value;
		nnz++;

		str += n;

		if (*str == ',')
			str++;
		else if (*str == '|')
			break;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed svector literal: \"%s\"", lit)));
	}

	if (sscanf(str, "|%d|%n", &dim, &n) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed svector literal: \"%s\"", lit)));

	str += n;

	if (*str != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed svector literal: \"%s\"", lit),
				 errdetail("Junk after closing pipe.")));

	pfree(lit);

	CheckDim(dim);
	CheckExpectedDim(typmod, dim);

	result = InitSVector(dim, nnz);
	rvalues = SVECTOR_VALUES(result);
	for (int i = 0; i < nnz; i++)
	{
		result->indices[i] = indices[i];
		rvalues[i] = values[i];

		CheckIndex(result->indices, i, dim);
		CheckElement(rvalues[i]);
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
	SVector    *svector = PG_GETARG_SVECTOR_P(0);
	float	   *values = SVECTOR_VALUES(svector);
	char	   *buf;
	char	   *ptr;
	int			n;

	/* TODO Improve code after deciding on format */

#if PG_VERSION_NUM < 120000
	int			ndig = FLT_DIG + extra_float_digits;

	if (ndig < 1)
		ndig = 1;

#define FLOAT_SHORTEST_DECIMAL_LEN (ndig + 10)
#endif

	/* TODO Move */
#define APPEND_CHAR(ptr, ch) (*(ptr)++ = (ch))

	/* TODO Improve */
	buf = (char *) palloc((FLOAT_SHORTEST_DECIMAL_LEN + 20) * svector->nnz + 20);
	ptr = buf;

	for (int i = 0; i < svector->nnz; i++)
	{
		if (i > 0)
			APPEND_CHAR(ptr, ',');

		n = sprintf(ptr, "(%d,", svector->indices[i]);
		ptr += n;

#if PG_VERSION_NUM >= 120000
		n = float_to_shortest_decimal_bufn(values[i], ptr);
#else
		n = sprintf(ptr, "%.*g", ndig, values[i]);
#endif
		ptr += n;

		APPEND_CHAR(ptr, ')');
	}

	n = sprintf(ptr, "|%d|", svector->dim);
	ptr += n;

	APPEND_CHAR(ptr, '\0');

	PG_FREE_IF_COPY(svector, 0);
	PG_RETURN_CSTRING(buf);
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

	if (*tl < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type svector must be at least 1")));

	if (*tl > SVECTOR_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type svector cannot exceed %d", SVECTOR_MAX_DIM)));

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
	SVector    *result;
	int32		dim;
	int32		nnz;
	int32		unused;
	float	   *values;

	dim = pq_getmsgint(buf, sizeof(int32));
	nnz = pq_getmsgint(buf, sizeof(int32));
	unused = pq_getmsgint(buf, sizeof(int32));

	CheckDim(dim);
	CheckNnz(nnz, dim);
	CheckExpectedDim(typmod, dim);

	if (unused != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("expected unused to be 0, not %d", unused)));

	result = InitSVector(dim, nnz);
	values = SVECTOR_VALUES(result);

	for (int i = 0; i < nnz; i++)
	{
		result->indices[i] = pq_getmsgint(buf, sizeof(int32));
		CheckIndex(result->indices, i, dim);
	}

	for (int i = 0; i < nnz; i++)
	{
		values[i] = pq_getmsgfloat4(buf);
		CheckElement(values[i]);
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
	SVector    *svec = PG_GETARG_SVECTOR_P(0);
	float	   *values = SVECTOR_VALUES(svec);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint(&buf, svec->dim, sizeof(int32));
	pq_sendint(&buf, svec->nnz, sizeof(int32));
	pq_sendint(&buf, svec->unused, sizeof(int32));
	for (int i = 0; i < svec->nnz; i++)
		pq_sendint(&buf, svec->indices[i], sizeof(int32));
	for (int i = 0; i < svec->nnz; i++)
		pq_sendfloat4(&buf, values[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert sparse vector to sparse vector
 * This is needed to check the type modifier
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector);
Datum
svector(PG_FUNCTION_ARGS)
{
	SVector    *svec = PG_GETARG_SVECTOR_P(0);
	int32		typmod = PG_GETARG_INT32(1);

	CheckExpectedDim(typmod, svec->dim);

	PG_RETURN_POINTER(svec);
}

/*
 * Convert dense vector to sparse vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_to_svector);
Datum
vector_to_svector(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	SVector    *result;
	int			dim = vec->dim;
	int			nnz = 0;
	float	   *values;
	int			j = 0;

	CheckDim(dim);
	CheckExpectedDim(typmod, dim);

	for (int i = 0; i < dim; i++)
	{
		if (vec->x[i] != 0)
			nnz++;
	}

	result = InitSVector(dim, nnz);
	values = SVECTOR_VALUES(result);
	for (int i = 0; i < dim; i++)
	{
		if (vec->x[i] != 0)
		{
			/* Safety check */
			if (j == nnz)
				elog(ERROR, "safety check failed");

			result->indices[j] = i;
			values[j] = vec->x[i];
			j++;
		}
	}

	PG_RETURN_POINTER(result);
}

/*
 *  Get the L2 squared distance between sparse vectors
 */
static double
l2_distance_squared_internal(SVector * a, SVector * b)
{
	float	   *ax = SVECTOR_VALUES(a);
	float	   *bx = SVECTOR_VALUES(b);
	double		distance = 0.0;
	int			bpos = 0;

	for (int i = 0; i < a->nnz; i++)
	{
		int			ai = a->indices[i];
		int			bi = -1;

		for (int j = bpos; j < b->nnz; j++)
		{
			bi = b->indices[j];

			if (ai == bi)
			{
				double		diff = ax[i] - bx[j];

				distance += diff * diff;
			}
			else if (ai > bi)
				distance += bx[j] * bx[j];

			/* Update start for next iteration */
			if (ai >= bi)
				bpos = j + 1;

			/* Found or passed it */
			if (bi >= ai)
				break;
		}

		if (ai != bi)
			distance += ax[i] * ax[i];
	}

	for (int j = bpos; j < b->nnz; j++)
		distance += bx[j] * bx[j];

	return distance;
}

/*
 * Get the L2 distance between sparse vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_l2_distance);
Datum
svector_l2_distance(PG_FUNCTION_ARGS)
{
	SVector    *a = PG_GETARG_SVECTOR_P(0);
	SVector    *b = PG_GETARG_SVECTOR_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8(sqrt(l2_distance_squared_internal(a, b)));
}

/*
 * Get the L2 squared distance between sparse vectors
 * This saves a sqrt calculation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_l2_squared_distance);
Datum
svector_l2_squared_distance(PG_FUNCTION_ARGS)
{
	SVector    *a = PG_GETARG_SVECTOR_P(0);
	SVector    *b = PG_GETARG_SVECTOR_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8(l2_distance_squared_internal(a, b));
}

/*
 * Get the inner product of two sparse vectors
 */
static double
inner_product_internal(SVector * a, SVector * b)
{
	float	   *ax = SVECTOR_VALUES(a);
	float	   *bx = SVECTOR_VALUES(b);
	double		distance = 0.0;
	int			bpos = 0;

	for (int i = 0; i < a->nnz; i++)
	{
		int			ai = a->indices[i];

		for (int j = bpos; j < b->nnz; j++)
		{
			int			bi = b->indices[j];

			/* Only update when the same index */
			if (ai == bi)
				distance += ax[i] * bx[j];

			/* Update start for next iteration */
			if (ai >= bi)
				bpos = j + 1;

			/* Found or passed it */
			if (bi >= ai)
				break;
		}
	}

	return distance;
}

/*
 * Get the inner product of two sparse vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_inner_product);
Datum
svector_inner_product(PG_FUNCTION_ARGS)
{
	SVector    *a = PG_GETARG_SVECTOR_P(0);
	SVector    *b = PG_GETARG_SVECTOR_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8(inner_product_internal(a, b));
}

/*
 * Get the negative inner product of two sparse vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_negative_inner_product);
Datum
svector_negative_inner_product(PG_FUNCTION_ARGS)
{
	SVector    *a = PG_GETARG_SVECTOR_P(0);
	SVector    *b = PG_GETARG_SVECTOR_P(1);

	CheckDims(a, b);

	PG_RETURN_FLOAT8(-inner_product_internal(a, b));
}

/*
 * Get the cosine distance between two sparse vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_cosine_distance);
Datum
svector_cosine_distance(PG_FUNCTION_ARGS)
{
	SVector    *a = PG_GETARG_SVECTOR_P(0);
	SVector    *b = PG_GETARG_SVECTOR_P(1);
	float	   *ax = SVECTOR_VALUES(a);
	float	   *bx = SVECTOR_VALUES(b);
	float		norma = 0.0;
	float		normb = 0.0;
	double		similarity;

	CheckDims(a, b);

	similarity = inner_product_internal(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < a->nnz; i++)
		norma += ax[i] * ax[i];

	/* Auto-vectorized */
	for (int i = 0; i < b->nnz; i++)
		normb += bx[i] * bx[i];

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	similarity /= sqrt((double) norma * (double) normb);

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
 * Get the weighted Jaccard distance between two sparse vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(svector_jaccard_distance);
Datum
svector_jaccard_distance(PG_FUNCTION_ARGS)
{
	SVector    *a = PG_GETARG_SVECTOR_P(0);
	SVector    *b = PG_GETARG_SVECTOR_P(1);
	float	   *ax = SVECTOR_VALUES(a);
	float	   *bx = SVECTOR_VALUES(b);
	double		num = 0.0;
	double		denom = 0.0;
	int			bpos = 0;

	CheckDims(a, b);

	/*
	 * Weighted Jaccard distance is not defined for vectors with negative
	 * values. Could check and return NaN if minimal impact on performance.
	 */

	for (int i = 0; i < a->nnz; i++)
	{
		int			ai = a->indices[i];
		int			bi = -1;

		for (int j = bpos; j < b->nnz; j++)
		{
			bi = b->indices[j];

			if (ai == bi)
			{
				num += ax[i] < bx[j] ? ax[i] : bx[j];
				denom += ax[i] > bx[j] ? ax[i] : bx[j];
			}
			else if (ai > bi)
				denom += bx[j];

			/* Update start for next iteration */
			if (ai >= bi)
				bpos = j + 1;

			/* Found or passed it */
			if (bi >= ai)
				break;
		}

		if (ai != bi)
			denom += ax[i];
	}

	for (int j = bpos; j < b->nnz; j++)
		denom += bx[j];

	if (denom > 0)
		PG_RETURN_FLOAT8(1.0 - (num / denom));
	else
		PG_RETURN_FLOAT8(NAN);
}
