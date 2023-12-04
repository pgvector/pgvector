#include "postgres.h"

#include <math.h>

#include "common/shortest_dec.h"
#include "fmgr.h"
#include "half.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/numeric.h"

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
 * Check if array is a vector
 */
static void
CheckArrayIsVector(ArrayType *array)
{
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("array must be 1-D")));

	if (ARR_HASNULL(array) && array_contains_nulls(array))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("array must not contain nulls")));
}

/*
 * Check if dimensions are the same
 */
static int
CheckDims(ArrayType *a, ArrayType *b)
{
	int			dima;
	int			dimb;

	CheckArrayIsVector(a);
	CheckArrayIsVector(b);

	dima = ARR_DIMS(a)[0];
	dimb = ARR_DIMS(b)[0];

	if (dima != dimb)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different dimensions %d and %d", dima, dimb)));

	return dima;
}

/*
 * Return the datum representation for a half
 */
static inline Datum
HalfGetDatum(half X)
{
	union
	{
		half		value;
		int16		retval;
	}			myunion;

	myunion.value = X;
	return Int16GetDatum(myunion.retval);
}

/*
 * Return the half value of a datum
 */
static inline half
DatumGetHalf(Datum X)
{
	union
	{
		int16		value;
		half		retval;
	}			myunion;

	myunion.value = DatumGetInt16(X);
	return myunion.retval;
}

/*
 * Append a half to a StringInfo buffer
 */
static half
pq_getmsghalf(StringInfo msg)
{
	union
	{
		half		h;
		uint16		i;
	}			swap;

	/* TODO Likely use float4 for clients */
	swap.i = pq_getmsgint(msg, 2);
	return swap.h;
}

/*
 * Get a half from a message buffer
 */
static void
pq_sendhalf(StringInfo buf, half h)
{
	union
	{
		half		h;
		uint16		i;
	}			swap;

	/* TODO Likely use float4 for clients */
	swap.h = h;
	pq_sendint16(buf, swap.i);
}

/*
 * Convert a half to a float4
 */
static float
HalfToFloat4(half num)
{
#ifdef FLT16_SUPPORT
	return (float) num;
#else
	/* TODO Improve performance */
	/* TODO Check endianness */
	uint16		bin = *((uint16 *) &num);
	uint32		exponent = (bin & 0x7C00) >> 10;
	uint32		mantissa = bin & 0x03FF;

	/* Sign */
	uint32		result = (bin & 0x8000) << 16;

	if (exponent == 31)
	{
		if (mantissa == 0)
		{
			/* Infinite */
			result |= 0x7F800000;
		}
		else
		{
			/* NaN */
			result |= 0x7FC00000;
			result |= mantissa << 13;
		}
	}
	else if (exponent == 0)
	{
		/* Subnormal */
		if (mantissa != 0)
		{
			exponent = -14;

			for (int i = 0; i < 10; i++)
			{
				mantissa <<= 1;
				exponent -= 1;

				if ((mantissa >> 10) % 2 == 1)
				{
					mantissa &= 0x03ff;
					break;
				}
			}

			result |= (exponent + 127) << 23;
			result |= mantissa << 13;
		}
	}
	else
	{
		/* Normal */
		result |= (exponent - 15 + 127) << 23;
		result |= mantissa << 13;
	}

	return *((float *) &result);
#endif
}

/*
 * Convert a float4 to a half
 */
static half
Float4ToHalfUnchecked(float num)
{
#ifdef FLT16_SUPPORT
	return (_Float16) num;
#else
	/* TODO Improve performance */
	/* TODO Check endianness */
	uint32		bin = *((uint32 *) &num);
	int			exponent = (bin & 0x7F800000) >> 23;
	int			mantissa = bin & 0x007FFFFF;

	/* Sign */
	uint16		result = (bin & 0x80000000) >> 16;

	if (isinf(num))
	{
		/* Infinite */
		result |= 0x7C00;
	}
	else if (isnan(num))
	{
		/* NaN */
		result |= 0x7E00;
		result |= mantissa >> 13;
	}
	else if (exponent > 98)
	{
		int			m;
		int			gr;
		int			s;

		exponent -= 127;
		s = mantissa & 0x00000FFF;

		/* Subnormal */
		if (exponent < -14)
		{
			int			diff = -exponent - 14;

			mantissa >>= diff;
			mantissa += 1 << (23 - diff);
			s |= mantissa & 0x00000FFF;
		}

		m = mantissa >> 13;

		/* Round */
		gr = (mantissa >> 12) % 4;
		if (gr == 3 || (gr == 1 && s != 0))
			m += 1;

		if (m == 1024)
		{
			m = 0;
			exponent += 1;
		}

		if (exponent > 15)
		{
			/* Infinite */
			result |= 0x7C00;
		}
		else
		{
			if (exponent >= -14)
				result |= (exponent + 15) << 10;

			result |= m;
		}
	}

	return *((half *) & result);
#endif
}

/*
 * Convert a float4 to a half
 */
static half
Float4ToHalf(float num)
{
	half		result = Float4ToHalfUnchecked(num);

	/* TODO Perform checks without HalfToFloat4 */
	if (unlikely(isinf(HalfToFloat4(result))) && !isinf(num))
		float_overflow_error();
	if (unlikely(HalfToFloat4(result) == 0.0f) && num != 0.0)
		float_underflow_error();

	return result;
}

/*
 * Convert a float8 to a half
 */
static half
Float8ToHalf(double num)
{
	/* TODO Convert directly for greater accuracy */
	return Float4ToHalf((float) num);
}

/*
 * Convert textual representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_in);
Datum
half_in(PG_FUNCTION_ARGS)
{
	char	   *num = PG_GETARG_CSTRING(0);
	char	   *orig_num;
	float		val;
	char	   *endptr;

	orig_num = num;

	/* Skip leading whitespace */
	while (*num != '\0' && isspace((unsigned char) *num))
		num++;

	/*
	 * Check for an empty-string input to begin with, to avoid the vagaries of
	 * strtof() on different platforms.
	 */
	if (*num == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"half", orig_num)));

	val = strtof(num, &endptr);

	if (val < -HALF_MAX || val > HALF_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("\"%s\" is out of range for type %s",
						orig_num, "half")));

	/* Skip trailing whitespace */
	while (*endptr != '\0' && isspace((unsigned char) *endptr))
		endptr++;

	/* If there is any junk left at the end of the string, bail out */
	if (*endptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"half", orig_num)));

	PG_RETURN_HALF(Float4ToHalf(val));
}

/*
 * Convert internal representation to textual representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_out);
Datum
half_out(PG_FUNCTION_ARGS)
{
	float		num = HalfToFloat4(PG_GETARG_HALF(0));
	char	   *ascii = (char *) palloc(32);
	int			ndig = FLT_DIG + extra_float_digits;

	if (extra_float_digits > 0)
	{
		float_to_shortest_decimal_buf(num, ascii);
		PG_RETURN_CSTRING(ascii);
	}

	(void) pg_strfromd(ascii, 32, ndig, num);
	PG_RETURN_CSTRING(ascii);
}

/*
 * Convert external binary representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_recv);
Datum
half_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_HALF(pq_getmsghalf(buf));
}

/*
 * Convert internal representation to the external binary representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_send);
Datum
half_send(PG_FUNCTION_ARGS)
{
	half		arg1 = PG_GETARG_HALF(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendhalf(&buf, arg1);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert integer to half
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(integer_to_half);
Datum
integer_to_half(PG_FUNCTION_ARGS)
{
	int32		i = PG_GETARG_INT32(0);

	/* TODO Figure out correct error */
	float		f = (float) i;
	half		h = Float4ToHalf(f);

	PG_RETURN_HALF(h);
}

/*
 * Convert numeric to half
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(numeric_to_half);
Datum
numeric_to_half(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	float		f = DatumGetFloat4(DirectFunctionCall1(numeric_float4, NumericGetDatum(num)));
	half		h = Float4ToHalf(f);

	PG_RETURN_HALF(h);
}

/*
 * Convert half to numeric
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_to_numeric);
Datum
half_to_numeric(PG_FUNCTION_ARGS)
{
	half		h = PG_GETARG_HALF(0);
	float		f = HalfToFloat4(h);
	Numeric		num = DatumGetNumeric(DirectFunctionCall1(float4_numeric, Float4GetDatum(f)));

	PG_RETURN_NUMERIC(num);
}

/*
 * Convert float4 to half
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(float4_to_half);
Datum
float4_to_half(PG_FUNCTION_ARGS)
{
	float		f = PG_GETARG_FLOAT4(0);
	half		h = Float4ToHalf(f);

	PG_RETURN_HALF(h);
}

/*
 * Convert half to float4
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_to_float4);
Datum
half_to_float4(PG_FUNCTION_ARGS)
{
	half		h = PG_GETARG_HALF(0);
	float		f = HalfToFloat4(h);

	PG_RETURN_FLOAT4(f);
}

/*
 * Convert float8 to half
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(float8_to_half);
Datum
float8_to_half(PG_FUNCTION_ARGS)
{
	float8		d = PG_GETARG_FLOAT8(0);
	half		h = Float8ToHalf(d);

	PG_RETURN_HALF(h);
}

/*
 * Convert half to float8
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_to_float8);
Datum
half_to_float8(PG_FUNCTION_ARGS)
{
	half		h = PG_GETARG_HALF(0);
	float		f = HalfToFloat4(h);

	PG_RETURN_FLOAT8((double) f);
}

/*
 * Get the L2 distance between half arrays
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_l2_distance);
Datum
half_l2_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	half	   *ax = (half *) ARR_DATA_PTR(a);
	half	   *bx = (half *) ARR_DATA_PTR(b);
	float		distance = 0.0;
	int			dim = CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		float		diff = HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]);

		distance += diff * diff;
	}

	PG_RETURN_FLOAT8(sqrt((double) distance));
}

/*
 * Get the L2 squared distance between half arrays
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_l2_squared_distance);
Datum
half_l2_squared_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	half	   *ax = (half *) ARR_DATA_PTR(a);
	half	   *bx = (half *) ARR_DATA_PTR(b);
	float		distance = 0.0;
	int			dim = CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		float		diff = HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]);

		distance += diff * diff;
	}

	PG_RETURN_FLOAT8((double) distance);
}

/*
 * Get the inner product of two half arrays
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_inner_product);
Datum
half_inner_product(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	half	   *ax = (half *) ARR_DATA_PTR(a);
	half	   *bx = (half *) ARR_DATA_PTR(b);
	float		distance = 0.0;
	int			dim = CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += HalfToFloat4(ax[i]) * HalfToFloat4(bx[i]);

	PG_RETURN_FLOAT8((double) distance);
}

/*
 * Get the negative inner product of two half arrays
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_negative_inner_product);
Datum
half_negative_inner_product(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	half	   *ax = (half *) ARR_DATA_PTR(a);
	half	   *bx = (half *) ARR_DATA_PTR(b);
	float		distance = 0.0;
	int			dim = CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += HalfToFloat4(ax[i]) * HalfToFloat4(bx[i]);

	PG_RETURN_FLOAT8((double) distance * -1);
}

/*
 * Get the cosine distance between two half arrays
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_cosine_distance);
Datum
half_cosine_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	half	   *ax = (half *) ARR_DATA_PTR(a);
	half	   *bx = (half *) ARR_DATA_PTR(b);
	float		distance = 0.0;
	float		norma = 0.0;
	float		normb = 0.0;
	double		similarity;
	int			dim = CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
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
 * Get the L1 distance between two half arrays
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(half_l1_distance);
Datum
half_l1_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	half	   *ax = (half *) ARR_DATA_PTR(a);
	half	   *bx = (half *) ARR_DATA_PTR(b);
	float		distance = 0.0;
	int			dim = CheckDims(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += fabsf(HalfToFloat4(ax[i]) - HalfToFloat4(bx[i]));

	PG_RETURN_FLOAT8((double) distance);
}
