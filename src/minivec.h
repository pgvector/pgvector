#ifndef MINIVEC_H
#define MINIVEC_H

#include <float.h>

#define MINIVEC_MAX_DIM 16000

#define fp8 uint8

#define MINIVEC_SIZE(_dim)		(offsetof(MiniVector, x) + sizeof(fp8)*(_dim))
#define DatumGetMiniVector(x)		((MiniVector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_MINIVEC_P(x)	DatumGetMiniVector(PG_GETARG_DATUM(x))
#define PG_RETURN_MINIVEC_P(x)	PG_RETURN_POINTER(x)

typedef struct MiniVector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int16		dim;			/* number of dimensions */
	int16		unused;			/* reserved for future use, always zero */
	fp8			x[FLEXIBLE_ARRAY_MEMBER];
}			MiniVector;

MiniVector *InitMiniVector(int dim);

/*
 * Check if fp8 is NaN
 */
static inline bool
Fp8IsNan(fp8 num)
{
	return (num & 0x7F) == 0x7F;
}

/*
 * Check if fp8 is zero
 */
static inline bool
Fp8IsZero(fp8 num)
{
	return num == 0;
}

/*
 * Convert a fp8 to a float4
 */
static inline float
Fp8ToFloat4(fp8 num)
{
	float		lookup[128] = {0, 0.00195312, 0.00390625, 0.00585938, 0.0078125, 0.00976562, 0.0117188, 0.0136719, 0.015625, 0.0175781, 0.0195312, 0.0214844, 0.0234375, 0.0253906, 0.0273438, 0.0292969, 0.03125, 0.0351562, 0.0390625, 0.0429688, 0.046875, 0.0507812, 0.0546875, 0.0585938, 0.0625, 0.0703125, 0.078125, 0.0859375, 0.09375, 0.101562, 0.109375, 0.117188, 0.125, 0.140625, 0.15625, 0.171875, 0.1875, 0.203125, 0.21875, 0.234375, 0.25, 0.28125, 0.3125, 0.34375, 0.375, 0.40625, 0.4375, 0.46875, 0.5, 0.5625, 0.625, 0.6875, 0.75, 0.8125, 0.875, 0.9375, 1, 1.125, 1.25, 1.375, 1.5, 1.625, 1.75, 1.875, 2, 2.25, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4, 4.5, 5, 5.5, 6, 6.5, 7, 7.5, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 20, 22, 24, 26, 28, 30, 32, 36, 40, 44, 48, 52, 56, 60, 64, 72, 80, 88, 96, 104, 112, 120, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352, 384, 416, 448, NAN};
	float		v = lookup[num & 0x7F];

	return (num & 0x80) == 0x80 ? -v : v;
}

/*
 * Convert a float4 to a fp8
 */
static inline fp8
Float4ToFp8Unchecked(float num)
{
	union
	{
		float		f;
		uint32		i;
	}			swapfloat;

	uint32		bin;
	int			exponent;
	int			mantissa;
	uint8		result;

	swapfloat.f = num;
	bin = swapfloat.i;
	exponent = (bin & 0x7F800000) >> 23;
	mantissa = bin & 0x007FFFFF;

	/* Sign */
	result = (bin & 0x80000000) >> 24;

	if (isinf(num) || isnan(num))
	{
		/* NaN */
		result |= 0x7F;
	}
	else if (exponent > 116)
	{
		int			m;
		int			gr;
		int			s;

		exponent -= 127;
		s = mantissa & 0x000FFFFF;

		/* Subnormal */
		if (exponent < -6)
		{
			int			diff = -exponent - 6;

			mantissa >>= diff;
			mantissa += 1 << (23 - diff);
			s |= mantissa & 0x000FFFFF;
		}

		m = mantissa >> 20;

		/* Round */
		gr = (mantissa >> 19) % 4;
		if (gr == 3 || (gr == 1 && s != 0))
			m += 1;

		if (m == 8)
		{
			m = 0;
			exponent += 1;
		}

		if (exponent > 8)
		{
			/* Infinite, which is NaN */
			result |= 0x7F;
		}
		else
		{
			if (exponent >= -7)
				result |= (exponent + 7) << 3;

			result |= m;
		}
	}

	return result;
}

/*
 * Convert a float4 to a fp8
 */
static inline fp8
Float4ToFp8(float num)
{
	fp8			result = Float4ToFp8Unchecked(num);

	if (unlikely(Fp8IsNan(result)) && !isnan(num))
	{
		char	   *buf = palloc(FLOAT_SHORTEST_DECIMAL_LEN);

		float_to_shortest_decimal_buf(num, buf);

		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("\"%s\" is out of range for type minivec", buf)));
	}

	return result;
}

#endif
