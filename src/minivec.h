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
	return (num & 0x7C) == 0x7C && (num & 0x7F) != 0x7C;
}

/*
 * Check if fp8 is infinite
 */
static inline bool
Fp8IsInf(fp8 num)
{
	return (num & 0x7F) == 0x7C;
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
	float		lookup[128] = {0, 0.0000152587890625, 0.000030517578125, 0.0000457763671875, 0.00006103515625, 0.0000762939453125, 0.000091552734375, 0.000106812, 0.00012207, 0.000152588, 0.000183105, 0.000213623, 0.000244141, 0.000305176, 0.000366211, 0.000427246, 0.000488281, 0.000610352, 0.000732422, 0.000854492, 0.000976562, 0.0012207, 0.00146484, 0.00170898, 0.00195312, 0.00244141, 0.00292969, 0.00341797, 0.00390625, 0.00488281, 0.00585938, 0.00683594, 0.0078125, 0.00976562, 0.0117188, 0.0136719, 0.015625, 0.0195312, 0.0234375, 0.0273438, 0.03125, 0.0390625, 0.046875, 0.0546875, 0.0625, 0.078125, 0.09375, 0.109375, 0.125, 0.15625, 0.1875, 0.21875, 0.25, 0.3125, 0.375, 0.4375, 0.5, 0.625, 0.75, 0.875, 1, 1.25, 1.5, 1.75, 2, 2.5, 3, 3.5, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, 448, 512, 640, 768, 896, 1024, 1280, 1536, 1792, 2048, 2560, 3072, 3584, 4096, 5120, 6144, 7168, 8192, 10240, 12288, 14336, 16384, 20480, 24576, 28672, 32768, 40960, 49152, 57344, INFINITY, NAN, NAN, NAN};
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

	if (isinf(num))
	{
		/* Infinite */
		result |= 0x7C;
	}
	else if (isnan(num))
	{
		/* NaN */
		result |= 0x7C;
		result |= mantissa >> 21;
	}
	else if (exponent > 98)
	{
		int			m;
		int			gr;
		int			s;

		exponent -= 127;
		s = mantissa & 0x001FFFFF;

		/* Subnormal */
		if (exponent < -14)
		{
			int			diff = -exponent - 14;

			mantissa >>= diff;
			mantissa += 1 << (23 - diff);
			s |= mantissa & 0x001FFFFF;
		}

		m = mantissa >> 21;

		/* Round */
		gr = (mantissa >> 20) % 4;
		if (gr == 3 || (gr == 1 && s != 0))
			m += 1;

		if (m == 4)
		{
			m = 0;
			exponent += 1;
		}

		if (exponent > 16)
		{
			/* Infinite */
			result |= 0x7C;
		}
		else
		{
			if (exponent >= -14)
				result |= (exponent + 15) << 2;

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

	if (unlikely(Fp8IsInf(result)) && !isinf(num))
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
