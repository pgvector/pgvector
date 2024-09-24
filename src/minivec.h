#ifndef MINIVEC_H
#define MINIVEC_H

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
	uint32		lookup[128] = {0, 931135488, 939524096, 943718400, 947912704, 950009856, 952107008, 954204160, 956301312, 958398464, 960495616, 962592768, 964689920, 966787072, 968884224, 970981376, 973078528, 975175680, 977272832, 979369984, 981467136, 983564288, 985661440, 987758592, 989855744, 991952896, 994050048, 996147200, 998244352, 1000341504, 1002438656, 1004535808, 1006632960, 1008730112, 1010827264, 1012924416, 1015021568, 1017118720, 1019215872, 1021313024, 1023410176, 1025507328, 1027604480, 1029701632, 1031798784, 1033895936, 1035993088, 1038090240, 1040187392, 1042284544, 1044381696, 1046478848, 1048576000, 1050673152, 1052770304, 1054867456, 1056964608, 1059061760, 1061158912, 1063256064, 1065353216, 1067450368, 1069547520, 1071644672, 1073741824, 1075838976, 1077936128, 1080033280, 1082130432, 1084227584, 1086324736, 1088421888, 1090519040, 1092616192, 1094713344, 1096810496, 1098907648, 1101004800, 1103101952, 1105199104, 1107296256, 1109393408, 1111490560, 1113587712, 1115684864, 1117782016, 1119879168, 1121976320, 1124073472, 1126170624, 1128267776, 1130364928, 1132462080, 1134559232, 1136656384, 1138753536, 1140850688, 1142947840, 1145044992, 1147142144, 1149239296, 1151336448, 1153433600, 1155530752, 1157627904, 1159725056, 1161822208, 1163919360, 1166016512, 1168113664, 1170210816, 1172307968, 1174405120, 1176502272, 1178599424, 1180696576, 1182793728, 1184890880, 1186988032, 1189085184, 1191182336, 1193279488, 1195376640, 1197473792, 2139095040, 2145386496, 2143289344, 2145386496};

	union
	{
		float		f;
		uint32		i;
	}			swap;

	swap.i = lookup[num & 0x7F];

	return (num & 0x80) == 0x80 ? -swap.f : swap.f;
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
		result |= 0x7F;
	}
	else if (exponent > 98)
	{
		int			m;
		int			gr;
		int			s;

		exponent -= 127;
		s = mantissa & 0x000FFFFF;

		/* Subnormal */
		if (exponent < -14)
		{
			int			diff = -exponent - 14;

			mantissa >>= diff;
			mantissa += 1 << (23 - diff);
			s |= mantissa & 0x000FFFFF;
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

		if (exponent > 15)
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
