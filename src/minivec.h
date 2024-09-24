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
	uint32		lookup[128] = {0, 989855744, 998244352, 1002438656, 1006632960, 1008730112, 1010827264, 1012924416, 1015021568, 1016070144, 1017118720, 1018167296, 1019215872, 1020264448, 1021313024, 1022361600, 1023410176, 1024458752, 1025507328, 1026555904, 1027604480, 1028653056, 1029701632, 1030750208, 1031798784, 1032847360, 1033895936, 1034944512, 1035993088, 1037041664, 1038090240, 1039138816, 1040187392, 1041235968, 1042284544, 1043333120, 1044381696, 1045430272, 1046478848, 1047527424, 1048576000, 1049624576, 1050673152, 1051721728, 1052770304, 1053818880, 1054867456, 1055916032, 1056964608, 1058013184, 1059061760, 1060110336, 1061158912, 1062207488, 1063256064, 1064304640, 1065353216, 1066401792, 1067450368, 1068498944, 1069547520, 1070596096, 1071644672, 1072693248, 1073741824, 1074790400, 1075838976, 1076887552, 1077936128, 1078984704, 1080033280, 1081081856, 1082130432, 1083179008, 1084227584, 1085276160, 1086324736, 1087373312, 1088421888, 1089470464, 1090519040, 1091567616, 1092616192, 1093664768, 1094713344, 1095761920, 1096810496, 1097859072, 1098907648, 1099956224, 1101004800, 1102053376, 1103101952, 1104150528, 1105199104, 1106247680, 1107296256, 1108344832, 1109393408, 1110441984, 1111490560, 1112539136, 1113587712, 1114636288, 1115684864, 1116733440, 1117782016, 1118830592, 1119879168, 1120927744, 1121976320, 1123024896, 1124073472, 1125122048, 1126170624, 1127219200, 1128267776, 1129316352, 1130364928, 1131413504, 1132462080, 1133510656, 1134559232, 1135607808, 1136656384, 1137704960, 1138753536, 2146435072};

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

	if (isinf(num) || isnan(num))
	{
		/* NaN */
		result |= 0x7F;
	}
	else if (exponent > 114)
	{
		int			m;
		int			gr;
		int			s;

		exponent -= 127;
		s = mantissa & 0x0007FFFF;

		/* Subnormal */
		if (exponent < -6)
		{
			int			diff = -exponent - 6;

			mantissa >>= diff;
			mantissa += 1 << (23 - diff);
			s |= mantissa & 0x0007FFFF;
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
			if (exponent >= -6)
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

	if (unlikely(Fp8IsNan(result)) && !isinf(num))
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
