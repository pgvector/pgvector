#ifndef HALF_H
#define HALF_H

#define __STDC_WANT_IEC_60559_TYPES_EXT__

#include <float.h>

#ifdef __FLT16_MAX__
#define FLT16_SUPPORT
#endif

#ifdef FLT16_SUPPORT
#define half _Float16
#define HALF_MAX FLT16_MAX
#else
#define half uint16
#define HALF_MAX 65504
#endif

#define PG_GETARG_HALF(n)  DatumGetHalf(PG_GETARG_DATUM(n))
#define PG_RETURN_HALF(x)  return HalfGetDatum(x)

#endif
