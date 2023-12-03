#ifndef HALF_H
#define HALF_H

#define __STDC_WANT_IEC_60559_TYPES_EXT__

#include <float.h>

/* _Float16 and __fp16 are not supported on x86_64 with GCC 11 */
#if defined(__is_identifier)
#if __is_identifier(_Float16)
#define FLT16_SUPPORT
#endif
#elif defined(FLT16_MAX)
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
