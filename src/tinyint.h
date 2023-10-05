#ifndef TINYINT_H
#define TINYINT_H

#define DatumGetInt8(X) ((int8) (X))
#define PG_GETARG_INT8(n)	 DatumGetInt8(PG_GETARG_DATUM(n))
#define PG_RETURN_INT8(x)	 return Int8GetDatum(x)

#endif
