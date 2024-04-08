#ifndef HALFUTILS_H
#define HALFUTILS_H

#include "halfvec.h"

extern float (*HalfvecL2DistanceSquared) (int dim, half * ax, half * bx);
extern float (*HalfvecInnerProduct) (int dim, half * ax, half * bx);

void		HalfvecInit(void);

#endif
