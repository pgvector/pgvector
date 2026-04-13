#ifndef INT8UTILS_H
#define INT8UTILS_H

#include "int8vec.h"

extern float (*Int8vecL2SquaredDistance) (int dim, int8 *ax, int8 *bx);
extern float (*Int8vecInnerProduct) (int dim, int8 *ax, int8 *bx);
extern double (*Int8vecCosineSimilarity) (int dim, int8 *ax, int8 *bx);
extern float (*Int8vecL1Distance) (int dim, int8 *ax, int8 *bx);

void		Int8vecInit(void);

#endif
