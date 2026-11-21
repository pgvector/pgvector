#ifndef SPARSEUTILS_H
#define SPARSEUTILS_H

#include "sparsevec.h"

float		SparsevecL2SquaredDistance(SparseVector * a, SparseVector * b);
float		SparsevecInnerProduct(SparseVector * a, SparseVector * b);
double		SparsevecCosineSimilarity(SparseVector * a, SparseVector * b);
float		SparsevecL1Distance(SparseVector * a, SparseVector * b);

#endif
