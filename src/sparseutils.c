#include "postgres.h"

#include <math.h>

#include "sparseutils.h"

float
SparsevecL2SquaredDistance(SparseVector * a, SparseVector * b)
{
	float	   *ax = SPARSEVEC_VALUES(a);
	float	   *bx = SPARSEVEC_VALUES(b);
	float		distance = 0.0;
	int			bpos = 0;

	for (int i = 0; i < a->nnz; i++)
	{
		int32		ai = a->indices[i];
		int32		bi = -1;

		for (int j = bpos; j < b->nnz; j++)
		{
			bi = b->indices[j];

			if (ai == bi)
			{
				float		diff = ax[i] - bx[j];

				distance += diff * diff;
			}
			else if (ai > bi)
				distance += bx[j] * bx[j];

			/* Update start for next iteration */
			if (ai >= bi)
				bpos = j + 1;

			/* Found or passed it */
			if (bi >= ai)
				break;
		}

		if (ai != bi)
			distance += ax[i] * ax[i];
	}

	for (int j = bpos; j < b->nnz; j++)
		distance += bx[j] * bx[j];

	return distance;
}

float
SparsevecInnerProduct(SparseVector * a, SparseVector * b)
{
	float	   *ax = SPARSEVEC_VALUES(a);
	float	   *bx = SPARSEVEC_VALUES(b);
	float		distance = 0.0;
	int			bpos = 0;

	for (int i = 0; i < a->nnz; i++)
	{
		int32		ai = a->indices[i];

		for (int j = bpos; j < b->nnz; j++)
		{
			int32		bi = b->indices[j];

			/* Only update when the same index */
			if (ai == bi)
				distance += ax[i] * bx[j];

			/* Update start for next iteration */
			if (ai >= bi)
				bpos = j + 1;

			/* Found or passed it */
			if (bi >= ai)
				break;
		}
	}

	return distance;
}

double
SparsevecCosineSimilarity(SparseVector * a, SparseVector * b)
{
	float	   *ax = SPARSEVEC_VALUES(a);
	float	   *bx = SPARSEVEC_VALUES(b);
	float		norma = 0.0;
	float		normb = 0.0;
	float		similarity;

	similarity = SparsevecInnerProduct(a, b);

	/* Auto-vectorized */
	for (int i = 0; i < a->nnz; i++)
		norma += ax[i] * ax[i];

	/* Auto-vectorized */
	for (int i = 0; i < b->nnz; i++)
		normb += bx[i] * bx[i];

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
}

float
SparsevecL1Distance(SparseVector * a, SparseVector * b)
{
	float	   *ax = SPARSEVEC_VALUES(a);
	float	   *bx = SPARSEVEC_VALUES(b);
	float		distance = 0.0;
	int			bpos = 0;

	for (int i = 0; i < a->nnz; i++)
	{
		int32		ai = a->indices[i];
		int32		bi = -1;

		for (int j = bpos; j < b->nnz; j++)
		{
			bi = b->indices[j];

			if (ai == bi)
				distance += fabsf(ax[i] - bx[j]);
			else if (ai > bi)
				distance += fabsf(bx[j]);

			/* Update start for next iteration */
			if (ai >= bi)
				bpos = j + 1;

			/* Found or passed it */
			if (bi >= ai)
				break;
		}

		if (ai != bi)
			distance += fabsf(ax[i]);
	}

	for (int j = bpos; j < b->nnz; j++)
		distance += fabsf(bx[j]);

	return distance;
}
