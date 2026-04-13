#include "postgres.h"

#include <math.h>

#include "int8utils.h"
#include "int8vec.h"

float		(*Int8vecL2SquaredDistance) (int dim, int8 *ax, int8 *bx);
float		(*Int8vecInnerProduct) (int dim, int8 *ax, int8 *bx);
double		(*Int8vecCosineSimilarity) (int dim, int8 *ax, int8 *bx);
float		(*Int8vecL1Distance) (int dim, int8 *ax, int8 *bx);

static float
Int8vecL2SquaredDistanceDefault(int dim, int8 *ax, int8 *bx)
{
	int32		distance = 0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		int32		diff = (int32) ax[i] - (int32) bx[i];

		distance += diff * diff;
	}

	return (float) distance;
}

static float
Int8vecInnerProductDefault(int dim, int8 *ax, int8 *bx)
{
	int32		distance = 0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
		distance += (int32) ax[i] * (int32) bx[i];

	return (float) distance;
}

static double
Int8vecCosineSimilarityDefault(int dim, int8 *ax, int8 *bx)
{
	int32		similarity = 0;
	int32		norma = 0;
	int32		normb = 0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		int32		axi = (int32) ax[i];
		int32		bxi = (int32) bx[i];

		similarity += axi * bxi;
		norma += axi * axi;
		normb += bxi * bxi;
	}

	/* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
	return (double) similarity / sqrt((double) norma * (double) normb);
}

static float
Int8vecL1DistanceDefault(int dim, int8 *ax, int8 *bx)
{
	int32		distance = 0;

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		int32		diff = (int32) ax[i] - (int32) bx[i];

		distance += (diff >= 0) ? diff : -diff;
	}

	return (float) distance;
}

void
Int8vecInit(void)
{
	Int8vecL2SquaredDistance = Int8vecL2SquaredDistanceDefault;
	Int8vecInnerProduct = Int8vecInnerProductDefault;
	Int8vecCosineSimilarity = Int8vecCosineSimilarityDefault;
	Int8vecL1Distance = Int8vecL1DistanceDefault;
}
