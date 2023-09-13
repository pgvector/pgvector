#include "postgres.h"

#include <math.h>

#include "utils/array.h"

/*
 * Get the L2 distance between vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(float4_l2_distance);
Datum
float4_l2_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	float	   *ax = (float *) ARR_DATA_PTR(a);
	float	   *bx = (float *) ARR_DATA_PTR(b);
	float		distance = 0.0;
	float		diff;

	/* TODO Check rank, dimensions, and nulls */
	int			dim = ARR_DIMS(a)[0];

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		diff = ax[i] - bx[i];
		distance += diff * diff;
	}

	PG_RETURN_FLOAT8(sqrt((double) distance));
}

/*
 * Get the L2 squared distance between vectors
 * This saves a sqrt calculation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(float4_l2_squared_distance);
Datum
float4_l2_squared_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	float	   *ax = (float *) ARR_DATA_PTR(a);
	float	   *bx = (float *) ARR_DATA_PTR(b);
	float		distance = 0.0;
	float		diff;

	/* TODO Check rank, dimensions, and nulls */
	int			dim = ARR_DIMS(a)[0];

	/* Auto-vectorized */
	for (int i = 0; i < dim; i++)
	{
		diff = ax[i] - bx[i];
		distance += diff * diff;
	}

	PG_RETURN_FLOAT8((double) distance);
}
