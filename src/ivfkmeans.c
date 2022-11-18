#include "postgres.h"

#include <float.h>

#include "ivfflat.h"
#include "miscadmin.h"

#if PG_VERSION_NUM >= 150000
#include "common/pg_prng.h"
#endif

#if PG_VERSION_NUM >= 150000
#define RandomDouble() pg_prng_double(&pg_global_prng_state)
#else
#define RandomDouble() (((double) random()) / MAX_RANDOM_VALUE)
#endif

/*
 * Initialize with kmeans++
 *
 * https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf
 */
static void
InitCenters(Relation index, VectorArray samples, VectorArray centers, float *lowerBound)
{
	FmgrInfo   *procinfo;
	Oid			collation;
	int			i;
	int64		j;
	double		distance;
	double		sum;
	double		choice;
	Vector	   *vec;
	float	   *weight = palloc(samples->length * sizeof(float));
	int			numCenters = centers->maxlen;
	int			numSamples = samples->length;

	procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
	collation = index->rd_indcollation[0];

	/* Choose an initial center uniformly at random */
	VectorArraySet(centers, 0, VectorArrayGet(samples, random() % samples->length));
	centers->length++;

	for (j = 0; j < numSamples; j++)
		weight[j] = DBL_MAX;

	for (i = 0; i < numCenters; i++)
	{
		CHECK_FOR_INTERRUPTS();

		sum = 0.0;

		for (j = 0; j < numSamples; j++)
		{
			vec = VectorArrayGet(samples, j);

			/* Only need to compute distance for new center */
			/* TODO Use triangle inequality to reduce distance calculations */
			distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(vec), PointerGetDatum(VectorArrayGet(centers, i))));

			/* Set lower bound */
			lowerBound[j * numCenters + i] = distance;

			/* Use distance squared for weighted probability distribution */
			distance *= distance;

			if (distance < weight[j])
				weight[j] = distance;

			sum += weight[j];
		}

		/* Only compute lower bound on last iteration */
		if (i + 1 == numCenters)
			break;

		/* Choose new center using weighted probability distribution. */
		choice = sum * RandomDouble();
		for (j = 0; j < numSamples - 1; j++)
		{
			choice -= weight[j];
			if (choice <= 0)
				break;
		}

		VectorArraySet(centers, i + 1, VectorArrayGet(samples, j));
		centers->length++;
	}

	pfree(weight);
}

/*
 * Apply norm to vector
 */
static inline void
ApplyNorm(FmgrInfo *normprocinfo, Oid collation, Vector * vec)
{
	int			i;
	double		norm = DatumGetFloat8(FunctionCall1Coll(normprocinfo, collation, PointerGetDatum(vec)));

	/* TODO Handle zero norm */
	if (norm > 0)
	{
		for (i = 0; i < vec->dim; i++)
			vec->x[i] /= norm;
	}
}

/*
 * Compare vectors
 */
static int
CompareVectors(const void *a, const void *b)
{
	return vector_cmp_internal((Vector *) a, (Vector *) b);
}

/*
 * Quick approach if we have little data
 */
static void
QuickCenters(Relation index, VectorArray samples, VectorArray centers)
{
	int			i;
	int			j;
	Vector	   *vec;
	int			dimensions = centers->dim;
	Oid			collation = index->rd_indcollation[0];
	FmgrInfo   *normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);

	/* Copy existing vectors while avoiding duplicates */
	if (samples->length > 0)
	{
		qsort(samples->items, samples->length, VECTOR_SIZE(samples->dim), CompareVectors);
		for (i = 0; i < samples->length; i++)
		{
			vec = VectorArrayGet(samples, i);

			if (i == 0 || CompareVectors(vec, VectorArrayGet(samples, i - 1)) != 0)
			{
				VectorArraySet(centers, centers->length, vec);
				centers->length++;
			}
		}
	}

	/* Fill remaining with random data */
	while (centers->length < centers->maxlen)
	{
		vec = VectorArrayGet(centers, centers->length);

		SET_VARSIZE(vec, VECTOR_SIZE(dimensions));
		vec->dim = dimensions;

		for (j = 0; j < dimensions; j++)
			vec->x[j] = RandomDouble();

		/* Normalize if needed (only needed for random centers) */
		if (normprocinfo != NULL)
			ApplyNorm(normprocinfo, collation, vec);

		centers->length++;
	}
}

/*
 * Use Elkan for performance. This requires distance function to satisfy triangle inequality.
 *
 * We use L2 distance for L2 (not L2 squared like index scan)
 * and angular distance for inner product and cosine distance
 *
 * https://www.aaai.org/Papers/ICML/2003/ICML03-022.pdf
 */
static void
ElkanKmeans(Relation index, VectorArray samples, VectorArray centers)
{
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;
	Vector	   *vec;
	Vector	   *newCenter;
	int			iteration;
	int64		j;
	int64		k;
	int			dimensions = centers->dim;
	int			numCenters = centers->maxlen;
	int			numSamples = samples->length;
	VectorArray newCenters;
	int		   *centerCounts;
	int		   *closestCenters;
	float	   *lowerBound;
	float	   *upperBound;
	float	   *s;
	float	   *halfcdist;
	float	   *newcdist;
	int			changes;
	double		minDistance;
	int			closestCenter;
	double		distance;
	bool		rj;
	bool		rjreset;
	double		dxcx;
	double		dxc;

	/* Calculate allocation sizes */
	Size		samplesSize = VECTOR_ARRAY_SIZE(samples->maxlen, samples->dim);
	Size		centersSize = VECTOR_ARRAY_SIZE(centers->maxlen, centers->dim);
	Size		newCentersSize = VECTOR_ARRAY_SIZE(numCenters, dimensions);
	Size		centerCountsSize = sizeof(int) * numCenters;
	Size		closestCentersSize = sizeof(int) * numSamples;
	Size		lowerBoundSize = sizeof(float) * numSamples * numCenters;
	Size		upperBoundSize = sizeof(float) * numSamples;
	Size		sSize = sizeof(float) * numCenters;
	Size		halfcdistSize = sizeof(float) * numCenters * numCenters;
	Size		newcdistSize = sizeof(float) * numCenters;

	/* Calculate total size */
	Size		totalSize = samplesSize + centersSize + newCentersSize + centerCountsSize + closestCentersSize + lowerBoundSize + upperBoundSize + sSize + halfcdistSize + newcdistSize;

	/* Check memory requirements */
	/* Add one to error message to ceil */
	if (totalSize / 1024 > maintenance_work_mem)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("memory required is %zu MB, maintenance_work_mem is %d MB",
						totalSize / (1024 * 1024) + 1, maintenance_work_mem / 1024)));

	/* Ensure indexing does not overflow */
	if (numCenters * numCenters > INT_MAX)
		elog(ERROR, "Indexing overflow detected. Please report a bug.");

	/* Set support functions */
	procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
	normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
	collation = index->rd_indcollation[0];

	/* Allocate space */
	/* Use float instead of double to save memory */
	centerCounts = palloc(centerCountsSize);
	closestCenters = palloc(closestCentersSize);
	lowerBound = palloc_extended(lowerBoundSize, MCXT_ALLOC_HUGE);
	upperBound = palloc(upperBoundSize);
	s = palloc(sSize);
	halfcdist = palloc_extended(halfcdistSize, MCXT_ALLOC_HUGE);
	newcdist = palloc(newcdistSize);

	newCenters = VectorArrayInit(numCenters, dimensions);
	for (j = 0; j < numCenters; j++)
	{
		vec = VectorArrayGet(newCenters, j);
		SET_VARSIZE(vec, VECTOR_SIZE(dimensions));
		vec->dim = dimensions;
	}

	/* Pick initial centers */
	InitCenters(index, samples, centers, lowerBound);

	/* Assign each x to its closest initial center c(x) = argmin d(x,c) */
	for (j = 0; j < numSamples; j++)
	{
		minDistance = DBL_MAX;
		closestCenter = -1;

		/* Find closest center */
		for (k = 0; k < numCenters; k++)
		{
			/* TODO Use Lemma 1 in k-means++ initialization */
			distance = lowerBound[j * numCenters + k];

			if (distance < minDistance)
			{
				minDistance = distance;
				closestCenter = k;
			}
		}

		upperBound[j] = minDistance;
		closestCenters[j] = closestCenter;
	}

	/* Give 500 iterations to converge */
	for (iteration = 0; iteration < 500; iteration++)
	{
		/* Can take a while, so ensure we can interrupt */
		CHECK_FOR_INTERRUPTS();

		changes = 0;

		/* Step 1: For all centers, compute distance */
		for (j = 0; j < numCenters; j++)
		{
			vec = VectorArrayGet(centers, j);

			for (k = j + 1; k < numCenters; k++)
			{
				distance = 0.5 * DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(vec), PointerGetDatum(VectorArrayGet(centers, k))));
				halfcdist[j * numCenters + k] = distance;
				halfcdist[k * numCenters + j] = distance;
			}
		}

		/* For all centers c, compute s(c) */
		for (j = 0; j < numCenters; j++)
		{
			minDistance = DBL_MAX;

			for (k = 0; k < numCenters; k++)
			{
				if (j == k)
					continue;

				distance = halfcdist[j * numCenters + k];
				if (distance < minDistance)
					minDistance = distance;
			}

			s[j] = minDistance;
		}

		rjreset = iteration != 0;

		for (j = 0; j < numSamples; j++)
		{
			/* Step 2: Identify all points x such that u(x) <= s(c(x)) */
			if (upperBound[j] <= s[closestCenters[j]])
				continue;

			rj = rjreset;

			for (k = 0; k < numCenters; k++)
			{
				/* Step 3: For all remaining points x and centers c */
				if (k == closestCenters[j])
					continue;

				if (upperBound[j] <= lowerBound[j * numCenters + k])
					continue;

				if (upperBound[j] <= halfcdist[closestCenters[j] * numCenters + k])
					continue;

				vec = VectorArrayGet(samples, j);

				/* Step 3a */
				if (rj)
				{
					dxcx = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(vec), PointerGetDatum(VectorArrayGet(centers, closestCenters[j]))));

					/* d(x,c(x)) computed, which is a form of d(x,c) */
					lowerBound[j * numCenters + closestCenters[j]] = dxcx;
					upperBound[j] = dxcx;

					rj = false;
				}
				else
					dxcx = upperBound[j];

				/* Step 3b */
				if (dxcx > lowerBound[j * numCenters + k] || dxcx > halfcdist[closestCenters[j] * numCenters + k])
				{
					dxc = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(vec), PointerGetDatum(VectorArrayGet(centers, k))));

					/* d(x,c) calculated */
					lowerBound[j * numCenters + k] = dxc;

					if (dxc < dxcx)
					{
						closestCenters[j] = k;

						/* c(x) changed */
						upperBound[j] = dxc;

						changes++;
					}

				}
			}
		}

		/* Step 4: For each center c, let m(c) be mean of all points assigned */
		for (j = 0; j < numCenters; j++)
		{
			vec = VectorArrayGet(newCenters, j);
			for (k = 0; k < dimensions; k++)
				vec->x[k] = 0.0;

			centerCounts[j] = 0;
		}

		for (j = 0; j < numSamples; j++)
		{
			vec = VectorArrayGet(samples, j);
			closestCenter = closestCenters[j];

			/* Increment sum and count of closest center */
			newCenter = VectorArrayGet(newCenters, closestCenter);
			for (k = 0; k < dimensions; k++)
				newCenter->x[k] += vec->x[k];

			centerCounts[closestCenter] += 1;
		}

		for (j = 0; j < numCenters; j++)
		{
			vec = VectorArrayGet(newCenters, j);

			if (centerCounts[j] > 0)
			{
				for (k = 0; k < dimensions; k++)
					vec->x[k] /= centerCounts[j];
			}
			else
			{
				/* TODO Handle empty centers properly */
				for (k = 0; k < dimensions; k++)
					vec->x[k] = RandomDouble();
			}

			/* Normalize if needed */
			if (normprocinfo != NULL)
				ApplyNorm(normprocinfo, collation, vec);
		}

		/* Step 5 */
		for (j = 0; j < numCenters; j++)
			newcdist[j] = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(VectorArrayGet(centers, j)), PointerGetDatum(VectorArrayGet(newCenters, j))));

		for (j = 0; j < numSamples; j++)
		{
			for (k = 0; k < numCenters; k++)
			{
				distance = lowerBound[j * numCenters + k] - newcdist[k];

				if (distance < 0)
					distance = 0;

				lowerBound[j * numCenters + k] = distance;
			}
		}

		/* Step 6 */
		/* We reset r(x) before Step 3 in the next iteration */
		for (j = 0; j < numSamples; j++)
			upperBound[j] += newcdist[closestCenters[j]];

		/* Step 7 */
		for (j = 0; j < numCenters; j++)
			memcpy(VectorArrayGet(centers, j), VectorArrayGet(newCenters, j), VECTOR_SIZE(dimensions));

		if (changes == 0 && iteration != 0)
			break;
	}

	pfree(newCenters);
	pfree(centerCounts);
	pfree(closestCenters);
	pfree(lowerBound);
	pfree(upperBound);
	pfree(s);
	pfree(halfcdist);
	pfree(newcdist);
}

/*
 * Detect issues with centers
 */
static void
CheckCenters(Relation index, VectorArray centers)
{
	FmgrInfo   *normprocinfo;
	Oid			collation;
	int			i;
	double		norm;

	if (centers->length != centers->maxlen)
		elog(ERROR, "Not enough centers. Please report a bug.");

	/* Ensure no duplicate centers */
	/* Fine to sort in-place */
	qsort(centers->items, centers->length, VECTOR_SIZE(centers->dim), CompareVectors);
	for (i = 1; i < centers->length; i++)
	{
		if (CompareVectors(VectorArrayGet(centers, i), VectorArrayGet(centers, i - 1)) == 0)
			elog(ERROR, "Duplicate centers detected. Please report a bug.");
	}

	/* Ensure no zero vectors for cosine distance */
	/* Check NORM_PROC instead of KMEANS_NORM_PROC */
	normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	if (normprocinfo != NULL)
	{
		collation = index->rd_indcollation[0];

		for (i = 0; i < centers->length; i++)
		{
			norm = DatumGetFloat8(FunctionCall1Coll(normprocinfo, collation, PointerGetDatum(VectorArrayGet(centers, i))));
			if (norm == 0)
				elog(ERROR, "Zero norm detected. Please report a bug.");
		}
	}
}

/*
 * Perform naive k-means centering
 * We use spherical k-means for inner product and cosine
 */
void
IvfflatKmeans(Relation index, VectorArray samples, VectorArray centers)
{
	if (samples->length <= centers->maxlen)
		QuickCenters(index, samples, centers);
	else
		ElkanKmeans(index, samples, centers);

	CheckCenters(index, centers);
}
