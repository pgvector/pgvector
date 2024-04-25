#include "postgres.h"

#include <float.h>
#include <math.h>

#include "bitvec.h"
#include "halfutils.h"
#include "halfvec.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "vector.h"

typedef struct KmeansState
{
	void		(*initCenter) (Pointer v, int dimensions);
	void		(*updateCenter) (Pointer v, float *x);
	void		(*sumCenter) (Pointer v, float *x);
	int			(*comp) (const void *a, const void *b);
	bool		separateAgg;
	bool		checkDuplicates;
}			KmeansState;

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
	int64		j;
	float	   *weight = palloc(samples->length * sizeof(float));
	int			numCenters = centers->maxlen;
	int			numSamples = samples->length;

	procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
	collation = index->rd_indcollation[0];

	/* Choose an initial center uniformly at random */
	VectorArraySet(centers, 0, VectorArrayGet(samples, RandomInt() % samples->length));
	centers->length++;

	for (j = 0; j < numSamples; j++)
		weight[j] = FLT_MAX;

	for (int i = 0; i < numCenters; i++)
	{
		double		sum;
		double		choice;

		CHECK_FOR_INTERRUPTS();

		sum = 0.0;

		for (j = 0; j < numSamples; j++)
		{
			Datum		vec = PointerGetDatum(VectorArrayGet(samples, j));
			double		distance;

			/* Only need to compute distance for new center */
			/* TODO Use triangle inequality to reduce distance calculations */
			distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, i))));

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
 * Norm centers
 */
static void
NormCenters(FmgrInfo *normalizeprocinfo, Oid collation, VectorArray centers)
{
	MemoryContext normCtx = AllocSetContextCreate(CurrentMemoryContext,
												  "Ivfflat norm temporary context",
												  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldCtx = MemoryContextSwitchTo(normCtx);

	for (int j = 0; j < centers->length; j++)
	{
		Datum		center = PointerGetDatum(VectorArrayGet(centers, j));
		Datum		newCenter = IvfflatNormValue(normalizeprocinfo, collation, center);
		Size		size = VARSIZE_ANY(DatumGetPointer(newCenter));

		if (size > centers->itemsize)
			elog(ERROR, "safety check failed");

		memcpy(DatumGetPointer(center), DatumGetPointer(newCenter), size);
		MemoryContextReset(normCtx);
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(normCtx);
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
 * Compare half vectors
 */
static int
CompareHalfVectors(const void *a, const void *b)
{
	return halfvec_cmp_internal((HalfVector *) a, (HalfVector *) b);
}

/*
 * Compare bit vectors
 */
static int
CompareBitVectors(const void *a, const void *b)
{
	return DirectFunctionCall2(bitcmp, VarBitPGetDatum((VarBit *) a), VarBitPGetDatum((VarBit *) b));
}

/*
 * Sort vector array
 */
static void
SortVectorArray(VectorArray arr, KmeansState * kmeansstate)
{
	qsort(arr->items, arr->length, arr->itemsize, kmeansstate->comp);
}

static void
VectorInitCenter(Pointer v, int dimensions)
{
	Vector	   *vec = (Vector *) v;

	SET_VARSIZE(vec, VECTOR_SIZE(dimensions));
	vec->dim = dimensions;
}

static void
HalfvecInitCenter(Pointer v, int dimensions)
{
	HalfVector *vec = (HalfVector *) v;

	SET_VARSIZE(vec, HALFVEC_SIZE(dimensions));
	vec->dim = dimensions;
}

static void
BitInitCenter(Pointer v, int dimensions)
{
	VarBit	   *vec = (VarBit *) v;

	SET_VARSIZE(vec, VARBITTOTALLEN(dimensions));
	VARBITLEN(vec) = dimensions;
}

static void
VectorUpdateCenter(Pointer v, float *x)
{
	Vector	   *newCenter = (Vector *) v;

	for (int k = 0; k < newCenter->dim; k++)
		newCenter->x[k] = x[k];
}

static void
HalfvecUpdateCenter(Pointer v, float *x)
{
	HalfVector *newCenter = (HalfVector *) v;

	for (int k = 0; k < newCenter->dim; k++)
		newCenter->x[k] = Float4ToHalfUnchecked(x[k]);
}

static void
BitUpdateCenter(Pointer v, float *x)
{
	VarBit	   *newCenter = (VarBit *) v;
	unsigned char *nx = VARBITS(newCenter);

	for (uint32 k = 0; k < VARBITBYTES(newCenter); k++)
		nx[k] = 0;

	for (int k = 0; k < VARBITLEN(newCenter); k++)
		nx[k / 8] |= (x[k] > 0.5 ? 1 : 0) << (7 - (k % 8));
}

/*
 * Quick approach if we have little data
 */
static void
QuickCenters(Relation index, VectorArray samples, VectorArray centers, KmeansState * kmeansstate)
{
	int			dimensions = centers->dim;
	Oid			collation = index->rd_indcollation[0];
	FmgrInfo   *normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
	FmgrInfo   *normalizeprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORMALIZE_PROC);
	float	   *x = (float *) palloc(sizeof(float) * dimensions);

	/* Copy existing vectors while avoiding duplicates */
	if (samples->length > 0)
	{
		SortVectorArray(samples, kmeansstate);

		for (int i = 0; i < samples->length; i++)
		{
			Datum		vec = PointerGetDatum(VectorArrayGet(samples, i));

			if (i == 0 || !datumIsEqual(vec, PointerGetDatum(VectorArrayGet(samples, i - 1)), false, -1))
			{
				VectorArraySet(centers, centers->length, DatumGetPointer(vec));
				centers->length++;
			}
		}
	}

	/* Fill remaining with random data */
	while (centers->length < centers->maxlen)
	{
		Pointer		center = VectorArrayGet(centers, centers->length);

		for (int i = 0; i < dimensions; i++)
			x[i] = (float) RandomDouble();

		kmeansstate->initCenter(center, dimensions);
		kmeansstate->updateCenter(center, x);

		centers->length++;
	}

	/* Fine if existing vectors are normalized twice */
	if (normprocinfo != NULL)
		NormCenters(normalizeprocinfo, collation, centers);

	pfree(x);
}

#ifdef IVFFLAT_MEMORY
/*
 * Show memory usage
 */
static void
ShowMemoryUsage(MemoryContext context, Size estimatedSize)
{
#if PG_VERSION_NUM >= 130000
	elog(INFO, "total memory: %zu MB",
		 MemoryContextMemAllocated(context, true) / (1024 * 1024));
#else
	MemoryContextStats(context);
#endif
	elog(INFO, "estimated memory: %zu MB", estimatedSize / (1024 * 1024));
}
#endif

static void
VectorSumCenter(Pointer v, float *x)
{
	Vector	   *vec = (Vector *) v;

	for (int k = 0; k < vec->dim; k++)
		x[k] += vec->x[k];
}

static void
HalfvecSumCenter(Pointer v, float *x)
{
	HalfVector *vec = (HalfVector *) v;

	for (int k = 0; k < vec->dim; k++)
		x[k] += HalfToFloat4(vec->x[k]);
}

static void
BitSumCenter(Pointer v, float *x)
{
	VarBit	   *vec = (VarBit *) v;

	for (int k = 0; k < VARBITLEN(v); k++)
		x[k] += (float) (((VARBITS(vec)[k / 8]) >> (7 - (k % 8))) & 0x01);
}

/*
 * Sum centers
 */
static void
SumCenters(VectorArray samples, VectorArray aggCenters, int *closestCenters, KmeansState * kmeansstate)
{
	for (int j = 0; j < samples->length; j++)
	{
		Vector	   *aggCenter = (Vector *) VectorArrayGet(aggCenters, closestCenters[j]);

		kmeansstate->sumCenter(VectorArrayGet(samples, j), aggCenter->x);
	}
}

/*
 * Set new centers
 */
static void
UpdateCenters(VectorArray aggCenters, VectorArray newCenters, KmeansState * kmeansstate)
{
	for (int j = 0; j < aggCenters->length; j++)
	{
		Vector	   *aggCenter = (Vector *) VectorArrayGet(aggCenters, j);

		kmeansstate->updateCenter(VectorArrayGet(newCenters, j), aggCenter->x);
	}
}

/*
 * Compute new centers
 */
static void
ComputeNewCenters(VectorArray samples, VectorArray aggCenters, VectorArray newCenters, int *centerCounts, int *closestCenters, FmgrInfo *normprocinfo, FmgrInfo *normalizeprocinfo, Oid collation, KmeansState * kmeansstate)
{
	int			dimensions = aggCenters->dim;
	int			numCenters = aggCenters->maxlen;
	int			numSamples = samples->length;

	/* Reset sum and count */
	for (int j = 0; j < numCenters; j++)
	{
		Vector	   *vec = (Vector *) VectorArrayGet(aggCenters, j);

		for (int k = 0; k < dimensions; k++)
			vec->x[k] = 0.0;

		centerCounts[j] = 0;
	}

	/* Increment sum of closest center */
	SumCenters(samples, aggCenters, closestCenters, kmeansstate);

	/* Increment count of closest center */
	for (int j = 0; j < numSamples; j++)
		centerCounts[closestCenters[j]] += 1;

	/* Divide sum by count */
	for (int j = 0; j < numCenters; j++)
	{
		Vector	   *vec = (Vector *) VectorArrayGet(aggCenters, j);

		if (centerCounts[j] > 0)
		{
			/* Double avoids overflow, but requires more memory */
			/* TODO Update bounds */
			for (int k = 0; k < dimensions; k++)
			{
				if (isinf(vec->x[k]))
					vec->x[k] = vec->x[k] > 0 ? FLT_MAX : -FLT_MAX;
			}

			for (int k = 0; k < dimensions; k++)
				vec->x[k] /= centerCounts[j];
		}
		else
		{
			/* TODO Handle empty centers properly */
			for (int k = 0; k < dimensions; k++)
				vec->x[k] = RandomDouble();
		}
	}

	/* Set new centers if different from agg centers */
	if (kmeansstate->separateAgg)
		UpdateCenters(aggCenters, newCenters, kmeansstate);

	/* Normalize if needed */
	if (normprocinfo != NULL)
		NormCenters(normalizeprocinfo, collation, newCenters);
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
ElkanKmeans(Relation index, VectorArray samples, VectorArray centers, KmeansState * kmeansstate)
{
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	FmgrInfo   *normalizeprocinfo;
	Oid			collation;
	int			dimensions = centers->dim;
	int			numCenters = centers->maxlen;
	int			numSamples = samples->length;
	VectorArray newCenters;
	VectorArray aggCenters;
	int		   *centerCounts;
	int		   *closestCenters;
	float	   *lowerBound;
	float	   *upperBound;
	float	   *s;
	float	   *halfcdist;
	float	   *newcdist;
	MemoryContext kmeansCtx;
	MemoryContext oldCtx;

	/* Calculate allocation sizes */
	Size		samplesSize = VECTOR_ARRAY_SIZE(samples->maxlen, samples->itemsize);
	Size		centersSize = VECTOR_ARRAY_SIZE(centers->maxlen, centers->itemsize);
	Size		newCentersSize = VECTOR_ARRAY_SIZE(numCenters, centers->itemsize);
	Size		aggCentersSize = !kmeansstate->separateAgg ? 0 : VECTOR_ARRAY_SIZE(numCenters, VECTOR_SIZE(dimensions));
	Size		centerCountsSize = sizeof(int) * numCenters;
	Size		closestCentersSize = sizeof(int) * numSamples;
	Size		lowerBoundSize = sizeof(float) * numSamples * numCenters;
	Size		upperBoundSize = sizeof(float) * numSamples;
	Size		sSize = sizeof(float) * numCenters;
	Size		halfcdistSize = sizeof(float) * numCenters * numCenters;
	Size		newcdistSize = sizeof(float) * numCenters;

	/* Calculate total size */
	Size		totalSize = samplesSize + centersSize + newCentersSize + aggCentersSize + centerCountsSize + closestCentersSize + lowerBoundSize + upperBoundSize + sSize + halfcdistSize + newcdistSize;

	/* Check memory requirements */
	/* Add one to error message to ceil */
	if (totalSize > (Size) maintenance_work_mem * 1024L)
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
	normalizeprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORMALIZE_PROC);
	collation = index->rd_indcollation[0];

	/* Use memory context */
	kmeansCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Ivfflat kmeans temporary context",
									  ALLOCSET_DEFAULT_SIZES);
	oldCtx = MemoryContextSwitchTo(kmeansCtx);

	/* Allocate space */
	/* Use float instead of double to save memory */
	centerCounts = palloc(centerCountsSize);
	closestCenters = palloc(closestCentersSize);
	lowerBound = palloc_extended(lowerBoundSize, MCXT_ALLOC_HUGE);
	upperBound = palloc(upperBoundSize);
	s = palloc(sSize);
	halfcdist = palloc_extended(halfcdistSize, MCXT_ALLOC_HUGE);
	newcdist = palloc(newcdistSize);

	/* Initialize new centers */
	newCenters = VectorArrayInit(numCenters, dimensions, centers->itemsize);
	newCenters->length = numCenters;

	for (int j = 0; j < numCenters; j++)
		kmeansstate->initCenter(VectorArrayGet(newCenters, j), dimensions);

	/* Initialize agg centers */
	if (!kmeansstate->separateAgg)
	{
		/* Use same centers to save memory */
		aggCenters = newCenters;
	}
	else
	{
		aggCenters = VectorArrayInit(numCenters, dimensions, VECTOR_SIZE(dimensions));
		aggCenters->length = numCenters;

		for (int j = 0; j < numCenters; j++)
			VectorInitCenter(VectorArrayGet(aggCenters, j), dimensions);
	}

#ifdef IVFFLAT_MEMORY
	ShowMemoryUsage(oldCtx, totalSize);
#endif

	/* Pick initial centers */
	InitCenters(index, samples, centers, lowerBound);

	/* Assign each x to its closest initial center c(x) = argmin d(x,c) */
	for (int64 j = 0; j < numSamples; j++)
	{
		float		minDistance = FLT_MAX;
		int			closestCenter = 0;

		/* Find closest center */
		for (int64 k = 0; k < numCenters; k++)
		{
			/* TODO Use Lemma 1 in k-means++ initialization */
			float		distance = lowerBound[j * numCenters + k];

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
	for (int iteration = 0; iteration < 500; iteration++)
	{
		int			changes = 0;
		bool		rjreset;

		/* Can take a while, so ensure we can interrupt */
		CHECK_FOR_INTERRUPTS();

		/* Step 1: For all centers, compute distance */
		for (int64 j = 0; j < numCenters; j++)
		{
			Datum		vec = PointerGetDatum(VectorArrayGet(centers, j));

			for (int64 k = j + 1; k < numCenters; k++)
			{
				float		distance = 0.5 * DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, k))));

				halfcdist[j * numCenters + k] = distance;
				halfcdist[k * numCenters + j] = distance;
			}
		}

		/* For all centers c, compute s(c) */
		for (int64 j = 0; j < numCenters; j++)
		{
			float		minDistance = FLT_MAX;

			for (int64 k = 0; k < numCenters; k++)
			{
				float		distance;

				if (j == k)
					continue;

				distance = halfcdist[j * numCenters + k];
				if (distance < minDistance)
					minDistance = distance;
			}

			s[j] = minDistance;
		}

		rjreset = iteration != 0;

		for (int64 j = 0; j < numSamples; j++)
		{
			bool		rj;

			/* Step 2: Identify all points x such that u(x) <= s(c(x)) */
			if (upperBound[j] <= s[closestCenters[j]])
				continue;

			rj = rjreset;

			for (int64 k = 0; k < numCenters; k++)
			{
				Datum		vec;
				float		dxcx;

				/* Step 3: For all remaining points x and centers c */
				if (k == closestCenters[j])
					continue;

				if (upperBound[j] <= lowerBound[j * numCenters + k])
					continue;

				if (upperBound[j] <= halfcdist[closestCenters[j] * numCenters + k])
					continue;

				vec = PointerGetDatum(VectorArrayGet(samples, j));

				/* Step 3a */
				if (rj)
				{
					dxcx = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, closestCenters[j]))));

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
					float		dxc = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, k))));

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
		ComputeNewCenters(samples, aggCenters, newCenters, centerCounts, closestCenters, normprocinfo, normalizeprocinfo, collation, kmeansstate);

		/* Step 5 */
		for (int j = 0; j < numCenters; j++)
			newcdist[j] = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(VectorArrayGet(centers, j)), PointerGetDatum(VectorArrayGet(newCenters, j))));

		for (int64 j = 0; j < numSamples; j++)
		{
			for (int64 k = 0; k < numCenters; k++)
			{
				float		distance = lowerBound[j * numCenters + k] - newcdist[k];

				if (distance < 0)
					distance = 0;

				lowerBound[j * numCenters + k] = distance;
			}
		}

		/* Step 6 */
		/* We reset r(x) before Step 3 in the next iteration */
		for (int j = 0; j < numSamples; j++)
			upperBound[j] += newcdist[closestCenters[j]];

		/* Step 7 */
		for (int j = 0; j < numCenters; j++)
			VectorArraySet(centers, j, VectorArrayGet(newCenters, j));

		if (changes == 0 && iteration != 0)
			break;
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(kmeansCtx);
}

/*
 * Detect issues with centers
 */
static void
CheckCenters(Relation index, VectorArray centers, KmeansState * kmeansstate)
{
	FmgrInfo   *normprocinfo;
	float	   *scratch = palloc(sizeof(float) * centers->dim);

	if (centers->length != centers->maxlen)
		elog(ERROR, "Not enough centers. Please report a bug.");

	/* Ensure no NaN or infinite values */
	for (int i = 0; i < centers->length; i++)
	{
		for (int j = 0; j < centers->dim; j++)
			scratch[j] = 0;

		kmeansstate->sumCenter(VectorArrayGet(centers, i), scratch);

		for (int j = 0; j < centers->dim; j++)
		{
			if (isnan(scratch[j]))
				elog(ERROR, "NaN detected. Please report a bug.");

			if (isinf(scratch[j]))
				elog(ERROR, "Infinite value detected. Please report a bug.");
		}
	}

	if (kmeansstate->checkDuplicates)
	{
		/* Ensure no duplicate centers */
		SortVectorArray(centers, kmeansstate);

		for (int i = 1; i < centers->length; i++)
		{
			if (datumIsEqual(PointerGetDatum(VectorArrayGet(centers, i)), PointerGetDatum(VectorArrayGet(centers, i - 1)), false, -1))
				elog(ERROR, "Duplicate centers detected. Please report a bug.");
		}
	}

	/* Ensure no zero vectors for cosine distance */
	/* Check NORM_PROC instead of KMEANS_NORM_PROC */
	normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	if (normprocinfo != NULL)
	{
		Oid			collation = index->rd_indcollation[0];

		for (int i = 0; i < centers->length; i++)
		{
			double		norm = DatumGetFloat8(FunctionCall1Coll(normprocinfo, collation, PointerGetDatum(VectorArrayGet(centers, i))));

			if (norm == 0)
				elog(ERROR, "Zero norm detected. Please report a bug.");
		}
	}

	pfree(scratch);
}

static void
InitKmeansState(KmeansState * kmeansstate, IvfflatType type)
{
	if (type == IVFFLAT_TYPE_VECTOR)
	{
		kmeansstate->initCenter = VectorInitCenter;
		kmeansstate->updateCenter = VectorUpdateCenter;
		kmeansstate->sumCenter = VectorSumCenter;
		kmeansstate->comp = CompareVectors;
		kmeansstate->separateAgg = false;
		kmeansstate->checkDuplicates = true;
	}
	else if (type == IVFFLAT_TYPE_HALFVEC)
	{
		kmeansstate->initCenter = HalfvecInitCenter;
		kmeansstate->updateCenter = HalfvecUpdateCenter;
		kmeansstate->sumCenter = HalfvecSumCenter;
		kmeansstate->comp = CompareHalfVectors;
		kmeansstate->separateAgg = true;
		kmeansstate->checkDuplicates = true;
	}
	else if (type == IVFFLAT_TYPE_BIT)
	{
		kmeansstate->initCenter = BitInitCenter;
		kmeansstate->updateCenter = BitUpdateCenter;
		kmeansstate->sumCenter = BitSumCenter;
		kmeansstate->comp = CompareBitVectors;
		kmeansstate->separateAgg = true;
		kmeansstate->checkDuplicates = false;
	}
	else
		elog(ERROR, "Unsupported type");
}

/*
 * Perform naive k-means centering
 * We use spherical k-means for inner product and cosine
 */
void
IvfflatKmeans(Relation index, VectorArray samples, VectorArray centers, IvfflatType type)
{
	KmeansState kmeansstate;

	InitKmeansState(&kmeansstate, type);

	if (samples->length <= centers->maxlen)
		QuickCenters(index, samples, centers, &kmeansstate);
	else
		ElkanKmeans(index, samples, centers, &kmeansstate);

	CheckCenters(index, centers, &kmeansstate);
}
