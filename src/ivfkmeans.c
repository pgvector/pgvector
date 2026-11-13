#include "postgres.h"

#include <float.h>
#include <limits.h>
#include <math.h>

#include "access/genam.h"
#include "fmgr.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/relcache.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

/*
 * Norm centers
 */
static void
NormCenters(const IvfflatTypeInfo * typeInfo, Oid collation, VectorArray centers, int dimensions)
{
	MemoryContext normCtx = AllocSetContextCreate(CurrentMemoryContext,
												  "Ivfflat norm temporary context",
												  ALLOCSET_DEFAULT_SIZES);

	IvfflatNormVectors(typeInfo, collation, centers, dimensions, normCtx);
	MemoryContextDelete(normCtx);
}

/*
 * Quick approach if we have no data
 */
static void
RandomCenters(Relation index, VectorArray centers, int dimensions, const IvfflatTypeInfo * typeInfo)
{
	FmgrInfo   *normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
	Oid			collation = index->rd_indcollation[0];
	float	   *x = palloc_array_checked(float, dimensions);

	/* Fill with random data */
	while (centers->length < centers->maxlen)
	{
		Pointer		center = VectorArrayGet(centers, centers->length++);

		for (int i = 0; i < dimensions; i++)
			x[i] = (float) RandomDouble();

		typeInfo->updateCenter(center, dimensions, x);
	}

	if (normprocinfo != NULL)
		NormCenters(typeInfo, collation, centers, dimensions);
}

#ifdef IVFFLAT_MEMORY
/*
 * Show memory usage
 */
static void
ShowMemoryUsage(MemoryContext context, Size estimatedSize)
{
	elog(INFO, "total memory: %zu MB",
		 MemoryContextMemAllocated(context, true) / (1024 * 1024));
	elog(INFO, "estimated memory: %zu MB", estimatedSize / (1024 * 1024));
}
#endif

/*
 * Fisher-Yates shuffle
 */
static void
VectorArrayShuffle(VectorArray arr)
{
	char	   *tmp = palloc(arr->itemsize);

	for (int i = arr->length - 1; i >= 1; i--)
	{
		int			j = RandomInt() % (i + 1);

		/* TODO Keep track of indices instead */
		memcpy(tmp, VectorArrayGet(arr, i), arr->itemsize);
		VectorArraySet(arr, i, VectorArrayGet(arr, j));
		VectorArraySet(arr, j, tmp);
	}

	pfree(tmp);
}

/*
 * Initialize with random centers
 */
static void
RandomChoice(VectorArray samples, VectorArray centers)
{
	VectorArrayShuffle(samples);

	for (int i = 0; i < centers->maxlen; i++)
		VectorArrayAdd(centers, VectorArrayGet(samples, i < samples->length ? i : 0));
}

/*
 * Materialize centers
 */
static void
MaterializeCenters(VectorArray centers, float *S, float *N, int dimensions, float *scratch, FmgrInfo *normprocinfo, Oid collation, const IvfflatTypeInfo * typeInfo)
{
	for (int j = 0; j < centers->length; j++)
	{
		float	   *Sj = S + (Size) j * dimensions;

		if (N[j] > 0)
		{
			for (int i = 0; i < dimensions; i++)
			{
				float		v = Sj[i] / N[j];

				if (isinf(v))
					v = v > 0 ? FLT_MAX : -FLT_MAX;

				scratch[i] = v;
			}
		}
		else
		{
			/* TODO Improve */
			for (int i = 0; i < dimensions; i++)
				scratch[i] = RandomDouble();
		}

		typeInfo->updateCenter(VectorArrayGet(centers, j), dimensions, scratch);
	}

	if (normprocinfo != NULL)
		NormCenters(typeInfo, collation, centers, dimensions);
}

/*
 * Assign clusters
 */
static void
AssignClusters(VectorArray centers, VectorArray samples, int start, int end, int *a, FmgrInfo *procinfo, Oid collation, const IvfflatTypeInfo * typeInfo)
{
	for (int i = start; i < end; i++)
	{
		Datum		vec = PointerGetDatum(VectorArrayGet(samples, i));
		float		minDistance = FLT_MAX;
		int			closestCenter = 0;

		for (int j = 0; j < centers->length; j++)
		{
			float		distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, j))));

			if (distance < minDistance)
			{
				minDistance = distance;
				closestCenter = j;
			}
		}

		a[i] = closestCenter;
	}
}

/*
 * Sum assignments
 */
static void
SumAssignments(int *n, float *s, int *a, int dimensions, int numCenters, VectorArray samples, int batchStart, int batchEnd, const IvfflatTypeInfo * typeInfo)
{
	for (int j = 0; j < numCenters; j++)
		n[j] = 0;

	for (int j = 0; j < (Size) numCenters * dimensions; j++)
		s[j] = 0;

	for (int i = batchStart; i < batchEnd; i++)
	{
		float	   *x = s + ((Size) a[i] * dimensions);

		n[a[i]]++;
		typeInfo->sumCenter(VectorArrayGet(samples, i), dimensions, x);
	}
}

/*
 * Revive dead clusters
 */
static void
ReviveDeadClusters(double tau, float *S, float *N, int k, int dimensions)
{
	double		sumN = 0.0;
	float		threshold;
	float		delta = FLT_EPSILON;

	for (int i = 0; i < k; i++)
		sumN += N[i];

	threshold = tau * (sumN / k);

	for (int j = 0; j < k; j++)
	{
		if (N[j] < threshold)
		{
			int			d;
			float	   *Sj;
			float	   *Sd;
			float		on;
			double		choice;
			double		choiceSum = 0.0;

			for (int i = 0; i < k; i++)
				choiceSum += N[i];

			/* TODO Draw donor without replacement */
			choice = choiceSum * RandomDouble();
			for (d = 0; d < k - 1; d++)
			{
				choice -= N[d];
				if (choice <= 0)
					break;
			}

			/* TODO Improve */
			if (d == j)
				continue;

			Sj = S + (Size) j * dimensions;
			Sd = S + (Size) d * dimensions;

			/* Donor's count is halved */
			on = N[d];
			N[d] /= 2;
			N[j] = N[d];

			/* Update target center */
			for (int i = 0; i < dimensions; i++)
				Sj[i] = (Sd[i] / on) * (1 + (i % 2 == 0 ? delta : -delta)) * N[j];

			/* Update donor center */
			for (int i = 0; i < dimensions; i++)
				Sd[i] = (Sd[i] / on) * (1 + (i % 2 == 0 ? -delta : delta)) * N[d];
		}
	}
}

/*
 * Use e-means for performance
 *
 * We use L2 distance for L2 (not L2 squared like index scan)
 * and angular distance for inner product and cosine distance
 *
 * https://arxiv.org/pdf/2611.09501
 */
static void
Emeans(Relation index, VectorArray samples, VectorArray centers, int dimensions, const IvfflatTypeInfo * typeInfo, Size memoryUsed)
{
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;
	int			numCenters = centers->maxlen;
	int			numSamples = samples->length;
	float	   *N;
	int		   *n;
	float	   *S;
	float	   *s;
	int		   *a;
	int			t = 0;
	int			T = 5;
	double		eps = 0.1;
	double		tau = 0.1;
	int			B;
	double		alpha0;
	int			M;
	float	   *scratch = palloc_array_checked(float, dimensions);

	/* Calculate allocation sizes */
	Size		NSize = mul_size(sizeof(float), numCenters);
	Size		nSize = mul_size(sizeof(int), numCenters);
	Size		SSize = mul_size(sizeof(float), mul_size(numCenters, dimensions));
	Size		sSize = mul_size(sizeof(float), mul_size(numCenters, dimensions));
	Size		aSize = mul_size(sizeof(int), numSamples);

	/* Calculate total size */
	Size		totalSize = memoryUsed;

	totalSize = add_size(totalSize, NSize);
	totalSize = add_size(totalSize, nSize);
	totalSize = add_size(totalSize, SSize);
	totalSize = add_size(totalSize, sSize);
	totalSize = add_size(totalSize, aSize);

	/* Check memory requirements */
	IvfflatCheckMemoryUsage(totalSize);

	/* Set support functions */
	procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
	normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
	collation = index->rd_indcollation[0];

	/* Allocate space */
	N = palloc(NSize);
	n = palloc(nSize);
	S = palloc(SSize);
	s = palloc(sSize);
	a = palloc(aSize);

#ifdef IVFFLAT_MEMORY
	ShowMemoryUsage(MemoryContextGetParent(CurrentMemoryContext), totalSize);
#endif

	/* Calculate batch size */
	B = 0.0044 * pow(numCenters, 0.13) * pow(numSamples, 0.88) * pow(T, 0.81);

	/* Calculate decay rate */
	alpha0 = 0.824 * pow(numCenters, -0.46) * pow(B, 0.9) * pow(numSamples, -0.45) * pow(T, -0.11);
	alpha0 = Min(alpha0, 1);

	M = T * (numSamples / B);

	/* Random initialization */
	RandomChoice(samples, centers);

	/* Initialize N */
	for (int j = 0; j < numCenters; j++)
		N[j] = 10e-10;

	/* Initialize S */
	for (int j = 0; j < numCenters; j++)
	{
		float	   *cj = S + ((Size) j * dimensions);

		for (int i = 0; i < dimensions; i++)
			cj[i] = 0;

		typeInfo->sumCenter(VectorArrayGet(centers, j), dimensions, cj);

		for (int i = 0; i < dimensions; i++)
			cj[i] *= N[j];
	}

	/* For T epochs */
	for (int epoch = 0; epoch < T; epoch++)
	{
		/* Fresh shuffle */
		VectorArrayShuffle(samples);

		/* For each batch of B points */
		for (int batch = 0; batch < numSamples / B; batch++)
		{
			int			batchStart = batch * B;
			int			batchEnd = batchStart + B;
			float		alpha;
			float		y;

			/* Can take a while, so ensure we can interrupt */
			CHECK_FOR_INTERRUPTS();

			/* Equation 5 */
			alpha = alpha0 * (eps + (1 - eps) * 0.5 * (1 + cos(M_PI * t / M)));
			y = 1 - alpha;

			t = t + 1;

			/* Assign to clusters */
			MaterializeCenters(centers, S, N, dimensions, scratch, normprocinfo, collation, typeInfo);
			AssignClusters(centers, samples, batchStart, batchEnd, a, procinfo, collation, typeInfo);

			/* Update nj and sj */
			SumAssignments(n, s, a, dimensions, numCenters, samples, batchStart, batchEnd, typeInfo);

			/* Equation 2: Nj */
			for (int j = 0; j < numCenters; j++)
				N[j] = y * N[j] + (1 - y) * n[j];

			/* Equation 2: Sj */
			for (int j = 0; j < (Size) numCenters * dimensions; j++)
				S[j] = y * S[j] + (1 - y) * s[j];

			/* Dead cluster revival */
			if (t % 5 == 0)
				ReviveDeadClusters(tau, S, N, numCenters, dimensions);
		}
	}

	/* Last-epoch mean finalization */
	MaterializeCenters(centers, S, N, dimensions, scratch, normprocinfo, collation, typeInfo);
	AssignClusters(centers, samples, 0, numSamples, a, procinfo, collation, typeInfo);
	SumAssignments(n, S, a, dimensions, numCenters, samples, 0, numSamples, typeInfo);
	for (int j = 0; j < numCenters; j++)
		N[j] = n[j];
	MaterializeCenters(centers, S, N, dimensions, scratch, normprocinfo, collation, typeInfo);
}

/*
 * Ensure no NaN or infinite values
 */
static void
CheckElements(VectorArray centers, int dimensions, const IvfflatTypeInfo * typeInfo)
{
	float	   *scratch = palloc_array_checked(float, dimensions);

	for (int i = 0; i < centers->length; i++)
	{
		for (int j = 0; j < dimensions; j++)
			scratch[j] = 0;

		/* /fp:fast may not propagate NaN with MSVC, but that's alright */
		typeInfo->sumCenter(VectorArrayGet(centers, i), dimensions, scratch);

		for (int j = 0; j < dimensions; j++)
		{
			if (isnan(scratch[j]))
				elog(ERROR, "NaN detected. Please report a bug.");

			if (isinf(scratch[j]))
				elog(ERROR, "Infinite value detected. Please report a bug.");
		}
	}
}

/*
 * Ensure no zero vectors for cosine distance
 */
static void
CheckNorms(VectorArray centers, Relation index)
{
	/* Check NORM_PROC instead of KMEANS_NORM_PROC */
	FmgrInfo   *normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	Oid			collation = index->rd_indcollation[0];

	if (normprocinfo == NULL)
		return;

	for (int i = 0; i < centers->length; i++)
	{
		double		norm = DatumGetFloat8(FunctionCall1Coll(normprocinfo, collation, PointerGetDatum(VectorArrayGet(centers, i))));

		if (norm == 0)
			elog(ERROR, "Zero norm detected. Please report a bug.");
	}
}

/*
 * Detect issues with centers
 */
static void
CheckCenters(Relation index, VectorArray centers, int dimensions, const IvfflatTypeInfo * typeInfo)
{
	if (centers->length != centers->maxlen)
		elog(ERROR, "Not enough centers. Please report a bug.");

	CheckElements(centers, dimensions, typeInfo);
	CheckNorms(centers, index);
}

/*
 * Perform naive k-means centering
 * We use spherical k-means for inner product and cosine
 */
void
IvfflatKmeans(Relation index, VectorArray samples, VectorArray centers, int dimensions, const IvfflatTypeInfo * typeInfo, Size memoryUsed)
{
	MemoryContext kmeansCtx = AllocSetContextCreate(CurrentMemoryContext,
													"Ivfflat kmeans temporary context",
													ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldCtx = MemoryContextSwitchTo(kmeansCtx);

	if (samples->length == 0)
		RandomCenters(index, centers, dimensions, typeInfo);
	else
		Emeans(index, samples, centers, dimensions, typeInfo, memoryUsed);

	CheckCenters(index, centers, dimensions, typeInfo);

	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(kmeansCtx);
}
