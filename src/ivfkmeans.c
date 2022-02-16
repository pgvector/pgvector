#include "postgres.h"

#include <float.h>

#include "catalog/index.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"

#if PG_VERSION_NUM >= 120000
#include "access/tableam.h"
#endif

#if PG_VERSION_NUM >= 130000
#define CALLBACK_ITEM_POINTER ItemPointer tid
#else
#define CALLBACK_ITEM_POINTER HeapTuple hup
#endif

/*
 * Initialize with kmeans++
 *
 * https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf
 */
static void
InitCenters(Relation index, VectorArray samples, VectorArray centers)
{
	FmgrInfo   *procinfo;
	Oid			collation;
	int			i;
	int			j;
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

	for (i = 0; i < numCenters - 1; i++)
	{
		CHECK_FOR_INTERRUPTS();

		sum = 0.0;

		for (j = 0; j < numSamples; j++)
		{
			vec = VectorArrayGet(samples, j);

			/* Only need to compute distance for new center */
			/* TODO Use triangle inequality to reduce distance calculations */
			distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(vec), PointerGetDatum(VectorArrayGet(centers, i))));

			/* Use distance squared for weighted probability distribution */
			distance *= distance;

			if (distance < weight[j])
				weight[j] = distance;

			sum += weight[j];
		}

		/* Choose new center using weighted probability distribution. */
		choice = sum * (((double) random()) / MAX_RANDOM_VALUE);
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
			vec->x[j] = ((double) random()) / MAX_RANDOM_VALUE;

		/* Normalize if needed (only needed for random centers) */
		if (normprocinfo != NULL)
			ApplyNorm(normprocinfo, collation, vec);

		centers->length++;
	}
}

/*
 * Callback for sampling
 */
static void
SampleCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
			   bool *isnull, bool tupleIsAlive, void *state)
{
	IvfflatBuildState *buildstate = (IvfflatBuildState *) state;
	VectorArray samples = buildstate->samples;
	int			targsamples = samples->maxlen;
	Datum		value = values[0];

	/* Skip nulls */
	if (isnull[0])
		return;

	/*
	 * Normalize with KMEANS_NORM_PROC since spherical distance function
	 * expects unit vectors
	 */
	if (buildstate->kmeansnormprocinfo != NULL)
	{
		if (!IvfflatNormValue(buildstate->kmeansnormprocinfo, buildstate->collation, &value, buildstate->normvec))
			return;
	}

	if (samples->length < targsamples)
	{
		VectorArraySet(samples, samples->length, DatumGetVector(value));
		samples->length++;
	}
	else
	{
		if (buildstate->rowstoskip < 0)
			buildstate->rowstoskip = reservoir_get_next_S(&buildstate->rstate, samples->length, targsamples);

		if (buildstate->rowstoskip <= 0)
		{
			int			k = (int) (targsamples * sampler_random_fract(buildstate->rstate.randstate));

			Assert(k >= 0 && k < targsamples);
			VectorArraySet(samples, k, DatumGetVector(value));
		}

		buildstate->rowstoskip -= 1;
	}
}

/*
 * Sample rows with same logic as ANALYZE
 */
static void
SampleRows(IvfflatBuildState * buildstate)
{
	int			targsamples = buildstate->samples->maxlen;
	BlockNumber totalblocks = RelationGetNumberOfBlocks(buildstate->heap);

	buildstate->rowstoskip = -1;
	buildstate->samples->length = 0;

	BlockSampler_Init(&buildstate->bs, totalblocks, targsamples, random());

	reservoir_init_selection_state(&buildstate->rstate, targsamples);
	while (BlockSampler_HasMore(&buildstate->bs))
	{
		BlockNumber targblock = BlockSampler_Next(&buildstate->bs);

#if PG_VERSION_NUM >= 120000
		table_index_build_range_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
									 false, true, false, targblock, 1, SampleCallback, (void *) buildstate, NULL);
#elif PG_VERSION_NUM >= 110000
		IndexBuildHeapRangeScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
								false, true, targblock, 1, SampleCallback, (void *) buildstate, NULL);
#else
		IndexBuildHeapRangeScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
								false, true, targblock, 1, SampleCallback, (void *) buildstate);
#endif
	}
}

/*
 * Use mini-batch k-means
 *
 * We use L2 distance for L2 (not L2 squared like index scan)
 * and angular distance for inner product and cosine distance
 *
 * https://www.eecs.tufts.edu/~dsculley/papers/fastkmeans.pdf
 */
static void
MiniBatchKmeans(IvfflatBuildState * buildstate)
{
	VectorArray centers = buildstate->centers;
	int			b = buildstate->samples->maxlen;
	int			t = 20;
	double		distance;
	double		minDistance;
	int			closestCenter;
	int			i;
	int			j;
	int			k;
	VectorArray m;
	Vector	   *c;
	Vector	   *x;
	int		   *v;
	int		   *d;
	double		eta;

	/* Set support functions */
	FmgrInfo   *procinfo = index_getprocinfo(buildstate->index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
	FmgrInfo   *normprocinfo = buildstate->kmeansnormprocinfo;
	Oid			collation = buildstate->index->rd_indcollation[0];

	/* Pick initial centers */
	InitCenters(buildstate->index, buildstate->samples, buildstate->centers);

	v = palloc(sizeof(int) * centers->maxlen);
	d = palloc(sizeof(int) * b);

	for (int i = 0; i < centers->length; i++)
		v[i] = 0;

	for (i = 0; i < t; i++)
	{
		/* Can take a while, so ensure we can interrupt */
		CHECK_FOR_INTERRUPTS();

		/* Get b examples picked randomly from X */
		SampleRows(buildstate);
		m = buildstate->samples;

		/* Cache nearest center to x */
		for (j = 0; j < m->length; j++)
		{
			/* compute closest */
			minDistance = DBL_MAX;
			closestCenter = -1;

			x = VectorArrayGet(m, j);

			/* Find closest center */
			for (k = 0; k < centers->length; k++)
			{
				distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(x), PointerGetDatum(VectorArrayGet(centers, k))));

				if (distance < minDistance)
				{
					minDistance = distance;
					closestCenter = k;
				}
			}

			d[j] = closestCenter;
		}

		for (j = 0; j < m->length; j++)
		{
			x = VectorArrayGet(m, j);

			/* Get cached center for this x */
			c = VectorArrayGet(centers, d[j]);

			/* Update per-center counts */
			v[d[j]]++;

			/* Get per-center learning rate */
			eta = 1.0 / v[d[j]];

			/* Take gradient step */
			for (k = 0; k < c->dim; k++)
				c->x[k] = (1 - eta) * c->x[k] + eta * x->x[k];
		}

		/* Check for empty centers (likely duplicates) */
		if (i == 0)
		{
			for (j = 0; j < centers->length; j++)
			{
				if (v[j] == 0)
				{
					c = VectorArrayGet(centers, j);

					/* TODO Handle empty centers properly */
					for (k = 0; k < c->dim; k++)
						c->x[k] = ((double) random()) / MAX_RANDOM_VALUE;
				}
			}
		}

		/* Normalize if needed */
		if (normprocinfo != NULL)
		{
			for (j = 0; j < centers->length; j++)
				ApplyNorm(normprocinfo, collation, VectorArrayGet(centers, j));
		}
	}

	pfree(v);
	pfree(d);
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
 * Perform k-means clustering
 * We use spherical k-means for inner product and cosine
 */
void
IvfflatKmeans(IvfflatBuildState * buildstate)
{
	int			numSamples;
	Size		totalSize;

	/* Target 10 samples per list, with at least 10000 samples */
	/* The number of samples has a large effect on index build time */
	numSamples = buildstate->lists * 10;
	if (numSamples < 10000)
		numSamples = 10000;

	/* Skip samples for unlogged table */
	if (buildstate->heap == NULL)
		numSamples = 1;

	/* Calculate total size */
	totalSize = VECTOR_ARRAY_SIZE(numSamples, buildstate->dimensions);

	/* Check memory requirements */
	/* Add one to error message to ceil */
	if (totalSize / 1024 > maintenance_work_mem)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("memory required is %zu MB, maintenance_work_mem is %d MB",
						totalSize / (1024 * 1024) + 1, maintenance_work_mem / 1024)));

	/* Sample rows */
	buildstate->samples = VectorArrayInit(numSamples, buildstate->dimensions);
	if (buildstate->heap != NULL)
		SampleRows(buildstate);

	if (buildstate->samples->length <= buildstate->centers->maxlen)
		QuickCenters(buildstate->index, buildstate->samples, buildstate->centers);
	else
		MiniBatchKmeans(buildstate);

	CheckCenters(buildstate->index, buildstate->centers);

	/* Free samples before we allocate more memory */
	pfree(buildstate->samples);
}
