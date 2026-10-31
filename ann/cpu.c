#include "backend.h"
#include "indexfile.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>

static void
Destroy(AnnIndex *index)
{
	free(index->vectors);
	memset(index, 0, sizeof(*index));
}

static AnnStatus
Build(AnnIndex *index, uint64_t generation, uint32_t rows,
	  uint32_t dimensions, const float *vectors)
{
	uint64_t	wideCount = (uint64_t) rows * dimensions;
	size_t		count;
	float	   *copy;

	if (generation == 0 || dimensions == 0 || dimensions > ANN_MAX_DIM ||
		wideCount > ANN_MAX_BYTES / sizeof(float) || (wideCount && vectors == NULL))
		return ANN_INVALID;
	count = (size_t) wideCount;
	for (size_t i = 0; i < count; i++)
		if (!isfinite(vectors[i]))
			return ANN_INVALID;
	copy = malloc(count ? count * sizeof(float) : 1);
	if (copy == NULL)
		return ANN_NOMEM;
	if (count)
		memcpy(copy, vectors, count * sizeof(float));
	Destroy(index);
	index->vectors = copy;
	index->rows = rows;
	index->dimensions = dimensions;
	index->generation = generation;
	return ANN_OK;
}

static int
Compare(const AnnCandidate *a, const AnnCandidate *b)
{
	if (a->distance != b->distance)
		return a->distance < b->distance ? -1 : 1;
	return a->id < b->id ? -1 : a->id > b->id;
}

/* Keep the worst result at the root of a bounded top-k heap. */
static void
SiftDown(AnnCandidate *heap, uint32_t count, uint32_t parent)
{
	for (;;)
	{
		uint32_t	child = parent * 2 + 1;
		AnnCandidate tmp;

		if (child >= count)
			break;
		if (child + 1 < count && Compare(&heap[child], &heap[child + 1]) < 0)
			child++;
		if (Compare(&heap[parent], &heap[child]) >= 0)
			break;
		tmp = heap[parent];
		heap[parent] = heap[child];
		heap[child] = tmp;
		parent = child;
	}
}

static int
SortCompare(const void *a, const void *b)
{
	return Compare(a, b);
}

static AnnStatus
Search(const AnnIndex *index, const float *query, uint32_t metric,
	   const uint32_t *ids, uint32_t nids, uint32_t k,
	   AnnCandidate *results, uint32_t *nresults)
{
	uint32_t	count = ids ? nids : index->rows;
	double		queryNorm = 0;

	*nresults = 0;
	if (metric > 2)
		return ANN_UNSUPPORTED;
	for (uint32_t d = 0; d < index->dimensions; d++)
	{
		if (!isfinite(query[d]))
			return ANN_INVALID;
		queryNorm += (double) query[d] * query[d];
	}
	if (metric == 2 && queryNorm == 0)
		return ANN_INVALID;

	/*
	 * Sorted unique local IDs prevent duplicate output and permit linear
	 * validation.
	 */
	for (uint32_t i = 0; ids && i < nids; i++)
		if (ids[i] >= index->rows || (i && ids[i] <= ids[i - 1]))
			return ANN_INVALID;
	for (uint32_t i = 0; i < count; i++)
	{
		uint32_t	id = ids ? ids[i] : i;
		const float *vector = index->vectors + (size_t) id * index->dimensions;
		double		distance = 0;
		double		norm = 0;
		AnnCandidate candidate;

		for (uint32_t d = 0; d < index->dimensions; d++)
		{
			double		a = vector[d];
			double		b = query[d];

			distance += metric == 0 ? (a - b) * (a - b) : a * b;
			norm += a * a;
		}
		if (metric == 2 && norm == 0)
			continue;
		candidate.id = id;
		candidate.distance = metric == 0 ? sqrt(distance) :
			metric == 1 ? -distance :
			1 - fmax(-1, fmin(1, distance / sqrt(norm * queryNorm)));
		if (k == 0)
			continue;
		if (*nresults < k)
		{
			uint32_t	child = (*nresults)++;

			results[child] = candidate;
			while (child && Compare(&results[(child - 1) / 2], &results[child]) < 0)
			{
				AnnCandidate tmp = results[child];

				results[child] = results[(child - 1) / 2];
				results[(child - 1) / 2] = tmp;
				child = (child - 1) / 2;
			}
		}
		else if (Compare(&candidate, &results[0]) < 0)
		{
			results[0] = candidate;
			SiftDown(results, *nresults, 0);
		}
	}
	qsort(results, *nresults, sizeof(*results), SortCompare);
	return ANN_OK;
}

/* Eight partial sums let compilers vectorize without fast-math flags. */
static float
L2Squared(const float *a, const float *b, uint32_t dimensions)
{
	float		sums[8] = {0};
	float		sum = 0;
	uint32_t	d = 0;

	for (; d + 8 <= dimensions; d += 8)
		for (int j = 0; j < 8; j++)
		{
			float		diff = a[d + j] - b[d + j];

			sums[j] += diff * diff;
		}
	for (; d < dimensions; d++)
		sum += (a[d] - b[d]) * (a[d] - b[d]);
	return sum + ((sums[0] + sums[1]) + (sums[2] + sums[3])) +
		((sums[4] + sums[5]) + (sums[6] + sums[7]));
}

/* Partition so position n holds the n-th smallest distance. */
static void
Select(AnnCandidate *items, uint32_t count, uint32_t n)
{
	uint32_t	left = 0;
	uint32_t	right = count - 1;

	while (left < right)
	{
		double		pivot = items[left + (right - left) / 2].distance;
		uint32_t	i = left;
		uint32_t	j = right;

		while (i <= j)
		{
			while (items[i].distance < pivot)
				i++;
			while (items[j].distance > pivot)
				j--;
			if (i <= j)
			{
				AnnCandidate tmp = items[i];

				items[i] = items[j];
				items[j] = tmp;
				i++;
				if (j == 0)
					break;
				j--;
			}
		}
		if (n <= j)
			right = j;
		else if (n >= i)
			left = i;
		else
			break;
	}
}

void
AnnGatheredExact(AnnGatheredJob *job)
{
	double		threshold;
	uint32_t	count = 0;

	job->nresults = 0;
	if (job->dimensions == 0 || job->dimensions > ANN_MAX_DIM || job->metric > 2)
	{
		job->status = ANN_INVALID;
		return;
	}
	if (job->metric != 0)
	{
		AnnIndex	index = {0};

		index.rows = job->rows;
		index.dimensions = job->dimensions;
		index.vectors = (float *) job->vectors;
		job->status = Search(&index, job->query, job->metric, NULL, 0,
							 job->rows, job->results, &job->nresults);
	}
	else
	{
		for (uint32_t i = 0; i < job->rows; i++)
		{
			double		distance = sqrt((double) L2Squared(job->vectors + (size_t) i * job->dimensions,
														   job->query, job->dimensions));

			if (!isfinite(distance))
			{
				job->status = ANN_INVALID;
				return;
			}
			job->results[i].id = i;
			job->results[i].distance = distance;
		}
		job->nresults = job->rows;
		job->status = ANN_OK;
	}
	if (job->status != ANN_OK)
		return;
	if (job->k == 0)
	{
		job->nresults = 0;
		return;
	}
	if (job->k < job->nresults)
	{
		/* Keep boundary ties and near-ties from differently rounded callers. */
		Select(job->results, job->nresults, job->k - 1);
		threshold = job->results[job->k - 1].distance;
		threshold += ANN_GATHERED_TOLERANCE * fabs(threshold) + 1e-6;
		for (uint32_t i = 0; i < job->nresults; i++)
			if (job->results[i].distance <= threshold)
				job->results[count++] = job->results[i];
		job->nresults = count;
	}
	qsort(job->results, job->nresults, sizeof(*job->results), SortCompare);
}

static void
SearchGathered(AnnGatheredJob *jobs, uint32_t count)
{
	for (uint32_t i = 0; i < count; i++)
		AnnGatheredExact(&jobs[i]);
}

static AnnStatus
LoadIndex(const char *name, const char *directory)
{
	AnnIndexFile *file;
	AnnStatus	status = AnnIndexFileOpen(name, directory, &file);

	if (status == ANN_OK && (status = AnnIndexRegister(file)) != ANN_OK)
		AnnIndexFileClose(file);
	return status;
}

/*
 * Exact reference for loaded indexes: each shard's key range is scanned
 * separately and the per-shard top-k lists are tree-merged, mirroring the
 * device route without approximation.
 */
static void
SearchIndexJob(AnnIndexJob *job)
{
	AnnIndexFile *file = AnnIndexGet(job->name);
	uint64_t   *ordinals = NULL;
	uint64_t	nordinals = 0;
	uint32_t	nshards;
	AnnIndexHit *lists = NULL;
	AnnIndexHit **heads = NULL;
	uint32_t   *counts = NULL;

	job->nresults = 0;
	if (file == NULL)
	{
		job->status = ANN_STALE;
		return;
	}
	if (job->dimensions != file->dimensions)
	{
		job->status = ANN_INVALID;
		goto done;
	}
	if (job->filtered)
	{
		ordinals = malloc(sizeof(*ordinals) * (job->nfilter ? job->nfilter : 1));
		if (ordinals == NULL)
		{
			job->status = ANN_NOMEM;
			goto done;
		}
		nordinals = AnnIndexOrdinals(file, job->filter, job->nfilter, ordinals);
	}
	nshards = file->nshards ? file->nshards : 1;
	lists = malloc(sizeof(*lists) * ((size_t) job->k + 1) * nshards);
	heads = malloc(sizeof(*heads) * nshards);
	counts = malloc(sizeof(*counts) * nshards);
	if (lists == NULL || heads == NULL || counts == NULL)
	{
		job->status = ANN_NOMEM;
		goto done;
	}
	for (uint32_t s = 0; s < nshards; s++)
	{
		AnnIndexJob part = *job;
		uint64_t	first = file->nshards ? file->shards[s].first : 0;
		uint64_t	end = file->nshards ? first + file->shards[s].rows : file->rows;
		uint64_t   *range = NULL;
		uint64_t	nrange = 0;
		uint64_t   *all = NULL;

		part.results = heads[s] = lists + (size_t) s * (job->k + 1);
		if (ordinals)
		{
			/* Ordinals are sorted; take this shard's slice. */
			uint64_t	lo = 0;

			while (lo < nordinals && ordinals[lo] < first)
				lo++;
			range = ordinals + lo;
			while (lo + nrange < nordinals && ordinals[lo + nrange] < end)
				nrange++;
		}
		else
		{
			all = malloc(sizeof(*all) * (end - first ? end - first : 1));
			if (all == NULL)
			{
				job->status = ANN_NOMEM;
				goto done;
			}
			for (uint64_t i = first; i < end; i++)
				all[i - first] = i;
			range = all;
			nrange = end - first;
		}
		AnnIndexExact(file, &part, range, nrange);
		counts[s] = part.nresults;
		free(all);
	}
	job->nresults = AnnIndexTreeMerge(heads, counts, nshards, job->k, job->results);
	job->route = job->filtered ? ANN_ROUTE_EXACT : ANN_ROUTE_ANN;
	job->status = ANN_OK;
done:
	free(lists);
	free(heads);
	free(counts);
	free(ordinals);
	AnnIndexPut(file);
}

static void
SearchIndex(AnnIndexJob *jobs, uint32_t count)
{
	for (uint32_t i = 0; i < count; i++)
		SearchIndexJob(&jobs[i]);
}

const AnnBackend *
AnnCpuBackend(void)
{
	static const AnnBackend backend = {
		"cpu-exact", 7, 7, "flat", "fp32", 1, 0, UINT32_MAX, Build, Search, Destroy,
		SearchGathered, NULL, LoadIndex, SearchIndex
	};

	return &backend;
}
