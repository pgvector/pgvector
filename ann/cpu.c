#include "backend.h"

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
	uint64_t wideCount = (uint64_t) rows * dimensions;
	size_t count;
	float *copy;

	if (generation == 0 || dimensions == 0 || dimensions > ANN_MAX_DIM ||
		wideCount > ANN_MAX_BYTES / sizeof(float))
		return ANN_INVALID;
	count = (size_t) wideCount;
	for (size_t i = 0; i < count; i++)
		if (!isfinite(vectors[i]))
			return ANN_INVALID;
	copy = malloc(count ? count * sizeof(float) : 1);
	if (copy == NULL)
		return ANN_NOMEM;
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
		uint32_t child = parent * 2 + 1;
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
	uint32_t count = ids ? nids : index->rows;
	double queryNorm = 0;

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
	/* Sorted unique local IDs prevent duplicate output and permit linear validation. */
	for (uint32_t i = 0; ids && i < nids; i++)
		if (ids[i] >= index->rows || (i && ids[i] <= ids[i - 1]))
			return ANN_INVALID;
	for (uint32_t i = 0; i < count; i++)
	{
		uint32_t id = ids ? ids[i] : i;
		const float *vector = index->vectors + (size_t) id * index->dimensions;
		double distance = 0;
		double norm = 0;
		AnnCandidate candidate;

		for (uint32_t d = 0; d < index->dimensions; d++)
		{
			double a = vector[d];
			double b = query[d];

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
			uint32_t child = (*nresults)++;

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

const AnnBackend *
AnnCpuBackend(void)
{
	static const AnnBackend backend = {
		"cpu-exact", 7, 7, "flat", "fp32", 1, 0, UINT32_MAX, Build, Search, Destroy
	};

	return &backend;
}
