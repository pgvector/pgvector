#ifndef __APPLE__
#define _POSIX_C_SOURCE 200809L
#endif
#include "batching.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static double
Seconds(void)
{
	struct timespec ts;

	(void) clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec + ts.tv_nsec / 1000000000.0;
}

static uint32_t
Number(const char *text, uint32_t maximum)
{
	char	   *end;
	unsigned long value = strtoul(text, &end, 10);

	if (!*text ||*text == '-' || *end || value > maximum)
	{
		fprintf(stderr, "invalid benchmark argument: %s\n", text);
		exit(2);
	}
	return (uint32_t) value;
}

int
main(int argc, char **argv)
{
	uint32_t	rows;
	uint32_t	dims;
	uint32_t	offered;
	uint32_t	passing;
	int			sequential;
	int			unique;
	AnnBatchOptions options;
	AnnBatcher *batcher = NULL;
	AnnIndex	index = {0};
	AnnBatchStats stats = {0};
	AnnRequest **requests;
	uint32_t   *expected;
	uint32_t   *ids;
	float	   *vectors;
	uint64_t	success = 0;
	uint64_t	rejected = 0;
	uint64_t	failed = 0;
	double		start;
	double		elapsed;

	if (argc != 10)
		return 2;
	rows = Number(argv[1], 1000000);
	dims = Number(argv[2], ANN_MAX_DIM);
	offered = Number(argv[3], 100000);
	passing = Number(argv[4], rows);
	AnnBatchDefaultOptions(&options);
	options.max_batch_rows = Number(argv[5], 1024);
	options.max_pending_requests = Number(argv[6], 100000);
	options.delay_us = Number(argv[7], 60000000);
	unique = (int) Number(argv[8], 1);
	sequential = (int) Number(argv[9], 1);
	if (!rows || !dims || !offered || (uint64_t) rows * dims > ANN_MAX_BYTES / 4)
		return 2;
	vectors = malloc((size_t) rows * dims * sizeof(float));
	ids = malloc((size_t) (passing ? passing : 1) * sizeof(uint32_t));
	requests = calloc(offered, sizeof(*requests));
	expected = calloc(offered, sizeof(*expected));
	if (!vectors || !ids || !requests || !expected)
		return 2;
	for (uint32_t i = 0; i < rows; i++)
		for (uint32_t d = 0; d < dims; d++)
			vectors[(size_t) i * dims + d] = (float) i + (float) d / (dims + 1);
	if (sequential)
	{
		if (AnnCpuBackend()->build(&index, 1, rows, dims, vectors) != ANN_OK)
			return 2;
	}
	else if (AnnBatchCreate(AnnCpuBackend(), 1, rows, dims, vectors, &options, &batcher) != ANN_OK)
		return 2;
	start = Seconds();
	for (uint32_t i = 0; i < offered; i++)
	{
		uint32_t	offset = unique ? i % (rows - passing + 1) : 0;
		AnnStatus	status;

		/*
		 * An empty subset has no query row requirement; avoid the end
		 * ordinal.
		 */
		if (offset >= rows)
			offset = 0;
		expected[i] = offset;
		for (uint32_t j = 0; j < passing; j++)
			ids[j] = offset + j;
		if (sequential)
		{
			AnnCandidate results[10];
			uint32_t	count;

			status = AnnCpuBackend()->search(&index, vectors + (size_t) offset * dims,
											 0, ids, passing, 10, results, &count);
			if (status != ANN_OK || (passing && (!count || results[0].id != offset || results[0].distance != 0)))
				failed++;
			else
				success++;
		}
		else
		{
			status = AnnBatchSubmit(batcher, vectors + (size_t) offset * dims,
									10, 0, ids, passing, 30000000, &requests[i]);
			if (status == ANN_OVERLOADED)
				rejected++;
			else if (status != ANN_OK)
				failed++;
		}
	}
	for (uint32_t i = 0; !sequential && i < offered; i++)
	{
		if (requests[i])
		{
			uint32_t	count;
			AnnStatus	status = AnnRequestWait(requests[i], 30000000);
			const AnnCandidate *results = AnnRequestResults(requests[i], &count);

			if (status != ANN_OK || (passing && (!count || results[0].id != expected[i] || results[0].distance != 0)))
				failed++;
			else
				success++;
			AnnRequestRelease(requests[i]);
		}
	}
	if (batcher)
	{
		do
		{
			struct timespec pause = {0, 100000};

			AnnBatchGetStats(batcher, &stats);
			if (stats.pending_requests)
				(void) nanosleep(&pause, NULL);
		} while (stats.pending_requests);
	}
	elapsed = Seconds() - start;
	printf("{\"implementation\":\"native-c\",\"offered\":%u,\"successful\":%" PRIu64
		   ",\"rejected\":%" PRIu64 ",\"failed\":%" PRIu64 ",\"elapsed_seconds\":%.9f,"
		   "\"successful_vector_searches_per_second\":%.3f,\"batches\":%" PRIu64
		   ",\"max_observed_batch\":%" PRIu64 ",\"pending_bytes\":%" PRIu64 "}\n",
		   offered, success, rejected, failed, elapsed, success / elapsed,
		   stats.batches, stats.max_observed_batch, stats.pending_bytes);
	if (batcher)
		AnnBatchClose(batcher);
	AnnCpuBackend()->destroy(&index);
	free(vectors);
	free(ids);
	free(requests);
	free(expected);
	return failed ? 1 : 0;
}
