#ifndef __APPLE__
#define _POSIX_C_SOURCE 200809L
#endif
#include "batching.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static void
Sleep(uint64_t milliseconds)
{
	struct timespec ts = {(time_t) (milliseconds / 1000), (long) ((milliseconds % 1000) * 1000000)};

	(void) nanosleep(&ts, NULL);
}

static AnnStatus
FailSearch(const AnnIndex *index, const float *query, uint32_t metric,
		   const uint32_t *ids, uint32_t nids, uint32_t k,
		   AnnCandidate *results, uint32_t *count)
{
	(void) index;
	(void) query;
	(void) metric;
	(void) ids;
	(void) nids;
	(void) k;
	(void) results;
	*count = 0;
	return ANN_BACKEND_ERROR;
}

static AnnStatus
SlowSearch(const AnnIndex *index, const float *query, uint32_t metric,
		   const uint32_t *ids, uint32_t nids, uint32_t k,
		   AnnCandidate *results, uint32_t *count)
{
	Sleep(50);
	return AnnCpuBackend()->search(index, query, metric, ids, nids, k, results, count);
}

static AnnBatcher *
Create(AnnBatchOptions *options, int slow)
{
	float		vectors[128];
	AnnBatcher *batcher;
	static AnnBackend backend;

	for (uint32_t i = 0; i < 64; i++)
	{
		vectors[i * 2] = (float) i;
		vectors[i * 2 + 1] = 1;
	}
	backend = *AnnCpuBackend();
	if (slow)
		backend.search = SlowSearch;
	if (slow == 2)
		backend.search = FailSearch;
	assert(AnnBatchCreate(&backend, 1, 64, 2, vectors, options, &batcher) == ANN_OK);
	return batcher;
}

static AnnRequest *
Submit(AnnBatcher *batcher, uint32_t ordinal, uint64_t timeout)
{
	float		query[2] = {(float) ordinal, 1};
	uint32_t	ids[2] = {ordinal, ordinal + 1};
	AnnRequest *request;

	assert(AnnBatchSubmit(batcher, query, UINT32_MAX, 0, ids, 2, timeout, &request) == ANN_OK);
	/* Inputs can change immediately after submission. */
	query[0] = -100;
	ids[0] = 63;
	return request;
}

static void
Result		(AnnRequest *request, uint32_t ordinal)
{
	uint32_t	count;
	const AnnCandidate *results;

	assert(AnnRequestWait(request, 3000000) == ANN_OK);
	results = AnnRequestResults(request, &count);
	assert(count == 2 && results[0].id == ordinal && results[0].distance == 0);
	assert(results[1].id == ordinal + 1 && results[1].distance == 1);
}

static void
Idle(AnnBatcher *batcher)
{
	AnnBatchStats stats;

	for (int i = 0; i < 3000; i++)
	{
		AnnBatchGetStats(batcher, &stats);
		if (!stats.pending_requests)
			break;
		Sleep(1);
	}
	assert(stats.pending_requests == 0 && stats.pending_bytes == 0 && stats.pending_work == 0);
}

static void
Batch(void)
{
	AnnBatchOptions options;
	AnnBatcher *batcher;
	AnnRequest *requests[4];
	AnnBatchStats stats;

	AnnBatchDefaultOptions(&options);
	options.max_batch_rows = 4;
	options.delay_us = 1000000;
	batcher = Create(&options, 0);
	for (uint32_t i = 0; i < 4; i++)
		requests[i] = Submit(batcher, i * 5, 3000000);
	for (uint32_t i = 0; i < 4; i++)
		Result		(requests[i], i * 5);

	Idle(batcher);
	AnnBatchGetStats(batcher, &stats);
	assert(stats.batches == 1 && stats.max_observed_batch == 4 && stats.completed == 4);
	AnnBatchClose(batcher);
	/* Result handles outlive the service and its index safely. */
	for (uint32_t i = 0; i < 4; i++)
	{
		Result		(requests[i], i * 5);

		AnnRequestRelease(requests[i]);
	}
}

static void
Admission(void)
{
	AnnBatchOptions options;
	AnnBatcher *batcher;
	AnnRequest *a;
	AnnRequest *b;
	float		query[2] = {1, 1};
	AnnBatchStats stats;
	uint64_t	one_request_bytes;

	AnnBatchDefaultOptions(&options);
	options.delay_us = 1000000;
	batcher = Create(&options, 0);
	a = Submit(batcher, 1, 3000000);
	AnnBatchGetStats(batcher, &stats);
	one_request_bytes = stats.pending_bytes;
	assert(AnnRequestCancel(a));
	AnnRequestRelease(a);
	AnnBatchClose(batcher);
	for (int mode = 0; mode < 3; mode++)
	{
		AnnBatchDefaultOptions(&options);
		options.delay_us = 1000000;
		if (mode == 0)
			options.max_pending_requests = 1;
		if (mode == 1)
			options.max_pending_bytes = one_request_bytes + one_request_bytes / 2;
		if (mode == 2)
			options.max_pending_work = 5;
		batcher = Create(&options, 0);
		a = Submit(batcher, 1, 3000000);
		assert(AnnBatchSubmit(batcher, query, 2, 0, NULL, 0, 3000000, &b) == ANN_OVERLOADED);
		assert(b == NULL && AnnRequestCancel(a));
		assert(AnnRequestPoll(a) == ANN_CANCELLED);
		Idle(batcher);
		AnnBatchGetStats(batcher, &stats);
		assert(stats.rejected == 1 && stats.cancelled == 1);
		AnnRequestRelease(a);
		AnnBatchClose(batcher);
	}
}

static void
Deadlines(void)
{
	AnnBatchOptions options;
	AnnBatcher *batcher;
	AnnRequest *request;
	AnnBatchStats stats;

	AnnBatchDefaultOptions(&options);
	options.delay_us = 1000000;
	batcher = Create(&options, 0);
	request = Submit(batcher, 1, 10000);
	assert(AnnRequestWait(request, 1000000) == ANN_TIMED_OUT);
	Idle(batcher);
	AnnBatchGetStats(batcher, &stats);
	assert(stats.batches == 0 && stats.timed_out == 1);
	AnnRequestRelease(request);
	AnnBatchClose(batcher);

	options.delay_us = 0;
	batcher = Create(&options, 1);
	request = Submit(batcher, 1, 20000);
	assert(AnnRequestWait(request, 1000000) == ANN_TIMED_OUT);
	AnnBatchGetStats(batcher, &stats);
	assert(stats.pending_requests == 1 && stats.pending_bytes > 0);
	assert(!AnnRequestCancel(request));
	Idle(batcher);
	AnnRequestRelease(request);
	AnnBatchClose(batcher);
}

typedef struct Producer
{
	AnnBatcher *batcher;
	uint32_t	offset;
} Producer;

static void *
Produce(void *arg)
{
	Producer   *producer = arg;
	AnnRequest *requests[16];

	for (uint32_t i = 0; i < 16; i++)
		requests[i] = Submit(producer->batcher, (producer->offset + i) % 60, 3000000);
	for (uint32_t i = 0; i < 16; i++)
	{
		Result		(requests[i], (producer->offset + i) % 60);

		AnnRequestRelease(requests[i]);
	}
	return NULL;
}

static void
Concurrent(void)
{
	AnnBatchOptions options;
	AnnBatcher *batcher;
	pthread_t	threads[8];
	Producer	producers[8];
	AnnBatchStats stats;

	AnnBatchDefaultOptions(&options);
	options.max_batch_rows = 16;
	options.delay_us = 5000;
	batcher = Create(&options, 0);
	for (uint32_t i = 0; i < 8; i++)
	{
		producers[i] = (Producer)
		{
			batcher, i * 8
		};
		assert(!pthread_create(&threads[i], NULL, Produce, &producers[i]));
	}
	for (uint32_t i = 0; i < 8; i++)
		assert(!pthread_join(threads[i], NULL));
	Idle(batcher);
	AnnBatchGetStats(batcher, &stats);
	assert(stats.completed == 128 && stats.max_observed_batch <= 16 && stats.batches < 128);
	AnnBatchClose(batcher);
}

static void
Lifetime(void)
{
	AnnBatchOptions options;
	AnnBatcher *batcher;
	AnnBatchStats stats;
	AnnRequest *request;
	uint32_t	count;

	AnnBatchDefaultOptions(&options);
	batcher = Create(&options, 0);
	for (uint32_t i = 0; i < 100; i++)
		AnnRequestRelease(Submit(batcher, i % 60, 3000000));
	Idle(batcher);
	AnnBatchGetStats(batcher, &stats);
	assert(stats.completed == 100);
	AnnBatchClose(batcher);

	options.delay_us = 0;
	batcher = Create(&options, 1);
	request = Submit(batcher, 1, 3000000);
	for (int i = 0; i < 1000; i++)
	{
		AnnBatchGetStats(batcher, &stats);
		if (stats.batches)
			break;
		Sleep(1);
	}
	assert(stats.batches == 1 && !AnnRequestCancel(request));
	assert(AnnRequestWait(request, 1) == ANN_PENDING);
	AnnBatchClose(batcher);
	assert(AnnRequestPoll(request) == ANN_CANCELLED);
	assert(AnnRequestResults(request, &count) == NULL && count == 0);
	AnnRequestRelease(request);
}

static void
Errors(void)
{
	AnnBatchOptions options;
	AnnBatcher *batcher;
	AnnRequest *request;
	AnnBatchStats stats;

	AnnBatchDefaultOptions(&options);
	options.delay_us = 0;
	batcher = Create(&options, 2);
	request = Submit(batcher, 1, 3000000);
	assert(AnnRequestWait(request, 1000000) == ANN_BACKEND_ERROR);
	Idle(batcher);
	AnnBatchGetStats(batcher, &stats);
	assert(stats.failed == 1);
	AnnRequestRelease(request);
	AnnBatchClose(batcher);
}

static void
Validate(void)
{
	AnnBatchOptions options;
	AnnBatcher *batcher;
	AnnRequest *request;
	float		query[2] = {1, 1};
	uint32_t	ids[2] = {2, 1};

	AnnBatchDefaultOptions(&options);
	options.delay_us = 1000000;
	batcher = Create(&options, 0);
	assert(AnnBatchSubmit(batcher, query, 1, 99, NULL, 0, 1000, &request) == ANN_INVALID);
	assert(AnnBatchSubmit(batcher, query, 1, 0, ids, 2, 1000, &request) == ANN_INVALID);
	assert(AnnBatchSubmit(batcher, query, 1, 0, ids, 2, 0, &request) == ANN_INVALID);
	request = Submit(batcher, 1, 3000000);
	AnnBatchClose(batcher);
	assert(AnnRequestPoll(request) == ANN_CANCELLED);
	AnnRequestRelease(request);
}

int
main(int argc, char **argv)
{
	if (argc != 2)
		return 2;
	if (!strcmp(argv[1], "batch"))
		Batch();
	else if (!strcmp(argv[1], "admission"))
		Admission();
	else if (!strcmp(argv[1], "deadlines"))
		Deadlines();
	else if (!strcmp(argv[1], "concurrent"))
		Concurrent();
	else if (!strcmp(argv[1], "validation"))
		Validate();
	else if (!strcmp(argv[1], "lifetime"))
		Lifetime();
	else if (!strcmp(argv[1], "errors"))
		Errors();
	else
		return 2;
	puts("ok");
	return 0;
}
