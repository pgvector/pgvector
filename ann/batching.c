/* POSIX standalone-worker scheduler. PostgreSQL backends use their own APIs. */
#ifndef __APPLE__
#define _POSIX_C_SOURCE 200809L
#endif
#include "batching.h"

#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define ANN_DAY_US UINT64_C(86400000000)

struct AnnRequest
{
	AnnBatcher *owner;
	AnnRequest *next;
	atomic_uint references;
	pthread_mutex_t mutex;
	pthread_cond_t condition;
	float	   *query;
	uint32_t   *ids;
	AnnCandidate *results;
	uint32_t	nids;
	uint32_t	k;
	uint32_t	metric;
	uint32_t	count;
	uint32_t	bucket;
	int			filtered;
	int			state;			/* 0 queued, 1 running, 2 physically complete */
	uint64_t	bytes;
	uint64_t	work;
	uint64_t	arrived;
	uint64_t	deadline;
	AnnStatus	status;			/* Public result status; may expire during
								 * execution */
};

struct AnnBatcher
{
	const AnnBackend *backend;
	AnnIndex	index;
	AnnBatchOptions options;
	AnnBatchStats stats;
	atomic_uint references;
	pthread_mutex_t mutex;
	pthread_cond_t condition;
	pthread_t	dispatcher;
	pthread_t	watcher;
	AnnRequest *head;
	AnnRequest *tail;
	AnnRequest **active;
	uint32_t	nactive;
	int			stopping;
	int			owns_index;
};

static uint64_t
Now(void)
{
	struct timespec ts;

	(void) clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t) ts.tv_sec * UINT64_C(1000000000) + (uint64_t) ts.tv_nsec;
}

static int
InitCondition(pthread_cond_t *condition)
{
#ifdef __APPLE__
	return pthread_cond_init(condition, NULL);
#else
	pthread_condattr_t attr;
	int			result;

	if (pthread_condattr_init(&attr))
		return -1;
	result = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
	if (!result)
		result = pthread_cond_init(condition, &attr);
	(void) pthread_condattr_destroy(&attr);
	return result;
#endif
}

static void
WaitUntil(pthread_cond_t *condition, pthread_mutex_t *mutex, uint64_t deadline)
{
	struct timespec ts;
#ifdef __APPLE__
	uint64_t	now = Now();

	if (now >= deadline)
		return;
	deadline -= now;
#endif
	ts.tv_sec = (time_t) (deadline / UINT64_C(1000000000));
	ts.tv_nsec = (long) (deadline % UINT64_C(1000000000));
#ifdef __APPLE__
	(void) pthread_cond_timedwait_relative_np(condition, mutex, &ts);
#else
	(void) pthread_cond_timedwait(condition, mutex, &ts);
#endif
}

static void
ReleaseBatcher(AnnBatcher *batcher)
{
	if (atomic_fetch_sub(&batcher->references, 1) == 1)
	{
		(void) pthread_cond_destroy(&batcher->condition);
		(void) pthread_mutex_destroy(&batcher->mutex);
		free(batcher->active);
		free(batcher);
	}
}

void
AnnRequestRelease(AnnRequest *request)
{
	if (request && atomic_fetch_sub(&request->references, 1) == 1)
	{
		AnnBatcher *owner = request->owner;

		(void) pthread_cond_destroy(&request->condition);
		(void) pthread_mutex_destroy(&request->mutex);
		free(request->query);
		free(request->ids);
		free(request->results);
		free(request);
		ReleaseBatcher(owner);
	}
}

/* Called with the scheduler mutex held; lock order is scheduler then request. */
static void
SetStatus(AnnBatcher *batcher, AnnRequest *request, AnnStatus status)
{
	(void) pthread_mutex_lock(&request->mutex);
	if (request->status == ANN_PENDING)
	{
		request->status = status;
		if (status == ANN_OK)
			batcher->stats.completed++;
		else if (status == ANN_TIMED_OUT)
			batcher->stats.timed_out++;
		else if (status == ANN_CANCELLED)
			batcher->stats.cancelled++;
		else
			batcher->stats.failed++;
		(void) pthread_cond_broadcast(&request->condition);
	}
	(void) pthread_mutex_unlock(&request->mutex);
}

static void
Finish(AnnBatcher *batcher, AnnRequest *request, AnnStatus status)
{
	if (status == ANN_OK && Now() >= request->deadline)
		status = ANN_TIMED_OUT;
	SetStatus(batcher, request, status);
	request->state = 2;
	free(request->query);
	free(request->ids);
	request->query = NULL;
	request->ids = NULL;
	batcher->stats.pending_requests--;
	batcher->stats.pending_bytes -= request->bytes;
	batcher->stats.pending_work -= request->work;
	AnnRequestRelease(request); /* Release scheduler reference after execution */
}

static void
Remove(AnnBatcher *batcher, AnnRequest *request, AnnRequest *previous)
{
	if (previous)
		previous->next = request->next;
	else
		batcher->head = request->next;
	if (batcher->tail == request)
		batcher->tail = previous;
	request->next = NULL;
}

static int
Compatible(const AnnRequest *a, const AnnRequest *b)
{
	return a->metric == b->metric && a->k == b->k &&
		a->filtered == b->filtered && a->bucket == b->bucket;
}

static void
Expire(AnnBatcher *batcher)
{
	AnnRequest *request = batcher->head;
	AnnRequest *previous = NULL;
	uint64_t	now = Now();

	while (request)
	{
		AnnRequest *next = request->next;

		if (now >= request->deadline)
		{
			Remove(batcher, request, previous);
			Finish(batcher, request, ANN_TIMED_OUT);
		}
		else
			previous = request;
		request = next;
	}
	for (uint32_t i = 0; i < batcher->nactive; i++)
		if (now >= batcher->active[i]->deadline)
			SetStatus(batcher, batcher->active[i], ANN_TIMED_OUT);
}

static void *
Watch(void *arg)
{
	AnnBatcher *batcher = arg;

	(void) pthread_mutex_lock(&batcher->mutex);
	while (!batcher->stopping)
	{
		uint64_t	deadline = UINT64_MAX;

		Expire(batcher);
		for (AnnRequest *r = batcher->head; r; r = r->next)
			if (r->deadline < deadline)
				deadline = r->deadline;
		for (uint32_t i = 0; i < batcher->nactive; i++)
		{
			AnnRequest *r = batcher->active[i];

			if (r->deadline > Now() && r->deadline < deadline)
				deadline = r->deadline;
		}
		(void) pthread_cond_broadcast(&batcher->condition);
		if (deadline == UINT64_MAX)
			(void) pthread_cond_wait(&batcher->condition, &batcher->mutex);
		else
			WaitUntil(&batcher->condition, &batcher->mutex, deadline);
	}
	(void) pthread_mutex_unlock(&batcher->mutex);
	return NULL;
}

static void *
Dispatch(void *arg)
{
	AnnBatcher *batcher = arg;

	(void) pthread_mutex_lock(&batcher->mutex);
	while (!batcher->stopping)
	{
		AnnRequest *first;
		uint64_t	bytes = 0;
		uint64_t	work = 0;
		uint32_t	count = 0;
		int			limited = 0;

		Expire(batcher);
		if (!batcher->head)
		{
			(void) pthread_cond_wait(&batcher->condition, &batcher->mutex);
			continue;
		}
		first = batcher->head;
		for (AnnRequest *r = first; r; r = r->next)
		{
			if (!Compatible(first, r))
				continue;
			if (count == batcher->options.max_batch_rows ||
				bytes + r->bytes > batcher->options.max_batch_bytes ||
				work + r->work > batcher->options.max_batch_work)
			{
				limited = 1;
				continue;
			}
			batcher->active[count++] = r;
			bytes += r->bytes;
			work += r->work;
		}
		if (!limited && count < batcher->options.max_batch_rows &&
			Now() < first->arrived + batcher->options.delay_us * 1000)
		{
			WaitUntil(&batcher->condition, &batcher->mutex,
					  first->arrived + batcher->options.delay_us * 1000);
			continue;
		}
		for (uint32_t i = 0; i < count; i++)
		{
			AnnRequest *r = batcher->active[i];
			AnnRequest *previous = NULL;

			for (AnnRequest *p = batcher->head; p != r; p = p->next)
				previous = p;
			Remove(batcher, r, previous);
			r->state = 1;
		}
		batcher->nactive = count;
		batcher->stats.batches++;
		batcher->stats.batch_rows += count;
		if (count > batcher->stats.max_observed_batch)
			batcher->stats.max_observed_batch = count;
		(void) pthread_cond_broadcast(&batcher->condition);
		(void) pthread_mutex_unlock(&batcher->mutex);
		for (uint32_t i = 0; i < count; i++)
		{
			AnnRequest *r = batcher->active[i];
			AnnStatus	status;
			uint32_t	nresults = 0;

			status = batcher->backend->search(&batcher->index, r->query, r->metric,
											  r->ids, r->nids, r->k, r->results, &nresults);
			(void) pthread_mutex_lock(&batcher->mutex);
			(void) pthread_mutex_lock(&r->mutex);
			r->count = nresults;
			(void) pthread_mutex_unlock(&r->mutex);

			/*
			 * Keep the request in the active set until the entire batch
			 * finishes.
			 */
			SetStatus(batcher, r, Now() >= r->deadline ? ANN_TIMED_OUT : status);
			(void) pthread_mutex_unlock(&batcher->mutex);
		}
		(void) pthread_mutex_lock(&batcher->mutex);
		for (uint32_t i = 0; i < count; i++)
			Finish(batcher, batcher->active[i], ANN_BACKEND_ERROR);
		batcher->nactive = 0;
		(void) pthread_cond_broadcast(&batcher->condition);
	}
	(void) pthread_mutex_unlock(&batcher->mutex);
	return NULL;
}

void
AnnBatchDefaultOptions(AnnBatchOptions *options)
{
	*options = (AnnBatchOptions)
	{
		64, 256, 8 * 1024 * 1024, 16000000,
			64 * 1024 * 1024, 128000000, 100
	};
}

static AnnStatus
Create(const AnnBackend *backend, uint64_t generation, uint32_t rows,
	   uint32_t dimensions, const float *vectors,
	   const AnnIndex *borrowed, const AnnBatchOptions *options, AnnBatcher **result)
{
	AnnBatcher *batcher;
	AnnStatus	status;

	*result = NULL;
	if (!generation || !backend || !options || !backend->build || !backend->search || !backend->destroy ||
		!dimensions || dimensions > ANN_MAX_DIM || (!vectors && rows) ||
		(uint64_t) rows * dimensions > ANN_MAX_BYTES / sizeof(float) ||
		!options->max_batch_rows || options->max_batch_rows > 1024 ||
		!options->max_pending_requests || !options->max_batch_bytes ||
		!options->max_batch_work || !options->max_pending_bytes || !options->max_pending_work ||
		options->delay_us > 60000000)
		return ANN_INVALID;
	batcher = calloc(1, sizeof(*batcher));
	if (!batcher)
		return ANN_NOMEM;
	batcher->backend = backend;
	batcher->options = *options;
	atomic_init(&batcher->references, 1);
	batcher->active = calloc(options->max_batch_rows, sizeof(*batcher->active));
	if (!batcher->active)
	{
		free(batcher);
		return ANN_NOMEM;
	}
	batcher->owns_index = borrowed == NULL;
	if (borrowed)
	{
		batcher->index = *borrowed;
		status = ANN_OK;
	}
	else
		status = backend->build(&batcher->index, generation, rows, dimensions, vectors);
	if (status != ANN_OK)
		goto fail;
	if (pthread_mutex_init(&batcher->mutex, NULL))
		goto init_fail;
	if (InitCondition(&batcher->condition))
	{
		(void) pthread_mutex_destroy(&batcher->mutex);
		goto init_fail;
	}
	if (pthread_create(&batcher->dispatcher, NULL, Dispatch, batcher))
		goto thread_fail;
	if (pthread_create(&batcher->watcher, NULL, Watch, batcher))
	{
		(void) pthread_mutex_lock(&batcher->mutex);
		batcher->stopping = 1;
		(void) pthread_cond_broadcast(&batcher->condition);
		(void) pthread_mutex_unlock(&batcher->mutex);
		(void) pthread_join(batcher->dispatcher, NULL);
		goto thread_fail;
	}
	*result = batcher;
	return ANN_OK;
thread_fail:
	(void) pthread_cond_destroy(&batcher->condition);
	(void) pthread_mutex_destroy(&batcher->mutex);
init_fail:
	status = ANN_BACKEND_ERROR;
fail:
	if (batcher->owns_index)
		backend->destroy(&batcher->index);
	free(batcher->active);
	free(batcher);
	return status;
}

AnnStatus
AnnBatchCreate(const AnnBackend *backend, uint64_t generation, uint32_t rows,
			   uint32_t dimensions, const float *vectors,
			   const AnnBatchOptions *options, AnnBatcher **result)
{
	return Create(backend, generation, rows, dimensions, vectors, NULL, options, result);
}

AnnStatus
AnnBatchCreateBorrowed(const AnnBackend *backend, const AnnIndex *index,
					   const AnnBatchOptions *options, AnnBatcher **result)
{
	if (!index)
		return ANN_INVALID;
	return Create(backend, index->generation, index->rows, index->dimensions,
				  index->vectors, index, options, result);
}

AnnStatus
AnnBatchSubmit(AnnBatcher *batcher, const float *query, uint32_t k, uint32_t metric,
			   const uint32_t *ids, uint32_t nids, uint64_t timeout_us, AnnRequest **result)
{
	AnnRequest *request;
	uint32_t	count = ids ? nids : batcher->index.rows;
	uint64_t	work = (uint64_t) count * batcher->index.dimensions;
	uint64_t	bytes;
	uint32_t	bucket = 0;
	uint32_t	bits = count;
	AnnStatus	status = ANN_NOMEM;

	*result = NULL;
	if (!query || metric > 2 || !(batcher->backend->metrics & (1U << metric)) ||
		!timeout_us || timeout_us > ANN_DAY_US || (ids && nids > batcher->index.rows))
		return ANN_INVALID;
	{
		double		norm = 0;

		for (uint32_t i = 0; i < batcher->index.dimensions; i++)
		{
			if (!isfinite(query[i]))
				return ANN_INVALID;
			norm += (double) query[i] * query[i];
		}
		if (metric == 2 && norm == 0)
			return ANN_INVALID;
	}
	for (uint32_t i = 0; ids && i < nids; i++)
		if (ids[i] >= batcher->index.rows || (i && ids[i] <= ids[i - 1]))
			return ANN_INVALID;
	if (k > count)
		k = count;
	bytes = sizeof(AnnRequest) + (uint64_t) batcher->index.dimensions * sizeof(float) +
		(ids ? (uint64_t) (nids ? nids : 1) * sizeof(uint32_t) : 0) +
		(uint64_t) (k ? k : 1) * sizeof(AnnCandidate);
	if (!work)
		work = 1;
	while (bits)
	{
		bucket++;
		bits >>= 1;
	}
	(void) pthread_mutex_lock(&batcher->mutex);
	if (batcher->stopping)
	{
		status = ANN_CLOSED;
		goto done;
	}
	if (bytes > batcher->options.max_batch_bytes || work > batcher->options.max_batch_work ||
		batcher->stats.pending_requests >= batcher->options.max_pending_requests ||
		bytes > batcher->options.max_pending_bytes - batcher->stats.pending_bytes ||
		work > batcher->options.max_pending_work - batcher->stats.pending_work)
	{
		batcher->stats.rejected++;
		status = ANN_OVERLOADED;
		goto done;
	}
	request = calloc(1, sizeof(*request));
	if (!request)
		goto done;
	if (pthread_mutex_init(&request->mutex, NULL))
	{
		free(request);
		goto done;
	}
	if (InitCondition(&request->condition))
	{
		(void) pthread_mutex_destroy(&request->mutex);
		free(request);
		goto done;
	}
	atomic_init(&request->references, 2);
	atomic_fetch_add(&batcher->references, 1);
	request->owner = batcher;
	request->query = malloc((size_t) batcher->index.dimensions * sizeof(float));
	request->ids = ids ? malloc((size_t) (nids ? nids : 1) * sizeof(uint32_t)) : NULL;
	request->results = malloc((size_t) (k ? k : 1) * sizeof(AnnCandidate));
	if (!request->query || (ids && !request->ids) || !request->results)
	{
		AnnRequestRelease(request);
		AnnRequestRelease(request);
		goto done;
	}
	memcpy(request->query, query, (size_t) batcher->index.dimensions * sizeof(float));
	if (ids)
		memcpy(request->ids, ids, (size_t) nids * sizeof(uint32_t));
	request->filtered = ids != NULL;
	request->nids = nids;
	request->k = k;
	request->metric = metric;
	request->bucket = bucket / 4;
	request->bytes = bytes;
	request->work = work;
	request->status = ANN_PENDING;
	request->arrived = Now();
	request->deadline = request->arrived + timeout_us * 1000;
	if (batcher->tail)
		batcher->tail->next = request;
	else
		batcher->head = request;
	batcher->tail = request;
	batcher->stats.admitted++;
	batcher->stats.pending_requests++;
	batcher->stats.pending_bytes += bytes;
	batcher->stats.pending_work += work;
	*result = request;
	status = ANN_OK;
	(void) pthread_cond_broadcast(&batcher->condition);
done:
	(void) pthread_mutex_unlock(&batcher->mutex);
	return status;
}

AnnStatus
AnnRequestWait(AnnRequest *request, uint64_t wait_us)
{
	AnnStatus	status;
	uint64_t	deadline;

	if (wait_us > ANN_DAY_US)
		return ANN_INVALID;
	deadline = Now() + wait_us * 1000;
	(void) pthread_mutex_lock(&request->mutex);
	while (request->status == ANN_PENDING && Now() < deadline)
		WaitUntil(&request->condition, &request->mutex, deadline);
	status = request->status;
	(void) pthread_mutex_unlock(&request->mutex);
	return status;
}

AnnStatus
AnnRequestPoll(AnnRequest *request)
{
	return AnnRequestWait(request, 0);
}

const AnnCandidate *
AnnRequestResults(AnnRequest *request, uint32_t *count)
{
	const AnnCandidate *results = NULL;

	(void) pthread_mutex_lock(&request->mutex);
	*count = 0;
	if (request->status == ANN_OK)
	{
		*count = request->count;
		results = request->results;
	}
	(void) pthread_mutex_unlock(&request->mutex);
	return results;
}

int
AnnRequestCancel(AnnRequest *request)
{
	AnnBatcher *batcher = request->owner;
	int			cancelled = 0;

	(void) pthread_mutex_lock(&batcher->mutex);
	if (request->state == 0)
	{
		AnnRequest *previous = NULL;

		for (AnnRequest *r = batcher->head; r != request; r = r->next)
			previous = r;
		Remove(batcher, request, previous);
		Finish(batcher, request, ANN_CANCELLED);
		cancelled = 1;
		(void) pthread_cond_broadcast(&batcher->condition);
	}
	(void) pthread_mutex_unlock(&batcher->mutex);
	return cancelled;
}

void
AnnBatchGetStats(AnnBatcher *batcher, AnnBatchStats *stats)
{
	(void) pthread_mutex_lock(&batcher->mutex);
	*stats = batcher->stats;
	(void) pthread_mutex_unlock(&batcher->mutex);
}

void
AnnBatchClose(AnnBatcher *batcher)
{
	(void) pthread_mutex_lock(&batcher->mutex);
	batcher->stopping = 1;
	while (batcher->head)
	{
		AnnRequest *r = batcher->head;

		Remove(batcher, r, NULL);
		Finish(batcher, r, ANN_CANCELLED);
	}
	for (uint32_t i = 0; i < batcher->nactive; i++)
		SetStatus(batcher, batcher->active[i], ANN_CANCELLED);
	(void) pthread_cond_broadcast(&batcher->condition);
	(void) pthread_mutex_unlock(&batcher->mutex);
	(void) pthread_join(batcher->dispatcher, NULL);
	(void) pthread_join(batcher->watcher, NULL);
	if (batcher->owns_index)
		batcher->backend->destroy(&batcher->index);
	ReleaseBatcher(batcher);
}
