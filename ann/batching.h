#ifndef ANN_BATCHING_H
#define ANN_BATCHING_H

#include "backend.h"

/* Standalone worker API. Never call PostgreSQL backend APIs on these threads. */
typedef struct AnnBatcher AnnBatcher;
typedef struct AnnRequest AnnRequest;

typedef struct AnnBatchOptions
{
	uint32_t	max_batch_rows;
	uint32_t	max_pending_requests;
	uint64_t	max_batch_bytes;
	uint64_t	max_batch_work;
	uint64_t	max_pending_bytes;
	uint64_t	max_pending_work;
	uint64_t	delay_us;
} AnnBatchOptions;

typedef struct AnnBatchStats
{
	uint64_t	admitted;
	uint64_t	rejected;
	uint64_t	completed;
	uint64_t	failed;
	uint64_t	cancelled;
	uint64_t	timed_out;
	uint64_t	batches;
	uint64_t	batch_rows;
	uint64_t	max_observed_batch;
	uint64_t	pending_requests;
	uint64_t	pending_bytes;
	uint64_t	pending_work;
} AnnBatchStats;

void		AnnBatchDefaultOptions(AnnBatchOptions *options);
AnnStatus	AnnBatchCreate(const AnnBackend *backend, uint64_t generation,
						   uint32_t rows, uint32_t dimensions, const float *vectors,
						   const AnnBatchOptions *options, AnnBatcher **result);

/* Caller keeps the immutable index alive until AnnBatchClose has returned. */
AnnStatus	AnnBatchCreateBorrowed(const AnnBackend *backend, const AnnIndex *index,
								   const AnnBatchOptions *options, AnnBatcher **result);
AnnStatus	AnnBatchSubmit(AnnBatcher *batcher, const float *query, uint32_t k,
						   uint32_t metric, const uint32_t *ids, uint32_t nids,
						   uint64_t timeout_us, AnnRequest **result);
AnnStatus	AnnRequestWait(AnnRequest *request, uint64_t wait_us);
AnnStatus	AnnRequestPoll(AnnRequest *request);
int			AnnRequestCancel(AnnRequest *request);
const AnnCandidate *AnnRequestResults(AnnRequest *request, uint32_t *count);
void		AnnRequestRelease(AnnRequest *request);
void		AnnBatchGetStats(AnnBatcher *batcher, AnnBatchStats *stats);

/* Cancels queued requests and waits for in-flight backend work to finish. */
void		AnnBatchClose(AnnBatcher *batcher);

#endif
