#ifndef ANN_BACKEND_H
#define ANN_BACKEND_H

#include <stddef.h>
#include <stdint.h>

#define ANN_MAX_BYTES (64U * 1024U * 1024U)
#define ANN_MAX_DIM 16000U

typedef enum AnnStatus
{
	ANN_OK,
	ANN_INVALID,
	ANN_UNSUPPORTED,
	ANN_STALE,
	ANN_NOMEM,
	ANN_BACKEND_ERROR,
	ANN_PENDING,
	ANN_OVERLOADED,
	ANN_TIMED_OUT,
	ANN_CANCELLED,
	ANN_CLOSED
} AnnStatus;

typedef struct AnnIndex
{
	uint64_t	generation;
	uint32_t	rows;
	uint32_t	dimensions;
	float	   *vectors;
	void	   *backend_state;
} AnnIndex;

typedef struct AnnCandidate
{
	uint32_t	id;
	double		distance;
} AnnCandidate;

/*
 * A gathered job scores caller-supplied eligible vectors against one query.
 * Results hold every row within a small tolerance of the k-th distance, so a
 * caller recomputing distances with different rounding cannot lose a row of
 * its own top k. Results capacity must be at least rows.
 */
typedef struct AnnGatheredJob
{
	uint32_t	rows;
	uint32_t	dimensions;
	uint32_t	k;
	uint32_t	metric;
	const float *query;
	const float *vectors;
	AnnCandidate *results;
	uint32_t	nresults;
	AnnStatus	status;
} AnnGatheredJob;

/* Vendor-specific objects and precision/layout conversion stay behind this API. */
typedef struct AnnBackend
{
	const char *name;
	uint32_t	metrics;
	uint32_t	filters;
	const char *algorithm;
	const char *storage;
	int			exact;
	uint32_t	min_rows;
	uint32_t	max_k;
	AnnStatus	(*build) (AnnIndex *, uint64_t, uint32_t, uint32_t, const float *);
	AnnStatus	(*search) (const AnnIndex *, const float *, uint32_t,
						   const uint32_t *, uint32_t, uint32_t,
						   AnnCandidate *, uint32_t *);
	void		(*destroy) (AnnIndex *);
	/* One device submission for a batch of gathered jobs; sets each status. */
	void		(*search_gathered) (AnnGatheredJob *, uint32_t);
} AnnBackend;

#define ANN_GATHERED_TOLERANCE 1e-3

void		AnnGatheredExact(AnnGatheredJob *job);

const AnnBackend *AnnCpuBackend(void);
const AnnBackend *AnnMockGpuBackend(void);
#ifdef ANN_USE_CUVS
const AnnBackend *AnnCuvsBackend(void);
#endif

#endif
