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
} AnnBackend;

const AnnBackend *AnnCpuBackend(void);
#ifdef ANN_USE_CUVS
const AnnBackend *AnnCuvsBackend(void);
#endif

#endif
