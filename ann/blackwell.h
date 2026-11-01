#ifndef ANN_BLACKWELL_H
#define ANN_BLACKWELL_H

#include "indexfile.h"

#include <stdint.h>

/*
 * Blackwell (compute capability 10.x) scan module for the cuVS worker. It
 * keeps an index's quantized copy (NVFP4 or FP8, see indexfile.h) in each
 * shard's GPU memory and scores filtered rows from it with kernels compiled
 * for sm_100a, which decode E2M1 and E4M3 with the hardware conversion
 * instructions (cvt.rn.f16x2.e2m1x2, cvt.rn.f16x2.e4m3x2). Callers select
 * k + slack candidates from those scores and re-score them in FP32, so
 * returned distances stay exact; graph search stays with cuVS CAGRA.
 */
/* Row formats the module scores, besides the quantized copies. */
#define ANN_BLACKWELL_HALF 3	/* a CAGRA FP16 dataset, read in place */
#define ANN_BLACKWELL_FLOAT 4	/* a CAGRA FP32 dataset, read in place */

typedef struct AnnBlackwellShard
{
	int			quant;			/* ANN_QUANT_* or ANN_BLACKWELL_*, NONE when
								 * not available */
	int			owned;			/* data is the module's own copy */
	uint8_t    *data;			/* the shard's rows on the device */
	uint8_t    *scales;
	float	   *rowScales;
	uint32_t	rowBytes;
	uint32_t	blocks;
	size_t		bytes;			/* device memory held */
} AnnBlackwellShard;

/* Whether the module runs on this device: compute capability 10.x and NVRTC. */
int			AnnBlackwellReady(int device);

/* Copy a shard's quantized rows to the current device; 0 on failure. */
int			AnnBlackwellUpload(const AnnIndexFile *file, uint32_t shard, int device, AnnBlackwellShard *out);

/*
 * Score a shard's CAGRA dataset in place (FP16 or FP32 rows of dims
 * elements, stride elements apart) for grouped searches; 0 when the module
 * is not available on the device.
 */
int			AnnBlackwellView(const void *dataset, int64_t stride, int half, uint32_t dims, int device,
							 AnnBlackwellShard *out);
void		AnnBlackwellFree(AnnBlackwellShard *shard);

/*
 * Squared L2 distance of each listed shard row (UINT32_MAX scores +inf) to
 * an FP32 device query, written to out, on a CUDA stream of that device.
 */
int			AnnBlackwellScore(const AnnBlackwellShard *shard, int device, const uint32_t *rows, unsigned n,
							  const float *query, int dims, float *out, void *stream);

/*
 * Fused search: score the listed rows for nq queries (contiguous FP32 on
 * the device) and select each query's best ANN_BLACKWELL_TOP in one launch,
 * then copy them to pinned host memory, all on the stream. A single query
 * uses eight threads a row; several use one thread a row, so every row is
 * read once for all of them. nq is at most AnnBlackwellGroupLimit(dims),
 * never above ANN_BLACKWELL_GROUP_MAX. Slot separates concurrent searches
 * of one caller thread on a device (one per shard). After the stream
 * completes, AnnBlackwellResults gives query q's positions in the row list
 * (UINT32_MAX past the end) and their quantized distances, ascending.
 */
#define ANN_BLACKWELL_TOP 32
#define ANN_BLACKWELL_GROUP_MAX 16
int			AnnBlackwellGroupLimit(int dims);
int			AnnBlackwellSearch(const AnnBlackwellShard *shard, int device, unsigned slot, const uint32_t *rows,
							   unsigned n, const float *queries, unsigned nq, int dims, void *stream);
void		AnnBlackwellResults(int device, unsigned slot, unsigned q, const uint32_t **positions,
								const float **distances);

#endif
