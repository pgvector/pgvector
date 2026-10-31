#ifndef ANN_SERVER_H
#define ANN_SERVER_H

#include "backend.h"

#define ANN_OP_GATHERED_SEARCH 7
#define ANN_OP_STATS 8
#define ANN_OP_ATTACH_SHARED 9
#define ANN_OP_SHARED_SEARCH 10
#define ANN_OP_LOAD_INDEX 11
#define ANN_OP_INDEX_SEARCH 12

typedef struct AnnServeOptions
{
	uint32_t	lanes;			/* concurrent backend submissions */
	uint32_t	max_batch_jobs; /* gathered jobs per submission */
	uint64_t	max_batch_bytes;	/* vector bytes per submission */
	uint64_t	max_pending_bytes;	/* queued and running payload bytes */
	uint64_t	delay_us;		/* wait for more jobs before a partial batch */
} AnnServeOptions;

void		AnnServeDefaultOptions(AnnServeOptions *options);

/*
 * Accept connections on a Unix socket until SIGINT/SIGTERM. Each connection
 * may pipeline requests; replies carry the request ID and can arrive out of
 * order. Jobs from all connections share one admission budget and batcher.
 */
int			AnnServe(const char *path, const AnnBackend *backend,
					 const AnnServeOptions *options);

#endif
