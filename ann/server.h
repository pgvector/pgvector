#ifndef ANN_SERVER_H
#define ANN_SERVER_H

#include "backend.h"

#define ANN_OP_GATHERED_SEARCH 7
#define ANN_OP_STATS 8
#define ANN_OP_ATTACH_SHARED 9
#define ANN_OP_SHARED_SEARCH 10
#define ANN_OP_LOAD_INDEX 11
#define ANN_OP_INDEX_SEARCH 12
#define ANN_OP_AUTH 13
#define ANN_OP_INDEX_SEARCH_PACKED 14
#define ANN_OP_INDEX_INFO 15

typedef struct AnnServeOptions
{
	uint32_t	lanes;			/* concurrent backend submissions */
	uint32_t	max_batch_jobs; /* gathered jobs per submission */
	uint64_t	max_batch_bytes;	/* vector bytes per submission */
	uint64_t	max_pending_bytes;	/* queued and running payload bytes */
	uint64_t	delay_us;		/* wait for more jobs before a partial batch */
	const char *tcp;			/* optional "host:port" network listener */
	const char *token;			/* shared secret network clients must send */
} AnnServeOptions;

void		AnnServeDefaultOptions(AnnServeOptions *options);

/*
 * Accept connections on a Unix socket, and optionally on TCP for remote
 * PostgreSQL hosts, until SIGINT/SIGTERM. Network clients must authenticate
 * with the shared token first and cannot use shared-memory operations. Each connection
 * may pipeline requests; replies carry the request ID and can arrive out of
 * order. Jobs from all connections share one admission budget and batcher.
 */
int			AnnServe(const char *path, const AnnBackend *backend,
					 const AnnServeOptions *options);

#endif
