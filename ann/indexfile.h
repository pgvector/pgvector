#ifndef ANN_INDEXFILE_H
#define ANN_INDEXFILE_H

#include "backend.h"

#include <pthread.h>

/*
 * Immutable on-disk index produced by scripts/ann/build_cuvs_index.py:
 *
 *   manifest      text, one "key value..." pair per line (see ann/README.md)
 *   keys.i64      rows sorted unique little-endian int64 table keys
 *   vectors.f32   rows x dimensions little-endian FP32 vectors, key order
 *   shard-N.cagra optional cuVS CAGRA graph and dataset for one key range
 *
 * Global ordinal i is the row with the i-th smallest key. Shards cover
 * contiguous ordinal ranges, so key to shard mapping is a binary search.
 */
#define ANN_INDEX_MAX_SHARDS 64
#define ANN_INDEX_NAME_MAX 63

typedef struct AnnIndexShard
{
	uint64_t	first;			/* first global ordinal */
	uint32_t	rows;
	char		file[256];		/* CAGRA file name inside the directory */
} AnnIndexShard;

typedef struct AnnIndexFile
{
	char		name[ANN_INDEX_NAME_MAX + 1];
	char		directory[1024];
	char		source[256];	/* table the index was built from, or empty */
	char		dtype[16];		/* CAGRA dataset type: float32 or float16 */
	uint32_t	dimensions;
	uint64_t	rows;
	uint32_t	nshards;
	AnnIndexShard shards[ANN_INDEX_MAX_SHARDS];
	const int64_t *keys;		/* mapped */
	const float *vectors;		/* mapped */
	size_t		keysBytes;
	size_t		vectorsBytes;
	void	   *device;			/* backend-owned device state */
} AnnIndexFile;

/* Worker-wide tuning shared by index backends; set once before serving. */
typedef struct AnnIndexOptions
{
	uint32_t	itopk;			/* CAGRA internal top-k for unfiltered queries */
	uint32_t	filtered_itopk; /* ceiling for filtered queries */
	uint32_t	exact_limit;	/* filtered queries this selective run exact */
	int			persistent;		/* CAGRA persistent kernels */
	int			replicate;		/* full shard set on every GPU */
} AnnIndexOptions;

extern AnnIndexOptions AnnIndexConfig;

AnnStatus	AnnIndexFileOpen(const char *name, const char *directory, AnnIndexFile **result);
void		AnnIndexFileClose(AnnIndexFile *file);

/* Registry of loaded indexes; lookups take a reference released by Put. */
AnnStatus	AnnIndexRegister(AnnIndexFile *file);
AnnIndexFile *AnnIndexGet(const char *name);
void		AnnIndexPut(AnnIndexFile *file);

/* Map sorted unique keys to sorted global ordinals; returns the count found. */
uint64_t	AnnIndexOrdinals(const AnnIndexFile *file, const int64_t *keys, uint32_t nkeys,
							 uint64_t *ordinals);

/* Exact search over all rows, or over sorted global ordinals when given. */
void		AnnIndexExact(const AnnIndexFile *file, AnnIndexJob *job,
						  const uint64_t *ordinals, uint64_t nordinals);

/* Merge sorted hit lists pairwise until one top-k list remains; buffers hold k. */
uint32_t	AnnIndexTreeMerge(AnnIndexHit **lists, uint32_t *counts, uint32_t nlists,
							  uint32_t k, AnnIndexHit *output);

#endif
