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
#define ANN_INDEX_MAX_ATTRIBUTES 8
#define ANN_INDEX_MAX_VALUES 64

typedef struct AnnIndexShard
{
	uint64_t	first;			/* first global ordinal */
	uint32_t	rows;
	char		file[256];		/* CAGRA file name inside the directory */
} AnnIndexShard;

/*
 * Rows of one attribute value: a slice of the attribute's key and ordinal
 * arrays, both sorted, and, for values matching many rows, one eligibility
 * bitset per shard, built once at load so graph searches need no per-query
 * bitset work.
 */
typedef struct AnnIndexPosting
{
	int32_t		value;
	uint64_t	offset;
	uint64_t	count;
	uint32_t  **bitsets;		/* nshards bitsets, or NULL */
} AnnIndexPosting;

/* An int32 column stored per row in key order, inverted at load. */
typedef struct AnnIndexAttribute
{
	char		name[64];
	char		file[256];
	uint32_t	nvalues;
	AnnIndexPosting *postings;	/* sorted by value */
	int64_t    *keys;			/* rows grouped by value, key order within */
	uint64_t   *ordinals;
} AnnIndexAttribute;

/*
 * A filter bitmap pushed by PostgreSQL (ann_filter_bitmap) and cached under
 * its id: the bitmap over global ordinals, one cuVS bitset per shard
 * (pointing into it when the shard starts on a 32-row boundary), and, for
 * filters the exact routes take, the matching ordinals and keys. Searches
 * hold a reference, so a replaced entry is freed by its last user.
 */
typedef struct AnnIndexBitmap
{
	uint64_t	id;
	uint64_t	count;
	uint32_t   *words;
	uint32_t   *bitsets[ANN_INDEX_MAX_SHARDS];
	uint32_t   *owned[ANN_INDEX_MAX_SHARDS];
	int64_t    *keys;			/* NULL for filters only the graph route takes */
	uint64_t   *ordinals;
	int			references;
	struct AnnIndexBitmap *next;
} AnnIndexBitmap;

#define ANN_INDEX_BITMAP_BUCKETS 1024

typedef struct AnnIndexFile
{
	char		name[ANN_INDEX_NAME_MAX + 1];
	char		directory[1024];
	char		source[256];	/* table the index was built from, or empty */
	char		key[64];		/* key column name in source, or empty */
	char		vector[64];		/* vector column name in source, or empty */
	char		dtype[16];		/* CAGRA dataset type: float32 or float16 */
	uint32_t	dimensions;
	uint64_t	rows;
	uint32_t	nshards;
	AnnIndexShard shards[ANN_INDEX_MAX_SHARDS];
	uint32_t	nattributes;
	AnnIndexAttribute attributes[ANN_INDEX_MAX_ATTRIBUTES];
	pthread_mutex_t bitmapMutex;
	AnnIndexBitmap *bitmaps[ANN_INDEX_BITMAP_BUCKETS];
	uint64_t	nbitmaps;
	uint64_t	bitmapBytes;
	const int64_t *keys;		/* mapped */
	const float *vectors;		/* mapped */
	size_t		keysBytes;		/* mapped or reserved bytes, for munmap */
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
	uint32_t	gather_limit;	/* filtered queries this selective run a GPU
								 * gather scan */
	uint64_t	bitset_min;		/* attribute values with more rows get
								 * prebuilt bitsets */
	uint32_t	filtered_algo;	/* bitset CAGRA: 0 auto, 1 single, 2 multi CTA */
	uint32_t	filtered_width; /* bitset CAGRA search width, 0 default */
	uint32_t	filtered_iterations;	/* bitset CAGRA max iterations, 0 auto */
} AnnIndexOptions;

extern AnnIndexOptions AnnIndexConfig;

AnnStatus	AnnIndexFileOpen(const char *name, const char *directory, AnnIndexFile **result);
void		AnnIndexFileClose(AnnIndexFile *file);

/* Registry of loaded indexes; lookups take a reference released by Put. */
AnnStatus	AnnIndexRegister(AnnIndexFile *file);
AnnIndexFile *AnnIndexGet(const char *name);
void		AnnIndexPut(AnnIndexFile *file);

/*
 * Rows matching attribute IN (values): sorted keys and ordinals, and the
 * prebuilt per-shard bitsets when exactly one value has them. A single value
 * returns slices of the index (*owned = 0); several are merged into new
 * arrays the caller frees (*owned = 1).
 */
AnnStatus	AnnIndexMatch(const AnnIndexFile *file, const char *attribute, const int32_t *values,
						  uint32_t nvalues, const int64_t **keys, const uint64_t **ordinals,
						  uint64_t *count, uint32_t *const **bitsets, int *owned);

/*
 * Cache a filter bitmap of (rows + 31) / 32 words under id, replacing any
 * entry with that id. Filters of at most expandLimit rows also get their
 * ordinals and keys.
 */
AnnStatus	AnnIndexBitmapPut(AnnIndexFile *file, uint64_t id, const uint32_t *words, size_t nwords,
							  uint64_t expandLimit);

/* A referenced cached bitmap, or NULL; release it with AnnIndexBitmapRelease. */
AnnIndexBitmap *AnnIndexBitmapGet(AnnIndexFile *file, uint64_t id);
void		AnnIndexBitmapRelease(AnnIndexFile *file, AnnIndexBitmap *bitmap);

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
