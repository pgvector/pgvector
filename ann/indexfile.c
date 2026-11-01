/* Huge-page advice and anonymous mappings are not strict C11. */
#ifdef __linux__
#define _GNU_SOURCE
#endif

#include "indexfile.h"

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

AnnIndexOptions AnnIndexConfig = {128, 512, 2000, 0, 0, 4000000, 4000000, 2, 4, 0, 1, 80, 500000, 1, 256};
AnnIndexBitmapHooks AnnIndexBitmapDevice = {NULL, NULL};

typedef struct Entry
{
	AnnIndexFile *file;
	int			references;
	struct Entry *next;
} Entry;

static pthread_mutex_t registryMutex = PTHREAD_MUTEX_INITIALIZER;
static Entry *registry;

#define ANN_HUGE_PAGE (2 * 1024 * 1024)

/* Bytes reserved for a file's contents: whole huge pages when copied. */
static size_t
Reserved(size_t size)
{
#ifdef MADV_HUGEPAGE
	return (size + ANN_HUGE_PAGE - 1) / ANN_HUGE_PAGE * ANN_HUGE_PAGE;
#else
	return size;
#endif
}

/*
 * Lookups touch keys and vectors at scattered rows, one page apart for
 * selective filters, so with 4 KB pages every row is a TLB miss. Where the
 * kernel supports it, read the file into anonymous memory backed by
 * transparent huge pages and drop its page cache; otherwise map it.
 */
static void *
ReadHuge(int fd, size_t size)
{
#ifdef MADV_HUGEPAGE
	size_t		reserved = Reserved(size);
	char	   *memory = mmap(NULL, reserved, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	size_t		done = 0;

	if (memory == MAP_FAILED)
		return NULL;
	(void) madvise(memory, reserved, MADV_HUGEPAGE);
	while (done < size)
	{
		ssize_t		n = pread(fd, memory + done, size - done < (1 << 26) ? size - done : (1 << 26), (off_t) done);

		if (n <= 0)
		{
			(void) munmap(memory, reserved);
			return NULL;
		}
		done += (size_t) n;
	}
	(void) mprotect(memory, reserved, PROT_READ);
#ifdef POSIX_FADV_DONTNEED
	(void) posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
#endif
	return memory;
#else
	(void) fd;
	(void) size;
	return NULL;
#endif
}

static int
MapFile(const char *directory, const char *name, size_t expected, const void **data, size_t *size)
{
	char		path[1400];
	struct stat info;
	void	   *mapping;
	int			fd;

	snprintf(path, sizeof(path), "%s/%s", directory, name);
	fd = open(path, O_RDONLY);
	if (fd < 0)
	{
		fprintf(stderr, "ann index: cannot open %s: %s\n", path, strerror(errno));
		return 0;
	}
	if (fstat(fd, &info) != 0 || (size_t) info.st_size != expected)
	{
		fprintf(stderr, "ann index: %s has %lld bytes, expected %zu\n", path,
				(long long) info.st_size, expected);
		(void) close(fd);
		return 0;
	}
	mapping = expected ? ReadHuge(fd, expected) : NULL;
	*size = mapping ? Reserved(expected) : expected;
	if (expected && mapping == NULL)
		mapping = mmap(NULL, expected, PROT_READ, MAP_SHARED, fd, 0);
	(void) close(fd);
	if (mapping == MAP_FAILED)
		return 0;
	*data = mapping;
	return 1;
}

static int
Int32Compare(const void *left, const void *right)
{
	int32_t		a = *(const int32_t *) left;
	int32_t		b = *(const int32_t *) right;

	return (a > b) - (a < b);
}

/* Index of value in the attribute's sorted values, or nvalues. */
static uint32_t
FindValue(const AnnIndexAttribute *attribute, int32_t value)
{
	uint32_t	low = 0,
				high = attribute->nvalues;

	while (low < high)
	{
		uint32_t	middle = low + (high - low) / 2;

		if (attribute->postings[middle].value < value)
			low = middle + 1;
		else
			high = middle;
	}
	return low < attribute->nvalues && attribute->postings[low].value == value ? low : attribute->nvalues;
}

/*
 * Invert one int32 column: distinct values by sorting a copy, then a
 * counting pass and a placement pass in ordinal order, so each value's rows
 * come out sorted. Values with more than bitset_min rows also get per-shard
 * bitsets.
 */
static int
LoadAttribute(AnnIndexFile *file, AnnIndexAttribute *attribute)
{
	const void *data;
	size_t		bytes;
	const int32_t *column;
	int32_t    *sorted;
	uint64_t   *fill;
	uint32_t	nvalues = 0;

	if (!MapFile(file->directory, attribute->file, file->rows * sizeof(int32_t), &data, &bytes))
		return 0;
	column = data;
	sorted = malloc(sizeof(int32_t) * file->rows);
	if (sorted == NULL)
	{
		(void) munmap((void *) data, bytes);
		return 0;
	}
	memcpy(sorted, column, sizeof(int32_t) * file->rows);
	qsort(sorted, file->rows, sizeof(int32_t), Int32Compare);
	for (uint64_t i = 0; i < file->rows; i++)
		if (i == 0 || sorted[i] != sorted[i - 1])
			sorted[nvalues++] = sorted[i];
	attribute->nvalues = nvalues;
	attribute->postings = calloc(nvalues ? nvalues : 1, sizeof(AnnIndexPosting));
	attribute->keys = malloc(sizeof(int64_t) * file->rows);
	attribute->ordinals = malloc(sizeof(uint64_t) * file->rows);
	fill = calloc(nvalues ? nvalues : 1, sizeof(uint64_t));
	if (attribute->postings == NULL || attribute->keys == NULL || attribute->ordinals == NULL || fill == NULL)
	{
		free(sorted);
		free(fill);
		(void) munmap((void *) data, bytes);
		return 0;
	}
	for (uint32_t v = 0; v < nvalues; v++)
		attribute->postings[v].value = sorted[v];
	free(sorted);
	for (uint64_t i = 0; i < file->rows; i++)
		attribute->postings[FindValue(attribute, column[i])].count++;
	for (uint32_t v = 1; v < nvalues; v++)
		attribute->postings[v].offset = attribute->postings[v - 1].offset + attribute->postings[v - 1].count;
	for (uint64_t i = 0; i < file->rows; i++)
	{
		uint32_t	v = FindValue(attribute, column[i]);
		uint64_t	at = attribute->postings[v].offset + fill[v]++;

		attribute->ordinals[at] = i;
		attribute->keys[at] = file->keys[i];
	}
	free(fill);
	(void) munmap((void *) data, bytes);

	for (uint32_t v = 0; file->nshards && v < nvalues; v++)
	{
		AnnIndexPosting *posting = &attribute->postings[v];
		uint32_t	s = 0;

		if (posting->count <= AnnIndexConfig.bitset_min)
			continue;
		posting->bitsets = calloc(file->nshards, sizeof(uint32_t *));
		if (posting->bitsets == NULL)
			return 0;
		for (uint32_t t = 0; t < file->nshards; t++)
			if ((posting->bitsets[t] = calloc(((size_t) file->shards[t].rows + 31) / 32, sizeof(uint32_t))) == NULL)
				return 0;
		for (uint64_t i = 0; i < posting->count; i++)
		{
			uint64_t	ordinal = attribute->ordinals[posting->offset + i];

			while (ordinal >= file->shards[s].first + file->shards[s].rows)
				s++;
			ordinal -= file->shards[s].first;
			posting->bitsets[s][ordinal / 32] |= UINT32_C(1) << (ordinal % 32);
		}
	}
	return 1;
}

static int
OrdinalCompare(const void *left, const void *right)
{
	uint64_t	a = *(const uint64_t *) left;
	uint64_t	b = *(const uint64_t *) right;

	return (a > b) - (a < b);
}

AnnStatus
AnnIndexMatch(const AnnIndexFile *file, const char *name, const int32_t *values, uint32_t nvalues,
			  const int64_t **keys, const uint64_t **ordinals, uint64_t *count,
			  uint32_t *const **bitsets, int *owned)
{
	const AnnIndexAttribute *attribute = NULL;
	const AnnIndexPosting *found[ANN_INDEX_MAX_VALUES];
	uint32_t	nfound = 0;
	uint64_t	total = 0;
	int64_t    *mergedKeys;
	uint64_t   *mergedOrdinals;

	*keys = NULL;
	*ordinals = NULL;
	*count = 0;
	*bitsets = NULL;
	*owned = 0;
	for (uint32_t a = 0; a < file->nattributes; a++)
		if (strcmp(file->attributes[a].name, name) == 0)
			attribute = &file->attributes[a];
	if (attribute == NULL || nvalues > ANN_INDEX_MAX_VALUES)
		return ANN_INVALID;
	for (uint32_t i = 0; i < nvalues; i++)
	{
		uint32_t	v = FindValue(attribute, values[i]);
		int			duplicate = 0;

		if (v == attribute->nvalues)
			continue;
		for (uint32_t j = 0; j < nfound; j++)
			duplicate |= found[j] == &attribute->postings[v];
		if (!duplicate)
		{
			found[nfound++] = &attribute->postings[v];
			total += attribute->postings[v].count;
		}
	}
	if (nfound == 0)
		return ANN_OK;
	if (nfound == 1)
	{
		*keys = attribute->keys + found[0]->offset;
		*ordinals = attribute->ordinals + found[0]->offset;
		*count = found[0]->count;
		*bitsets = found[0]->bitsets;
		return ANN_OK;
	}

	/* Several values: concatenate ordinals, sort, and look keys up. */
	mergedKeys = malloc(sizeof(int64_t) * total);
	mergedOrdinals = malloc(sizeof(uint64_t) * total);
	if (mergedKeys == NULL || mergedOrdinals == NULL)
	{
		free(mergedKeys);
		free(mergedOrdinals);
		return ANN_NOMEM;
	}
	total = 0;
	for (uint32_t j = 0; j < nfound; j++)
	{
		memcpy(mergedOrdinals + total, attribute->ordinals + found[j]->offset, sizeof(uint64_t) * found[j]->count);
		total += found[j]->count;
	}
	qsort(mergedOrdinals, total, sizeof(uint64_t), OrdinalCompare);
	for (uint64_t i = 0; i < total; i++)
		mergedKeys[i] = file->keys[mergedOrdinals[i]];
	*keys = mergedKeys;
	*ordinals = mergedOrdinals;
	*count = total;
	*owned = 1;
	return ANN_OK;
}

static void
FreeBitmap(AnnIndexFile *file, AnnIndexBitmap *bitmap)
{
	if (bitmap->device && AnnIndexBitmapDevice.release)
		AnnIndexBitmapDevice.release(file, bitmap);
	for (uint32_t s = 0; s < ANN_INDEX_MAX_SHARDS; s++)
		free(bitmap->owned[s]);
	free(bitmap->words);
	free(bitmap->keys);
	free(bitmap->ordinals);
	free(bitmap);
}

static size_t
BitmapBytes(const AnnIndexFile *file, const AnnIndexBitmap *bitmap)
{
	size_t		bytes = ((size_t) file->rows + 31) / 32 * 4;

	for (uint32_t s = 0; s < ANN_INDEX_MAX_SHARDS; s++)
		if (bitmap->owned[s])
			bytes += ((size_t) file->shards[s].rows + 31) / 32 * 4;
	if (bitmap->keys)
		bytes += bitmap->count * 16;
	return bytes;
}

AnnStatus
AnnIndexBitmapPut(AnnIndexFile *file, uint64_t id, const uint32_t *words, size_t nwords, uint64_t expandLimit)
{
	AnnIndexBitmap *bitmap;
	AnnIndexBitmap **link;
	AnnIndexBitmap *old = NULL;
	size_t		expected = ((size_t) file->rows + 31) / 32;

	if (nwords != expected)
		return ANN_INVALID;
	bitmap = calloc(1, sizeof(*bitmap));
	if (bitmap == NULL || (bitmap->words = malloc(nwords * 4)) == NULL)
	{
		free(bitmap);
		return ANN_NOMEM;
	}
	bitmap->id = id;
	bitmap->references = 1;
	memcpy(bitmap->words, words, nwords * 4);
	if (file->rows % 32)
		bitmap->words[nwords - 1] &= (UINT32_C(1) << (file->rows % 32)) - 1;
	for (size_t w = 0; w < nwords; w++)
		bitmap->count += (uint64_t) __builtin_popcount(bitmap->words[w]);

	/* Each shard's slice: in place when word aligned, else shifted. */
	for (uint32_t s = 0; s < file->nshards; s++)
	{
		uint64_t	first = file->shards[s].first;
		size_t		shardWords = ((size_t) file->shards[s].rows + 31) / 32;
		uint32_t	shift = (uint32_t) (first % 32);

		if (shift == 0)
		{
			bitmap->bitsets[s] = bitmap->words + first / 32;
			continue;
		}
		if ((bitmap->owned[s] = malloc(shardWords * 4)) == NULL)
		{
			FreeBitmap(file, bitmap);
			return ANN_NOMEM;
		}
		for (size_t w = 0; w < shardWords; w++)
		{
			size_t		at = first / 32 + w;
			uint32_t	low = bitmap->words[at] >> shift;
			uint32_t	high = at + 1 < nwords ? bitmap->words[at + 1] << (32 - shift) : 0;

			bitmap->owned[s][w] = low | high;
		}
		bitmap->bitsets[s] = bitmap->owned[s];
	}
	if (bitmap->count <= expandLimit)
	{
		uint64_t	i = 0;

		bitmap->keys = malloc(sizeof(int64_t) * (bitmap->count ? bitmap->count : 1));
		bitmap->ordinals = malloc(sizeof(uint64_t) * (bitmap->count ? bitmap->count : 1));
		if (bitmap->keys == NULL || bitmap->ordinals == NULL)
		{
			FreeBitmap(file, bitmap);
			return ANN_NOMEM;
		}
		for (size_t w = 0; w < nwords; w++)
			for (uint32_t bits = bitmap->words[w]; bits; bits &= bits - 1)
			{
				uint64_t	ordinal = (uint64_t) w * 32 + (uint64_t) __builtin_ctz(bits);

				bitmap->ordinals[i] = ordinal;
				bitmap->keys[i++] = file->keys[ordinal];
			}
	}

	if (AnnIndexConfig.device_bitmaps && AnnIndexBitmapDevice.upload && file->device)
		AnnIndexBitmapDevice.upload(file, bitmap);

	(void) pthread_mutex_lock(&file->bitmapMutex);
	for (link = &file->bitmaps[id % ANN_INDEX_BITMAP_BUCKETS]; *link; link = &(*link)->next)
		if ((*link)->id == id)
		{
			old = *link;
			*link = old->next;
			file->nbitmaps--;
			file->bitmapBytes -= BitmapBytes(file, old);
			break;
		}
	bitmap->next = file->bitmaps[id % ANN_INDEX_BITMAP_BUCKETS];
	file->bitmaps[id % ANN_INDEX_BITMAP_BUCKETS] = bitmap;
	file->nbitmaps++;
	file->bitmapBytes += BitmapBytes(file, bitmap);
	(void) pthread_mutex_unlock(&file->bitmapMutex);
	if (old)
		AnnIndexBitmapRelease(file, old);
	return ANN_OK;
}

AnnIndexBitmap *
AnnIndexBitmapGet(AnnIndexFile *file, uint64_t id)
{
	AnnIndexBitmap *bitmap;

	(void) pthread_mutex_lock(&file->bitmapMutex);
	for (bitmap = file->bitmaps[id % ANN_INDEX_BITMAP_BUCKETS]; bitmap; bitmap = bitmap->next)
		if (bitmap->id == id)
		{
			bitmap->references++;
			break;
		}
	(void) pthread_mutex_unlock(&file->bitmapMutex);
	return bitmap;
}

/* The cache holds one reference, dropped when the entry is replaced. */
void
AnnIndexBitmapRelease(AnnIndexFile *file, AnnIndexBitmap *bitmap)
{
	int			last;

	(void) pthread_mutex_lock(&file->bitmapMutex);
	last = --bitmap->references == 0;
	(void) pthread_mutex_unlock(&file->bitmapMutex);
	if (last)
		FreeBitmap(file, bitmap);
}

/* Parse "key value" lines; unknown keys are rejected so typos fail loudly. */
static AnnStatus
ParseManifest(AnnIndexFile *file)
{
	char		path[1400];
	char		line[1400];
	FILE	   *stream;
	int			version = 0;
	uint64_t	covered = 0;

	snprintf(path, sizeof(path), "%s/manifest", file->directory);
	stream = fopen(path, "r");
	if (stream == NULL)
	{
		fprintf(stderr, "ann index: cannot open %s: %s\n", path, strerror(errno));
		return ANN_INVALID;
	}
	strcpy(file->dtype, "float32");
	while (fgets(line, sizeof(line), stream))
	{
		char		key[64];
		unsigned long long a,
					b,
					c;
		char text[	256];

		if (line[0] == '#' || line[0] == '\n' || sscanf(line, "%63s", key) != 1)
			continue;
		if (strcmp(key, "pgvector-ann-index") == 0 && sscanf(line, "%*s %d", &version) == 1)
			continue;
		if (strcmp(key, "dimensions") == 0 && sscanf(line, "%*s %llu", &a) == 1 && a > 0 && a <= ANN_MAX_DIM)
			file->dimensions = (uint32_t) a;
		else if (strcmp(key, "rows") == 0 && sscanf(line, "%*s %llu", &a) == 1)
			file->rows = a;
		else if (strcmp(key, "metric") == 0 && sscanf(line, "%*s %255s", text) == 1 && strcmp(text, "l2") == 0)
			continue;
		else if (strcmp(key, "dtype") == 0 && sscanf(line, "%*s %15s", file->dtype) == 1 &&
				 (strcmp(file->dtype, "float32") == 0 || strcmp(file->dtype, "float16") == 0))
			continue;
		else if (strcmp(key, "source") == 0 && sscanf(line, "%*s %255s", file->source) == 1)
			continue;
		else if (strcmp(key, "key") == 0 && sscanf(line, "%*s %63s", file->key) == 1)
			continue;
		else if (strcmp(key, "vector") == 0 && sscanf(line, "%*s %63s", file->vector) == 1)
			continue;
		else if (strcmp(key, "attribute") == 0 && file->nattributes < ANN_INDEX_MAX_ATTRIBUTES &&
				 sscanf(line, "%*s %63s %255s", file->attributes[file->nattributes].name, text) == 2 &&
				 !strchr(text, '/'))
			strcpy(file->attributes[file->nattributes++].file, text);
		else if (strcmp(key, "quant") == 0 && sscanf(line, "%*s %15s", text) == 1 &&
				 (strcmp(text, "nvfp4") == 0 || strcmp(text, "fp8") == 0))
			file->quant = strcmp(text, "nvfp4") == 0 ? ANN_QUANT_NVFP4 : ANN_QUANT_FP8;
		else if (strcmp(key, "shards") == 0 && sscanf(line, "%*s %llu", &a) == 1 && a <= ANN_INDEX_MAX_SHARDS)
			continue;
		else if (strcmp(key, "shard") == 0 &&
				 sscanf(line, "%*s %llu %llu %llu %255s", &a, &b, &c, text) == 4 &&
				 a == file->nshards && file->nshards < ANN_INDEX_MAX_SHARDS &&
				 b == covered && c > 0 && c <= UINT32_MAX && !strchr(text, '/'))
		{
			AnnIndexShard *shard = &file->shards[file->nshards++];

			shard->first = b;
			shard->rows = (uint32_t) c;
			strcpy(shard->file, text);
			covered += c;
		}
		else
		{
			fprintf(stderr, "ann index: invalid manifest line: %s", line);
			(void) fclose(stream);
			return ANN_INVALID;
		}
	}
	(void) fclose(stream);
	if (version != 1 || file->dimensions == 0 || file->rows == 0 ||
		(file->nshards && covered != file->rows))
	{
		fprintf(stderr, "ann index: incomplete manifest in %s\n", file->directory);
		return ANN_INVALID;
	}
	return ANN_OK;
}

const char *
AnnIndexQuantName(int quant)
{
	return quant == ANN_QUANT_NVFP4 ? "nvfp4" : quant == ANN_QUANT_FP8 ? "fp8" : "none";
}

AnnStatus
AnnIndexFileOpen(const char *name, const char *directory, AnnIndexFile **result)
{
	AnnIndexFile *file;
	AnnStatus	status;
	const void *data;

	if (strlen(name) == 0 || strlen(name) > ANN_INDEX_NAME_MAX ||
		strlen(directory) >= sizeof(file->directory))
		return ANN_INVALID;
	file = calloc(1, sizeof(*file));
	if (file == NULL)
		return ANN_NOMEM;
	(void) pthread_mutex_init(&file->bitmapMutex, NULL);
	strcpy(file->name, name);
	strcpy(file->directory, directory);
	status = ParseManifest(file);
	if (status == ANN_OK &&
		(!MapFile(directory, "keys.i64", file->rows * sizeof(int64_t), &data, &file->keysBytes) ||
		 !(file->keys = data, 1) ||
		 !MapFile(directory, "vectors.f32", file->rows * file->dimensions * sizeof(float),
				  &data, &file->vectorsBytes) ||
		 !(file->vectors = data, 1)))
		status = ANN_INVALID;

	/* Ordinals rely on strictly increasing keys. */
	for (uint64_t i = 1; status == ANN_OK && i < file->rows; i++)
		if (file->keys[i] <= file->keys[i - 1])
		{
			fprintf(stderr, "ann index: keys.i64 is not strictly increasing\n");
			status = ANN_INVALID;
		}
	if (status == ANN_OK && file->quant != ANN_QUANT_NONE)
	{
		const void *scales = NULL;
		const void *rows = NULL;

		file->quantBlocks = (file->dimensions + 15) / 16;
		file->quantRowBytes = file->quantBlocks * (file->quant == ANN_QUANT_NVFP4 ? 8 : 16);
		if (!MapFile(directory, file->quant == ANN_QUANT_NVFP4 ? "vectors.nvfp4" : "vectors.e4m3",
					 file->rows * file->quantRowBytes, &data, &file->quantDataBytes) ||
			!(file->quantData = data, 1) ||
			(file->quant == ANN_QUANT_NVFP4 &&
			 (!MapFile(directory, "scales.e4m3", file->rows * file->quantBlocks, &scales, &file->quantScalesBytes) ||
			  !(file->quantScales = scales, 1))) ||
			!MapFile(directory, "rowscale.f32", file->rows * sizeof(float), &rows, &file->rowScalesBytes) ||
			!(file->rowScales = rows, 1))
			status = ANN_INVALID;
	}
	for (uint32_t a = 0; status == ANN_OK && a < file->nattributes; a++)
		if (!LoadAttribute(file, &file->attributes[a]))
		{
			fprintf(stderr, "ann index: cannot load attribute %s\n", file->attributes[a].name);
			status = ANN_INVALID;
		}
	if (status != ANN_OK)
	{
		AnnIndexFileClose(file);
		return status;
	}
	*result = file;
	return ANN_OK;
}

void
AnnIndexFileClose(AnnIndexFile *file)
{
	if (file == NULL)
		return;
	if (file->keys)
		(void) munmap((void *) file->keys, file->keysBytes);
	if (file->vectors)
		(void) munmap((void *) file->vectors, file->vectorsBytes);
	if (file->quantData)
		(void) munmap((void *) file->quantData, file->quantDataBytes);
	if (file->quantScales)
		(void) munmap((void *) file->quantScales, file->quantScalesBytes);
	if (file->rowScales)
		(void) munmap((void *) file->rowScales, file->rowScalesBytes);
	for (uint32_t a = 0; a < file->nattributes; a++)
	{
		AnnIndexAttribute *attribute = &file->attributes[a];

		for (uint32_t v = 0; attribute->postings && v < attribute->nvalues; v++)
			if (attribute->postings[v].bitsets)
			{
				for (uint32_t s = 0; s < file->nshards; s++)
					free(attribute->postings[v].bitsets[s]);
				free(attribute->postings[v].bitsets);
			}
		free(attribute->postings);
		free(attribute->keys);
		free(attribute->ordinals);
	}
	for (uint32_t b = 0; b < ANN_INDEX_BITMAP_BUCKETS; b++)
		while (file->bitmaps[b])
		{
			AnnIndexBitmap *bitmap = file->bitmaps[b];

			file->bitmaps[b] = bitmap->next;
			FreeBitmap(file, bitmap);
		}
	(void) pthread_mutex_destroy(&file->bitmapMutex);
	free(file);
}

/* Names are unique; replacing a loaded index requires a worker restart. */
AnnStatus
AnnIndexRegister(AnnIndexFile *file)
{
	Entry	   *entry;

	(void) pthread_mutex_lock(&registryMutex);
	for (entry = registry; entry; entry = entry->next)
		if (strcmp(entry->file->name, file->name) == 0)
		{
			(void) pthread_mutex_unlock(&registryMutex);
			return ANN_STALE;
		}
	entry = calloc(1, sizeof(*entry));
	if (entry == NULL)
	{
		(void) pthread_mutex_unlock(&registryMutex);
		return ANN_NOMEM;
	}
	entry->file = file;
	entry->references = 1;
	entry->next = registry;
	registry = entry;
	(void) pthread_mutex_unlock(&registryMutex);
	return ANN_OK;
}

AnnIndexFile *
AnnIndexGet(const char *name)
{
	AnnIndexFile *file = NULL;

	(void) pthread_mutex_lock(&registryMutex);
	for (Entry *entry = registry; entry; entry = entry->next)
		if (strcmp(entry->file->name, name) == 0)
		{
			entry->references++;
			file = entry->file;
			break;
		}
	(void) pthread_mutex_unlock(&registryMutex);
	return file;
}

void
AnnIndexPut(AnnIndexFile *file)
{
	(void) pthread_mutex_lock(&registryMutex);
	for (Entry *entry = registry; entry; entry = entry->next)
		if (entry->file == file)
		{
			entry->references--;
			break;
		}
	(void) pthread_mutex_unlock(&registryMutex);
}

uint64_t
AnnIndexOrdinals(const AnnIndexFile *file, const int64_t *keys, uint32_t nkeys, uint64_t *ordinals)
{
	const int64_t *index = file->keys;
	uint64_t	rows = file->rows;
	uint64_t	count = 0;
	uint64_t	low = 0;
	double		density = 0;

	/*
	 * Both lists are sorted. Each key's position is first guessed from the
	 * previous one by the index's average key density, then bracketed by
	 * galloping from the guess and binary searched. Near-dense keys (serial
	 * ids) land on or next to the guess, touching one or two cache lines per
	 * key; skewed keys cost a gallop, as without the guess.
	 */
	if (rows > 1 && index[rows - 1] > index[0])
		density = (double) (rows - 1) / ((double) index[rows - 1] - (double) index[0]);
	for (uint32_t i = 0; i < nkeys && low < rows; i++)
	{
		int64_t		key = keys[i];
		uint64_t	below = low;	/* index[below] < key */
		uint64_t	above;		/* index[above] >= key, or rows */
		uint64_t	guess;
		uint64_t	step = 1;
		double		ahead;

#if defined(__GNUC__) || defined(__clang__)

		/*
		 * Each key is usually one cache miss: start loading a later key's
		 * guessed position so the misses overlap.
		 */
		if (density > 0 && i + 16 < nkeys)
		{
			double		later = ((double) keys[i + 16] - (double) index[low]) * density;

			if (later >= 0 && later < (double) (rows - low))
				__builtin_prefetch(&index[low + (uint64_t) later]);
		}
#endif
		if (index[low] >= key)
		{
			if (index[low] == key)
				ordinals[count++] = low++;
			continue;
		}
		if (low + 1 >= rows)
			break;
		ahead = ((double) key - (double) index[low]) * density;
		guess = ahead < 1 ? low + 1 :
			ahead < (double) (rows - 1 - low) ? low + (uint64_t) ahead : rows - 1;
		if (index[guess] < key)
		{
			below = guess;
			above = rows;
			while (below + step < rows)
			{
				if (index[below + step] >= key)
				{
					above = below + step;
					break;
				}
				below += step;
				step *= 2;
			}
		}
		else
		{
			above = guess;
			while (above - low > step)
			{
				if (index[above - step] < key)
				{
					below = above - step;
					break;
				}
				above -= step;
				step *= 2;
			}
		}
		while (above - below > 1)
		{
			uint64_t	middle = below + (above - below) / 2;

			if (index[middle] < key)
				below = middle;
			else
				above = middle;
		}
		if (above < rows && index[above] == key)
			ordinals[count++] = above++;
		low = above;
	}
	return count;
}

static int
HitCompare(const AnnIndexHit *a, const AnnIndexHit *b)
{
	if (a->distance != b->distance)
		return a->distance < b->distance ? -1 : 1;
	return a->key < b->key ? -1 : a->key > b->key;
}

static int
HitSortCompare(const void *a, const void *b)
{
	return HitCompare(a, b);
}

static float
Distance(const float *a, const float *b, uint32_t dimensions)
{
	float		sums[8] = {0};
	float		sum = 0;
	uint32_t	d = 0;

	for (; d + 8 <= dimensions; d += 8)
		for (int j = 0; j < 8; j++)
		{
			float		diff = a[d + j] - b[d + j];

			sums[j] += diff * diff;
		}
	for (; d < dimensions; d++)
		sum += (a[d] - b[d]) * (a[d] - b[d]);
	sum += ((sums[0] + sums[1]) + (sums[2] + sums[3])) + ((sums[4] + sums[5]) + (sums[6] + sums[7]));
	return sqrtf(sum);
}

/* Bounded max-heap on distance keeps the best k hits. */
static void
Offer(AnnIndexHit *heap, uint32_t *count, uint32_t k, AnnIndexHit hit)
{
	uint32_t	i;

	if (*count < k)
	{
		i = (*count)++;
		while (i && HitCompare(&heap[(i - 1) / 2], &hit) < 0)
		{
			heap[i] = heap[(i - 1) / 2];
			i = (i - 1) / 2;
		}
		heap[i] = hit;
		return;
	}
	if (HitCompare(&hit, &heap[0]) >= 0)
		return;
	i = 0;
	for (;;)
	{
		uint32_t	child = i * 2 + 1;

		if (child >= *count)
			break;
		if (child + 1 < *count && HitCompare(&heap[child], &heap[child + 1]) < 0)
			child++;
		if (HitCompare(&heap[child], &hit) <= 0)
			break;
		heap[i] = heap[child];
		i = child;
	}
	heap[i] = hit;
}

void
AnnIndexExact(const AnnIndexFile *file, AnnIndexJob *job, const uint64_t *ordinals, uint64_t nordinals)
{
	uint64_t	count = ordinals ? nordinals : file->rows;

	job->nresults = 0;
	for (uint64_t i = 0; i < count && job->k; i++)
	{
		uint64_t	row = ordinals ? ordinals[i] : i;
		AnnIndexHit hit;

		hit.key = file->keys[row];
		hit.distance = Distance(file->vectors + row * file->dimensions, job->query, file->dimensions);
		Offer(job->results, &job->nresults, job->k, hit);
	}
	qsort(job->results, job->nresults, sizeof(*job->results), HitSortCompare);
	job->route = ANN_ROUTE_EXACT;
	job->status = ANN_OK;
}

static uint32_t
MergeTwo(const AnnIndexHit *a, uint32_t na, const AnnIndexHit *b, uint32_t nb,
		 uint32_t k, AnnIndexHit *output)
{
	uint32_t	i = 0,
				j = 0,
				n = 0;

	while (n < k && (i < na || j < nb))
	{
		if (j >= nb || (i < na && HitCompare(&a[i], &b[j]) <= 0))
			output[n++] = a[i++];
		else
			output[n++] = b[j++];
	}
	return n;
}

/*
 * Each round merges neighbouring lists, halving their number, as a GPU
 * reduction would. Lists must each be sorted and hold distinct keys; shards
 * cover disjoint key ranges, so the merged list holds distinct keys too.
 * Every list buffer must hold at least k hits; merged lists are stored back.
 */
uint32_t
AnnIndexTreeMerge(AnnIndexHit **lists, uint32_t *counts, uint32_t nlists,
				  uint32_t k, AnnIndexHit *output)
{
	AnnIndexHit *scratch;
	uint32_t	n;

	if (nlists == 0)
		return 0;
	scratch = malloc(sizeof(*scratch) * (size_t) k * nlists);
	if (scratch == NULL)
		return 0;
	for (uint32_t width = 1; width < nlists; width *= 2)
		for (uint32_t i = 0; i + width < nlists; i += width * 2)
		{
			AnnIndexHit *target = scratch + (size_t) i * k;

			counts[i] = MergeTwo(lists[i], counts[i], lists[i + width], counts[i + width], k, target);
			memcpy(lists[i], target, sizeof(*target) * counts[i]);
		}
	n = counts[0] < k ? counts[0] : k;
	memcpy(output, lists[0], sizeof(*output) * n);
	free(scratch);
	return n;
}
