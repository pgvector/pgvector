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

AnnIndexOptions AnnIndexConfig = {128, 512, 2000, 0, 0, 4000000};

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
