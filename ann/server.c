/* getaddrinfo and TCP keepalive options are POSIX/GNU, not strict C11. */
#ifdef __linux__
#define _GNU_SOURCE
#endif

#include "server.h"
#include "indexfile.h"
#include "rdma.h"

#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

typedef struct Connection
{
	int			fd;
	int			references;
	int			broken;
	int			remote;			/* accepted on the TCP listener */
	int			authenticated;
	const unsigned char *shared;	/* client-written read-only vectors */
	size_t		shared_size;
	AnnRdma    *rdma;			/* set once the client connected a queue pair */
	uint64_t	peer;
	pthread_mutex_t mutex;		/* serializes replies and guards references */
} Connection;

#define JOB_GATHERED 0
#define JOB_INDEX 1

typedef struct Job
{
	int			kind;
	AnnGatheredJob job;
	AnnIndexJob index;
	int64_t    *keys;			/* sorted filter keys owned by the job */
	uint64_t   *ordinals;		/* their ordinals, when owned by the job */
	uint32_t   *shardBitsets[ANN_INDEX_MAX_SHARDS]; /* bitmap route, per shard */
	uint32_t   *ownedBitsets[ANN_INDEX_MAX_SHARDS]; /* those this job
													 * allocated */
	AnnIndexFile *bitmapFile;	/* cached bitmap this job references */
	AnnIndexBitmap *bitmap;
	Connection *connection;
	int			rdma;			/* reply over RDMA, as requested */
	uint16_t	operation;
	uint64_t	request;
	unsigned char *payload;
	uint64_t	charge;
	struct Job *next;
} Job;

typedef struct Service
{
	const AnnBackend *backend;
	AnnServeOptions options;
	pthread_mutex_t mutex;
	pthread_cond_t condition;
	Job		   *head;
	Job		   *tail;
	uint64_t	pending_bytes;
	uint64_t	queued;
	uint32_t	idle;
	int			stopping;
	uint64_t	jobs;
	uint64_t	rejected;
	uint64_t	failed;
	uint64_t	batches;
	uint64_t	max_batch;
	uint64_t	rows;
	AnnRdma    *rdma;
	Connection **peers;			/* by RDMA peer id, under mutex */
	uint64_t	npeers;
	uint64_t	rdma_frames;
} Service;

static volatile sig_atomic_t stopRequested;

void
AnnServeDefaultOptions(AnnServeOptions *options)
{
	long		cpus = sysconf(_SC_NPROCESSORS_ONLN);

	options->lanes = cpus > 0 ? (uint32_t) cpus : 2;
	options->max_batch_jobs = 64;
	options->max_batch_bytes = 32U * 1024U * 1024U;
	options->max_pending_bytes = 512U * 1024U * 1024U;
	options->delay_us = 0;
	options->tcp = NULL;
	options->token = NULL;
	options->rdma = NULL;
	options->rdma_lanes = 1;
	options->rdma_spin = 1;
}

static void
OnSignal(int signal)
{
	(void) signal;
	stopRequested = 1;
}

static uint64_t
Now(void)
{
	struct timespec ts;

	(void) clock_gettime(CLOCK_REALTIME, &ts);
	return (uint64_t) ts.tv_sec * UINT64_C(1000000000) + (uint64_t) ts.tv_nsec;
}

static int
LittleEndian(void)
{
	const uint16_t value = 1;

	return *(const unsigned char *) &value == 1;
}

static uint64_t
Read(const unsigned char *data, unsigned int width)
{
	uint64_t	value = 0;

	for (unsigned int i = 0; i < width; i++)
		value |= (uint64_t) data[i] << (8 * i);
	return value;
}

static void
Write(unsigned char *data, uint64_t value, unsigned int width)
{
	for (unsigned int i = 0; i < width; i++)
		data[i] = (unsigned char) (value >> (8 * i));
}

static int
ReadFull(int fd, unsigned char *data, size_t size)
{
	while (size)
	{
		ssize_t		n = read(fd, data, size);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return 0;
		data += n;
		size -= (size_t) n;
	}
	return 1;
}

static int
WriteFull(int fd, const unsigned char *data, size_t size)
{
	while (size)
	{
		ssize_t		n = write(fd, data, size);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return 0;
		data += n;
		size -= (size_t) n;
	}
	return 1;
}

static void
Release(Connection *connection)
{
	int			last;

	(void) pthread_mutex_lock(&connection->mutex);
	last = --connection->references == 0;
	(void) pthread_mutex_unlock(&connection->mutex);
	if (last)
	{
		/* No job or reader can send to the queue pair any more. */
		if (connection->rdma)
			AnnRdmaRemovePeer(connection->rdma, connection->peer);
		(void) close(connection->fd);
		if (connection->shared)
			(void) munmap((void *) connection->shared, connection->shared_size);
		(void) pthread_mutex_destroy(&connection->mutex);
		free(connection);
	}
}

/* Replies are written whole under the connection mutex, in completion order. */
static void
Reply(Connection *connection, uint16_t operation, uint64_t request,
	  AnnStatus status, const unsigned char *data, size_t size)
{
	unsigned char header[32] = {'P', 'V', 'A', 'N'};

	Write(header + 4, 1, 2);
	Write(header + 6, operation, 2);
	Write(header + 8, request, 8);
	Write(header + 16, 0, 8);
	Write(header + 24, size, 4);
	Write(header + 28, status, 4);
	(void) pthread_mutex_lock(&connection->mutex);
	if (!connection->broken &&
		(!WriteFull(connection->fd, header, sizeof(header)) ||
		 (size && !WriteFull(connection->fd, data, size))))
	{
		connection->broken = 1;
		(void) shutdown(connection->fd, SHUT_RDWR);
	}
	(void) pthread_mutex_unlock(&connection->mutex);
}

/* Reply on the transport the request used. */
static void
SendReply(Connection *connection, int rdma, uint16_t operation, uint64_t request,
		  AnnStatus status, const unsigned char *data, size_t size)
{
	unsigned char header[32] = {'P', 'V', 'A', 'N'};

	if (!rdma)
	{
		Reply(connection, operation, request, status, data, size);
		return;
	}
	Write(header + 4, 1, 2);
	Write(header + 6, operation, 2);
	Write(header + 8, request, 8);
	Write(header + 16, 0, 8);
	Write(header + 24, size, 4);
	Write(header + 28, status, 4);

	/* Replies larger than an RDMA frame fall back to the TCP connection. */
	if (!AnnRdmaSend(connection->rdma, connection->peer, header, sizeof(header), data, size))
		Reply(connection, operation, request, status, data, size);
}

static void
Finish(Service *service, Job *job)
{
	unsigned char *data = NULL;
	size_t		size = 0;

	if (job->kind == JOB_INDEX)
	{
		/* u32 count, u32 route, then (i64 key, f32 distance) per hit */
		job->job.status = job->index.status;
		if (job->index.status == ANN_OK)
		{
			size = 8 + (size_t) job->index.nresults * 12;
			data = malloc(size);
			if (data == NULL)
				job->job.status = ANN_NOMEM;
			else
			{
				Write(data, job->index.nresults, 4);
				Write(data + 4, job->index.route, 4);
				for (uint32_t i = 0; i < job->index.nresults; i++)
				{
					uint32_t	bits;

					memcpy(&bits, &job->index.results[i].distance, sizeof(bits));
					Write(data + 8 + (size_t) i * 12, (uint64_t) job->index.results[i].key, 8);
					Write(data + 16 + (size_t) i * 12, bits, 4);
				}
			}
		}
	}
	else if (job->job.status == ANN_OK)
	{
		size = 4 + (size_t) job->job.nresults * 12;
		data = malloc(size);
		if (data == NULL)
			job->job.status = ANN_NOMEM;
		else
		{
			Write(data, job->job.nresults, 4);
			for (uint32_t i = 0; i < job->job.nresults; i++)
			{
				uint64_t	bits;

				memcpy(&bits, &job->job.results[i].distance, sizeof(bits));
				Write(data + 4 + (size_t) i * 12, job->job.results[i].id, 4);
				Write(data + 8 + (size_t) i * 12, bits, 8);
			}
		}
	}
	SendReply(job->connection, job->rdma, job->operation, job->request, job->job.status,
			  data, job->job.status == ANN_OK ? size : 0);
	free(data);
	(void) pthread_mutex_lock(&service->mutex);
	service->pending_bytes -= job->charge;
	if (job->job.status != ANN_OK)
		service->failed++;
	(void) pthread_mutex_unlock(&service->mutex);
	Release(job->connection);
	free(job->job.results);
	free(job->index.results);
	free(job->keys);
	free(job->ordinals);
	for (uint32_t s = 0; s < ANN_INDEX_MAX_SHARDS; s++)
		free(job->ownedBitsets[s]);
	if (job->bitmap)
		AnnIndexBitmapRelease(job->bitmapFile, job->bitmap);
	free(job->payload);
	free(job);
}

/*
 * A lane takes the oldest job and every queued job that fits the batch
 * limits, optionally waiting delay_us for more, then makes one submission.
 */
static void *
Lane(void *argument)
{
	Service    *service = argument;
	Job		  **batch = calloc(service->options.max_batch_jobs, sizeof(*batch));
	AnnGatheredJob *jobs = calloc(service->options.max_batch_jobs, sizeof(*jobs));
	AnnIndexJob *indexJobs = calloc(service->options.max_batch_jobs, sizeof(*indexJobs));

	if (batch == NULL || jobs == NULL || indexJobs == NULL)
		abort();
	for (;;)
	{
		uint32_t	count = 0;
		uint32_t	limit;
		uint64_t	bytes = 0;
		uint64_t	deadline;

		(void) pthread_mutex_lock(&service->mutex);
		service->idle++;
		while (service->head == NULL && !service->stopping)
			(void) pthread_cond_wait(&service->condition, &service->mutex);
		service->idle--;
		if (service->head == NULL)
		{
			(void) pthread_mutex_unlock(&service->mutex);
			break;
		}
		deadline = Now() + service->options.delay_us * 1000;

		/*
		 * Leave a share of the queue for idle lanes instead of serializing
		 * it.
		 */
		limit = (uint32_t) ((service->queued + service->idle) / (service->idle + 1));
		if (limit > service->options.max_batch_jobs)
			limit = service->options.max_batch_jobs;
		for (;;)
		{
			Job		   *job = service->head;

			if (job && count < limit)
			{
				uint64_t	size = (uint64_t) job->job.rows * job->job.dimensions * 4;

				/* One submission serves one kind of job. */
				if (count && (job->kind != batch[0]->kind ||
							  bytes + size > service->options.max_batch_bytes))
					break;
				service->head = job->next;
				service->queued--;
				if (service->head == NULL)
					service->tail = NULL;
				batch[count++] = job;
				bytes += size;
				continue;
			}

			/* Optionally hold a partial batch only while no lane is idle. */
			if (job || count >= service->options.max_batch_jobs || service->idle ||
				service->stopping || Now() >= deadline)
				break;
			{
				struct timespec ts = {(time_t) (deadline / 1000000000),
				(long) (deadline % 1000000000)};

				(void) pthread_cond_timedwait(&service->condition, &service->mutex, &ts);
			}
			limit = service->options.max_batch_jobs;
		}
		service->batches++;
		service->jobs += count;
		if (count > service->max_batch)
			service->max_batch = count;
		for (uint32_t i = 0; i < count; i++)
			service->rows += batch[i]->job.rows;
		(void) pthread_mutex_unlock(&service->mutex);
		if (batch[0]->kind == JOB_INDEX)
		{
			for (uint32_t i = 0; i < count; i++)
				indexJobs[i] = batch[i]->index;
			service->backend->search_index(indexJobs, count);
			for (uint32_t i = 0; i < count; i++)
				batch[i]->index = indexJobs[i];
		}
		else
		{
			for (uint32_t i = 0; i < count; i++)
				jobs[i] = batch[i]->job;
			service->backend->search_gathered(jobs, count);
			for (uint32_t i = 0; i < count; i++)
				batch[i]->job = jobs[i];
		}
		for (uint32_t i = 0; i < count; i++)
			Finish(service, batch[i]);
	}
	free(batch);
	free(jobs);
	free(indexJobs);
	return NULL;
}

/*
 * Inline payload: u32 rows, dimensions, k, metric; FP32 query; row-major FP32
 * vectors. Shared payload: the same four fields, u64 byte offset into the
 * attached segment, then the FP32 query; vectors stay in the segment.
 */
static AnnStatus
Submit(Service *service, Connection *connection, int rdma, uint16_t operation,
	   uint64_t request, unsigned char *payload, uint32_t size)
{
	Job		   *job;
	uint32_t	rows,
				dimensions;
	uint64_t	floats;
	uint64_t	charge;
	size_t		header = operation == ANN_OP_SHARED_SEARCH ? 24 : 16;
	float	   *values = (float *) (void *) (payload + header);
	const float *vectors;

	if (size < header)
		return ANN_INVALID;
	rows = (uint32_t) Read(payload, 4);
	dimensions = (uint32_t) Read(payload + 4, 4);
	floats = operation == ANN_OP_SHARED_SEARCH ? dimensions : ((uint64_t) rows + 1) * dimensions;
	if (rows == 0 || dimensions == 0 || dimensions > ANN_MAX_DIM ||
		Read(payload + 12, 4) > 2 || floats * 4 != (uint64_t) size - header)
		return ANN_INVALID;
	if (service->backend->search_gathered == NULL)
		return ANN_UNSUPPORTED;
	if (operation == ANN_OP_SHARED_SEARCH)
	{
		uint64_t	offset = Read(payload + 16, 8);
		uint64_t	bytes = (uint64_t) rows * dimensions * 4;

		/* The client must not rewrite this range until the reply arrives. */
		if (connection->shared == NULL || offset % 4 != 0 ||
			offset > connection->shared_size || bytes > connection->shared_size - offset)
			return ANN_INVALID;
		vectors = (const float *) (const void *) (connection->shared + offset);
	}
	else
		vectors = values + dimensions;

	/* Decode in place; the payload buffer is malloc-aligned. */
	for (uint64_t i = 0; !LittleEndian() && i < floats; i++)
	{
		uint32_t	bits = (uint32_t) Read(payload + header + i * 4, 4);

		memcpy(&values[i], &bits, sizeof(bits));
	}
	if (operation == ANN_OP_SHARED_SEARCH && !LittleEndian())
		return ANN_UNSUPPORTED;
	charge = (uint64_t) size + (uint64_t) rows * sizeof(AnnCandidate);
	job = calloc(1, sizeof(*job));
	if (job == NULL || (job->job.results = malloc((size_t) rows * sizeof(AnnCandidate))) == NULL)
	{
		free(job);
		return ANN_NOMEM;
	}
	job->job.rows = rows;
	job->job.dimensions = dimensions;
	job->job.k = (uint32_t) Read(payload + 8, 4);
	job->job.metric = (uint32_t) Read(payload + 12, 4);
	job->job.query = values;
	job->job.vectors = vectors;
	job->connection = connection;
	job->operation = operation;
	job->rdma = rdma;
	job->request = request;
	job->payload = payload;
	job->charge = charge;
	(void) pthread_mutex_lock(&service->mutex);
	if (service->pending_bytes + charge > service->options.max_pending_bytes)
	{
		service->rejected++;
		(void) pthread_mutex_unlock(&service->mutex);
		free(job->job.results);
		free(job);
		return ANN_OVERLOADED;
	}
	service->pending_bytes += charge;
	(void) pthread_mutex_lock(&connection->mutex);
	connection->references++;
	(void) pthread_mutex_unlock(&connection->mutex);
	if (service->tail)
		service->tail->next = job;
	else
		service->head = job;
	service->tail = job;
	service->queued++;
	(void) pthread_cond_signal(&service->condition);
	(void) pthread_mutex_unlock(&service->mutex);
	return ANN_PENDING;
}

/* Map a client's POSIX shared memory segment read-only, once per connection. */
static AnnStatus
Attach(Connection *connection, const unsigned char *name, uint32_t size)
{
	char		path[64];
	struct stat info;
	void	   *mapping;
	int			fd;

	if (connection->shared || size < 8 || size >= sizeof(path) ||
		memcmp(name, "/pgann.", 7) != 0 || memchr(name, '\0', size))
		return ANN_INVALID;
	memcpy(path, name, size);
	path[size] = '\0';
	if (strchr(path + 1, '/'))
		return ANN_INVALID;
	fd = shm_open(path, O_RDONLY, 0);
	if (fd < 0)
		return ANN_INVALID;
	if (fstat(fd, &info) != 0 || info.st_size <= 0)
	{
		(void) close(fd);
		return ANN_INVALID;
	}
	mapping = mmap(NULL, (size_t) info.st_size, PROT_READ, MAP_SHARED, fd, 0);
	(void) close(fd);
	if (mapping == MAP_FAILED)
		return ANN_NOMEM;
	connection->shared = mapping;
	connection->shared_size = (size_t) info.st_size;
	return ANN_OK;
}

/* Admit a prepared job and queue it; false when over the pending budget. */
static int
Enqueue(Service *service, Connection *connection, Job *job)
{
	(void) pthread_mutex_lock(&service->mutex);
	if (service->pending_bytes + job->charge > service->options.max_pending_bytes)
	{
		service->rejected++;
		(void) pthread_mutex_unlock(&service->mutex);
		return 0;
	}
	service->pending_bytes += job->charge;
	(void) pthread_mutex_lock(&connection->mutex);
	connection->references++;
	(void) pthread_mutex_unlock(&connection->mutex);
	if (service->tail)
		service->tail->next = job;
	else
		service->head = job;
	service->tail = job;
	service->queued++;
	(void) pthread_cond_signal(&service->condition);
	(void) pthread_mutex_unlock(&service->mutex);
	return 1;
}

static int
KeyCompare(const void *a, const void *b)
{
	int64_t		x = *(const int64_t *) a;
	int64_t		y = *(const int64_t *) b;

	return x < y ? -1 : x > y;
}

/*
 * Strictly increasing keys: a zigzag varint for the first, then varint
 * gaps. Dense filters cost one or two bytes per key. Returns 0 unless the
 * bytes hold exactly count keys.
 */
static int
DecodeKeys(const unsigned char *data, uint32_t bytes, uint64_t count, int64_t *keys)
{
	const unsigned char *end = data + bytes;
	uint64_t	previous = 0;

	for (uint64_t i = 0; i < count; i++)
	{
		uint64_t	value = 0;
		int			shift = 0;

		do
		{
			if (data == end || shift > 63)
				return 0;
			value |= (uint64_t) (*data & 0x7f) << shift;
			shift += 7;
		} while (*data++ & 0x80);
		if (i == 0)
			previous = (value >> 1) ^ (~(value & 1) + 1);
		else if (value == 0)
			return 0;
		else
			previous += value;
		keys[i] = (int64_t) previous;
	}
	return data == end;
}

/*
 * Index search payload: u32 name length, name, u32 dimensions, k, filter
 * count (UINT32_MAX for none); FP32 query; i64 filter keys in any order.
 */
static AnnStatus
SubmitIndex(Service *service, Connection *connection, int rdma, uint16_t operation, uint64_t request,
			unsigned char *payload, uint32_t size)
{
	Job		   *job;
	uint32_t	nameLength,
				dimensions,
				k,
				nfilter,
				packedBytes = 0;
	size_t		offset;
	uint64_t	keys;
	int			packed = operation == ANN_OP_INDEX_SEARCH_PACKED;

	if (service->backend->search_index == NULL)
		return ANN_UNSUPPORTED;
	if (size < 4 || (nameLength = (uint32_t) Read(payload, 4)) == 0 ||
		nameLength > ANN_INDEX_NAME_MAX || (uint64_t) size < 16 + (uint64_t) nameLength)
		return ANN_INVALID;
	offset = 4 + nameLength;
	dimensions = (uint32_t) Read(payload + offset, 4);
	k = (uint32_t) Read(payload + offset + 4, 4);
	nfilter = (uint32_t) Read(payload + offset + 8, 4);
	offset += 12;
	if (packed)
	{
		if ((uint64_t) size < offset + 4)
			return ANN_INVALID;
		packedBytes = (uint32_t) Read(payload + offset, 4);
		offset += 4;
	}
	keys = nfilter == UINT32_MAX ? 0 : nfilter;
	if (dimensions == 0 || dimensions > ANN_MAX_DIM || k == 0 || k > 10000 ||
		memchr(payload + 4, '\0', nameLength) ||
		(uint64_t) size != offset + (uint64_t) dimensions * 4 + (packed ? packedBytes : keys * 8))
		return ANN_INVALID;
	job = calloc(1, sizeof(*job));
	if (job == NULL)
		return ANN_NOMEM;
	job->kind = JOB_INDEX;
	memcpy(job->index.name, payload + 4, nameLength);
	job->index.dimensions = dimensions;
	job->index.k = k;
	job->index.filtered = nfilter != UINT32_MAX;
	job->index.query = (const float *) (const void *) payload;	/* replaced below */
	job->index.results = malloc(sizeof(AnnIndexHit) * k);
	job->keys = malloc(sizeof(int64_t) * (keys ? keys : 1));
	if (job->index.results == NULL || job->keys == NULL)
	{
		free(job->index.results);
		free(job->keys);
		free(job);
		return ANN_NOMEM;
	}

	/* Decode the query in place at an aligned offset and copy the keys. */
	{
		float	   *query = (float *) (void *) payload;

		for (uint32_t d = 0; d < dimensions; d++)
		{
			uint32_t	bits = (uint32_t) Read(payload + offset + (size_t) d * 4, 4);

			memcpy(&query[d], &bits, sizeof(bits));
		}
		job->index.query = query;
	}
	offset += (size_t) dimensions * 4;
	if (packed)
	{
		if (!DecodeKeys(payload + offset, packedBytes, keys, job->keys))
		{
			free(job->index.results);
			free(job->keys);
			free(job);
			return ANN_INVALID;
		}
	}
	else if (LittleEndian())
		memcpy(job->keys, payload + offset, keys * 8);
	else
		for (uint64_t i = 0; i < keys; i++)
			job->keys[i] = (int64_t) Read(payload + offset + i * 8, 8);

	/* Clients normally send sorted keys; sort only when they did not. */
	for (uint64_t i = 1; i < keys; i++)
		if (job->keys[i] < job->keys[i - 1])
		{
			qsort(job->keys, keys, sizeof(int64_t), KeyCompare);
			break;
		}
	job->index.nfilter = 0;
	for (uint64_t i = 0; i < keys; i++)
		if (i == 0 || job->keys[i] != job->keys[i - 1])
			job->keys[job->index.nfilter++] = job->keys[i];
	job->index.filter = job->keys;
	job->connection = connection;
	job->operation = operation;
	job->rdma = rdma;
	job->request = request;
	job->payload = payload;
	job->charge = (uint64_t) size + keys * 8 + sizeof(AnnIndexHit) * k;
	if (!Enqueue(service, connection, job))
	{
		free(job->index.results);
		free(job->keys);
		free(job);
		return ANN_OVERLOADED;
	}
	return ANN_PENDING;
}

/*
 * Attribute search payload: u32 name length, name, u32 dimensions, k,
 * attribute name length, attribute name, value count; i32 values; FP32
 * query. The worker resolves "attribute IN (values)" from the index's own
 * inverted attribute columns, so the caller sends no keys.
 */
static AnnStatus
SubmitMatch(Service *service, Connection *connection, int rdma, uint64_t request,
			unsigned char *payload, uint32_t size)
{
	Job		   *job;
	AnnIndexFile *file;
	char		attribute[64];
	int32_t		values[ANN_INDEX_MAX_VALUES];
	uint32_t	nameLength,
				attributeLength,
				dimensions,
				k,
				nvalues;
	size_t		offset;
	const int64_t *keys;
	const uint64_t *ordinals;
	uint64_t	count;
	uint32_t   *const *bitsets;
	int			owned;
	AnnStatus	status;

	if (service->backend->search_index == NULL)
		return ANN_UNSUPPORTED;
	if (size < 4 || (nameLength = (uint32_t) Read(payload, 4)) == 0 ||
		nameLength > ANN_INDEX_NAME_MAX || (uint64_t) size < 16 + (uint64_t) nameLength ||
		memchr(payload + 4, '\0', nameLength))
		return ANN_INVALID;
	offset = 4 + nameLength;
	dimensions = (uint32_t) Read(payload + offset, 4);
	k = (uint32_t) Read(payload + offset + 4, 4);
	attributeLength = (uint32_t) Read(payload + offset + 8, 4);
	offset += 12;
	if (attributeLength == 0 || attributeLength >= sizeof(attribute) ||
		(uint64_t) size < offset + attributeLength + 4 || memchr(payload + offset, '\0', attributeLength))
		return ANN_INVALID;
	memcpy(attribute, payload + offset, attributeLength);
	attribute[attributeLength] = '\0';
	offset += attributeLength;
	nvalues = (uint32_t) Read(payload + offset, 4);
	offset += 4;
	if (dimensions == 0 || dimensions > ANN_MAX_DIM || k == 0 || k > 10000 ||
		nvalues == 0 || nvalues > ANN_INDEX_MAX_VALUES ||
		(uint64_t) size != offset + (uint64_t) nvalues * 4 + (uint64_t) dimensions * 4)
		return ANN_INVALID;
	for (uint32_t i = 0; i < nvalues; i++)
		values[i] = (int32_t) (uint32_t) Read(payload + offset + (size_t) i * 4, 4);
	offset += (size_t) nvalues * 4;

	/* Loaded indexes are never freed, so the slices outlive the job. */
	{
		char		name[ANN_INDEX_NAME_MAX + 1];

		memcpy(name, payload + 4, nameLength);
		name[nameLength] = '\0';
		if ((file = AnnIndexGet(name)) == NULL)
			return ANN_STALE;
	}
	status = AnnIndexMatch(file, attribute, values, nvalues, &keys, &ordinals, &count, &bitsets, &owned);
	AnnIndexPut(file);
	if (status != ANN_OK)
		return status;
	if (count > UINT32_MAX - 1)
	{
		if (owned)
		{
			free((void *) keys);
			free((void *) ordinals);
		}
		return ANN_INVALID;
	}
	job = calloc(1, sizeof(*job));
	if (job == NULL || (job->index.results = malloc(sizeof(AnnIndexHit) * k)) == NULL)
	{
		free(job);
		if (owned)
		{
			free((void *) keys);
			free((void *) ordinals);
		}
		return ANN_NOMEM;
	}
	job->kind = JOB_INDEX;
	memcpy(job->index.name, payload + 4, nameLength);
	job->index.dimensions = dimensions;
	job->index.k = k;
	job->index.filtered = 1;
	{
		float	   *query = (float *) (void *) payload;

		for (uint32_t d = 0; d < dimensions; d++)
		{
			uint32_t	bits = (uint32_t) Read(payload + offset + (size_t) d * 4, 4);

			memcpy(&query[d], &bits, sizeof(bits));
		}
		job->index.query = query;
	}
	job->index.filter = keys;
	job->index.nfilter = (uint32_t) count;
	job->index.ordinals = ordinals;
	job->index.bitsets = bitsets;
	if (owned)
	{
		job->keys = (int64_t *) keys;
		job->ordinals = (uint64_t *) ordinals;
	}
	job->connection = connection;
	job->operation = ANN_OP_INDEX_SEARCH_MATCH;
	job->rdma = rdma;
	job->request = request;
	job->payload = payload;
	job->charge = (uint64_t) size + sizeof(AnnIndexHit) * k + (owned ? count * 16 : 0);
	if (!Enqueue(service, connection, job))
	{
		free(job->index.results);
		free(job->keys);
		free(job->ordinals);
		free(job);
		return ANN_OVERLOADED;
	}
	return ANN_PENDING;
}

/*
 * Bitmap build payload: u32 name length, name, u32 key count, u32 packed
 * bytes, packed keys (as for packed index search). The reply is u64 eligible
 * rows, then one bit per index row in ordinal (key) order, as little-endian
 * u32 words: bit i of word w is ordinal 32 * w + i. Keys missing from the
 * index are dropped. PostgreSQL stores the reply in ann_filter_bitmap.
 */
static AnnStatus
BuildBitmap(const unsigned char *payload, uint32_t size, unsigned char **reply, size_t *replyLength)
{
	char		name[ANN_INDEX_NAME_MAX + 1];
	uint32_t	nameLength,
				nkeys,
				packedBytes;
	AnnIndexFile *file;
	int64_t    *keys;
	uint64_t   *ordinals;
	uint64_t	n;
	size_t		words;
	unsigned char *data;

	if (size < 12 || (nameLength = (uint32_t) Read(payload, 4)) == 0 ||
		nameLength > ANN_INDEX_NAME_MAX || (uint64_t) size < 12 + (uint64_t) nameLength ||
		memchr(payload + 4, '\0', nameLength))
		return ANN_INVALID;
	memcpy(name, payload + 4, nameLength);
	name[nameLength] = '\0';
	nkeys = (uint32_t) Read(payload + 4 + nameLength, 4);
	packedBytes = (uint32_t) Read(payload + 8 + nameLength, 4);
	if ((uint64_t) size != 12 + (uint64_t) nameLength + packedBytes)
		return ANN_INVALID;
	if ((file = AnnIndexGet(name)) == NULL)
		return ANN_STALE;
	words = (file->rows + 31) / 32;
	keys = malloc(sizeof(int64_t) * (nkeys ? nkeys : 1));
	ordinals = malloc(sizeof(uint64_t) * (nkeys ? nkeys : 1));
	data = calloc(8 + words * 4, 1);
	if (keys == NULL || ordinals == NULL || data == NULL ||
		!DecodeKeys(payload + 12 + nameLength, packedBytes, nkeys, keys))
	{
		AnnStatus	status = keys && ordinals && data ? ANN_INVALID : ANN_NOMEM;

		AnnIndexPut(file);
		free(keys);
		free(ordinals);
		free(data);
		return status;
	}
	n = AnnIndexOrdinals(file, keys, nkeys, ordinals);
	AnnIndexPut(file);
	Write(data, n, 8);
	for (uint64_t i = 0; i < n; i++)
		data[8 + ordinals[i] / 8] |= (unsigned char) (1U << (ordinals[i] % 8));
	free(keys);
	free(ordinals);
	*reply = data;
	*replyLength = 8 + words * 4;
	return ANN_OK;
}

/*
 * Bitmap search payload: u32 name length, name, u32 dimensions, k, bitmap
 * words; FP32 query; the bitmap as built by BuildBitmap, without its count.
 * Broad filters go straight to CAGRA with per-shard slices of the bitmap as
 * cuVS bitsets; filters within the bitset gather limit, and all filters
 * on indexes without device state (CPU backends, files without CAGRA
 * shards), are expanded to ordinals and keys and routed as usual.
 */
static AnnStatus
SubmitBitmap(Service *service, Connection *connection, int rdma, uint64_t request,
			 unsigned char *payload, uint32_t size)
{
	Job		   *job;
	AnnIndexFile *file;
	char		name[ANN_INDEX_NAME_MAX + 1];
	uint32_t	nameLength,
				dimensions,
				k,
				words;
	size_t		offset;
	uint32_t   *bitmap;
	uint64_t	n = 0;
	uint64_t	rows;

	if (service->backend->search_index == NULL)
		return ANN_UNSUPPORTED;
	if (size < 4 || (nameLength = (uint32_t) Read(payload, 4)) == 0 ||
		nameLength > ANN_INDEX_NAME_MAX || (uint64_t) size < 16 + (uint64_t) nameLength ||
		memchr(payload + 4, '\0', nameLength))
		return ANN_INVALID;
	memcpy(name, payload + 4, nameLength);
	name[nameLength] = '\0';
	offset = 4 + nameLength;
	dimensions = (uint32_t) Read(payload + offset, 4);
	k = (uint32_t) Read(payload + offset + 4, 4);
	words = (uint32_t) Read(payload + offset + 8, 4);
	offset += 12;
	if (dimensions == 0 || dimensions > ANN_MAX_DIM || k == 0 || k > 10000 ||
		(uint64_t) size != offset + (uint64_t) dimensions * 4 + (uint64_t) words * 4)
		return ANN_INVALID;
	if ((file = AnnIndexGet(name)) == NULL)
		return ANN_STALE;
	rows = file->rows;
	AnnIndexPut(file);
	if (words != (rows + 31) / 32)
		return ANN_INVALID;
	job = calloc(1, sizeof(*job));
	if (job == NULL || (job->index.results = malloc(sizeof(AnnIndexHit) * k)) == NULL)
	{
		free(job);
		return ANN_NOMEM;
	}

	/*
	 * Decode the query to the front of the payload and move the bitmap right
	 * behind it, both now aligned; clear bits past the last row.
	 */
	{
		float	   *query = (float *) (void *) payload;

		for (uint32_t d = 0; d < dimensions; d++)
		{
			uint32_t	bits = (uint32_t) Read(payload + offset + (size_t) d * 4, 4);

			memcpy(&query[d], &bits, sizeof(bits));
		}
		job->index.query = query;
	}
	offset += (size_t) dimensions * 4;
	bitmap = (uint32_t *) (void *) (payload + (size_t) dimensions * 4);
	memmove(bitmap, payload + offset, (size_t) words * 4);
	if (!LittleEndian())
		for (uint32_t w = 0; w < words; w++)
			bitmap[w] = (uint32_t) Read((unsigned char *) &bitmap[w], 4);
	if (rows % 32)
		bitmap[words - 1] &= (UINT32_C(1) << (rows % 32)) - 1;
	for (uint32_t w = 0; w < words; w++)
		n += (uint64_t) __builtin_popcount(bitmap[w]);

	job->kind = JOB_INDEX;
	strcpy(job->index.name, name);
	job->index.dimensions = dimensions;
	job->index.k = k;
	job->index.filtered = 1;
	job->index.nfilter = (uint32_t) (n < UINT32_MAX ? n : UINT32_MAX - 1);
	if (n <= AnnIndexConfig.bitset_gather_limit || file->device == NULL)
	{
		/* Expand to ordinals and keys; loaded indexes are never freed. */
		uint64_t	i = 0;

		job->keys = malloc(sizeof(int64_t) * (n ? n : 1));
		job->ordinals = malloc(sizeof(uint64_t) * (n ? n : 1));
		if (job->keys == NULL || job->ordinals == NULL)
			goto nomem;
		for (uint32_t w = 0; w < words; w++)
			for (uint32_t bits = bitmap[w]; bits; bits &= bits - 1)
			{
				uint64_t	ordinal = (uint64_t) w * 32 + (uint64_t) __builtin_ctz(bits);

				job->ordinals[i] = ordinal;
				job->keys[i++] = file->keys[ordinal];
			}
		job->index.filter = job->keys;
		job->index.ordinals = job->ordinals;
	}
	else
	{
		/* Each shard's slice: in place when word aligned, else shifted. */
		for (uint32_t s = 0; s < file->nshards; s++)
		{
			uint64_t	first = file->shards[s].first;
			size_t		shardWords = ((size_t) file->shards[s].rows + 31) / 32;
			uint32_t	shift = (uint32_t) (first % 32);

			if (shift == 0)
			{
				job->shardBitsets[s] = bitmap + first / 32;
				continue;
			}
			if ((job->ownedBitsets[s] = malloc(shardWords * 4)) == NULL)
				goto nomem;
			for (size_t w = 0; w < shardWords; w++)
			{
				size_t		at = first / 32 + w;
				uint32_t	low = bitmap[at] >> shift;
				uint32_t	high = at + 1 < words ? bitmap[at + 1] << (32 - shift) : 0;

				job->ownedBitsets[s][w] = low | high;
			}
			job->shardBitsets[s] = job->ownedBitsets[s];
		}
		job->index.bitsets = job->shardBitsets;
	}
	job->connection = connection;
	job->operation = ANN_OP_INDEX_SEARCH_BITMAP;
	job->rdma = rdma;
	job->request = request;
	job->payload = payload;
	job->charge = (uint64_t) size + sizeof(AnnIndexHit) * k + (job->keys ? n * 16 : 0);
	if (Enqueue(service, connection, job))
		return ANN_PENDING;
	job->payload = NULL;
	free(job->index.results);
	free(job->keys);
	free(job->ordinals);
	for (uint32_t s = 0; s < ANN_INDEX_MAX_SHARDS; s++)
		free(job->ownedBitsets[s]);
	free(job);
	return ANN_OVERLOADED;
nomem:
	free(job->index.results);
	free(job->keys);
	free(job->ordinals);
	for (uint32_t s = 0; s < ANN_INDEX_MAX_SHARDS; s++)
		free(job->ownedBitsets[s]);
	free(job);
	return ANN_NOMEM;
}

/*
 * Bitmap put payload: u32 name length, name, u64 filter id, u32 words, then
 * the bitmap as BuildBitmap returns it (without the count). The bitmap is
 * cached under the id until replaced or the worker exits.
 */
static AnnStatus
PutBitmap(const unsigned char *payload, uint32_t size)
{
	char		name[ANN_INDEX_NAME_MAX + 1];
	uint32_t	nameLength,
				words;
	uint64_t	id;
	AnnIndexFile *file;
	uint32_t   *bitmap;
	AnnStatus	status;

	if (size < 4 || (nameLength = (uint32_t) Read(payload, 4)) == 0 ||
		nameLength > ANN_INDEX_NAME_MAX || (uint64_t) size < 16 + (uint64_t) nameLength ||
		memchr(payload + 4, '\0', nameLength))
		return ANN_INVALID;
	memcpy(name, payload + 4, nameLength);
	name[nameLength] = '\0';
	id = Read(payload + 4 + nameLength, 8);
	words = (uint32_t) Read(payload + 12 + nameLength, 4);
	if ((uint64_t) size != 16 + (uint64_t) nameLength + (uint64_t) words * 4)
		return ANN_INVALID;
	if ((bitmap = malloc((size_t) words * 4 + 4)) == NULL)
		return ANN_NOMEM;
	for (uint32_t w = 0; w < words; w++)
		bitmap[w] = (uint32_t) Read(payload + 16 + nameLength + (size_t) w * 4, 4);
	if ((file = AnnIndexGet(name)) == NULL)
	{
		free(bitmap);
		return ANN_STALE;
	}

	/* Expand filters the exact routes take; see SubmitBitmap. */
	status = AnnIndexBitmapPut(file, id, bitmap, words,
							   file->device == NULL ? UINT64_MAX : AnnIndexConfig.bitset_gather_limit);
	AnnIndexPut(file);
	free(bitmap);
	return status;
}

/*
 * Cached bitmap search payload: u32 name length, name, u32 dimensions, k,
 * u64 filter id; FP32 query. ANN_MISSING when the id is not cached here, so
 * the caller can push the bitmap and retry.
 */
static AnnStatus
SubmitCached(Service *service, Connection *connection, int rdma, uint64_t request,
			 unsigned char *payload, uint32_t size)
{
	Job		   *job;
	AnnIndexFile *file;
	AnnIndexBitmap *bitmap;
	char		name[ANN_INDEX_NAME_MAX + 1];
	uint32_t	nameLength,
				dimensions,
				k;
	uint64_t	id;
	size_t		offset;

	if (service->backend->search_index == NULL)
		return ANN_UNSUPPORTED;
	if (size < 4 || (nameLength = (uint32_t) Read(payload, 4)) == 0 ||
		nameLength > ANN_INDEX_NAME_MAX || (uint64_t) size < 20 + (uint64_t) nameLength ||
		memchr(payload + 4, '\0', nameLength))
		return ANN_INVALID;
	memcpy(name, payload + 4, nameLength);
	name[nameLength] = '\0';
	offset = 4 + nameLength;
	dimensions = (uint32_t) Read(payload + offset, 4);
	k = (uint32_t) Read(payload + offset + 4, 4);
	id = Read(payload + offset + 8, 8);
	offset += 16;
	if (dimensions == 0 || dimensions > ANN_MAX_DIM || k == 0 || k > 10000 ||
		(uint64_t) size != offset + (uint64_t) dimensions * 4)
		return ANN_INVALID;
	if ((file = AnnIndexGet(name)) == NULL)
		return ANN_STALE;
	bitmap = AnnIndexBitmapGet(file, id);
	AnnIndexPut(file);
	if (bitmap == NULL)
		return ANN_MISSING;
	job = calloc(1, sizeof(*job));
	if (job == NULL || (job->index.results = malloc(sizeof(AnnIndexHit) * k)) == NULL)
	{
		free(job);
		AnnIndexBitmapRelease(file, bitmap);
		return ANN_NOMEM;
	}
	job->kind = JOB_INDEX;
	strcpy(job->index.name, name);
	job->index.dimensions = dimensions;
	job->index.k = k;
	job->index.filtered = 1;
	{
		float	   *query = (float *) (void *) payload;

		for (uint32_t d = 0; d < dimensions; d++)
		{
			uint32_t	bits = (uint32_t) Read(payload + offset + (size_t) d * 4, 4);

			memcpy(&query[d], &bits, sizeof(bits));
		}
		job->index.query = query;
	}

	/*
	 * Filters within the bitset gather limit carry ordinals and keys for the
	 * exact routes; broader ones only bitsets, which send them to filtered
	 * CAGRA.
	 */
	job->index.nfilter = (uint32_t) (bitmap->count < UINT32_MAX ? bitmap->count : UINT32_MAX - 1);
	if (bitmap->keys && (bitmap->count <= AnnIndexConfig.bitset_gather_limit || file->device == NULL))
	{
		job->index.filter = bitmap->keys;
		job->index.ordinals = bitmap->ordinals;
	}
	if (file->nshards)
	{
		job->index.bitsets = bitmap->bitsets;
		job->index.deviceBitsets = bitmap->device;
	}
	if (job->index.filter == NULL && job->index.bitsets == NULL)
	{
		/* Neither form usable: the worker's limits changed under the entry. */
		free(job->index.results);
		free(job);
		AnnIndexBitmapRelease(file, bitmap);
		return ANN_MISSING;
	}
	job->bitmapFile = file;
	job->bitmap = bitmap;
	job->connection = connection;
	job->operation = ANN_OP_INDEX_SEARCH_CACHED;
	job->rdma = rdma;
	job->request = request;
	job->payload = payload;
	job->charge = (uint64_t) size + sizeof(AnnIndexHit) * k;
	if (Enqueue(service, connection, job))
		return ANN_PENDING;
	free(job->index.results);
	AnnIndexBitmapRelease(file, bitmap);
	free(job);
	return ANN_OVERLOADED;
}

/*
 * Load payload: u32 name length, name, then the directory path. The reply
 * is text: "rows N dimensions D shards S dtype T source NAME".
 */
static AnnStatus
LoadIndex(Service *service, const unsigned char *payload, uint32_t size,
		  char *reply, size_t replySize, size_t *replyLength)
{
	char		name[ANN_INDEX_NAME_MAX + 1];
	char		directory[1024];
	uint32_t	nameLength;
	AnnIndexFile *file;
	AnnStatus	status;
	int			written;

	if (service->backend->load_index == NULL)
		return ANN_UNSUPPORTED;
	if (size < 4 || (nameLength = (uint32_t) Read(payload, 4)) == 0 ||
		nameLength > ANN_INDEX_NAME_MAX || size - 4 <= nameLength ||
		size - 4 - nameLength >= sizeof(directory))
		return ANN_INVALID;
	memcpy(name, payload + 4, nameLength);
	name[nameLength] = '\0';
	memcpy(directory, payload + 4 + nameLength, size - 4 - nameLength);
	directory[size - 4 - nameLength] = '\0';
	if (strlen(name) != nameLength || strlen(directory) != size - 4 - nameLength)
		return ANN_INVALID;
	status = service->backend->load_index(name, directory);
	if (status != ANN_OK)
		return status;
	file = AnnIndexGet(name);
	if (file == NULL)
		return ANN_BACKEND_ERROR;
	written = snprintf(reply, replySize, "rows %llu dimensions %u shards %u dtype %s source %s key %s vector %s",
					   (unsigned long long) file->rows, file->dimensions, file->nshards,
					   file->dtype, file->source[0] ? file->source : "-",
					   file->key[0] ? file->key : "-", file->vector[0] ? file->vector : "-");
	AnnIndexPut(file);
	*replyLength = written > 0 && (size_t) written < replySize ? (size_t) written : 0;
	return ANN_OK;
}

/* Index info payload: the index name. Replies like LoadIndex. */
static AnnStatus
DescribeIndex(const unsigned char *payload, uint32_t size, char *reply, size_t replySize, size_t *replyLength)
{
	char		name[ANN_INDEX_NAME_MAX + 1];
	AnnIndexFile *file;
	int			written;

	if (size == 0 || size > ANN_INDEX_NAME_MAX || memchr(payload, '\0', size))
		return ANN_INVALID;
	memcpy(name, payload, size);
	name[size] = '\0';
	if ((file = AnnIndexGet(name)) == NULL)
		return ANN_STALE;
	(void) pthread_mutex_lock(&file->bitmapMutex);
	written = snprintf(reply, replySize, "rows %llu dimensions %u shards %u dtype %s source %s key %s vector %s "
					   "bitmaps %llu bitmap_bytes %llu",
					   (unsigned long long) file->rows, file->dimensions, file->nshards,
					   file->dtype, file->source[0] ? file->source : "-",
					   file->key[0] ? file->key : "-", file->vector[0] ? file->vector : "-",
					   (unsigned long long) file->nbitmaps, (unsigned long long) file->bitmapBytes);
	(void) pthread_mutex_unlock(&file->bitmapMutex);
	AnnIndexPut(file);
	*replyLength = written > 0 && (size_t) written < replySize ? (size_t) written : 0;
	return ANN_OK;
}

/* Constant-time token comparison; lengths are not secret. */
static int
TokenMatches(const char *token, const unsigned char *given, uint32_t size)
{
	size_t		length = strlen(token);
	unsigned char difference = 0;

	if (size != length)
		return 0;
	for (size_t i = 0; i < length; i++)
		difference |= (unsigned char) token[i] ^ given[i];
	return difference == 0;
}

/*
 * RDMA hello over an authenticated connection: the payload is the client's
 * queue pair address. The worker connects a queue pair to it, and frames on
 * that queue pair are then answered as this connection's. Reply: u32
 * largest frame, the worker's queue pair address, device name.
 */
static AnnStatus
Hello(Service *service, Connection *connection, const unsigned char *payload, uint32_t size,
	  unsigned char *reply, size_t replySize, size_t *replyLength)
{
	const char *device;
	uint64_t	peer;

	if (service->rdma == NULL)
		return ANN_UNSUPPORTED;
	if (connection->rdma || size != ANN_RDMA_ADDRESS || (connection->remote && !connection->authenticated))
		return ANN_INVALID;
	device = AnnRdmaDevice(service->rdma);
	if (4 + ANN_RDMA_ADDRESS + strlen(device) > replySize ||
		!AnnRdmaAddPeer(service->rdma, payload, reply + 4, &peer))
		return ANN_BACKEND_ERROR;
	(void) pthread_mutex_lock(&service->mutex);
	if (peer >= service->npeers)
	{
		uint64_t	count = peer + 64;
		Connection **peers = realloc(service->peers, sizeof(*peers) * count);

		if (peers == NULL)
		{
			(void) pthread_mutex_unlock(&service->mutex);
			AnnRdmaRemovePeer(service->rdma, peer);
			return ANN_NOMEM;
		}
		memset(peers + service->npeers, 0, sizeof(*peers) * (count - service->npeers));
		service->peers = peers;
		service->npeers = count;
	}
	service->peers[peer] = connection;
	(void) pthread_mutex_unlock(&service->mutex);
	(void) pthread_mutex_lock(&connection->mutex);
	connection->references++;	/* held by the peer table */
	connection->rdma = service->rdma;
	connection->peer = peer;
	(void) pthread_mutex_unlock(&connection->mutex);
	Write(reply, AnnRdmaMaxFrame(service->rdma), 4);
	memcpy(reply + 4 + ANN_RDMA_ADDRESS, device, strlen(device));
	*replyLength = 4 + ANN_RDMA_ADDRESS + strlen(device);
	return ANN_OK;
}

/*
 * A closing connection stops accepting RDMA frames. Its queue pair lives
 * until the connection's last job has replied; Release destroys it.
 */
static void
Goodbye(Service *service, Connection *connection)
{
	if (connection->rdma == NULL)
		return;
	(void) pthread_mutex_lock(&service->mutex);
	if (connection->peer < service->npeers && service->peers[connection->peer] == connection)
		service->peers[connection->peer] = NULL;
	(void) pthread_mutex_unlock(&service->mutex);
	Release(connection);
}

/*
 * A frame from the RDMA progress thread. Only searches and probes are
 * served here; loads, hellos, statistics and shared memory use TCP.
 */
static void
RdmaFrame(void *context, uint64_t peer, unsigned char *frame, size_t size)
{
	Service    *service = context;
	Connection *connection = NULL;
	uint16_t	operation;
	uint64_t	request;
	uint32_t	length;
	unsigned char *payload;
	AnnStatus	status;

	if (size < 32 || memcmp(frame, "PVAN", 4) != 0 || Read(frame + 4, 2) != 1 ||
		(length = (uint32_t) Read(frame + 24, 4)) != size - 32)
	{
		free(frame);
		return;
	}
	(void) pthread_mutex_lock(&service->mutex);
	if (peer < service->npeers)
		connection = service->peers[peer];
	if (connection)
	{
		(void) pthread_mutex_lock(&connection->mutex);
		connection->references++;
		(void) pthread_mutex_unlock(&connection->mutex);
	}
	service->rdma_frames++;
	(void) pthread_mutex_unlock(&service->mutex);
	if (connection == NULL)
	{
		free(frame);
		return;
	}
	operation = (uint16_t) Read(frame + 6, 2);
	request = Read(frame + 8, 8);
	payload = malloc(length ? length : 1);
	if (payload)
		memcpy(payload, frame + 32, length);
	free(frame);
	if (payload == NULL)
		status = ANN_NOMEM;
	else if (operation == ANN_OP_INDEX_SEARCH || operation == ANN_OP_INDEX_SEARCH_PACKED)
		status = SubmitIndex(service, connection, 1, operation, request, payload, length);
	else if (operation == ANN_OP_INDEX_SEARCH_MATCH)
		status = SubmitMatch(service, connection, 1, request, payload, length);
	else if (operation == ANN_OP_INDEX_SEARCH_BITMAP)
		status = SubmitBitmap(service, connection, 1, request, payload, length);
	else if (operation == ANN_OP_INDEX_SEARCH_CACHED)
		status = SubmitCached(service, connection, 1, request, payload, length);
	else if (operation == ANN_OP_GATHERED_SEARCH)
		status = Submit(service, connection, 1, operation, request, payload, length);
	else if (operation == 5)
		status = ANN_OK;
	else
		status = ANN_UNSUPPORTED;
	if (status != ANN_PENDING)
	{
		SendReply(connection, 1, operation, request, status, NULL, 0);
		free(payload);
	}
	Release(connection);
}

typedef struct ReaderArgs
{
	Service    *service;
	Connection *connection;
} ReaderArgs;

static void *
Reader(void *argument)
{
	ReaderArgs *args = argument;
	Service    *service = args->service;
	Connection *connection = args->connection;

	free(args);
	for (;;)
	{
		unsigned char header[32];
		unsigned char *payload;
		uint16_t	operation;
		uint64_t	request;
		uint32_t	length;
		AnnStatus	status;

		if (!ReadFull(connection->fd, header, sizeof(header)) ||
			memcmp(header, "PVAN", 4) != 0 || Read(header + 4, 2) != 1 ||
			Read(header + 28, 4) != 0 || (length = (uint32_t) Read(header + 24, 4)) > ANN_MAX_BYTES)
			break;
		operation = (uint16_t) Read(header + 6, 2);
		request = Read(header + 8, 8);
		payload = malloc(length ? length : 1);
		if (payload == NULL || !ReadFull(connection->fd, payload, length))
		{
			free(payload);
			break;
		}
		if (connection->remote && !connection->authenticated)
		{
			/*
			 * The first network frame must carry the token; anything else
			 * closes.
			 */
			int			ok = operation == ANN_OP_AUTH && service->options.token &&
				TokenMatches(service->options.token, payload, length);

			free(payload);
			if (!ok)
				break;
			connection->authenticated = 1;
			{
				struct timeval none = {0, 0};

				/* Authenticated clients may idle between queries. */
				(void) setsockopt(connection->fd, SOL_SOCKET, SO_RCVTIMEO, &none, sizeof(none));
			}
			Reply(connection, operation, request, ANN_OK, NULL, 0);
			continue;
		}
		if (connection->remote &&
			(operation == ANN_OP_ATTACH_SHARED || operation == ANN_OP_SHARED_SEARCH))
		{
			/* Shared memory only works on the same host. */
			free(payload);
			Reply(connection, operation, request, ANN_UNSUPPORTED, NULL, 0);
			continue;
		}
		if (operation == ANN_OP_GATHERED_SEARCH || operation == ANN_OP_SHARED_SEARCH)
		{
			status = Submit(service, connection, 0, operation, request, payload, length);
			if (status == ANN_PENDING)
				continue;		/* the lane owns the payload */
			Reply(connection, operation, request, status, NULL, 0);
		}
		else if (operation == 0 || operation == ANN_OP_STATS)
		{
			char		message[512];
			int			size;

			uint64_t	rdmaFrames = 0,
						rdmaFallbacks = 0;
			int			rdmaLanes = 0;

			if (service->rdma)
				AnnRdmaStats(service->rdma, &rdmaFrames, &rdmaFallbacks, &rdmaLanes);
			(void) rdmaFrames;
			(void) pthread_mutex_lock(&service->mutex);
			if (operation == 0)
				size = snprintf(message, sizeof(message),
								"{\"backend\":\"%s\",\"mode\":\"server\",\"operations\":[0,5,7,8,9,10,11,12,13,14,15,16],"
								"\"metrics_mask\":%u,\"exact\":%s,\"lanes\":%u,\"max_batch_jobs\":%u,"
								"\"max_frame_bytes\":%u}",
								service->backend->name, service->backend->metrics,
								service->backend->exact ? "true" : "false",
								service->options.lanes, service->options.max_batch_jobs, ANN_MAX_BYTES);
			else
				size = snprintf(message, sizeof(message),
								"{\"jobs\":%llu,\"batches\":%llu,\"max_batch\":%llu,\"rows\":%llu,"
								"\"rejected\":%llu,\"failed\":%llu,\"pending_bytes\":%llu,\"device_jobs\":%llu,"
								"\"rdma_frames\":%llu,\"rdma_fallbacks\":%llu,\"rdma_lanes\":%d}",
								(unsigned long long) service->jobs, (unsigned long long) service->batches,
								(unsigned long long) service->max_batch, (unsigned long long) service->rows,
								(unsigned long long) service->rejected, (unsigned long long) service->failed,
								(unsigned long long) service->pending_bytes,
								(unsigned long long) (service->backend->device_jobs ?
													  service->backend->device_jobs() : 0),
								(unsigned long long) service->rdma_frames, (unsigned long long) rdmaFallbacks,
								rdmaLanes);
			(void) pthread_mutex_unlock(&service->mutex);
			Reply(connection, operation, request, ANN_OK, (unsigned char *) message,
				  size > 0 && (size_t) size < sizeof(message) ? (size_t) size : 0);
		}
		else if (operation == ANN_OP_INDEX_SEARCH || operation == ANN_OP_INDEX_SEARCH_PACKED)
		{
			status = SubmitIndex(service, connection, 0, operation, request, payload, length);
			if (status == ANN_PENDING)
				continue;
			Reply(connection, operation, request, status, NULL, 0);
		}
		else if (operation == ANN_OP_INDEX_SEARCH_MATCH)
		{
			status = SubmitMatch(service, connection, 0, request, payload, length);
			if (status == ANN_PENDING)
				continue;
			Reply(connection, operation, request, status, NULL, 0);
		}
		else if (operation == ANN_OP_INDEX_SEARCH_BITMAP)
		{
			status = SubmitBitmap(service, connection, 0, request, payload, length);
			if (status == ANN_PENDING)
				continue;
			Reply(connection, operation, request, status, NULL, 0);
		}
		else if (operation == ANN_OP_INDEX_SEARCH_CACHED)
		{
			status = SubmitCached(service, connection, 0, request, payload, length);
			if (status == ANN_PENDING)
				continue;
			Reply(connection, operation, request, status, NULL, 0);
		}
		else if (operation == ANN_OP_BITMAP_PUT)
			Reply(connection, operation, request, PutBitmap(payload, length), NULL, 0);
		else if (operation == ANN_OP_INDEX_BITMAP)
		{
			unsigned char *bitmap = NULL;
			size_t		bitmapLength = 0;

			status = BuildBitmap(payload, length, &bitmap, &bitmapLength);
			Reply(connection, operation, request, status, bitmap, bitmapLength);
			free(bitmap);
		}
		else if (operation == ANN_OP_LOAD_INDEX)
		{
			char		message[512];
			size_t		textLength = 0;

			/* Loading blocks only this connection's reader. */
			status = LoadIndex(service, payload, length, message, sizeof(message), &textLength);
			Reply(connection, operation, request, status, (unsigned char *) message,
				  status == ANN_OK ? textLength : 0);
		}
		else if (operation == ANN_OP_INDEX_INFO)
		{
			char		message[512];
			size_t		textLength = 0;

			status = DescribeIndex(payload, length, message, sizeof(message), &textLength);

			Reply(connection, operation, request, status, (unsigned char *) message,
				  status == ANN_OK ? textLength : 0);
		}
		else if (operation == ANN_OP_AUTH)
			Reply(connection, operation, request, ANN_OK, NULL, 0);
		else if (operation == ANN_OP_RDMA_HELLO)
		{
			unsigned char message[1400];
			size_t		messageLength = 0;

			status = Hello(service, connection, payload, length, message, sizeof(message), &messageLength);
			Reply(connection, operation, request, status, message, status == ANN_OK ? messageLength : 0);
		}
		else if (operation == ANN_OP_ATTACH_SHARED)
			Reply(connection, operation, request, Attach(connection, payload, length), NULL, 0);
		else if (operation == 5)
			Reply(connection, operation, request, ANN_OK, NULL, 0);
		else
			Reply(connection, operation, request, ANN_UNSUPPORTED, NULL, 0);
		free(payload);
	}
	(void) pthread_mutex_lock(&connection->mutex);
	connection->broken = 1;
	(void) shutdown(connection->fd, SHUT_RDWR);
	(void) pthread_mutex_unlock(&connection->mutex);
	Goodbye(service, connection);
	Release(connection);
	return NULL;
}

int
AnnServe(const char *path, const AnnBackend *backend, const AnnServeOptions *options)
{
	/*
	 * Process lifetime: detached readers and RDMA progress threads hold a
	 * pointer to it, and readers can outlive this function.
	 */
	static Service service;
	struct sockaddr_un address = {0};
	struct sigaction action = {0};
	pthread_t  *lanes;
	int			listener;
	int			network = -1;

	if (options->lanes == 0 || options->max_batch_jobs == 0 ||
		strlen(path) >= sizeof(address.sun_path))
	{
		fprintf(stderr, "invalid server options or socket path\n");
		return 2;
	}
	service.backend = backend;
	service.options = *options;
	(void) pthread_mutex_init(&service.mutex, NULL);
	(void) pthread_cond_init(&service.condition, NULL);
	action.sa_handler = OnSignal;
	(void) sigaction(SIGINT, &action, NULL);
	(void) sigaction(SIGTERM, &action, NULL);
	(void) signal(SIGPIPE, SIG_IGN);

	address.sun_family = AF_UNIX;
	strcpy(address.sun_path, path);
	(void) unlink(path);
	listener = socket(AF_UNIX, SOCK_STREAM, 0);
	if (listener < 0 || bind(listener, (struct sockaddr *) &address, sizeof(address)) != 0 ||
		listen(listener, 128) != 0)
	{
		perror("ann-worker listen");
		return 1;
	}
	if (options->tcp)
	{
		char		host[256];
		const char *colon = strrchr(options->tcp, ':');
		struct addrinfo hints = {0};
		struct addrinfo *result;
		int			on = 1;

		if (colon == NULL || colon == options->tcp || (size_t) (colon - options->tcp) >= sizeof(host) ||
			options->token == NULL || strlen(options->token) < 16)
		{
			fprintf(stderr, "--listen-tcp needs HOST:PORT and an --auth-token-file of 16+ bytes\n");
			return 2;
		}
		memcpy(host, options->tcp, (size_t) (colon - options->tcp));
		host[colon - options->tcp] = '\0';
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_flags = AI_PASSIVE;
		if (getaddrinfo(host, colon + 1, &hints, &result) != 0 ||
			(network = socket(result->ai_family, SOCK_STREAM, 0)) < 0 ||
			setsockopt(network, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0 ||
			bind(network, result->ai_addr, result->ai_addrlen) != 0 || listen(network, 1024) != 0)
		{
			perror("ann-worker tcp listen");
			return 1;
		}
		freeaddrinfo(result);
	}
	lanes = calloc(options->lanes, sizeof(*lanes));
	if (lanes == NULL)
		return 1;
	for (uint32_t i = 0; i < options->lanes; i++)
		if (pthread_create(&lanes[i], NULL, Lane, &service) != 0)
			return 1;
	if (options->rdma)
	{
		char		error[512];

		service.rdma = AnnRdmaOpen(options->rdma, (int) options->rdma_lanes, options->rdma_spin, RdmaFrame, &service,
								   error, sizeof(error));
		if (service.rdma == NULL)
		{
			fprintf(stderr, "ann-worker: %s\n", error);
			return 1;
		}
		fprintf(stderr, "ann-worker: RDMA on %s, %u lane(s), frames up to %zu bytes, %s completions\n",
				AnnRdmaDevice(service.rdma), options->rdma_lanes, AnnRdmaMaxFrame(service.rdma),
				options->rdma_spin ? "spin-polled" : "interrupt-driven");
	}
	fprintf(stderr, "ann-worker: backend %s listening on %s%s%s\n", backend->name, path,
			options->tcp ? " and " : "", options->tcp ? options->tcp : "");
	while (!stopRequested)
	{
		struct pollfd pollers[2] = {{listener, POLLIN, 0}, {network, POLLIN, 0}};
		struct timeval timeout = {10, 0};
		int			buffer = 4 * 1024 * 1024;
		Connection *connection;
		ReaderArgs *args;
		pthread_t	thread;
		int			fd;

		int			remote;

		if (poll(pollers, network >= 0 ? 2 : 1, 200) <= 0)
			continue;
		remote = !(pollers[0].revents & POLLIN);
		fd = accept(remote ? network : listener, NULL, NULL);
		if (fd < 0)
			continue;
		if (remote)
		{
			int			on = 1;
			int			idle = 10,
						interval = 5,
						count = 3;
			struct timeval authDeadline = {10, 0};

			/* Small request/reply frames: disable Nagle batching. */
			(void) setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));

			/*
			 * Keepalives drop connections from PostgreSQL hosts that crashed
			 * or vanished, releasing their reader and queued replies.
			 */
			(void) setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on));
#ifdef TCP_KEEPIDLE
			(void) setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
#elif defined(TCP_KEEPALIVE)
			(void) setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &idle, sizeof(idle));
#endif
#ifdef TCP_KEEPINTVL
			(void) setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval));
#endif
#ifdef TCP_KEEPCNT
			(void) setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(count));
#endif
			/* Unauthenticated peers get a short deadline to send the token. */
			(void) setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &authDeadline, sizeof(authDeadline));
			(void) idle;
			(void) interval;
			(void) count;
		}
		/* A stalled client cannot block a lane indefinitely. */
		(void) setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
		(void) setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buffer, sizeof(buffer));
		(void) setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buffer, sizeof(buffer));
		connection = calloc(1, sizeof(*connection));
		args = malloc(sizeof(*args));
		if (connection == NULL || args == NULL)
		{
			free(connection);
			free(args);
			(void) close(fd);
			continue;
		}
		connection->fd = fd;
		connection->remote = remote;
		connection->references = 1;
		(void) pthread_mutex_init(&connection->mutex, NULL);
		args->service = &service;
		args->connection = connection;
		if (pthread_create(&thread, NULL, Reader, args) != 0)
		{
			free(args);
			Release(connection);
			continue;
		}
		(void) pthread_detach(thread);
	}
	(void) close(listener);
	if (network >= 0)
		(void) close(network);
	(void) unlink(path);
	(void) pthread_mutex_lock(&service.mutex);
	service.stopping = 1;
	(void) pthread_cond_broadcast(&service.condition);
	(void) pthread_mutex_unlock(&service.mutex);
	for (uint32_t i = 0; i < options->lanes; i++)
		(void) pthread_join(lanes[i], NULL);
	free(lanes);

	/*
	 * Stop RDMA progress threads so no new frames are handled; the rest of
	 * the RDMA state is left for process exit to release, because detached
	 * readers may still drop connections, and with them queue pairs.
	 */
	if (service.rdma)
		AnnRdmaStop(service.rdma);
	return 0;
}
