/* getaddrinfo and TCP keepalive options are POSIX/GNU, not strict C11. */
#ifdef __linux__
#define _GNU_SOURCE
#endif

#include "server.h"
#include "indexfile.h"

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
	Connection *connection;
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
	Reply(job->connection, job->operation, job->request, job->job.status,
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
Submit(Service *service, Connection *connection, uint16_t operation,
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
 * Index search payload: u32 name length, name, u32 dimensions, k, filter
 * count (UINT32_MAX for none); FP32 query; i64 filter keys in any order.
 */
static AnnStatus
SubmitIndex(Service *service, Connection *connection, uint16_t operation, uint64_t request,
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
		/*
		 * Strictly increasing keys: a zigzag varint for the first, then
		 * varint gaps. Dense filters cost one or two bytes per key.
		 */
		const unsigned char *data = payload + offset;
		const unsigned char *end = data + packedBytes;
		uint64_t	previous = 0;

		for (uint64_t i = 0; i < keys; i++)
		{
			uint64_t	value = 0;
			int			shift = 0;

			do
			{
				if (data == end || shift > 63)
				{
					free(job->index.results);
					free(job->keys);
					free(job);
					return ANN_INVALID;
				}
				value |= (uint64_t) (*data & 0x7f) << shift;
				shift += 7;
			} while (*data++ & 0x80);
			if (i == 0)
				previous = (value >> 1) ^ (~(value & 1) + 1);
			else if (value == 0)
			{
				free(job->index.results);
				free(job->keys);
				free(job);
				return ANN_INVALID;
			}
			else
				previous += value;
			job->keys[i] = (int64_t) previous;
		}
		if (data != end)
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
IndexInfo	(const unsigned char *payload, uint32_t size, char *reply, size_t replySize, size_t *replyLength)
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
	written = snprintf(reply, replySize, "rows %llu dimensions %u shards %u dtype %s source %s key %s vector %s",
					   (unsigned long long) file->rows, file->dimensions, file->nshards,
					   file->dtype, file->source[0] ? file->source : "-",
					   file->key[0] ? file->key : "-", file->vector[0] ? file->vector : "-");
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
			status = Submit(service, connection, operation, request, payload, length);
			if (status == ANN_PENDING)
				continue;		/* the lane owns the payload */
			Reply(connection, operation, request, status, NULL, 0);
		}
		else if (operation == 0 || operation == ANN_OP_STATS)
		{
			char text[	512];
			int			size;

			(void) pthread_mutex_lock(&service->mutex);
			if (operation == 0)
				size = snprintf(text, sizeof(text),
								"{\"backend\":\"%s\",\"mode\":\"server\",\"operations\":[0,5,7,8,9,10,11,12,13,14,15],"
								"\"metrics_mask\":%u,\"exact\":%s,\"lanes\":%u,\"max_batch_jobs\":%u,"
								"\"max_frame_bytes\":%u}",
								service->backend->name, service->backend->metrics,
								service->backend->exact ? "true" : "false",
								service->options.lanes, service->options.max_batch_jobs, ANN_MAX_BYTES);
			else
				size = snprintf(text, sizeof(text),
								"{\"jobs\":%llu,\"batches\":%llu,\"max_batch\":%llu,\"rows\":%llu,"
								"\"rejected\":%llu,\"failed\":%llu,\"pending_bytes\":%llu,\"device_jobs\":%llu}",
								(unsigned long long) service->jobs, (unsigned long long) service->batches,
								(unsigned long long) service->max_batch, (unsigned long long) service->rows,
								(unsigned long long) service->rejected, (unsigned long long) service->failed,
								(unsigned long long) service->pending_bytes,
								(unsigned long long) (service->backend->device_jobs ?
													  service->backend->device_jobs() : 0));
			(void) pthread_mutex_unlock(&service->mutex);
			Reply(connection, operation, request, ANN_OK, (unsigned char *) text,
				  size > 0 && (size_t) size < sizeof(text) ? (size_t) size : 0);
		}
		else if (operation == ANN_OP_INDEX_SEARCH || operation == ANN_OP_INDEX_SEARCH_PACKED)
		{
			status = SubmitIndex(service, connection, operation, request, payload, length);
			if (status == ANN_PENDING)
				continue;
			Reply(connection, operation, request, status, NULL, 0);
		}
		else if (operation == ANN_OP_LOAD_INDEX)
		{
			char text[	512];
			size_t		textLength = 0;

			/* Loading blocks only this connection's reader. */
			status = LoadIndex(service, payload, length, text, sizeof(text), &textLength);
			Reply(connection, operation, request, status, (unsigned char *) text,
				  status == ANN_OK ? textLength : 0);
		}
		else if (operation == ANN_OP_INDEX_INFO)
		{
			char text[	512];
			size_t		textLength = 0;

			status = IndexInfo (payload, length, text, sizeof(text), &textLength);

			Reply(connection, operation, request, status, (unsigned char *) text,
				  status == ANN_OK ? textLength : 0);
		}
		else if (operation == ANN_OP_AUTH)
			Reply(connection, operation, request, ANN_OK, NULL, 0);
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
	Release(connection);
	return NULL;
}

int
AnnServe(const char *path, const AnnBackend *backend, const AnnServeOptions *options)
{
	Service		service = {0};
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
	return 0;
}
