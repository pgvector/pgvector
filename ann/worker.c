#include "backend.h"
#include "batching.h"

#include <float.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

_Static_assert(sizeof(float) == 4 && FLT_RADIX == 2 && FLT_MANT_DIG == 24,
			   "protocol requires IEEE binary32");
_Static_assert(sizeof(double) == 8 && DBL_MANT_DIG == 53,
			   "protocol requires IEEE binary64");

typedef struct Cursor
{
	unsigned char *data;
	size_t		size;
	size_t		offset;
	int			valid;
} Cursor;

static uint64_t
ReadInteger(Cursor *cursor, unsigned int width)
{
	uint64_t	value = 0;

	if (!cursor->valid || width > cursor->size - cursor->offset)
	{
		cursor->valid = 0;
		return 0;
	}
	for (unsigned int i = 0; i < width; i++)
		value |= (uint64_t) cursor->data[cursor->offset++] << (8 * i);
	return value;
}

static void
WriteInteger(Cursor *cursor, uint64_t value, unsigned int width)
{
	if (!cursor->valid || width > cursor->size - cursor->offset)
	{
		cursor->valid = 0;
		return;
	}
	for (unsigned int i = 0; i < width; i++)
		cursor->data[cursor->offset++] = (unsigned char) (value >> (8 * i));
}

static float
ReadFloat(Cursor *cursor)
{
	uint32_t	bits = (uint32_t) ReadInteger(cursor, 4);
	float		value;

	memcpy(&value, &bits, sizeof(value));
	return value;
}

static void
WriteDouble(Cursor *cursor, double value)
{
	uint64_t	bits;

	memcpy(&bits, &value, sizeof(bits));
	WriteInteger(cursor, bits, 8);
}

static uint32_t *
ReadIds(Cursor *cursor, uint32_t rows, uint32_t *count)
{
	uint32_t   *ids;

	*count = (uint32_t) ReadInteger(cursor, 4);
	if (!cursor->valid || *count > rows || *count > (cursor->size - cursor->offset) / 4)
	{
		cursor->valid = 0;
		return NULL;
	}
	/* Non-NULL distinguishes an empty allowlist from no filter. */
	ids = malloc(*count ? (size_t) *count * sizeof(*ids) : sizeof(*ids));
	if (ids == NULL)
	{
		cursor->valid = 0;
		return NULL;
	}
	for (uint32_t i = 0; i < *count; i++)
		ids[i] = (uint32_t) ReadInteger(cursor, 4);
	return ids;
}

static AnnStatus
Build(const AnnBackend *backend, AnnIndex *index, uint64_t generation, Cursor *input)
{
	uint32_t	rows = (uint32_t) ReadInteger(input, 4);
	uint32_t	dimensions = (uint32_t) ReadInteger(input, 4);
	uint64_t	count = (uint64_t) rows * dimensions;
	float	   *vectors;
	AnnStatus	status;

	if (!input->valid || dimensions == 0 || dimensions > ANN_MAX_DIM ||
		count > ANN_MAX_BYTES / 4 || count * 4 != input->size - input->offset)
		return ANN_INVALID;
	vectors = malloc(count ? (size_t) count * sizeof(*vectors) : 1);
	if (vectors == NULL)
		return ANN_NOMEM;
	for (size_t i = 0; i < (size_t) count; i++)
		vectors[i] = ReadFloat(input);
	status = backend->build(index, generation, rows, dimensions, vectors);
	free(vectors);
	return status;
}

static AnnStatus
Search(const AnnBackend *backend, const AnnIndex *index, AnnBatcher *batcher,
	   Cursor *input, Cursor *output)
{
	uint32_t	queries = (uint32_t) ReadInteger(input, 4);
	uint32_t	dimensions = (uint32_t) ReadInteger(input, 4);
	uint32_t	k = (uint32_t) ReadInteger(input, 4);
	uint32_t	metric = (uint32_t) ReadInteger(input, 4);
	uint32_t	filter = (uint32_t) ReadInteger(input, 4);
	uint32_t   *shared = NULL;
	uint32_t	sharedCount = 0;
	float	   *vectors = NULL;
	AnnCandidate *results = NULL;
	AnnRequest **requests = NULL;
	AnnStatus	status = ANN_INVALID;
	uint64_t	count = (uint64_t) queries * dimensions;
	uint64_t	capacity;

	if (!input->valid || queries == 0 || queries > 1024 ||
		dimensions != index->dimensions || filter > 2 ||
		count > (input->size - input->offset) / 4)
		return ANN_INVALID;
	if (metric > 2 || !(backend->metrics & (1U << metric)) ||
		!(backend->filters & (1U << filter)))
		return ANN_UNSUPPORTED;
	if (k > index->rows)
		k = index->rows;
	capacity = (uint64_t) queries * (4 + (uint64_t) k * 12);
	if (capacity > ANN_MAX_BYTES)
		return ANN_INVALID;
	vectors = malloc((size_t) count * sizeof(*vectors));
	results = malloc(k ? (size_t) k * sizeof(*results) : sizeof(*results));
	output->data = malloc((size_t) capacity);
	output->size = (size_t) capacity;
	requests = calloc(queries, sizeof(*requests));
	if (vectors == NULL || results == NULL || output->data == NULL || requests == NULL)
	{
		status = ANN_NOMEM;
		goto done;
	}
	for (size_t i = 0; i < (size_t) count; i++)
		vectors[i] = ReadFloat(input);
	if (filter == 1)
		shared = ReadIds(input, index->rows, &sharedCount);
	for (uint32_t i = 0; i < queries; i++)
	{
		uint32_t	nids = sharedCount;
		uint32_t   *ids = shared;
		uint32_t	nresults;

		if (filter == 2)
			ids = ReadIds(input, index->rows, &nids);
		if (!input->valid)
		{
			if (filter == 2)
				free(ids);
			status = ANN_INVALID;
			goto done;
		}
		if (batcher)
			status = AnnBatchSubmit(batcher, vectors + (size_t) i * dimensions,
									k, metric, ids, nids, 30000000, &requests[i]);
		else
			status = backend->search(index, vectors + (size_t) i * dimensions,
									 metric, ids, nids, k, results, &nresults);
		if (filter == 2)
			free(ids);
		if (status != ANN_OK)
			goto done;
		if (batcher)
			continue;
		WriteInteger(output, nresults, 4);
		for (uint32_t j = 0; j < nresults; j++)
		{
			WriteInteger(output, results[j].id, 4);
			WriteDouble(output, results[j].distance);
		}
	}
	if (!input->valid || input->offset != input->size || !output->valid)
	{
		status = ANN_INVALID;
		goto done;
	}
	for (uint32_t i = 0; batcher && i < queries; i++)
	{
		uint32_t	nresults;
		const AnnCandidate *found;

		status = AnnRequestWait(requests[i], 30000000);
		if (status == ANN_PENDING)
			status = ANN_TIMED_OUT;
		if (status != ANN_OK)
			goto done;
		found = AnnRequestResults(requests[i], &nresults);
		WriteInteger(output, nresults, 4);
		for (uint32_t j = 0; j < nresults; j++)
		{
			WriteInteger(output, found[j].id, 4);
			WriteDouble(output, found[j].distance);
		}
	}
done:
	for (uint32_t i = 0; requests && i < queries; i++)
		if (requests[i])
		{
			(void) AnnRequestCancel(requests[i]);
			AnnRequestRelease(requests[i]);
		}
	free(requests);
	free(shared);
	free(vectors);
	free(results);
	return status;
}

static int
Reply(uint16_t operation, uint64_t request, uint64_t generation,
	  AnnStatus status, const void *data, size_t size)
{
	unsigned char bytes[32] = {'P', 'V', 'A', 'N'};
	Cursor		header = {bytes, sizeof(bytes), 4, 1};

	WriteInteger(&header, 1, 2);
	WriteInteger(&header, operation, 2);
	WriteInteger(&header, request, 8);
	WriteInteger(&header, generation, 8);
	WriteInteger(&header, size, 4);
	WriteInteger(&header, status, 4);
	return fwrite(bytes, 1, sizeof(bytes), stdout) == sizeof(bytes) &&
		(size == 0 || fwrite(data, 1, size, stdout) == size) && fflush(stdout) == 0;
}

int
main(int argc, char **argv)
{
	const AnnBackend *backend = AnnCpuBackend();
	AnnIndex	index = {0};
	AnnBatcher *batcher = NULL;
	uint64_t	lastGeneration = 0;
	int			exitCode = 0;

	if (argc == 3 && strcmp(argv[1], "--backend") == 0)
	{
#ifdef ANN_USE_CUVS
		if (strcmp(argv[2], "cuvs") == 0)
			backend = AnnCuvsBackend();
#endif
	}
	if (argc != 1 && !(argc == 3 && strcmp(argv[1], "--backend") == 0 &&
					   strcmp(argv[2], backend->name) == 0))
	{
#ifdef ANN_USE_CUVS
		fprintf(stderr, "available backends: cpu-exact, cuvs; hipvs and fused-nvfp4 require vendor adapters\n");
#else
		fprintf(stderr, "available backend: cpu-exact; cuvs, hipvs, and fused-nvfp4 require vendor adapters\n");
#endif
		return 2;
	}
	for (;;)
	{
		unsigned char bytes[32];
		Cursor		header = {bytes, sizeof(bytes), 4, 1};
		Cursor		input = {NULL, 0, 0, 1};
		Cursor		output = {NULL, 0, 0, 1};
		size_t		received = fread(bytes, 1, sizeof(bytes), stdin);
		uint16_t	version;
		uint16_t	operation;
		uint64_t	request;
		uint64_t	generation;
		uint32_t	length;
		uint32_t	requestStatus;
		AnnStatus	status = ANN_INVALID;

		if (received == 0 && feof(stdin))
			break;
		if (received != sizeof(bytes) || memcmp(bytes, "PVAN", 4) != 0)
		{
			exitCode = 1;
			break;
		}
		version = (uint16_t) ReadInteger(&header, 2);
		operation = (uint16_t) ReadInteger(&header, 2);
		request = ReadInteger(&header, 8);
		generation = ReadInteger(&header, 8);
		length = (uint32_t) ReadInteger(&header, 4);
		requestStatus = (uint32_t) ReadInteger(&header, 4);
		if (version != 1 || length > ANN_MAX_BYTES || requestStatus != 0)
		{
			exitCode = 1;
			break;
		}
		input.data = malloc(length ? length : 1);
		input.size = length;
		if (input.data == NULL || fread(input.data, 1, length, stdin) != length)
		{
			free(input.data);
			exitCode = 1;
			break;
		}
		if (operation == 0 && length == 0)
		{
			char		caps[512];
			int			size = snprintf(caps, sizeof(caps),
										"{\"backend\":\"%s\",\"algorithm\":\"%s\",\"storage\":\"%s\","
										"\"metrics_mask\":%u,\"filters_mask\":%u,\"input\":\"fp32\","
										"\"min_rows\":%u,\"max_k\":%u,"
										"\"exact\":%s,\"max_frame_bytes\":%u,\"max_queries\":1024}",
										backend->name, backend->algorithm, backend->storage,
										backend->metrics, backend->filters, backend->min_rows, backend->max_k,
										backend->exact ? "true" : "false",
										ANN_MAX_BYTES);

			status = ANN_OK;
			if (size < 0 || (size_t) size >= sizeof(caps))
			{
				exitCode = 1;
				free(input.data);
				break;
			}
			output.offset = (size_t) size;
			output.data = malloc(output.offset);
			if (output.data == NULL)
				status = ANN_NOMEM;
			else
				memcpy(output.data, caps, output.offset);
		}
		else if (operation == 1)
		{
			AnnIndex	next = {0};
			AnnBatcher *nextBatcher = NULL;

			status = generation <= lastGeneration ? ANN_STALE :
				Build(backend, &next, generation, &input);
			if (status == ANN_OK && backend == AnnCpuBackend())
			{
				AnnBatchOptions options;

				AnnBatchDefaultOptions(&options);
				options.max_pending_requests = 1024;
				status = AnnBatchCreateBorrowed(backend, &next, &options, &nextBatcher);
			}
			if (status == ANN_OK)
			{
				if (batcher)
					AnnBatchClose(batcher);
				backend->destroy(&index);
				index = next;
				memset(&next, 0, sizeof(next));
				batcher = nextBatcher;
				lastGeneration = generation;
			}
			backend->destroy(&next);
		}
		else if (generation == 0 || generation != index.generation)
			status = ANN_STALE;
		else if (operation == 2)
			status = Search(backend, &index, batcher, &input, &output);
		else if (operation == 3 && length == 0)
		{
			if (batcher)
				AnnBatchClose(batcher);
			batcher = NULL;
			backend->destroy(&index);
			status = ANN_OK;
		}
		else if (operation == 4 && length == 0)
		{
			size_t		count = (size_t) index.rows * index.dimensions;

			output.size = 8 + count * 4;
			output.data = malloc(output.size);
			status = output.data ? ANN_OK : ANN_NOMEM;
			if (output.data)
			{
				WriteInteger(&output, index.rows, 4);
				WriteInteger(&output, index.dimensions, 4);
				for (size_t i = 0; i < count; i++)
				{
					uint32_t	bits;

					memcpy(&bits, &index.vectors[i], sizeof(bits));
					WriteInteger(&output, bits, 4);
				}
			}
		}
		else if (operation == 5)
		{
			/* Diagnostic transport probe, never a search result. */
			status = ANN_OK;
		}
		else if (operation == 6 && length == 0)
		{
			output.size = 8;
			output.data = malloc(output.size);
			status = output.data ? ANN_OK : ANN_NOMEM;
			if (output.data)
			{
				WriteInteger(&output, index.rows, 4);
				WriteInteger(&output, index.dimensions, 4);
			}
		}
		else
			status = ANN_UNSUPPORTED;
		if (!Reply(operation, request, generation, status,
				   output.data, status == ANN_OK ? output.offset : 0))
			exitCode = 1;
		free(input.data);
		free(output.data);
		if (exitCode)
			break;
	}
	if (batcher)
		AnnBatchClose(batcher);
	backend->destroy(&index);
	return exitCode;
}
