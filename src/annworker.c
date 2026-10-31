#include "postgres.h"

#include "ann.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "utils/guc.h"
#include "utils/memutils.h"

char	   *ann_worker_socket = NULL;
int			ann_worker_chunk_rows = 4096;
int			ann_worker_pipeline_depth = 4;
int			ann_worker_timeout_ms = 30000;
int			ann_worker_shared_memory_mb = 64;

#if defined(WIN32) || defined(WORDS_BIGENDIAN)

/* The worker route is POSIX and little-endian only; scans stay local. */
bool
AnnWorkerBegin(void)
{
	return false;
}

void
AnnWorkerEnd(void)
{
}

float *
AnnWorkerSharedBuffer(size_t *size)
{
	*size = 0;
	return NULL;
}

uint64
AnnWorkerSubmit(uint32 rows, uint32 dimensions, uint32 k, const float *query,
				const float *vectors)
{
	return 0;
}

bool
AnnWorkerReceive(uint64 *request, uint32 *status, uint32 **ids, uint32 *count)
{
	return false;
}

bool
AnnWorkerReady(void)
{
	return false;
}

bool
AnnWorkerCall(uint16 operation, const char *payload, size_t length, int timeoutMs,
			  uint32 *status, char **reply, uint32 *replyLength)
{
	return false;
}

#else

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#define ANN_HEADER_SIZE 32
#define ANN_OP_GATHERED_SEARCH 7
#define ANN_OP_ATTACH_SHARED 9
#define ANN_OP_SHARED_SEARCH 10

/*
 * One connection per backend, reused across scans. Replies may arrive in any
 * order. A scan that errors out leaves replies outstanding; the next scan
 * reconnects rather than reading them.
 */
static int	workerFd = -1;
static uint32 outstanding = 0;
static uint64 nextRequest = 1;
static char *recvBuffer = NULL;
static size_t recvLength = 0;
static size_t recvCapacity = 0;

/*
 * Chunk vectors are written directly into a segment the worker maps
 * read-only, so requests carry offsets instead of vector bytes. A failed
 * connection keeps the mapping until no scan can still be reading it.
 */
static char *shared = NULL;
static size_t sharedSize = 0;
static bool sharedDetached = false;
static bool scanActive = false;
static int	waitTimeoutMs = 30000;

static void
Disconnect(void)
{
	if (workerFd >= 0)
		close(workerFd);
	workerFd = -1;
	outstanding = 0;
	recvLength = 0;
	if (shared)
		sharedDetached = true;
}

static void
Unmap(void)
{
	if (shared)
		munmap(shared, sharedSize);
	shared = NULL;
	sharedSize = 0;
	sharedDetached = false;
}

static bool Send(const char *data, size_t size);
static void PutHeader(char *header, uint16 operation, uint64 request, uint32 length);
static bool ReceiveFrame(uint64 *request, uint32 *status, const char **payload, uint32 *length);
static void ConsumeFrame(uint32 length);

/* Create, attach, and immediately unlink a segment; failure keeps inline mode. */
static void
AttachShared(void)
{
	static uint32 counter = 0;
	char		name[32];
	char		header[ANN_HEADER_SIZE];
	size_t		size = (size_t) ann_worker_shared_memory_mb * 1024 * 1024;
	uint64		request = nextRequest++;
	uint64		replyRequest;
	uint32		status;
	uint32		length;
	const char *payload;
	int			fd;

	if (size == 0)
		return;
	snprintf(name, sizeof(name), "/pgann.%d.%u", MyProcPid, counter++);
	fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0600);
	if (fd < 0)
		return;
	if (ftruncate(fd, size) != 0 ||
		(shared = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED)
	{
		shared = NULL;
		close(fd);
		shm_unlink(name);
		return;
	}
	close(fd);
	sharedSize = size;
	PutHeader(header, ANN_OP_ATTACH_SHARED, request, strlen(name));
	if (Send(header, sizeof(header)) && Send(name, strlen(name)))
	{
		outstanding++;
		if (ReceiveFrame(&replyRequest, &status, &payload, &length))
		{
			ConsumeFrame(length);
			if (replyRequest == request && status == 0)
			{
				shm_unlink(name);
				return;
			}
		}
	}
	shm_unlink(name);
	Unmap();
}

static bool
Connect(void)
{
	struct sockaddr_un address = {0};

	waitTimeoutMs = ann_worker_timeout_ms;
	if (ann_worker_socket == NULL || ann_worker_socket[0] == '\0')
		return false;
	if (outstanding > 0)
		Disconnect();
	if (sharedDetached)
		Unmap();
	if (workerFd >= 0)
		return true;
	if (strlen(ann_worker_socket) >= sizeof(address.sun_path))
		return false;
	address.sun_family = AF_UNIX;
	strcpy(address.sun_path, ann_worker_socket);
	workerFd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (workerFd < 0)
		return false;
	if (connect(workerFd, (struct sockaddr *) &address, sizeof(address)) != 0 ||
		fcntl(workerFd, F_SETFL, fcntl(workerFd, F_GETFL) | O_NONBLOCK) != 0)
	{
		elog(DEBUG1, "ann worker unavailable at \"%s\": %m", ann_worker_socket);
		Disconnect();
		return false;
	}
	{
		/*
		 * Chunks are megabytes; small default Unix socket buffers cost
		 * syscalls.
		 */
		int			buffer = 4 * 1024 * 1024;

		(void) setsockopt(workerFd, SOL_SOCKET, SO_SNDBUF, &buffer, sizeof(buffer));
		(void) setsockopt(workerFd, SOL_SOCKET, SO_RCVBUF, &buffer, sizeof(buffer));
	}
#ifdef SO_NOSIGPIPE
	{
		int			on = 1;

		(void) setsockopt(workerFd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
	}
#endif
	if (recvBuffer == NULL)
	{
		recvCapacity = 64 * 1024;
		recvBuffer = MemoryContextAlloc(TopMemoryContext, recvCapacity);
	}
	AttachShared();
	return workerFd >= 0;
}

bool
AnnWorkerBegin(void)
{
	if (scanActive || !Connect())
		return false;
	scanActive = true;
	return true;
}

/* Callers pair this with a successful AnnWorkerBegin, including on error. */
void
AnnWorkerEnd(void)
{
	scanActive = false;
}

float *
AnnWorkerSharedBuffer(size_t *size)
{
	*size = workerFd >= 0 && !sharedDetached ? sharedSize : 0;
	return *size ? (float *) shared : NULL;
}

/* Read whatever is available without blocking; false on EOF or error. */
static bool
ReadAvailable(void)
{
	for (;;)
	{
		ssize_t		n;

		if (recvLength == recvCapacity)
		{
			recvCapacity *= 2;
			recvBuffer = repalloc(recvBuffer, recvCapacity);
		}
		n = recv(workerFd, recvBuffer + recvLength, recvCapacity - recvLength, 0);
		if (n > 0)
		{
			recvLength += n;
			continue;
		}
		if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
			return true;
		return false;
	}
}

/*
 * Wait for the socket while servicing interrupts. Reading while blocked on
 * writes keeps a pipelined worker from stalling on a full reply buffer.
 */
static bool
Wait(short events, instr_time start)
{
	struct pollfd poller = {workerFd, events | POLLIN, 0};
	instr_time	now;
	int			result;

	CHECK_FOR_INTERRUPTS();
	INSTR_TIME_SET_CURRENT(now);
	INSTR_TIME_SUBTRACT(now, start);
	if (INSTR_TIME_GET_MILLISEC(now) > waitTimeoutMs)
	{
		elog(DEBUG1, "ann worker timed out");
		return false;
	}
	result = poll(&poller, 1, 100);
	if (result < 0 && errno != EINTR)
		return false;
	if (result > 0 && (poller.revents & (POLLIN | POLLHUP | POLLERR)))
		return ReadAvailable();
	return true;
}

static bool
Send(const char *data, size_t size)
{
	instr_time	start;

	INSTR_TIME_SET_CURRENT(start);
	while (size > 0)
	{
#ifdef MSG_NOSIGNAL
		ssize_t		n = send(workerFd, data, size, MSG_NOSIGNAL);
#else
		ssize_t		n = send(workerFd, data, size, 0);
#endif

		if (n > 0)
		{
			data += n;
			size -= n;
			continue;
		}
		if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
			return false;
		if (!Wait(POLLOUT, start))
			return false;
	}
	return true;
}

static void
Put32(char *data, uint32 value)
{
	memcpy(data, &value, sizeof(value));
}

static uint32
Get32(const char *data)
{
	uint32		value;

	memcpy(&value, data, sizeof(value));
	return value;
}

static uint64
Get64(const char *data)
{
	uint64		value;

	memcpy(&value, data, sizeof(value));
	return value;
}

static void
PutHeader(char *header, uint16 operation, uint64 request, uint32 length)
{
	uint16		version = 1;
	uint64		generation = 0;

	memcpy(header, "PVAN", 4);
	memcpy(header + 4, &version, 2);
	memcpy(header + 6, &operation, 2);
	memcpy(header + 8, &request, 8);
	memcpy(header + 16, &generation, 8);
	Put32(header + 24, length);
	Put32(header + 28, 0);
}

/*
 * Vectors inside the shared buffer are sent by offset and must stay unchanged
 * until the reply. Returns the request ID, or 0 after closing the connection.
 */
uint64
AnnWorkerSubmit(uint32 rows, uint32 dimensions, uint32 k, const float *query,
				const float *vectors)
{
	char		header[ANN_HEADER_SIZE + 24];
	uint64		request = nextRequest++;
	size_t		vectorBytes = (size_t) rows * dimensions * sizeof(float);
	bool		inShared = shared && !sharedDetached && (const char *) vectors >= shared &&
		(const char *) vectors + vectorBytes <= shared + sharedSize;
	size_t		fields = inShared ? 24 : 16;
	size_t		payload = fields + dimensions * sizeof(float) + (inShared ? 0 : vectorBytes);

	if (workerFd < 0 || payload > 64 * 1024 * 1024)
		return 0;
	PutHeader(header, inShared ? ANN_OP_SHARED_SEARCH : ANN_OP_GATHERED_SEARCH,
			  request, (uint32) payload);
	Put32(header + 32, rows);
	Put32(header + 36, dimensions);
	Put32(header + 40, k);
	Put32(header + 44, 0);		/* L2 */
	if (inShared)
	{
		uint64		offset = (const char *) vectors - shared;

		memcpy(header + 48, &offset, 8);
	}
	if (!Send(header, ANN_HEADER_SIZE + fields) ||
		!Send((const char *) query, dimensions * sizeof(float)) ||
		(!inShared && !Send((const char *) vectors, vectorBytes)))
	{
		Disconnect();
		return 0;
	}
	outstanding++;
	return request;
}

/* True when a complete reply is buffered, reading without blocking. */
bool
AnnWorkerReady(void)
{
	if (workerFd < 0 || outstanding == 0)
		return false;
	if (!ReadAvailable())
	{
		/* A real failure surfaces on the next blocking receive. */
		return true;
	}
	return recvLength >= ANN_HEADER_SIZE &&
		recvLength >= ANN_HEADER_SIZE + (size_t) Get32(recvBuffer + 24);
}

/* Wait for a complete frame at the front of the receive buffer. */
static bool
ReceiveFrame(uint64 *request, uint32 *status, const char **payload, uint32 *length)
{
	instr_time	start;

	if (workerFd < 0 || outstanding == 0)
		return false;
	INSTR_TIME_SET_CURRENT(start);
	for (;;)
	{
		if (recvLength >= ANN_HEADER_SIZE)
		{
			*length = Get32(recvBuffer + 24);
			if (memcmp(recvBuffer, "PVAN", 4) != 0 || *length > 64 * 1024 * 1024)
			{
				Disconnect();
				return false;
			}
			if (recvLength >= ANN_HEADER_SIZE + (size_t) *length)
				break;
		}
		if (!Wait(0, start))
		{
			Disconnect();
			return false;
		}
	}
	*request = Get64(recvBuffer + 8);
	*status = Get32(recvBuffer + 28);
	*payload = recvBuffer + ANN_HEADER_SIZE;
	return true;
}

static void
ConsumeFrame(uint32 length)
{
	size_t		total = ANN_HEADER_SIZE + (size_t) length;

	memmove(recvBuffer, recvBuffer + total, recvLength - total);
	recvLength -= total;
	outstanding--;
}

/*
 * Wait for the next search reply. On success, *ids is palloc'd in the current
 * context. Returns false after closing a failed or malformed connection.
 */
bool
AnnWorkerReceive(uint64 *request, uint32 *status, uint32 **ids, uint32 *count)
{
	const char *payload;
	uint32		length;

	if (!ReceiveFrame(request, status, &payload, &length))
		return false;
	*ids = NULL;
	*count = 0;
	if (*status == 0)
	{
		if (length < 4 || (*count = Get32(payload)) > (length - 4) / 12 ||
			(size_t) *count * 12 + 4 != length)
		{
			Disconnect();
			return false;
		}
		*ids = palloc(sizeof(uint32) * Max(*count, 1));
		for (uint32 i = 0; i < *count; i++)
			(*ids)[i] = Get32(payload + 4 + (size_t) i * 12);
	}
	ConsumeFrame(length);
	return true;
}

/*
 * One synchronous request outside a scan. On success the reply payload is
 * palloc'd and NUL-terminated. Returns false when the worker is unavailable
 * or the connection failed; *status carries the worker's status otherwise.
 */
bool
AnnWorkerCall(uint16 operation, const char *payload, size_t length, int timeoutMs,
			  uint32 *status, char **reply, uint32 *replyLength)
{
	char		header[ANN_HEADER_SIZE];
	uint64		request;
	uint64		replyRequest;
	const char *data;
	uint32		dataLength;

	if (scanActive || length > 64 * 1024 * 1024 || !Connect())
		return false;
	waitTimeoutMs = timeoutMs > 0 ? timeoutMs : ann_worker_timeout_ms;
	request = nextRequest++;
	PutHeader(header, operation, request, (uint32) length);
	if (!Send(header, sizeof(header)) || (length && !Send(payload, length)))
	{
		Disconnect();
		return false;
	}
	outstanding++;
	if (!ReceiveFrame(&replyRequest, status, &data, &dataLength) || replyRequest != request)
	{
		Disconnect();
		return false;
	}
	*reply = palloc(dataLength + 1);
	memcpy(*reply, data, dataLength);
	(*reply)[dataLength] = '\0';
	*replyLength = dataLength;
	ConsumeFrame(dataLength);
	waitTimeoutMs = ann_worker_timeout_ms;
	return true;
}

#endif

void
AnnWorkerInit(void)
{
	DefineCustomStringVariable("ann.worker_socket",
							   "Unix socket of the ann worker for filtered search.",
							   "Empty disables the worker route.",
							   &ann_worker_socket, "", PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("ann.worker_chunk_rows",
							"Eligible rows sent to the ann worker per request.",
							NULL, &ann_worker_chunk_rows, 4096, 64, 1 << 20,
							PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("ann.worker_pipeline_depth",
							"Requests a scan keeps in flight to the ann worker.",
							NULL, &ann_worker_pipeline_depth, 4, 1, 64,
							PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("ann.worker_shared_memory_mb",
							"Shared memory per backend for vectors sent to the ann worker.",
							"Zero sends vectors through the socket instead.",
							&ann_worker_shared_memory_mb, 64, 0, 4096,
							PGC_SUSET, GUC_UNIT_MB, NULL, NULL, NULL);
	DefineCustomIntVariable("ann.worker_timeout_ms",
							"Time to wait for ann worker progress before scoring locally.",
							NULL, &ann_worker_timeout_ms, 30000, 1, INT_MAX,
							PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);
}
