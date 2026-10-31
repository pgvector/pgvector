#include "postgres.h"

#include "ann.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "utils/guc.h"
#include "utils/memutils.h"

char	   *ann_worker_address = NULL;
char	   *ann_worker_auth_token = NULL;
int			ann_gpu_min_eligible = 1000;
int			ann_worker_heartbeat_ms = 5000;
int			ann_worker_retry_ms = 10000;

#if defined(WIN32) || defined(WORDS_BIGENDIAN)

bool
AnnRemoteConfigured(void)
{
	return false;
}

bool
AnnRemoteCall(uint16 operation, const char *payload, size_t length, int timeoutMs,
			  uint32 *status, char **reply, uint32 *replyLength)
{
	return false;
}

int
AnnRemoteBroadcast(uint16 operation, const char *payload, size_t length, int timeoutMs,
				   uint32 *status, char **reply, uint32 *replyLength)
{
	return -1;
}

#else

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#define ANN_HEADER_SIZE 32
#define ANN_OP_PROBE 5
#define ANN_OP_AUTH 13
#define ANN_MAX_NODES 64

/*
 * One connection per backend to one GPU node, chosen from ann.worker_address
 * by process ID so backends spread across nodes. A failed node is skipped
 * and the next one tried. Requests are synchronous: one outstanding at most.
 */
static int	remoteFd = -1;
static int	remoteNode = -1;
static uint64 remoteRequest = 1;
static bool remoteBusy = false; /* an error interrupted an exchange */
static instr_time remoteUsed;	/* last successful exchange */
static instr_time downSince[ANN_MAX_NODES]; /* zero when believed healthy */

static void
CloseRemote(void)
{
	if (remoteFd >= 0)
		close(remoteFd);
	remoteFd = -1;
}

static double
ElapsedMs(instr_time since)
{
	instr_time	now;

	INSTR_TIME_SET_CURRENT(now);
	INSTR_TIME_SUBTRACT(now, since);
	return INSTR_TIME_GET_MILLISEC(now);
}

/* A node that failed is skipped until ann.worker_retry_ms has passed. */
static void
MarkDown(int node)
{
	INSTR_TIME_SET_CURRENT(downSince[node]);
}

static bool
NodeAvailable(int node)
{
	if (INSTR_TIME_IS_ZERO(downSince[node]))
		return true;
	if (ElapsedMs(downSince[node]) < ann_worker_retry_ms)
		return false;
	INSTR_TIME_SET_ZERO(downSince[node]);
	return true;
}

/*
 * Kernel keepalives detect a node that vanished mid-request (host down or
 * partitioned), which a blocked receive would otherwise only notice at the
 * request timeout.
 */
static void
SetKeepalive(int fd)
{
	int			on = 1;
	int			idle = Max(1, ann_worker_heartbeat_ms / 1000);
	int			interval = Max(1, idle / 2);
	int			count = 3;

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
}

bool
AnnRemoteConfigured(void)
{
	return ann_worker_address != NULL && ann_worker_address[0] != '\0';
}

/* Split ann.worker_address into host and port strings; returns the count. */
static int
ParseNodes(char **hosts, char **ports)
{
	char	   *list = pstrdup(ann_worker_address);
	char	   *token;
	char	   *save = NULL;
	int			count = 0;

	for (token = strtok_r(list, ", ", &save); token && count < ANN_MAX_NODES;
		 token = strtok_r(NULL, ", ", &save))
	{
		char	   *colon = strrchr(token, ':');

		if (colon == NULL || colon == token)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid ann.worker_address entry \"%s\"", token),
					 errhint("Use a comma-separated list of host:port.")));
		*colon = '\0';
		hosts[count] = token;
		ports[count] = colon + 1;
		count++;
	}
	return count;
}

static bool
WaitFor(int fd, short events, instr_time start, int timeoutMs)
{
	for (;;)
	{
		struct pollfd poller = {fd, events, 0};
		instr_time	now;
		int			result;

		CHECK_FOR_INTERRUPTS();
		INSTR_TIME_SET_CURRENT(now);
		INSTR_TIME_SUBTRACT(now, start);
		if (INSTR_TIME_GET_MILLISEC(now) > timeoutMs)
			return false;
		result = poll(&poller, 1, 100);
		if (result > 0)
			return (poller.revents & (POLLERR | POLLNVAL)) == 0 || (poller.revents & events);
		if (result < 0 && errno != EINTR)
			return false;
	}
}

static bool
SendAll(int fd, const char *data, size_t size, instr_time start, int timeoutMs)
{
	while (size > 0)
	{
#ifdef MSG_NOSIGNAL
		ssize_t		n = send(fd, data, size, MSG_NOSIGNAL);
#else
		ssize_t		n = send(fd, data, size, 0);
#endif

		if (n > 0)
		{
			data += n;
			size -= n;
		}
		else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
		{
			if (!WaitFor(fd, POLLOUT, start, timeoutMs))
				return false;
		}
		else
			return false;
	}
	return true;
}

static bool
RecvAll(int fd, char *data, size_t size, instr_time start, int timeoutMs)
{
	while (size > 0)
	{
		ssize_t		n = recv(fd, data, size, 0);

		if (n > 0)
		{
			data += n;
			size -= n;
		}
		else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
		{
			if (!WaitFor(fd, POLLIN, start, timeoutMs))
				return false;
		}
		else
			return false;
	}
	return true;
}

static void
PutHeader(char *header, uint16 operation, uint64 request, uint32 length)
{
	uint16		version = 1;
	uint64		generation = 0;
	uint32		status = 0;

	memcpy(header, "PVAN", 4);
	memcpy(header + 4, &version, 2);
	memcpy(header + 6, &operation, 2);
	memcpy(header + 8, &request, 8);
	memcpy(header + 16, &generation, 8);
	memcpy(header + 24, &length, 4);
	memcpy(header + 28, &status, 4);
}

/*
 * Send one frame and read its reply on fd. The reply payload is palloc'd in
 * the current context and NUL-terminated.
 */
static bool
Exchange(int fd, uint16 operation, const char *payload, size_t length, int timeoutMs,
		 uint32 *status, char **reply, uint32 *replyLength)
{
	char		header[ANN_HEADER_SIZE];
	uint64		request = remoteRequest++;
	uint64		replyRequest;
	uint32		size;
	instr_time	start;

	INSTR_TIME_SET_CURRENT(start);
	PutHeader(header, operation, request, (uint32) length);
	if (!SendAll(fd, header, sizeof(header), start, timeoutMs) ||
		(length && !SendAll(fd, payload, length, start, timeoutMs)) ||
		!RecvAll(fd, header, sizeof(header), start, timeoutMs) ||
		memcmp(header, "PVAN", 4) != 0)
		return false;
	memcpy(&replyRequest, header + 8, 8);
	memcpy(&size, header + 24, 4);
	memcpy(status, header + 28, 4);
	if (replyRequest != request || size > 64 * 1024 * 1024)
		return false;
	*reply = palloc(size + 1);
	if (!RecvAll(fd, *reply, size, start, timeoutMs))
		return false;
	(*reply)[size] = '\0';
	*replyLength = size;
	return true;
}

/* Connect and authenticate to one node; returns the socket or -1. */
static int
ConnectNode(const char *host, const char *port, int timeoutMs)
{
	struct addrinfo hints = {0};
	struct addrinfo *addresses;
	int			fd = -1;
	instr_time	start;

	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	if (getaddrinfo(host, port, &hints, &addresses) != 0)
		return -1;
	INSTR_TIME_SET_CURRENT(start);
	for (struct addrinfo *a = addresses; a && fd < 0; a = a->ai_next)
	{
		int			on = 1;
		int			error = 0;
		socklen_t	errorLength = sizeof(error);

		fd = socket(a->ai_family, SOCK_STREAM, 0);
		if (fd < 0)
			continue;
		if (fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK) != 0 ||
			(connect(fd, a->ai_addr, a->ai_addrlen) != 0 &&
			 (errno != EINPROGRESS || !WaitFor(fd, POLLOUT, start, timeoutMs) ||
			  getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &errorLength) != 0 || error != 0)))
		{
			close(fd);
			fd = -1;
			continue;
		}
		(void) setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
		if (ann_worker_heartbeat_ms > 0)
			SetKeepalive(fd);
#ifdef SO_NOSIGPIPE
		(void) setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
	}
	freeaddrinfo(addresses);
	if (fd >= 0)
	{
		const char *token = ann_worker_auth_token ? ann_worker_auth_token : "";
		uint32		status;
		char	   *reply;
		uint32		replyLength;

		if (!Exchange(fd, ANN_OP_AUTH, token, strlen(token), timeoutMs, &status, &reply, &replyLength) ||
			status != 0)
		{
			elog(DEBUG1, "ann worker %s:%s rejected authentication", host, port);
			close(fd);
			fd = -1;
		}
	}
	return fd;
}

/*
 * Synchronous request to this backend's node. A transport failure moves to
 * the next node and retries once there; requests sent this way are
 * idempotent searches and lookups.
 */
bool
AnnRemoteCall(uint16 operation, const char *payload, size_t length, int timeoutMs,
			  uint32 *status, char **reply, uint32 *replyLength)
{
	char	   *hosts[ANN_MAX_NODES];
	char	   *ports[ANN_MAX_NODES];
	int			count = ParseNodes(hosts, ports);

	if (timeoutMs <= 0)
		timeoutMs = ann_worker_timeout_ms;
	if (count == 0)
		return false;

	/* A cancelled exchange may have left part of a reply unread. */
	if (remoteBusy)
		CloseRemote();

	/*
	 * Heartbeat: a connection idle longer than ann.worker_heartbeat_ms is
	 * probed first, so a node that died while idle costs one short probe
	 * rather than a full request timeout.
	 */
	if (remoteFd >= 0 && ann_worker_heartbeat_ms > 0 &&
		ElapsedMs(remoteUsed) > ann_worker_heartbeat_ms)
	{
		uint32		probeStatus;
		char	   *probeReply;
		uint32		probeLength;

		remoteBusy = true;
		if (!Exchange(remoteFd, ANN_OP_PROBE, NULL, 0, ann_worker_heartbeat_ms,
					  &probeStatus, &probeReply, &probeLength))
		{
			elog(DEBUG1, "ann worker %s:%s missed a heartbeat", hosts[remoteNode], ports[remoteNode]);
			MarkDown(remoteNode);
			CloseRemote();
		}
		remoteBusy = false;
	}
	for (int attempt = 0; attempt < count + 1; attempt++)
	{
		if (remoteFd < 0)
		{
			/*
			 * Prefer healthy nodes; try cooling-down ones only as a last
			 * resort.
			 */
			for (int pass = 0; pass < 2 && remoteFd < 0; pass++)
				for (int i = 0; i < count && remoteFd < 0; i++)
				{
					int			node = (remoteNode < 0 ? MyProcPid + i : remoteNode + 1 + i) % count;

					if (pass == 0 && !NodeAvailable(node))
						continue;
					remoteFd = ConnectNode(hosts[node], ports[node], Min(timeoutMs, Max(ann_worker_heartbeat_ms, 1000)));
					if (remoteFd >= 0)
						remoteNode = node;
					else
						MarkDown(node);
				}
			if (remoteFd < 0)
				return false;
		}
		remoteBusy = true;
		if (Exchange(remoteFd, operation, payload, length, timeoutMs, status, reply, replyLength))
		{
			remoteBusy = false;
			INSTR_TIME_SET_CURRENT(remoteUsed);
			return true;
		}
		remoteBusy = false;
		elog(DEBUG1, "ann worker %s:%s failed; trying the next node", hosts[remoteNode], ports[remoteNode]);
		MarkDown(remoteNode);
		CloseRemote();
	}
	return false;
}

/*
 * Send one request to every node, for loads. Returns the number of nodes on
 * success, or -1 after the first failure; *status holds that node's status.
 */
int
AnnRemoteBroadcast(uint16 operation, const char *payload, size_t length, int timeoutMs,
				   uint32 *status, char **reply, uint32 *replyLength)
{
	char	   *hosts[ANN_MAX_NODES];
	char	   *ports[ANN_MAX_NODES];
	int			count = ParseNodes(hosts, ports);

	for (int i = 0; i < count; i++)
	{
		int			fd = ConnectNode(hosts[i], ports[i], timeoutMs);
		bool		ok;

		if (fd < 0)
		{
			*status = UINT32_MAX;
			return -1;
		}
		ok = Exchange(fd, operation, payload, length, timeoutMs, status, reply, replyLength);
		close(fd);
		if (!ok)
			*status = UINT32_MAX;
		if (!ok || *status != 0)
			return -1;
	}
	return count;
}

#endif

void
AnnRemoteInit(void)
{
	DefineCustomStringVariable("ann.worker_address",
							   "Comma-separated host:port list of networked ann GPU workers.",
							   "When set, loaded-index functions use these nodes instead of ann.worker_socket.",
							   &ann_worker_address, "", PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("ann.worker_auth_token",
							   "Shared secret sent to networked ann workers.",
							   NULL, &ann_worker_auth_token, "", PGC_SUSET,
							   GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL, NULL, NULL, NULL);
	DefineCustomIntVariable("ann.worker_heartbeat_ms",
							"Idle time after which a GPU node connection is probed before reuse.",
							"Also sets TCP keepalive timing; 0 disables heartbeats.",
							&ann_worker_heartbeat_ms, 5000, 0, INT_MAX, PGC_USERSET, GUC_UNIT_MS,
							NULL, NULL, NULL);
	DefineCustomIntVariable("ann.worker_retry_ms",
							"Time a failed GPU node is skipped before being tried again.",
							NULL, &ann_worker_retry_ms, 10000, 0, INT_MAX, PGC_USERSET, GUC_UNIT_MS,
							NULL, NULL, NULL);
	DefineCustomIntVariable("ann.gpu_min_eligible",
							"Filters with fewer eligible rows are scored exactly on this host.",
							"Filtered loaded-index searches below this count never leave the host.",
							&ann_gpu_min_eligible, 1000, 0, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
}
