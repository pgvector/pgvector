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
char	   *ann_worker_rdma = NULL;
int			ann_worker_rdma_spin_us = 50;

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
#define ANN_OP_INDEX_SEARCH 12
#define ANN_OP_INDEX_SEARCH_PACKED 14
#define ANN_OP_RDMA_HELLO 16
#define ANN_OP_INDEX_SEARCH_MATCH 17
#define ANN_OP_INDEX_SEARCH_BITMAP 19
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

static bool Exchange(int fd, uint16 operation, const char *payload, size_t length, int timeoutMs,
					 uint32 *status, char **reply, uint32 *replyLength);
static double ElapsedMs(instr_time since);
static void PutHeader(char *header, uint16 operation, uint64 request, uint32 length);
static bool RecvAll(int fd, char *data, size_t size, instr_time start, int timeoutMs);

#ifdef ANN_USE_RDMA

#include <infiniband/verbs.h>

#define ANN_RDMA_ADDRESS 32

/*
 * Optional RDMA path to the connected node over InfiniBand/RoCE verbs: one
 * reliable connected queue pair per backend, connected through the HELLO
 * exchange on the authenticated TCP connection. A backend has one request
 * outstanding, so one receive and one send buffer suffice, registered once.
 * Completions are polled from user space, so a search costs no system call
 * or interrupt. TCP stays open for liveness, loads, and oversized frames.
 */
typedef struct RdmaClient
{
	struct ibv_context *context;
	struct ibv_pd *pd;
	struct ibv_comp_channel *channel;	/* wakes a backend that stopped
										 * spinning */
	struct ibv_cq *cq;
	struct ibv_qp *qp;
	struct ibv_mr *mr;
	char	   *region;			/* receive buffer, then send buffer */
	size_t		frame;
	bool		sending;		/* send completion not yet reaped */
} RdmaClient;

static RdmaClient *rdmaClient = NULL;

static void
CloseRdma(void)
{
	RdmaClient *client = rdmaClient;

	if (client == NULL)
		return;
	rdmaClient = NULL;
	if (client->qp)
		(void) ibv_destroy_qp(client->qp);
	if (client->mr)
		(void) ibv_dereg_mr(client->mr);
	if (client->cq)
		(void) ibv_destroy_cq(client->cq);
	if (client->channel)
		(void) ibv_destroy_comp_channel(client->channel);
	if (client->pd)
		(void) ibv_dealloc_pd(client->pd);
	if (client->context)
		(void) ibv_close_device(client->context);
	free(client->region);
	free(client);
}

static bool
PostRdmaReceive(RdmaClient *client)
{
	struct ibv_sge sge = {(uintptr_t) client->region, (uint32_t) client->frame, client->mr->lkey};
	struct ibv_recv_wr wr = {0};
	struct ibv_recv_wr *bad;

	wr.wr_id = 1;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	return ibv_post_recv(client->qp, &wr, &bad) == 0;
}

/*
 * Open the device, connect a queue pair with the node over the
 * authenticated TCP connection fd, and post the receive buffer. Any failure
 * leaves the backend on TCP only.
 */
static void
OpenRdma(int fd, int timeoutMs)
{
	RdmaClient *client = calloc(1, sizeof(RdmaClient));
	struct ibv_device **list = NULL;
	struct ibv_port_attr port;
	struct ibv_qp_init_attr init = {0};
	struct ibv_qp_attr attr = {0};
	union ibv_gid gid;
	unsigned char address[ANN_RDMA_ADDRESS] = {0};
	char		device[64];
	const char *colon = strchr(ann_worker_rdma, ':');
	uint8		portNumber = colon ? (uint8) atoi(colon + 1) : 1;
	uint32		status;
	char	   *reply;
	uint32		replyLength;
	uint32		workerFrame;
	uint16		lid,
				mtu;
	uint32		qpn,
				psn,
				localPsn = (uint32) random() & 0xffffff;
	int			devices = 0;
	const char *step = "find device";

	if (client == NULL)
		goto fail;
	rdmaClient = client;
	snprintf(device, sizeof(device), "%.*s",
			 (int) (colon ? (size_t) (colon - ann_worker_rdma) : strlen(ann_worker_rdma)), ann_worker_rdma);
	list = ibv_get_device_list(&devices);
	for (int i = 0; list && i < devices && client->context == NULL; i++)
		if (strcmp(ibv_get_device_name(list[i]), device) == 0)
			client->context = ibv_open_device(list[i]);
	if (list)
		ibv_free_device_list(list);
	if (client->context == NULL)
		goto fail;
	step = "query port";
	if (ibv_query_port(client->context, portNumber, &port) != 0 || port.state != IBV_PORT_ACTIVE ||
		ibv_query_gid(client->context, portNumber, 0, &gid) != 0)
		goto fail;
	step = "create queue pair";
	init.cap.max_send_wr = 4;
	init.cap.max_recv_wr = 4;
	init.cap.max_send_sge = 1;
	init.cap.max_recv_sge = 1;
	init.qp_type = IBV_QPT_RC;
	if ((client->pd = ibv_alloc_pd(client->context)) == NULL ||
		(client->channel = ibv_create_comp_channel(client->context)) == NULL ||
		fcntl(client->channel->fd, F_SETFL, fcntl(client->channel->fd, F_GETFL) | O_NONBLOCK) != 0 ||
		(client->cq = ibv_create_cq(client->context, 16, NULL, client->channel, 0)) == NULL)
		goto fail;
	init.send_cq = client->cq;
	init.recv_cq = client->cq;
	if ((client->qp = ibv_create_qp(client->pd, &init)) == NULL)
		goto fail;
	attr.qp_state = IBV_QPS_INIT;
	attr.port_num = portNumber;
	if (ibv_modify_qp(client->qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) != 0)
		goto fail;

	/* Exchange queue pair addresses over the authenticated connection. */
	step = "hello";
	lid = port.lid;
	mtu = (uint16) port.active_mtu;
	qpn = client->qp->qp_num;
	memcpy(address, &lid, 2);
	memcpy(address + 2, &mtu, 2);
	memcpy(address + 4, &qpn, 4);
	memcpy(address + 8, &localPsn, 4);
	memcpy(address + 12, gid.raw, 16);
	if (!Exchange(fd, ANN_OP_RDMA_HELLO, (const char *) address, sizeof(address), timeoutMs,
				  &status, &reply, &replyLength) || status != 0 || replyLength < 4 + ANN_RDMA_ADDRESS)
		goto fail;
	memcpy(&workerFrame, reply, 4);
	memcpy(&lid, reply + 4, 2);
	memcpy(&mtu, reply + 6, 2);
	memcpy(&qpn, reply + 8, 4);
	memcpy(&psn, reply + 12, 4);
	memcpy(gid.raw, reply + 16, 16);

	/* Buffers before RTR, so the receive is posted before the node can send. */
	step = "register buffers";
	client->frame = Min((size_t) workerFrame, (size_t) 256 * 1024);
	client->region = malloc(client->frame * 2);
	if (client->region == NULL ||
		(client->mr = ibv_reg_mr(client->pd, client->region, client->frame * 2, IBV_ACCESS_LOCAL_WRITE)) == NULL ||
		!PostRdmaReceive(client))
		goto fail;
	step = "connect queue pair";
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = (enum ibv_mtu) mtu;
	attr.dest_qp_num = qpn;
	attr.rq_psn = psn;
	attr.max_dest_rd_atomic = 1;
	attr.min_rnr_timer = 12;
	attr.ah_attr.dlid = lid;
	attr.ah_attr.port_num = portNumber;
	if (port.link_layer == IBV_LINK_LAYER_ETHERNET)
	{
		attr.ah_attr.is_global = 1;
		attr.ah_attr.grh.dgid = gid;
		attr.ah_attr.grh.hop_limit = 1;
	}
	if (ibv_modify_qp(client->qp, &attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
					  IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER) != 0)
		goto fail;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 14;
	attr.retry_cnt = 7;
	attr.rnr_retry = 7;
	attr.sq_psn = localPsn;
	attr.max_rd_atomic = 1;
	if (ibv_modify_qp(client->qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
					  IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC) != 0)
		goto fail;
	elog(DEBUG1, "ann worker RDMA on %s port %u ready, frames up to %zu bytes", device, portNumber, client->frame);
	return;

fail:
	elog(DEBUG1, "ann worker RDMA on %s unavailable: %s failed", ann_worker_rdma, step);
	if (rdmaClient == NULL)
		free(client);
	CloseRdma();
}

/*
 * Only searches travel over RDMA; the worker answers other operations
 * (loads, index info, statistics) on the TCP connection alone.
 */
static bool
RdmaOperation(uint16 operation)
{
	return operation == ANN_OP_INDEX_SEARCH || operation == ANN_OP_INDEX_SEARCH_PACKED ||
		operation == ANN_OP_INDEX_SEARCH_MATCH || operation == ANN_OP_INDEX_SEARCH_BITMAP;
}

/* Reap the previous send's completion before reusing the send buffer. */
static bool
ReapRdmaSend(RdmaClient *client)
{
	struct ibv_wc wc;
	int			n;

	while (client->sending && (n = ibv_poll_cq(client->cq, 1, &wc)) >= 0)
		if (n == 1)
		{
			if (wc.status != IBV_WC_SUCCESS || wc.wr_id != 2)
				return false;
			client->sending = false;
		}
	return !client->sending;
}

static bool
CopyReply(const char *frame, size_t size, uint64 request, uint32 *status, char **reply, uint32 *replyLength)
{
	uint64		replyRequest;
	uint32		length;

	if (size < ANN_HEADER_SIZE || memcmp(frame, "PVAN", 4) != 0)
		return false;
	memcpy(&replyRequest, frame + 8, 8);
	memcpy(&length, frame + 24, 4);
	memcpy(status, frame + 28, 4);
	if (replyRequest != request || length != size - ANN_HEADER_SIZE)
		return false;
	*reply = palloc(length + 1);
	memcpy(*reply, frame + ANN_HEADER_SIZE, length);
	(*reply)[length] = '\0';
	*replyLength = length;
	return true;
}

/*
 * One request over RDMA. Completions are spin-polled in user space;
 * interrupts, the timeout, and the TCP socket are checked periodically,
 * because the node answers over TCP when a reply exceeds the RDMA frame.
 */
static bool
RdmaExchange(int fd, uint16 operation, const char *payload, size_t length, int timeoutMs,
			 uint32 *status, char **reply, uint32 *replyLength)
{
	RdmaClient *client = rdmaClient;
	char	   *buffer = client->region + client->frame;
	uint64		request = remoteRequest++;
	struct ibv_sge sge;
	struct ibv_send_wr wr = {0};
	struct ibv_send_wr *bad;
	instr_time	start;
	instr_time	spinStart;
	bool		armed = false;

	INSTR_TIME_SET_CURRENT(start);
	spinStart = start;
	if (!ReapRdmaSend(client))
		return false;
	PutHeader(buffer, operation, request, (uint32) length);
	if (length)
		memcpy(buffer + ANN_HEADER_SIZE, payload, length);
	sge.addr = (uintptr_t) buffer;
	sge.length = (uint32) (ANN_HEADER_SIZE + length);
	sge.lkey = client->mr->lkey;
	wr.wr_id = 2;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_SEND;
	wr.send_flags = IBV_SEND_SIGNALED;
	if (ibv_post_send(client->qp, &wr, &bad) != 0)
		return false;
	client->sending = true;
	for (;;)
	{
		struct ibv_wc wc;
		int			n = ibv_poll_cq(client->cq, 1, &wc);

		if (n < 0)
			return false;
		if (n == 1)
		{
			if (wc.status != IBV_WC_SUCCESS)
				return false;
			if (wc.wr_id == 2)
			{
				client->sending = false;
				continue;
			}
			if (!CopyReply(client->region, wc.byte_len, request, status, reply, replyLength) ||
				!PostRdmaReceive(client))
				return false;
			return ReapRdmaSend(client);
		}

		/*
		 * Spin briefly for latency, then sleep: hundreds of backends spinning
		 * on their own completion queues would starve the CPUs serving them.
		 * Arming the queue makes the adapter signal the channel; a completion
		 * that raced the arm is caught by polling the queue again first.
		 */
		if (!armed && ElapsedMs(spinStart) * 1000.0 < ann_worker_rdma_spin_us)
			continue;
		if (!armed)
		{
			if (ibv_req_notify_cq(client->cq, 0) != 0)
				return false;
			armed = true;
			continue;
		}
		{
			struct pollfd pollers[2] = {{client->channel->fd, POLLIN, 0}, {fd, POLLIN, 0}};

			CHECK_FOR_INTERRUPTS();
			if (ElapsedMs(start) > timeoutMs)
				return false;
			if (poll(pollers, 2, 10) < 0 && errno != EINTR)
				return false;
			if (pollers[0].revents & POLLIN)
			{
				struct ibv_cq *eventCq;
				void	   *ignored;

				if (ibv_get_cq_event(client->channel, &eventCq, &ignored) == 0)
					ibv_ack_cq_events(eventCq, 1);
				armed = false;
				INSTR_TIME_SET_CURRENT(spinStart);
				continue;
			}

			/* A reply that did not fit an RDMA send arrives on TCP instead. */
			if (pollers[1].revents & POLLIN)
			{
				char		header[ANN_HEADER_SIZE];
				uint64		replyRequest;
				uint32		size;

				if (!RecvAll(fd, header, sizeof(header), start, timeoutMs) ||
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
				return ReapRdmaSend(client);
			}
		}
	}
}
#endif

static void
CloseRemote(void)
{
#ifdef ANN_USE_RDMA
	CloseRdma();
#endif
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
					{
						remoteNode = node;
#ifdef ANN_USE_RDMA
						if (ann_worker_rdma && ann_worker_rdma[0] != '\0')
							OpenRdma(remoteFd, timeoutMs);
#endif
					}
					else
						MarkDown(node);
				}
			if (remoteFd < 0)
				return false;
		}
		remoteBusy = true;
		if (
#ifdef ANN_USE_RDMA
			(rdmaClient && RdmaOperation(operation) && ANN_HEADER_SIZE + length <= rdmaClient->frame) ?
			RdmaExchange(remoteFd, operation, payload, length, timeoutMs, status, reply, replyLength) :
#endif
			Exchange(remoteFd, operation, payload, length, timeoutMs, status, reply, replyLength))
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
	DefineCustomStringVariable("ann.worker_rdma",
							   "InfiniBand/RoCE device[:port] for RDMA search frames to GPU nodes.",
							   "Empty uses TCP only. Requires a build with USE_RDMA=1.",
							   &ann_worker_rdma, "", PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("ann.worker_rdma_spin_us",
							"Time a backend busy-polls for an RDMA reply before sleeping.",
							"After this the backend waits on a completion channel and its TCP socket.",
							&ann_worker_rdma_spin_us, 50, 0, 1000000, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("ann.gpu_min_eligible",
							"Filters with fewer eligible rows are scored exactly on this host.",
							"Filtered loaded-index searches below this count never leave the host.",
							&ann_gpu_min_eligible, 1000, 0, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
}
