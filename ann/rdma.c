/* posix_memalign, nanosleep, and random under -std=c11 */
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include "rdma.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef ANN_USE_RDMA

AnnRdma *
AnnRdmaOpen(const char *device, int lanes, int spin, AnnRdmaFrame onFrame, void *context,
			char *error, size_t errorSize)
{
	(void) device;
	(void) lanes;
	(void) spin;
	(void) onFrame;
	(void) context;
	snprintf(error, errorSize, "built without RDMA support (make USE_RDMA=1)");
	return NULL;
}

void
AnnRdmaClose(AnnRdma *rdma)
{
	(void) rdma;
}

void
AnnRdmaStop(AnnRdma *rdma)
{
	(void) rdma;
}

const char *
AnnRdmaDevice(AnnRdma *rdma)
{
	(void) rdma;
	return "";
}

size_t
AnnRdmaMaxFrame(AnnRdma *rdma)
{
	(void) rdma;
	return 0;
}

void
AnnRdmaStats(AnnRdma *rdma, uint64_t *frames, uint64_t *fallbacks, int *lanes)
{
	(void) rdma;
	*frames = 0;
	*fallbacks = 0;
	*lanes = 0;
}

int
AnnRdmaAddPeer(AnnRdma *rdma, const unsigned char *client, unsigned char *local, uint64_t *peer)
{
	(void) rdma;
	(void) client;
	(void) local;
	(void) peer;
	return 0;
}

void
AnnRdmaRemovePeer(AnnRdma *rdma, uint64_t peer)
{
	(void) rdma;
	(void) peer;
}

int
AnnRdmaSend(AnnRdma *rdma, uint64_t peer, const unsigned char *header, size_t headerSize,
			const unsigned char *payload, size_t payloadSize)
{
	(void) rdma;
	(void) peer;
	(void) header;
	(void) headerSize;
	(void) payload;
	(void) payloadSize;
	return 0;
}

#else

#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <time.h>

#include <infiniband/verbs.h>

#define ANN_RDMA_FRAME (256 * 1024)
#define ANN_RDMA_LANE_RX 256	/* shared receive queue depth per lane */
#define ANN_RDMA_LANE_TX 256	/* send buffers per lane */
#define ANN_RDMA_QP_SEND 64		/* outstanding sends per queue pair */
#define ANN_RDMA_MAX_LANES 32

typedef struct Lane Lane;

typedef struct Buffer
{
	unsigned char *data;
	Lane	   *lane;
	int			receive;
	struct Buffer *next;		/* free list for send buffers */
} Buffer;

typedef struct Peer
{
	struct ibv_qp *qp;
	uint32_t	qpn;
	Lane	   *lane;
} Peer;

/*
 * A lane is one completion queue, one shared receive queue, its own buffers,
 * and one progress thread. Clients are spread across lanes at hello, so
 * receive processing and send-buffer recycling scale with lanes instead of
 * funnelling through one thread.
 */
struct Lane
{
	AnnRdma    *rdma;
	struct ibv_comp_channel *channel;
	struct ibv_cq *cq;
	struct ibv_srq *srq;
	Buffer		buffers[ANN_RDMA_LANE_RX + ANN_RDMA_LANE_TX];
	Buffer	   *free;
	pthread_mutex_t mutex;		/* the free list */
	pthread_t	progress;
	int			started;
};

struct AnnRdma
{
	char		device[64];
	uint8_t		port;
	struct ibv_context *context;
	struct ibv_port_attr portAttr;
	union ibv_gid gid;
	struct ibv_pd *pd;
	struct ibv_mr *mr;
	unsigned char *region;
	Lane		lanes[ANN_RDMA_MAX_LANES];
	int			nlanes;
	pthread_mutex_t mutex;		/* peers, the queue pair map, lane choice */
	Peer	   *peers;			/* by peer id; qp NULL when unused */
	uint64_t	npeers;
	uint64_t   *byQpn;			/* open-addressing map: qpn to peer id + 1 */
	uint64_t	mapSize;
	uint64_t	nextLane;
	atomic_ullong frames;
	atomic_ullong fallbacks;
	volatile int running;
	int			spin;
	AnnRdmaFrame onFrame;
	void	   *callback;
};

static uint64_t
Slot(AnnRdma *rdma, uint32_t qpn)
{
	return ((uint64_t) qpn * 0x9E3779B97F4A7C15ull) & (rdma->mapSize - 1);
}

/* Called with mutex held. Lookup, insert (peer + 1), or remove (0). */
static uint64_t
MapFind(AnnRdma *rdma, uint32_t qpn)
{
	for (uint64_t i = Slot(rdma, qpn), n = 0; n < rdma->mapSize; i = (i + 1) & (rdma->mapSize - 1), n++)
	{
		uint64_t	entry = rdma->byQpn[i];

		if (entry == 0)
			return 0;
		if (entry != UINT64_MAX && rdma->peers[entry - 1].qpn == qpn && rdma->peers[entry - 1].qp)
			return entry;
	}
	return 0;
}

static int
MapPut(AnnRdma *rdma, uint32_t qpn, uint64_t value)
{
	uint64_t	tomb = UINT64_MAX;

	for (uint64_t i = Slot(rdma, qpn), n = 0; n < rdma->mapSize; i = (i + 1) & (rdma->mapSize - 1), n++)
	{
		uint64_t	entry = rdma->byQpn[i];

		if (value == 0 && entry != 0 && entry != UINT64_MAX && rdma->peers[entry - 1].qpn == qpn)
		{
			rdma->byQpn[i] = UINT64_MAX;	/* tombstone keeps probe chains */
			return 1;
		}
		if (value && entry == UINT64_MAX && tomb == UINT64_MAX)
			tomb = i;
		if (value && entry == 0)
		{
			rdma->byQpn[tomb != UINT64_MAX ? tomb : i] = value;
			return 1;
		}
	}
	if (value && tomb != UINT64_MAX)
	{
		rdma->byQpn[tomb] = value;
		return 1;
	}
	return 0;
}

static int
PostReceive(Lane *lane, Buffer *buffer)
{
	struct ibv_sge sge = {(uintptr_t) buffer->data, ANN_RDMA_FRAME, lane->rdma->mr->lkey};
	struct ibv_recv_wr wr = {0};
	struct ibv_recv_wr *bad;

	wr.wr_id = (uintptr_t) buffer;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	return ibv_post_srq_recv(lane->srq, &wr, &bad) == 0;
}

static void
ReturnSend(Lane *lane, Buffer *buffer)
{
	(void) pthread_mutex_lock(&lane->mutex);
	buffer->next = lane->free;
	lane->free = buffer;
	(void) pthread_mutex_unlock(&lane->mutex);
}

static void
Complete(Lane *lane, const struct ibv_wc *wc)
{
	AnnRdma    *rdma = lane->rdma;
	Buffer	   *buffer = (Buffer *) (uintptr_t) wc->wr_id;
	uint64_t	peer = 0;

	if (!buffer->receive)
	{
		ReturnSend(lane, buffer);
		return;
	}
	if (wc->status == IBV_WC_SUCCESS && wc->byte_len > 0)
	{
		(void) pthread_mutex_lock(&rdma->mutex);
		peer = MapFind(rdma, wc->qp_num);
		(void) pthread_mutex_unlock(&rdma->mutex);
	}
	if (peer)
	{
		unsigned char *frame = malloc(wc->byte_len);

		if (frame)
		{
			memcpy(frame, buffer->data, wc->byte_len);
			(void) PostReceive(lane, buffer);
			atomic_fetch_add(&rdma->frames, 1);
			rdma->onFrame(rdma->callback, peer - 1, frame, wc->byte_len);
			return;
		}
	}
	/* Errors (such as flushes from a closed queue pair) just recycle. */
	(void) PostReceive(lane, buffer);
}

/*
 * A lane's progress thread reaps its completions. Spinning keeps the data
 * path free of system calls and interrupts; waiting arms the completion
 * channel and sleeps until the adapter raises one.
 */
static void *
Progress(void *argument)
{
	Lane	   *lane = argument;
	AnnRdma    *rdma = lane->rdma;
	struct ibv_wc wcs[64];
	unsigned	idle = 0;

	while (rdma->running)
	{
		int			count = ibv_poll_cq(lane->cq, 64, wcs);

		if (count > 0)
		{
			idle = 0;
			for (int i = 0; i < count; i++)
				Complete(lane, &wcs[i]);
			continue;
		}
		if (rdma->spin)
		{
			if (++idle > 4096)
			{
				/* Long idle spins yield the core without sleeping. */
				idle = 0;
				(void) sched_yield();
			}
			continue;
		}
		if (ibv_req_notify_cq(lane->cq, 0) == 0)
		{
			struct pollfd poller = {lane->channel->fd, POLLIN, 0};
			struct ibv_cq *cq;
			void	   *ignored;

			/* Recheck after arming: a completion may have raced the arm. */
			if ((count = ibv_poll_cq(lane->cq, 64, wcs)) > 0)
			{
				for (int i = 0; i < count; i++)
					Complete(lane, &wcs[i]);
				continue;
			}
			if (poll(&poller, 1, 100) > 0 && ibv_get_cq_event(lane->channel, &cq, &ignored) == 0)
				ibv_ack_cq_events(cq, 1);
		}
	}
	return NULL;
}

void
AnnRdmaStop(AnnRdma *rdma)
{
	rdma->running = 0;
	for (int l = 0; l < rdma->nlanes; l++)
		if (rdma->lanes[l].started)
		{
			(void) pthread_join(rdma->lanes[l].progress, NULL);
			rdma->lanes[l].started = 0;
		}
}

void
AnnRdmaClose(AnnRdma *rdma)
{
	if (rdma == NULL)
		return;
	AnnRdmaStop(rdma);
	for (uint64_t i = 0; i < rdma->npeers; i++)
		if (rdma->peers[i].qp)
			(void) ibv_destroy_qp(rdma->peers[i].qp);
	for (int l = 0; l < rdma->nlanes; l++)
	{
		Lane	   *lane = &rdma->lanes[l];

		if (lane->srq)
			(void) ibv_destroy_srq(lane->srq);
		if (lane->cq)
			(void) ibv_destroy_cq(lane->cq);
		if (lane->channel)
			(void) ibv_destroy_comp_channel(lane->channel);
		(void) pthread_mutex_destroy(&lane->mutex);
	}
	if (rdma->mr)
		(void) ibv_dereg_mr(rdma->mr);
	if (rdma->pd)
		(void) ibv_dealloc_pd(rdma->pd);
	if (rdma->context)
		(void) ibv_close_device(rdma->context);
	free(rdma->region);
	free(rdma->peers);
	free(rdma->byQpn);
	(void) pthread_mutex_destroy(&rdma->mutex);
	free(rdma);
}

AnnRdma *
AnnRdmaOpen(const char *device, int lanes, int spin, AnnRdmaFrame onFrame, void *context,
			char *error, size_t errorSize)
{
	AnnRdma    *rdma = calloc(1, sizeof(*rdma));
	struct ibv_device **list = NULL;
	const char *colon;
	const char *step = "allocate";
	size_t		perLane = ANN_RDMA_LANE_RX + ANN_RDMA_LANE_TX;
	int			devices = 0;

	if (rdma == NULL)
		goto fail;
	(void) pthread_mutex_init(&rdma->mutex, NULL);
	rdma->nlanes = lanes < 1 ? 1 : lanes > ANN_RDMA_MAX_LANES ? ANN_RDMA_MAX_LANES : lanes;
	rdma->spin = spin;
	rdma->onFrame = onFrame;
	rdma->callback = context;
	colon = strchr(device, ':');
	snprintf(rdma->device, sizeof(rdma->device), "%.*s",
			 (int) (colon ? (size_t) (colon - device) : strlen(device)), device);
	rdma->port = colon ? (uint8_t) atoi(colon + 1) : 1;
	step = "find device";
	list = ibv_get_device_list(&devices);
	for (int i = 0; list && i < devices && rdma->context == NULL; i++)
		if (strcmp(ibv_get_device_name(list[i]), rdma->device) == 0)
			rdma->context = ibv_open_device(list[i]);
	if (list)
		ibv_free_device_list(list);
	if (rdma->context == NULL)
		goto fail;
	step = "query port";
	if (ibv_query_port(rdma->context, rdma->port, &rdma->portAttr) != 0 ||
		rdma->portAttr.state != IBV_PORT_ACTIVE ||
		ibv_query_gid(rdma->context, rdma->port, 0, &rdma->gid) != 0)
		goto fail;
	step = "register buffers";
	if ((rdma->pd = ibv_alloc_pd(rdma->context)) == NULL)
		goto fail;

	/* One registration covers every lane's buffers. */
	if (posix_memalign((void **) &rdma->region, 4096, rdma->nlanes * perLane * ANN_RDMA_FRAME) != 0)
	{
		rdma->region = NULL;
		goto fail;
	}
	if ((rdma->mr = ibv_reg_mr(rdma->pd, rdma->region, rdma->nlanes * perLane * ANN_RDMA_FRAME,
							   IBV_ACCESS_LOCAL_WRITE)) == NULL)
		goto fail;
	rdma->mapSize = 65536;		/* up to 32768 client queue pairs */
	if ((rdma->byQpn = calloc(rdma->mapSize, sizeof(uint64_t))) == NULL)
		goto fail;
	rdma->running = 1;
	for (int l = 0; l < rdma->nlanes; l++)
	{
		Lane	   *lane = &rdma->lanes[l];
		struct ibv_srq_init_attr srqAttr = {0};

		lane->rdma = rdma;
		(void) pthread_mutex_init(&lane->mutex, NULL);
		step = "allocate lane queues";
		if ((!spin && (lane->channel = ibv_create_comp_channel(rdma->context)) == NULL) ||
			(lane->cq = ibv_create_cq(rdma->context, 16384, NULL, lane->channel, 0)) == NULL)
			goto fail;
		srqAttr.attr.max_wr = ANN_RDMA_LANE_RX;
		srqAttr.attr.max_sge = 1;
		if ((lane->srq = ibv_create_srq(rdma->pd, &srqAttr)) == NULL)
			goto fail;
		step = "post receives";
		for (size_t i = 0; i < perLane; i++)
		{
			Buffer	   *buffer = &lane->buffers[i];

			buffer->data = rdma->region + ((size_t) l * perLane + i) * ANN_RDMA_FRAME;
			buffer->lane = lane;
			buffer->receive = i < ANN_RDMA_LANE_RX;
			if (buffer->receive)
			{
				if (!PostReceive(lane, buffer))
					goto fail;
			}
			else
				ReturnSend(lane, buffer);
		}
		step = "start progress thread";
		if (pthread_create(&lane->progress, NULL, Progress, lane) != 0)
			goto fail;
		lane->started = 1;
	}
	return rdma;

fail:
	snprintf(error, errorSize, "RDMA device %s: %s failed%s", device, step,
			 rdma && rdma->context && rdma->portAttr.state != IBV_PORT_ACTIVE && strcmp(step, "query port") == 0 ?
			 " (port not active)" : "");
	AnnRdmaClose(rdma);
	return NULL;
}

const char *
AnnRdmaDevice(AnnRdma *rdma)
{
	return rdma->device;
}

size_t
AnnRdmaMaxFrame(AnnRdma *rdma)
{
	(void) rdma;
	return ANN_RDMA_FRAME;
}

void
AnnRdmaStats(AnnRdma *rdma, uint64_t *frames, uint64_t *fallbacks, int *lanes)
{
	*frames = atomic_load(&rdma->frames);
	*fallbacks = atomic_load(&rdma->fallbacks);
	*lanes = rdma->nlanes;
}

static void
PutAddress(unsigned char *out, uint16_t lid, uint16_t mtu, uint32_t qpn, uint32_t psn, const union ibv_gid *gid)
{
	memset(out, 0, ANN_RDMA_ADDRESS);
	memcpy(out, &lid, 2);
	memcpy(out + 2, &mtu, 2);
	memcpy(out + 4, &qpn, 4);
	memcpy(out + 8, &psn, 4);
	memcpy(out + 12, gid->raw, 16);
}

/*
 * A reliable connected queue pair on the next lane, taken through INIT, RTR
 * (with the client's address), and RTS. Receives come from the lane's
 * shared queue.
 */
int
AnnRdmaAddPeer(AnnRdma *rdma, const unsigned char *client, unsigned char *local, uint64_t *peer)
{
	struct ibv_qp_init_attr init = {0};
	struct ibv_qp_attr attr = {0};
	struct ibv_qp *qp;
	Lane	   *lane;
	uint16_t	lid,
				mtu,
				pathMtu;
	uint32_t	qpn,
				psn,
				localPsn = (uint32_t) random() & 0xffffff;
	union ibv_gid gid;
	uint64_t	id;

	memcpy(&lid, client, 2);
	memcpy(&mtu, client + 2, 2);
	memcpy(&qpn, client + 4, 4);
	memcpy(&psn, client + 8, 4);
	memcpy(gid.raw, client + 12, 16);
	pathMtu = mtu < rdma->portAttr.active_mtu ? mtu : (uint16_t) rdma->portAttr.active_mtu;
	(void) pthread_mutex_lock(&rdma->mutex);
	lane = &rdma->lanes[rdma->nextLane++ % (uint64_t) rdma->nlanes];
	(void) pthread_mutex_unlock(&rdma->mutex);
	init.send_cq = lane->cq;
	init.recv_cq = lane->cq;
	init.srq = lane->srq;
	init.cap.max_send_wr = ANN_RDMA_QP_SEND;
	init.cap.max_send_sge = 1;
	init.qp_type = IBV_QPT_RC;
	if ((qp = ibv_create_qp(rdma->pd, &init)) == NULL)
		return 0;
	attr.qp_state = IBV_QPS_INIT;
	attr.port_num = rdma->port;
	if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) != 0)
		goto fail;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = (enum ibv_mtu) pathMtu;
	attr.dest_qp_num = qpn;
	attr.rq_psn = psn;
	attr.max_dest_rd_atomic = 1;
	attr.min_rnr_timer = 12;
	attr.ah_attr.dlid = lid;
	attr.ah_attr.port_num = rdma->port;
	if (rdma->portAttr.link_layer == IBV_LINK_LAYER_ETHERNET)
	{
		/* RoCE routes by GID. */
		attr.ah_attr.is_global = 1;
		attr.ah_attr.grh.dgid = gid;
		attr.ah_attr.grh.hop_limit = 1;
	}
	if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
					  IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER) != 0)
		goto fail;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 14;
	attr.retry_cnt = 7;
	attr.rnr_retry = 7;			/* retry forever while receivers repost */
	attr.sq_psn = localPsn;
	attr.max_rd_atomic = 1;
	if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
					  IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC) != 0)
		goto fail;
	(void) pthread_mutex_lock(&rdma->mutex);
	for (id = 0; id < rdma->npeers && rdma->peers[id].qp; id++)
		;
	if (id == rdma->npeers)
	{
		uint64_t	n = rdma->npeers ? rdma->npeers * 2 : 64;
		Peer	   *peers = realloc(rdma->peers, sizeof(Peer) * n);

		if (peers == NULL || n * 2 > rdma->mapSize)
		{
			if (peers)
				rdma->peers = peers;
			(void) pthread_mutex_unlock(&rdma->mutex);
			goto fail;
		}
		memset(peers + rdma->npeers, 0, sizeof(Peer) * (n - rdma->npeers));
		rdma->peers = peers;
		rdma->npeers = n;
	}
	rdma->peers[id].qp = qp;
	rdma->peers[id].qpn = qp->qp_num;
	rdma->peers[id].lane = lane;
	(void) MapPut(rdma, qp->qp_num, id + 1);
	(void) pthread_mutex_unlock(&rdma->mutex);
	PutAddress(local, rdma->portAttr.lid, pathMtu, qp->qp_num, localPsn, &rdma->gid);
	*peer = id;
	return 1;

fail:
	(void) ibv_destroy_qp(qp);
	return 0;
}

void
AnnRdmaRemovePeer(AnnRdma *rdma, uint64_t peer)
{
	struct ibv_qp *qp = NULL;

	(void) pthread_mutex_lock(&rdma->mutex);
	if (peer < rdma->npeers && rdma->peers[peer].qp)
	{
		qp = rdma->peers[peer].qp;
		(void) MapPut(rdma, rdma->peers[peer].qpn, 0);
		rdma->peers[peer].qp = NULL;
	}
	(void) pthread_mutex_unlock(&rdma->mutex);

	/*
	 * Callers remove a peer only once nothing can still send to it.
	 * Outstanding sends complete with flush errors and return their buffers.
	 */
	if (qp)
		(void) ibv_destroy_qp(qp);
}

/*
 * Never blocks: with no free send buffer on the peer's lane it returns 0 and
 * the caller replies over TCP instead. Waiting here could deadlock, since a
 * lane's progress thread both frees send buffers and handles requests.
 */
int
AnnRdmaSend(AnnRdma *rdma, uint64_t peer, const unsigned char *header, size_t headerSize,
			const unsigned char *payload, size_t payloadSize)
{
	struct ibv_send_wr wr = {0};
	struct ibv_send_wr *bad;
	struct ibv_sge sge;
	struct ibv_qp *qp = NULL;
	Lane	   *lane = NULL;
	Buffer	   *buffer = NULL;

	if (headerSize + payloadSize > ANN_RDMA_FRAME)
		return 0;
	(void) pthread_mutex_lock(&rdma->mutex);
	if (peer < rdma->npeers)
	{
		qp = rdma->peers[peer].qp;
		lane = rdma->peers[peer].lane;
	}
	(void) pthread_mutex_unlock(&rdma->mutex);
	if (qp == NULL)
		return 0;
	(void) pthread_mutex_lock(&lane->mutex);
	buffer = lane->free;
	if (buffer)
		lane->free = buffer->next;
	(void) pthread_mutex_unlock(&lane->mutex);
	if (buffer == NULL)
	{
		atomic_fetch_add(&rdma->fallbacks, 1);
		return 0;
	}
	memcpy(buffer->data, header, headerSize);
	if (payloadSize)
		memcpy(buffer->data + headerSize, payload, payloadSize);
	sge.addr = (uintptr_t) buffer->data;
	sge.length = (uint32_t) (headerSize + payloadSize);
	sge.lkey = rdma->mr->lkey;
	wr.wr_id = (uintptr_t) buffer;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_SEND;
	wr.send_flags = IBV_SEND_SIGNALED;

	/* Peers are removed only after their last sender is done; see header. */
	if (ibv_post_send(qp, &wr, &bad) != 0)
	{
		ReturnSend(lane, buffer);
		atomic_fetch_add(&rdma->fallbacks, 1);
		return 0;
	}
	return 1;
}

#endif
