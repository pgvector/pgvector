#ifndef ANN_RDMA_H
#define ANN_RDMA_H

#include <stddef.h>
#include <stdint.h>

/*
 * RDMA message transport over InfiniBand verbs (rdma-core): one reliable
 * connected queue pair per client, a shared receive queue, and completions
 * polled from user space, so search frames bypass the kernel network stack
 * and its interrupts. RoCE adapters work through the port's GID. Queue pair
 * addresses are exchanged over the authenticated TCP connection, so no
 * IP-over-InfiniBand is needed. Built only with USE_RDMA=1.
 */
typedef struct AnnRdma AnnRdma;

/*
 * A queue pair address: u16 LID, u16 MTU enum, u32 queue pair number,
 * u32 starting packet sequence number, 16-byte GID, 4 bytes reserved.
 */
#define ANN_RDMA_ADDRESS 32

/* Receives one whole frame from a peer; the callee owns frame. */
typedef void (*AnnRdmaFrame) (void *context, uint64_t peer, unsigned char *frame, size_t size);

/*
 * Open "device" or "device:port" (port 1 by default) with lanes progress
 * threads, each owning a completion queue, a shared receive queue and send
 * buffers; clients are spread across lanes. spin busy-polls completion
 * queues; otherwise progress threads sleep on completion channels, which
 * costs an interrupt per wakeup.
 */
AnnRdma    *AnnRdmaOpen(const char *device, int lanes, int spin, AnnRdmaFrame onFrame, void *context,
						char *error, size_t errorSize);
void		AnnRdmaClose(AnnRdma *rdma);

/* Stop and join the progress threads, keeping every other resource. */
void		AnnRdmaStop(AnnRdma *rdma);
const char *AnnRdmaDevice(AnnRdma *rdma);
size_t		AnnRdmaMaxFrame(AnnRdma *rdma);

/* Frames received, replies that fell back to TCP, and the lane count. */
void		AnnRdmaStats(AnnRdma *rdma, uint64_t *frames, uint64_t *fallbacks, int *lanes);

/*
 * Connect a queue pair to the client's address; writes this side's address
 * for that queue pair to local. Returns 0 on failure.
 */
int			AnnRdmaAddPeer(AnnRdma *rdma, const unsigned char *client, unsigned char *local, uint64_t *peer);

/* Destroys the queue pair: call only when no thread can still send to peer. */
void		AnnRdmaRemovePeer(AnnRdma *rdma, uint64_t peer);

/*
 * Send header and payload as one frame; copies, so callers keep their
 * buffers. Never blocks: returns 0 when the peer's lane has no free send
 * buffer, and the caller should use TCP.
 */
int			AnnRdmaSend(AnnRdma *rdma, uint64_t peer, const unsigned char *header, size_t headerSize,
						const unsigned char *payload, size_t payloadSize);

#endif
