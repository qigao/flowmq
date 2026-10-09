#ifndef FLOWMQ_PEER_POOL_H
#define FLOWMQ_PEER_POOL_H

#include "flowmq_socket.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_PEER_POOL_VERSION 1u
#define FLOWMQ_PEER_POOL_MAX_PEERS 4u

typedef struct flowmq_peer_pool_config_s {
  size_t size;
  uint32_t version;
  size_t max_peers;
  size_t max_connecting;
} flowmq_peer_pool_config_t;

#define FLOWMQ_PEER_POOL_CONFIG_INIT \
  { sizeof(flowmq_peer_pool_config_t), FLOWMQ_PEER_POOL_VERSION, \
    FLOWMQ_PEER_POOL_MAX_PEERS, FLOWMQ_PEER_POOL_MAX_PEERS }

typedef struct flowmq_peer_pool_snapshot_s {
  size_t size;
  size_t max_peers;
  size_t max_connecting;
  size_t connecting;
  size_t ready;
  size_t draining;
  size_t terminal_waiting_for_leases;
  size_t physical_in_use;
  size_t active_leases;
  int enabled;
  int sealed;
  int drained;
} flowmq_peer_pool_snapshot_t;

#define FLOWMQ_PEER_POOL_SNAPSHOT_INIT \
  { sizeof(flowmq_peer_pool_snapshot_t), 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0, 0, 1 }

/* Opt in before the first bind/connect on the socket's progress owner.
 * Copies config; no borrowed configuration storage survives this call.
 * One long-lived, generation-specific lease per fully READY peer, never a
 * per-message acquisition. No cross-socket sharing, queue, prewarm or retry.
 * Existing send/receive HWM remain authoritative byte/message limits.
 * EINVAL: NULL, invalid ABI, or not 1 <= max_connecting <= max_peers <= 4.
 * EBUSY: socket runtime has already started. Default is disabled.
 * Example: flowmq_peer_pool_config_t c = FLOWMQ_PEER_POOL_CONFIG_INIT;
 *          c.max_peers = c.max_connecting = 1;
 *          flowmq_socket_set_peer_pool(socket, &c); */
FLOWMQ_C_API int flowmq_socket_set_peer_pool(
    flowmq_socket_t *socket, const flowmq_peer_pool_config_t *config);

/* Synchronous owner-only copy; no progress or allocation. EINVAL for NULL or
 * incorrect output size. Disabled/unstarted pools have no live records.
 * Counts come from CNet: draining excludes ready but can overlap connecting; terminal
 * records with queued inbound messages keep their physical slot and lease.
 * Success does not imply remote application receipt or authorization. */
FLOWMQ_C_API int flowmq_socket_get_peer_pool(
    flowmq_socket_t *socket, flowmq_peer_pool_snapshot_t *snapshot);

#ifdef __cplusplus
}
#endif
#endif /* FLOWMQ_PEER_POOL_H */
