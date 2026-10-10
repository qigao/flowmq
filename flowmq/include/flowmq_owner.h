#ifndef FLOWMQ_OWNER_H
#define FLOWMQ_OWNER_H

#include "flowmq_socket.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_OWNER_DEFAULT_SOCKET_CAPACITY 8u
#define FLOWMQ_OWNER_MAX_SOCKET_CAPACITY 64u

typedef struct flowmq_owner_s flowmq_owner_t;

typedef struct flowmq_owner_config_s {
  size_t size;
  size_t socket_capacity;
} flowmq_owner_config_t;

#define FLOWMQ_OWNER_CONFIG_INIT \
  { sizeof(flowmq_owner_config_t), 0u }

/**
 * Creates one explicit caller-owned progress lane.
 *
 * The owner creates no worker thread. All sockets created through this owner
 * remain affine to the thread that drives flowmq_owner_poll().
 * Use one context per independent thread, or serialize all context lifecycle
 * operations as required by flowmq_ctx_new().
 *
 * socket_capacity is a hard startup bound. Zero selects
 * FLOWMQ_OWNER_DEFAULT_SOCKET_CAPACITY.
 */
FLOWMQ_C_API flowmq_owner_t *
flowmq_owner_new(flowmq_ctx_t *ctx, const flowmq_owner_config_t *config);

/**
 * Creates one socket permanently attached to this owner lane.
 *
 * The returned socket uses the ordinary flowmq_send/recv APIs but its network
 * progress is driven by flowmq_owner_poll(), not flowmq_poll(). Listener bind
 * is intentionally not part of the first owner-lane contract.
 * Use FLOWMQ_DONTWAIT for send/recv (including slice APIs), and owner_poll to
 * wait for progress before retrying busy/full operations. Without DONTWAIT an
 * immediately satisfiable operation may succeed, but an initialized owner
 * socket returns SALTS_ENOTSUP when the call needs ordinary blocking progress.
 */
FLOWMQ_C_API flowmq_socket_t *flowmq_owner_socket(flowmq_owner_t *owner,
                                                  int type);

/**
 * Drives all live sockets in this owner lane through one shared NativeIO wait
 * and reports readiness for the supplied owner-local items.
 *
 * The item sockets must all belong to owner. Completions for other sockets in
 * the same owner lane are still progressed and routed even when those sockets
 * are not present in items.
 *
 * items must be nonempty; ready must be non-NULL. Zero timeout performs one
 * nonblocking progress pass. A positive timeout waits until requested readiness
 * (or a socket error), the caller deadline, or a progress error; internal
 * transport/control deadlines can shorten individual backend waits. Success
 * with *ready == 0 means no requested readiness was reported.
 *
 * To avoid idle spinning, request POLLIN on receive sockets and POLLOUT only
 * for pending sends. Always-writable items return immediately. With events=0,
 * ordinary data readiness cannot end a positive-timeout call early. Polling
 * does not provide a cross-thread application-command wakeup contract.
 */
FLOWMQ_C_API int flowmq_owner_poll(flowmq_owner_t *owner,
                                   flowmq_pollitem_t *items,
                                   size_t item_count,
                                   uint32_t timeout_ms,
                                   size_t *ready);

/**
 * Closes one socket owned by this lane.
 *
 * External CNet shutdown is drained through the shared owner wait before the
 * socket storage is released. Passing a socket from another owner is rejected.
 */
FLOWMQ_C_API int flowmq_owner_close_socket(flowmq_owner_t *owner,
                                           flowmq_socket_t *socket);

/**
 * Destroys an empty owner lane.
 *
 * Returns SALTS_EBUSY while owner-created sockets remain. The shared backend is
 * closed and destroyed only after the last socket has been closed.
 */
FLOWMQ_C_API int flowmq_owner_term(flowmq_owner_t *owner);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_OWNER_H */
