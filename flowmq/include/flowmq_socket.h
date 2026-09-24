#ifndef FLOWMQ_SOCKET_H
#define FLOWMQ_SOCKET_H

#include "flowmq_export.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Classic socket type values intentionally match libzmq. */
typedef enum flowmq_socket_type_e {
  FLOWMQ_PAIR = 0,
  FLOWMQ_PUB = 1,
  FLOWMQ_SUB = 2,
  FLOWMQ_REQ = 3,
  FLOWMQ_REP = 4,
  FLOWMQ_DEALER = 5,
  FLOWMQ_ROUTER = 6,
  FLOWMQ_PULL = 7,
  FLOWMQ_PUSH = 8,
  FLOWMQ_XPUB = 9,
  FLOWMQ_XSUB = 10
} flowmq_socket_type_t;

enum {
  FLOWMQ_DONTWAIT = 1,
  FLOWMQ_SNDMORE = 2,
  FLOWMQ_POLLIN = 1,
  FLOWMQ_POLLOUT = 2,
  FLOWMQ_POLLERR = 4
};

typedef enum flowmq_socket_option_e {
  FLOWMQ_IDENTITY = 5,
  FLOWMQ_SUBSCRIBE = 6,
  FLOWMQ_UNSUBSCRIBE = 7,
  FLOWMQ_RCVMORE = 13,
  FLOWMQ_RECONNECT_IVL = 18,
  FLOWMQ_RECONNECT_IVL_MAX = 21,
  FLOWMQ_SNDHWM = 23,
  FLOWMQ_RCVHWM = 24,
  FLOWMQ_HEARTBEAT_IVL = 75,
  FLOWMQ_HEARTBEAT_TIMEOUT = 77,
  FLOWMQ_TLS_CA_FILE = 1001,
  FLOWMQ_TLS_CERT_FILE = 1002,
  FLOWMQ_TLS_KEY_FILE = 1003,
  FLOWMQ_TLS_KEY_PASSWORD = 1004,
  FLOWMQ_TLS_SERVER_NAME = 1005,
  FLOWMQ_TLS_REQUIRE_CLIENT_CERTIFICATE = 1006,
  FLOWMQ_TLS_IDENTITY_POLICY = 1007,
  FLOWMQ_TLS_IDENTITY_REJECTIONS = 1008,
  FLOWMQ_SNDHWM_BYTES = 1101,
  FLOWMQ_RCVHWM_BYTES = 1102,
  FLOWMQ_FLOW_UPDATE_QUANTUM = 1103,
  FLOWMQ_FLOW_UPDATE_IVL = 1104
} flowmq_socket_option_t;

typedef struct flowmq_ctx_s flowmq_ctx_t;
typedef struct flowmq_socket_s flowmq_socket_t;

typedef struct flowmq_pollitem_s {
  flowmq_socket_t *socket;
  short events;
  short revents;
} flowmq_pollitem_t;

/**
 * Read-only status for one current ROUTER peer session.
 *
 * outstanding_* counts application DATA accepted by FlowMQ but not yet
 * acknowledged by CNet send completion. It includes direct in-flight DATA and
 * retained per-peer outbound DATA. send_credit_bytes is the currently usable
 * remote DATA credit; it does not expose FMQ cumulative wire counters.
 *
 * All counters are scoped to the current live peer session. Reconnect with the
 * same routing identity creates a new session with fresh counters.
 */
typedef struct flowmq_router_peer_status_s {
  size_t size;
  uint64_t admitted_messages;
  uint64_t admitted_bytes;
  uint64_t completed_messages;
  uint64_t completed_bytes;
  uint64_t rejected_messages;
  uint64_t rejected_bytes;
  uint64_t send_credit_bytes;
  size_t outstanding_messages;
  size_t outstanding_bytes;
  size_t peak_outstanding_messages;
  size_t peak_outstanding_bytes;
  int connected;
  int ready;
} flowmq_router_peer_status_t;

#define FLOWMQ_ROUTER_PEER_STATUS_INIT \
  { sizeof(flowmq_router_peer_status_t) }

/** Create a context. The context owns no progress thread. */
FLOWMQ_C_API flowmq_ctx_t *flowmq_ctx_new(void);

/** Destroy an empty context, or return SALTS_EBUSY while sockets remain. */
FLOWMQ_C_API int flowmq_ctx_term(flowmq_ctx_t *ctx);

/** Create one caller-owned classic socket. */
FLOWMQ_C_API flowmq_socket_t *flowmq_socket(flowmq_ctx_t *ctx, int type);

/** Close and release one socket on its owner thread. */
FLOWMQ_C_API int flowmq_close(flowmq_socket_t *socket);

/** Bind one TCP/TLS endpoint. Port zero selects an ephemeral listener port. */
FLOWMQ_C_API int flowmq_bind(flowmq_socket_t *socket, const char *endpoint);

/**
 * Admit one asynchronous TCP/TLS connection. A terminal connection state is
 * retried according to FLOWMQ_RECONNECT_IVL while the owner continues to call
 * flowmq_send(), flowmq_recv(), or flowmq_poll().
 */
FLOWMQ_C_API int flowmq_connect(flowmq_socket_t *socket, const char *endpoint);

/** Copy the most recently bound or connected endpoint including its trailing NUL. */
FLOWMQ_C_API int flowmq_last_endpoint(const flowmq_socket_t *socket, char *buffer,
                                      size_t capacity, size_t *size);

/**
 * Set a socket option. HWM message, heartbeat, and reconnect options use `int`;
 * HWM byte options and FLOWMQ_FLOW_UPDATE_QUANTUM use `size_t`;
 * FLOWMQ_FLOW_UPDATE_IVL uses positive integer milliseconds. HWM values must
 * be positive. Heartbeat values are milliseconds and must be nonnegative.
 * FLOWMQ_HEARTBEAT_IVL defaults to
 * zero (disabled); once enabled, FLOWMQ_HEARTBEAT_TIMEOUT defaults to the
 * interval unless explicitly set, and zero disables local timeout detection.
 * FLOWMQ_RECONNECT_IVL defaults to 100 ms; -1 disables reconnect and zero
 * schedules the next attempt without an interval. FLOWMQ_RECONNECT_IVL_MAX
 * defaults to zero (fixed interval); a value at least as large as IVL enables
 * bounded exponential backoff, while a smaller positive value is ignored.
 * Reconnect intervals may be randomized to avoid synchronized retries.
 * FLOWMQ_TLS_IDENTITY_POLICY takes a complete
 * `flowmq_tls_identity_map_config_t` from `flowmq_tls_identity_map.h`. It is
 * valid only on a ROUTER before runtime initialization; FlowMQ validates and
 * copies the immutable policy synchronously. A socket with this policy may
 * bind only a TLS listener configured to require client certificates.
 * FLOW_UPDATE quantum defaults to one quarter of receive byte HWM with a
 * bounded 64 KiB floor, and its interval defaults to 10 ms. These options are
 * fixed before bind/connect. Reconnect, heartbeat, and flow-credit progress are
 * caller-driven by flowmq_send(), flowmq_recv(), or flowmq_poll().
 */
FLOWMQ_C_API int flowmq_setsockopt(flowmq_socket_t *socket, int option,
                                   const void *value, size_t size);

/**
 * Read a socket option into caller storage. FLOWMQ_RECONNECT_IVL and
 * FLOWMQ_RECONNECT_IVL_MAX return their configured `int` millisecond values.
 * FLOWMQ_RCVMORE returns an `int` describing whether another part follows the
 * most recently received part. FLOWMQ_TLS_IDENTITY_REJECTIONS returns the
 * saturating `uint64_t` count of TLS peers rejected because their verified
 * certificate and claimed HELLO identity were not authorized. `size` is both
 * input capacity and output size.
 */
FLOWMQ_C_API int flowmq_getsockopt(const flowmq_socket_t *socket, int option,
                                   void *value, size_t *size);

/**
 * Snapshot one ROUTER peer by its current live routing identity without
 * driving transport progress.
 *
 * The caller must initialize status->size to at least
 * sizeof(flowmq_router_peer_status_t), normally with
 * FLOWMQ_ROUTER_PEER_STATUS_INIT. A larger caller struct is accepted and only
 * the canonical current prefix is written.
 *
 * @return SALTS_OK on success; SALTS_ENOTSUP for non-ROUTER sockets;
 * SALTS_ENOENT when no current non-retired peer has the identity; or
 * SALTS_EINVAL for malformed arguments/status size.
 */
FLOWMQ_C_API int flowmq_router_peer_status(
    const flowmq_socket_t *socket, const void *identity, size_t identity_size,
    flowmq_router_peer_status_t *status);

/**
 * Copy one message part into the socket data path. A FLOWMQ_SNDMORE success
 * retains the part in bounded socket-owned staging; the final part admits the
 * complete multipart message to its selected peer queue atomically. Success
 * never means remote receipt. Without FLOWMQ_DONTWAIT, a runtime-backed
 * would-block condition advances this socket on the calling thread until the
 * message can be admitted or progress fails. If the peer bound to an active
 * transaction disconnects, the next affected send returns SALTS_ENOTCONN once
 * instead of retrying the cancelled transaction. An operation forbidden by
 * the pattern or multipart FSM returns SALTS_EPROTO without blocking. Returns
 * a Turbo status.
 */
FLOWMQ_C_API int flowmq_send(flowmq_socket_t *socket, const void *data,
                             size_t size, int flags);

/**
 * Copy one available message part into caller storage. Without
 * FLOWMQ_DONTWAIT, advance this socket on the calling thread until a part is
 * available or progress fails. If the peer bound to an active transaction
 * disconnects, the next affected receive returns SALTS_ENOTCONN once instead
 * of retrying the cancelled transaction. An operation forbidden by the
 * pattern or multipart FSM returns SALTS_EPROTO without blocking. Returns a
 * Turbo status.
 */
FLOWMQ_C_API int flowmq_recv(flowmq_socket_t *socket, void *data,
                             size_t capacity, size_t *received, int flags);

/**
 * Drive all listed sockets on the calling thread until an item is ready or the
 * timeout expires, then report the level-triggered ready items. A pending
 * transaction-cancellation error is reported as FLOWMQ_POLLERR until the
 * affected send or receive consumes it.
 */
FLOWMQ_C_API int flowmq_poll(flowmq_pollitem_t *items, size_t item_count,
                             uint32_t timeout_ms, size_t *ready);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_SOCKET_H */
