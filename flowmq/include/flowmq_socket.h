#ifndef FLOWMQ_SOCKET_H
#define FLOWMQ_SOCKET_H

#include "flowmq_export.h"
#include "salts_buffer.h"

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
  FLOWMQ_FLOW_UPDATE_IVL = 1104,
  FLOWMQ_REUSE_PORT = 1105
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
 * FLOWMQ_FLOW_UPDATE_IVL uses positive integer milliseconds. FLOWMQ_REUSE_PORT
 * uses int 0/1 and is consumed only when creating a listener. HWM values must
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
 * bounded 64 KiB floor, and its interval defaults to 10 ms. FLOWMQ_REUSE_PORT
 * defaults to 0; value 1 requests SO_REUSEPORT and fails bind with
 * SALTS_ENOTSUP when the released CNet/backend does not support it. These
 * options are fixed before bind/connect. Reconnect, heartbeat, and flow-credit progress are
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
 * Admit one canonical non-empty Salts Core slice through the plaintext TCP
 * immediate DATA lane without copying payload bytes.
 *
 * This is an explicit ownership-bearing alternative to flowmq_send(); it does
 * not change the borrowed-input/copy contract of that API. On SALTS_OK, CNet
 * has retained the slice backing and the caller may immediately
 * mem_slice_release() its own slice/reference. The admitted backing bytes and
 * buffer data/used/capacity must remain immutable until the ordinary FlowMQ
 * send completion settles.
 *
 * FLOWMQ_SNDMORE stages retained DATA ownership in a fixed-capacity socket
 * transaction. Every successful staged part may be released immediately by the
 * caller. The complete multipart message is admitted as exactly one CNet
 * logical retained vector when the final part arrives; the aggregate encoded
 * range count must fit CNET_RETAINED_VECTOR_MAX and the aggregate encoded bytes
 * must fit the configured CNet send bound. An over-bound final/staged shape
 * fails explicitly without flattening or copying and leaves already-staged
 * retained ownership intact for retry/cancel.
 *
 * Copy-staged and retained-staged DATA are not mixed in one multipart message.
 * ROUTER may still select its routing identity with
 * flowmq_send(..., FLOWMQ_SNDMORE) before retained DATA staging. Retained
 * multipart PUB/XPUB fanout is fail-closed with SALTS_ENOTSUP in this bounded
 * single-logical-terminal design. TLS also returns SALTS_ENOTSUP; there is no
 * retained-to-copy fallback.
 *
 * FLOWMQ_DONTWAIT has the same would-block meaning as flowmq_send(). Without
 * it, the owner thread advances this socket until immediate retained admission
 * succeeds or progress fails.
 *
 * This API is intended for payloads that are already in canonical owned
 * buffers, especially larger DATA where avoiding the admission copy offsets
 * slice/refcount/framing overhead. Small borrowed payloads should normally use
 * flowmq_send().
 */
FLOWMQ_C_API int flowmq_send_slice(flowmq_socket_t *socket,
                                   const mem_slice_t *slice, int flags);

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
 * Transfer one available inbound message part as a canonical Salts Core slice
 * without the final copy performed by flowmq_recv().
 *
 * On SALTS_OK, ownership of the dequeued inbound buffer is transferred to
 * `out`; the caller releases it with mem_slice_release(). The returned slice
 * remains valid across later owner-thread socket progress, sends, and receives.
 * Because FlowMQ 1.1 receive storage is backed by the socket-owned Salts memory
 * pool, every returned slice must be released before flowmq_close(). The API
 * does not copy into a detached buffer to extend lifetime across socket close.
 *
 * Flow credit, HWM occupancy, multipart state and REQ/REP state advance when
 * the message part is dequeued, exactly as for flowmq_recv(); application hold
 * time for the returned slice does not delay transport credit.
 *
 * `out` must be an empty/zero-initialized slice. Passing a descriptor that
 * still owns a buffer returns SALTS_EINVAL without modifying it, so this API
 * can never discard the caller's live reference. With a valid empty output,
 * error and would-block results leave it empty and owning nothing.
 *
 * Without FLOWMQ_DONTWAIT, advance this socket on the calling thread until a
 * part is available or progress fails.
 */
FLOWMQ_C_API int flowmq_recv_slice(flowmq_socket_t *socket,
                                   mem_slice_t *out, int flags);

/**
 * Transfer one available inbound message part as one or more canonical Salts
 * Core slices without coalescing segmented DATA payload ranges.
 *
 * On SALTS_OK, ownership of exactly *count slice descriptors is transferred to
 * the caller. Release every returned descriptor with mem_slice_release().
 * One-range DATA returns count 1; multi-range DATA returns the bounded payload
 * vector already owned by the FlowMQ inbound queue.
 *
 * The caller provides capacity empty/zero-initialized descriptors. If capacity
 * is smaller than the queued part's required range count, the call returns
 * SALTS_ENOBUFS, writes the required count to *count, and leaves the queued
 * part, flow credit, pattern/FSM state, and every output descriptor unchanged.
 * A zero-capacity NULL segments pointer is therefore a valid size query once a
 * part is available.
 *
 * Every descriptor in the caller-provided capacity must be empty before the
 * call; malformed output storage returns SALTS_EINVAL before transport/FSM
 * progress. Would-block and other error results transfer no ownership. Except
 * for the capacity-query count on SALTS_ENOBUFS, error results leave *count
 * zero.
 *
 * Flow credit, HWM occupancy, multipart state, REQ/REP state and peer
 * retirement advance exactly once when ownership is successfully dequeued,
 * matching flowmq_recv() and flowmq_recv_slice().
 *
 * Without FLOWMQ_DONTWAIT, advance this socket on the calling thread until a
 * part is available or progress fails. This API does not expose CNet or
 * NativeIO physical descriptors; only canonical mem_slice_t ownership crosses
 * the public boundary.
 */
FLOWMQ_C_API int flowmq_recv_slicev(flowmq_socket_t *socket,
                                    mem_slice_t *segments, size_t capacity,
                                    size_t *count, int flags);

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
