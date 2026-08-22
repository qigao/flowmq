#ifndef FLOWMQ_CONNECT_ENDPOINT_H
#define FLOWMQ_CONNECT_ENDPOINT_H

#include "flowmq_export.h"

#include "flowmq_coronet.h"
#include "flowmq_protocol.h"
#include "flowmq_send_admission.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_CONNECT_ENDPOINT_API_VERSION 3u
#define FLOWMQ_CONNECT_ENDPOINT_DEFAULT_MAX_FRAME_SIZE (8u * 1024u * 1024u)
#define FLOWMQ_CONNECT_ENDPOINT_DEFAULT_TIMEOUT_MS 1000u
#define FLOWMQ_CONNECT_ENDPOINT_DEFAULT_RECONNECT_INITIAL_MS 1000u
#define FLOWMQ_CONNECT_ENDPOINT_DEFAULT_RECONNECT_MAX_MS 30000u

typedef struct flowmq_connect_endpoint_s flowmq_connect_endpoint_t;

typedef enum flowmq_connect_endpoint_connection_state_e {
  FLOWMQ_ENDPOINT_CONNECTION_STOPPED = 0,
  FLOWMQ_ENDPOINT_CONNECTION_CONNECTING,
  FLOWMQ_ENDPOINT_CONNECTION_READY,
  FLOWMQ_ENDPOINT_CONNECTION_BACKOFF,
  FLOWMQ_ENDPOINT_CONNECTION_FAILED
} flowmq_connect_endpoint_connection_state_t;

typedef enum flowmq_connect_endpoint_event_kind_e {
  FLOWMQ_ENDPOINT_EVENT_RECONNECT_SCHEDULED = 1,
  FLOWMQ_ENDPOINT_EVENT_RECONNECT_SUCCEEDED,
  FLOWMQ_ENDPOINT_EVENT_RECONNECT_FAILED,
  FLOWMQ_ENDPOINT_EVENT_HEARTBEAT_TIMEOUT
} flowmq_connect_endpoint_event_kind_t;

typedef enum flowmq_connect_endpoint_exchange_state_e {
  FLOWMQ_ENDPOINT_EXCHANGE_READY = 0,
  FLOWMQ_ENDPOINT_EXCHANGE_WAIT_REPLY,
  FLOWMQ_ENDPOINT_EXCHANGE_PROCESSING_REQUEST,
  FLOWMQ_ENDPOINT_EXCHANGE_RESETTING
} flowmq_connect_endpoint_exchange_state_t;

typedef struct flowmq_connect_endpoint_event_s {
  flowmq_connect_endpoint_event_kind_t kind;
  int status;
  uint64_t delay_ms;
  vstr peer_identity;
  vstr peer_topic;
} flowmq_connect_endpoint_event_t;

/** Decoded frame views remain valid only until this callback returns. */
typedef int (*flowmq_connect_endpoint_frame_fn)(void *ctx, const flowmq_protocol_frame_t *frame,
                                                uint64_t generation);
typedef int (*flowmq_connect_endpoint_receive_ready_fn)(void *ctx);
typedef void (*flowmq_connect_endpoint_state_fn)(void *ctx,
                                                 flowmq_connect_endpoint_connection_state_t state,
                                                 int status, size_t connections_current);
typedef void (*flowmq_connect_endpoint_event_fn)(void *ctx,
                                                 const flowmq_connect_endpoint_event_t *event);
/**
 * Bind the verified TLS server certificate fingerprint to the identity
 * claimed in the server HELLO. Returning anything other than TURBO_OK rejects
 * the connection before subscriptions are replayed or READY is published.
 * Inputs are borrowed until the callback returns. The callback runs on the
 * endpoint context and must not block, perform I/O, or call lifecycle APIs.
 */
typedef int (*flowmq_connect_endpoint_peer_identity_fn)(
    void *ctx, const char *certificate_sha256, vstr claimed_identity);

/**
 * Configuration is copied by create, including all referenced strings.
 * callback_ctx remains borrowed and must outlive the endpoint.
 *
 * When context is NULL, drive_context and own_context must both be non-zero and
 * the endpoint creates its own CoroNet context. A non-NULL context is borrowed
 * unless own_context is non-zero, which transfers context ownership.
 */
typedef struct flowmq_connect_endpoint_config_s {
  size_t size;
  flowmq_coronet_transport_t transport;
  flowmq_protocol_pattern_t pattern;
  const char *host;
  const char *path;
  const char *topic;
  const char *identity;
  const flowmq_coronet_tls_client_config_t *tls;
  int port;
  size_t max_frame_size;
  size_t stream_recv_buffer_bytes;
  flowmq_coronet_timeout_config_t timeouts;
  flowmq_coronet_socket_options_t socket_options;
  flowmq_coronet_udp_options_t udp_options;
  flowmq_coronet_kcp_options_t kcp_options;
  int initial_subscription_configured;
  uint64_t reconnect_initial_ms;
  uint64_t reconnect_max_ms;
  uint64_t heartbeat_interval_ms;
  uint64_t heartbeat_timeout_ms;
  coro_context_t *context;
  int drive_context;
  int own_context;
  flowmq_connect_endpoint_frame_fn on_frame;
  flowmq_connect_endpoint_receive_ready_fn receive_ready;
  flowmq_connect_endpoint_state_fn on_state;
  flowmq_connect_endpoint_event_fn on_event;
  void *callback_ctx;
  flowmq_send_admission_config_t send_admission;
  flowmq_connect_endpoint_peer_identity_fn verify_peer_identity;
  /** Borrowed and required to outlive the endpoint. */
  void *verify_peer_identity_ctx;
} flowmq_connect_endpoint_config_t;

/** Size of the public v2 prefix, for source and binary compatibility tests. */
#define FLOWMQ_CONNECT_ENDPOINT_CONFIG_V2_SIZE \
  offsetof(flowmq_connect_endpoint_config_t, verify_peer_identity)

/**
 * Initialize a configuration with bounded standalone defaults.
 * @param config Output configuration; NULL is ignored.
 */
FLOWMQ_C_API void flowmq_connect_endpoint_config_init(flowmq_connect_endpoint_config_t *config);

/**
 * Create a stopped CONNECT endpoint and copy its configuration.
 * @param config Complete configuration initialized by flowmq_connect_endpoint_config_init().
 * @param out Receives the owned endpoint on success and is set to NULL on validated failures.
 * @return TURBO_OK, TURBO_EINVAL, TURBO_ERANGE, or TURBO_ENOMEM.
 */
FLOWMQ_C_API int flowmq_connect_endpoint_create(const flowmq_connect_endpoint_config_t *config,
                                              flowmq_connect_endpoint_t **out);
/**
 * Connect, complete FMQ HELLO, and wait until ready or the deadline expires.
 * @param endpoint Endpoint owner.
 * @param timeout_ns Non-zero caller wait budget in nanoseconds.
 * @return TURBO_OK or the concrete connect, protocol, timeout, or shutdown error.
 */
FLOWMQ_C_API int flowmq_connect_endpoint_start(flowmq_connect_endpoint_t *endpoint,
                                             uint64_t timeout_ns);
/** Stop accepting sends, interrupt waits, and join endpoint-owned execution. NULL is accepted. */
FLOWMQ_C_API void flowmq_connect_endpoint_stop(flowmq_connect_endpoint_t *endpoint);
/** Stop and release the endpoint, its socket, copied strings, and any owned context. */
FLOWMQ_C_API void flowmq_connect_endpoint_destroy(flowmq_connect_endpoint_t *endpoint);

/** Borrow the endpoint context. The pointer becomes invalid when an owned endpoint is destroyed. */
FLOWMQ_C_API coro_context_t *flowmq_connect_endpoint_context(flowmq_connect_endpoint_t *endpoint);

/**
 * Send one already encoded FMQ v3 frame. This function must run on the endpoint
 * context (for example from on_frame/on_state or a caller-managed coro_post).
 * The input is borrowed only for the duration of the call.
 */
FLOWMQ_C_API int flowmq_connect_endpoint_send(flowmq_connect_endpoint_t *endpoint,
                                           const char *encoded, size_t encoded_size);
/**
 * Copy and admit one encoded frame from any thread. TURBO_OK transfers the
 * copy to the endpoint; local send completion is reported exactly once through
 * send_admission.on_complete. TURBO_ENOSPC and other failures retain caller
 * ownership and do not invoke completion.
 */
FLOWMQ_C_API int flowmq_connect_endpoint_send_copy(
    flowmq_connect_endpoint_t *endpoint, uint64_t completion_id,
    const char *encoded, size_t encoded_size);
/** Snapshot the bounded copied-send queue and cumulative completion counters. */
FLOWMQ_C_API void flowmq_connect_endpoint_send_stats(
    flowmq_connect_endpoint_t *endpoint, flowmq_send_admission_stats_t *stats);
/**
 * Interrupt the active receive/reconnect wait. May be called from another thread.
 * @return TURBO_OK when posted, TURBO_ENOTCONN when no socket is active, or a concrete error.
 */
FLOWMQ_C_API int flowmq_connect_endpoint_interrupt(flowmq_connect_endpoint_t *endpoint, int status);
/**
 * Atomically replace copied address strings while stopped.
 * @return TURBO_OK, TURBO_EINVAL, TURBO_EBUSY, or TURBO_ENOMEM.
 */
FLOWMQ_C_API int flowmq_connect_endpoint_update_endpoint(flowmq_connect_endpoint_t *endpoint,
                                                       const char *host, int port,
                                                       const char *path);
/** Begin one strict REQ exchange and return its generation. */
FLOWMQ_C_API int flowmq_connect_endpoint_request_begin(flowmq_connect_endpoint_t *endpoint,
                                                     uint64_t correlation_id,
                                                     uint64_t *generation);
/** Finish the matching strict REQ exchange. */
FLOWMQ_C_API int flowmq_connect_endpoint_request_finish(
    flowmq_connect_endpoint_t *endpoint, uint64_t generation, uint64_t correlation_id,
    flowmq_connect_endpoint_exchange_state_t terminal_state);
/** Read the current strict exchange state without advancing it. */
FLOWMQ_C_API int flowmq_connect_endpoint_exchange_snapshot(
    const flowmq_connect_endpoint_t *endpoint, flowmq_connect_endpoint_exchange_state_t *state,
    uint64_t *generation, uint64_t *correlation_id);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_CONNECT_ENDPOINT_H */
