#ifndef FLOWMQ_ROUTER_ENDPOINT_H
#define FLOWMQ_ROUTER_ENDPOINT_H

#include "flowmq_coronet.h"
#include "flowmq_protocol.h"
#include "flowmq_send_admission.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_ROUTER_ENDPOINT_API_VERSION 4u
#define FLOWMQ_ROUTER_ENDPOINT_DEFAULT_MAX_FRAME_SIZE (8u * 1024u * 1024u)
#define FLOWMQ_ROUTER_ENDPOINT_DEFAULT_MAX_CONNECTIONS 1024u
#define FLOWMQ_ROUTER_ENDPOINT_MAX_CONNECTIONS 65535u
#define FLOWMQ_ROUTER_ENDPOINT_DEFAULT_TIMEOUT_MS 1000u

typedef struct flowmq_router_endpoint_s flowmq_router_endpoint_t;

/** Pointer-free route capability. A token becomes stale after peer close or endpoint restart. */
typedef struct flowmq_router_route_s {
  uint64_t endpoint_id;
  uint64_t generation;
  uint64_t session_id;
} flowmq_router_route_t;

typedef enum flowmq_router_endpoint_state_e {
  FLOWMQ_ROUTER_ENDPOINT_STOPPED = 0,
  FLOWMQ_ROUTER_ENDPOINT_STARTING,
  FLOWMQ_ROUTER_ENDPOINT_LISTENING,
  FLOWMQ_ROUTER_ENDPOINT_FAILED
} flowmq_router_endpoint_state_t;

typedef enum flowmq_router_endpoint_event_kind_e {
  FLOWMQ_ROUTER_EVENT_PEER_CONNECTED = 1,
  FLOWMQ_ROUTER_EVENT_PEER_DISCONNECTED,
  FLOWMQ_ROUTER_EVENT_HEARTBEAT_TIMEOUT
} flowmq_router_endpoint_event_kind_t;

typedef struct flowmq_router_endpoint_event_s {
  flowmq_router_endpoint_event_kind_t kind;
  int status;
  flowmq_router_route_t route;
  tstr_v peer_identity;
  tstr_v peer_topic;
  size_t connections_current;
} flowmq_router_endpoint_event_t;

/**
 * All views are borrowed until the callback returns. route is a value token
 * and may be copied for later use; later sends still require the endpoint context.
 * Returning a non-zero status closes that peer. Lifecycle functions must not
 * be called from endpoint callbacks or from the endpoint context thread.
 */
typedef int (*flowmq_router_endpoint_frame_fn)(void *ctx,
                                               const flowmq_router_route_t *route,
                                               tstr_v peer_identity, tstr_v peer_topic,
                                               const flowmq_protocol_frame_t *frame);
typedef void (*flowmq_router_endpoint_state_fn)(void *ctx,
                                                flowmq_router_endpoint_state_t state,
                                                int status,
                                                size_t connections_current);
typedef void (*flowmq_router_endpoint_event_fn)(void *ctx,
                                                const flowmq_router_endpoint_event_t *event);
/**
 * Bind a verified mTLS client certificate fingerprint to the identity claimed
 * in the DEALER HELLO. Returning anything other than TURBO_OK rejects the peer
 * before it enters the route registry. Views are callback-borrowed.
 */
typedef int (*flowmq_router_endpoint_peer_identity_fn)(
    void *ctx, const char *certificate_sha256, tstr_v claimed_identity);

/**
 * Configuration is copied by create, including all referenced strings.
 * callback_ctx remains borrowed and must outlive the endpoint.
 *
 * When context is NULL, drive_context and own_context must both be non-zero.
 * A non-NULL context is borrowed unless own_context transfers ownership.
 * For a borrowed context, another thread must keep it running while the
 * blocking start/stop calls wait for owner-context work to complete.
 */
typedef struct flowmq_router_endpoint_config_s {
  size_t size;
  flowmq_coronet_transport_t transport;
  const char *host;
  const char *path;
  const char *topic;
  const char *identity;
  const flowmq_coronet_tls_server_config_t *tls;
  int port;
  size_t max_frame_size;
  size_t max_connections;
  size_t stream_recv_buffer_bytes;
  flowmq_coronet_timeout_config_t timeouts;
  flowmq_coronet_socket_options_t socket_options;
  flowmq_coronet_udp_options_t udp_options;
  flowmq_coronet_kcp_options_t kcp_options;
  uint64_t heartbeat_interval_ms;
  uint64_t heartbeat_timeout_ms;
  int reuse_port;
  coro_context_t *context;
  int drive_context;
  int own_context;
  flowmq_router_endpoint_frame_fn on_frame;
  flowmq_router_endpoint_state_fn on_state;
  flowmq_router_endpoint_event_fn on_event;
  flowmq_router_endpoint_peer_identity_fn verify_peer_identity;
  void *callback_ctx;
  flowmq_send_admission_config_t send_admission;
  /**
   * Borrowed verifier-only context. It must outlive the endpoint. A v3-sized
   * configuration keeps the legacy behavior and passes callback_ctx instead.
   */
  void *verify_peer_identity_ctx;
} flowmq_router_endpoint_config_t;

/** Size of the public v3 prefix, for source and binary compatibility tests. */
#define FLOWMQ_ROUTER_ENDPOINT_CONFIG_V3_SIZE \
  offsetof(flowmq_router_endpoint_config_t, verify_peer_identity_ctx)

/** Initialize bounded defaults. NULL is ignored. */
CXX_C_API void flowmq_router_endpoint_config_init(flowmq_router_endpoint_config_t *config);
/**
 * Create a stopped ROUTER/BIND owner and copy its configuration.
 * @param config Complete configuration initialized by config_init().
 * @param out Receives the owned endpoint on success and is set to NULL on validated failures.
 * @return TURBO_OK, TURBO_EINVAL, TURBO_ERANGE, or TURBO_ENOMEM.
 */
CXX_C_API int flowmq_router_endpoint_create(const flowmq_router_endpoint_config_t *config,
                                             flowmq_router_endpoint_t **out);
/**
 * Start listening and wait for the listener result.
 * @param endpoint Stopped endpoint owner.
 * @param timeout_ns Non-zero caller wait budget in nanoseconds.
 * @return TURBO_OK or a concrete bind, configuration, timeout, or shutdown error.
 */
CXX_C_API int flowmq_router_endpoint_start(flowmq_router_endpoint_t *endpoint,
                                            uint64_t timeout_ns);
/**
 * Stop admission, cancel peers, and wait for full CoroNet quiescence.
 * NULL and an already stopped endpoint are accepted.
 */
CXX_C_API void flowmq_router_endpoint_stop(flowmq_router_endpoint_t *endpoint);
/** Stop and release the listener, route registry, copied strings, and any owned context. */
CXX_C_API void flowmq_router_endpoint_destroy(flowmq_router_endpoint_t *endpoint);
/** Borrow the endpoint context; an owned context expires when the endpoint is destroyed. */
CXX_C_API coro_context_t *flowmq_router_endpoint_context(flowmq_router_endpoint_t *endpoint);

/**
 * Send an already encoded FMQ v3 frame to one live route. Must run on the
 * endpoint context. The input is borrowed only for the duration of the call.
 * Returns TURBO_ENOTCONN for a stale or foreign route.
 * @return TURBO_OK, TURBO_EINVAL, TURBO_ESHUTDOWN, TURBO_ENOTCONN, or a transport error.
 */
CXX_C_API int flowmq_router_endpoint_send(flowmq_router_endpoint_t *endpoint,
                                          flowmq_router_route_t route,
                                          const char *encoded, size_t encoded_size);
/**
 * Copy and admit one route-fenced encoded frame from any thread. A stale route
 * is reported asynchronously through send_admission.on_complete.
 */
CXX_C_API int flowmq_router_endpoint_send_copy(
    flowmq_router_endpoint_t *endpoint, flowmq_router_route_t route,
    uint64_t completion_id, const char *encoded, size_t encoded_size);
/** Snapshot the bounded copied-send queue and cumulative completion counters. */
CXX_C_API void flowmq_router_endpoint_send_stats(
    flowmq_router_endpoint_t *endpoint, flowmq_send_admission_stats_t *stats);
/** Lock-free snapshot of admitted DEALER peers; NULL returns zero. */
CXX_C_API size_t flowmq_router_endpoint_connections(
    const flowmq_router_endpoint_t *endpoint);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_ROUTER_ENDPOINT_H */
