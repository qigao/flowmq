#ifndef TURBO_FLOW_FMQ_H
#define TURBO_FLOW_FMQ_H

#include "CoroNet/turbo_coro_context.h"
#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_coronet_execution.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_security.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE (8u * 1024u * 1024u)
#define TURBO_FLOW_FMQ_DEFAULT_MAX_CONNECTIONS 1024u
#define TURBO_FLOW_FMQ_MAX_CONNECTIONS_LIMIT 65535u
#define TURBO_FLOW_FMQ_MIN_STREAM_RECV_BUFFER_SIZE 1024u
#define TURBO_FLOW_FMQ_MAX_STREAM_RECV_BUFFER_SIZE (1024u * 1024u)
#define TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE 255u
#define TURBO_FLOW_FMQ_MAX_TOPIC_SIZE 1024u
#define TURBO_FLOW_FMQ_WIRE_VERSION 3u
#define TURBO_FLOW_FMQ_MODULE "io.fmq"
#define TURBO_FLOW_FMQ_PUB_SEND_OPERATION "fmq.pub.send"
#define TURBO_FLOW_FMQ_SUB_RECEIVE_OPERATION "fmq.sub.receive"
#define TURBO_FLOW_FMQ_PUSH_SEND_OPERATION "fmq.push.send"
#define TURBO_FLOW_FMQ_PULL_RECEIVE_OPERATION "fmq.pull.receive"
#define TURBO_FLOW_FMQ_ROUTER_RECEIVE_OPERATION "fmq.router.receive"
#define TURBO_FLOW_FMQ_ROUTER_SEND_OPERATION "fmq.router.send"
#define TURBO_FLOW_FMQ_DEALER_RECEIVE_OPERATION "fmq.dealer.receive"
#define TURBO_FLOW_FMQ_DEALER_SEND_OPERATION "fmq.dealer.send"
#define TURBO_FLOW_FMQ_PAIR_RECEIVE_OPERATION "fmq.pair.receive"
#define TURBO_FLOW_FMQ_PAIR_SEND_OPERATION "fmq.pair.send"
#define TURBO_FLOW_FMQ_REQ_REQUEST_OPERATION "fmq.req.request"
#define TURBO_FLOW_FMQ_REQ_REPLY_OPERATION "fmq.req.reply"
#define TURBO_FLOW_FMQ_REP_REQUEST_OPERATION "fmq.rep.request"
#define TURBO_FLOW_FMQ_REP_REPLY_OPERATION "fmq.rep.reply"
#define TURBO_FLOW_FMQ_XPUB_RECEIVE_OPERATION "fmq.xpub.receive"
#define TURBO_FLOW_FMQ_XPUB_SEND_OPERATION "fmq.xpub.send"
#define TURBO_FLOW_FMQ_XSUB_RECEIVE_OPERATION "fmq.xsub.receive"
#define TURBO_FLOW_FMQ_XSUB_SEND_OPERATION "fmq.xsub.send"

typedef enum turbo_flow_fmq_endpoint_mode_e {
  TURBO_FLOW_FMQ_BIND = 1,
  TURBO_FLOW_FMQ_CONNECT
} turbo_flow_fmq_endpoint_mode_t;

typedef enum turbo_flow_fmq_transport_e {
  TURBO_FLOW_FMQ_TCP = 1,
  TURBO_FLOW_FMQ_TLS,
  TURBO_FLOW_FMQ_UDP,
  TURBO_FLOW_FMQ_KCP,
  TURBO_FLOW_FMQ_PIPE,
  TURBO_FLOW_FMQ_WS,
  TURBO_FLOW_FMQ_WSS
} turbo_flow_fmq_transport_t;

/** Per-endpoint TLS material copied by the FMQ facade during create. */
typedef struct turbo_flow_fmq_tls_config_s {
  size_t size;
  const char *ca_file;
  const char *cert_file;
  const char *key_file;
  const char *key_password;
  const char *server_name;
  int verify_peer;
  int require_client_certificate;
  uint64_t rotation_generation;
} turbo_flow_fmq_tls_config_t;

#define TURBO_FLOW_FMQ_TLS_CONFIG_INIT \
  {sizeof(turbo_flow_fmq_tls_config_t), NULL, NULL, NULL, NULL, NULL, 1, 0, 0u}

typedef enum turbo_flow_fmq_metadata_policy_e {
  TURBO_FLOW_FMQ_METADATA_STATIC = 1,
  TURBO_FLOW_FMQ_METADATA_INHERIT,
  /** Read pointer-free metadata from turbo_flow_content_descriptor_t::identity. */
  TURBO_FLOW_FMQ_METADATA_CONTENT
} turbo_flow_fmq_metadata_policy_t;

typedef enum turbo_flow_fmq_frame_admission_policy_e {
  TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL = 0,
  TURBO_FLOW_FMQ_FRAME_ADMISSION_BLOCK,
  TURBO_FLOW_FMQ_FRAME_ADMISSION_DROP_OLDEST
} turbo_flow_fmq_frame_admission_policy_t;

typedef enum turbo_flow_fmq_event_kind_e {
  TURBO_FLOW_FMQ_EVENT_PEER_CONNECTED = 1,
  TURBO_FLOW_FMQ_EVENT_PEER_DISCONNECTED,
  TURBO_FLOW_FMQ_EVENT_RECONNECT_SCHEDULED,
  TURBO_FLOW_FMQ_EVENT_RECONNECT_SUCCEEDED,
  TURBO_FLOW_FMQ_EVENT_RECONNECT_FAILED,
  TURBO_FLOW_FMQ_EVENT_HEARTBEAT_TIMEOUT,
  TURBO_FLOW_FMQ_EVENT_FRAME_SENT,
  TURBO_FLOW_FMQ_EVENT_HWM_REACHED,
  TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED,
  TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM,
  TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DROPPED,
  TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DISCONNECTED,
  TURBO_FLOW_FMQ_EVENT_AUTHENTICATION_FAILED,
  TURBO_FLOW_FMQ_EVENT_AUTHORIZATION_DENIED
} turbo_flow_fmq_event_kind_t;

typedef struct turbo_flow_fmq_event_s {
  size_t size;
  turbo_flow_fmq_event_kind_t kind;
  int status;
  turbo_flow_fmq_pattern_t pattern;
  turbo_flow_fmq_endpoint_mode_t mode;
  turbo_flow_fmq_transport_t transport;
  tstr_v peer_identity;
  tstr_v peer_topic;
  uint64_t reconnect_delay_ms;
  uint64_t frame_bytes;
} turbo_flow_fmq_event_t;

#define TURBO_FLOW_FMQ_EVENT_INIT {sizeof(turbo_flow_fmq_event_t)}

/**
 * Host-owned monitoring callback. Values are borrowed for the call only, and
 * callback code must not re-enter the same flow or adapter.
 */
typedef void (*turbo_flow_fmq_event_fn)(void *ctx, const turbo_flow_fmq_event_t *event);

#define TURBO_FLOW_FMQ_TIMEOUT_DISABLED UINT64_MAX
#define TURBO_FLOW_FMQ_RECONNECT_DISABLED UINT64_MAX
#define TURBO_FLOW_FMQ_RECONNECT_MAX_UNBOUNDED UINT64_MAX

typedef struct turbo_flow_fmq_config_s {
  size_t size;
  turbo_flow_fmq_pattern_t pattern;
  turbo_flow_fmq_endpoint_mode_t mode;
  turbo_flow_fmq_transport_t transport;
  /**
   * Bind address or remote host. May carry a scheme prefix ("tcp://",
   * "tls://", "udp://", "kcp://", "ws://", "wss://") that selects the
   * transport and may embed the port (e.g. "tls://192.168.2.1:5000"); an
   * embedded port must match transport-unset or equal config.port, and a
   * scheme that conflicts with an explicit config.transport is rejected.
   */
  const char *host;
  int port;
  /**
   * PUB/XPUB output topic or SUB/XSUB prefix. Empty SUB/XSUB prefix subscribes to all topics.
   * A NULL XSUB topic sends no initial subscription so graph input can drive proxy controls.
   */
  const char *topic;
  /** Optional application payload media type; unknown or NULL remains opaque. */
  const char *content_type;
  /** Optional host registry and trusted application schema selector. */
  const turbo_flow_content_binding_t *content_binding;
  /** Required and unique for DEALER; optional diagnostic identity for PAIR. */
  const char *identity;
  size_t max_frame_size;
  uint32_t max_connections;
  /** Default timeout in milliseconds for operations without a phase-specific value. */
  uint64_t timeout_ms;
  /** Per-phase connect timeout in milliseconds; 0 falls back to timeout_ms. */
  uint64_t connect_timeout_ms;
  /** Per-phase send timeout in milliseconds; 0 falls back to timeout_ms. */
  uint64_t send_timeout_ms;
  /** Per-phase receive timeout in milliseconds; 0 falls back to timeout_ms. */
  uint64_t recv_timeout_ms;
  /**
   * TLS/WS/WSS connect+handshake deadline in milliseconds; 0 falls back to timeout_ms.
   * CoroNet currently exposes one combined client wait, so an explicitly different
   * connect_timeout_ms is rejected. Explicit BIND-side handshake timeout is unsupported.
   */
  uint64_t handshake_timeout_ms;
  /**
   * Reconnect timing. 0 means keep the default profile:
   * - reconnect_initial_ms default: 1000 ms
   * - reconnect_max_ms default: 30000 ms
   */
  uint64_t reconnect_initial_ms;
  uint64_t reconnect_max_ms;
  /** Heartbeat interval and timeout in milliseconds. Both 0 disables protocol heartbeat. */
  uint64_t heartbeat_interval_ms;
  uint64_t heartbeat_timeout_ms;
  turbo_flow_fmq_event_fn event_callback;
  void *event_ctx;
  /**
   * Outbound metadata policy. STATIC uses topic/identity above. INHERIT copies
   * the current FMQ message metadata and requires synchronous FMQ input.
   * CONTENT uses the message-owned content descriptor identity.
   */
  turbo_flow_fmq_metadata_policy_t topic_policy;
  turbo_flow_fmq_metadata_policy_t identity_policy;
  /** Pipe endpoint for PIPE; WebSocket request path for WS/WSS, default "/". */
  const char *path;
  /** Optional object-level TLS client/server material for TLS/WSS. */
  const turbo_flow_fmq_tls_config_t *tls;
  /** Required 64-character hexadecimal PSK when transport is KCP. */
  const char *kcp_pre_shared_key;
  uint32_t kcp_mtu;
  uint32_t kcp_send_window;
  uint32_t kcp_receive_window;
  uint32_t kcp_interval_ms;
  uint32_t kcp_handshake_retry_ms;
  uint32_t kcp_fast_resend;
  /** Enable KCP congestion-window control; disabled by default for low latency. */
  int kcp_congestion_control;
  uint32_t kcp_fec_data_shards;
  uint32_t kcp_fec_parity_shards;
  uint32_t kcp_fec_max_payload_size;
  uint32_t kcp_fec_receive_groups;
  /** Enable listener reuse-port binding where CoroNet supports it. BIND only. */
  int reuse_port;
  /** Enable OS TCP keepalive on TCP-backed transports. */
  int tcp_keepalive;
  uint64_t tcp_keepalive_idle_ms;
  uint64_t tcp_keepalive_interval_ms;
  uint32_t tcp_keepalive_count;
  /** Enable OS SO_LINGER on TCP-backed transports. linger_ms may be 0 for abortive close. */
  int linger;
  uint64_t linger_ms;
  /** CoroNet socket send high-water mark in bytes; 0 disables the socket-level cap. */
  size_t send_hwm_bytes;
  /** BIND only: multicast group joined after the UDP listener binds. */
  const char *udp_multicast_group;
  /** IPv4 local address or IPv6 decimal interface index used for multicast membership. */
  const char *udp_multicast_interface;
  /** Presence bits distinguish omitted loop/TTL/broadcast values from explicit zero values. */
  uint32_t udp_option_flags;
  int udp_multicast_loop;
  uint32_t udp_multicast_ttl;
  int udp_broadcast;
  /** FMQ in-flight frame count high-water mark; 0 disables the FMQ-level cap. */
  size_t frame_hwm_messages;
  /** FMQ in-flight encoded-frame byte high-water mark; 0 disables the FMQ-level cap. */
  size_t frame_hwm_bytes;
  /** Capacity behavior. FAIL is the default. */
  turbo_flow_fmq_frame_admission_policy_t frame_admission_policy;
  /** BLOCK only: zero checks immediately; UINT64_MAX waits until capacity or stop. */
  uint64_t frame_admission_timeout_ms;
  /** FMQ stop drain timeout for in-flight frames; 0 closes immediately. */
  uint64_t frame_linger_ms;
  /**
   * Capacity of each of the two CoroNet user-space receive buffers per stream.
   * 0 keeps the CoroNet default. Supported only for endpoint-owned private contexts.
   */
  size_t stream_recv_buffer_bytes;
  /**
   * Requested OS SO_RCVBUF size in bytes for TCP/TLS/WS/WSS.
   * 0 preserves the OS default. Configure before bind/connect; not hot-reloadable.
   */
  size_t socket_recv_buffer_bytes;
  /**
   * Requested OS SO_SNDBUF size in bytes for TCP/TLS/WS/WSS.
   * 0 preserves the OS default. Configure before bind/connect; not hot-reloadable.
   */
  size_t socket_send_buffer_bytes;
} turbo_flow_fmq_config_t;

#define TURBO_FLOW_FMQ_CONFIG_INIT {sizeof(turbo_flow_fmq_config_t)}

/**
 * Optional FMQ v3 authentication and default-deny authorization owners.
 *
 * BIND endpoints use auth_provider, realm, realm_channel, and auth_method.
 * CONNECT endpoints use key_provider, secret_reference, and auth_method; the
 * endpoint config identity is the authentication identity. Provider objects and
 * realms are borrowed and must outlive the adapter. String fields are copied.
 *
 * Authentication and authorization are available on every FMQ transport. TLS
 * and WSS additionally require a verified TLS 1.3 connection and exporter channel
 * binding;
 * TCP, UDP, KCP, Pipe, and WS require a trusted network or an external secure
 * tunnel because their credential-bearing HELLO is not encrypted. The credential
 * is borrowed only while constructing the HELLO payload and is released before
 * the socket write. Encoded credential bytes are cleared after that write returns
 * and are never retained in a graph message.
 */
typedef struct turbo_flow_fmq_security_binding_s {
  size_t size;
  const char *realm_channel;
  const char *auth_method;
  const turbo_flow_security_auth_provider_t *auth_provider;
  turbo_flow_security_realm_t *realm;
  const turbo_flow_security_key_provider_t *key_provider;
  const char *secret_reference;
  /**
   * Optional BIND-side mTLS identity verifier. The fingerprint is the
   * canonical verified `sha256:<64 lowercase hex>` peer certificate identity;
   * claimed_identity is the FMQ HELLO identity. Both are borrowed for the
   * callback only. The callback context must outlive the adapter.
   */
  int (*verify_peer_certificate_identity)(void *ctx,
                                          const char *certificate_sha256,
                                          const char *claimed_identity);
  void *peer_certificate_identity_ctx;
} turbo_flow_fmq_security_binding_t;

#define TURBO_FLOW_FMQ_SECURITY_BINDING_INIT                                                      \
  {sizeof(turbo_flow_fmq_security_binding_t), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}

typedef enum turbo_flow_fmq_slow_peer_policy_e {
  TURBO_FLOW_FMQ_SLOW_PEER_FAIL = 1,
  TURBO_FLOW_FMQ_SLOW_PEER_DROP_OLDEST,
  TURBO_FLOW_FMQ_SLOW_PEER_DISCONNECT
} turbo_flow_fmq_slow_peer_policy_t;

/**
 * Optional bounded per-peer queue policy for PUB/XPUB fan-out.
 *
 * At least one HWM must be non-zero. A remote SUB/XSUB must send a non-empty identity that is
 * unique among live peers. A successful publish is a volatile queue-admission ACK; actual peer
 * writes and policy actions are reported through turbo_flow_fmq_event_fn.
 */
typedef struct turbo_flow_fmq_fanout_config_s {
  size_t size;
  size_t peer_hwm_messages;
  size_t peer_hwm_bytes;
  turbo_flow_fmq_slow_peer_policy_t slow_peer_policy;
} turbo_flow_fmq_fanout_config_t;

#define TURBO_FLOW_FMQ_FANOUT_CONFIG_INIT                                                          \
  {sizeof(turbo_flow_fmq_fanout_config_t), 0u, 0u, TURBO_FLOW_FMQ_SLOW_PEER_FAIL}

#define TURBO_FLOW_FMQ_UDP_OPTION_MULTICAST_LOOP (1u << 0)
#define TURBO_FLOW_FMQ_UDP_OPTION_MULTICAST_TTL (1u << 1)
#define TURBO_FLOW_FMQ_UDP_OPTION_BROADCAST (1u << 2)

/**
 * Register one ZeroMQ-like FMQ endpoint under a dotted DSL adapter binding.
 *
 * FMQ has its own bounded, versioned framing protocol over CoroNet transports.
 * It provides messaging-pattern semantics but is not ZeroMQ wire compatible.
 * The supported pairings are PUB(bind)/SUB(connect), PUSH(bind)/PULL(connect),
 * REQ(connect)/REP(bind), ROUTER(bind)/DEALER(connect), and
 * PAIR(bind)/PAIR(connect).
 *
 * `config` must be the complete structure initialized with
 * TURBO_FLOW_FMQ_CONFIG_INIT. The wire protocol is v3-only and is independent
 * from this local C layout.
 */
/**
 * Register an FMQ adapter with an explicit CoroNet execution binding.
 *
 * Borrowed contexts and pool
 * lanes are driven by their owner, which must keep them running until adapter
 * shutdown completes. Returns TURBO_OK or a concrete config, lane-resolution,
 * allocation, or adapter-registration error.
 */
CXX_C_API int
turbo_flow_fmq_register_adapter_ex(turbo_flow_t *flow, const char *name,
                                   const turbo_flow_fmq_config_t *config,
                                   const turbo_flow_coronet_execution_binding_t *execution);

/** Register a v3-only adapter with mandatory authentication and default-deny ACL. */
CXX_C_API int turbo_flow_fmq_register_secure_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_fmq_config_t *config,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security);

/** Register bounded per-peer fan-out with an explicit CoroNet execution binding. */
CXX_C_API int
turbo_flow_fmq_register_fanout_adapter_ex(turbo_flow_t *flow, const char *name,
                                          const turbo_flow_fmq_config_t *config,
                                          const turbo_flow_fmq_fanout_config_t *fanout,
                                          const turbo_flow_coronet_execution_binding_t *execution);

/** Register secure bounded PUB/XPUB fan-out with per-peer ACL enforcement. */
CXX_C_API int turbo_flow_fmq_register_secure_fanout_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_fmq_config_t *config,
    const turbo_flow_fmq_fanout_config_t *fanout,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security);

/**
 * Register one FMQ adapter from an immutable resolved YAML snapshot.
 *
 * `name` selects the adapter entry and is also used as the TurboFlow binding
 * name. Only serializable FMQ fields are accepted; host callbacks, schema
 * pointers, and CoroNet execution ownership remain explicit host API inputs.
 */
CXX_C_API int turbo_flow_fmq_register_resolved_adapter(turbo_flow_t *flow, const char *name,
                                                       const turbo_flow_resolved_config_t *resolved,
                                                       turbo_flow_config_error_t *error);

CXX_C_API int turbo_flow_fmq_register_resolved_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution, turbo_flow_config_error_t *error);

/** Resolve serializable endpoint fields and inject host-owned security capabilities explicitly. */
CXX_C_API int turbo_flow_fmq_register_resolved_secure_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_config_error_t *error);

CXX_C_API int turbo_flow_fmq_register_resolved_secure_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_config_error_t *error);

/** Message-backed topic view, valid until the message is mutated or cleaned up. */
CXX_C_API int turbo_flow_fmq_message_topic(const turbo_flow_msg_t *msg, tstr_v *topic);

/** Message-backed peer identity view, valid until the message is mutated or cleaned up. */
CXX_C_API int turbo_flow_fmq_message_identity(const turbo_flow_msg_t *msg, tstr_v *identity);

/** Read the message-owned wire correlation ID. */
CXX_C_API int turbo_flow_fmq_message_correlation_id(const turbo_flow_msg_t *msg,
                                                    uint64_t *correlation_id);

/**
 * Detach a ROUTER request from its synchronous receive callback.
 *
 * ROUTER ingress already attaches a message-owned, generation-fenced route.
 * This function validates that capability, ensures the content descriptor is
 * owned, and clears FMQ metadata that the delayed reply no longer needs. The
 * message may then cross worker/fan-out/memory-queue boundaries and later enter
 * the same ROUTER sink. A disconnected, reconnected, or restarted session fails
 * with TURBO_ENOTCONN. REP remains deliberately synchronous.
 */
CXX_C_API int turbo_flow_fmq_message_detach_router_route(turbo_flow_msg_t *msg);

/**
 * Read a message-backed XPUB subscription event.
 * Returns TURBO_ENOENT for ordinary DATA messages.
 */
CXX_C_API int turbo_flow_fmq_message_subscription(const turbo_flow_msg_t *msg, int *subscribe,
                                                  tstr_v *topic);

typedef struct turbo_flow_fmq_app_s turbo_flow_fmq_app_t;

/**
 * Borrowed-message application callback.
 *
 * The message and its payload/topic/identity views remain valid only for this
 * Core dispatch unless explicitly cloned. Returning an error fails the current
 * receive operation. REP callbacks may replace the payload and return TURBO_OK to send
 * the synchronous reply. Do not re-enter lifecycle or send APIs on the same
 * application from this callback.
 */
typedef int (*turbo_flow_fmq_app_message_fn)(turbo_flow_fmq_app_t *app,
                                             turbo_flow_msg_t *message, void *ctx);

typedef struct turbo_flow_fmq_app_options_s {
  size_t size;
  turbo_flow_fmq_app_message_fn on_message;
  void *message_ctx;
} turbo_flow_fmq_app_options_t;

#define TURBO_FLOW_FMQ_APP_OPTIONS_INIT                                                             \
  {sizeof(turbo_flow_fmq_app_options_t), NULL, NULL}

/**
 * Create one ZeroMQ-like application facade over one FMQ pattern endpoint.
 *
 * The returned application directly owns one graph-neutral FMQ endpoint Core.
 * It does not create or compile a TurboFlow graph. Receive-only patterns
 * require on_message. Bidirectional patterns may omit it to discard ingress.
 * Send-only patterns reject a callback.
 */
CXX_C_API int turbo_flow_fmq_app_create(const turbo_flow_fmq_config_t *endpoint,
                                        const turbo_flow_fmq_app_options_t *options,
                                        turbo_flow_fmq_app_t **out);

/**
 * Create an application facade with explicit CoroNet execution placement.
 *
 * PRIVATE, BORROWED_CONTEXT, and POOL_LANE bindings are supported. The host
 * must drive a borrowed context or pool lane and keep it alive until facade
 * destruction completes. The facade never takes ownership of those host
 * resources, including on failure. OWNED_CONTEXT is rejected because a create
 * failure cannot safely report partial ownership transfer.
 *
 * @param endpoint Complete FMQ endpoint configuration, copied during create.
 * @param options Complete callback and application options.
 * @param execution Valid immutable execution binding. Must not be OWNED_CONTEXT.
 * @param out Receives the created facade on success and NULL on failure.
 * @return TURBO_OK; TURBO_EINVAL for an invalid endpoint, options, binding, or
 * callback contract; TURBO_ERANGE for an invalid pool lane; TURBO_ENOMEM for
 * allocation failure; or a concrete endpoint/security initialization error.
 */
CXX_C_API int turbo_flow_fmq_app_create_ex(
    const turbo_flow_fmq_config_t *endpoint, const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_coronet_execution_binding_t *execution, turbo_flow_fmq_app_t **out);

/** Create an application facade backed by one mandatory-secure v3 endpoint. */
CXX_C_API int turbo_flow_fmq_app_create_secure(
    const turbo_flow_fmq_config_t *endpoint, const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out);

/**
 * Secure counterpart of turbo_flow_fmq_app_create_ex.
 *
 * @param endpoint Complete mandatory-secure FMQ endpoint configuration.
 * @param options Complete callback and application options.
 * @param execution Valid immutable execution binding. Must not be OWNED_CONTEXT.
 * @param security Mandatory v3 authentication and authorization binding.
 * @param out Receives the created facade on success and NULL on failure.
 * @return The same errors as turbo_flow_fmq_app_create_ex, plus concrete
 * security binding validation or registration errors.
 */
CXX_C_API int turbo_flow_fmq_app_create_secure_ex(
    const turbo_flow_fmq_config_t *endpoint, const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out);

/** Create the same facade from one resolved YAML adapter entry. */
CXX_C_API int turbo_flow_fmq_app_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_app_options_t *options, turbo_flow_fmq_app_t **out,
    turbo_flow_config_error_t *error);

/** Create a secure facade from YAML endpoint fields plus an explicit host security binding. */
CXX_C_API int turbo_flow_fmq_app_create_resolved_secure(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out,
    turbo_flow_config_error_t *error);

CXX_C_API int turbo_flow_fmq_app_start(turbo_flow_fmq_app_t *app);
CXX_C_API int turbo_flow_fmq_app_stop(turbo_flow_fmq_app_t *app);

/**
 * Send one copied message and wait for the facade's local delivery boundary.
 *
 * Use this serialized synchronous mode when messages are infrequent, the
 * producer needs an immediate per-message status, or the pattern requires
 * strict request/session sequencing. REQ must use this mode; REP replies are
 * produced by its receive callback. It also minimizes intentional queueing for
 * latency-sensitive control messages.
 *
 * For sustained small-message traffic, prefer
 * turbo_flow_fmq_app_send_batch() when the caller already owns a burst, or
 * turbo_flow_fmq_app_send_async() when messages arrive individually and the
 * producer must not wait for socket delivery. All three modes copy payloads;
 * successful local delivery does not mean remote processing or durability.
 * Benchmark the selected pattern, transport, payload size, and batch limits:
 * coalescing behavior is transport/layout dependent.
 * "Serialized" describes one-at-a-time synchronous submission, not a
 * different wire encoding. Send mode never changes FMQ/3, pattern semantics,
 * or the configured transport.
 *
 * @param app Started facade whose pattern supports sending.
 * @param data Payload copied before return; NULL is valid only when
 * data_size is zero.
 * @param data_size Payload byte count.
 * @return TURBO_OK at the local delivery boundary, TURBO_EBUSY before start,
 * TURBO_ENOTSUP for a receive-only pattern, or another explicit
 * validation/allocation/delivery error.
 */
CXX_C_API int turbo_flow_fmq_app_send(turbo_flow_fmq_app_t *app, const void *data,
                                      size_t data_size);

typedef struct turbo_flow_fmq_app_send_item_s {
  const void *data;
  size_t data_size;
} turbo_flow_fmq_app_send_item_t;

#define TURBO_FLOW_FMQ_APP_SEND_ITEM_INIT {NULL, 0u}
#define TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_ITEMS 1024u
#define TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES (64u * 1024u * 1024u)
#define TURBO_FLOW_FMQ_APP_ASYNC_SEND_DEFAULT_QUEUE_ITEMS 8192u
#define TURBO_FLOW_FMQ_APP_ASYNC_SEND_DEFAULT_QUEUE_BYTES (64u * 1024u * 1024u)
#define TURBO_FLOW_FMQ_APP_ASYNC_SEND_DEFAULT_BATCH_ITEMS 256u
#define TURBO_FLOW_FMQ_APP_ASYNC_SEND_DEFAULT_LINGER_NS UINT64_C(50000)
#define TURBO_FLOW_FMQ_APP_ASYNC_SEND_MAX_QUEUE_ITEMS (1024u * 1024u)
#define TURBO_FLOW_FMQ_APP_ASYNC_SEND_MAX_LINGER_NS UINT64_C(1000000000)

typedef void (*turbo_flow_fmq_app_send_completion_fn)(void *ctx, int status);

typedef struct turbo_flow_fmq_app_async_send_config_s {
  size_t size;                 /**< Must equal sizeof(turbo_flow_fmq_app_async_send_config_t). */
  size_t queue_capacity;       /**< Maximum queued messages waiting for the worker. */
  size_t queue_capacity_bytes; /**< Maximum copied payload bytes waiting in the queue. */
  size_t batch_size;           /**< Maximum messages in one worker-built micro-batch. */
  uint64_t linger_ns;          /**< Maximum wait for more messages after a non-empty wake. */
} turbo_flow_fmq_app_async_send_config_t;

#define TURBO_FLOW_FMQ_APP_ASYNC_SEND_CONFIG_INIT                                      \
  {sizeof(turbo_flow_fmq_app_async_send_config_t),                                     \
   TURBO_FLOW_FMQ_APP_ASYNC_SEND_DEFAULT_QUEUE_ITEMS,                                  \
   TURBO_FLOW_FMQ_APP_ASYNC_SEND_DEFAULT_QUEUE_BYTES,                                  \
   TURBO_FLOW_FMQ_APP_ASYNC_SEND_DEFAULT_BATCH_ITEMS,                                  \
   TURBO_FLOW_FMQ_APP_ASYNC_SEND_DEFAULT_LINGER_NS}

/**
 * Send a batch of copied application messages and wait for every submitted
 * frame to reach the same delivery boundary as turbo_flow_fmq_app_send().
 *
 * Use explicit synchronous batching when the producer already has two or more
 * independent messages ready and can wait for the complete burst. It avoids
 * per-message submission overhead and, on supported stream layouts, can reduce
 * socket calls. This is the preferred throughput mode for sustained small
 * PUB/PUSH/DEALER messages. Large-message batches must also be sized by total
 * bytes and tail-latency goals; the maximum limits are safety bounds, not
 * recommended operating sizes.
 *
 * PUB, PUSH, and DEALER facades are supported. TCP connect endpoints coalesce
 * the encoded frames into one stream write; other valid endpoint layouts keep
 * the same batch admission/completion semantics without coalescing. On error,
 * frames before the failing item remain submitted and @p submitted reports
 * their count. A batch is bounded by
 * TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_ITEMS and
 * TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES.
 */
CXX_C_API int turbo_flow_fmq_app_send_batch(turbo_flow_fmq_app_t *app,
                                            const turbo_flow_fmq_app_send_item_t *items,
                                            size_t item_count, size_t *submitted);

/**
 * Configure copied, ordered asynchronous send admission before the first start.
 *
 * Use asynchronous micro-batching when producers receive messages one at a
 * time, cannot construct explicit batches, or must avoid waiting for the
 * socket-delivery boundary. Admission is fast but not unbounded: callers must
 * handle TURBO_ENOSPC and every accepted message completes later on the worker.
 * A zero linger favors latency; a nonzero linger gives sparse traffic time to
 * form fuller batches and therefore trades added queueing latency for
 * throughput. Explicit batching remains preferable when the caller already
 * owns the burst and can wait synchronously.
 *
 * PUB, PUSH, and DEALER facades are supported. The queue is bounded by both
 * waiting item count and copied payload bytes. The active batch is bounded
 * separately by TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_ITEMS and
 * TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES. One facade-owned worker
 * drains admitted items through the same batch submission and delivery
 * boundary as turbo_flow_fmq_app_send_batch(), preserving admission order
 * without copying the admitted payload a second time. Configuration may be
 * performed once and cannot be changed after start.
 *
 * @param app Facade to configure.
 * @param config Complete queue, byte quota, batch, and linger limits.
 * @return TURBO_OK, TURBO_EINVAL for an invalid config, TURBO_ENOTSUP for an
 * unsupported pattern, TURBO_EBUSY after the first start, or TURBO_EALREADY
 * when already configured.
 */
CXX_C_API int turbo_flow_fmq_app_configure_async_send(
    turbo_flow_fmq_app_t *app, const turbo_flow_fmq_app_async_send_config_t *config);

/**
 * Copy and admit one message without waiting for socket delivery.
 *
 * This is the per-message producer API for the configured asynchronous
 * micro-batch mode. TURBO_OK is admission, not delivery: release or reuse the
 * caller's input after return, but use @p completion when delivery status
 * matters. This queue is an in-memory load-leveling boundary and is not a
 * durable store.
 *
 * TURBO_OK means the facade owns a copy and will invoke @p completion exactly
 * once when provided. A full item or byte quota returns TURBO_ENOSPC without
 * invoking the callback. Completion runs on the facade worker and must not
 * call stop or destroy for the same application.
 *
 * @param app Started facade with asynchronous send configured.
 * @param data Bytes copied before successful admission; NULL is valid only
 * when data_size is zero.
 * @param data_size Payload byte count.
 * @param completion Optional per-message delivery-boundary callback.
 * @param ctx Opaque callback context.
 * @return TURBO_OK on accepted ownership, TURBO_ENOSPC when either queue quota
 * is full, TURBO_EBUSY outside the started admission window, TURBO_EMSGSIZE
 * above the per-message limit, or another explicit validation/allocation error.
 */
CXX_C_API int turbo_flow_fmq_app_send_async(
    turbo_flow_fmq_app_t *app, const void *data, size_t data_size,
    turbo_flow_fmq_app_send_completion_fn completion, void *ctx);

/**
 * Send one existing message through the facade.
 *
 * The endpoint consumes the message synchronously and retains any buffer needed
 * past the call. This form preserves a detached ROUTER route and is therefore
 * the delayed-reply entry.
 */
CXX_C_API int turbo_flow_fmq_app_send_message(turbo_flow_fmq_app_t *app,
                                              const turbo_flow_msg_t *message);

/** Replace application payload while retaining FMQ route/session context. */
CXX_C_API int turbo_flow_fmq_app_message_set_payload_copy(turbo_flow_msg_t *message,
                                                          const void *data, size_t data_size);

/** Stop when needed and release the facade-owned Flow generation. */
CXX_C_API void turbo_flow_fmq_app_destroy(turbo_flow_fmq_app_t *app);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_H */
