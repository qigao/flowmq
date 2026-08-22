#include "flowmq_router_endpoint.h"
#include "flowmq_stl_adapter.h"

#include "flowmq_coronet_transport.h"
#include "flowmq_pattern.h"
#include "flowmq_posted_send.h"
#include "flowmq_stream_decoder.h"
#include "CoroNet/turbo_coro_socket.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"
#include <turbostl/vec.h>

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

typedef struct flowmq_router_peer_s {
  flowmq_router_endpoint_t *endpoint;
  coro_socket_t *socket;
  flowmq_router_route_t route;
  tstr identity;
  tstr topic;
  flowmq_stream_decoder_t reader;
} flowmq_router_peer_t;

struct flowmq_router_endpoint_s {
  flowmq_router_endpoint_config_t config;
  coro_context_t *context;
  coro_socket_t *server;
  turbo_thread_t loop_thread;
  int loop_thread_started;
  turbo_vec_t peers;
  int peers_initialized;
  tstr host;
  tstr path;
  tstr topic;
  tstr identity;
  tstr multicast_group;
  tstr multicast_interface;
  tstr tls_ca_file;
  tstr tls_cert_file;
  tstr tls_key_file;
  tstr tls_key_password;
  flowmq_coronet_tls_server_config_t tls;
  turbo_kcp_config_t kcp_config;
  int kcp_configured;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  int sync_initialized;
  int start_done;
  int shutdown_done;
  int barrier_done;
  uint64_t endpoint_id;
  uint64_t generation;
  uint64_t next_session_id;
  atomic_int started;
  atomic_int state;
  atomic_int status;
  atomic_size_t connections_current;
  flowmq_posted_send_t posted_send;
};

static atomic_uint_fast64_t flowmq_router_next_endpoint_id = 0u;

void flowmq_router_endpoint_config_init(flowmq_router_endpoint_config_t *config) {
  if (!config) return;
  memset(config, 0, sizeof(*config));
  config->size = sizeof(*config);
  config->transport = FLOWMQ_TRANSPORT_TCP;
  config->max_frame_size = FLOWMQ_ROUTER_ENDPOINT_DEFAULT_MAX_FRAME_SIZE;
  config->max_connections = FLOWMQ_ROUTER_ENDPOINT_DEFAULT_MAX_CONNECTIONS;
  config->timeouts.timeout_ms = FLOWMQ_ROUTER_ENDPOINT_DEFAULT_TIMEOUT_MS;
  config->timeouts.set_flags = FLOWMQ_TIMEOUT_SET_DEFAULT;
  flowmq_coronet_timeouts_resolve(&config->timeouts,
                                  FLOWMQ_ROUTER_ENDPOINT_DEFAULT_TIMEOUT_MS);
  config->send_admission =
      (flowmq_send_admission_config_t)FLOWMQ_SEND_ADMISSION_CONFIG_INIT;
}

static int flowmq_router_execute_posted_send(void *owner, const void *route,
                                             size_t route_size,
                                             const char *encoded,
                                             size_t encoded_size) {
  if (!route || route_size != sizeof(flowmq_router_route_t)) return TURBO_EINVAL;
  return flowmq_router_endpoint_send(
      (flowmq_router_endpoint_t *)owner,
      *(const flowmq_router_route_t *)route, encoded, encoded_size);
}

static void flowmq_router_set_state(flowmq_router_endpoint_t *endpoint,
                                    flowmq_router_endpoint_state_t state, int status) {
  size_t connections =
      atomic_load_explicit(&endpoint->connections_current, memory_order_acquire);
  atomic_store_explicit(&endpoint->status, status, memory_order_relaxed);
  atomic_store_explicit(&endpoint->state, state, memory_order_release);
  turbo_mutex_lock(&endpoint->mutex);
  turbo_cond_broadcast(&endpoint->changed);
  turbo_mutex_unlock(&endpoint->mutex);
  if (endpoint->config.on_state)
    endpoint->config.on_state(endpoint->config.callback_ctx, state, status, connections);
}

static void flowmq_router_emit(flowmq_router_endpoint_t *endpoint,
                               flowmq_router_endpoint_event_kind_t kind, int status,
                               const flowmq_router_peer_t *peer) {
  flowmq_router_endpoint_event_t event;
  if (!endpoint->config.on_event) return;
  memset(&event, 0, sizeof(event));
  event.kind = kind;
  event.status = status;
  if (peer) {
    event.route = peer->route;
    event.peer_identity = tstr_to_v(peer->identity);
    event.peer_topic = tstr_to_v(peer->topic);
  }
  event.connections_current =
      atomic_load_explicit(&endpoint->connections_current, memory_order_acquire);
  endpoint->config.on_event(endpoint->config.callback_ctx, &event);
}

static flowmq_router_peer_t *flowmq_router_find_peer(flowmq_router_endpoint_t *endpoint,
                                                     flowmq_router_route_t route) {
  size_t count = turbo_vec_size(&endpoint->peers);
  if (route.endpoint_id != endpoint->endpoint_id || route.generation != endpoint->generation ||
      route.session_id == 0u)
    return NULL;
  for (size_t index = 0u; index < count; ++index) {
    flowmq_router_peer_t *const *slot =
        (flowmq_router_peer_t *const *)turbo_vec_at_const(&endpoint->peers, index);
    if (slot && *slot && (*slot)->route.session_id == route.session_id) return *slot;
  }
  return NULL;
}

static int flowmq_router_add_peer(flowmq_router_endpoint_t *endpoint,
                                  flowmq_router_peer_t *peer) {
  size_t count = turbo_vec_size(&endpoint->peers);
  if (count >= endpoint->config.max_connections) return TURBO_ENOBUFS;
  for (size_t index = 0u; index < count; ++index) {
    flowmq_router_peer_t *const *slot =
        (flowmq_router_peer_t *const *)turbo_vec_at_const(&endpoint->peers, index);
    if (slot && *slot && tstr_cmp((*slot)->identity, peer->identity) == 0)
      return TURBO_EALREADY;
  }
  if (turbo_vec_push(&endpoint->peers, &peer) != TURBO_OK) return TURBO_ENOMEM;
  atomic_fetch_add_explicit(&endpoint->connections_current, 1u, memory_order_release);
  return TURBO_OK;
}

static void flowmq_router_remove_peer(flowmq_router_endpoint_t *endpoint,
                                      flowmq_router_peer_t *peer) {
  size_t count = turbo_vec_size(&endpoint->peers);
  for (size_t index = 0u; index < count; ++index) {
    flowmq_router_peer_t *const *slot =
        (flowmq_router_peer_t *const *)turbo_vec_at_const(&endpoint->peers, index);
    if (slot && *slot == peer) {
      (void)turbo_vec_swap_remove(&endpoint->peers, index, NULL);
      atomic_fetch_sub_explicit(&endpoint->connections_current, 1u, memory_order_release);
      return;
    }
  }
}

static int flowmq_router_reader_next(flowmq_router_peer_t *peer, uint64_t receive_deadline_ns,
                                     flowmq_protocol_frame_t *frame, size_t *consumed) {
  flowmq_router_endpoint_t *endpoint = peer->endpoint;
  int rc = flowmq_stream_decoder_prepare(&peer->reader, endpoint->config.max_frame_size);
  if (rc != TURBO_OK) return rc;
  for (;;) {
    char *chunk = NULL;
    size_t chunk_size = 0u;
    rc = flowmq_stream_decoder_next(&peer->reader, frame, consumed);
    if (rc != FLOWMQ_PROTOCOL_INCOMPLETE) return rc;
    if (receive_deadline_ns != 0u) {
      uint64_t now_ns = turbo_hrtime();
      uint64_t remaining_ns;
      uint64_t timeout_ms;
      if (receive_deadline_ns <= now_ns) return TURBO_ETIMEDOUT;
      remaining_ns = receive_deadline_ns - now_ns;
      timeout_ms = remaining_ns > UINT64_MAX - UINT64_C(999999)
                       ? UINT64_MAX / UINT64_C(1000000)
                       : (remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
      (void)coro_socket_set_timeout(peer->socket, timeout_ms ? timeout_ms : 1u);
    }
    rc = coro_socket_recv(peer->socket, &chunk, &chunk_size);
    if (rc != TURBO_OK) return rc;
    if (!chunk || chunk_size == 0u) {
      if (chunk) coro_socket_free_recv(chunk);
      return TURBO_EOF;
    }
    if (chunk_size > flowmq_stream_decoder_available(&peer->reader)) {
      coro_socket_free_recv(chunk);
      return TURBO_EMSGSIZE;
    }
    rc = flowmq_stream_decoder_append(&peer->reader, chunk, chunk_size);
    coro_socket_free_recv(chunk);
    if (rc != TURBO_OK) return rc;
  }
}

static int flowmq_router_socket_send(flowmq_router_peer_t *peer, const char *data, size_t size) {
  return flowmq_coronet_transport_send(peer->socket, peer->endpoint->config.transport,
                                       &peer->endpoint->config.timeouts, data, size);
}

static int flowmq_router_send_hello(flowmq_router_peer_t *peer) {
  flowmq_router_endpoint_t *endpoint = peer->endpoint;
  tstr encoded = NULL;
  int rc = flowmq_pattern_encode_hello(
      FLOWMQ_PROTOCOL_ROUTER, tstr_to_v(endpoint->identity), tstr_to_v(endpoint->topic),
      endpoint->config.max_frame_size, &encoded);
  if (rc == TURBO_OK) rc = flowmq_router_socket_send(peer, encoded, tstr_len(encoded));
  tstr_freep(&encoded);
  return rc;
}

static int flowmq_router_send_heartbeat(flowmq_router_peer_t *peer,
                                        flowmq_protocol_frame_kind_t kind) {
  tstr encoded = NULL;
  int rc = flowmq_pattern_encode_heartbeat(FLOWMQ_PROTOCOL_ROUTER, kind,
                                           peer->endpoint->config.max_frame_size, &encoded);
  if (rc == TURBO_OK) rc = flowmq_router_socket_send(peer, encoded, tstr_len(encoded));
  tstr_freep(&encoded);
  return rc;
}

static int flowmq_router_read_hello(flowmq_router_peer_t *peer) {
  flowmq_protocol_frame_t hello;
  flowmq_protocol_security_t security;
  char certificate_sha256[CORO_TLS_PEER_CERT_SHA256_CAPACITY] = {0};
  size_t consumed = 0u;
  int rc;
  memset(&hello, 0, sizeof(hello));
  rc = flowmq_router_reader_next(peer, 0u, &hello, &consumed);
  if (rc == TURBO_OK) rc = flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello);
  if (rc == TURBO_OK && hello.pattern != FLOWMQ_PROTOCOL_DEALER) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    rc = flowmq_protocol_security_decode(hello.payload, &security);
    if (rc == TURBO_OK && security.mode != FLOWMQ_PROTOCOL_SECURITY_NONE) rc = TURBO_EPERM;
  }
  if (rc == TURBO_OK && peer->endpoint->config.verify_peer_identity) {
    rc = coro_socket_tls_get_verified_peer_certificate_sha256(
        peer->socket, certificate_sha256);
    if (rc == TURBO_OK)
      rc = peer->endpoint->config.verify_peer_identity(
          peer->endpoint->config.verify_peer_identity_ctx, certificate_sha256,
          hello.identity);
    if (rc != TURBO_OK) rc = TURBO_EPERM;
  }
  if (rc == TURBO_OK) {
    peer->identity = tstr_from_v(hello.identity);
    peer->topic = tstr_from_v(hello.topic);
    if (!peer->identity || !peer->topic) rc = TURBO_ENOMEM;
  }
  if (rc == TURBO_OK) rc = flowmq_stream_decoder_consume(&peer->reader, consumed);
  flowmq_protocol_frame_cleanup(&hello);
  memset(certificate_sha256, 0, sizeof(certificate_sha256));
  return rc;
}

static int flowmq_router_receive(flowmq_router_peer_t *peer) {
  flowmq_router_endpoint_t *endpoint = peer->endpoint;
  flowmq_protocol_heartbeat_deadlines_t heartbeat;
  int heartbeat_enabled = endpoint->config.heartbeat_interval_ms != 0u &&
                          endpoint->config.heartbeat_timeout_ms != 0u;
  flowmq_protocol_heartbeat_deadlines_init(
      &heartbeat, turbo_hrtime(), endpoint->config.heartbeat_interval_ms,
      endpoint->config.heartbeat_timeout_ms, endpoint->config.timeouts.recv_timeout_ms);
  while (atomic_load_explicit(&endpoint->started, memory_order_acquire)) {
    flowmq_protocol_frame_t frame;
    size_t consumed = 0u;
    uint64_t receive_deadline_ns = 0u;
    int rc;
    memset(&frame, 0, sizeof(frame));
    if (heartbeat_enabled) {
      for (;;) {
        uint64_t now_ns = turbo_hrtime();
        flowmq_protocol_heartbeat_action_t action =
            flowmq_protocol_heartbeat_deadlines_next(&heartbeat, now_ns, &receive_deadline_ns);
        if (action == FLOWMQ_PROTOCOL_HEARTBEAT_EXPIRED ||
            action == FLOWMQ_PROTOCOL_HEARTBEAT_RECV_EXPIRED) {
          flowmq_router_emit(endpoint, FLOWMQ_ROUTER_EVENT_HEARTBEAT_TIMEOUT,
                             TURBO_ETIMEDOUT, peer);
          return TURBO_ETIMEDOUT;
        }
        if (action == FLOWMQ_PROTOCOL_HEARTBEAT_SEND_PING) {
          rc = flowmq_router_send_heartbeat(peer, FLOWMQ_PROTOCOL_FRAME_PING);
          if (rc != TURBO_OK) return rc;
          flowmq_protocol_heartbeat_deadlines_on_ping(&heartbeat, now_ns);
          continue;
        }
        break;
      }
    }
    rc = flowmq_router_reader_next(peer, receive_deadline_ns, &frame, &consumed);
    if (rc == TURBO_ETIMEDOUT && heartbeat_enabled) continue;
    if (rc != TURBO_OK) return rc;
    if (heartbeat_enabled)
      flowmq_protocol_heartbeat_deadlines_on_receive(&heartbeat, turbo_hrtime());
    if (!flowmq_patterns_compatible(FLOWMQ_PROTOCOL_ROUTER, frame.pattern)) {
      rc = TURBO_EPROTO;
    } else if (frame.kind == FLOWMQ_PROTOCOL_FRAME_PING) {
      rc = flowmq_router_send_heartbeat(peer, FLOWMQ_PROTOCOL_FRAME_PONG);
    } else if (frame.kind == FLOWMQ_PROTOCOL_FRAME_PONG) {
      rc = TURBO_OK;
    } else {
      rc = flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_ROUTER, &frame);
      if (rc == TURBO_OK && endpoint->config.on_frame)
        rc = endpoint->config.on_frame(endpoint->config.callback_ctx, &peer->route,
                                       tstr_to_v(peer->identity), tstr_to_v(peer->topic), &frame);
    }
    (void)flowmq_stream_decoder_consume(&peer->reader, consumed);
    flowmq_protocol_frame_cleanup(&frame);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_ESHUTDOWN;
}

static void flowmq_router_peer_handler(coro_socket_t *client, void *arg) {
  flowmq_router_endpoint_t *endpoint = (flowmq_router_endpoint_t *)arg;
  flowmq_router_peer_t *peer;
  int admitted = 0;
  int rc;
  if (!endpoint || !client ||
      !atomic_load_explicit(&endpoint->started, memory_order_acquire))
    return;
  peer = (flowmq_router_peer_t *)calloc(1, sizeof(*peer));
  if (!peer) return;
  peer->endpoint = endpoint;
  peer->socket = client;
  rc = flowmq_router_read_hello(peer);
  if (rc == TURBO_OK) {
    peer->route.endpoint_id = endpoint->endpoint_id;
    peer->route.generation = endpoint->generation;
    peer->route.session_id = endpoint->next_session_id++;
    if (endpoint->next_session_id == 0u) endpoint->next_session_id = 1u;
    rc = flowmq_router_add_peer(endpoint, peer);
    if (rc == TURBO_OK) admitted = 1;
  }
  if (rc == TURBO_OK) rc = flowmq_router_send_hello(peer);
  if (rc == TURBO_OK) {
    flowmq_router_emit(endpoint, FLOWMQ_ROUTER_EVENT_PEER_CONNECTED, TURBO_OK, peer);
    rc = flowmq_router_receive(peer);
  }
  if (admitted) {
    flowmq_router_remove_peer(endpoint, peer);
    flowmq_router_emit(endpoint, FLOWMQ_ROUTER_EVENT_PEER_DISCONNECTED, rc, peer);
  }
  flowmq_stream_decoder_destroy(&peer->reader);
  tstr_freep(&peer->identity);
  tstr_freep(&peer->topic);
  free(peer);
}

static void flowmq_router_start_post(void *arg1, void *arg2) {
  flowmq_router_endpoint_t *endpoint = (flowmq_router_endpoint_t *)arg1;
  size_t encoded_limit = 0u;
  int rc = TURBO_ESHUTDOWN;
  (void)arg2;
  if (!atomic_load_explicit(&endpoint->started, memory_order_acquire)) goto done;
  endpoint->server =
      flowmq_coronet_transport_create(endpoint->context, endpoint->config.transport, 1);
  if (!endpoint->server) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = flowmq_coronet_transport_apply(
      endpoint->server, endpoint->config.transport, &endpoint->kcp_config,
      endpoint->kcp_configured, &endpoint->config.socket_options);
  if (rc == TURBO_OK)
    rc = coro_socket_set_server_admission_limit(endpoint->server,
                                                 endpoint->config.max_connections);
  if (rc == TURBO_OK && (endpoint->config.transport == FLOWMQ_TRANSPORT_WS ||
                         endpoint->config.transport == FLOWMQ_TRANSPORT_WSS)) {
    coro_ws_server_config_t ws = CORO_WS_SERVER_CONFIG_DEFAULT;
    rc = flowmq_protocol_encoded_size_limit(endpoint->config.max_frame_size, &encoded_limit);
    if (rc == TURBO_OK) {
      ws.path = endpoint->path[0] ? endpoint->path : "/";
      ws.max_message_size = encoded_limit;
      ws.binary_only = 1;
      rc = coro_socket_set_ws_server_config(endpoint->server, &ws);
    }
  }
  if (rc == TURBO_OK && endpoint->config.tls) {
    const turbo_tls_server_config_t tls = {
        sizeof(tls), endpoint->tls.cert_file, endpoint->tls.key_file,
        endpoint->tls.key_password, endpoint->tls.ca_file, NULL,
        endpoint->tls.require_client_certificate ? TURBO_TLS_CLIENT_AUTH_REQUIRED
                                                 : TURBO_TLS_CLIENT_AUTH_NONE,
        NULL, 0u};
    rc = coro_socket_set_tls_server_config(endpoint->server, &tls);
  }
  if (rc == TURBO_OK) {
    rc = flowmq_coronet_transport_listen(
        endpoint->server, endpoint->config.transport, endpoint->host, endpoint->config.port,
        endpoint->path, &endpoint->config.timeouts, &endpoint->config.udp_options,
        endpoint->config.reuse_port, flowmq_router_peer_handler, endpoint);
  }
done:
  if (rc != TURBO_OK && endpoint->server) {
    coro_socket_destroy(endpoint->server);
    endpoint->server = NULL;
  }
  flowmq_router_set_state(endpoint,
                          rc == TURBO_OK ? FLOWMQ_ROUTER_ENDPOINT_LISTENING
                                         : FLOWMQ_ROUTER_ENDPOINT_FAILED,
                          rc);
  turbo_mutex_lock(&endpoint->mutex);
  endpoint->start_done = 1;
  turbo_cond_broadcast(&endpoint->changed);
  turbo_mutex_unlock(&endpoint->mutex);
}

static void flowmq_router_shutdown_task(coro_t *co, void *arg) {
  flowmq_router_endpoint_t *endpoint = (flowmq_router_endpoint_t *)arg;
  (void)co;
  if (endpoint->server) {
    (void)coro_socket_server_stop(endpoint->server);
    while (!coro_socket_server_is_stopped(endpoint->server)) coro_sleep(endpoint->context, 1u);
    (void)flowmq_coronet_transport_leave_multicast(
        endpoint->server, endpoint->config.transport, &endpoint->config.udp_options);
    coro_socket_destroy(endpoint->server);
    endpoint->server = NULL;
  }
  turbo_mutex_lock(&endpoint->mutex);
  endpoint->shutdown_done = 1;
  turbo_cond_broadcast(&endpoint->changed);
  turbo_mutex_unlock(&endpoint->mutex);
}

static void flowmq_router_shutdown_post(void *arg1, void *arg2) {
  flowmq_router_endpoint_t *endpoint = (flowmq_router_endpoint_t *)arg1;
  int rc;
  (void)arg2;
  rc = coro_context_spawn(endpoint->context, flowmq_router_shutdown_task, endpoint);
  if (rc == TURBO_OK) return;
  flowmq_router_set_state(endpoint, FLOWMQ_ROUTER_ENDPOINT_FAILED, rc);
  turbo_mutex_lock(&endpoint->mutex);
  endpoint->shutdown_done = 1;
  turbo_cond_broadcast(&endpoint->changed);
  turbo_mutex_unlock(&endpoint->mutex);
}

static void flowmq_router_loop(void *arg) {
  flowmq_router_endpoint_t *endpoint = (flowmq_router_endpoint_t *)arg;
  (void)coro_context_run(endpoint->context, TURBO_RUN_DEFAULT);
}

static void flowmq_router_context_stop_post(void *arg1, void *arg2) {
  (void)arg2;
  coro_context_stop((coro_context_t *)arg1);
}

static void flowmq_router_barrier_post(void *arg1, void *arg2) {
  flowmq_router_endpoint_t *endpoint = (flowmq_router_endpoint_t *)arg1;
  (void)arg2;
  turbo_mutex_lock(&endpoint->mutex);
  endpoint->barrier_done = 1;
  turbo_cond_broadcast(&endpoint->changed);
  turbo_mutex_unlock(&endpoint->mutex);
}

int flowmq_router_endpoint_create(const flowmq_router_endpoint_config_t *config,
                                  flowmq_router_endpoint_t **out) {
  flowmq_router_endpoint_config_t normalized;
  flowmq_router_endpoint_t *endpoint;
  size_t config_size;
  size_t copy_size;
  size_t encoded_limit;
  int rc;
  if (!config || !out) return TURBO_EINVAL;
  *out = NULL;
  config_size = config->size;
  if (config_size < FLOWMQ_ROUTER_ENDPOINT_CONFIG_V3_SIZE) return TURBO_EINVAL;
  flowmq_router_endpoint_config_init(&normalized);
  copy_size = config_size < sizeof(normalized) ? config_size : sizeof(normalized);
  memcpy(&normalized, config, copy_size);
  if (config_size < sizeof(normalized) && normalized.verify_peer_identity)
    normalized.verify_peer_identity_ctx = normalized.callback_ctx;
  normalized.size = sizeof(normalized);
  config = &normalized;
  if (!config->host || !config->path || !config->topic ||
      !config->identity || config->max_frame_size == 0u ||
      config->max_connections == 0u ||
      config->max_connections > FLOWMQ_ROUTER_ENDPOINT_MAX_CONNECTIONS ||
      flowmq_coronet_transport_validate(config->transport) != TURBO_OK ||
      flowmq_coronet_endpoint_validate(config->transport, config->host, config->port,
                                       config->path) != TURBO_OK ||
      flowmq_coronet_server_config_validate(config->transport, &config->timeouts,
                                            &config->socket_options, &config->udp_options,
                                            config->tls) != TURBO_OK ||
      (config->verify_peer_identity &&
       (!config->tls || !config->tls->require_client_certificate)) ||
      (!config->verify_peer_identity && config->verify_peer_identity_ctx) ||
      (config->reuse_port != 0 && config->reuse_port != 1) ||
      (config->drive_context != 0 && config->drive_context != 1) ||
      (config->own_context != 0 && config->own_context != 1) ||
      (config->drive_context && !config->own_context) ||
      (!config->context && (!config->drive_context || !config->own_context)))
    return TURBO_EINVAL;
  rc = flowmq_protocol_encoded_size_limit(config->max_frame_size, &encoded_limit);
  if (rc != TURBO_OK) return rc;
  (void)encoded_limit;
  endpoint = (flowmq_router_endpoint_t *)calloc(1, sizeof(*endpoint));
  if (!endpoint) return TURBO_ENOMEM;
  atomic_init(&endpoint->started, 0);
  atomic_init(&endpoint->state, FLOWMQ_ROUTER_ENDPOINT_STOPPED);
  atomic_init(&endpoint->status, TURBO_ENOTCONN);
  atomic_init(&endpoint->connections_current, 0u);
  endpoint->config = *config;
  endpoint->config.tls = NULL;
  endpoint->host = tstr_dup(config->host);
  endpoint->path = tstr_dup(config->path);
  endpoint->topic = tstr_dup(config->topic);
  endpoint->identity = tstr_dup(config->identity);
  endpoint->multicast_group =
      tstr_dup(config->udp_options.multicast_group ? config->udp_options.multicast_group : "");
  endpoint->multicast_interface = tstr_dup(config->udp_options.multicast_interface
                                                ? config->udp_options.multicast_interface
                                                : "");
  endpoint->tls_ca_file =
      tstr_dup(config->tls && config->tls->ca_file ? config->tls->ca_file : "");
  endpoint->tls_cert_file =
      tstr_dup(config->tls && config->tls->cert_file ? config->tls->cert_file : "");
  endpoint->tls_key_file =
      tstr_dup(config->tls && config->tls->key_file ? config->tls->key_file : "");
  endpoint->tls_key_password =
      tstr_dup(config->tls && config->tls->key_password ? config->tls->key_password : "");
  if (!endpoint->host || !endpoint->path || !endpoint->topic || !endpoint->identity ||
      !endpoint->multicast_group || !endpoint->multicast_interface || !endpoint->tls_ca_file ||
      !endpoint->tls_cert_file || !endpoint->tls_key_file || !endpoint->tls_key_password) {
    flowmq_router_endpoint_destroy(endpoint);
    return TURBO_ENOMEM;
  }
  endpoint->config.host = endpoint->host;
  endpoint->config.path = endpoint->path;
  endpoint->config.topic = endpoint->topic;
  endpoint->config.identity = endpoint->identity;
  endpoint->config.udp_options.multicast_group = endpoint->multicast_group;
  endpoint->config.udp_options.multicast_interface = endpoint->multicast_interface;
  if (config->tls) {
    endpoint->tls.ca_file = config->tls->ca_file ? endpoint->tls_ca_file : NULL;
    endpoint->tls.cert_file = endpoint->tls_cert_file;
    endpoint->tls.key_file = endpoint->tls_key_file;
    endpoint->tls.key_password = config->tls->key_password ? endpoint->tls_key_password : NULL;
    endpoint->tls.require_client_certificate = config->tls->require_client_certificate;
    endpoint->config.tls = &endpoint->tls;
  }
  rc = flowmq_coronet_transport_kcp_resolve(config->transport, &config->kcp_options,
                                             &endpoint->kcp_config, &endpoint->kcp_configured);
  if (rc != TURBO_OK) {
    flowmq_router_endpoint_destroy(endpoint);
    return rc;
  }
  endpoint->context = config->context ? config->context : coro_context_create(NULL);
  if (!endpoint->context) {
    flowmq_router_endpoint_destroy(endpoint);
    return TURBO_ENOMEM;
  }
  if (config->stream_recv_buffer_bytes != 0u) {
    rc = coro_context_set_stream_recv_buffer_size(endpoint->context,
                                                  config->stream_recv_buffer_bytes);
    if (rc != TURBO_OK) {
      flowmq_router_endpoint_destroy(endpoint);
      return rc;
    }
  }
  if (config->drive_context) coro_context_set_persistent(endpoint->context, 1);
  turbo_mutex_init(&endpoint->mutex);
  turbo_cond_init(&endpoint->changed);
  endpoint->sync_initialized = 1;
  rc = turbo_vec_init_bytes(&endpoint->peers, sizeof(flowmq_router_peer_t *),
                            _Alignof(flowmq_router_peer_t *), config->max_connections);
  if (rc == TURBO_STL_OK) endpoint->peers_initialized = 1;
  if (rc == TURBO_STL_OK) rc = turbo_vec_reserve(&endpoint->peers, config->max_connections);
  if (rc == TURBO_STL_OK)
    rc = flowmq_posted_send_init(&endpoint->posted_send, endpoint->context,
                                 endpoint, flowmq_router_execute_posted_send,
                                 &config->send_admission);
  else
    rc = flowmq_stl_status_to_error((turbo_stl_status)rc);
  if (rc != TURBO_OK) {
    flowmq_router_endpoint_destroy(endpoint);
    return rc;
  }
  endpoint->endpoint_id = atomic_fetch_add_explicit(
                              &flowmq_router_next_endpoint_id, 1u, memory_order_relaxed) +
                          1u;
  if (endpoint->endpoint_id == 0u)
    endpoint->endpoint_id = atomic_fetch_add_explicit(
                                &flowmq_router_next_endpoint_id, 1u, memory_order_relaxed) +
                            1u;
  endpoint->next_session_id = 1u;
  *out = endpoint;
  return TURBO_OK;
}

int flowmq_router_endpoint_start(flowmq_router_endpoint_t *endpoint, uint64_t timeout_ns) {
  uint64_t deadline_ns;
  int rc;
  if (!endpoint || timeout_ns == 0u) return TURBO_EINVAL;
  if (atomic_exchange_explicit(&endpoint->started, 1, memory_order_acq_rel)) return TURBO_OK;
  endpoint->generation++;
  if (endpoint->generation == 0u) endpoint->generation = 1u;
  turbo_mutex_lock(&endpoint->mutex);
  endpoint->start_done = 0;
  endpoint->shutdown_done = 0;
  turbo_mutex_unlock(&endpoint->mutex);
  flowmq_router_set_state(endpoint, FLOWMQ_ROUTER_ENDPOINT_STARTING, TURBO_EALREADY);
  if (endpoint->config.drive_context && !endpoint->loop_thread_started) {
    coro_context_set_persistent(endpoint->context, 1);
    rc = turbo_thread_create(&endpoint->loop_thread, flowmq_router_loop, endpoint);
    if (rc != TURBO_OK) {
      atomic_store_explicit(&endpoint->started, 0, memory_order_release);
      flowmq_router_set_state(endpoint, FLOWMQ_ROUTER_ENDPOINT_FAILED, rc);
      return rc;
    }
    endpoint->loop_thread_started = 1;
  }
  rc = coro_post(endpoint->context, flowmq_router_start_post, endpoint, NULL);
  if (rc != TURBO_OK) goto failed;
  deadline_ns = turbo_hrtime();
  deadline_ns = timeout_ns > UINT64_MAX - deadline_ns ? UINT64_MAX : deadline_ns + timeout_ns;
  turbo_mutex_lock(&endpoint->mutex);
  while (!endpoint->start_done) {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    (void)turbo_cond_timedwait(&endpoint->changed, &endpoint->mutex, deadline_ns - now_ns);
  }
  rc = endpoint->start_done ? atomic_load_explicit(&endpoint->status, memory_order_acquire)
                            : TURBO_ETIMEDOUT;
  turbo_mutex_unlock(&endpoint->mutex);
  if (rc == TURBO_OK) {
    flowmq_posted_send_start(&endpoint->posted_send);
    return TURBO_OK;
  }
failed:
  flowmq_router_endpoint_stop(endpoint);
  return rc;
}

void flowmq_router_endpoint_stop(flowmq_router_endpoint_t *endpoint) {
  if (endpoint) flowmq_posted_send_stop(&endpoint->posted_send);
  if (!endpoint || !atomic_exchange_explicit(&endpoint->started, 0, memory_order_acq_rel)) return;
  turbo_mutex_lock(&endpoint->mutex);
  endpoint->shutdown_done = 0;
  turbo_mutex_unlock(&endpoint->mutex);
  if (coro_post(endpoint->context, flowmq_router_shutdown_post, endpoint, NULL) == TURBO_OK) {
    turbo_mutex_lock(&endpoint->mutex);
    while (!endpoint->shutdown_done) turbo_cond_wait(&endpoint->changed, &endpoint->mutex);
    turbo_mutex_unlock(&endpoint->mutex);
  }
  if (endpoint->config.drive_context && endpoint->loop_thread_started) {
    turbo_mutex_lock(&endpoint->mutex);
    endpoint->barrier_done = 0;
    turbo_mutex_unlock(&endpoint->mutex);
    if (coro_post(endpoint->context, flowmq_router_barrier_post, endpoint, NULL) == TURBO_OK) {
      turbo_mutex_lock(&endpoint->mutex);
      while (!endpoint->barrier_done) turbo_cond_wait(&endpoint->changed, &endpoint->mutex);
      turbo_mutex_unlock(&endpoint->mutex);
    }
    if (coro_post(endpoint->context, flowmq_router_context_stop_post, endpoint->context, NULL) !=
        TURBO_OK)
      coro_context_stop(endpoint->context);
    coro_context_set_persistent(endpoint->context, 0);
    (void)turbo_thread_join(&endpoint->loop_thread);
    endpoint->loop_thread_started = 0;
  }
  flowmq_router_set_state(endpoint, FLOWMQ_ROUTER_ENDPOINT_STOPPED, TURBO_ESHUTDOWN);
}

void flowmq_router_endpoint_destroy(flowmq_router_endpoint_t *endpoint) {
  if (!endpoint) return;
  flowmq_router_endpoint_stop(endpoint);
  flowmq_posted_send_destroy(&endpoint->posted_send);
  if (endpoint->peers_initialized) turbo_vec_destroy(&endpoint->peers);
  if (endpoint->sync_initialized) {
    turbo_cond_destroy(&endpoint->changed);
    turbo_mutex_destroy(&endpoint->mutex);
  }
  if (endpoint->config.drive_context && endpoint->context)
    coro_context_set_persistent(endpoint->context, 0);
  if (endpoint->config.own_context && endpoint->context) coro_context_destroy(endpoint->context);
  tstr_freep(&endpoint->host);
  tstr_freep(&endpoint->path);
  tstr_freep(&endpoint->topic);
  tstr_freep(&endpoint->identity);
  tstr_freep(&endpoint->multicast_group);
  tstr_freep(&endpoint->multicast_interface);
  tstr_freep(&endpoint->tls_ca_file);
  tstr_freep(&endpoint->tls_cert_file);
  tstr_freep(&endpoint->tls_key_file);
  if (endpoint->tls_key_password) {
    volatile unsigned char *bytes = (volatile unsigned char *)endpoint->tls_key_password;
    size_t size = tstr_len(endpoint->tls_key_password);
    while (size--) *bytes++ = 0u;
  }
  tstr_freep(&endpoint->tls_key_password);
  turbo_kcp_config_wipe(&endpoint->kcp_config);
  free(endpoint);
}

coro_context_t *flowmq_router_endpoint_context(flowmq_router_endpoint_t *endpoint) {
  return endpoint ? endpoint->context : NULL;
}

int flowmq_router_endpoint_send(flowmq_router_endpoint_t *endpoint, flowmq_router_route_t route,
                                const char *encoded, size_t encoded_size) {
  flowmq_router_peer_t *peer;
  if (!endpoint || !encoded || encoded_size == 0u) return TURBO_EINVAL;
  if (coro_context_current() != endpoint->context) return TURBO_EINVAL;
  if (!atomic_load_explicit(&endpoint->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  peer = flowmq_router_find_peer(endpoint, route);
  if (!peer) return TURBO_ENOTCONN;
  return flowmq_router_socket_send(peer, encoded, encoded_size);
}

int flowmq_router_endpoint_send_copy(flowmq_router_endpoint_t *endpoint,
                                     flowmq_router_route_t route,
                                     uint64_t completion_id,
                                     const char *encoded,
                                     size_t encoded_size) {
  if (!endpoint) return TURBO_EINVAL;
  return flowmq_posted_send_copy(&endpoint->posted_send, &route,
                                 sizeof(route), completion_id, encoded,
                                 encoded_size);
}

void flowmq_router_endpoint_send_stats(
    flowmq_router_endpoint_t *endpoint,
    flowmq_send_admission_stats_t *stats) {
  flowmq_posted_send_get_stats(endpoint ? &endpoint->posted_send : NULL, stats);
}

size_t flowmq_router_endpoint_connections(const flowmq_router_endpoint_t *endpoint) {
  return endpoint ? atomic_load_explicit(&endpoint->connections_current, memory_order_acquire) : 0u;
}
