#include "flowmq_connect_endpoint.h"

#include "flowmq_coronet_transport.h"
#include "platform.h"
#include "flowmq_pattern.h"
#include "flowmq_peer_session.h"
#include "flowmq_posted_send.h"
#include "flowmq_reconnect.h"
#include "flowmq_stream_decoder.h"
#include "flowmq_subscription_set.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

struct flowmq_connect_endpoint_s {
  flowmq_connect_endpoint_config_t config;
  coro_context_t *context;
  coro_socket_t *socket;
  coro_wait_t *reconnect_wait;
  turbo_thread_t loop_thread;
  int loop_thread_started;
  flowmq_reconnect_t reconnect;
  flowmq_subscription_set_t subscriptions;
  flowmq_peer_session_t session;
  tstr_t host;
  tstr_t path;
  tstr_t topic;
  tstr_t identity;
  tstr_t multicast_group;
  tstr_t multicast_interface;
  tstr_t tls_ca_file;
  tstr_t tls_cert_file;
  tstr_t tls_key_file;
  tstr_t tls_key_password;
  tstr_t tls_server_name;
  flowmq_coronet_tls_client_config_t tls;
  turbo_kcp_config_t kcp_config;
  int kcp_configured;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  int sync_initialized;
  int subscriptions_initialized;
  int task_active;
  int barrier_done;
  atomic_int reconnect_failure_reported;
  atomic_int started;
  atomic_int state;
  atomic_int status;
  flowmq_posted_send_t posted_send;
};

void flowmq_connect_endpoint_config_init(flowmq_connect_endpoint_config_t *config) {
  if (!config) return;
  memset(config, 0, sizeof(*config));
  config->size = sizeof(*config);
  config->transport = FLOWMQ_TRANSPORT_TCP;
  config->max_frame_size = FLOWMQ_CONNECT_ENDPOINT_DEFAULT_MAX_FRAME_SIZE;
  config->timeouts.timeout_ms = FLOWMQ_CONNECT_ENDPOINT_DEFAULT_TIMEOUT_MS;
  config->timeouts.set_flags = FLOWMQ_TIMEOUT_SET_DEFAULT;
  flowmq_coronet_timeouts_resolve(&config->timeouts,
                                  FLOWMQ_CONNECT_ENDPOINT_DEFAULT_TIMEOUT_MS);
  config->reconnect_initial_ms = FLOWMQ_CONNECT_ENDPOINT_DEFAULT_RECONNECT_INITIAL_MS;
  config->reconnect_max_ms = FLOWMQ_CONNECT_ENDPOINT_DEFAULT_RECONNECT_MAX_MS;
  config->send_admission =
      (flowmq_send_admission_config_t)FLOWMQ_SEND_ADMISSION_CONFIG_INIT;
}

static int flowmq_connect_endpoint_execute_posted_send(
    void *owner, const void *route, size_t route_size, const char *encoded,
    size_t encoded_size) {
  (void)route;
  if (route_size != 0u) return TURBO_EINVAL;
  return flowmq_connect_endpoint_send((flowmq_connect_endpoint_t *)owner,
                                      encoded, encoded_size);
}

static void flowmq_connect_endpoint_set_state(flowmq_connect_endpoint_t *endpoint,
                                              flowmq_connect_endpoint_connection_state_t state,
                                              int status, size_t connections_current) {
  atomic_store_explicit(&endpoint->status, status, memory_order_relaxed);
  atomic_store_explicit(&endpoint->state, state, memory_order_release);
  turbo_mutex_lock(&endpoint->mutex);
  turbo_cond_broadcast(&endpoint->changed);
  turbo_mutex_unlock(&endpoint->mutex);
  if (endpoint->config.on_state) {
    endpoint->config.on_state(endpoint->config.callback_ctx, state, status, connections_current);
  }
}

static void flowmq_connect_endpoint_emit(flowmq_connect_endpoint_t *endpoint,
                                         flowmq_connect_endpoint_event_kind_t kind, int status,
                                         uint64_t delay_ms, tstr_v identity, tstr_v topic) {
  flowmq_connect_endpoint_event_t event;
  if (!endpoint->config.on_event) return;
  memset(&event, 0, sizeof(event));
  event.kind = kind;
  event.status = status;
  event.delay_ms = delay_ms;
  event.peer_identity = identity;
  event.peer_topic = topic;
  endpoint->config.on_event(endpoint->config.callback_ctx, &event);
}

static void flowmq_connect_endpoint_emit_failure_once(flowmq_connect_endpoint_t *endpoint,
                                                      int status) {
  if (status == TURBO_OK || status == TURBO_ESHUTDOWN ||
      atomic_exchange_explicit(&endpoint->reconnect_failure_reported, 1, memory_order_acq_rel)) {
    return;
  }
  flowmq_connect_endpoint_emit(endpoint, FLOWMQ_ENDPOINT_EVENT_RECONNECT_FAILED, status, 0u,
                               (tstr_v){0}, (tstr_v){0});
}

static int flowmq_connect_endpoint_socket_send(flowmq_connect_endpoint_t *endpoint,
                                               const char *data, size_t size) {
  if (!endpoint->socket) return TURBO_ENOTCONN;
  return flowmq_coronet_transport_send(endpoint->socket, endpoint->config.transport,
                                       &endpoint->config.timeouts, data, size);
}

static int flowmq_connect_endpoint_send_hello(flowmq_connect_endpoint_t *endpoint) {
  tstr_t encoded = NULL;
  int rc;
  rc = flowmq_pattern_encode_hello(endpoint->config.pattern, tstr_to_v(endpoint->identity),
                                   tstr_to_v(endpoint->topic), endpoint->config.max_frame_size,
                                   &encoded);
  if (rc == TURBO_OK)
    rc = flowmq_connect_endpoint_socket_send(endpoint, encoded, tstr_len(encoded));
  tstr_freep(&encoded);
  return rc;
}

static int flowmq_connect_endpoint_send_heartbeat(flowmq_connect_endpoint_t *endpoint,
                                                  flowmq_protocol_frame_kind_t kind) {
  tstr_t encoded = NULL;
  int rc = flowmq_pattern_encode_heartbeat(endpoint->config.pattern, kind,
                                           endpoint->config.max_frame_size, &encoded);
  if (rc == TURBO_OK)
    rc = flowmq_connect_endpoint_socket_send(endpoint, encoded, tstr_len(encoded));
  tstr_freep(&encoded);
  return rc;
}

static int flowmq_connect_endpoint_send_subscription(flowmq_connect_endpoint_t *endpoint,
                                                     flowmq_protocol_frame_kind_t kind,
                                                     tstr_v topic) {
  tstr_t encoded = NULL;
  int rc = flowmq_pattern_encode_subscription(endpoint->config.pattern, kind, topic,
                                              endpoint->config.max_frame_size, &encoded);
  if (rc == TURBO_OK)
    rc = flowmq_connect_endpoint_socket_send(endpoint, encoded, tstr_len(encoded));
  tstr_freep(&encoded);
  return rc;
}

static int flowmq_connect_endpoint_reader_next(flowmq_connect_endpoint_t *endpoint,
                                               flowmq_stream_decoder_t *reader,
                                               uint64_t receive_deadline_ns,
                                               flowmq_protocol_frame_t *frame, size_t *consumed) {
  int rc = flowmq_stream_decoder_prepare(reader, endpoint->config.max_frame_size);
  if (rc != TURBO_OK) return rc;
  for (;;) {
    char *chunk = NULL;
    size_t chunk_size = 0u;
    rc = flowmq_stream_decoder_next(reader, frame, consumed);
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
      (void)coro_socket_set_timeout(endpoint->socket, timeout_ms ? timeout_ms : 1u);
    }
    rc = coro_socket_recv(endpoint->socket, &chunk, &chunk_size);
    if (rc != TURBO_OK) return rc;
    if (!chunk || chunk_size == 0u) {
      if (chunk) coro_socket_free_recv(chunk);
      return TURBO_EOF;
    }
    if (chunk_size > flowmq_stream_decoder_available(reader)) {
      coro_socket_free_recv(chunk);
      return TURBO_EMSGSIZE;
    }
    rc = flowmq_stream_decoder_append(reader, chunk, chunk_size);
    coro_socket_free_recv(chunk);
    if (rc != TURBO_OK) return rc;
  }
}

static int flowmq_connect_endpoint_read_hello(flowmq_connect_endpoint_t *endpoint,
                                              flowmq_stream_decoder_t *reader,
                                              flowmq_protocol_frame_t *hello, size_t *consumed) {
  int rc = flowmq_connect_endpoint_reader_next(endpoint, reader, 0u, hello, consumed);
  if (rc == TURBO_OK) rc = flowmq_pattern_hello_validate(endpoint->config.pattern, hello);
  if (rc == TURBO_OK) {
    flowmq_protocol_security_t security;
    rc = flowmq_protocol_security_decode(hello->payload, &security);
    if (rc == TURBO_OK && security.mode != FLOWMQ_PROTOCOL_SECURITY_NONE) rc = TURBO_EPERM;
  }
  if (rc != TURBO_OK) flowmq_protocol_frame_cleanup(hello);
  return rc;
}

static int flowmq_connect_endpoint_verify_peer_identity(
    flowmq_connect_endpoint_t *endpoint, tstr_v claimed_identity) {
  char certificate_sha256[CORO_TLS_PEER_CERT_SHA256_CAPACITY] = {0};
  int rc;
  if (!endpoint || !endpoint->config.verify_peer_identity) return TURBO_OK;
  rc = coro_socket_tls_get_verified_peer_certificate_sha256(
      endpoint->socket, certificate_sha256);
  if (rc == TURBO_OK) {
    rc = endpoint->config.verify_peer_identity(
        endpoint->config.verify_peer_identity_ctx, certificate_sha256,
        claimed_identity);
    if (rc != TURBO_OK) rc = TURBO_EPERM;
  }
  memset(certificate_sha256, 0, sizeof(certificate_sha256));
  return rc;
}

static int flowmq_connect_endpoint_replay_subscriptions(flowmq_connect_endpoint_t *endpoint) {
  size_t count = flowmq_subscription_set_count(&endpoint->subscriptions);
  for (size_t i = 0u; i < count; ++i) {
    const flowmq_subscription_t *subscription =
        flowmq_subscription_set_at(&endpoint->subscriptions, i);
    if (!subscription) continue;
    for (size_t ref = 0u; ref < subscription->refs; ++ref) {
      int rc = flowmq_connect_endpoint_send_subscription(endpoint, FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
                                                         tstr_to_v(subscription->topic));
      if (rc != TURBO_OK) return rc;
    }
  }
  return TURBO_OK;
}

static int flowmq_connect_endpoint_dispatch_frame(flowmq_connect_endpoint_t *endpoint,
                                                  const flowmq_protocol_frame_t *frame) {
  flowmq_peer_exchange_state_t state;
  uint64_t generation = 0u;
  uint64_t correlation_id = 0u;
  int rc = flowmq_pattern_data_direction_validate(endpoint->config.pattern, frame);
  if (rc != TURBO_OK) return rc;
  rc = flowmq_peer_session_snapshot(&endpoint->session, &state, &generation, &correlation_id);
  if (rc != TURBO_OK) return rc;
  if (endpoint->config.pattern == FLOWMQ_PROTOCOL_REQ) {
    rc = flowmq_peer_session_match(&endpoint->session, generation, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY,
                                   frame->message_id);
    if (rc != TURBO_OK) return rc;
  }
  if (endpoint->config.on_frame) {
    rc = endpoint->config.on_frame(endpoint->config.callback_ctx, frame, generation);
  } else {
    rc = TURBO_OK;
  }
  if (endpoint->config.pattern == FLOWMQ_PROTOCOL_REQ) {
    int finish_rc = flowmq_peer_session_finish(
        &endpoint->session, generation, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY, frame->message_id,
        rc == TURBO_OK ? FLOWMQ_PEER_EXCHANGE_READY : FLOWMQ_PEER_EXCHANGE_RESETTING);
    if (rc == TURBO_OK) rc = finish_rc;
  }
  return rc;
}

static int flowmq_connect_endpoint_receive(flowmq_connect_endpoint_t *endpoint,
                                           flowmq_stream_decoder_t *reader) {
  flowmq_protocol_heartbeat_deadlines_t heartbeat;
  int heartbeat_enabled =
      endpoint->config.heartbeat_interval_ms != 0u && endpoint->config.heartbeat_timeout_ms != 0u;
  flowmq_protocol_heartbeat_deadlines_init(
      &heartbeat, turbo_hrtime(), endpoint->config.heartbeat_interval_ms,
      endpoint->config.heartbeat_timeout_ms, endpoint->config.timeouts.recv_timeout_ms);
  while (atomic_load_explicit(&endpoint->started, memory_order_acquire) &&
         endpoint->config.receive_ready &&
         !endpoint->config.receive_ready(endpoint->config.callback_ctx)) {
    coro_sleep(endpoint->context, 1u);
  }
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
        if (action == FLOWMQ_PROTOCOL_HEARTBEAT_EXPIRED) {
          flowmq_connect_endpoint_emit(endpoint, FLOWMQ_ENDPOINT_EVENT_HEARTBEAT_TIMEOUT,
                                       TURBO_ETIMEDOUT, 0u, (tstr_v){0}, (tstr_v){0});
          return TURBO_ETIMEDOUT;
        }
        if (action == FLOWMQ_PROTOCOL_HEARTBEAT_RECV_EXPIRED) return TURBO_ETIMEDOUT;
        if (action == FLOWMQ_PROTOCOL_HEARTBEAT_SEND_PING) {
          rc = flowmq_connect_endpoint_send_heartbeat(endpoint, FLOWMQ_PROTOCOL_FRAME_PING);
          if (rc != TURBO_OK) return rc;
          flowmq_protocol_heartbeat_deadlines_on_ping(&heartbeat, now_ns);
          continue;
        }
        break;
      }
    }
    rc = flowmq_connect_endpoint_reader_next(endpoint, reader, receive_deadline_ns, &frame,
                                             &consumed);
    if (rc == TURBO_ETIMEDOUT && heartbeat_enabled) continue;
    if (rc != TURBO_OK) return rc;
    if (heartbeat_enabled) {
      flowmq_protocol_heartbeat_deadlines_on_receive(&heartbeat, turbo_hrtime());
    }
    if (!flowmq_patterns_compatible(endpoint->config.pattern, frame.pattern)) {
      rc = TURBO_EPROTO;
    } else if (frame.kind == FLOWMQ_PROTOCOL_FRAME_PING) {
      rc = flowmq_connect_endpoint_send_heartbeat(endpoint, FLOWMQ_PROTOCOL_FRAME_PONG);
    } else if (frame.kind == FLOWMQ_PROTOCOL_FRAME_PONG) {
      rc = TURBO_OK;
    } else if (frame.kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE ||
               frame.kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE) {
      rc = TURBO_EPROTO;
    } else {
      rc = flowmq_connect_endpoint_dispatch_frame(endpoint, &frame);
    }
    (void)flowmq_stream_decoder_consume(reader, consumed);
    flowmq_protocol_frame_cleanup(&frame);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_ESHUTDOWN;
}

static void flowmq_connect_endpoint_task_done(flowmq_connect_endpoint_t *endpoint) {
  turbo_mutex_lock(&endpoint->mutex);
  endpoint->task_active = 0;
  turbo_cond_broadcast(&endpoint->changed);
  turbo_mutex_unlock(&endpoint->mutex);
}

static void flowmq_connect_endpoint_connect_task(coro_t *co, void *arg) {
  flowmq_connect_endpoint_t *endpoint = (flowmq_connect_endpoint_t *)arg;
  int rc = TURBO_ESHUTDOWN;
  (void)co;
  for (;;) {
    flowmq_stream_decoder_t reader;
    flowmq_protocol_frame_t hello;
    size_t consumed = 0u;
    uint64_t wait_ms = 0u;
    int attempt_status;
    memset(&reader, 0, sizeof(reader));
    memset(&hello, 0, sizeof(hello));
    if (!atomic_load_explicit(&endpoint->started, memory_order_acquire)) break;
    flowmq_connect_endpoint_set_state(endpoint, FLOWMQ_ENDPOINT_CONNECTION_CONNECTING,
                                      TURBO_EALREADY, 0u);
    endpoint->socket =
        flowmq_coronet_transport_create(endpoint->context, endpoint->config.transport, 0);
    if (!endpoint->socket) {
      rc = TURBO_ENOMEM;
      goto attempt_done;
    }
    rc = flowmq_coronet_transport_apply(
        endpoint->socket, endpoint->config.transport, &endpoint->kcp_config,
        endpoint->kcp_configured,
        &endpoint->config.socket_options);
    if (rc == TURBO_OK && endpoint->config.tls) {
      const flowmq_coronet_tls_client_config_t *tls = endpoint->config.tls;
      const turbo_tls_client_config_t client = {
          tls->ca_file, tls->cert_file, tls->key_file, tls->key_password, NULL,
          tls->verify_peer};
      rc = coro_socket_set_tls_client_config(endpoint->socket, &client);
    }
    if (rc == TURBO_OK) {
      rc = flowmq_coronet_transport_connect(
          endpoint->socket, endpoint->config.transport, endpoint->host, endpoint->config.port,
          endpoint->path,
          endpoint->config.tls ? endpoint->config.tls->server_name : NULL,
          &endpoint->config.timeouts, &endpoint->config.udp_options);
    }
    if (rc == TURBO_OK) rc = flowmq_connect_endpoint_send_hello(endpoint);
    if (rc == TURBO_OK)
      rc = flowmq_connect_endpoint_read_hello(endpoint, &reader, &hello, &consumed);
    if (rc == TURBO_OK)
      rc = flowmq_connect_endpoint_verify_peer_identity(endpoint, hello.identity);
    if (rc == TURBO_OK) {
      (void)flowmq_stream_decoder_consume(&reader, consumed);
      if ((endpoint->config.pattern == FLOWMQ_PROTOCOL_SUB ||
           (endpoint->config.pattern == FLOWMQ_PROTOCOL_XSUB &&
            endpoint->config.initial_subscription_configured)) &&
          hello.pattern == FLOWMQ_PROTOCOL_XPUB) {
        rc = flowmq_connect_endpoint_send_subscription(endpoint, FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
                                                       tstr_to_v(endpoint->topic));
      }
      if (rc == TURBO_OK && endpoint->config.pattern == FLOWMQ_PROTOCOL_XSUB &&
          hello.pattern == FLOWMQ_PROTOCOL_XPUB) {
        rc = flowmq_connect_endpoint_replay_subscriptions(endpoint);
      }
    }
    if (rc == TURBO_OK) {
      uint64_t generation = 0u;
      flowmq_reconnect_reset(&endpoint->reconnect);
      rc = flowmq_peer_session_handshake_complete(&endpoint->session, &generation);
      if (rc == TURBO_OK) {
        flowmq_connect_endpoint_set_state(endpoint, FLOWMQ_ENDPOINT_CONNECTION_READY, TURBO_OK, 1u);
        flowmq_connect_endpoint_emit(endpoint, FLOWMQ_ENDPOINT_EVENT_RECONNECT_SUCCEEDED, TURBO_OK,
                                     0u, hello.identity, hello.topic);
        rc = flowmq_connect_endpoint_receive(endpoint, &reader);
      }
      if (endpoint->config.pattern == FLOWMQ_PROTOCOL_REQ)
        (void)flowmq_peer_session_mark_resetting(&endpoint->session);
    }
  attempt_done:
    flowmq_protocol_frame_cleanup(&hello);
    flowmq_stream_decoder_destroy(&reader);
    if (endpoint->socket) {
      coro_socket_destroy(endpoint->socket);
      endpoint->socket = NULL;
    }
    attempt_status = rc;
    if (!atomic_load_explicit(&endpoint->started, memory_order_acquire)) {
      rc = TURBO_ESHUTDOWN;
      break;
    }
    rc = flowmq_reconnect_next(&endpoint->reconnect, &wait_ms);
    if (rc == TURBO_ENOENT) {
      rc = attempt_status;
      break;
    }
    if (rc != TURBO_OK) break;
    flowmq_connect_endpoint_set_state(endpoint, FLOWMQ_ENDPOINT_CONNECTION_BACKOFF, TURBO_EALREADY,
                                      0u);
    flowmq_connect_endpoint_emit(endpoint, FLOWMQ_ENDPOINT_EVENT_RECONNECT_SCHEDULED, TURBO_OK,
                                 wait_ms, (tstr_v){0}, (tstr_v){0});
    if (wait_ms != 0u) {
      rc = coro_wait_for(endpoint->reconnect_wait, wait_ms);
      if (rc != TURBO_OK) break;
    }
  }
  if (rc != TURBO_ESHUTDOWN) {
    flowmq_connect_endpoint_emit_failure_once(endpoint, rc);
    flowmq_connect_endpoint_set_state(endpoint, FLOWMQ_ENDPOINT_CONNECTION_FAILED, rc, 0u);
  }
  flowmq_connect_endpoint_task_done(endpoint);
}

static void flowmq_connect_endpoint_spawn_post(void *arg1, void *arg2) {
  flowmq_connect_endpoint_t *endpoint = (flowmq_connect_endpoint_t *)arg1;
  int rc;
  (void)arg2;
  if (!atomic_load_explicit(&endpoint->started, memory_order_acquire)) {
    flowmq_connect_endpoint_task_done(endpoint);
    return;
  }
  rc = coro_context_spawn(endpoint->context, flowmq_connect_endpoint_connect_task, endpoint);
  if (rc == TURBO_OK) return;
  flowmq_connect_endpoint_set_state(endpoint, FLOWMQ_ENDPOINT_CONNECTION_FAILED, rc, 0u);
  flowmq_connect_endpoint_task_done(endpoint);
}

static void flowmq_connect_endpoint_loop(void *arg) {
  flowmq_connect_endpoint_t *endpoint = (flowmq_connect_endpoint_t *)arg;
  (void)coro_context_run(endpoint->context, TURBO_RUN_DEFAULT);
}

static void flowmq_connect_endpoint_interrupt_post(void *arg1, void *arg2) {
  flowmq_connect_endpoint_t *endpoint = (flowmq_connect_endpoint_t *)arg1;
  int status = (int)(intptr_t)arg2;
  if (endpoint->socket) (void)coro_socket_interrupt_wait(endpoint->socket, status);
  if (endpoint->reconnect_wait) (void)coro_wait_interrupt(endpoint->reconnect_wait, status);
}

static void flowmq_connect_endpoint_context_stop_post(void *arg1, void *arg2) {
  (void)arg2;
  coro_context_stop((coro_context_t *)arg1);
}

static void flowmq_connect_endpoint_barrier_post(void *arg1, void *arg2) {
  flowmq_connect_endpoint_t *endpoint = (flowmq_connect_endpoint_t *)arg1;
  (void)arg2;
  turbo_mutex_lock(&endpoint->mutex);
  endpoint->barrier_done = 1;
  turbo_cond_broadcast(&endpoint->changed);
  turbo_mutex_unlock(&endpoint->mutex);
}

int flowmq_connect_endpoint_create(const flowmq_connect_endpoint_config_t *config,
                                   flowmq_connect_endpoint_t **out) {
  flowmq_connect_endpoint_config_t normalized;
  flowmq_connect_endpoint_t *endpoint;
  size_t config_size;
  size_t copy_size;
  int rc;
  if (!config || !out) return TURBO_EINVAL;
  *out = NULL;
  config_size = config->size;
  if (config_size < FLOWMQ_CONNECT_ENDPOINT_CONFIG_V2_SIZE) return TURBO_EINVAL;
  flowmq_connect_endpoint_config_init(&normalized);
  copy_size = config_size < sizeof(normalized) ? config_size : sizeof(normalized);
  memcpy(&normalized, config, copy_size);
  normalized.size = sizeof(normalized);
  config = &normalized;
  if (!config->host || !config->path || !config->topic || !config->identity ||
      config->max_frame_size == 0u ||
      flowmq_coronet_transport_validate(config->transport) != TURBO_OK ||
      flowmq_pattern_validate(config->pattern) != TURBO_OK ||
      flowmq_coronet_endpoint_validate(config->transport, config->host, config->port,
                                       config->path) != TURBO_OK ||
      flowmq_coronet_connect_config_validate(config->transport, &config->timeouts,
                                             &config->socket_options, &config->udp_options,
                                             config->tls) != TURBO_OK ||
      (config->verify_peer_identity &&
       ((config->transport != FLOWMQ_TRANSPORT_TLS &&
         config->transport != FLOWMQ_TRANSPORT_WSS) ||
        !config->tls || config->tls->verify_peer != 1)) ||
      (!config->verify_peer_identity && config->verify_peer_identity_ctx) ||
      (config->drive_context && !config->own_context) ||
      (!config->context && (!config->drive_context || !config->own_context))) {
    return TURBO_EINVAL;
  }
  endpoint = (flowmq_connect_endpoint_t *)calloc(1, sizeof(*endpoint));
  if (!endpoint) return TURBO_ENOMEM;
  atomic_init(&endpoint->started, 0);
  atomic_init(&endpoint->state, FLOWMQ_ENDPOINT_CONNECTION_STOPPED);
  atomic_init(&endpoint->status, TURBO_ENOTCONN);
  atomic_init(&endpoint->reconnect_failure_reported, 0);
  endpoint->config = *config;
  endpoint->config.tls = NULL;
  endpoint->host = tstr_dup(config->host);
  endpoint->path = tstr_dup(config->path);
  endpoint->topic = tstr_dup(config->topic);
  endpoint->identity = tstr_dup(config->identity);
  endpoint->multicast_group = tstr_dup(config->udp_options.multicast_group
                                           ? config->udp_options.multicast_group
                                           : "");
  endpoint->multicast_interface = tstr_dup(config->udp_options.multicast_interface
                                               ? config->udp_options.multicast_interface
                                               : "");
  endpoint->tls_ca_file = tstr_dup(config->tls && config->tls->ca_file ? config->tls->ca_file : "");
  endpoint->tls_cert_file =
      tstr_dup(config->tls && config->tls->cert_file ? config->tls->cert_file : "");
  endpoint->tls_key_file =
      tstr_dup(config->tls && config->tls->key_file ? config->tls->key_file : "");
  endpoint->tls_key_password =
      tstr_dup(config->tls && config->tls->key_password ? config->tls->key_password : "");
  endpoint->tls_server_name =
      tstr_dup(config->tls && config->tls->server_name ? config->tls->server_name : "");
  if (!endpoint->host || !endpoint->path || !endpoint->topic || !endpoint->identity ||
      !endpoint->multicast_group || !endpoint->multicast_interface || !endpoint->tls_ca_file ||
      !endpoint->tls_cert_file || !endpoint->tls_key_file || !endpoint->tls_key_password ||
      !endpoint->tls_server_name) {
    flowmq_connect_endpoint_destroy(endpoint);
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
    endpoint->tls.cert_file = config->tls->cert_file ? endpoint->tls_cert_file : NULL;
    endpoint->tls.key_file = config->tls->key_file ? endpoint->tls_key_file : NULL;
    endpoint->tls.key_password = config->tls->key_password ? endpoint->tls_key_password : NULL;
    endpoint->tls.server_name = config->tls->server_name ? endpoint->tls_server_name : NULL;
    endpoint->tls.verify_peer = config->tls->verify_peer;
    endpoint->config.tls = &endpoint->tls;
  }
  rc = flowmq_coronet_transport_kcp_resolve(config->transport, &config->kcp_options,
                                             &endpoint->kcp_config, &endpoint->kcp_configured);
  if (rc != TURBO_OK) {
    flowmq_connect_endpoint_destroy(endpoint);
    return rc;
  }
  endpoint->context = config->context ? config->context : coro_context_create(NULL);
  if (!endpoint->context) {
    flowmq_connect_endpoint_destroy(endpoint);
    return TURBO_ENOMEM;
  }
  if (config->stream_recv_buffer_bytes != 0u) {
    rc = coro_context_set_stream_recv_buffer_size(endpoint->context,
                                                  config->stream_recv_buffer_bytes);
    if (rc != TURBO_OK) {
      flowmq_connect_endpoint_destroy(endpoint);
      return rc;
    }
  }
  if (config->drive_context) coro_context_set_persistent(endpoint->context, 1);
  turbo_mutex_init(&endpoint->mutex);
  turbo_cond_init(&endpoint->changed);
  endpoint->sync_initialized = 1;
  endpoint->reconnect_wait = coro_wait_create(endpoint->context);
  rc = flowmq_subscription_set_init(&endpoint->subscriptions);
  if (rc == TURBO_OK) endpoint->subscriptions_initialized = 1;
  if (rc == TURBO_OK) {
    rc = flowmq_reconnect_init(&endpoint->reconnect, config->reconnect_initial_ms,
                               config->reconnect_max_ms,
                               turbo_hrtime() ^ (uint64_t)(uintptr_t)endpoint);
  }
  if (rc == TURBO_OK) rc = flowmq_peer_session_init(&endpoint->session, 1u);
  if (rc == TURBO_OK)
    rc = flowmq_posted_send_init(
        &endpoint->posted_send, endpoint->context, endpoint,
        flowmq_connect_endpoint_execute_posted_send,
        &config->send_admission);
  if (rc != TURBO_OK || !endpoint->reconnect_wait) {
    flowmq_connect_endpoint_destroy(endpoint);
    return rc != TURBO_OK ? rc : TURBO_ENOMEM;
  }
  *out = endpoint;
  return TURBO_OK;
}

int flowmq_connect_endpoint_start(flowmq_connect_endpoint_t *endpoint, uint64_t timeout_ns) {
  uint64_t deadline_ns;
  int rc;
  if (!endpoint || timeout_ns == 0u) return TURBO_EINVAL;
  if (atomic_exchange_explicit(&endpoint->started, 1, memory_order_acq_rel)) return TURBO_OK;
  atomic_store_explicit(&endpoint->reconnect_failure_reported, 0, memory_order_release);
  flowmq_connect_endpoint_set_state(endpoint, FLOWMQ_ENDPOINT_CONNECTION_CONNECTING, TURBO_EALREADY,
                                    0u);
  if (endpoint->config.drive_context && !endpoint->loop_thread_started) {
    coro_context_set_persistent(endpoint->context, 1);
    rc = turbo_thread_create(&endpoint->loop_thread, flowmq_connect_endpoint_loop, endpoint);
    if (rc != TURBO_OK) goto failed;
    endpoint->loop_thread_started = 1;
  }
  turbo_mutex_lock(&endpoint->mutex);
  endpoint->task_active = 1;
  turbo_mutex_unlock(&endpoint->mutex);
  rc = coro_post(endpoint->context, flowmq_connect_endpoint_spawn_post, endpoint, NULL);
  if (rc != TURBO_OK) {
    flowmq_connect_endpoint_task_done(endpoint);
    goto failed;
  }
  deadline_ns = turbo_hrtime();
  deadline_ns = timeout_ns > UINT64_MAX - deadline_ns ? UINT64_MAX : deadline_ns + timeout_ns;
  turbo_mutex_lock(&endpoint->mutex);
  while (atomic_load_explicit(&endpoint->status, memory_order_acquire) == TURBO_EALREADY &&
         endpoint->task_active) {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    (void)turbo_cond_timedwait(&endpoint->changed, &endpoint->mutex, deadline_ns - now_ns);
  }
  rc = atomic_load_explicit(&endpoint->status, memory_order_acquire);
  if (rc == TURBO_EALREADY) rc = TURBO_ETIMEDOUT;
  turbo_mutex_unlock(&endpoint->mutex);
  if (rc == TURBO_OK) {
    flowmq_posted_send_start(&endpoint->posted_send);
    return TURBO_OK;
  }
failed:
  flowmq_connect_endpoint_emit_failure_once(endpoint, rc);
  flowmq_connect_endpoint_stop(endpoint);
  return rc;
}

void flowmq_connect_endpoint_stop(flowmq_connect_endpoint_t *endpoint) {
  if (endpoint) flowmq_posted_send_stop(&endpoint->posted_send);
  if (!endpoint || !atomic_exchange_explicit(&endpoint->started, 0, memory_order_acq_rel)) {
    return;
  }
  (void)coro_post(endpoint->context, flowmq_connect_endpoint_interrupt_post, endpoint,
                  (void *)(intptr_t)TURBO_ESHUTDOWN);
  turbo_mutex_lock(&endpoint->mutex);
  while (endpoint->task_active)
    turbo_cond_wait(&endpoint->changed, &endpoint->mutex);
  turbo_mutex_unlock(&endpoint->mutex);
  if (endpoint->config.drive_context && endpoint->loop_thread_started) {
    turbo_mutex_lock(&endpoint->mutex);
    endpoint->barrier_done = 0;
    turbo_mutex_unlock(&endpoint->mutex);
    if (coro_post(endpoint->context, flowmq_connect_endpoint_barrier_post, endpoint, NULL) ==
        TURBO_OK) {
      turbo_mutex_lock(&endpoint->mutex);
      while (!endpoint->barrier_done)
        turbo_cond_wait(&endpoint->changed, &endpoint->mutex);
      turbo_mutex_unlock(&endpoint->mutex);
    }
    /* Keep the loop alive until its stop callback is queued; otherwise a stale stop can survive
     * into the next start generation. */
    if (coro_post(endpoint->context, flowmq_connect_endpoint_context_stop_post, endpoint->context,
                  NULL) != TURBO_OK) {
      coro_context_stop(endpoint->context);
    }
    coro_context_set_persistent(endpoint->context, 0);
    (void)turbo_thread_join(&endpoint->loop_thread);
    endpoint->loop_thread_started = 0;
  }
  flowmq_connect_endpoint_set_state(endpoint, FLOWMQ_ENDPOINT_CONNECTION_STOPPED, TURBO_ESHUTDOWN,
                                    0u);
}

void flowmq_connect_endpoint_destroy(flowmq_connect_endpoint_t *endpoint) {
  if (!endpoint) return;
  flowmq_connect_endpoint_stop(endpoint);
  flowmq_posted_send_destroy(&endpoint->posted_send);
  if (endpoint->reconnect_wait) (void)coro_wait_destroy(endpoint->reconnect_wait);
  if (endpoint->subscriptions_initialized)
    flowmq_subscription_set_destroy(&endpoint->subscriptions);
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
  tstr_freep(&endpoint->tls_server_name);
  turbo_kcp_config_wipe(&endpoint->kcp_config);
  free(endpoint);
}

coro_context_t *flowmq_connect_endpoint_context(flowmq_connect_endpoint_t *endpoint) {
  return endpoint ? endpoint->context : NULL;
}

int flowmq_connect_endpoint_send(flowmq_connect_endpoint_t *endpoint, const char *encoded,
                                 size_t encoded_size) {
  if (!endpoint || !encoded || encoded_size == 0u) return TURBO_EINVAL;
  if (coro_context_current() != endpoint->context) return TURBO_EINVAL;
  if (!atomic_load_explicit(&endpoint->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  if (endpoint->config.pattern == FLOWMQ_PROTOCOL_XSUB) {
    flowmq_protocol_frame_t frame;
    size_t consumed = 0u;
    int changed = 0;
    int rc;
    memset(&frame, 0, sizeof(frame));
    rc = flowmq_protocol_decode_frame(encoded, encoded_size, endpoint->config.max_frame_size,
                                      &frame, &consumed);
    if (rc == TURBO_OK &&
        (consumed != encoded_size || (frame.kind != FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE &&
                                      frame.kind != FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE))) {
      rc = TURBO_EPROTO;
    }
    if (rc == TURBO_OK) {
      rc = flowmq_subscription_set_update(&endpoint->subscriptions,
                                          frame.kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
                                          frame.topic, &changed);
    }
    flowmq_protocol_frame_cleanup(&frame);
    if (rc != TURBO_OK) return rc;
  }
  return flowmq_connect_endpoint_socket_send(endpoint, encoded, encoded_size);
}

int flowmq_connect_endpoint_send_copy(flowmq_connect_endpoint_t *endpoint,
                                      uint64_t completion_id,
                                      const char *encoded,
                                      size_t encoded_size) {
  if (!endpoint) return TURBO_EINVAL;
  return flowmq_posted_send_copy(&endpoint->posted_send, NULL, 0u,
                                 completion_id, encoded, encoded_size);
}

void flowmq_connect_endpoint_send_stats(
    flowmq_connect_endpoint_t *endpoint,
    flowmq_send_admission_stats_t *stats) {
  flowmq_posted_send_get_stats(endpoint ? &endpoint->posted_send : NULL, stats);
}

int flowmq_connect_endpoint_interrupt(flowmq_connect_endpoint_t *endpoint, int status) {
  if (!endpoint || status == TURBO_OK) return TURBO_EINVAL;
  if (coro_context_current() == endpoint->context) {
    if (!endpoint->socket) return TURBO_ENOTCONN;
    return coro_socket_interrupt_wait(endpoint->socket, status);
  }
  return coro_post(endpoint->context, flowmq_connect_endpoint_interrupt_post, endpoint,
                   (void *)(intptr_t)status);
}

int flowmq_connect_endpoint_update_endpoint(flowmq_connect_endpoint_t *endpoint, const char *host,
                                            int port, const char *path) {
  tstr_t next_host;
  tstr_t next_path;
  if (!endpoint || !host || !path ||
      flowmq_coronet_endpoint_validate(endpoint->config.transport, host, port, path) != TURBO_OK)
    return TURBO_EINVAL;
  if (atomic_load_explicit(&endpoint->started, memory_order_acquire)) return TURBO_EBUSY;
  next_host = tstr_dup(host);
  next_path = tstr_dup(path);
  if (!next_host || !next_path) {
    tstr_freep(&next_host);
    tstr_freep(&next_path);
    return TURBO_ENOMEM;
  }
  tstr_freep(&endpoint->host);
  tstr_freep(&endpoint->path);
  endpoint->host = next_host;
  endpoint->path = next_path;
  endpoint->config.host = endpoint->host;
  endpoint->config.path = endpoint->path;
  endpoint->config.port = port;
  return TURBO_OK;
}

int flowmq_connect_endpoint_request_begin(flowmq_connect_endpoint_t *endpoint,
                                          uint64_t correlation_id, uint64_t *generation) {
  if (!endpoint || endpoint->config.pattern != FLOWMQ_PROTOCOL_REQ) return TURBO_EINVAL;
  return flowmq_peer_session_begin(&endpoint->session, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY,
                                   correlation_id, generation);
}

int flowmq_connect_endpoint_request_finish(flowmq_connect_endpoint_t *endpoint, uint64_t generation,
                                           uint64_t correlation_id,
                                           flowmq_connect_endpoint_exchange_state_t terminal_state) {
  if (!endpoint || endpoint->config.pattern != FLOWMQ_PROTOCOL_REQ) return TURBO_EINVAL;
  return flowmq_peer_session_finish(&endpoint->session, generation, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY,
                                    correlation_id, (flowmq_peer_exchange_state_t)terminal_state);
}

int flowmq_connect_endpoint_exchange_snapshot(const flowmq_connect_endpoint_t *endpoint,
                                              flowmq_connect_endpoint_exchange_state_t *state,
                                              uint64_t *generation, uint64_t *correlation_id) {
  flowmq_peer_exchange_state_t internal_state;
  int rc;
  if (!endpoint) return TURBO_EINVAL;
  rc = flowmq_peer_session_snapshot(&endpoint->session, &internal_state, generation,
                                    correlation_id);
  if (rc == TURBO_OK && state) *state = (flowmq_connect_endpoint_exchange_state_t)internal_state;
  return rc;
}
