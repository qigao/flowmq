#include "flowmq_socket.h"

#include "flowmq_cnet_transport.h"
#include "flowmq_flow_control.h"
#include "flowmq_pattern.h"
#include "flowmq_pattern_state.h"
#include "flowmq_peer_state.h"
#include "flowmq_protocol_internal.h"
#include "flowmq_reconnect.h"
#include "flowmq_stream_decoder.h"
#include "flowmq_subscription_set.h"
#include "flowmq_socket_option.h"
#include "flowmq_tls_identity_map.h"
#include "salts_error.h"
#include "salts_buffer.h"
#include "str.h"

#include <cnet/cnet.h>
#include <salts/clock.h>
#include <salts/thread.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  FLOWMQ_SOCKET_PEER_CAPACITY = 4u,
  FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY = FLOWMQ_SOCKET_PEER_CAPACITY,
  FLOWMQ_SOCKET_ENDPOINT_NONE = FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY,
  FLOWMQ_SOCKET_INBOUND_CAPACITY = 1024u,
  FLOWMQ_SOCKET_OUTBOUND_CAPACITY = FLOWMQ_SOCKET_OPTION_MESSAGE_HWM_MAX,
  FLOWMQ_SOCKET_MULTIPART_CAPACITY = 64u,
  FLOWMQ_SOCKET_DEFAULT_HWM = 1000u,
  FLOWMQ_SOCKET_DEFAULT_HWM_BYTES = 16u * 1024u * 1024u,
  FLOWMQ_SOCKET_DEFAULT_RECONNECT_IVL_MS = 100,
  FLOWMQ_SOCKET_DEFAULT_RECONNECT_IVL_MAX_MS = 0,
  FLOWMQ_SOCKET_HARD_HWM_BYTES = FLOWMQ_SOCKET_OPTION_HWM_BYTES_MAX,
  FLOWMQ_SOCKET_MAX_FRAME_SIZE = 1024u * 1024u,
  FLOWMQ_SOCKET_FRAME_PACKET_CAPACITY =
      (FLOWMQ_SOCKET_MAX_FRAME_SIZE + FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE - 1u) /
      FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE,
  FLOWMQ_SOCKET_FRAME_SEGMENT_CAPACITY =
      FLOWMQ_SOCKET_FRAME_PACKET_CAPACITY * 2u,
  FLOWMQ_SOCKET_FRAMING_CAPACITY =
      FLOWMQ_SOCKET_FRAME_PACKET_CAPACITY * FLOWMQ_PROTOCOL_HEADER_SIZE +
      FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE + FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE,
  FLOWMQ_SOCKET_BLOCKING_SLICE_MS = 10u,
  FLOWMQ_SOCKET_SHUTDOWN_TIMEOUT_MS = 1000u,
  FLOWMQ_SOCKET_DEFAULT_TIMEOUT_MS = 1000u,
  FLOWMQ_SOCKET_CNET_COMMAND_CAPACITY = 16u,
  FLOWMQ_SOCKET_HOST_CAPACITY = FLOWMQ_SOCKET_OPTION_TLS_SERVER_NAME_CAPACITY,
  FLOWMQ_SOCKET_ENDPOINT_CAPACITY = 320u,
  FLOWMQ_SOCKET_TLS_PATH_CAPACITY = FLOWMQ_SOCKET_OPTION_TLS_PATH_CAPACITY,
  FLOWMQ_SOCKET_TLS_PASSWORD_CAPACITY = FLOWMQ_SOCKET_OPTION_TLS_PASSWORD_CAPACITY,
  FLOWMQ_SOCKET_TLS_FINGERPRINT_PREFIX_SIZE = 7u,
  FLOWMQ_SOCKET_TLS_FINGERPRINT_CAPACITY =
      FLOWMQ_SOCKET_TLS_FINGERPRINT_PREFIX_SIZE +
      CNET_TLS_PEER_CERTIFICATE_SHA256_CAPACITY
};

_Static_assert(FLOWMQ_SOCKET_FRAME_SEGMENT_CAPACITY <=
                   FLOWMQ_SOCKET_OUTBOUND_CAPACITY,
               "frame descriptors must fit the reusable CNet vector");

#if defined(FLOWMQ_EXPERIMENT_ROUTE_REPLY_FACT_SOURCE)
#define FLOWMQ_SOCKET_IS_REPLY_PEER_PATTERN(socket_)                              \
  ((socket_)->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_REPLY_PEER)
#else
#define FLOWMQ_SOCKET_IS_REPLY_PEER_PATTERN(socket_)                              \
  ((socket_)->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REP)
#endif

typedef struct flowmq_socket_peer_s flowmq_socket_peer_t;

/* Endpoint policy survives a connection; all mutable wire/session state does not. */
typedef struct flowmq_socket_endpoint_s {
  flowmq_reconnect_t reconnect;
  uint64_t next_attempt_ms;
  char uri[FLOWMQ_SOCKET_ENDPOINT_CAPACITY];
  unsigned used : 1;
  unsigned active : 1;
  unsigned retry_pending : 1;
} flowmq_socket_endpoint_t;

typedef struct flowmq_socket_message_s {
  mem_buffer_t *buffer;
  size_t size;
  size_t credit_size;
  size_t peer_index;
  uint64_t peer_generation;
  int more;
} flowmq_socket_message_t;

typedef struct flowmq_socket_outbound_s {
  mem_buffer_t *buffer;
  size_t encoded_size;
  size_t payload_size;
  int message_end;
} flowmq_socket_outbound_t;

struct flowmq_socket_peer_s {
  struct flowmq_socket_s *owner;
  flowmq_peer_state_t state;
  cnet_connection connection;
  flowmq_stream_decoder_t decoder;
  flowmq_subscription_set_t subscriptions;
  flowmq_subscription_set_t synced_subscriptions;
  flowmq_protocol_heartbeat_deadlines_t heartbeat;
  flowmq_flow_control_t flow_control;
  flowmq_socket_outbound_t *outbound;
  flowmq_protocol_pattern_t remote_pattern;
  size_t identity_size;
  size_t endpoint_index;
  flowmq_socket_message_t staged[FLOWMQ_SOCKET_MULTIPART_CAPACITY];
  size_t staged_count;
  size_t staged_bytes;
  size_t outbound_read;
  size_t outbound_write;
  size_t outbound_count;
  size_t outbound_messages;
  size_t outbound_bytes;
  size_t queued_parts;
  size_t inflight_payload_size;
  size_t inflight_messages;
  char identity[FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE + 1u];
  unsigned receiving_multipart : 1;
  unsigned commit_pending : 1;
  unsigned heartbeat_active : 1;
  unsigned pong_pending : 1;
};

struct flowmq_ctx_s {
  size_t socket_count;
  uint64_t next_socket_id;
};

struct flowmq_socket_s {
  flowmq_ctx_t *ctx;
  flowmq_pattern_state_t pattern;
  cnet_client client;
  cnet_listener listener;
  cnet_tls_client tls_client;
  cnet_tls_server tls_server;
  mem_pool_t message_pool;
  flowmq_subscription_set_t subscriptions;
  flowmq_tls_identity_map_t *tls_identity_policy;
  flowmq_socket_peer_t *peers;
  flowmq_socket_endpoint_t endpoints[FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY];
  flowmq_socket_message_t *inbound;
  flowmq_socket_outbound_t send_staged[FLOWMQ_SOCKET_MULTIPART_CAPACITY];
  flowmq_protocol_segment_t frame_segments[FLOWMQ_SOCKET_FRAME_SEGMENT_CAPACITY];
  cnet_const_buffer send_segments[FLOWMQ_SOCKET_OUTBOUND_CAPACITY];
  unsigned char framing_scratch[FLOWMQ_SOCKET_FRAMING_CAPACITY];
  size_t max_encoded_size;
  size_t inbound_read;
  size_t inbound_write;
  size_t inbound_count;
  size_t inbound_messages;
  size_t inbound_bytes;
  size_t send_staged_count;
  size_t send_staged_bytes;
  size_t send_hwm;
  size_t receive_hwm;
  size_t send_hwm_bytes;
  size_t receive_hwm_bytes;
  size_t peer_cursor;
  size_t send_peer_index;
  size_t reply_peer_index;
  size_t request_peer_index;
  uint32_t publish_peer_mask;
  uint64_t next_message_id;
  uint64_t send_peer_generation;
  uint64_t reply_peer_generation;
  uint64_t request_peer_generation;
  uint64_t tls_identity_rejections;
  uint64_t publish_peer_generations[FLOWMQ_SOCKET_PEER_CAPACITY];
  uint32_t flow_update_quantum;
  int heartbeat_interval_ms;
  int heartbeat_timeout_ms;
  int reconnect_interval_ms;
  int reconnect_interval_max_ms;
  int flow_update_interval_ms;
  int async_error;
  int send_cancel_error;
  int recv_cancel_error;
  int transport;
  int last_rcvmore;
  unsigned runtime_initialized : 1;
  unsigned listener_initialized : 1;
  unsigned pool_initialized : 1;
  unsigned tls_client_initialized : 1;
  unsigned tls_server_initialized : 1;
  unsigned tls_require_client_certificate : 1;
  unsigned send_peer_active : 1;
  unsigned reply_peer_valid : 1;
  unsigned request_peer_valid : 1;
  unsigned heartbeat_timeout_set : 1;
  unsigned reconnect_pending : 1;
  char last_endpoint[FLOWMQ_SOCKET_ENDPOINT_CAPACITY];
  char identity[FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE + 1u];
  size_t identity_size;
  char tls_ca_file[FLOWMQ_SOCKET_TLS_PATH_CAPACITY];
  char tls_cert_file[FLOWMQ_SOCKET_TLS_PATH_CAPACITY];
  char tls_key_file[FLOWMQ_SOCKET_TLS_PATH_CAPACITY];
  char tls_key_password[FLOWMQ_SOCKET_TLS_PASSWORD_CAPACITY];
  char tls_server_name[FLOWMQ_SOCKET_HOST_CAPACITY];
};

typedef struct flowmq_endpoint_parts_s {
  flowmq_transport_t transport;
  char host[FLOWMQ_SOCKET_HOST_CAPACITY];
  uint16_t port;
} flowmq_endpoint_parts_t;

static int flowmq_socket_drive(flowmq_socket_t *socket, uint32_t timeout_ms,
                               size_t *events);
static void flowmq_socket_cancel_send_route(flowmq_socket_t *socket);
static void flowmq_socket_fail(flowmq_socket_t *socket, int status);

static int flowmq_endpoint_parse(const char *endpoint, int allow_zero_port,
                                 flowmq_endpoint_parts_t *parts) {
  const char *host_begin;
  const char *host_end;
  const char *port_begin;
  char *port_end = NULL;
  unsigned long port;
  size_t host_size;
  if (endpoint == NULL || parts == NULL) return SALTS_EINVAL;
  memset(parts, 0, sizeof(*parts));
  if (strncmp(endpoint, "tcp://", 6u) == 0) {
    parts->transport = FLOWMQ_TRANSPORT_TCP;
    host_begin = endpoint + 6u;
  } else if (strncmp(endpoint, "tls://", 6u) == 0) {
    parts->transport = FLOWMQ_TRANSPORT_TLS;
    host_begin = endpoint + 6u;
  } else {
    return SALTS_EINVAL;
  }
  if (*host_begin == '[') {
    host_begin++;
    host_end = strchr(host_begin, ']');
    if (host_end == NULL || host_end[1] != ':') return SALTS_EINVAL;
    port_begin = host_end + 2u;
  } else {
    host_end = strrchr(host_begin, ':');
    if (host_end == NULL) return SALTS_EINVAL;
    port_begin = host_end + 1u;
  }
  host_size = (size_t)(host_end - host_begin);
  if (host_size == 0u || host_size >= sizeof(parts->host) || *port_begin == '\0')
    return SALTS_EINVAL;
  memcpy(parts->host, host_begin, host_size);
  parts->host[host_size] = '\0';
  port = strtoul(port_begin, &port_end, 10);
  if (port_end == port_begin || *port_end != '\0' || port > 65535u ||
      (!allow_zero_port && port == 0u))
    return SALTS_EINVAL;
  parts->port = (uint16_t)port;
  return SALTS_OK;
}

static flowmq_socket_peer_t *flowmq_socket_peer_acquire(flowmq_socket_t *socket) {
  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
    flowmq_socket_peer_t *peer = &socket->peers[i];
    if (!flowmq_peer_state_is_used(&peer->state)) {
      memset(peer, 0, sizeof(*peer));
      if (flowmq_peer_state_allocate(&peer->state) != SALTS_OK) return NULL;
      peer->owner = socket;
      peer->endpoint_index = FLOWMQ_SOCKET_ENDPOINT_NONE;
      peer->outbound = (flowmq_socket_outbound_t *)calloc(
          FLOWMQ_SOCKET_OUTBOUND_CAPACITY, sizeof(*peer->outbound));
      if (flowmq_stream_decoder_prepare(&peer->decoder,
                                        FLOWMQ_SOCKET_MAX_FRAME_SIZE) != SALTS_OK ||
          flowmq_subscription_set_init(&peer->subscriptions) != SALTS_OK ||
          flowmq_subscription_set_init(&peer->synced_subscriptions) != SALTS_OK ||
          peer->outbound == NULL) {
        flowmq_stream_decoder_destroy(&peer->decoder);
        flowmq_subscription_set_destroy(&peer->subscriptions);
        flowmq_subscription_set_destroy(&peer->synced_subscriptions);
        free(peer->outbound);
        (void)flowmq_peer_state_release(&peer->state);
        memset(peer, 0, sizeof(*peer));
        return NULL;
      }
      return peer;
    }
  }
  return NULL;
}

static size_t flowmq_socket_peer_index(const flowmq_socket_t *socket,
                                       const flowmq_socket_peer_t *peer) {
  return (size_t)(peer - socket->peers);
}

static void flowmq_socket_peer_storage_release(flowmq_socket_peer_t *peer) {
  for (size_t i = 0u; i < peer->staged_count; ++i)
    mem_buffer_release(peer->staged[i].buffer);
  for (size_t i = 0u; i < peer->outbound_count; ++i) {
    size_t index =
        (peer->outbound_read + i) % FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
    mem_buffer_release(peer->outbound[index].buffer);
  }
  peer->staged_count = 0u;
  peer->staged_bytes = 0u;
  peer->outbound_count = 0u;
  peer->outbound_messages = 0u;
  peer->outbound_bytes = 0u;
  peer->commit_pending = 0u;
  flowmq_peer_state_write_cancel(&peer->state);
  flowmq_stream_decoder_destroy(&peer->decoder);
  flowmq_subscription_set_destroy(&peer->subscriptions);
  flowmq_subscription_set_destroy(&peer->synced_subscriptions);
  free(peer->outbound);
  peer->outbound = NULL;
}

static void flowmq_socket_peer_release(flowmq_socket_peer_t *peer) {
  int status;
  if (peer == NULL || !flowmq_peer_state_is_used(&peer->state)) return;
  if (!flowmq_peer_state_is_retired(&peer->state)) {
    flowmq_socket_peer_storage_release(peer);
    if (peer->state.lifecycle != FLOWMQ_PEER_LIFECYCLE_ALLOCATED) {
      status = flowmq_peer_state_transition(
          &peer->state, FLOWMQ_PEER_LIFECYCLE_RETIRED);
      if (status != SALTS_OK && peer->owner != NULL)
        flowmq_socket_fail(peer->owner, SALTS_EPROTO);
    }
  }
  if (peer->state.lifecycle == FLOWMQ_PEER_LIFECYCLE_RETIRED ||
      peer->state.lifecycle == FLOWMQ_PEER_LIFECYCLE_ALLOCATED)
    (void)flowmq_peer_state_release(&peer->state);
  memset(peer, 0, sizeof(*peer));
}

static void flowmq_socket_peer_retire(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket;
  size_t peer_index;
  uint64_t generation;
  int state_status;
  if (peer == NULL || !flowmq_peer_state_is_used(&peer->state) ||
      flowmq_peer_state_is_retired(&peer->state))
    return;
  socket = peer->owner;
  peer_index = flowmq_socket_peer_index(socket, peer);
  generation = peer->flow_control.local_generation;
  if (peer_index < FLOWMQ_SOCKET_PEER_CAPACITY &&
      socket->publish_peer_generations[peer_index] == generation) {
    socket->publish_peer_mask &= ~(UINT32_C(1) << peer_index);
    socket->publish_peer_generations[peer_index] = 0u;
  }
  if (socket->send_peer_active && socket->send_peer_index == peer_index &&
      socket->send_peer_generation == generation) {
    if (socket->send_cancel_error == SALTS_OK)
      socket->send_cancel_error = SALTS_ENOTCONN;
    flowmq_socket_cancel_send_route(socket);
  }
  if (socket->reply_peer_valid && socket->reply_peer_index == peer_index &&
      socket->reply_peer_generation == generation) {
    if (socket->send_cancel_error == SALTS_OK)
      socket->send_cancel_error = SALTS_ENOTCONN;
    socket->reply_peer_valid = 0u;
    socket->reply_peer_generation = 0u;
    flowmq_pattern_state_cancel_transaction(&socket->pattern);
  }
  if (socket->request_peer_valid && socket->request_peer_index == peer_index &&
      socket->request_peer_generation == generation &&
      peer->queued_parts == 0u) {
    if (socket->recv_cancel_error == SALTS_OK)
      socket->recv_cancel_error = SALTS_ENOTCONN;
    socket->request_peer_valid = 0u;
    socket->request_peer_generation = 0u;
    flowmq_pattern_state_cancel_transaction(&socket->pattern);
  }
  peer->heartbeat_active = 0u;
  flowmq_socket_peer_storage_release(peer);
  state_status =
      flowmq_peer_state_transition(&peer->state, FLOWMQ_PEER_LIFECYCLE_RETIRED);
  if (state_status != SALTS_OK) {
    flowmq_socket_fail(socket, SALTS_EPROTO);
    return;
  }
  if (peer->queued_parts == 0u) flowmq_socket_peer_release(peer);
}

static void flowmq_socket_fail(flowmq_socket_t *socket, int status) {
  if (socket->async_error == SALTS_OK) socket->async_error = status;
}

static int flowmq_socket_endpoint_schedule(flowmq_socket_t *socket,
                                           flowmq_socket_endpoint_t *endpoint) {
  uint64_t delay_ms = 0u;
  uint64_t now_ms;
  int status;
  if (socket == NULL || endpoint == NULL || !endpoint->used)
    return SALTS_EINVAL;
  endpoint->active = 0u;
  endpoint->retry_pending = 0u;
  if (socket->reconnect_interval_ms < 0) return SALTS_OK;
  status = flowmq_reconnect_next(&endpoint->reconnect, &delay_ms);
  if (status != SALTS_OK) return status;
  now_ms = salts_monotonic_ms();
  endpoint->next_attempt_ms =
      delay_ms > UINT64_MAX - now_ms ? UINT64_MAX : now_ms + delay_ms;
  endpoint->retry_pending = 1u;
  socket->reconnect_pending = 1u;
  return SALTS_OK;
}

static void flowmq_socket_peer_fail(flowmq_socket_peer_t *peer) {
  int status;
  int state_status;
  if (peer == NULL || !flowmq_peer_state_is_connected(&peer->state))
    return;
  peer->heartbeat_active = 0u;
  status = cnet_close(&peer->owner->client, peer->connection);
  if (status == SALTS_OK || status == SALTS_EALREADY) {
    state_status =
        flowmq_peer_state_transition(&peer->state,
                                     FLOWMQ_PEER_LIFECYCLE_CLOSING);
  } else if (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    state_status =
        flowmq_peer_state_transition(&peer->state,
                                     FLOWMQ_PEER_LIFECYCLE_CLOSE_RETRY);
  } else {
    state_status =
        flowmq_peer_state_transition(&peer->state,
                                     FLOWMQ_PEER_LIFECYCLE_CLOSING);
    flowmq_socket_fail(peer->owner, status);
  }
  if (state_status != SALTS_OK && state_status != SALTS_EALREADY)
    flowmq_socket_fail(peer->owner, SALTS_EPROTO);
}

static int flowmq_socket_encode_frame_segments(
    flowmq_socket_t *socket, const flowmq_protocol_frame_t *frame,
    size_t *segment_count, size_t *encoded_size) {
  int status = flowmq_protocol_encode_frame_segmented_into_internal(
      frame, FLOWMQ_SOCKET_MAX_FRAME_SIZE, socket->frame_segments,
      FLOWMQ_SOCKET_FRAME_SEGMENT_CAPACITY, socket->framing_scratch,
      sizeof(socket->framing_scratch), segment_count, encoded_size);
  if (status != SALTS_OK) return status;
  for (size_t i = 0u; i < *segment_count; ++i) {
    socket->send_segments[i] = (cnet_const_buffer){
        .data = socket->frame_segments[i].data,
        .size = socket->frame_segments[i].size};
  }
  return SALTS_OK;
}

static int flowmq_socket_copy_segments(const cnet_const_buffer *segments,
                                       size_t segment_count,
                                       size_t encoded_size, void *storage,
                                       size_t storage_size) {
  unsigned char *destination = (unsigned char *)storage;
  size_t offset = 0u;
  if (segments == NULL || segment_count == 0u || storage == NULL)
    return SALTS_EINVAL;
  for (size_t i = 0u; i < segment_count; ++i) {
    if (segments[i].data == NULL || segments[i].size == 0u)
      return SALTS_EPROTO;
    if (segments[i].size > storage_size - offset) return SALTS_ENOSPC;
    memcpy(destination + offset, segments[i].data, segments[i].size);
    offset += segments[i].size;
  }
  return offset == encoded_size ? SALTS_OK : SALTS_EPROTO;
}

static int flowmq_socket_send_frame(flowmq_socket_t *socket,
                                    cnet_connection connection,
                                    const flowmq_protocol_frame_t *frame) {
  size_t segment_count = 0u;
  size_t encoded_size = 0u;
  int status = flowmq_socket_encode_frame_segments(
      socket, frame, &segment_count, &encoded_size);
  if (status != SALTS_OK) return status;
  if (encoded_size > socket->max_encoded_size) return SALTS_EMSGSIZE;
  return cnet_sendv(&socket->client, connection, socket->send_segments,
                    segment_count);
}

static int flowmq_socket_send_hello(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  flowmq_protocol_frame_t frame = {0};
  vstr identity = {.data = socket->identity, .len = socket->identity_size};
  int status;
  frame.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
  frame.pattern = socket->pattern.pattern;
  frame.identity = identity;
  status =
      flowmq_peer_state_write_begin(&peer->state, FLOWMQ_PEER_WRITE_HELLO);
  if (status != SALTS_OK) return status;
  status = flowmq_socket_send_frame(socket, peer->connection, &frame);
  if (status != SALTS_OK) flowmq_peer_state_write_cancel(&peer->state);
  return status;
}

static uint64_t flowmq_socket_session_generation(cnet_connection connection) {
  return ((uint64_t)connection.generation << 32u) |
         ((uint64_t)connection.slot + 1u);
}

static int flowmq_socket_flow_control_init(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  uint32_t quantum = flowmq_flow_control_default_quantum(
      (uint64_t)socket->receive_hwm_bytes);
  if (socket->flow_update_quantum != 0u)
    quantum = socket->flow_update_quantum;
  return flowmq_flow_control_init_local(
      &peer->flow_control,
      flowmq_socket_session_generation(peer->connection),
      (uint64_t)socket->receive_hwm_bytes, quantum,
      (uint32_t)socket->flow_update_interval_ms);
}

static int flowmq_socket_send_settings(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  flowmq_protocol_settings_t settings;
  flowmq_protocol_frame_t frame = {0};
  unsigned char payload[FLOWMQ_PROTOCOL_SETTINGS_PAYLOAD_SIZE];
  int status;
  if (!flowmq_peer_state_is_connected(&peer->state) ||
      !flowmq_peer_state_handshake_has(
          &peer->state, FLOWMQ_PEER_HANDSHAKE_HELLO_TX) ||
      flowmq_peer_state_handshake_has(
          &peer->state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_TX) ||
      !flowmq_peer_state_write_idle(&peer->state)) {
    return SALTS_EBUSY;
  }
  status = flowmq_flow_control_make_settings(
      &peer->flow_control, FLOWMQ_SOCKET_MAX_FRAME_SIZE, &settings);
  if (status == SALTS_OK)
    status = flowmq_protocol_settings_encode(&settings, payload);
  frame.kind = FLOWMQ_PROTOCOL_FRAME_SETTINGS;
  frame.pattern = socket->pattern.pattern;
  frame.payload = vstr_from_buf((const char *)payload, sizeof(payload));
  if (status == SALTS_OK)
    status = flowmq_peer_state_write_begin(
        &peer->state, FLOWMQ_PEER_WRITE_SETTINGS);
  if (status == SALTS_OK)
    status = flowmq_socket_send_frame(socket, peer->connection, &frame);
  if (status != SALTS_OK &&
      peer->state.write_lane == FLOWMQ_PEER_WRITE_SETTINGS)
    flowmq_peer_state_write_cancel(&peer->state);
  return status;
}

static int flowmq_socket_peer_can_admit(const flowmq_socket_peer_t *peer,
                                        size_t payload_size,
                                        int message_end) {
  const flowmq_socket_t *socket = peer->owner;
  if (!flowmq_peer_state_ready(&peer->state) ||
      payload_size > socket->send_hwm_bytes ||
      peer->outbound_bytes > socket->send_hwm_bytes - payload_size ||
      (message_end && peer->outbound_messages >= socket->send_hwm) ||
      flowmq_flow_control_send_check(&peer->flow_control, payload_size) !=
          SALTS_OK)
    return 0;
  return flowmq_peer_state_write_idle(&peer->state) &&
                 peer->outbound_count == 0u
             ? 1
             : peer->outbound_count < FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
}

static int flowmq_socket_peer_ready(const flowmq_socket_peer_t *peer) {
  return flowmq_peer_state_ready(&peer->state);
}

static int flowmq_socket_peer_generation_ready(
    const flowmq_socket_peer_t *peer, uint64_t generation) {
  return generation != 0u && flowmq_socket_peer_ready(peer) &&
         peer->flow_control.local_generation == generation;
}

static int flowmq_socket_peer_can_admit_message(const flowmq_socket_peer_t *peer,
                                                size_t payload_size,
                                                size_t part_count,
                                                size_t final_part_size) {
  const flowmq_socket_t *socket = peer->owner;
  if (!flowmq_socket_peer_ready(peer) || part_count == 0u ||
      part_count > FLOWMQ_SOCKET_OUTBOUND_CAPACITY - peer->outbound_count ||
      payload_size > socket->send_hwm_bytes ||
      peer->outbound_bytes > socket->send_hwm_bytes - payload_size ||
      peer->outbound_messages >= socket->send_hwm ||
      flowmq_flow_control_send_credit_check(&peer->flow_control,
                                            payload_size) != SALTS_OK)
    return 0;
  if (final_part_size > peer->flow_control.remote_max_frame_size) return 0;
  for (size_t i = 0u; i < socket->send_staged_count; ++i) {
    if (socket->send_staged[i].payload_size >
        peer->flow_control.remote_max_frame_size)
      return 0;
  }
  return 1;
}

static int flowmq_socket_prepare_outbound(flowmq_socket_t *socket, const void *data, size_t size,
                                          int more, flowmq_socket_outbound_t *outbound) {
  flowmq_protocol_frame_t frame = {.kind = FLOWMQ_PROTOCOL_FRAME_DATA,
                                   .pattern = socket->pattern.pattern,
                                   .message_id = socket->next_message_id + 1u,
                                   .more = more != 0,
                                   .payload = {.data = (char *)data, .len = size}};
  mem_buffer_t *buffer;
  size_t segment_count = 0u;
  size_t encoded_size = 0u;
  int status;
  status = flowmq_socket_encode_frame_segments(socket, &frame, &segment_count,
                                               &encoded_size);
  if (status != SALTS_OK) return status;
  buffer = mem_get_buffer(&socket->message_pool, encoded_size);
  if (buffer == NULL) return SALTS_ENOMEM;
  status = flowmq_socket_copy_segments(
      socket->send_segments, segment_count, encoded_size,
      mem_buffer_data(buffer), encoded_size);
  if (status != SALTS_OK) {
    mem_buffer_release(buffer);
    return status;
  }
  mem_set_used(buffer, encoded_size);
  *outbound = (flowmq_socket_outbound_t){.buffer = buffer,
                                         .encoded_size = encoded_size,
                                         .payload_size = size,
                                         .message_end = more == 0};
  return SALTS_OK;
}

static void flowmq_socket_release_send_staged(flowmq_socket_t *socket) {
  for (size_t i = 0u; i < socket->send_staged_count; ++i) {
    mem_buffer_release(socket->send_staged[i].buffer);
    memset(&socket->send_staged[i], 0, sizeof(socket->send_staged[i]));
  }
  socket->send_staged_count = 0u;
  socket->send_staged_bytes = 0u;
}

static void flowmq_socket_cancel_send_route(flowmq_socket_t *socket) {
  flowmq_socket_release_send_staged(socket);
  socket->send_peer_active = 0u;
  socket->send_peer_generation = 0u;
  socket->publish_peer_mask = 0u;
  memset(socket->publish_peer_generations, 0,
         sizeof(socket->publish_peer_generations));
  flowmq_pattern_state_cancel_transaction(&socket->pattern);
}

static int flowmq_socket_peer_admit_staged(flowmq_socket_peer_t *peer,
                                            flowmq_socket_t *socket) {
  int status = flowmq_flow_control_send_credit_commit(
      &peer->flow_control, socket->send_staged_bytes);
  if (status != SALTS_OK) return status;
  for (size_t i = 0u; i < socket->send_staged_count; ++i) {
    flowmq_socket_outbound_t *outbound = &peer->outbound[peer->outbound_write];
    *outbound = socket->send_staged[i];
    outbound->buffer = mem_buffer_retain(outbound->buffer);
    peer->outbound_write = (peer->outbound_write + 1u) % FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
    ++peer->outbound_count;
  }
  peer->outbound_bytes += socket->send_staged_bytes;
  ++peer->outbound_messages;
  return SALTS_OK;
}

static int flowmq_socket_peer_admit(flowmq_socket_peer_t *peer,
                                    const cnet_const_buffer *segments,
                                    size_t segment_count, size_t encoded_size,
                                    size_t payload_size, int message_end) {
  flowmq_socket_t *socket = peer->owner;
  flowmq_socket_outbound_t *outbound;
  mem_buffer_t *buffer;
  int status;
  if (!flowmq_socket_peer_can_admit(peer, payload_size, message_end))
    return SALTS_ENOBUFS;
  if (flowmq_peer_state_write_idle(&peer->state) &&
      peer->outbound_count == 0u) {
    status =
        flowmq_peer_state_write_begin(&peer->state, FLOWMQ_PEER_WRITE_DATA);
    if (status != SALTS_OK) return status;
    status = cnet_sendv(&socket->client, peer->connection, segments,
                        segment_count);
    if (status != SALTS_OK) {
      flowmq_peer_state_write_cancel(&peer->state);
      return status;
    }
    peer->inflight_payload_size = payload_size;
    peer->inflight_messages = message_end ? 1u : 0u;
  } else {
    outbound = &peer->outbound[peer->outbound_write];
    buffer = mem_get_buffer(&socket->message_pool, encoded_size);
    if (buffer == NULL) return SALTS_ENOMEM;
    status = flowmq_socket_copy_segments(segments, segment_count, encoded_size,
                                         mem_buffer_data(buffer), encoded_size);
    if (status != SALTS_OK) {
      mem_buffer_release(buffer);
      return status;
    }
    mem_set_used(buffer, encoded_size);
    *outbound = (flowmq_socket_outbound_t){.buffer = buffer,
                                           .encoded_size = encoded_size,
                                           .payload_size = payload_size,
                                           .message_end = message_end != 0};
    peer->outbound_write =
        (peer->outbound_write + 1u) % FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
    ++peer->outbound_count;
  }
  peer->outbound_bytes += payload_size;
  if (message_end) ++peer->outbound_messages;
  status = flowmq_flow_control_send_commit(&peer->flow_control, payload_size);
  if (status != SALTS_OK) {
    flowmq_socket_fail(socket, SALTS_EPROTO);
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int flowmq_socket_peer_flush(flowmq_socket_peer_t *peer) {
  /* CNet permits one pending write per connection. A vector admission copies
   * queued frames directly into that final command slot before returning. */
  flowmq_socket_t *socket = peer->owner;
  flowmq_socket_outbound_t *outbound;
  size_t batch_count = 0u;
  size_t batch_encoded_size = 0u;
  size_t batch_payload_size = 0u;
  size_t batch_messages = 0u;
  int status;
  if (!flowmq_peer_state_is_connected(&peer->state) ||
      !flowmq_peer_state_write_idle(&peer->state) ||
      peer->outbound_count == 0u)
    return SALTS_OK;
  while (batch_count < peer->outbound_count) {
    size_t index = (peer->outbound_read + batch_count) %
                   FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
    outbound = &peer->outbound[index];
    if (outbound->encoded_size >
        socket->max_encoded_size - batch_encoded_size)
      break;
    socket->send_segments[batch_count] = (cnet_const_buffer){
        .data = mem_buffer_const_data(outbound->buffer),
        .size = outbound->encoded_size};
    batch_encoded_size += outbound->encoded_size;
    batch_payload_size += outbound->payload_size;
    if (outbound->message_end) ++batch_messages;
    ++batch_count;
  }
  if (batch_count == 0u) return SALTS_EMSGSIZE;
  status =
      flowmq_peer_state_write_begin(&peer->state, FLOWMQ_PEER_WRITE_DATA);
  if (status != SALTS_OK) return status;
  status = cnet_sendv(&socket->client, peer->connection,
                      socket->send_segments, batch_count);
  if (status != SALTS_OK) {
    flowmq_peer_state_write_cancel(&peer->state);
    return status;
  }
  peer->inflight_payload_size = batch_payload_size;
  peer->inflight_messages = batch_messages;
  for (size_t i = 0u; i < batch_count; ++i) {
    outbound = &peer->outbound[peer->outbound_read];
    mem_buffer_release(outbound->buffer);
    memset(outbound, 0, sizeof(*outbound));
    peer->outbound_read =
        (peer->outbound_read + 1u) % FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
    --peer->outbound_count;
  }
  return SALTS_OK;
}

static int flowmq_socket_stage_bytes(flowmq_socket_peer_t *peer,
                                     const void *data, size_t size,
                                     size_t credit_size, int more) {
  flowmq_socket_t *socket = peer->owner;
  flowmq_socket_message_t *message;
  mem_buffer_t *buffer;
  if (peer->staged_count == FLOWMQ_SOCKET_MULTIPART_CAPACITY)
    return SALTS_ENOBUFS;
  if (credit_size > size) return SALTS_EINVAL;
  if (size > FLOWMQ_SOCKET_HARD_HWM_BYTES - peer->staged_bytes)
    return SALTS_EMSGSIZE;
  buffer = mem_get_buffer(&socket->message_pool,
                          size == 0u ? 1u : size);
  if (buffer == NULL) return SALTS_ENOMEM;
  if (size != 0u) memcpy(mem_buffer_data(buffer), data, size);
  mem_set_used(buffer, size);
  message = &peer->staged[peer->staged_count];
  *message = (flowmq_socket_message_t){
      .buffer = buffer,
      .size = size,
      .credit_size = credit_size,
      .peer_index = flowmq_socket_peer_index(socket, peer),
      .peer_generation = peer->flow_control.local_generation,
      .more = more};
  ++peer->staged_count;
  peer->staged_bytes += size;
  return SALTS_OK;
}

static int flowmq_socket_commit_staged(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  size_t message_credit = 0u;
  if (peer->staged_bytes > socket->receive_hwm_bytes)
    return SALTS_EMSGSIZE;
  if (peer->staged_count >
          FLOWMQ_SOCKET_INBOUND_CAPACITY - socket->inbound_count ||
      socket->inbound_messages >= socket->receive_hwm ||
      socket->inbound_bytes > socket->receive_hwm_bytes - peer->staged_bytes)
    return SALTS_ENOBUFS;
  if (peer->queued_parts > SIZE_MAX - peer->staged_count)
    return SALTS_ERANGE;
  for (size_t i = 0u; i < peer->staged_count; ++i) {
    if (peer->staged[i].credit_size > SIZE_MAX - message_credit)
      return SALTS_ERANGE;
    message_credit += peer->staged[i].credit_size;
  }
  for (size_t i = 0u; i < peer->staged_count; ++i)
    peer->staged[i].credit_size = 0u;
  if (peer->staged_count != 0u)
    peer->staged[peer->staged_count - 1u].credit_size = message_credit;
  for (size_t i = 0u; i < peer->staged_count; ++i) {
    socket->inbound[socket->inbound_write] = peer->staged[i];
    memset(&peer->staged[i], 0, sizeof(peer->staged[i]));
    socket->inbound_write =
        (socket->inbound_write + 1u) % FLOWMQ_SOCKET_INBOUND_CAPACITY;
    ++socket->inbound_count;
  }
  socket->inbound_bytes += peer->staged_bytes;
  ++socket->inbound_messages;
  peer->queued_parts += peer->staged_count;
  peer->staged_count = 0u;
  peer->staged_bytes = 0u;
  return SALTS_OK;
}

static int flowmq_socket_stage_frame(flowmq_socket_peer_t *peer,
                                     const flowmq_protocol_frame_t *frame) {
  return flowmq_socket_stage_bytes(peer, frame->payload.data,
                                   frame->payload.len, frame->payload.len,
                                   frame->more);
}

static int flowmq_socket_subscription_event(flowmq_socket_peer_t *peer,
                                            const flowmq_protocol_frame_t *frame) {
  flowmq_socket_t *socket = peer->owner;
  int changed = 0;
  int subscribe = frame->kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE;
  int status = flowmq_subscription_set_update(&peer->subscriptions, subscribe,
                                               frame->topic, &changed);
  if (status != SALTS_OK || !changed ||
      (socket->pattern.desc->capabilities & FLOWMQ_PATTERN_CAP_SUB_EVENTS) == 0u)
    return status;
  {
    unsigned char event[FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE + 1u];
    event[0] = subscribe ? 1u : 0u;
    if (frame->topic.len != 0u)
      memcpy(event + 1u, frame->topic.data, frame->topic.len);
    status = flowmq_socket_stage_bytes(peer, event, frame->topic.len + 1u,
                                       0u, 0);
    return status == SALTS_OK ? flowmq_socket_commit_staged(peer) : status;
  }
}

static int flowmq_socket_peer_admit_control(
    flowmq_socket_peer_t *peer, flowmq_protocol_frame_kind_t kind,
    vstr payload) {
  flowmq_socket_t *socket = peer->owner;
  flowmq_protocol_frame_t frame = {0};
  int status;
  if (!flowmq_socket_peer_ready(peer) ||
      !flowmq_peer_state_write_idle(&peer->state))
    return SALTS_ENOBUFS;
  frame.kind = kind;
  frame.pattern = socket->pattern.pattern;
  frame.payload = payload;
  status =
      flowmq_peer_state_write_begin(&peer->state, FLOWMQ_PEER_WRITE_CONTROL);
  if (status != SALTS_OK) return status;
  status = flowmq_socket_send_frame(socket, peer->connection, &frame);
  if (status != SALTS_OK) flowmq_peer_state_write_cancel(&peer->state);
  return status;
}

static int flowmq_socket_peer_flow_update_progress(
    flowmq_socket_peer_t *peer) {
  flowmq_protocol_flow_update_t update;
  unsigned char payload[FLOWMQ_PROTOCOL_FLOW_UPDATE_PAYLOAD_SIZE];
  int status;
  if (!flowmq_socket_peer_ready(peer) ||
      !flowmq_peer_state_write_idle(&peer->state))
    return SALTS_OK;
  status = flowmq_flow_control_next_update(&peer->flow_control, salts_hrtime(),
                                           &update);
  if (status == FLOWMQ_FLOW_CONTROL_NO_UPDATE) return SALTS_OK;
  if (status != SALTS_OK) return status;
  status = flowmq_protocol_flow_update_encode(&update, payload);
  if (status == SALTS_OK)
    status = flowmq_socket_peer_admit_control(
        peer, FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE,
        vstr_from_buf((const char *)payload, sizeof(payload)));
  if (status == SALTS_ENOBUFS || status == SALTS_EBUSY) return SALTS_OK;
  if (status != SALTS_OK) return status;
  return flowmq_flow_control_mark_update_sent(&peer->flow_control, &update);
}

static int flowmq_socket_peer_heartbeat_progress(flowmq_socket_peer_t *peer) {
  uint64_t wait_deadline_ns = 0u;
  uint64_t now_ns;
  flowmq_protocol_heartbeat_action_t action;
  int status;
  if (!flowmq_socket_peer_ready(peer)) return SALTS_OK;
  if (peer->pong_pending) {
    status = flowmq_socket_peer_admit_control(
        peer, FLOWMQ_PROTOCOL_FRAME_PONG, (vstr){0});
    if (status == SALTS_ENOBUFS || status == SALTS_EBUSY) return SALTS_OK;
    if (status != SALTS_OK) return status;
    peer->pong_pending = 0u;
  }
  if (!peer->heartbeat_active) return SALTS_OK;
  now_ns = salts_hrtime();
  action = flowmq_protocol_heartbeat_deadlines_next(
      &peer->heartbeat, now_ns, &wait_deadline_ns);
  (void)wait_deadline_ns;
  if (action == FLOWMQ_PROTOCOL_HEARTBEAT_WAIT) return SALTS_OK;
  if (action == FLOWMQ_PROTOCOL_HEARTBEAT_SEND_PING) {
    /* Control frames take the next serialized CNet write slot so application
     * backlog cannot postpone dead-peer detection indefinitely. */
    if (!flowmq_peer_state_write_idle(&peer->state)) return SALTS_OK;
    status = flowmq_socket_peer_admit_control(
        peer, FLOWMQ_PROTOCOL_FRAME_PING, (vstr){0});
    if (status == SALTS_ENOBUFS || status == SALTS_EBUSY) return SALTS_OK;
    if (status != SALTS_OK) return status;
    flowmq_protocol_heartbeat_deadlines_on_ping(&peer->heartbeat, now_ns);
    return SALTS_OK;
  }
  flowmq_socket_peer_fail(peer);
  return SALTS_OK;
}

static int flowmq_socket_tls_identity_verify(flowmq_socket_peer_t *peer,
                                             vstr claimed_identity) {
  static const char prefix[] = "sha256:";
  flowmq_socket_t *socket = peer->owner;
  char digest[CNET_TLS_PEER_CERTIFICATE_SHA256_CAPACITY];
  char fingerprint[FLOWMQ_SOCKET_TLS_FINGERPRINT_CAPACITY];
  int status;
  if (socket->tls_identity_policy == NULL) return SALTS_OK;
  status = cnet_tls_peer_certificate_sha256(&socket->client, peer->connection,
                                            digest);
  if (status == SALTS_OK) {
    memcpy(fingerprint, prefix, FLOWMQ_SOCKET_TLS_FINGERPRINT_PREFIX_SIZE);
    memcpy(fingerprint + FLOWMQ_SOCKET_TLS_FINGERPRINT_PREFIX_SIZE, digest,
           sizeof(digest));
    status = flowmq_tls_identity_map_verify(socket->tls_identity_policy,
                                            fingerprint, claimed_identity);
  }
  if (status == SALTS_OK) return SALTS_OK;
  if (socket->tls_identity_rejections != UINT64_MAX)
    ++socket->tls_identity_rejections;
  return SALTS_EPERM;
}

static int flowmq_socket_process_receive(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  if (peer->commit_pending) return SALTS_ENOBUFS;
  for (;;) {
    flowmq_protocol_frame_t frame = {0};
    size_t consumed = 0u;
    int pause_receive = 0;
    int status = flowmq_stream_decoder_next(&peer->decoder, &frame, &consumed);
    if (status == FLOWMQ_PROTOCOL_INCOMPLETE) return SALTS_OK;
    if (status != SALTS_OK) return status;
    if (!flowmq_peer_state_handshake_has(
            &peer->state, FLOWMQ_PEER_HANDSHAKE_HELLO_RX)) {
      status =
          flowmq_pattern_socket_hello_validate(socket->pattern.pattern, &frame);
      if (status == SALTS_OK) {
        status = flowmq_socket_tls_identity_verify(peer, frame.identity);
        if (status == SALTS_OK &&
            socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY) {
          for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
            flowmq_socket_peer_t *candidate = &socket->peers[i];
            if (candidate == peer ||
                !flowmq_peer_state_is_used(&candidate->state) ||
                flowmq_peer_state_is_retired(&candidate->state) ||
                !flowmq_peer_state_handshake_has(
                    &candidate->state, FLOWMQ_PEER_HANDSHAKE_HELLO_RX) ||
                candidate->identity_size != frame.identity.len)
              continue;
            if (frame.identity.len == 0u ||
                memcmp(candidate->identity, frame.identity.data,
                       frame.identity.len) == 0) {
              status = SALTS_EPROTO;
              break;
            }
          }
        }
        if (status == SALTS_OK &&
            frame.identity.len > FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE) {
          status = SALTS_EMSGSIZE;
        } else if (status == SALTS_OK) {
          if (frame.identity.len != 0u)
            memcpy(peer->identity, frame.identity.data, frame.identity.len);
          peer->identity[frame.identity.len] = '\0';
          peer->identity_size = frame.identity.len;
        }
        peer->remote_pattern = frame.pattern;
        if (status == SALTS_OK)
          status = flowmq_peer_state_handshake_mark(
              &peer->state, FLOWMQ_PEER_HANDSHAKE_HELLO_RX);
        if (status == SALTS_OK && peer->heartbeat_active)
          flowmq_protocol_heartbeat_deadlines_on_receive(
              &peer->heartbeat, salts_hrtime());
      }
    } else if (!flowmq_peer_state_handshake_has(
                   &peer->state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_RX)) {
      flowmq_protocol_settings_t settings;
      status = frame.pattern == peer->remote_pattern &&
                       frame.kind == FLOWMQ_PROTOCOL_FRAME_SETTINGS
                   ? flowmq_pattern_data_direction_validate(
                         socket->pattern.pattern, &frame)
                   : SALTS_EPROTO;
      if (status == SALTS_OK)
        status = flowmq_protocol_settings_decode(frame.payload, &settings);
      if (status == SALTS_OK)
        status = flowmq_flow_control_apply_settings(&peer->flow_control,
                                                    &settings);
      if (status == SALTS_OK)
        status = flowmq_peer_state_handshake_mark(
            &peer->state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_RX);
      if (status == SALTS_OK && peer->heartbeat_active)
        flowmq_protocol_heartbeat_deadlines_on_receive(
            &peer->heartbeat, salts_hrtime());
    } else {
      status = frame.pattern == peer->remote_pattern
                   ? flowmq_pattern_data_direction_validate(
                         socket->pattern.pattern, &frame)
                   : SALTS_EPROTO;
      if (status == SALTS_OK && frame.kind == FLOWMQ_PROTOCOL_FRAME_SETTINGS)
        status = SALTS_EPROTO;
      if (status == SALTS_OK && peer->heartbeat_active)
        flowmq_protocol_heartbeat_deadlines_on_receive(&peer->heartbeat,
                                                       salts_hrtime());
      if (status == SALTS_OK && frame.kind == FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE) {
        flowmq_protocol_flow_update_t update;
        status = flowmq_protocol_flow_update_decode(frame.payload, &update);
        if (status == SALTS_OK)
          status = flowmq_flow_control_apply_remote_update(
              &peer->flow_control, &update);
      } else if (status == SALTS_OK &&
                 frame.kind == FLOWMQ_PROTOCOL_FRAME_PING) {
        peer->pong_pending = 1u;
      } else if (status == SALTS_OK &&
                 (frame.kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE ||
                  frame.kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE)) {
        status = flowmq_socket_subscription_event(peer, &frame);
      } else if (status == SALTS_OK && frame.kind == FLOWMQ_PROTOCOL_FRAME_DATA) {
        if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ &&
            (!socket->request_peer_valid ||
             socket->request_peer_index !=
                 flowmq_socket_peer_index(socket, peer) ||
             socket->request_peer_generation !=
                 peer->flow_control.local_generation))
          status = SALTS_EPROTO;
        if (status == SALTS_OK)
          status = flowmq_flow_control_receive_check(&peer->flow_control,
                                                     frame.payload.len);
        if (status == SALTS_OK &&
            socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY &&
            !peer->receiving_multipart) {
          status = flowmq_socket_stage_bytes(
              peer, peer->identity, peer->identity_size, 0u, 1);
        }
        if (status == SALTS_OK) status = flowmq_socket_stage_frame(peer, &frame);
        if (status == SALTS_OK)
          status = flowmq_flow_control_receive_commit(&peer->flow_control,
                                                      frame.payload.len);
        if (status == SALTS_OK) peer->receiving_multipart = frame.more != 0;
        if (status == SALTS_OK && !frame.more) {
          status = flowmq_socket_commit_staged(peer);
          if (status == SALTS_ENOBUFS) {
            peer->commit_pending = 1u;
            pause_receive = 1;
            status = SALTS_OK;
          }
        }
      }
    }
    flowmq_protocol_frame_cleanup(&frame);
    if (status != SALTS_OK) return status;
    status = flowmq_stream_decoder_consume(&peer->decoder, consumed);
    if (status != SALTS_OK) return status;
    if (pause_receive) return SALTS_ENOBUFS;
  }
}

static void flowmq_socket_on_state(void *user, cnet_connection connection,
                                   cnet_connection_state state,
                                   const cnet_error *error) {
  flowmq_socket_peer_t *peer = (flowmq_socket_peer_t *)user;
  flowmq_socket_t *socket = peer->owner;
  peer->connection = connection;
  if (state == CNET_CONNECTION_CONNECTED) {
    int flow_control_status;
    if (peer->endpoint_index < FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY) {
      flowmq_socket_endpoint_t *endpoint =
          &socket->endpoints[peer->endpoint_index];
      if (endpoint->used) {
        endpoint->active = 1u;
        endpoint->retry_pending = 0u;
        flowmq_reconnect_reset(&endpoint->reconnect);
      }
    }
    if (flowmq_peer_state_transition(
            &peer->state, FLOWMQ_PEER_LIFECYCLE_CONNECTED) != SALTS_OK) {
      flowmq_socket_fail(socket, SALTS_EPROTO);
      return;
    }
    if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_SINGLE) {
      for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
        flowmq_socket_peer_t *candidate = &socket->peers[i];
        if (candidate != peer &&
            flowmq_peer_state_is_connected(&candidate->state)) {
          flowmq_socket_peer_fail(peer);
          return;
        }
      }
    }
    flow_control_status = flowmq_socket_flow_control_init(peer);
    if (flow_control_status != SALTS_OK) {
      flowmq_socket_peer_fail(peer);
      return;
    }
    if (socket->heartbeat_interval_ms > 0) {
      int timeout_ms = socket->heartbeat_timeout_set
                           ? socket->heartbeat_timeout_ms
                           : socket->heartbeat_interval_ms;
      flowmq_protocol_heartbeat_deadlines_init(
          &peer->heartbeat, salts_hrtime(),
          (uint64_t)socket->heartbeat_interval_ms, (uint64_t)timeout_ms, 0u);
      peer->heartbeat_active = 1u;
    }
    if (cnet_receive(&socket->client, connection, 1u) != SALTS_OK)
      flowmq_socket_peer_fail(peer);
    else {
      int status = flowmq_socket_send_hello(peer);
      if (status != SALTS_OK) flowmq_socket_peer_fail(peer);
    }
  } else if (state == CNET_CONNECTION_CLOSED ||
             state == CNET_CONNECTION_FAILED) {
    if (peer->endpoint_index < FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY) {
      flowmq_socket_endpoint_t *endpoint =
          &socket->endpoints[peer->endpoint_index];
      int reconnect_status = flowmq_socket_endpoint_schedule(socket, endpoint);
      if (reconnect_status != SALTS_OK)
        flowmq_socket_fail(socket, reconnect_status);
    }
    (void)error;
    flowmq_socket_peer_retire(peer);
  }
}

static void flowmq_socket_on_receive(void *user, cnet_connection connection,
                                     const cnet_receive_view *view) {
  flowmq_socket_peer_t *peer = (flowmq_socket_peer_t *)user;
  flowmq_socket_t *socket = peer->owner;
  int status = flowmq_stream_decoder_append(&peer->decoder, view->data,
                                            view->size);
  if (status == SALTS_OK) status = flowmq_socket_process_receive(peer);
  if (status == SALTS_ENOBUFS) return;
  if (status == SALTS_OK) status = cnet_receive(&socket->client, connection, 1u);
  if (status != SALTS_OK) flowmq_socket_peer_fail(peer);
}

static void flowmq_socket_on_send(void *user, cnet_connection connection,
                                  size_t size) {
  flowmq_socket_peer_t *peer = (flowmq_socket_peer_t *)user;
  flowmq_peer_write_lane_t completed = FLOWMQ_PEER_WRITE_IDLE;
  int status;
  (void)connection;
  (void)size;
  status = flowmq_peer_state_write_complete(&peer->state, &completed);
  if (status != SALTS_OK) {
    flowmq_socket_fail(peer->owner, SALTS_EPROTO);
    return;
  }
  if (completed == FLOWMQ_PEER_WRITE_DATA) {
    if (peer->outbound_bytes >= peer->inflight_payload_size)
      peer->outbound_bytes -= peer->inflight_payload_size;
    else
      flowmq_socket_fail(peer->owner, SALTS_EPROTO);
    if (peer->outbound_messages >= peer->inflight_messages)
      peer->outbound_messages -= peer->inflight_messages;
    else
      flowmq_socket_fail(peer->owner, SALTS_EPROTO);
    peer->inflight_payload_size = 0u;
    peer->inflight_messages = 0u;
  }
}

static cnet_observer flowmq_socket_observer(flowmq_socket_peer_t *peer) {
  return (cnet_observer){.on_state = flowmq_socket_on_state,
                         .on_receive = flowmq_socket_on_receive,
                         .user = peer,
                         .on_send = flowmq_socket_on_send};
}

static flowmq_socket_endpoint_t *flowmq_socket_endpoint_acquire(
    flowmq_socket_t *socket, size_t *endpoint_index) {
  if (socket == NULL || endpoint_index == NULL) return NULL;
  for (size_t i = 0u; i < FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY; ++i) {
    flowmq_socket_endpoint_t *endpoint = &socket->endpoints[i];
    if (!endpoint->used) {
      memset(endpoint, 0, sizeof(*endpoint));
      endpoint->used = 1u;
      *endpoint_index = i;
      return endpoint;
    }
  }
  return NULL;
}

static int flowmq_socket_endpoint_connect(flowmq_socket_t *socket,
                                          size_t endpoint_index) {
  flowmq_socket_endpoint_t *endpoint;
  flowmq_socket_peer_t *peer;
  cnet_connect_options options;
  cnet_observer observer;
  int status;
  if (socket == NULL || endpoint_index >= FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY)
    return SALTS_EINVAL;
  endpoint = &socket->endpoints[endpoint_index];
  if (!endpoint->used) return SALTS_ENOENT;
  if (endpoint->active) return SALTS_EALREADY;
  peer = flowmq_socket_peer_acquire(socket);
  if (peer == NULL) return SALTS_ENOBUFS;
  peer->endpoint_index = endpoint_index;
  observer = flowmq_socket_observer(peer);
  options = (cnet_connect_options){
      .uri = endpoint->uri,
      .observer = observer,
      .tls = NULL,
      .tls_client = socket->transport == FLOWMQ_TRANSPORT_TLS
                        ? &socket->tls_client
                        : NULL};
  status = cnet_connect(&socket->client, &options, &peer->connection);
  if (status != SALTS_OK) {
    flowmq_socket_peer_release(peer);
    return status;
  }
  endpoint->active = 1u;
  endpoint->retry_pending = 0u;
  return SALTS_OK;
}

static int flowmq_socket_reconnect_progress(flowmq_socket_t *socket) {
  const uint64_t now_ms = salts_monotonic_ms();
  /* Keep established-connection progress at one predictable branch. */
  if (!socket->reconnect_pending) return SALTS_OK;
  socket->reconnect_pending = 0u;
  for (size_t i = 0u; i < FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY; ++i) {
    flowmq_socket_endpoint_t *endpoint = &socket->endpoints[i];
    int status;
    if (!endpoint->used || endpoint->active || !endpoint->retry_pending)
      continue;
    if (now_ms < endpoint->next_attempt_ms) {
      socket->reconnect_pending = 1u;
      continue;
    }
    status = flowmq_socket_endpoint_connect(socket, i);
    if (status == SALTS_OK) continue;
    if (status == SALTS_EBUSY || status == SALTS_ENOBUFS ||
        status == SALTS_EALREADY) {
      socket->reconnect_pending = 1u;
      continue;
    }
    return status;
  }
  return SALTS_OK;
}

static int flowmq_socket_runtime_init(flowmq_socket_t *socket,
                                      flowmq_transport_t transport) {
  flowmq_io_config_t io;
  flowmq_timeout_config_t timeouts = {0};
  cnet_client_config config;
  int status;
  if (socket->runtime_initialized)
    return socket->transport == (int)transport ? SALTS_OK : SALTS_ENOTSUP;
  flowmq_io_config_init(&io);
  io.command_capacity = FLOWMQ_SOCKET_CNET_COMMAND_CAPACITY;
  /* A messaging connection remains valid while either direction is idle. */
  timeouts.set_flags = FLOWMQ_TIMEOUT_SET_RECV;
  flowmq_timeouts_resolve(&timeouts, FLOWMQ_SOCKET_DEFAULT_TIMEOUT_MS);
  status = flowmq_cnet_client_config(
      &io, &timeouts, transport, FLOWMQ_SOCKET_PEER_CAPACITY,
      socket->max_encoded_size, &config);
  if (status == SALTS_OK) status = cnet_client_init(&socket->client, &config);
  if (status != SALTS_OK) return status;
  socket->transport = (int)transport;
  socket->runtime_initialized = 1u;
  return SALTS_OK;
}

static void flowmq_socket_clear_secret(char *value, size_t size) {
  volatile char *bytes = (volatile char *)value;
  for (size_t i = 0u; i < size; ++i) bytes[i] = 0;
}

static int flowmq_socket_store_string(char *destination, size_t capacity,
                                      const void *value, size_t size) {
  /*
   * ABI/range/string validation is owned by the option descriptor layer.
   * This capacity check is only a defensive implementation-drift guard.
   */
  if (destination == NULL || value == NULL || size >= capacity)
    return SALTS_EPROTO;
  memcpy(destination, value, size);
  destination[size] = '\0';
  return SALTS_OK;
}

static int flowmq_socket_tls_server_init(flowmq_socket_t *socket) {
  cnet_tls_server_config config;
  int status;
  if (socket->tls_server_initialized) return SALTS_OK;
  if (socket->tls_cert_file[0] == '\0' || socket->tls_key_file[0] == '\0')
    return SALTS_EINVAL;
  config = (cnet_tls_server_config){
      .size = sizeof(config),
      .cert_file = socket->tls_cert_file,
      .key_file = socket->tls_key_file,
      .key_password = socket->tls_key_password[0] != '\0'
                          ? socket->tls_key_password
                          : NULL,
      .ca_file = socket->tls_ca_file[0] != '\0' ? socket->tls_ca_file : NULL,
      .ca_path = NULL,
      .client_auth = socket->tls_require_client_certificate
                         ? CNET_TLS_CLIENT_AUTH_REQUIRED
                         : CNET_TLS_CLIENT_AUTH_NONE,
      .alpn_protocols = NULL,
      .alpn_protocol_count = 0u};
  status = cnet_tls_server_init(&socket->tls_server, &config);
  if (status == SALTS_OK) socket->tls_server_initialized = 1u;
  return status;
}

static int flowmq_socket_tls_client_init(flowmq_socket_t *socket) {
  cnet_tls_client_config config;
  int status;
  if (socket->tls_client_initialized) return SALTS_OK;
  if ((socket->tls_cert_file[0] == '\0') !=
      (socket->tls_key_file[0] == '\0'))
    return SALTS_EINVAL;
  config = (cnet_tls_client_config){
      .size = sizeof(config),
      .ca_file = socket->tls_ca_file[0] != '\0' ? socket->tls_ca_file : NULL,
      .ca_path = NULL,
      .cert_file = socket->tls_cert_file[0] != '\0'
                       ? socket->tls_cert_file
                       : NULL,
      .key_file = socket->tls_key_file[0] != '\0' ? socket->tls_key_file : NULL,
      .key_password = socket->tls_key_password[0] != '\0'
                          ? socket->tls_key_password
                          : NULL,
      .server_name = socket->tls_server_name[0] != '\0'
                         ? socket->tls_server_name
                         : NULL,
      .alpn_protocols = NULL,
      .alpn_protocol_count = 0u};
  status = cnet_tls_client_init(&socket->tls_client, &config);
  if (status == SALTS_OK) socket->tls_client_initialized = 1u;
  return status;
}

static int flowmq_socket_subscription_contains(
    const flowmq_subscription_set_t *subscriptions, vstr topic) {
  for (size_t i = 0u; i < flowmq_subscription_set_count(subscriptions); ++i) {
    const flowmq_subscription_t *subscription =
        flowmq_subscription_set_at(subscriptions, i);
    const size_t subscription_size =
        subscription != NULL && subscription->topic != NULL
            ? tstr_len(subscription->topic)
            : 0u;
    if (subscription_size == topic.len &&
        (topic.len == 0u ||
         memcmp(subscription->topic, topic.data, topic.len) == 0))
      return 1;
  }
  return 0;
}

static int flowmq_socket_sync_subscription(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  const flowmq_subscription_t *subscription;
  tstr encoded = NULL;
  vstr topic = {0};
  flowmq_protocol_frame_kind_t kind = FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE;
  int changed = 0;
  int status;
  if (socket->pattern.desc->subscription_class != FLOWMQ_PATTERN_SUB_SUBSCRIBER)
    return SALTS_OK;
  if (!flowmq_socket_peer_ready(peer) ||
      !flowmq_peer_state_write_idle(&peer->state))
    return SALTS_OK;

  for (size_t i = 0u;
       i < flowmq_subscription_set_count(&socket->subscriptions); ++i) {
    subscription = flowmq_subscription_set_at(&socket->subscriptions, i);
    if (subscription == NULL) return SALTS_EPROTO;
    topic = (vstr){.data = subscription->topic,
                   .len = tstr_len(subscription->topic)};
    if (!flowmq_socket_subscription_contains(&peer->synced_subscriptions,
                                             topic))
      break;
    topic = (vstr){0};
  }
  if (topic.data == NULL) {
    kind = FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE;
    for (size_t i = 0u;
         i < flowmq_subscription_set_count(&peer->synced_subscriptions); ++i) {
      subscription =
          flowmq_subscription_set_at(&peer->synced_subscriptions, i);
      if (subscription == NULL) return SALTS_EPROTO;
      topic = (vstr){.data = subscription->topic,
                     .len = tstr_len(subscription->topic)};
      if (!flowmq_socket_subscription_contains(&socket->subscriptions, topic))
        break;
      topic = (vstr){0};
    }
  }
  if (topic.data == NULL) return SALTS_OK;

  status = flowmq_pattern_encode_subscription(socket->pattern.pattern, kind,
                                               topic,
                                               FLOWMQ_SOCKET_MAX_FRAME_SIZE,
                                               &encoded);
  if (status == SALTS_OK)
    status = flowmq_peer_state_write_begin(
        &peer->state, FLOWMQ_PEER_WRITE_CONTROL);
  if (status == SALTS_OK && kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE)
    status = flowmq_subscription_set_update(&peer->synced_subscriptions, 1,
                                             topic, &changed);
  if (status == SALTS_OK)
    status = cnet_send(&socket->client, peer->connection, encoded,
                       tstr_len(encoded));
  if (status != SALTS_OK) {
    flowmq_peer_state_write_cancel(&peer->state);
    if (kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE)
      (void)flowmq_subscription_set_update(&peer->synced_subscriptions, 0,
                                           topic, &changed);
  }
  if (status == SALTS_OK && kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE)
    status = flowmq_subscription_set_update(&peer->synced_subscriptions, 0,
                                             topic, &changed);
  if (encoded != NULL) tstr_free(encoded);
  return status;
}

static int flowmq_socket_pattern(int type, flowmq_protocol_pattern_t *pattern) {
  if (pattern == NULL) return SALTS_EINVAL;
  switch (type) {
  case FLOWMQ_PAIR: *pattern = FLOWMQ_PROTOCOL_PAIR; break;
  case FLOWMQ_PUB: *pattern = FLOWMQ_PROTOCOL_PUB; break;
  case FLOWMQ_SUB: *pattern = FLOWMQ_PROTOCOL_SUB; break;
  case FLOWMQ_REQ: *pattern = FLOWMQ_PROTOCOL_REQ; break;
  case FLOWMQ_REP: *pattern = FLOWMQ_PROTOCOL_REP; break;
  case FLOWMQ_DEALER: *pattern = FLOWMQ_PROTOCOL_DEALER; break;
  case FLOWMQ_ROUTER: *pattern = FLOWMQ_PROTOCOL_ROUTER; break;
  case FLOWMQ_PULL: *pattern = FLOWMQ_PROTOCOL_PULL; break;
  case FLOWMQ_PUSH: *pattern = FLOWMQ_PROTOCOL_PUSH; break;
  case FLOWMQ_XPUB: *pattern = FLOWMQ_PROTOCOL_XPUB; break;
  case FLOWMQ_XSUB: *pattern = FLOWMQ_PROTOCOL_XSUB; break;
  default: return SALTS_EINVAL;
  }
  return SALTS_OK;
}

flowmq_ctx_t *flowmq_ctx_new(void) {
  return (flowmq_ctx_t *)calloc(1u, sizeof(flowmq_ctx_t));
}

int flowmq_ctx_term(flowmq_ctx_t *ctx) {
  if (ctx == NULL) return SALTS_EINVAL;
  if (ctx->socket_count != 0u) return SALTS_EBUSY;
  free(ctx);
  return SALTS_OK;
}

flowmq_socket_t *flowmq_socket(flowmq_ctx_t *ctx, int type) {
  flowmq_protocol_pattern_t pattern;
  flowmq_socket_t *socket;
  size_t encoded_limit = 0u;
  if (ctx == NULL || flowmq_socket_pattern(type, &pattern) != SALTS_OK) return NULL;
  socket = (flowmq_socket_t *)calloc(1u, sizeof(*socket));
  if (socket == NULL) return NULL;
  socket->send_hwm = FLOWMQ_SOCKET_DEFAULT_HWM;
  socket->receive_hwm = FLOWMQ_SOCKET_DEFAULT_HWM;
  socket->send_hwm_bytes = FLOWMQ_SOCKET_DEFAULT_HWM_BYTES;
  socket->receive_hwm_bytes = FLOWMQ_SOCKET_DEFAULT_HWM_BYTES;
  socket->reconnect_interval_ms = FLOWMQ_SOCKET_DEFAULT_RECONNECT_IVL_MS;
  socket->reconnect_interval_max_ms =
      FLOWMQ_SOCKET_DEFAULT_RECONNECT_IVL_MAX_MS;
  socket->flow_update_interval_ms =
      FLOWMQ_FLOW_CONTROL_DEFAULT_UPDATE_INTERVAL_MS;
  socket->peers = (flowmq_socket_peer_t *)calloc(
      FLOWMQ_SOCKET_PEER_CAPACITY, sizeof(*socket->peers));
  socket->inbound = (flowmq_socket_message_t *)calloc(
      FLOWMQ_SOCKET_INBOUND_CAPACITY, sizeof(*socket->inbound));
  if (flowmq_protocol_encoded_size_limit(FLOWMQ_SOCKET_MAX_FRAME_SIZE,
                                         &encoded_limit) != SALTS_OK) {
    free(socket->inbound);
    free(socket->peers);
    free(socket);
    return NULL;
  }
  socket->max_encoded_size = encoded_limit;
  if (socket->peers == NULL || socket->inbound == NULL) {
    free(socket->inbound);
    free(socket->peers);
    free(socket);
    return NULL;
  }
  if (mem_init(&socket->message_pool, 0u) != 0) {
    free(socket->inbound);
    free(socket->peers);
    free(socket);
    return NULL;
  }
  socket->pool_initialized = 1u;
  if (flowmq_subscription_set_init(&socket->subscriptions) != SALTS_OK) {
    mem_destroy(&socket->message_pool);
    free(socket->inbound);
    free(socket->peers);
    free(socket);
    return NULL;
  }
  if (flowmq_pattern_state_init(&socket->pattern, pattern) != SALTS_OK) {
    mem_destroy(&socket->message_pool);
    flowmq_subscription_set_destroy(&socket->subscriptions);
    free(socket->inbound);
    free(socket->peers);
    free(socket);
    return NULL;
  }
  socket->ctx = ctx;
  ++ctx->next_socket_id;
  {
    int written = snprintf(socket->identity, sizeof(socket->identity),
                           "flowmq-%llu",
                           (unsigned long long)ctx->next_socket_id);
    if (written < 0 || (size_t)written >= sizeof(socket->identity)) {
      mem_destroy(&socket->message_pool);
      flowmq_subscription_set_destroy(&socket->subscriptions);
      free(socket->inbound);
      free(socket->peers);
      free(socket);
      return NULL;
    }
    socket->identity_size = (size_t)written;
  }
  ++ctx->socket_count;
  return socket;
}

int flowmq_close(flowmq_socket_t *socket) {
  int status;
  if (socket == NULL || socket->ctx == NULL || socket->ctx->socket_count == 0u)
    return SALTS_EINVAL;
  if (socket->listener_initialized) {
    status = cnet_listener_close(&socket->listener);
    if (status != SALTS_OK && status != SALTS_EALREADY) return status;
    status = cnet_listener_destroy(&socket->listener);
    if (status != SALTS_OK) return status;
    socket->listener_initialized = 0u;
  }
  if (socket->runtime_initialized) {
    status = cnet_client_stop(&socket->client,
                              FLOWMQ_SOCKET_SHUTDOWN_TIMEOUT_MS);
    if (status != SALTS_OK) return status;
    status = cnet_client_destroy(&socket->client);
    if (status != SALTS_OK) return status;
    socket->runtime_initialized = 0u;
  }
  if (socket->tls_client_initialized) {
    status = cnet_tls_client_destroy(&socket->tls_client);
    if (status != SALTS_OK) return status;
    socket->tls_client_initialized = 0u;
  }
  if (socket->tls_server_initialized) {
    status = cnet_tls_server_destroy(&socket->tls_server);
    if (status != SALTS_OK) return status;
    socket->tls_server_initialized = 0u;
  }
  for (size_t i = 0u; i < socket->inbound_count; ++i) {
    size_t index = (socket->inbound_read + i) % FLOWMQ_SOCKET_INBOUND_CAPACITY;
    mem_buffer_release(socket->inbound[index].buffer);
  }
  flowmq_socket_release_send_staged(socket);
  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i)
    flowmq_socket_peer_release(&socket->peers[i]);
  flowmq_subscription_set_destroy(&socket->subscriptions);
  flowmq_tls_identity_map_destroy(socket->tls_identity_policy);
  socket->tls_identity_policy = NULL;
  if (socket->pool_initialized) mem_destroy(&socket->message_pool);
  flowmq_socket_clear_secret(socket->tls_key_password,
                             sizeof(socket->tls_key_password));
  free(socket->inbound);
  free(socket->peers);
  --socket->ctx->socket_count;
  socket->ctx = NULL;
  free(socket);
  return SALTS_OK;
}

int flowmq_bind(flowmq_socket_t *socket, const char *endpoint) {
  flowmq_endpoint_parts_t parts;
  cnet_listener_config config;
  uint16_t bound_port = 0u;
  int bracket;
  int written;
  int status;
  if (socket == NULL || socket->ctx == NULL) return SALTS_EINVAL;
  status = flowmq_endpoint_parse(endpoint, 1, &parts);
  if (status != SALTS_OK) return status;
  if (socket->listener_initialized) return SALTS_EALREADY;
  if (socket->tls_identity_policy != NULL &&
      (socket->pattern.desc->routing_class != FLOWMQ_PATTERN_ROUTE_IDENTITY ||
       parts.transport != FLOWMQ_TRANSPORT_TLS ||
       !socket->tls_require_client_certificate))
    return SALTS_EINVAL;
  if (parts.transport == FLOWMQ_TRANSPORT_TLS) {
    status = flowmq_socket_tls_server_init(socket);
    if (status != SALTS_OK) return status;
  }
  status = flowmq_socket_runtime_init(socket, parts.transport);
  if (status != SALTS_OK) return status;
  config = (cnet_listener_config){.backend = flowmq_cnet_backend(),
                                  .host = parts.host,
                                  .port = parts.port,
                                  .backlog = FLOWMQ_SOCKET_PEER_CAPACITY};
  status = cnet_listener_init(&socket->listener, &config);
  if (status != SALTS_OK) return status;
  socket->listener_initialized = 1u;
  status = cnet_listener_port(&socket->listener, &bound_port);
  if (status != SALTS_OK) return status;
  bracket = strchr(parts.host, ':') != NULL;
  written = snprintf(socket->last_endpoint, sizeof(socket->last_endpoint),
                     bracket ? "%s://[%s]:%u" : "%s://%s:%u",
                     parts.transport == FLOWMQ_TRANSPORT_TLS ? "tls" : "tcp",
                     parts.host, (unsigned)bound_port);
  if (written < 0 || (size_t)written >= sizeof(socket->last_endpoint))
    return SALTS_EMSGSIZE;
  return SALTS_OK;
}

int flowmq_connect(flowmq_socket_t *socket, const char *endpoint) {
  flowmq_endpoint_parts_t parts;
  flowmq_socket_endpoint_t *owned_endpoint;
  size_t endpoint_index = FLOWMQ_SOCKET_ENDPOINT_NONE;
  size_t endpoint_size;
  uint64_t reconnect_max_ms;
  int status;
  if (socket == NULL || socket->ctx == NULL) return SALTS_EINVAL;
  status = flowmq_endpoint_parse(endpoint, 0, &parts);
  if (status != SALTS_OK) return status;
  if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_SINGLE) {
    for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
      if (flowmq_peer_state_is_used(&socket->peers[i].state))
        return SALTS_EBUSY;
    }
    for (size_t i = 0u; i < FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY; ++i) {
      if (socket->endpoints[i].used) return SALTS_EBUSY;
    }
  }
  endpoint_size = strlen(endpoint) + 1u;
  if (endpoint_size > FLOWMQ_SOCKET_ENDPOINT_CAPACITY) return SALTS_EMSGSIZE;
  status = flowmq_socket_runtime_init(socket, parts.transport);
  if (status != SALTS_OK) return status;
  if (parts.transport == FLOWMQ_TRANSPORT_TLS) {
    status = flowmq_socket_tls_client_init(socket);
    if (status != SALTS_OK) return status;
  }
  owned_endpoint = flowmq_socket_endpoint_acquire(socket, &endpoint_index);
  if (owned_endpoint == NULL) return SALTS_ENOBUFS;
  memcpy(owned_endpoint->uri, endpoint, endpoint_size);
  reconnect_max_ms =
      socket->reconnect_interval_max_ms >= socket->reconnect_interval_ms &&
              socket->reconnect_interval_ms >= 0
          ? (uint64_t)socket->reconnect_interval_max_ms
          : 0u;
  status = flowmq_reconnect_init(
      &owned_endpoint->reconnect,
      socket->reconnect_interval_ms >= 0
          ? (uint64_t)socket->reconnect_interval_ms
          : 0u,
      reconnect_max_ms, salts_hrtime() ^ (uint64_t)(endpoint_index + 1u));
  if (status == SALTS_OK)
    status = flowmq_socket_endpoint_connect(socket, endpoint_index);
  if (status != SALTS_OK) {
    memset(owned_endpoint, 0, sizeof(*owned_endpoint));
    return status;
  }
  memcpy(socket->last_endpoint, endpoint, endpoint_size);
  return SALTS_OK;
}

int flowmq_last_endpoint(const flowmq_socket_t *socket, char *buffer,
                         size_t capacity, size_t *size) {
  size_t required;
  if (size != NULL) *size = 0u;
  if (socket == NULL || buffer == NULL || size == NULL ||
      socket->last_endpoint[0] == '\0')
    return SALTS_EINVAL;
  required = strlen(socket->last_endpoint) + 1u;
  *size = required;
  if (capacity < required) return SALTS_EMSGSIZE;
  memcpy(buffer, socket->last_endpoint, required);
  return SALTS_OK;
}

int flowmq_setsockopt(flowmq_socket_t *socket, int option, const void *value,
                      size_t size) {
  const flowmq_socket_option_desc_t *desc = NULL;
  int status;
  if (socket == NULL || socket->ctx == NULL) return SALTS_EINVAL;

  status = flowmq_socket_option_validate_set(
      option, socket->runtime_initialized, socket->pattern.desc, value, size,
      &desc);
  if (status != SALTS_OK) return status;
  if (desc == NULL) return SALTS_EPROTO;

  switch (option) {
  case FLOWMQ_SUBSCRIBE:
  case FLOWMQ_UNSUBSCRIBE: {
    int changed = 0;
    return flowmq_subscription_set_update(
        &socket->subscriptions, option == FLOWMQ_SUBSCRIBE,
        (vstr){.data = (char *)value, .len = size}, &changed);
  }

  case FLOWMQ_IDENTITY:
    memcpy(socket->identity, value, size);
    socket->identity[size] = '\0';
    socket->identity_size = size;
    return SALTS_OK;

  case FLOWMQ_SNDHWM:
  case FLOWMQ_RCVHWM: {
    int value_int;
    memcpy(&value_int, value, sizeof(value_int));
    if (option == FLOWMQ_SNDHWM)
      socket->send_hwm = (size_t)value_int;
    else
      socket->receive_hwm = (size_t)value_int;
    return SALTS_OK;
  }

  case FLOWMQ_HEARTBEAT_IVL:
  case FLOWMQ_HEARTBEAT_TIMEOUT: {
    int value_int;
    memcpy(&value_int, value, sizeof(value_int));
    if (option == FLOWMQ_HEARTBEAT_IVL)
      socket->heartbeat_interval_ms = value_int;
    else {
      socket->heartbeat_timeout_ms = value_int;
      socket->heartbeat_timeout_set = 1u;
    }
    return SALTS_OK;
  }

  case FLOWMQ_RECONNECT_IVL:
  case FLOWMQ_RECONNECT_IVL_MAX: {
    int value_int;
    memcpy(&value_int, value, sizeof(value_int));
    if (option == FLOWMQ_RECONNECT_IVL)
      socket->reconnect_interval_ms = value_int;
    else
      socket->reconnect_interval_max_ms = value_int;
    return SALTS_OK;
  }

  case FLOWMQ_FLOW_UPDATE_IVL: {
    int value_int;
    memcpy(&value_int, value, sizeof(value_int));
    socket->flow_update_interval_ms = value_int;
    return SALTS_OK;
  }

  case FLOWMQ_FLOW_UPDATE_QUANTUM: {
    size_t value_size;
    memcpy(&value_size, value, sizeof(value_size));
    if (value_size > socket->receive_hwm_bytes) return SALTS_EINVAL;
    socket->flow_update_quantum = (uint32_t)value_size;
    return SALTS_OK;
  }

  case FLOWMQ_SNDHWM_BYTES:
  case FLOWMQ_RCVHWM_BYTES: {
    size_t value_size;
    memcpy(&value_size, value, sizeof(value_size));
    if (option == FLOWMQ_SNDHWM_BYTES)
      socket->send_hwm_bytes = value_size;
    else {
      if (socket->flow_update_quantum != 0u &&
          socket->flow_update_quantum > value_size)
        return SALTS_EINVAL;
      socket->receive_hwm_bytes = value_size;
    }
    return SALTS_OK;
  }

  case FLOWMQ_TLS_CA_FILE:
    return flowmq_socket_store_string(socket->tls_ca_file,
                                      sizeof(socket->tls_ca_file), value, size);
  case FLOWMQ_TLS_CERT_FILE:
    return flowmq_socket_store_string(socket->tls_cert_file,
                                      sizeof(socket->tls_cert_file), value,
                                      size);
  case FLOWMQ_TLS_KEY_FILE:
    return flowmq_socket_store_string(socket->tls_key_file,
                                      sizeof(socket->tls_key_file), value, size);
  case FLOWMQ_TLS_KEY_PASSWORD:
    return flowmq_socket_store_string(socket->tls_key_password,
                                      sizeof(socket->tls_key_password), value,
                                      size);
  case FLOWMQ_TLS_SERVER_NAME:
    return flowmq_socket_store_string(socket->tls_server_name,
                                      sizeof(socket->tls_server_name), value,
                                      size);

  case FLOWMQ_TLS_REQUIRE_CLIENT_CERTIFICATE: {
    int value_int;
    memcpy(&value_int, value, sizeof(value_int));
    socket->tls_require_client_certificate = value_int != 0;
    return SALTS_OK;
  }

  case FLOWMQ_TLS_IDENTITY_POLICY: {
    flowmq_tls_identity_map_t *replacement = NULL;
    status = flowmq_tls_identity_map_create(
        (const flowmq_tls_identity_map_config_t *)value, &replacement);
    if (status != SALTS_OK) return status;
    flowmq_tls_identity_map_destroy(socket->tls_identity_policy);
    socket->tls_identity_policy = replacement;
    return SALTS_OK;
  }

  default:
    /* A SET-capable schema row without an apply path is an internal drift. */
    return SALTS_EPROTO;
  }
}

int flowmq_getsockopt(const flowmq_socket_t *socket, int option, void *value,
                      size_t *size) {
  int status;
  if (socket == NULL || socket->ctx == NULL || size == NULL)
    return SALTS_EINVAL;
  status = flowmq_socket_option_prepare_get(option, value, size, NULL);
  if (status != SALTS_OK) return status;

  switch (option) {
  case FLOWMQ_RECONNECT_IVL: {
    int result = socket->reconnect_interval_ms;
    memcpy(value, &result, sizeof(result));
    return SALTS_OK;
  }
  case FLOWMQ_RECONNECT_IVL_MAX: {
    int result = socket->reconnect_interval_max_ms;
    memcpy(value, &result, sizeof(result));
    return SALTS_OK;
  }
  case FLOWMQ_RCVMORE: {
    int result = socket->last_rcvmore;
    memcpy(value, &result, sizeof(result));
    return SALTS_OK;
  }
  case FLOWMQ_TLS_IDENTITY_REJECTIONS: {
    uint64_t result = socket->tls_identity_rejections;
    memcpy(value, &result, sizeof(result));
    return SALTS_OK;
  }
  default:
    /* A GET-capable schema row without a read path is an internal drift. */
    return SALTS_EPROTO;
  }
}

/* All fallible validation and allocation precedes peer-ring mutation, so the
 * final commit cannot leave a subset of parts admitted. Time is O(peers +
 * parts), space is O(parts), with both dimensions bounded by socket constants. */
static int flowmq_socket_try_send_multipart(flowmq_socket_t *socket, const void *data, size_t size,
                                            int flags, int starting_message) {
  flowmq_socket_outbound_t part = {0};
  flowmq_socket_peer_t *peer = NULL;
  size_t selected_peer_index = socket->send_peer_index;
  uint64_t selected_peer_generation = socket->send_peer_generation;
  uint64_t peer_generations[FLOWMQ_SOCKET_PEER_CAPACITY] = {0};
  size_t part_count = socket->send_staged_count + 1u;
  size_t max_parts = FLOWMQ_SOCKET_MULTIPART_CAPACITY;
  size_t message_size;
  uint32_t peer_mask = socket->publish_peer_mask;
  int message_end = (flags & FLOWMQ_SNDMORE) == 0;
  int status;
  if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY) --max_parts;
  if (part_count > max_parts || socket->send_staged_bytes > socket->send_hwm_bytes ||
      size > socket->send_hwm_bytes - socket->send_staged_bytes)
    return SALTS_EMSGSIZE;
  message_size = socket->send_staged_bytes + size;

  if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_FANOUT) {
    if (starting_message) {
      peer_mask = 0u;
      for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
        flowmq_socket_peer_t *candidate = &socket->peers[i];
        if (flowmq_socket_peer_ready(candidate) &&
            flowmq_subscription_set_match(
                &candidate->subscriptions,
                (vstr){.data = (char *)data, .len = size})) {
          peer_mask |= UINT32_C(1) << i;
          peer_generations[i] = candidate->flow_control.local_generation;
        }
      }
    } else {
      memcpy(peer_generations, socket->publish_peer_generations,
             sizeof(peer_generations));
    }
    if (message_end) {
      for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
        uint32_t bit = UINT32_C(1) << i;
        if ((peer_mask & bit) != 0u &&
            (!flowmq_socket_peer_generation_ready(
                 &socket->peers[i], peer_generations[i]) ||
             !flowmq_socket_peer_can_admit_message(
                 &socket->peers[i], message_size, part_count, size)))
          peer_mask &= ~bit;
      }
    }
  } else if (starting_message) {
    if (FLOWMQ_SOCKET_IS_REPLY_PEER_PATTERN(socket)) {
      if (!socket->reply_peer_valid ||
          socket->reply_peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY)
        return SALTS_EBUSY;
      selected_peer_index = socket->reply_peer_index;
      peer = &socket->peers[selected_peer_index];
      if (!flowmq_socket_peer_generation_ready(
              peer, socket->reply_peer_generation))
        return SALTS_EBUSY;
      selected_peer_generation = socket->reply_peer_generation;
    } else {
      for (size_t offset = 0u; offset < FLOWMQ_SOCKET_PEER_CAPACITY; ++offset) {
        size_t index = (socket->peer_cursor + offset) % FLOWMQ_SOCKET_PEER_CAPACITY;
        if (flowmq_socket_peer_ready(&socket->peers[index])) {
          selected_peer_index = index;
          peer = &socket->peers[index];
          selected_peer_generation =
              peer->flow_control.local_generation;
          break;
        }
      }
      if (peer == NULL) return SALTS_EBUSY;
    }
  } else {
    if (!socket->send_peer_active || socket->send_peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY)
      return SALTS_EPROTO;
    selected_peer_index = socket->send_peer_index;
    peer = &socket->peers[selected_peer_index];
    if (!flowmq_socket_peer_generation_ready(peer, selected_peer_generation)) {
      flowmq_socket_cancel_send_route(socket);
      return SALTS_ENOTCONN;
    }
  }

  if (message_end && peer != NULL &&
      !flowmq_socket_peer_can_admit_message(peer, message_size, part_count,
                                            size)) {
    /*
     * ROUTER routing-id selection is part of the same atomic message
     * transaction. A target-specific capacity failure must not leave the
     * socket pinned to that peer; otherwise unrelated ready peers inherit
     * cross-peer HOL blocking. Other multipart patterns retain their staged
     * retry semantics.
     */
    if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY)
      flowmq_socket_cancel_send_route(socket);
    return SALTS_ENOBUFS;
  }
  status = flowmq_socket_prepare_outbound(socket, data, size, !message_end, &part);
  if (status != SALTS_OK) return status;
  socket->send_staged[socket->send_staged_count] = part;
  ++socket->send_staged_count;
  socket->send_staged_bytes = message_size;

  if (!message_end) {
    if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_FANOUT) {
      socket->publish_peer_mask = peer_mask;
      memcpy(socket->publish_peer_generations, peer_generations,
             sizeof(peer_generations));
    } else {
      socket->send_peer_index = selected_peer_index;
      socket->send_peer_generation = selected_peer_generation;
      socket->send_peer_active = 1u;
      if (starting_message)
        socket->peer_cursor = (selected_peer_index + 1u) % FLOWMQ_SOCKET_PEER_CAPACITY;
    }
    flowmq_pattern_state_send_commit(&socket->pattern, 1);
    return SALTS_OK;
  }

  if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_FANOUT) {
    for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
      if ((peer_mask & (UINT32_C(1) << i)) != 0u) {
        status = flowmq_socket_peer_admit_staged(&socket->peers[i], socket);
        if (status != SALTS_OK) return status;
      }
    }
    socket->publish_peer_mask = 0u;
    memset(socket->publish_peer_generations, 0,
           sizeof(socket->publish_peer_generations));
  } else {
    status = flowmq_socket_peer_admit_staged(peer, socket);
    if (status != SALTS_OK) return status;
    if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ) {
      socket->request_peer_index = selected_peer_index;
      socket->request_peer_generation =
          peer->flow_control.local_generation;
      socket->request_peer_valid = 1u;
      socket->recv_cancel_error = SALTS_OK;
    }
    if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REP) {
      socket->reply_peer_valid = 0u;
      socket->reply_peer_generation = 0u;
    }
  }
  flowmq_socket_release_send_staged(socket);
  socket->send_peer_active = 0u;
  socket->send_peer_generation = 0u;
  ++socket->next_message_id;
  flowmq_pattern_state_send_commit(&socket->pattern, 0);
  return SALTS_OK;
}

static int flowmq_socket_try_send(flowmq_socket_t *socket, const void *data,
                                  size_t size, int flags) {
  flowmq_protocol_frame_t frame;
  flowmq_socket_peer_t *peer = NULL;
  size_t segment_count = 0u;
  size_t encoded_size = 0u;
  int starting_message;
  int message_end;
  int peer_saturated = 0;
  int status;
  if (socket == NULL || (data == NULL && size != 0u) ||
      (flags & ~(FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE)) != 0)
    return SALTS_EINVAL;
  if (socket->send_cancel_error != SALTS_OK) {
    status = socket->send_cancel_error;
    socket->send_cancel_error = SALTS_OK;
    return status;
  }
  status = flowmq_pattern_state_send_validate(&socket->pattern);
  if (status != SALTS_OK) return status;
  if (size > FLOWMQ_SOCKET_MAX_FRAME_SIZE) return SALTS_EMSGSIZE;
  if (size > socket->send_hwm_bytes) return SALTS_EMSGSIZE;
  starting_message = !socket->pattern.sending_multipart;
  message_end = (flags & FLOWMQ_SNDMORE) == 0;
  if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY &&
      !socket->pattern.sending_multipart) {
    if ((flags & FLOWMQ_SNDMORE) == 0 || size == 0u) return SALTS_EINVAL;
    for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
      flowmq_socket_peer_t *candidate = &socket->peers[i];
      if (flowmq_socket_peer_ready(candidate) &&
          candidate->identity_size == size &&
          memcmp(candidate->identity, data, size) == 0) {
        socket->send_peer_index = i;
        socket->send_peer_generation =
            candidate->flow_control.local_generation;
        socket->send_peer_active = 1u;
        flowmq_pattern_state_send_commit(&socket->pattern, 1);
        return SALTS_OK;
      }
    }
    return SALTS_ENOENT;
  }
  if (socket->pattern.sending_multipart ||
      (flags & FLOWMQ_SNDMORE) != 0)
    return flowmq_socket_try_send_multipart(socket, data, size, flags,
                                            starting_message);
  if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_FANOUT) {
    uint32_t peer_mask = socket->pattern.sending_multipart
                             ? socket->publish_peer_mask
                             : 0u;
    if (!socket->pattern.sending_multipart) {
      for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
        flowmq_socket_peer_t *candidate = &socket->peers[i];
        if (flowmq_socket_peer_ready(candidate) &&
            flowmq_socket_peer_can_admit(candidate, size, message_end) &&
            flowmq_subscription_set_match(
                &candidate->subscriptions,
                (vstr){.data = (char *)data, .len = size}))
          peer_mask |= UINT32_C(1) << i;
      }
    } else {
      for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
        if ((peer_mask & (UINT32_C(1) << i)) != 0u &&
            !flowmq_socket_peer_can_admit(&socket->peers[i], size,
                                          message_end))
          return SALTS_ENOBUFS;
      }
    }
    if (peer_mask != 0u) {
      frame = (flowmq_protocol_frame_t){
          .kind = FLOWMQ_PROTOCOL_FRAME_DATA,
          .pattern = socket->pattern.pattern,
          .message_id = socket->next_message_id + 1u,
          .more = (flags & FLOWMQ_SNDMORE) != 0,
          .payload = {.data = (char *)data, .len = size}};
      status = flowmq_socket_encode_frame_segments(
          socket, &frame, &segment_count, &encoded_size);
      if (status != SALTS_OK) return status;
      for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
        flowmq_socket_peer_t *candidate = &socket->peers[i];
        if ((peer_mask & (UINT32_C(1) << i)) == 0u) continue;
        status = flowmq_socket_peer_admit(
            candidate, socket->send_segments, segment_count, encoded_size,
            size, message_end);
        if (status != SALTS_OK) break;
      }
      if (status != SALTS_OK) return status;
    }
    ++socket->next_message_id;
    socket->publish_peer_mask =
        (flags & FLOWMQ_SNDMORE) != 0 ? peer_mask : 0u;
    flowmq_pattern_state_send_commit(&socket->pattern,
                                     (flags & FLOWMQ_SNDMORE) != 0);
    return SALTS_OK;
  }
  if (FLOWMQ_SOCKET_IS_REPLY_PEER_PATTERN(socket) &&
      !socket->send_peer_active) {
    if (!socket->reply_peer_valid ||
        socket->reply_peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY)
      return SALTS_EBUSY;
    peer = &socket->peers[socket->reply_peer_index];
    if (!flowmq_socket_peer_generation_ready(
            peer, socket->reply_peer_generation))
      peer = NULL;
    else if (!flowmq_socket_peer_can_admit(peer, size, message_end)) {
      peer_saturated = 1;
      peer = NULL;
    }
    else {
      socket->send_peer_index = socket->reply_peer_index;
    }
  } else if (socket->send_peer_active) {
    flowmq_socket_peer_t *candidate = &socket->peers[socket->send_peer_index];
    if (flowmq_socket_peer_generation_ready(
            candidate, socket->send_peer_generation)) {
      if (flowmq_socket_peer_can_admit(candidate, size, message_end))
        peer = candidate;
      else
        peer_saturated = 1;
    }
  } else {
    for (size_t offset = 0u; offset < FLOWMQ_SOCKET_PEER_CAPACITY; ++offset) {
      size_t index = (socket->peer_cursor + offset) % FLOWMQ_SOCKET_PEER_CAPACITY;
      flowmq_socket_peer_t *candidate = &socket->peers[index];
      if (flowmq_socket_peer_ready(candidate)) {
        if (flowmq_socket_peer_can_admit(candidate, size, message_end)) {
          peer = candidate;
          socket->peer_cursor = (index + 1u) % FLOWMQ_SOCKET_PEER_CAPACITY;
          socket->send_peer_index = index;
          break;
        }
        peer_saturated = 1;
      }
    }
  }
  if (peer == NULL) {
    return peer_saturated ? SALTS_ENOBUFS : SALTS_EBUSY;
  }
  frame = (flowmq_protocol_frame_t){
      .kind = FLOWMQ_PROTOCOL_FRAME_DATA,
      .pattern = socket->pattern.pattern,
      .message_id = socket->next_message_id + 1u,
      .more = (flags & FLOWMQ_SNDMORE) != 0,
      .payload = {.data = (char *)data, .len = size}};
  status = flowmq_socket_encode_frame_segments(
      socket, &frame, &segment_count, &encoded_size);
  if (status == SALTS_OK)
    status = flowmq_socket_peer_admit(
        peer, socket->send_segments, segment_count, encoded_size, size,
        message_end);
  if (status != SALTS_OK) return status;
  ++socket->next_message_id;
  if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ && starting_message) {
    socket->request_peer_index = socket->send_peer_index;
    socket->request_peer_generation = peer->flow_control.local_generation;
    socket->request_peer_valid = 1u;
    socket->recv_cancel_error = SALTS_OK;
  }
  if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REP &&
      (flags & FLOWMQ_SNDMORE) == 0) {
    socket->reply_peer_valid = 0u;
    socket->reply_peer_generation = 0u;
  }
  socket->send_peer_active = (flags & FLOWMQ_SNDMORE) != 0;
  if (!socket->send_peer_active) socket->send_peer_generation = 0u;
  flowmq_pattern_state_send_commit(&socket->pattern,
                                   (flags & FLOWMQ_SNDMORE) != 0);
  return SALTS_OK;
}

static int flowmq_socket_try_recv(flowmq_socket_t *socket, void *data,
                                  size_t capacity, size_t *received,
                                  int flags) {
  flowmq_socket_message_t *message;
  flowmq_socket_peer_t *peer;
  int message_more;
  int status;
  if (received != NULL) *received = 0u;
  if (socket == NULL || received == NULL || (data == NULL && capacity != 0u) ||
      (flags & ~FLOWMQ_DONTWAIT) != 0)
    return SALTS_EINVAL;
  if (socket->recv_cancel_error != SALTS_OK) {
    status = socket->recv_cancel_error;
    socket->recv_cancel_error = SALTS_OK;
    return status;
  }
  status = flowmq_pattern_state_receive_validate(&socket->pattern);
  if (status != SALTS_OK) return status;
  if (socket->inbound_count == 0u) return SALTS_EBUSY;
  message = &socket->inbound[socket->inbound_read];
  if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ &&
      socket->request_peer_valid &&
      (message->peer_index != socket->request_peer_index ||
       message->peer_generation != socket->request_peer_generation))
    return SALTS_EPROTO;
  *received = message->size;
  if (capacity < message->size) return SALTS_EMSGSIZE;
  if (socket->inbound_bytes < message->size ||
      (!message->more && socket->inbound_messages == 0u))
    return SALTS_EPROTO;
  if (message->peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY) return SALTS_EPROTO;
  peer = &socket->peers[message->peer_index];
  if (!flowmq_peer_state_is_used(&peer->state) ||
      message->peer_generation == 0u ||
      message->peer_generation != peer->flow_control.local_generation ||
      peer->queued_parts == 0u)
    return SALTS_EPROTO;
  if (message->credit_size != 0u &&
      !flowmq_peer_state_is_retired(&peer->state)) {
    uint64_t consumed_at_ns = peer->flow_control.update_pending
                                  ? 0u
                                  : salts_hrtime();
    status = flowmq_flow_control_consume(
        &peer->flow_control, message->credit_size, consumed_at_ns);
    if (status != SALTS_OK) return status;
  }
  if (message->size != 0u)
    memcpy(data, mem_buffer_const_data(message->buffer), message->size);
  message_more = message->more;
  socket->last_rcvmore = message_more != 0;
  flowmq_pattern_state_receive_commit(&socket->pattern, message_more);
  if (!message_more && socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REP) {
    if (flowmq_peer_state_is_retired(&peer->state)) {
      if (socket->send_cancel_error == SALTS_OK)
        socket->send_cancel_error = SALTS_ENOTCONN;
      socket->reply_peer_valid = 0u;
      socket->reply_peer_generation = 0u;
      flowmq_pattern_state_cancel_transaction(&socket->pattern);
    } else {
      socket->reply_peer_index = message->peer_index;
      socket->reply_peer_generation = message->peer_generation;
      socket->reply_peer_valid = 1u;
      socket->send_cancel_error = SALTS_OK;
    }
  }
  if (!message_more && socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ) {
    socket->request_peer_valid = 0u;
    socket->request_peer_generation = 0u;
  }
  mem_buffer_release(message->buffer);
  socket->inbound_bytes -= message->size;
  if (!message_more) --socket->inbound_messages;
  --peer->queued_parts;
  memset(message, 0, sizeof(*message));
  socket->inbound_read =
      (socket->inbound_read + 1u) % FLOWMQ_SOCKET_INBOUND_CAPACITY;
  --socket->inbound_count;
  if (flowmq_peer_state_is_retired(&peer->state) &&
      peer->queued_parts == 0u)
    flowmq_socket_peer_release(peer);
  return SALTS_OK;
}

static int flowmq_socket_resume_receive(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  int status;
  if (!peer->commit_pending) return SALTS_OK;
  status = flowmq_socket_commit_staged(peer);
  if (status == SALTS_ENOBUFS) return SALTS_OK;
  if (status != SALTS_OK) return status;
  peer->commit_pending = 0u;
  status = flowmq_socket_process_receive(peer);
  if (status == SALTS_ENOBUFS) return SALTS_OK;
  if (status != SALTS_OK) return status;
  return cnet_receive(&socket->client, peer->connection, 1u);
}

static int flowmq_socket_drive(flowmq_socket_t *socket, uint32_t timeout_ms,
                               size_t *events) {
  size_t client_events = 0u;
  int status;
  if (events != NULL) *events = 0u;
  if (socket == NULL || !socket->runtime_initialized) return SALTS_OK;
  if (socket->listener_initialized) {
    int ready = 0;
    status = cnet_listener_wait(&socket->listener, 0u, &ready);
    if (status != SALTS_OK) return status;
    if (ready) {
      flowmq_socket_peer_t *peer = flowmq_socket_peer_acquire(socket);
      cnet_observer observer;
      if (peer == NULL) return SALTS_ENOBUFS;
      observer = flowmq_socket_observer(peer);
      status = socket->transport == FLOWMQ_TRANSPORT_TLS
                   ? cnet_listener_accept_tls(&socket->listener, &socket->client,
                                              &socket->tls_server, &observer,
                                              &peer->connection)
                   : cnet_listener_accept(&socket->listener, &socket->client,
                                          &observer, &peer->connection);
      if (status != SALTS_OK) {
        flowmq_socket_peer_release(peer);
        if (status != SALTS_ETIMEDOUT) return status;
      }
    }
  }
  status = cnet_client_poll(&socket->client, timeout_ms, &client_events);
  if (status != SALTS_OK) return status;
  status = flowmq_socket_reconnect_progress(socket);
  if (status != SALTS_OK) return status;
  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
    flowmq_socket_peer_t *peer = &socket->peers[i];
    if (!flowmq_peer_state_is_used(&peer->state)) continue;
    if (flowmq_peer_state_needs_close_retry(&peer->state)) {
      status = cnet_close(&socket->client, peer->connection);
      if (status == SALTS_OK || status == SALTS_EALREADY) {
        if (flowmq_peer_state_transition(
                &peer->state, FLOWMQ_PEER_LIFECYCLE_CLOSING) != SALTS_OK) {
          flowmq_socket_fail(socket, SALTS_EPROTO);
          return SALTS_EPROTO;
        }
      } else if (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
        continue;
      } else {
        flowmq_socket_fail(socket, status);
        return status;
      }
    }
    if (!flowmq_peer_state_is_connected(&peer->state)) continue;
    if (peer->commit_pending) {
      status = flowmq_socket_resume_receive(peer);
      if (status != SALTS_OK && status != SALTS_EBUSY &&
          status != SALTS_ENOBUFS) {
        flowmq_socket_peer_fail(peer);
        continue;
      }
    }
    if (flowmq_peer_state_handshake_has(
            &peer->state, FLOWMQ_PEER_HANDSHAKE_HELLO_TX) &&
        !flowmq_peer_state_handshake_has(
            &peer->state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_TX) &&
        flowmq_peer_state_write_idle(&peer->state)) {
      status = flowmq_socket_send_settings(peer);
      if (status != SALTS_OK && status != SALTS_EBUSY &&
          status != SALTS_ENOBUFS) {
        flowmq_socket_peer_fail(peer);
        continue;
      }
    }
    status = flowmq_socket_peer_heartbeat_progress(peer);
    if (status != SALTS_OK) {
      flowmq_socket_peer_fail(peer);
      continue;
    }
    status = flowmq_socket_peer_flow_update_progress(peer);
    if (status != SALTS_OK) {
      flowmq_socket_peer_fail(peer);
      continue;
    }
    if (socket->pattern.desc->subscription_class == FLOWMQ_PATTERN_SUB_SUBSCRIBER) {
      status = flowmq_socket_sync_subscription(peer);
      if (status != SALTS_OK && status != SALTS_EBUSY &&
          status != SALTS_ENOBUFS) {
        flowmq_socket_peer_fail(peer);
        continue;
      }
    }
    if (flowmq_peer_state_write_idle(&peer->state) &&
        peer->outbound_count != 0u) {
      status = flowmq_socket_peer_flush(peer);
      if (status != SALTS_OK && status != SALTS_EBUSY &&
          status != SALTS_ENOBUFS) {
        flowmq_socket_peer_fail(peer);
        continue;
      }
    }
  }
  if (events != NULL) *events = client_events;
  if (socket->async_error != SALTS_OK) return socket->async_error;
  return SALTS_OK;
}

int flowmq_send(flowmq_socket_t *socket, const void *data, size_t size,
                int flags) {
  int status;
  for (;;) {
    status = flowmq_socket_try_send(socket, data, size, flags);
    if (status != SALTS_EBUSY && status != SALTS_ENOBUFS) return status;
    if ((flags & FLOWMQ_DONTWAIT) != 0 || !socket->runtime_initialized)
      return status;
    status = flowmq_socket_drive(socket, FLOWMQ_SOCKET_BLOCKING_SLICE_MS, NULL);
    if (status != SALTS_OK) return status;
  }
}

int flowmq_recv(flowmq_socket_t *socket, void *data, size_t capacity,
                size_t *received, int flags) {
  int status;
  for (;;) {
    status = flowmq_socket_try_recv(socket, data, capacity, received, flags);
    if (status != SALTS_EBUSY) return status;
    if ((flags & FLOWMQ_DONTWAIT) != 0 || !socket->runtime_initialized)
      return status;
    status = flowmq_socket_drive(socket, FLOWMQ_SOCKET_BLOCKING_SLICE_MS, NULL);
    if (status != SALTS_OK) return status;
  }
}

static int flowmq_socket_pollin_ready(const flowmq_socket_t *socket) {
  return socket->inbound_count != 0u &&
         flowmq_pattern_state_receive_validate(&socket->pattern) == SALTS_OK;
}

static int flowmq_socket_pollout_ready(const flowmq_socket_t *socket) {
  if (flowmq_pattern_state_send_validate(&socket->pattern) != SALTS_OK ||
      socket->send_staged_count >= FLOWMQ_SOCKET_MULTIPART_CAPACITY ||
      socket->send_staged_bytes >= socket->send_hwm_bytes)
    return 0;
  if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_FANOUT)
    return 1;
  if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY &&
      !socket->pattern.sending_multipart) {
    for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
      if (flowmq_socket_peer_ready(&socket->peers[i])) return 1;
    }
    return 0;
  }
  if (socket->send_peer_active) {
    if (socket->send_peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY) return 0;
    if (!flowmq_socket_peer_generation_ready(
            &socket->peers[socket->send_peer_index],
            socket->send_peer_generation))
      return 0;
    return flowmq_socket_peer_can_admit(
        &socket->peers[socket->send_peer_index], 1u, 1);
  }
  if (FLOWMQ_SOCKET_IS_REPLY_PEER_PATTERN(socket)) {
    if (!socket->reply_peer_valid ||
        socket->reply_peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY)
      return 0;
    if (!flowmq_socket_peer_generation_ready(
            &socket->peers[socket->reply_peer_index],
            socket->reply_peer_generation))
      return 0;
    return flowmq_socket_peer_can_admit(
        &socket->peers[socket->reply_peer_index], 1u, 1);
  }
  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
    if (flowmq_socket_peer_can_admit(&socket->peers[i], 1u, 1)) return 1;
  }
  return 0;
}

int flowmq_poll(flowmq_pollitem_t *items, size_t item_count,
                uint32_t timeout_ms, size_t *ready) {
  const uint64_t started_ms = salts_monotonic_ms();
  if (ready != NULL) *ready = 0u;
  if (items == NULL || item_count == 0u || ready == NULL) return SALTS_EINVAL;
  for (;;) {
    *ready = 0u;
    for (size_t i = 0u; i < item_count; ++i) {
      flowmq_socket_t *socket = items[i].socket;
      size_t events = 0u;
      int status;
      items[i].revents = 0;
      if (socket == NULL) return SALTS_EINVAL;
      status = flowmq_socket_drive(socket, 0u, &events);
      if (status != SALTS_OK) {
        if (socket->async_error == status) {
          items[i].revents |= FLOWMQ_POLLERR;
          ++*ready;
          continue;
        }
        return status;
      }
      if (socket->send_cancel_error != SALTS_OK ||
          socket->recv_cancel_error != SALTS_OK)
        items[i].revents |= FLOWMQ_POLLERR;
      if ((items[i].events & FLOWMQ_POLLIN) &&
          flowmq_socket_pollin_ready(socket))
        items[i].revents |= FLOWMQ_POLLIN;
      if ((items[i].events & FLOWMQ_POLLOUT) &&
          flowmq_socket_pollout_ready(socket))
        items[i].revents |= FLOWMQ_POLLOUT;
      if (items[i].revents != 0) ++*ready;
      (void)events;
    }
    if (*ready != 0u || timeout_ms == 0u) return SALTS_OK;
    {
      const uint64_t elapsed_ms = salts_monotonic_ms() - started_ms;
      const uint64_t remaining_ms =
          elapsed_ms >= timeout_ms ? 0u : (uint64_t)timeout_ms - elapsed_ms;
      if (remaining_ms == 0u) return SALTS_OK;
      salts_sleep_ms(remaining_ms > 1u ? 1u : (uint32_t)remaining_ms);
    }
  }
}
