#include "flowmq_socket.h"

#include "flowmq_cnet_transport.h"
#include "flowmq_flow_control.h"
#include "flowmq_pattern.h"
#include "flowmq_pattern_state.h"
#include "flowmq_protocol_internal.h"
#include "flowmq_stream_decoder.h"
#include "flowmq_subscription_set.h"
#include "turbo_error.h"
#include "turbo_buffer.h"
#include "turbo_str.h"

#include <cnet/cnet.h>
#include <turbo/clock.h>
#include <turbo/thread.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  FLOWMQ_SOCKET_PEER_CAPACITY = 4u,
  FLOWMQ_SOCKET_INBOUND_CAPACITY = 1024u,
  FLOWMQ_SOCKET_OUTBOUND_CAPACITY = 1024u,
  FLOWMQ_SOCKET_MULTIPART_CAPACITY = 64u,
  FLOWMQ_SOCKET_DEFAULT_HWM = 1000u,
  FLOWMQ_SOCKET_DEFAULT_HWM_BYTES = 16u * 1024u * 1024u,
  FLOWMQ_SOCKET_HARD_HWM_BYTES = 64u * 1024u * 1024u,
  FLOWMQ_SOCKET_MAX_FRAME_SIZE = 1024u * 1024u,
  FLOWMQ_SOCKET_BLOCKING_SLICE_MS = 10u,
  FLOWMQ_SOCKET_SHUTDOWN_TIMEOUT_MS = 1000u,
  FLOWMQ_SOCKET_DEFAULT_TIMEOUT_MS = 1000u,
  FLOWMQ_SOCKET_CNET_COMMAND_CAPACITY = 16u,
  FLOWMQ_SOCKET_HOST_CAPACITY = 256u,
  FLOWMQ_SOCKET_ENDPOINT_CAPACITY = 320u,
  FLOWMQ_SOCKET_TLS_PATH_CAPACITY = 1024u,
  FLOWMQ_SOCKET_TLS_PASSWORD_CAPACITY = 512u
};

typedef struct flowmq_socket_peer_s flowmq_socket_peer_t;

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
  cnet_connection connection;
  flowmq_stream_decoder_t decoder;
  flowmq_subscription_set_t subscriptions;
  flowmq_subscription_set_t synced_subscriptions;
  flowmq_protocol_heartbeat_deadlines_t heartbeat;
  flowmq_flow_control_t flow_control;
  flowmq_socket_outbound_t *outbound;
  flowmq_protocol_pattern_t remote_pattern;
  size_t identity_size;
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
  unsigned used : 1;
  unsigned connected : 1;
  unsigned hello_sent : 1;
  unsigned hello_received : 1;
  unsigned settings_sent : 1;
  unsigned settings_received : 1;
  unsigned write_busy : 1;
  unsigned writing_hello : 1;
  unsigned writing_settings : 1;
  unsigned writing_data : 1;
  unsigned receiving_multipart : 1;
  unsigned commit_pending : 1;
  unsigned heartbeat_active : 1;
  unsigned heartbeat_closing : 1;
  unsigned pong_pending : 1;
  unsigned close_pending : 1;
  unsigned retired : 1;
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
  flowmq_socket_peer_t *peers;
  flowmq_socket_message_t *inbound;
  flowmq_socket_outbound_t send_staged[FLOWMQ_SOCKET_MULTIPART_CAPACITY];
  unsigned char *send_scratch;
  size_t send_scratch_capacity;
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
  uint64_t publish_peer_generations[FLOWMQ_SOCKET_PEER_CAPACITY];
  uint32_t flow_update_quantum;
  int heartbeat_interval_ms;
  int heartbeat_timeout_ms;
  int flow_update_interval_ms;
  int async_error;
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

static int flowmq_endpoint_parse(const char *endpoint, int allow_zero_port,
                                 flowmq_endpoint_parts_t *parts) {
  const char *host_begin;
  const char *host_end;
  const char *port_begin;
  char *port_end = NULL;
  unsigned long port;
  size_t host_size;
  if (endpoint == NULL || parts == NULL) return TURBO_EINVAL;
  memset(parts, 0, sizeof(*parts));
  if (strncmp(endpoint, "tcp://", 6u) == 0) {
    parts->transport = FLOWMQ_TRANSPORT_TCP;
    host_begin = endpoint + 6u;
  } else if (strncmp(endpoint, "tls://", 6u) == 0) {
    parts->transport = FLOWMQ_TRANSPORT_TLS;
    host_begin = endpoint + 6u;
  } else {
    return TURBO_EINVAL;
  }
  if (*host_begin == '[') {
    host_begin++;
    host_end = strchr(host_begin, ']');
    if (host_end == NULL || host_end[1] != ':') return TURBO_EINVAL;
    port_begin = host_end + 2u;
  } else {
    host_end = strrchr(host_begin, ':');
    if (host_end == NULL) return TURBO_EINVAL;
    port_begin = host_end + 1u;
  }
  host_size = (size_t)(host_end - host_begin);
  if (host_size == 0u || host_size >= sizeof(parts->host) || *port_begin == '\0')
    return TURBO_EINVAL;
  memcpy(parts->host, host_begin, host_size);
  parts->host[host_size] = '\0';
  port = strtoul(port_begin, &port_end, 10);
  if (port_end == port_begin || *port_end != '\0' || port > 65535u ||
      (!allow_zero_port && port == 0u))
    return TURBO_EINVAL;
  parts->port = (uint16_t)port;
  return TURBO_OK;
}

static flowmq_socket_peer_t *flowmq_socket_peer_acquire(flowmq_socket_t *socket) {
  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
    flowmq_socket_peer_t *peer = &socket->peers[i];
    if (!peer->used) {
      memset(peer, 0, sizeof(*peer));
      peer->owner = socket;
      peer->used = 1u;
      peer->outbound = (flowmq_socket_outbound_t *)calloc(
          FLOWMQ_SOCKET_OUTBOUND_CAPACITY, sizeof(*peer->outbound));
      if (flowmq_stream_decoder_prepare(&peer->decoder,
                                        FLOWMQ_SOCKET_MAX_FRAME_SIZE) != TURBO_OK ||
          flowmq_subscription_set_init(&peer->subscriptions) != TURBO_OK ||
          flowmq_subscription_set_init(&peer->synced_subscriptions) != TURBO_OK ||
          peer->outbound == NULL) {
        flowmq_stream_decoder_destroy(&peer->decoder);
        flowmq_subscription_set_destroy(&peer->subscriptions);
        flowmq_subscription_set_destroy(&peer->synced_subscriptions);
        free(peer->outbound);
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
  peer->write_busy = 0u;
  peer->writing_hello = 0u;
  peer->writing_settings = 0u;
  peer->writing_data = 0u;
  flowmq_stream_decoder_destroy(&peer->decoder);
  flowmq_subscription_set_destroy(&peer->subscriptions);
  flowmq_subscription_set_destroy(&peer->synced_subscriptions);
  free(peer->outbound);
  peer->outbound = NULL;
}

static void flowmq_socket_peer_release(flowmq_socket_peer_t *peer) {
  if (peer == NULL || !peer->used) return;
  if (!peer->retired) flowmq_socket_peer_storage_release(peer);
  memset(peer, 0, sizeof(*peer));
}

static void flowmq_socket_peer_retire(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket;
  size_t peer_index;
  uint64_t generation;
  if (peer == NULL || !peer->used || peer->retired) return;
  socket = peer->owner;
  peer_index = flowmq_socket_peer_index(socket, peer);
  generation = peer->flow_control.local_generation;
  if (peer_index < FLOWMQ_SOCKET_PEER_CAPACITY &&
      socket->publish_peer_generations[peer_index] == generation) {
    socket->publish_peer_mask &= ~(UINT32_C(1) << peer_index);
    socket->publish_peer_generations[peer_index] = 0u;
  }
  if (socket->send_peer_active && socket->send_peer_index == peer_index &&
      socket->send_peer_generation == generation)
    flowmq_socket_cancel_send_route(socket);
  if (socket->reply_peer_valid && socket->reply_peer_index == peer_index &&
      socket->reply_peer_generation == generation) {
    socket->reply_peer_valid = 0u;
    socket->reply_peer_generation = 0u;
    flowmq_pattern_state_cancel_transaction(&socket->pattern);
  }
  if (socket->request_peer_valid && socket->request_peer_index == peer_index &&
      socket->request_peer_generation == generation &&
      peer->queued_parts == 0u) {
    socket->request_peer_valid = 0u;
    socket->request_peer_generation = 0u;
    flowmq_pattern_state_cancel_transaction(&socket->pattern);
  }
  peer->connected = 0u;
  peer->heartbeat_active = 0u;
  peer->close_pending = 0u;
  flowmq_socket_peer_storage_release(peer);
  peer->retired = 1u;
  if (peer->queued_parts == 0u) flowmq_socket_peer_release(peer);
}

static void flowmq_socket_fail(flowmq_socket_t *socket, int status) {
  if (socket->async_error == TURBO_OK) socket->async_error = status;
}

static void flowmq_socket_peer_fail(flowmq_socket_peer_t *peer) {
  int status;
  if (peer == NULL || !peer->used || peer->retired || peer->close_pending)
    return;
  peer->connected = 0u;
  peer->heartbeat_active = 0u;
  status = cnet_close(&peer->owner->client, peer->connection);
  if (status == TURBO_OK || status == TURBO_EALREADY) return;
  if (status == TURBO_EBUSY || status == TURBO_ENOBUFS) {
    peer->close_pending = 1u;
    return;
  }
  flowmq_socket_fail(peer->owner, status);
}

static int flowmq_socket_send_hello(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  flowmq_protocol_frame_t frame = {0};
  size_t encoded_size = 0u;
  vstr identity = {.data = socket->identity, .len = socket->identity_size};
  int status;
  frame.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
  frame.pattern = socket->pattern.pattern;
  frame.identity = identity;
  status = flowmq_protocol_encode_frame_into_internal(
      &frame, FLOWMQ_SOCKET_MAX_FRAME_SIZE, socket->send_scratch,
      socket->send_scratch_capacity, &encoded_size);
  if (status == TURBO_OK)
    status = cnet_send(&socket->client, peer->connection,
                       socket->send_scratch, encoded_size);
  if (status == TURBO_OK) {
    peer->write_busy = 1u;
    peer->writing_hello = 1u;
  }
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
  size_t encoded_size = 0u;
  int status;
  if (!peer->connected || !peer->hello_sent || peer->settings_sent ||
      peer->writing_settings || peer->write_busy) {
    return TURBO_EBUSY;
  }
  status = flowmq_flow_control_make_settings(
      &peer->flow_control, FLOWMQ_SOCKET_MAX_FRAME_SIZE, &settings);
  if (status == TURBO_OK)
    status = flowmq_protocol_settings_encode(&settings, payload);
  frame.kind = FLOWMQ_PROTOCOL_FRAME_SETTINGS;
  frame.pattern = socket->pattern.pattern;
  frame.payload = vstr_from_buf((const char *)payload, sizeof(payload));
  if (status == TURBO_OK)
    status = flowmq_protocol_encode_frame_into_internal(
        &frame, FLOWMQ_SOCKET_MAX_FRAME_SIZE, socket->send_scratch,
        socket->send_scratch_capacity, &encoded_size);
  if (status == TURBO_OK)
    status = cnet_send(&socket->client, peer->connection,
                       socket->send_scratch, encoded_size);
  if (status == TURBO_OK) {
    peer->write_busy = 1u;
    peer->writing_settings = 1u;
  }
  return status;
}

static int flowmq_socket_peer_can_admit(const flowmq_socket_peer_t *peer,
                                        size_t payload_size,
                                        int message_end) {
  const flowmq_socket_t *socket = peer->owner;
  if (!peer->used || !peer->connected || !peer->hello_sent ||
      !peer->hello_received || !peer->settings_sent ||
      !peer->settings_received || payload_size > socket->send_hwm_bytes ||
      peer->outbound_bytes > socket->send_hwm_bytes - payload_size ||
      (message_end && peer->outbound_messages >= socket->send_hwm) ||
      flowmq_flow_control_send_check(&peer->flow_control, payload_size) != TURBO_OK)
    return 0;
  return !peer->write_busy && peer->outbound_count == 0u
             ? 1
             : peer->outbound_count < FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
}

static int flowmq_socket_peer_ready(const flowmq_socket_peer_t *peer) {
  return peer->used && peer->connected && peer->hello_sent &&
         peer->hello_received && peer->settings_sent &&
         peer->settings_received;
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
                                            payload_size) != TURBO_OK)
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
  size_t encoded_size = 0u;
  int status;
  status = flowmq_protocol_encode_frame_into_internal(&frame, FLOWMQ_SOCKET_MAX_FRAME_SIZE,
                                                      socket->send_scratch,
                                                      socket->send_scratch_capacity, &encoded_size);
  if (status != TURBO_OK) return status;
  buffer = mem_get_buffer(&socket->message_pool, encoded_size);
  if (buffer == NULL) return TURBO_ENOMEM;
  memcpy(mem_buffer_data(buffer), socket->send_scratch, encoded_size);
  mem_set_used(buffer, encoded_size);
  *outbound = (flowmq_socket_outbound_t){.buffer = buffer,
                                         .encoded_size = encoded_size,
                                         .payload_size = size,
                                         .message_end = more == 0};
  return TURBO_OK;
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
  if (status != TURBO_OK) return status;
  for (size_t i = 0u; i < socket->send_staged_count; ++i) {
    flowmq_socket_outbound_t *outbound = &peer->outbound[peer->outbound_write];
    *outbound = socket->send_staged[i];
    outbound->buffer = mem_buffer_retain(outbound->buffer);
    peer->outbound_write = (peer->outbound_write + 1u) % FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
    ++peer->outbound_count;
  }
  peer->outbound_bytes += socket->send_staged_bytes;
  ++peer->outbound_messages;
  return TURBO_OK;
}

static int flowmq_socket_peer_admit(flowmq_socket_peer_t *peer,
                                    const void *encoded, size_t encoded_size,
                                    size_t payload_size, int message_end) {
  flowmq_socket_t *socket = peer->owner;
  flowmq_socket_outbound_t *outbound;
  mem_buffer_t *buffer;
  int status;
  if (!flowmq_socket_peer_can_admit(peer, payload_size, message_end))
    return TURBO_ENOBUFS;
  if (!peer->write_busy && peer->outbound_count == 0u) {
    status = cnet_send(&socket->client, peer->connection, encoded, encoded_size);
    if (status != TURBO_OK) return status;
    peer->write_busy = 1u;
    peer->writing_data = 1u;
    peer->inflight_payload_size = payload_size;
    peer->inflight_messages = message_end ? 1u : 0u;
  } else {
    outbound = &peer->outbound[peer->outbound_write];
    buffer = mem_get_buffer(&socket->message_pool, encoded_size);
    if (buffer == NULL) return TURBO_ENOMEM;
    memcpy(mem_buffer_data(buffer), encoded, encoded_size);
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
  if (status != TURBO_OK) {
    flowmq_socket_fail(socket, TURBO_EPROTO);
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flowmq_socket_peer_flush(flowmq_socket_peer_t *peer) {
  /* CNet permits one pending write per connection, so one admission batches
   * complete encoded frames. cnet_send copies the batch before returning. */
  flowmq_socket_t *socket = peer->owner;
  flowmq_socket_outbound_t *outbound;
  const void *send_data;
  size_t batch_count = 0u;
  size_t batch_encoded_size = 0u;
  size_t batch_payload_size = 0u;
  size_t batch_messages = 0u;
  int status;
  if (!peer->used || !peer->connected || peer->write_busy ||
      peer->outbound_count == 0u)
    return TURBO_OK;
  if (peer->outbound_count == 1u) {
    outbound = &peer->outbound[peer->outbound_read];
    send_data = mem_buffer_const_data(outbound->buffer);
    batch_count = 1u;
    batch_encoded_size = outbound->encoded_size;
    batch_payload_size = outbound->payload_size;
    batch_messages = outbound->message_end ? 1u : 0u;
  } else {
    while (batch_count < peer->outbound_count) {
      size_t index = (peer->outbound_read + batch_count) %
                     FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
      outbound = &peer->outbound[index];
      if (outbound->encoded_size >
          socket->send_scratch_capacity - batch_encoded_size)
        break;
      memcpy(socket->send_scratch + batch_encoded_size,
             mem_buffer_const_data(outbound->buffer), outbound->encoded_size);
      batch_encoded_size += outbound->encoded_size;
      batch_payload_size += outbound->payload_size;
      if (outbound->message_end) ++batch_messages;
      ++batch_count;
    }
    if (batch_count == 0u) return TURBO_EMSGSIZE;
    send_data = socket->send_scratch;
  }
  status = cnet_send(&socket->client, peer->connection, send_data,
                     batch_encoded_size);
  if (status != TURBO_OK) return status;
  peer->write_busy = 1u;
  peer->writing_data = 1u;
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
  return TURBO_OK;
}

static int flowmq_socket_stage_bytes(flowmq_socket_peer_t *peer,
                                     const void *data, size_t size,
                                     size_t credit_size, int more) {
  flowmq_socket_t *socket = peer->owner;
  flowmq_socket_message_t *message;
  mem_buffer_t *buffer;
  if (peer->staged_count == FLOWMQ_SOCKET_MULTIPART_CAPACITY)
    return TURBO_ENOBUFS;
  if (credit_size > size) return TURBO_EINVAL;
  if (size > FLOWMQ_SOCKET_HARD_HWM_BYTES - peer->staged_bytes)
    return TURBO_EMSGSIZE;
  buffer = mem_get_buffer(&socket->message_pool,
                          size == 0u ? 1u : size);
  if (buffer == NULL) return TURBO_ENOMEM;
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
  return TURBO_OK;
}

static int flowmq_socket_commit_staged(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  size_t message_credit = 0u;
  if (peer->staged_bytes > socket->receive_hwm_bytes)
    return TURBO_EMSGSIZE;
  if (peer->staged_count >
          FLOWMQ_SOCKET_INBOUND_CAPACITY - socket->inbound_count ||
      socket->inbound_messages >= socket->receive_hwm ||
      socket->inbound_bytes > socket->receive_hwm_bytes - peer->staged_bytes)
    return TURBO_ENOBUFS;
  if (peer->queued_parts > SIZE_MAX - peer->staged_count)
    return TURBO_ERANGE;
  for (size_t i = 0u; i < peer->staged_count; ++i) {
    if (peer->staged[i].credit_size > SIZE_MAX - message_credit)
      return TURBO_ERANGE;
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
  return TURBO_OK;
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
  if (status != TURBO_OK || !changed ||
      socket->pattern.pattern != FLOWMQ_PROTOCOL_XPUB)
    return status;
  {
    unsigned char event[FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE + 1u];
    event[0] = subscribe ? 1u : 0u;
    if (frame->topic.len != 0u)
      memcpy(event + 1u, frame->topic.data, frame->topic.len);
    status = flowmq_socket_stage_bytes(peer, event, frame->topic.len + 1u,
                                       0u, 0);
    return status == TURBO_OK ? flowmq_socket_commit_staged(peer) : status;
  }
}

static int flowmq_socket_peer_admit_control(
    flowmq_socket_peer_t *peer, flowmq_protocol_frame_kind_t kind,
    vstr payload) {
  flowmq_socket_t *socket = peer->owner;
  flowmq_protocol_frame_t frame = {0};
  size_t encoded_size = 0u;
  int status;
  if (!flowmq_socket_peer_ready(peer) || peer->write_busy)
    return TURBO_ENOBUFS;
  frame.kind = kind;
  frame.pattern = socket->pattern.pattern;
  frame.payload = payload;
  status = flowmq_protocol_encode_frame_into_internal(
      &frame, FLOWMQ_SOCKET_MAX_FRAME_SIZE, socket->send_scratch,
      socket->send_scratch_capacity, &encoded_size);
  if (status != TURBO_OK) return status;
  status = cnet_send(&socket->client, peer->connection, socket->send_scratch,
                     encoded_size);
  if (status == TURBO_OK) peer->write_busy = 1u;
  return status;
}

static int flowmq_socket_peer_flow_update_progress(
    flowmq_socket_peer_t *peer) {
  flowmq_protocol_flow_update_t update;
  unsigned char payload[FLOWMQ_PROTOCOL_FLOW_UPDATE_PAYLOAD_SIZE];
  int status;
  if (!flowmq_socket_peer_ready(peer) || peer->write_busy) return TURBO_OK;
  status = flowmq_flow_control_next_update(&peer->flow_control, turbo_hrtime(),
                                           &update);
  if (status == FLOWMQ_FLOW_CONTROL_NO_UPDATE) return TURBO_OK;
  if (status != TURBO_OK) return status;
  status = flowmq_protocol_flow_update_encode(&update, payload);
  if (status == TURBO_OK)
    status = flowmq_socket_peer_admit_control(
        peer, FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE,
        vstr_from_buf((const char *)payload, sizeof(payload)));
  if (status == TURBO_ENOBUFS || status == TURBO_EBUSY) return TURBO_OK;
  if (status != TURBO_OK) return status;
  return flowmq_flow_control_mark_update_sent(&peer->flow_control, &update);
}

static int flowmq_socket_peer_heartbeat_progress(flowmq_socket_peer_t *peer) {
  uint64_t wait_deadline_ns = 0u;
  uint64_t now_ns;
  flowmq_protocol_heartbeat_action_t action;
  int status;
  if (!flowmq_socket_peer_ready(peer) || peer->heartbeat_closing)
    return TURBO_OK;
  if (peer->pong_pending) {
    status = flowmq_socket_peer_admit_control(
        peer, FLOWMQ_PROTOCOL_FRAME_PONG, (vstr){0});
    if (status == TURBO_ENOBUFS || status == TURBO_EBUSY) return TURBO_OK;
    if (status != TURBO_OK) return status;
    peer->pong_pending = 0u;
  }
  if (!peer->heartbeat_active) return TURBO_OK;
  now_ns = turbo_hrtime();
  action = flowmq_protocol_heartbeat_deadlines_next(
      &peer->heartbeat, now_ns, &wait_deadline_ns);
  (void)wait_deadline_ns;
  if (action == FLOWMQ_PROTOCOL_HEARTBEAT_WAIT) return TURBO_OK;
  if (action == FLOWMQ_PROTOCOL_HEARTBEAT_SEND_PING) {
    /* Control frames take the next serialized CNet write slot so application
     * backlog cannot postpone dead-peer detection indefinitely. */
    if (peer->write_busy) return TURBO_OK;
    status = flowmq_socket_peer_admit_control(
        peer, FLOWMQ_PROTOCOL_FRAME_PING, (vstr){0});
    if (status == TURBO_ENOBUFS || status == TURBO_EBUSY) return TURBO_OK;
    if (status != TURBO_OK) return status;
    flowmq_protocol_heartbeat_deadlines_on_ping(&peer->heartbeat, now_ns);
    return TURBO_OK;
  }
  status = cnet_close(&peer->owner->client, peer->connection);
  if (status != TURBO_OK && status != TURBO_EALREADY) return status;
  peer->heartbeat_closing = 1u;
  peer->connected = 0u;
  return TURBO_OK;
}

static int flowmq_socket_process_receive(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  if (peer->commit_pending) return TURBO_ENOBUFS;
  for (;;) {
    flowmq_protocol_frame_t frame = {0};
    size_t consumed = 0u;
    int pause_receive = 0;
    int status = flowmq_stream_decoder_next(&peer->decoder, &frame, &consumed);
    if (status == FLOWMQ_PROTOCOL_INCOMPLETE) return TURBO_OK;
    if (status != TURBO_OK) return status;
    if (!peer->hello_received) {
      status =
          flowmq_pattern_socket_hello_validate(socket->pattern.pattern, &frame);
      if (status == TURBO_OK) {
        if (socket->pattern.pattern == FLOWMQ_PROTOCOL_ROUTER) {
          for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
            flowmq_socket_peer_t *candidate = &socket->peers[i];
            if (candidate == peer || !candidate->used || candidate->retired ||
                !candidate->hello_received ||
                candidate->identity_size != frame.identity.len)
              continue;
            if (frame.identity.len == 0u ||
                memcmp(candidate->identity, frame.identity.data,
                       frame.identity.len) == 0) {
              status = TURBO_EPROTO;
              break;
            }
          }
        }
        if (status == TURBO_OK &&
            frame.identity.len > FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE) {
          status = TURBO_EMSGSIZE;
        } else if (status == TURBO_OK) {
          if (frame.identity.len != 0u)
            memcpy(peer->identity, frame.identity.data, frame.identity.len);
          peer->identity[frame.identity.len] = '\0';
          peer->identity_size = frame.identity.len;
        }
        peer->remote_pattern = frame.pattern;
        if (status == TURBO_OK) {
          peer->hello_received = 1u;
          if (peer->heartbeat_active)
            flowmq_protocol_heartbeat_deadlines_on_receive(
                &peer->heartbeat, turbo_hrtime());
        }
      }
    } else if (!peer->settings_received) {
      flowmq_protocol_settings_t settings;
      status = frame.pattern == peer->remote_pattern &&
                       frame.kind == FLOWMQ_PROTOCOL_FRAME_SETTINGS
                   ? flowmq_pattern_data_direction_validate(
                         socket->pattern.pattern, &frame)
                   : TURBO_EPROTO;
      if (status == TURBO_OK)
        status = flowmq_protocol_settings_decode(frame.payload, &settings);
      if (status == TURBO_OK)
        status = flowmq_flow_control_apply_settings(&peer->flow_control,
                                                    &settings);
      if (status == TURBO_OK) {
        peer->settings_received = 1u;
        if (peer->heartbeat_active)
          flowmq_protocol_heartbeat_deadlines_on_receive(
              &peer->heartbeat, turbo_hrtime());
      }
    } else {
      status = frame.pattern == peer->remote_pattern
                   ? flowmq_pattern_data_direction_validate(
                         socket->pattern.pattern, &frame)
                   : TURBO_EPROTO;
      if (status == TURBO_OK && frame.kind == FLOWMQ_PROTOCOL_FRAME_SETTINGS)
        status = TURBO_EPROTO;
      if (status == TURBO_OK && peer->heartbeat_active)
        flowmq_protocol_heartbeat_deadlines_on_receive(&peer->heartbeat,
                                                       turbo_hrtime());
      if (status == TURBO_OK && frame.kind == FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE) {
        flowmq_protocol_flow_update_t update;
        status = flowmq_protocol_flow_update_decode(frame.payload, &update);
        if (status == TURBO_OK)
          status = flowmq_flow_control_apply_remote_update(
              &peer->flow_control, &update);
      } else if (status == TURBO_OK &&
                 frame.kind == FLOWMQ_PROTOCOL_FRAME_PING) {
        peer->pong_pending = 1u;
      } else if (status == TURBO_OK &&
                 (frame.kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE ||
                  frame.kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE)) {
        status = flowmq_socket_subscription_event(peer, &frame);
      } else if (status == TURBO_OK && frame.kind == FLOWMQ_PROTOCOL_FRAME_DATA) {
        if (socket->pattern.pattern == FLOWMQ_PROTOCOL_REQ &&
            (!socket->request_peer_valid ||
             socket->request_peer_index !=
                 flowmq_socket_peer_index(socket, peer) ||
             socket->request_peer_generation !=
                 peer->flow_control.local_generation))
          status = TURBO_EPROTO;
        if (status == TURBO_OK)
          status = flowmq_flow_control_receive_check(&peer->flow_control,
                                                     frame.payload.len);
        if (status == TURBO_OK &&
            socket->pattern.pattern == FLOWMQ_PROTOCOL_ROUTER &&
            !peer->receiving_multipart) {
          status = flowmq_socket_stage_bytes(
              peer, peer->identity, peer->identity_size, 0u, 1);
        }
        if (status == TURBO_OK) status = flowmq_socket_stage_frame(peer, &frame);
        if (status == TURBO_OK)
          status = flowmq_flow_control_receive_commit(&peer->flow_control,
                                                      frame.payload.len);
        if (status == TURBO_OK) peer->receiving_multipart = frame.more != 0;
        if (status == TURBO_OK && !frame.more) {
          status = flowmq_socket_commit_staged(peer);
          if (status == TURBO_ENOBUFS) {
            peer->commit_pending = 1u;
            pause_receive = 1;
            status = TURBO_OK;
          }
        }
      }
    }
    flowmq_protocol_frame_cleanup(&frame);
    if (status != TURBO_OK) return status;
    status = flowmq_stream_decoder_consume(&peer->decoder, consumed);
    if (status != TURBO_OK) return status;
    if (pause_receive) return TURBO_ENOBUFS;
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
    peer->connected = 1u;
    if (socket->pattern.pattern == FLOWMQ_PROTOCOL_PAIR) {
      for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
        flowmq_socket_peer_t *candidate = &socket->peers[i];
        if (candidate != peer && candidate->used && !candidate->retired &&
            candidate->connected) {
          flowmq_socket_peer_fail(peer);
          return;
        }
      }
    }
    flow_control_status = flowmq_socket_flow_control_init(peer);
    if (flow_control_status != TURBO_OK) {
      flowmq_socket_peer_fail(peer);
      return;
    }
    if (socket->heartbeat_interval_ms > 0) {
      int timeout_ms = socket->heartbeat_timeout_set
                           ? socket->heartbeat_timeout_ms
                           : socket->heartbeat_interval_ms;
      flowmq_protocol_heartbeat_deadlines_init(
          &peer->heartbeat, turbo_hrtime(),
          (uint64_t)socket->heartbeat_interval_ms, (uint64_t)timeout_ms, 0u);
      peer->heartbeat_active = 1u;
    }
    if (cnet_receive(&socket->client, connection, 1u) != TURBO_OK)
      flowmq_socket_peer_fail(peer);
    else {
      int status = flowmq_socket_send_hello(peer);
      if (status != TURBO_OK) flowmq_socket_peer_fail(peer);
    }
  } else if (state == CNET_CONNECTION_CLOSED ||
             state == CNET_CONNECTION_FAILED) {
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
  if (status == TURBO_OK) status = flowmq_socket_process_receive(peer);
  if (status == TURBO_ENOBUFS) return;
  if (status == TURBO_OK) status = cnet_receive(&socket->client, connection, 1u);
  if (status != TURBO_OK) flowmq_socket_peer_fail(peer);
}

static void flowmq_socket_on_send(void *user, cnet_connection connection,
                                  size_t size) {
  flowmq_socket_peer_t *peer = (flowmq_socket_peer_t *)user;
  (void)connection;
  (void)size;
  peer->write_busy = 0u;
  if (peer->writing_hello) {
    peer->writing_hello = 0u;
    peer->hello_sent = 1u;
  }
  if (peer->writing_settings) {
    peer->writing_settings = 0u;
    peer->settings_sent = 1u;
  }
  if (peer->writing_data) {
    peer->writing_data = 0u;
    if (peer->outbound_bytes >= peer->inflight_payload_size)
      peer->outbound_bytes -= peer->inflight_payload_size;
    else
      flowmq_socket_fail(peer->owner, TURBO_EPROTO);
    if (peer->outbound_messages >= peer->inflight_messages)
      peer->outbound_messages -= peer->inflight_messages;
    else
      flowmq_socket_fail(peer->owner, TURBO_EPROTO);
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

static int flowmq_socket_runtime_init(flowmq_socket_t *socket,
                                      flowmq_transport_t transport) {
  flowmq_io_config_t io;
  flowmq_timeout_config_t timeouts = {0};
  cnet_client_config config;
  int status;
  if (socket->runtime_initialized)
    return socket->transport == (int)transport ? TURBO_OK : TURBO_ENOTSUP;
  flowmq_io_config_init(&io);
  io.command_capacity = FLOWMQ_SOCKET_CNET_COMMAND_CAPACITY;
  /* A messaging connection remains valid while either direction is idle. */
  timeouts.set_flags = FLOWMQ_TIMEOUT_SET_RECV;
  flowmq_timeouts_resolve(&timeouts, FLOWMQ_SOCKET_DEFAULT_TIMEOUT_MS);
  status = flowmq_cnet_client_config(
      &io, &timeouts, transport, FLOWMQ_SOCKET_PEER_CAPACITY,
      socket->send_scratch_capacity, &config);
  if (status == TURBO_OK) status = cnet_client_init(&socket->client, &config);
  if (status != TURBO_OK) return status;
  socket->transport = (int)transport;
  socket->runtime_initialized = 1u;
  return TURBO_OK;
}

static void flowmq_socket_clear_secret(char *value, size_t size) {
  volatile char *bytes = (volatile char *)value;
  for (size_t i = 0u; i < size; ++i) bytes[i] = 0;
}

static int flowmq_socket_copy_option(char *destination, size_t capacity,
                                     const void *value, size_t size) {
  if (destination == NULL || value == NULL || size == 0u || size >= capacity ||
      memchr(value, '\0', size) != NULL)
    return TURBO_EINVAL;
  memcpy(destination, value, size);
  destination[size] = '\0';
  return TURBO_OK;
}

static int flowmq_socket_tls_server_init(flowmq_socket_t *socket) {
  cnet_tls_server_config config;
  int status;
  if (socket->tls_server_initialized) return TURBO_OK;
  if (socket->tls_cert_file[0] == '\0' || socket->tls_key_file[0] == '\0')
    return TURBO_EINVAL;
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
  if (status == TURBO_OK) socket->tls_server_initialized = 1u;
  return status;
}

static int flowmq_socket_tls_client_init(flowmq_socket_t *socket) {
  cnet_tls_client_config config;
  int status;
  if (socket->tls_client_initialized) return TURBO_OK;
  if ((socket->tls_cert_file[0] == '\0') !=
      (socket->tls_key_file[0] == '\0'))
    return TURBO_EINVAL;
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
  if (status == TURBO_OK) socket->tls_client_initialized = 1u;
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
  if (socket->pattern.pattern != FLOWMQ_PROTOCOL_SUB &&
      socket->pattern.pattern != FLOWMQ_PROTOCOL_XSUB)
    return TURBO_OK;
  if (!flowmq_socket_peer_ready(peer) || peer->write_busy)
    return TURBO_OK;

  for (size_t i = 0u;
       i < flowmq_subscription_set_count(&socket->subscriptions); ++i) {
    subscription = flowmq_subscription_set_at(&socket->subscriptions, i);
    if (subscription == NULL) return TURBO_EPROTO;
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
      if (subscription == NULL) return TURBO_EPROTO;
      topic = (vstr){.data = subscription->topic,
                     .len = tstr_len(subscription->topic)};
      if (!flowmq_socket_subscription_contains(&socket->subscriptions, topic))
        break;
      topic = (vstr){0};
    }
  }
  if (topic.data == NULL) return TURBO_OK;

  status = flowmq_pattern_encode_subscription(socket->pattern.pattern, kind,
                                               topic,
                                               FLOWMQ_SOCKET_MAX_FRAME_SIZE,
                                               &encoded);
  if (status == TURBO_OK && kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE)
    status = flowmq_subscription_set_update(&peer->synced_subscriptions, 1,
                                             topic, &changed);
  if (status == TURBO_OK)
    status = cnet_send(&socket->client, peer->connection, encoded,
                       tstr_len(encoded));
  if (status != TURBO_OK && kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE)
    (void)flowmq_subscription_set_update(&peer->synced_subscriptions, 0, topic,
                                         &changed);
  if (status == TURBO_OK && kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE)
    status = flowmq_subscription_set_update(&peer->synced_subscriptions, 0,
                                             topic, &changed);
  if (encoded != NULL) tstr_free(encoded);
  if (status == TURBO_OK) peer->write_busy = 1u;
  return status;
}

static int flowmq_socket_pattern(int type, flowmq_protocol_pattern_t *pattern) {
  if (pattern == NULL) return TURBO_EINVAL;
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
  default: return TURBO_EINVAL;
  }
  return TURBO_OK;
}

flowmq_ctx_t *flowmq_ctx_new(void) {
  return (flowmq_ctx_t *)calloc(1u, sizeof(flowmq_ctx_t));
}

int flowmq_ctx_term(flowmq_ctx_t *ctx) {
  if (ctx == NULL) return TURBO_EINVAL;
  if (ctx->socket_count != 0u) return TURBO_EBUSY;
  free(ctx);
  return TURBO_OK;
}

flowmq_socket_t *flowmq_socket(flowmq_ctx_t *ctx, int type) {
  flowmq_protocol_pattern_t pattern;
  flowmq_socket_t *socket;
  size_t encoded_limit = 0u;
  if (ctx == NULL || flowmq_socket_pattern(type, &pattern) != TURBO_OK) return NULL;
  socket = (flowmq_socket_t *)calloc(1u, sizeof(*socket));
  if (socket == NULL) return NULL;
  socket->send_hwm = FLOWMQ_SOCKET_DEFAULT_HWM;
  socket->receive_hwm = FLOWMQ_SOCKET_DEFAULT_HWM;
  socket->send_hwm_bytes = FLOWMQ_SOCKET_DEFAULT_HWM_BYTES;
  socket->receive_hwm_bytes = FLOWMQ_SOCKET_DEFAULT_HWM_BYTES;
  socket->flow_update_interval_ms =
      FLOWMQ_FLOW_CONTROL_DEFAULT_UPDATE_INTERVAL_MS;
  socket->peers = (flowmq_socket_peer_t *)calloc(
      FLOWMQ_SOCKET_PEER_CAPACITY, sizeof(*socket->peers));
  socket->inbound = (flowmq_socket_message_t *)calloc(
      FLOWMQ_SOCKET_INBOUND_CAPACITY, sizeof(*socket->inbound));
  if (flowmq_protocol_encoded_size_limit(FLOWMQ_SOCKET_MAX_FRAME_SIZE,
                                         &encoded_limit) != TURBO_OK) {
    free(socket->inbound);
    free(socket->peers);
    free(socket);
    return NULL;
  }
  socket->send_scratch = (unsigned char *)malloc(encoded_limit);
  socket->send_scratch_capacity = encoded_limit;
  if (socket->peers == NULL || socket->inbound == NULL ||
      socket->send_scratch == NULL) {
    free(socket->send_scratch);
    free(socket->inbound);
    free(socket->peers);
    free(socket);
    return NULL;
  }
  if (mem_init(&socket->message_pool, 0u) != 0) {
    free(socket->send_scratch);
    free(socket->inbound);
    free(socket->peers);
    free(socket);
    return NULL;
  }
  socket->pool_initialized = 1u;
  if (flowmq_subscription_set_init(&socket->subscriptions) != TURBO_OK) {
    mem_destroy(&socket->message_pool);
    free(socket->send_scratch);
    free(socket->inbound);
    free(socket->peers);
    free(socket);
    return NULL;
  }
  if (flowmq_pattern_state_init(&socket->pattern, pattern) != TURBO_OK) {
    mem_destroy(&socket->message_pool);
    flowmq_subscription_set_destroy(&socket->subscriptions);
    free(socket->send_scratch);
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
      free(socket->send_scratch);
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
    return TURBO_EINVAL;
  if (socket->listener_initialized) {
    status = cnet_listener_close(&socket->listener);
    if (status != TURBO_OK && status != TURBO_EALREADY) return status;
    status = cnet_listener_destroy(&socket->listener);
    if (status != TURBO_OK) return status;
    socket->listener_initialized = 0u;
  }
  if (socket->runtime_initialized) {
    status = cnet_client_stop(&socket->client,
                              FLOWMQ_SOCKET_SHUTDOWN_TIMEOUT_MS);
    if (status != TURBO_OK) return status;
    status = cnet_client_destroy(&socket->client);
    if (status != TURBO_OK) return status;
    socket->runtime_initialized = 0u;
  }
  if (socket->tls_client_initialized) {
    status = cnet_tls_client_destroy(&socket->tls_client);
    if (status != TURBO_OK) return status;
    socket->tls_client_initialized = 0u;
  }
  if (socket->tls_server_initialized) {
    status = cnet_tls_server_destroy(&socket->tls_server);
    if (status != TURBO_OK) return status;
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
  if (socket->pool_initialized) mem_destroy(&socket->message_pool);
  flowmq_socket_clear_secret(socket->tls_key_password,
                             sizeof(socket->tls_key_password));
  free(socket->inbound);
  free(socket->peers);
  free(socket->send_scratch);
  --socket->ctx->socket_count;
  socket->ctx = NULL;
  free(socket);
  return TURBO_OK;
}

int flowmq_bind(flowmq_socket_t *socket, const char *endpoint) {
  flowmq_endpoint_parts_t parts;
  cnet_listener_config config;
  uint16_t bound_port = 0u;
  int bracket;
  int written;
  int status;
  if (socket == NULL || socket->ctx == NULL) return TURBO_EINVAL;
  status = flowmq_endpoint_parse(endpoint, 1, &parts);
  if (status != TURBO_OK) return status;
  if (socket->listener_initialized) return TURBO_EALREADY;
  if (parts.transport == FLOWMQ_TRANSPORT_TLS) {
    status = flowmq_socket_tls_server_init(socket);
    if (status != TURBO_OK) return status;
  }
  status = flowmq_socket_runtime_init(socket, parts.transport);
  if (status != TURBO_OK) return status;
  config = (cnet_listener_config){.backend = flowmq_cnet_backend(),
                                  .host = parts.host,
                                  .port = parts.port,
                                  .backlog = FLOWMQ_SOCKET_PEER_CAPACITY};
  status = cnet_listener_init(&socket->listener, &config);
  if (status != TURBO_OK) return status;
  socket->listener_initialized = 1u;
  status = cnet_listener_port(&socket->listener, &bound_port);
  if (status != TURBO_OK) return status;
  bracket = strchr(parts.host, ':') != NULL;
  written = snprintf(socket->last_endpoint, sizeof(socket->last_endpoint),
                     bracket ? "%s://[%s]:%u" : "%s://%s:%u",
                     parts.transport == FLOWMQ_TRANSPORT_TLS ? "tls" : "tcp",
                     parts.host, (unsigned)bound_port);
  if (written < 0 || (size_t)written >= sizeof(socket->last_endpoint))
    return TURBO_EMSGSIZE;
  return TURBO_OK;
}

int flowmq_connect(flowmq_socket_t *socket, const char *endpoint) {
  flowmq_endpoint_parts_t parts;
  flowmq_socket_peer_t *peer;
  cnet_connect_options options;
  cnet_observer observer;
  size_t endpoint_size;
  int status;
  if (socket == NULL || socket->ctx == NULL) return TURBO_EINVAL;
  status = flowmq_endpoint_parse(endpoint, 0, &parts);
  if (status != TURBO_OK) return status;
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_PAIR) {
    for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
      if (socket->peers[i].used) return TURBO_EBUSY;
    }
  }
  status = flowmq_socket_runtime_init(socket, parts.transport);
  if (status != TURBO_OK) return status;
  if (parts.transport == FLOWMQ_TRANSPORT_TLS) {
    status = flowmq_socket_tls_client_init(socket);
    if (status != TURBO_OK) return status;
  }
  peer = flowmq_socket_peer_acquire(socket);
  if (peer == NULL) return TURBO_ENOBUFS;
  observer = flowmq_socket_observer(peer);
  options = (cnet_connect_options){
      .uri = endpoint,
      .observer = observer,
      .tls = NULL,
      .tls_client = parts.transport == FLOWMQ_TRANSPORT_TLS
                        ? &socket->tls_client
                        : NULL};
  status = cnet_connect(&socket->client, &options, &peer->connection);
  if (status != TURBO_OK) {
    flowmq_socket_peer_release(peer);
    return status;
  }
  endpoint_size = strlen(endpoint) + 1u;
  if (endpoint_size > sizeof(socket->last_endpoint)) return TURBO_EMSGSIZE;
  memcpy(socket->last_endpoint, endpoint, endpoint_size);
  return TURBO_OK;
}

int flowmq_last_endpoint(const flowmq_socket_t *socket, char *buffer,
                         size_t capacity, size_t *size) {
  size_t required;
  if (size != NULL) *size = 0u;
  if (socket == NULL || buffer == NULL || size == NULL ||
      socket->last_endpoint[0] == '\0')
    return TURBO_EINVAL;
  required = strlen(socket->last_endpoint) + 1u;
  *size = required;
  if (capacity < required) return TURBO_EMSGSIZE;
  memcpy(buffer, socket->last_endpoint, required);
  return TURBO_OK;
}

int flowmq_setsockopt(flowmq_socket_t *socket, int option, const void *value,
                      size_t size) {
  if (socket == NULL || socket->ctx == NULL) return TURBO_EINVAL;
  if (option == FLOWMQ_SUBSCRIBE || option == FLOWMQ_UNSUBSCRIBE) {
    int changed = 0;
    if ((socket->pattern.pattern != FLOWMQ_PROTOCOL_SUB &&
         socket->pattern.pattern != FLOWMQ_PROTOCOL_XSUB) ||
        (value == NULL && size != 0u) || size > FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE)
      return TURBO_EINVAL;
    return flowmq_subscription_set_update(
        &socket->subscriptions, option == FLOWMQ_SUBSCRIBE,
        (vstr){.data = (char *)value, .len = size}, &changed);
  }
  if (socket->runtime_initialized) return TURBO_EBUSY;
  switch (option) {
  case FLOWMQ_IDENTITY:
    if (value == NULL || size == 0u ||
        size > FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE || memchr(value, '\0', size) != NULL)
      return TURBO_EINVAL;
    memcpy(socket->identity, value, size);
    socket->identity[size] = '\0';
    socket->identity_size = size;
    return TURBO_OK;
  case FLOWMQ_SNDHWM:
  case FLOWMQ_RCVHWM: {
    int value_int;
    if (value == NULL || size != sizeof(value_int)) return TURBO_EINVAL;
    value_int = *(const int *)value;
    if (value_int <= 0 || value_int > FLOWMQ_SOCKET_OUTBOUND_CAPACITY)
      return TURBO_EINVAL;
    if (option == FLOWMQ_SNDHWM)
      socket->send_hwm = (size_t)value_int;
    else
      socket->receive_hwm = (size_t)value_int;
    return TURBO_OK;
  }
  case FLOWMQ_HEARTBEAT_IVL:
  case FLOWMQ_HEARTBEAT_TIMEOUT: {
    int value_int;
    if (value == NULL || size != sizeof(value_int)) return TURBO_EINVAL;
    value_int = *(const int *)value;
    if (value_int < 0) return TURBO_EINVAL;
    if (option == FLOWMQ_HEARTBEAT_IVL)
      socket->heartbeat_interval_ms = value_int;
    else {
      socket->heartbeat_timeout_ms = value_int;
      socket->heartbeat_timeout_set = 1u;
    }
    return TURBO_OK;
  }
  case FLOWMQ_FLOW_UPDATE_IVL: {
    int value_int;
    if (value == NULL || size != sizeof(value_int)) return TURBO_EINVAL;
    value_int = *(const int *)value;
    if (value_int <= 0) return TURBO_EINVAL;
    socket->flow_update_interval_ms = value_int;
    return TURBO_OK;
  }
  case FLOWMQ_FLOW_UPDATE_QUANTUM: {
    size_t value_size;
    if (value == NULL || size != sizeof(value_size)) return TURBO_EINVAL;
    value_size = *(const size_t *)value;
    if (value_size == 0u || value_size > socket->receive_hwm_bytes ||
        value_size > UINT32_MAX)
      return TURBO_EINVAL;
    socket->flow_update_quantum = (uint32_t)value_size;
    return TURBO_OK;
  }
  case FLOWMQ_SNDHWM_BYTES:
  case FLOWMQ_RCVHWM_BYTES: {
    size_t value_size;
    if (value == NULL || size != sizeof(value_size)) return TURBO_EINVAL;
    value_size = *(const size_t *)value;
    if (value_size == 0u || value_size > FLOWMQ_SOCKET_HARD_HWM_BYTES)
      return TURBO_EINVAL;
    if (option == FLOWMQ_SNDHWM_BYTES)
      socket->send_hwm_bytes = value_size;
    else {
      if (socket->flow_update_quantum != 0u &&
          socket->flow_update_quantum > value_size)
        return TURBO_EINVAL;
      socket->receive_hwm_bytes = value_size;
    }
    return TURBO_OK;
  }
  case FLOWMQ_TLS_CA_FILE:
    return flowmq_socket_copy_option(socket->tls_ca_file,
                                     sizeof(socket->tls_ca_file), value, size);
  case FLOWMQ_TLS_CERT_FILE:
    return flowmq_socket_copy_option(socket->tls_cert_file,
                                     sizeof(socket->tls_cert_file), value, size);
  case FLOWMQ_TLS_KEY_FILE:
    return flowmq_socket_copy_option(socket->tls_key_file,
                                     sizeof(socket->tls_key_file), value, size);
  case FLOWMQ_TLS_KEY_PASSWORD:
    return flowmq_socket_copy_option(socket->tls_key_password,
                                     sizeof(socket->tls_key_password), value,
                                     size);
  case FLOWMQ_TLS_SERVER_NAME:
    return flowmq_socket_copy_option(socket->tls_server_name,
                                     sizeof(socket->tls_server_name), value,
                                     size);
  case FLOWMQ_TLS_REQUIRE_CLIENT_CERTIFICATE:
    if (value == NULL || size != sizeof(int)) return TURBO_EINVAL;
    socket->tls_require_client_certificate = *(const int *)value != 0;
    return TURBO_OK;
  default: return TURBO_ENOTSUP;
  }
}

int flowmq_getsockopt(const flowmq_socket_t *socket, int option, void *value,
                      size_t *size) {
  size_t required;
  if (socket == NULL || socket->ctx == NULL || size == NULL)
    return TURBO_EINVAL;
  switch (option) {
  case FLOWMQ_RCVMORE:
    required = sizeof(int);
    if (value == NULL || *size < required) {
      *size = required;
      return value == NULL ? TURBO_EINVAL : TURBO_EMSGSIZE;
    }
    *(int *)value = socket->last_rcvmore;
    *size = required;
    return TURBO_OK;
  default: return TURBO_ENOTSUP;
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
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_ROUTER) --max_parts;
  if (part_count > max_parts || socket->send_staged_bytes > socket->send_hwm_bytes ||
      size > socket->send_hwm_bytes - socket->send_staged_bytes)
    return TURBO_EMSGSIZE;
  message_size = socket->send_staged_bytes + size;

  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_PUB ||
      socket->pattern.pattern == FLOWMQ_PROTOCOL_XPUB) {
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
    if (socket->pattern.pattern == FLOWMQ_PROTOCOL_REP) {
      if (!socket->reply_peer_valid ||
          socket->reply_peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY)
        return TURBO_EBUSY;
      selected_peer_index = socket->reply_peer_index;
      peer = &socket->peers[selected_peer_index];
      if (!flowmq_socket_peer_generation_ready(
              peer, socket->reply_peer_generation))
        return TURBO_EBUSY;
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
      if (peer == NULL) return TURBO_EBUSY;
    }
  } else {
    if (!socket->send_peer_active || socket->send_peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY)
      return TURBO_EPROTO;
    selected_peer_index = socket->send_peer_index;
    peer = &socket->peers[selected_peer_index];
    if (!flowmq_socket_peer_generation_ready(peer, selected_peer_generation)) {
      flowmq_socket_cancel_send_route(socket);
      return TURBO_ENOTCONN;
    }
  }

  if (message_end && peer != NULL &&
      !flowmq_socket_peer_can_admit_message(peer, message_size, part_count,
                                            size))
    return TURBO_ENOBUFS;
  status = flowmq_socket_prepare_outbound(socket, data, size, !message_end, &part);
  if (status != TURBO_OK) return status;
  socket->send_staged[socket->send_staged_count] = part;
  ++socket->send_staged_count;
  socket->send_staged_bytes = message_size;

  if (!message_end) {
    if (socket->pattern.pattern == FLOWMQ_PROTOCOL_PUB ||
        socket->pattern.pattern == FLOWMQ_PROTOCOL_XPUB) {
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
    return TURBO_OK;
  }

  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_PUB ||
      socket->pattern.pattern == FLOWMQ_PROTOCOL_XPUB) {
    for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
      if ((peer_mask & (UINT32_C(1) << i)) != 0u) {
        status = flowmq_socket_peer_admit_staged(&socket->peers[i], socket);
        if (status != TURBO_OK) return status;
      }
    }
    socket->publish_peer_mask = 0u;
    memset(socket->publish_peer_generations, 0,
           sizeof(socket->publish_peer_generations));
  } else {
    status = flowmq_socket_peer_admit_staged(peer, socket);
    if (status != TURBO_OK) return status;
    if (socket->pattern.pattern == FLOWMQ_PROTOCOL_REQ) {
      socket->request_peer_index = selected_peer_index;
      socket->request_peer_generation =
          peer->flow_control.local_generation;
      socket->request_peer_valid = 1u;
    }
    if (socket->pattern.pattern == FLOWMQ_PROTOCOL_REP) {
      socket->reply_peer_valid = 0u;
      socket->reply_peer_generation = 0u;
    }
  }
  flowmq_socket_release_send_staged(socket);
  socket->send_peer_active = 0u;
  socket->send_peer_generation = 0u;
  ++socket->next_message_id;
  flowmq_pattern_state_send_commit(&socket->pattern, 0);
  return TURBO_OK;
}

static int flowmq_socket_try_send(flowmq_socket_t *socket, const void *data,
                                  size_t size, int flags) {
  flowmq_protocol_frame_t frame;
  flowmq_socket_peer_t *peer = NULL;
  size_t encoded_size = 0u;
  int starting_message;
  int message_end;
  int peer_saturated = 0;
  int status;
  if (socket == NULL || (data == NULL && size != 0u) ||
      (flags & ~(FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE)) != 0)
    return TURBO_EINVAL;
  status = flowmq_pattern_state_send_validate(&socket->pattern);
  if (status != TURBO_OK) return status;
  if (size > FLOWMQ_SOCKET_MAX_FRAME_SIZE) return TURBO_EMSGSIZE;
  if (size > socket->send_hwm_bytes) return TURBO_EMSGSIZE;
  starting_message = !socket->pattern.sending_multipart;
  message_end = (flags & FLOWMQ_SNDMORE) == 0;
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_ROUTER &&
      !socket->pattern.sending_multipart) {
    if ((flags & FLOWMQ_SNDMORE) == 0 || size == 0u) return TURBO_EINVAL;
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
        return TURBO_OK;
      }
    }
    return TURBO_ENOENT;
  }
  if (socket->pattern.sending_multipart ||
      (flags & FLOWMQ_SNDMORE) != 0)
    return flowmq_socket_try_send_multipart(socket, data, size, flags,
                                            starting_message);
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_PUB ||
      socket->pattern.pattern == FLOWMQ_PROTOCOL_XPUB) {
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
          return TURBO_ENOBUFS;
      }
    }
    if (peer_mask != 0u) {
      frame = (flowmq_protocol_frame_t){
          .kind = FLOWMQ_PROTOCOL_FRAME_DATA,
          .pattern = socket->pattern.pattern,
          .message_id = socket->next_message_id + 1u,
          .more = (flags & FLOWMQ_SNDMORE) != 0,
          .payload = {.data = (char *)data, .len = size}};
      status = flowmq_protocol_encode_frame_into_internal(
          &frame, FLOWMQ_SOCKET_MAX_FRAME_SIZE, socket->send_scratch,
          socket->send_scratch_capacity, &encoded_size);
      if (status != TURBO_OK) return status;
      for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
        flowmq_socket_peer_t *candidate = &socket->peers[i];
        if ((peer_mask & (UINT32_C(1) << i)) == 0u) continue;
        status = flowmq_socket_peer_admit(candidate, socket->send_scratch,
                                          encoded_size, size, message_end);
        if (status != TURBO_OK) break;
      }
      if (status != TURBO_OK) return status;
    }
    ++socket->next_message_id;
    socket->publish_peer_mask =
        (flags & FLOWMQ_SNDMORE) != 0 ? peer_mask : 0u;
    flowmq_pattern_state_send_commit(&socket->pattern,
                                     (flags & FLOWMQ_SNDMORE) != 0);
    return TURBO_OK;
  }
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_REP &&
      !socket->send_peer_active) {
    if (!socket->reply_peer_valid ||
        socket->reply_peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY)
      return TURBO_EBUSY;
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
    return peer_saturated ? TURBO_ENOBUFS : TURBO_EBUSY;
  }
  frame = (flowmq_protocol_frame_t){
      .kind = FLOWMQ_PROTOCOL_FRAME_DATA,
      .pattern = socket->pattern.pattern,
      .message_id = socket->next_message_id + 1u,
      .more = (flags & FLOWMQ_SNDMORE) != 0,
      .payload = {.data = (char *)data, .len = size}};
  status = flowmq_protocol_encode_frame_into_internal(
      &frame, FLOWMQ_SOCKET_MAX_FRAME_SIZE, socket->send_scratch,
      socket->send_scratch_capacity, &encoded_size);
  if (status == TURBO_OK)
    status = flowmq_socket_peer_admit(peer, socket->send_scratch, encoded_size,
                                      size, message_end);
  if (status != TURBO_OK) return status;
  ++socket->next_message_id;
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_REQ && starting_message) {
    socket->request_peer_index = socket->send_peer_index;
    socket->request_peer_generation = peer->flow_control.local_generation;
    socket->request_peer_valid = 1u;
  }
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_REP &&
      (flags & FLOWMQ_SNDMORE) == 0) {
    socket->reply_peer_valid = 0u;
    socket->reply_peer_generation = 0u;
  }
  socket->send_peer_active = (flags & FLOWMQ_SNDMORE) != 0;
  if (!socket->send_peer_active) socket->send_peer_generation = 0u;
  flowmq_pattern_state_send_commit(&socket->pattern,
                                   (flags & FLOWMQ_SNDMORE) != 0);
  return TURBO_OK;
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
    return TURBO_EINVAL;
  status = flowmq_pattern_state_receive_validate(&socket->pattern);
  if (status != TURBO_OK) return status;
  if (socket->inbound_count == 0u) return TURBO_EBUSY;
  message = &socket->inbound[socket->inbound_read];
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_REQ &&
      socket->request_peer_valid &&
      (message->peer_index != socket->request_peer_index ||
       message->peer_generation != socket->request_peer_generation))
    return TURBO_EPROTO;
  *received = message->size;
  if (capacity < message->size) return TURBO_EMSGSIZE;
  if (socket->inbound_bytes < message->size ||
      (!message->more && socket->inbound_messages == 0u))
    return TURBO_EPROTO;
  if (message->peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY) return TURBO_EPROTO;
  peer = &socket->peers[message->peer_index];
  if (!peer->used || message->peer_generation == 0u ||
      message->peer_generation != peer->flow_control.local_generation ||
      peer->queued_parts == 0u)
    return TURBO_EPROTO;
  if (message->credit_size != 0u && !peer->retired) {
    uint64_t consumed_at_ns = peer->flow_control.update_pending
                                  ? 0u
                                  : turbo_hrtime();
    status = flowmq_flow_control_consume(
        &peer->flow_control, message->credit_size, consumed_at_ns);
    if (status != TURBO_OK) return status;
  }
  if (message->size != 0u)
    memcpy(data, mem_buffer_const_data(message->buffer), message->size);
  message_more = message->more;
  socket->last_rcvmore = message_more != 0;
  flowmq_pattern_state_receive_commit(&socket->pattern, message_more);
  if (!message_more && socket->pattern.pattern == FLOWMQ_PROTOCOL_REP) {
    if (peer->retired) {
      socket->reply_peer_valid = 0u;
      socket->reply_peer_generation = 0u;
      flowmq_pattern_state_cancel_transaction(&socket->pattern);
    } else {
      socket->reply_peer_index = message->peer_index;
      socket->reply_peer_generation = message->peer_generation;
      socket->reply_peer_valid = 1u;
    }
  }
  if (!message_more && socket->pattern.pattern == FLOWMQ_PROTOCOL_REQ) {
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
  if (peer->retired && peer->queued_parts == 0u)
    flowmq_socket_peer_release(peer);
  return TURBO_OK;
}

static int flowmq_socket_resume_receive(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  int status;
  if (!peer->commit_pending) return TURBO_OK;
  status = flowmq_socket_commit_staged(peer);
  if (status == TURBO_ENOBUFS) return TURBO_OK;
  if (status != TURBO_OK) return status;
  peer->commit_pending = 0u;
  status = flowmq_socket_process_receive(peer);
  if (status == TURBO_ENOBUFS) return TURBO_OK;
  if (status != TURBO_OK) return status;
  return cnet_receive(&socket->client, peer->connection, 1u);
}

static int flowmq_socket_drive(flowmq_socket_t *socket, uint32_t timeout_ms,
                               size_t *events) {
  size_t client_events = 0u;
  int status;
  if (events != NULL) *events = 0u;
  if (socket == NULL || !socket->runtime_initialized) return TURBO_OK;
  if (socket->listener_initialized) {
    int ready = 0;
    status = cnet_listener_wait(&socket->listener, 0u, &ready);
    if (status != TURBO_OK) return status;
    if (ready) {
      flowmq_socket_peer_t *peer = flowmq_socket_peer_acquire(socket);
      cnet_observer observer;
      if (peer == NULL) return TURBO_ENOBUFS;
      observer = flowmq_socket_observer(peer);
      status = socket->transport == FLOWMQ_TRANSPORT_TLS
                   ? cnet_listener_accept_tls(&socket->listener, &socket->client,
                                              &socket->tls_server, &observer,
                                              &peer->connection)
                   : cnet_listener_accept(&socket->listener, &socket->client,
                                          &observer, &peer->connection);
      if (status != TURBO_OK) {
        flowmq_socket_peer_release(peer);
        if (status != TURBO_ETIMEDOUT) return status;
      }
    }
  }
  status = cnet_client_poll(&socket->client, timeout_ms, &client_events);
  if (status != TURBO_OK) return status;
  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
    flowmq_socket_peer_t *peer = &socket->peers[i];
    if (!peer->used) continue;
    if (peer->close_pending) {
      status = cnet_close(&socket->client, peer->connection);
      if (status == TURBO_OK || status == TURBO_EALREADY) {
        peer->close_pending = 0u;
      } else if (status == TURBO_EBUSY || status == TURBO_ENOBUFS) {
        continue;
      } else {
        flowmq_socket_fail(socket, status);
        return status;
      }
    }
    if (peer->retired || !peer->connected) continue;
    if (peer->commit_pending) {
      status = flowmq_socket_resume_receive(peer);
      if (status != TURBO_OK && status != TURBO_EBUSY &&
          status != TURBO_ENOBUFS) {
        flowmq_socket_peer_fail(peer);
        continue;
      }
    }
    if (peer->connected && peer->hello_sent && !peer->settings_sent &&
        !peer->writing_settings) {
      status = flowmq_socket_send_settings(peer);
      if (status != TURBO_OK && status != TURBO_EBUSY &&
          status != TURBO_ENOBUFS) {
        flowmq_socket_peer_fail(peer);
        continue;
      }
    }
    status = flowmq_socket_peer_heartbeat_progress(peer);
    if (status != TURBO_OK) {
      flowmq_socket_peer_fail(peer);
      continue;
    }
    status = flowmq_socket_peer_flow_update_progress(peer);
    if (status != TURBO_OK) {
      flowmq_socket_peer_fail(peer);
      continue;
    }
    if (socket->pattern.pattern == FLOWMQ_PROTOCOL_SUB ||
        socket->pattern.pattern == FLOWMQ_PROTOCOL_XSUB) {
      status = flowmq_socket_sync_subscription(peer);
      if (status != TURBO_OK && status != TURBO_EBUSY &&
          status != TURBO_ENOBUFS) {
        flowmq_socket_peer_fail(peer);
        continue;
      }
    }
    if (!peer->write_busy && peer->outbound_count != 0u) {
      status = flowmq_socket_peer_flush(peer);
      if (status != TURBO_OK && status != TURBO_EBUSY &&
          status != TURBO_ENOBUFS) {
        flowmq_socket_peer_fail(peer);
        continue;
      }
    }
  }
  if (events != NULL) *events = client_events;
  if (socket->async_error != TURBO_OK) return socket->async_error;
  return TURBO_OK;
}

int flowmq_send(flowmq_socket_t *socket, const void *data, size_t size,
                int flags) {
  int status;
  for (;;) {
    status = flowmq_socket_try_send(socket, data, size, flags);
    if (status != TURBO_EBUSY && status != TURBO_ENOBUFS) return status;
    if ((flags & FLOWMQ_DONTWAIT) != 0 || !socket->runtime_initialized)
      return status;
    status = flowmq_socket_drive(socket, FLOWMQ_SOCKET_BLOCKING_SLICE_MS, NULL);
    if (status != TURBO_OK) return status;
  }
}

int flowmq_recv(flowmq_socket_t *socket, void *data, size_t capacity,
                size_t *received, int flags) {
  int status;
  for (;;) {
    status = flowmq_socket_try_recv(socket, data, capacity, received, flags);
    if (status != TURBO_EBUSY) return status;
    if ((flags & FLOWMQ_DONTWAIT) != 0 || !socket->runtime_initialized)
      return status;
    status = flowmq_socket_drive(socket, FLOWMQ_SOCKET_BLOCKING_SLICE_MS, NULL);
    if (status != TURBO_OK) return status;
  }
}

static int flowmq_socket_pollin_ready(const flowmq_socket_t *socket) {
  return socket->inbound_count != 0u &&
         flowmq_pattern_state_receive_validate(&socket->pattern) == TURBO_OK;
}

static int flowmq_socket_pollout_ready(const flowmq_socket_t *socket) {
  if (flowmq_pattern_state_send_validate(&socket->pattern) != TURBO_OK ||
      socket->send_staged_count >= FLOWMQ_SOCKET_MULTIPART_CAPACITY ||
      socket->send_staged_bytes >= socket->send_hwm_bytes)
    return 0;
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_PUB ||
      socket->pattern.pattern == FLOWMQ_PROTOCOL_XPUB)
    return 1;
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_ROUTER &&
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
  if (socket->pattern.pattern == FLOWMQ_PROTOCOL_REP) {
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
  const uint64_t started_ms = turbo_monotonic_ms();
  if (ready != NULL) *ready = 0u;
  if (items == NULL || item_count == 0u || ready == NULL) return TURBO_EINVAL;
  for (;;) {
    *ready = 0u;
    for (size_t i = 0u; i < item_count; ++i) {
      flowmq_socket_t *socket = items[i].socket;
      size_t events = 0u;
      int status;
      items[i].revents = 0;
      if (socket == NULL) return TURBO_EINVAL;
      status = flowmq_socket_drive(socket, 0u, &events);
      if (status != TURBO_OK) {
        if (socket->async_error == status) {
          items[i].revents |= FLOWMQ_POLLERR;
          ++*ready;
          continue;
        }
        return status;
      }
      if ((items[i].events & FLOWMQ_POLLIN) &&
          flowmq_socket_pollin_ready(socket))
        items[i].revents |= FLOWMQ_POLLIN;
      if ((items[i].events & FLOWMQ_POLLOUT) &&
          flowmq_socket_pollout_ready(socket))
        items[i].revents |= FLOWMQ_POLLOUT;
      if (items[i].revents != 0) ++*ready;
      (void)events;
    }
    if (*ready != 0u || timeout_ms == 0u) return TURBO_OK;
    {
      const uint64_t elapsed_ms = turbo_monotonic_ms() - started_ms;
      const uint64_t remaining_ms =
          elapsed_ms >= timeout_ms ? 0u : (uint64_t)timeout_ms - elapsed_ms;
      if (remaining_ms == 0u) return TURBO_OK;
      turbo_sleep_ms(remaining_ms > 1u ? 1u : (uint32_t)remaining_ms);
    }
  }
}
