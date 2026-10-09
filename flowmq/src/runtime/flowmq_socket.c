#include "flowmq_socket.h"
#include "flowmq_peer_pool.h"

#include "flowmq_cnet_transport.h"
#include "flowmq_flow_control.h"
#include "flowmq_owned_stream.h"
#include "flowmq_pattern.h"
#include "flowmq_pattern_state.h"
#include "flowmq_peer_state.h"
#include "flowmq_protocol_internal.h"
#include "flowmq_reconnect.h"
#include "flowmq_reconnect_ready.h"
#include "flowmq_stream_decoder.h"
#include "flowmq_subscription_set.h"
#include "flowmq_socket_option.h"
#include "flowmq_socket_external_internal.h"
#if defined(FLOWMQ_BATCH_PROBE)
#include "flowmq_socket_batch_probe.h"
#endif
#include "flowmq_tls_identity_map.h"
#include "cmeta_error.h"
#include "cmeta_buffer.h"
#include "str.h"

#include <cnet/cnet.h>
#include <cnet/manager.h>
#include <cnet/client_pool.h>
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
  FLOWMQ_SOCKET_CNET_PACKET_RECEIVE_BUFFER_BYTES =
      FLOWMQ_PROTOCOL_HEADER_SIZE + FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE +
      FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE + FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE,
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
_Static_assert(FLOWMQ_SOCKET_PEER_CAPACITY == FLOWMQ_PEER_POOL_MAX_PEERS,
               "public pool capacity must match socket peer storage");
_Static_assert(CNET_RETAINED_VECTOR_MAX >= 32u,
               "FlowMQ 1 MiB retained batching requires 32 logical CNet ranges");
_Static_assert(CNET_RETAINED_VECTOR_MAX <= FLOWMQ_SOCKET_OUTBOUND_CAPACITY,
               "CNet retained-vector bound must fit FlowMQ outbound storage");
_Static_assert(FLOWMQ_SOCKET_FRAME_SEGMENT_CAPACITY <= CNET_RETAINED_VECTOR_MAX,
               "one retained FMQ frame must fit one logical CNet vector");

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

typedef struct flowmq_socket_segmented_payload_s {
  mem_slice_t segments[FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY];
  size_t segment_count;
} flowmq_socket_segmented_payload_t;

typedef struct flowmq_socket_message_s {
  mem_buffer_t *buffer;
  flowmq_socket_segmented_payload_t *segmented;
  size_t offset;
  size_t size;
  size_t credit_size;
  size_t peer_index;
  uint64_t peer_generation;
  int more;
} flowmq_socket_message_t;

typedef struct flowmq_socket_retained_publication_s {
  mem_slice_t slices[CNET_RETAINED_VECTOR_MAX];
  size_t slice_count;
  size_t encoded_size;
  size_t payload_size;
  size_t refs;
} flowmq_socket_retained_publication_t;

typedef struct flowmq_socket_outbound_s {
  mem_buffer_t *buffer;
  flowmq_socket_retained_publication_t *retained;
  size_t encoded_size;
  size_t payload_size;
  int message_end;
} flowmq_socket_outbound_t;

typedef struct flowmq_socket_retained_frame_s {
  mem_buffer_t *framing;
  mem_slice_t slices[CNET_RETAINED_VECTOR_MAX];
  size_t slice_count;
  size_t encoded_size;
  size_t payload_size;
} flowmq_socket_retained_frame_t;

struct flowmq_socket_peer_s {
  struct flowmq_socket_s *owner;
  flowmq_peer_state_t state;
  cnet_connection connection;
  cnet_managed_connection managed;
  cnet_pool_key pool_key;
  cnet_pool_connection pool_connection;
  cnet_pool_lease pool_lease;
  unsigned pipe_reserved : 1;
  unsigned pool_terminal : 1;
  flowmq_stream_decoder_t decoder;
  flowmq_owned_stream_t owned_stream;
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
  uint64_t admitted_messages;
  uint64_t admitted_bytes;
  uint64_t completed_messages;
  uint64_t completed_bytes;
  uint64_t rejected_messages;
  uint64_t rejected_bytes;
  size_t peak_outstanding_messages;
  size_t peak_outstanding_bytes;
  char identity[FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE + 1u];
  unsigned receiving_multipart : 1;
  unsigned commit_pending : 1;
  unsigned heartbeat_active : 1;
  unsigned pong_pending : 1;
  unsigned owned_receive_active : 1;
  unsigned reconnect_ready_recorded;
};

struct flowmq_ctx_s {
  size_t socket_count;
  size_t owner_count;
  uint64_t next_socket_id;
};

struct flowmq_socket_s {
#if defined(FLOWMQ_BATCH_PROBE)
  flowmq_socket_batch_probe_t batch_probe;
  int batch_probe_coalesce;
#endif
  flowmq_ctx_t *ctx;
  flowmq_pattern_state_t pattern;
  cnet_client client;
  cnet_manager manager;
  cnet_client_pool peer_pool;
  flowmq_peer_pool_config_t peer_pool_config;
  uint64_t socket_id;
  uint64_t next_pipe_id;
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
  mem_slice_t send_retained_staged[CNET_RETAINED_VECTOR_MAX];
  size_t send_retained_part_sizes[FLOWMQ_SOCKET_MULTIPART_CAPACITY];
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
  size_t send_retained_count;
  size_t send_retained_payload_bytes;
  size_t send_retained_encoded_bytes;
  size_t send_retained_parts;
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
  int reuse_port;
  int async_error;
  int send_cancel_error;
  int recv_cancel_error;
  int transport;
  int last_rcvmore;
  native_io_backend *external_backend;
  const void *external_owner;
  unsigned runtime_initialized : 1;
  unsigned external_stopping : 1;
  unsigned external_stopped : 1;
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

#if defined(FLOWMQ_BATCH_PROBE)
int flowmq_socket_batch_probe_read(const flowmq_socket_t *socket,
                                 flowmq_socket_batch_probe_t *out) {
  if (socket == NULL || out == NULL) return SALTS_EINVAL;
  *out = socket->batch_probe;
  return SALTS_OK;
}

int flowmq_socket_batch_probe_coalesce(flowmq_socket_t *socket, int mode) {
  if (socket == NULL || mode < 0 || mode > 2) return SALTS_EINVAL;
  if (socket->runtime_initialized) return SALTS_EBUSY;
  socket->batch_probe_coalesce = mode;
  return SALTS_OK;
}
#endif

static int flowmq_socket_drive(flowmq_socket_t *socket, uint32_t timeout_ms,
                               size_t *events);
static void flowmq_socket_on_receive_slice(
    void *user, cnet_connection connection, mem_slice_t slice,
    cnet_message_kind kind);
static int flowmq_socket_rearm_receive(flowmq_socket_peer_t *peer);
static void flowmq_socket_cancel_send_route(flowmq_socket_t *socket);
static void flowmq_socket_fail(flowmq_socket_t *socket, int status);
static int flowmq_socket_peer_ready(const flowmq_socket_peer_t *peer);

static int flowmq_socket_pool_reserve(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket = peer->owner;
  if (socket->peer_pool.impl == NULL) return SALTS_OK;
  if (socket->next_pipe_id == UINT64_MAX) return SALTS_ERANGE;
  const uint64_t generation = ++socket->next_pipe_id;
  const uint64_t tls_id = socket->transport == FLOWMQ_TRANSPORT_TLS
                              ? socket->socket_id : 0u;
  /* The pool is permanently local to this socket/Manager. Startup TLS policy
   * is immutable, and a new session never shares the old generation's key. */
  peer->pool_key = (cnet_pool_key){
      .size = sizeof(peer->pool_key), .version = CNET_CLIENT_POOL_VERSION,
      .runtime_id = socket->socket_id, .owner_id = socket->socket_id,
      .endpoint_id = peer->endpoint_index + 1u, .peer_generation = generation,
      .authority_id = socket->socket_id,
      .transport_id = (uint64_t)socket->transport + 1u,
      .tls_trust_id = tls_id, .tls_sni_id = tls_id,
      .client_identity_id = socket->socket_id,
      .protocol_id = ((uint64_t)FLOWMQ_PROTOCOL_WIRE_VERSION << 32u) |
                     (uint32_t)socket->pattern.pattern,
      .session_id = generation};
  return cnet_pool_reserve_connecting(
      &socket->peer_pool, &peer->pool_key, &peer->pool_connection);
}

static int flowmq_socket_pool_terminal(flowmq_socket_peer_t *peer) {
  int status;
  if (peer->pool_connection.slot == 0u || peer->pool_terminal) return SALTS_OK;
  /* CONNECTING entries are not yet bound inside CNet pool. Check their real
   * Manager record too, so a close request can never stand in for terminal. */
  if (peer->managed.slot != 0u) {
    cnet_manager_entry entry;
    status = cnet_manager_lookup(&peer->owner->manager, peer->managed, &entry);
    if (status != SALTS_OK) return status;
    if (entry.state != CNET_MANAGER_RETIRED) return SALTS_EBUSY;
  }
  status = cnet_pool_terminal(&peer->owner->peer_pool, peer->pool_connection);
  if (status == SALTS_OK) peer->pool_terminal = 1u;
  return status;
}

static int flowmq_socket_pipe_reserve(
    void *user, cnet_managed_connection managed, uint64_t *token) {
  flowmq_socket_peer_t *peer = user;
  if (managed.manager != peer->managed.manager ||
      managed.incarnation != peer->managed.incarnation ||
      managed.generation != peer->managed.generation ||
      managed.slot != peer->managed.slot ||
      !flowmq_peer_state_ready(&peer->state)) return SALTS_EPROTO;
  if (peer->pipe_reserved) return SALTS_ENOBUFS;
  peer->pipe_reserved = 1u;
  *token = peer->pool_key.session_id;
  return SALTS_OK;
}

static void flowmq_socket_pipe_release(void *user, uint64_t token) {
  flowmq_socket_peer_t *peer = user;
  if (!peer->pipe_reserved || token != peer->pool_key.session_id) {
    flowmq_socket_fail(peer->owner, SALTS_EPROTO);
    return;
  }
  peer->pipe_reserved = 0u;
}

static int flowmq_socket_pool_ready(flowmq_socket_peer_t *peer) {
  cnet_managed_connection managed;
  int status;
  if (peer->pool_connection.slot == 0u || peer->pool_lease.slot != 0u)
    return SALTS_OK;
  const cnet_pool_protocol_ops ops = {
      flowmq_socket_pipe_reserve, flowmq_socket_pipe_release, peer};
  status = cnet_pool_bind_ready(&peer->owner->peer_pool,
                                peer->pool_connection, peer->managed, 1u);
  if (status != SALTS_OK) return status;
  return cnet_pool_try_acquire(&peer->owner->peer_pool, &peer->pool_key,
                                &ops, &peer->pool_lease, &managed);
}

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

static uint64_t flowmq_socket_counter_add_saturating(uint64_t current,
                                                     size_t increment) {
  uint64_t value;
  if (increment > (size_t)UINT64_MAX) return UINT64_MAX;
  value = (uint64_t)increment;
  return value > UINT64_MAX - current ? UINT64_MAX : current + value;
}

static void flowmq_socket_peer_record_admission(flowmq_socket_peer_t *peer,
                                                size_t payload_size,
                                                int message_end) {
  if (peer == NULL) return;
  peer->admitted_bytes =
      flowmq_socket_counter_add_saturating(peer->admitted_bytes, payload_size);
  if (message_end)
    peer->admitted_messages =
        flowmq_socket_counter_add_saturating(peer->admitted_messages, 1u);
  if (peer->outbound_messages > peer->peak_outstanding_messages)
    peer->peak_outstanding_messages = peer->outbound_messages;
  if (peer->outbound_bytes > peer->peak_outstanding_bytes)
    peer->peak_outstanding_bytes = peer->outbound_bytes;
}

static void flowmq_socket_peer_record_completion(flowmq_socket_peer_t *peer) {
  if (peer == NULL) return;
  peer->completed_bytes = flowmq_socket_counter_add_saturating(
      peer->completed_bytes, peer->inflight_payload_size);
  peer->completed_messages = flowmq_socket_counter_add_saturating(
      peer->completed_messages, peer->inflight_messages);
}

static void flowmq_socket_peer_record_rejection(flowmq_socket_peer_t *peer,
                                                size_t payload_size) {
  if (peer == NULL) return;
  peer->rejected_messages =
      flowmq_socket_counter_add_saturating(peer->rejected_messages, 1u);
  peer->rejected_bytes =
      flowmq_socket_counter_add_saturating(peer->rejected_bytes, payload_size);
}


static flowmq_socket_retained_publication_t *
flowmq_socket_retained_publication_retain(
    flowmq_socket_retained_publication_t *publication) {
  if (publication == NULL || publication->refs == SIZE_MAX) return NULL;
  ++publication->refs;
  return publication;
}

static void flowmq_socket_retained_publication_release(
    flowmq_socket_retained_publication_t *publication) {
  if (publication == NULL || publication->refs == 0u) return;
  --publication->refs;
  if (publication->refs != 0u) return;
  for (size_t i = 0u; i < publication->slice_count; ++i)
    mem_slice_release(&publication->slices[i]);
  free(publication);
}

static void flowmq_socket_outbound_release(
    flowmq_socket_outbound_t *outbound) {
  if (outbound == NULL) return;
  mem_buffer_release(outbound->buffer);
  flowmq_socket_retained_publication_release(outbound->retained);
  memset(outbound, 0, sizeof(*outbound));
}

static void flowmq_socket_segmented_payload_release(
    flowmq_socket_segmented_payload_t *payload) {
  if (payload == NULL) return;
  for (size_t i = 0u; i < payload->segment_count; ++i)
    mem_slice_release(&payload->segments[i]);
  free(payload);
}

static void flowmq_socket_message_storage_release(
    flowmq_socket_message_t *message) {
  if (message == NULL) return;
  mem_buffer_release(message->buffer);
  message->buffer = NULL;
  flowmq_socket_segmented_payload_release(message->segmented);
  message->segmented = NULL;
}

static void flowmq_socket_peer_storage_release(flowmq_socket_peer_t *peer) {
  for (size_t i = 0u; i < peer->staged_count; ++i)
    flowmq_socket_message_storage_release(&peer->staged[i]);
  for (size_t i = 0u; i < peer->outbound_count; ++i) {
    size_t index =
        (peer->outbound_read + i) % FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
    flowmq_socket_outbound_release(&peer->outbound[index]);
  }
  peer->staged_count = 0u;
  peer->staged_bytes = 0u;
  peer->outbound_count = 0u;
  peer->outbound_messages = 0u;
  peer->outbound_bytes = 0u;
  peer->commit_pending = 0u;
  flowmq_peer_state_write_cancel(&peer->state);
  flowmq_owned_stream_reset(&peer->owned_stream);
  flowmq_stream_decoder_destroy(&peer->decoder);
  flowmq_subscription_set_destroy(&peer->subscriptions);
  flowmq_subscription_set_destroy(&peer->synced_subscriptions);
  free(peer->outbound);
  peer->outbound = NULL;
}

static void flowmq_socket_peer_release(flowmq_socket_peer_t *peer) {
  int status;
  if (peer == NULL || !flowmq_peer_state_is_used(&peer->state)) return;
  status = flowmq_socket_pool_terminal(peer);
  if (status == SALTS_OK && peer->pool_lease.slot != 0u) {
    status = cnet_pool_release(&peer->owner->peer_pool, peer->pool_lease);
    if (status == SALTS_OK) memset(&peer->pool_lease, 0, sizeof(peer->pool_lease));
  }
  if (status != SALTS_OK) {
    flowmq_socket_fail(peer->owner, status);
    return;
  }
  if (peer->managed.slot != 0u) {
    status = cnet_manager_release_context(&peer->owner->manager, peer->managed);
    if (status != SALTS_OK) {
      flowmq_socket_fail(peer->owner, status);
      return;
    }
  }
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
  if (socket->external_stopping) return SALTS_OK;
  if (socket->reconnect_interval_ms < 0) return SALTS_OK;
  status = flowmq_reconnect_next(&endpoint->reconnect, &delay_ms);
  if (status != SALTS_OK) return status;
  now_ms = cmeta_monotonic_ms();
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
  if (peer->pool_connection.slot != 0u) {
    status = cnet_pool_begin_drain(&peer->owner->peer_pool, peer->pool_connection);
    if (status != SALTS_OK) flowmq_socket_fail(peer->owner, status);
  }
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

static int flowmq_socket_slice_validate(const mem_slice_t *slice,
                                        size_t *backing_offset) {
  const char *backing;
  size_t used;
  uintptr_t base;
  uintptr_t data;
  uintptr_t delta;
  if (slice == NULL || backing_offset == NULL || slice->buffer == NULL ||
      slice->data == NULL || slice->length == 0u)
    return SALTS_EINVAL;
  backing = mem_buffer_const_data(slice->buffer);
  used = mem_buffer_used(slice->buffer);
  if (backing == NULL || used == 0u) return SALTS_EINVAL;
  base = (uintptr_t)(const void *)backing;
  data = (uintptr_t)(const void *)slice->data;
  if (data < base) return SALTS_EINVAL;
  delta = data - base;
  if (delta > (uintptr_t)SIZE_MAX || (size_t)delta >= used ||
      slice->length > used - (size_t)delta)
    return SALTS_EINVAL;
  *backing_offset = (size_t)delta;
  return SALTS_OK;
}

static void flowmq_socket_retained_frame_release(
    flowmq_socket_retained_frame_t *frame) {
  if (frame == NULL) return;
  for (size_t i = 0u; i < frame->slice_count; ++i)
    mem_slice_release(&frame->slices[i]);
  if (frame->framing != NULL) mem_buffer_release(frame->framing);
  memset(frame, 0, sizeof(*frame));
}

static int flowmq_socket_prepare_retained_frame(
    flowmq_socket_t *socket, const flowmq_protocol_frame_t *frame,
    const mem_slice_t *payload, flowmq_socket_retained_frame_t *retained) {
  char *framing_data;
  const char *payload_data;
  const char *payload_backing;
  size_t payload_backing_offset = 0u;
  size_t framing_capacity;
  size_t framing_used = 0u;
  size_t segment_count = 0u;
  size_t encoded_size = 0u;
  uintptr_t framing_base;
  uintptr_t payload_base;
  int status;

  if (socket == NULL || frame == NULL || retained == NULL) return SALTS_EINVAL;
  memset(retained, 0, sizeof(*retained));
  status = flowmq_socket_slice_validate(payload, &payload_backing_offset);
  if (status != SALTS_OK) return status;

  retained->framing =
      mem_get_buffer(&socket->message_pool, FLOWMQ_SOCKET_FRAMING_CAPACITY);
  if (retained->framing == NULL) return SALTS_ENOMEM;
  framing_data = mem_buffer_data(retained->framing);
  framing_capacity = mem_buffer_capacity(retained->framing);
  if (framing_data == NULL || framing_capacity < FLOWMQ_SOCKET_FRAMING_CAPACITY) {
    flowmq_socket_retained_frame_release(retained);
    return SALTS_EPROTO;
  }

  status = flowmq_protocol_encode_frame_segmented_into_internal(
      frame, FLOWMQ_SOCKET_MAX_FRAME_SIZE, socket->frame_segments,
      FLOWMQ_SOCKET_FRAME_SEGMENT_CAPACITY, framing_data, framing_capacity,
      &segment_count, &encoded_size);
  if (status != SALTS_OK || segment_count == 0u ||
      segment_count > CNET_RETAINED_VECTOR_MAX ||
      encoded_size > socket->max_encoded_size) {
    flowmq_socket_retained_frame_release(retained);
    return status != SALTS_OK
               ? status
               : (encoded_size > socket->max_encoded_size ? SALTS_EMSGSIZE
                                                           : SALTS_EPROTO);
  }

  payload_data = payload->data;
  payload_backing = mem_buffer_const_data(payload->buffer);
  framing_base = (uintptr_t)(void *)framing_data;
  payload_base = (uintptr_t)(const void *)payload_data;

  for (size_t i = 0u; i < segment_count; ++i) {
    const flowmq_protocol_segment_t *segment = &socket->frame_segments[i];
    uintptr_t address;
    uintptr_t delta;
    if (segment->data == NULL || segment->size == 0u) {
      flowmq_socket_retained_frame_release(retained);
      return SALTS_EPROTO;
    }
    address = (uintptr_t)(const void *)segment->data;
    if (address >= framing_base) {
      delta = address - framing_base;
      if (delta <= (uintptr_t)SIZE_MAX &&
          (size_t)delta < framing_capacity &&
          segment->size <= framing_capacity - (size_t)delta) {
        size_t end = (size_t)delta + segment->size;
        if (end > framing_used) framing_used = end;
        continue;
      }
    }
    if (address < payload_base) {
      flowmq_socket_retained_frame_release(retained);
      return SALTS_EPROTO;
    }
    delta = address - payload_base;
    if (delta > (uintptr_t)SIZE_MAX || (size_t)delta >= payload->length ||
        segment->size > payload->length - (size_t)delta) {
      flowmq_socket_retained_frame_release(retained);
      return SALTS_EPROTO;
    }
  }

  if (framing_used == 0u || payload_backing == NULL) {
    flowmq_socket_retained_frame_release(retained);
    return SALTS_EPROTO;
  }
  mem_set_used(retained->framing, framing_used);

  for (size_t i = 0u; i < segment_count; ++i) {
    const flowmq_protocol_segment_t *segment = &socket->frame_segments[i];
    uintptr_t address = (uintptr_t)(const void *)segment->data;
    uintptr_t delta;
    mem_slice_t slice = {0};
    if (address >= framing_base) {
      delta = address - framing_base;
      if (delta <= (uintptr_t)SIZE_MAX && (size_t)delta < framing_used &&
          segment->size <= framing_used - (size_t)delta)
        slice = mem_slice(retained->framing, (size_t)delta, segment->size);
    }
    if (slice.buffer == NULL) {
      if (address < payload_base) {
        flowmq_socket_retained_frame_release(retained);
        return SALTS_EPROTO;
      }
      delta = address - payload_base;
      if (delta > (uintptr_t)SIZE_MAX ||
          (size_t)delta > SIZE_MAX - payload_backing_offset) {
        flowmq_socket_retained_frame_release(retained);
        return SALTS_EPROTO;
      }
      slice = mem_slice(payload->buffer,
                        payload_backing_offset + (size_t)delta,
                        segment->size);
    }
    if (slice.buffer == NULL || slice.length != segment->size) {
      mem_slice_release(&slice);
      flowmq_socket_retained_frame_release(retained);
      return SALTS_EPROTO;
    }
    retained->slices[retained->slice_count++] = slice;
  }

  retained->encoded_size = encoded_size;
  retained->payload_size = payload->length;
  return SALTS_OK;
}

static int flowmq_socket_send_segments_buffered(
    flowmq_socket_t *socket, cnet_connection connection,
    const cnet_const_buffer *segments, size_t segment_count,
    size_t encoded_size) {
  mem_buffer_t *buffer;
  int status;

  if (socket == NULL || segments == NULL || segment_count == 0u ||
      encoded_size == 0u || encoded_size > socket->max_encoded_size)
    return SALTS_EINVAL;

  buffer = mem_get_buffer(&socket->message_pool, encoded_size);
  if (buffer == NULL) return SALTS_ENOMEM;
  status = flowmq_socket_copy_segments(
      segments, segment_count, encoded_size,
      mem_buffer_data(buffer), encoded_size);
  if (status == SALTS_OK) {
    mem_set_used(buffer, encoded_size);
    status = cnet_send_buffer(&socket->client, connection, buffer);
  }
  mem_buffer_release(buffer);
  return status;
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
  return flowmq_socket_send_segments_buffered(
      socket, connection, socket->send_segments, segment_count, encoded_size);
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
  if (!flowmq_socket_peer_ready(peer) ||
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
  return flowmq_peer_state_ready(&peer->state) &&
         (peer->owner->peer_pool.impl == NULL ||
          (peer->pool_lease.slot != 0u && peer->pipe_reserved));
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

static void flowmq_socket_release_retained_staged(flowmq_socket_t *socket) {
  if (socket == NULL) return;
  for (size_t i = 0u; i < socket->send_retained_count; ++i)
    mem_slice_release(&socket->send_retained_staged[i]);
  socket->send_retained_count = 0u;
  socket->send_retained_payload_bytes = 0u;
  socket->send_retained_encoded_bytes = 0u;
  socket->send_retained_parts = 0u;
  memset(socket->send_retained_part_sizes, 0,
         sizeof(socket->send_retained_part_sizes));
}

static int flowmq_socket_stage_retained_frame(
    flowmq_socket_t *socket, flowmq_socket_retained_frame_t *frame) {
  if (socket == NULL || frame == NULL || frame->slice_count == 0u ||
      frame->payload_size == 0u)
    return SALTS_EINVAL;
  if (frame->slice_count >
          CNET_RETAINED_VECTOR_MAX - socket->send_retained_count ||
      frame->encoded_size >
          socket->max_encoded_size - socket->send_retained_encoded_bytes ||
      frame->payload_size >
          socket->send_hwm_bytes - socket->send_retained_payload_bytes ||
      socket->send_retained_parts >= FLOWMQ_SOCKET_MULTIPART_CAPACITY)
    return SALTS_EMSGSIZE;

  for (size_t i = 0u; i < frame->slice_count; ++i) {
    socket->send_retained_staged[socket->send_retained_count++] =
        frame->slices[i];
    frame->slices[i] = (mem_slice_t){0};
  }
  socket->send_retained_part_sizes[socket->send_retained_parts] =
      frame->payload_size;
  socket->send_retained_payload_bytes += frame->payload_size;
  socket->send_retained_encoded_bytes += frame->encoded_size;
  ++socket->send_retained_parts;

  /*
   * Each moved framing slice owns a reference to this backing. Drop the
   * temporary frame's construction reference after ownership moves.
   */
  if (frame->framing != NULL) {
    mem_buffer_release(frame->framing);
    frame->framing = NULL;
  }
  frame->slice_count = 0u;
  frame->encoded_size = 0u;
  frame->payload_size = 0u;
  return SALTS_OK;
}

static int flowmq_socket_retained_publication_clone_slice(
    flowmq_socket_retained_publication_t *publication,
    const mem_slice_t *source) {
  size_t offset = 0u;
  mem_slice_t retained;
  int status;
  if (publication == NULL || source == NULL ||
      publication->slice_count >= CNET_RETAINED_VECTOR_MAX)
    return SALTS_EINVAL;
  status = flowmq_socket_slice_validate(source, &offset);
  if (status != SALTS_OK) return status;
  retained = mem_slice(source->buffer, offset, source->length);
  if (retained.buffer == NULL || retained.data != source->data ||
      retained.length != source->length) {
    mem_slice_release(&retained);
    return SALTS_EPROTO;
  }
  publication->slices[publication->slice_count++] = retained;
  return SALTS_OK;
}

static int flowmq_socket_retained_publication_create(
    const flowmq_socket_t *socket,
    const flowmq_socket_retained_frame_t *final_frame,
    flowmq_socket_retained_publication_t **out) {
  flowmq_socket_retained_publication_t *publication;
  int status = SALTS_OK;
  if (socket == NULL || final_frame == NULL || out == NULL || *out != NULL ||
      final_frame->slice_count == 0u || final_frame->payload_size == 0u ||
      final_frame->slice_count >
          CNET_RETAINED_VECTOR_MAX - socket->send_retained_count ||
      final_frame->encoded_size >
          socket->max_encoded_size - socket->send_retained_encoded_bytes ||
      final_frame->payload_size >
          socket->send_hwm_bytes - socket->send_retained_payload_bytes)
    return SALTS_EINVAL;

  publication = (flowmq_socket_retained_publication_t *)calloc(
      1u, sizeof(*publication));
  if (publication == NULL) return SALTS_ENOMEM;
  publication->refs = 1u;
  publication->encoded_size =
      socket->send_retained_encoded_bytes + final_frame->encoded_size;
  publication->payload_size =
      socket->send_retained_payload_bytes + final_frame->payload_size;

  for (size_t i = 0u;
       i < socket->send_retained_count && status == SALTS_OK; ++i)
    status = flowmq_socket_retained_publication_clone_slice(
        publication, &socket->send_retained_staged[i]);
  for (size_t i = 0u;
       i < final_frame->slice_count && status == SALTS_OK; ++i)
    status = flowmq_socket_retained_publication_clone_slice(
        publication, &final_frame->slices[i]);

  if (status != SALTS_OK ||
      publication->slice_count !=
          socket->send_retained_count + final_frame->slice_count) {
    flowmq_socket_retained_publication_release(publication);
    return status != SALTS_OK ? status : SALTS_EPROTO;
  }

  *out = publication;
  return SALTS_OK;
}

static int flowmq_socket_peer_can_queue_retained_message(
    const flowmq_socket_peer_t *peer, const flowmq_socket_t *socket,
    size_t payload_size, size_t final_part_size) {
  const flowmq_socket_outbound_t *slot;
  if (peer == NULL || socket == NULL || !flowmq_socket_peer_ready(peer) ||
      peer->outbound_count >= FLOWMQ_SOCKET_OUTBOUND_CAPACITY ||
      payload_size > socket->send_hwm_bytes ||
      peer->outbound_bytes > socket->send_hwm_bytes - payload_size ||
      peer->outbound_messages >= socket->send_hwm ||
      flowmq_flow_control_send_credit_check(
          &peer->flow_control, payload_size) != SALTS_OK ||
      final_part_size > peer->flow_control.remote_max_frame_size)
    return 0;
  for (size_t i = 0u; i < socket->send_retained_parts; ++i) {
    if (socket->send_retained_part_sizes[i] >
        peer->flow_control.remote_max_frame_size)
      return 0;
  }
  slot = &peer->outbound[peer->outbound_write];
  return slot->buffer == NULL && slot->retained == NULL;
}

static int flowmq_socket_commit_retained_fanout(
    flowmq_socket_t *socket,
    flowmq_socket_retained_publication_t *publication,
    uint32_t peer_mask) {
  uint64_t old_sent[FLOWMQ_SOCKET_PEER_CAPACITY] = {0};
  size_t committed = 0u;
  int status = SALTS_OK;
  if (socket == NULL || publication == NULL ||
      publication->refs == 0u || peer_mask == 0u)
    return SALTS_EINVAL;

  /*
   * All queue/HWM/generation checks are complete before entry. Credit commit
   * is the only remaining status-returning mutation; save exact old values so
   * an invariant failure can roll back before any peer queue becomes visible.
   */
  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
    flowmq_socket_peer_t *peer;
    if ((peer_mask & (UINT32_C(1) << i)) == 0u) continue;
    peer = &socket->peers[i];
    old_sent[i] = peer->flow_control.sent_data;
    status = flowmq_flow_control_send_credit_commit(
        &peer->flow_control, publication->payload_size);
    if (status != SALTS_OK) {
      for (size_t j = 0u; j < i; ++j) {
        if ((peer_mask & (UINT32_C(1) << j)) != 0u)
          socket->peers[j].flow_control.sent_data = old_sent[j];
      }
      return status;
    }
    ++committed;
  }

  if (publication->refs > SIZE_MAX - committed) {
    for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
      if ((peer_mask & (UINT32_C(1) << i)) != 0u)
        socket->peers[i].flow_control.sent_data = old_sent[i];
    }
    return SALTS_ERANGE;
  }

  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
    flowmq_socket_peer_t *peer;
    flowmq_socket_outbound_t *outbound;
    flowmq_socket_retained_publication_t *retained;
    if ((peer_mask & (UINT32_C(1) << i)) == 0u) continue;
    peer = &socket->peers[i];
    outbound = &peer->outbound[peer->outbound_write];
    retained = flowmq_socket_retained_publication_retain(publication);
    if (retained == NULL) {
      /*
       * Refcount overflow was precluded above; reaching this branch indicates
       * internal corruption. Queue mutation has not started for this peer, but
       * earlier peers may already own references, so fail the socket hard.
       */
      flowmq_socket_fail(socket, SALTS_EPROTO);
      return SALTS_EPROTO;
    }
    *outbound = (flowmq_socket_outbound_t){
        .retained = retained,
        .encoded_size = publication->encoded_size,
        .payload_size = publication->payload_size,
        .message_end = 1};
    peer->outbound_write =
        (peer->outbound_write + 1u) % FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
    ++peer->outbound_count;
    peer->outbound_bytes += publication->payload_size;
    ++peer->outbound_messages;
    flowmq_socket_peer_record_admission(
        peer, publication->payload_size, 1);
  }
  return SALTS_OK;
}

static void flowmq_socket_cancel_send_route(flowmq_socket_t *socket) {
  flowmq_socket_release_send_staged(socket);
  flowmq_socket_release_retained_staged(socket);
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
  flowmq_socket_peer_record_admission(
      peer, socket->send_staged_bytes, 1);
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
    status = flowmq_socket_send_segments_buffered(
        socket, peer->connection, segments, segment_count, encoded_size);
    if (status != SALTS_OK) {
      flowmq_peer_state_write_cancel(&peer->state);
      return status;
    }
    peer->inflight_payload_size = payload_size;
    peer->inflight_messages = message_end ? 1u : 0u;
#if defined(FLOWMQ_BATCH_PROBE)
    ++socket->batch_probe.direct_writes;
    ++socket->batch_probe.submitted_ranges;
    socket->batch_probe.messages += message_end ? 1u : 0u;
    socket->batch_probe.payload_bytes += payload_size;
#endif
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
  flowmq_socket_peer_record_admission(peer, payload_size, message_end);
  return SALTS_OK;
}

static int flowmq_socket_peer_admit_retained(
    flowmq_socket_peer_t *peer,
    const flowmq_socket_retained_frame_t *frame) {
  flowmq_socket_t *socket;
  int status;
  if (peer == NULL || frame == NULL || frame->slice_count == 0u ||
      frame->payload_size == 0u)
    return SALTS_EINVAL;
  socket = peer->owner;
  if (socket->transport != FLOWMQ_TRANSPORT_TCP &&
      socket->transport != FLOWMQ_TRANSPORT_TLS)
    return SALTS_ENOTSUP;
  if (!flowmq_socket_peer_can_admit(peer, frame->payload_size, 1))
    return SALTS_ENOBUFS;
  if (!flowmq_peer_state_write_idle(&peer->state) ||
      peer->outbound_count != 0u)
    return SALTS_EBUSY;

  status = flowmq_peer_state_write_begin(&peer->state,
                                         FLOWMQ_PEER_WRITE_DATA);
  if (status != SALTS_OK) return status;
  status = cnet_send_slicev(&socket->client, peer->connection,
                            frame->slices, frame->slice_count);
  if (status != SALTS_OK) {
    flowmq_peer_state_write_cancel(&peer->state);
    return status;
  }

  peer->inflight_payload_size = frame->payload_size;
  peer->inflight_messages = 1u;
  peer->outbound_bytes += frame->payload_size;
  ++peer->outbound_messages;
  status =
      flowmq_flow_control_send_commit(&peer->flow_control, frame->payload_size);
  if (status != SALTS_OK) {
    flowmq_socket_fail(socket, SALTS_EPROTO);
    return SALTS_EPROTO;
  }
  flowmq_socket_peer_record_admission(peer, frame->payload_size, 1);
  return SALTS_OK;
}

static int flowmq_socket_peer_admit_retained_message(
    flowmq_socket_peer_t *peer, const mem_slice_t *slices,
    size_t slice_count, size_t payload_size) {
  flowmq_socket_t *socket;
  int status;
  if (peer == NULL || slices == NULL || slice_count == 0u ||
      slice_count > CNET_RETAINED_VECTOR_MAX || payload_size == 0u)
    return SALTS_EINVAL;
  socket = peer->owner;
  if (socket->transport != FLOWMQ_TRANSPORT_TCP &&
      socket->transport != FLOWMQ_TRANSPORT_TLS)
    return SALTS_ENOTSUP;
  if (!flowmq_socket_peer_ready(peer)) return SALTS_EBUSY;
  if (payload_size > socket->send_hwm_bytes ||
      peer->outbound_bytes > socket->send_hwm_bytes - payload_size ||
      peer->outbound_messages >= socket->send_hwm)
    return SALTS_ENOBUFS;
  status =
      flowmq_flow_control_send_credit_check(&peer->flow_control, payload_size);
  if (status != SALTS_OK) return status;
  if (!flowmq_peer_state_write_idle(&peer->state) ||
      peer->outbound_count != 0u)
    return SALTS_EBUSY;

  status =
      flowmq_peer_state_write_begin(&peer->state, FLOWMQ_PEER_WRITE_DATA);
  if (status != SALTS_OK) return status;
  status =
      cnet_send_slicev(&socket->client, peer->connection, slices, slice_count);
  if (status != SALTS_OK) {
    flowmq_peer_state_write_cancel(&peer->state);
    return status;
  }

  peer->inflight_payload_size = payload_size;
  peer->inflight_messages = 1u;
  peer->outbound_bytes += payload_size;
  ++peer->outbound_messages;
  status =
      flowmq_flow_control_send_credit_commit(&peer->flow_control, payload_size);
  if (status != SALTS_OK) {
    flowmq_socket_fail(socket, SALTS_EPROTO);
    return SALTS_EPROTO;
  }
  flowmq_socket_peer_record_admission(peer, payload_size, 1);
  return SALTS_OK;
}

static void flowmq_socket_release_send_slices(mem_slice_t *slices,
                                              size_t slice_count) {
  for (size_t i = 0u; i < slice_count; ++i)
    mem_slice_release(&slices[i]);
}

static int flowmq_socket_peer_flush(flowmq_socket_peer_t *peer) {
  /*
   * Copied queue entries are batched as retained canonical buffers. A retained
   * publication entry already owns the exact logical vector and therefore
   * flushes alone as one CNet logical write.
   */
  flowmq_socket_t *socket = peer->owner;
  flowmq_socket_outbound_t *outbound;
#if defined(FLOWMQ_BATCH_PROBE)
  const size_t batch_capacity = socket->batch_probe_coalesce == 2
      ? FLOWMQ_BATCH_PROBE_MAX_FRAMES : CNET_RETAINED_VECTOR_MAX;
  mem_slice_t slices[FLOWMQ_BATCH_PROBE_MAX_FRAMES] = {0};
#else
  const size_t batch_capacity = CNET_RETAINED_VECTOR_MAX;
  mem_slice_t slices[CNET_RETAINED_VECTOR_MAX] = {0};
#endif
  size_t batch_count = 0u;
  size_t batch_encoded_size = 0u;
  size_t batch_payload_size = 0u;
  size_t batch_messages = 0u;
  int status;

  if (!flowmq_peer_state_is_connected(&peer->state) ||
      !flowmq_peer_state_write_idle(&peer->state) ||
      peer->outbound_count == 0u)
    return SALTS_OK;
  if (socket->transport != FLOWMQ_TRANSPORT_TCP &&
      socket->transport != FLOWMQ_TRANSPORT_TLS)
    return SALTS_ENOTSUP;

  outbound = &peer->outbound[peer->outbound_read];
  if (outbound->retained != NULL) {
    flowmq_socket_retained_publication_t *publication = outbound->retained;
    if (outbound->buffer != NULL || publication->slice_count == 0u ||
        publication->slice_count > CNET_RETAINED_VECTOR_MAX ||
        publication->payload_size != outbound->payload_size ||
        publication->encoded_size != outbound->encoded_size ||
        !outbound->message_end)
      return SALTS_EPROTO;

    status =
        flowmq_peer_state_write_begin(&peer->state, FLOWMQ_PEER_WRITE_DATA);
    if (status != SALTS_OK) return status;
    status = cnet_send_slicev(&socket->client, peer->connection,
                              publication->slices,
                              publication->slice_count);
    if (status != SALTS_OK) {
      flowmq_peer_state_write_cancel(&peer->state);
      return status;
    }

    peer->inflight_payload_size = publication->payload_size;
    peer->inflight_messages = 1u;
    flowmq_socket_outbound_release(outbound);
    peer->outbound_read =
        (peer->outbound_read + 1u) % FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
    --peer->outbound_count;
    return SALTS_OK;
  }

  while (batch_count < peer->outbound_count &&
         batch_count < batch_capacity) {
    size_t index =
        (peer->outbound_read + batch_count) %
        FLOWMQ_SOCKET_OUTBOUND_CAPACITY;
    outbound = &peer->outbound[index];
    if (outbound->retained != NULL) break;
    if (outbound->buffer == NULL ||
        outbound->encoded_size >
            socket->max_encoded_size - batch_encoded_size)
      break;
    slices[batch_count] =
        mem_slice(outbound->buffer, 0u, outbound->encoded_size);
    if (slices[batch_count].buffer == NULL ||
        slices[batch_count].length != outbound->encoded_size) {
      flowmq_socket_release_send_slices(slices, batch_count + 1u);
      return SALTS_EPROTO;
    }
    batch_encoded_size += outbound->encoded_size;
    batch_payload_size += outbound->payload_size;
    if (outbound->message_end) ++batch_messages;
    ++batch_count;
  }
  if (batch_count == 0u) return SALTS_EMSGSIZE;

  status =
      flowmq_peer_state_write_begin(&peer->state, FLOWMQ_PEER_WRITE_DATA);
  if (status != SALTS_OK) {
    flowmq_socket_release_send_slices(slices, batch_count);
    return status;
  }
#if defined(FLOWMQ_BATCH_PROBE)
  if (socket->batch_probe_coalesce) {
    mem_buffer_t *buffer = mem_get_buffer(&socket->message_pool, batch_encoded_size);
    if (buffer == NULL) {
      status = SALTS_ENOMEM;
    } else {
      size_t offset = 0u;
      for (size_t i = 0u; i < batch_count; ++i) {
        memcpy((unsigned char *)mem_buffer_data(buffer) + offset,
               slices[i].data, slices[i].length);
        offset += slices[i].length;
      }
      mem_set_used(buffer, batch_encoded_size);
      status = cnet_send_buffer(&socket->client, peer->connection, buffer);
      mem_buffer_release(buffer);
    }
  } else
#endif
  {
    status = cnet_send_slicev(&socket->client, peer->connection, slices,
                            batch_count);
  }
  flowmq_socket_release_send_slices(slices, batch_count);
  if (status != SALTS_OK) {
    flowmq_peer_state_write_cancel(&peer->state);
    return status;
  }

  peer->inflight_payload_size = batch_payload_size;
  peer->inflight_messages = batch_messages;
#if defined(FLOWMQ_BATCH_PROBE)
  ++socket->batch_probe.queued_writes;
  socket->batch_probe.messages += batch_messages;
  socket->batch_probe.payload_bytes += batch_payload_size;
  socket->batch_probe.queued_ranges += batch_count;
  ++socket->batch_probe.queued_range_histogram[batch_count];
  socket->batch_probe.coalesced_writes += socket->batch_probe_coalesce != 0;
  socket->batch_probe.submitted_ranges += socket->batch_probe_coalesce ? 1u : batch_count;
#endif
  for (size_t i = 0u; i < batch_count; ++i) {
    outbound = &peer->outbound[peer->outbound_read];
    flowmq_socket_outbound_release(outbound);
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
      .offset = 0u,
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


static int flowmq_socket_stage_owned_frame(
    flowmq_socket_peer_t *peer,
    const flowmq_protocol_frame_t *frame,
    mem_slice_t *owned) {
  flowmq_socket_t *socket;
  flowmq_socket_message_t *message;
  const unsigned char *base;
  const unsigned char *payload;
  size_t offset;
  if (peer == NULL || frame == NULL || owned == NULL ||
      owned->buffer == NULL || owned->data == NULL)
    return SALTS_EINVAL;
  socket = peer->owner;
  if (peer->staged_count == FLOWMQ_SOCKET_MULTIPART_CAPACITY)
    return SALTS_ENOBUFS;
  if (frame->payload.len >
      FLOWMQ_SOCKET_HARD_HWM_BYTES - peer->staged_bytes)
    return SALTS_EMSGSIZE;
  base = (const unsigned char *)mem_buffer_const_data(owned->buffer);
  payload = (const unsigned char *)frame->payload.data;
  if (base == NULL || payload < base)
    return SALTS_EPROTO;
  offset = (size_t)(payload - base);
  if (offset > mem_buffer_used(owned->buffer) ||
      frame->payload.len > mem_buffer_used(owned->buffer) - offset)
    return SALTS_EPROTO;

  message = &peer->staged[peer->staged_count];
  *message = (flowmq_socket_message_t){
      .buffer = owned->buffer,
      .offset = offset,
      .size = frame->payload.len,
      .credit_size = frame->payload.len,
      .peer_index = flowmq_socket_peer_index(socket, peer),
      .peer_generation = peer->flow_control.local_generation,
      .more = frame->more};
  owned->buffer = NULL;
  owned->data = NULL;
  owned->length = 0u;
  ++peer->staged_count;
  peer->staged_bytes += frame->payload.len;
  return SALTS_OK;
}


static int flowmq_socket_stage_owned_projection(
    flowmq_socket_peer_t *peer,
    const flowmq_protocol_frame_t *frame,
    flowmq_owned_data_projection_t *projection) {
  flowmq_socket_t *socket;
  flowmq_socket_message_t *message;
  flowmq_socket_segmented_payload_t *segmented;
  size_t total = 0u;
  int status;

  if (peer == NULL || frame == NULL || projection == NULL ||
      projection->segment_count == 0u ||
      projection->payload_size != frame->payload.len)
    return SALTS_EINVAL;
  socket = peer->owner;
  if (peer->staged_count == FLOWMQ_SOCKET_MULTIPART_CAPACITY)
    return SALTS_ENOBUFS;
  if (frame->payload.len >
      FLOWMQ_SOCKET_HARD_HWM_BYTES - peer->staged_bytes)
    return SALTS_EMSGSIZE;

  if (projection->segment_count == 1u) {
    flowmq_protocol_frame_t direct = *frame;
    direct.payload.data = projection->segments[0].data;
    status = flowmq_socket_stage_owned_frame(
        peer, &direct, &projection->segments[0]);
    if (status == SALTS_OK) projection->segment_count = 0u;
    return status;
  }

  segmented = (flowmq_socket_segmented_payload_t *)calloc(
      1u, sizeof(*segmented));
  if (segmented == NULL) return SALTS_ENOMEM;

  for (size_t i = 0u; i < projection->segment_count; ++i) {
    const mem_slice_t *slice = &projection->segments[i];
    if (!flowmq_owned_stream_slice_valid(slice) ||
        slice->length > frame->payload.len - total) {
      flowmq_socket_segmented_payload_release(segmented);
      return SALTS_EPROTO;
    }
    total += slice->length;
  }
  if (total != frame->payload.len) {
    flowmq_socket_segmented_payload_release(segmented);
    return SALTS_EPROTO;
  }

  segmented->segment_count = projection->segment_count;
  for (size_t i = 0u; i < projection->segment_count; ++i) {
    segmented->segments[i] = projection->segments[i];
    memset(&projection->segments[i], 0, sizeof(projection->segments[i]));
  }
  projection->segment_count = 0u;

  message = &peer->staged[peer->staged_count];
  *message = (flowmq_socket_message_t){
      .segmented = segmented,
      .size = frame->payload.len,
      .credit_size = frame->payload.len,
      .peer_index = flowmq_socket_peer_index(socket, peer),
      .peer_generation = peer->flow_control.local_generation,
      .more = frame->more};
  ++peer->staged_count;
  peer->staged_bytes += frame->payload.len;
  return SALTS_OK;
}

static int flowmq_socket_process_data_frame(
    flowmq_socket_peer_t *peer,
    const flowmq_protocol_frame_t *frame,
    mem_slice_t *owned_payload,
    flowmq_owned_data_projection_t *owned_projection,
    int *pause_receive) {
  flowmq_socket_t *socket;
  int status;
  if (peer == NULL || frame == NULL || pause_receive == NULL ||
      (owned_payload != NULL && owned_projection != NULL))
    return SALTS_EINVAL;
  socket = peer->owner;
  *pause_receive = 0;

  if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ &&
      (!socket->request_peer_valid ||
       socket->request_peer_index != flowmq_socket_peer_index(socket, peer) ||
       socket->request_peer_generation !=
           peer->flow_control.local_generation))
    return SALTS_EPROTO;

  status = flowmq_flow_control_receive_check(
      &peer->flow_control, frame->payload.len);
  if (status == SALTS_OK &&
      socket->pattern.desc->routing_class ==
          FLOWMQ_PATTERN_ROUTE_IDENTITY &&
      !peer->receiving_multipart) {
    status = flowmq_socket_stage_bytes(
        peer, peer->identity, peer->identity_size, 0u, 1);
  }
  if (status == SALTS_OK) {
    if (owned_projection != NULL)
      status = flowmq_socket_stage_owned_projection(
          peer, frame, owned_projection);
    else if (owned_payload != NULL)
      status = flowmq_socket_stage_owned_frame(peer, frame, owned_payload);
    else
      status = flowmq_socket_stage_frame(peer, frame);
  }
  if (status == SALTS_OK)
    status = flowmq_flow_control_receive_commit(
        &peer->flow_control, frame->payload.len);
  if (status == SALTS_OK)
    peer->receiving_multipart = frame->more != 0;
  if (status == SALTS_OK && !frame->more) {
    status = flowmq_socket_commit_staged(peer);
    if (status == SALTS_ENOBUFS) {
      peer->commit_pending = 1u;
      *pause_receive = 1;
      status = SALTS_OK;
    }
  }
  return status;
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
  status = flowmq_flow_control_next_update(&peer->flow_control, cmeta_hrtime(),
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
  now_ns = cmeta_hrtime();
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

/* A successful transport connect is not a recovered FMQ/6 peer session.
 * Only the full HELLO + SETTINGS exchange can reset reconnect backoff. */
static void flowmq_socket_peer_reconnect_protocol_ready(flowmq_socket_peer_t *peer) {
  flowmq_socket_endpoint_t *endpoint;
  int status;
  if (peer == NULL || !flowmq_peer_state_ready(&peer->state)) return;
  status = flowmq_socket_pool_ready(peer);
  if (status != SALTS_OK) {
    flowmq_socket_fail(peer->owner, status);
    flowmq_socket_peer_fail(peer);
    return;
  }
  if (peer->endpoint_index >= FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY) return;
  endpoint = &peer->owner->endpoints[peer->endpoint_index];
  if (!endpoint->used || !endpoint->active) return;
  status = flowmq_reconnect_reset_on_protocol_ready(
      &endpoint->reconnect, &peer->state, &peer->reconnect_ready_recorded);
  if (status != SALTS_OK && status != SALTS_EBUSY &&
      status != SALTS_EALREADY)
    flowmq_socket_fail(peer->owner, status);
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
              &peer->heartbeat, cmeta_hrtime());
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
      if (status == SALTS_OK)
        flowmq_socket_peer_reconnect_protocol_ready(peer);
      if (status == SALTS_OK && peer->heartbeat_active)
        flowmq_protocol_heartbeat_deadlines_on_receive(
            &peer->heartbeat, cmeta_hrtime());
    } else {
      status = frame.pattern == peer->remote_pattern
                   ? flowmq_pattern_data_direction_validate(
                         socket->pattern.pattern, &frame)
                   : SALTS_EPROTO;
      if (status == SALTS_OK && frame.kind == FLOWMQ_PROTOCOL_FRAME_SETTINGS)
        status = SALTS_EPROTO;
      if (status == SALTS_OK && peer->heartbeat_active)
        flowmq_protocol_heartbeat_deadlines_on_receive(&peer->heartbeat,
                                                       cmeta_hrtime());
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
        status =
            flowmq_socket_process_data_frame(
                peer, &frame, NULL, NULL, &pause_receive);
      }
    }
    flowmq_protocol_frame_cleanup(&frame);
    if (status != SALTS_OK) return status;
    status = flowmq_stream_decoder_consume(&peer->decoder, consumed);
    if (status != SALTS_OK) return status;
    if (pause_receive) return SALTS_ENOBUFS;
  }
}


static int flowmq_socket_process_owned_stream(
    flowmq_socket_peer_t *peer, int *pause_receive) {
  flowmq_socket_t *socket;
  int status;
  if (peer == NULL || pause_receive == NULL) return SALTS_EINVAL;
  socket = peer->owner;
  *pause_receive = 0;

  while (flowmq_owned_stream_size(&peer->owned_stream) != 0u) {
    flowmq_owned_data_projection_t projection =
        FLOWMQ_OWNED_DATA_PROJECTION_INIT;
    size_t frame_size = 0u;

    status = flowmq_owned_stream_first_frame_size(
        &peer->owned_stream, FLOWMQ_SOCKET_MAX_FRAME_SIZE, &frame_size);
    if (status == FLOWMQ_PROTOCOL_INCOMPLETE) return SALTS_OK;
    if (status != SALTS_OK) return status;
    if (frame_size == 0u) return SALTS_EPROTO;

    status = flowmq_owned_stream_project_first_data(
        &peer->owned_stream, FLOWMQ_SOCKET_MAX_FRAME_SIZE, &projection);
    if (status == SALTS_OK) {
      flowmq_protocol_frame_t frame = {
          .kind = FLOWMQ_PROTOCOL_FRAME_DATA,
          .pattern = projection.pattern,
          .message_id = projection.message_id,
          .more = projection.more,
          .payload = {.data = NULL, .len = projection.payload_size}};
      status = frame.pattern == peer->remote_pattern
                   ? flowmq_pattern_data_direction_validate(
                         socket->pattern.pattern, &frame)
                   : SALTS_EPROTO;
      if (status == SALTS_OK && peer->heartbeat_active)
        flowmq_protocol_heartbeat_deadlines_on_receive(
            &peer->heartbeat, cmeta_hrtime());
      if (status == SALTS_OK)
        status = flowmq_socket_process_data_frame(
            peer, &frame, NULL, &projection, pause_receive);
      if (status == SALTS_OK)
        status = flowmq_owned_stream_consume(
            &peer->owned_stream, frame_size);
      flowmq_owned_data_projection_reset(&projection);
      if (status != SALTS_OK) return status;
      if (*pause_receive) return SALTS_OK;
      continue;
    }

    /*
     * A complete valid non-DATA frame or a DATA payload-vector shape that
     * exceeds the fixed projection bound stays on the proven copied decoder.
     */
    flowmq_owned_data_projection_reset(&projection);
    if (status != SALTS_ENOTSUP && status != SALTS_ENOSPC)
      return status;

    status = flowmq_owned_stream_replay_prefix(
        &peer->owned_stream, &peer->decoder, frame_size);
    if (status != SALTS_OK) return status;
    status = flowmq_socket_process_receive(peer);
    if (status == SALTS_ENOBUFS) {
      *pause_receive = 1;
      return SALTS_OK;
    }
    if (status != SALTS_OK) return status;
    if (flowmq_stream_decoder_size(&peer->decoder) != 0u)
      return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int flowmq_socket_owned_stream_fallback(
    flowmq_socket_peer_t *peer, const mem_slice_t *current,
    int *pause_receive) {
  int status;
  if (peer == NULL || current == NULL || pause_receive == NULL ||
      current->buffer == NULL || current->data == NULL ||
      current->length == 0u)
    return SALTS_EINVAL;
  *pause_receive = 0;

  status = flowmq_owned_stream_replay(&peer->owned_stream, &peer->decoder);
  if (status == SALTS_OK)
    status = flowmq_stream_decoder_append(
        &peer->decoder, current->data, current->length);
  if (status == SALTS_OK)
    status = flowmq_socket_process_receive(peer);
  if (status == SALTS_ENOBUFS) {
    *pause_receive = 1;
    return SALTS_OK;
  }
  return status;
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
        /* CONNECTED has no authority to reset protocol recovery state. */
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
          &peer->heartbeat, cmeta_hrtime(),
          (uint64_t)socket->heartbeat_interval_ms, (uint64_t)timeout_ms, 0u);
      peer->heartbeat_active = 1u;
    }
    {
      int status = cnet_receive(&socket->client, connection, 1u);
      if (status == SALTS_OK)
        status = flowmq_socket_send_hello(peer);
      if (status != SALTS_OK)
        flowmq_socket_peer_fail(peer);
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
    const int pool_status = flowmq_socket_pool_terminal(peer);
    if (pool_status != SALTS_OK) {
      flowmq_socket_fail(socket, pool_status);
      return;
    }
    flowmq_socket_peer_retire(peer);
  }
}



static int flowmq_socket_rearm_receive(flowmq_socket_peer_t *peer) {
  flowmq_socket_t *socket;
  int status = SALTS_OK;
  if (peer == NULL || peer->owner == NULL) return SALTS_EINVAL;
  socket = peer->owner;

  /*
   * Keep HELLO/SETTINGS on the legacy borrowed surface. Switch only at a
   * demand-free boundary after the handshake is complete and the copied
   * decoder has drained, so the first owned receive is application DATA.
   */
  if (socket->transport == FLOWMQ_TRANSPORT_TCP &&
      !peer->owned_receive_active &&
      flowmq_peer_state_handshake_has(
          &peer->state, FLOWMQ_PEER_HANDSHAKE_HELLO_RX) &&
      flowmq_peer_state_handshake_has(
          &peer->state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_RX) &&
      flowmq_stream_decoder_size(&peer->decoder) == 0u &&
      flowmq_owned_stream_size(&peer->owned_stream) == 0u) {
    status = cnet_manager_set_receive_slice_handler(
        &socket->manager, peer->managed, flowmq_socket_on_receive_slice, peer);
    if (status != SALTS_OK) return status;
    peer->owned_receive_active = 1u;
  }
  return cnet_receive(&socket->client, peer->connection, 1u);
}

static void flowmq_socket_on_receive_slice(
    void *user, cnet_connection connection, mem_slice_t slice,
    cnet_message_kind kind) {
  flowmq_socket_peer_t *peer = (flowmq_socket_peer_t *)user;
  flowmq_socket_t *socket = peer->owner;
#if defined(FLOWMQ_BATCH_PROBE)
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
  const uint64_t probe_started = cmeta_hrtime();
#endif
  ++socket->batch_probe.receive_callbacks;
  socket->batch_probe.receive_bytes += slice.length;
#endif
  int status = SALTS_OK;
  int pause_receive = 0;
  int consumed_owned = 0;
  size_t decoder_size = flowmq_stream_decoder_size(&peer->decoder);
  (void)connection;

  if (kind != CNET_MESSAGE_BYTES || slice.buffer == NULL ||
      slice.data == NULL || slice.length == 0u) {
    status = SALTS_EPROTO;
  }

  /*
   * Preserve the existing one-slice DATA transfer first. It is the cheapest
   * class and avoids touching the bounded stream when one CNet receive already
   * contains exactly one single-packet DATA frame.
   */
  if (status == SALTS_OK &&
      socket->transport == FLOWMQ_TRANSPORT_TCP &&
      !peer->commit_pending && decoder_size == 0u &&
      flowmq_owned_stream_size(&peer->owned_stream) == 0u &&
      flowmq_peer_state_handshake_has(
          &peer->state, FLOWMQ_PEER_HANDSHAKE_HELLO_RX) &&
      flowmq_peer_state_handshake_has(
          &peer->state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_RX)) {
    flowmq_protocol_frame_t frame = {0};
    size_t consumed = 0u;
    status = flowmq_protocol_decode_frame(
        (const char *)slice.data, slice.length, FLOWMQ_SOCKET_MAX_FRAME_SIZE,
        &frame, &consumed);
    if (status == SALTS_OK && consumed == slice.length &&
        frame.kind == FLOWMQ_PROTOCOL_FRAME_DATA &&
        frame.owned_payload == NULL) {
      status = frame.pattern == peer->remote_pattern
                   ? flowmq_pattern_data_direction_validate(
                         socket->pattern.pattern, &frame)
                   : SALTS_EPROTO;
      if (status == SALTS_OK && peer->heartbeat_active)
        flowmq_protocol_heartbeat_deadlines_on_receive(
            &peer->heartbeat, cmeta_hrtime());
      if (status == SALTS_OK)
        status = flowmq_socket_process_data_frame(
            peer, &frame, &slice, NULL, &pause_receive);
      if (slice.buffer == NULL) {
        consumed_owned = 1;
#if defined(FLOWMQ_BATCH_PROBE)
        ++socket->batch_probe.receive_fast_slices;
#endif
      }
    } else if (status == FLOWMQ_PROTOCOL_INCOMPLETE ||
               (status == SALTS_OK && consumed != slice.length) ||
               (status == SALTS_OK &&
                (frame.kind != FLOWMQ_PROTOCOL_FRAME_DATA ||
                 frame.owned_payload != NULL))) {
      status = SALTS_OK;
    }
    flowmq_protocol_frame_cleanup(&frame);
  }

  if (!consumed_owned && status == SALTS_OK) {
    decoder_size = flowmq_stream_decoder_size(&peer->decoder);
    if (decoder_size != 0u || peer->commit_pending ||
        !peer->owned_receive_active ||
        socket->transport != FLOWMQ_TRANSPORT_TCP) {
#if defined(FLOWMQ_BATCH_PROBE)
      ++socket->batch_probe.receive_decoder_slices;
#endif
      status = flowmq_stream_decoder_append(
          &peer->decoder, slice.data, slice.length);
      if (status == SALTS_OK)
        status = flowmq_socket_process_receive(peer);
      if (status == SALTS_ENOBUFS) {
        pause_receive = 1;
        status = SALTS_OK;
      }
    } else {
#if defined(FLOWMQ_BATCH_PROBE)
      ++socket->batch_probe.receive_stream_slices;
#endif
      status = flowmq_owned_stream_append_move(&peer->owned_stream, &slice);
      if (status == SALTS_ENOBUFS) {
        status = flowmq_socket_owned_stream_fallback(
            peer, &slice, &pause_receive);
      } else if (status == SALTS_OK) {
        status = flowmq_socket_process_owned_stream(peer, &pause_receive);
      }
    }
  }

  mem_slice_release(&slice);
  if (status == SALTS_OK && !pause_receive)
    status = flowmq_socket_rearm_receive(peer);
  if (status != SALTS_OK)
    flowmq_socket_peer_fail(peer);
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
  socket->batch_probe.receive_callback_ns += cmeta_hrtime() - probe_started;
#endif
}

static void flowmq_socket_on_receive(void *user, cnet_connection connection,
                                     const cnet_receive_view *view) {
  flowmq_socket_peer_t *peer = (flowmq_socket_peer_t *)user;
  flowmq_socket_t *socket = peer->owner;
  int status = flowmq_stream_decoder_append(&peer->decoder, view->data,
                                            view->size);
  if (status == SALTS_OK) status = flowmq_socket_process_receive(peer);
  if (status == SALTS_ENOBUFS) return;
  if (status == SALTS_OK) status = flowmq_socket_rearm_receive(peer);
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
  if (completed == FLOWMQ_PEER_WRITE_SETTINGS)
    flowmq_socket_peer_reconnect_protocol_ready(peer);
  if (completed == FLOWMQ_PEER_WRITE_DATA) {
    flowmq_socket_peer_record_completion(peer);
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
  size_t work;
  status = cnet_manager_advance(&socket->manager, FLOWMQ_SOCKET_PEER_CAPACITY, &work);
  if (status != SALTS_OK) return status;
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
  const cnet_manager_attachment attachment = {.observer = observer, .hold_context = true};
  status = flowmq_socket_pool_reserve(peer);
  if (status == SALTS_OK)
    status = cnet_manager_reserve(&socket->manager, &attachment, &peer->managed);
  if (status == SALTS_OK)
    status = cnet_manager_connect(&socket->manager, peer->managed, &options, &peer->connection);
  if (status != SALTS_OK) {
    flowmq_socket_peer_release(peer);
    return status;
  }
  endpoint->active = 1u;
  endpoint->retry_pending = 0u;
  return SALTS_OK;
}

static int flowmq_socket_reconnect_progress(flowmq_socket_t *socket) {
  const uint64_t now_ms = cmeta_monotonic_ms();
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
  if (transport == FLOWMQ_TRANSPORT_TCP)
    io.receive_buffer_bytes = FLOWMQ_SOCKET_CNET_PACKET_RECEIVE_BUFFER_BYTES;
  /* A messaging connection remains valid while either direction is idle. */
  timeouts.set_flags = FLOWMQ_TIMEOUT_SET_RECV;
  flowmq_timeouts_resolve(&timeouts, FLOWMQ_SOCKET_DEFAULT_TIMEOUT_MS);
  status = flowmq_cnet_client_config(
      &io, &timeouts, transport, FLOWMQ_SOCKET_PEER_CAPACITY,
      socket->max_encoded_size, &config);
  if (status == SALTS_OK && socket->external_backend != NULL) {
    if (transport != FLOWMQ_TRANSPORT_TCP)
      status = SALTS_ENOTSUP;
    else
      status = cnet_client_init_external(
          &socket->client, &config, socket->external_backend);
  } else if (status == SALTS_OK) {
    status = cnet_client_init(&socket->client, &config);
  }
  if (status == SALTS_OK && transport == FLOWMQ_TRANSPORT_TCP) {
    cnet_stream_socket_options socket_options = CNET_STREAM_SOCKET_OPTIONS_INIT;
    socket_options.nodelay = 1;
    status =
        cnet_client_set_stream_socket_options(&socket->client, &socket_options);
  }
  if (status == SALTS_OK) {
    const cnet_manager_config manager_config = {sizeof(manager_config), CNET_MANAGER_VERSION,
        &socket->client, FLOWMQ_SOCKET_PEER_CAPACITY, FLOWMQ_SOCKET_PEER_CAPACITY};
    status = cnet_manager_init(&socket->manager, &manager_config);
  }
  if (status == SALTS_OK && socket->peer_pool_config.max_peers != 0u) {
    const cnet_pool_config pool_config = {
        sizeof(pool_config), CNET_CLIENT_POOL_VERSION, &socket->manager,
        socket->socket_id, socket->peer_pool_config.max_peers,
        socket->peer_pool_config.max_connecting, socket->peer_pool_config.max_peers};
    status = cnet_pool_init(&socket->peer_pool, &pool_config);
  }
  if (status != SALTS_OK) {
    if (socket->manager.impl != NULL) {
      const int destroy_status = cnet_manager_destroy(&socket->manager);
      if (destroy_status != SALTS_OK) return destroy_status;
    }
    if (socket->client.impl != NULL) {
      int stop_status;
      if (socket->external_backend != NULL)
        stop_status = cnet_client_stop_external(&socket->client);
      else
        stop_status =
            cnet_client_stop(&socket->client, FLOWMQ_SOCKET_SHUTDOWN_TIMEOUT_MS);
      if (stop_status == SALTS_OK || stop_status == SALTS_EALREADY) {
        const int destroy_status = cnet_client_destroy(&socket->client);
        if (destroy_status != SALTS_OK) return destroy_status;
      } else {
        return stop_status;
      }
    }
    return status;
  }
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
  if (status == SALTS_OK) {
    const cnet_const_buffer segment = {
        .data = encoded,
        .size = tstr_len(encoded)};
    status = flowmq_socket_send_segments_buffered(
        socket, peer->connection, &segment, 1u, tstr_len(encoded));
  }
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
  if (ctx->socket_count != 0u || ctx->owner_count != 0u) return SALTS_EBUSY;
  free(ctx);
  return SALTS_OK;
}

int flowmq_ctx_internal_owner_acquire(flowmq_ctx_t *ctx) {
  if (ctx == NULL || ctx->owner_count == SIZE_MAX) return SALTS_EINVAL;
  ++ctx->owner_count;
  return SALTS_OK;
}

int flowmq_ctx_internal_owner_release(flowmq_ctx_t *ctx) {
  if (ctx == NULL || ctx->owner_count == 0u) return SALTS_EINVAL;
  --ctx->owner_count;
  return SALTS_OK;
}

flowmq_socket_t *flowmq_socket(flowmq_ctx_t *ctx, int type) {
  flowmq_protocol_pattern_t pattern;
  flowmq_socket_t *socket;
  size_t encoded_limit = 0u;
  if (ctx == NULL || ctx->next_socket_id == UINT64_MAX ||
      flowmq_socket_pattern(type, &pattern) != SALTS_OK) return NULL;
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
  socket->socket_id = ctx->next_socket_id;
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

int flowmq_socket_set_peer_pool(
    flowmq_socket_t *socket, const flowmq_peer_pool_config_t *config) {
  if (socket == NULL || config == NULL || config->size != sizeof(*config) ||
      config->version != FLOWMQ_PEER_POOL_VERSION ||
      config->max_peers == 0u || config->max_peers > FLOWMQ_SOCKET_PEER_CAPACITY ||
      config->max_connecting == 0u || config->max_connecting > config->max_peers)
    return SALTS_EINVAL;
  if (socket->runtime_initialized) return SALTS_EBUSY;
  socket->peer_pool_config = *config;
  return SALTS_OK;
}

int flowmq_socket_get_peer_pool(
    flowmq_socket_t *socket, flowmq_peer_pool_snapshot_t *snapshot) {
  cnet_pool_snapshot state;
  int status;
  if (socket == NULL || snapshot == NULL || snapshot->size != sizeof(*snapshot))
    return SALTS_EINVAL;
  *snapshot = (flowmq_peer_pool_snapshot_t)FLOWMQ_PEER_POOL_SNAPSHOT_INIT;
  snapshot->enabled = socket->peer_pool_config.max_peers != 0u;
  snapshot->max_peers = socket->peer_pool_config.max_peers;
  snapshot->max_connecting = socket->peer_pool_config.max_connecting;
  if (socket->peer_pool.impl == NULL) return SALTS_OK;
  status = cnet_pool_get_snapshot(&socket->peer_pool, &state);
  if (status != SALTS_OK) return status;
  snapshot->connecting = state.connecting;
  snapshot->ready = state.ready;
  snapshot->draining = state.draining;
  snapshot->terminal_waiting_for_leases = state.terminal_waiting_for_leases;
  snapshot->physical_in_use = state.physical_in_use;
  snapshot->active_leases = state.active_leases;
  snapshot->sealed = state.sealed;
  snapshot->drained = state.drained;
  return SALTS_OK;
}

int flowmq_close(flowmq_socket_t *socket) {
  int status;
  if (socket == NULL || socket->ctx == NULL || socket->ctx->socket_count == 0u)
    return SALTS_EINVAL;
  if (socket->external_owner != NULL) return SALTS_EBUSY;
  if (socket->peer_pool.impl != NULL) {
    status = cnet_pool_seal(&socket->peer_pool);
    if (status != SALTS_OK) return status;
  }
  if (socket->listener_initialized) {
    status = cnet_listener_close(&socket->listener);
    if (status != SALTS_OK && status != SALTS_EALREADY) return status;
    status = cnet_listener_destroy(&socket->listener);
    if (status != SALTS_OK) return status;
    socket->listener_initialized = 0u;
  }
  if (socket->runtime_initialized) {
    if (socket->external_backend != NULL) {
      if (!socket->external_stopped) return SALTS_EBUSY;
    } else {
      status = cnet_client_stop(&socket->client,
                                FLOWMQ_SOCKET_SHUTDOWN_TIMEOUT_MS);
      if (status != SALTS_OK) return status;
    }
    if (socket->manager.impl != NULL) {
      /* All transport callbacks have ended. Discarding this socket also ends
       * queued-message holds; normal peer retirement keeps them until consumed. */
      for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i)
        flowmq_socket_peer_release(&socket->peers[i]);
      size_t work;
      status = cnet_manager_advance(&socket->manager, FLOWMQ_SOCKET_PEER_CAPACITY, &work);
      if (status != SALTS_OK) return status;
      if (socket->peer_pool.impl != NULL) {
        status = cnet_pool_destroy(&socket->peer_pool);
        if (status != SALTS_OK) return status;
      }
      status = cnet_manager_destroy(&socket->manager);
      if (status != SALTS_OK) return status;
    }
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
    flowmq_socket_message_storage_release(&socket->inbound[index]);
  }
  flowmq_socket_release_send_staged(socket);
  flowmq_socket_release_retained_staged(socket);
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
  cnet_listener_options listener_options = CNET_LISTENER_OPTIONS_INIT;
  uint16_t bound_port = 0u;
  int bracket;
  int written;
  int status;
  if (socket == NULL || socket->ctx == NULL) return SALTS_EINVAL;
  if (socket->external_backend != NULL) return SALTS_ENOTSUP;
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
  listener_options.reuse_port = socket->reuse_port;
  status = cnet_listener_init_ex(
      &socket->listener, &config, &listener_options);
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
      reconnect_max_ms, cmeta_hrtime() ^ (uint64_t)(endpoint_index + 1u));
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

  case FLOWMQ_REUSE_PORT: {
    int value_int;
    memcpy(&value_int, value, sizeof(value_int));
    socket->reuse_port = value_int;
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

int flowmq_router_peer_status(
    const flowmq_socket_t *socket, const void *identity, size_t identity_size,
    flowmq_router_peer_status_t *status) {
  size_t caller_size;
  if (socket == NULL || socket->ctx == NULL || status == NULL ||
      identity == NULL || identity_size == 0u ||
      identity_size > FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE)
    return SALTS_EINVAL;
  caller_size = status->size;
  if (caller_size < sizeof(*status)) return SALTS_EINVAL;
  if (socket->pattern.desc == NULL ||
      socket->pattern.desc->routing_class != FLOWMQ_PATTERN_ROUTE_IDENTITY)
    return SALTS_ENOTSUP;

  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
    const flowmq_socket_peer_t *peer = &socket->peers[i];
    flowmq_router_peer_status_t snapshot = FLOWMQ_ROUTER_PEER_STATUS_INIT;
    if (!flowmq_peer_state_is_used(&peer->state) ||
        flowmq_peer_state_is_retired(&peer->state) ||
        peer->identity_size != identity_size ||
        memcmp(peer->identity, identity, identity_size) != 0)
      continue;

    snapshot.admitted_messages = peer->admitted_messages;
    snapshot.admitted_bytes = peer->admitted_bytes;
    snapshot.completed_messages = peer->completed_messages;
    snapshot.completed_bytes = peer->completed_bytes;
    snapshot.rejected_messages = peer->rejected_messages;
    snapshot.rejected_bytes = peer->rejected_bytes;
    if (peer->flow_control.remote_initialized &&
        peer->flow_control.sent_data <= peer->flow_control.remote_max_data)
      snapshot.send_credit_bytes =
          peer->flow_control.remote_max_data - peer->flow_control.sent_data;
    snapshot.outstanding_messages = peer->outbound_messages;
    snapshot.outstanding_bytes = peer->outbound_bytes;
    snapshot.peak_outstanding_messages = peer->peak_outstanding_messages;
    snapshot.peak_outstanding_bytes = peer->peak_outstanding_bytes;
    snapshot.connected = flowmq_peer_state_is_connected(&peer->state);
    snapshot.ready = flowmq_socket_peer_ready(peer);

    /*
     * Only the current canonical prefix is written. A newer caller may pass a
     * larger struct; unknown tail bytes remain caller-owned.
     */
    memcpy(status, &snapshot, sizeof(snapshot));
    return SALTS_OK;
  }
  return SALTS_ENOENT;
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
    if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REP) {
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
    if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY) {
      flowmq_socket_peer_record_rejection(peer, message_size);
      flowmq_socket_cancel_send_route(socket);
    }
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
  if (socket->send_retained_count != 0u) return SALTS_ENOTSUP;
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
  if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REP &&
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

static int flowmq_socket_try_send_slice(flowmq_socket_t *socket,
                                          const mem_slice_t *slice,
                                          int flags) {
  flowmq_socket_retained_frame_t retained = {0};
  flowmq_protocol_frame_t frame;
  flowmq_socket_peer_t *peer = NULL;
  mem_slice_t aggregate[CNET_RETAINED_VECTOR_MAX] = {0};
  size_t payload_offset = 0u;
  size_t selected_peer_index;
  uint64_t selected_peer_generation;
  size_t size;
  size_t total_payload_size;
  size_t total_encoded_size;
  int starting_message;
  int message_end;
  int peer_busy = 0;
  int peer_saturated = 0;
  int status;

  if (socket == NULL || (flags & ~(FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE)) != 0)
    return SALTS_EINVAL;
  status = flowmq_socket_slice_validate(slice, &payload_offset);
  if (status != SALTS_OK) return status;
  (void)payload_offset;
  size = slice->length;

  if (socket->send_cancel_error != SALTS_OK) {
    status = socket->send_cancel_error;
    socket->send_cancel_error = SALTS_OK;
    return status;
  }
  status = flowmq_pattern_state_send_validate(&socket->pattern);
  if (status != SALTS_OK) return status;
  if (socket->send_staged_count != 0u) return SALTS_ENOTSUP;
  if (size > FLOWMQ_SOCKET_MAX_FRAME_SIZE || size > socket->send_hwm_bytes)
    return SALTS_EMSGSIZE;
  if (socket->runtime_initialized &&
      socket->transport != FLOWMQ_TRANSPORT_TCP &&
      socket->transport != FLOWMQ_TRANSPORT_TLS)
    return SALTS_ENOTSUP;

  starting_message = !socket->pattern.sending_multipart;
  message_end = (flags & FLOWMQ_SNDMORE) == 0;
  selected_peer_index = socket->send_peer_index;
  selected_peer_generation = socket->send_peer_generation;

  if (starting_message &&
      socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_IDENTITY) {
    /* ROUTER still requires its copied routing-id envelope first. */
    return SALTS_EINVAL;
  }

  if (socket->pattern.desc->routing_class == FLOWMQ_PATTERN_ROUTE_FANOUT) {
    uint64_t peer_generations[FLOWMQ_SOCKET_PEER_CAPACITY] = {0};
    uint32_t peer_mask =
        starting_message ? 0u : socket->publish_peer_mask;
    flowmq_socket_retained_publication_t *publication = NULL;

    if (starting_message) {
      for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
        flowmq_socket_peer_t *candidate = &socket->peers[i];
        if (flowmq_socket_peer_ready(candidate) &&
            flowmq_subscription_set_match(
                &candidate->subscriptions,
                (vstr){.data = slice->data, .len = size})) {
          peer_mask |= UINT32_C(1) << i;
          peer_generations[i] =
              candidate->flow_control.local_generation;
        }
      }
    } else {
      memcpy(peer_generations, socket->publish_peer_generations,
             sizeof(peer_generations));
    }

    /*
     * Preserve ordinary PUB/XPUB mute/drop semantics: an empty first-part
     * snapshot never needs to allocate or retain publication storage.
     */
    if (peer_mask == 0u) {
      if (starting_message) {
        socket->publish_peer_mask = 0u;
        memset(socket->publish_peer_generations, 0,
               sizeof(socket->publish_peer_generations));
      }
      if (message_end) {
        flowmq_socket_release_retained_staged(socket);
        ++socket->next_message_id;
      }
      flowmq_pattern_state_send_commit(&socket->pattern, !message_end);
      return SALTS_OK;
    }

    frame = (flowmq_protocol_frame_t){
        .kind = FLOWMQ_PROTOCOL_FRAME_DATA,
        .pattern = socket->pattern.pattern,
        .message_id = socket->next_message_id + 1u,
        .more = !message_end,
        .payload = {.data = slice->data, .len = size}};
    status =
        flowmq_socket_prepare_retained_frame(socket, &frame, slice, &retained);
    if (status != SALTS_OK) return status;

    if (!message_end) {
      status = flowmq_socket_stage_retained_frame(socket, &retained);
      flowmq_socket_retained_frame_release(&retained);
      if (status != SALTS_OK) return status;
      if (starting_message) {
        socket->publish_peer_mask = peer_mask;
        memcpy(socket->publish_peer_generations, peer_generations,
               sizeof(peer_generations));
      }
      flowmq_pattern_state_send_commit(&socket->pattern, 1);
      return SALTS_OK;
    }

    if (socket->send_retained_payload_bytes >
            socket->send_hwm_bytes - size ||
        retained.slice_count >
            CNET_RETAINED_VECTOR_MAX - socket->send_retained_count ||
        retained.encoded_size >
            socket->max_encoded_size - socket->send_retained_encoded_bytes) {
      flowmq_socket_retained_frame_release(&retained);
      return SALTS_EMSGSIZE;
    }
    total_payload_size = socket->send_retained_payload_bytes + size;
    total_encoded_size =
        socket->send_retained_encoded_bytes + retained.encoded_size;
    if (total_encoded_size > socket->max_encoded_size) {
      flowmq_socket_retained_frame_release(&retained);
      return SALTS_EMSGSIZE;
    }

    /*
     * PUB/XPUB mute/drop semantics select the first-part subscription snapshot,
     * then omit peers that are no longer the same live generation or cannot
     * accept the complete logical message at final commit.
     */
    for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
      uint32_t bit = UINT32_C(1) << i;
      if ((peer_mask & bit) == 0u) continue;
      if (!flowmq_socket_peer_generation_ready(
              &socket->peers[i], peer_generations[i]) ||
          !flowmq_socket_peer_can_queue_retained_message(
              &socket->peers[i], socket, total_payload_size, size))
        peer_mask &= ~bit;
    }

    if (peer_mask != 0u) {
      status = flowmq_socket_retained_publication_create(
          socket, &retained, &publication);
      if (status != SALTS_OK) {
        flowmq_socket_retained_frame_release(&retained);
        return status;
      }
      status =
          flowmq_socket_commit_retained_fanout(socket, publication, peer_mask);
      flowmq_socket_retained_publication_release(publication);
      if (status != SALTS_OK) {
        flowmq_socket_retained_frame_release(&retained);
        return status;
      }
    }

    flowmq_socket_release_retained_staged(socket);
    flowmq_socket_retained_frame_release(&retained);
    socket->publish_peer_mask = 0u;
    memset(socket->publish_peer_generations, 0,
           sizeof(socket->publish_peer_generations));
    ++socket->next_message_id;
    flowmq_pattern_state_send_commit(&socket->pattern, 0);
    return SALTS_OK;
  }

  if (!starting_message) {
    if (!socket->send_peer_active ||
        socket->send_peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY) {
      flowmq_socket_cancel_send_route(socket);
      return SALTS_ENOTCONN;
    }
    peer = &socket->peers[socket->send_peer_index];
    if (!flowmq_socket_peer_generation_ready(
            peer, socket->send_peer_generation)) {
      flowmq_socket_cancel_send_route(socket);
      return SALTS_ENOTCONN;
    }
    selected_peer_index = socket->send_peer_index;
    selected_peer_generation = socket->send_peer_generation;
  } else if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REP) {
    if (!socket->reply_peer_valid ||
        socket->reply_peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY)
      return SALTS_EBUSY;
    peer = &socket->peers[socket->reply_peer_index];
    if (!flowmq_socket_peer_generation_ready(
            peer, socket->reply_peer_generation))
      return SALTS_EBUSY;
    selected_peer_index = socket->reply_peer_index;
    selected_peer_generation = socket->reply_peer_generation;
  } else {
    for (size_t offset = 0u; offset < FLOWMQ_SOCKET_PEER_CAPACITY; ++offset) {
      size_t index =
          (socket->peer_cursor + offset) % FLOWMQ_SOCKET_PEER_CAPACITY;
      flowmq_socket_peer_t *candidate = &socket->peers[index];
      if (!flowmq_socket_peer_ready(candidate)) continue;

      if (!message_end) {
        peer = candidate;
        selected_peer_index = index;
        selected_peer_generation =
            candidate->flow_control.local_generation;
        break;
      }

      if (!flowmq_socket_peer_can_admit(candidate, size, 1)) {
        peer_saturated = 1;
        continue;
      }
      if (!flowmq_peer_state_write_idle(&candidate->state) ||
          candidate->outbound_count != 0u) {
        peer_busy = 1;
        continue;
      }
      peer = candidate;
      selected_peer_index = index;
      selected_peer_generation =
          candidate->flow_control.local_generation;
      break;
    }
    if (peer == NULL)
      return peer_saturated ? SALTS_ENOBUFS
                            : (peer_busy ? SALTS_EBUSY : SALTS_EBUSY);
  }

  if (size > peer->flow_control.remote_max_frame_size) return SALTS_EMSGSIZE;
  if (socket->send_retained_payload_bytes >
          socket->send_hwm_bytes - size)
    return SALTS_EMSGSIZE;
  total_payload_size = socket->send_retained_payload_bytes + size;

  frame = (flowmq_protocol_frame_t){
      .kind = FLOWMQ_PROTOCOL_FRAME_DATA,
      .pattern = socket->pattern.pattern,
      .message_id = socket->next_message_id + 1u,
      .more = !message_end,
      .payload = {.data = slice->data, .len = size}};
  status =
      flowmq_socket_prepare_retained_frame(socket, &frame, slice, &retained);
  if (status != SALTS_OK) return status;

  if (!message_end) {
    status = flowmq_socket_stage_retained_frame(socket, &retained);
    flowmq_socket_retained_frame_release(&retained);
    if (status != SALTS_OK) return status;

    if (starting_message) {
      socket->send_peer_index = selected_peer_index;
      socket->send_peer_generation = selected_peer_generation;
      socket->send_peer_active = 1u;
      if (socket->pattern.desc->fsm_class != FLOWMQ_PATTERN_FSM_REP)
        socket->peer_cursor =
            (selected_peer_index + 1u) % FLOWMQ_SOCKET_PEER_CAPACITY;
    }
    flowmq_pattern_state_send_commit(&socket->pattern, 1);
    return SALTS_OK;
  }

  if (retained.slice_count >
          CNET_RETAINED_VECTOR_MAX - socket->send_retained_count ||
      retained.encoded_size >
          socket->max_encoded_size - socket->send_retained_encoded_bytes) {
    flowmq_socket_retained_frame_release(&retained);
    return SALTS_EMSGSIZE;
  }
  total_encoded_size =
      socket->send_retained_encoded_bytes + retained.encoded_size;
  if (total_encoded_size > socket->max_encoded_size) {
    flowmq_socket_retained_frame_release(&retained);
    return SALTS_EMSGSIZE;
  }

  for (size_t i = 0u; i < socket->send_retained_count; ++i)
    aggregate[i] = socket->send_retained_staged[i];
  for (size_t i = 0u; i < retained.slice_count; ++i)
    aggregate[socket->send_retained_count + i] = retained.slices[i];

  if (socket->send_retained_count == 0u) {
    status = flowmq_socket_peer_admit_retained(peer, &retained);
  } else {
    status = flowmq_socket_peer_admit_retained_message(
        peer, aggregate, socket->send_retained_count + retained.slice_count,
        total_payload_size);
  }
  if (status != SALTS_OK) {
    flowmq_socket_retained_frame_release(&retained);
    return status;
  }

  flowmq_socket_release_retained_staged(socket);
  flowmq_socket_retained_frame_release(&retained);
  ++socket->next_message_id;

  if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ) {
    socket->request_peer_index = selected_peer_index;
    socket->request_peer_generation = peer->flow_control.local_generation;
    socket->request_peer_valid = 1u;
    socket->recv_cancel_error = SALTS_OK;
  }
  if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REP) {
    socket->reply_peer_valid = 0u;
    socket->reply_peer_generation = 0u;
  }
  socket->send_peer_active = 0u;
  socket->send_peer_generation = 0u;
  flowmq_pattern_state_send_commit(&socket->pattern, 0);
  return SALTS_OK;
}


static int flowmq_socket_message_validate(
    const flowmq_socket_message_t *message) {
  size_t total = 0u;
  if (message == NULL) return SALTS_EINVAL;
  if ((message->buffer == NULL) == (message->segmented == NULL))
    return SALTS_EPROTO;

  if (message->buffer != NULL) {
    size_t used = mem_buffer_used(message->buffer);
    if (message->offset > used || message->size > used - message->offset)
      return SALTS_EPROTO;
    return SALTS_OK;
  }

  if (message->segmented->segment_count == 0u ||
      message->segmented->segment_count >
          FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY)
    return SALTS_EPROTO;
  for (size_t i = 0u; i < message->segmented->segment_count; ++i) {
    const mem_slice_t *slice = &message->segmented->segments[i];
    if (!flowmq_owned_stream_slice_valid(slice) ||
        slice->length > message->size - total)
      return SALTS_EPROTO;
    total += slice->length;
  }
  return total == message->size ? SALTS_OK : SALTS_EPROTO;
}

static int flowmq_socket_message_copy_payload(
    const flowmq_socket_message_t *message, void *destination) {
  unsigned char *out = (unsigned char *)destination;
  size_t copied = 0u;
  int status = flowmq_socket_message_validate(message);
  if (status != SALTS_OK) return status;
  if (message->size == 0u) return SALTS_OK;
  if (destination == NULL) return SALTS_EINVAL;

  if (message->buffer != NULL) {
    memcpy(out,
           (const unsigned char *)mem_buffer_const_data(message->buffer) +
               message->offset,
           message->size);
    return SALTS_OK;
  }

  for (size_t i = 0u; i < message->segmented->segment_count; ++i) {
    const mem_slice_t *slice = &message->segmented->segments[i];
    memcpy(out + copied, slice->data, slice->length);
    copied += slice->length;
  }
  return copied == message->size ? SALTS_OK : SALTS_EPROTO;
}

static int flowmq_socket_message_coalesce(
    flowmq_socket_t *socket, const flowmq_socket_message_t *message,
    mem_buffer_t **out_buffer) {
  mem_buffer_t *buffer;
  int status;
  if (socket == NULL || message == NULL || out_buffer == NULL ||
      *out_buffer != NULL || message->segmented == NULL ||
      message->segmented->segment_count <= 1u)
    return SALTS_EINVAL;
  status = flowmq_socket_message_validate(message);
  if (status != SALTS_OK) return status;

  buffer = mem_get_buffer(&socket->message_pool,
                          message->size == 0u ? 1u : message->size);
  if (buffer == NULL) return SALTS_ENOMEM;
  status = flowmq_socket_message_copy_payload(
      message, mem_buffer_data(buffer));
  if (status != SALTS_OK) {
    mem_buffer_release(buffer);
    return status;
  }
  mem_set_used(buffer, message->size);
  *out_buffer = buffer;
  return SALTS_OK;
}

static int flowmq_socket_receive_lookup(
    flowmq_socket_t *socket,
    flowmq_socket_message_t **out_message,
    flowmq_socket_peer_t **out_peer) {
  flowmq_socket_message_t *message;
  flowmq_socket_peer_t *peer;
  int status;
  if (socket == NULL || out_message == NULL || out_peer == NULL)
    return SALTS_EINVAL;
  *out_message = NULL;
  *out_peer = NULL;

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
  if (socket->inbound_bytes < message->size ||
      (!message->more && socket->inbound_messages == 0u))
    return SALTS_EPROTO;
  if (message->peer_index >= FLOWMQ_SOCKET_PEER_CAPACITY)
    return SALTS_EPROTO;
  status = flowmq_socket_message_validate(message);
  if (status != SALTS_OK) return status;

  peer = &socket->peers[message->peer_index];
  if (!flowmq_peer_state_is_used(&peer->state) ||
      message->peer_generation == 0u ||
      message->peer_generation != peer->flow_control.local_generation ||
      peer->queued_parts == 0u)
    return SALTS_EPROTO;

  *out_message = message;
  *out_peer = peer;
  return SALTS_OK;
}

static int flowmq_socket_receive_consume_credit(
    flowmq_socket_message_t *message, flowmq_socket_peer_t *peer) {
  if (message == NULL || peer == NULL) return SALTS_EINVAL;
  if (message->credit_size != 0u &&
      !flowmq_peer_state_is_retired(&peer->state)) {
    uint64_t consumed_at_ns = peer->flow_control.update_pending
                                  ? 0u
                                  : cmeta_hrtime();
    return flowmq_flow_control_consume(
        &peer->flow_control, message->credit_size, consumed_at_ns);
  }
  return SALTS_OK;
}

static void flowmq_socket_receive_commit_pattern(
    flowmq_socket_t *socket, flowmq_socket_message_t *message,
    flowmq_socket_peer_t *peer) {
  const int message_more = message->more;
  socket->last_rcvmore = message_more != 0;
  flowmq_pattern_state_receive_commit(&socket->pattern, message_more);

  if (!message_more &&
      socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REP) {
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
  if (!message_more &&
      socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ) {
    socket->request_peer_valid = 0u;
    socket->request_peer_generation = 0u;
  }
}

static void flowmq_socket_receive_finish(
    flowmq_socket_t *socket, flowmq_socket_message_t *message,
    flowmq_socket_peer_t *peer) {
  const size_t size = message->size;
  const int message_more = message->more;
  socket->inbound_bytes -= size;
  if (!message_more) --socket->inbound_messages;
  --peer->queued_parts;
  memset(message, 0, sizeof(*message));
  socket->inbound_read =
      (socket->inbound_read + 1u) % FLOWMQ_SOCKET_INBOUND_CAPACITY;
  --socket->inbound_count;
  if (flowmq_peer_state_is_retired(&peer->state) &&
      peer->queued_parts == 0u)
    flowmq_socket_peer_release(peer);
}

static int flowmq_socket_try_recv(flowmq_socket_t *socket, void *data,
                                  size_t capacity, size_t *received,
                                  int flags) {
  flowmq_socket_message_t *message;
  flowmq_socket_peer_t *peer;
  int status;
  if (received != NULL) *received = 0u;
  if (socket == NULL || received == NULL ||
      (data == NULL && capacity != 0u) ||
      (flags & ~FLOWMQ_DONTWAIT) != 0)
    return SALTS_EINVAL;

  status = flowmq_socket_receive_lookup(socket, &message, &peer);
  if (status != SALTS_OK) return status;
  *received = message->size;
  if (capacity < message->size) return SALTS_EMSGSIZE;

  status = flowmq_socket_receive_consume_credit(message, peer);
  if (status != SALTS_OK) return status;
  status = flowmq_socket_message_copy_payload(message, data);
  if (status != SALTS_OK) return status;

  flowmq_socket_receive_commit_pattern(socket, message, peer);
  flowmq_socket_message_storage_release(message);
  flowmq_socket_receive_finish(socket, message, peer);
  return SALTS_OK;
}

static int flowmq_socket_try_recv_slice(flowmq_socket_t *socket,
                                        mem_slice_t *out, int flags) {
  flowmq_socket_message_t *message;
  flowmq_socket_peer_t *peer;
  mem_buffer_t *coalesced = NULL;
  int status;
  if (socket == NULL || out == NULL || (flags & ~FLOWMQ_DONTWAIT) != 0)
    return SALTS_EINVAL;
  if (out->buffer != NULL || out->data != NULL || out->length != 0u)
    return SALTS_EINVAL;

  status = flowmq_socket_receive_lookup(socket, &message, &peer);
  if (status != SALTS_OK) return status;

  /*
   * Preserve the legacy one-contiguous-slice contract. Build the only required
   * coalesce before consuming credit/FSM state so allocation failure leaves
   * the queued message completely unchanged.
   */
  if (message->segmented != NULL &&
      message->segmented->segment_count > 1u) {
    status = flowmq_socket_message_coalesce(socket, message, &coalesced);
    if (status != SALTS_OK) return status;
  }

  status = flowmq_socket_receive_consume_credit(message, peer);
  if (status != SALTS_OK) {
    mem_buffer_release(coalesced);
    return status;
  }
  flowmq_socket_receive_commit_pattern(socket, message, peer);

  if (message->buffer != NULL) {
    out->data =
        mem_buffer_data(message->buffer) + message->offset;
    out->length = message->size;
    out->buffer = message->buffer;
    message->buffer = NULL;
  } else if (message->segmented->segment_count == 1u) {
    *out = message->segmented->segments[0];
    memset(&message->segmented->segments[0], 0,
           sizeof(message->segmented->segments[0]));
    free(message->segmented);
    message->segmented = NULL;
  } else {
    out->data = mem_buffer_data(coalesced);
    out->length = message->size;
    out->buffer = coalesced;
    coalesced = NULL;
    flowmq_socket_message_storage_release(message);
  }

  flowmq_socket_receive_finish(socket, message, peer);
  return SALTS_OK;
}

static int flowmq_socket_try_recv_slicev(
    flowmq_socket_t *socket, mem_slice_t *segments, size_t capacity,
    size_t *count, int flags) {
  flowmq_socket_message_t *message;
  flowmq_socket_peer_t *peer;
  size_t required;
  int status;

  if (count != NULL) *count = 0u;
  if (socket == NULL || count == NULL ||
      (segments == NULL && capacity != 0u) ||
      (flags & ~FLOWMQ_DONTWAIT) != 0)
    return SALTS_EINVAL;
  for (size_t i = 0u; i < capacity; ++i) {
    if (segments[i].buffer != NULL || segments[i].data != NULL ||
        segments[i].length != 0u)
      return SALTS_EINVAL;
  }

  status = flowmq_socket_receive_lookup(socket, &message, &peer);
  if (status != SALTS_OK) return status;

  required = message->buffer != NULL
                 ? 1u
                 : message->segmented->segment_count;
  if (capacity < required) {
    *count = required;
    return SALTS_ENOBUFS;
  }
  if (required != 0u && segments == NULL)
    return SALTS_EINVAL;

  status = flowmq_socket_receive_consume_credit(message, peer);
  if (status != SALTS_OK) return status;
  flowmq_socket_receive_commit_pattern(socket, message, peer);

  if (message->buffer != NULL) {
    segments[0].data =
        mem_buffer_data(message->buffer) + message->offset;
    segments[0].length = message->size;
    segments[0].buffer = message->buffer;
    message->buffer = NULL;
  } else {
    for (size_t i = 0u; i < required; ++i) {
      segments[i] = message->segmented->segments[i];
      memset(&message->segmented->segments[i], 0,
             sizeof(message->segmented->segments[i]));
    }
    free(message->segmented);
    message->segmented = NULL;
  }

  *count = required;
  flowmq_socket_receive_finish(socket, message, peer);
  return SALTS_OK;
}

static int flowmq_socket_resume_receive(flowmq_socket_peer_t *peer) {
  int pause_receive = 0;
  int status;
  if (!peer->commit_pending) return SALTS_OK;
  status = flowmq_socket_commit_staged(peer);
  if (status == SALTS_ENOBUFS) return SALTS_OK;
  if (status != SALTS_OK) return status;
  peer->commit_pending = 0u;

  status = flowmq_socket_process_receive(peer);
  if (status == SALTS_ENOBUFS) return SALTS_OK;
  if (status != SALTS_OK) return status;
  if (flowmq_stream_decoder_size(&peer->decoder) == 0u &&
      flowmq_owned_stream_size(&peer->owned_stream) != 0u) {
    status = flowmq_socket_process_owned_stream(peer, &pause_receive);
    if (status != SALTS_OK) return status;
    if (pause_receive) return SALTS_OK;
  }
  return flowmq_socket_rearm_receive(peer);
}

static int flowmq_socket_listener_progress(flowmq_socket_t *socket) {
  int status;
  if (!socket->listener_initialized) return SALTS_OK;
  size_t work;
  status = cnet_manager_advance(&socket->manager, FLOWMQ_SOCKET_PEER_CAPACITY, &work);
  if (status != SALTS_OK) return status;
  if (socket->peer_pool.impl != NULL) {
    cnet_pool_snapshot pool;
    status = cnet_pool_get_snapshot(&socket->peer_pool, &pool);
    if (status != SALTS_OK) return status;
    /* Leave excess connections in the listener backlog without allocating a
     * temporary peer each progress turn. reserve_connecting below remains the
     * authoritative admission; this snapshot never grants a protocol slot. */
    if (pool.sealed || pool.physical_in_use == pool.max_connections ||
        pool.connecting == pool.max_connecting) return SALTS_OK;
  }
  {
    int ready = 0;
    status = cnet_listener_wait(&socket->listener, 0u, &ready);
    if (status != SALTS_OK) return status;
    if (!ready) return SALTS_OK;
  }
  {
    flowmq_socket_peer_t *peer = flowmq_socket_peer_acquire(socket);
    cnet_observer observer;
    if (peer == NULL) return SALTS_ENOBUFS;
    status = flowmq_socket_pool_reserve(peer);
    if (status != SALTS_OK) {
      flowmq_socket_peer_release(peer);
      return status == SALTS_ENOBUFS ? SALTS_OK : status;
    }
    observer = flowmq_socket_observer(peer);
    cnet_accepted_stream accepted = CNET_ACCEPTED_STREAM_INIT;
    status = cnet_listener_accept_detached(&socket->listener, &accepted);
    if (status == SALTS_OK) {
      const cnet_manager_attachment attachment = {.observer = observer, .hold_context = true};
      status = cnet_manager_reserve(&socket->manager, &attachment, &peer->managed);
      if (status == SALTS_OK)
        status = cnet_manager_adopt(&socket->manager, peer->managed, &accepted,
            socket->transport == FLOWMQ_TRANSPORT_TLS ? &socket->tls_server : NULL,
            &peer->connection);
      else
        (void)cnet_accepted_stream_close(&accepted);
    }
    if (status != SALTS_OK) {
      flowmq_socket_peer_release(peer);
      if (status != SALTS_ETIMEDOUT) return status;
    }
  }
  return SALTS_OK;
}

static int flowmq_socket_progress_local(flowmq_socket_t *socket) {
  size_t work;
  int status = cnet_manager_advance(&socket->manager, FLOWMQ_SOCKET_PEER_CAPACITY, &work);
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
  if (socket->async_error != SALTS_OK) return socket->async_error;
  return SALTS_OK;
}

static int flowmq_socket_drive(flowmq_socket_t *socket, uint32_t timeout_ms,
                               size_t *events) {
  size_t client_events = 0u;
  int status;
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
  uint64_t probe_started;
#endif
  if (events != NULL) *events = 0u;
  if (socket == NULL || !socket->runtime_initialized) return SALTS_OK;
  if (socket->external_backend != NULL) return SALTS_ENOTSUP;
#if defined(FLOWMQ_BATCH_PROBE)
  ++socket->batch_probe.drive_calls;
#endif
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
  probe_started = cmeta_hrtime();
#endif
  status = flowmq_socket_listener_progress(socket);
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
  socket->batch_probe.listener_ns += cmeta_hrtime() - probe_started;
#endif
  if (status != SALTS_OK) return status;
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
  probe_started = cmeta_hrtime();
#endif
  status = cnet_client_poll(&socket->client, timeout_ms, &client_events);
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
  socket->batch_probe.client_poll_ns += cmeta_hrtime() - probe_started;
#endif
  if (status != SALTS_OK) return status;
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
  probe_started = cmeta_hrtime();
#endif
  status = flowmq_socket_progress_local(socket);
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
  socket->batch_probe.local_progress_ns += cmeta_hrtime() - probe_started;
#endif
  if (status != SALTS_OK) return status;
  if (events != NULL) *events = client_events;
  return SALTS_OK;
}

static int flowmq_socket_attach_external_common(
    flowmq_socket_t *socket, native_io_backend *backend,
    const void *owner_token) {
  native_io_backend_config config;
  if (socket == NULL || backend == NULL || socket->ctx == NULL)
    return SALTS_EINVAL;
  if (socket->runtime_initialized || socket->listener_initialized)
    return SALTS_EBUSY;
  if (socket->external_backend != NULL || socket->external_owner != NULL)
    return SALTS_EALREADY;
  if (!native_io_backend_get_config(backend, &config)) return SALTS_EINVAL;
  if (config.kind != flowmq_cnet_backend()) return SALTS_ENOTSUP;
  socket->external_backend = backend;
  socket->external_owner = owner_token;
  socket->external_stopping = 0u;
  socket->external_stopped = 0u;
  return SALTS_OK;
}

int flowmq_socket_internal_attach_external_backend(
    flowmq_socket_t *socket, native_io_backend *backend) {
  return flowmq_socket_attach_external_common(socket, backend, NULL);
}

int flowmq_socket_internal_attach_owner_backend(
    flowmq_socket_t *socket, native_io_backend *backend,
    const void *owner_token) {
  if (owner_token == NULL) return SALTS_EINVAL;
  return flowmq_socket_attach_external_common(socket, backend, owner_token);
}

int flowmq_socket_internal_owned_by(
    const flowmq_socket_t *socket, const void *owner_token) {
  return socket != NULL && owner_token != NULL &&
         socket->external_owner == owner_token;
}

int flowmq_socket_internal_runtime_active(const flowmq_socket_t *socket) {
  return socket != NULL && socket->external_backend != NULL &&
         socket->runtime_initialized && !socket->external_stopped;
}

int flowmq_socket_internal_owner_backend_config(
    size_t socket_capacity, native_io_backend_config *config) {
  flowmq_io_config_t io;
  const size_t endpoints_per_socket = FLOWMQ_SOCKET_PEER_CAPACITY * 2u;
  if (config == NULL || socket_capacity == 0u) return SALTS_EINVAL;
  flowmq_io_config_init(&io);
  io.command_capacity = FLOWMQ_SOCKET_CNET_COMMAND_CAPACITY;
  if (socket_capacity > SIZE_MAX / endpoints_per_socket ||
      socket_capacity > SIZE_MAX / io.request_capacity)
    return SALTS_ERANGE;
  *config = (native_io_backend_config){
      .kind = flowmq_cnet_backend(),
      .endpoint_capacity = socket_capacity * endpoints_per_socket,
      .request_capacity = socket_capacity * io.request_capacity,
      .completion_batch_capacity = socket_capacity * io.request_capacity};
  return native_io_backend_kind_supported(config->kind)
             ? SALTS_OK
             : SALTS_ENOTSUP;
}

int flowmq_socket_internal_advance_external(
    flowmq_socket_t *socket, size_t *events) {
  size_t client_events = 0u;
  int status;
  if (events != NULL) *events = 0u;
  if (socket == NULL || socket->external_backend == NULL ||
      !socket->runtime_initialized)
    return SALTS_EINVAL;
  status = cnet_client_advance_external(&socket->client, &client_events);
  if (status != SALTS_OK) return status;
  if (events != NULL) *events = client_events;
  return SALTS_OK;
}

int flowmq_socket_internal_progress_local(flowmq_socket_t *socket) {
  if (socket == NULL || socket->external_backend == NULL ||
      !socket->runtime_initialized)
    return SALTS_EINVAL;
  if (socket->external_stopping) return SALTS_OK;
  return flowmq_socket_progress_local(socket);
}

static uint32_t flowmq_socket_wait_until_ns(
    uint64_t now_ns, uint64_t deadline_ns, uint32_t cap_ms) {
  uint64_t delta_ns;
  uint64_t delta_ms;
  if (deadline_ns <= now_ns) return 0u;
  delta_ns = deadline_ns - now_ns;
  delta_ms = (delta_ns + UINT64_C(999999)) / UINT64_C(1000000);
  return delta_ms < cap_ms ? (uint32_t)delta_ms : cap_ms;
}

static uint32_t flowmq_socket_wait_until_ms(
    uint64_t now_ms, uint64_t deadline_ms, uint32_t cap_ms) {
  uint64_t delta_ms;
  if (deadline_ms <= now_ms) return 0u;
  delta_ms = deadline_ms - now_ms;
  return delta_ms < cap_ms ? (uint32_t)delta_ms : cap_ms;
}

static int flowmq_socket_subscription_sync_pending(
    const flowmq_socket_t *socket, const flowmq_socket_peer_t *peer) {
  const flowmq_subscription_t *subscription;
  vstr topic;
  if (socket->pattern.desc->subscription_class !=
          FLOWMQ_PATTERN_SUB_SUBSCRIBER ||
      !flowmq_socket_peer_ready(peer) ||
      !flowmq_peer_state_write_idle(&peer->state))
    return 0;
  for (size_t i = 0u;
       i < flowmq_subscription_set_count(&socket->subscriptions); ++i) {
    subscription = flowmq_subscription_set_at(&socket->subscriptions, i);
    if (subscription == NULL) return 1;
    topic = (vstr){.data = subscription->topic,
                   .len = tstr_len(subscription->topic)};
    if (!flowmq_socket_subscription_contains(
            &peer->synced_subscriptions, topic))
      return 1;
  }
  for (size_t i = 0u;
       i < flowmq_subscription_set_count(&peer->synced_subscriptions); ++i) {
    subscription =
        flowmq_subscription_set_at(&peer->synced_subscriptions, i);
    if (subscription == NULL) return 1;
    topic = (vstr){.data = subscription->topic,
                   .len = tstr_len(subscription->topic)};
    if (!flowmq_socket_subscription_contains(&socket->subscriptions, topic))
      return 1;
  }
  return 0;
}

static uint32_t flowmq_socket_local_external_timeout(
    const flowmq_socket_t *socket, uint32_t max_wait_ms) {
  uint32_t wait_ms = max_wait_ms;
  const uint64_t now_ms = cmeta_monotonic_ms();
  const uint64_t now_ns = cmeta_hrtime();
  if (socket->async_error != SALTS_OK ||
      socket->send_cancel_error != SALTS_OK ||
      socket->recv_cancel_error != SALTS_OK)
    return 0u;

  if (socket->reconnect_pending) {
    for (size_t i = 0u; i < FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY; ++i) {
      const flowmq_socket_endpoint_t *endpoint = &socket->endpoints[i];
      uint32_t candidate;
      if (!endpoint->used || endpoint->active || !endpoint->retry_pending)
        continue;
      candidate = flowmq_socket_wait_until_ms(
          now_ms, endpoint->next_attempt_ms, wait_ms);
      if (candidate < wait_ms) wait_ms = candidate;
      if (wait_ms == 0u) return 0u;
    }
  }

  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
    const flowmq_socket_peer_t *peer = &socket->peers[i];
    if (!flowmq_peer_state_is_used(&peer->state)) continue;
    if (flowmq_peer_state_needs_close_retry(&peer->state))
      return 0u;
    if (!flowmq_peer_state_is_connected(&peer->state)) continue;
    if (peer->commit_pending) return 0u;
    if (flowmq_peer_state_handshake_has(
            &peer->state, FLOWMQ_PEER_HANDSHAKE_HELLO_TX) &&
        !flowmq_peer_state_handshake_has(
            &peer->state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_TX) &&
        flowmq_peer_state_write_idle(&peer->state))
      return 0u;
    if (peer->pong_pending && flowmq_socket_peer_ready(peer) &&
        flowmq_peer_state_write_idle(&peer->state))
      return 0u;

    if (peer->heartbeat_active && flowmq_socket_peer_ready(peer)) {
      uint64_t deadline_ns = 0u;
      const flowmq_protocol_heartbeat_action_t action =
          flowmq_protocol_heartbeat_deadlines_next(
              &peer->heartbeat, now_ns, &deadline_ns);
      if (action != FLOWMQ_PROTOCOL_HEARTBEAT_WAIT)
        return 0u;
      {
        const uint32_t candidate = flowmq_socket_wait_until_ns(
            now_ns, deadline_ns, wait_ms);
        if (candidate < wait_ms) wait_ms = candidate;
        if (wait_ms == 0u) return 0u;
      }
    }

    if (flowmq_socket_peer_ready(peer) &&
        flowmq_peer_state_write_idle(&peer->state) &&
        peer->flow_control.update_pending) {
      uint64_t unadvertised = 0u;
      if (peer->flow_control.consumed_data <
          peer->flow_control.advertised_consumed_data)
        return 0u;
      unadvertised =
          peer->flow_control.consumed_data -
          peer->flow_control.advertised_consumed_data;
      if (unadvertised >= peer->flow_control.flow_update_quantum ||
          peer->flow_control.update_deadline_ns <= now_ns)
        return 0u;
      {
        const uint32_t candidate = flowmq_socket_wait_until_ns(
            now_ns, peer->flow_control.update_deadline_ns, wait_ms);
        if (candidate < wait_ms) wait_ms = candidate;
      }
    }

    if (flowmq_socket_subscription_sync_pending(socket, peer))
      return 0u;
    if (flowmq_peer_state_write_idle(&peer->state) &&
        peer->outbound_count != 0u)
      return 0u;
  }
  return wait_ms;
}

int flowmq_socket_internal_external_timeout(
    flowmq_socket_t *socket, uint32_t max_wait_ms, uint32_t *wait_ms) {
  uint32_t cnet_wait = max_wait_ms;
  uint32_t local_wait;
  int status;
  if (socket == NULL || socket->external_backend == NULL ||
      !socket->runtime_initialized || wait_ms == NULL)
    return SALTS_EINVAL;
  status = cnet_client_external_timeout(
      &socket->client, max_wait_ms, &cnet_wait);
  if (status != SALTS_OK) return status;
  local_wait = flowmq_socket_local_external_timeout(socket, max_wait_ms);
  *wait_ms = local_wait < cnet_wait ? local_wait : cnet_wait;
  return SALTS_OK;
}

int flowmq_socket_internal_route_external_completion(
    flowmq_socket_t *socket, const native_io_completion *completion,
    bool *consumed, size_t *events) {
  if (consumed != NULL) *consumed = false;
  if (events != NULL) *events = 0u;
  if (socket == NULL || socket->external_backend == NULL ||
      !socket->runtime_initialized || completion == NULL || consumed == NULL)
    return SALTS_EINVAL;
  return cnet_client_route_external_completion(
      &socket->client, completion, consumed, events);
}

int flowmq_socket_internal_stop_external(flowmq_socket_t *socket) {
  int pending = 0;
  int status;
  if (socket == NULL || socket->external_backend == NULL ||
      !socket->runtime_initialized)
    return SALTS_EINVAL;

  /*
   * External CNet stop never observes I/O and does not replace connection
   * close admission. Initiate/continue close for every live FlowMQ peer, then
   * let the embedding owner advance/observe/route terminal callbacks.
   */
  socket->external_stopping = 1u;
  if (socket->peer_pool.impl != NULL) {
    status = cnet_pool_seal(&socket->peer_pool);
    if (status != SALTS_OK) return status;
  }
  socket->reconnect_pending = 0u;
  for (size_t i = 0u; i < FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY; ++i)
    socket->endpoints[i].retry_pending = 0u;
  status = cnet_manager_request_close(&socket->manager);
  if (status != SALTS_OK) return status;
  size_t work;
  status = cnet_manager_advance(&socket->manager, FLOWMQ_SOCKET_PEER_CAPACITY, &work);
  if (status != SALTS_OK && status != SALTS_EBUSY && status != SALTS_ENOBUFS)
    return status;
  cnet_manager_snapshot snapshot;
  status = cnet_manager_get_snapshot(&socket->manager, &snapshot);
  if (status != SALTS_OK) return status;
  pending = snapshot.reserved != 0u || snapshot.bound != 0u;

  status = cnet_client_stop_external(&socket->client);
  if (status == SALTS_OK) {
    socket->external_stopped = 1u;
    return SALTS_OK;
  }
  if (status == SALTS_EBUSY || pending) return SALTS_EBUSY;
  return status;
}

int flowmq_socket_internal_owner_close_storage(
    flowmq_socket_t *socket, const void *owner_token) {
  const void *saved_owner;
  int status;
  if (socket == NULL || owner_token == NULL ||
      socket->external_owner != owner_token)
    return SALTS_EINVAL;
  if (socket->runtime_initialized && !socket->external_stopped)
    return SALTS_EBUSY;
  saved_owner = socket->external_owner;
  socket->external_owner = NULL;
  status = flowmq_close(socket);
  if (status != SALTS_OK)
    socket->external_owner = saved_owner;
  return status;
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

int flowmq_send_slice(flowmq_socket_t *socket,
                      const mem_slice_t *slice, int flags) {
  int status;
  for (;;) {
    status = flowmq_socket_try_send_slice(socket, slice, flags);
    if (status != SALTS_EBUSY && status != SALTS_ENOBUFS) return status;
    if ((flags & FLOWMQ_DONTWAIT) != 0 ||
        socket == NULL || !socket->runtime_initialized)
      return status;
    status =
        flowmq_socket_drive(socket, FLOWMQ_SOCKET_BLOCKING_SLICE_MS, NULL);
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

int flowmq_recv_slice(flowmq_socket_t *socket,
                      mem_slice_t *out, int flags) {
  int status;
  for (;;) {
    status = flowmq_socket_try_recv_slice(socket, out, flags);
    if (status != SALTS_EBUSY) return status;
    if ((flags & FLOWMQ_DONTWAIT) != 0 ||
        socket == NULL || !socket->runtime_initialized)
      return status;
    status =
        flowmq_socket_drive(socket, FLOWMQ_SOCKET_BLOCKING_SLICE_MS, NULL);
    if (status != SALTS_OK) return status;
  }
}

int flowmq_recv_slicev(flowmq_socket_t *socket,
                       mem_slice_t *segments, size_t capacity,
                       size_t *count, int flags) {
  int status;
  for (;;) {
    status =
        flowmq_socket_try_recv_slicev(socket, segments, capacity, count, flags);
    if (status != SALTS_EBUSY) return status;
    if ((flags & FLOWMQ_DONTWAIT) != 0 ||
        socket == NULL || !socket->runtime_initialized)
      return status;
    status =
        flowmq_socket_drive(socket, FLOWMQ_SOCKET_BLOCKING_SLICE_MS, NULL);
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
  if (socket->pattern.desc->fsm_class == FLOWMQ_PATTERN_FSM_REP) {
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


int flowmq_socket_internal_poll_revents(
    const flowmq_socket_t *socket, short events, short *revents) {
  if (socket == NULL || revents == NULL) return SALTS_EINVAL;
  *revents = 0;
  if (socket->async_error != SALTS_OK ||
      socket->send_cancel_error != SALTS_OK ||
      socket->recv_cancel_error != SALTS_OK)
    *revents |= FLOWMQ_POLLERR;
  if ((events & FLOWMQ_POLLIN) && flowmq_socket_pollin_ready(socket))
    *revents |= FLOWMQ_POLLIN;
  if ((events & FLOWMQ_POLLOUT) && flowmq_socket_pollout_ready(socket))
    *revents |= FLOWMQ_POLLOUT;
  return SALTS_OK;
}

int flowmq_socket_internal_async_error_matches(
    const flowmq_socket_t *socket, int status) {
  return socket != NULL && status != SALTS_OK &&
         socket->async_error == status;
}

int flowmq_socket_internal_endpoint_backoff(
    const flowmq_socket_t *socket, size_t endpoint_index,
    uint64_t *current_delay_ms) {
  if (socket == NULL || current_delay_ms == NULL ||
      endpoint_index >= FLOWMQ_SOCKET_ENDPOINT_SLOT_CAPACITY ||
      !socket->endpoints[endpoint_index].used)
    return SALTS_EINVAL;
  *current_delay_ms =
      socket->endpoints[endpoint_index].reconnect.current_delay_ms;
  return SALTS_OK;
}

int flowmq_socket_internal_fanout_match_count(
    const flowmq_socket_t *socket, const void *topic, size_t topic_size,
    size_t *count) {
  size_t matched = 0u;
  if (count != NULL) *count = 0u;
  if (socket == NULL || count == NULL ||
      (topic == NULL && topic_size != 0u))
    return SALTS_EINVAL;
  if (socket->pattern.desc == NULL ||
      socket->pattern.desc->routing_class != FLOWMQ_PATTERN_ROUTE_FANOUT)
    return SALTS_ENOTSUP;
  for (size_t i = 0u; i < FLOWMQ_SOCKET_PEER_CAPACITY; ++i) {
    const flowmq_socket_peer_t *peer = &socket->peers[i];
    if (!flowmq_socket_peer_ready(peer))
      continue;
    if (flowmq_subscription_set_match(
            &peer->subscriptions,
            (vstr){.data = (char *)topic, .len = topic_size}))
      ++matched;
  }
  *count = matched;
  return SALTS_OK;
}


int flowmq_poll(flowmq_pollitem_t *items, size_t item_count,
                uint32_t timeout_ms, size_t *ready) {
  const uint64_t started_ms = cmeta_monotonic_ms();
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
      status = flowmq_socket_internal_poll_revents(
          socket, items[i].events, &items[i].revents);
      if (status != SALTS_OK) return status;
      if (items[i].revents != 0) ++*ready;
      (void)events;
    }
    if (*ready != 0u || timeout_ms == 0u) return SALTS_OK;
    {
      const uint64_t elapsed_ms = cmeta_monotonic_ms() - started_ms;
      const uint64_t remaining_ms =
          elapsed_ms >= timeout_ms ? 0u : (uint64_t)timeout_ms - elapsed_ms;
      if (remaining_ms == 0u) return SALTS_OK;
      cmeta_sleep_ms(remaining_ms > 1u ? 1u : (uint32_t)remaining_ms);
    }
  }
}
