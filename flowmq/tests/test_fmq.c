#include "turbo_flow_fmq.h"
#include "turbo_flow_fmq_broker.h"
#include "turbo_flow_fmq_management.h"
#include "turbo_flow_fmq_pubsub.h"

#include "CoroNet/turbo_kcp.h"
#include "CoroNet/turbo_coro_context.h"
#include "fmq_bench_stats.h"
#include "fmq_delivery.h"
#include "fmq_protocol.h"
#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_error.h"
#include "turbo_flow_discovery.h"
#include "turbo_parser.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <inttypes.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
#endif

#ifdef _WIN32
typedef SOCKET fmq_test_socket_t;
  #define FMQ_TEST_INVALID_SOCKET INVALID_SOCKET
#else
typedef int fmq_test_socket_t;
  #define FMQ_TEST_INVALID_SOCKET (-1)
#endif

#define FMQ_TEST_LARGE_PAYLOAD_SIZE TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE
#define FMQ_TEST_STOP_LIMIT_NS UINT64_C(500000000)
#define FMQ_TEST_POST_SETTLE_MS 20u
#define FMQ_TEST_RECONNECT_SOAK_CYCLES 12
#define FMQ_BENCH_TCP_WARMUP 32u
#define FMQ_BENCH_TCP_SAMPLES 256u
#define FMQ_BENCH_TCP_PAYLOAD_BYTES 64u
#define FMQ_BENCH_TCP_WAIT_LIMIT_NS UINT64_C(2000000000)

static const char FMQ_TEST_KCP_PSK[] =
    "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a";
static const char FMQ_TEST_CERTIFICATE_SHA256[] =
    "sha256:ebd76f304bc43bc2be697fca2f054206978c0558931529a7c1b2bb7d82a7a3c4";

typedef struct fmq_capture_state_s {
  char payload[128];
  char topic[64];
  char identity[64];
  char content_identity[TURBO_FLOW_CONTENT_IDENTITY_MAX + 1u];
  size_t payload_len;
  size_t topic_len;
  size_t identity_len;
  uint64_t correlation_id;
  int subscribe;
  atomic_int subscription_events;
  atomic_int subscriptions;
  atomic_int unsubscriptions;
  atomic_int called;
  turbo_flow_content_profile_t content_profile;
  turbo_flow_data_encoding_t content_encoding;
  uint32_t content_flags;
} fmq_capture_state_t;

typedef struct fmq_async_completion_state_s {
  atomic_int called;
  atomic_int failures;
  atomic_int hold;
  atomic_int entered;
} fmq_async_completion_state_t;

typedef struct fmq_fragmented_batch_capture_s {
  const char *expected[2];
  size_t expected_size;
  atomic_int called;
  atomic_int failures;
} fmq_fragmented_batch_capture_t;

typedef struct fmq_bench_capture_state_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  uint64_t received_ns;
  size_t payload_len;
  int called;
} fmq_bench_capture_state_t;

typedef struct fmq_count_capture_s {
  atomic_int called;
  atomic_size_t last_payload_size;
} fmq_count_capture_t;

typedef struct fmq_tfmp_capture_s {
  uint8_t payload[2048];
  size_t payload_size;
  atomic_int called;
} fmq_tfmp_capture_t;

typedef struct fmq_pubsub_stage_capture_s {
  turbo_flow_fmq_pubsub_state_t *state;
  atomic_int called;
  atomic_int status;
} fmq_pubsub_stage_capture_t;

typedef struct fmq_stage_gate_s {
  atomic_int entered;
  atomic_int release;
  atomic_uint_fast64_t correlation_id;
} fmq_stage_gate_t;

typedef struct fmq_reconnect_start_ctx_s {
  turbo_flow_t *flow;
  uint32_t delay_ms;
  atomic_int rc;
} fmq_reconnect_start_ctx_t;

typedef struct fmq_publish_thread_ctx_s {
  turbo_flow_t *flow;
  const char *payload;
  size_t payload_len;
  atomic_int rc;
} fmq_publish_thread_ctx_t;

typedef struct fmq_app_batch_thread_ctx_s {
  turbo_flow_fmq_app_t *app;
  const turbo_flow_fmq_app_send_item_t *items;
  size_t item_count;
  size_t submitted;
  atomic_int entered;
  atomic_int done;
  atomic_int rc;
} fmq_app_batch_thread_ctx_t;

typedef struct fmq_delayed_reply_state_s {
  turbo_flow_msg_t message;
  atomic_int ready;
  atomic_int capture_status;
} fmq_delayed_reply_state_t;

typedef struct fmq_broker_route_capture_s {
  turbo_flow_msg_t message;
  char identity[TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX + 1u];
  size_t identity_len;
  atomic_int ready;
  atomic_int capture_status;
} fmq_broker_route_capture_t;

typedef struct fmq_delayed_reply_thread_ctx_s {
  turbo_flow_t *flow;
  turbo_flow_msg_t *message;
  atomic_int status;
} fmq_delayed_reply_thread_ctx_t;

typedef struct fmq_stop_thread_ctx_s {
  turbo_flow_t *flow;
  atomic_int entered;
  atomic_int rc;
} fmq_stop_thread_ctx_t;

typedef struct fmq_hwm_result_s {
  int setup_status;
  int usage_status;
  int rejected_status;
  int publish_status;
  int drain_status;
  int stop_status;
  int peer_status;
  int hwm_events;
  turbo_flow_connection_snapshot_t active_snapshot;
  turbo_flow_connection_snapshot_t drained_snapshot;
} fmq_hwm_result_t;

typedef struct fmq_linger_result_s {
  int setup_status;
  int usage_status;
  int stop_status;
  int publish_status;
  int peer_status;
  uint64_t stop_elapsed_ns;
  turbo_flow_connection_snapshot_t active_snapshot;
  turbo_flow_connection_snapshot_t stopped_snapshot;
} fmq_linger_result_t;

typedef struct fmq_drop_oldest_result_s {
  int setup_status;
  int first_status;
  int second_status;
  int third_status;
  int stop_status;
  int peer_status;
  int hwm_events;
  int dropped_events;
  int dropped_status;
  turbo_flow_resource_snapshot_t connection_snapshot;
  turbo_flow_resource_snapshot_t saturated_snapshot;
  turbo_flow_connection_snapshot_t drained_snapshot;
} fmq_drop_oldest_result_t;

typedef struct fmq_block_stop_result_s {
  int setup_status;
  int first_status;
  int blocked_status;
  int stop_status;
  int peer_status;
  turbo_flow_connection_snapshot_t drained_snapshot;
} fmq_block_stop_result_t;

typedef struct fmq_event_state_s {
  atomic_int peer_connected;
  atomic_int peer_disconnected;
  atomic_int reconnect_succeeded;
  atomic_int reconnect_scheduled;
  atomic_int reconnect_failed;
  atomic_int heartbeat_timeout;
  atomic_int frame_sent;
  atomic_int slow_peer_sent;
  atomic_int hwm_reached;
  atomic_int frame_dropped;
  atomic_int slow_peer_hwm;
  atomic_int slow_peer_dropped;
  atomic_int slow_peer_disconnected;
  atomic_int authentication_failed;
  atomic_int authorization_denied;
  atomic_int last_dropped_status;
  atomic_int last_status;
  atomic_int last_reconnect_delay_ms;
  atomic_int last_frame_bytes;
  char last_peer_identity[TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE + 1u];
  atomic_size_t last_peer_identity_len;
  atomic_int contract_valid;
} fmq_event_state_t;

typedef struct fmq_certificate_identity_state_s {
  const char *expected_fingerprint;
  const char *expected_identity;
  atomic_int calls;
} fmq_certificate_identity_state_t;

typedef struct fmq_batch_route_gate_s {
  turbo_flow_fmq_app_t *disconnect_app;
  fmq_event_state_t *events;
  size_t trigger_evaluation;
  atomic_size_t evaluations;
  atomic_int entered;
  atomic_int stop_status;
  atomic_int disconnect_status;
} fmq_batch_route_gate_t;

typedef struct fmq_send_gate_s {
  fmq_event_state_t *events;
  turbo_flow_fmq_event_kind_t event_kind;
  atomic_int entered;
  atomic_int release;
  uint32_t max_hold_ms;
} fmq_send_gate_t;

typedef struct fmq_silent_peer_s {
  fmq_test_socket_t listener;
  atomic_int saw_ping;
  atomic_int status;
} fmq_silent_peer_t;

typedef struct fmq_slow_peer_s {
  unsigned short port;
  const char *identity;
  turbo_flow_fmq_pattern_t pattern;
  atomic_int ready;
  atomic_int release_reads;
  atomic_int status;
} fmq_slow_peer_t;

typedef struct fmq_context_runner_s {
  coro_context_t *context;
  turbo_thread_t thread;
  int started;
} fmq_context_runner_t;

typedef struct fmq_context_post_state_s {
  atomic_int observed;
} fmq_context_post_state_t;

static void fmq_context_runner_thread(void *arg) {
  fmq_context_runner_t *runner = (fmq_context_runner_t *)arg;
  if (runner && runner->context) (void)coro_context_run(runner->context, TURBO_RUN_DEFAULT);
}

static void fmq_context_stop_post(void *arg1, void *arg2) {
  (void)arg2;
  coro_context_stop((coro_context_t *)arg1);
}

static void fmq_context_mark_post(void *arg1, void *arg2) {
  fmq_context_post_state_t *state = (fmq_context_post_state_t *)arg1;
  (void)arg2;
  if (state) atomic_store_explicit(&state->observed, 1, memory_order_release);
}

static void fmq_wait_context_post(fmq_context_post_state_t *state) {
  for (int i = 0; state && i < 2000 &&
                  atomic_load_explicit(&state->observed, memory_order_acquire) == 0;
       ++i)
    turbo_sleep_ms(1);
}

static int fmq_context_runner_start(fmq_context_runner_t *runner, coro_context_t *context) {
  if (!runner || !context || runner->started) return TURBO_EINVAL;
  memset(runner, 0, sizeof(*runner));
  runner->context = context;
  coro_context_set_persistent(context, 1);
  if (turbo_thread_create(&runner->thread, fmq_context_runner_thread, runner) != TURBO_OK) {
    coro_context_set_persistent(context, 0);
    runner->context = NULL;
    return TURBO_EIO;
  }
  runner->started = 1;
  return TURBO_OK;
}

static void fmq_context_runner_stop(fmq_context_runner_t *runner) {
  if (!runner || !runner->started || !runner->context) return;
  coro_context_set_persistent(runner->context, 0);
  if (coro_post(runner->context, fmq_context_stop_post, runner->context, NULL) != TURBO_OK) {
    coro_context_stop(runner->context);
  }
  (void)turbo_thread_join(&runner->thread);
  memset(runner, 0, sizeof(*runner));
}

static void fmq_test_close_socket(fmq_test_socket_t socket_handle) {
  if (socket_handle == FMQ_TEST_INVALID_SOCKET) return;
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
}

static int fmq_test_recv_exact(fmq_test_socket_t socket_handle, char *data, size_t len) {
  size_t received = 0;
  while (received < len) {
    int rc = recv(socket_handle, data + received, (int)(len - received), 0);
    if (rc <= 0) return TURBO_EIO;
    received += (size_t)rc;
  }
  return TURBO_OK;
}

static int fmq_test_send_all(fmq_test_socket_t socket_handle, const char *data, size_t len) {
  size_t sent = 0;
  while (sent < len) {
    int rc = send(socket_handle, data + sent, (int)(len - sent), 0);
    if (rc <= 0) return TURBO_EIO;
    sent += (size_t)rc;
  }
  return TURBO_OK;
}

static size_t fmq_test_encoded_payload_size(size_t payload_size) {
  size_t packets = payload_size > 0u ? (payload_size - 1u) / FLOW_FMQ_PACKET_PAYLOAD_SIZE + 1u : 1u;
  return payload_size + packets * FLOW_FMQ_HEADER_SIZE;
}

static unsigned short fmq_silent_peer_open(fmq_silent_peer_t *peer) {
  struct sockaddr_in address;
#ifdef _WIN32
  int address_len = (int)sizeof(address);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0;
#else
  socklen_t address_len = (socklen_t)sizeof(address);
#endif
  if (!peer) return 0;
  memset(peer, 0, sizeof(*peer));
  peer->listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  atomic_init(&peer->saw_ping, 0);
  atomic_init(&peer->status, TURBO_EIO);
  if (peer->listener == FMQ_TEST_INVALID_SOCKET) return 0;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(peer->listener, (struct sockaddr *)&address, sizeof(address)) != 0 ||
      listen(peer->listener, 1) != 0 ||
      getsockname(peer->listener, (struct sockaddr *)&address, &address_len) != 0) {
    fmq_test_close_socket(peer->listener);
    peer->listener = FMQ_TEST_INVALID_SOCKET;
    return 0;
  }
  return ntohs(address.sin_port);
}

static void fmq_silent_peer_thread(void *ctx) {
  fmq_silent_peer_t *peer = (fmq_silent_peer_t *)ctx;
  fmq_test_socket_t client;
  flow_fmq_frame_t hello;
  flow_fmq_frame_t incoming;
  char header[FLOW_FMQ_HEADER_SIZE];
  tstr_t encoded = NULL;
  size_t consumed = 0;
  int rc;
  client = accept(peer->listener, NULL, NULL);
  fmq_test_close_socket(peer->listener);
  peer->listener = FMQ_TEST_INVALID_SOCKET;
  if (client == FMQ_TEST_INVALID_SOCKET) return;
  rc = fmq_test_recv_exact(client, header, sizeof(header));
  if (rc != TURBO_OK) goto done;
  rc = flow_fmq_decode_frame(header, sizeof(header), TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE,
                             &incoming, &consumed);
  if (rc != TURBO_OK || incoming.kind != FLOW_FMQ_FRAME_HELLO ||
      incoming.pattern != TURBO_FLOW_FMQ_SUB) {
    rc = TURBO_EPROTO;
    goto done;
  }
  memset(&hello, 0, sizeof(hello));
  hello.kind = FLOW_FMQ_FRAME_HELLO;
  hello.pattern = TURBO_FLOW_FMQ_PUB;
  rc = flow_fmq_encode_frame(&hello, TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE, &encoded);
  if (rc != TURBO_OK) goto done;
  rc = fmq_test_send_all(client, encoded, tstr_len(encoded));
  if (rc != TURBO_OK) goto done;
  rc = fmq_test_recv_exact(client, header, sizeof(header));
  if (rc != TURBO_OK) goto done;
  rc = flow_fmq_decode_frame(header, sizeof(header), TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE,
                             &incoming, &consumed);
  if (rc != TURBO_OK || incoming.kind != FLOW_FMQ_FRAME_PING) {
    rc = TURBO_EPROTO;
    goto done;
  }
  atomic_store_explicit(&peer->saw_ping, 1, memory_order_release);
  while (recv(client, header, sizeof(header), 0) > 0) {
  }
  rc = TURBO_OK;
done:
  tstr_free(encoded);
  fmq_test_close_socket(client);
  atomic_store_explicit(&peer->status, rc, memory_order_release);
}

static void fmq_slow_peer_thread(void *ctx) {
  fmq_slow_peer_t *peer = (fmq_slow_peer_t *)ctx;
  fmq_test_socket_t socket_handle = FMQ_TEST_INVALID_SOCKET;
  struct sockaddr_in address;
  flow_fmq_frame_t hello;
  flow_fmq_frame_t incoming;
  char header[FLOW_FMQ_HEADER_SIZE];
  char read_buffer[64u * 1024u];
  tstr_t encoded = NULL;
  size_t consumed = 0;
  turbo_flow_fmq_pattern_t expected_remote_pattern;
  int receive_buffer_size = 1024;
  int rc = TURBO_EIO;
  if (!peer || peer->port == 0 ||
      (peer->pattern != TURBO_FLOW_FMQ_SUB && peer->pattern != TURBO_FLOW_FMQ_PULL)) {
    return;
  }
  expected_remote_pattern = peer->pattern == TURBO_FLOW_FMQ_SUB ? TURBO_FLOW_FMQ_PUB
                                                                : TURBO_FLOW_FMQ_PUSH;
  socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle == FMQ_TEST_INVALID_SOCKET) return;
  (void)setsockopt(socket_handle, SOL_SOCKET, SO_RCVBUF, (const char *)&receive_buffer_size,
                   (int)sizeof(receive_buffer_size));
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(peer->port);
  if (connect(socket_handle, (const struct sockaddr *)&address, sizeof(address)) != 0) goto done;
  memset(&hello, 0, sizeof(hello));
  hello.kind = FLOW_FMQ_FRAME_HELLO;
  hello.pattern = peer->pattern;
  hello.identity = tstr_v_from_cstr(peer->identity);
  rc = flow_fmq_encode_frame(&hello, TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE, &encoded);
  if (rc != TURBO_OK) goto done;
  rc = fmq_test_send_all(socket_handle, encoded, tstr_len(encoded));
  if (rc != TURBO_OK) goto done;
  rc = fmq_test_recv_exact(socket_handle, header, sizeof(header));
  if (rc != TURBO_OK) goto done;
  rc = flow_fmq_decode_frame(header, sizeof(header), TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE,
                             &incoming, &consumed);
  if (rc != TURBO_OK || incoming.kind != FLOW_FMQ_FRAME_HELLO ||
      incoming.pattern != expected_remote_pattern) {
    rc = TURBO_EPROTO;
    goto done;
  }
  atomic_store_explicit(&peer->ready, 1, memory_order_release);
  while (!atomic_load_explicit(&peer->release_reads, memory_order_acquire))
    turbo_sleep_ms(1);
  while (recv(socket_handle, read_buffer, sizeof(read_buffer), 0) > 0) {
  }
  rc = TURBO_OK;
done:
  tstr_free(encoded);
  fmq_test_close_socket(socket_handle);
  atomic_store_explicit(&peer->status, rc, memory_order_release);
}

static unsigned short fmq_test_port_for_socket(int socket_type, int protocol) {
  struct sockaddr_in addr;
#ifdef _WIN32
  SOCKET socket_handle = INVALID_SOCKET;
  int addr_len = (int)sizeof(addr);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0;
#else
  int socket_handle = -1;
  socklen_t addr_len = (socklen_t)sizeof(addr);
#endif
  unsigned short port = 0;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socket_handle = socket(AF_INET, socket_type, protocol);
#ifdef _WIN32
  if (socket_handle == INVALID_SOCKET) return 0;
#else
  if (socket_handle < 0) return 0;
#endif
  if (bind(socket_handle, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
      getsockname(socket_handle, (struct sockaddr *)&addr, &addr_len) == 0) {
    port = ntohs(addr.sin_port);
  }
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
  return port;
}

static unsigned short fmq_test_port(void) {
  return fmq_test_port_for_socket(SOCK_STREAM, IPPROTO_TCP);
}

static unsigned short fmq_test_udp_port(void) {
  return fmq_test_port_for_socket(SOCK_DGRAM, IPPROTO_UDP);
}

static int fmq_capture(turbo_flow_msg_t *msg, void *ctx) {
  fmq_capture_state_t *state = (fmq_capture_state_t *)ctx;
  tstr_v topic;
  tstr_v identity;
  uint64_t correlation_id;
  if (!state || !msg || msg->payload.len > sizeof(state->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0) memcpy(state->payload, msg->payload.data, msg->payload.len);
  state->payload_len = msg->payload.len;
  {
    const turbo_flow_content_descriptor_t *descriptor = turbo_flow_msg_content_descriptor(msg);
    if (descriptor) {
      state->content_profile = descriptor->profile;
      state->content_encoding = descriptor->encoding;
      state->content_flags = descriptor->flags;
      memcpy(state->content_identity, descriptor->identity, sizeof(state->content_identity));
    }
  }
  if (turbo_flow_fmq_message_topic(msg, &topic) == TURBO_OK) {
    state->topic_len = topic.len < sizeof(state->topic) ? topic.len : sizeof(state->topic);
    if (state->topic_len > 0) memcpy(state->topic, topic.data, state->topic_len);
  }
  if (turbo_flow_fmq_message_identity(msg, &identity) == TURBO_OK) {
    state->identity_len =
        identity.len < sizeof(state->identity) ? identity.len : sizeof(state->identity);
    if (state->identity_len > 0) memcpy(state->identity, identity.data, state->identity_len);
  }
  if (turbo_flow_fmq_message_correlation_id(msg, &correlation_id) == TURBO_OK) {
    state->correlation_id = correlation_id;
  }
  {
    int subscribe;
    tstr_v subscription_topic;
    if (turbo_flow_fmq_message_subscription(msg, &subscribe, &subscription_topic) == TURBO_OK) {
      state->subscribe = subscribe;
      state->topic_len = subscription_topic.len < sizeof(state->topic) ? subscription_topic.len
                                                                       : sizeof(state->topic);
      if (state->topic_len > 0) memcpy(state->topic, subscription_topic.data, state->topic_len);
      atomic_fetch_add_explicit(&state->subscription_events, 1, memory_order_acq_rel);
      atomic_fetch_add_explicit(subscribe ? &state->subscriptions : &state->unsubscriptions, 1,
                                memory_order_acq_rel);
    }
  }
  atomic_fetch_add_explicit(&state->called, 1, memory_order_release);
  return TURBO_OK;
}

static int fmq_app_capture(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *msg, void *ctx) {
  (void)app;
  return fmq_capture(msg, ctx);
}

static int fmq_fragmented_batch_capture(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *msg,
                                        void *ctx) {
  fmq_fragmented_batch_capture_t *state = (fmq_fragmented_batch_capture_t *)ctx;
  int index;
  (void)app;
  if (!state || !msg) return TURBO_EINVAL;
  if (msg->payload.len == sizeof("ready") - 1u &&
      memcmp(msg->payload.data, "ready", sizeof("ready") - 1u) == 0) {
    return TURBO_OK;
  }
  index = atomic_fetch_add_explicit(&state->called, 1, memory_order_acq_rel);
  if (index < 0 || index >= 2 || msg->payload.len != state->expected_size ||
      memcmp(msg->payload.data, state->expected[index], state->expected_size) != 0) {
    atomic_fetch_add_explicit(&state->failures, 1, memory_order_acq_rel);
  }
  return TURBO_OK;
}

static int fmq_app_reply(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *msg, void *ctx) {
  const char *payload = (const char *)ctx;
  (void)app;
  return turbo_flow_fmq_app_message_set_payload_copy(msg, payload, strlen(payload));
}

static void fmq_async_completion(void *ctx, int status) {
  fmq_async_completion_state_t *state = (fmq_async_completion_state_t *)ctx;
  if (!state) return;
  if (status != TURBO_OK)
    atomic_fetch_add_explicit(&state->failures, 1, memory_order_acq_rel);
  if (atomic_load_explicit(&state->hold, memory_order_acquire)) {
    atomic_store_explicit(&state->entered, 1, memory_order_release);
    while (atomic_load_explicit(&state->hold, memory_order_acquire)) turbo_thread_yield();
  }
  atomic_fetch_add_explicit(&state->called, 1, memory_order_release);
}

static void fmq_bench_capture_init(fmq_bench_capture_state_t *state) {
  memset(state, 0, sizeof(*state));
  turbo_mutex_init(&state->mutex);
  turbo_cond_init(&state->changed);
}

static void fmq_bench_capture_destroy(fmq_bench_capture_state_t *state) {
  if (!state) return;
  turbo_cond_destroy(&state->changed);
  turbo_mutex_destroy(&state->mutex);
}

static int fmq_bench_capture(turbo_flow_msg_t *msg, void *ctx) {
  fmq_bench_capture_state_t *state = (fmq_bench_capture_state_t *)ctx;
  uint64_t received_ns = turbo_hrtime();
  if (!state || !msg) return TURBO_EINVAL;
  turbo_mutex_lock(&state->mutex);
  state->received_ns = received_ns;
  state->payload_len = msg->payload.len;
  state->called += 1;
  turbo_cond_signal(&state->changed);
  turbo_mutex_unlock(&state->mutex);
  return TURBO_OK;
}

static int fmq_count_capture(turbo_flow_msg_t *msg, void *ctx) {
  fmq_count_capture_t *state = (fmq_count_capture_t *)ctx;
  if (!state || !msg) return TURBO_EINVAL;
  atomic_store_explicit(&state->last_payload_size, msg->payload.len, memory_order_release);
  atomic_fetch_add_explicit(&state->called, 1, memory_order_release);
  return TURBO_OK;
}

static int fmq_tfmp_capture(turbo_flow_msg_t *msg, void *ctx) {
  fmq_tfmp_capture_t *capture = (fmq_tfmp_capture_t *)ctx;
  if (!capture || !msg || msg->payload.len > sizeof(capture->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0u) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_size = msg->payload.len;
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return TURBO_OK;
}

static int fmq_pubsub_put_capture(turbo_flow_msg_t *msg, void *ctx) {
  fmq_pubsub_stage_capture_t *capture = (fmq_pubsub_stage_capture_t *)ctx;
  int rc;
  if (!capture || !capture->state) return TURBO_EINVAL;
  rc = turbo_flow_fmq_pubsub_put_stage(msg, capture->state);
  atomic_store_explicit(&capture->status, rc, memory_order_release);
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return rc;
}

static int fmq_echo(turbo_flow_msg_t *msg, void *ctx) {
  fmq_capture_state_t *state = (fmq_capture_state_t *)ctx;
  tstr_v identity;
  uint64_t correlation_id;
  if (!state || !msg) return TURBO_EINVAL;
  if (turbo_flow_fmq_message_identity(msg, &identity) == TURBO_OK) {
    state->identity_len =
        identity.len < sizeof(state->identity) ? identity.len : sizeof(state->identity);
    if (state->identity_len > 0) memcpy(state->identity, identity.data, state->identity_len);
  }
  if (turbo_flow_fmq_message_correlation_id(msg, &correlation_id) == TURBO_OK) {
    state->correlation_id = correlation_id;
  }
  return TURBO_OK;
}

static void fmq_delayed_reply_state_init(fmq_delayed_reply_state_t *state) {
  memset(state, 0, sizeof(*state));
  turbo_flow_msg_init(&state->message);
  atomic_init(&state->ready, 0);
  atomic_init(&state->capture_status, TURBO_EALREADY);
}

static void fmq_broker_route_capture_init(fmq_broker_route_capture_t *capture) {
  memset(capture, 0, sizeof(*capture));
  turbo_flow_msg_init(&capture->message);
  atomic_init(&capture->ready, 0);
  atomic_init(&capture->capture_status, TURBO_EALREADY);
}

static void fmq_broker_route_capture_reset(fmq_broker_route_capture_t *capture) {
  turbo_flow_msg_cleanup(&capture->message);
  fmq_broker_route_capture_init(capture);
}

static int fmq_broker_capture_route(turbo_flow_msg_t *msg, void *ctx) {
  fmq_broker_route_capture_t *capture = (fmq_broker_route_capture_t *)ctx;
  tstr_v identity = {0};
  int rc;
  if (!capture || !msg || atomic_load_explicit(&capture->ready, memory_order_acquire))
    return TURBO_EBUSY;
  rc = turbo_flow_fmq_message_identity(msg, &identity);
  if (rc == TURBO_OK && identity.len > TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX) rc = TURBO_EMSGSIZE;
  if (rc == TURBO_OK) {
    capture->identity_len = identity.len;
    if (identity.len > 0u) memcpy(capture->identity, identity.data, identity.len);
  }
  if (rc == TURBO_OK) rc = turbo_flow_fmq_message_detach_router_route(msg);
  if (rc == TURBO_OK) rc = turbo_flow_msg_clone(&capture->message, msg);
  atomic_store_explicit(&capture->capture_status, rc, memory_order_release);
  atomic_store_explicit(&capture->ready, 1, memory_order_release);
  return rc;
}

static void fmq_wait_broker_capture(fmq_broker_route_capture_t *capture) {
  for (int i = 0; i < 2000 && !atomic_load_explicit(&capture->ready, memory_order_acquire); ++i)
    turbo_sleep_ms(1);
}

static int fmq_detach_router_route(turbo_flow_msg_t *msg, void *ctx) {
  fmq_delayed_reply_state_t *state = (fmq_delayed_reply_state_t *)ctx;
  int rc;
  if (!state || !msg || atomic_load_explicit(&state->ready, memory_order_acquire)) {
    return TURBO_EBUSY;
  }
  rc = turbo_flow_fmq_message_detach_router_route(msg);
  if (rc == TURBO_OK) rc = turbo_flow_msg_clone(&state->message, msg);
  atomic_store_explicit(&state->capture_status, rc, memory_order_release);
  atomic_store_explicit(&state->ready, 1, memory_order_release);
  return rc;
}

static void fmq_delayed_reply_thread(void *ctx) {
  fmq_delayed_reply_thread_ctx_t *reply = (fmq_delayed_reply_thread_ctx_t *)ctx;
  int rc = TURBO_EINVAL;
  if (reply && reply->flow && reply->message) {
    rc = turbo_flow_publish(reply->flow, "delayed", reply->message);
  }
  if (reply) atomic_store_explicit(&reply->status, rc, memory_order_release);
}

static void fmq_wait_delayed_reply(fmq_delayed_reply_state_t *state) {
  for (int i = 0; i < 2000 && !atomic_load_explicit(&state->ready, memory_order_acquire); ++i) {
    turbo_sleep_ms(1);
  }
}

static int fmq_wait_at_stage_gate(turbo_flow_msg_t *msg, void *ctx) {
  fmq_stage_gate_t *gate = (fmq_stage_gate_t *)ctx;
  uint64_t correlation_id = 0u;
  if (!gate || !msg || turbo_flow_fmq_message_correlation_id(msg, &correlation_id) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  atomic_store_explicit(&gate->correlation_id, correlation_id, memory_order_release);
  atomic_store_explicit(&gate->entered, 1, memory_order_release);
  while (!atomic_load_explicit(&gate->release, memory_order_acquire))
    turbo_sleep_ms(1);
  return TURBO_OK;
}

static void fmq_init_capture(fmq_capture_state_t *state) {
  memset(state, 0, sizeof(*state));
  atomic_init(&state->subscription_events, 0);
  atomic_init(&state->subscriptions, 0);
  atomic_init(&state->unsubscriptions, 0);
  atomic_init(&state->called, 0);
}

static void fmq_init_count_capture(fmq_count_capture_t *state) {
  atomic_init(&state->called, 0);
  atomic_init(&state->last_payload_size, 0u);
}

static void fmq_wait_called(fmq_capture_state_t *state, int expected) {
  for (int i = 0; i < 400 && atomic_load_explicit(&state->called, memory_order_acquire) < expected;
       ++i) {
    turbo_sleep_ms(5);
  }
}

static void fmq_wait_count_called(fmq_count_capture_t *state, int expected) {
  for (int i = 0; i < 6000 && atomic_load_explicit(&state->called, memory_order_acquire) < expected;
       ++i) {
    turbo_sleep_ms(5);
  }
}

static void fmq_wait_subscription_events(fmq_capture_state_t *state, int expected) {
  for (int i = 0; i < 400 && atomic_load_explicit(&state->subscription_events,
                                                  memory_order_acquire) < expected;
       ++i) {
    turbo_sleep_ms(5);
  }
}

static void fmq_wait_subscriptions(fmq_capture_state_t *state, int expected) {
  for (int i = 0;
       i < 400 && atomic_load_explicit(&state->subscriptions, memory_order_acquire) < expected;
       ++i) {
    turbo_sleep_ms(5);
  }
}

static void fmq_init_events(fmq_event_state_t *state) {
  atomic_init(&state->peer_connected, 0);
  atomic_init(&state->peer_disconnected, 0);
  atomic_init(&state->reconnect_succeeded, 0);
  atomic_init(&state->reconnect_scheduled, 0);
  atomic_init(&state->reconnect_failed, 0);
  atomic_init(&state->heartbeat_timeout, 0);
  atomic_init(&state->frame_sent, 0);
  atomic_init(&state->slow_peer_sent, 0);
  atomic_init(&state->hwm_reached, 0);
  atomic_init(&state->frame_dropped, 0);
  atomic_init(&state->slow_peer_hwm, 0);
  atomic_init(&state->slow_peer_dropped, 0);
  atomic_init(&state->slow_peer_disconnected, 0);
  atomic_init(&state->authentication_failed, 0);
  atomic_init(&state->authorization_denied, 0);
  atomic_init(&state->last_dropped_status, TURBO_OK);
  atomic_init(&state->last_status, TURBO_OK);
  atomic_init(&state->last_reconnect_delay_ms, 0);
  atomic_init(&state->last_frame_bytes, 0);
  atomic_init(&state->last_peer_identity_len, 0u);
  atomic_init(&state->contract_valid, 1);
}

static void fmq_event_capture(void *ctx, const turbo_flow_fmq_event_t *event) {
  fmq_event_state_t *state = (fmq_event_state_t *)ctx;
  if (!state || !event) return;
  if (event->size != sizeof(*event)) {
    atomic_store_explicit(&state->contract_valid, 0, memory_order_release);
    return;
  }
  atomic_store_explicit(&state->last_status, event->status, memory_order_release);
  if (event->reconnect_delay_ms <= (uint64_t)INT_MAX) {
    atomic_store_explicit(&state->last_reconnect_delay_ms, (int)event->reconnect_delay_ms,
                          memory_order_release);
  }
  if (event->frame_bytes <= (uint64_t)INT_MAX) {
    atomic_store_explicit(&state->last_frame_bytes, (int)event->frame_bytes, memory_order_release);
  }
  if (event->kind >= TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM &&
      event->kind <= TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DISCONNECTED &&
      event->peer_identity.len <= TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE &&
      (event->peer_identity.len == 0u || event->peer_identity.data)) {
    if (event->peer_identity.len > 0u) {
      memcpy(state->last_peer_identity, event->peer_identity.data, event->peer_identity.len);
    }
    state->last_peer_identity[event->peer_identity.len] = '\0';
    atomic_store_explicit(&state->last_peer_identity_len, event->peer_identity.len,
                          memory_order_release);
  }
  switch (event->kind) {
  case TURBO_FLOW_FMQ_EVENT_PEER_CONNECTED:
    atomic_fetch_add_explicit(&state->peer_connected, 1, memory_order_acq_rel);
    break;
  case TURBO_FLOW_FMQ_EVENT_PEER_DISCONNECTED:
    atomic_fetch_add_explicit(&state->peer_disconnected, 1, memory_order_acq_rel);
    break;
  case TURBO_FLOW_FMQ_EVENT_RECONNECT_SCHEDULED:
    atomic_fetch_add_explicit(&state->reconnect_scheduled, 1, memory_order_acq_rel);
    break;
  case TURBO_FLOW_FMQ_EVENT_RECONNECT_SUCCEEDED:
    atomic_fetch_add_explicit(&state->reconnect_succeeded, 1, memory_order_acq_rel);
    break;
  case TURBO_FLOW_FMQ_EVENT_RECONNECT_FAILED:
    atomic_fetch_add_explicit(&state->reconnect_failed, 1, memory_order_acq_rel);
    break;
  case TURBO_FLOW_FMQ_EVENT_HEARTBEAT_TIMEOUT:
    atomic_fetch_add_explicit(&state->heartbeat_timeout, 1, memory_order_acq_rel);
    break;
  case TURBO_FLOW_FMQ_EVENT_FRAME_SENT:
    atomic_fetch_add_explicit(&state->frame_sent, 1, memory_order_acq_rel);
    if (event->peer_identity.len == 9u && event->peer_identity.data &&
        memcmp(event->peer_identity.data, "slow-peer", 9u) == 0) {
      atomic_fetch_add_explicit(&state->slow_peer_sent, 1, memory_order_acq_rel);
    }
    break;
  case TURBO_FLOW_FMQ_EVENT_HWM_REACHED:
    atomic_fetch_add_explicit(&state->hwm_reached, 1, memory_order_acq_rel);
    break;
  case TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED:
    atomic_fetch_add_explicit(&state->frame_dropped, 1, memory_order_acq_rel);
    atomic_store_explicit(&state->last_dropped_status, event->status, memory_order_release);
    break;
  case TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM:
    atomic_fetch_add_explicit(&state->slow_peer_hwm, 1, memory_order_acq_rel);
    break;
  case TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DROPPED:
    atomic_fetch_add_explicit(&state->slow_peer_dropped, 1, memory_order_acq_rel);
    atomic_store_explicit(&state->last_dropped_status, event->status, memory_order_release);
    break;
  case TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DISCONNECTED:
    atomic_fetch_add_explicit(&state->slow_peer_disconnected, 1, memory_order_acq_rel);
    break;
  case TURBO_FLOW_FMQ_EVENT_AUTHENTICATION_FAILED:
    atomic_fetch_add_explicit(&state->authentication_failed, 1, memory_order_acq_rel);
    break;
  case TURBO_FLOW_FMQ_EVENT_AUTHORIZATION_DENIED:
    atomic_fetch_add_explicit(&state->authorization_denied, 1, memory_order_acq_rel);
    break;
  default:
    break;
  }
}

static void fmq_send_gate_event(void *ctx, const turbo_flow_fmq_event_t *event) {
  fmq_send_gate_t *gate = (fmq_send_gate_t *)ctx;
  uint64_t deadline_ns;
  if (!gate || !event) return;
  if (gate->events) fmq_event_capture(gate->events, event);
  if (event->kind != gate->event_kind) return;
  atomic_store_explicit(&gate->entered, 1, memory_order_release);
  deadline_ns = turbo_hrtime() + (uint64_t)gate->max_hold_ms * UINT64_C(1000000);
  while (!atomic_load_explicit(&gate->release, memory_order_acquire) &&
         turbo_hrtime() < deadline_ns) {
    turbo_thread_yield();
  }
}

static void fmq_init_send_gate(fmq_send_gate_t *gate, fmq_event_state_t *events,
                               turbo_flow_fmq_event_kind_t event_kind, uint32_t max_hold_ms) {
  memset(gate, 0, sizeof(*gate));
  gate->events = events;
  gate->event_kind = event_kind;
  gate->max_hold_ms = max_hold_ms;
  atomic_init(&gate->entered, 0);
  atomic_init(&gate->release, 0);
}

static int fmq_wait_send_gate(fmq_send_gate_t *gate) {
  if (!gate) return TURBO_EINVAL;
  for (int i = 0; i < 400; ++i) {
    if (atomic_load_explicit(&gate->entered, memory_order_acquire)) return TURBO_OK;
    turbo_sleep_ms(5);
  }
  return TURBO_ETIMEDOUT;
}

static uint32_t fmq_linger_gate_hold_ms(uint64_t linger_ms) {
  const uint32_t margin_ms = 100u;
  if (linger_ms >= UINT32_MAX - margin_ms) return UINT32_MAX;
  return (uint32_t)linger_ms + margin_ms;
}

static void fmq_wait_event_count(atomic_int *counter, int expected) {
  for (int i = 0; i < 200 && atomic_load_explicit(counter, memory_order_acquire) < expected; ++i) {
    turbo_sleep_ms(5);
  }
}

static int fmq_wait_connection_state(turbo_flow_t *flow, turbo_flow_connection_state_t expected,
                                     turbo_flow_connection_snapshot_t *snapshot) {
  if (!flow || !snapshot) return TURBO_EINVAL;
  for (int i = 0; i < 400; ++i) {
    int rc = turbo_flow_adapter_connection_snapshot_at(flow, 0, snapshot);
    if (rc != TURBO_OK) return rc;
    if (snapshot->state == expected) return TURBO_OK;
    turbo_sleep_ms(5);
  }
  return TURBO_ETIMEDOUT;
}

static int fmq_wait_connection_count(turbo_flow_t *flow, size_t expected) {
  turbo_flow_connection_snapshot_t snapshot;
  if (!flow) return TURBO_EINVAL;
  for (int i = 0; i < 400; ++i) {
    int rc = turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot);
    if (rc != TURBO_OK) return rc;
    if (snapshot.connections_current == expected) return TURBO_OK;
    turbo_sleep_ms(5);
  }
  return TURBO_ETIMEDOUT;
}

static int fmq_wait_slow_peer(fmq_slow_peer_t *peer) {
  if (!peer) return TURBO_EINVAL;
  for (int i = 0; i < 400; ++i) {
    if (atomic_load_explicit(&peer->ready, memory_order_acquire)) return TURBO_OK;
    turbo_sleep_ms(5);
  }
  return TURBO_ETIMEDOUT;
}

static void fmq_init_slow_peer(fmq_slow_peer_t *peer, unsigned short port) {
  memset(peer, 0, sizeof(*peer));
  peer->port = port;
  peer->identity = "slow-peer";
  peer->pattern = TURBO_FLOW_FMQ_SUB;
  atomic_init(&peer->ready, 0);
  atomic_init(&peer->release_reads, 0);
  atomic_init(&peer->status, TURBO_EIO);
}

static void fmq_init_slow_pull(fmq_slow_peer_t *peer, unsigned short port) {
  fmq_init_slow_peer(peer, port);
  peer->pattern = TURBO_FLOW_FMQ_PULL;
}

static int fmq_wait_in_flight(turbo_flow_t *flow, size_t expected_messages, size_t minimum_bytes,
                              turbo_flow_connection_snapshot_t *snapshot) {
  if (!flow || !snapshot) return TURBO_EINVAL;
  for (int i = 0; i < 400; ++i) {
    int rc = turbo_flow_adapter_connection_snapshot_at(flow, 0, snapshot);
    if (rc != TURBO_OK) return rc;
    if (snapshot->in_flight_messages == expected_messages &&
        snapshot->in_flight_bytes >= minimum_bytes) {
      return TURBO_OK;
    }
    turbo_sleep_ms(5);
  }
  return TURBO_ETIMEDOUT;
}

static void fmq_start_flow_after_delay(void *arg) {
  fmq_reconnect_start_ctx_t *ctx = (fmq_reconnect_start_ctx_t *)arg;
  if (!ctx || !ctx->flow) return;
  turbo_sleep_ms(ctx->delay_ms);
  atomic_store(&ctx->rc, turbo_flow_start(ctx->flow));
}

static int fmq_publish_payload(turbo_flow_t *flow, const char *payload);
static int fmq_publish_bytes(turbo_flow_t *flow, const char *payload, size_t payload_len);

static void fmq_publish_in_thread(void *arg) {
  fmq_publish_thread_ctx_t *ctx = (fmq_publish_thread_ctx_t *)arg;
  if (!ctx || !ctx->flow || !ctx->payload) return;
  atomic_store_explicit(&ctx->rc,
                        ctx->payload_len > 0
                            ? fmq_publish_bytes(ctx->flow, ctx->payload, ctx->payload_len)
                            : fmq_publish_payload(ctx->flow, ctx->payload),
                        memory_order_release);
}

static void fmq_app_send_batch_in_thread(void *arg) {
  fmq_app_batch_thread_ctx_t *ctx = (fmq_app_batch_thread_ctx_t *)arg;
  int rc;
  if (!ctx || !ctx->app || !ctx->items || ctx->item_count == 0u) return;
  atomic_store_explicit(&ctx->entered, 1, memory_order_release);
  rc = turbo_flow_fmq_app_send_batch(ctx->app, ctx->items, ctx->item_count, &ctx->submitted);
  atomic_store_explicit(&ctx->rc, rc, memory_order_release);
  atomic_store_explicit(&ctx->done, 1, memory_order_release);
}

static void fmq_stop_in_thread(void *arg) {
  fmq_stop_thread_ctx_t *ctx = (fmq_stop_thread_ctx_t *)arg;
  if (!ctx || !ctx->flow) return;
  atomic_store_explicit(&ctx->entered, 1, memory_order_release);
  atomic_store_explicit(&ctx->rc, turbo_flow_stop(ctx->flow), memory_order_release);
}

static const turbo_flow_coronet_execution_binding_t FMQ_PRIVATE_EXECUTION = {
    sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};

static int fmq_register_private_adapter(turbo_flow_t *flow, const char *name,
                                        const turbo_flow_fmq_config_t *config) {
  return turbo_flow_fmq_register_adapter_ex(flow, name, config, &FMQ_PRIVATE_EXECUTION);
}

static int fmq_register_private_secure_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_fmq_config_t *config,
    const turbo_flow_fmq_security_binding_t *security) {
  return turbo_flow_fmq_register_secure_adapter_ex(flow, name, config, &FMQ_PRIVATE_EXECUTION,
                                                   security);
}

static int fmq_register_private_fanout_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_fmq_config_t *config,
    const turbo_flow_fmq_fanout_config_t *fanout) {
  return turbo_flow_fmq_register_fanout_adapter_ex(flow, name, config, fanout,
                                                   &FMQ_PRIVATE_EXECUTION);
}

static void fmq_config(turbo_flow_fmq_config_t *config, turbo_flow_fmq_pattern_t pattern,
                       turbo_flow_fmq_endpoint_mode_t mode, unsigned short port) {
  *config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
  config->pattern = pattern;
  config->mode = mode;
  config->transport = TURBO_FLOW_FMQ_TCP;
  config->host = "127.0.0.1";
  config->port = (int)port;
  config->timeout_ms = 2000;
}

static int fmq_test_authenticate(void *ctx, const turbo_flow_security_auth_request_t *request,
                                 turbo_flow_security_principal_t *principal_out) {
  const char *expected_secret = (const char *)ctx;
  size_t expected_size = expected_secret ? strlen(expected_secret) : 0u;
  if (!request || !principal_out || !request->identity || !request->method ||
      strcmp(request->identity, "client-a") != 0 || strcmp(request->method, "token") != 0 ||
      request->secret_size != expected_size ||
      (expected_size != 0u && memcmp(request->secret, expected_secret, expected_size) != 0)) {
    return TURBO_EPERM;
  }
  *principal_out = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  (void)snprintf(principal_out->principal_id, sizeof(principal_out->principal_id), "client-a");
  (void)snprintf(principal_out->principal_type, sizeof(principal_out->principal_type), "service");
  (void)snprintf(principal_out->domain_id, sizeof(principal_out->domain_id), "root-a");
  (void)snprintf(principal_out->auth_method, sizeof(principal_out->auth_method), "token");
  principal_out->scope = TURBO_FLOW_SECURITY_SCOPE_SELF;
  principal_out->group_count = 1u;
  (void)snprintf(principal_out->groups[0], sizeof(principal_out->groups[0]), "root-a");
  principal_out->policy_version = 1u;
  return TURBO_OK;
}

static int fmq_test_verify_certificate_identity(void *ctx, const char *certificate_sha256,
                                                const char *claimed_identity) {
  fmq_certificate_identity_state_t *state = (fmq_certificate_identity_state_t *)ctx;
  if (!state || !certificate_sha256 || !claimed_identity) return TURBO_EINVAL;
  atomic_fetch_add_explicit(&state->calls, 1, memory_order_acq_rel);
  if (!state->expected_fingerprint || !state->expected_identity ||
      strcmp(certificate_sha256, state->expected_fingerprint) != 0 ||
      strcmp(claimed_identity, state->expected_identity) != 0) {
    return TURBO_EPERM;
  }
  return TURBO_OK;
}

static int fmq_test_secret_acquire(void *ctx, const char *reference,
                                   turbo_flow_security_secret_lease_t *lease_out) {
  const char *secret = (const char *)ctx;
  if (!secret || !reference || strcmp(reference, "secret/fmq-client") != 0 || !lease_out)
    return TURBO_EINVAL;
  *lease_out = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  lease_out->bytes = (const uint8_t *)secret;
  lease_out->byte_count = strlen(secret);
  lease_out->version = 1u;
  lease_out->provider_lease = ctx;
  return TURBO_OK;
}

static void fmq_test_secret_release(void *ctx, turbo_flow_security_secret_lease_t *lease) {
  (void)ctx;
  if (lease) *lease = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
}

static int fmq_batch_route_gate_compile(void *ctx,
                                        const turbo_flow_security_matcher_leaf_t *input,
                                        void **compiled_leaf_out) {
  if (!ctx || !input || input->size < sizeof(*input) || input->candidate_count != 1u ||
      !compiled_leaf_out) {
    return TURBO_EINVAL;
  }
  *compiled_leaf_out = ctx;
  return TURBO_OK;
}

static int fmq_batch_route_gate_evaluate(void *ctx, const void *compiled_leaf,
                                         const turbo_flow_security_request_t *request,
                                         turbo_flow_security_match_emit_fn emit, void *emit_ctx) {
  fmq_batch_route_gate_t *gate = (fmq_batch_route_gate_t *)ctx;
  size_t evaluation;
  if (!gate || compiled_leaf != gate || !request || !emit) return TURBO_EINVAL;
  evaluation = atomic_fetch_add_explicit(&gate->evaluations, 1u, memory_order_acq_rel) + 1u;
  if (evaluation == gate->trigger_evaluation) {
    coro_context_t *context = coro_context_current();
    int stop_status;
    atomic_store_explicit(&gate->entered, 1, memory_order_release);
    stop_status = turbo_flow_fmq_app_stop(gate->disconnect_app);
    atomic_store_explicit(&gate->stop_status, stop_status, memory_order_release);
    if (stop_status == TURBO_OK && context) {
      for (int i = 0;
           i < 2000 &&
           atomic_load_explicit(&gate->events->peer_disconnected, memory_order_acquire) < 1;
           ++i) {
        coro_sleep(context, 1u);
      }
    }
    atomic_store_explicit(
        &gate->disconnect_status,
        stop_status == TURBO_OK && context &&
                atomic_load_explicit(&gate->events->peer_disconnected, memory_order_acquire) >= 1
            ? TURBO_OK
            : TURBO_ETIMEDOUT,
        memory_order_release);
  }
  return emit(emit_ctx, 0u);
}

static void fmq_batch_route_gate_destroy(void *ctx, void *compiled_leaf) {
  (void)ctx;
  (void)compiled_leaf;
}

static turbo_flow_t *
fmq_make_source_flow_with_security(const char *binding, const turbo_flow_fmq_config_t *config,
                                   fmq_capture_state_t *capture,
                                   const turbo_flow_fmq_security_binding_t *security) {
  static const char *dsl = "source incoming adapter fmq.input\n"
                           "stage capture\n"
                           "stage main {\n"
                           "  incoming -> capture\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  int register_rc =
      flow ? (security ? fmq_register_private_secure_adapter(flow, binding, config, security)
                       : fmq_register_private_adapter(flow, binding, config))
           : TURBO_ENOMEM;
  if (!flow || register_rc != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "capture", fmq_capture, capture, NULL) != TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *fmq_make_source_flow(const char *binding,
                                          const turbo_flow_fmq_config_t *config,
                                          fmq_capture_state_t *capture) {
  return fmq_make_source_flow_with_security(binding, config, capture, NULL);
}

static turbo_flow_t *fmq_make_count_source_flow(const char *binding,
                                                const turbo_flow_fmq_config_t *config,
                                                fmq_count_capture_t *capture) {
  static const char *dsl = "source incoming adapter fmq.input\n"
                           "stage capture\n"
                           "stage main {\n"
                           "  incoming -> capture\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || fmq_register_private_adapter(flow, binding, config) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "capture", fmq_count_capture, capture, NULL) != TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *
fmq_make_sink_flow_with_security(const char *binding, const turbo_flow_fmq_config_t *config,
                                 const turbo_flow_fmq_security_binding_t *security) {
  static const char *dsl = "source input\n"
                           "stage outgoing adapter fmq.output\n"
                           "stage main {\n"
                           "  input -> outgoing\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  int register_rc =
      flow ? (security ? fmq_register_private_secure_adapter(flow, binding, config, security)
                       : fmq_register_private_adapter(flow, binding, config))
           : TURBO_ENOMEM;
  if (!flow || register_rc != TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *fmq_make_sink_flow(const char *binding,
                                        const turbo_flow_fmq_config_t *config) {
  return fmq_make_sink_flow_with_security(binding, config, NULL);
}

static turbo_flow_t *fmq_make_fanout_sink_flow(const char *binding,
                                               const turbo_flow_fmq_config_t *config,
                                               const turbo_flow_fmq_fanout_config_t *fanout) {
  static const char *dsl = "source input\n"
                           "stage outgoing adapter fmq.output\n"
                           "stage main {\n"
                           "  input -> outgoing\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || fmq_register_private_fanout_adapter(flow, binding, config, fanout) != TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, strlen(dsl)) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *fmq_make_retry_sink_flow(const char *binding,
                                              const turbo_flow_fmq_config_t *config,
                                              uint32_t attempts, uint32_t delay_ms) {
  char dsl[256];
  turbo_flow_t *flow = turbo_flow_create();
  int dsl_len = snprintf(dsl, sizeof(dsl),
                         "source input\n"
                         "stage outgoing adapter fmq.output retry attempts %u delay %u\n"
                         "stage main {\n"
                         "  input -> outgoing\n"
                         "}\n",
                         attempts, delay_ms);
  if (dsl_len <= 0 || (size_t)dsl_len >= sizeof(dsl) || !flow ||
      fmq_register_private_adapter(flow, binding, config) != TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, (size_t)dsl_len) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static int fmq_publish_payload(turbo_flow_t *flow, const char *payload) {
  return payload ? fmq_publish_bytes(flow, payload, strlen(payload)) : TURBO_EINVAL;
}

static int fmq_publish_payload_after_peer_ready(turbo_flow_t *flow, const char *payload) {
  int rc = TURBO_ENOTCONN;
  for (int attempt = 0; attempt < 400 && rc == TURBO_ENOTCONN; ++attempt) {
    rc = fmq_publish_payload(flow, payload);
    if (rc == TURBO_ENOTCONN) turbo_sleep_ms(5);
  }
  return rc;
}

static int fmq_publish_bytes(turbo_flow_t *flow, const char *payload, size_t payload_len) {
  turbo_flow_msg_t msg;
  int rc;
  if (!flow || (!payload && payload_len > 0)) return TURBO_EINVAL;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_new_len(payload, payload_len);
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  rc = turbo_flow_publish(flow, "input", &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static int fmq_bench_echo_roundtrip(turbo_flow_t *dealer, fmq_bench_capture_state_t *capture,
                                    const char *payload, size_t payload_len, int expected,
                                    uint64_t *latency_ns) {
  uint64_t started_ns;
  uint64_t deadline_ns;
  uint64_t received_ns = 0u;
  int rc;
  if (!dealer || !capture || !payload || payload_len == 0u || expected < 1) return TURBO_EINVAL;
  started_ns = turbo_hrtime();
  rc = fmq_publish_bytes(dealer, payload, payload_len);
  if (rc != TURBO_OK) return rc;
  deadline_ns = started_ns > UINT64_MAX - FMQ_BENCH_TCP_WAIT_LIMIT_NS
                    ? UINT64_MAX
                    : started_ns + FMQ_BENCH_TCP_WAIT_LIMIT_NS;

  turbo_mutex_lock(&capture->mutex);
  while (capture->called < expected) {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns ||
        turbo_cond_timedwait(&capture->changed, &capture->mutex, deadline_ns - now_ns) != 0) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
  }
  if (rc == TURBO_OK) received_ns = capture->received_ns;
  turbo_mutex_unlock(&capture->mutex);
  if (rc != TURBO_OK) return rc;
  if (received_ns < started_ns) return TURBO_EPROTO;
  if (latency_ns) *latency_ns = received_ns - started_ns;
  return TURBO_OK;
}

static void fmq_tfcw_write_u64(uint8_t out[8], uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    out[i] = (uint8_t)(value >> (56u - 8u * i));
}

static size_t fmq_tfcw_append(uint8_t *body, size_t capacity, size_t offset, uint8_t field_id,
                              const void *value, size_t value_size) {
  size_t written =
      turbo_ltv_build(field_id | TURBO_FLOW_TFCW_FIELD_CRITICAL, (const uint8_t *)value, value_size,
                      body + offset, capacity - offset);
  return written > 0u ? offset + written : 0u;
}

static size_t fmq_tfcw_ready_body(uint8_t *body, size_t capacity, const char *worker_id,
                                  const char *service, uint64_t messages, uint64_t bytes) {
  uint8_t encoded_messages[8];
  uint8_t encoded_bytes[8];
  size_t offset = 0u;
  fmq_tfcw_write_u64(encoded_messages, messages);
  fmq_tfcw_write_u64(encoded_bytes, bytes);
  offset = fmq_tfcw_append(body, capacity, offset, TURBO_FLOW_TFCW_FIELD_WORKER_ID, worker_id,
                           strlen(worker_id));
  offset = fmq_tfcw_append(body, capacity, offset, TURBO_FLOW_TFCW_FIELD_SERVICE, service,
                           strlen(service));
  offset = fmq_tfcw_append(body, capacity, offset, TURBO_FLOW_TFCW_FIELD_GRANT_MESSAGES,
                           encoded_messages, sizeof(encoded_messages));
  return fmq_tfcw_append(body, capacity, offset, TURBO_FLOW_TFCW_FIELD_GRANT_BYTES, encoded_bytes,
                         sizeof(encoded_bytes));
}

static size_t fmq_tfcw_job_body(uint8_t *body, size_t capacity, const char *logical_address,
                                const char *payload) {
  size_t offset = fmq_tfcw_append(body, capacity, 0u, TURBO_FLOW_TFCW_FIELD_LOGICAL_ADDRESS,
                                  logical_address, strlen(logical_address));
  return fmq_tfcw_append(body, capacity, offset, TURBO_FLOW_TFCW_FIELD_PAYLOAD, payload,
                         strlen(payload));
}

static size_t fmq_tfcw_complete_body(uint8_t *body, size_t capacity, const char *worker_id) {
  return fmq_tfcw_append(body, capacity, 0u, TURBO_FLOW_TFCW_FIELD_WORKER_ID, worker_id,
                         strlen(worker_id));
}

static fmq_hwm_result_t fmq_run_hwm_case(size_t payload_size, size_t message_limit,
                                         size_t byte_limit,
                                         turbo_flow_fmq_frame_admission_policy_t admission,
                                         uint64_t admission_timeout_ms) {
  fmq_hwm_result_t result;
  turbo_flow_fmq_config_t config;
  fmq_slow_peer_t peer;
  fmq_event_state_t events;
  fmq_send_gate_t gate;
  fmq_publish_thread_ctx_t publish_ctx;
  turbo_thread_t peer_thread;
  turbo_thread_t publish_thread;
  turbo_flow_t *publisher = NULL;
  char *payload = NULL;
  unsigned short port = fmq_test_port();
  int publisher_started = 0;
  int peer_thread_started = 0;
  int publish_thread_started = 0;

  memset(&result, 0, sizeof(result));
  result.setup_status = TURBO_EIO;
  result.usage_status = TURBO_EALREADY;
  result.rejected_status = TURBO_EALREADY;
  result.publish_status = TURBO_EALREADY;
  result.drain_status = TURBO_EALREADY;
  result.stop_status = TURBO_EALREADY;
  result.peer_status = TURBO_EALREADY;
  if (port == 0 || payload_size == 0) return result;
  payload = (char *)malloc(payload_size);
  if (!payload) {
    result.setup_status = TURBO_ENOMEM;
    return result;
  }
  memset(payload, 'h', payload_size);
  fmq_init_slow_peer(&peer, port);
  fmq_init_events(&events);
  fmq_init_send_gate(&gate, &events, TURBO_FLOW_FMQ_EVENT_FRAME_SENT, 5000u);
  fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
  config.send_timeout_ms = 5000;
  config.frame_hwm_messages = message_limit;
  config.frame_hwm_bytes = byte_limit;
  config.frame_admission_policy = admission;
  config.frame_admission_timeout_ms = admission_timeout_ms;
  config.event_callback = fmq_send_gate_event;
  config.event_ctx = &gate;
  publisher = fmq_make_sink_flow("fmq.output", &config);
  if (!publisher) goto cleanup;
  result.setup_status = turbo_flow_start(publisher);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publisher_started = 1;
  result.setup_status = turbo_thread_create(&peer_thread, fmq_slow_peer_thread, &peer);
  if (result.setup_status != TURBO_OK) goto cleanup;
  peer_thread_started = 1;
  result.setup_status = fmq_wait_slow_peer(&peer);
  if (result.setup_status != TURBO_OK) goto cleanup;
  result.setup_status = fmq_wait_connection_count(publisher, 1);
  if (result.setup_status != TURBO_OK) goto cleanup;
  memset(&publish_ctx, 0, sizeof(publish_ctx));
  publish_ctx.flow = publisher;
  publish_ctx.payload = payload;
  publish_ctx.payload_len = payload_size;
  atomic_init(&publish_ctx.rc, TURBO_EALREADY);
  result.setup_status = turbo_thread_create(&publish_thread, fmq_publish_in_thread, &publish_ctx);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publish_thread_started = 1;
  result.usage_status = fmq_wait_in_flight(
      publisher, 1, fmq_test_encoded_payload_size(payload_size), &result.active_snapshot);
  if (result.usage_status == TURBO_OK) {
    result.rejected_status = fmq_publish_payload(publisher, "rejected");
  }

cleanup:
  atomic_store_explicit(&gate.release, 1, memory_order_release);
  /* Let the peer drain the accepted frame before waiting for its completion. On
   * Linux the default TCP send buffer is smaller than the test frame, so joining
   * first would turn a valid admitted send into a shutdown cancellation. */
  atomic_store_explicit(&peer.release_reads, 1, memory_order_release);
  if (publish_thread_started) {
    (void)turbo_thread_join(&publish_thread);
    result.publish_status = atomic_load_explicit(&publish_ctx.rc, memory_order_acquire);
    result.drain_status = fmq_wait_in_flight(publisher, 0, 0, &result.drained_snapshot);
  }
  if (publisher_started) {
    result.stop_status = turbo_flow_stop(publisher);
    publisher_started = 0;
  }
  atomic_store_explicit(&peer.release_reads, 1, memory_order_release);
  if (peer_thread_started) {
    (void)turbo_thread_join(&peer_thread);
    result.peer_status = atomic_load_explicit(&peer.status, memory_order_acquire);
  }
  result.hwm_events = atomic_load_explicit(&events.hwm_reached, memory_order_acquire);
  turbo_flow_destroy(publisher);
  free(payload);
  return result;
}

static fmq_drop_oldest_result_t fmq_run_drop_oldest_case(void) {
  fmq_drop_oldest_result_t result;
  turbo_flow_fmq_config_t config;
  fmq_slow_peer_t peer;
  fmq_event_state_t events;
  fmq_send_gate_t gate;
  fmq_publish_thread_ctx_t publish_ctx[3];
  turbo_thread_t peer_thread;
  turbo_thread_t publish_threads[3];
  turbo_flow_t *publisher = NULL;
  unsigned short port = fmq_test_port();
  int publisher_started = 0;
  int peer_thread_started = 0;
  int publish_thread_started[3] = {0, 0, 0};

  memset(&result, 0, sizeof(result));
  result.setup_status = TURBO_EIO;
  result.first_status = TURBO_EALREADY;
  result.second_status = TURBO_EALREADY;
  result.third_status = TURBO_EALREADY;
  result.stop_status = TURBO_EALREADY;
  result.peer_status = TURBO_EALREADY;
  if (port == 0) return result;
  fmq_init_slow_peer(&peer, port);
  fmq_init_events(&events);
  fmq_init_send_gate(&gate, &events, TURBO_FLOW_FMQ_EVENT_FRAME_SENT, 5000u);
  fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
  config.send_timeout_ms = 5000;
  config.frame_hwm_messages = 2;
  config.frame_admission_policy = TURBO_FLOW_FMQ_FRAME_ADMISSION_DROP_OLDEST;
  config.event_callback = fmq_send_gate_event;
  config.event_ctx = &gate;
  publisher = fmq_make_sink_flow("fmq.output", &config);
  if (!publisher) goto cleanup;
  result.setup_status = turbo_flow_start(publisher);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publisher_started = 1;
  result.setup_status = turbo_thread_create(&peer_thread, fmq_slow_peer_thread, &peer);
  if (result.setup_status != TURBO_OK) goto cleanup;
  peer_thread_started = 1;
  result.setup_status = fmq_wait_slow_peer(&peer);
  if (result.setup_status != TURBO_OK) goto cleanup;
  result.setup_status = fmq_wait_connection_count(publisher, 1);
  if (result.setup_status != TURBO_OK) goto cleanup;

  memset(publish_ctx, 0, sizeof(publish_ctx));
  publish_ctx[0].flow = publisher;
  publish_ctx[0].payload = "first";
  atomic_init(&publish_ctx[0].rc, TURBO_EALREADY);
  result.setup_status =
      turbo_thread_create(&publish_threads[0], fmq_publish_in_thread, &publish_ctx[0]);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publish_thread_started[0] = 1;
  result.setup_status = fmq_wait_send_gate(&gate);
  if (result.setup_status != TURBO_OK) goto cleanup;

  publish_ctx[1].flow = publisher;
  publish_ctx[1].payload = "second";
  atomic_init(&publish_ctx[1].rc, TURBO_EALREADY);
  result.setup_status =
      turbo_thread_create(&publish_threads[1], fmq_publish_in_thread, &publish_ctx[1]);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publish_thread_started[1] = 1;
  {
    turbo_flow_connection_snapshot_t active;
    result.setup_status = fmq_wait_in_flight(publisher, 2, 0, &active);
  }
  if (result.setup_status != TURBO_OK) goto cleanup;

  publish_ctx[2].flow = publisher;
  publish_ctx[2].payload = "third";
  atomic_init(&publish_ctx[2].rc, TURBO_EALREADY);
  result.setup_status =
      turbo_thread_create(&publish_threads[2], fmq_publish_in_thread, &publish_ctx[2]);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publish_thread_started[2] = 1;
  (void)turbo_thread_join(&publish_threads[1]);
  result.second_status = atomic_load_explicit(&publish_ctx[1].rc, memory_order_acquire);
  publish_thread_started[1] = 0;
  {
    size_t connection_count = 0u;
    size_t queue_count = 0u;
    size_t protocol_count = 0u;
    result.connection_snapshot = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    result.saturated_snapshot = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    result.setup_status = TURBO_ENOENT;
    for (size_t i = 0; i < turbo_flow_resource_count(publisher); ++i) {
      turbo_flow_resource_snapshot_t snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
      int snapshot_rc = turbo_flow_resource_snapshot_at(publisher, i, &snapshot);
      if (snapshot_rc != TURBO_OK) {
        result.setup_status = snapshot_rc;
        break;
      }
      if (snapshot.kind == TURBO_FLOW_RESOURCE_CONNECTION) {
        result.connection_snapshot = snapshot;
        ++connection_count;
      } else if (snapshot.kind == TURBO_FLOW_RESOURCE_QUEUE_BUFFER) {
        result.saturated_snapshot = snapshot;
        ++queue_count;
      } else if (snapshot.kind == TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE) {
        ++protocol_count;
      }
      result.setup_status = TURBO_OK;
    }
    if (result.setup_status == TURBO_OK &&
        (connection_count != 1u || queue_count != 1u || protocol_count != 1u)) {
      result.setup_status = TURBO_EPROTO;
    }
  }

cleanup:
  atomic_store_explicit(&gate.release, 1, memory_order_release);
  if (publish_thread_started[0]) {
    (void)turbo_thread_join(&publish_threads[0]);
    result.first_status = atomic_load_explicit(&publish_ctx[0].rc, memory_order_acquire);
  }
  if (publish_thread_started[1]) {
    (void)turbo_thread_join(&publish_threads[1]);
    result.second_status = atomic_load_explicit(&publish_ctx[1].rc, memory_order_acquire);
  }
  if (publish_thread_started[2]) {
    (void)turbo_thread_join(&publish_threads[2]);
    result.third_status = atomic_load_explicit(&publish_ctx[2].rc, memory_order_acquire);
  }
  if (publisher_started) {
    result.stop_status = turbo_flow_stop(publisher);
    publisher_started = 0;
  }
  atomic_store_explicit(&peer.release_reads, 1, memory_order_release);
  if (peer_thread_started) {
    (void)turbo_thread_join(&peer_thread);
    result.peer_status = atomic_load_explicit(&peer.status, memory_order_acquire);
  }
  if (publisher) {
    (void)turbo_flow_adapter_connection_snapshot_at(publisher, 0, &result.drained_snapshot);
  }
  result.hwm_events = atomic_load_explicit(&events.hwm_reached, memory_order_acquire);
  result.dropped_events = atomic_load_explicit(&events.frame_dropped, memory_order_acquire);
  result.dropped_status = atomic_load_explicit(&events.last_dropped_status, memory_order_acquire);
  turbo_flow_destroy(publisher);
  return result;
}

static fmq_block_stop_result_t fmq_run_block_stop_case(void) {
  fmq_block_stop_result_t result;
  turbo_flow_fmq_config_t config;
  fmq_slow_peer_t peer;
  fmq_send_gate_t gate;
  fmq_publish_thread_ctx_t publish_ctx[2];
  fmq_stop_thread_ctx_t stop_ctx;
  turbo_thread_t peer_thread;
  turbo_thread_t publish_threads[2];
  turbo_thread_t stop_thread;
  turbo_flow_t *publisher = NULL;
  unsigned short port = fmq_test_port();
  int publisher_started = 0;
  int peer_thread_started = 0;
  int publish_thread_started[2] = {0, 0};
  int stop_thread_started = 0;

  memset(&result, 0, sizeof(result));
  result.setup_status = TURBO_EIO;
  result.first_status = TURBO_EALREADY;
  result.blocked_status = TURBO_EALREADY;
  result.stop_status = TURBO_EALREADY;
  result.peer_status = TURBO_EALREADY;
  if (port == 0) return result;
  fmq_init_slow_peer(&peer, port);
  fmq_init_send_gate(&gate, NULL, TURBO_FLOW_FMQ_EVENT_FRAME_SENT, 5000u);
  fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
  config.send_timeout_ms = 5000;
  config.frame_hwm_messages = 1;
  config.frame_admission_policy = TURBO_FLOW_FMQ_FRAME_ADMISSION_BLOCK;
  config.frame_admission_timeout_ms = UINT64_MAX;
  config.event_callback = fmq_send_gate_event;
  config.event_ctx = &gate;
  publisher = fmq_make_sink_flow("fmq.output", &config);
  if (!publisher) goto cleanup;
  result.setup_status = turbo_flow_start(publisher);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publisher_started = 1;
  result.setup_status = turbo_thread_create(&peer_thread, fmq_slow_peer_thread, &peer);
  if (result.setup_status != TURBO_OK) goto cleanup;
  peer_thread_started = 1;
  result.setup_status = fmq_wait_slow_peer(&peer);
  if (result.setup_status != TURBO_OK) goto cleanup;
  result.setup_status = fmq_wait_connection_count(publisher, 1);
  if (result.setup_status != TURBO_OK) goto cleanup;

  memset(publish_ctx, 0, sizeof(publish_ctx));
  publish_ctx[0].flow = publisher;
  publish_ctx[0].payload = "active";
  atomic_init(&publish_ctx[0].rc, TURBO_EALREADY);
  result.setup_status =
      turbo_thread_create(&publish_threads[0], fmq_publish_in_thread, &publish_ctx[0]);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publish_thread_started[0] = 1;
  result.setup_status = fmq_wait_send_gate(&gate);
  if (result.setup_status != TURBO_OK) goto cleanup;

  publish_ctx[1].flow = publisher;
  publish_ctx[1].payload = "blocked";
  atomic_init(&publish_ctx[1].rc, TURBO_EALREADY);
  result.setup_status =
      turbo_thread_create(&publish_threads[1], fmq_publish_in_thread, &publish_ctx[1]);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publish_thread_started[1] = 1;
  turbo_sleep_ms(FMQ_TEST_POST_SETTLE_MS);
  if (atomic_load_explicit(&publish_ctx[1].rc, memory_order_acquire) != TURBO_EALREADY) {
    result.setup_status = TURBO_EIO;
    goto cleanup;
  }

  memset(&stop_ctx, 0, sizeof(stop_ctx));
  stop_ctx.flow = publisher;
  atomic_init(&stop_ctx.entered, 0);
  atomic_init(&stop_ctx.rc, TURBO_EALREADY);
  result.setup_status = turbo_thread_create(&stop_thread, fmq_stop_in_thread, &stop_ctx);
  if (result.setup_status != TURBO_OK) goto cleanup;
  stop_thread_started = 1;
  while (!atomic_load_explicit(&stop_ctx.entered, memory_order_acquire))
    turbo_thread_yield();
  (void)turbo_thread_join(&publish_threads[1]);
  publish_thread_started[1] = 0;
  result.blocked_status = atomic_load_explicit(&publish_ctx[1].rc, memory_order_acquire);
  result.setup_status = TURBO_OK;

cleanup:
  atomic_store_explicit(&gate.release, 1, memory_order_release);
  if (publish_thread_started[0]) {
    (void)turbo_thread_join(&publish_threads[0]);
    result.first_status = atomic_load_explicit(&publish_ctx[0].rc, memory_order_acquire);
  }
  if (publish_thread_started[1]) {
    (void)turbo_thread_join(&publish_threads[1]);
    result.blocked_status = atomic_load_explicit(&publish_ctx[1].rc, memory_order_acquire);
  }
  if (stop_thread_started) {
    (void)turbo_thread_join(&stop_thread);
    result.stop_status = atomic_load_explicit(&stop_ctx.rc, memory_order_acquire);
    publisher_started = 0;
  } else if (publisher_started) {
    result.stop_status = turbo_flow_stop(publisher);
    publisher_started = 0;
  }
  atomic_store_explicit(&peer.release_reads, 1, memory_order_release);
  if (peer_thread_started) {
    (void)turbo_thread_join(&peer_thread);
    result.peer_status = atomic_load_explicit(&peer.status, memory_order_acquire);
  }
  if (publisher) {
    (void)turbo_flow_adapter_connection_snapshot_at(publisher, 0, &result.drained_snapshot);
  }
  turbo_flow_destroy(publisher);
  return result;
}

static fmq_linger_result_t fmq_run_linger_case(size_t payload_size, uint64_t linger_ms,
                                               uint32_t release_delay_ms) {
  fmq_linger_result_t result;
  turbo_flow_fmq_config_t config;
  fmq_slow_peer_t peer;
  fmq_send_gate_t gate;
  fmq_publish_thread_ctx_t publish_ctx;
  fmq_stop_thread_ctx_t stop_ctx;
  turbo_thread_t peer_thread;
  turbo_thread_t publish_thread;
  turbo_thread_t stop_thread;
  turbo_flow_t *publisher = NULL;
  char *payload = NULL;
  unsigned short port = fmq_test_port();
  uint64_t stop_started_ns = 0;
  int publisher_started = 0;
  int peer_thread_started = 0;
  int publish_thread_started = 0;
  int stop_thread_started = 0;

  memset(&result, 0, sizeof(result));
  result.setup_status = TURBO_EIO;
  result.usage_status = TURBO_EALREADY;
  result.stop_status = TURBO_EALREADY;
  result.publish_status = TURBO_EALREADY;
  result.peer_status = TURBO_EALREADY;
  if (port == 0) return result;
  payload = (char *)malloc(payload_size);
  if (!payload) {
    result.setup_status = TURBO_ENOMEM;
    return result;
  }
  memset(payload, 'l', payload_size);
  fmq_init_slow_peer(&peer, port);
  fmq_init_send_gate(&gate, NULL,
                     linger_ms == 0 ? TURBO_FLOW_FMQ_EVENT_PEER_CONNECTED
                                    : TURBO_FLOW_FMQ_EVENT_FRAME_SENT,
                     fmq_linger_gate_hold_ms(linger_ms));
  fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
  config.max_frame_size = payload_size;
  config.send_timeout_ms = 5000;
  config.frame_linger_ms = linger_ms;
  config.event_callback = fmq_send_gate_event;
  config.event_ctx = &gate;
  publisher = fmq_make_sink_flow("fmq.output", &config);
  if (!publisher) goto cleanup;
  result.setup_status = turbo_flow_start(publisher);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publisher_started = 1;
  result.setup_status = turbo_thread_create(&peer_thread, fmq_slow_peer_thread, &peer);
  if (result.setup_status != TURBO_OK) goto cleanup;
  peer_thread_started = 1;
  result.setup_status = fmq_wait_slow_peer(&peer);
  if (result.setup_status != TURBO_OK) goto cleanup;
  result.setup_status = fmq_wait_connection_count(publisher, 1);
  if (result.setup_status != TURBO_OK) goto cleanup;
  if (linger_ms == 0) {
    result.setup_status = fmq_wait_send_gate(&gate);
    if (result.setup_status != TURBO_OK) goto cleanup;
  }
  memset(&publish_ctx, 0, sizeof(publish_ctx));
  publish_ctx.flow = publisher;
  publish_ctx.payload = payload;
  publish_ctx.payload_len = payload_size;
  atomic_init(&publish_ctx.rc, TURBO_EALREADY);
  result.setup_status = turbo_thread_create(&publish_thread, fmq_publish_in_thread, &publish_ctx);
  if (result.setup_status != TURBO_OK) goto cleanup;
  publish_thread_started = 1;
  result.usage_status = fmq_wait_in_flight(
      publisher, 1, fmq_test_encoded_payload_size(payload_size), &result.active_snapshot);
  if (result.usage_status != TURBO_OK) goto cleanup;
  if (linger_ms == 0) turbo_sleep_ms(FMQ_TEST_POST_SETTLE_MS);
  memset(&stop_ctx, 0, sizeof(stop_ctx));
  stop_ctx.flow = publisher;
  atomic_init(&stop_ctx.entered, 0);
  atomic_init(&stop_ctx.rc, TURBO_EALREADY);
  stop_started_ns = turbo_hrtime();
  result.setup_status = turbo_thread_create(&stop_thread, fmq_stop_in_thread, &stop_ctx);
  if (result.setup_status != TURBO_OK) goto cleanup;
  stop_thread_started = 1;
  while (!atomic_load_explicit(&stop_ctx.entered, memory_order_acquire))
    turbo_thread_yield();
  if (linger_ms == 0) {
    uint64_t deadline_ns = stop_started_ns + FMQ_TEST_STOP_LIMIT_NS;
    while (atomic_load_explicit(&publish_ctx.rc, memory_order_acquire) == TURBO_EALREADY &&
           turbo_hrtime() < deadline_ns) {
      turbo_thread_yield();
    }
    if (atomic_load_explicit(&publish_ctx.rc, memory_order_acquire) == TURBO_EALREADY) {
      result.usage_status = TURBO_ETIMEDOUT;
    }
    atomic_store_explicit(&gate.release, 1, memory_order_release);
  } else if (release_delay_ms > 0) {
    turbo_sleep_ms(release_delay_ms);
    atomic_store_explicit(&gate.release, 1, memory_order_release);
    atomic_store_explicit(&peer.release_reads, 1, memory_order_release);
  }

cleanup:
  if (stop_thread_started) {
    (void)turbo_thread_join(&stop_thread);
    result.stop_elapsed_ns = turbo_hrtime() - stop_started_ns;
    result.stop_status = atomic_load_explicit(&stop_ctx.rc, memory_order_acquire);
    publisher_started = 0;
  } else if (publisher_started) {
    result.stop_status = turbo_flow_stop(publisher);
    publisher_started = 0;
  }
  atomic_store_explicit(&gate.release, 1, memory_order_release);
  atomic_store_explicit(&peer.release_reads, 1, memory_order_release);
  if (publish_thread_started) {
    (void)turbo_thread_join(&publish_thread);
    result.publish_status = atomic_load_explicit(&publish_ctx.rc, memory_order_acquire);
  }
  if (peer_thread_started) {
    (void)turbo_thread_join(&peer_thread);
    result.peer_status = atomic_load_explicit(&peer.status, memory_order_acquire);
  }
  if (publisher) {
    (void)turbo_flow_adapter_connection_snapshot_at(publisher, 0, &result.stopped_snapshot);
  }
  turbo_flow_destroy(publisher);
  free(payload);
  return result;
}

spec("flow_fmq_protocol") {
  it("round trips binary frames and reports partial input") {
    static const char payload[] = {'a', '\0', 'b'};
    flow_fmq_frame_t input;
    flow_fmq_frame_t output;
    tstr_t encoded = NULL;
    size_t consumed = 0;
    memset(&input, 0, sizeof(input));
    input.kind = FLOW_FMQ_FRAME_DATA;
    input.pattern = TURBO_FLOW_FMQ_PUB;
    input.message_id = UINT64_C(42);
    input.identity = tstr_v_from_cstr("publisher");
    input.topic = tstr_v_from_cstr("orders.created");
    input.payload = tstr_v_from_buf(payload, sizeof(payload));
    check_int_eq(flow_fmq_encode_frame(&input, 1024, &encoded), TURBO_OK);
    check_int_eq(
        flow_fmq_decode_frame(encoded, FLOW_FMQ_HEADER_SIZE - 1u, 1024, &output, &consumed),
        FLOW_FMQ_INCOMPLETE);
    check_int_eq(flow_fmq_decode_frame(encoded, tstr_len(encoded), 1024, &output, &consumed),
                 TURBO_OK);
    check_int_eq(output.kind, FLOW_FMQ_FRAME_DATA);
    check_int_eq(output.pattern, TURBO_FLOW_FMQ_PUB);
    check_size_eq(output.identity.len, 9);
    check_mem_eq(output.identity.data, "publisher", 9);
    check_size_eq(output.topic.len, 14);
    check_mem_eq(output.topic.data, "orders.created", 14);
    check_size_eq(output.payload.len, sizeof(payload));
    check_mem_eq(output.payload.data, payload, sizeof(payload));
    check_size_eq(consumed, tstr_len(encoded));
    flow_fmq_frame_cleanup(&output);
    tstr_free(encoded);
  }

  it("writes byte-identical frames into caller-owned private batch storage") {
    static const char payload[] = {'a', '\0', 'b'};
    flow_fmq_frame_t input;
    unsigned char storage[128];
    tstr_t encoded = NULL;
    size_t encoded_size = 0u;
    size_t written = 0u;
    memset(&input, 0, sizeof(input));
    input.kind = FLOW_FMQ_FRAME_DATA;
    input.pattern = TURBO_FLOW_FMQ_DEALER;
    input.message_id = UINT64_C(43);
    input.identity = tstr_v_from_cstr("dealer");
    input.topic = tstr_v_from_cstr("orders");
    input.payload = tstr_v_from_buf(payload, sizeof(payload));
    check_int_eq(flow_fmq_encode_frame(&input, 1024u, &encoded), TURBO_OK);
    check_int_eq(flow_fmq_encoded_size(&input, 1024u, &encoded_size), TURBO_OK);
    check_size_eq(encoded_size, tstr_len(encoded));
    check_int_eq(flowmq_protocol_encode_frame_into_internal(
                     &input, 1024u, storage, encoded_size - 1u, &written),
                 TURBO_ENOSPC);
    check_size_eq(written, encoded_size);
    written = 0u;
    check_int_eq(flowmq_protocol_encode_frame_into_internal(
                     &input, 1024u, storage, sizeof(storage), &written),
                 TURBO_OK);
    check_size_eq(written, encoded_size);
    check_mem_eq(storage, encoded, encoded_size);
    tstr_free(encoded);
  }

  it("splits and reassembles payloads across protocol v3 packets") {
    static char payload[FLOW_FMQ_PACKET_PAYLOAD_SIZE + 17u];
    flow_fmq_frame_t input;
    flow_fmq_frame_t output;
    tstr_t encoded = NULL;
    size_t consumed = 0u;
    size_t encoded_size = 0u;
    size_t second_header;
    for (size_t i = 0u; i < sizeof(payload); ++i)
      payload[i] = (char)(i % 251u);
    memset(&input, 0, sizeof(input));
    input.kind = FLOW_FMQ_FRAME_DATA;
    input.pattern = TURBO_FLOW_FMQ_PUSH;
    input.message_id = UINT64_C(0x1020304050607080);
    input.payload = tstr_v_from_buf(payload, sizeof(payload));
    check_int_eq(flow_fmq_encoded_size(&input, sizeof(payload), &encoded_size), TURBO_OK);
    check_int_eq(flow_fmq_encode_frame(&input, sizeof(payload), &encoded), TURBO_OK);
    check_size_eq(encoded_size, tstr_len(encoded));
    check_size_eq(tstr_len(encoded), sizeof(payload) + 2u * FLOW_FMQ_HEADER_SIZE);
    check_int_eq(
        flow_fmq_decode_frame(encoded, tstr_len(encoded) - 1u, sizeof(payload), &output, &consumed),
        FLOW_FMQ_INCOMPLETE);
    check_int_eq(
        flow_fmq_decode_frame(encoded, tstr_len(encoded), sizeof(payload), &output, &consumed),
        TURBO_OK);
    check_size_eq(output.message_id, input.message_id);
    check_size_eq(output.payload.len, sizeof(payload));
    check_mem_eq(output.payload.data, payload, sizeof(payload));
    check_size_eq(consumed, tstr_len(encoded));
    flow_fmq_frame_cleanup(&output);

    second_header = FLOW_FMQ_HEADER_SIZE + FLOW_FMQ_PACKET_PAYLOAD_SIZE;
    memset(encoded + second_header + 28u, 0, sizeof(uint32_t));
    check_int_eq(
        flow_fmq_decode_frame(encoded, tstr_len(encoded), sizeof(payload), &output, &consumed),
        TURBO_EPROTO);
    tstr_free(encoded);
  }

  it("rejects malformed and oversized frames") {
    flow_fmq_frame_t frame;
    flow_fmq_frame_t decoded;
    tstr_t encoded = NULL;
    size_t consumed = 0;
    memset(&frame, 0, sizeof(frame));
    frame.kind = FLOW_FMQ_FRAME_DATA;
    frame.pattern = TURBO_FLOW_FMQ_PUSH;
    frame.message_id = UINT64_C(1);
    frame.payload = tstr_v_from_cstr("oversized");
    check_int_eq(flow_fmq_encode_frame(&frame, 4, &encoded), TURBO_EMSGSIZE);
    check_null(encoded);
    encoded = tstr_new_len("TFMQ\x01\x02\x03\x01\0\0\0\0\0\0\0\0", 16);
    check_not_null(encoded);
    check_int_eq(flow_fmq_decode_frame(encoded, tstr_len(encoded), 1024, &decoded, &consumed),
                 TURBO_EPROTO);
    tstr_free(encoded);
  }

  it("round trips heartbeat control frames") {
    flow_fmq_frame_t input;
    flow_fmq_frame_t output;
    tstr_t encoded = NULL;
    size_t consumed = 0;
    memset(&input, 0, sizeof(input));
    input.kind = FLOW_FMQ_FRAME_PING;
    input.pattern = TURBO_FLOW_FMQ_SUB;
    check_int_eq(flow_fmq_encode_frame(&input, 1024, &encoded), TURBO_OK);
    check_int_eq(flow_fmq_decode_frame(encoded, tstr_len(encoded), 1024, &output, &consumed),
                 TURBO_OK);
    check_int_eq(output.kind, FLOW_FMQ_FRAME_PING);
    check_int_eq(output.pattern, TURBO_FLOW_FMQ_SUB);
    check_size_eq(output.identity.len, 0);
    check_size_eq(output.topic.len, 0);
    check_size_eq(output.payload.len, 0);
    check_size_eq(consumed, FLOW_FMQ_HEADER_SIZE);
    flow_fmq_frame_cleanup(&output);
    tstr_free(encoded);

    encoded = NULL;
    input.kind = FLOW_FMQ_FRAME_PONG;
    input.payload = tstr_v_from_cstr("invalid");
    check_int_eq(flow_fmq_encode_frame(&input, 1024, &encoded), TURBO_EPROTO);
    check_null(encoded);
  }

  it("round trips subscription control frames") {
    flow_fmq_frame_t input;
    flow_fmq_frame_t output;
    tstr_t encoded = NULL;
    size_t consumed = 0;
    memset(&input, 0, sizeof(input));
    input.kind = FLOW_FMQ_FRAME_SUBSCRIBE;
    input.pattern = TURBO_FLOW_FMQ_XSUB;
    input.topic = tstr_v_from_cstr("orders.");
    check_int_eq(flow_fmq_encode_frame(&input, 1024, &encoded), TURBO_OK);
    check_int_eq(flow_fmq_decode_frame(encoded, tstr_len(encoded), 1024, &output, &consumed),
                 TURBO_OK);
    check_int_eq(output.kind, FLOW_FMQ_FRAME_SUBSCRIBE);
    check_int_eq(output.pattern, TURBO_FLOW_FMQ_XSUB);
    check_size_eq(output.topic.len, 7);
    check_mem_eq(output.topic.data, "orders.", 7);
    check_size_eq(output.identity.len, 0);
    check_size_eq(output.payload.len, 0);
    check_size_eq(output.message_id, 0);
    check_size_eq(consumed, tstr_len(encoded));
    flow_fmq_frame_cleanup(&output);
    tstr_freep(&encoded);

    input.kind = FLOW_FMQ_FRAME_UNSUBSCRIBE;
    input.topic = (tstr_v){0};
    check_int_eq(flow_fmq_encode_frame(&input, 1024, &encoded), TURBO_OK);
    check_int_eq(flow_fmq_decode_frame(encoded, tstr_len(encoded), 1024, &output, &consumed),
                 TURBO_OK);
    check_int_eq(output.kind, FLOW_FMQ_FRAME_UNSUBSCRIBE);
    check_size_eq(output.topic.len, 0);
    flow_fmq_frame_cleanup(&output);
    tstr_freep(&encoded);
  }

  it("rejects metadata payload and message IDs on subscription controls") {
    flow_fmq_frame_t frame;
    tstr_t encoded = NULL;
    memset(&frame, 0, sizeof(frame));
    frame.kind = FLOW_FMQ_FRAME_SUBSCRIBE;
    frame.pattern = TURBO_FLOW_FMQ_XSUB;
    frame.identity = tstr_v_from_cstr("peer");
    check_int_eq(flow_fmq_encode_frame(&frame, 1024, &encoded), TURBO_EPROTO);
    check_null(encoded);
    frame.identity = (tstr_v){0};
    frame.payload = tstr_v_from_cstr("payload");
    check_int_eq(flow_fmq_encode_frame(&frame, 1024, &encoded), TURBO_EPROTO);
    check_null(encoded);
    frame.payload = (tstr_v){0};
    frame.message_id = 1;
    check_int_eq(flow_fmq_encode_frame(&frame, 1024, &encoded), TURBO_EPROTO);
    check_null(encoded);
    frame.message_id = 0;
    frame.pattern = TURBO_FLOW_FMQ_PUB;
    check_int_eq(flow_fmq_encode_frame(&frame, 1024, &encoded), TURBO_EPROTO);
    check_null(encoded);
  }

  it("drives heartbeat waits from absolute deadlines") {
    flow_fmq_heartbeat_deadlines_t heartbeat;
    uint64_t wait_deadline_ns = 0u;
    const uint64_t start_ns = UINT64_C(1000000000);
    flow_fmq_heartbeat_deadlines_init(&heartbeat, start_ns, 20u, 100u, 250u);
    check_int_eq(flow_fmq_heartbeat_deadlines_next(&heartbeat, start_ns, &wait_deadline_ns),
                 FLOW_FMQ_HEARTBEAT_WAIT);
    check_size_eq(wait_deadline_ns, start_ns + UINT64_C(20000000));
    check_int_eq(flow_fmq_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(20000000),
                                                   &wait_deadline_ns),
                 FLOW_FMQ_HEARTBEAT_SEND_PING);
    flow_fmq_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(20000000));
    check_int_eq(flow_fmq_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(40000000),
                                                   &wait_deadline_ns),
                 FLOW_FMQ_HEARTBEAT_SEND_PING);
    check_int_eq(flow_fmq_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(100000000),
                                                   &wait_deadline_ns),
                 FLOW_FMQ_HEARTBEAT_EXPIRED);

    flow_fmq_heartbeat_deadlines_on_receive(&heartbeat, start_ns + UINT64_C(90000000));
    check_int_eq(flow_fmq_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(100000000),
                                                   &wait_deadline_ns),
                 FLOW_FMQ_HEARTBEAT_WAIT);
    check_size_eq(wait_deadline_ns, start_ns + UINT64_C(110000000));
  }

  it("keeps the receive deadline stable across heartbeat pings") {
    flow_fmq_heartbeat_deadlines_t heartbeat;
    uint64_t wait_deadline_ns = 0u;
    const uint64_t start_ns = UINT64_C(1000000000);
    flow_fmq_heartbeat_deadlines_init(&heartbeat, start_ns, 20u, 500u, 55u);
    flow_fmq_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(20000000));
    flow_fmq_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(40000000));
    check_int_eq(flow_fmq_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(54000000),
                                                   &wait_deadline_ns),
                 FLOW_FMQ_HEARTBEAT_WAIT);
    check_size_eq(wait_deadline_ns, start_ns + UINT64_C(55000000));
    check_int_eq(flow_fmq_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(55000000),
                                                   &wait_deadline_ns),
                 FLOW_FMQ_HEARTBEAT_RECV_EXPIRED);
  }

  it("defines only the supported peer pairings") {
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_SUB));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_XSUB));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_XPUB, TURBO_FLOW_FMQ_SUB));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_XPUB, TURBO_FLOW_FMQ_XSUB));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_XPUB));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_XSUB, TURBO_FLOW_FMQ_PUB));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_XSUB, TURBO_FLOW_FMQ_XPUB));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_PUSH, TURBO_FLOW_FMQ_PULL));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_DEALER));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_PAIR, TURBO_FLOW_FMQ_PAIR));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_REP));
    check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_REQ));
    check_false(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_PULL));
    check_false(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_PAIR));
    check_false(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_ROUTER));
    check_false(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_DEALER));
    check_false(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_XPUB, TURBO_FLOW_FMQ_PUB));
    check_false(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_XSUB, TURBO_FLOW_FMQ_SUB));
  }

  it("retries only failures with no socket side effect") {
    check_true(flow_fmq_delivery_retryable(FLOW_FMQ_DELIVERY_NOT_SUBMITTED, TURBO_ENOTCONN));
    check_true(flow_fmq_delivery_retryable(FLOW_FMQ_DELIVERY_NOT_SUBMITTED, TURBO_ETIMEDOUT));
    check_false(flow_fmq_delivery_retryable(FLOW_FMQ_DELIVERY_WRITE_STARTED, TURBO_ETIMEDOUT));
    check_false(flow_fmq_delivery_retryable(FLOW_FMQ_DELIVERY_PARTIAL, TURBO_ECONNRESET));
    check_false(flow_fmq_delivery_retryable(FLOW_FMQ_DELIVERY_DELIVERED, TURBO_EIO));
    check_false(flow_fmq_delivery_retryable(FLOW_FMQ_DELIVERY_NOT_SUBMITTED, TURBO_ENOSPC));
    check_false(flow_fmq_delivery_requires_session_reset(FLOW_FMQ_DELIVERY_NOT_SUBMITTED));
    check_true(flow_fmq_delivery_requires_session_reset(FLOW_FMQ_DELIVERY_WRITE_STARTED));
    check_true(flow_fmq_delivery_requires_session_reset(FLOW_FMQ_DELIVERY_PARTIAL));
    check_true(flow_fmq_delivery_requires_session_reset(FLOW_FMQ_DELIVERY_DELIVERED));
  }

  it("matches direct routes only within the current runtime generation") {
    flow_fmq_route_token_t requested = {7u, 3u};
    flow_fmq_route_token_t same = {7u, 3u};
    flow_fmq_route_token_t other_session = {8u, 3u};
    flow_fmq_route_token_t old_generation = {7u, 2u};
    flow_fmq_route_token_t empty = {0u, 0u};
    check_true(flow_fmq_route_matches(3u, requested, same));
    check_false(flow_fmq_route_matches(3u, requested, other_session));
    check_false(flow_fmq_route_matches(3u, requested, old_generation));
    check_false(flow_fmq_route_matches(4u, requested, same));
    check_false(flow_fmq_route_matches(3u, empty, empty));
  }
}

static int fmq_discovery_fetch_error(void *ctx, uint64_t *registry_version,
                                     turbo_flow_discovery_peer_t *peers, size_t capacity,
                                     size_t *count) {
  int *calls = (int *)ctx;
  (void)registry_version;
  (void)peers;
  (void)capacity;
  (void)count;
  *calls += 1;
  return TURBO_EIO;
}

spec("flow_fmq_config") {
  it("exposes a typed connection snapshot before start") {
    turbo_flow_fmq_config_t config;
    turbo_flow_connection_snapshot_t snapshot;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, 7001);
    config.max_connections = 17;
    check_int_eq(fmq_register_private_adapter(flow, "fmq.output", &config), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    check_str_eq(snapshot.adapter_name, "fmq.output");
    check_int_eq(snapshot.adapter_kind, TURBO_FLOW_ADAPTER_KIND_FMQ);
    check_int_eq(snapshot.direction, TURBO_FLOW_ADAPTER_OUTPUT);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_str_eq(snapshot.endpoint, "tcp://127.0.0.1:7001");
    check_size_eq(snapshot.connections_current, 0);
    check_size_eq(snapshot.connection_limit, 17);
    check_size_eq(snapshot.in_flight_messages, 0);
    check_size_eq(snapshot.in_flight_bytes, 0);
    turbo_flow_destroy(flow);
  }

  it("quiesces resumes and replaces an FMQ endpoint through owner commands") {
    unsigned short first_port = fmq_test_port();
    unsigned short second_port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    turbo_flow_adapter_command_t command;
    turbo_flow_connection_snapshot_t snapshot;
    turbo_flow_t *flow;

    check_int_gt(first_port, 0);
    for (int attempt = 0; attempt < 8 && second_port == first_port; ++attempt) {
      second_port = fmq_test_port();
    }
    check_int_gt(second_port, 0);
    check_int_ne(first_port, second_port);
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, first_port);
    flow = fmq_make_sink_flow("fmq.output", &config);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_READY);
    check_int_eq(snapshot.last_status, TURBO_OK);

    memset(&command, 0, sizeof(command));
    command.size = sizeof(command);
    command.kind = TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT;
    command.endpoint.host = "127.0.0.1";
    command.endpoint.port = (int)second_port;
    check_int_eq(turbo_flow_adapter_command(flow, "fmq.output", &command), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    check_str_contains(snapshot.endpoint, "127.0.0.1");
    {
      char expected[64];
      (void)snprintf(expected, sizeof(expected), "tcp://127.0.0.1:%u", second_port);
      check_str_eq(snapshot.endpoint, expected);
    }
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_READY);

    command.kind = TURBO_FLOW_ADAPTER_QUIESCE;
    check_int_eq(turbo_flow_adapter_command(flow, "fmq.output", &command), TURBO_OK);
    check_int_eq(turbo_flow_adapter_command(flow, "fmq.output", &command), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(snapshot.last_status, TURBO_ESHUTDOWN);
    check_int_eq(fmq_publish_payload(flow, "paused"), TURBO_ESHUTDOWN);

    command.kind = TURBO_FLOW_ADAPTER_RESUME;
    check_int_eq(turbo_flow_adapter_command(flow, "fmq.output", &command), TURBO_OK);
    check_int_eq(turbo_flow_adapter_command(flow, "fmq.output", &command), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_READY);

    command.kind = TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT;
    command.endpoint.host = "";
    command.endpoint.port = 0;
    check_int_eq(turbo_flow_adapter_command(flow, "fmq.output", &command), TURBO_EINVAL);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    {
      char expected[64];
      (void)snprintf(expected, sizeof(expected), "tcp://127.0.0.1:%u", second_port);
      check_str_eq(snapshot.endpoint, expected);
    }
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("reconciles discovered FMQ peers and closes a removed binding") {
    static const char *adapter_names[] = {"fmq.output"};
    unsigned short first_port = fmq_test_port();
    unsigned short discovered_port = fmq_test_port();
    turbo_flow_fmq_config_t fmq;
    turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
    turbo_flow_discovery_controller_t *controller = NULL;
    turbo_flow_discovery_peer_t peer = TURBO_FLOW_DISCOVERY_PEER_INIT;
    turbo_flow_discovery_peer_list_t list = TURBO_FLOW_DISCOVERY_PEER_LIST_INIT;
    turbo_flow_discovery_replace_result_t result = TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    turbo_flow_discovery_source_t source = TURBO_FLOW_DISCOVERY_SOURCE_INIT;
    turbo_flow_connection_snapshot_t snapshot;
    turbo_flow_t *flow;
    int fetch_calls = 0;

    check_int_gt(first_port, 0);
    for (int attempt = 0; attempt < 8 && discovered_port == first_port; ++attempt) {
      discovered_port = fmq_test_port();
    }
    check_int_gt(discovered_port, 0);
    check_int_ne(first_port, discovered_port);
    fmq_config(&fmq, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, first_port);
    flow = fmq_make_sink_flow("fmq.output", &fmq);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

    config.flow = flow;
    config.adapter_names = adapter_names;
    config.adapter_count = 1u;
    check_int_eq(turbo_flow_discovery_controller_create(&config, &controller), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);

    (void)snprintf(peer.peer_id, sizeof(peer.peer_id), "peer-1");
    (void)snprintf(peer.host, sizeof(peer.host), "127.0.0.1");
    peer.port = (int)discovered_port;
    list.registry_version = 1u;
    list.peers = &peer;
    list.peer_count = 1u;
    check_int_eq(turbo_flow_discovery_replace_peer_list(controller, &list, &result), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_READY);
    {
      char expected[64];
      (void)snprintf(expected, sizeof(expected), "tcp://127.0.0.1:%u", discovered_port);
      check_str_eq(snapshot.endpoint, expected);
    }

    source.fetch = fmq_discovery_fetch_error;
    source.ctx = &fetch_calls;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_int_eq(turbo_flow_discovery_poll(controller, &source, &result), TURBO_EIO);
    check_int_eq(fetch_calls, 1);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_READY);

    list.registry_version = 2u;
    list.peers = NULL;
    list.peer_count = 0u;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_int_eq(turbo_flow_discovery_replace_peer_list(controller, &list, &result), TURBO_OK);
    check_uint_eq(result.removed, 1u);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);

    turbo_flow_discovery_controller_destroy(controller);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("rolls back an active endpoint replacement when the new bind fails") {
    unsigned short original_port = fmq_test_port();
    turbo_flow_fmq_config_t original_config;
    turbo_flow_adapter_command_t command;
    turbo_flow_connection_snapshot_t snapshot;
    turbo_flow_t *original;

    check_int_gt(original_port, 0);
    fmq_config(&original_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, original_port);
    original = fmq_make_sink_flow("fmq.output", &original_config);
    check_not_null(original);
    check_int_eq(turbo_flow_start(original), TURBO_OK);

    memset(&command, 0, sizeof(command));
    command.size = sizeof(command);
    command.kind = TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT;
    command.endpoint.host = "invalid host name";
    command.endpoint.port = 9000;
    check_int_ne(turbo_flow_adapter_command(original, "fmq.output", &command), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(original, 0, &snapshot), TURBO_OK);
    {
      char expected[64];
      (void)snprintf(expected, sizeof(expected), "tcp://127.0.0.1:%u", original_port);
      check_str_eq(snapshot.endpoint, expected);
    }
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_READY);
    check_int_eq(turbo_flow_stop(original), TURBO_OK);
    turbo_flow_destroy(original);
  }

  it("accepts valid primitive endpoint configurations") {
    turbo_flow_fmq_config_t config;
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.transport = TURBO_FLOW_FMQ_TCP;
    config.host = "127.0.0.1";
    config.port = 7001;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.reuse_port = 1;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.pattern = TURBO_FLOW_FMQ_SUB;
    config.mode = TURBO_FLOW_FMQ_CONNECT;
    config.topic = "orders.";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.topic = NULL;
    config.reuse_port = 0;
    config.tcp_keepalive = 1;
    config.tcp_keepalive_idle_ms = 30000;
    config.tcp_keepalive_interval_ms = 5000;
    config.tcp_keepalive_count = 3;
    config.linger = 1;
    config.linger_ms = 250;
    config.send_hwm_bytes = 8192;
    config.frame_hwm_messages = 8;
    config.frame_hwm_bytes = 8192;
    config.frame_linger_ms = 50;
    config.heartbeat_interval_ms = 25;
    config.heartbeat_timeout_ms = 100;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.tcp_keepalive = 0;
    config.tcp_keepalive_idle_ms = 0;
    config.tcp_keepalive_interval_ms = 0;
    config.tcp_keepalive_count = 0;
    config.linger = 0;
    config.linger_ms = 0;
    config.send_hwm_bytes = 0;
    config.frame_hwm_messages = 0;
    config.frame_hwm_bytes = 0;
    config.frame_linger_ms = 0;
    config.heartbeat_interval_ms = 0;
    config.heartbeat_timeout_ms = 0;
    config.pattern = TURBO_FLOW_FMQ_SUB;
    config.mode = TURBO_FLOW_FMQ_CONNECT;
    config.topic = "orders.";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.pattern = TURBO_FLOW_FMQ_DEALER;
    config.identity = "worker-1";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.pattern = TURBO_FLOW_FMQ_PAIR;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.identity = NULL;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.topic_policy = TURBO_FLOW_FMQ_METADATA_INHERIT;
    config.identity_policy = TURBO_FLOW_FMQ_METADATA_STATIC;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.topic_policy = TURBO_FLOW_FMQ_METADATA_CONTENT;
    config.identity_policy = TURBO_FLOW_FMQ_METADATA_CONTENT;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.topic_policy = 0;
    config.identity_policy = 0;
    config.transport = TURBO_FLOW_FMQ_UDP;
    config.udp_option_flags = TURBO_FLOW_FMQ_UDP_OPTION_MULTICAST_LOOP |
                              TURBO_FLOW_FMQ_UDP_OPTION_MULTICAST_TTL |
                              TURBO_FLOW_FMQ_UDP_OPTION_BROADCAST;
    config.udp_multicast_loop = 1;
    config.udp_multicast_ttl = 1;
    config.udp_broadcast = 1;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.udp_multicast_group = "239.255.0.1";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.mode = TURBO_FLOW_FMQ_CONNECT;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.udp_multicast_group = NULL;
    config.udp_option_flags = 0;
    config.udp_multicast_loop = 0;
    config.udp_multicast_ttl = 0;
    config.udp_broadcast = 0;
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.transport = TURBO_FLOW_FMQ_KCP;
    config.host = "127.0.0.1";
    config.port = 7001;
    config.kcp_pre_shared_key = FMQ_TEST_KCP_PSK;
    {
      turbo_kcp_config_t kcp_config;
      int kcp_configured = 0;
      check_int_eq(flow_fmq_kcp_config_resolve(&config, &kcp_config, &kcp_configured), TURBO_OK);
      check_int_eq(kcp_configured, 1);
      turbo_kcp_config_wipe(&kcp_config);
    }
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.transport = TURBO_FLOW_FMQ_WS;
    config.kcp_pre_shared_key = NULL;
    config.path = "/fmq";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.transport = TURBO_FLOW_FMQ_WSS;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.transport = TURBO_FLOW_FMQ_PIPE;
    config.host = NULL;
    config.port = 0;
    config.path = "pipe://turbo_flow_fmq_config";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.send_hwm_bytes = 4096;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
  }

  it("accepts only the complete endpoint configuration layout") {
    turbo_flow_fmq_config_t config = TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.transport = TURBO_FLOW_FMQ_TCP;
    config.host = "127.0.0.1";
    config.port = 7001;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.size = sizeof(config) - 1u;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.size = sizeof(config) + 1u;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.size = sizeof(config);
    check_int_eq(TURBO_FLOW_FMQ_WIRE_VERSION, 3u);
  }

  it("accepts scheme-prefixed endpoint hosts and resolves the transport") {
    turbo_flow_fmq_config_t config;

    /* tcp:// with embedded port and transport unset */
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "tcp://127.0.0.1:7001";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);

    /* tls:// / wss:// select the secure transport; material is enforced at
       endpoint creation, matching the existing WSS-without-tls validation */
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "tls://127.0.0.1:7002";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_SUB;
    config.mode = TURBO_FLOW_FMQ_CONNECT;
    config.host = "wss://127.0.0.1:7003";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);

    /* ws:// without an embedded port falls back to config->port */
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_SUB;
    config.mode = TURBO_FLOW_FMQ_CONNECT;
    config.host = "ws://127.0.0.1";
    config.port = 7004;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);

    /* kcp:// (psk required by kcp resolution) */
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "kcp://127.0.0.1:7005";
    config.kcp_pre_shared_key = FMQ_TEST_KCP_PSK;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);

    /* bracketed IPv6 */
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "tcp://[::1]:7006";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);

    /* explicit transport matching the prefix is accepted */
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.transport = TURBO_FLOW_FMQ_TCP;
    config.host = "tcp://127.0.0.1:7007";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);

    /* conflicts and malformed inputs fail fast */
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.transport = TURBO_FLOW_FMQ_TLS;
    config.host = "tcp://127.0.0.1:7008";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "tcp://127.0.0.1:7008";
    config.port = 7009;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "http://127.0.0.1:7010";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "tls://";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "tls://127.0.0.1:abc";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "tls://:7011";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "tls://127.0.0.1/path";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
  }

  it("binds a scheme-prefixed tcp endpoint and reports the resolved snapshot") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    turbo_flow_t *flow;
    turbo_flow_connection_snapshot_t snapshot;
    char host[64];

    check_int_gt(port, 0);
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    (void)snprintf(host, sizeof(host), "tcp://127.0.0.1:%u", port);
    config.host = host;
    config.port = 0; /* port comes from the prefix */
    config.timeout_ms = 2000;
    flow = fmq_make_sink_flow("fmq.output", &config);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    memset(&snapshot, 0, sizeof(snapshot));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_READY);
    {
      char expected[64];
      (void)snprintf(expected, sizeof(expected), "tcp://127.0.0.1:%u", port);
      check_str_eq(snapshot.endpoint, expected);
    }
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects a tls-prefixed endpoint without tls material") {
    turbo_flow_fmq_config_t config;
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.host = "tls://127.0.0.1:7020";
    config.timeout_ms = 2000;
    check_null(fmq_make_sink_flow("fmq.output", &config));
  }

  it("validates object-level TLS configuration and rejects insecure secure endpoints") {
    turbo_flow_fmq_config_t config;
    turbo_flow_fmq_tls_config_t tls = TURBO_FLOW_FMQ_TLS_CONFIG_INIT;
    turbo_flow_fmq_security_binding_t security = TURBO_FLOW_FMQ_SECURITY_BINDING_INIT;
    turbo_flow_t *flow;

    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, 7001u);
    config.transport = TURBO_FLOW_FMQ_TLS;
    tls.cert_file = "server.crt";
    tls.key_file = "server.key";
    tls.rotation_generation = 1u;
    config.tls = &tls;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);

    tls.size -= 1u;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    tls.size = sizeof(tls);
    config.transport = TURBO_FLOW_FMQ_TCP;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_ENOTSUP);
    config.transport = TURBO_FLOW_FMQ_TLS;

    tls.key_file = NULL;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    tls.key_file = "server.key";
    tls.require_client_certificate = 1;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    tls.ca_file = "clients-ca.pem";
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    tls.require_client_certificate = 0;
    tls.rotation_generation = 0u;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    tls = (turbo_flow_fmq_tls_config_t)TURBO_FLOW_FMQ_TLS_CONFIG_INIT;
    tls.ca_file = "servers-ca.pem";
    tls.server_name = "fmq.example.test";
    tls.rotation_generation = 2u;
    config.pattern = TURBO_FLOW_FMQ_SUB;
    config.mode = TURBO_FLOW_FMQ_CONNECT;
    config.host = "127.0.0.1";
    config.topic = "secure";
    config.tls = &tls;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    tls.server_name = NULL;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    tls.server_name = "fmq.example.test";
    tls.verify_peer = 0;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.transport = TURBO_FLOW_FMQ_TLS;
    config.topic = NULL;
    config.tls = NULL;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(fmq_register_private_secure_adapter(flow, "fmq.secure", &config, &security),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
    config.transport = TURBO_FLOW_FMQ_WSS;
    config.path = "/fmq-secure";
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(fmq_register_private_secure_adapter(flow, "fmq.secure", &config, &security),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("validates TCP-backed OS socket buffer requests") {
    turbo_flow_fmq_config_t config = TURBO_FLOW_FMQ_CONFIG_INIT;
    turbo_flow_coronet_execution_binding_t execution = {0};
    coro_context_t *borrowed_context;
    turbo_flow_t *flow;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.transport = TURBO_FLOW_FMQ_TCP;
    config.host = "127.0.0.1";
    config.port = 7001;
    config.socket_recv_buffer_bytes = 1024u * 1024u;
    config.socket_send_buffer_bytes = 512u * 1024u;
    config.stream_recv_buffer_bytes = 128u * 1024u;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(fmq_register_private_adapter(flow, "fmq.tuned", &config), TURBO_OK);
    turbo_flow_destroy(flow);

    borrowed_context = coro_context_create(NULL);
    check_not_null(borrowed_context);
    execution.size = sizeof(execution);
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
    execution.context = borrowed_context;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_fmq_register_adapter_ex(flow, "fmq.borrowed", &config, &execution),
                 TURBO_ENOTSUP);
    turbo_flow_destroy(flow);
    coro_context_destroy(borrowed_context);

    config.transport = TURBO_FLOW_FMQ_UDP;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.transport = TURBO_FLOW_FMQ_TCP;
    config.socket_recv_buffer_bytes = (size_t)INT_MAX + 1u;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_ERANGE);
    config.socket_recv_buffer_bytes = 0u;
    config.stream_recv_buffer_bytes = TURBO_FLOW_FMQ_MIN_STREAM_RECV_BUFFER_SIZE - 1u;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_ERANGE);
  }

  it("validates the bounded per-peer fan-out contract") {
    turbo_flow_fmq_config_t config;
    turbo_flow_fmq_fanout_config_t fanout = TURBO_FLOW_FMQ_FANOUT_CONFIG_INIT;
    turbo_flow_t *flow;
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, 7001u);

    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(fmq_register_private_fanout_adapter(flow, "fmq.out", &config, &fanout),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);

    fanout.peer_hwm_messages = 2u;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(fmq_register_private_fanout_adapter(flow, "fmq.out", &config, &fanout),
                 TURBO_OK);
    turbo_flow_destroy(flow);

    config.pattern = TURBO_FLOW_FMQ_PUSH;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(fmq_register_private_fanout_adapter(flow, "fmq.out", &config, &fanout),
                 TURBO_ENOTSUP);
    turbo_flow_destroy(flow);

    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.transport = TURBO_FLOW_FMQ_UDP;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(fmq_register_private_fanout_adapter(flow, "fmq.out", &config, &fanout),
                 TURBO_ENOTSUP);
    turbo_flow_destroy(flow);

    config.transport = TURBO_FLOW_FMQ_TCP;
    fanout.size -= 1u;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(fmq_register_private_fanout_adapter(flow, "fmq.out", &config, &fanout),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("registers a typed FMQ adapter from a resolved YAML profile") {
    char yaml[2048];
    unsigned short port = fmq_test_port();
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_connection_snapshot_t snapshot;
    turbo_flow_t *flow = turbo_flow_create();
    const char *adapter_name = NULL;
    check_int_gt(port, 0);
    check_not_null(flow);
    (void)snprintf(yaml, sizeof(yaml),
                   "version: 1\n"
                   "profiles:\n"
                   "  production:\n"
                   "    publisher: fmq.out\n"
                   "fragments:\n"
                   "  connection:\n"
                   "    local:\n"
                   "      transport: tcp\n"
                   "      host: 127.0.0.1\n"
                   "      port: %u\n"
                   "  timer:\n"
                   "    bounded:\n"
                   "      timeout_ms: 1000\n"
                   "adapters:\n"
                   "  fmq.out:\n"
                   "    kind: fmq\n"
                   "    fragments:\n"
                   "      connection: local\n"
                   "      timer: bounded\n"
                   "    config:\n"
                   "      pattern: pub\n"
                   "      mode: bind\n"
                   "      frame_hwm_messages: 16\n"
                   "      frame_admission_policy: fail\n",
                   (unsigned int)port);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), TURBO_OK);
    check_not_null(resolved);
    check_int_eq(turbo_flow_resolved_config_profile_adapter(resolved, "production", "publisher",
                                                            &adapter_name),
                 TURBO_OK);
    check_str_eq(adapter_name, "fmq.out");
    check_int_eq(turbo_flow_fmq_register_resolved_adapter(flow, adapter_name, resolved, &error),
                 TURBO_OK);
    memset(&snapshot, 0, sizeof(snapshot));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
    check_str_contains(snapshot.endpoint, "tcp://127.0.0.1:");
    check_size_eq(snapshot.connection_limit, TURBO_FLOW_FMQ_DEFAULT_MAX_CONNECTIONS);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
  }

  it("registers bounded per-peer fan-out only from a complete YAML policy") {
    static const char valid[] = "version: 1\n"
                                "adapters:\n"
                                "  fmq.out:\n"
                                "    kind: fmq\n"
                                "    config:\n"
                                "      pattern: pub\n"
                                "      mode: bind\n"
                                "      transport: tcp\n"
                                "      host: 127.0.0.1\n"
                                "      port: 7001\n"
                                "      peer_hwm_messages: 2\n"
                                "      peer_hwm_bytes: 1048576\n"
                                "      slow_peer_policy: drop_oldest\n";
    static const char missing_policy[] = "version: 1\n"
                                         "adapters:\n"
                                         "  fmq.out:\n"
                                         "    kind: fmq\n"
                                         "    config:\n"
                                         "      pattern: pub\n"
                                         "      mode: bind\n"
                                         "      transport: tcp\n"
                                         "      host: 127.0.0.1\n"
                                         "      port: 7001\n"
                                         "      peer_hwm_messages: 2\n";
    static const char missing_hwm[] = "version: 1\n"
                                      "adapters:\n"
                                      "  fmq.out:\n"
                                      "    kind: fmq\n"
                                      "    config:\n"
                                      "      pattern: pub\n"
                                      "      mode: bind\n"
                                      "      transport: tcp\n"
                                      "      host: 127.0.0.1\n"
                                      "      port: 7001\n"
                                      "      slow_peer_policy: fail\n";
    static const char wrong_policy[] = "version: 1\n"
                                       "adapters:\n"
                                       "  fmq.out:\n"
                                       "    kind: fmq\n"
                                       "    config:\n"
                                       "      pattern: pub\n"
                                       "      mode: bind\n"
                                       "      transport: tcp\n"
                                       "      host: 127.0.0.1\n"
                                       "      port: 7001\n"
                                       "      peer_hwm_messages: 2\n"
                                       "      slow_peer_policy: silent_drop\n";
    const char *invalid[] = {missing_policy, missing_hwm, wrong_policy};
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(valid, strlen(valid), &resolved, &error), TURBO_OK);
    check_int_eq(turbo_flow_fmq_register_resolved_adapter(flow, "fmq.out", resolved, &error),
                 TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);

    for (size_t i = 0u; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
      resolved = NULL;
      error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      flow = turbo_flow_create();
      check_not_null(flow);
      check_int_eq(
          turbo_flow_config_resolve_yaml(invalid[i], strlen(invalid[i]), &resolved, &error),
          TURBO_OK);
      check_int_ne(turbo_flow_fmq_register_resolved_adapter(flow, "fmq.out", resolved, &error),
                   TURBO_OK);
      check_str_contains(error.path, "fmq.out");
      turbo_flow_resolved_config_destroy(resolved);
      turbo_flow_destroy(flow);
    }
  }

  it("fails typed YAML projection for unknown wrong-type and non-FMQ fields") {
    static const char unknown[] = "version: 1\n"
                                  "adapters:\n"
                                  "  fmq.out:\n"
                                  "    kind: fmq\n"
                                  "    config:\n"
                                  "      pattern: pub\n"
                                  "      mode: bind\n"
                                  "      transport: tcp\n"
                                  "      host: 127.0.0.1\n"
                                  "      port: 7001\n"
                                  "      bogus: true\n";
    static const char wrong_type[] = "version: 1\n"
                                     "adapters:\n"
                                     "  fmq.out:\n"
                                     "    kind: fmq\n"
                                     "    config:\n"
                                     "      pattern: pub\n"
                                     "      mode: bind\n"
                                     "      transport: tcp\n"
                                     "      host: 127.0.0.1\n"
                                     "      port: wrong\n";
    static const char wrong_kind[] = "version: 1\n"
                                     "adapters:\n"
                                     "  fmq.out:\n"
                                     "    kind: redis\n"
                                     "    config:\n"
                                     "      pattern: pub\n"
                                     "      mode: bind\n"
                                     "      transport: tcp\n"
                                     "      host: 127.0.0.1\n"
                                     "      port: 7001\n";
    const struct {
      const char *yaml;
      const char *path;
    } cases[] = {{unknown, "bogus"}, {wrong_type, "port"}, {wrong_kind, "fmq.out"}};
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      turbo_flow_resolved_config_t *resolved = NULL;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      check_not_null(flow);
      check_int_eq(
          turbo_flow_config_resolve_yaml(cases[i].yaml, strlen(cases[i].yaml), &resolved, &error),
          TURBO_OK);
      check_not_null(resolved);
      check_int_ne(turbo_flow_fmq_register_resolved_adapter(flow, "fmq.out", resolved, &error),
                   TURBO_OK);
      check_str_contains(error.path, cases[i].path);
      turbo_flow_resolved_config_destroy(resolved);
      turbo_flow_destroy(flow);
    }
  }

  it("keeps the installed FMQ YAML example resolvable") {
    char path[1024];
    char *yaml;
    size_t yaml_len = 0u;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    const char *publisher = NULL;
    const char *subscriber = NULL;
    const char *management_server = NULL;
    const char *management_client = NULL;
    (void)snprintf(path, sizeof(path), "%s/examples/fmq.yml", TURBO_FLOW_FMQ_SOURCE_DIR);
    yaml = tt_read_file(path, &yaml_len);
    check_not_null(yaml);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, yaml_len, &resolved, &error), TURBO_OK);
    check_int_eq(
        turbo_flow_resolved_config_profile_adapter(resolved, "production", "publisher", &publisher),
        TURBO_OK);
    check_int_eq(turbo_flow_resolved_config_profile_adapter(resolved, "production", "subscriber",
                                                            &subscriber),
                 TURBO_OK);
    check_int_eq(turbo_flow_resolved_config_profile_adapter(resolved, "production",
                                                            "management_server",
                                                            &management_server),
                 TURBO_OK);
    check_int_eq(turbo_flow_resolved_config_profile_adapter(resolved, "production",
                                                            "management_client",
                                                            &management_client),
                 TURBO_OK);
    check_str_eq(publisher, "fmq.events.pub");
    check_str_eq(subscriber, "fmq.events.sub");
    check_str_eq(management_server, "fmq.management.router");
    check_str_eq(management_client, "fmq.management.dealer");
    turbo_flow_resolved_config_destroy(resolved);
    free(yaml);
  }

  it("keeps the ZMQ-style Application YAML endpoints resolvable") {
    static const char *const adapter_names[] = {
        "fmq.example.pub", "fmq.example.sub",    "fmq.example.rep",
        "fmq.example.req", "fmq.example.router", "fmq.example.dealer",
    };
    char path[1024];
    char *yaml;
    size_t yaml_len = 0u;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    (void)snprintf(path, sizeof(path), "%s/examples/zmq_style.yml", TURBO_FLOW_FMQ_SOURCE_DIR);
    yaml = tt_read_file(path, &yaml_len);
    check_not_null(yaml);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, yaml_len, &resolved, &error), TURBO_OK);
    check_not_null(resolved);
    for (size_t i = 0u; i < sizeof(adapter_names) / sizeof(adapter_names[0]); ++i) {
      turbo_flow_t *flow = turbo_flow_create();
      check_not_null(flow);
      check_int_eq(
          turbo_flow_fmq_register_resolved_adapter(flow, adapter_names[i], resolved, &error),
          TURBO_OK);
      turbo_flow_destroy(flow);
    }
    turbo_flow_resolved_config_destroy(resolved);
    free(yaml);
  }

  it("keeps unknown FMQ content opaque unless a schema is declared") {
    turbo_flow_fmq_config_t config;
    turbo_flow_content_binding_t binding = TURBO_FLOW_CONTENT_BINDING_INIT;
    turbo_flow_schema_registry_t *registry = turbo_flow_schema_registry_create();
    turbo_flow_t *flow;

    check_not_null(registry);
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, 7001);
    config.content_type = "application/x-fmq-opaque";
    binding.registry = registry;
    config.content_binding = &binding;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(fmq_register_private_adapter(flow, "fmq.opaque", &config), TURBO_OK);
    turbo_flow_destroy(flow);

    binding.schema.schema_name = "fmq.events";
    binding.schema.type_name = "Event";
    binding.schema.schema_version = 1u;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(fmq_register_private_adapter(flow, "fmq.declared", &config), TURBO_EPROTO);
    turbo_flow_destroy(flow);
    turbo_flow_schema_registry_destroy(registry);
  }

  it("rejects incompatible modes and missing dealer identity") {
    turbo_flow_fmq_config_t config;
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_CONNECT;
    config.transport = TURBO_FLOW_FMQ_TCP;
    config.host = "127.0.0.1";
    config.port = 7001;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.pattern = TURBO_FLOW_FMQ_DEALER;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.identity = "worker-1";
    config.port = 0;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.port = 7001;
    config.topic_policy = (turbo_flow_fmq_metadata_policy_t)99;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
  }

  it("enforces REQ connect and REP bind endpoint modes") {
    turbo_flow_fmq_config_t config;
    fmq_config(&config, TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT, 7001);
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.mode = TURBO_FLOW_FMQ_BIND;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config.pattern = TURBO_FLOW_FMQ_REP;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.mode = TURBO_FLOW_FMQ_CONNECT;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
  }

  it("rejects incomplete primitive endpoint configurations") {
    static const char KCP_PSK[] =
        "102132435465768798a9bacbdcedfe0f1f2e3d4c5b6a798897a6b5c4d3e2f101";
    turbo_flow_fmq_config_t config;
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.transport = TURBO_FLOW_FMQ_TCP;
    config.port = 7001;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config.host = "127.0.0.1";
    config.port = 0;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config.port = 65536;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config.transport = TURBO_FLOW_FMQ_PIPE;
    config.host = NULL;
    config.port = 0;
    config.path = NULL;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.path = "pipe://turbo_flow_fmq_config_invalid";
    config.reuse_port = 1;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.reuse_port = 0;
    config.path = NULL;

    config.transport = TURBO_FLOW_FMQ_TCP;
    config.host = "127.0.0.1";
    config.port = 7001;
    config.path = NULL;
    config.kcp_pre_shared_key = KCP_PSK;
    config.kcp_fec_data_shards = 4;
    config.kcp_fec_parity_shards = 2;
    config.kcp_fec_max_payload_size = 1248;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config.transport = TURBO_FLOW_FMQ_KCP;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.kcp_pre_shared_key = NULL;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);

    config.kcp_pre_shared_key = NULL;
    config.kcp_fec_data_shards = 0;
    config.kcp_fec_parity_shards = 0;
    config.kcp_fec_max_payload_size = 0;

    config.transport = TURBO_FLOW_FMQ_UDP;
    config.tcp_keepalive = 1;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.tcp_keepalive = 0;

    config.udp_option_flags = TURBO_FLOW_FMQ_UDP_OPTION_MULTICAST_TTL;
    config.udp_multicast_ttl = 256;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_ERANGE);
    config.udp_multicast_ttl = 1;
    config.transport = TURBO_FLOW_FMQ_TCP;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.udp_option_flags = 0;
    config.udp_multicast_ttl = 0;

    config.transport = TURBO_FLOW_FMQ_TCP;
    config.tcp_keepalive_idle_ms = 30000;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.tcp_keepalive_idle_ms = 0;

    config.transport = TURBO_FLOW_FMQ_KCP;
    config.kcp_pre_shared_key = KCP_PSK;
    config.send_hwm_bytes = 4096;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.send_hwm_bytes = 0;

    config.transport = TURBO_FLOW_FMQ_PIPE;
    config.kcp_pre_shared_key = NULL;
    config.linger = 1;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.linger = 0;

    config.frame_hwm_bytes = FLOW_FMQ_HEADER_SIZE - 1;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_ERANGE);
    config.frame_hwm_bytes = 0;

    config.frame_admission_policy = (turbo_flow_fmq_frame_admission_policy_t)99;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.frame_admission_policy = TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL;
    config.frame_admission_timeout_ms = 1;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.frame_admission_policy = TURBO_FLOW_FMQ_FRAME_ADMISSION_BLOCK;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.frame_hwm_messages = 1;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.frame_admission_policy = TURBO_FLOW_FMQ_FRAME_ADMISSION_DROP_OLDEST;
    config.frame_admission_timeout_ms = 0;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.frame_admission_policy = TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL;
    config.frame_hwm_messages = 0;
    config.frame_admission_timeout_ms = 0;

    config.heartbeat_interval_ms = 50;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.heartbeat_timeout_ms = 25;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_ERANGE);

    config.heartbeat_interval_ms = 0;
    config.heartbeat_timeout_ms = 0;
    config.timeout_ms = TURBO_FLOW_FMQ_TIMEOUT_DISABLED;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.timeout_ms = 0;
    config.connect_timeout_ms = TURBO_FLOW_FMQ_TIMEOUT_DISABLED;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
  }

  it("validates reconnect timing constraints") {
    turbo_flow_fmq_config_t config;
    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_SUB;
    config.mode = TURBO_FLOW_FMQ_CONNECT;
    config.transport = TURBO_FLOW_FMQ_TCP;
    config.host = "127.0.0.1";
    config.port = 7001;
    config.reconnect_initial_ms = TURBO_FLOW_FMQ_RECONNECT_DISABLED;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.reconnect_initial_ms = 100;
    config.reconnect_max_ms = 50;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_ERANGE);
    config.reconnect_max_ms = 150;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.reconnect_initial_ms = 0;
    config.reconnect_max_ms = 0;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
  }

  it("validates combined connect and handshake timeout capability") {
    turbo_flow_fmq_config_t config;
    fmq_config(&config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, 7001);
    config.transport = TURBO_FLOW_FMQ_WSS;
    config.connect_timeout_ms = 250;
    config.handshake_timeout_ms = 500;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
    config.handshake_timeout_ms = 250;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.connect_timeout_ms = 0;
    config.handshake_timeout_ms = 500;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);

    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_ENOTSUP);
    config.handshake_timeout_ms = 0;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_OK);
    config.transport = TURBO_FLOW_FMQ_TCP;
    config.handshake_timeout_ms = 500;
    check_int_eq(flow_fmq_config_validate(&config), TURBO_EINVAL);
  }

  it("rejects source sink role mismatches") {
    static const char *dsl = "source input\n"
                             "stage invalid adapter fmq.sub\n"
                             "stage main {\n"
                             "  input -> invalid\n"
                             "}\n";
    turbo_flow_fmq_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    fmq_config(&config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, 7001);
    check_int_eq(fmq_register_private_adapter(flow, "fmq.sub", &config), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("moves owned SUB messages from one CoroNet lane through a graph thread") {
    static const char *sink_dsl = "source input\n"
                                  "stage outgoing adapter fmq.output\n"
                                  "stage main {\n"
                                  "  input -> outgoing\n"
                                  "}\n";
    static const char *source_dsl = "source incoming adapter fmq.input\n"
                                    "stage capture exec thread workers 1\n"
                                    "stage main {\n"
                                    "  incoming -> capture\n"
                                    "}\n";
    coro_thread_pool_t *pool = coro_thread_pool_create(1);
    turbo_flow_coronet_execution_binding_t execution;
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    fmq_event_state_t events;
    turbo_flow_t *publisher = turbo_flow_create();
    turbo_flow_t *subscriber = turbo_flow_create();
    unsigned short port = fmq_test_port();

    memset(&execution, 0, sizeof(execution));
    fmq_init_capture(&capture);
    fmq_init_events(&events);
    check_not_null(pool);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_gt(port, 0);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    pub_config.content_type = "application/json";
    sub_config.content_type = "application/json";
    execution.size = sizeof(execution);
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_POOL_LANE;
    execution.pool = pool;
    execution.lane = 0;

    check_int_eq(
        turbo_flow_fmq_register_adapter_ex(publisher, "fmq.output", &pub_config, &execution),
        TURBO_OK);
    check_int_eq(
        turbo_flow_fmq_register_adapter_ex(subscriber, "fmq.input", &sub_config, &execution),
        TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(subscriber, "capture", fmq_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(publisher, sink_dsl, strlen(sink_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(subscriber, source_dsl, strlen(source_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(publisher), TURBO_OK);
    check_int_eq(turbo_flow_compile(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "same-lane"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_int_eq(capture.content_profile, TURBO_FLOW_CONTENT_PROFILE_FMQ_DATA);
    check_int_eq(capture.content_encoding, TURBO_FLOW_DATA_ENCODING_JSON);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);

    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
    coro_thread_pool_destroy(pool);
  }

  it("runs two connected FMQ adapters on different CoroNet pool lanes") {
    static const char *sink_dsl = "source input\n"
                                  "stage outgoing adapter fmq.output\n"
                                  "stage main {\n"
                                  "  input -> outgoing\n"
                                  "}\n";
    static const char *source_dsl = "source incoming adapter fmq.input\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  incoming -> capture\n"
                                    "}\n";
    coro_thread_pool_t *pool = coro_thread_pool_create(2);
    turbo_flow_coronet_execution_binding_t publisher_execution;
    turbo_flow_coronet_execution_binding_t subscriber_execution;
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    turbo_flow_t *publisher = turbo_flow_create();
    turbo_flow_t *subscriber = turbo_flow_create();
    unsigned short port = fmq_test_port();

    memset(&publisher_execution, 0, sizeof(publisher_execution));
    memset(&subscriber_execution, 0, sizeof(subscriber_execution));
    fmq_init_capture(&capture);
    check_not_null(pool);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_gt(port, 0);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    publisher_execution.size = sizeof(publisher_execution);
    publisher_execution.kind = TURBO_FLOW_CORONET_EXECUTION_POOL_LANE;
    publisher_execution.pool = pool;
    publisher_execution.lane = 0;
    subscriber_execution = publisher_execution;
    subscriber_execution.lane = 1;

    check_int_eq(turbo_flow_fmq_register_adapter_ex(publisher, "fmq.output", &pub_config,
                                                    &publisher_execution),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_register_adapter_ex(subscriber, "fmq.input", &sub_config,
                                                    &subscriber_execution),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(subscriber, "capture", fmq_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(publisher, sink_dsl, strlen(sink_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(subscriber, source_dsl, strlen(source_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(publisher), TURBO_OK);
    check_int_eq(turbo_flow_compile(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "different-lanes"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 15u);
    check_mem_eq(capture.payload, "different-lanes", 15u);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);

    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
    coro_thread_pool_destroy(pool);
  }

  it("round trips REQ REP on explicitly transferred owned contexts") {
    static const char *rep_dsl =
        "source request adapter fmq.rep operation " TURBO_FLOW_FMQ_REP_REQUEST_OPERATION "\n"
        "stage reply adapter fmq.rep operation " TURBO_FLOW_FMQ_REP_REPLY_OPERATION "\n"
        "stage main {\n"
        "  request -> reply\n"
        "}\n";
    static const char *req_dsl =
        "source response adapter fmq.req operation " TURBO_FLOW_FMQ_REQ_REPLY_OPERATION "\n"
        "source input\n"
        "stage send adapter fmq.req operation " TURBO_FLOW_FMQ_REQ_REQUEST_OPERATION "\n"
        "stage capture\n"
        "stage main {\n"
        "  input -> send\n"
        "  response -> capture\n"
        "}\n";
    coro_context_t *rep_ctx = coro_context_create(NULL);
    coro_context_t *req_ctx = coro_context_create(NULL);
    turbo_flow_coronet_execution_binding_t rep_execution;
    turbo_flow_coronet_execution_binding_t req_execution;
    turbo_flow_fmq_config_t rep_config;
    turbo_flow_fmq_config_t req_config;
    fmq_capture_state_t capture;
    turbo_flow_t *rep = turbo_flow_create();
    turbo_flow_t *req = turbo_flow_create();
    unsigned short port = fmq_test_port();

    memset(&rep_execution, 0, sizeof(rep_execution));
    memset(&req_execution, 0, sizeof(req_execution));
    fmq_init_capture(&capture);
    check_not_null(rep_ctx);
    check_not_null(req_ctx);
    check_not_null(rep);
    check_not_null(req);
    check_int_gt(port, 0);
    fmq_config(&rep_config, TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&req_config, TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT, port);
    rep_execution.size = sizeof(rep_execution);
    rep_execution.kind = TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT;
    rep_execution.context = rep_ctx;
    req_execution.size = sizeof(req_execution);
    req_execution.kind = TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT;
    req_execution.context = req_ctx;

    check_int_eq(turbo_flow_fmq_register_adapter_ex(rep, "fmq.rep", &rep_config, &rep_execution),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_register_adapter_ex(req, "fmq.req", &req_config, &req_execution),
                 TURBO_OK);
    check_str_eq(
        turbo_flow_adapter_operation_module(rep, "fmq.rep", TURBO_FLOW_FMQ_REP_REQUEST_OPERATION),
        TURBO_FLOW_FMQ_MODULE);
    check_str_eq(
        turbo_flow_adapter_operation_module(req, "fmq.req", TURBO_FLOW_FMQ_REQ_REQUEST_OPERATION),
        TURBO_FLOW_FMQ_MODULE);
    check_int_eq(turbo_flow_register_stage_ex(req, "capture", fmq_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(rep, rep_dsl, strlen(rep_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(req, req_dsl, strlen(req_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(rep), TURBO_OK);
    check_int_eq(turbo_flow_compile(req), TURBO_OK);
    check_int_eq(turbo_flow_start(rep), TURBO_OK);
    check_int_eq(turbo_flow_start(req), TURBO_OK);
    check_int_eq(fmq_publish_payload(req, "owned-context"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 13);
    check_mem_eq(capture.payload, "owned-context", 13);
    check_true(capture.correlation_id != 0u);
    check_int_eq(turbo_flow_stop(req), TURBO_OK);
    check_int_eq(turbo_flow_stop(rep), TURBO_OK);

    turbo_flow_destroy(req);
    turbo_flow_destroy(rep);
  }

  it("round trips REQ REP while the host drives borrowed contexts") {
    static const char *rep_dsl = "source request adapter fmq.rep\n"
                                 "stage reply adapter fmq.rep\n"
                                 "stage main {\n"
                                 "  request -> reply\n"
                                 "}\n";
    static const char *req_dsl = "source response adapter fmq.req\n"
                                 "source input\n"
                                 "stage send adapter fmq.req\n"
                                 "stage capture\n"
                                 "stage main {\n"
                                 "  input -> send\n"
                                 "  response -> capture\n"
                                 "}\n";
    coro_context_t *rep_ctx = coro_context_create(NULL);
    coro_context_t *req_ctx = coro_context_create(NULL);
    fmq_context_runner_t rep_runner;
    fmq_context_runner_t req_runner;
    turbo_flow_coronet_execution_binding_t rep_execution;
    turbo_flow_coronet_execution_binding_t req_execution;
    turbo_flow_fmq_config_t rep_config;
    turbo_flow_fmq_config_t req_config;
    fmq_capture_state_t capture;
    turbo_flow_t *rep = turbo_flow_create();
    turbo_flow_t *req = turbo_flow_create();
    unsigned short port = fmq_test_port();

    memset(&rep_runner, 0, sizeof(rep_runner));
    memset(&req_runner, 0, sizeof(req_runner));
    memset(&rep_execution, 0, sizeof(rep_execution));
    memset(&req_execution, 0, sizeof(req_execution));
    fmq_init_capture(&capture);
    check_not_null(rep_ctx);
    check_not_null(req_ctx);
    check_not_null(rep);
    check_not_null(req);
    check_int_gt(port, 0);
    check_int_eq(fmq_context_runner_start(&rep_runner, rep_ctx), TURBO_OK);
    check_int_eq(fmq_context_runner_start(&req_runner, req_ctx), TURBO_OK);
    fmq_config(&rep_config, TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&req_config, TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT, port);
    rep_execution.size = sizeof(rep_execution);
    rep_execution.kind = TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
    rep_execution.context = rep_ctx;
    req_execution.size = sizeof(req_execution);
    req_execution.kind = TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
    req_execution.context = req_ctx;

    check_int_eq(turbo_flow_fmq_register_adapter_ex(rep, "fmq.rep", &rep_config, &rep_execution),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_register_adapter_ex(req, "fmq.req", &req_config, &req_execution),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(req, "capture", fmq_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(rep, rep_dsl, strlen(rep_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(req, req_dsl, strlen(req_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(rep), TURBO_OK);
    check_int_eq(turbo_flow_compile(req), TURBO_OK);
    check_int_eq(turbo_flow_start(rep), TURBO_OK);
    check_int_eq(turbo_flow_start(req), TURBO_OK);
    check_int_eq(fmq_publish_payload(req, "borrowed-context"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 16);
    check_mem_eq(capture.payload, "borrowed-context", 16);
    check_true(capture.correlation_id != 0u);
    check_int_eq(turbo_flow_stop(req), TURBO_OK);
    check_int_eq(turbo_flow_stop(rep), TURBO_OK);
    turbo_flow_destroy(req);
    turbo_flow_destroy(rep);
    fmq_context_runner_stop(&req_runner);
    fmq_context_runner_stop(&rep_runner);
    coro_context_destroy(req_ctx);
    coro_context_destroy(rep_ctx);
  }

  it("registers schema roles for every primitive") {
    static const struct {
      turbo_flow_fmq_pattern_t pattern;
      turbo_flow_fmq_endpoint_mode_t mode;
      uint32_t roles;
    } cases[] = {{TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, TURBO_FLOW_ADAPTER_SINK},
                 {TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, TURBO_FLOW_ADAPTER_SOURCE},
                 {TURBO_FLOW_FMQ_PUSH, TURBO_FLOW_FMQ_BIND, TURBO_FLOW_ADAPTER_SINK},
                 {TURBO_FLOW_FMQ_PULL, TURBO_FLOW_FMQ_CONNECT, TURBO_FLOW_ADAPTER_SOURCE},
                 {TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_BIND,
                  TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK},
                 {TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT,
                  TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK},
                 {TURBO_FLOW_FMQ_PAIR, TURBO_FLOW_FMQ_BIND,
                  TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK},
                 {TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT,
                  TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK},
                 {TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND,
                  TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK},
                 {TURBO_FLOW_FMQ_XPUB, TURBO_FLOW_FMQ_BIND,
                  TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK},
                 {TURBO_FLOW_FMQ_XSUB, TURBO_FLOW_FMQ_CONNECT,
                  TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK}};
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_fmq_config_t config;
      const turbo_flow_adapter_schema_t *schema;
      check_not_null(flow);
      config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
      config.pattern = cases[i].pattern;
      config.mode = cases[i].mode;
      config.transport = TURBO_FLOW_FMQ_TCP;
      config.host = "127.0.0.1";
      config.port = 7001 + (int)i;
      if (config.pattern == TURBO_FLOW_FMQ_DEALER) config.identity = "dealer-1";
      check_int_eq(fmq_register_private_adapter(flow, "fmq.test", &config), TURBO_OK);
      schema = turbo_flow_adapter_schema_at(flow, 0);
      check_not_null(schema);
      check_int_eq(schema->kind, TURBO_FLOW_ADAPTER_KIND_FMQ);
      check_int_eq(schema->roles, cases[i].roles);
      check_size_eq(schema->field_count, 59);
      turbo_flow_destroy(flow);
    }
  }

  it("exposes all CoroNet transport options") {
    turbo_flow_fmq_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *schema;
    const turbo_flow_option_field_t *pattern = NULL;
    const turbo_flow_option_field_t *transport = NULL;
    const turbo_flow_option_field_t *kcp_pre_shared_key = NULL;
    const turbo_flow_option_field_t *kcp_mtu = NULL;
    const turbo_flow_option_field_t *kcp_send_window = NULL;
    const turbo_flow_option_field_t *kcp_receive_window = NULL;
    const turbo_flow_option_field_t *kcp_interval_ms = NULL;
    const turbo_flow_option_field_t *kcp_handshake_retry_ms = NULL;
    const turbo_flow_option_field_t *kcp_fast_resend = NULL;
    const turbo_flow_option_field_t *kcp_congestion_control = NULL;
    const turbo_flow_option_field_t *kcp_fec_data_shards = NULL;
    const turbo_flow_option_field_t *kcp_fec_parity_shards = NULL;
    const turbo_flow_option_field_t *kcp_fec_max_payload_size = NULL;
    const turbo_flow_option_field_t *kcp_fec_receive_groups = NULL;
    const turbo_flow_option_field_t *content_type = NULL;
    const turbo_flow_option_field_t *content_binding = NULL;

    config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
    config.pattern = TURBO_FLOW_FMQ_PUB;
    config.mode = TURBO_FLOW_FMQ_BIND;
    config.transport = TURBO_FLOW_FMQ_TCP;
    config.host = "127.0.0.1";
    config.port = 7001;

    check_not_null(flow);
    check_int_eq(fmq_register_private_adapter(flow, "fmq.test", &config), TURBO_OK);
    schema = turbo_flow_adapter_schema_at(flow, 0);
    check_not_null(schema);
    const turbo_flow_option_field_t *connect_timeout_ms = NULL;
    const turbo_flow_option_field_t *send_timeout_ms = NULL;
    const turbo_flow_option_field_t *recv_timeout_ms = NULL;
    const turbo_flow_option_field_t *handshake_timeout_ms = NULL;
    const turbo_flow_option_field_t *reconnect_initial_ms = NULL;
    const turbo_flow_option_field_t *reconnect_max_ms = NULL;
    const turbo_flow_option_field_t *heartbeat_interval_ms = NULL;
    const turbo_flow_option_field_t *heartbeat_timeout_ms = NULL;
    const turbo_flow_option_field_t *event_callback = NULL;
    const turbo_flow_option_field_t *event_ctx = NULL;
    const turbo_flow_option_field_t *reuse_port = NULL;
    const turbo_flow_option_field_t *tcp_keepalive = NULL;
    const turbo_flow_option_field_t *tcp_keepalive_idle_ms = NULL;
    const turbo_flow_option_field_t *tcp_keepalive_interval_ms = NULL;
    const turbo_flow_option_field_t *tcp_keepalive_count = NULL;
    const turbo_flow_option_field_t *linger = NULL;
    const turbo_flow_option_field_t *linger_ms = NULL;
    const turbo_flow_option_field_t *send_hwm_bytes = NULL;
    const turbo_flow_option_field_t *udp_multicast_group = NULL;
    const turbo_flow_option_field_t *udp_multicast_interface = NULL;
    const turbo_flow_option_field_t *udp_option_flags = NULL;
    const turbo_flow_option_field_t *udp_multicast_loop = NULL;
    const turbo_flow_option_field_t *udp_multicast_ttl = NULL;
    const turbo_flow_option_field_t *udp_broadcast = NULL;
    const turbo_flow_option_field_t *frame_hwm_messages = NULL;
    const turbo_flow_option_field_t *frame_hwm_bytes = NULL;
    const turbo_flow_option_field_t *frame_admission_policy = NULL;
    const turbo_flow_option_field_t *frame_admission_timeout_ms = NULL;
    const turbo_flow_option_field_t *frame_linger_ms = NULL;
    const turbo_flow_option_field_t *peer_hwm_messages = NULL;
    const turbo_flow_option_field_t *peer_hwm_bytes = NULL;
    const turbo_flow_option_field_t *slow_peer_policy = NULL;
    for (size_t i = 0; i < schema->field_count; ++i) {
      if (strcmp(schema->fields[i].name, "pattern") == 0) pattern = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "transport") == 0) transport = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_pre_shared_key") == 0)
        kcp_pre_shared_key = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_mtu") == 0) kcp_mtu = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_send_window") == 0)
        kcp_send_window = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_receive_window") == 0)
        kcp_receive_window = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_interval_ms") == 0)
        kcp_interval_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_handshake_retry_ms") == 0)
        kcp_handshake_retry_ms = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_fast_resend") == 0)
        kcp_fast_resend = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_congestion_control") == 0)
        kcp_congestion_control = &schema->fields[i];
      if (strcmp(schema->fields[i].name, "kcp_fec_data_shards") == 0) {
        kcp_fec_data_shards = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "kcp_fec_parity_shards") == 0) {
        kcp_fec_parity_shards = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "kcp_fec_max_payload_size") == 0) {
        kcp_fec_max_payload_size = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "kcp_fec_receive_groups") == 0) {
        kcp_fec_receive_groups = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "content_type") == 0) {
        content_type = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "content_binding") == 0) {
        content_binding = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "connect_timeout_ms") == 0) {
        connect_timeout_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "send_timeout_ms") == 0) {
        send_timeout_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "recv_timeout_ms") == 0) {
        recv_timeout_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "handshake_timeout_ms") == 0) {
        handshake_timeout_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "reconnect_initial_ms") == 0) {
        reconnect_initial_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "reconnect_max_ms") == 0) {
        reconnect_max_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "heartbeat_interval_ms") == 0) {
        heartbeat_interval_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "heartbeat_timeout_ms") == 0) {
        heartbeat_timeout_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "event_callback") == 0) {
        event_callback = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "event_ctx") == 0) {
        event_ctx = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "reuse_port") == 0) {
        reuse_port = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "tcp_keepalive") == 0) {
        tcp_keepalive = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "tcp_keepalive_idle_ms") == 0) {
        tcp_keepalive_idle_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "tcp_keepalive_interval_ms") == 0) {
        tcp_keepalive_interval_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "tcp_keepalive_count") == 0) {
        tcp_keepalive_count = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "linger") == 0) {
        linger = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "linger_ms") == 0) {
        linger_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "send_hwm_bytes") == 0) {
        send_hwm_bytes = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "udp_multicast_group") == 0) {
        udp_multicast_group = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "udp_multicast_interface") == 0) {
        udp_multicast_interface = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "udp_option_flags") == 0) {
        udp_option_flags = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "udp_multicast_loop") == 0) {
        udp_multicast_loop = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "udp_multicast_ttl") == 0) {
        udp_multicast_ttl = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "udp_broadcast") == 0) {
        udp_broadcast = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "frame_hwm_messages") == 0) {
        frame_hwm_messages = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "frame_hwm_bytes") == 0) {
        frame_hwm_bytes = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "frame_admission_policy") == 0) {
        frame_admission_policy = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "frame_admission_timeout_ms") == 0) {
        frame_admission_timeout_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "frame_linger_ms") == 0) {
        frame_linger_ms = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "peer_hwm_messages") == 0) {
        peer_hwm_messages = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "peer_hwm_bytes") == 0) {
        peer_hwm_bytes = &schema->fields[i];
      }
      if (strcmp(schema->fields[i].name, "slow_peer_policy") == 0) {
        slow_peer_policy = &schema->fields[i];
      }
    }
    check_not_null(pattern);
    check_not_null(transport);
    check_not_null(kcp_pre_shared_key);
    check_not_null(kcp_mtu);
    check_not_null(kcp_send_window);
    check_not_null(kcp_receive_window);
    check_not_null(kcp_interval_ms);
    check_not_null(kcp_handshake_retry_ms);
    check_not_null(kcp_fast_resend);
    check_not_null(kcp_congestion_control);
    check_not_null(kcp_fec_data_shards);
    check_not_null(kcp_fec_parity_shards);
    check_not_null(kcp_fec_max_payload_size);
    check_not_null(kcp_fec_receive_groups);
    check_not_null(content_type);
    check_not_null(content_binding);
    check_int_eq(content_type->type, TURBO_FLOW_OPTION_STRING);
    check_int_eq(content_binding->type, TURBO_FLOW_OPTION_HOST_OBJECT);
    check_bits(content_binding->flags, TURBO_FLOW_OPTION_NOT_SERIALIZABLE);
    check_not_null(connect_timeout_ms);
    check_not_null(send_timeout_ms);
    check_not_null(recv_timeout_ms);
    check_not_null(handshake_timeout_ms);
    check_not_null(reconnect_initial_ms);
    check_not_null(reconnect_max_ms);
    check_not_null(heartbeat_interval_ms);
    check_not_null(heartbeat_timeout_ms);
    check_not_null(event_callback);
    check_not_null(event_ctx);
    check_not_null(reuse_port);
    check_not_null(tcp_keepalive);
    check_not_null(tcp_keepalive_idle_ms);
    check_not_null(tcp_keepalive_interval_ms);
    check_not_null(tcp_keepalive_count);
    check_not_null(linger);
    check_not_null(linger_ms);
    check_not_null(send_hwm_bytes);
    check_not_null(udp_multicast_group);
    check_not_null(udp_multicast_interface);
    check_not_null(udp_option_flags);
    check_not_null(udp_multicast_loop);
    check_not_null(udp_multicast_ttl);
    check_not_null(udp_broadcast);
    check_not_null(frame_hwm_messages);
    check_not_null(frame_hwm_bytes);
    check_not_null(frame_admission_policy);
    check_not_null(frame_admission_timeout_ms);
    check_not_null(frame_linger_ms);
    check_not_null(peer_hwm_messages);
    check_not_null(peer_hwm_bytes);
    check_not_null(slow_peer_policy);
    check_size_eq(pattern->enum_value_count, 11);
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_PUB - 1], "pub");
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_SUB - 1], "sub");
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_PUSH - 1], "push");
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_PULL - 1], "pull");
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_ROUTER - 1], "router");
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_DEALER - 1], "dealer");
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_PAIR - 1], "pair");
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_REQ - 1], "req");
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_REP - 1], "rep");
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_XPUB - 1], "xpub");
    check_str_eq(pattern->enum_values[TURBO_FLOW_FMQ_XSUB - 1], "xsub");
    check_size_eq(transport->enum_value_count, 7);
    check_str_eq(transport->enum_values[TURBO_FLOW_FMQ_TCP - 1], "tcp");
    check_str_eq(transport->enum_values[TURBO_FLOW_FMQ_TLS - 1], "tls");
    check_str_eq(transport->enum_values[TURBO_FLOW_FMQ_UDP - 1], "udp");
    check_str_eq(transport->enum_values[TURBO_FLOW_FMQ_KCP - 1], "kcp");
    check_str_eq(transport->enum_values[TURBO_FLOW_FMQ_PIPE - 1], "pipe");
    check_str_eq(transport->enum_values[TURBO_FLOW_FMQ_WS - 1], "ws");
    check_str_eq(transport->enum_values[TURBO_FLOW_FMQ_WSS - 1], "wss");
    check_size_eq(frame_admission_policy->enum_value_count, 3);
    check_str_eq(frame_admission_policy->enum_values[TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL], "fail");
    check_str_eq(frame_admission_policy->enum_values[TURBO_FLOW_FMQ_FRAME_ADMISSION_BLOCK],
                 "block");
    check_str_eq(frame_admission_policy->enum_values[TURBO_FLOW_FMQ_FRAME_ADMISSION_DROP_OLDEST],
                 "drop_oldest");
    check_size_eq(slow_peer_policy->enum_value_count, 3);
    check_str_eq(slow_peer_policy->enum_values[TURBO_FLOW_FMQ_SLOW_PEER_FAIL - 1], "fail");
    check_str_eq(slow_peer_policy->enum_values[TURBO_FLOW_FMQ_SLOW_PEER_DROP_OLDEST - 1],
                 "drop_oldest");
    check_str_eq(slow_peer_policy->enum_values[TURBO_FLOW_FMQ_SLOW_PEER_DISCONNECT - 1],
                 "disconnect");
    turbo_flow_destroy(flow);
  }
}

spec("flow_fmq_network") {
  it("retries a temporary disconnect until a subscriber becomes available") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    fmq_reconnect_start_ctx_t starter_ctx;
    turbo_thread_t starter;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    int publish_rc;
    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    publisher = fmq_make_retry_sink_flow("fmq.output", &pub_config, 5, 100);
    subscriber = fmq_make_source_flow("fmq.input", &sub_config, &capture);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    starter_ctx.flow = subscriber;
    starter_ctx.delay_ms = 40;
    atomic_init(&starter_ctx.rc, TURBO_EALREADY);
    check_int_eq(turbo_thread_create(&starter, fmq_start_flow_after_delay, &starter_ctx), TURBO_OK);
    publish_rc = fmq_publish_payload(publisher, "retried");
    check_int_eq(turbo_thread_join(&starter), TURBO_OK);
    check_int_eq(publish_rc, TURBO_OK);
    check_int_eq(atomic_load_explicit(&starter_ctx.rc, memory_order_acquire), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
  }

  it("does not retry non-retryable HWM failures") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    fmq_event_state_t events;
    turbo_flow_t *publisher;
    check_int_gt(port, 0);
    fmq_init_events(&events);
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    config.frame_hwm_bytes = FLOW_FMQ_HEADER_SIZE;
    config.event_callback = fmq_event_capture;
    config.event_ctx = &events;
    publisher = fmq_make_retry_sink_flow("fmq.output", &config, 3, 10);
    check_not_null(publisher);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "x"), TURBO_ENOSPC);
    check_int_eq(atomic_load_explicit(&events.hwm_reached, memory_order_acquire), 1);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(publisher);
  }

  it("interrupts retry delay on stop and resets it on restart") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    fmq_publish_thread_ctx_t publish_ctx;
    turbo_thread_t publish_thread;
    turbo_flow_t *publisher;
    uint64_t stop_started_ns;
    check_int_gt(port, 0);
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    publisher = fmq_make_retry_sink_flow("fmq.output", &config, 2, 500);
    check_not_null(publisher);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    memset(&publish_ctx, 0, sizeof(publish_ctx));
    publish_ctx.flow = publisher;
    publish_ctx.payload = "stop-retry";
    atomic_init(&publish_ctx.rc, TURBO_EALREADY);
    check_int_eq(turbo_thread_create(&publish_thread, fmq_publish_in_thread, &publish_ctx),
                 TURBO_OK);
    turbo_sleep_ms(50);
    stop_started_ns = turbo_hrtime();
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    check_true(turbo_hrtime() - stop_started_ns < UINT64_C(250000000));
    (void)turbo_thread_join(&publish_thread);
    check_int_eq(atomic_load_explicit(&publish_ctx.rc, memory_order_acquire), TURBO_ESHUTDOWN);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "restart"), TURBO_ENOTCONN);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(publisher);
  }

  it("fails PUB sends when no subscription matches") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    fmq_event_state_t events;
    turbo_flow_t *publisher;
    check_int_gt(port, 0);
    fmq_init_events(&events);
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    config.topic = "orders";
    config.event_callback = fmq_event_capture;
    config.event_ctx = &events;
    publisher = fmq_make_sink_flow("fmq.output", &config);
    check_not_null(publisher);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "not-dropped"), TURBO_ENOTCONN);
    check_int_eq(atomic_load_explicit(&events.frame_dropped, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&events.contract_valid, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&events.last_status, memory_order_acquire), TURBO_ENOTCONN);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(publisher);
  }

  it("rejects sends that exceed the FMQ frame byte high-water mark") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    fmq_event_state_t events;
    turbo_flow_t *publisher;
    check_int_gt(port, 0);
    fmq_init_events(&events);
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    config.frame_hwm_bytes = FLOW_FMQ_HEADER_SIZE;
    config.event_callback = fmq_event_capture;
    config.event_ctx = &events;
    publisher = fmq_make_sink_flow("fmq.output", &config);
    check_not_null(publisher);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "x"), TURBO_ENOSPC);
    check_int_eq(atomic_load_explicit(&events.hwm_reached, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&events.last_status, memory_order_acquire), TURBO_ENOSPC);
    check_int_gt(atomic_load_explicit(&events.last_frame_bytes, memory_order_acquire),
                 (int)FLOW_FMQ_HEADER_SIZE);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(publisher);
  }

  it("enforces message high-water mark across concurrent in-flight sends") {
    fmq_hwm_result_t result =
        fmq_run_hwm_case(FMQ_TEST_LARGE_PAYLOAD_SIZE, 1, 0, TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL, 0);
    check_int_eq(result.setup_status, TURBO_OK);
    check_int_eq(result.usage_status, TURBO_OK);
    check_size_eq(result.active_snapshot.in_flight_messages, 1);
    check_size_gt(result.active_snapshot.in_flight_bytes, FMQ_TEST_LARGE_PAYLOAD_SIZE);
    check_int_eq(result.rejected_status, TURBO_ENOSPC);
    check_int_eq(result.hwm_events, 1);
    check_int_eq(result.publish_status, TURBO_OK);
    check_size_eq(result.drained_snapshot.in_flight_messages, 0);
    check_size_eq(result.drained_snapshot.in_flight_bytes, 0);
    check_int_eq(result.drain_status, TURBO_OK);
    check_int_eq(result.stop_status, TURBO_OK);
    check_int_eq(result.peer_status, TURBO_OK);
  }

  it("enforces byte high-water mark across concurrent in-flight sends") {
    size_t frame_limit = fmq_test_encoded_payload_size(FMQ_TEST_LARGE_PAYLOAD_SIZE);
    fmq_hwm_result_t result = fmq_run_hwm_case(FMQ_TEST_LARGE_PAYLOAD_SIZE, 0, frame_limit,
                                               TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL, 0);
    check_int_eq(result.setup_status, TURBO_OK);
    check_int_eq(result.usage_status, TURBO_OK);
    check_size_eq(result.active_snapshot.in_flight_messages, 1);
    check_size_eq(result.active_snapshot.in_flight_bytes, frame_limit);
    check_int_eq(result.rejected_status, TURBO_ENOSPC);
    check_int_eq(result.hwm_events, 1);
    check_int_eq(result.publish_status, TURBO_OK);
    check_size_eq(result.drained_snapshot.in_flight_bytes, 0);
    check_size_eq(result.drained_snapshot.in_flight_messages, 0);
    check_int_eq(result.drain_status, TURBO_OK);
    check_int_eq(result.stop_status, TURBO_OK);
    check_int_eq(result.peer_status, TURBO_OK);
  }

  it("bounds blocked frame admission by its configured deadline") {
    fmq_hwm_result_t result = fmq_run_hwm_case(FMQ_TEST_LARGE_PAYLOAD_SIZE, 1, 0,
                                               TURBO_FLOW_FMQ_FRAME_ADMISSION_BLOCK, 50);
    check_int_eq(result.setup_status, TURBO_OK);
    check_int_eq(result.usage_status, TURBO_OK);
    check_int_eq(result.rejected_status, TURBO_ETIMEDOUT);
    check_int_eq(result.hwm_events, 1);
    check_int_eq(result.drain_status, TURBO_OK);
    check_int_eq(result.stop_status, TURBO_OK);
    check_int_eq(result.peer_status, TURBO_OK);
  }

  it("drops the oldest queued frame while preserving the active send") {
    fmq_drop_oldest_result_t result = fmq_run_drop_oldest_case();
    check_int_eq(result.setup_status, TURBO_OK);
    check_int_eq(result.first_status, TURBO_OK);
    check_int_eq(result.second_status, TURBO_ECANCELED);
    check_int_eq(result.third_status, TURBO_OK);
    check_int_eq(result.hwm_events, 1);
    check_int_eq(result.dropped_events, 1);
    check_int_eq(result.dropped_status, TURBO_ECANCELED);
    check_int_eq(result.connection_snapshot.kind, TURBO_FLOW_RESOURCE_CONNECTION);
    check_int_eq(result.saturated_snapshot.kind, TURBO_FLOW_RESOURCE_QUEUE_BUFFER);
    check_int_eq(result.saturated_snapshot.saturated, 1);
    check_int_eq(result.saturated_snapshot.load, 2);
    check_int_eq(result.saturated_snapshot.capacity, 2);
    check_int_eq(result.stop_status, TURBO_OK);
    check_int_eq(result.peer_status, TURBO_OK);
    check_size_eq(result.drained_snapshot.in_flight_messages, 0);
    check_size_eq(result.drained_snapshot.in_flight_bytes, 0);
  }

  it("drains an in-flight frame before the linger deadline") {
    fmq_linger_result_t result = fmq_run_linger_case(FMQ_TEST_LARGE_PAYLOAD_SIZE, 500, 30);
    check_int_eq(result.setup_status, TURBO_OK);
    check_int_eq(result.usage_status, TURBO_OK);
    check_size_eq(result.active_snapshot.in_flight_messages, 1);
    check_size_gt(result.active_snapshot.in_flight_bytes, FMQ_TEST_LARGE_PAYLOAD_SIZE);
    check_int_eq(result.stop_status, TURBO_OK);
    check_true(result.stop_elapsed_ns >= UINT64_C(20000000));
    check_true(result.stop_elapsed_ns < FMQ_TEST_STOP_LIMIT_NS);
    check_int_eq(result.publish_status, TURBO_OK);
    check_int_eq(result.peer_status, TURBO_OK);
    check_int_eq(result.stopped_snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_size_eq(result.stopped_snapshot.in_flight_messages, 0);
    check_size_eq(result.stopped_snapshot.in_flight_bytes, 0);
  }

  it("bounds stop by the frame linger deadline") {
    fmq_linger_result_t result = fmq_run_linger_case(FMQ_TEST_LARGE_PAYLOAD_SIZE, 80, 0);
    check_int_eq(result.setup_status, TURBO_OK);
    check_int_eq(result.usage_status, TURBO_OK);
    check_int_eq(result.stop_status, TURBO_OK);
    check_true(result.stop_elapsed_ns < FMQ_TEST_STOP_LIMIT_NS);
    check_true(result.publish_status == TURBO_OK || result.publish_status == TURBO_ESHUTDOWN);
    check_int_eq(result.peer_status, TURBO_OK);
    check_int_eq(result.stopped_snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_size_eq(result.stopped_snapshot.in_flight_messages, 0);
    check_size_eq(result.stopped_snapshot.in_flight_bytes, 0);
  }

  it("closes immediately when frame linger is disabled") {
    fmq_linger_result_t result = fmq_run_linger_case(FMQ_TEST_LARGE_PAYLOAD_SIZE, 0, 0);
    check_int_eq(result.setup_status, TURBO_OK);
    check_int_eq(result.usage_status, TURBO_OK);
    check_int_eq(result.stop_status, TURBO_OK);
    check_true(result.stop_elapsed_ns < FMQ_TEST_STOP_LIMIT_NS);
    check_int_eq(result.publish_status, TURBO_ESHUTDOWN);
    check_int_eq(result.peer_status, TURBO_OK);
    check_int_eq(result.stopped_snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_size_eq(result.stopped_snapshot.in_flight_messages, 0);
    check_size_eq(result.stopped_snapshot.in_flight_bytes, 0);
  }

  it("reconnects and receives after delayed broker startup") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    turbo_flow_t *publisher = NULL;
    turbo_flow_t *subscriber = NULL;
    turbo_thread_t starter;
    fmq_reconnect_start_ctx_t starter_ctx;
    const char payload[] = "reconnected";
    int publish_result;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    sub_config.reconnect_initial_ms = 20;
    sub_config.reconnect_max_ms = 120;

    publisher = fmq_make_sink_flow("fmq.output", &pub_config);
    subscriber = fmq_make_source_flow("fmq.input", &sub_config, &capture);
    check_not_null(publisher);
    check_not_null(subscriber);

    atomic_init(&starter_ctx.rc, 0);
    starter_ctx.flow = publisher;
    starter_ctx.delay_ms = 100;
    check_int_eq(turbo_thread_create(&starter, fmq_start_flow_after_delay, &starter_ctx), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    (void)turbo_thread_join(&starter);
    check_int_eq(atomic_load(&starter_ctx.rc), TURBO_OK);

    publish_result = fmq_publish_payload(publisher, payload);
    check_int_eq(publish_result, TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, sizeof(payload) - 1);
    check_mem_eq(capture.payload, payload, sizeof(payload) - 1);

    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
  }

  it("moves a connected subscriber to a replacement endpoint") {
    unsigned short first_port = fmq_test_port();
    unsigned short second_port = fmq_test_port();
    turbo_flow_fmq_config_t first_config;
    turbo_flow_fmq_config_t second_config;
    turbo_flow_fmq_config_t subscriber_config;
    turbo_flow_adapter_command_t command;
    turbo_flow_connection_snapshot_t snapshot;
    fmq_capture_state_t capture;
    turbo_flow_t *first;
    turbo_flow_t *second;
    turbo_flow_t *subscriber;

    check_int_gt(first_port, 0);
    for (int attempt = 0; attempt < 8 && second_port == first_port; ++attempt)
      second_port = fmq_test_port();
    check_int_gt(second_port, 0);
    check_int_ne(first_port, second_port);
    fmq_init_capture(&capture);
    fmq_config(&first_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, first_port);
    fmq_config(&second_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, second_port);
    fmq_config(&subscriber_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, first_port);
    first = fmq_make_sink_flow("fmq.output", &first_config);
    second = fmq_make_sink_flow("fmq.output", &second_config);
    subscriber = fmq_make_source_flow("fmq.input", &subscriber_config, &capture);
    check_not_null(first);
    check_not_null(second);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(first), TURBO_OK);
    check_int_eq(turbo_flow_start(second), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(first, 1u), TURBO_OK);
    check_int_eq(fmq_publish_payload(first, "first-endpoint"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);

    memset(&command, 0, sizeof(command));
    command.size = sizeof(command);
    command.kind = TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT;
    command.endpoint.host = "127.0.0.1";
    command.endpoint.port = (int)second_port;
    check_int_eq(turbo_flow_adapter_command(subscriber, "fmq.input", &command), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(first, 0u), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(second, 1u), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(subscriber, 0, &snapshot), TURBO_OK);
    {
      char expected[64];
      (void)snprintf(expected, sizeof(expected), "tcp://127.0.0.1:%u", second_port);
      check_str_eq(snapshot.endpoint, expected);
    }
    check_int_eq(fmq_publish_payload(first, "stale-endpoint"), TURBO_ENOTCONN);
    check_int_eq(fmq_publish_payload(second, "second-endpoint"), TURBO_OK);
    fmq_wait_called(&capture, 2);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
    check_size_eq(capture.payload_len, 15u);
    check_mem_eq(capture.payload, "second-endpoint", 15u);

    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(second), TURBO_OK);
    check_int_eq(turbo_flow_stop(first), TURBO_OK);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(second);
    turbo_flow_destroy(first);
  }

  it("recovers after an established broker disconnect without replaying delivered messages") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    fmq_event_state_t events;
    turbo_flow_connection_snapshot_t snapshot;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    uint64_t stop_started_ns;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_init_events(&events);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    sub_config.reconnect_initial_ms = 20;
    sub_config.reconnect_max_ms = 80;
    sub_config.event_callback = fmq_event_capture;
    sub_config.event_ctx = &events;
    publisher = fmq_make_sink_flow("fmq.output", &pub_config);
    subscriber = fmq_make_source_flow("fmq.input", &sub_config, &capture);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    fmq_wait_event_count(&events.reconnect_succeeded, 1);
    check_int_eq(fmq_publish_payload(publisher, "before-disconnect"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);

    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    fmq_wait_event_count(&events.reconnect_scheduled, 1);
    check_int_ge(atomic_load_explicit(&events.reconnect_scheduled, memory_order_acquire), 1);
    check_int_ge(atomic_load_explicit(&events.last_reconnect_delay_ms, memory_order_acquire), 10);
    check_int_lt(atomic_load_explicit(&events.last_reconnect_delay_ms, memory_order_acquire), 20);
    fmq_wait_event_count(&events.reconnect_scheduled, 2);
    check_int_ge(atomic_load_explicit(&events.reconnect_scheduled, memory_order_acquire), 2);
    check_int_ge(atomic_load_explicit(&events.last_reconnect_delay_ms, memory_order_acquire), 20);
    check_int_lt(atomic_load_explicit(&events.last_reconnect_delay_ms, memory_order_acquire), 40);
    check_int_eq(fmq_wait_connection_state(subscriber, TURBO_FLOW_CONNECTION_BACKOFF, &snapshot),
                 TURBO_OK);
    check_size_eq(snapshot.connections_current, 0);

    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(fmq_wait_connection_state(subscriber, TURBO_FLOW_CONNECTION_READY, &snapshot),
                 TURBO_OK);
    fmq_wait_event_count(&events.reconnect_succeeded, 2);
    check_int_ge(atomic_load_explicit(&events.reconnect_succeeded, memory_order_acquire), 2);
    turbo_sleep_ms(50);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_int_eq(fmq_publish_payload(publisher, "after-reconnect"), TURBO_OK);
    fmq_wait_called(&capture, 2);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
    check_size_eq(capture.payload_len, 15);
    check_mem_eq(capture.payload, "after-reconnect", 15);

    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    fmq_wait_event_count(&events.reconnect_scheduled, 3);
    check_int_ge(atomic_load_explicit(&events.reconnect_scheduled, memory_order_acquire), 3);
    stop_started_ns = turbo_hrtime();
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_true(turbo_hrtime() - stop_started_ns < UINT64_C(250000000));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(subscriber, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(snapshot.last_status, TURBO_ESHUTDOWN);
    check_size_eq(snapshot.connections_current, 0);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
  }

  it("survives repeated real TCP broker restarts without replay or slow shutdown") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    fmq_event_state_t events;
    turbo_flow_connection_snapshot_t snapshot;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    int expected_calls = 1;
    int final_reconnect_scheduled_before;
    int cycle;
    uint64_t stop_started_ns;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_init_events(&events);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    sub_config.reconnect_initial_ms = 20;
    sub_config.reconnect_max_ms = 80;
    sub_config.event_callback = fmq_event_capture;
    sub_config.event_ctx = &events;
    publisher = fmq_make_sink_flow("fmq.output", &pub_config);
    subscriber = fmq_make_source_flow("fmq.input", &sub_config, &capture);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    fmq_wait_event_count(&events.reconnect_succeeded, 1);
    check_int_eq(fmq_wait_connection_state(subscriber, TURBO_FLOW_CONNECTION_READY, &snapshot),
                 TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "soak-baseline"), TURBO_OK);
    fmq_wait_called(&capture, expected_calls);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), expected_calls);

    for (cycle = 0; cycle < FMQ_TEST_RECONNECT_SOAK_CYCLES; ++cycle) {
      char payload[32];
      int payload_len;
      int reconnect_scheduled_before =
          atomic_load_explicit(&events.reconnect_scheduled, memory_order_acquire);
      int reconnect_succeeded_before =
          atomic_load_explicit(&events.reconnect_succeeded, memory_order_acquire);

      check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
      fmq_wait_event_count(&events.reconnect_scheduled, reconnect_scheduled_before + 1);
      check_int_ge(atomic_load_explicit(&events.reconnect_scheduled, memory_order_acquire),
                   reconnect_scheduled_before + 1);
      check_int_eq(fmq_wait_connection_count(subscriber, 0), TURBO_OK);
      turbo_sleep_ms(FMQ_TEST_POST_SETTLE_MS);
      check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), expected_calls);

      check_int_eq(turbo_flow_start(publisher), TURBO_OK);
      fmq_wait_event_count(&events.reconnect_succeeded, reconnect_succeeded_before + 1);
      check_int_ge(atomic_load_explicit(&events.reconnect_succeeded, memory_order_acquire),
                   reconnect_succeeded_before + 1);
      check_int_eq(fmq_wait_connection_state(subscriber, TURBO_FLOW_CONNECTION_READY, &snapshot),
                   TURBO_OK);
      turbo_sleep_ms(FMQ_TEST_POST_SETTLE_MS);
      check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), expected_calls);

      payload_len = snprintf(payload, sizeof(payload), "soak-cycle-%d", cycle);
      check_int_gt(payload_len, 0);
      check_int_lt(payload_len, (int)sizeof(payload));
      check_int_eq(fmq_publish_payload(publisher, payload), TURBO_OK);
      expected_calls += 1;
      fmq_wait_called(&capture, expected_calls);
      check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), expected_calls);
      check_size_eq(capture.payload_len, (size_t)payload_len);
      check_mem_eq(capture.payload, payload, (size_t)payload_len);
    }

    final_reconnect_scheduled_before =
        atomic_load_explicit(&events.reconnect_scheduled, memory_order_acquire);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    fmq_wait_event_count(&events.reconnect_scheduled, final_reconnect_scheduled_before + 1);
    check_int_ge(atomic_load_explicit(&events.reconnect_scheduled, memory_order_acquire),
                 final_reconnect_scheduled_before + 1);
    check_int_eq(fmq_wait_connection_count(subscriber, 0), TURBO_OK);
    stop_started_ns = turbo_hrtime();
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_true(turbo_hrtime() - stop_started_ns < FMQ_TEST_STOP_LIMIT_NS);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(subscriber, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(snapshot.last_status, TURBO_ESHUTDOWN);
    check_size_eq(snapshot.connections_current, 0);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), expected_calls);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
  }

  it("releases failed sends during disconnect and reconnects only future sends") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    turbo_flow_connection_snapshot_t snapshot;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    int disconnected_status;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    publisher = fmq_make_sink_flow("fmq.output", &pub_config);
    subscriber = fmq_make_source_flow("fmq.input", &sub_config, &capture);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(publisher, 1), TURBO_OK);

    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(publisher, 0), TURBO_OK);
    disconnected_status = fmq_publish_payload(publisher, "during-disconnect");
    check_int_ne(disconnected_status, TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(publisher, 0, &snapshot), TURBO_OK);
    check_size_eq(snapshot.in_flight_messages, 0);
    check_size_eq(snapshot.in_flight_bytes, 0);

    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(publisher, 1), TURBO_OK);
    turbo_sleep_ms(50);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 0);
    check_int_eq(fmq_publish_payload(publisher, "after-disconnect"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 16);
    check_mem_eq(capture.payload, "after-disconnect", 16);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);

    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
  }

  it("times out reconnect startup when broker stays unavailable") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    fmq_capture_state_t capture;
    turbo_flow_connection_snapshot_t snapshot;
    turbo_flow_t *subscriber = NULL;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_config(&config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    config.connect_timeout_ms = 120;
    config.reconnect_initial_ms = 20;
    config.reconnect_max_ms = 40;

    subscriber = fmq_make_source_flow("fmq.input", &config, &capture);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(subscriber), TURBO_ETIMEDOUT);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(subscriber, 0, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_FAILED);
    check_int_eq(snapshot.last_status, TURBO_ETIMEDOUT);
    check_size_eq(snapshot.connections_current, 0u);
    turbo_flow_destroy(subscriber);
  }

  it("does not schedule reconnect when explicitly disabled") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    fmq_capture_state_t capture;
    fmq_event_state_t events;
    turbo_flow_t *subscriber = NULL;
    int start_rc;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_init_events(&events);
    fmq_config(&config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    config.connect_timeout_ms = 120;
    config.reconnect_initial_ms = TURBO_FLOW_FMQ_RECONNECT_DISABLED;
    config.event_callback = fmq_event_capture;
    config.event_ctx = &events;

    subscriber = fmq_make_source_flow("fmq.input", &config, &capture);
    check_not_null(subscriber);
    start_rc = turbo_flow_start(subscriber);
    check_int_ne(start_rc, TURBO_OK);
    check_int_eq(atomic_load_explicit(&events.reconnect_scheduled, memory_order_acquire), 0);
    check_int_eq(atomic_load_explicit(&events.reconnect_failed, memory_order_acquire), 1);
    turbo_flow_destroy(subscriber);
  }

  it("keeps a quiet PUB SUB connection alive with heartbeat control frames") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    fmq_event_state_t events;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    turbo_flow_connection_snapshot_t publisher_connection;
    turbo_flow_connection_snapshot_t subscriber_connection;
    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_init_events(&events);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    pub_config.heartbeat_interval_ms = 20;
    pub_config.heartbeat_timeout_ms = 1000;
    pub_config.event_callback = fmq_event_capture;
    pub_config.event_ctx = &events;
    sub_config.heartbeat_interval_ms = 20;
    sub_config.heartbeat_timeout_ms = 1000;
    sub_config.event_callback = fmq_event_capture;
    sub_config.event_ctx = &events;
    publisher = fmq_make_sink_flow("fmq.output", &pub_config);
    subscriber = fmq_make_source_flow("fmq.input", &sub_config, &capture);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    fmq_wait_event_count(&events.peer_connected, 1);
    fmq_wait_event_count(&events.reconnect_succeeded, 1);
    check_int_ge(atomic_load_explicit(&events.peer_connected, memory_order_acquire), 1);
    check_int_ge(atomic_load_explicit(&events.reconnect_succeeded, memory_order_acquire), 1);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(publisher, 0, &publisher_connection),
                 TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(subscriber, 0, &subscriber_connection),
                 TURBO_OK);
    check_int_eq(publisher_connection.state, TURBO_FLOW_CONNECTION_READY);
    check_size_ge(publisher_connection.connections_current, 1);
    check_int_eq(subscriber_connection.state, TURBO_FLOW_CONNECTION_READY);
    check_size_eq(subscriber_connection.connections_current, 1);
    turbo_sleep_ms(90);
    check_int_eq(fmq_publish_payload(publisher, "after-heartbeat"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 15);
    check_mem_eq(capture.payload, "after-heartbeat", 15);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(publisher, 0, &publisher_connection),
                 TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(subscriber, 0, &subscriber_connection),
                 TURBO_OK);
    check_int_eq(publisher_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(publisher_connection.last_status, TURBO_ESHUTDOWN);
    check_size_eq(publisher_connection.connections_current, 0);
    check_int_eq(subscriber_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(subscriber_connection.last_status, TURBO_ESHUTDOWN);
    check_size_eq(subscriber_connection.connections_current, 0);
    fmq_wait_event_count(&events.peer_disconnected, 1);
    check_int_ge(atomic_load_explicit(&events.peer_disconnected, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&events.heartbeat_timeout, memory_order_acquire), 0);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
  }

  it("removes a silent peer after one heartbeat timeout event") {
    fmq_silent_peer_t silent_peer;
    unsigned short port = fmq_silent_peer_open(&silent_peer);
    turbo_flow_fmq_config_t config;
    fmq_capture_state_t capture;
    fmq_event_state_t events;
    turbo_flow_connection_snapshot_t snapshot;
    turbo_thread_t peer_thread;
    turbo_flow_t *subscriber;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_init_events(&events);
    check_int_eq(turbo_thread_create(&peer_thread, fmq_silent_peer_thread, &silent_peer), TURBO_OK);
    fmq_config(&config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    config.heartbeat_interval_ms = 20;
    config.heartbeat_timeout_ms = 80;
    config.reconnect_initial_ms = 250;
    config.reconnect_max_ms = 250;
    config.event_callback = fmq_event_capture;
    config.event_ctx = &events;
    subscriber = fmq_make_source_flow("fmq.input", &config, &capture);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    fmq_wait_event_count(&events.heartbeat_timeout, 1);
    check_int_eq(atomic_load_explicit(&silent_peer.saw_ping, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&events.heartbeat_timeout, memory_order_acquire), 1);
    fmq_wait_event_count(&events.reconnect_scheduled, 1);
    check_int_ge(atomic_load_explicit(&events.reconnect_scheduled, memory_order_acquire), 1);
    check_int_eq(fmq_wait_connection_state(subscriber, TURBO_FLOW_CONNECTION_BACKOFF, &snapshot),
                 TURBO_OK);
    check_size_eq(snapshot.connections_current, 0);
    turbo_sleep_ms(100);
    check_int_eq(atomic_load_explicit(&events.heartbeat_timeout, memory_order_acquire), 1);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_thread_join(&peer_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&silent_peer.status, memory_order_acquire), TURBO_OK);
    turbo_flow_destroy(subscriber);
#ifdef _WIN32
    WSACleanup();
#endif
  }

  it("fans PUB messages out to matching SUB topic prefixes") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_a_config;
    turbo_flow_fmq_config_t sub_b_config;
    turbo_flow_fmq_config_t filtered_config;
    fmq_capture_state_t sub_a;
    fmq_capture_state_t sub_b;
    fmq_capture_state_t filtered;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber_a;
    turbo_flow_t *subscriber_b;
    turbo_flow_t *subscriber_filtered;
    check_int_gt(port, 0);
    fmq_init_capture(&sub_a);
    fmq_init_capture(&sub_b);
    fmq_init_capture(&filtered);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_a_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    fmq_config(&sub_b_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    fmq_config(&filtered_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    pub_config.topic = "orders.created";
    sub_a_config.topic = "orders.";
    sub_b_config.topic = "orders.created";
    filtered_config.topic = "metrics.";
    publisher = fmq_make_sink_flow("fmq.output", &pub_config);
    subscriber_a = fmq_make_source_flow("fmq.input", &sub_a_config, &sub_a);
    subscriber_b = fmq_make_source_flow("fmq.input", &sub_b_config, &sub_b);
    subscriber_filtered = fmq_make_source_flow("fmq.input", &filtered_config, &filtered);
    check_not_null(publisher);
    check_not_null(subscriber_a);
    check_not_null(subscriber_b);
    check_not_null(subscriber_filtered);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber_a), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber_b), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber_filtered), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "order-7"), TURBO_OK);
    fmq_wait_called(&sub_a, 1);
    fmq_wait_called(&sub_b, 1);
    check_int_eq(atomic_load_explicit(&sub_a.called, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&sub_b.called, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&filtered.called, memory_order_acquire), 0);
    check_size_eq(sub_a.payload_len, 7);
    check_mem_eq(sub_a.payload, "order-7", 7);
    check_size_eq(sub_a.topic_len, 14);
    check_mem_eq(sub_a.topic, "orders.created", 14);
    check_int_eq(turbo_flow_stop(subscriber_filtered), TURBO_OK);
    check_int_eq(turbo_flow_stop(subscriber_b), TURBO_OK);
    check_int_eq(turbo_flow_stop(subscriber_a), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(subscriber_filtered);
    turbo_flow_destroy(subscriber_b);
    turbo_flow_destroy(subscriber_a);
    turbo_flow_destroy(publisher);
  }

  it("drops only a slow peer backlog and keeps a fast subscriber live") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t fast_config;
    turbo_flow_fmq_fanout_config_t fanout = TURBO_FLOW_FMQ_FANOUT_CONFIG_INIT;
    fmq_slow_peer_t slow;
    fmq_event_state_t events;
    fmq_count_capture_t fast;
    turbo_thread_t slow_thread;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    char *payload;
    int published = 0;
    int slow_sent_before_release;

    check_int_gt(port, 0);
    payload = (char *)malloc(FMQ_TEST_LARGE_PAYLOAD_SIZE);
    check_not_null(payload);
    memset(payload, 'f', FMQ_TEST_LARGE_PAYLOAD_SIZE);
    fmq_init_slow_peer(&slow, port);
    fmq_init_events(&events);
    fmq_init_count_capture(&fast);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    pub_config.send_timeout_ms = 30000u;
    pub_config.event_callback = fmq_event_capture;
    pub_config.event_ctx = &events;
    fmq_config(&fast_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    fast_config.identity = "fast-peer";
    fast_config.recv_timeout_ms = 30000u;
    fanout.peer_hwm_messages = 2u;
    fanout.slow_peer_policy = TURBO_FLOW_FMQ_SLOW_PEER_DROP_OLDEST;
    publisher = fmq_make_fanout_sink_flow("fmq.output", &pub_config, &fanout);
    subscriber = fmq_make_count_source_flow("fmq.input", &fast_config, &fast);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_thread_create(&slow_thread, fmq_slow_peer_thread, &slow), TURBO_OK);
    check_int_eq(fmq_wait_slow_peer(&slow), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(publisher, 1u), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(publisher, 2u), TURBO_OK);
    turbo_sleep_ms(100u);

    for (int i = 0;
         i < 64 && atomic_load_explicit(&events.slow_peer_dropped, memory_order_acquire) == 0;
         ++i) {
      check_int_eq(fmq_publish_bytes(publisher, payload, FMQ_TEST_LARGE_PAYLOAD_SIZE), TURBO_OK);
      published += 1;
      fmq_wait_count_called(&fast, published);
      check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire), published);
    }
    fmq_wait_event_count(&events.slow_peer_dropped, 1);
    fmq_wait_count_called(&fast, published);
    check_int_gt(atomic_load_explicit(&events.slow_peer_hwm, memory_order_acquire), 0);
    check_int_gt(atomic_load_explicit(&events.slow_peer_dropped, memory_order_acquire), 0);
    check_size_eq(atomic_load_explicit(&events.last_peer_identity_len, memory_order_acquire), 9u);
    check_mem_eq(events.last_peer_identity, "slow-peer", 9u);
    check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire), published);

    {
      int drops_before_sustained =
          atomic_load_explicit(&events.slow_peer_dropped, memory_order_acquire);
      for (int i = 0; i < 5; ++i) {
        check_int_eq(fmq_publish_bytes(publisher, payload, FMQ_TEST_LARGE_PAYLOAD_SIZE), TURBO_OK);
        published += 1;
        fmq_wait_count_called(&fast, published);
        check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire), published);
      }
      fmq_wait_event_count(&events.slow_peer_dropped, drops_before_sustained + 1);
      check_int_gt(atomic_load_explicit(&events.slow_peer_dropped, memory_order_acquire),
                   drops_before_sustained);
    }

    slow_sent_before_release = atomic_load_explicit(&events.slow_peer_sent, memory_order_acquire);
    atomic_store_explicit(&slow.release_reads, 1, memory_order_release);
    fmq_wait_event_count(&events.slow_peer_sent, slow_sent_before_release + 1);
    check_int_eq(fmq_publish_payload(publisher, "recovered"), TURBO_OK);
    fmq_wait_count_called(&fast, published + 1);
    check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire), published + 1);
    check_size_eq(atomic_load_explicit(&fast.last_payload_size, memory_order_acquire), 9u);

    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    check_int_eq(turbo_thread_join(&slow_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&slow.status, memory_order_acquire), TURBO_OK);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
    free(payload);
  }

  it("rejects a full fan-out atomically without sending to a fast peer") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t fast_config;
    turbo_flow_fmq_fanout_config_t fanout = TURBO_FLOW_FMQ_FANOUT_CONFIG_INIT;
    fmq_slow_peer_t slow;
    fmq_event_state_t events;
    fmq_count_capture_t fast;
    turbo_thread_t slow_thread;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    char *payload;
    int accepted = 0;
    int rejected = TURBO_EALREADY;
    int slow_sent_before_release;

    check_int_gt(port, 0);
    payload = (char *)malloc(FMQ_TEST_LARGE_PAYLOAD_SIZE);
    check_not_null(payload);
    memset(payload, 'a', FMQ_TEST_LARGE_PAYLOAD_SIZE);
    fmq_init_slow_peer(&slow, port);
    fmq_init_events(&events);
    fmq_init_count_capture(&fast);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    pub_config.send_timeout_ms = 30000u;
    pub_config.event_callback = fmq_event_capture;
    pub_config.event_ctx = &events;
    fmq_config(&fast_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    fast_config.identity = "fast-peer";
    fast_config.recv_timeout_ms = 30000u;
    fanout.peer_hwm_messages = 2u;
    fanout.slow_peer_policy = TURBO_FLOW_FMQ_SLOW_PEER_FAIL;
    publisher = fmq_make_fanout_sink_flow("fmq.output", &pub_config, &fanout);
    subscriber = fmq_make_count_source_flow("fmq.input", &fast_config, &fast);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_thread_create(&slow_thread, fmq_slow_peer_thread, &slow), TURBO_OK);
    check_int_eq(fmq_wait_slow_peer(&slow), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(publisher, 2u), TURBO_OK);

    for (int i = 0; i < 64 && rejected == TURBO_EALREADY; ++i) {
      int rc = fmq_publish_bytes(publisher, payload, FMQ_TEST_LARGE_PAYLOAD_SIZE);
      if (rc == TURBO_OK) {
        accepted += 1;
        fmq_wait_count_called(&fast, accepted);
        check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire), accepted);
      } else {
        rejected = rc;
      }
    }
    check_int_eq(rejected, TURBO_ENOSPC);
    fmq_wait_event_count(&events.slow_peer_hwm, 1);
    turbo_sleep_ms(50u);
    check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire), accepted);
    check_size_eq(atomic_load_explicit(&events.last_peer_identity_len, memory_order_acquire), 9u);
    check_mem_eq(events.last_peer_identity, "slow-peer", 9u);

    slow_sent_before_release = atomic_load_explicit(&events.slow_peer_sent, memory_order_acquire);
    atomic_store_explicit(&slow.release_reads, 1, memory_order_release);
    fmq_wait_event_count(&events.slow_peer_sent, slow_sent_before_release + 1);
    check_int_eq(fmq_publish_payload(publisher, "after-fail"), TURBO_OK);
    fmq_wait_count_called(&fast, accepted + 1);
    check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire), accepted + 1);

    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    check_int_eq(turbo_thread_join(&slow_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&slow.status, memory_order_acquire), TURBO_OK);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
    free(payload);
  }

  it("disconnects only the saturated peer and preserves fast fan-out") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t fast_config;
    turbo_flow_fmq_fanout_config_t fanout = TURBO_FLOW_FMQ_FANOUT_CONFIG_INIT;
    fmq_slow_peer_t slow;
    fmq_event_state_t events;
    fmq_count_capture_t fast;
    turbo_thread_t slow_thread;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    char *payload;
    int published = 0;

    check_int_gt(port, 0);
    payload = (char *)malloc(FMQ_TEST_LARGE_PAYLOAD_SIZE);
    check_not_null(payload);
    memset(payload, 'd', FMQ_TEST_LARGE_PAYLOAD_SIZE);
    fmq_init_slow_peer(&slow, port);
    fmq_init_events(&events);
    fmq_init_count_capture(&fast);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    pub_config.send_timeout_ms = 30000u;
    pub_config.event_callback = fmq_event_capture;
    pub_config.event_ctx = &events;
    fmq_config(&fast_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    fast_config.identity = "fast-peer";
    fast_config.recv_timeout_ms = 30000u;
    fanout.peer_hwm_messages = 2u;
    fanout.slow_peer_policy = TURBO_FLOW_FMQ_SLOW_PEER_DISCONNECT;
    publisher = fmq_make_fanout_sink_flow("fmq.output", &pub_config, &fanout);
    subscriber = fmq_make_count_source_flow("fmq.input", &fast_config, &fast);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_thread_create(&slow_thread, fmq_slow_peer_thread, &slow), TURBO_OK);
    check_int_eq(fmq_wait_slow_peer(&slow), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(publisher, 2u), TURBO_OK);

    for (int i = 0;
         i < 64 && atomic_load_explicit(&events.slow_peer_disconnected, memory_order_acquire) == 0;
         ++i) {
      check_int_eq(fmq_publish_bytes(publisher, payload, FMQ_TEST_LARGE_PAYLOAD_SIZE), TURBO_OK);
      published += 1;
      fmq_wait_count_called(&fast, published);
      check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire), published);
    }
    fmq_wait_event_count(&events.slow_peer_disconnected, 1);
    check_int_gt(atomic_load_explicit(&events.slow_peer_hwm, memory_order_acquire), 0);
    check_int_gt(atomic_load_explicit(&events.slow_peer_disconnected, memory_order_acquire), 0);
    check_size_eq(atomic_load_explicit(&events.last_peer_identity_len, memory_order_acquire), 9u);
    check_mem_eq(events.last_peer_identity, "slow-peer", 9u);
    check_int_eq(fmq_wait_connection_count(publisher, 1u), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "after-disconnect"), TURBO_OK);
    fmq_wait_count_called(&fast, published + 1);
    check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire), published + 1);

    atomic_store_explicit(&slow.release_reads, 1, memory_order_release);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    check_int_eq(turbo_thread_join(&slow_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&slow.status, memory_order_acquire), TURBO_OK);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
    free(payload);
  }

  it("updates XPUB fan-out when XSUB subscriptions reconnect") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t xpub_config;
    turbo_flow_fmq_config_t orders_config;
    turbo_flow_fmq_config_t metrics_config;
    fmq_capture_state_t xpub_events;
    fmq_capture_state_t orders;
    fmq_capture_state_t metrics;
    turbo_flow_t *publisher;
    turbo_flow_t *orders_subscriber;
    turbo_flow_t *metrics_subscriber;

    check_int_gt(port, 0);
    fmq_init_capture(&xpub_events);
    fmq_init_capture(&orders);
    fmq_init_capture(&metrics);
    fmq_config(&xpub_config, TURBO_FLOW_FMQ_XPUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&orders_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    fmq_config(&metrics_config, TURBO_FLOW_FMQ_XSUB, TURBO_FLOW_FMQ_CONNECT, port);
    xpub_config.topic = "orders.created";
    orders_config.topic = "orders.";
    metrics_config.topic = "metrics.";
    publisher = turbo_flow_create();
    check_not_null(publisher);
    check_int_eq(fmq_register_private_adapter(publisher, "fmq.xpub", &xpub_config), TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(publisher, "capture", fmq_capture, &xpub_events, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(publisher,
                                         "source subscriptions adapter fmq.xpub\n"
                                         "source input\n"
                                         "stage send adapter fmq.xpub\n"
                                         "stage capture\n"
                                         "stage main {\n"
                                         "  subscriptions -> capture\n"
                                         "  input -> send\n"
                                         "}\n",
                                         strlen("source subscriptions adapter fmq.xpub\n"
                                                "source input\n"
                                                "stage send adapter fmq.xpub\n"
                                                "stage capture\n"
                                                "stage main {\n"
                                                "  subscriptions -> capture\n"
                                                "  input -> send\n"
                                                "}\n")),
                 TURBO_OK);
    check_int_eq(turbo_flow_compile(publisher), TURBO_OK);
    orders_subscriber = fmq_make_source_flow("fmq.input", &orders_config, &orders);
    metrics_subscriber = fmq_make_source_flow("fmq.input", &metrics_config, &metrics);
    check_not_null(orders_subscriber);
    check_not_null(metrics_subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(orders_subscriber), TURBO_OK);
    fmq_wait_subscription_events(&xpub_events, 1);
    check_int_eq(atomic_load_explicit(&xpub_events.subscription_events, memory_order_acquire), 1);
    check_int_eq(xpub_events.content_profile, TURBO_FLOW_CONTENT_PROFILE_FMQ_CONTROL);
    check_bits(xpub_events.content_flags, TURBO_FLOW_CONTENT_PROTOCOL_CONTROL);
    check_str_eq(xpub_events.content_identity, "orders.");
    check_int_eq(turbo_flow_start(metrics_subscriber), TURBO_OK);
    fmq_wait_subscription_events(&xpub_events, 2);
    check_int_eq(atomic_load_explicit(&xpub_events.subscription_events, memory_order_acquire), 2);
    check_int_eq(fmq_publish_payload(publisher, "order-x"), TURBO_OK);
    fmq_wait_called(&orders, 1);
    check_int_eq(atomic_load_explicit(&orders.called, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&metrics.called, memory_order_acquire), 0);

    check_int_eq(turbo_flow_stop(orders_subscriber), TURBO_OK);
    fmq_wait_subscription_events(&xpub_events, 3);
    check_int_eq(atomic_load_explicit(&xpub_events.subscription_events, memory_order_acquire), 3);
    check_false(xpub_events.subscribe);
    check_int_eq(fmq_publish_payload(publisher, "no-orders"), TURBO_ENOTCONN);
    check_int_eq(turbo_flow_start(orders_subscriber), TURBO_OK);
    fmq_wait_subscription_events(&xpub_events, 4);
    check_int_eq(atomic_load_explicit(&xpub_events.subscription_events, memory_order_acquire), 4);
    check_true(xpub_events.subscribe);
    check_int_eq(fmq_publish_payload(publisher, "order-y"), TURBO_OK);
    fmq_wait_called(&orders, 2);
    check_int_eq(atomic_load_explicit(&orders.called, memory_order_acquire), 2);
    check_int_eq(turbo_flow_stop(metrics_subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(orders_subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(metrics_subscriber);
    turbo_flow_destroy(orders_subscriber);
    turbo_flow_destroy(publisher);
  }

  it("proxies XPUB XSUB subscription controls and publications") {
    static const char *proxy_dsl = "source subscriptions adapter fmq.front\n"
                                   "source publications adapter fmq.back\n"
                                   "stage upstream adapter fmq.back\n"
                                   "stage downstream adapter fmq.front\n"
                                   "stage main {\n"
                                   "  subscriptions -> upstream\n"
                                   "  publications -> downstream\n"
                                   "}\n";
    unsigned short front_port = fmq_test_port();
    unsigned short back_port = fmq_test_port();
    turbo_flow_fmq_config_t front_xpub_config;
    turbo_flow_fmq_config_t back_xsub_config;
    turbo_flow_fmq_config_t upstream_xpub_config;
    turbo_flow_fmq_config_t downstream_xsub_config;
    fmq_capture_state_t upstream_events;
    fmq_capture_state_t downstream;
    fmq_capture_state_t downstream_second;
    turbo_flow_t *upstream;
    turbo_flow_t *proxy = turbo_flow_create();
    turbo_flow_t *subscriber;
    turbo_flow_t *subscriber_second;

    check_int_gt(front_port, 0);
    check_int_gt(back_port, 0);
    check_not_null(proxy);
    fmq_init_capture(&upstream_events);
    fmq_init_capture(&downstream);
    fmq_init_capture(&downstream_second);
    fmq_config(&front_xpub_config, TURBO_FLOW_FMQ_XPUB, TURBO_FLOW_FMQ_BIND, front_port);
    fmq_config(&back_xsub_config, TURBO_FLOW_FMQ_XSUB, TURBO_FLOW_FMQ_CONNECT, back_port);
    fmq_config(&upstream_xpub_config, TURBO_FLOW_FMQ_XPUB, TURBO_FLOW_FMQ_BIND, back_port);
    fmq_config(&downstream_xsub_config, TURBO_FLOW_FMQ_XSUB, TURBO_FLOW_FMQ_CONNECT, front_port);
    front_xpub_config.topic_policy = TURBO_FLOW_FMQ_METADATA_INHERIT;
    upstream_xpub_config.topic = "orders.created";
    downstream_xsub_config.topic = "orders.";
    upstream = turbo_flow_create();
    check_not_null(upstream);
    check_int_eq(fmq_register_private_adapter(upstream, "fmq.xpub", &upstream_xpub_config),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(upstream, "capture", fmq_capture, &upstream_events, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(upstream,
                                         "source subscriptions adapter fmq.xpub\n"
                                         "source input\n"
                                         "stage send adapter fmq.xpub\n"
                                         "stage capture\n"
                                         "stage main {\n"
                                         "  subscriptions -> capture\n"
                                         "  input -> send\n"
                                         "}\n",
                                         strlen("source subscriptions adapter fmq.xpub\n"
                                                "source input\n"
                                                "stage send adapter fmq.xpub\n"
                                                "stage capture\n"
                                                "stage main {\n"
                                                "  subscriptions -> capture\n"
                                                "  input -> send\n"
                                                "}\n")),
                 TURBO_OK);
    check_int_eq(turbo_flow_compile(upstream), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(proxy, "fmq.front", &front_xpub_config), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(proxy, "fmq.back", &back_xsub_config), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(proxy, proxy_dsl, strlen(proxy_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(proxy), TURBO_OK);
    subscriber = fmq_make_source_flow("fmq.input", &downstream_xsub_config, &downstream);
    subscriber_second =
        fmq_make_source_flow("fmq.input", &downstream_xsub_config, &downstream_second);
    check_not_null(subscriber);
    check_not_null(subscriber_second);

    check_int_eq(turbo_flow_start(upstream), TURBO_OK);
    check_int_eq(turbo_flow_start(proxy), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber_second), TURBO_OK);
    fmq_wait_subscription_events(&upstream_events, 2);
    check_int_eq(atomic_load_explicit(&upstream_events.subscription_events, memory_order_acquire),
                 2);
    check_true(upstream_events.subscribe);
    check_size_eq(upstream_events.topic_len, 7);
    check_mem_eq(upstream_events.topic, "orders.", 7);
    check_int_eq(fmq_publish_payload(upstream, "proxied"), TURBO_OK);
    fmq_wait_called(&downstream, 1);
    fmq_wait_called(&downstream_second, 1);
    check_int_eq(atomic_load_explicit(&downstream.called, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&downstream_second.called, memory_order_acquire), 1);
    check_size_eq(downstream.payload_len, 7);
    check_mem_eq(downstream.payload, "proxied", 7);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    fmq_wait_subscription_events(&upstream_events, 3);
    check_int_eq(atomic_load_explicit(&upstream_events.subscription_events, memory_order_acquire),
                 3);
    check_false(upstream_events.subscribe);
    check_int_eq(fmq_publish_payload(upstream, "still-live"), TURBO_OK);
    fmq_wait_called(&downstream_second, 2);
    check_int_eq(atomic_load_explicit(&downstream_second.called, memory_order_acquire), 2);
    check_size_eq(downstream_second.payload_len, 10);
    check_mem_eq(downstream_second.payload, "still-live", 10);
    check_int_eq(turbo_flow_stop(upstream), TURBO_OK);
    check_int_eq(turbo_flow_start(upstream), TURBO_OK);
    fmq_wait_subscriptions(&upstream_events, 3);
    check_int_ge(atomic_load_explicit(&upstream_events.subscriptions, memory_order_acquire), 3);
    check_int_eq(fmq_publish_payload(upstream, "reconnected"), TURBO_OK);
    fmq_wait_called(&downstream_second, 3);
    check_int_eq(atomic_load_explicit(&downstream_second.called, memory_order_acquire), 3);
    check_size_eq(downstream_second.payload_len, 11);
    check_mem_eq(downstream_second.payload, "reconnected", 11);
    check_int_eq(turbo_flow_stop(subscriber_second), TURBO_OK);
    for (int i = 0; i < 400 && atomic_load_explicit(&upstream_events.unsubscriptions,
                                                    memory_order_acquire) < 2;
         ++i) {
      turbo_sleep_ms(5);
    }
    check_int_eq(atomic_load_explicit(&upstream_events.subscriptions, memory_order_acquire), 3);
    check_int_eq(atomic_load_explicit(&upstream_events.unsubscriptions, memory_order_acquire), 2);
    check_int_eq(fmq_publish_payload(upstream, "no-subscribers"), TURBO_ENOTCONN);
    check_int_eq(turbo_flow_stop(proxy), TURBO_OK);
    check_int_eq(turbo_flow_stop(upstream), TURBO_OK);
    turbo_flow_destroy(subscriber_second);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(proxy);
    turbo_flow_destroy(upstream);
  }

  it("serves TFMP terminal replies and recovers REQ state over TCP and WebSocket") {
    static const char server_dsl[] = "source request adapter fmq.rep\n"
                                     "stage management\n"
                                     "stage reply adapter fmq.rep\n"
                                     "stage main {\n"
                                     "  request -> management -> reply\n"
                                     "}\n";
    static const char client_dsl[] = "source response adapter fmq.req\n"
                                     "source input\n"
                                     "stage send adapter fmq.req\n"
                                     "stage capture\n"
                                     "stage main {\n"
                                     "  input -> send\n"
                                     "  response -> capture\n"
                                     "}\n";
    static const struct {
      turbo_flow_fmq_transport_t transport;
      const char *path;
    } cases[] = {{TURBO_FLOW_FMQ_TCP, NULL}, {TURBO_FLOW_FMQ_WS, "/tfmp"}};

    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      turbo_flow_tfmp_management_config_t service_config = TURBO_FLOW_TFMP_MANAGEMENT_CONFIG_INIT;
      turbo_flow_fmq_config_t rep_config;
      turbo_flow_fmq_config_t req_config;
      turbo_flow_tfmp_management_service_t *service = NULL;
      turbo_flow_tfmp_envelope_t request = TURBO_FLOW_TFMP_ENVELOPE_INIT;
      turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
      turbo_flow_t *server = turbo_flow_create();
      turbo_flow_t *client = turbo_flow_create();
      fmq_tfmp_capture_t capture;
      uint8_t wire[128];
      size_t wire_size = 0u;
      unsigned short port = fmq_test_port();

      memset(&capture, 0, sizeof(capture));
      atomic_init(&capture.called, 0);
      memcpy(service_config.authority_id, "transport-contract", sizeof("transport-contract"));
      check_int_gt(port, 0);
      check_not_null(server);
      check_not_null(client);
      fmq_config(&rep_config, TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND, port);
      fmq_config(&req_config, TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT, port);
      rep_config.transport = cases[i].transport;
      rep_config.path = cases[i].path;
      rep_config.timeout_ms = 3000u;
      req_config.transport = cases[i].transport;
      req_config.path = cases[i].path;
      req_config.timeout_ms = 3000u;

      check_int_eq(turbo_flow_tfmp_management_service_create(&service_config, &service), TURBO_OK);
      check_int_eq(
          turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
          TURBO_OK);
      check_int_eq(fmq_register_private_adapter(server, "fmq.rep", &rep_config), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(server, "management",
                                                turbo_flow_tfmp_management_stage, service, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(server, server_dsl, strlen(server_dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(server), TURBO_OK);
      check_int_eq(fmq_register_private_adapter(client, "fmq.req", &req_config), TURBO_OK);
      check_int_eq(
          turbo_flow_register_stage_ex(client, "capture", fmq_tfmp_capture, &capture, NULL),
          TURBO_OK);
      check_int_eq(turbo_flow_parse_string(client, client_dsl, strlen(client_dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(client), TURBO_OK);
      check_int_eq(turbo_flow_start(server), TURBO_OK);
      check_int_eq(turbo_flow_start(client), TURBO_OK);

      request.kind = TURBO_FLOW_TFMP_CAPABILITIES_GET;
      request.correlation_id = 500u + i;
      check_int_eq(turbo_flow_tfmp_envelope_encode(&request, wire, sizeof(wire), &wire_size),
                   TURBO_OK);
      check_int_eq(fmq_publish_bytes(client, (const char *)wire, wire_size), TURBO_OK);
      fmq_wait_event_count(&capture.called, 1);
      check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
      check_int_eq(
          turbo_flow_tfmp_envelope_decode(capture.payload, capture.payload_size, &response),
          TURBO_OK);
      check_uint_eq(response.correlation_id, 500u + i);
      check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);

      wire[0] = 0u;
      check_int_eq(fmq_publish_bytes(client, (const char *)wire, 1u), TURBO_OK);
      fmq_wait_event_count(&capture.called, 2);
      check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
      response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
      check_int_eq(
          turbo_flow_tfmp_envelope_decode(capture.payload, capture.payload_size, &response),
          TURBO_OK);
      check_int_eq(response.kind, TURBO_FLOW_TFMP_PROTOCOL_ERROR);
      check_int_ne(response.status, TURBO_FLOW_TFMP_STATUS_OK);

      request = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
      request.kind = TURBO_FLOW_TFMP_HEALTH_GET;
      request.correlation_id = 600u + i;
      check_int_eq(turbo_flow_tfmp_envelope_encode(&request, wire, sizeof(wire), &wire_size),
                   TURBO_OK);
      check_int_eq(fmq_publish_bytes(client, (const char *)wire, wire_size), TURBO_OK);
      fmq_wait_event_count(&capture.called, 3);
      check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 3);
      response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
      check_int_eq(
          turbo_flow_tfmp_envelope_decode(capture.payload, capture.payload_size, &response),
          TURBO_OK);
      check_uint_eq(response.correlation_id, 600u + i);
      check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);

      check_int_eq(turbo_flow_stop(client), TURBO_OK);
      check_int_eq(turbo_flow_stop(server), TURBO_OK);
      turbo_flow_destroy(client);
      turbo_flow_destroy(server);
      turbo_flow_tfmp_management_service_destroy(service);
    }
  }

  it("delivers PUB SUB messages over CoroNet transport variants") {
    static const struct {
      turbo_flow_fmq_transport_t transport;
      const char *name;
      const char *path;
    } cases[] = {
        {TURBO_FLOW_FMQ_UDP, "udp", NULL},
        {TURBO_FLOW_FMQ_KCP, "kcp", NULL},
        {TURBO_FLOW_FMQ_WS, "ws", "/fmq"},
#ifdef _WIN32
        {TURBO_FLOW_FMQ_PIPE, "pipe", NULL},
#endif
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      unsigned short port =
          cases[i].transport == TURBO_FLOW_FMQ_UDP || cases[i].transport == TURBO_FLOW_FMQ_KCP
              ? fmq_test_udp_port()
              : fmq_test_port();
      char pipe_path[128];
      turbo_flow_fmq_config_t pub_config;
      turbo_flow_fmq_config_t sub_config;
      fmq_capture_state_t capture;
      turbo_flow_t *publisher;
      turbo_flow_t *subscriber;

      check_int_gt(port, 0);
      fmq_init_capture(&capture);
      fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
      fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
      pub_config.transport = cases[i].transport;
      sub_config.transport = cases[i].transport;
      if (cases[i].transport == TURBO_FLOW_FMQ_KCP) {
        pub_config.kcp_pre_shared_key = FMQ_TEST_KCP_PSK;
        sub_config.kcp_pre_shared_key = FMQ_TEST_KCP_PSK;
      }
      pub_config.topic = cases[i].name;
      sub_config.topic = cases[i].name;
      if (cases[i].path) {
        pub_config.path = cases[i].path;
        sub_config.path = cases[i].path;
      }
      if (cases[i].transport == TURBO_FLOW_FMQ_PIPE) {
        check_int_gt(snprintf(pipe_path, sizeof(pipe_path), "pipe://turbo_flow_fmq_%u", port), 0);
        pub_config.host = NULL;
        pub_config.port = 0;
        pub_config.path = pipe_path;
        sub_config.host = NULL;
        sub_config.port = 0;
        sub_config.path = pipe_path;
      }

      check_int_eq(flow_fmq_config_validate(&pub_config), TURBO_OK);
      check_int_eq(flow_fmq_config_validate(&sub_config), TURBO_OK);
      publisher = fmq_make_sink_flow("fmq.output", &pub_config);
      subscriber = fmq_make_source_flow("fmq.input", &sub_config, &capture);
      check_not_null(publisher);
      check_not_null(subscriber);
      check_int_eq(turbo_flow_start(publisher), TURBO_OK);
      check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
      {
        int publish_rc = fmq_publish_payload_after_peer_ready(publisher, cases[i].name);
        check_int_eq(publish_rc, TURBO_OK);
      }
      fmq_wait_called(&capture, 1);
      check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
      check_size_eq(capture.payload_len, strlen(cases[i].name));
      check_mem_eq(capture.payload, cases[i].name, strlen(cases[i].name));
      check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
      check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
      turbo_flow_destroy(subscriber);
      turbo_flow_destroy(publisher);
    }
  }

  it("bridges SUB input to PUB output while inheriting the message topic") {
    static const char *bridge_dsl = "source incoming adapter fmq.input\n"
                                    "stage outgoing adapter fmq.output\n"
                                    "stage main {\n"
                                    "  incoming -> outgoing\n"
                                    "}\n";
    unsigned short in_port = fmq_test_port();
    unsigned short out_port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t bridge_sub_config;
    turbo_flow_fmq_config_t bridge_pub_config;
    turbo_flow_fmq_config_t final_sub_config;
    fmq_capture_state_t capture;
    turbo_flow_t *publisher;
    turbo_flow_t *bridge = turbo_flow_create();
    turbo_flow_t *subscriber;
    check_int_gt(in_port, 0);
    check_int_gt(out_port, 0);
    check_not_null(bridge);
    fmq_init_capture(&capture);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, in_port);
    fmq_config(&bridge_sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, in_port);
    fmq_config(&bridge_pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, out_port);
    fmq_config(&final_sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, out_port);
    pub_config.topic = "orders.created";
    bridge_sub_config.topic = "orders.";
    bridge_pub_config.topic_policy = TURBO_FLOW_FMQ_METADATA_INHERIT;
    final_sub_config.topic = "orders.created";

    publisher = fmq_make_sink_flow("fmq.output", &pub_config);
    check_not_null(publisher);
    check_int_eq(fmq_register_private_adapter(bridge, "fmq.input", &bridge_sub_config),
                 TURBO_OK);
    check_int_eq(fmq_register_private_adapter(bridge, "fmq.output", &bridge_pub_config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(bridge, bridge_dsl, strlen(bridge_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(bridge), TURBO_OK);
    subscriber = fmq_make_source_flow("fmq.input", &final_sub_config, &capture);
    check_not_null(subscriber);

    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(bridge), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "bridge-data"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 11);
    check_mem_eq(capture.payload, "bridge-data", 11);
    check_size_eq(capture.topic_len, 14);
    check_mem_eq(capture.topic, "orders.created", 14);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(bridge), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(bridge);
    turbo_flow_destroy(publisher);
  }

  it("rejects inherited outbound metadata without an FMQ input context") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    turbo_flow_t *publisher;
    check_int_gt(port, 0);
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    config.topic_policy = TURBO_FLOW_FMQ_METADATA_INHERIT;
    publisher = fmq_make_sink_flow("fmq.output", &config);
    check_not_null(publisher);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "plain"), TURBO_ENOENT);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(publisher);
  }

  it("publishes a dynamic topic from message-owned content metadata") {
    static const char topic[] = "tfmp/1/event/operation";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    turbo_flow_content_descriptor_t descriptor = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
    turbo_flow_msg_t msg;
    fmq_capture_state_t capture;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    int publish_status;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    pub_config.topic_policy = TURBO_FLOW_FMQ_METADATA_CONTENT;
    sub_config.topic = topic;
    publisher = fmq_make_sink_flow("fmq.output", &pub_config);
    subscriber = fmq_make_source_flow("fmq.input", &sub_config, &capture);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "missing-topic"), TURBO_ENOENT);
    check_int_eq(turbo_flow_content_descriptor_init(&descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                                                    TURBO_FLOW_CONTENT_PROFILE_FMQ_DATA,
                                                    TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                                    "application/octet-stream", topic),
                 TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup("event-wire");
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_msg_copy_content_descriptor(&msg, &descriptor), TURBO_OK);
    publish_status = turbo_flow_publish(publisher, "input", &msg);
    turbo_flow_msg_cleanup(&msg);
    check_int_eq(publish_status, TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, sizeof("event-wire") - 1u);
    check_mem_eq(capture.payload, "event-wire", sizeof("event-wire") - 1u);
    check_size_eq(capture.topic_len, sizeof(topic) - 1u);
    check_mem_eq(capture.topic, topic, sizeof(topic) - 1u);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
  }

  it("round robins PUSH messages across PULL consumers") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t push_config;
    turbo_flow_fmq_config_t pull_a_config;
    turbo_flow_fmq_config_t pull_b_config;
    fmq_capture_state_t pull_a;
    fmq_capture_state_t pull_b;
    fmq_event_state_t push_events;
    turbo_flow_t *push;
    turbo_flow_t *pull_a_flow;
    turbo_flow_t *pull_b_flow;
    check_int_gt(port, 0);
    fmq_init_capture(&pull_a);
    fmq_init_capture(&pull_b);
    fmq_init_events(&push_events);
    fmq_config(&push_config, TURBO_FLOW_FMQ_PUSH, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&pull_a_config, TURBO_FLOW_FMQ_PULL, TURBO_FLOW_FMQ_CONNECT, port);
    fmq_config(&pull_b_config, TURBO_FLOW_FMQ_PULL, TURBO_FLOW_FMQ_CONNECT, port);
    push_config.event_callback = fmq_event_capture;
    push_config.event_ctx = &push_events;
    push = fmq_make_sink_flow("fmq.output", &push_config);
    pull_a_flow = fmq_make_source_flow("fmq.input", &pull_a_config, &pull_a);
    pull_b_flow = fmq_make_source_flow("fmq.input", &pull_b_config, &pull_b);
    check_not_null(push);
    check_not_null(pull_a_flow);
    check_not_null(pull_b_flow);
    check_int_eq(turbo_flow_start(push), TURBO_OK);
    check_int_eq(turbo_flow_start(pull_a_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(pull_b_flow), TURBO_OK);
    check_int_eq(fmq_publish_payload(push, "job-a"), TURBO_OK);
    check_int_eq(fmq_publish_payload(push, "job-b"), TURBO_OK);
    fmq_wait_called(&pull_a, 1);
    fmq_wait_called(&pull_b, 1);
    check_int_eq(atomic_load_explicit(&pull_a.called, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&pull_b.called, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&push_events.frame_sent, memory_order_acquire), 2);
    check_int_eq(turbo_flow_stop(pull_b_flow), TURBO_OK);
    check_int_eq(turbo_flow_stop(pull_a_flow), TURBO_OK);
    check_int_eq(turbo_flow_stop(push), TURBO_OK);
    turbo_flow_destroy(pull_b_flow);
    turbo_flow_destroy(pull_a_flow);
    turbo_flow_destroy(push);
  }

  it("enforces correlated REQ REP lockstep across repeated round trips") {
    static const char *rep_dsl = "source request adapter fmq.rep\n"
                                 "stage gate\n"
                                 "stage reply adapter fmq.rep\n"
                                 "stage main {\n"
                                 "  request -> gate -> reply\n"
                                 "}\n";
    static const char *req_dsl = "source response adapter fmq.req\n"
                                 "source input\n"
                                 "stage send adapter fmq.req\n"
                                 "stage capture\n"
                                 "stage main {\n"
                                 "  input -> send\n"
                                 "  response -> capture\n"
                                 "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t rep_config;
    turbo_flow_fmq_config_t req_config;
    fmq_stage_gate_t gate;
    fmq_capture_state_t capture;
    turbo_flow_t *rep = turbo_flow_create();
    turbo_flow_t *req = turbo_flow_create();

    check_int_gt(port, 0);
    check_not_null(rep);
    check_not_null(req);
    memset(&gate, 0, sizeof(gate));
    atomic_init(&gate.entered, 0);
    atomic_init(&gate.release, 0);
    atomic_init(&gate.correlation_id, 0u);
    fmq_init_capture(&capture);
    fmq_config(&rep_config, TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&req_config, TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT, port);
    check_int_eq(fmq_register_private_adapter(rep, "fmq.rep", &rep_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(rep, "gate", fmq_wait_at_stage_gate, &gate, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(rep, rep_dsl, strlen(rep_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(rep), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(req, "fmq.req", &req_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(req, "capture", fmq_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(req, req_dsl, strlen(req_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(req), TURBO_OK);
    check_int_eq(turbo_flow_start(rep), TURBO_OK);
    check_int_eq(turbo_flow_start(req), TURBO_OK);

    check_int_eq(fmq_publish_payload(req, "request-1"), TURBO_OK);
    for (int i = 0; i < 400 && !atomic_load_explicit(&gate.entered, memory_order_acquire); ++i)
      turbo_sleep_ms(5);
    check_true(atomic_load_explicit(&gate.entered, memory_order_acquire));
    check_int_eq(fmq_publish_payload(req, "request-while-waiting"), TURBO_EBUSY);
    check_true(atomic_load_explicit(&gate.correlation_id, memory_order_acquire) != 0u);
    atomic_store_explicit(&gate.release, 1, memory_order_release);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 9);
    check_mem_eq(capture.payload, "request-1", 9);
    check_hex64_eq(capture.correlation_id,
                   atomic_load_explicit(&gate.correlation_id, memory_order_acquire));

    check_int_eq(fmq_publish_payload(req, "request-2"), TURBO_OK);
    fmq_wait_called(&capture, 2);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
    check_size_eq(capture.payload_len, 9);
    check_mem_eq(capture.payload, "request-2", 9);
    check_int_eq(turbo_flow_stop(req), TURBO_OK);
    check_int_eq(turbo_flow_stop(rep), TURBO_OK);
    turbo_flow_destroy(req);
    turbo_flow_destroy(rep);
  }

  it("discards an outstanding REQ reply until a new session reconnects") {
    static const char *rep_dsl = "source request adapter fmq.rep\n"
                                 "stage gate\n"
                                 "stage reply adapter fmq.rep\n"
                                 "stage main {\n"
                                 "  request -> gate -> reply\n"
                                 "}\n";
    static const char *req_dsl = "source response adapter fmq.req\n"
                                 "source input\n"
                                 "stage send adapter fmq.req\n"
                                 "stage capture\n"
                                 "stage main {\n"
                                 "  input -> send\n"
                                 "  response -> capture\n"
                                 "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t rep_config;
    turbo_flow_fmq_config_t req_config;
    turbo_flow_connection_snapshot_t req_snapshot;
    fmq_stage_gate_t gate;
    fmq_capture_state_t capture;
    turbo_flow_t *rep = turbo_flow_create();
    turbo_flow_t *req = turbo_flow_create();
    uint64_t first_correlation;

    check_int_gt(port, 0);
    check_not_null(rep);
    check_not_null(req);
    memset(&gate, 0, sizeof(gate));
    atomic_init(&gate.entered, 0);
    atomic_init(&gate.release, 0);
    atomic_init(&gate.correlation_id, 0u);
    fmq_init_capture(&capture);
    fmq_config(&rep_config, TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&req_config, TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT, port);
    req_config.reconnect_initial_ms = 10;
    req_config.reconnect_max_ms = 20;
    check_int_eq(fmq_register_private_adapter(rep, "fmq.rep", &rep_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(rep, "gate", fmq_wait_at_stage_gate, &gate, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(rep, rep_dsl, strlen(rep_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(rep), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(req, "fmq.req", &req_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(req, "capture", fmq_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(req, req_dsl, strlen(req_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(req), TURBO_OK);
    check_int_eq(turbo_flow_start(rep), TURBO_OK);
    check_int_eq(turbo_flow_start(req), TURBO_OK);

    check_int_eq(fmq_publish_payload(req, "old-session"), TURBO_OK);
    for (int i = 0; i < 400 && !atomic_load_explicit(&gate.entered, memory_order_acquire); ++i)
      turbo_sleep_ms(5);
    check_true(atomic_load_explicit(&gate.entered, memory_order_acquire));
    first_correlation = atomic_load_explicit(&gate.correlation_id, memory_order_acquire);
    check_true(first_correlation != 0u);

    check_int_eq(turbo_flow_stop(req), TURBO_OK);
    check_int_eq(fmq_publish_payload(req, "while-stopped"), TURBO_EINVAL);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(req, 0, &req_snapshot), TURBO_OK);
    check_int_eq(req_snapshot.state, TURBO_FLOW_CONNECTION_STOPPED);
    atomic_store_explicit(&gate.release, 1, memory_order_release);
    check_int_eq(fmq_wait_connection_count(rep, 0), TURBO_OK);
    turbo_sleep_ms(20);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 0);

    check_int_eq(turbo_flow_start(req), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(rep, 1), TURBO_OK);
    check_int_eq(fmq_publish_payload(req, "new-session"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 11);
    check_mem_eq(capture.payload, "new-session", 11);
    check_true(capture.correlation_id != 0u);
    check_true(capture.correlation_id != first_correlation);

    check_int_eq(turbo_flow_stop(req), TURBO_OK);
    check_int_eq(turbo_flow_stop(rep), TURBO_OK);
    turbo_flow_destroy(req);
    turbo_flow_destroy(rep);
  }

  it("resets REQ after a reply timeout and recovers on a new session") {
    static const char *rep_dsl = "source request adapter fmq.rep\n"
                                 "stage gate\n"
                                 "stage reply adapter fmq.rep\n"
                                 "stage main {\n"
                                 "  request -> gate -> reply\n"
                                 "}\n";
    static const char *req_dsl = "source response adapter fmq.req\n"
                                 "source input\n"
                                 "stage send adapter fmq.req\n"
                                 "stage capture\n"
                                 "stage main {\n"
                                 "  input -> send\n"
                                 "  response -> capture\n"
                                 "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t rep_config;
    turbo_flow_fmq_config_t req_config;
    turbo_flow_connection_snapshot_t req_snapshot;
    fmq_stage_gate_t gate;
    fmq_capture_state_t capture;
    turbo_flow_t *rep = turbo_flow_create();
    turbo_flow_t *req = turbo_flow_create();
    uint64_t first_correlation;

    check_int_gt(port, 0);
    check_not_null(rep);
    check_not_null(req);
    memset(&gate, 0, sizeof(gate));
    atomic_init(&gate.entered, 0);
    atomic_init(&gate.release, 0);
    atomic_init(&gate.correlation_id, 0u);
    fmq_init_capture(&capture);
    fmq_config(&rep_config, TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&req_config, TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT, port);
    req_config.recv_timeout_ms = 50;
    req_config.reconnect_initial_ms = 10;
    req_config.reconnect_max_ms = 20;
    check_int_eq(fmq_register_private_adapter(rep, "fmq.rep", &rep_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(rep, "gate", fmq_wait_at_stage_gate, &gate, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(rep, rep_dsl, strlen(rep_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(rep), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(req, "fmq.req", &req_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(req, "capture", fmq_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(req, req_dsl, strlen(req_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(req), TURBO_OK);
    check_int_eq(turbo_flow_start(rep), TURBO_OK);
    check_int_eq(turbo_flow_start(req), TURBO_OK);

    check_int_eq(fmq_publish_payload(req, "times-out"), TURBO_OK);
    for (int i = 0; i < 400 && !atomic_load_explicit(&gate.entered, memory_order_acquire); ++i)
      turbo_sleep_ms(5);
    check_true(atomic_load_explicit(&gate.entered, memory_order_acquire));
    first_correlation = atomic_load_explicit(&gate.correlation_id, memory_order_acquire);
    check_true(first_correlation != 0u);
    check_int_eq(fmq_wait_connection_state(req, TURBO_FLOW_CONNECTION_BACKOFF, &req_snapshot),
                 TURBO_OK);
    check_size_eq(req_snapshot.connections_current, 0);
    check_int_eq(fmq_publish_payload(req, "while-resetting"), TURBO_EBUSY);

    atomic_store_explicit(&gate.release, 1, memory_order_release);
    check_int_eq(fmq_wait_connection_state(req, TURBO_FLOW_CONNECTION_READY, &req_snapshot),
                 TURBO_OK);
    turbo_sleep_ms(20);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 0);
    check_int_eq(fmq_publish_payload(req, "after-timeout"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 13);
    check_mem_eq(capture.payload, "after-timeout", 13);
    check_true(capture.correlation_id != 0u);
    check_true(capture.correlation_id != first_correlation);

    check_int_eq(turbo_flow_stop(req), TURBO_OK);
    check_int_eq(turbo_flow_stop(rep), TURBO_OK);
    turbo_flow_destroy(req);
    turbo_flow_destroy(rep);
  }

#ifdef _WIN32
  it("round trips REQ REP over Pipe on one CoroNet pool lane") {
    static const char *rep_dsl = "source request adapter fmq.rep\n"
                                 "stage reply adapter fmq.rep\n"
                                 "stage main {\n"
                                 "  request -> reply\n"
                                 "}\n";
    static const char *req_dsl = "source response adapter fmq.req\n"
                                 "source input\n"
                                 "stage send adapter fmq.req\n"
                                 "stage capture\n"
                                 "stage main {\n"
                                 "  input -> send\n"
                                 "  response -> capture\n"
                                 "}\n";
    unsigned short suffix = fmq_test_port();
    char pipe_path[128];
    coro_thread_pool_t *pool = coro_thread_pool_create(1);
    turbo_flow_coronet_execution_binding_t execution;
    turbo_flow_fmq_config_t rep_config;
    turbo_flow_fmq_config_t req_config;
    fmq_capture_state_t capture;
    turbo_flow_t *rep = turbo_flow_create();
    turbo_flow_t *req = turbo_flow_create();

    check_int_gt(suffix, 0);
    check_int_gt(snprintf(pipe_path, sizeof(pipe_path), "pipe://turbo_flow_fmq_req_rep_%u", suffix),
                 0);
    check_not_null(pool);
    check_not_null(rep);
    check_not_null(req);
    memset(&execution, 0, sizeof(execution));
    fmq_init_capture(&capture);
    fmq_config(&rep_config, TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND, 0);
    fmq_config(&req_config, TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT, 0);
    rep_config.transport = TURBO_FLOW_FMQ_PIPE;
    rep_config.host = NULL;
    rep_config.path = pipe_path;
    req_config.transport = TURBO_FLOW_FMQ_PIPE;
    req_config.host = NULL;
    req_config.path = pipe_path;
    execution.size = sizeof(execution);
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_POOL_LANE;
    execution.pool = pool;
    execution.lane = 0;

    check_int_eq(turbo_flow_fmq_register_adapter_ex(rep, "fmq.rep", &rep_config, &execution),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(rep, rep_dsl, strlen(rep_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(rep), TURBO_OK);
    check_int_eq(turbo_flow_fmq_register_adapter_ex(req, "fmq.req", &req_config, &execution),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(req, "capture", fmq_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(req, req_dsl, strlen(req_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(req), TURBO_OK);
    check_int_eq(turbo_flow_start(rep), TURBO_OK);
    check_int_eq(turbo_flow_start(req), TURBO_OK);
    check_int_eq(fmq_publish_payload(req, "pipe-request"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 12);
    check_mem_eq(capture.payload, "pipe-request", 12);
    check_true(capture.correlation_id != 0u);
    check_int_eq(turbo_flow_stop(req), TURBO_OK);
    check_int_eq(turbo_flow_stop(rep), TURBO_OK);
    turbo_flow_destroy(req);
    turbo_flow_destroy(rep);
    coro_thread_pool_destroy(pool);
  }
#endif

  it("rejects a REP reply before receiving a request") {
    static const char *dsl = "source request adapter fmq.rep\n"
                             "source input\n"
                             "stage reply adapter fmq.rep\n"
                             "stage main {\n"
                             "  input -> reply\n"
                             "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    turbo_flow_t *rep = turbo_flow_create();
    check_int_gt(port, 0);
    check_not_null(rep);
    fmq_config(&config, TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND, port);
    check_int_eq(fmq_register_private_adapter(rep, "fmq.rep", &config), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(rep, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(rep), TURBO_OK);
    check_int_eq(turbo_flow_start(rep), TURBO_OK);
    check_int_eq(fmq_publish_payload(rep, "orphan-reply"), TURBO_EBUSY);
    check_int_eq(turbo_flow_stop(rep), TURBO_OK);
    turbo_flow_destroy(rep);
  }

  it("correlates ROUTER replies to DEALER identity") {
    static const char *router_dsl = "source request adapter fmq.router\n"
                                    "stage echo\n"
                                    "stage reply adapter fmq.router\n"
                                    "stage main {\n"
                                    "  request -> echo -> reply\n"
                                    "}\n";
    static const char *dealer_dsl = "source response adapter fmq.dealer\n"
                                    "source input\n"
                                    "stage send adapter fmq.dealer\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  input -> send\n"
                                    "  response -> capture\n"
                                    "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t router_config;
    turbo_flow_fmq_config_t dealer_config;
    fmq_capture_state_t router_state;
    fmq_capture_state_t dealer_state;
    turbo_flow_t *router = turbo_flow_create();
    turbo_flow_t *dealer = turbo_flow_create();
    check_int_gt(port, 0);
    check_not_null(router);
    check_not_null(dealer);
    fmq_init_capture(&router_state);
    fmq_init_capture(&dealer_state);
    fmq_config(&router_config, TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&dealer_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    dealer_config.identity = "worker-17";
    check_int_eq(fmq_register_private_adapter(router, "fmq.router", &router_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(router, "echo", fmq_echo, &router_state, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(router, router_dsl, strlen(router_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(router), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(dealer, "fmq.dealer", &dealer_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(dealer, "capture", fmq_capture, &dealer_state, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(dealer, dealer_dsl, strlen(dealer_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(dealer), TURBO_OK);
    check_int_eq(turbo_flow_start(router), TURBO_OK);
    check_int_eq(turbo_flow_start(dealer), TURBO_OK);
    check_int_eq(fmq_publish_payload(dealer, "request-9"), TURBO_OK);
    fmq_wait_called(&dealer_state, 1);
    check_int_eq(atomic_load_explicit(&dealer_state.called, memory_order_acquire), 1);
    check_size_eq(dealer_state.payload_len, 9);
    check_mem_eq(dealer_state.payload, "request-9", 9);
    check_size_eq(router_state.identity_len, 9);
    check_mem_eq(router_state.identity, "worker-17", 9);
    check_int_eq(turbo_flow_stop(dealer), TURBO_OK);
    check_int_eq(turbo_flow_stop(router), TURBO_OK);
    turbo_flow_destroy(dealer);
    turbo_flow_destroy(router);
  }

  bench("reports real TCP ROUTER DEALER echo latency percentiles") {
    static const char *router_dsl = "source request adapter fmq.router\n"
                                    "stage echo\n"
                                    "stage reply adapter fmq.router\n"
                                    "stage main {\n"
                                    "  request -> echo -> reply\n"
                                    "}\n";
    static const char *dealer_dsl = "source response adapter fmq.dealer\n"
                                    "source input\n"
                                    "stage send adapter fmq.dealer\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  input -> send\n"
                                    "  response -> capture\n"
                                    "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t router_config;
    turbo_flow_fmq_config_t dealer_config;
    fmq_capture_state_t router_state;
    fmq_bench_capture_state_t dealer_state;
    turbo_flow_t *router = turbo_flow_create();
    turbo_flow_t *dealer = turbo_flow_create();
    uint64_t *latencies = NULL;
    uint64_t total_started_ns;
    uint64_t total_elapsed_ns;
    char payload[FMQ_BENCH_TCP_PAYLOAD_BYTES];
    size_t completed = 0u;
    size_t captured_payload_len = 0u;
    int expected = 0;
    int captured_calls = 0;
    int status = TURBO_OK;
    int capture_initialized = 0;
    int router_started = 0;
    int dealer_started = 0;
    double throughput;

    check_int_gt(port, 0);
    check_not_null(router);
    check_not_null(dealer);
    if (port == 0u || !router || !dealer) goto cleanup;
    latencies = (uint64_t *)calloc(FMQ_BENCH_TCP_SAMPLES, sizeof(*latencies));
    check_not_null(latencies);
    if (!latencies) goto cleanup;
    memset(payload, 'x', sizeof(payload));
    fmq_init_capture(&router_state);
    fmq_bench_capture_init(&dealer_state);
    capture_initialized = 1;
    fmq_config(&router_config, TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&dealer_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    dealer_config.identity = "bench-worker";

    status = fmq_register_private_adapter(router, "fmq.router", &router_config);
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;
    status = turbo_flow_register_stage_ex(router, "echo", fmq_echo, &router_state, NULL);
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;
    status = turbo_flow_parse_string(router, router_dsl, strlen(router_dsl));
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;
    status = turbo_flow_compile(router);
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;

    status = fmq_register_private_adapter(dealer, "fmq.dealer", &dealer_config);
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;
    status =
        turbo_flow_register_stage_ex(dealer, "capture", fmq_bench_capture, &dealer_state, NULL);
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;
    status = turbo_flow_parse_string(dealer, dealer_dsl, strlen(dealer_dsl));
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;
    status = turbo_flow_compile(dealer);
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;
    status = turbo_flow_start(router);
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;
    router_started = 1;
    status = turbo_flow_start(dealer);
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;
    dealer_started = 1;
    status = fmq_wait_connection_count(router, 1u);
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;

    for (size_t i = 0u; i < FMQ_BENCH_TCP_WARMUP; ++i) {
      status = fmq_bench_echo_roundtrip(dealer, &dealer_state, payload, sizeof(payload),
                                        expected + 1, NULL);
      if (status != TURBO_OK) break;
      expected += 1;
    }
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;

    total_started_ns = turbo_hrtime();
    for (size_t i = 0u; i < FMQ_BENCH_TCP_SAMPLES; ++i) {
      status = fmq_bench_echo_roundtrip(dealer, &dealer_state, payload, sizeof(payload),
                                        expected + 1, &latencies[i]);
      if (status != TURBO_OK) break;
      expected += 1;
      completed += 1u;
    }
    total_elapsed_ns = turbo_hrtime() - total_started_ns;
    check_int_eq(status, TURBO_OK);
    check_size_eq(completed, FMQ_BENCH_TCP_SAMPLES);
    if (status != TURBO_OK || completed != FMQ_BENCH_TCP_SAMPLES) goto cleanup;
    turbo_mutex_lock(&dealer_state.mutex);
    captured_payload_len = dealer_state.payload_len;
    captured_calls = dealer_state.called;
    turbo_mutex_unlock(&dealer_state.mutex);
    check_size_eq(captured_payload_len, sizeof(payload));
    check_int_eq(captured_calls, expected);

    /* Reporting sorts one bounded 1024-element array: O(n log n) time and O(n) samples. */
    qsort(latencies, completed, sizeof(*latencies), fmq_bench_u64_compare);
    throughput =
        total_elapsed_ns > 0u ? ((double)completed * 1000000000.0) / (double)total_elapsed_ns : 0.0;
    printf("FMQ_BENCH_RESULT component=data_plane pattern=router_dealer transport=tcp"
           " mode=serialized-echo payload_bytes=%u warmup=%u samples=%u"
           " throughput_roundtrip_s=%.2f p50_ns=%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64
           "\n",
           FMQ_BENCH_TCP_PAYLOAD_BYTES, FMQ_BENCH_TCP_WARMUP, FMQ_BENCH_TCP_SAMPLES, throughput,
           fmq_bench_percentile(latencies, completed, 50u),
           fmq_bench_percentile(latencies, completed, 95u),
           fmq_bench_percentile(latencies, completed, 99u));

  cleanup:
    if (dealer_started) check_int_eq(turbo_flow_stop(dealer), TURBO_OK);
    if (router_started) check_int_eq(turbo_flow_stop(router), TURBO_OK);
    if (capture_initialized) fmq_bench_capture_destroy(&dealer_state);
    free(latencies);
    turbo_flow_destroy(dealer);
    turbo_flow_destroy(router);
  }

  it("delivers a detached ROUTER reply from another thread") {
    static const char *router_dsl = "source request adapter fmq.router\n"
                                    "source delayed\n"
                                    "stage detach\n"
                                    "stage reply adapter fmq.router\n"
                                    "stage main {\n"
                                    "  request -> detach\n"
                                    "  delayed -> reply\n"
                                    "}\n";
    static const char *dealer_dsl = "source response adapter fmq.dealer\n"
                                    "source input\n"
                                    "stage send adapter fmq.dealer\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  input -> send\n"
                                    "  response -> capture\n"
                                    "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t router_config;
    turbo_flow_fmq_config_t dealer_config;
    fmq_delayed_reply_state_t delayed;
    fmq_delayed_reply_thread_ctx_t reply;
    fmq_capture_state_t capture;
    turbo_thread_t thread;
    turbo_flow_t *router = turbo_flow_create();
    turbo_flow_t *dealer = turbo_flow_create();
    const turbo_flow_protocol_route_t *route;
    const turbo_flow_content_descriptor_t *descriptor;
    check_int_gt(port, 0);
    check_not_null(router);
    check_not_null(dealer);
    fmq_delayed_reply_state_init(&delayed);
    memset(&reply, 0, sizeof(reply));
    fmq_init_capture(&capture);
    fmq_config(&router_config, TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&dealer_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    router_config.content_type = "application/json";
    dealer_config.identity = "delayed-worker";
    check_int_eq(fmq_register_private_adapter(router, "fmq.router", &router_config), TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(router, "detach", fmq_detach_router_route, &delayed, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(router, router_dsl, strlen(router_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(router), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(dealer, "fmq.dealer", &dealer_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(dealer, "capture", fmq_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(dealer, dealer_dsl, strlen(dealer_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(dealer), TURBO_OK);
    check_int_eq(turbo_flow_start(router), TURBO_OK);
    check_int_eq(turbo_flow_start(dealer), TURBO_OK);
    check_int_eq(fmq_publish_payload(dealer, "{\"job\":17}"), TURBO_OK);
    fmq_wait_delayed_reply(&delayed);
    check_int_eq(atomic_load_explicit(&delayed.ready, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&delayed.capture_status, memory_order_acquire), TURBO_OK);
    check_null(delayed.message.transport_context);
    route = turbo_flow_msg_protocol_route(&delayed.message);
    check_not_null(route);
    check_int_eq(route->protocol, TURBO_FLOW_PROTOCOL_FMQ);
    check_true(route->owner_instance_id != 0u);
    check_true(route->session_id != 0u);
    check_true(route->session_generation != 0u);
    descriptor = turbo_flow_msg_content_descriptor(&delayed.message);
    check_not_null(descriptor);
    check_int_eq(descriptor->profile, TURBO_FLOW_CONTENT_PROFILE_FMQ_DATA);
    check_str_eq(descriptor->media_type, "application/json");
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 0);
    reply.flow = router;
    reply.message = &delayed.message;
    atomic_init(&reply.status, TURBO_EALREADY);
    check_int_eq(turbo_thread_create(&thread, fmq_delayed_reply_thread, &reply), TURBO_OK);
    check_int_eq(turbo_thread_join(&thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&reply.status, memory_order_acquire), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 10);
    check_mem_eq(capture.payload, "{\"job\":17}", 10);
    check_int_eq(turbo_flow_stop(dealer), TURBO_OK);
    check_int_eq(turbo_flow_stop(router), TURBO_OK);
    turbo_flow_msg_cleanup(&delayed.message);
    turbo_flow_destroy(dealer);
    turbo_flow_destroy(router);
  }

  it("invalidates detached routes across session and ROUTER generations") {
    static const char *router_dsl = "source request adapter fmq.router\n"
                                    "source delayed\n"
                                    "stage detach\n"
                                    "stage reply adapter fmq.router\n"
                                    "stage main {\n"
                                    "  request -> detach\n"
                                    "  delayed -> reply\n"
                                    "}\n";
    static const char *dealer_dsl = "source input\n"
                                    "stage send adapter fmq.dealer\n"
                                    "stage main {\n"
                                    "  input -> send\n"
                                    "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t router_config;
    turbo_flow_fmq_config_t dealer_config;
    fmq_delayed_reply_state_t delayed;
    turbo_flow_t *router = turbo_flow_create();
    turbo_flow_t *dealer = turbo_flow_create();
    check_int_gt(port, 0);
    check_not_null(router);
    check_not_null(dealer);
    fmq_delayed_reply_state_init(&delayed);
    fmq_config(&router_config, TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&dealer_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    dealer_config.identity = "stale-worker";
    check_int_eq(fmq_register_private_adapter(router, "fmq.router", &router_config), TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(router, "detach", fmq_detach_router_route, &delayed, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(router, router_dsl, strlen(router_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(router), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(dealer, "fmq.dealer", &dealer_config), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(dealer, dealer_dsl, strlen(dealer_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(dealer), TURBO_OK);
    check_int_eq(turbo_flow_start(router), TURBO_OK);
    check_int_eq(turbo_flow_start(dealer), TURBO_OK);
    check_int_eq(fmq_publish_payload(dealer, "will-expire"), TURBO_OK);
    fmq_wait_delayed_reply(&delayed);
    check_int_eq(atomic_load_explicit(&delayed.capture_status, memory_order_acquire), TURBO_OK);
    check_int_eq(turbo_flow_stop(dealer), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(router, 0), TURBO_OK);
    check_int_eq(turbo_flow_start(dealer), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(router, 1), TURBO_OK);
    check_int_eq(turbo_flow_publish(router, "delayed", &delayed.message), TURBO_ENOTCONN);
    check_int_eq(turbo_flow_stop(dealer), TURBO_OK);
    check_int_eq(turbo_flow_stop(router), TURBO_OK);
    check_int_eq(turbo_flow_start(router), TURBO_OK);
    check_int_eq(turbo_flow_publish(router, "delayed", &delayed.message), TURBO_ENOTCONN);
    check_int_eq(turbo_flow_stop(router), TURBO_OK);
    turbo_flow_msg_cleanup(&delayed.message);
    turbo_flow_destroy(dealer);
    turbo_flow_destroy(router);
  }

  it("routes a remote client request through a READY worker and back") {
    static const char *router_dsl = "source inbound adapter fmq.router\n"
                                    "source routed\n"
                                    "stage capture\n"
                                    "stage send adapter fmq.router\n"
                                    "stage main {\n"
                                    "  inbound -> capture\n"
                                    "  routed -> send\n"
                                    "}\n";
    static const char *dealer_dsl = "source response adapter fmq.dealer\n"
                                    "source input\n"
                                    "stage send adapter fmq.dealer\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  input -> send\n"
                                    "  response -> capture\n"
                                    "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t router_config;
    turbo_flow_fmq_config_t worker_config;
    turbo_flow_fmq_config_t client_config;
    turbo_flow_fmq_broker_config_t broker_config = TURBO_FLOW_FMQ_BROKER_CONFIG_INIT;
    turbo_flow_fmq_broker_t *broker;
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_completion_result_t completion =
        TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    fmq_broker_route_capture_t router_capture;
    fmq_capture_state_t worker_capture;
    fmq_capture_state_t client_capture;
    turbo_flow_t *router = turbo_flow_create();
    turbo_flow_t *worker = turbo_flow_create();
    turbo_flow_t *client = turbo_flow_create();
    const turbo_flow_protocol_route_t *route;

    check_int_gt(port, 0);
    check_not_null(router);
    check_not_null(worker);
    check_not_null(client);
    broker_config.max_workers = 4u;
    broker_config.max_inflight = 16u;
    broker = turbo_flow_fmq_broker_create(&broker_config);
    check_not_null(broker);
    fmq_broker_route_capture_init(&router_capture);
    fmq_init_capture(&worker_capture);
    fmq_init_capture(&client_capture);
    fmq_config(&router_config, TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&worker_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    fmq_config(&client_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    worker_config.identity = "worker-1";
    client_config.identity = "client-1";

    check_int_eq(fmq_register_private_adapter(router, "fmq.router", &router_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(router, "capture", fmq_broker_capture_route,
                                              &router_capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(router, router_dsl, strlen(router_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(router), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(worker, "fmq.dealer", &worker_config), TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(worker, "capture", fmq_capture, &worker_capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(worker, dealer_dsl, strlen(dealer_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(worker), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(client, "fmq.dealer", &client_config), TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(client, "capture", fmq_capture, &client_capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(client, dealer_dsl, strlen(dealer_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(client), TURBO_OK);

    check_int_eq(turbo_flow_start(router), TURBO_OK);
    check_int_eq(turbo_flow_start(worker), TURBO_OK);
    check_int_eq(turbo_flow_start(client), TURBO_OK);

    check_int_eq(fmq_publish_payload(worker, "READY echo"), TURBO_OK);
    fmq_wait_broker_capture(&router_capture);
    check_int_eq(atomic_load_explicit(&router_capture.capture_status, memory_order_acquire),
                 TURBO_OK);
    check_str_eq(router_capture.identity, "worker-1");
    route = turbo_flow_msg_protocol_route(&router_capture.message);
    check_not_null(route);
    check_int_eq(turbo_flow_fmq_broker_worker_ready(broker, "worker-1", "echo", route), TURBO_OK);

    fmq_broker_route_capture_reset(&router_capture);
    check_int_eq(fmq_publish_payload(client, "REQ 101 hello"), TURBO_OK);
    fmq_wait_broker_capture(&router_capture);
    check_int_eq(atomic_load_explicit(&router_capture.capture_status, memory_order_acquire),
                 TURBO_OK);
    check_str_eq(router_capture.identity, "client-1");
    route = turbo_flow_msg_protocol_route(&router_capture.message);
    check_not_null(route);
    check_int_eq(turbo_flow_fmq_broker_dispatch(broker, "echo", 101u, route, &dispatch), TURBO_OK);
    check_str_eq(dispatch.worker_id, "worker-1");
    check_int_eq(turbo_flow_msg_set_protocol_route(&router_capture.message, &dispatch.worker_route),
                 TURBO_OK);
    check_int_eq(turbo_flow_publish(router, "routed", &router_capture.message), TURBO_OK);
    fmq_wait_called(&worker_capture, 1);
    check_int_eq(atomic_load_explicit(&worker_capture.called, memory_order_acquire), 1);
    check_mem_eq(worker_capture.payload, "REQ 101 hello", 13u);

    fmq_broker_route_capture_reset(&router_capture);
    check_int_eq(fmq_publish_payload(worker, "REP 101 hello"), TURBO_OK);
    fmq_wait_broker_capture(&router_capture);
    check_int_eq(atomic_load_explicit(&router_capture.capture_status, memory_order_acquire),
                 TURBO_OK);
    check_str_eq(router_capture.identity, "worker-1");
    check_int_eq(turbo_flow_fmq_broker_complete(broker, "worker-1", 101u, &completion), TURBO_OK);
    check_int_eq(
        turbo_flow_msg_set_protocol_route(&router_capture.message, &completion.client_route),
        TURBO_OK);
    check_int_eq(turbo_flow_publish(router, "routed", &router_capture.message), TURBO_OK);
    fmq_wait_called(&client_capture, 1);
    check_int_eq(atomic_load_explicit(&client_capture.called, memory_order_acquire), 1);
    check_mem_eq(client_capture.payload, "REP 101 hello", 13u);

    check_int_eq(turbo_flow_stop(client), TURBO_OK);
    check_int_eq(turbo_flow_stop(worker), TURBO_OK);
    check_int_eq(turbo_flow_stop(router), TURBO_OK);
    turbo_flow_msg_cleanup(&router_capture.message);
    turbo_flow_fmq_broker_destroy(broker);
    turbo_flow_destroy(client);
    turbo_flow_destroy(worker);
    turbo_flow_destroy(router);
  }

  it("pipelines TFCW jobs and fences a reconnected worker generation") {
    static const char *router_dsl = "source inbound adapter fmq.router\n"
                                    "source routed\n"
                                    "stage capture\n"
                                    "stage send adapter fmq.router\n"
                                    "stage main {\n"
                                    "  inbound -> capture\n"
                                    "  routed -> send\n"
                                    "}\n";
    static const char *dealer_dsl = "source response adapter fmq.dealer\n"
                                    "source input\n"
                                    "stage send adapter fmq.dealer\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  input -> send\n"
                                    "  response -> capture\n"
                                    "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t router_config;
    turbo_flow_fmq_config_t worker_config;
    turbo_flow_fmq_config_t client_config;
    turbo_flow_fmq_credit_worker_config_t owner_config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_worker_t *owner;
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_completion_result_t completion =
        TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    turbo_flow_fmq_broker_expire_result_t expired = TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;
    turbo_flow_fmq_credit_worker_snapshot_t snapshot = TURBO_FLOW_FMQ_CREDIT_WORKER_SNAPSHOT_INIT;
    turbo_flow_tfcw_envelope_t envelope = TURBO_FLOW_TFCW_ENVELOPE_INIT;
    turbo_flow_tfcw_envelope_t decoded = TURBO_FLOW_TFCW_ENVELOPE_INIT;
    turbo_flow_tfcw_fields_t fields = TURBO_FLOW_TFCW_FIELDS_INIT;
    fmq_broker_route_capture_t router_capture;
    fmq_capture_state_t worker_capture;
    fmq_capture_state_t client_capture;
    turbo_flow_t *router = turbo_flow_create();
    turbo_flow_t *worker = turbo_flow_create();
    turbo_flow_t *client = turbo_flow_create();
    turbo_flow_msg_t routed;
    uint8_t body[128];
    uint8_t frame[256];
    size_t body_size;
    size_t frame_size = 0u;
    const turbo_flow_protocol_route_t *route;
    uint64_t first_worker_session_id;
    uint64_t first_router_generation;
    char worker_id[TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX + 1u] = {0};
    char service[TURBO_FLOW_FMQ_BROKER_SERVICE_MAX + 1u] = {0};

    check_int_gt(port, 0);
    check_not_null(router);
    check_not_null(worker);
    check_not_null(client);
    owner_config.max_workers = 1u;
    owner_config.max_inflight = 2u;
    owner_config.worker_lease_ms = 1000u;
    owner_config.max_credit_messages_per_worker = 2u;
    owner_config.max_credit_bytes_per_worker = 512u;
    owner_config.max_job_bytes = sizeof(frame);
    owner = turbo_flow_fmq_credit_worker_create(&owner_config);
    check_not_null(owner);
    fmq_broker_route_capture_init(&router_capture);
    fmq_init_capture(&worker_capture);
    fmq_init_capture(&client_capture);
    fmq_config(&router_config, TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&worker_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    fmq_config(&client_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    worker_config.identity = "credit-worker-1";
    client_config.identity = "credit-client-1";
    check_int_eq(fmq_register_private_adapter(router, "fmq.router", &router_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(router, "capture", fmq_broker_capture_route,
                                              &router_capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(router, router_dsl, strlen(router_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(router), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(worker, "fmq.dealer", &worker_config), TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(worker, "capture", fmq_capture, &worker_capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(worker, dealer_dsl, strlen(dealer_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(worker), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(client, "fmq.dealer", &client_config), TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(client, "capture", fmq_capture, &client_capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(client, dealer_dsl, strlen(dealer_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(client), TURBO_OK);
    check_int_eq(turbo_flow_start(router), TURBO_OK);
    check_int_eq(turbo_flow_start(worker), TURBO_OK);
    check_int_eq(turbo_flow_start(client), TURBO_OK);

    body_size = fmq_tfcw_ready_body(body, sizeof(body), "credit-worker-1", "jobs", 2u, 512u);
    envelope.kind = TURBO_FLOW_TFCW_READY;
    envelope.credit_sequence = 1u;
    envelope.sender_timestamp_ms = 10u;
    envelope.body = body;
    envelope.body_size = body_size;
    check_int_eq(turbo_flow_tfcw_encode(&envelope, frame, sizeof(frame), &frame_size), TURBO_OK);
    check_int_eq(fmq_publish_bytes(worker, (const char *)frame, frame_size), TURBO_OK);
    fmq_wait_broker_capture(&router_capture);
    check_int_eq(atomic_load_explicit(&router_capture.capture_status, memory_order_acquire),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfcw_decode((const uint8_t *)router_capture.message.payload.data,
                                        router_capture.message.payload.len, &decoded),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfcw_fields_decode(&decoded, &fields), TURBO_OK);
    memcpy(worker_id, fields.worker_id.data, fields.worker_id.len);
    memcpy(service, fields.service.data, fields.service.len);
    route = turbo_flow_msg_protocol_route(&router_capture.message);
    check_not_null(route);
    first_worker_session_id = route->session_id;
    first_router_generation = route->session_generation;
    grant.worker_id = worker_id;
    grant.service = service;
    grant.worker_route = *route;
    grant.sequence = decoded.credit_sequence;
    grant.grant_messages = (size_t)fields.grant_messages;
    grant.grant_bytes = (size_t)fields.grant_bytes;
    grant.now_ms = 10u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant), TURBO_OK);

    fmq_broker_route_capture_reset(&router_capture);
    check_int_eq(fmq_publish_payload(client, "request-101"), TURBO_OK);
    fmq_wait_broker_capture(&router_capture);
    route = turbo_flow_msg_protocol_route(&router_capture.message);
    check_not_null(route);
    body_size = fmq_tfcw_job_body(body, sizeof(body), "credit-client-1/101", "alpha");
    envelope = (turbo_flow_tfcw_envelope_t)TURBO_FLOW_TFCW_ENVELOPE_INIT;
    envelope.kind = TURBO_FLOW_TFCW_JOB;
    envelope.request_id = 101u;
    envelope.body = body;
    envelope.body_size = body_size;
    check_int_eq(turbo_flow_tfcw_encode(&envelope, frame, sizeof(frame), &frame_size), TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", 101u, frame_size, route, 20u,
                                                       &dispatch),
                 TURBO_OK);
    turbo_flow_msg_init(&routed);
    routed.owned_payload = tstr_new_len(frame, frame_size);
    check_not_null(routed.owned_payload);
    routed.payload = tstr_to_v(routed.owned_payload);
    check_int_eq(turbo_flow_msg_set_protocol_route(&routed, &dispatch.worker_route), TURBO_OK);
    check_int_eq(turbo_flow_publish(router, "routed", &routed), TURBO_OK);
    turbo_flow_msg_cleanup(&routed);
    fmq_wait_called(&worker_capture, 1);
    decoded = (turbo_flow_tfcw_envelope_t)TURBO_FLOW_TFCW_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfcw_decode((const uint8_t *)worker_capture.payload,
                                        worker_capture.payload_len, &decoded),
                 TURBO_OK);
    check_uint_eq(decoded.request_id, 101u);

    fmq_broker_route_capture_reset(&router_capture);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(fmq_publish_payload(client, "request-102"), TURBO_OK);
    fmq_wait_broker_capture(&router_capture);
    route = turbo_flow_msg_protocol_route(&router_capture.message);
    body_size = fmq_tfcw_job_body(body, sizeof(body), "credit-client-1/102", "beta");
    envelope.request_id = 102u;
    envelope.body_size = body_size;
    check_int_eq(turbo_flow_tfcw_encode(&envelope, frame, sizeof(frame), &frame_size), TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", 102u, frame_size, route, 21u,
                                                       &dispatch),
                 TURBO_OK);
    turbo_flow_msg_init(&routed);
    routed.owned_payload = tstr_new_len(frame, frame_size);
    routed.payload = tstr_to_v(routed.owned_payload);
    check_int_eq(turbo_flow_msg_set_protocol_route(&routed, &dispatch.worker_route), TURBO_OK);
    check_int_eq(turbo_flow_publish(router, "routed", &routed), TURBO_OK);
    turbo_flow_msg_cleanup(&routed);
    fmq_wait_called(&worker_capture, 2);
    decoded = (turbo_flow_tfcw_envelope_t)TURBO_FLOW_TFCW_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfcw_decode((const uint8_t *)worker_capture.payload,
                                        worker_capture.payload_len, &decoded),
                 TURBO_OK);
    check_uint_eq(decoded.request_id, 102u);

    body_size = fmq_tfcw_complete_body(body, sizeof(body), "credit-worker-1");
    envelope = (turbo_flow_tfcw_envelope_t)TURBO_FLOW_TFCW_ENVELOPE_INIT;
    envelope.kind = TURBO_FLOW_TFCW_COMPLETE;
    envelope.request_id = 101u;
    envelope.body = body;
    envelope.body_size = body_size;
    check_int_eq(turbo_flow_tfcw_encode(&envelope, frame, sizeof(frame), &frame_size), TURBO_OK);
    fmq_broker_route_capture_reset(&router_capture);
    check_int_eq(fmq_publish_bytes(worker, (const char *)frame, frame_size), TURBO_OK);
    fmq_wait_broker_capture(&router_capture);
    decoded = (turbo_flow_tfcw_envelope_t)TURBO_FLOW_TFCW_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfcw_decode((const uint8_t *)router_capture.message.payload.data,
                                        router_capture.message.payload.len, &decoded),
                 TURBO_OK);
    route = turbo_flow_msg_protocol_route(&router_capture.message);
    completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_credit_worker_complete(owner, "credit-worker-1", route, 101u, &completion),
        TURBO_OK);
    check_int_eq(
        turbo_flow_msg_set_protocol_route(&router_capture.message, &completion.client_route),
        TURBO_OK);
    check_int_eq(turbo_flow_publish(router, "routed", &router_capture.message), TURBO_OK);
    fmq_wait_called(&client_capture, 1);

    check_int_eq(turbo_flow_stop(worker), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(router, 1), TURBO_OK);
    check_int_eq(turbo_flow_start(worker), TURBO_OK);
    check_int_eq(fmq_wait_connection_count(router, 2), TURBO_OK);
    body_size = fmq_tfcw_ready_body(body, sizeof(body), "credit-worker-1", "jobs", 2u, 512u);
    envelope = (turbo_flow_tfcw_envelope_t)TURBO_FLOW_TFCW_ENVELOPE_INIT;
    envelope.kind = TURBO_FLOW_TFCW_READY;
    envelope.credit_sequence = 1u;
    envelope.sender_timestamp_ms = 1020u;
    envelope.body = body;
    envelope.body_size = body_size;
    check_int_eq(turbo_flow_tfcw_encode(&envelope, frame, sizeof(frame), &frame_size), TURBO_OK);
    fmq_broker_route_capture_reset(&router_capture);
    check_int_eq(fmq_publish_bytes(worker, (const char *)frame, frame_size), TURBO_OK);
    fmq_wait_broker_capture(&router_capture);
    decoded = (turbo_flow_tfcw_envelope_t)TURBO_FLOW_TFCW_ENVELOPE_INIT;
    fields = (turbo_flow_tfcw_fields_t)TURBO_FLOW_TFCW_FIELDS_INIT;
    check_int_eq(turbo_flow_tfcw_decode((const uint8_t *)router_capture.message.payload.data,
                                        router_capture.message.payload.len, &decoded),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfcw_fields_decode(&decoded, &fields), TURBO_OK);
    route = turbo_flow_msg_protocol_route(&router_capture.message);
    check_not_null(route);
    check_true(route->session_id != first_worker_session_id);
    check_uint_eq(route->session_generation, first_router_generation);
    completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_credit_worker_complete(owner, "credit-worker-1", route, 102u, &completion),
        TURBO_EPROTO);
    grant.worker_route = *route;
    grant.sequence = decoded.credit_sequence;
    grant.grant_messages = (size_t)fields.grant_messages;
    grant.grant_bytes = (size_t)fields.grant_bytes;
    grant.now_ms = 1020u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant), TURBO_EBUSY);

    check_int_eq(turbo_flow_fmq_credit_worker_expire(owner, 1010u, &expired), TURBO_OK);
    check_int_eq(expired.disposition, TURBO_FLOW_FMQ_BROKER_EXPIRED_DROP);
    check_uint_eq(expired.request_id, 102u);
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant), TURBO_OK);
    completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_credit_worker_complete(owner, "credit-worker-1", route, 102u, &completion),
        TURBO_ENOENT);

    fmq_broker_route_capture_reset(&router_capture);
    check_int_eq(fmq_publish_payload(client, "request-103"), TURBO_OK);
    fmq_wait_broker_capture(&router_capture);
    route = turbo_flow_msg_protocol_route(&router_capture.message);
    check_not_null(route);
    body_size = fmq_tfcw_job_body(body, sizeof(body), "credit-client-1/103", "gamma");
    envelope = (turbo_flow_tfcw_envelope_t)TURBO_FLOW_TFCW_ENVELOPE_INIT;
    envelope.kind = TURBO_FLOW_TFCW_JOB;
    envelope.request_id = 103u;
    envelope.body = body;
    envelope.body_size = body_size;
    check_int_eq(turbo_flow_tfcw_encode(&envelope, frame, sizeof(frame), &frame_size), TURBO_OK);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", 103u, frame_size, route,
                                                       1030u, &dispatch),
                 TURBO_OK);
    turbo_flow_msg_init(&routed);
    routed.owned_payload = tstr_new_len(frame, frame_size);
    check_not_null(routed.owned_payload);
    routed.payload = tstr_to_v(routed.owned_payload);
    check_int_eq(turbo_flow_msg_set_protocol_route(&routed, &dispatch.worker_route), TURBO_OK);
    check_int_eq(turbo_flow_publish(router, "routed", &routed), TURBO_OK);
    turbo_flow_msg_cleanup(&routed);
    fmq_wait_called(&worker_capture, 3);
    decoded = (turbo_flow_tfcw_envelope_t)TURBO_FLOW_TFCW_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfcw_decode((const uint8_t *)worker_capture.payload,
                                        worker_capture.payload_len, &decoded),
                 TURBO_OK);
    check_uint_eq(decoded.request_id, 103u);

    body_size = fmq_tfcw_complete_body(body, sizeof(body), "credit-worker-1");
    envelope = (turbo_flow_tfcw_envelope_t)TURBO_FLOW_TFCW_ENVELOPE_INIT;
    envelope.kind = TURBO_FLOW_TFCW_COMPLETE;
    envelope.request_id = 103u;
    envelope.body = body;
    envelope.body_size = body_size;
    check_int_eq(turbo_flow_tfcw_encode(&envelope, frame, sizeof(frame), &frame_size), TURBO_OK);
    fmq_broker_route_capture_reset(&router_capture);
    check_int_eq(fmq_publish_bytes(worker, (const char *)frame, frame_size), TURBO_OK);
    fmq_wait_broker_capture(&router_capture);
    route = turbo_flow_msg_protocol_route(&router_capture.message);
    completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_credit_worker_complete(owner, "credit-worker-1", route, 103u, &completion),
        TURBO_OK);
    check_int_eq(
        turbo_flow_msg_set_protocol_route(&router_capture.message, &completion.client_route),
        TURBO_OK);
    check_int_eq(turbo_flow_publish(router, "routed", &router_capture.message), TURBO_OK);
    fmq_wait_called(&client_capture, 2);

    check_int_eq(turbo_flow_fmq_credit_worker_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.inflight, 0u);
    check_size_eq(snapshot.workers, 1u);
    check_size_eq(snapshot.available_messages, 1u);
    check_size_gt(snapshot.available_bytes, 0u);
    check_size_lt(snapshot.available_bytes, 512u);
    check_uint_eq(snapshot.completed, 2u);
    check_uint_eq(snapshot.expired_workers, 1u);
    check_uint_eq(snapshot.expired_requests, 1u);

    check_int_eq(turbo_flow_stop(client), TURBO_OK);
    check_int_eq(turbo_flow_stop(worker), TURBO_OK);
    check_int_eq(turbo_flow_stop(router), TURBO_OK);
    turbo_flow_msg_cleanup(&router_capture.message);
    turbo_flow_fmq_credit_worker_destroy(owner);
    turbo_flow_destroy(client);
    turbo_flow_destroy(worker);
    turbo_flow_destroy(router);
  }

  it("supports one bidirectional PAIR peer") {
    static const char *bind_dsl = "source request adapter fmq.pair\n"
                                  "stage reply adapter fmq.pair\n"
                                  "stage main {\n"
                                  "  request -> reply\n"
                                  "}\n";
    static const char *connect_dsl = "source response adapter fmq.pair\n"
                                     "source input\n"
                                     "stage send adapter fmq.pair\n"
                                     "stage capture\n"
                                     "stage main {\n"
                                     "  input -> send\n"
                                     "  response -> capture\n"
                                     "}\n";
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t bind_config;
    turbo_flow_fmq_config_t connect_config;
    fmq_capture_state_t capture;
    turbo_flow_t *bound = turbo_flow_create();
    turbo_flow_t *connected = turbo_flow_create();
    check_int_gt(port, 0);
    check_not_null(bound);
    check_not_null(connected);
    fmq_init_capture(&capture);
    fmq_config(&bind_config, TURBO_FLOW_FMQ_PAIR, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&connect_config, TURBO_FLOW_FMQ_PAIR, TURBO_FLOW_FMQ_CONNECT, port);
    check_int_eq(fmq_register_private_adapter(bound, "fmq.pair", &bind_config), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(bound, bind_dsl, strlen(bind_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(bound), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(connected, "fmq.pair", &connect_config), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(connected, "capture", fmq_capture, &capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(connected, connect_dsl, strlen(connect_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(connected), TURBO_OK);
    check_int_eq(turbo_flow_start(bound), TURBO_OK);
    check_int_eq(turbo_flow_start(connected), TURBO_OK);
    check_int_eq(fmq_publish_payload(connected, "pair-data"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 9);
    check_mem_eq(capture.payload, "pair-data", 9);
    check_int_eq(turbo_flow_stop(connected), TURBO_OK);
    check_int_eq(turbo_flow_stop(bound), TURBO_OK);
    turbo_flow_destroy(connected);
    turbo_flow_destroy(bound);
  }

  it("delivers PUB SUB messages over WSS") {
    unsigned short port = fmq_test_port();
    char ca_file[512];
    char cert_file[512];
    char key_file[512];
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;

    check_int_gt(port, 0);
    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_int_eq(tls_test_set_server_env(cert_file, key_file), 0);
    check_int_eq(tls_test_set_ca_file_env(ca_file), 0);
    fmq_init_capture(&capture);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    pub_config.transport = TURBO_FLOW_FMQ_WSS;
    pub_config.topic = "secure-ws";
    pub_config.path = "/fmq";
    sub_config.transport = TURBO_FLOW_FMQ_WSS;
    sub_config.host = "localhost";
    sub_config.topic = "secure-ws";
    sub_config.path = "/fmq";
    publisher = fmq_make_sink_flow("fmq.output", &pub_config);
    subscriber = fmq_make_source_flow("fmq.input", &sub_config, &capture);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "wss-payload"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 11);
    check_mem_eq(capture.payload, "wss-payload", 11);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(publisher);
    tls_test_clear_ca_env();
    tls_test_clear_server_env();
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
  }

  it("authenticates TLS peers and rejects a hostname mismatch") {
    unsigned short port = fmq_test_port();
    char ca_file[512];
    char cert_file[512];
    char key_file[512];
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    turbo_flow_t *untrusted;
    int rc;
    check_int_gt(port, 0);
    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_int_eq(tls_test_set_server_env(cert_file, key_file), 0);
    check_int_eq(tls_test_set_ca_file_env(ca_file), 0);
    fmq_init_capture(&capture);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    pub_config.transport = TURBO_FLOW_FMQ_TLS;
    pub_config.topic = "secure";
    sub_config.transport = TURBO_FLOW_FMQ_TLS;
    sub_config.host = "localhost";
    sub_config.topic = "secure";
    publisher = fmq_make_sink_flow("fmq.output", &pub_config);
    subscriber = fmq_make_source_flow("fmq.input", &sub_config, &capture);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "encrypted"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    turbo_flow_destroy(subscriber);

    sub_config.host = "127.0.0.1";
    untrusted = fmq_make_source_flow("fmq.input", &sub_config, &capture);
    check_not_null(untrusted);
    rc = turbo_flow_start(untrusted);
    check_true(rc != TURBO_OK);
    turbo_flow_destroy(untrusted);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(publisher);
    tls_test_clear_ca_env();
    tls_test_clear_server_env();
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
  }

  it("authenticates and authorizes v3 HELLO over every CoroNet transport") {
    static const struct {
      turbo_flow_fmq_transport_t transport;
      const char *path;
    } cases[] = {
        {TURBO_FLOW_FMQ_TCP, NULL},         {TURBO_FLOW_FMQ_KCP, NULL},
        {TURBO_FLOW_FMQ_UDP, NULL},         {TURBO_FLOW_FMQ_TLS, NULL},
        {TURBO_FLOW_FMQ_WS, "/fmq-secure"}, {TURBO_FLOW_FMQ_WSS, "/fmq-secure"},
#ifdef _WIN32
        {TURBO_FLOW_FMQ_PIPE, NULL},
#endif
    };
    char ca_file[512];
    char cert_file[512];
    char key_file[512];
    turbo_flow_security_rule_t rules[3];
    turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    turbo_flow_security_auth_provider_t auth_provider = TURBO_FLOW_SECURITY_AUTH_PROVIDER_INIT;
    turbo_flow_security_key_provider_t key_provider = TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT;
    turbo_flow_fmq_security_binding_t server_security = TURBO_FLOW_FMQ_SECURITY_BINDING_INIT;
    turbo_flow_fmq_security_binding_t client_security = TURBO_FLOW_FMQ_SECURITY_BINDING_INIT;
    turbo_flow_fmq_tls_config_t server_tls = TURBO_FLOW_FMQ_TLS_CONFIG_INIT;
    turbo_flow_fmq_tls_config_t client_tls = TURBO_FLOW_FMQ_TLS_CONFIG_INIT;

    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_int_eq(tls_test_set_server_env(cert_file, key_file), 0);
    check_int_eq(tls_test_set_ca_file_env(ca_file), 0);
    server_tls.cert_file = cert_file;
    server_tls.key_file = key_file;
    server_tls.rotation_generation = 1u;
    client_tls.ca_file = ca_file;
    client_tls.server_name = "localhost";
    client_tls.rotation_generation = 1u;
    memset(rules, 0, sizeof(rules));
    for (size_t i = 0u; i < 3u; ++i) {
      rules[i] = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
      rules[i].effect = TURBO_FLOW_SECURITY_ALLOW;
      rules[i].subject_kind = TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL;
      (void)snprintf(rules[i].subject, sizeof(rules[i].subject), "client-a");
      (void)snprintf(rules[i].domain_id, sizeof(rules[i].domain_id), "root-a");
      rules[i].resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
      rules[i].match_kind = TURBO_FLOW_SECURITY_MATCH_EXACT;
    }
    rules[0].action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    (void)snprintf(rules[0].pattern, sizeof(rules[0].pattern), "fmq:fmq.output:connection");
    rules[1].action_mask = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    (void)snprintf(rules[1].pattern, sizeof(rules[1].pattern), "secure");
    rules[2].action_mask = TURBO_FLOW_SECURITY_ACTION_READ;
    (void)snprintf(rules[2].pattern, sizeof(rules[2].pattern), "secure");
    realm_config.resource_uid = "security/fmq-transport-test";
    realm_config.owner_name = "test";
    realm_config.policy_version = 1u;
    realm_config.rules = rules;
    realm_config.rule_count = 3u;
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    check_not_null(realm);

    auth_provider.ctx = (void *)"correct-secret";
    auth_provider.authenticate = fmq_test_authenticate;
    key_provider.ctx = (void *)"correct-secret";
    key_provider.acquire = fmq_test_secret_acquire;
    key_provider.release = fmq_test_secret_release;
    server_security.realm_channel = "security.fmq";
    server_security.auth_method = "token";
    server_security.auth_provider = &auth_provider;
    server_security.realm = realm;
    client_security.auth_method = "token";
    client_security.key_provider = &key_provider;
    client_security.secret_reference = "secret/fmq-client";

    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      unsigned short port =
          cases[i].transport == TURBO_FLOW_FMQ_UDP || cases[i].transport == TURBO_FLOW_FMQ_KCP
              ? fmq_test_udp_port()
              : fmq_test_port();
      char pipe_path[128];
      turbo_flow_fmq_config_t pub_config;
      turbo_flow_fmq_config_t sub_config;
      fmq_capture_state_t capture;
      turbo_flow_t *publisher;
      turbo_flow_t *subscriber;

      check_int_gt(port, 0);
      fmq_init_capture(&capture);
      fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
      fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
      pub_config.transport = cases[i].transport;
      pub_config.topic = "secure";
      pub_config.path = cases[i].path;
      sub_config.transport = cases[i].transport;
      if (cases[i].transport == TURBO_FLOW_FMQ_KCP) {
        pub_config.kcp_pre_shared_key = FMQ_TEST_KCP_PSK;
        sub_config.kcp_pre_shared_key = FMQ_TEST_KCP_PSK;
      }
      sub_config.host =
          cases[i].transport == TURBO_FLOW_FMQ_TLS || cases[i].transport == TURBO_FLOW_FMQ_WSS
              ? "localhost"
              : "127.0.0.1";
      sub_config.topic = "secure";
      sub_config.path = cases[i].path;
      sub_config.identity = "client-a";
      sub_config.reconnect_initial_ms = TURBO_FLOW_FMQ_RECONNECT_DISABLED;
      if (cases[i].transport == TURBO_FLOW_FMQ_TLS ||
          cases[i].transport == TURBO_FLOW_FMQ_WSS) {
        pub_config.tls = &server_tls;
        sub_config.tls = &client_tls;
      }
      if (cases[i].transport == TURBO_FLOW_FMQ_PIPE) {
        check_int_gt(
            snprintf(pipe_path, sizeof(pipe_path), "pipe://turbo_flow_fmq_secure_%u", port), 0);
        pub_config.host = NULL;
        pub_config.port = 0;
        pub_config.path = pipe_path;
        sub_config.host = NULL;
        sub_config.port = 0;
        sub_config.path = pipe_path;
      }
      check_int_eq(flow_fmq_config_validate(&pub_config), TURBO_OK);
      check_int_eq(flow_fmq_config_validate(&sub_config), TURBO_OK);
      publisher = fmq_make_sink_flow_with_security("fmq.output", &pub_config, &server_security);
      subscriber =
          fmq_make_source_flow_with_security("fmq.input", &sub_config, &capture, &client_security);
      check_not_null(publisher);
      check_not_null(subscriber);
      check_int_eq(turbo_flow_start(publisher), TURBO_OK);
      check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
      {
        int publish_rc = fmq_publish_payload_after_peer_ready(publisher, "authorized");
        check_int_eq(publish_rc, TURBO_OK);
      }
      fmq_wait_called(&capture, 1);
      check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
      check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
      check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
      turbo_flow_destroy(subscriber);
      turbo_flow_destroy(publisher);
    }

    turbo_flow_security_realm_destroy(realm);
    tls_test_clear_ca_env();
    tls_test_clear_server_env();
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
  }

  it("authenticates v3 HELLO before peer admission and enforces default-deny ACL") {
    unsigned short port = fmq_test_port();
    char ca_file[512];
    char cert_file[512];
    char key_file[512];
    turbo_flow_security_rule_t rules[3];
    turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    turbo_flow_security_auth_provider_t auth_provider = TURBO_FLOW_SECURITY_AUTH_PROVIDER_INIT;
    turbo_flow_security_key_provider_t key_provider = TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT;
    turbo_flow_fmq_security_binding_t server_security = TURBO_FLOW_FMQ_SECURITY_BINDING_INIT;
    turbo_flow_fmq_security_binding_t client_security = TURBO_FLOW_FMQ_SECURITY_BINDING_INIT;
    turbo_flow_fmq_tls_config_t server_tls = TURBO_FLOW_FMQ_TLS_CONFIG_INIT;
    turbo_flow_fmq_tls_config_t client_tls = TURBO_FLOW_FMQ_TLS_CONFIG_INIT;
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    fmq_capture_state_t capture;
    fmq_event_state_t server_events;
    fmq_event_state_t client_events;
    fmq_certificate_identity_state_t certificate_identity;
    turbo_flow_t *publisher;
    turbo_flow_t *subscriber;
    turbo_flow_t *certificate_denied;
    turbo_flow_t *identity_denied;
    turbo_flow_t *auth_denied;
    turbo_flow_t *denied;
    int start_rc;

    check_int_gt(port, 0);
    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_int_eq(tls_test_set_server_env(cert_file, key_file), 0);
    check_int_eq(tls_test_set_ca_file_env(ca_file), 0);
    server_tls.cert_file = cert_file;
    server_tls.key_file = key_file;
    server_tls.ca_file = ca_file;
    server_tls.require_client_certificate = 1;
    server_tls.rotation_generation = 1u;
    client_tls.ca_file = ca_file;
    client_tls.cert_file = cert_file;
    client_tls.key_file = key_file;
    client_tls.server_name = "localhost";
    client_tls.rotation_generation = 1u;
    certificate_identity.expected_fingerprint = FMQ_TEST_CERTIFICATE_SHA256;
    certificate_identity.expected_identity = "client-a";
    atomic_init(&certificate_identity.calls, 0);

    memset(rules, 0, sizeof(rules));
    for (size_t i = 0u; i < 3u; ++i) {
      rules[i] = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
      rules[i].effect = TURBO_FLOW_SECURITY_ALLOW;
      rules[i].subject_kind = TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL;
      (void)snprintf(rules[i].subject, sizeof(rules[i].subject), "client-a");
      (void)snprintf(rules[i].domain_id, sizeof(rules[i].domain_id), "root-a");
      rules[i].resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
      rules[i].match_kind = TURBO_FLOW_SECURITY_MATCH_EXACT;
    }
    rules[0].action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    (void)snprintf(rules[0].pattern, sizeof(rules[0].pattern), "fmq:fmq.output:connection");
    rules[1].action_mask = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    (void)snprintf(rules[1].pattern, sizeof(rules[1].pattern), "secure");
    rules[2].action_mask = TURBO_FLOW_SECURITY_ACTION_READ;
    (void)snprintf(rules[2].pattern, sizeof(rules[2].pattern), "secure");
    realm_config.resource_uid = "security/fmq-test";
    realm_config.owner_name = "test";
    realm_config.policy_version = 1u;
    realm_config.rules = rules;
    realm_config.rule_count = 3u;
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    check_not_null(realm);

    auth_provider.ctx = (void *)"correct-secret";
    auth_provider.authenticate = fmq_test_authenticate;
    key_provider.ctx = (void *)"correct-secret";
    key_provider.acquire = fmq_test_secret_acquire;
    key_provider.release = fmq_test_secret_release;
    server_security.realm_channel = "security.fmq";
    server_security.auth_method = "token";
    server_security.auth_provider = &auth_provider;
    server_security.realm = realm;
    server_security.verify_peer_certificate_identity = fmq_test_verify_certificate_identity;
    server_security.peer_certificate_identity_ctx = &certificate_identity;
    client_security.auth_method = "token";
    client_security.key_provider = &key_provider;
    client_security.secret_reference = "secret/fmq-client";

    fmq_init_capture(&capture);
    fmq_init_events(&server_events);
    fmq_init_events(&client_events);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    pub_config.transport = TURBO_FLOW_FMQ_TLS;
    pub_config.tls = &server_tls;
    pub_config.topic = "secure";
    pub_config.event_callback = fmq_event_capture;
    pub_config.event_ctx = &server_events;
    sub_config.transport = TURBO_FLOW_FMQ_TLS;
    sub_config.tls = &client_tls;
    sub_config.host = "localhost";
    sub_config.topic = "secure";
    sub_config.identity = "client-a";
    sub_config.reconnect_initial_ms = TURBO_FLOW_FMQ_RECONNECT_DISABLED;
    sub_config.event_callback = fmq_event_capture;
    sub_config.event_ctx = &client_events;
    publisher = fmq_make_sink_flow_with_security("fmq.output", &pub_config, &server_security);
    subscriber =
        fmq_make_source_flow_with_security("fmq.input", &sub_config, &capture, &client_security);
    check_not_null(publisher);
    check_not_null(subscriber);
    check_int_eq(turbo_flow_start(publisher), TURBO_OK);
    start_rc = turbo_flow_start(subscriber);
    check_int_eq(atomic_load_explicit(&server_events.last_status, memory_order_acquire), TURBO_OK);
    check_int_eq(atomic_load_explicit(&server_events.authentication_failed, memory_order_acquire),
                 0);
    check_int_eq(atomic_load_explicit(&server_events.authorization_denied, memory_order_acquire),
                 0);
    fmq_wait_event_count(&server_events.peer_connected, 1);
    check_int_eq(atomic_load_explicit(&server_events.peer_connected, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&client_events.last_status, memory_order_acquire), TURBO_OK);
    check_int_eq(start_rc, TURBO_OK);
    check_int_eq(fmq_publish_payload(publisher, "authorized"), TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    turbo_flow_destroy(subscriber);
    check_int_eq(atomic_load_explicit(&certificate_identity.calls, memory_order_acquire), 1);

    certificate_identity.expected_fingerprint =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000";
    certificate_denied =
        fmq_make_source_flow_with_security("fmq.input", &sub_config, &capture, &client_security);
    check_not_null(certificate_denied);
    check_true(turbo_flow_start(certificate_denied) != TURBO_OK);
    turbo_flow_destroy(certificate_denied);
    check_int_ge(atomic_load_explicit(&certificate_identity.calls, memory_order_acquire), 2);
    certificate_identity.expected_fingerprint = FMQ_TEST_CERTIFICATE_SHA256;

    sub_config.identity = "client-b";
    identity_denied =
        fmq_make_source_flow_with_security("fmq.input", &sub_config, &capture, &client_security);
    check_not_null(identity_denied);
    check_true(turbo_flow_start(identity_denied) != TURBO_OK);
    turbo_flow_destroy(identity_denied);
    check_int_ge(atomic_load_explicit(&certificate_identity.calls, memory_order_acquire), 3);
    sub_config.identity = "client-a";

    key_provider.ctx = (void *)"wrong-secret";
    auth_denied =
        fmq_make_source_flow_with_security("fmq.input", &sub_config, &capture, &client_security);
    check_not_null(auth_denied);
    check_true(turbo_flow_start(auth_denied) != TURBO_OK);
    turbo_flow_destroy(auth_denied);
    check_int_ge(atomic_load_explicit(&server_events.authentication_failed, memory_order_acquire),
                 1);
    key_provider.ctx = (void *)"correct-secret";
    sub_config.topic = "forbidden";
    denied =
        fmq_make_source_flow_with_security("fmq.input", &sub_config, &capture, &client_security);
    check_not_null(denied);
    check_true(turbo_flow_start(denied) != TURBO_OK);
    turbo_flow_destroy(denied);
    check_int_ge(atomic_load_explicit(&server_events.authorization_denied, memory_order_acquire),
                 1);
    check_int_eq(atomic_load_explicit(&server_events.peer_connected, memory_order_acquire), 1);
    check_int_eq(turbo_flow_stop(publisher), TURBO_OK);
    turbo_flow_destroy(publisher);
    turbo_flow_security_realm_destroy(realm);
    tls_test_clear_ca_env();
    tls_test_clear_server_env();
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
  }
}

spec("flow_fmq_application") {
  it("keeps explicitly borrowed facade contexts owned by the host") {
    unsigned short port = fmq_test_port();
    coro_context_t *context = coro_context_create(NULL);
    fmq_context_runner_t runner;
    turbo_flow_coronet_execution_binding_t execution;
    turbo_flow_fmq_config_t config;
    turbo_flow_fmq_app_options_t options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_t *app = NULL;
    fmq_context_post_state_t post_state;

    memset(&runner, 0, sizeof(runner));
    memset(&execution, 0, sizeof(execution));
    atomic_init(&post_state.observed, 0);
    check_int_gt(port, 0);
    check_not_null(context);
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    check_int_eq(turbo_flow_fmq_app_create_ex(&config, &options, NULL, &app), TURBO_EINVAL);
    check_null(app);
    execution.size = sizeof(execution);
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT;
    execution.context = context;
    check_int_eq(turbo_flow_fmq_app_create_ex(&config, &options, &execution, &app),
                 TURBO_EINVAL);
    check_null(app);
    check_int_eq(turbo_flow_fmq_app_create_secure_ex(&config, &options, &execution, NULL, &app),
                 TURBO_EINVAL);
    check_null(app);

    execution.kind = TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
    options.on_message = fmq_app_capture;
    check_int_eq(turbo_flow_fmq_app_create_ex(&config, &options, &execution, &app),
                 TURBO_EINVAL);
    check_null(app);

    check_int_eq(fmq_context_runner_start(&runner, context), TURBO_OK);
    check_int_eq(coro_post(context, fmq_context_mark_post, &post_state, NULL), TURBO_OK);
    fmq_wait_context_post(&post_state);
    check_int_eq(atomic_load_explicit(&post_state.observed, memory_order_acquire), 1);

    options.on_message = NULL;
    check_int_eq(turbo_flow_fmq_app_create_ex(&config, &options, &execution, &app), TURBO_OK);
    check_not_null(app);
    if (app) {
      check_int_eq(turbo_flow_fmq_app_start(app), TURBO_OK);
      check_int_eq(turbo_flow_fmq_app_stop(app), TURBO_OK);
      turbo_flow_fmq_app_destroy(app);
      app = NULL;
    }
    atomic_store_explicit(&post_state.observed, 0, memory_order_release);
    check_int_eq(coro_post(context, fmq_context_mark_post, &post_state, NULL), TURBO_OK);
    fmq_wait_context_post(&post_state);
    check_int_eq(atomic_load_explicit(&post_state.observed, memory_order_acquire), 1);

    fmq_context_runner_stop(&runner);
    coro_context_destroy(context);
  }

  it("validates facade structure sizes and the pattern callback contract") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t config;
    turbo_flow_fmq_app_options_t options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_t *app = NULL;

    check_int_gt(port, 0);
    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    options.size -= 1u;
    check_int_eq(turbo_flow_fmq_app_create(&config, &options, &app), TURBO_EINVAL);
    check_null(app);

    options = (turbo_flow_fmq_app_options_t)TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    options.on_message = fmq_app_capture;
    check_int_eq(turbo_flow_fmq_app_create(&config, &options, &app), TURBO_EINVAL);
    check_null(app);

    fmq_config(&config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    options.on_message = NULL;
    check_int_eq(turbo_flow_fmq_app_create(&config, &options, &app), TURBO_EINVAL);
    check_null(app);

    fmq_config(&config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    check_int_eq(turbo_flow_fmq_app_create(&config, &options, &app), TURBO_OK);
    check_not_null(app);
    if (app) {
      turbo_flow_fmq_app_async_send_config_t async_config =
          TURBO_FLOW_FMQ_APP_ASYNC_SEND_CONFIG_INIT;
      async_config.size -= 1u;
      check_int_eq(turbo_flow_fmq_app_configure_async_send(app, &async_config), TURBO_EINVAL);
      async_config = (turbo_flow_fmq_app_async_send_config_t)
          TURBO_FLOW_FMQ_APP_ASYNC_SEND_CONFIG_INIT;
      async_config.batch_size = async_config.queue_capacity + 1u;
      check_int_eq(turbo_flow_fmq_app_configure_async_send(app, &async_config), TURBO_EINVAL);
      async_config = (turbo_flow_fmq_app_async_send_config_t)
          TURBO_FLOW_FMQ_APP_ASYNC_SEND_CONFIG_INIT;
      check_int_eq(turbo_flow_fmq_app_configure_async_send(app, &async_config), TURBO_OK);
      check_int_eq(turbo_flow_fmq_app_configure_async_send(app, &async_config), TURBO_EALREADY);
      check_int_eq(turbo_flow_fmq_app_send_async(app, "early", 5u, NULL, NULL), TURBO_EBUSY);
      turbo_flow_fmq_app_destroy(app);
    }
  }

  it("drains accepted asynchronous sends and enforces bounded admission") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t router_config;
    turbo_flow_fmq_config_t dealer_config;
    turbo_flow_fmq_app_options_t router_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t dealer_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_async_send_config_t async_config =
        TURBO_FLOW_FMQ_APP_ASYNC_SEND_CONFIG_INIT;
    turbo_flow_fmq_app_t *router = NULL;
    turbo_flow_fmq_app_t *dealer = NULL;
    fmq_capture_state_t capture;
    fmq_async_completion_state_t completion;
    char copied_payload[] = "last";
    int send_status = TURBO_ENOTCONN;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    atomic_init(&completion.called, 0);
    atomic_init(&completion.failures, 0);
    atomic_init(&completion.hold, 1);
    atomic_init(&completion.entered, 0);
    fmq_config(&router_config, TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&dealer_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    dealer_config.identity = "async-dealer";
    router_options.on_message = fmq_app_capture;
    router_options.message_ctx = &capture;
    async_config.queue_capacity = 3u;
    async_config.queue_capacity_bytes = 8u;
    async_config.batch_size = 1u;
    async_config.linger_ns = 0u;

    check_int_eq(turbo_flow_fmq_app_create(&router_config, &router_options, &router), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_create(&dealer_config, &dealer_options, &dealer), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_configure_async_send(dealer, &async_config), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(router), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(dealer), TURBO_OK);
    for (int i = 0; i < 400 && send_status == TURBO_ENOTCONN; ++i) {
      send_status = turbo_flow_fmq_app_send(dealer, "ready", 5u);
      if (send_status == TURBO_ENOTCONN) turbo_sleep_ms(5);
    }
    check_int_eq(send_status, TURBO_OK);
    fmq_wait_called(&capture, 1);

    check_int_eq(turbo_flow_fmq_app_send_async(
                     dealer, "one", 3u, fmq_async_completion, &completion),
                 TURBO_OK);
    for (int i = 0; i < 2000 &&
                    !atomic_load_explicit(&completion.entered, memory_order_acquire);
         ++i)
      turbo_sleep_ms(1);
    check_int_eq(atomic_load_explicit(&completion.entered, memory_order_acquire), 1);
    check_int_eq(turbo_flow_fmq_app_send_async(
                     dealer, "two", 3u, fmq_async_completion, &completion),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_send_async(
                     dealer, NULL, 0u, fmq_async_completion, &completion),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_send_async(
                     dealer, copied_payload, 4u, fmq_async_completion, &completion),
                 TURBO_OK);
    memcpy(copied_payload, "bad!", 4u);
    check_int_eq(turbo_flow_fmq_app_send_async(
                     dealer, "full", 4u, fmq_async_completion, &completion),
                 TURBO_ENOSPC);
    atomic_store_explicit(&completion.hold, 0, memory_order_release);
    check_int_eq(turbo_flow_fmq_app_stop(dealer), TURBO_OK);
    check_int_eq(atomic_load_explicit(&completion.called, memory_order_acquire), 4);
    check_int_eq(atomic_load_explicit(&completion.failures, memory_order_acquire), 0);
    fmq_wait_called(&capture, 5);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 5);
    check_size_eq(capture.payload_len, 4u);
    check_mem_eq(capture.payload, "last", 4u);
    check_int_eq(turbo_flow_fmq_app_send_async(
                     dealer, "stopped", 7u, fmq_async_completion, &completion),
                 TURBO_EBUSY);
    check_int_eq(turbo_flow_fmq_app_configure_async_send(dealer, &async_config), TURBO_EBUSY);
    check_int_eq(turbo_flow_fmq_app_stop(router), TURBO_OK);
    turbo_flow_fmq_app_destroy(dealer);
    turbo_flow_fmq_app_destroy(router);
  }

  it("reports asynchronous delivery failures through completion") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_app_options_t pub_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_async_send_config_t async_config =
        TURBO_FLOW_FMQ_APP_ASYNC_SEND_CONFIG_INIT;
    turbo_flow_fmq_app_t *publisher = NULL;
    fmq_async_completion_state_t completion;

    check_int_gt(port, 0);
    atomic_init(&completion.called, 0);
    atomic_init(&completion.failures, 0);
    atomic_init(&completion.hold, 0);
    atomic_init(&completion.entered, 0);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    async_config.queue_capacity = 2u;
    async_config.queue_capacity_bytes = 16u;
    async_config.batch_size = 2u;
    async_config.linger_ns = 0u;

    check_int_eq(turbo_flow_fmq_app_create(&pub_config, &pub_options, &publisher), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_configure_async_send(publisher, &async_config), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_send_async(
                     publisher, "orphan", 6u, fmq_async_completion, &completion),
                 TURBO_OK);
    for (int i = 0; i < 2000 &&
                    atomic_load_explicit(&completion.called, memory_order_acquire) == 0;
         ++i)
      turbo_sleep_ms(1);
    check_int_eq(atomic_load_explicit(&completion.called, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&completion.failures, memory_order_acquire), 1);
    check_int_eq(turbo_flow_fmq_app_stop(publisher), TURBO_OK);
    turbo_flow_fmq_app_destroy(publisher);
  }

  it("sends and receives through a graph-neutral PAIR facade Core") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t bound_config;
    turbo_flow_fmq_config_t connected_config;
    turbo_flow_fmq_app_options_t bound_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t connected_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_t *bound = NULL;
    turbo_flow_fmq_app_t *connected = NULL;
    fmq_capture_state_t capture;
    int send_status = TURBO_ENOTCONN;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_config(&bound_config, TURBO_FLOW_FMQ_PAIR, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&connected_config, TURBO_FLOW_FMQ_PAIR, TURBO_FLOW_FMQ_CONNECT, port);
    bound_options.on_message = fmq_app_capture;
    bound_options.message_ctx = &capture;

    check_int_eq(turbo_flow_fmq_app_create(&bound_config, &bound_options, &bound), TURBO_OK);
    check_not_null(bound);
    check_int_eq(turbo_flow_fmq_app_create(&connected_config, &connected_options, &connected),
                 TURBO_OK);
    check_not_null(connected);
    check_int_eq(turbo_flow_fmq_app_send(connected, "early", 5u), TURBO_EBUSY);
    check_int_eq(turbo_flow_fmq_app_start(bound), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(bound), TURBO_EALREADY);
    check_int_eq(turbo_flow_fmq_app_start(connected), TURBO_OK);
    for (int i = 0; i < 400 && send_status == TURBO_ENOTCONN; ++i) {
      send_status = turbo_flow_fmq_app_send(connected, "pair-data", 9u);
      if (send_status == TURBO_ENOTCONN) turbo_sleep_ms(5);
    }
    check_int_eq(send_status, TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 9u);
    check_mem_eq(capture.payload, "pair-data", 9u);
    check_int_eq(turbo_flow_fmq_app_send(connected, NULL, 0u), TURBO_OK);
    fmq_wait_called(&capture, 2);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
    check_size_eq(capture.payload_len, 0u);
    check_int_eq(turbo_flow_fmq_app_stop(connected), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(connected), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(bound), TURBO_OK);
    turbo_flow_fmq_app_destroy(connected);
    turbo_flow_fmq_app_destroy(bound);
  }

  it("coalesces DEALER messages and reports partial batch preparation") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t router_config;
    turbo_flow_fmq_config_t dealer_config;
    turbo_flow_fmq_app_options_t router_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t dealer_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_t *router = NULL;
    turbo_flow_fmq_app_t *dealer = NULL;
    turbo_flow_fmq_app_send_item_t items[] = {
        {"batch-one", 9u}, {"batch-two", 9u}, TURBO_FLOW_FMQ_APP_SEND_ITEM_INIT};
    turbo_flow_fmq_app_send_item_t partial_items[] = {{"accepted", 8u}, {NULL, 1u}};
    fmq_capture_state_t capture;
    size_t submitted = SIZE_MAX;
    int send_status = TURBO_ENOTCONN;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_config(&router_config, TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&dealer_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    dealer_config.identity = "batch-dealer";
    router_options.on_message = fmq_app_capture;
    router_options.message_ctx = &capture;

    check_int_eq(turbo_flow_fmq_app_create(&router_config, &router_options, &router), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_create(&dealer_config, &dealer_options, &dealer), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_send_batch(dealer, items, 3u, &submitted), TURBO_EBUSY);
    check_size_eq(submitted, 0u);
    check_int_eq(turbo_flow_fmq_app_start(router), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(dealer), TURBO_OK);
    for (int i = 0; i < 400 && send_status == TURBO_ENOTCONN; ++i) {
      send_status = turbo_flow_fmq_app_send(dealer, "ready", 5u);
      if (send_status == TURBO_ENOTCONN) turbo_sleep_ms(5);
    }
    check_int_eq(send_status, TURBO_OK);
    fmq_wait_called(&capture, 1);

    submitted = SIZE_MAX;
    check_int_eq(turbo_flow_fmq_app_send_batch(
                     dealer, items, TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_ITEMS + 1u, &submitted),
                 TURBO_ERANGE);
    check_size_eq(submitted, 0u);
    {
      const turbo_flow_fmq_app_send_item_t oversized = {
          "x", TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES + 1u};
      submitted = SIZE_MAX;
      check_int_eq(turbo_flow_fmq_app_send_batch(dealer, &oversized, 1u, &submitted),
                   TURBO_EMSGSIZE);
      check_size_eq(submitted, 0u);
    }

    submitted = SIZE_MAX;
    check_int_eq(turbo_flow_fmq_app_send_batch(dealer, items, 3u, &submitted), TURBO_OK);
    check_size_eq(submitted, 3u);
    fmq_wait_called(&capture, 4);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 4);
    check_size_eq(capture.payload_len, 0u);

    {
      turbo_flow_fmq_app_send_item_t growth_items[80];
      for (size_t i = 0u; i < 80u; ++i)
        growth_items[i] = (turbo_flow_fmq_app_send_item_t){"relocate", 8u};
      submitted = SIZE_MAX;
      check_int_eq(turbo_flow_fmq_app_send_batch(dealer, growth_items, 80u, &submitted),
                   TURBO_OK);
      check_size_eq(submitted, 80u);
      fmq_wait_called(&capture, 84);
      check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 84);
      check_size_eq(capture.payload_len, 8u);
      check_mem_eq(capture.payload, "relocate", 8u);
    }

    submitted = SIZE_MAX;
    check_int_eq(turbo_flow_fmq_app_send_batch(dealer, partial_items, 2u, &submitted),
                 TURBO_EINVAL);
    check_size_eq(submitted, 1u);
    fmq_wait_called(&capture, 85);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 85);
    check_size_eq(capture.payload_len, 8u);
    check_mem_eq(capture.payload, "accepted", 8u);

    check_int_eq(turbo_flow_fmq_app_stop(dealer), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(router), TURBO_OK);
    turbo_flow_fmq_app_destroy(dealer);
    turbo_flow_fmq_app_destroy(router);
  }

  it("coalesces fragmented DEALER frames while retaining payload backing") {
    static char first[FLOW_FMQ_PACKET_PAYLOAD_SIZE + 17u];
    static char second[FLOW_FMQ_PACKET_PAYLOAD_SIZE + 17u];
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t router_config;
    turbo_flow_fmq_config_t dealer_config;
    turbo_flow_fmq_app_options_t router_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t dealer_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_t *router = NULL;
    turbo_flow_fmq_app_t *dealer = NULL;
    turbo_flow_fmq_app_send_item_t items[2];
    fmq_fragmented_batch_capture_t capture;
    size_t submitted = 0u;
    int send_status = TURBO_ENOTCONN;

    memset(first, 'a', sizeof(first));
    memset(second, 'b', sizeof(second));
    memset(&capture, 0, sizeof(capture));
    capture.expected[0] = first;
    capture.expected[1] = second;
    capture.expected_size = sizeof(first);
    atomic_init(&capture.called, 0);
    atomic_init(&capture.failures, 0);
    items[0] = (turbo_flow_fmq_app_send_item_t){first, sizeof(first)};
    items[1] = (turbo_flow_fmq_app_send_item_t){second, sizeof(second)};
    check_int_gt(port, 0);
    fmq_config(&router_config, TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&dealer_config, TURBO_FLOW_FMQ_DEALER, TURBO_FLOW_FMQ_CONNECT, port);
    dealer_config.identity = "fragmented-batch-dealer";
    router_options.on_message = fmq_fragmented_batch_capture;
    router_options.message_ctx = &capture;

    check_int_eq(turbo_flow_fmq_app_create(&router_config, &router_options, &router), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_create(&dealer_config, &dealer_options, &dealer), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(router), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(dealer), TURBO_OK);
    for (int i = 0; i < 400 && send_status == TURBO_ENOTCONN; ++i) {
      send_status = turbo_flow_fmq_app_send(dealer, "ready", sizeof("ready") - 1u);
      if (send_status == TURBO_ENOTCONN) turbo_sleep_ms(5);
    }
    check_int_eq(send_status, TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_send_batch(dealer, items, 2u, &submitted), TURBO_OK);
    check_size_eq(submitted, 2u);
    for (int i = 0;
         i < 2000 && atomic_load_explicit(&capture.called, memory_order_acquire) < 2; ++i) {
      turbo_sleep_ms(1);
    }
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
    check_int_eq(atomic_load_explicit(&capture.failures, memory_order_acquire), 0);

    check_int_eq(turbo_flow_fmq_app_stop(dealer), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(router), TURBO_OK);
    turbo_flow_fmq_app_destroy(dealer);
    turbo_flow_fmq_app_destroy(router);
  }

  it("coalesces PUB messages while preserving subscriber order") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t pub_config;
    turbo_flow_fmq_config_t sub_config;
    turbo_flow_fmq_app_options_t pub_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t sub_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_t *publisher = NULL;
    turbo_flow_fmq_app_t *subscriber = NULL;
    const turbo_flow_fmq_app_send_item_t items[] = {
        {"pub-one", 7u}, {"pub-two", 7u}, {"pub-three", 9u}, {"pub-four", 8u}};
    fmq_capture_state_t capture;
    size_t submitted = SIZE_MAX;
    int send_status = TURBO_ENOTCONN;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_config(&pub_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&sub_config, TURBO_FLOW_FMQ_SUB, TURBO_FLOW_FMQ_CONNECT, port);
    sub_options.on_message = fmq_app_capture;
    sub_options.message_ctx = &capture;
    check_int_eq(turbo_flow_fmq_app_create(&pub_config, &pub_options, &publisher), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_create(&sub_config, &sub_options, &subscriber), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(subscriber), TURBO_OK);
    for (int i = 0; i < 400 && send_status == TURBO_ENOTCONN; ++i) {
      send_status = turbo_flow_fmq_app_send(publisher, "ready", 5u);
      if (send_status == TURBO_ENOTCONN) turbo_sleep_ms(5);
    }
    check_int_eq(send_status, TURBO_OK);
    fmq_wait_called(&capture, 1);

    check_int_eq(turbo_flow_fmq_app_send_batch(publisher, items, 4u, &submitted), TURBO_OK);
    check_size_eq(submitted, 4u);
    fmq_wait_called(&capture, 5);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 5);
    check_size_eq(capture.payload_len, 8u);
    check_mem_eq(capture.payload, "pub-four", 8u);

    check_int_eq(turbo_flow_fmq_app_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(publisher), TURBO_OK);
    turbo_flow_fmq_app_destroy(subscriber);
    turbo_flow_fmq_app_destroy(publisher);
  }

  it("coalesces PUSH messages without changing round-robin fairness") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t push_config;
    turbo_flow_fmq_config_t pull_a_config;
    turbo_flow_fmq_config_t pull_b_config;
    turbo_flow_fmq_app_options_t push_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t pull_a_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t pull_b_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_t *push = NULL;
    turbo_flow_fmq_app_t *pull_a = NULL;
    turbo_flow_fmq_app_t *pull_b = NULL;
    const turbo_flow_fmq_app_send_item_t items[] = {
        {"job-one", 7u}, {"job-two", 7u}, {"job-three", 9u}, {"job-four", 8u}};
    fmq_capture_state_t capture_a;
    fmq_capture_state_t capture_b;
    size_t submitted = SIZE_MAX;
    int base_a;
    int base_b;

    check_int_gt(port, 0);
    fmq_init_capture(&capture_a);
    fmq_init_capture(&capture_b);
    fmq_config(&push_config, TURBO_FLOW_FMQ_PUSH, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&pull_a_config, TURBO_FLOW_FMQ_PULL, TURBO_FLOW_FMQ_CONNECT, port);
    fmq_config(&pull_b_config, TURBO_FLOW_FMQ_PULL, TURBO_FLOW_FMQ_CONNECT, port);
    pull_a_options.on_message = fmq_app_capture;
    pull_a_options.message_ctx = &capture_a;
    pull_b_options.on_message = fmq_app_capture;
    pull_b_options.message_ctx = &capture_b;
    check_int_eq(turbo_flow_fmq_app_create(&push_config, &push_options, &push), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_create(&pull_a_config, &pull_a_options, &pull_a), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_create(&pull_b_config, &pull_b_options, &pull_b), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(push), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(pull_a), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(pull_b), TURBO_OK);
    for (int i = 0;
         i < 400 && (atomic_load_explicit(&capture_a.called, memory_order_acquire) == 0 ||
                     atomic_load_explicit(&capture_b.called, memory_order_acquire) == 0);
         ++i) {
      (void)turbo_flow_fmq_app_send(push, "ready", 5u);
      turbo_sleep_ms(5);
    }
    base_a = atomic_load_explicit(&capture_a.called, memory_order_acquire);
    base_b = atomic_load_explicit(&capture_b.called, memory_order_acquire);
    check_int_gt(base_a, 0);
    check_int_gt(base_b, 0);

    check_int_eq(turbo_flow_fmq_app_send_batch(push, items, 4u, &submitted), TURBO_OK);
    check_size_eq(submitted, 4u);
    fmq_wait_called(&capture_a, base_a + 2);
    fmq_wait_called(&capture_b, base_b + 2);
    check_int_eq(atomic_load_explicit(&capture_a.called, memory_order_acquire), base_a + 2);
    check_int_eq(atomic_load_explicit(&capture_b.called, memory_order_acquire), base_b + 2);

    check_int_eq(turbo_flow_fmq_app_stop(pull_b), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(pull_a), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(push), TURBO_OK);
    turbo_flow_fmq_app_destroy(pull_b);
    turbo_flow_fmq_app_destroy(pull_a);
    turbo_flow_fmq_app_destroy(push);
  }

  it("keeps dispatching to a fast PULL while a slow PULL owns one write credit") {
    enum { FIRST_BATCH_ITEMS = 512, SECOND_BATCH_ITEMS = 8 };
    const size_t payload_size = 64u * 1024u;
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t push_config;
    turbo_flow_fmq_config_t fast_config;
    turbo_flow_fmq_app_options_t push_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_send_item_t first_items[FIRST_BATCH_ITEMS];
    turbo_flow_fmq_app_send_item_t second_items[SECOND_BATCH_ITEMS];
    turbo_flow_fmq_app_t *push = NULL;
    turbo_flow_t *fast_pull = NULL;
    fmq_slow_peer_t slow;
    fmq_count_capture_t fast;
    fmq_event_state_t push_events;
    fmq_app_batch_thread_ctx_t first_send;
    turbo_thread_t slow_thread;
    turbo_thread_t first_send_thread;
    char *payload = NULL;
    size_t second_submitted = SIZE_MAX;

    check_int_gt(port, 0);
    payload = (char *)malloc(payload_size);
    check_not_null(payload);
    memset(payload, 'p', payload_size);
    for (size_t i = 0u; i < FIRST_BATCH_ITEMS; ++i) {
      first_items[i].data = payload;
      first_items[i].data_size = payload_size;
    }
    for (size_t i = 0u; i < SECOND_BATCH_ITEMS; ++i) {
      second_items[i].data = "fast-work";
      second_items[i].data_size = sizeof("fast-work") - 1u;
    }
    fmq_init_slow_pull(&slow, port);
    fmq_init_count_capture(&fast);
    fmq_init_events(&push_events);
    memset(&first_send, 0, sizeof(first_send));
    atomic_init(&first_send.entered, 0);
    atomic_init(&first_send.done, 0);
    atomic_init(&first_send.rc, TURBO_EALREADY);

    fmq_config(&push_config, TURBO_FLOW_FMQ_PUSH, TURBO_FLOW_FMQ_BIND, port);
    push_config.send_timeout_ms = 30000u;
    push_config.send_hwm_bytes = fmq_test_encoded_payload_size(payload_size);
    push_config.event_callback = fmq_event_capture;
    push_config.event_ctx = &push_events;
    fmq_config(&fast_config, TURBO_FLOW_FMQ_PULL, TURBO_FLOW_FMQ_CONNECT, port);
    fast_config.recv_timeout_ms = 30000u;
    check_int_eq(turbo_flow_fmq_app_create(&push_config, &push_options, &push), TURBO_OK);
    fast_pull = fmq_make_count_source_flow("fmq.input", &fast_config, &fast);
    check_not_null(push);
    check_not_null(fast_pull);
    check_int_eq(turbo_flow_fmq_app_start(push), TURBO_OK);
    check_int_eq(turbo_thread_create(&slow_thread, fmq_slow_peer_thread, &slow), TURBO_OK);
    check_int_eq(fmq_wait_slow_peer(&slow), TURBO_OK);
    fmq_wait_event_count(&push_events.peer_connected, 1);
    check_int_eq(atomic_load_explicit(&push_events.peer_connected, memory_order_acquire), 1);
    check_int_eq(turbo_flow_start(fast_pull), TURBO_OK);
    fmq_wait_event_count(&push_events.peer_connected, 2);
    check_int_eq(atomic_load_explicit(&push_events.peer_connected, memory_order_acquire), 2);

    first_send.app = push;
    first_send.items = first_items;
    first_send.item_count = FIRST_BATCH_ITEMS;
    check_int_eq(turbo_thread_create(&first_send_thread, fmq_app_send_batch_in_thread,
                                     &first_send),
                 TURBO_OK);
    fmq_wait_count_called(&fast, FIRST_BATCH_ITEMS / 2);
    check_int_eq(atomic_load_explicit(&first_send.entered, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire),
                 FIRST_BATCH_ITEMS / 2);
    check_int_eq(atomic_load_explicit(&first_send.done, memory_order_acquire), 0);

    check_int_eq(turbo_flow_fmq_app_send_batch(push, second_items, SECOND_BATCH_ITEMS,
                                               &second_submitted),
                 TURBO_OK);
    check_size_eq(second_submitted, SECOND_BATCH_ITEMS);
    fmq_wait_count_called(&fast, FIRST_BATCH_ITEMS / 2 + SECOND_BATCH_ITEMS);
    check_int_eq(atomic_load_explicit(&fast.called, memory_order_acquire),
                 FIRST_BATCH_ITEMS / 2 + SECOND_BATCH_ITEMS);
    check_int_eq(atomic_load_explicit(&first_send.done, memory_order_acquire), 0);

    atomic_store_explicit(&slow.release_reads, 1, memory_order_release);
    check_int_eq(turbo_thread_join(&first_send_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&first_send.rc, memory_order_acquire), TURBO_OK);
    check_size_eq(first_send.submitted, FIRST_BATCH_ITEMS);
    check_int_eq(turbo_flow_stop(fast_pull), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(push), TURBO_OK);
    check_int_eq(turbo_thread_join(&slow_thread), TURBO_OK);
    check_int_eq(atomic_load_explicit(&slow.status, memory_order_acquire), TURBO_OK);
    turbo_flow_destroy(fast_pull);
    turbo_flow_fmq_app_destroy(push);
    free(payload);
  }

  it("keeps batch PUSH routes stable when a middle PULL disconnects") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t push_config;
    turbo_flow_fmq_config_t pull_a_config;
    turbo_flow_fmq_config_t pull_b_config;
    turbo_flow_fmq_config_t pull_c_config;
    turbo_flow_fmq_app_options_t push_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t pull_a_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t pull_b_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t pull_c_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_t *push = NULL;
    turbo_flow_fmq_app_t *pull_a = NULL;
    turbo_flow_fmq_app_t *pull_b = NULL;
    turbo_flow_fmq_app_t *pull_c = NULL;
    fmq_capture_state_t capture_a;
    fmq_capture_state_t capture_b;
    fmq_capture_state_t capture_c;
    fmq_event_state_t push_events;
    fmq_batch_route_gate_t gate;
    turbo_flow_security_rule_t rules[2];
    turbo_flow_security_realm_config_t realm_config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
    turbo_flow_security_realm_t *realm = NULL;
    turbo_flow_security_auth_provider_t auth_provider = TURBO_FLOW_SECURITY_AUTH_PROVIDER_INIT;
    turbo_flow_security_key_provider_t key_provider = TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT;
    turbo_flow_fmq_security_binding_t server_security =
        TURBO_FLOW_FMQ_SECURITY_BINDING_INIT;
    turbo_flow_fmq_security_binding_t client_security =
        TURBO_FLOW_FMQ_SECURITY_BINDING_INIT;
    turbo_flow_fmq_app_send_item_t items[] = {
        {"for-a", 5u}, {"for-b", 5u}, {"for-c", 5u}, {"for-a", 5u}, {"for-b", 5u},
        {"for-c", 5u}, {"for-a", 5u}, {"for-b", 5u}, {"for-c", 5u}, {"gate-a", 6u},
        {"after-c", 7u}};
    size_t submitted = SIZE_MAX;

    check_int_gt(port, 0);
    fmq_init_capture(&capture_a);
    fmq_init_capture(&capture_b);
    fmq_init_capture(&capture_c);
    fmq_init_events(&push_events);
    memset(&gate, 0, sizeof(gate));
    gate.events = &push_events;
    gate.trigger_evaluation = 10u;
    atomic_init(&gate.evaluations, 0u);
    atomic_init(&gate.entered, 0);
    atomic_init(&gate.stop_status, TURBO_EALREADY);
    atomic_init(&gate.disconnect_status, TURBO_EALREADY);

    memset(rules, 0, sizeof(rules));
    for (size_t i = 0u; i < 2u; ++i) {
      rules[i] = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
      rules[i].effect = TURBO_FLOW_SECURITY_ALLOW;
      rules[i].subject_kind = TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL;
      (void)snprintf(rules[i].subject, sizeof(rules[i].subject), "client-a");
      (void)snprintf(rules[i].domain_id, sizeof(rules[i].domain_id), "root-a");
      rules[i].resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
    }
    rules[0].action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    rules[0].match_kind = TURBO_FLOW_SECURITY_MATCH_EXACT;
    (void)snprintf(rules[0].pattern, sizeof(rules[0].pattern),
                   "fmq:fmq.app.endpoint:connection");
    rules[1].action_mask = TURBO_FLOW_SECURITY_ACTION_READ;
    rules[1].match_kind = TURBO_FLOW_SECURITY_MATCH_ADAPTER;
    (void)snprintf(rules[1].pattern, sizeof(rules[1].pattern), "*");
    realm_config.resource_uid = "security/fmq-batch-route-test";
    realm_config.owner_name = "test";
    realm_config.policy_version = 1u;
    realm_config.rules = rules;
    realm_config.rule_count = 2u;
    realm_config.matcher.ctx = &gate;
    realm_config.matcher.compile_leaf = fmq_batch_route_gate_compile;
    realm_config.matcher.evaluate_leaf = fmq_batch_route_gate_evaluate;
    realm_config.matcher.destroy_leaf = fmq_batch_route_gate_destroy;
    check_int_eq(turbo_flow_security_realm_create(&realm_config, &realm), TURBO_OK);
    check_not_null(realm);

    auth_provider.ctx = (void *)"correct-secret";
    auth_provider.authenticate = fmq_test_authenticate;
    key_provider.ctx = (void *)"correct-secret";
    key_provider.acquire = fmq_test_secret_acquire;
    key_provider.release = fmq_test_secret_release;
    server_security.realm_channel = "security.fmq";
    server_security.auth_method = "token";
    server_security.auth_provider = &auth_provider;
    server_security.realm = realm;
    client_security.auth_method = "token";
    client_security.key_provider = &key_provider;
    client_security.secret_reference = "secret/fmq-client";

    fmq_config(&push_config, TURBO_FLOW_FMQ_PUSH, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&pull_a_config, TURBO_FLOW_FMQ_PULL, TURBO_FLOW_FMQ_CONNECT, port);
    fmq_config(&pull_b_config, TURBO_FLOW_FMQ_PULL, TURBO_FLOW_FMQ_CONNECT, port);
    fmq_config(&pull_c_config, TURBO_FLOW_FMQ_PULL, TURBO_FLOW_FMQ_CONNECT, port);
    push_config.send_timeout_ms = 2000u;
    push_config.event_callback = fmq_event_capture;
    push_config.event_ctx = &push_events;
    pull_a_config.identity = "client-a";
    pull_a_config.reconnect_initial_ms = TURBO_FLOW_FMQ_RECONNECT_DISABLED;
    pull_b_config.identity = "client-a";
    pull_b_config.reconnect_initial_ms = TURBO_FLOW_FMQ_RECONNECT_DISABLED;
    pull_c_config.identity = "client-a";
    pull_c_config.reconnect_initial_ms = TURBO_FLOW_FMQ_RECONNECT_DISABLED;
    pull_a_options.on_message = fmq_app_capture;
    pull_a_options.message_ctx = &capture_a;
    pull_b_options.on_message = fmq_app_capture;
    pull_b_options.message_ctx = &capture_b;
    pull_c_options.on_message = fmq_app_capture;
    pull_c_options.message_ctx = &capture_c;
    check_int_eq(
        turbo_flow_fmq_app_create_secure(&push_config, &push_options, &server_security, &push),
        TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_create_secure(&pull_a_config, &pull_a_options,
                                                  &client_security, &pull_a),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_create_secure(&pull_b_config, &pull_b_options,
                                                  &client_security, &pull_b),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_create_secure(&pull_c_config, &pull_c_options,
                                                  &client_security, &pull_c),
                 TURBO_OK);
    gate.disconnect_app = pull_b;
    check_int_eq(turbo_flow_fmq_app_start(push), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(pull_a), TURBO_OK);
    fmq_wait_event_count(&push_events.peer_connected, 1);
    check_int_eq(atomic_load_explicit(&push_events.peer_connected, memory_order_acquire), 1);
    check_int_eq(turbo_flow_fmq_app_start(pull_b), TURBO_OK);
    fmq_wait_event_count(&push_events.peer_connected, 2);
    check_int_eq(atomic_load_explicit(&push_events.peer_connected, memory_order_acquire), 2);
    check_int_eq(turbo_flow_fmq_app_start(pull_c), TURBO_OK);
    fmq_wait_event_count(&push_events.peer_connected, 3);
    check_int_eq(atomic_load_explicit(&push_events.peer_connected, memory_order_acquire), 3);

    check_int_eq(turbo_flow_fmq_app_send_batch(push, items, sizeof(items) / sizeof(items[0]),
                                               &submitted),
                 TURBO_ENOTCONN);
    check_size_eq(submitted, sizeof(items) / sizeof(items[0]));
    check_int_eq(atomic_load_explicit(&gate.entered, memory_order_acquire), 1);
    check_size_eq(atomic_load_explicit(&gate.evaluations, memory_order_acquire),
                  sizeof(items) / sizeof(items[0]));
    check_int_eq(atomic_load_explicit(&gate.stop_status, memory_order_acquire), TURBO_OK);
    check_int_eq(atomic_load_explicit(&gate.disconnect_status, memory_order_acquire), TURBO_OK);
    check_int_eq(atomic_load_explicit(&push_events.peer_disconnected, memory_order_acquire), 1);

    fmq_wait_called(&capture_a, 4);
    fmq_wait_called(&capture_c, 4);
    check_int_eq(atomic_load_explicit(&capture_a.called, memory_order_acquire), 4);
    check_size_eq(capture_a.payload_len, 6u);
    check_mem_eq(capture_a.payload, "gate-a", 6u);
    check_int_eq(atomic_load_explicit(&capture_b.called, memory_order_acquire), 0);
    check_int_eq(atomic_load_explicit(&capture_c.called, memory_order_acquire), 4);
    check_size_eq(capture_c.payload_len, 7u);
    check_mem_eq(capture_c.payload, "after-c", 7u);

    check_int_eq(turbo_flow_fmq_app_stop(pull_c), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(pull_a), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(push), TURBO_OK);
    turbo_flow_fmq_app_destroy(pull_c);
    turbo_flow_fmq_app_destroy(pull_b);
    turbo_flow_fmq_app_destroy(pull_a);
    turbo_flow_fmq_app_destroy(push);
    turbo_flow_security_realm_destroy(realm);
  }

  it("keeps REP replies in the request dispatch") {
    unsigned short port = fmq_test_port();
    turbo_flow_fmq_config_t rep_config;
    turbo_flow_fmq_config_t req_config;
    turbo_flow_fmq_app_options_t rep_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_options_t req_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_t *rep = NULL;
    turbo_flow_fmq_app_t *req = NULL;
    fmq_capture_state_t capture;
    int send_status = TURBO_ENOTCONN;

    check_int_gt(port, 0);
    fmq_init_capture(&capture);
    fmq_config(&rep_config, TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND, port);
    fmq_config(&req_config, TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT, port);
    rep_options.on_message = fmq_app_reply;
    rep_options.message_ctx = (void *)"facade-reply";
    req_options.on_message = fmq_app_capture;
    req_options.message_ctx = &capture;

    check_int_eq(turbo_flow_fmq_app_create(&rep_config, &rep_options, &rep), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_create(&req_config, &req_options, &req), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(rep), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(req), TURBO_OK);
    {
      const turbo_flow_fmq_app_send_item_t item = {"unsupported", 11u};
      size_t submitted = SIZE_MAX;
      check_int_eq(turbo_flow_fmq_app_send_batch(req, &item, 1u, &submitted), TURBO_ENOTSUP);
      check_size_eq(submitted, 0u);
    }
    for (int i = 0; i < 400 && send_status == TURBO_ENOTCONN; ++i) {
      send_status = turbo_flow_fmq_app_send(req, "facade-request", 14u);
      if (send_status == TURBO_ENOTCONN) turbo_sleep_ms(5);
    }
    check_int_eq(send_status, TURBO_OK);
    fmq_wait_called(&capture, 1);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 12u);
    check_mem_eq(capture.payload, "facade-reply", 12u);
    check_true(capture.correlation_id != 0u);
    check_int_eq(turbo_flow_fmq_app_stop(req), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(rep), TURBO_OK);
    turbo_flow_fmq_app_destroy(req);
    turbo_flow_fmq_app_destroy(rep);
  }

  it("creates the same facade from an immutable YAML snapshot") {
    char yaml[1024];
    unsigned short port = fmq_test_port();
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_fmq_app_options_t options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_fmq_app_t *app = NULL;
    int yaml_size;

    check_int_gt(port, 0);
    yaml_size = snprintf(yaml, sizeof(yaml),
                         "version: 1\n"
                         "adapters:\n"
                         "  fmq-app:\n"
                         "    kind: fmq\n"
                         "    config:\n"
                         "      pattern: pub\n"
                         "      mode: bind\n"
                         "      transport: tcp\n"
                         "      host: 127.0.0.1\n"
                         "      port: %u\n",
                         (unsigned int)port);
    check_int_gt(yaml_size, 0);
    check_true((size_t)yaml_size < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)yaml_size, &resolved, &error),
                 TURBO_OK);
    check_not_null(resolved);
    check_int_eq(turbo_flow_fmq_app_create_resolved(resolved, "fmq-app", &options, &app, &error),
                 TURBO_OK);
    check_not_null(app);
    turbo_flow_fmq_app_destroy(app);
    turbo_flow_resolved_config_destroy(resolved);
  }
}

spec("flow_fmq_pubsub_recovery_network") {
  it("shares one state owner across XPUB XSUB live fan-out and REQ REP recovery") {
    static const char proxy_dsl[] =
        "source subscriptions adapter fmq.front\n"
        "source publications adapter fmq.back\n"
        "source recovery_request adapter fmq.recovery\n"
        "stage publication_capture\n"
        "stage state_put\n"
        "stage upstream adapter fmq.back\n"
        "stage downstream adapter fmq.front\n"
        "stage recover\n"
        "stage recovery_reply adapter fmq.recovery\n"
        "stage main {\n"
        "  subscriptions -> upstream\n"
        "  publications -> publication_capture -> state_put -> downstream\n"
        "  recovery_request -> recover -> recovery_reply\n"
        "}\n";
    static const char upstream_dsl[] = "source subscriptions adapter fmq.xpub\n"
                                       "source input\n"
                                       "stage send adapter fmq.xpub\n"
                                       "stage capture\n"
                                       "stage main {\n"
                                       "  subscriptions -> capture\n"
                                       "  input -> send\n"
                                       "}\n";
    static const char recovery_client_dsl[] = "source response adapter fmq.req\n"
                                              "source input\n"
                                              "stage send adapter fmq.req\n"
                                              "stage capture\n"
                                              "stage main {\n"
                                              "  input -> send\n"
                                              "  response -> capture\n"
                                              "}\n";
    unsigned short front_port = fmq_test_port();
    unsigned short back_port = fmq_test_port();
    unsigned short recovery_port = fmq_test_port();
    turbo_flow_fmq_config_t front_config;
    turbo_flow_fmq_config_t back_config;
    turbo_flow_fmq_config_t recovery_rep_config;
    turbo_flow_fmq_config_t upstream_config;
    turbo_flow_fmq_config_t subscriber_config;
    turbo_flow_fmq_config_t recovery_req_config;
    turbo_flow_fmq_pubsub_config_t state_config = TURBO_FLOW_FMQ_PUBSUB_CONFIG_INIT;
    turbo_flow_fmq_pubsub_recovery_config_t recovery_config =
        TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CONFIG_INIT;
    turbo_flow_fmq_pubsub_state_t *state = NULL;
    turbo_flow_fmq_pubsub_recovery_service_t *recovery_service = NULL;
    turbo_flow_fmq_pubsub_recovery_message_t live = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    turbo_flow_fmq_pubsub_recovery_message_t snapshot = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    turbo_flow_fmq_pubsub_recovery_record_iterator_t iterator =
        TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_ITERATOR_INIT;
    turbo_flow_fmq_pubsub_record_t record = TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
    turbo_flow_fmq_pubsub_status_t state_status = TURBO_FLOW_FMQ_PUBSUB_STATUS_INIT;
    fmq_capture_state_t upstream_events;
    fmq_capture_state_t publication_capture;
    fmq_capture_state_t subscriber_capture;
    fmq_pubsub_stage_capture_t state_put_capture;
    fmq_tfmp_capture_t recovery_capture;
    turbo_flow_t *proxy = turbo_flow_create();
    turbo_flow_t *upstream = turbo_flow_create();
    turbo_flow_t *subscriber = NULL;
    turbo_flow_t *recovery_client = turbo_flow_create();
    uint8_t request[128];
    size_t request_size = 0u;

    check_int_gt(front_port, 0);
    check_int_gt(back_port, 0);
    check_int_gt(recovery_port, 0);
    check_int_ne(front_port, back_port);
    check_int_ne(front_port, recovery_port);
    check_int_ne(back_port, recovery_port);
    check_not_null(proxy);
    check_not_null(upstream);
    check_not_null(recovery_client);
    fmq_init_capture(&upstream_events);
    fmq_init_capture(&publication_capture);
    fmq_init_capture(&subscriber_capture);
    memset(&state_put_capture, 0, sizeof(state_put_capture));
    memset(&recovery_capture, 0, sizeof(recovery_capture));
    atomic_init(&recovery_capture.called, 0);

    state_config.max_topics = 16u;
    state_config.max_state_bytes = 4096u;
    state_config.update_capacity = 32u;
    state_config.max_update_bytes = 8192u;
    recovery_config.max_reply_bytes = sizeof(recovery_capture.payload);
    recovery_config.max_update_records = 16u;
    state = turbo_flow_fmq_pubsub_state_create(&state_config);
    check_not_null(state);
    check_int_eq(
        turbo_flow_fmq_pubsub_recovery_service_create(state, &recovery_config, &recovery_service),
        TURBO_OK);
    state_put_capture.state = state;
    atomic_init(&state_put_capture.called, 0);
    atomic_init(&state_put_capture.status, TURBO_EALREADY);

    fmq_config(&front_config, TURBO_FLOW_FMQ_XPUB, TURBO_FLOW_FMQ_BIND, front_port);
    fmq_config(&back_config, TURBO_FLOW_FMQ_XSUB, TURBO_FLOW_FMQ_CONNECT, back_port);
    fmq_config(&recovery_rep_config, TURBO_FLOW_FMQ_REP, TURBO_FLOW_FMQ_BIND, recovery_port);
    fmq_config(&upstream_config, TURBO_FLOW_FMQ_XPUB, TURBO_FLOW_FMQ_BIND, back_port);
    fmq_config(&subscriber_config, TURBO_FLOW_FMQ_XSUB, TURBO_FLOW_FMQ_CONNECT, front_port);
    fmq_config(&recovery_req_config, TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_CONNECT, recovery_port);
    front_config.topic_policy = TURBO_FLOW_FMQ_METADATA_INHERIT;
    upstream_config.topic = "orders.created";
    subscriber_config.topic = "orders.";

    check_int_eq(fmq_register_private_adapter(proxy, "fmq.front", &front_config), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(proxy, "fmq.back", &back_config), TURBO_OK);
    check_int_eq(fmq_register_private_adapter(proxy, "fmq.recovery", &recovery_rep_config),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(proxy, "publication_capture", fmq_capture,
                                              &publication_capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(proxy, "state_put", fmq_pubsub_put_capture,
                                              &state_put_capture, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(proxy, "recover",
                                              turbo_flow_fmq_pubsub_recovery_stage,
                                              recovery_service, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(proxy, proxy_dsl, strlen(proxy_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(proxy), TURBO_OK);

    check_int_eq(fmq_register_private_adapter(upstream, "fmq.xpub", &upstream_config), TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(upstream, "capture", fmq_capture, &upstream_events, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(upstream, upstream_dsl, strlen(upstream_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(upstream), TURBO_OK);

    subscriber = fmq_make_source_flow("fmq.input", &subscriber_config, &subscriber_capture);
    check_not_null(subscriber);
    check_int_eq(fmq_register_private_adapter(recovery_client, "fmq.req", &recovery_req_config),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(recovery_client, "capture", fmq_tfmp_capture,
                                              &recovery_capture, NULL),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_parse_string(recovery_client, recovery_client_dsl, strlen(recovery_client_dsl)),
        TURBO_OK);
    check_int_eq(turbo_flow_compile(recovery_client), TURBO_OK);

    check_int_eq(turbo_flow_start(upstream), TURBO_OK);
    check_int_eq(turbo_flow_start(proxy), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_start(recovery_client), TURBO_OK);
    fmq_wait_subscription_events(&upstream_events, 1);
    check_int_eq(atomic_load_explicit(&upstream_events.subscription_events, memory_order_acquire),
                 1);
    check_true(upstream_events.subscribe);
    check_mem_eq(upstream_events.topic, "orders.", 7u);

    check_int_eq(fmq_publish_payload(upstream, "created"), TURBO_OK);
    fmq_wait_called(&publication_capture, 1);
    check_int_eq(atomic_load_explicit(&publication_capture.called, memory_order_acquire), 1);
    check_size_eq(publication_capture.topic_len, 14u);
    check_mem_eq(publication_capture.topic, "orders.created", 14u);
    fmq_wait_event_count(&state_put_capture.called, 1);
    check_int_eq(atomic_load_explicit(&state_put_capture.called, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&state_put_capture.status, memory_order_acquire), TURBO_OK);
    for (int i = 0; i < 400; ++i) {
      check_int_eq(turbo_flow_fmq_pubsub_status(state, &state_status), TURBO_OK);
      if (state_status.latest_sequence == 1u) break;
      turbo_sleep_ms(5);
    }
    check_uint_eq(state_status.latest_sequence, 1u);
    check_size_eq(state_status.topics, 1u);
    fmq_wait_called(&subscriber_capture, 1);
    check_int_eq(atomic_load_explicit(&subscriber_capture.called, memory_order_acquire), 1);
    check_int_eq(
        turbo_flow_fmq_pubsub_recovery_message_decode((const uint8_t *)subscriber_capture.payload,
                                                      subscriber_capture.payload_len, &live),
        TURBO_OK);
    check_int_eq(live.kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_LIVE_UPDATE);
    check_uint_eq(live.sequence, 1u);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_iterator_init(&live, &iterator), TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_next(&iterator, &record), TURBO_OK);
    check_int_eq(record.operation, TURBO_FLOW_FMQ_PUBSUB_PUT);
    check_uint_eq(record.sequence, 1u);
    check_mem_eq(record.topic.data, "orders.created", 14u);
    check_mem_eq(record.payload.data, "created", 7u);

    check_int_eq(turbo_flow_fmq_pubsub_recovery_request_encode(
                     TURBO_FLOW_FMQ_PUBSUB_RECOVERY_SNAPSHOT, 71u, tstr_v_from_buf("orders.", 7u),
                     0u, 0u, 0u, request, sizeof(request), &request_size),
                 TURBO_OK);
    check_int_eq(fmq_publish_bytes(recovery_client, (const char *)request, request_size), TURBO_OK);
    fmq_wait_event_count(&recovery_capture.called, 1);
    check_int_eq(atomic_load_explicit(&recovery_capture.called, memory_order_acquire), 1);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode(
                     recovery_capture.payload, recovery_capture.payload_size, &snapshot),
                 TURBO_OK);
    check_int_eq(snapshot.kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_SNAPSHOT);
    check_int_eq(snapshot.status, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK);
    check_uint_eq(snapshot.request_id, 71u);
    check_uint_eq(snapshot.sequence, live.sequence);
    check_uint_eq(snapshot.record_count, 1u);
    iterator = (turbo_flow_fmq_pubsub_recovery_record_iterator_t)
        TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_ITERATOR_INIT;
    record = (turbo_flow_fmq_pubsub_record_t)TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_iterator_init(&snapshot, &iterator),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_next(&iterator, &record), TURBO_OK);
    check_int_eq(record.operation, TURBO_FLOW_FMQ_PUBSUB_PUT);
    check_uint_eq(record.sequence, snapshot.sequence);
    check_mem_eq(record.topic.data, "orders.created", 14u);
    check_mem_eq(record.payload.data, "created", 7u);

    check_int_eq(turbo_flow_stop(recovery_client), TURBO_OK);
    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_stop(proxy), TURBO_OK);
    check_int_eq(turbo_flow_stop(upstream), TURBO_OK);
    turbo_flow_destroy(recovery_client);
    turbo_flow_destroy(subscriber);
    turbo_flow_destroy(proxy);
    turbo_flow_destroy(upstream);
    turbo_flow_fmq_pubsub_recovery_service_destroy(recovery_service);
    turbo_flow_fmq_pubsub_state_destroy(state);
  }
}
