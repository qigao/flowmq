#include "turbo_flow_fmq.h"

#include "flowmq_connect_endpoint.h"
#include "flowmq_coronet_transport.h"
#include "flow_fmq_internal.h"
#include "flow_fmq_profile_internal.h"
#include "flowmq_pattern.h"
#include "flowmq_peer_session.h"
#include "flowmq_stream_decoder.h"
#include "flowmq_subscription_set.h"
#include "flowmq_security.h"
#include "fmq_delivery.h"
#include "fmq_protocol.h"

#include "CoroNet/turbo_coro_socket.h"
#include "flow_coronet_actor.h"
#include "flow_coronet_execution.h"
#include "flow_coronet_runtime.h"
#include "flow_io_policy.h"
#include "flow_timer.h"
#include "fmt.h"
#include "turbo_buffer.h"
#include "turbo_deque.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef TURBO_FLOW_FMQ_INTERNAL_PROFILING
  #ifdef _WIN32
    #include <windows.h>
  #else
    #include <time.h>
  #endif
#endif

#define FLOW_FMQ_DEFAULT_TIMEOUT_MS 1000u
#define FLOW_FMQ_RECONNECT_INITIAL_MS 1000u
#define FLOW_FMQ_RECONNECT_MAX_MS 30000u
#define FLOW_FMQ_MESSAGE_CONTEXT_MAGIC UINT32_C(0x464d5143)
#define FLOW_FMQ_STACK_IOV_CAPACITY 8u
#ifndef FLOW_FMQ_ZERO_COPY_MIN_PAYLOAD_SIZE
  #define FLOW_FMQ_ZERO_COPY_MIN_PAYLOAD_SIZE 1024u
#endif
#define FLOW_FMQ_BATCH_ENCODED_RESERVE_ITEMS 64u
#define FLOW_FMQ_BATCH_ENCODED_PREALLOC_MAX_FRAME_SIZE 4096u
#define FLOW_FMQ_PROFILE_SLOW_SAMPLE_NS UINT64_C(1000000)
#define FLOW_FMQ_PUSH_SENDV_MAX_IOV 64u
#define FLOW_FMQ_PUSH_SENDV_MAX_BYTES (1024u * 1024u)

#define FLOW_FMQ_TRANSPORT_VALUE_ASSERT(name, value)                                               \
  _Static_assert(TURBO_FLOW_FMQ_##name == value, "TurboFlow FMQ transport mapping changed");       \
  _Static_assert(FLOWMQ_TRANSPORT_##name == value, "FlowMQ client transport mapping changed")
FLOW_FMQ_TRANSPORT_VALUE_ASSERT(TCP, 1);
FLOW_FMQ_TRANSPORT_VALUE_ASSERT(TLS, 2);
FLOW_FMQ_TRANSPORT_VALUE_ASSERT(UDP, 3);
FLOW_FMQ_TRANSPORT_VALUE_ASSERT(KCP, 4);
FLOW_FMQ_TRANSPORT_VALUE_ASSERT(PIPE, 5);
FLOW_FMQ_TRANSPORT_VALUE_ASSERT(WS, 6);
FLOW_FMQ_TRANSPORT_VALUE_ASSERT(WSS, 7);
#undef FLOW_FMQ_TRANSPORT_VALUE_ASSERT

static atomic_uint_fast64_t flow_fmq_next_adapter_instance_id = 0u;

#ifdef TURBO_FLOW_FMQ_INTERNAL_PROFILING
typedef struct flow_fmq_send_profile_aggregate_s {
  atomic_int enabled;
  atomic_uint_fast64_t samples;
  atomic_uint_fast64_t post_samples;
  atomic_uint_fast64_t socket_samples;
  atomic_uint_fast64_t socket_cpu_samples;
  atomic_uint_fast64_t enqueue_sum_ns;
  atomic_uint_fast64_t owner_wait_sum_ns;
  atomic_uint_fast64_t post_call_sum_ns;
  atomic_uint_fast64_t owner_dispatch_sum_ns;
  atomic_uint_fast64_t socket_send_sum_ns;
  atomic_uint_fast64_t socket_send_max_ns;
  atomic_uint_fast64_t socket_send_slow_samples;
  atomic_uint_fast64_t socket_cpu_wall_sum_ns;
  atomic_uint_fast64_t socket_thread_cpu_sum_ns;
  atomic_uint_fast64_t completion_sum_ns;
  atomic_uint_fast64_t waiter_wake_sum_ns;
  atomic_uint_fast64_t total_sum_ns;
  atomic_uint_fast64_t total_max_ns;
  atomic_uint_fast64_t total_slow_samples;
  atomic_uint_fast64_t batch_socket_calls;
  atomic_uint_fast64_t batch_socket_cpu_samples;
  atomic_uint_fast64_t batch_socket_frames;
  atomic_uint_fast64_t batch_socket_iov_segments;
  atomic_uint_fast64_t batch_socket_send_sum_ns;
  atomic_uint_fast64_t batch_socket_send_max_ns;
  atomic_uint_fast64_t batch_socket_send_slow_samples;
  atomic_uint_fast64_t batch_socket_cpu_wall_sum_ns;
  atomic_uint_fast64_t batch_socket_thread_cpu_sum_ns;
  atomic_uint_fast64_t batch_samples;
  atomic_uint_fast64_t batch_post_samples;
  atomic_uint_fast64_t batch_build_sum_ns;
  atomic_uint_fast64_t batch_build_max_ns;
  atomic_uint_fast64_t batch_build_slow_samples;
  atomic_uint_fast64_t batch_payload_prepare_sum_ns;
  atomic_uint_fast64_t batch_payload_prepare_max_ns;
  atomic_uint_fast64_t batch_payload_prepare_slow_samples;
  atomic_uint_fast64_t batch_submit_sum_ns;
  atomic_uint_fast64_t batch_adapter_consume_sum_ns;
  atomic_uint_fast64_t batch_enqueue_prepare_sum_ns;
  atomic_uint_fast64_t batch_owner_wait_sum_ns;
  atomic_uint_fast64_t batch_owner_wait_max_ns;
  atomic_uint_fast64_t batch_owner_wait_slow_samples;
  atomic_uint_fast64_t batch_post_call_sum_ns;
  atomic_uint_fast64_t batch_owner_work_sum_ns;
  atomic_uint_fast64_t batch_owner_work_max_ns;
  atomic_uint_fast64_t batch_owner_work_slow_samples;
  atomic_uint_fast64_t batch_waiter_wake_sum_ns;
  atomic_uint_fast64_t batch_waiter_wake_max_ns;
  atomic_uint_fast64_t batch_waiter_wake_slow_samples;
  atomic_uint_fast64_t batch_total_sum_ns;
  atomic_uint_fast64_t batch_total_max_ns;
  atomic_uint_fast64_t batch_total_slow_samples;
} flow_fmq_send_profile_aggregate_t;

static flow_fmq_send_profile_aggregate_t flow_fmq_send_profile;
#endif

void flow_fmq_send_profile_reset(void) {
#ifdef TURBO_FLOW_FMQ_INTERNAL_PROFILING
  atomic_store_explicit(&flow_fmq_send_profile.samples, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.post_samples, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.socket_samples, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.socket_cpu_samples, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.enqueue_sum_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.owner_wait_sum_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.post_call_sum_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.owner_dispatch_sum_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.socket_send_sum_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.socket_send_max_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.socket_send_slow_samples, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.socket_cpu_wall_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.socket_thread_cpu_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.completion_sum_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.waiter_wake_sum_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.total_sum_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.total_max_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.total_slow_samples, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_socket_calls, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_socket_cpu_samples, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_socket_frames, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_socket_iov_segments, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_socket_send_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_socket_send_max_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_socket_send_slow_samples, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_socket_cpu_wall_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_socket_thread_cpu_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_samples, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_post_samples, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_build_sum_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_build_max_ns, 0u, memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_build_slow_samples, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_payload_prepare_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_payload_prepare_max_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_payload_prepare_slow_samples, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_submit_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_adapter_consume_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_enqueue_prepare_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_owner_wait_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_owner_wait_max_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_owner_wait_slow_samples, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_post_call_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_owner_work_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_owner_work_max_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_owner_work_slow_samples, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_waiter_wake_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_waiter_wake_max_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_waiter_wake_slow_samples, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_total_sum_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_total_max_ns, 0u,
                        memory_order_relaxed);
  atomic_store_explicit(&flow_fmq_send_profile.batch_total_slow_samples, 0u,
                        memory_order_relaxed);
#endif
}

void flow_fmq_send_profile_set_enabled(int enabled) {
#ifdef TURBO_FLOW_FMQ_INTERNAL_PROFILING
  atomic_store_explicit(&flow_fmq_send_profile.enabled, enabled != 0, memory_order_release);
#else
  (void)enabled;
#endif
}

int flow_fmq_send_profile_snapshot(flow_fmq_send_profile_snapshot_t *out) {
  if (!out || out->size < sizeof(*out)) return TURBO_EINVAL;
#ifdef TURBO_FLOW_FMQ_INTERNAL_PROFILING
  uint64_t socket_cpu_wall_sum_ns;
  uint64_t batch_socket_cpu_wall_sum_ns;
  out->samples = atomic_load_explicit(&flow_fmq_send_profile.samples, memory_order_relaxed);
  out->post_samples =
      atomic_load_explicit(&flow_fmq_send_profile.post_samples, memory_order_relaxed);
  out->socket_samples =
      atomic_load_explicit(&flow_fmq_send_profile.socket_samples, memory_order_relaxed);
  out->socket_cpu_samples =
      atomic_load_explicit(&flow_fmq_send_profile.socket_cpu_samples, memory_order_relaxed);
  out->enqueue_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.enqueue_sum_ns, memory_order_relaxed);
  out->owner_wait_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.owner_wait_sum_ns, memory_order_relaxed);
  out->post_call_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.post_call_sum_ns, memory_order_relaxed);
  out->owner_dispatch_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.owner_dispatch_sum_ns, memory_order_relaxed);
  out->socket_send_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.socket_send_sum_ns, memory_order_relaxed);
  out->socket_send_max_ns =
      atomic_load_explicit(&flow_fmq_send_profile.socket_send_max_ns, memory_order_relaxed);
  out->socket_send_slow_samples = atomic_load_explicit(
      &flow_fmq_send_profile.socket_send_slow_samples, memory_order_relaxed);
  out->socket_thread_cpu_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.socket_thread_cpu_sum_ns, memory_order_relaxed);
  socket_cpu_wall_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.socket_cpu_wall_sum_ns, memory_order_relaxed);
  out->socket_estimated_off_cpu_sum_ns =
      socket_cpu_wall_sum_ns > out->socket_thread_cpu_sum_ns
          ? socket_cpu_wall_sum_ns - out->socket_thread_cpu_sum_ns
          : 0u;
  out->completion_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.completion_sum_ns, memory_order_relaxed);
  out->waiter_wake_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.waiter_wake_sum_ns, memory_order_relaxed);
  out->total_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.total_sum_ns, memory_order_relaxed);
  out->total_max_ns =
      atomic_load_explicit(&flow_fmq_send_profile.total_max_ns, memory_order_relaxed);
  out->total_slow_samples =
      atomic_load_explicit(&flow_fmq_send_profile.total_slow_samples, memory_order_relaxed);
  out->batch_socket_calls =
      atomic_load_explicit(&flow_fmq_send_profile.batch_socket_calls, memory_order_relaxed);
  out->batch_socket_cpu_samples = atomic_load_explicit(
      &flow_fmq_send_profile.batch_socket_cpu_samples, memory_order_relaxed);
  out->batch_socket_frames =
      atomic_load_explicit(&flow_fmq_send_profile.batch_socket_frames, memory_order_relaxed);
  out->batch_socket_iov_segments = atomic_load_explicit(
      &flow_fmq_send_profile.batch_socket_iov_segments, memory_order_relaxed);
  out->batch_socket_send_sum_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_socket_send_sum_ns, memory_order_relaxed);
  out->batch_socket_send_max_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_socket_send_max_ns, memory_order_relaxed);
  out->batch_socket_send_slow_samples = atomic_load_explicit(
      &flow_fmq_send_profile.batch_socket_send_slow_samples, memory_order_relaxed);
  out->batch_socket_thread_cpu_sum_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_socket_thread_cpu_sum_ns, memory_order_relaxed);
  batch_socket_cpu_wall_sum_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_socket_cpu_wall_sum_ns, memory_order_relaxed);
  out->batch_socket_estimated_off_cpu_sum_ns =
      batch_socket_cpu_wall_sum_ns > out->batch_socket_thread_cpu_sum_ns
          ? batch_socket_cpu_wall_sum_ns - out->batch_socket_thread_cpu_sum_ns
          : 0u;
  out->batch_samples =
      atomic_load_explicit(&flow_fmq_send_profile.batch_samples, memory_order_relaxed);
  out->batch_post_samples =
      atomic_load_explicit(&flow_fmq_send_profile.batch_post_samples, memory_order_relaxed);
  out->batch_build_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.batch_build_sum_ns, memory_order_relaxed);
  out->batch_build_max_ns =
      atomic_load_explicit(&flow_fmq_send_profile.batch_build_max_ns, memory_order_relaxed);
  out->batch_build_slow_samples = atomic_load_explicit(
      &flow_fmq_send_profile.batch_build_slow_samples, memory_order_relaxed);
  out->batch_payload_prepare_sum_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_payload_prepare_sum_ns, memory_order_relaxed);
  out->batch_payload_prepare_max_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_payload_prepare_max_ns, memory_order_relaxed);
  out->batch_payload_prepare_slow_samples = atomic_load_explicit(
      &flow_fmq_send_profile.batch_payload_prepare_slow_samples, memory_order_relaxed);
  out->batch_submit_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.batch_submit_sum_ns, memory_order_relaxed);
  out->batch_adapter_consume_sum_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_adapter_consume_sum_ns, memory_order_relaxed);
  out->batch_enqueue_prepare_sum_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_enqueue_prepare_sum_ns, memory_order_relaxed);
  out->batch_owner_wait_sum_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_owner_wait_sum_ns, memory_order_relaxed);
  out->batch_owner_wait_max_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_owner_wait_max_ns, memory_order_relaxed);
  out->batch_owner_wait_slow_samples = atomic_load_explicit(
      &flow_fmq_send_profile.batch_owner_wait_slow_samples, memory_order_relaxed);
  out->batch_post_call_sum_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_post_call_sum_ns, memory_order_relaxed);
  out->batch_owner_work_sum_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_owner_work_sum_ns, memory_order_relaxed);
  out->batch_owner_work_max_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_owner_work_max_ns, memory_order_relaxed);
  out->batch_owner_work_slow_samples = atomic_load_explicit(
      &flow_fmq_send_profile.batch_owner_work_slow_samples, memory_order_relaxed);
  out->batch_waiter_wake_sum_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_waiter_wake_sum_ns, memory_order_relaxed);
  out->batch_waiter_wake_max_ns = atomic_load_explicit(
      &flow_fmq_send_profile.batch_waiter_wake_max_ns, memory_order_relaxed);
  out->batch_waiter_wake_slow_samples = atomic_load_explicit(
      &flow_fmq_send_profile.batch_waiter_wake_slow_samples, memory_order_relaxed);
  out->batch_total_sum_ns =
      atomic_load_explicit(&flow_fmq_send_profile.batch_total_sum_ns, memory_order_relaxed);
  out->batch_total_max_ns =
      atomic_load_explicit(&flow_fmq_send_profile.batch_total_max_ns, memory_order_relaxed);
  out->batch_total_slow_samples = atomic_load_explicit(
      &flow_fmq_send_profile.batch_total_slow_samples, memory_order_relaxed);
  return TURBO_OK;
#else
  return TURBO_ENOTSUP;
#endif
}

static const char FLOW_FMQ_CONNECTION_SCHEMA_TEXT[] =
    "schema TurboFlowFmqResource [id(1), version(1)];\n"
    "message ConnectionStatus {\n"
    "  uint32 state;\n"
    "  int32 last_status;\n"
    "  string connections_current;\n"
    "  string connection_limit;\n"
    "  string in_flight_messages;\n"
    "  string in_flight_bytes;\n"
    "}\n";
static const char FLOW_FMQ_QUEUE_SCHEMA_TEXT[] =
    "schema TurboFlowFmqResource [id(2), version(1)];\n"
    "message FmqQueueStatus {\n"
    "  string load;\n"
    "  string capacity;\n"
    "  string messages;\n"
    "  string bytes;\n"
    "  bool saturated;\n"
    "  bool accepting;\n"
    "}\n";
static const char FLOW_FMQ_PROTOCOL_SCHEMA_TEXT[] =
    "schema TurboFlowFmqResource [id(3), version(1)];\n"
    "message FmqProtocolStatus {\n"
    "  uint32 pattern;\n"
    "  uint32 mode;\n"
    "  uint32 transport;\n"
    "  uint32 pattern_state;\n"
    "  string sessions;\n"
    "  bool started;\n"
    "  bool quiesced;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOW_FMQ_CONNECTION_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_IO_TRANSPORT,
    TURBO_FLOW_RESOURCE_CONNECTION,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowFmqResource",
    "ConnectionStatus",
    1u,
    1u,
    FLOW_FMQ_CONNECTION_SCHEMA_TEXT};
static const turbo_flow_resource_schema_t FLOW_FMQ_QUEUE_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_QUEUE_BUFFER,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowFmqResource",
    "FmqQueueStatus",
    2u,
    1u,
    FLOW_FMQ_QUEUE_SCHEMA_TEXT};
static const turbo_flow_resource_schema_t FLOW_FMQ_PROTOCOL_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowFmqResource",
    "FmqProtocolStatus",
    3u,
    1u,
    FLOW_FMQ_PROTOCOL_SCHEMA_TEXT};

typedef flowmq_peer_exchange_state_t flow_fmq_pattern_state_t;
#define FLOW_FMQ_PATTERN_READY FLOWMQ_PEER_EXCHANGE_READY
#define FLOW_FMQ_PATTERN_WAIT_REPLY FLOWMQ_PEER_EXCHANGE_WAIT_REPLY
#define FLOW_FMQ_PATTERN_PROCESSING_REQUEST FLOWMQ_PEER_EXCHANGE_PROCESSING_REQUEST
#define FLOW_FMQ_PATTERN_RESETTING FLOWMQ_PEER_EXCHANGE_RESETTING

static flow_fmq_pattern_state_t flow_fmq_exchange_state(const flowmq_peer_session_t *exchange) {
  flowmq_peer_exchange_state_t state = FLOWMQ_PEER_EXCHANGE_RESETTING;
  uint64_t generation = 0u;
  uint64_t correlation_id = 0u;
  if (flowmq_peer_session_snapshot(exchange, &state, &generation, &correlation_id) != TURBO_OK)
    return FLOWMQ_PEER_EXCHANGE_RESETTING;
  return state;
}

static void flow_fmq_timeout_config_resolve(tf_coronet_socket_timeout_config_t *timeouts,
                                            const turbo_flow_fmq_config_t *config) {
  if (!timeouts || !config) return;
  timeouts->timeout_ms = config->timeout_ms;
  timeouts->connect_timeout_ms = config->connect_timeout_ms;
  timeouts->send_timeout_ms = config->send_timeout_ms;
  timeouts->recv_timeout_ms = config->recv_timeout_ms;
  timeouts->handshake_timeout_ms = config->handshake_timeout_ms;
  if (timeouts->timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_DEFAULT;
  if (timeouts->connect_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_CONNECT;
  if (timeouts->send_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_SEND;
  if (timeouts->recv_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_RECV;
  if (timeouts->handshake_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_HANDSHAKE;
  if (timeouts->timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->timeout_ms = 0;
  if (timeouts->connect_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED)
    timeouts->connect_timeout_ms = 0;
  if (timeouts->send_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->send_timeout_ms = 0;
  if (timeouts->recv_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->recv_timeout_ms = 0;
  if (timeouts->handshake_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED)
    timeouts->handshake_timeout_ms = 0;
  tf_coronet_socket_timeouts_resolve(timeouts, FLOW_FMQ_DEFAULT_TIMEOUT_MS);
}

static int flow_fmq_connect_endpoint_execution_resolve(
    flowmq_connect_endpoint_config_t *endpoint,
    const turbo_flow_coronet_execution_binding_t *execution) {
  if (!endpoint || !execution) return TURBO_EINVAL;
  switch (execution->kind) {
  case TURBO_FLOW_CORONET_EXECUTION_PRIVATE:
    endpoint->drive_context = 1;
    endpoint->own_context = 1;
    return TURBO_OK;
  case TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT:
    endpoint->context = execution->context;
    return TURBO_OK;
  case TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT:
    endpoint->context = execution->context;
    endpoint->drive_context = 1;
    endpoint->own_context = 1;
    return TURBO_OK;
  case TURBO_FLOW_CORONET_EXECUTION_POOL_LANE:
    endpoint->context = coro_thread_pool_get_context(execution->pool, (int)execution->lane);
    return endpoint->context ? TURBO_OK : TURBO_ERANGE;
  default:
    return TURBO_EINVAL;
  }
}

static const char *const FLOW_FMQ_PATTERN_VALUES[] = {
    "pub", "sub", "push", "pull", "router", "dealer", "pair", "req", "rep", "xpub", "xsub"};
static const char *const FLOW_FMQ_MODE_VALUES[] = {"bind", "connect"};
static const char *const FLOW_FMQ_TRANSPORT_VALUES[] = {"tcp",  "tls", "udp", "kcp",
                                                        "pipe", "ws",  "wss"};
static const char *const FLOW_FMQ_METADATA_POLICY_VALUES[] = {"static", "inherit", "content"};
static const char *const FLOW_FMQ_FRAME_ADMISSION_POLICY_VALUES[] = {"fail", "block",
                                                                     "drop_oldest"};
static const char *const FLOW_FMQ_SLOW_PEER_POLICY_VALUES[] = {"fail", "drop_oldest", "disconnect"};

#define FLOW_FMQ_OPTION_FIELD(field_name, field_type) \
  {field_name, field_type, 0u, 0u, 0u, NULL, 0u}
#define FLOW_FMQ_OPTION_ENUM(field_name, values, count) \
  {field_name, TURBO_FLOW_OPTION_ENUM, 0u, 0u, 0u, values, count}
#define FLOW_FMQ_OPTION_REQUIRED_ENUM(field_name, values, count) \
  {field_name, TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0u, 0u, values, count}
#define FLOW_FMQ_OPTION_RANGE(field_name, field_type, minimum, maximum) \
  {field_name, field_type, TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, minimum, maximum, \
   NULL, 0u}
#define FLOW_FMQ_OPTION_STRING_LIMIT(field_name, maximum) \
  {field_name, TURBO_FLOW_OPTION_STRING, 0u, 0u, maximum, NULL, 0u}
#define FLOW_FMQ_OPTION_HOST_OBJECT(field_name) \
  {field_name, TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0u, 0u, NULL, 0u}

static const turbo_flow_option_field_t FLOW_FMQ_OPTION_FIELDS[] = {
    FLOW_FMQ_OPTION_REQUIRED_ENUM("pattern", FLOW_FMQ_PATTERN_VALUES, 11u),
    FLOW_FMQ_OPTION_REQUIRED_ENUM("mode", FLOW_FMQ_MODE_VALUES, 2u),
    FLOW_FMQ_OPTION_REQUIRED_ENUM("transport", FLOW_FMQ_TRANSPORT_VALUES, 7u),
    FLOW_FMQ_OPTION_FIELD("kcp_pre_shared_key", TURBO_FLOW_OPTION_STRING),
    FLOW_FMQ_OPTION_RANGE("kcp_mtu", TURBO_FLOW_OPTION_U32, 576u, UINT16_MAX),
    FLOW_FMQ_OPTION_RANGE("kcp_send_window", TURBO_FLOW_OPTION_U32, 1u, UINT16_MAX),
    FLOW_FMQ_OPTION_RANGE("kcp_receive_window", TURBO_FLOW_OPTION_U32, 1u, UINT16_MAX),
    FLOW_FMQ_OPTION_RANGE("kcp_interval_ms", TURBO_FLOW_OPTION_U32, 1u, 100u),
    FLOW_FMQ_OPTION_RANGE("kcp_handshake_retry_ms", TURBO_FLOW_OPTION_U32, 1u, UINT16_MAX),
    FLOW_FMQ_OPTION_RANGE("kcp_fast_resend", TURBO_FLOW_OPTION_U32, 0u, UINT8_MAX),
    FLOW_FMQ_OPTION_FIELD("kcp_congestion_control", TURBO_FLOW_OPTION_BOOL),
    FLOW_FMQ_OPTION_RANGE("kcp_fec_data_shards", TURBO_FLOW_OPTION_U32, 1u, 255u),
    FLOW_FMQ_OPTION_RANGE("kcp_fec_parity_shards", TURBO_FLOW_OPTION_U32, 1u, 255u),
    FLOW_FMQ_OPTION_RANGE("kcp_fec_max_payload_size", TURBO_FLOW_OPTION_U32, 1u, UINT16_MAX),
    FLOW_FMQ_OPTION_RANGE("kcp_fec_receive_groups", TURBO_FLOW_OPTION_U32, 1u, 64u),
    FLOW_FMQ_OPTION_FIELD("host", TURBO_FLOW_OPTION_STRING),
    FLOW_FMQ_OPTION_RANGE("port", TURBO_FLOW_OPTION_U32, 1u, 65535u),
    FLOW_FMQ_OPTION_FIELD("path", TURBO_FLOW_OPTION_STRING),
    FLOW_FMQ_OPTION_STRING_LIMIT("topic", TURBO_FLOW_FMQ_MAX_TOPIC_SIZE),
    FLOW_FMQ_OPTION_FIELD("content_type", TURBO_FLOW_OPTION_STRING),
    FLOW_FMQ_OPTION_HOST_OBJECT("content_binding"),
    FLOW_FMQ_OPTION_STRING_LIMIT("identity", TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE),
    FLOW_FMQ_OPTION_ENUM("topic_policy", FLOW_FMQ_METADATA_POLICY_VALUES, 3u),
    FLOW_FMQ_OPTION_ENUM("identity_policy", FLOW_FMQ_METADATA_POLICY_VALUES, 3u),
    FLOW_FMQ_OPTION_RANGE("max_frame_size", TURBO_FLOW_OPTION_SIZE, 1u, UINT32_MAX),
    FLOW_FMQ_OPTION_RANGE("max_connections", TURBO_FLOW_OPTION_U32, 1u,
                          TURBO_FLOW_FMQ_MAX_CONNECTIONS_LIMIT),
    FLOW_FMQ_OPTION_FIELD(TF_CORONET_OPTION_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD(TF_CORONET_OPTION_CONNECT_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD(TF_CORONET_OPTION_SEND_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD(TF_CORONET_OPTION_RECV_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD(TF_CORONET_OPTION_HANDSHAKE_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD("reconnect_initial_ms", TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD("reconnect_max_ms", TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD("heartbeat_interval_ms", TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD("heartbeat_timeout_ms", TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_HOST_OBJECT("event_callback"),
    FLOW_FMQ_OPTION_HOST_OBJECT("event_ctx"),
    FLOW_FMQ_OPTION_FIELD("reuse_port", TURBO_FLOW_OPTION_BOOL),
    FLOW_FMQ_OPTION_FIELD("tcp_keepalive", TURBO_FLOW_OPTION_BOOL),
    FLOW_FMQ_OPTION_FIELD("tcp_keepalive_idle_ms", TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD("tcp_keepalive_interval_ms", TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD("tcp_keepalive_count", TURBO_FLOW_OPTION_U32),
    FLOW_FMQ_OPTION_FIELD("linger", TURBO_FLOW_OPTION_BOOL),
    FLOW_FMQ_OPTION_FIELD("linger_ms", TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD("send_hwm_bytes", TURBO_FLOW_OPTION_SIZE),
    FLOW_FMQ_OPTION_FIELD("udp_multicast_group", TURBO_FLOW_OPTION_STRING),
    FLOW_FMQ_OPTION_FIELD("udp_multicast_interface", TURBO_FLOW_OPTION_STRING),
    FLOW_FMQ_OPTION_FIELD("udp_option_flags", TURBO_FLOW_OPTION_U32),
    FLOW_FMQ_OPTION_FIELD("udp_multicast_loop", TURBO_FLOW_OPTION_BOOL),
    FLOW_FMQ_OPTION_RANGE("udp_multicast_ttl", TURBO_FLOW_OPTION_U32, 0u, 255u),
    FLOW_FMQ_OPTION_FIELD("udp_broadcast", TURBO_FLOW_OPTION_BOOL),
    FLOW_FMQ_OPTION_FIELD("frame_hwm_messages", TURBO_FLOW_OPTION_SIZE),
    FLOW_FMQ_OPTION_FIELD("frame_hwm_bytes", TURBO_FLOW_OPTION_SIZE),
    FLOW_FMQ_OPTION_ENUM("frame_admission_policy", FLOW_FMQ_FRAME_ADMISSION_POLICY_VALUES, 3u),
    FLOW_FMQ_OPTION_FIELD("frame_admission_timeout_ms", TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD("frame_linger_ms", TURBO_FLOW_OPTION_DURATION_MS),
    FLOW_FMQ_OPTION_FIELD("peer_hwm_messages", TURBO_FLOW_OPTION_SIZE),
    FLOW_FMQ_OPTION_FIELD("peer_hwm_bytes", TURBO_FLOW_OPTION_SIZE),
    FLOW_FMQ_OPTION_ENUM("slow_peer_policy", FLOW_FMQ_SLOW_PEER_POLICY_VALUES, 3u)};

#undef FLOW_FMQ_OPTION_HOST_OBJECT
#undef FLOW_FMQ_OPTION_STRING_LIMIT
#undef FLOW_FMQ_OPTION_RANGE
#undef FLOW_FMQ_OPTION_REQUIRED_ENUM
#undef FLOW_FMQ_OPTION_ENUM
#undef FLOW_FMQ_OPTION_FIELD

typedef struct flow_fmq_adapter_s flow_fmq_adapter_t;

typedef enum flow_fmq_reactor_command_kind_e {
  FLOW_FMQ_REACTOR_START_BIND = 1,
  FLOW_FMQ_REACTOR_CLOSE_RESOURCES,
  FLOW_FMQ_REACTOR_CLEAR_PEERS
} flow_fmq_reactor_command_kind_t;

typedef struct flow_fmq_reactor_command_s {
  uint32_t kind;
  uint32_t reserved;
} flow_fmq_reactor_command_t;

/* Public lifecycle and resource-command entry points are host-serialized. */
#define FLOW_FMQ_REACTOR_COMMAND_CAPACITY 1u

typedef struct flow_fmq_resource_context_s {
  flow_fmq_adapter_t *adapter;
  turbo_flow_resource_kind_t kind;
} flow_fmq_resource_context_t;

typedef flowmq_stream_decoder_t flow_fmq_reader_t;

typedef struct flow_fmq_send_request_s flow_fmq_send_request_t;
typedef struct flow_fmq_send_batch_s flow_fmq_send_batch_t;
TURBO_DEQUE_DEFINE(flow_fmq_peer_send_requests, flow_fmq_send_request_t *)

typedef enum flow_fmq_send_batch_mode_e {
  FLOW_FMQ_SEND_BATCH_NONE = 0,
  FLOW_FMQ_SEND_BATCH_CONNECT,
  FLOW_FMQ_SEND_BATCH_BIND_PUB,
  FLOW_FMQ_SEND_BATCH_BIND_PUSH
} flow_fmq_send_batch_mode_t;

typedef enum flow_fmq_push_dispatch_result_e {
  FLOW_FMQ_PUSH_DISPATCHED = TURBO_OK,
  FLOW_FMQ_PUSH_WAIT_CREDIT = 1
} flow_fmq_push_dispatch_result_t;

typedef struct flow_fmq_send_batch_item_s {
  tstr_v topic;
  flow_fmq_route_token_t target_route;
  size_t delivered;
  int status;
  int started;
  int terminal;
} flow_fmq_send_batch_item_t;

typedef struct flow_fmq_peer_s {
  flow_fmq_adapter_t *adapter;
  coro_socket_t *socket;
  flow_fmq_route_token_t route;
  tstr_t identity;
  tstr_t topic;
  flowmq_subscription_set_t subscriptions;
  flow_fmq_reader_t reader;
  flow_fmq_peer_send_requests send_queue;
  turbo_vec_t send_iov;
  tf_io_budget_t send_budget;
  int send_queue_initialized;
  int send_iov_initialized;
  int send_budget_initialized;
  int send_drain_active;
  int push_dispatch_reserved;
  int closing;
  atomic_int disconnect_event_sent;
  flowmq_peer_session_t exchange;
  turbo_flow_security_principal_t principal;
  int authenticated;
} flow_fmq_peer_t;

typedef struct flow_fmq_message_context_s {
  uint32_t magic;
  flow_fmq_adapter_t *adapter;
  flow_fmq_peer_t *peer;
  turbo_flow_fmq_pattern_t pattern;
  uint64_t owner_instance_id;
  flow_fmq_route_token_t route;
  tstr_v identity;
  tstr_v topic;
  uint64_t correlation_id;
  uint64_t correlation_generation;
  int subscription;
} flow_fmq_message_context_t;

struct flow_fmq_send_request_s {
  flow_fmq_adapter_t *adapter;
  flow_fmq_route_token_t route;
  tstr_t frame;
  flow_fmq_segmented_frame_t segmented_frame;
  mem_buffer_t *payload_buffer;
  tstr_v encoded_topic;
  turbo_mutex_t mutex;
  turbo_cond_t cond;
  coro_wait_t *completion_wait;
  atomic_int refs;
  int done;
  int completion_wait_armed;
  int status;
  int cancel_requested;
  flow_fmq_delivery_stage_t delivery_stage;
  size_t budget_bytes;
  int budget_reserved;
  int drain_allowed;
  size_t fanout_pending;
  size_t fanout_delivered;
  size_t fanout_dropped;
  int fanout_active;
  int fanout_first_error;
  int peer_send_started;
  int peer_send_status;
  flow_fmq_send_batch_t *batch;
  size_t batch_index;
  int batch_state_shared;
  int batch_storage_owned;
  size_t batch_frame_offset;
  size_t batch_frame_size;
  struct flow_fmq_send_request_s *drop_next;
#ifdef TURBO_FLOW_FMQ_INTERNAL_PROFILING
  int profile_active;
  uint64_t profile_submit_started_ns;
  uint64_t profile_enqueued_ns;
  uint64_t profile_post_started_ns;
  uint64_t profile_post_finished_ns;
  uint64_t profile_owner_dequeued_ns;
  uint64_t profile_socket_started_ns;
  uint64_t profile_socket_finished_ns;
  uint64_t profile_socket_thread_cpu_started_ns;
  uint64_t profile_socket_thread_cpu_finished_ns;
  int profile_socket_thread_cpu_available;
  int profile_socket_shared;
  uint64_t profile_completion_signaled_ns;
#endif
};

struct flow_fmq_send_batch_s {
  const void *owner;
  flow_fmq_adapter_t *adapter;
  flow_fmq_send_request_t **requests;
  flow_fmq_send_request_t *request_storage;
  size_t request_storage_used;
  turbo_iovec_t *iov;
  size_t *iov_indices;
  void *iov_storage;
  size_t iov_capacity;
  unsigned char *encoded_storage;
  size_t encoded_storage_used;
  size_t encoded_storage_capacity;
  flow_fmq_send_batch_item_t *items;
  size_t capacity;
  size_t count;
  size_t completed;
  atomic_int refs;
  turbo_mutex_t mutex;
  turbo_cond_t cond;
  coro_wait_t *completion_wait;
  int completion_wait_armed;
  int committed;
  int first_status;
#ifdef TURBO_FLOW_FMQ_INTERNAL_PROFILING
  int profile_active;
  uint64_t profile_submit_started_ns;
  uint64_t profile_payload_prepare_sum_ns;
  uint64_t profile_submit_sum_ns;
  uint64_t profile_adapter_consume_sum_ns;
  uint64_t profile_enqueue_started_ns;
  uint64_t profile_enqueued_ns;
  uint64_t profile_post_started_ns;
  uint64_t profile_post_finished_ns;
  uint64_t profile_owner_dequeued_ns;
  uint64_t profile_completion_signaled_ns;
#endif
};

static _Thread_local flow_fmq_send_batch_t *flow_fmq_active_send_batch;

#ifdef TURBO_FLOW_FMQ_INTERNAL_PROFILING
static uint64_t flow_fmq_profile_delta(uint64_t later, uint64_t earlier) {
  return earlier != 0u && later >= earlier ? later - earlier : 0u;
}

static void flow_fmq_profile_add(atomic_uint_fast64_t *sum, uint64_t value) {
  atomic_fetch_add_explicit(sum, value, memory_order_relaxed);
}

static void flow_fmq_profile_max(atomic_uint_fast64_t *maximum, uint64_t value) {
  uint_fast64_t observed = atomic_load_explicit(maximum, memory_order_relaxed);
  while (observed < value &&
         !atomic_compare_exchange_weak_explicit(maximum, &observed, value,
                                                memory_order_relaxed,
                                                memory_order_relaxed)) {
  }
}

static void flow_fmq_profile_record_duration(atomic_uint_fast64_t *sum,
                                             atomic_uint_fast64_t *maximum,
                                             atomic_uint_fast64_t *slow_samples,
                                             uint64_t value) {
  flow_fmq_profile_add(sum, value);
  flow_fmq_profile_max(maximum, value);
  if (value >= FLOW_FMQ_PROFILE_SLOW_SAMPLE_NS) {
    atomic_fetch_add_explicit(slow_samples, 1u, memory_order_relaxed);
  }
}

/* During coro_socket_send() the owner coroutine may yield. The delta therefore includes all CPU
 * consumed by its owner thread in that wall-clock interval and makes off-CPU a conservative
 * estimate, not an exact per-coroutine measurement. */
static int flow_fmq_profile_thread_cpu_ns(uint64_t *out) {
  if (!out) return 0;
#ifdef _WIN32
  FILETIME created;
  FILETIME exited;
  FILETIME kernel;
  FILETIME user;
  ULARGE_INTEGER kernel_ticks;
  ULARGE_INTEGER user_ticks;
  if (!GetThreadTimes(GetCurrentThread(), &created, &exited, &kernel, &user)) return 0;
  kernel_ticks.LowPart = kernel.dwLowDateTime;
  kernel_ticks.HighPart = kernel.dwHighDateTime;
  user_ticks.LowPart = user.dwLowDateTime;
  user_ticks.HighPart = user.dwHighDateTime;
  *out = (kernel_ticks.QuadPart + user_ticks.QuadPart) * UINT64_C(100);
  return 1;
#elif defined(CLOCK_THREAD_CPUTIME_ID)
  struct timespec value;
  if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) != 0) return 0;
  *out = (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
  return 1;
#else
  return 0;
#endif
}

static void flow_fmq_profile_request_begin(flow_fmq_send_request_t *request) {
  request->profile_active =
      atomic_load_explicit(&flow_fmq_send_profile.enabled, memory_order_acquire);
  if (request->profile_active) request->profile_submit_started_ns = turbo_hrtime();
}

static void flow_fmq_profile_request_enqueued(flow_fmq_send_request_t *request) {
  if (request->profile_active) request->profile_enqueued_ns = turbo_hrtime();
}

static void flow_fmq_profile_request_owner_dequeued(flow_fmq_send_request_t *request) {
  if (request->profile_active) request->profile_owner_dequeued_ns = turbo_hrtime();
}

static void flow_fmq_profile_request_post_begin(flow_fmq_send_request_t *request) {
  if (request->profile_active) request->profile_post_started_ns = turbo_hrtime();
}

static void flow_fmq_profile_request_post_end(flow_fmq_send_request_t *request) {
  if (request->profile_active) request->profile_post_finished_ns = turbo_hrtime();
}

static void flow_fmq_profile_request_socket_begin(flow_fmq_send_request_t *request) {
  if (request->profile_active && request->profile_socket_started_ns == 0u) {
    request->profile_socket_started_ns = turbo_hrtime();
    request->profile_socket_thread_cpu_available = flow_fmq_profile_thread_cpu_ns(
        &request->profile_socket_thread_cpu_started_ns);
  }
}

static void flow_fmq_profile_request_socket_end(flow_fmq_send_request_t *request) {
  if (request->profile_active) {
    if (request->profile_socket_thread_cpu_available &&
        !flow_fmq_profile_thread_cpu_ns(&request->profile_socket_thread_cpu_finished_ns)) {
      request->profile_socket_thread_cpu_available = 0;
    }
    request->profile_socket_finished_ns = turbo_hrtime();
  }
}

static void flow_fmq_profile_request_shared_socket_begin(flow_fmq_send_request_t *request) {
  flow_fmq_profile_request_socket_begin(request);
  if (request->profile_active) request->profile_socket_shared = 1;
}

static void flow_fmq_profile_request_shared_socket_end(flow_fmq_send_request_t *request) {
  flow_fmq_profile_request_socket_end(request);
}

typedef struct flow_fmq_profile_batch_socket_sample_s {
  uint64_t started_ns;
  uint64_t thread_cpu_started_ns;
  int active;
  int thread_cpu_available;
} flow_fmq_profile_batch_socket_sample_t;

static void flow_fmq_profile_batch_socket_begin(
    flow_fmq_profile_batch_socket_sample_t *sample) {
  if (!sample) return;
  memset(sample, 0, sizeof(*sample));
  sample->active =
      atomic_load_explicit(&flow_fmq_send_profile.enabled, memory_order_acquire);
  if (!sample->active) return;
  sample->started_ns = turbo_hrtime();
  sample->thread_cpu_available =
      flow_fmq_profile_thread_cpu_ns(&sample->thread_cpu_started_ns);
}

static void flow_fmq_profile_batch_socket_end(flow_fmq_profile_batch_socket_sample_t *sample,
                                              size_t frames, size_t iov_segments) {
  uint64_t finished_ns;
  uint64_t socket_wall_ns;
  uint64_t thread_cpu_finished_ns = 0u;
  uint64_t thread_cpu_ns = 0u;
  if (!sample || !sample->active || sample->started_ns == 0u) return;
  if (sample->thread_cpu_available &&
      !flow_fmq_profile_thread_cpu_ns(&thread_cpu_finished_ns)) {
    sample->thread_cpu_available = 0;
  }
  finished_ns = turbo_hrtime();
  socket_wall_ns = flow_fmq_profile_delta(finished_ns, sample->started_ns);
  flow_fmq_profile_record_duration(
      &flow_fmq_send_profile.batch_socket_send_sum_ns,
      &flow_fmq_send_profile.batch_socket_send_max_ns,
      &flow_fmq_send_profile.batch_socket_send_slow_samples, socket_wall_ns);
  flow_fmq_profile_add(&flow_fmq_send_profile.batch_socket_frames, frames);
  flow_fmq_profile_add(&flow_fmq_send_profile.batch_socket_iov_segments, iov_segments);
  atomic_fetch_add_explicit(&flow_fmq_send_profile.batch_socket_calls, 1u,
                            memory_order_relaxed);
  if (sample->thread_cpu_available) {
    thread_cpu_ns =
        flow_fmq_profile_delta(thread_cpu_finished_ns, sample->thread_cpu_started_ns);
    flow_fmq_profile_add(&flow_fmq_send_profile.batch_socket_cpu_wall_sum_ns,
                         socket_wall_ns);
    flow_fmq_profile_add(&flow_fmq_send_profile.batch_socket_thread_cpu_sum_ns,
                         thread_cpu_ns);
    atomic_fetch_add_explicit(&flow_fmq_send_profile.batch_socket_cpu_samples, 1u,
                              memory_order_relaxed);
  }
}

static void flow_fmq_profile_batch_begin(flow_fmq_send_batch_t *batch) {
  batch->profile_active =
      atomic_load_explicit(&flow_fmq_send_profile.enabled, memory_order_acquire);
  if (batch->profile_active) batch->profile_submit_started_ns = turbo_hrtime();
}

static void flow_fmq_profile_batch_enqueued(flow_fmq_send_batch_t *batch) {
  if (batch->profile_active) batch->profile_enqueued_ns = turbo_hrtime();
}

static uint64_t flow_fmq_profile_batch_step_begin(const flow_fmq_send_batch_t *batch) {
  return batch->profile_active ? turbo_hrtime() : 0u;
}

static void flow_fmq_profile_batch_payload_prepared(flow_fmq_send_batch_t *batch,
                                                    uint64_t started_ns) {
  if (batch->profile_active) {
    batch->profile_payload_prepare_sum_ns +=
        flow_fmq_profile_delta(turbo_hrtime(), started_ns);
  }
}

static void flow_fmq_profile_batch_submitted(flow_fmq_send_batch_t *batch,
                                             uint64_t started_ns) {
  if (batch->profile_active) {
    batch->profile_submit_sum_ns +=
        flow_fmq_profile_delta(turbo_hrtime(), started_ns);
  }
}

static void flow_fmq_profile_batch_adapter_consumed(flow_fmq_send_batch_t *batch,
                                                    uint64_t started_ns) {
  if (batch->profile_active) {
    batch->profile_adapter_consume_sum_ns +=
        flow_fmq_profile_delta(turbo_hrtime(), started_ns);
  }
}

static void flow_fmq_profile_batch_enqueue_begin(flow_fmq_send_batch_t *batch) {
  if (batch->profile_active) batch->profile_enqueue_started_ns = turbo_hrtime();
}

static void flow_fmq_profile_batch_post_begin(flow_fmq_send_batch_t *batch) {
  if (batch->profile_active) batch->profile_post_started_ns = turbo_hrtime();
}

static void flow_fmq_profile_batch_post_end(flow_fmq_send_batch_t *batch) {
  if (batch->profile_active) batch->profile_post_finished_ns = turbo_hrtime();
}

static void flow_fmq_profile_batch_owner_dequeued(flow_fmq_send_batch_t *batch) {
  if (batch->profile_active) batch->profile_owner_dequeued_ns = turbo_hrtime();
}

static void flow_fmq_profile_batch_completed(flow_fmq_send_batch_t *batch) {
  if (batch->profile_active) batch->profile_completion_signaled_ns = turbo_hrtime();
}

static void flow_fmq_profile_batch_record(flow_fmq_send_batch_t *batch,
                                          uint64_t waiter_resumed_ns) {
  uint64_t build_ns;
  uint64_t completion_ns;
  uint64_t owner_wait_ns;
  uint64_t owner_work_ns;
  uint64_t waiter_wake_ns;
  uint64_t total_ns;
  if (!batch->profile_active || batch->profile_submit_started_ns == 0u) return;
  completion_ns = batch->profile_completion_signaled_ns != 0u
                      ? batch->profile_completion_signaled_ns
                      : batch->profile_owner_dequeued_ns;
  build_ns =
      flow_fmq_profile_delta(batch->profile_enqueued_ns, batch->profile_submit_started_ns);
  owner_wait_ns =
      flow_fmq_profile_delta(batch->profile_owner_dequeued_ns, batch->profile_enqueued_ns);
  owner_work_ns = flow_fmq_profile_delta(completion_ns, batch->profile_owner_dequeued_ns);
  waiter_wake_ns = flow_fmq_profile_delta(waiter_resumed_ns, completion_ns);
  total_ns = flow_fmq_profile_delta(waiter_resumed_ns, batch->profile_submit_started_ns);
  flow_fmq_profile_record_duration(
      &flow_fmq_send_profile.batch_build_sum_ns,
      &flow_fmq_send_profile.batch_build_max_ns,
      &flow_fmq_send_profile.batch_build_slow_samples, build_ns);
  flow_fmq_profile_record_duration(
      &flow_fmq_send_profile.batch_payload_prepare_sum_ns,
      &flow_fmq_send_profile.batch_payload_prepare_max_ns,
      &flow_fmq_send_profile.batch_payload_prepare_slow_samples,
      batch->profile_payload_prepare_sum_ns);
  flow_fmq_profile_add(&flow_fmq_send_profile.batch_submit_sum_ns,
                       batch->profile_submit_sum_ns);
  flow_fmq_profile_add(&flow_fmq_send_profile.batch_adapter_consume_sum_ns,
                       batch->profile_adapter_consume_sum_ns);
  flow_fmq_profile_add(
      &flow_fmq_send_profile.batch_enqueue_prepare_sum_ns,
      flow_fmq_profile_delta(batch->profile_enqueued_ns,
                             batch->profile_enqueue_started_ns));
  flow_fmq_profile_record_duration(
      &flow_fmq_send_profile.batch_owner_wait_sum_ns,
      &flow_fmq_send_profile.batch_owner_wait_max_ns,
      &flow_fmq_send_profile.batch_owner_wait_slow_samples, owner_wait_ns);
  if (batch->profile_post_started_ns != 0u && batch->profile_post_finished_ns != 0u) {
    flow_fmq_profile_add(
        &flow_fmq_send_profile.batch_post_call_sum_ns,
        flow_fmq_profile_delta(batch->profile_post_finished_ns,
                               batch->profile_post_started_ns));
    atomic_fetch_add_explicit(&flow_fmq_send_profile.batch_post_samples, 1u,
                              memory_order_relaxed);
  }
  flow_fmq_profile_record_duration(
      &flow_fmq_send_profile.batch_owner_work_sum_ns,
      &flow_fmq_send_profile.batch_owner_work_max_ns,
      &flow_fmq_send_profile.batch_owner_work_slow_samples, owner_work_ns);
  flow_fmq_profile_record_duration(
      &flow_fmq_send_profile.batch_waiter_wake_sum_ns,
      &flow_fmq_send_profile.batch_waiter_wake_max_ns,
      &flow_fmq_send_profile.batch_waiter_wake_slow_samples, waiter_wake_ns);
  flow_fmq_profile_record_duration(
      &flow_fmq_send_profile.batch_total_sum_ns,
      &flow_fmq_send_profile.batch_total_max_ns,
      &flow_fmq_send_profile.batch_total_slow_samples, total_ns);
  atomic_fetch_add_explicit(&flow_fmq_send_profile.batch_samples, 1u,
                            memory_order_relaxed);
  batch->profile_active = 0;
}

static void flow_fmq_profile_request_completed(flow_fmq_send_request_t *request) {
  if (request->profile_active) request->profile_completion_signaled_ns = turbo_hrtime();
}

static void flow_fmq_profile_request_record(flow_fmq_send_request_t *request,
                                            uint64_t waiter_resumed_ns) {
  uint64_t completion_base;
  if (!request->profile_active || request->profile_submit_started_ns == 0u) return;

  completion_base = request->profile_socket_finished_ns != 0u
                        ? request->profile_socket_finished_ns
                        : request->profile_owner_dequeued_ns;
  flow_fmq_profile_add(
      &flow_fmq_send_profile.enqueue_sum_ns,
      flow_fmq_profile_delta(request->profile_enqueued_ns, request->profile_submit_started_ns));
  flow_fmq_profile_add(
      &flow_fmq_send_profile.owner_wait_sum_ns,
      flow_fmq_profile_delta(request->profile_owner_dequeued_ns, request->profile_enqueued_ns));
  if (request->profile_post_started_ns != 0u && request->profile_post_finished_ns != 0u) {
    flow_fmq_profile_add(
        &flow_fmq_send_profile.post_call_sum_ns,
        flow_fmq_profile_delta(request->profile_post_finished_ns,
                               request->profile_post_started_ns));
    atomic_fetch_add_explicit(&flow_fmq_send_profile.post_samples, 1u, memory_order_relaxed);
  }
  flow_fmq_profile_add(
      &flow_fmq_send_profile.owner_dispatch_sum_ns,
      flow_fmq_profile_delta(request->profile_socket_started_ns,
                             request->profile_owner_dequeued_ns));
  if (!request->profile_socket_shared && request->profile_socket_started_ns != 0u &&
      request->profile_socket_finished_ns != 0u) {
    uint64_t socket_wall_ns = flow_fmq_profile_delta(request->profile_socket_finished_ns,
                                                     request->profile_socket_started_ns);
    flow_fmq_profile_record_duration(
        &flow_fmq_send_profile.socket_send_sum_ns,
        &flow_fmq_send_profile.socket_send_max_ns,
        &flow_fmq_send_profile.socket_send_slow_samples, socket_wall_ns);
    atomic_fetch_add_explicit(&flow_fmq_send_profile.socket_samples, 1u, memory_order_relaxed);
    if (request->profile_socket_thread_cpu_available) {
      uint64_t socket_cpu_ns =
          flow_fmq_profile_delta(request->profile_socket_thread_cpu_finished_ns,
                                 request->profile_socket_thread_cpu_started_ns);
      flow_fmq_profile_add(&flow_fmq_send_profile.socket_cpu_wall_sum_ns, socket_wall_ns);
      flow_fmq_profile_add(&flow_fmq_send_profile.socket_thread_cpu_sum_ns, socket_cpu_ns);
      atomic_fetch_add_explicit(&flow_fmq_send_profile.socket_cpu_samples, 1u,
                                memory_order_relaxed);
    }
  }
  flow_fmq_profile_add(
      &flow_fmq_send_profile.completion_sum_ns,
      flow_fmq_profile_delta(request->profile_completion_signaled_ns, completion_base));
  flow_fmq_profile_add(
      &flow_fmq_send_profile.waiter_wake_sum_ns,
      flow_fmq_profile_delta(waiter_resumed_ns, request->profile_completion_signaled_ns));
  {
    uint64_t total_ns =
        flow_fmq_profile_delta(waiter_resumed_ns, request->profile_submit_started_ns);
    flow_fmq_profile_record_duration(
        &flow_fmq_send_profile.total_sum_ns, &flow_fmq_send_profile.total_max_ns,
        &flow_fmq_send_profile.total_slow_samples, total_ns);
  }
  atomic_fetch_add_explicit(&flow_fmq_send_profile.samples, 1u, memory_order_relaxed);
  request->profile_active = 0;
}

static void flow_fmq_profile_request_record_batch(flow_fmq_send_request_t *request) {
  /* A batch has one shared waiter, not one waiter per frame. Record each frame through its
   * owner-completion boundary and leave the per-frame waiter-wake component at zero. */
  flow_fmq_profile_request_record(request, request->profile_completion_signaled_ns);
}
#else
#define flow_fmq_profile_request_begin(request) ((void)(request))
#define flow_fmq_profile_request_enqueued(request) ((void)(request))
#define flow_fmq_profile_request_owner_dequeued(request) ((void)(request))
#define flow_fmq_profile_request_post_begin(request) ((void)(request))
#define flow_fmq_profile_request_post_end(request) ((void)(request))
#define flow_fmq_profile_request_socket_begin(request) ((void)(request))
#define flow_fmq_profile_request_socket_end(request) ((void)(request))
#define flow_fmq_profile_request_shared_socket_begin(request) ((void)(request))
#define flow_fmq_profile_request_shared_socket_end(request) ((void)(request))
#define flow_fmq_profile_request_completed(request) ((void)(request))
#define flow_fmq_profile_request_record(request, waiter_resumed_ns)                                  \
  ((void)(request), (void)(waiter_resumed_ns))
#define flow_fmq_profile_request_record_batch(request) ((void)(request))
typedef struct flow_fmq_profile_batch_socket_sample_s {
  int unused;
} flow_fmq_profile_batch_socket_sample_t;
#define flow_fmq_profile_batch_socket_begin(sample) ((void)(sample))
#define flow_fmq_profile_batch_socket_end(sample, frames, iov_segments)                              \
  ((void)(sample), (void)(frames), (void)(iov_segments))
#define flow_fmq_profile_batch_begin(batch) ((void)(batch))
#define flow_fmq_profile_batch_step_begin(batch) ((void)(batch), UINT64_C(0))
#define flow_fmq_profile_batch_payload_prepared(batch, started_ns)                                 \
  ((void)(batch), (void)(started_ns))
#define flow_fmq_profile_batch_submitted(batch, started_ns)                                        \
  ((void)(batch), (void)(started_ns))
#define flow_fmq_profile_batch_adapter_consumed(batch, started_ns)                                 \
  ((void)(batch), (void)(started_ns))
#define flow_fmq_profile_batch_enqueue_begin(batch) ((void)(batch))
#define flow_fmq_profile_batch_enqueued(batch) ((void)(batch))
#define flow_fmq_profile_batch_post_begin(batch) ((void)(batch))
#define flow_fmq_profile_batch_post_end(batch) ((void)(batch))
#define flow_fmq_profile_batch_owner_dequeued(batch) ((void)(batch))
#define flow_fmq_profile_batch_completed(batch) ((void)(batch))
#define flow_fmq_profile_batch_record(batch, waiter_resumed_ns)                                     \
  ((void)(batch), (void)(waiter_resumed_ns))
#endif

TURBO_DEQUE_DEFINE(flow_fmq_send_requests, flow_fmq_send_request_t *)

typedef int (*flow_fmq_ingress_dispatch_fn)(void *ctx, turbo_flow_msg_t *message);

struct flow_fmq_adapter_s {
  turbo_flow_t *flow;
  flow_fmq_ingress_dispatch_fn ingress_dispatch;
  void *ingress_dispatch_ctx;
  tf_coronet_execution_t execution;
  tf_coronet_actor_t reactor_actor;
  int reactor_actor_initialized;
  coro_context_t *ctx;
  coro_socket_t *server;
  flowmq_connect_endpoint_t *connect_endpoint;
  turbo_vec_t peers;
  turbo_vec_t security_selection;
  tstr_t host;
  tstr_t path;
  tstr_t topic;
  tstr_t identity;
  tstr_t source_name;
  tstr_t resource_owner;
  tstr_t connection_uid;
  tstr_t queue_uid;
  tstr_t protocol_uid;
  tstr_t udp_multicast_group;
  tstr_t udp_multicast_interface;
  tstr_t tls_ca_file;
  tstr_t tls_cert_file;
  tstr_t tls_key_file;
  tstr_t tls_key_password;
  tstr_t tls_server_name;
  turbo_flow_fmq_tls_config_t tls;
  turbo_flow_fmq_pattern_t pattern;
  turbo_flow_fmq_endpoint_mode_t mode;
  turbo_flow_fmq_transport_t transport;
  turbo_flow_fmq_metadata_policy_t topic_policy;
  turbo_flow_fmq_metadata_policy_t identity_policy;
  turbo_kcp_config_t kcp_config;
  int port;
  size_t max_frame_size;
  uint32_t max_connections;
  size_t stream_recv_buffer_bytes;
  tf_coronet_socket_timeout_config_t timeouts;
  tf_coronet_socket_options_t socket_options;
  tf_coronet_udp_options_t udp_options;
  size_t frame_hwm_messages;
  size_t frame_hwm_bytes;
  turbo_flow_fmq_frame_admission_policy_t frame_admission_policy;
  turbo_flow_fmq_fanout_config_t fanout;
  int fanout_enabled;
  uint64_t frame_linger_ms;
  uint64_t heartbeat_interval_ms;
  uint64_t heartbeat_timeout_ms;
  turbo_flow_fmq_event_fn event_callback;
  void *event_ctx;
  turbo_flow_pattern_selector_t selector;
  int kcp_configured;
  int initial_subscription_configured;
  int reuse_port;
  int tls_configured;
  int start_refs;
  atomic_int started;
  atomic_int connect_status;
  atomic_int connection_state;
  atomic_int quiesced;
  atomic_size_t connections_current;
  tf_io_budget_t send_budget;
  flow_fmq_send_requests send_queue;
  turbo_mutex_t send_queue_mutex;
  turbo_mutex_t send_admission_mutex;
  int send_queue_initialized;
  int send_drain_active;
  uint64_t next_session_id;
  uint64_t instance_id;
  atomic_uint_fast64_t generation;
  flow_fmq_resource_context_t resource_contexts[3];
  atomic_uint_fast64_t next_message_id;
  tf_timer_t retry_wait;
  int retry_wait_initialized;
  turbo_mutex_t lane_task_mutex;
  turbo_cond_t lane_task_cond;
  size_t lane_task_count;
  int lane_task_sync_initialized;
  turbo_flow_content_descriptor_t data_descriptor;
  turbo_flow_content_descriptor_t control_descriptor;
  flowmq_security_binding_runtime_t security;
  int has_data_descriptor;
  int security_selection_initialized;
};

static void flow_fmq_server_handler(coro_socket_t *client, void *arg);
static int flow_fmq_publish(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                            const flow_fmq_frame_t *frame);
static int flow_fmq_reserve_drop_oldest(flow_fmq_adapter_t *adapter, size_t frame_bytes);
static void flow_fmq_fail_peer_send_queue(flow_fmq_peer_t *peer, int status);
static void flow_fmq_request_release(flow_fmq_send_request_t *request);
static void flow_fmq_complete_send_request(flow_fmq_send_request_t *request, int status);
static void flow_fmq_append_request(flow_fmq_send_request_t **head,
                                    flow_fmq_send_request_t **tail,
                                    flow_fmq_send_request_t *request);
static void flow_fmq_resume_send_drain(flow_fmq_adapter_t *adapter);
static int flow_fmq_request_contiguous_frame(const flow_fmq_send_request_t *request,
                                             const char **data, size_t *size);
static int flow_fmq_request_encoded_topic(const flow_fmq_send_request_t *request,
                                          size_t max_frame_size, tstr_v *topic);

static int flow_fmq_allocate_adapter_instance_id(uint64_t *out) {
  uint64_t current;
  if (!out) return TURBO_EINVAL;
  current = atomic_load_explicit(&flow_fmq_next_adapter_instance_id, memory_order_relaxed);
  for (;;) {
    if (current == UINT64_MAX) return TURBO_ERANGE;
    if (atomic_compare_exchange_weak_explicit(&flow_fmq_next_adapter_instance_id, &current,
                                              current + 1u, memory_order_relaxed,
                                              memory_order_relaxed)) {
      *out = current + 1u;
      return TURBO_OK;
    }
  }
}

static void flow_fmq_lane_task_begin(flow_fmq_adapter_t *adapter) {
  if (!adapter || !adapter->lane_task_sync_initialized) return;
  turbo_mutex_lock(&adapter->lane_task_mutex);
  adapter->lane_task_count += 1u;
  turbo_mutex_unlock(&adapter->lane_task_mutex);
}

static void flow_fmq_lane_task_end(flow_fmq_adapter_t *adapter) {
  if (!adapter || !adapter->lane_task_sync_initialized) return;
  turbo_mutex_lock(&adapter->lane_task_mutex);
  if (adapter->lane_task_count > 0u) adapter->lane_task_count -= 1u;
  if (adapter->lane_task_count == 0u) turbo_cond_broadcast(&adapter->lane_task_cond);
  turbo_mutex_unlock(&adapter->lane_task_mutex);
}

static void flow_fmq_wait_lane_tasks(flow_fmq_adapter_t *adapter) {
  if (!adapter || !adapter->lane_task_sync_initialized) return;
  turbo_mutex_lock(&adapter->lane_task_mutex);
  while (adapter->lane_task_count != 0u) {
    turbo_cond_wait(&adapter->lane_task_cond, &adapter->lane_task_mutex);
  }
  turbo_mutex_unlock(&adapter->lane_task_mutex);
}

static void flow_fmq_emit_event(flow_fmq_adapter_t *adapter, turbo_flow_fmq_event_kind_t kind,
                                int status, const flow_fmq_peer_t *peer, tstr_v peer_identity,
                                tstr_v peer_topic, uint64_t reconnect_delay_ms) {
  turbo_flow_fmq_event_t event = TURBO_FLOW_FMQ_EVENT_INIT;
  if (!adapter || !adapter->event_callback) return;
  event.kind = kind;
  event.status = status;
  event.pattern = adapter->pattern;
  event.mode = adapter->mode;
  event.transport = adapter->transport;
  if (peer) {
    if (peer->identity) event.peer_identity = tstr_to_v(peer->identity);
    if (peer->topic) event.peer_topic = tstr_to_v(peer->topic);
  }
  if (peer_identity.data || peer_identity.len != 0) event.peer_identity = peer_identity;
  if (peer_topic.data || peer_topic.len != 0) event.peer_topic = peer_topic;
  event.reconnect_delay_ms = reconnect_delay_ms;
  adapter->event_callback(adapter->event_ctx, &event);
}

static int flow_fmq_peer_authorize(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                                   uint32_t action, tstr_v resource, int emit_denial) {
  char resource_text[TURBO_FLOW_FMQ_MAX_TOPIC_SIZE + 1u];
  const char *selected_resource;
  int rc;
  if (!adapter || !peer) return TURBO_EINVAL;
  if (!adapter->security.enabled) return TURBO_OK;
  if (!peer->authenticated) return TURBO_EPERM;
  if (resource.len > TURBO_FLOW_FMQ_MAX_TOPIC_SIZE || (resource.len != 0u && !resource.data))
    return TURBO_EMSGSIZE;
  if (resource.len != 0u && memchr(resource.data, '\0', resource.len) != NULL) {
    if (emit_denial) {
      flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_AUTHORIZATION_DENIED, TURBO_EPERM, peer,
                          (tstr_v){0}, resource, 0u);
    }
    return TURBO_EPERM;
  }
  if (resource.len == 0u) {
    selected_resource = adapter->connection_uid;
  } else {
    memcpy(resource_text, resource.data, resource.len);
    resource_text[resource.len] = '\0';
    selected_resource = resource_text;
  }
  rc = flowmq_security_authorize(&adapter->security, &peer->principal, action,
                                 TURBO_FLOW_SECURITY_RESOURCE_GENERIC, selected_resource, NULL);
  if (rc != TURBO_OK && emit_denial) {
    flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_AUTHORIZATION_DENIED, TURBO_EPERM, peer,
                        (tstr_v){0}, resource, 0u);
  }
  return rc;
}

static void flow_fmq_emit_peer_disconnected(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                                            int status) {
  if (!adapter || !peer) return;
  if (atomic_exchange_explicit(&peer->disconnect_event_sent, 1, memory_order_acq_rel) != 0) {
    return;
  }
  flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_PEER_DISCONNECTED, status, peer, (tstr_v){0},
                      (tstr_v){0}, 0);
}

static void flow_fmq_emit_frame_event(flow_fmq_adapter_t *adapter, turbo_flow_fmq_event_kind_t kind,
                                      int status, size_t frame_bytes) {
  turbo_flow_fmq_event_t event = TURBO_FLOW_FMQ_EVENT_INIT;
  if (!adapter || !adapter->event_callback) return;
  event.kind = kind;
  event.status = status;
  event.pattern = adapter->pattern;
  event.mode = adapter->mode;
  event.transport = adapter->transport;
  event.frame_bytes = (uint64_t)frame_bytes;
  adapter->event_callback(adapter->event_ctx, &event);
}

static void flow_fmq_emit_peer_frame_event(flow_fmq_adapter_t *adapter,
                                           turbo_flow_fmq_event_kind_t kind, int status,
                                           const flow_fmq_peer_t *peer, size_t frame_bytes) {
  turbo_flow_fmq_event_t event = TURBO_FLOW_FMQ_EVENT_INIT;
  if (!adapter || !adapter->event_callback) return;
  event.kind = kind;
  event.status = status;
  event.pattern = adapter->pattern;
  event.mode = adapter->mode;
  event.transport = adapter->transport;
  if (peer) {
    if (peer->identity) event.peer_identity = tstr_to_v(peer->identity);
    if (peer->topic) event.peer_topic = tstr_to_v(peer->topic);
  }
  event.frame_bytes = (uint64_t)frame_bytes;
  adapter->event_callback(adapter->event_ctx, &event);
}

static void flow_fmq_transition(flow_fmq_adapter_t *adapter, turbo_flow_connection_state_t state,
                                int status, size_t connections_current) {
  if (!adapter) return;
  atomic_store_explicit(&adapter->connections_current, connections_current, memory_order_relaxed);
  atomic_store_explicit(&adapter->connect_status, status, memory_order_relaxed);
  atomic_store_explicit(&adapter->connection_state, state, memory_order_release);
}

static int flow_fmq_connect_endpoint_receive_ready(void *ctx) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  if (!adapter || !atomic_load_explicit(&adapter->started, memory_order_acquire)) return 0;
  return !adapter->flow || turbo_flow_state(adapter->flow) == TURBO_FLOW_STATE_STARTED;
}

static turbo_flow_connection_state_t
flow_fmq_connect_endpoint_connection_state(flowmq_connect_endpoint_connection_state_t state) {
  switch (state) {
  case FLOWMQ_ENDPOINT_CONNECTION_STOPPED:
    return TURBO_FLOW_CONNECTION_STOPPED;
  case FLOWMQ_ENDPOINT_CONNECTION_CONNECTING:
    return TURBO_FLOW_CONNECTION_CONNECTING;
  case FLOWMQ_ENDPOINT_CONNECTION_READY:
    return TURBO_FLOW_CONNECTION_READY;
  case FLOWMQ_ENDPOINT_CONNECTION_BACKOFF:
    return TURBO_FLOW_CONNECTION_BACKOFF;
  case FLOWMQ_ENDPOINT_CONNECTION_FAILED:
  default:
    return TURBO_FLOW_CONNECTION_FAILED;
  }
}

static void flow_fmq_connect_endpoint_state(void *ctx,
                                            flowmq_connect_endpoint_connection_state_t state,
                                            int status, size_t connections_current) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  if (!adapter) return;
  flow_fmq_transition(adapter, flow_fmq_connect_endpoint_connection_state(state), status,
                      connections_current);
}

static void flow_fmq_connect_endpoint_event(void *ctx,
                                            const flowmq_connect_endpoint_event_t *client_event) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  turbo_flow_fmq_event_kind_t kind;
  if (!adapter || !client_event) return;
  switch (client_event->kind) {
  case FLOWMQ_ENDPOINT_EVENT_RECONNECT_SCHEDULED:
    kind = TURBO_FLOW_FMQ_EVENT_RECONNECT_SCHEDULED;
    break;
  case FLOWMQ_ENDPOINT_EVENT_RECONNECT_SUCCEEDED:
    kind = TURBO_FLOW_FMQ_EVENT_RECONNECT_SUCCEEDED;
    break;
  case FLOWMQ_ENDPOINT_EVENT_RECONNECT_FAILED:
    kind = TURBO_FLOW_FMQ_EVENT_RECONNECT_FAILED;
    break;
  case FLOWMQ_ENDPOINT_EVENT_HEARTBEAT_TIMEOUT:
    kind = TURBO_FLOW_FMQ_EVENT_HEARTBEAT_TIMEOUT;
    break;
  default:
    return;
  }
  flow_fmq_emit_event(adapter, kind, client_event->status, NULL, client_event->peer_identity,
                      client_event->peer_topic, client_event->delay_ms);
}

static int flow_fmq_connect_endpoint_frame(void *ctx, const flowmq_protocol_frame_t *frame,
                                           uint64_t generation) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  (void)generation;
  if (!adapter || !frame) return TURBO_EINVAL;
  return flow_fmq_publish(adapter, NULL, frame);
}

static uint64_t flow_fmq_send_timeout_ns(const flow_fmq_adapter_t *adapter) {
  if (!adapter || adapter->timeouts.send_timeout_ms == 0) return 0;
  if (adapter->timeouts.send_timeout_ms > (UINT64_MAX / UINT64_C(1000000))) return UINT64_MAX;
  return adapter->timeouts.send_timeout_ms * UINT64_C(1000000);
}

static void flow_fmq_reader_destroy(flow_fmq_reader_t *reader) {
  flowmq_stream_decoder_destroy(reader);
}

static int flow_fmq_reader_consume(flow_fmq_reader_t *reader, size_t count) {
  return flowmq_stream_decoder_consume(reader, count);
}

static int flow_fmq_reader_next(coro_socket_t *socket, flow_fmq_reader_t *reader,
                                size_t max_frame_size, uint64_t receive_deadline_ns,
                                int sensitive, flow_fmq_frame_t *frame, size_t *consumed) {
  int rc;
  if (!socket || !reader || !frame || !consumed) return TURBO_EINVAL;
  rc = flowmq_stream_decoder_prepare(reader, max_frame_size);
  if (rc != TURBO_OK) return rc;
  for (;;) {
    rc = flowmq_stream_decoder_next(reader, frame, consumed);
    if (rc != FLOW_FMQ_INCOMPLETE) return rc;
    {
      char *chunk = NULL;
      size_t chunk_len = 0;
      if (receive_deadline_ns != 0u) {
        uint64_t now_ns = turbo_hrtime();
        uint64_t remaining_ns;
        uint64_t timeout_ms;
        if (receive_deadline_ns <= now_ns) return TURBO_ETIMEDOUT;
        remaining_ns = receive_deadline_ns - now_ns;
        timeout_ms = remaining_ns > UINT64_MAX - UINT64_C(999999)
                         ? UINT64_MAX / UINT64_C(1000000)
                         : (remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
        if (timeout_ms == 0u) timeout_ms = 1u;
        coro_socket_set_timeout(socket, timeout_ms);
      }
      rc = coro_socket_recv(socket, &chunk, &chunk_len);
      if (rc != TURBO_OK) return rc;
      if (!chunk || chunk_len == 0) {
        if (chunk) coro_socket_free_recv(chunk);
        return TURBO_EOF;
      }
      if (chunk_len > flowmq_stream_decoder_available(reader)) {
        if (sensitive) flowmq_security_clear(chunk, chunk_len);
        coro_socket_free_recv(chunk);
        return TURBO_EMSGSIZE;
      }
      rc = flowmq_stream_decoder_append(reader, chunk, chunk_len);
      if (sensitive) flowmq_security_clear(chunk, chunk_len);
      coro_socket_free_recv(chunk);
      if (rc != TURBO_OK) return rc;
    }
  }
}

static int flow_fmq_socket_send(flow_fmq_adapter_t *adapter, coro_socket_t *socket,
                                const char *data, size_t len) {
  if (!adapter || !socket || !data || len == 0) return TURBO_EINVAL;
  return flowmq_coronet_transport_send(socket, (flowmq_coronet_transport_t)adapter->transport,
                                       &adapter->timeouts, data, len);
}

static int flow_fmq_socket_sendv(flow_fmq_adapter_t *adapter, coro_socket_t *socket,
                                 const turbo_iovec_t *iov, size_t iovcnt) {
  if (!adapter || !socket || !iov || iovcnt == 0u) return TURBO_EINVAL;
  return flowmq_coronet_transport_sendv(socket, (flowmq_coronet_transport_t)adapter->transport,
                                        &adapter->timeouts, iov, iovcnt);
}

static int flow_fmq_send_hello(flow_fmq_adapter_t *adapter, coro_socket_t *socket) {
  tstr_t encoded = NULL;
  tstr_t security_payload = NULL;
  int rc;
  if (!adapter || !socket) return TURBO_EINVAL;
  if (adapter->security.enabled) {
    rc = flowmq_security_server_accept(&adapter->security, socket, &security_payload);
  } else {
    security_payload = tstr_new_len(NULL, 0u);
    rc = security_payload ? TURBO_OK : TURBO_ENOMEM;
  }
  if (rc == TURBO_OK) {
    rc = flowmq_pattern_encode_hello_ex(adapter->pattern, tstr_to_v(adapter->identity),
                                        tstr_to_v(adapter->topic), tstr_to_v(security_payload),
                                        adapter->max_frame_size, &encoded);
  }
  if (rc == TURBO_OK) rc = flow_fmq_socket_send(adapter, socket, encoded, tstr_len(encoded));
  tstr_freep(&security_payload);
  tstr_freep(&encoded);
  return rc;
}

static int flow_fmq_send_control(flow_fmq_adapter_t *adapter, coro_socket_t *socket,
                                 flow_fmq_frame_kind_t kind) {
  tstr_t encoded = NULL;
  int rc;
  if (!adapter || !socket || (kind != FLOW_FMQ_FRAME_PING && kind != FLOW_FMQ_FRAME_PONG)) {
    return TURBO_EINVAL;
  }
  rc = flowmq_pattern_encode_heartbeat(adapter->pattern, kind, adapter->max_frame_size, &encoded);
  if (rc == TURBO_OK) rc = flow_fmq_socket_send(adapter, socket, encoded, tstr_len(encoded));
  tstr_freep(&encoded);
  return rc;
}

static int flow_fmq_read_hello(flow_fmq_adapter_t *adapter, coro_socket_t *socket,
                               flow_fmq_reader_t *reader, flow_fmq_frame_t *hello,
                               size_t *consumed) {
  int rc = flow_fmq_reader_next(socket, reader, adapter->max_frame_size, 0u,
                                adapter->security.enabled, hello, consumed);
  if (rc != TURBO_OK) return rc;
  rc = flowmq_pattern_hello_validate(adapter->pattern, hello);
  if (rc != TURBO_OK) {
    flow_fmq_frame_cleanup(hello);
    return rc;
  }
  return TURBO_OK;
}

static int flow_fmq_add_peer(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer) {
  int rc;
  if (turbo_vec_size(&adapter->peers) >= adapter->max_connections) return TURBO_ENOBUFS;
  if (adapter->pattern == TURBO_FLOW_FMQ_PAIR && !turbo_vec_empty(&adapter->peers)) {
    return TURBO_EBUSY;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_ROUTER || adapter->fanout_enabled) {
    for (size_t i = 0; i < turbo_vec_size(&adapter->peers); ++i) {
      flow_fmq_peer_t *const *existing =
          (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
      if (existing && *existing && tstr_cmp((*existing)->identity, peer->identity) == 0) {
        return TURBO_EALREADY;
      }
    }
  }
  rc = turbo_vec_push(&adapter->peers, &peer);
  if (rc == TURBO_OK)
    atomic_fetch_add_explicit(&adapter->connections_current, 1u, memory_order_release);
  return rc;
}

static flow_fmq_peer_t *flow_fmq_find_peer(flow_fmq_adapter_t *adapter,
                                           flow_fmq_route_token_t route) {
  if (!adapter) return NULL;
  for (size_t i = 0; i < turbo_vec_size(&adapter->peers); ++i) {
    flow_fmq_peer_t *const *peer = (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
    if (peer && *peer &&
        flow_fmq_route_matches(atomic_load_explicit(&adapter->generation, memory_order_acquire),
                               route, (*peer)->route)) {
      return *peer;
    }
  }
  return NULL;
}

static int flow_fmq_routes_equal(flow_fmq_route_token_t left, flow_fmq_route_token_t right) {
  return left.session_id != 0u && left.session_id == right.session_id &&
         left.generation == right.generation;
}

static void flow_fmq_remove_peer(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer) {
  size_t count;
  if (!adapter || !peer) return;
  count = turbo_vec_size(&adapter->peers);
  for (size_t i = 0; i < count; ++i) {
    flow_fmq_peer_t **slot = (flow_fmq_peer_t **)turbo_vec_at(&adapter->peers, i);
    if (slot && *slot == peer) {
      if (i + 1u < count) {
        flow_fmq_peer_t *const *last =
            (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, count - 1u);
        *slot = last ? *last : NULL;
      }
      (void)turbo_vec_resize(&adapter->peers, count - 1u);
      atomic_fetch_sub_explicit(&adapter->connections_current, 1u, memory_order_release);
      return;
    }
  }
}

static void flow_fmq_interrupt_peers(flow_fmq_adapter_t *adapter) {
  if (!adapter) return;
  for (size_t i = 0; i < turbo_vec_size(&adapter->peers); ++i) {
    flow_fmq_peer_t *const *peer = (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
    if (peer && *peer && (*peer)->socket) {
      flow_fmq_emit_peer_disconnected(adapter, *peer, TURBO_ESHUTDOWN);
      (void)coro_socket_interrupt_wait((*peer)->socket, TURBO_ESHUTDOWN);
    }
  }
}

static void flow_fmq_peer_subscriptions_destroy(flow_fmq_peer_t *peer) {
  if (!peer) return;
  flowmq_subscription_set_destroy(&peer->subscriptions);
}

static int flow_fmq_peer_send_queue_init(flow_fmq_peer_t *peer) {
  tf_io_budget_config_t budget_config;
  if (!peer || !peer->adapter) return TURBO_EINVAL;
  if (!peer->adapter->fanout_enabled &&
      !(peer->adapter->mode == TURBO_FLOW_FMQ_BIND &&
        peer->adapter->pattern == TURBO_FLOW_FMQ_PUSH)) {
    return TURBO_OK;
  }
  if (flow_fmq_peer_send_requests_init(&peer->send_queue) != TURBO_OK) return TURBO_ENOMEM;
  peer->send_queue_initialized = 1;
  if (peer->adapter->pattern == TURBO_FLOW_FMQ_PUSH) {
    if (turbo_vec_init(&peer->send_iov, sizeof(turbo_iovec_t)) != TURBO_OK) {
      flow_fmq_peer_send_requests_destroy(&peer->send_queue);
      peer->send_queue_initialized = 0;
      return TURBO_ENOMEM;
    }
    peer->send_iov_initialized = 1;
    return TURBO_OK;
  }
  memset(&budget_config, 0, sizeof(budget_config));
  budget_config.max_messages = peer->adapter->fanout.peer_hwm_messages;
  budget_config.max_bytes = peer->adapter->fanout.peer_hwm_bytes;
  budget_config.admission = TF_IO_ADMISSION_FAIL;
  if (tf_io_budget_init(&peer->send_budget, &budget_config) != TURBO_OK) {
    flow_fmq_peer_send_requests_destroy(&peer->send_queue);
    peer->send_queue_initialized = 0;
    return TURBO_ENOMEM;
  }
  peer->send_budget_initialized = 1;
  if (tf_io_budget_open(&peer->send_budget) != TURBO_OK) {
    tf_io_budget_destroy(&peer->send_budget);
    peer->send_budget_initialized = 0;
    flow_fmq_peer_send_requests_destroy(&peer->send_queue);
    peer->send_queue_initialized = 0;
    return TURBO_EIO;
  }
  return TURBO_OK;
}

static void flow_fmq_peer_send_queue_destroy(flow_fmq_peer_t *peer) {
  if (!peer || !peer->send_queue_initialized) return;
  flow_fmq_peer_send_requests_destroy(&peer->send_queue);
  if (peer->send_iov_initialized) {
    turbo_vec_destroy(&peer->send_iov);
    peer->send_iov_initialized = 0;
  }
  if (peer->send_budget_initialized) {
    tf_io_budget_destroy(&peer->send_budget);
    peer->send_budget_initialized = 0;
  }
  peer->send_queue_initialized = 0;
}

static int flow_fmq_peer_subscription_update(flow_fmq_peer_t *peer, int subscribe, tstr_v topic,
                                             int *changed) {
  if (!peer) return TURBO_EINVAL;
  return flowmq_subscription_set_update(&peer->subscriptions, subscribe, topic, changed);
}

static int flow_fmq_message_block_size(const flow_fmq_frame_t *frame, size_t identity_size,
                                       size_t *out) {
  size_t total;
  if (!frame || !out || (frame->topic.len > 0u && !frame->topic.data) ||
      (frame->payload.len > 0u && !frame->payload.data)) {
    return TURBO_EINVAL;
  }
  total = sizeof(flow_fmq_message_context_t);
  if (frame->topic.len > SIZE_MAX - total) return TURBO_ERANGE;
  total += frame->topic.len;
  if (identity_size > SIZE_MAX - total) return TURBO_ERANGE;
  total += identity_size;
  if (frame->payload.len > SIZE_MAX - total) return TURBO_ERANGE;
  *out = total + frame->payload.len;
  return TURBO_OK;
}

static int flow_fmq_message_init(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                                 const flow_fmq_frame_t *frame, turbo_flow_msg_t *msg,
                                 flow_fmq_message_context_t **context) {
  flow_fmq_message_context_t *owned_context;
  mem_buffer_t *buffer;
  char *block;
  char *cursor;
  tstr_v peer_identity = {0};
  size_t total;
  int rc;

  if (!adapter || !frame || !msg || !context) return TURBO_EINVAL;
  if (peer && peer->identity) peer_identity = tstr_to_v(peer->identity);
  rc = flow_fmq_message_block_size(frame, peer_identity.len, &total);
  if (rc != TURBO_OK) return rc;
  buffer = mem_get_buffer(mem_global(), total);
  if (!buffer) return TURBO_ENOMEM;
  mem_set_used(buffer, total);
  block = mem_buffer_data(buffer);

  owned_context = (flow_fmq_message_context_t *)block;
  memset(owned_context, 0, sizeof(*owned_context));
  owned_context->magic = FLOW_FMQ_MESSAGE_CONTEXT_MAGIC;
  owned_context->pattern = adapter->pattern;
  owned_context->owner_instance_id = adapter->instance_id;
  owned_context->route = peer ? peer->route : (flow_fmq_route_token_t){0u, 0u};
  owned_context->correlation_id = frame->message_id;
  owned_context->correlation_generation = peer ? peer->route.generation : 0u;
  if (!peer && adapter->connect_endpoint) {
    flowmq_peer_exchange_state_t state;
    uint64_t correlation_id;
    (void)flowmq_connect_endpoint_exchange_snapshot(
        adapter->connect_endpoint, &state, &owned_context->correlation_generation, &correlation_id);
  }
  if (frame->kind == FLOW_FMQ_FRAME_SUBSCRIBE) owned_context->subscription = 1;
  if (frame->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) owned_context->subscription = -1;

  cursor = block + sizeof(*owned_context);
  if (frame->topic.len > 0u) memcpy(cursor, frame->topic.data, frame->topic.len);
  owned_context->topic = tstr_v_from_buf(cursor, frame->topic.len);
  cursor += frame->topic.len;
  if (peer_identity.len > 0u) memcpy(cursor, peer_identity.data, peer_identity.len);
  owned_context->identity = tstr_v_from_buf(cursor, peer_identity.len);
  cursor += peer_identity.len;
  if (frame->payload.len > 0u) memcpy(cursor, frame->payload.data, frame->payload.len);

  turbo_flow_msg_init(msg);
  msg->buffer = buffer;
  msg->payload = tstr_v_from_buf(cursor, frame->payload.len);
  msg->transport_context = owned_context;
  *context = owned_context;
  return TURBO_OK;
}

static int flow_fmq_publish(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                            const flow_fmq_frame_t *frame) {
  flow_fmq_message_context_t *owned_context;
  flow_fmq_message_context_t borrowed_context;
  turbo_flow_content_descriptor_t content_descriptor;
  turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  turbo_flow_msg_t msg;
  int rc;
  if (!adapter || !adapter->ingress_dispatch || !frame) return TURBO_EINVAL;
  rc = flow_fmq_message_init(adapter, peer, frame, &msg, &owned_context);
  if (rc != TURBO_OK) return rc;
  if (adapter->pattern == TURBO_FLOW_FMQ_REP) {
    borrowed_context = *owned_context;
    borrowed_context.adapter = adapter;
    borrowed_context.peer = peer;
    msg.transport_context = &borrowed_context;
  }
  if (peer && (adapter->pattern == TURBO_FLOW_FMQ_ROUTER ||
               (adapter->pattern == TURBO_FLOW_FMQ_PAIR && adapter->mode == TURBO_FLOW_FMQ_BIND))) {
    route.protocol = TURBO_FLOW_PROTOCOL_FMQ;
    route.owner_instance_id = adapter->instance_id;
    route.session_id = peer->route.session_id;
    route.session_generation = peer->route.generation;
    rc = turbo_flow_msg_set_protocol_route(&msg, &route);
    if (rc != TURBO_OK) {
      turbo_flow_msg_cleanup(&msg);
      return rc;
    }
  }
  if (frame->kind == FLOW_FMQ_FRAME_DATA && adapter->has_data_descriptor) {
    content_descriptor = adapter->data_descriptor;
    if (frame->topic.len > TURBO_FLOW_CONTENT_IDENTITY_MAX) {
      turbo_flow_msg_cleanup(&msg);
      return TURBO_ENOSPC;
    }
    if (frame->topic.len > 0u) {
      memcpy(content_descriptor.identity, frame->topic.data, frame->topic.len);
    }
    content_descriptor.identity[frame->topic.len] = '\0';
    rc = turbo_flow_msg_copy_content_descriptor(&msg, &content_descriptor);
  } else if (frame->kind == FLOW_FMQ_FRAME_SUBSCRIBE || frame->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) {
    content_descriptor = adapter->control_descriptor;
    if (frame->topic.len > TURBO_FLOW_CONTENT_IDENTITY_MAX) {
      turbo_flow_msg_cleanup(&msg);
      return TURBO_ENOSPC;
    }
    if (frame->topic.len > 0u) {
      memcpy(content_descriptor.identity, frame->topic.data, frame->topic.len);
    }
    content_descriptor.identity[frame->topic.len] = '\0';
    rc = turbo_flow_msg_copy_content_descriptor(&msg, &content_descriptor);
  } else {
    rc = TURBO_OK;
  }
  if (rc != TURBO_OK) {
    turbo_flow_msg_cleanup(&msg);
    return rc;
  }
  rc = adapter->ingress_dispatch(adapter->ingress_dispatch_ctx, &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static int flow_fmq_receive_subscription(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                                         const flow_fmq_frame_t *frame) {
  int changed;
  int rc;
  if (!adapter || adapter->pattern != TURBO_FLOW_FMQ_XPUB || !peer || !frame ||
      (frame->kind != FLOW_FMQ_FRAME_SUBSCRIBE && frame->kind != FLOW_FMQ_FRAME_UNSUBSCRIBE)) {
    return TURBO_EPROTO;
  }
  rc = flow_fmq_peer_authorize(adapter, peer, TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE, frame->topic,
                               1);
  if (rc != TURBO_OK) return TURBO_EPERM;
  rc = flow_fmq_peer_subscription_update(peer, frame->kind == FLOW_FMQ_FRAME_SUBSCRIBE,
                                         frame->topic, &changed);
  if (rc != TURBO_OK || !changed) return rc;
  if (!adapter->ingress_dispatch) return TURBO_OK;
  return flow_fmq_publish(adapter, peer, frame);
}

static void flow_fmq_publish_peer_unsubscriptions(flow_fmq_adapter_t *adapter,
                                                  flow_fmq_peer_t *peer) {
  if (!adapter || adapter->pattern != TURBO_FLOW_FMQ_XPUB || !peer ||
      !adapter->ingress_dispatch) {
    return;
  }
  for (size_t i = 0; i < flowmq_subscription_set_count(&peer->subscriptions); ++i) {
    const flowmq_subscription_t *subscription = flowmq_subscription_set_at(&peer->subscriptions, i);
    flow_fmq_frame_t frame;
    if (!subscription) continue;
    memset(&frame, 0, sizeof(frame));
    frame.kind = FLOW_FMQ_FRAME_UNSUBSCRIBE;
    frame.pattern = TURBO_FLOW_FMQ_XSUB;
    frame.topic = tstr_to_v(subscription->topic);
    for (size_t ref = 0; ref < subscription->refs; ++ref)
      (void)flow_fmq_publish(adapter, peer, &frame);
  }
}

static int flow_fmq_receive_data(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                                 flow_fmq_reader_t *reader, coro_socket_t *socket) {
  flow_fmq_heartbeat_deadlines_t heartbeat;
  uint64_t base_recv_timeout_ms;
  int heartbeat_enabled;
  if (!adapter || !reader || !socket) return TURBO_EINVAL;
  heartbeat_enabled = adapter->heartbeat_interval_ms != 0 && adapter->heartbeat_timeout_ms != 0;
  base_recv_timeout_ms = adapter->timeouts.recv_timeout_ms;
  flow_fmq_heartbeat_deadlines_init(&heartbeat, turbo_hrtime(), adapter->heartbeat_interval_ms,
                                    adapter->heartbeat_timeout_ms, base_recv_timeout_ms);
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED) {
    coro_sleep(adapter->ctx, 1);
  }
  while (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    flow_fmq_frame_t frame;
    size_t consumed = 0;
    int rc;
    uint64_t receive_deadline_ns = 0u;
    if (heartbeat_enabled) {
      for (;;) {
        uint64_t now_ns = turbo_hrtime();
        flow_fmq_heartbeat_action_t action =
            flow_fmq_heartbeat_deadlines_next(&heartbeat, now_ns, &receive_deadline_ns);
        if (action == FLOW_FMQ_HEARTBEAT_EXPIRED) {
          flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_HEARTBEAT_TIMEOUT, TURBO_ETIMEDOUT,
                              peer, (tstr_v){0}, (tstr_v){0}, 0);
          return TURBO_ETIMEDOUT;
        }
        if (action == FLOW_FMQ_HEARTBEAT_RECV_EXPIRED) return TURBO_ETIMEDOUT;
        if (action == FLOW_FMQ_HEARTBEAT_SEND_PING) {
          rc = flow_fmq_send_control(adapter, socket, FLOW_FMQ_FRAME_PING);
          if (rc != TURBO_OK) return rc;
          flow_fmq_heartbeat_deadlines_on_ping(&heartbeat, now_ns);
          continue;
        }
        break;
      }
    }
    rc = flow_fmq_reader_next(socket, reader, adapter->max_frame_size, receive_deadline_ns, 0,
                              &frame, &consumed);
    if (rc == TURBO_ETIMEDOUT && heartbeat_enabled) continue;
    if (rc != TURBO_OK) return rc;
    if (heartbeat_enabled) {
      flow_fmq_heartbeat_deadlines_on_receive(&heartbeat, turbo_hrtime());
    }
    if (!flowmq_patterns_compatible(adapter->pattern, frame.pattern)) {
      flow_fmq_frame_cleanup(&frame);
      return TURBO_EPROTO;
    }
    if (frame.kind == FLOW_FMQ_FRAME_PING) {
      rc = flow_fmq_send_control(adapter, socket, FLOW_FMQ_FRAME_PONG);
      flow_fmq_reader_consume(reader, consumed);
      flow_fmq_frame_cleanup(&frame);
      if (rc != TURBO_OK) return rc;
      continue;
    }
    if (frame.kind == FLOW_FMQ_FRAME_PONG) {
      flow_fmq_reader_consume(reader, consumed);
      flow_fmq_frame_cleanup(&frame);
      continue;
    }
    if (frame.kind == FLOW_FMQ_FRAME_SUBSCRIBE || frame.kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) {
      rc = flow_fmq_receive_subscription(adapter, peer, &frame);
      flow_fmq_reader_consume(reader, consumed);
      flow_fmq_frame_cleanup(&frame);
      if (rc != TURBO_OK) return rc;
      continue;
    }
    rc = flowmq_pattern_data_direction_validate(adapter->pattern, &frame);
    if (rc != TURBO_OK) {
      flow_fmq_frame_cleanup(&frame);
      return rc;
    }
    if (adapter->security.enabled) {
      uint32_t action = adapter->pattern == TURBO_FLOW_FMQ_REP
                            ? TURBO_FLOW_SECURITY_ACTION_EXECUTE
                            : TURBO_FLOW_SECURITY_ACTION_WRITE;
      rc = flow_fmq_peer_authorize(adapter, peer, action, frame.topic, 1);
      if (rc != TURBO_OK) {
        flow_fmq_reader_consume(reader, consumed);
        flow_fmq_frame_cleanup(&frame);
        return TURBO_EPERM;
      }
    }
    if (adapter->pattern == TURBO_FLOW_FMQ_REP) {
      uint64_t generation = 0u;
      if (!peer ||
          flowmq_peer_session_begin(&peer->exchange, FLOW_FMQ_PATTERN_PROCESSING_REQUEST,
                                    frame.message_id, &generation) != TURBO_OK ||
          generation != peer->route.generation) {
        flow_fmq_frame_cleanup(&frame);
        return TURBO_EPROTO;
      }
    }
    rc = flow_fmq_publish(adapter, peer, &frame);
    if (adapter->pattern == TURBO_FLOW_FMQ_REP) {
      if (rc == TURBO_OK && flow_fmq_exchange_state(&peer->exchange) != FLOW_FMQ_PATTERN_READY) {
        rc = TURBO_EPROTO;
      }
      if (rc != TURBO_OK) (void)flowmq_peer_session_mark_resetting(&peer->exchange);
    }
    flow_fmq_reader_consume(reader, consumed);
    flow_fmq_frame_cleanup(&frame);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_ESHUTDOWN;
}

static void flow_fmq_server_handler(coro_socket_t *client, void *arg) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg;
  flow_fmq_peer_t *peer = NULL;
  flow_fmq_frame_t hello;
  size_t consumed = 0;
  int peer_added = 0;
  int rc;
  if (!adapter) return;
  flow_fmq_lane_task_begin(adapter);
  if (!client || !atomic_load_explicit(&adapter->started, memory_order_acquire)) goto finished;
  peer = (flow_fmq_peer_t *)calloc(1, sizeof(*peer));
  if (!peer) goto finished;
  rc = flowmq_subscription_set_init(&peer->subscriptions);
  if (rc != TURBO_OK) goto done;
  peer->adapter = adapter;
  peer->socket = client;
  adapter->next_session_id += 1u;
  if (adapter->next_session_id == 0u) adapter->next_session_id += 1u;
  peer->route.session_id = adapter->next_session_id;
  peer->route.generation = atomic_load_explicit(&adapter->generation, memory_order_acquire);
  rc = flowmq_peer_session_init(&peer->exchange, peer->route.generation);
  if (rc != TURBO_OK) goto done;
  (void)tf_coronet_apply_socket_timeout(client, &adapter->timeouts, TF_CORONET_TIMEOUT_RECV);
  rc = flow_fmq_read_hello(adapter, client, &peer->reader, &hello, &consumed);
  if (rc != TURBO_OK) {
    if (adapter->security.enabled) {
      flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_AUTHENTICATION_FAILED, TURBO_EPERM, NULL,
                          (tstr_v){0}, (tstr_v){0}, 0u);
    }
    goto done;
  }
  if (adapter->security.enabled) {
    rc = flowmq_security_server_authenticate(
        &adapter->security, client, hello.identity, hello.payload, adapter->connection_uid,
        &peer->principal);
    if (rc != TURBO_OK) {
      flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_AUTHENTICATION_FAILED, TURBO_EPERM, NULL,
                          hello.identity, (tstr_v){0}, 0u);
      (void)flowmq_stream_decoder_consume_sensitive(&peer->reader, consumed);
      consumed = 0u;
      rc = TURBO_EPERM;
      goto done;
    }
    peer->authenticated = 1;
  } else {
    flowmq_protocol_security_t security;
    rc = flowmq_protocol_security_decode(hello.payload, &security);
    if (rc != TURBO_OK || security.mode != FLOWMQ_PROTOCOL_SECURITY_NONE) {
      rc = TURBO_EPERM;
      goto done;
    }
  }
  if (adapter->fanout_enabled && hello.identity.len == 0u) {
    rc = TURBO_EPROTO;
    goto done;
  }
  peer->identity = tstr_from_v(hello.identity);
  peer->topic = tstr_from_v(hello.topic);
  if (adapter->security.enabled) {
    int clear_rc = flowmq_stream_decoder_consume_sensitive(&peer->reader, consumed);
    consumed = 0u;
    if (clear_rc != TURBO_OK) {
      rc = clear_rc;
      goto done;
    }
  }
  if ((hello.identity.len > 0 && !peer->identity) || (hello.topic.len > 0 && !peer->topic)) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  if (adapter->security.enabled && adapter->pattern == TURBO_FLOW_FMQ_PUB) {
    tstr_v subscription = tstr_to_v(peer->topic);
    if (subscription.len == 0u) subscription = tstr_v_from_cstr("*");
    rc = flow_fmq_peer_authorize(adapter, peer, TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE,
                                 subscription, 1);
    if (rc != TURBO_OK) {
      rc = TURBO_EPERM;
      goto done;
    }
  }
  rc = flow_fmq_peer_send_queue_init(peer);
  if (rc != TURBO_OK) goto done;
  if (consumed != 0u) flow_fmq_reader_consume(&peer->reader, consumed);
  rc = flow_fmq_send_hello(adapter, client);
  if (rc != TURBO_OK) goto done;
  rc = flow_fmq_add_peer(adapter, peer);
  if (rc != TURBO_OK) goto done;
  peer_added = 1;
  flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_PEER_CONNECTED, TURBO_OK, peer, (tstr_v){0},
                      (tstr_v){0}, 0);
  if (adapter->pattern == TURBO_FLOW_FMQ_PUSH) flow_fmq_resume_send_drain(adapter);
  rc = flow_fmq_receive_data(adapter, peer, &peer->reader, client);
  peer->closing = 1;
  flow_fmq_publish_peer_unsubscriptions(adapter, peer);
  flow_fmq_emit_peer_disconnected(adapter, peer, rc);
  flow_fmq_remove_peer(adapter, peer);
  peer_added = 0;
done:
  if (peer && adapter->security.enabled && peer->authenticated && !peer_added && rc != TURBO_OK &&
      rc != TURBO_EPERM) {
    flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_AUTHENTICATION_FAILED, rc, peer,
                        (tstr_v){0}, (tstr_v){0}, 0u);
  }
  if (peer) {
    peer->closing = 1;
    if (peer_added) flow_fmq_remove_peer(adapter, peer);
    if (peer->socket) {
      (void)coro_socket_interrupt_wait(peer->socket, rc == TURBO_OK ? TURBO_ENOTCONN : rc);
    }
    flow_fmq_fail_peer_send_queue(peer, rc == TURBO_OK ? TURBO_ENOTCONN : rc);
    while (peer->send_drain_active)
      coro_sleep(adapter->ctx, 1);
    flow_fmq_peer_send_queue_destroy(peer);
    flow_fmq_peer_subscriptions_destroy(peer);
    if (adapter->security.enabled)
      flowmq_stream_decoder_destroy_sensitive(&peer->reader);
    else
      flow_fmq_reader_destroy(&peer->reader);
    tstr_freep(&peer->identity);
    tstr_freep(&peer->topic);
    free(peer);
  }
finished:
  flow_fmq_lane_task_end(adapter);
}

static uint64_t flow_fmq_connect_timeout_ns(const flow_fmq_adapter_t *adapter) {
  uint64_t timeout_ms;
  if (!adapter) return 0;
  timeout_ms = adapter->timeouts.connect_timeout_ms ? adapter->timeouts.connect_timeout_ms
                                                    : adapter->timeouts.timeout_ms;
  if (timeout_ms == 0) timeout_ms = FLOW_FMQ_DEFAULT_TIMEOUT_MS;
  if (timeout_ms > (UINT64_MAX / UINT64_C(1000000))) return UINT64_MAX;
  return timeout_ms * UINT64_C(1000000);
}

static int flow_fmq_start_bind(flow_fmq_adapter_t *adapter) {
  int rc;
  if (adapter->server) return TURBO_OK;
  adapter->server = flowmq_coronet_transport_create(
      adapter->ctx, (flowmq_coronet_transport_t)adapter->transport, 1);
  if (!adapter->server) return TURBO_ENOMEM;
  rc = flowmq_coronet_transport_apply(
      adapter->server, (flowmq_coronet_transport_t)adapter->transport,
      &adapter->kcp_config, adapter->kcp_configured,
      &adapter->socket_options);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
    return rc;
  }
  if (adapter->tls_configured) {
    const turbo_tls_server_config_t tls = {
        sizeof(tls),
        adapter->tls.cert_file,
        adapter->tls.key_file,
        adapter->tls.key_password,
        adapter->tls.ca_file,
        NULL,
        adapter->tls.require_client_certificate
            ? TURBO_TLS_CLIENT_AUTH_REQUIRED
            : TURBO_TLS_CLIENT_AUTH_NONE,
        NULL,
        0u};
    rc = coro_socket_set_tls_server_config(adapter->server, &tls);
    if (rc != TURBO_OK) {
      coro_socket_destroy(adapter->server);
      adapter->server = NULL;
      return rc;
    }
  }
  rc = flowmq_coronet_transport_listen(
      adapter->server, (flowmq_coronet_transport_t)adapter->transport, adapter->host, adapter->port,
      adapter->path, &adapter->timeouts, &adapter->udp_options, adapter->reuse_port,
      flow_fmq_server_handler, adapter);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
  }
  return rc;
}

static int flow_fmq_start_bind_call(void *arg) {
  return flow_fmq_start_bind((flow_fmq_adapter_t *)arg);
}

static int flow_fmq_close_resources_call(void *arg) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg;
  if (!adapter) return TURBO_EINVAL;
  if (adapter->server) {
    (void)flowmq_coronet_transport_leave_multicast(
        adapter->server, (flowmq_coronet_transport_t)adapter->transport, &adapter->udp_options);
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
  }
  flow_fmq_interrupt_peers(adapter);
  return TURBO_OK;
}

static int flow_fmq_clear_peers_call(void *arg) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg;
  if (!adapter) return TURBO_EINVAL;
  turbo_vec_clear(&adapter->peers);
  return TURBO_OK;
}

static int flow_fmq_reactor_command(void *ctx, uint64_t command_id, const void *bytes,
                                    size_t size) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  const flow_fmq_reactor_command_t *command = (const flow_fmq_reactor_command_t *)bytes;
  (void)command_id;
  if (!adapter || !command || size != sizeof(*command) || command->reserved != 0u) {
    return TURBO_EINVAL;
  }
  switch ((flow_fmq_reactor_command_kind_t)command->kind) {
  case FLOW_FMQ_REACTOR_START_BIND:
    return flow_fmq_start_bind_call(adapter);
  case FLOW_FMQ_REACTOR_CLOSE_RESOURCES:
    return flow_fmq_close_resources_call(adapter);
  case FLOW_FMQ_REACTOR_CLEAR_PEERS:
    return flow_fmq_clear_peers_call(adapter);
  default:
    return TURBO_EINVAL;
  }
}

static int flow_fmq_reactor_call(flow_fmq_adapter_t *adapter, flow_fmq_reactor_command_kind_t kind,
                                 uint64_t timeout_ns) {
  const flow_fmq_reactor_command_t command = {(uint32_t)kind, 0u};
  if (!adapter || !adapter->reactor_actor_initialized) return TURBO_EINVAL;
  return tf_coronet_actor_call(&adapter->reactor_actor, &command, sizeof(command), timeout_ns);
}

static int flow_fmq_start_resources(flow_fmq_adapter_t *adapter) {
  int rc;
  uint64_t timeout_ns;
  if (!adapter || adapter->start_refs <= 0) return TURBO_EINVAL;
  if (atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_OK;
  rc = tf_io_budget_open(&adapter->send_budget);
  if (rc != TURBO_OK) return rc;
  if (atomic_fetch_add_explicit(&adapter->generation, 1u, memory_order_acq_rel) == UINT64_MAX) {
    atomic_store_explicit(&adapter->generation, 1u, memory_order_release);
  }
  if (adapter->retry_wait_initialized) tf_timer_reset(&adapter->retry_wait);
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_ENOTCONN, 0u);
  timeout_ns = flow_fmq_connect_timeout_ns(adapter);
  if (adapter->mode == TURBO_FLOW_FMQ_BIND) {
    rc = tf_coronet_execution_start(&adapter->execution);
  } else {
    rc = TURBO_OK;
  }
  if (rc == TURBO_OK && adapter->mode == TURBO_FLOW_FMQ_BIND) {
    rc = flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_START_BIND, timeout_ns);
  } else if (rc == TURBO_OK) {
    rc = flowmq_connect_endpoint_start(adapter->connect_endpoint, timeout_ns);
  }
  if (rc == TURBO_OK && adapter->mode == TURBO_FLOW_FMQ_BIND) {
    flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_READY, TURBO_OK, 0u);
  }
  if (rc != TURBO_OK) {
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    tf_io_budget_close(&adapter->send_budget);
    flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_FAILED, rc, 0u);
    if (adapter->connect_endpoint) {
      flowmq_connect_endpoint_stop(adapter->connect_endpoint);
    } else {
      (void)flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_CLOSE_RESOURCES, timeout_ns);
    }
    flow_fmq_wait_lane_tasks(adapter);
    if (adapter->mode == TURBO_FLOW_FMQ_BIND) {
      (void)flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_CLEAR_PEERS, timeout_ns);
      tf_coronet_execution_stop(&adapter->execution);
    }
  }
  return rc;
}

static int flow_fmq_graph_ingress_dispatch(void *ctx, turbo_flow_msg_t *message) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  if (!adapter || !adapter->flow || !adapter->source_name || !message) return TURBO_EINVAL;
  return turbo_flow_publish(adapter->flow, adapter->source_name, message);
}

static int flow_fmq_start(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  int rc;
  if (!adapter || !flow || !stage) return TURBO_EINVAL;
  if (stage->is_source) {
    tstr_t source_name = tstr_dup(stage->name);
    if (!source_name) return TURBO_ENOMEM;
    if (adapter->source_name && tstr_cmp(adapter->source_name, source_name) != 0) {
      tstr_free(source_name);
      return TURBO_EALREADY;
    }
    tstr_freep(&adapter->source_name);
    adapter->source_name = source_name;
  }
  adapter->start_refs += 1;
  if (adapter->start_refs > 1) return TURBO_OK;
  adapter->flow = flow;
  adapter->ingress_dispatch = flow_fmq_graph_ingress_dispatch;
  adapter->ingress_dispatch_ctx = adapter;
  atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
  rc = flow_fmq_start_resources(adapter);
  if (rc != TURBO_OK) {
    adapter->start_refs -= 1;
    if (adapter->start_refs == 0) {
      adapter->flow = NULL;
      adapter->ingress_dispatch = NULL;
      adapter->ingress_dispatch_ctx = NULL;
    }
  }
  return rc;
}

static int flow_fmq_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  tf_io_budget_snapshot_t budget;
  const char *scheme;
  int status;
  if (!adapter || !out) return TURBO_EINVAL;
  out->state = (turbo_flow_connection_state_t)atomic_load_explicit(&adapter->connection_state,
                                                                   memory_order_acquire);
  status = atomic_load_explicit(&adapter->connect_status, memory_order_relaxed);
  out->connections_current =
      atomic_load_explicit(&adapter->connections_current, memory_order_relaxed);
  out->connection_limit = adapter->max_connections;
  if (tf_io_budget_snapshot(&adapter->send_budget, &budget) != TURBO_OK) return TURBO_EINVAL;
  out->in_flight_messages = budget.messages;
  out->in_flight_bytes = budget.bytes;
  out->last_status = status;
  if (adapter->transport == TURBO_FLOW_FMQ_PIPE) {
    if (strncmp(adapter->path, "pipe://", sizeof("pipe://") - 1u) == 0) {
      (void)snprintf(out->endpoint, sizeof(out->endpoint), "%s", adapter->path);
    } else {
      (void)snprintf(out->endpoint, sizeof(out->endpoint), "pipe://%s", adapter->path);
    }
  } else {
    switch (adapter->transport) {
    case TURBO_FLOW_FMQ_TCP:
      scheme = "tcp";
      break;
    case TURBO_FLOW_FMQ_TLS:
      scheme = "tls";
      break;
    case TURBO_FLOW_FMQ_UDP:
      scheme = "udp";
      break;
    case TURBO_FLOW_FMQ_KCP:
      scheme = "kcp";
      break;
    case TURBO_FLOW_FMQ_WS:
      scheme = "ws";
      break;
    case TURBO_FLOW_FMQ_WSS:
      scheme = "wss";
      break;
    default:
      return TURBO_EINVAL;
    }
    (void)snprintf(out->endpoint, sizeof(out->endpoint), "%s://%s:%d", scheme, adapter->host,
                   adapter->port);
  }
  return TURBO_OK;
}

static int flow_fmq_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_fmq_resource_context_t *resource = (flow_fmq_resource_context_t *)ctx;
  flow_fmq_adapter_t *adapter;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  const char *uid;
  int written;
  if (!resource || !resource->adapter || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  adapter = resource->adapter;
  metadata.kind = resource->kind;
  switch (resource->kind) {
  case TURBO_FLOW_RESOURCE_CONNECTION:
    metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    uid = adapter->connection_uid;
    break;
  case TURBO_FLOW_RESOURCE_QUEUE_BUFFER:
    metadata.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
    uid = adapter->queue_uid;
    break;
  case TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE:
    metadata.domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
    uid = adapter->protocol_uid;
    break;
  default:
    return TURBO_EINVAL;
  }
  metadata.generation = atomic_load_explicit(&adapter->generation, memory_order_acquire);
  if (metadata.generation == 0u) metadata.generation = 1u;
  metadata.observed_generation = metadata.generation;
  written = snprintf(metadata.uid, sizeof(metadata.uid), "%s", uid);
  if (written < 0 || (size_t)written >= sizeof(metadata.uid)) return TURBO_ENAMETOOLONG;
  written =
      snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", adapter->resource_owner);
  if (written < 0 || (size_t)written >= sizeof(metadata.owner_name)) return TURBO_ENAMETOOLONG;
  *out = metadata;
  return TURBO_OK;
}

static int flow_fmq_resource_snapshot(void *ctx, turbo_flow_resource_snapshot_t *out) {
  flow_fmq_resource_context_t *resource = (flow_fmq_resource_context_t *)ctx;
  flow_fmq_adapter_t *adapter;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  tf_io_budget_snapshot_t budget;
  int rc;
  if (!resource || !resource->adapter || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  adapter = resource->adapter;
  rc = flow_fmq_resource_metadata(resource, &metadata);
  if (rc != TURBO_OK) return rc;
  if (tf_io_budget_snapshot(&adapter->send_budget, &budget) != TURBO_OK) return TURBO_EINVAL;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = metadata.domain;
  out->kind = metadata.kind;
  memcpy(out->uid, metadata.uid, sizeof(out->uid));
  memcpy(out->owner_name, metadata.owner_name, sizeof(out->owner_name));
  out->generation = metadata.generation;
  out->observed_generation = metadata.observed_generation;
  if (resource->kind == TURBO_FLOW_RESOURCE_QUEUE_BUFFER) {
    if (adapter->frame_hwm_messages != 0) {
      out->load = budget.messages;
      out->capacity = adapter->frame_hwm_messages;
    } else {
      out->load = budget.bytes;
      out->capacity = adapter->frame_hwm_bytes;
    }
  } else {
    out->load = atomic_load_explicit(&adapter->connections_current, memory_order_relaxed);
    out->capacity = adapter->max_connections;
  }
  out->saturated = out->capacity != 0 && out->load >= out->capacity;
  out->last_status = atomic_load_explicit(&adapter->connect_status, memory_order_relaxed);
  return TURBO_OK;
}

static int flow_fmq_resource_document(void *ctx, turbo_flow_resource_document_kind_t document_kind,
                                      turbo_flow_resource_document_t *out) {
  flow_fmq_resource_context_t *resource = (flow_fmq_resource_context_t *)ctx;
  flow_fmq_adapter_t *adapter;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_connection_snapshot_t connection;
  tf_io_budget_snapshot_t budget;
  const turbo_flow_resource_schema_t *schema;
  tstr_t payload = NULL;
  int rc;
  if (!resource || !resource->adapter || !out || out->size < sizeof(*out) || out->payload) {
    return TURBO_EINVAL;
  }
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  adapter = resource->adapter;
  rc = flow_fmq_resource_metadata(resource, &metadata);
  if (rc != TURBO_OK) return rc;
  if (resource->kind == TURBO_FLOW_RESOURCE_CONNECTION) {
    memset(&connection, 0, sizeof(connection));
    rc = flow_fmq_connection_snapshot(adapter, &connection);
    if (rc != TURBO_OK) return rc;
    schema = &FLOW_FMQ_CONNECTION_SCHEMA;
    payload = tstr_format("{\"state\":{},\"last_status\":{},\"connections_current\":\"{}\","
                          "\"connection_limit\":\"{}\",\"in_flight_messages\":\"{}\","
                          "\"in_flight_bytes\":\"{}\"}",
                          (unsigned)connection.state, connection.last_status,
                          connection.connections_current, connection.connection_limit,
                          connection.in_flight_messages, connection.in_flight_bytes);
  } else if (resource->kind == TURBO_FLOW_RESOURCE_QUEUE_BUFFER) {
    uint64_t load;
    uint64_t capacity;
    if (tf_io_budget_snapshot(&adapter->send_budget, &budget) != TURBO_OK) return TURBO_EINVAL;
    load = adapter->frame_hwm_messages ? budget.messages : budget.bytes;
    capacity = adapter->frame_hwm_messages ? adapter->frame_hwm_messages : adapter->frame_hwm_bytes;
    schema = &FLOW_FMQ_QUEUE_SCHEMA;
    payload = tstr_format("{\"load\":\"{}\",\"capacity\":\"{}\",\"messages\":\"{}\","
                          "\"bytes\":\"{}\",\"saturated\":{},\"accepting\":{}}",
                          load, capacity, budget.messages, budget.bytes,
                          capacity != 0u && load >= capacity,
                          atomic_load_explicit(&adapter->started, memory_order_acquire) &&
                              !atomic_load_explicit(&adapter->quiesced, memory_order_acquire));
  } else if (resource->kind == TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE) {
    flow_fmq_pattern_state_t pattern_state = FLOW_FMQ_PATTERN_READY;
    if (adapter->connect_endpoint) {
      uint64_t generation;
      uint64_t correlation_id;
      (void)flowmq_connect_endpoint_exchange_snapshot(adapter->connect_endpoint, &pattern_state,
                                                      &generation, &correlation_id);
    }
    schema = &FLOW_FMQ_PROTOCOL_SCHEMA;
    payload = tstr_format("{\"pattern\":{},\"mode\":{},\"transport\":{},\"pattern_state\":{},"
                          "\"sessions\":\"{}\",\"started\":{},\"quiesced\":{}}",
                          (unsigned)adapter->pattern, (unsigned)adapter->mode,
                          (unsigned)adapter->transport, (unsigned)pattern_state,
                          atomic_load_explicit(&adapter->connections_current, memory_order_relaxed),
                          atomic_load_explicit(&adapter->started, memory_order_acquire),
                          atomic_load_explicit(&adapter->quiesced, memory_order_acquire));
  } else {
    return TURBO_EINVAL;
  }
  if (!payload) return TURBO_ENOMEM;
  rc = turbo_flow_resource_document_set_payload_copy(out, &metadata, schema, payload,
                                                     tstr_len(payload));
  tstr_free(payload);
  return rc;
}

static int flow_fmq_topic_matches(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                                  tstr_v topic) {
  if (!adapter || !peer || peer->closing) return 0;
  if (adapter->pattern == TURBO_FLOW_FMQ_XPUB) {
    return flowmq_subscription_set_match(&peer->subscriptions, topic);
  }
  {
    size_t prefix_len = peer->topic ? tstr_len(peer->topic) : 0u;
    return prefix_len == 0u ||
           (topic.len >= prefix_len && memcmp(topic.data, peer->topic, prefix_len) == 0);
  }
}

static int flow_fmq_fanout_selected(flow_fmq_adapter_t *adapter, size_t peer_index,
                                    flow_fmq_peer_t *peer, tstr_v topic) {
  const uint8_t *selected;
  if (!adapter || !peer || !adapter->security_selection_initialized ||
      peer_index >= turbo_vec_size(&adapter->security_selection) ||
      !flow_fmq_topic_matches(adapter, peer, topic)) {
    return 0;
  }
  selected = (const uint8_t *)turbo_vec_at_const(&adapter->security_selection, peer_index);
  return selected && *selected != 0u;
}

static void flow_fmq_release_send_budget(flow_fmq_adapter_t *adapter, size_t frame_bytes) {
  if (!adapter) return;
  (void)tf_io_budget_release(&adapter->send_budget, frame_bytes);
}

static int flow_fmq_reserve_send_budget(flow_fmq_adapter_t *adapter, size_t frame_bytes) {
  int rc;
  if (!adapter) return TURBO_EINVAL;
  if (adapter->frame_admission_policy == TURBO_FLOW_FMQ_FRAME_ADMISSION_DROP_OLDEST) {
    return flow_fmq_reserve_drop_oldest(adapter, frame_bytes);
  }
  rc = tf_io_budget_acquire(&adapter->send_budget, frame_bytes);
  if (rc != TURBO_OK) {
    if (rc == TURBO_ENOSPC || rc == TURBO_ETIMEDOUT) {
      flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_HWM_REACHED, rc, frame_bytes);
    }
    return rc;
  }
  return TURBO_OK;
}

static void flow_fmq_wait_send_drain(flow_fmq_adapter_t *adapter) {
  if (!adapter || adapter->frame_linger_ms == 0) return;
  (void)tf_io_budget_drain(&adapter->send_budget, adapter->frame_linger_ms);
}

static void flow_fmq_request_init_private_state(flow_fmq_send_request_t *request) {
  if (!request || !request->batch_state_shared) return;
  turbo_mutex_init(&request->mutex);
  turbo_cond_init(&request->cond);
  request->batch_state_shared = 0;
}

static int flow_fmq_request_begin_socket_send(flow_fmq_send_request_t *request) {
  turbo_mutex_t *state_mutex;
  int rc = TURBO_OK;
  if (!request) return TURBO_EINVAL;
  if (request->batch_state_shared && !request->batch)
    flow_fmq_request_init_private_state(request);
  state_mutex = request->batch_state_shared && request->batch
                    ? &request->batch->mutex
                    : &request->mutex;
  turbo_mutex_lock(state_mutex);
  if (request->cancel_requested) {
    rc = atomic_load_explicit(&request->adapter->started, memory_order_acquire) ? TURBO_ECANCELED
                                                                                : TURBO_ESHUTDOWN;
  } else if (request->delivery_stage == FLOW_FMQ_DELIVERY_NOT_SUBMITTED) {
    request->delivery_stage = FLOW_FMQ_DELIVERY_WRITE_STARTED;
  }
  turbo_mutex_unlock(state_mutex);
  return rc;
}

static void flow_fmq_request_mark_delivery(flow_fmq_send_request_t *request,
                                           flow_fmq_delivery_stage_t stage) {
  turbo_mutex_t *state_mutex;
  if (!request) return;
  if (request->batch_state_shared && !request->batch)
    flow_fmq_request_init_private_state(request);
  state_mutex = request->batch_state_shared && request->batch
                    ? &request->batch->mutex
                    : &request->mutex;
  turbo_mutex_lock(state_mutex);
  if (request->delivery_stage < stage) request->delivery_stage = stage;
  turbo_mutex_unlock(state_mutex);
}

static int flow_fmq_request_build_iov(const flow_fmq_send_request_t *request,
                                      turbo_iovec_t *stack_iov, size_t stack_capacity,
                                      turbo_iovec_t **iov, size_t *iovcnt) {
  if (!request || !request->segmented_frame.storage || !stack_iov || stack_capacity == 0u ||
      !iov || !iovcnt) {
    return TURBO_EINVAL;
  }
  *iovcnt = request->segmented_frame.segment_count;
  if (*iovcnt == 0u || *iovcnt > SIZE_MAX / sizeof(**iov)) return TURBO_ERANGE;
  *iov = stack_iov;
  if (*iovcnt > stack_capacity) {
    *iov = (turbo_iovec_t *)mem_alloc(mem_global(), *iovcnt * sizeof(**iov));
    if (!*iov) return TURBO_ENOMEM;
  }
  for (size_t i = 0u; i < *iovcnt; ++i) {
    (*iov)[i].data = (const char *)request->segmented_frame.segments[i].data;
    (*iov)[i].len = request->segmented_frame.segments[i].size;
  }
  return TURBO_OK;
}

static int flow_fmq_request_socket_send(flow_fmq_send_request_t *request, coro_socket_t *socket) {
  turbo_iovec_t stack_iov[FLOW_FMQ_STACK_IOV_CAPACITY];
  turbo_iovec_t *iov = stack_iov;
  size_t iovcnt;
  int rc = flow_fmq_request_begin_socket_send(request);
  if (rc != TURBO_OK) return rc;
  flow_fmq_profile_request_socket_begin(request);
  if (!request->segmented_frame.storage) {
    const char *data;
    size_t size;
    rc = flow_fmq_request_contiguous_frame(request, &data, &size);
    if (rc == TURBO_OK) rc = flow_fmq_socket_send(request->adapter, socket, data, size);
  } else {
    rc = flow_fmq_request_build_iov(request, stack_iov, FLOW_FMQ_STACK_IOV_CAPACITY, &iov,
                                    &iovcnt);
    if (rc != TURBO_OK) goto send_done;
    rc = flow_fmq_socket_sendv(request->adapter, socket, iov, iovcnt);
    if (iov != stack_iov) mem_free(mem_global(), iov);
  }
send_done:
  flow_fmq_profile_request_socket_end(request);
  /* A socket wait interrupted by adapter shutdown is a lifecycle result, not a
   * caller cancellation. Preserve that boundary in the public FMQ status. */
  if (rc == TURBO_ECANCELED &&
      !atomic_load_explicit(&request->adapter->started, memory_order_acquire)) {
    rc = TURBO_ESHUTDOWN;
  }
  return rc;
}

static int flow_fmq_request_connect_endpoint_send(flow_fmq_send_request_t *request) {
  turbo_iovec_t stack_iov[FLOW_FMQ_STACK_IOV_CAPACITY];
  turbo_iovec_t *iov = stack_iov;
  size_t iovcnt;
  int rc = flow_fmq_request_begin_socket_send(request);
  if (rc != TURBO_OK) return rc;
  flow_fmq_profile_request_socket_begin(request);
  if (!request->segmented_frame.storage) {
    const char *data;
    size_t size;
    rc = flow_fmq_request_contiguous_frame(request, &data, &size);
    if (rc == TURBO_OK)
      rc = flowmq_connect_endpoint_send(request->adapter->connect_endpoint, data, size);
  } else {
    rc = flow_fmq_request_build_iov(request, stack_iov, FLOW_FMQ_STACK_IOV_CAPACITY, &iov,
                                    &iovcnt);
    if (rc != TURBO_OK) goto send_done;
    rc = flowmq_connect_endpoint_sendv(request->adapter->connect_endpoint, iov, iovcnt);
    if (iov != stack_iov) mem_free(mem_global(), iov);
  }
send_done:
  flow_fmq_profile_request_socket_end(request);
  return rc;
}

static void flow_fmq_complete_fanout_peer(flow_fmq_peer_t *peer, flow_fmq_send_request_t *request,
                                          int status, turbo_flow_fmq_event_kind_t event_kind) {
  int final = 0;
  if (!peer || !request) return;
  if (peer->send_budget_initialized) {
    (void)tf_io_budget_release(&peer->send_budget, request->budget_bytes);
  }
  turbo_mutex_lock(&request->mutex);
  if (status == TURBO_OK) {
    request->fanout_delivered += 1u;
    if (request->delivery_stage < FLOW_FMQ_DELIVERY_PARTIAL) {
      request->delivery_stage = FLOW_FMQ_DELIVERY_PARTIAL;
    }
  } else {
    request->fanout_dropped += 1u;
    if (request->fanout_first_error == TURBO_OK) request->fanout_first_error = status;
  }
  if (request->fanout_pending > 0u) request->fanout_pending -= 1u;
  if (request->fanout_pending == 0u && request->fanout_active) {
    if (request->fanout_dropped == 0u) {
      request->delivery_stage = FLOW_FMQ_DELIVERY_DELIVERED;
    }
    final = 1;
  }
  turbo_mutex_unlock(&request->mutex);
  flow_fmq_emit_peer_frame_event(request->adapter, event_kind, status, peer, request->budget_bytes);
  if (final) {
    if (request->budget_reserved) {
      flow_fmq_release_send_budget(request->adapter, request->budget_bytes);
      request->budget_reserved = 0;
    }
    flow_fmq_request_release(request);
  }
}

static void flow_fmq_drop_peer_send_queue(flow_fmq_peer_t *peer, int status,
                                          turbo_flow_fmq_event_kind_t event_kind) {
  flow_fmq_send_request_t *request = NULL;
  if (!peer || !peer->send_queue_initialized) return;
  if (peer->send_budget_initialized) (void)tf_io_budget_close(&peer->send_budget);
  while (flow_fmq_peer_send_requests_pop_front(&peer->send_queue, &request)) {
    if (peer->adapter && peer->adapter->fanout_enabled) {
      flow_fmq_complete_fanout_peer(peer, request, status, event_kind);
    } else {
      flow_fmq_complete_send_request(request, status);
    }
  }
}

static void flow_fmq_fail_peer_send_queue(flow_fmq_peer_t *peer, int status) {
  flow_fmq_drop_peer_send_queue(peer, status, TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED);
}

/*
 * A PUSH peer owns one write credit. One explicit batch may occupy that credit
 * as a bounded write group, so the peer keeps a reusable iovec workspace while
 * the globally admitted requests remain the payload owners.
 */
static int flow_fmq_push_peer_send_group(flow_fmq_peer_t *peer) {
  flow_fmq_send_request_t *head = NULL;
  flow_fmq_send_request_t *tail = NULL;
  flow_fmq_send_request_t *request = NULL;
  flow_fmq_profile_batch_socket_sample_t socket_sample;
  turbo_iovec_t *iov = NULL;
  size_t available_count;
  size_t selected_count = 0u;
  size_t selected_iov_count = 0u;
  size_t selected_bytes = 0u;
  size_t chunk_byte_limit = FLOW_FMQ_PUSH_SENDV_MAX_BYTES;
  size_t iov_count = 0u;
  size_t started_count = 0u;
  int send_status = TURBO_OK;
  int rc;
  if (!peer || !peer->adapter || !peer->send_iov_initialized) return TURBO_EINVAL;
  available_count = flow_fmq_peer_send_requests_size(&peer->send_queue);
  if (available_count < 2u) return TURBO_EINVAL;
  if (peer->adapter->socket_options.send_hwm_bytes != 0u &&
      peer->adapter->socket_options.send_hwm_bytes < chunk_byte_limit) {
    chunk_byte_limit = peer->adapter->socket_options.send_hwm_bytes;
  }

  /*
   * Select a prefix only. The peer lane is the ordering authority, so requests
   * after the selected prefix remain queued until this write completes.
   */
  for (size_t i = 0u; i < available_count; ++i) {
    flow_fmq_send_request_t **slot = flow_fmq_peer_send_requests_at(&peer->send_queue, i);
    size_t request_iov_count = 0u;
    size_t request_bytes = 0u;
    if (!slot || !(request = *slot)) return TURBO_EPROTO;
    request->peer_send_started = 0;
    request->peer_send_status = TURBO_OK;
    if (peer->closing || !flow_fmq_routes_equal(request->route, peer->route)) {
      request->peer_send_status = TURBO_ENOTCONN;
      selected_count += 1u;
      continue;
    }
    if (request->segmented_frame.storage) {
      request_iov_count = request->segmented_frame.segment_count;
      for (size_t segment = 0u; segment < request_iov_count; ++segment) {
        const size_t segment_size = request->segmented_frame.segments[segment].size;
        if (segment_size > SIZE_MAX - request_bytes) {
          request->peer_send_status = TURBO_ERANGE;
          break;
        }
        request_bytes += segment_size;
      }
    } else {
      const char *data;
      rc = flow_fmq_request_contiguous_frame(request, &data, &request_bytes);
      if (rc != TURBO_OK) request->peer_send_status = rc;
      request_iov_count = 1u;
    }
    if (request->peer_send_status == TURBO_OK && request_iov_count == 0u) {
      request->peer_send_status = TURBO_ERANGE;
    }
    if (request->peer_send_status != TURBO_OK) {
      selected_count += 1u;
      continue;
    }
    if (peer->adapter->socket_options.send_hwm_bytes != 0u &&
        request_bytes > peer->adapter->socket_options.send_hwm_bytes) {
      request->peer_send_status = TURBO_ENOBUFS;
      selected_count += 1u;
      continue;
    }
    if (selected_iov_count != 0u &&
        (request_iov_count > FLOW_FMQ_PUSH_SENDV_MAX_IOV - selected_iov_count ||
         request_bytes > chunk_byte_limit - selected_bytes)) {
      break;
    }
    if (request_iov_count > SIZE_MAX - selected_iov_count ||
        request_bytes > SIZE_MAX - selected_bytes) {
      request->peer_send_status = TURBO_ERANGE;
      selected_count += 1u;
      continue;
    }
    selected_count += 1u;
    selected_iov_count += request_iov_count;
    selected_bytes += request_bytes;
    if (selected_iov_count >= FLOW_FMQ_PUSH_SENDV_MAX_IOV ||
        selected_bytes >= chunk_byte_limit) {
      break;
    }
  }

  if (selected_count == 0u) return TURBO_EPROTO;
  if (selected_iov_count > 0u) {
    rc = turbo_vec_resize(&peer->send_iov, selected_iov_count);
    if (rc == TURBO_OK) {
      iov = (turbo_iovec_t *)turbo_vec_data(&peer->send_iov);
      if (!iov) rc = TURBO_EPROTO;
    }
    if (rc != TURBO_OK) {
      send_status = rc;
      for (size_t i = 0u; i < selected_count; ++i) {
        flow_fmq_send_request_t **slot = flow_fmq_peer_send_requests_at(&peer->send_queue, i);
        if (slot && *slot && (*slot)->peer_send_status == TURBO_OK) {
          (*slot)->peer_send_status = rc;
        }
      }
    }
  }

  for (size_t i = 0u; i < selected_count; ++i) {
    if (!flow_fmq_peer_send_requests_pop_front(&peer->send_queue, &request) || !request) {
      send_status = TURBO_EPROTO;
      break;
    }
    flow_fmq_append_request(&head, &tail, request);
    if (request->peer_send_status != TURBO_OK || send_status != TURBO_OK) continue;
    rc = flow_fmq_request_begin_socket_send(request);
    if (rc != TURBO_OK) {
      request->peer_send_status = rc;
      continue;
    }
    request->peer_send_started = 1;
    started_count += 1u;
    flow_fmq_profile_request_shared_socket_begin(request);
    if (request->segmented_frame.storage) {
      for (size_t segment = 0u; segment < request->segmented_frame.segment_count; ++segment) {
        iov[iov_count].data =
            (const char *)request->segmented_frame.segments[segment].data;
        iov[iov_count].len = request->segmented_frame.segments[segment].size;
        iov_count += 1u;
      }
    } else {
      const char *data;
      size_t size;
      rc = flow_fmq_request_contiguous_frame(request, &data, &size);
      if (rc != TURBO_OK) {
        request->peer_send_started = 0;
        request->peer_send_status = rc;
        started_count -= 1u;
        flow_fmq_profile_request_shared_socket_end(request);
        continue;
      }
      iov[iov_count].data = data;
      iov[iov_count].len = size;
      iov_count += 1u;
    }
  }

  if (send_status == TURBO_OK && iov_count > 0u) {
    flow_fmq_profile_batch_socket_begin(&socket_sample);
    send_status = flow_fmq_socket_sendv(peer->adapter, peer->socket, iov, iov_count);
    flow_fmq_profile_batch_socket_end(&socket_sample, started_count, iov_count);
    if (send_status == TURBO_ECANCELED &&
        !atomic_load_explicit(&peer->adapter->started, memory_order_acquire)) {
      send_status = TURBO_ESHUTDOWN;
    }
  }
  (void)turbo_vec_resize(&peer->send_iov, 0u);
  while (head) {
    flow_fmq_send_request_t *next = head->drop_next;
    request = head;
    request->drop_next = NULL;
    if (request->peer_send_started) {
      flow_fmq_profile_request_shared_socket_end(request);
      request->peer_send_status = send_status;
      if (send_status == TURBO_OK) {
        flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
      }
    }
    flow_fmq_complete_send_request(request, request->peer_send_status);
    head = next;
  }
  return send_status;
}

static void flow_fmq_peer_send_drain_task(coro_t *co, void *arg) {
  flow_fmq_peer_t *peer = (flow_fmq_peer_t *)arg;
  flow_fmq_adapter_t *adapter;
  (void)co;
  if (!peer || !(adapter = peer->adapter)) return;
  for (;;) {
    flow_fmq_send_request_t *request = NULL;
    int rc;
    if (peer->closing || !flow_fmq_peer_send_requests_pop_front(&peer->send_queue, &request)) {
      if (peer->closing) flow_fmq_fail_peer_send_queue(peer, TURBO_ENOTCONN);
      peer->send_drain_active = 0;
      peer->push_dispatch_reserved = 0;
      if (adapter->pattern == TURBO_FLOW_FMQ_PUSH) flow_fmq_resume_send_drain(adapter);
      flow_fmq_lane_task_end(adapter);
      return;
    }
    if (adapter->pattern == TURBO_FLOW_FMQ_PUSH &&
        adapter->transport == TURBO_FLOW_FMQ_TCP) {
      (void)flow_fmq_peer_send_requests_push_front(&peer->send_queue, request);
      if (flow_fmq_peer_send_requests_size(&peer->send_queue) > 1u) {
        rc = flow_fmq_push_peer_send_group(peer);
      } else {
        (void)flow_fmq_peer_send_requests_pop_front(&peer->send_queue, &request);
        if (!flow_fmq_routes_equal(request->route, peer->route)) {
          rc = TURBO_ENOTCONN;
        } else {
          rc = flow_fmq_request_socket_send(request, peer->socket);
          if (rc == TURBO_OK) {
            flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
          }
        }
        flow_fmq_complete_send_request(request, rc);
      }
    } else if (adapter->pattern == TURBO_FLOW_FMQ_PUSH) {
      if (!flow_fmq_routes_equal(request->route, peer->route)) {
        rc = TURBO_ENOTCONN;
      } else {
        rc = flow_fmq_request_socket_send(request, peer->socket);
        if (rc == TURBO_OK) {
          flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
        }
      }
      flow_fmq_complete_send_request(request, rc);
    } else {
      flow_fmq_profile_request_owner_dequeued(request);
      rc = flow_fmq_request_socket_send(request, peer->socket);
      flow_fmq_complete_fanout_peer(peer, request, rc,
                                    rc == TURBO_OK ? TURBO_FLOW_FMQ_EVENT_FRAME_SENT
                                                   : TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED);
    }
    if (rc != TURBO_OK) {
      peer->closing = 1;
      (void)coro_socket_interrupt_wait(peer->socket, rc);
      flow_fmq_fail_peer_send_queue(peer, rc);
    }
  }
}

static int flow_fmq_schedule_peer_send_drain(flow_fmq_peer_t *peer) {
  flow_fmq_adapter_t *adapter;
  int rc;
  if (!peer || !(adapter = peer->adapter) || !peer->send_queue_initialized) {
    return TURBO_EINVAL;
  }
  if (peer->send_drain_active) return TURBO_OK;
  peer->send_drain_active = 1;
  flow_fmq_lane_task_begin(adapter);
  rc = coro_context_spawn(adapter->ctx, flow_fmq_peer_send_drain_task, peer);
  if (rc == TURBO_OK) return TURBO_OK;
  peer->send_drain_active = 0;
  flow_fmq_lane_task_end(adapter);
  return rc;
}

static int flow_fmq_reserve_peer_queue_slot(flow_fmq_peer_t *peer) {
  size_t size;
  if (!peer || !peer->send_queue_initialized) return TURBO_EINVAL;
  size = flow_fmq_peer_send_requests_size(&peer->send_queue);
  if (size == SIZE_MAX) return TURBO_ERANGE;
  return flow_fmq_peer_send_requests_reserve(&peer->send_queue, size + 1u);
}

static int flow_fmq_acquire_peer_drop_oldest(flow_fmq_peer_t *peer,
                                             flow_fmq_send_request_t *incoming) {
  flow_fmq_send_request_t *dropped = NULL;
  int rc;
  if (!peer || !incoming || !peer->send_queue_initialized) return TURBO_EINVAL;
  if (peer->adapter->fanout.peer_hwm_bytes != 0u &&
      incoming->budget_bytes > peer->adapter->fanout.peer_hwm_bytes) {
    flow_fmq_emit_peer_frame_event(peer->adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM, TURBO_ENOSPC,
                                   peer, incoming->budget_bytes);
    return TURBO_ENOSPC;
  }
  rc = tf_io_budget_acquire(&peer->send_budget, incoming->budget_bytes);
  if (rc != TURBO_ENOSPC) return rc;
  flow_fmq_emit_peer_frame_event(peer->adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM, rc, peer,
                                 incoming->budget_bytes);
  while (rc == TURBO_ENOSPC && flow_fmq_peer_send_requests_pop_front(&peer->send_queue, &dropped)) {
    flow_fmq_complete_fanout_peer(peer, dropped, TURBO_ECANCELED,
                                  TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DROPPED);
    rc = tf_io_budget_acquire(&peer->send_budget, incoming->budget_bytes);
  }
  return rc;
}

static void flow_fmq_release_peer_reservations(flow_fmq_adapter_t *adapter, tstr_v topic,
                                               size_t end_index, size_t frame_bytes) {
  size_t count;
  if (!adapter) return;
  count = turbo_vec_size(&adapter->peers);
  if (end_index > count) end_index = count;
  for (size_t i = 0; i < end_index; ++i) {
    flow_fmq_peer_t *const *slot = (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
    if (!slot || !*slot || !flow_fmq_fanout_selected(adapter, i, *slot, topic)) continue;
    (void)tf_io_budget_release(&(*slot)->send_budget, frame_bytes);
  }
}

static void flow_fmq_rollback_fanout_enqueue(flow_fmq_adapter_t *adapter,
                                             flow_fmq_send_request_t *request, tstr_v topic) {
  if (!adapter || !request) return;
  for (size_t i = 0; i < turbo_vec_size(&adapter->peers); ++i) {
    flow_fmq_peer_t *const *slot = (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
    flow_fmq_send_request_t **back;
    flow_fmq_send_request_t *removed = NULL;
    if (!slot || !*slot || !flow_fmq_fanout_selected(adapter, i, *slot, topic)) continue;
    back = flow_fmq_peer_send_requests_back(&(*slot)->send_queue);
    if (back && *back == request) {
      (void)flow_fmq_peer_send_requests_pop_back(&(*slot)->send_queue, &removed);
    }
    (void)tf_io_budget_release(&(*slot)->send_budget, request->budget_bytes);
  }
  request->fanout_active = 0;
  request->fanout_pending = 0u;
  request->fanout_delivered = 0u;
  request->fanout_dropped = 0u;
  request->fanout_first_error = TURBO_OK;
  flow_fmq_request_release(request);
}

static void flow_fmq_signal_fanout_admission(flow_fmq_send_request_t *request) {
  if (!request) return;
  turbo_mutex_lock(&request->mutex);
  request->status = TURBO_OK;
  request->done = 1;
  turbo_cond_signal(&request->cond);
  turbo_mutex_unlock(&request->mutex);
}

static int flow_fmq_peer_selection_begin(flow_fmq_adapter_t *adapter,
                                         turbo_flow_pattern_selection_t selection,
                                         turbo_flow_pattern_selection_iterator_t *iterator) {
  if (!adapter || !iterator) return TURBO_EINVAL;
  *iterator = (turbo_flow_pattern_selection_iterator_t)TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
  return turbo_flow_pattern_selection_begin(&adapter->selector, selection,
                                            turbo_vec_size(&adapter->peers), iterator);
}

static int flow_fmq_enqueue_fanout(flow_fmq_send_request_t *request, tstr_v topic) {
  flow_fmq_adapter_t *adapter;
  turbo_flow_pattern_selection_iterator_t selection = TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
  size_t peer_index;
  size_t matched = 0u;
  size_t accepted = 0u;
  int rc;
  if (!request || !(adapter = request->adapter) || !adapter->fanout_enabled) {
    return TURBO_EINVAL;
  }
  if (!adapter->security_selection_initialized) return TURBO_EPROTO;
  memset(turbo_vec_data(&adapter->security_selection), 0,
         turbo_vec_size(&adapter->security_selection));
  rc = flow_fmq_peer_selection_begin(adapter, TURBO_FLOW_PATTERN_SELECT_FAN_OUT, &selection);
  if (rc == TURBO_ENOENT) return TURBO_ENOTCONN;
  if (rc != TURBO_OK) return rc;
  while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    uint8_t *selected;
    if (!slot || !*slot || !flow_fmq_topic_matches(adapter, *slot, topic)) continue;
    if (flow_fmq_peer_authorize(adapter, *slot, TURBO_FLOW_SECURITY_ACTION_READ, topic, 1) !=
        TURBO_OK) {
      continue;
    }
    selected = (uint8_t *)turbo_vec_at(&adapter->security_selection, peer_index);
    if (!selected) return TURBO_EPROTO;
    *selected = 1u;
    matched += 1u;
    rc = flow_fmq_reserve_peer_queue_slot(*slot);
    if (rc != TURBO_OK) return rc;
  }
  if (rc != TURBO_ENOENT) return rc;
  if (matched == 0u) return TURBO_ENOTCONN;

  rc = flow_fmq_peer_selection_begin(adapter, TURBO_FLOW_PATTERN_SELECT_FAN_OUT, &selection);
  if (rc != TURBO_OK) return rc;
  while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    flow_fmq_peer_t *peer;
    if (!slot || !(peer = *slot) ||
        !flow_fmq_fanout_selected(adapter, peer_index, peer, topic))
      continue;
    rc = adapter->fanout.slow_peer_policy == TURBO_FLOW_FMQ_SLOW_PEER_DROP_OLDEST
             ? flow_fmq_acquire_peer_drop_oldest(peer, request)
             : tf_io_budget_acquire(&peer->send_budget, request->budget_bytes);
    if (rc == TURBO_OK) {
      accepted += 1u;
      continue;
    }
    if (adapter->fanout.slow_peer_policy == TURBO_FLOW_FMQ_SLOW_PEER_DISCONNECT &&
        rc == TURBO_ENOSPC) {
      flow_fmq_emit_peer_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM, rc, peer,
                                     request->budget_bytes);
      flow_fmq_emit_peer_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DROPPED, rc, peer,
                                     request->budget_bytes);
      peer->closing = 1;
      flow_fmq_emit_peer_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DISCONNECTED, rc, peer,
                                     request->budget_bytes);
      (void)coro_socket_interrupt_wait(peer->socket, TURBO_ENOSPC);
      flow_fmq_drop_peer_send_queue(peer, TURBO_ENOSPC, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DROPPED);
      continue;
    }
    if (rc == TURBO_ENOSPC) {
      flow_fmq_emit_peer_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM, rc, peer,
                                     request->budget_bytes);
    }
    flow_fmq_release_peer_reservations(adapter, topic, peer_index, request->budget_bytes);
    return rc;
  }
  if (rc != TURBO_ENOENT) return rc;
  if (accepted == 0u) return TURBO_ENOTCONN;

  request->fanout_pending = accepted;
  request->fanout_active = 1;
  request->fanout_first_error = TURBO_OK;
  atomic_fetch_add_explicit(&request->refs, 1, memory_order_relaxed);
  rc = flow_fmq_peer_selection_begin(adapter, TURBO_FLOW_PATTERN_SELECT_FAN_OUT, &selection);
  if (rc != TURBO_OK) {
    flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
    return rc;
  }
  while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    if (!slot || !*slot || !flow_fmq_fanout_selected(adapter, peer_index, *slot, topic)) continue;
    rc = flow_fmq_peer_send_requests_push_back(&(*slot)->send_queue, request);
    if (rc != TURBO_OK) {
      flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
      return rc;
    }
  }
  if (rc != TURBO_ENOENT) {
    flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
    return rc;
  }
  rc = flow_fmq_peer_selection_begin(adapter, TURBO_FLOW_PATTERN_SELECT_FAN_OUT, &selection);
  if (rc != TURBO_OK) {
    flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
    return rc;
  }
  while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    if (!slot || !*slot || !flow_fmq_fanout_selected(adapter, peer_index, *slot, topic)) continue;
    rc = flow_fmq_schedule_peer_send_drain(*slot);
    if (rc != TURBO_OK) {
      flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
      return rc;
    }
  }
  if (rc != TURBO_ENOENT) {
    flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
    return rc;
  }
  flow_fmq_signal_fanout_admission(request);
  return TURBO_OK;
}

static int flow_fmq_send_frame_now(flow_fmq_send_request_t *request) {
  flow_fmq_adapter_t *adapter = request->adapter;
  int rc;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire) && !request->drain_allowed)
    return TURBO_ESHUTDOWN;
  if (request->route.session_id != 0u) {
    flow_fmq_peer_t *peer = flow_fmq_find_peer(adapter, request->route);
    tstr_v topic;
    if (!peer) return TURBO_ENOTCONN;
    rc = flow_fmq_request_encoded_topic(request, adapter->max_frame_size, &topic);
    if (rc != TURBO_OK) return rc;
    rc = flow_fmq_peer_authorize(adapter, peer, TURBO_FLOW_SECURITY_ACTION_READ, topic, 1);
    if (rc != TURBO_OK) return TURBO_EPERM;
    rc = flow_fmq_request_socket_send(request, peer->socket);
    if (rc == TURBO_OK) flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
    return rc;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_PUB || adapter->pattern == TURBO_FLOW_FMQ_XPUB) {
    int sent = 0;
    int first_error = TURBO_OK;
    tstr_v topic;
    turbo_flow_pattern_selection_iterator_t selection = TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
    size_t peer_index;
    if (flow_fmq_request_encoded_topic(request, adapter->max_frame_size, &topic) != TURBO_OK) {
      return TURBO_EPROTO;
    }
    if (adapter->fanout_enabled) return flow_fmq_enqueue_fanout(request, topic);
    rc = turbo_flow_pattern_selection_begin(&adapter->selector, TURBO_FLOW_PATTERN_SELECT_FAN_OUT,
                                            turbo_vec_size(&adapter->peers), &selection);
    if (rc == TURBO_ENOENT) return TURBO_ENOTCONN;
    if (rc != TURBO_OK) return rc;
    while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
      flow_fmq_peer_t *const *peer =
          (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
      int peer_rc;
      if (!peer || !*peer || !flow_fmq_topic_matches(adapter, *peer, topic)) continue;
      if (flow_fmq_peer_authorize(adapter, *peer, TURBO_FLOW_SECURITY_ACTION_READ, topic, 1) !=
          TURBO_OK) {
        continue;
      }
      peer_rc = flow_fmq_request_socket_send(request, (*peer)->socket);
      if (peer_rc == TURBO_OK) {
        sent += 1;
        flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_PARTIAL);
      } else if (first_error == TURBO_OK) {
        first_error = peer_rc;
      }
      if (peer_rc == TURBO_ECANCELED) break;
    }
    if (rc != TURBO_ENOENT && rc != TURBO_OK) return rc;
    if (first_error != TURBO_OK) return first_error;
    if (sent == 0) return TURBO_ENOTCONN;
    flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
    return TURBO_OK;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_PUSH) {
    size_t count = turbo_vec_size(&adapter->peers);
    size_t index;
    flow_fmq_peer_t *const *peer = NULL;
    tstr_v topic;
    if (count == 0) return TURBO_ENOTCONN;
    rc = flow_fmq_request_encoded_topic(request, adapter->max_frame_size, &topic);
    if (rc != TURBO_OK) return rc;
    for (size_t attempt = 0u; attempt < count; ++attempt) {
      turbo_flow_pattern_selection_iterator_t selection =
          TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
      rc = turbo_flow_pattern_selection_begin(
          &adapter->selector, TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN, count, &selection);
      if (rc != TURBO_OK) return rc;
      rc = turbo_flow_pattern_selection_next(&selection, &index);
      if (rc != TURBO_OK) return rc;
      peer = (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, index);
      if (peer && *peer &&
          flow_fmq_peer_authorize(adapter, *peer, TURBO_FLOW_SECURITY_ACTION_READ, topic, 0) ==
              TURBO_OK) {
        break;
      }
      peer = NULL;
    }
    if (!peer || !*peer) return TURBO_ENOTCONN;
    rc = flow_fmq_request_socket_send(request, (*peer)->socket);
    if (rc == TURBO_OK) flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
    return rc;
  }
  if (!adapter->connect_endpoint) return TURBO_ENOTCONN;
  if (adapter->pattern == TURBO_FLOW_FMQ_XSUB) {
    rc = flow_fmq_request_connect_endpoint_send(request);
    if (rc == TURBO_OK) flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
    return rc;
  }
  rc = flow_fmq_request_connect_endpoint_send(request);
  if (rc == TURBO_OK) flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
  return rc;
}

static void flow_fmq_request_release(flow_fmq_send_request_t *request) {
  if (atomic_fetch_sub_explicit(&request->refs, 1, memory_order_acq_rel) != 1) return;
  tstr_freep(&request->frame);
  flow_fmq_segmented_frame_cleanup(&request->segmented_frame);
  mem_buffer_release(request->payload_buffer);
  if (request->completion_wait) {
    (void)coro_wait_destroy(request->completion_wait);
    request->completion_wait = NULL;
  }
  if (!request->batch_state_shared) {
    turbo_cond_destroy(&request->cond);
    turbo_mutex_destroy(&request->mutex);
  }
  if (!request->batch_storage_owned) mem_free(mem_global(), request);
}

static void flow_fmq_send_batch_release(flow_fmq_send_batch_t *batch) {
  if (!batch || atomic_fetch_sub_explicit(&batch->refs, 1, memory_order_acq_rel) != 1) return;
  if (batch->completion_wait) {
    (void)coro_wait_destroy(batch->completion_wait);
    batch->completion_wait = NULL;
  }
  turbo_cond_destroy(&batch->cond);
  turbo_mutex_destroy(&batch->mutex);
  mem_free(mem_global(), batch->iov_storage);
  mem_free(mem_global(), batch->encoded_storage);
  mem_free(mem_global(), batch->request_storage);
  mem_free(mem_global(), batch);
}

static void flow_fmq_retry_batch_wait_interrupt(void *arg1, void *arg2) {
  flow_fmq_send_batch_t *batch = (flow_fmq_send_batch_t *)arg1;
  coro_wait_t *wait = NULL;
  (void)arg2;
  if (!batch) return;
  turbo_mutex_lock(&batch->mutex);
  if (batch->completion_wait_armed) wait = batch->completion_wait;
  turbo_mutex_unlock(&batch->mutex);
  if (wait) (void)coro_wait_interrupt(wait, TURBO_EINTR);
  flow_fmq_send_batch_release(batch);
}

static void flow_fmq_send_batch_complete_n(flow_fmq_send_batch_t *batch, int status,
                                           size_t completed_count) {
  coro_wait_t *wait = NULL;
  int completed = 0;
  int wait_rc;
  if (!batch || completed_count == 0u) return;
  turbo_mutex_lock(&batch->mutex);
  if (batch->first_status == TURBO_OK && status != TURBO_OK) batch->first_status = status;
  if (batch->completed < batch->count) {
    const size_t remaining = batch->count - batch->completed;
    if (completed_count > remaining && batch->first_status == TURBO_OK) {
      batch->first_status = TURBO_EPROTO;
    }
    batch->completed += completed_count < remaining ? completed_count : remaining;
    completed = batch->completed == batch->count;
  }
  if (completed) flow_fmq_profile_batch_completed(batch);
  if (completed && batch->completion_wait_armed) wait = batch->completion_wait;
  if (completed) turbo_cond_broadcast(&batch->cond);
  turbo_mutex_unlock(&batch->mutex);
  if (wait) {
    wait_rc = coro_wait_interrupt(wait, TURBO_EINTR);
    if (wait_rc == TURBO_EALREADY) {
      atomic_fetch_add_explicit(&batch->refs, 1, memory_order_relaxed);
      if (coro_post(batch->adapter->ctx, flow_fmq_retry_batch_wait_interrupt, batch, NULL) !=
          TURBO_OK) {
        flow_fmq_send_batch_release(batch);
      }
    }
  }
  if (completed) flow_fmq_send_batch_release(batch);
}

static void flow_fmq_send_batch_complete(flow_fmq_send_batch_t *batch, int status) {
  flow_fmq_send_batch_complete_n(batch, status, 1u);
}

static void flow_fmq_retry_send_wait_interrupt(void *arg1, void *arg2) {
  flow_fmq_send_request_t *request = (flow_fmq_send_request_t *)arg1;
  coro_wait_t *wait = NULL;
  (void)arg2;
  if (!request) return;
  turbo_mutex_lock(&request->mutex);
  if (request->completion_wait_armed) wait = request->completion_wait;
  turbo_mutex_unlock(&request->mutex);
  if (wait) (void)coro_wait_interrupt(wait, TURBO_EINTR);
  flow_fmq_request_release(request);
}

static void flow_fmq_complete_send_request(flow_fmq_send_request_t *request, int status) {
  coro_wait_t *wait = NULL;
  flow_fmq_send_batch_t *batch;
  turbo_mutex_t *state_mutex;
  int wait_rc;
  if (!request) return;
  if (request->batch_state_shared && !request->batch)
    flow_fmq_request_init_private_state(request);
  state_mutex = request->batch_state_shared && request->batch
                    ? &request->batch->mutex
                    : &request->mutex;
  turbo_mutex_lock(state_mutex);
  flow_fmq_profile_request_completed(request);
  request->status = status;
  request->done = 1;
  if (request->completion_wait_armed) wait = request->completion_wait;
  turbo_mutex_unlock(state_mutex);
  /* The send result is final before observers run; budget ownership remains until observers
   * return so HWM and linger snapshots still include this completion boundary. Wake the
   * caller only after the observer has consumed the corresponding event. */
  flow_fmq_emit_frame_event(request->adapter,
                            status == TURBO_OK ? TURBO_FLOW_FMQ_EVENT_FRAME_SENT
                                               : TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED,
                            status, request->budget_bytes);
  if (!request->batch_state_shared) {
    turbo_mutex_lock(&request->mutex);
    turbo_cond_signal(&request->cond);
    turbo_mutex_unlock(&request->mutex);
  }
  if (wait) {
    wait_rc = coro_wait_interrupt(wait, TURBO_EINTR);
    if (wait_rc == TURBO_EALREADY) {
      /* A foreign thread can complete between arming and coro_wait_for() becoming active.
       * Retry on the owner lane so that boundary cannot lose the completion wake. */
      atomic_fetch_add_explicit(&request->refs, 1, memory_order_relaxed);
      if (coro_post(request->adapter->ctx, flow_fmq_retry_send_wait_interrupt, request, NULL) !=
          TURBO_OK) {
        flow_fmq_request_release(request);
      }
    }
  }
  if (request->budget_reserved) {
    flow_fmq_release_send_budget(request->adapter, request->budget_bytes);
    request->budget_reserved = 0;
  }
  batch = request->batch;
  request->batch = NULL;
  if (batch) {
    if (request->batch_index < batch->count) batch->requests[request->batch_index] = NULL;
    flow_fmq_profile_request_record_batch(request);
    /*
     * A batch waiter may release the batch immediately after the final completion.
     * Finish request-owned resources before publishing that terminal batch state.
     */
    flow_fmq_request_release(request);
    flow_fmq_send_batch_complete(batch, status);
    return;
  }
  flow_fmq_request_release(request);
}

/* The drain owns a fully collected batch, and its requests have no individual waiter. */
static void flow_fmq_complete_owned_batch_request(flow_fmq_send_request_t *request, int status) {
  if (!request) return;
  flow_fmq_profile_request_completed(request);
  request->status = status;
  request->done = 1;
  flow_fmq_emit_frame_event(request->adapter,
                            status == TURBO_OK ? TURBO_FLOW_FMQ_EVENT_FRAME_SENT
                                               : TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED,
                            status, request->budget_bytes);
  if (request->budget_reserved) {
    flow_fmq_release_send_budget(request->adapter, request->budget_bytes);
    request->budget_reserved = 0;
  }
  request->batch = NULL;
  flow_fmq_profile_request_record_batch(request);
  flow_fmq_request_release(request);
}

static void flow_fmq_append_request(flow_fmq_send_request_t **head, flow_fmq_send_request_t **tail,
                                    flow_fmq_send_request_t *request) {
  request->drop_next = NULL;
  if (*tail) {
    (*tail)->drop_next = request;
  } else {
    *head = request;
  }
  *tail = request;
}

static void flow_fmq_complete_request_chain(flow_fmq_send_request_t *head, int status) {
  while (head) {
    flow_fmq_send_request_t *request = head;
    head = head->drop_next;
    request->drop_next = NULL;
    flow_fmq_complete_send_request(request, status);
  }
}

static void flow_fmq_fail_send_queue(flow_fmq_adapter_t *adapter, int status) {
  flow_fmq_send_request_t *head = NULL;
  flow_fmq_send_request_t *tail = NULL;
  flow_fmq_send_request_t *request = NULL;
  if (!adapter || !adapter->send_queue_initialized) return;
  turbo_mutex_lock(&adapter->send_queue_mutex);
  while (flow_fmq_send_requests_pop_front(&adapter->send_queue, &request)) {
    flow_fmq_append_request(&head, &tail, request);
  }
  adapter->send_drain_active = 0;
  turbo_mutex_unlock(&adapter->send_queue_mutex);
  flow_fmq_complete_request_chain(head, status);
}

static int flow_fmq_reserve_drop_oldest(flow_fmq_adapter_t *adapter, size_t frame_bytes) {
  flow_fmq_send_request_t *head = NULL;
  flow_fmq_send_request_t *tail = NULL;
  flow_fmq_send_request_t *dropped = NULL;
  int rc;
  int reached_hwm = 0;
  if (!adapter || !adapter->send_queue_initialized) return TURBO_EINVAL;
  if (adapter->frame_hwm_bytes != 0 && frame_bytes > adapter->frame_hwm_bytes) {
    flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_HWM_REACHED, TURBO_ENOSPC, frame_bytes);
    return TURBO_ENOSPC;
  }
  turbo_mutex_lock(&adapter->send_queue_mutex);
  rc = tf_io_budget_acquire(&adapter->send_budget, frame_bytes);
  while (rc == TURBO_ENOSPC) {
    reached_hwm = 1;
    if (!flow_fmq_send_requests_pop_front(&adapter->send_queue, &dropped)) break;
    if (dropped->budget_reserved) {
      flow_fmq_release_send_budget(adapter, dropped->budget_bytes);
      dropped->budget_reserved = 0;
    }
    flow_fmq_append_request(&head, &tail, dropped);
    rc = tf_io_budget_acquire(&adapter->send_budget, frame_bytes);
  }
  turbo_mutex_unlock(&adapter->send_queue_mutex);
  if (reached_hwm) {
    flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_HWM_REACHED, TURBO_ENOSPC, frame_bytes);
  }
  flow_fmq_complete_request_chain(head, TURBO_ECANCELED);
  return rc;
}

static flow_fmq_send_batch_mode_t flow_fmq_send_batch_mode_for_adapter(
    const flow_fmq_adapter_t *adapter, size_t count) {
  if (!adapter || count < 2u || adapter->transport != TURBO_FLOW_FMQ_TCP) {
    return FLOW_FMQ_SEND_BATCH_NONE;
  }
  if (adapter->mode == TURBO_FLOW_FMQ_CONNECT && adapter->connect_endpoint &&
      adapter->pattern == TURBO_FLOW_FMQ_DEALER) {
    return FLOW_FMQ_SEND_BATCH_CONNECT;
  }
  if (adapter->mode != TURBO_FLOW_FMQ_BIND || adapter->fanout_enabled) {
    return FLOW_FMQ_SEND_BATCH_NONE;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_PUB) return FLOW_FMQ_SEND_BATCH_BIND_PUB;
  if (adapter->pattern == TURBO_FLOW_FMQ_PUSH) return FLOW_FMQ_SEND_BATCH_BIND_PUSH;
  return FLOW_FMQ_SEND_BATCH_NONE;
}

static flow_fmq_send_batch_mode_t flow_fmq_send_batch_mode(
    const flow_fmq_send_batch_t *batch) {
  return batch ? flow_fmq_send_batch_mode_for_adapter(batch->adapter, batch->count)
               : FLOW_FMQ_SEND_BATCH_NONE;
}

static size_t flow_fmq_request_iov_count(const flow_fmq_send_request_t *request) {
  if (!request) return 0u;
  return request->segmented_frame.storage ? request->segmented_frame.segment_count : 1u;
}

static int flow_fmq_request_batch_frame(const flow_fmq_send_request_t *request,
                                        const char **data, size_t *size) {
  const flow_fmq_send_batch_t *batch;
  if (!request || !data || !size || request->batch_frame_size == 0u ||
      !(batch = request->batch) || !batch->encoded_storage ||
      request->batch_frame_offset > batch->encoded_storage_used ||
      request->batch_frame_size >
          batch->encoded_storage_used - request->batch_frame_offset) {
    return TURBO_EINVAL;
  }
  *data = (const char *)batch->encoded_storage + request->batch_frame_offset;
  *size = request->batch_frame_size;
  return TURBO_OK;
}

static int flow_fmq_request_contiguous_frame(const flow_fmq_send_request_t *request,
                                             const char **data, size_t *size) {
  if (!request || !data || !size) return TURBO_EINVAL;
  if (request->batch_frame_size != 0u) return flow_fmq_request_batch_frame(request, data, size);
  if (!request->frame) return TURBO_EINVAL;
  *data = request->frame;
  *size = tstr_len(request->frame);
  return TURBO_OK;
}

static int flow_fmq_send_batch_contiguous_frame_span(const flow_fmq_send_batch_t *batch,
                                                     const char **data, size_t *size) {
  size_t first_offset;
  size_t end_offset;
  if (!batch || !data || !size || batch->count == 0u || !batch->encoded_storage ||
      !batch->requests[0] || batch->requests[0]->batch_frame_size == 0u) {
    return TURBO_EINVAL;
  }
  first_offset = batch->requests[0]->batch_frame_offset;
  end_offset = first_offset;
  for (size_t i = 0u; i < batch->count; ++i) {
    const flow_fmq_send_request_t *request = batch->requests[i];
    if (!request || request->batch != batch || request->batch_frame_size == 0u ||
        request->batch_frame_offset != end_offset || end_offset > batch->encoded_storage_used ||
        request->batch_frame_size > batch->encoded_storage_used - end_offset) {
      return TURBO_EINVAL;
    }
    end_offset += request->batch_frame_size;
  }
  *data = (const char *)batch->encoded_storage + first_offset;
  *size = end_offset - first_offset;
  return TURBO_OK;
}

static int flow_fmq_send_batch_coalesce_complete_peer_iov(flow_fmq_send_batch_t *batch,
                                                          size_t frame_count,
                                                          size_t *iovcnt) {
  const char *data;
  size_t size;
  if (!batch || !iovcnt || frame_count != batch->count || batch->iov_capacity < 1u ||
      flow_fmq_send_batch_contiguous_frame_span(batch, &data, &size) != TURBO_OK) {
    return 0;
  }
  batch->iov[0].data = data;
  batch->iov[0].len = size;
  *iovcnt = 1u;
  return 1;
}

static int flow_fmq_send_batch_prepare_iov(flow_fmq_send_batch_t *batch) {
  size_t iov_capacity = 0u;
  size_t iov_bytes;
  size_t index_bytes;
  size_t allocation_size;
  unsigned char *storage;
  if (!batch || batch->iov_storage) return TURBO_EINVAL;
  if (flow_fmq_send_batch_mode(batch) == FLOW_FMQ_SEND_BATCH_CONNECT) {
    const char *data;
    size_t size;
    if (flow_fmq_send_batch_contiguous_frame_span(batch, &data, &size) == TURBO_OK) {
      iov_capacity = 1u;
    }
  }
  if (iov_capacity == 0u) {
    for (size_t i = 0u; i < batch->count; ++i) {
      size_t request_count = flow_fmq_request_iov_count(batch->requests[i]);
      if (request_count == 0u || request_count > SIZE_MAX - iov_capacity) return TURBO_ERANGE;
      iov_capacity += request_count;
    }
  }
  if (iov_capacity > SIZE_MAX / sizeof(*batch->iov)) return TURBO_ERANGE;
  iov_bytes = iov_capacity * sizeof(*batch->iov);
  if (iov_capacity > SIZE_MAX / sizeof(*batch->iov_indices)) return TURBO_ERANGE;
  index_bytes = iov_capacity * sizeof(*batch->iov_indices);
  if (iov_bytes > SIZE_MAX - index_bytes) return TURBO_ERANGE;
  allocation_size = iov_bytes + index_bytes;
  storage = (unsigned char *)mem_alloc(mem_global(), allocation_size);
  if (!storage) return TURBO_ENOMEM;
  batch->iov_storage = storage;
  batch->iov = (turbo_iovec_t *)storage;
  batch->iov_indices = (size_t *)(storage + iov_bytes);
  batch->iov_capacity = iov_capacity;
  return TURBO_OK;
}

static int flow_fmq_send_batch_append_request_iov(flow_fmq_send_batch_t *batch,
                                                  size_t request_index, size_t *iovcnt) {
  flow_fmq_send_request_t *request;
  size_t required;
  if (!batch || !iovcnt || request_index >= batch->count ||
      !(request = batch->requests[request_index])) {
    return TURBO_EINVAL;
  }
  required = flow_fmq_request_iov_count(request);
  if (required == 0u || *iovcnt > batch->iov_capacity ||
      required > batch->iov_capacity - *iovcnt) {
    return TURBO_ERANGE;
  }
  if (request->segmented_frame.storage) {
    for (size_t i = 0u; i < required; ++i) {
      batch->iov[*iovcnt].data =
          (const char *)request->segmented_frame.segments[i].data;
      batch->iov[*iovcnt].len = request->segmented_frame.segments[i].size;
      batch->iov_indices[*iovcnt] = request_index;
      *iovcnt += 1u;
    }
  } else {
    const char *data;
    size_t size;
    if (flow_fmq_request_contiguous_frame(request, &data, &size) != TURBO_OK) {
      return TURBO_EPROTO;
    }
    batch->iov[*iovcnt].data = data;
    batch->iov[*iovcnt].len = size;
    batch->iov_indices[*iovcnt] = request_index;
    *iovcnt += 1u;
  }
  return TURBO_OK;
}

static int flow_fmq_request_encoded_topic(const flow_fmq_send_request_t *request,
                                          size_t max_frame_size, tstr_v *topic) {
  if (!request || !topic) return TURBO_EINVAL;
  if (request->segmented_frame.storage) {
    *topic = request->encoded_topic;
    return TURBO_OK;
  }
  {
    const char *data;
    size_t size;
    int rc = flow_fmq_request_contiguous_frame(request, &data, &size);
    return rc == TURBO_OK ? flow_fmq_encoded_topic(data, size, max_frame_size, topic) : rc;
  }
}

static void flow_fmq_send_connect_batch(flow_fmq_send_batch_t *batch) {
  flow_fmq_profile_batch_socket_sample_t socket_sample;
  size_t count;
  size_t iovcnt = 0u;
  int rc = TURBO_OK;
  if (!batch || !batch->adapter) return;
  count = batch->count;
  for (size_t i = 0u; i < count; ++i) {
    flow_fmq_send_request_t *request = batch->requests[i];
    if (!request || request->batch != batch || request->batch_index != i) {
      rc = TURBO_EPROTO;
      break;
    }
    rc = flow_fmq_request_begin_socket_send(request);
    if (rc != TURBO_OK) break;
    flow_fmq_profile_request_shared_socket_begin(request);
  }
  if (rc == TURBO_OK) {
    const char *data;
    size_t size;
    if (flow_fmq_send_batch_contiguous_frame_span(batch, &data, &size) == TURBO_OK) {
      if (batch->iov_capacity < 1u) {
        rc = TURBO_EPROTO;
      } else {
        batch->iov[0].data = data;
        batch->iov[0].len = size;
        iovcnt = 1u;
      }
    } else {
      for (size_t i = 0u; i < count && rc == TURBO_OK; ++i)
        rc = flow_fmq_send_batch_append_request_iov(batch, i, &iovcnt);
    }
  }
  if (rc == TURBO_OK) {
    flow_fmq_profile_batch_socket_begin(&socket_sample);
    rc = flowmq_connect_endpoint_sendv(batch->adapter->connect_endpoint, batch->iov, iovcnt);
    flow_fmq_profile_batch_socket_end(&socket_sample, count, iovcnt);
  }
  for (size_t i = 0u; i < count; ++i) {
    flow_fmq_send_request_t *request = batch->requests[i];
    if (!request) continue;
    flow_fmq_profile_request_shared_socket_end(request);
    if (rc == TURBO_OK) {
      flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
    }
    batch->requests[i] = NULL;
    flow_fmq_complete_owned_batch_request(request, rc);
  }
  flow_fmq_send_batch_complete_n(batch, rc, count);
}

static int flow_fmq_send_batch_prepare_item(flow_fmq_send_batch_t *batch, size_t index) {
  flow_fmq_send_request_t *request;
  flow_fmq_send_batch_item_t *item;
  if (!batch || index >= batch->count) return TURBO_EINVAL;
  request = batch->requests[index];
  item = &batch->items[index];
  item->target_route = (flow_fmq_route_token_t){0u, 0u};
  item->status = TURBO_OK;
  if (!request || request->batch != batch || request->batch_index != index) {
    item->status = TURBO_EPROTO;
    item->terminal = 1;
    return item->status;
  }
  if (!atomic_load_explicit(&batch->adapter->started, memory_order_acquire) &&
      !request->drain_allowed) {
    item->status = TURBO_ESHUTDOWN;
    item->terminal = 1;
  }
  return item->status;
}

static int flow_fmq_send_batch_begin_item(flow_fmq_send_batch_t *batch, size_t index) {
  flow_fmq_send_batch_item_t *item;
  flow_fmq_send_request_t *request;
  int rc;
  if (!batch || index >= batch->count) return TURBO_EINVAL;
  item = &batch->items[index];
  request = batch->requests[index];
  if (item->terminal || !request) return item->status;
  rc = flow_fmq_request_begin_socket_send(request);
  if (rc != TURBO_OK) {
    if (item->status == TURBO_OK) item->status = rc;
    item->terminal = 1;
    return rc;
  }
  if (!item->started) {
    flow_fmq_profile_request_shared_socket_begin(request);
    item->started = 1;
  }
  return TURBO_OK;
}

static void flow_fmq_send_batch_finish_items(flow_fmq_send_batch_t *batch) {
  int first_status = TURBO_OK;
  if (!batch) return;
  for (size_t i = 0u; i < batch->count; ++i) {
    flow_fmq_send_request_t *request = batch->requests[i];
    flow_fmq_send_batch_item_t *item = &batch->items[i];
    int status = item->status;
    if (!request) continue;
    if (item->started) flow_fmq_profile_request_shared_socket_end(request);
    if (status == TURBO_OK && item->delivered == 0u) status = TURBO_ENOTCONN;
    if (item->delivered != 0u) {
      flow_fmq_request_mark_delivery(
          request, status == TURBO_OK ? FLOW_FMQ_DELIVERY_DELIVERED : FLOW_FMQ_DELIVERY_PARTIAL);
    }
    if (first_status == TURBO_OK && status != TURBO_OK) first_status = status;
    batch->requests[i] = NULL;
    flow_fmq_complete_owned_batch_request(request, status);
  }
  flow_fmq_send_batch_complete_n(batch, first_status, batch->count);
}

static void flow_fmq_send_bind_pub_batch(flow_fmq_send_batch_t *batch) {
  flow_fmq_adapter_t *adapter;
  turbo_flow_pattern_selection_iterator_t selection = TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
  size_t peer_index;
  int rc;
  if (!batch || !(adapter = batch->adapter)) return;
  for (size_t i = 0u; i < batch->count; ++i) {
    flow_fmq_send_request_t *request = batch->requests[i];
    if (flow_fmq_send_batch_prepare_item(batch, i) != TURBO_OK) continue;
    if (flow_fmq_request_encoded_topic(request, adapter->max_frame_size,
                                       &batch->items[i].topic) != TURBO_OK) {
      batch->items[i].status = TURBO_EPROTO;
      batch->items[i].terminal = 1;
    }
  }
  rc = turbo_flow_pattern_selection_begin(&adapter->selector, TURBO_FLOW_PATTERN_SELECT_FAN_OUT,
                                          turbo_vec_size(&adapter->peers), &selection);
  if (rc == TURBO_ENOENT) {
    flow_fmq_send_batch_finish_items(batch);
    return;
  }
  if (rc != TURBO_OK) {
    for (size_t i = 0u; i < batch->count; ++i) {
      if (batch->items[i].status == TURBO_OK) batch->items[i].status = rc;
    }
    flow_fmq_send_batch_finish_items(batch);
    return;
  }
  while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
    flow_fmq_profile_batch_socket_sample_t socket_sample;
    flow_fmq_peer_t *const *peer =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    size_t iovcnt = 0u;
    size_t frame_count = 0u;
    int complete_peer_iov;
    int peer_rc;
    if (!peer || !*peer) continue;
    for (size_t i = 0u; i < batch->count; ++i) {
      flow_fmq_send_request_t *request = batch->requests[i];
      flow_fmq_send_batch_item_t *item = &batch->items[i];
      if (!request || item->terminal || !flow_fmq_topic_matches(adapter, *peer, item->topic)) {
        continue;
      }
      if (flow_fmq_peer_authorize(adapter, *peer, TURBO_FLOW_SECURITY_ACTION_READ, item->topic,
                                  1) != TURBO_OK) {
        continue;
      }
      if (flow_fmq_send_batch_begin_item(batch, i) != TURBO_OK) continue;
      if (flow_fmq_send_batch_append_request_iov(batch, i, &iovcnt) != TURBO_OK) {
        item->status = TURBO_EPROTO;
        item->terminal = 1;
      } else {
        frame_count += 1u;
      }
    }
    if (iovcnt == 0u) continue;
    complete_peer_iov =
        flow_fmq_send_batch_coalesce_complete_peer_iov(batch, frame_count, &iovcnt);
    flow_fmq_profile_batch_socket_begin(&socket_sample);
    peer_rc = flow_fmq_socket_sendv(adapter, (*peer)->socket, batch->iov, iovcnt);
    flow_fmq_profile_batch_socket_end(&socket_sample, frame_count, iovcnt);
    if (complete_peer_iov) {
      for (size_t i = 0u; i < batch->count; ++i) {
        flow_fmq_send_batch_item_t *item = &batch->items[i];
        if (peer_rc == TURBO_OK) {
          item->delivered += 1u;
        } else {
          if (item->status == TURBO_OK) item->status = peer_rc;
          if (peer_rc == TURBO_ECANCELED) item->terminal = 1;
        }
      }
    } else {
      for (size_t i = 0u; i < iovcnt; ++i) {
        flow_fmq_send_batch_item_t *item;
        if (i > 0u && batch->iov_indices[i] == batch->iov_indices[i - 1u]) continue;
        item = &batch->items[batch->iov_indices[i]];
        if (peer_rc == TURBO_OK) {
          item->delivered += 1u;
        } else {
          if (item->status == TURBO_OK) item->status = peer_rc;
          if (peer_rc == TURBO_ECANCELED) item->terminal = 1;
        }
      }
    }
  }
  if (rc != TURBO_ENOENT && rc != TURBO_OK) {
    for (size_t i = 0u; i < batch->count; ++i) {
      if (batch->items[i].status == TURBO_OK) batch->items[i].status = rc;
    }
  }
  flow_fmq_send_batch_finish_items(batch);
}

static int flow_fmq_select_push_peer(flow_fmq_adapter_t *adapter, tstr_v topic,
                                     size_t candidate_count, flow_fmq_peer_t **out) {
  turbo_flow_pattern_selection_iterator_t selection =
      TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
  size_t peer_index;
  int waiting_for_credit = 0;
  int rc;
  if (!adapter || !out) return TURBO_EINVAL;
  *out = NULL;
  rc = turbo_flow_pattern_selection_begin(&adapter->selector,
                                          TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN,
                                          candidate_count, &selection);
  if (rc == TURBO_ENOENT) return TURBO_ENOTCONN;
  if (rc != TURBO_OK) return rc;
  while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    flow_fmq_peer_t *peer;
    if (!slot || !(peer = *slot) || peer->closing || !peer->send_queue_initialized) continue;
    if (peer->send_drain_active && !peer->push_dispatch_reserved) {
      waiting_for_credit = 1;
      continue;
    }
    if (flow_fmq_peer_authorize(adapter, peer, TURBO_FLOW_SECURITY_ACTION_READ, topic, 0) !=
        TURBO_OK) {
      continue;
    }
    *out = peer;
    return TURBO_OK;
  }
  if (rc != TURBO_ENOENT) return rc;
  return waiting_for_credit ? FLOW_FMQ_PUSH_WAIT_CREDIT : TURBO_ENOTCONN;
}

static void flow_fmq_clear_push_dispatch_reservations(flow_fmq_adapter_t *adapter) {
  if (!adapter) return;
  for (size_t i = 0u; i < turbo_vec_size(&adapter->peers); ++i) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
    if (slot && *slot) (*slot)->push_dispatch_reserved = 0;
  }
}

static void flow_fmq_complete_push_batch_item(flow_fmq_send_batch_t *batch, size_t index,
                                              int status) {
  flow_fmq_send_request_t *request;
  if (!batch || index >= batch->count || !(request = batch->requests[index])) return;
  batch->items[index].status = status;
  batch->items[index].terminal = 1;
  flow_fmq_complete_send_request(request, status);
}

/*
 * Returns FLOW_FMQ_PUSH_WAIT_CREDIT only while every potentially eligible PULL has its
 * single local write credit occupied. The caller must then restore the whole
 * batch to the global unassigned queue without changing its order.
 */
static int flow_fmq_send_bind_push_batch(flow_fmq_send_batch_t *batch) {
  flow_fmq_adapter_t *adapter;
  size_t candidate_count;
  size_t assigned = 0u;
  int rc = TURBO_OK;
  if (!batch || !(adapter = batch->adapter)) return TURBO_EINVAL;
  candidate_count = turbo_vec_size(&adapter->peers);
  flow_fmq_clear_push_dispatch_reservations(adapter);
  for (size_t i = 0u; i < batch->count; ++i) {
    flow_fmq_send_batch_item_t *item = &batch->items[i];
    flow_fmq_send_request_t *request = batch->requests[i];
    flow_fmq_peer_t *peer = NULL;
    if (flow_fmq_send_batch_prepare_item(batch, i) != TURBO_OK) continue;
    rc = flow_fmq_request_encoded_topic(request, adapter->max_frame_size, &item->topic);
    if (rc == TURBO_OK)
      rc = flow_fmq_select_push_peer(adapter, item->topic, candidate_count, &peer);
    if (rc == FLOW_FMQ_PUSH_WAIT_CREDIT) {
      flow_fmq_clear_push_dispatch_reservations(adapter);
      return FLOW_FMQ_PUSH_WAIT_CREDIT;
    }
    if (rc != TURBO_OK) {
      item->status = rc;
      item->terminal = 1;
      continue;
    }
    peer->push_dispatch_reserved = 1;
    item->target_route = peer->route;
    request->route = peer->route;
    assigned += 1u;
  }

  for (size_t i = 0u; i < batch->count; ++i) {
    flow_fmq_send_batch_item_t *item = &batch->items[i];
    if (!item->terminal && !flow_fmq_find_peer(adapter, item->target_route)) {
      item->status = TURBO_ENOTCONN;
      item->terminal = 1;
      assigned -= 1u;
    }
  }
  if (assigned == 0u) {
    const size_t count = batch->count;
    flow_fmq_clear_push_dispatch_reservations(adapter);
    for (size_t i = 0u; i < count; ++i) {
      flow_fmq_complete_push_batch_item(batch, i, batch->items[i].status);
    }
    return TURBO_OK;
  }

  for (size_t peer_index = 0u; peer_index < turbo_vec_size(&adapter->peers); ++peer_index) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    flow_fmq_peer_t *peer;
    size_t peer_count = 0u;
    if (!slot || !(peer = *slot) || !peer->push_dispatch_reserved) continue;
    for (size_t i = 0u; i < batch->count; ++i) {
      flow_fmq_send_batch_item_t *item = &batch->items[i];
      if (!item->terminal && flow_fmq_routes_equal(item->target_route, peer->route))
        peer_count += 1u;
    }
    rc = flow_fmq_peer_send_requests_reserve(&peer->send_queue, peer_count);
    if (rc != TURBO_OK) goto fail_unsubmitted;
  }

  for (size_t i = 0u; i < batch->count; ++i) {
    flow_fmq_send_batch_item_t *item = &batch->items[i];
    flow_fmq_send_request_t *request = batch->requests[i];
    flow_fmq_peer_t *peer;
    if (!request || item->terminal) continue;
    peer = flow_fmq_find_peer(adapter, item->target_route);
    if (!peer) {
      item->status = TURBO_ENOTCONN;
      item->terminal = 1;
      continue;
    }
    rc = flow_fmq_peer_send_requests_push_back(&peer->send_queue, request);
    if (rc != TURBO_OK) goto rollback_peer_queues;
  }

  for (size_t i = 0u; i < batch->count; ++i) {
    if (batch->items[i].terminal) {
      flow_fmq_complete_push_batch_item(batch, i, batch->items[i].status);
    }
  }
  for (size_t peer_index = 0u; peer_index < turbo_vec_size(&adapter->peers); ++peer_index) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    flow_fmq_peer_t *peer;
    if (!slot || !(peer = *slot) || !peer->push_dispatch_reserved) continue;
    peer->push_dispatch_reserved = 0;
    if (flow_fmq_peer_send_requests_empty(&peer->send_queue)) continue;
    rc = flow_fmq_schedule_peer_send_drain(peer);
    if (rc != TURBO_OK) {
      flow_fmq_fail_peer_send_queue(peer, rc);
    }
  }
  return TURBO_OK;

rollback_peer_queues:
  for (size_t peer_index = 0u; peer_index < turbo_vec_size(&adapter->peers); ++peer_index) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    flow_fmq_send_request_t *removed = NULL;
    if (!slot || !*slot || !(*slot)->push_dispatch_reserved) continue;
    while (flow_fmq_peer_send_requests_pop_front(&(*slot)->send_queue, &removed)) {
      if (removed) {
        removed->route = (flow_fmq_route_token_t){0u, 0u};
      }
    }
  }
fail_unsubmitted:
  {
    const size_t count = batch->count;
  flow_fmq_clear_push_dispatch_reservations(adapter);
  for (size_t i = 0u; i < count; ++i) {
    if (batch->requests[i]) flow_fmq_complete_push_batch_item(batch, i, rc);
  }
  }
  return TURBO_OK;
}

static int flow_fmq_dispatch_push_request(flow_fmq_send_request_t *request) {
  flow_fmq_adapter_t *adapter;
  flow_fmq_peer_t *peer = NULL;
  flow_fmq_send_request_t *removed = NULL;
  tstr_v topic;
  int rc;
  if (!request || !(adapter = request->adapter)) return TURBO_EINVAL;
  rc = flow_fmq_request_encoded_topic(request, adapter->max_frame_size, &topic);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_select_push_peer(adapter, topic, turbo_vec_size(&adapter->peers), &peer);
  if (rc != TURBO_OK) return rc;
  request->route = peer->route;
  rc = flow_fmq_reserve_peer_queue_slot(peer);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_peer_send_requests_push_back(&peer->send_queue, request);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_schedule_peer_send_drain(peer);
  if (rc == TURBO_OK) return TURBO_OK;
  (void)flow_fmq_peer_send_requests_pop_back(&peer->send_queue, &removed);
  request->route = (flow_fmq_route_token_t){0u, 0u};
  return rc;
}

static int flow_fmq_requeue_push_batch(flow_fmq_send_batch_t *batch) {
  flow_fmq_adapter_t *adapter;
  size_t queue_size;
  size_t pushed = 0u;
  int rc;
  if (!batch || !(adapter = batch->adapter)) return TURBO_EINVAL;
  turbo_mutex_lock(&adapter->send_queue_mutex);
  queue_size = flow_fmq_send_requests_size(&adapter->send_queue);
  if (batch->count > SIZE_MAX - queue_size) {
    rc = TURBO_ERANGE;
  } else {
    rc = flow_fmq_send_requests_reserve(&adapter->send_queue, queue_size + batch->count);
  }
  if (rc == TURBO_OK) {
    for (size_t i = batch->count; i > 0u; --i) {
      rc = flow_fmq_send_requests_push_front(&adapter->send_queue, batch->requests[i - 1u]);
      if (rc != TURBO_OK) break;
      pushed += 1u;
    }
  }
  if (rc != TURBO_OK) {
    flow_fmq_send_request_t *removed = NULL;
    while (pushed > 0u) {
      (void)flow_fmq_send_requests_pop_front(&adapter->send_queue, &removed);
      pushed -= 1u;
    }
  }
  if (rc == TURBO_OK) adapter->send_drain_active = 0;
  turbo_mutex_unlock(&adapter->send_queue_mutex);
  return rc;
}

static int flow_fmq_requeue_push_request(flow_fmq_adapter_t *adapter,
                                         flow_fmq_send_request_t *request) {
  size_t queue_size;
  int rc;
  if (!adapter || !request) return TURBO_EINVAL;
  turbo_mutex_lock(&adapter->send_queue_mutex);
  queue_size = flow_fmq_send_requests_size(&adapter->send_queue);
  rc = queue_size == SIZE_MAX
           ? TURBO_ERANGE
           : flow_fmq_send_requests_reserve(&adapter->send_queue, queue_size + 1u);
  if (rc == TURBO_OK) rc = flow_fmq_send_requests_push_front(&adapter->send_queue, request);
  if (rc == TURBO_OK) adapter->send_drain_active = 0;
  turbo_mutex_unlock(&adapter->send_queue_mutex);
  return rc;
}

static void flow_fmq_send_drain_task(coro_t *co, void *arg) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg;
  (void)co;
  for (;;) {
    flow_fmq_send_request_t *request = NULL;
    flow_fmq_send_batch_t *batch = NULL;
    flow_fmq_send_batch_mode_t batch_mode = FLOW_FMQ_SEND_BATCH_NONE;
    turbo_mutex_lock(&adapter->send_queue_mutex);
    if (!flow_fmq_send_requests_pop_front(&adapter->send_queue, &request)) {
      adapter->send_drain_active = 0;
      turbo_mutex_unlock(&adapter->send_queue_mutex);
      flow_fmq_lane_task_end(adapter);
      return;
    }
    if (request->batch && request->batch_index == 0u &&
        (batch_mode = flow_fmq_send_batch_mode(request->batch)) != FLOW_FMQ_SEND_BATCH_NONE) {
      size_t collected = 1u;
      batch = request->batch;
      while (collected < batch->count) {
        flow_fmq_send_request_t **front = flow_fmq_send_requests_front(&adapter->send_queue);
        flow_fmq_send_request_t *next = NULL;
        if (!front || *front != batch->requests[collected] ||
            !flow_fmq_send_requests_pop_front(&adapter->send_queue, &next)) {
          break;
        }
        ++collected;
      }
      if (collected != batch->count) {
        while (collected > 1u) {
          --collected;
          (void)flow_fmq_send_requests_push_front(&adapter->send_queue,
                                                   batch->requests[collected]);
        }
        batch = NULL;
        batch_mode = FLOW_FMQ_SEND_BATCH_NONE;
      }
    }
    turbo_mutex_unlock(&adapter->send_queue_mutex);
    if (batch) {
      flow_fmq_profile_batch_owner_dequeued(batch);
      for (size_t i = 0u; i < batch->count; ++i)
        flow_fmq_profile_request_owner_dequeued(batch->requests[i]);
      if (batch_mode == FLOW_FMQ_SEND_BATCH_CONNECT) {
        flow_fmq_send_connect_batch(batch);
      } else if (batch_mode == FLOW_FMQ_SEND_BATCH_BIND_PUB) {
        flow_fmq_send_bind_pub_batch(batch);
      } else {
        int rc = flow_fmq_send_bind_push_batch(batch);
        if (rc == FLOW_FMQ_PUSH_WAIT_CREDIT) {
          rc = flow_fmq_requeue_push_batch(batch);
          if (rc == TURBO_OK) {
            flow_fmq_lane_task_end(adapter);
            return;
          }
          {
            const size_t count = batch->count;
            for (size_t i = 0u; i < count; ++i) {
              flow_fmq_complete_push_batch_item(batch, i, rc);
            }
          }
        }
      }
      continue;
    }
    flow_fmq_profile_request_owner_dequeued(request);
    {
      int rc;
      if (adapter->mode == TURBO_FLOW_FMQ_BIND &&
          adapter->pattern == TURBO_FLOW_FMQ_PUSH) {
        rc = flow_fmq_dispatch_push_request(request);
        if (rc == FLOW_FMQ_PUSH_WAIT_CREDIT) {
          rc = flow_fmq_requeue_push_request(adapter, request);
          if (rc == TURBO_OK) {
            flow_fmq_lane_task_end(adapter);
            return;
          }
        }
        if (rc == TURBO_OK) continue;
      } else {
        rc = flow_fmq_send_frame_now(request);
      }
      if (request->fanout_active) {
        flow_fmq_request_release(request);
      } else {
        flow_fmq_complete_send_request(request, rc);
      }
    }
  }
}

static void flow_fmq_resume_send_drain(flow_fmq_adapter_t *adapter) {
  int schedule = 0;
  int rc;
  if (!adapter || !adapter->send_queue_initialized) return;
  turbo_mutex_lock(&adapter->send_queue_mutex);
  if (!adapter->send_drain_active && !flow_fmq_send_requests_empty(&adapter->send_queue)) {
    adapter->send_drain_active = 1;
    flow_fmq_lane_task_begin(adapter);
    schedule = 1;
  }
  turbo_mutex_unlock(&adapter->send_queue_mutex);
  if (!schedule) return;
  rc = coro_context_spawn(adapter->ctx, flow_fmq_send_drain_task, adapter);
  if (rc == TURBO_OK) return;
  flow_fmq_fail_send_queue(adapter, rc);
  flow_fmq_lane_task_end(adapter);
}

static void flow_fmq_send_drain_post(void *arg1, void *arg2) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg1;
  int rc;
  (void)arg2;
  rc = coro_context_spawn(adapter->ctx, flow_fmq_send_drain_task, adapter);
  if (rc == TURBO_OK) return;
  flow_fmq_fail_send_queue(adapter, rc);
  flow_fmq_lane_task_end(adapter);
}

static int flow_fmq_enqueue_send(flow_fmq_adapter_t *adapter, flow_fmq_send_request_t *request) {
  int schedule_drain = 0;
  int rc;
  turbo_mutex_lock(&adapter->send_queue_mutex);
  /* Linger preserves requests that reserved budget before stop closed admission. */
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire) && !request->drain_allowed) {
    turbo_mutex_unlock(&adapter->send_queue_mutex);
    return TURBO_ESHUTDOWN;
  }
  rc = flow_fmq_send_requests_push_back(&adapter->send_queue, request);
  if (rc == TURBO_OK) flow_fmq_profile_request_enqueued(request);
  if (rc == TURBO_OK && !adapter->send_drain_active) {
    adapter->send_drain_active = 1;
    /* Lock order is send_queue_mutex -> lane_task_mutex; stop never takes the reverse order. */
    flow_fmq_lane_task_begin(adapter);
    schedule_drain = 1;
  }
  turbo_mutex_unlock(&adapter->send_queue_mutex);
  if (rc != TURBO_OK || !schedule_drain) return rc;
  flow_fmq_profile_request_post_begin(request);
  rc = coro_post(adapter->ctx, flow_fmq_send_drain_post, adapter, NULL);
  flow_fmq_profile_request_post_end(request);
  if (rc == TURBO_OK) return TURBO_OK;
  flow_fmq_fail_send_queue(adapter, rc);
  flow_fmq_lane_task_end(adapter);
  return rc;
}

static int flow_fmq_enqueue_send_batch(flow_fmq_send_batch_t *batch) {
  flow_fmq_adapter_t *adapter;
  size_t original_size;
  size_t pushed = 0u;
  int schedule_drain = 0;
  int rc;
  if (!batch || !batch->adapter || batch->count == 0u) return TURBO_EINVAL;
  flow_fmq_profile_batch_enqueue_begin(batch);
  adapter = batch->adapter;
  if (flow_fmq_send_batch_mode(batch) != FLOW_FMQ_SEND_BATCH_NONE &&
      flow_fmq_send_batch_mode(batch) != FLOW_FMQ_SEND_BATCH_BIND_PUSH) {
    rc = flow_fmq_send_batch_prepare_iov(batch);
    if (rc != TURBO_OK) return rc;
  }
  turbo_mutex_lock(&adapter->send_queue_mutex);
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    turbo_mutex_unlock(&adapter->send_queue_mutex);
    return TURBO_ESHUTDOWN;
  }
  original_size = flow_fmq_send_requests_size(&adapter->send_queue);
  if (batch->count > SIZE_MAX - original_size) {
    turbo_mutex_unlock(&adapter->send_queue_mutex);
    return TURBO_ERANGE;
  }
  rc = flow_fmq_send_requests_reserve(&adapter->send_queue, original_size + batch->count);
  if (rc != TURBO_OK) {
    turbo_mutex_unlock(&adapter->send_queue_mutex);
    return rc;
  }
  for (; pushed < batch->count; ++pushed) {
    rc = flow_fmq_send_requests_push_back(&adapter->send_queue, batch->requests[pushed]);
    if (rc != TURBO_OK) break;
    flow_fmq_profile_request_enqueued(batch->requests[pushed]);
  }
  if (rc != TURBO_OK) {
    while (pushed > 0u) {
      flow_fmq_send_request_t *removed = NULL;
      --pushed;
      (void)flow_fmq_send_requests_pop_back(&adapter->send_queue, &removed);
    }
    turbo_mutex_unlock(&adapter->send_queue_mutex);
    return rc;
  }
  flow_fmq_profile_batch_enqueued(batch);
  atomic_fetch_add_explicit(&batch->refs, 1, memory_order_relaxed);
  batch->committed = 1;
  if (!adapter->send_drain_active) {
    adapter->send_drain_active = 1;
    flow_fmq_lane_task_begin(adapter);
    schedule_drain = 1;
  }
  turbo_mutex_unlock(&adapter->send_queue_mutex);
  if (!schedule_drain) return TURBO_OK;
  flow_fmq_profile_batch_post_begin(batch);
  flow_fmq_profile_request_post_begin(batch->requests[0]);
  rc = coro_post(adapter->ctx, flow_fmq_send_drain_post, adapter, NULL);
  flow_fmq_profile_request_post_end(batch->requests[0]);
  flow_fmq_profile_batch_post_end(batch);
  if (rc == TURBO_OK) return TURBO_OK;
  flow_fmq_fail_send_queue(adapter, rc);
  flow_fmq_lane_task_end(adapter);
  return rc;
}

static void flow_fmq_discard_deferred_batch(flow_fmq_send_batch_t *batch, int status) {
  if (!batch) return;
  for (size_t i = 0u; i < batch->count; ++i) {
    flow_fmq_send_request_t *request = batch->requests[i];
    if (!request) continue;
    request->batch = NULL;
    flow_fmq_emit_frame_event(request->adapter, TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED, status,
                              request->budget_bytes);
    if (request->budget_reserved) {
      flow_fmq_release_send_budget(request->adapter, request->budget_bytes);
      request->budget_reserved = 0;
    }
    flow_fmq_request_release(request);
    batch->requests[i] = NULL;
  }
}

static uint64_t flow_fmq_send_deadline_ns(const flow_fmq_adapter_t *adapter) {
  uint64_t timeout_ns = flow_fmq_send_timeout_ns(adapter);
  uint64_t now = turbo_hrtime();
  if (timeout_ns > UINT64_MAX - now) return UINT64_MAX;
  return now + timeout_ns;
}

static int flow_fmq_wait_send_request(flow_fmq_adapter_t *adapter, flow_fmq_send_request_t *request,
                                      flow_fmq_delivery_stage_t *delivery_stage) {
  uint64_t deadline_ns = flow_fmq_send_deadline_ns(adapter);
  int rc;
  if (coro_context_current() == adapter->ctx) {
    uint64_t now = turbo_hrtime();
    uint64_t remaining_ns;
    uint64_t remaining_ms;
    int wait_rc;
    if (!request->completion_wait) return TURBO_ENOMEM;
    turbo_mutex_lock(&request->mutex);
    if (request->done) {
      rc = request->status;
      if (delivery_stage) *delivery_stage = request->delivery_stage;
      flow_fmq_profile_request_record(request, turbo_hrtime());
      turbo_mutex_unlock(&request->mutex);
      return rc;
    }
    if (deadline_ns != UINT64_MAX && now >= deadline_ns) {
      request->cancel_requested = 1;
      if (delivery_stage) *delivery_stage = request->delivery_stage;
      rc = atomic_load_explicit(&adapter->started, memory_order_acquire) ? TURBO_ETIMEDOUT
                                                                         : TURBO_ESHUTDOWN;
      turbo_mutex_unlock(&request->mutex);
      return rc;
    }
    remaining_ns = deadline_ns - now;
    remaining_ms = remaining_ns > UINT64_MAX - UINT64_C(999999)
                       ? UINT64_MAX / UINT64_C(1000000)
                       : (remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
    if (remaining_ms == 0u) remaining_ms = 1u;
    request->completion_wait_armed = 1;
    turbo_mutex_unlock(&request->mutex);

    /* Completion interrupts this wait; the timer is only the send deadline. This keeps the
     * owner lane runnable without quantizing every reply through a 1 ms polling timer. */
    wait_rc = coro_wait_for(request->completion_wait, remaining_ms);

    turbo_mutex_lock(&request->mutex);
    request->completion_wait_armed = 0;
    if (!request->done) request->cancel_requested = 1;
    if (delivery_stage) *delivery_stage = request->delivery_stage;
    if (request->done) {
      rc = request->status;
      flow_fmq_profile_request_record(request, turbo_hrtime());
    } else if (wait_rc != TURBO_OK && wait_rc != TURBO_EINTR) {
      rc = wait_rc;
    } else {
      rc = atomic_load_explicit(&adapter->started, memory_order_acquire) ? TURBO_ETIMEDOUT
                                                                         : TURBO_ESHUTDOWN;
    }
    turbo_mutex_unlock(&request->mutex);
    return rc;
  }
  turbo_mutex_lock(&request->mutex);
  while (!request->done) {
    uint64_t now = turbo_hrtime();
    if (deadline_ns != UINT64_MAX && now >= deadline_ns) break;
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&request->cond, &request->mutex);
    } else if (turbo_cond_timedwait(&request->cond, &request->mutex, deadline_ns - now) != 0) {
      break;
    }
  }
  if (!request->done) request->cancel_requested = 1;
  if (delivery_stage) *delivery_stage = request->delivery_stage;
  rc = request->done
           ? request->status
           : (atomic_load_explicit(&adapter->started, memory_order_acquire) ? TURBO_ETIMEDOUT
                                                                            : TURBO_ESHUTDOWN);
  if (request->done) flow_fmq_profile_request_record(request, turbo_hrtime());
  turbo_mutex_unlock(&request->mutex);
  return rc;
}

static int flow_fmq_wait_send_batch(flow_fmq_send_batch_t *batch) {
  uint64_t deadline_ns;
  int rc;
  if (!batch || !batch->adapter || batch->count == 0u) return TURBO_EINVAL;
  deadline_ns = flow_fmq_send_deadline_ns(batch->adapter);
  if (coro_context_current() == batch->adapter->ctx) {
    uint64_t now = turbo_hrtime();
    uint64_t remaining_ns;
    uint64_t remaining_ms;
    int wait_rc;
    if (!batch->completion_wait) return TURBO_ENOMEM;
    turbo_mutex_lock(&batch->mutex);
    if (batch->completed == batch->count) {
      rc = batch->first_status;
      turbo_mutex_unlock(&batch->mutex);
      flow_fmq_profile_batch_record(batch, turbo_hrtime());
      return rc;
    }
    if (deadline_ns != UINT64_MAX && now >= deadline_ns) {
      turbo_mutex_unlock(&batch->mutex);
      return TURBO_ETIMEDOUT;
    }
    remaining_ns = deadline_ns - now;
    remaining_ms = remaining_ns > UINT64_MAX - UINT64_C(999999)
                       ? UINT64_MAX / UINT64_C(1000000)
                       : (remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
    if (remaining_ms == 0u) remaining_ms = 1u;
    batch->completion_wait_armed = 1;
    turbo_mutex_unlock(&batch->mutex);
    wait_rc = coro_wait_for(batch->completion_wait, remaining_ms);
    turbo_mutex_lock(&batch->mutex);
    batch->completion_wait_armed = 0;
    rc = batch->completed == batch->count
             ? batch->first_status
             : (wait_rc != TURBO_OK && wait_rc != TURBO_EINTR ? wait_rc : TURBO_ETIMEDOUT);
    turbo_mutex_unlock(&batch->mutex);
    flow_fmq_profile_batch_record(batch, turbo_hrtime());
    return rc;
  }
  turbo_mutex_lock(&batch->mutex);
  while (batch->completed < batch->count) {
    uint64_t now = turbo_hrtime();
    if (deadline_ns != UINT64_MAX && now >= deadline_ns) break;
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&batch->cond, &batch->mutex);
    } else if (turbo_cond_timedwait(&batch->cond, &batch->mutex, deadline_ns - now) != TURBO_OK) {
      break;
    }
  }
  rc = batch->completed == batch->count ? batch->first_status : TURBO_ETIMEDOUT;
  turbo_mutex_unlock(&batch->mutex);
  flow_fmq_profile_batch_record(batch, turbo_hrtime());
  return rc;
}

static flow_fmq_send_batch_t *flow_fmq_send_batch_create(const void *owner, size_t capacity) {
  flow_fmq_send_batch_t *batch;
  const size_t item_allocation_size =
      sizeof(flow_fmq_send_request_t *) + sizeof(flow_fmq_send_batch_item_t);
  size_t arrays_size;
  size_t allocation_size;
  if (!owner || capacity == 0u ||
      capacity > (SIZE_MAX - sizeof(*batch)) / item_allocation_size) {
    return NULL;
  }
  arrays_size = capacity * item_allocation_size;
  allocation_size = sizeof(*batch) + arrays_size;
  batch = (flow_fmq_send_batch_t *)mem_alloc(mem_global(), allocation_size);
  if (!batch) return NULL;
  memset(batch, 0, allocation_size);
  batch->owner = owner;
  batch->capacity = capacity;
  batch->requests = (flow_fmq_send_request_t **)(batch + 1);
  batch->items = (flow_fmq_send_batch_item_t *)(batch->requests + capacity);
  batch->first_status = TURBO_OK;
  flow_fmq_profile_batch_begin(batch);
  atomic_init(&batch->refs, 1);
  turbo_mutex_init(&batch->mutex);
  turbo_cond_init(&batch->cond);
  return batch;
}

static int flow_fmq_send_batch_append(flow_fmq_send_batch_t *batch,
                                      flow_fmq_send_request_t *request) {
  if (!batch || !request || batch->count >= batch->capacity) return TURBO_EINVAL;
  if (!batch->adapter) {
    batch->adapter = request->adapter;
    if (coro_context_current() == batch->adapter->ctx) {
      batch->completion_wait = coro_wait_create(batch->adapter->ctx);
      if (!batch->completion_wait) return TURBO_ENOMEM;
    }
  } else if (batch->adapter != request->adapter) {
    return TURBO_EINVAL;
  }
  request->batch = batch;
  request->batch_index = batch->count;
  batch->requests[batch->count++] = request;
  return TURBO_OK;
}

static void flow_fmq_discard_unsubmitted_request(flow_fmq_send_request_t *request, int status) {
  if (!request) return;
  request->batch = NULL;
  flow_fmq_emit_frame_event(request->adapter, TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED, status,
                            request->budget_bytes);
  if (request->budget_reserved) {
    flow_fmq_release_send_budget(request->adapter, request->budget_bytes);
    request->budget_reserved = 0;
  }
  flow_fmq_request_release(request);
  flow_fmq_request_release(request);
}

static int flow_fmq_send_batch_claim_request_storage(flow_fmq_send_batch_t *batch,
                                                     flow_fmq_send_request_t **out) {
  if (!batch || !out) return TURBO_EINVAL;
  *out = NULL;
  if (batch->request_storage_used >= batch->capacity) return TURBO_EPROTO;
  if (!batch->request_storage) {
    if (batch->capacity > SIZE_MAX / sizeof(*batch->request_storage)) return TURBO_ERANGE;
    batch->request_storage = (flow_fmq_send_request_t *)mem_alloc(
        mem_global(), batch->capacity * sizeof(*batch->request_storage));
    if (!batch->request_storage) return TURBO_ENOMEM;
  }
  *out = &batch->request_storage[batch->request_storage_used++];
  return TURBO_OK;
}

static int flow_fmq_send_batch_reserve_encoded(flow_fmq_send_batch_t *batch,
                                               size_t additional_size) {
  unsigned char *storage;
  size_t required;
  size_t new_capacity;
  if (!batch || additional_size == 0u) return TURBO_EINVAL;
  if (additional_size > SIZE_MAX - batch->encoded_storage_used) return TURBO_ERANGE;
  required = batch->encoded_storage_used + additional_size;
  if (required <= batch->encoded_storage_capacity) return TURBO_OK;
  new_capacity = batch->encoded_storage_capacity;
  if (new_capacity == 0u) {
    size_t reserve_items = batch->capacity < FLOW_FMQ_BATCH_ENCODED_RESERVE_ITEMS
                               ? batch->capacity
                               : FLOW_FMQ_BATCH_ENCODED_RESERVE_ITEMS;
    new_capacity = additional_size;
    if (additional_size <= FLOW_FMQ_BATCH_ENCODED_PREALLOC_MAX_FRAME_SIZE &&
        reserve_items > 1u && additional_size <= SIZE_MAX / reserve_items) {
      new_capacity = additional_size * reserve_items;
    }
  }
  while (new_capacity < required) {
    if (new_capacity > SIZE_MAX / 2u) {
      new_capacity = required;
      break;
    }
    new_capacity *= 2u;
  }
  storage = (unsigned char *)mem_alloc(mem_global(), new_capacity);
  if (!storage) return TURBO_ENOMEM;
  if (batch->encoded_storage_used > 0u) {
    memcpy(storage, batch->encoded_storage, batch->encoded_storage_used);
  }
  mem_free(mem_global(), batch->encoded_storage);
  batch->encoded_storage = storage;
  batch->encoded_storage_capacity = new_capacity;
  return TURBO_OK;
}

static int flow_fmq_send_batch_encode_frame(flow_fmq_send_batch_t *batch,
                                            const flow_fmq_frame_t *frame,
                                            size_t max_frame_size, size_t encoded_size,
                                            size_t *offset) {
  size_t written = 0u;
  int rc;
  if (!batch || !frame || !offset || encoded_size == 0u) return TURBO_EINVAL;
  rc = flow_fmq_send_batch_reserve_encoded(batch, encoded_size);
  if (rc != TURBO_OK) return rc;
  *offset = batch->encoded_storage_used;
  rc = flowmq_protocol_encode_frame_into_internal(
      frame, max_frame_size, batch->encoded_storage + *offset, encoded_size, &written);
  if (rc != TURBO_OK) return rc;
  if (written != encoded_size) return TURBO_EPROTO;
  batch->encoded_storage_used += written;
  return TURBO_OK;
}

static int flow_fmq_submit_send(flow_fmq_adapter_t *adapter, flow_fmq_route_token_t route,
                                tstr_t frame, flow_fmq_segmented_frame_t *segmented_frame,
                                mem_buffer_t *payload_buffer, tstr_v encoded_topic,
                                size_t encoded_size, int defer_enqueue,
                                flow_fmq_send_batch_t *batch,
                                flow_fmq_send_request_t **out_request) {
  flow_fmq_send_request_t *request;
  flow_fmq_send_batch_mode_t batch_mode;
  int batch_storage_owned = 0;
  int rc;
  if (!out_request) return TURBO_EINVAL;
  *out_request = NULL;
  batch_mode = batch ? flow_fmq_send_batch_mode_for_adapter(adapter, batch->capacity)
                     : FLOW_FMQ_SEND_BATCH_NONE;
  if (batch_mode != FLOW_FMQ_SEND_BATCH_NONE) {
    rc = flow_fmq_send_batch_claim_request_storage(batch, &request);
    batch_storage_owned = rc == TURBO_OK;
  } else {
    request = (flow_fmq_send_request_t *)mem_alloc(mem_global(), sizeof(*request));
    rc = request ? TURBO_OK : TURBO_ENOMEM;
  }
  if (rc != TURBO_OK) {
    flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED, rc,
                              encoded_size);
    flow_fmq_release_send_budget(adapter, encoded_size);
    tstr_free(frame);
    flow_fmq_segmented_frame_cleanup(segmented_frame);
    mem_buffer_release(payload_buffer);
    return rc;
  }
  memset(request, 0, sizeof(*request));
  request->adapter = adapter;
  request->route = route;
  request->frame = frame;
  request->segmented_frame = segmented_frame
                                 ? *segmented_frame
                                 : (flow_fmq_segmented_frame_t)
                                       FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
  if (segmented_frame) {
    *segmented_frame =
        (flow_fmq_segmented_frame_t)FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
  }
  request->payload_buffer = payload_buffer;
  request->encoded_topic = encoded_topic;
  request->status = TURBO_EALREADY;
  request->delivery_stage = FLOW_FMQ_DELIVERY_NOT_SUBMITTED;
  request->budget_bytes = encoded_size;
  request->budget_reserved = 1;
  request->drain_allowed = adapter->frame_linger_ms != 0;
  request->batch = batch;
  request->batch_state_shared = batch_mode != FLOW_FMQ_SEND_BATCH_NONE;
  request->batch_storage_owned = batch_storage_owned;
  atomic_init(&request->refs, 2);
  if (!request->batch_state_shared) {
    turbo_mutex_init(&request->mutex);
    turbo_cond_init(&request->cond);
  }
  if (!defer_enqueue && coro_context_current() == adapter->ctx) {
    request->completion_wait = coro_wait_create(adapter->ctx);
    if (!request->completion_wait) {
      flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED, TURBO_ENOMEM,
                                request->budget_bytes);
      flow_fmq_release_send_budget(adapter, request->budget_bytes);
      request->budget_reserved = 0;
      flow_fmq_request_release(request);
      flow_fmq_request_release(request);
      return TURBO_ENOMEM;
    }
  }
  flow_fmq_profile_request_begin(request);
  if (defer_enqueue) {
    *out_request = request;
    return TURBO_OK;
  }
  rc = flow_fmq_enqueue_send(adapter, request);
  if (rc != TURBO_OK) {
    int completed;
    turbo_mutex_lock(&request->mutex);
    completed = request->done;
    if (completed) rc = request->status;
    turbo_mutex_unlock(&request->mutex);
    if (!completed) {
      flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED, rc,
                                request->budget_bytes);
      flow_fmq_release_send_budget(adapter, request->budget_bytes);
      request->budget_reserved = 0;
      flow_fmq_request_release(request);
    }
    flow_fmq_request_release(request);
    return rc;
  }
  *out_request = request;
  return TURBO_OK;
}

static int flow_fmq_select_metadata(flow_fmq_adapter_t *adapter, const turbo_flow_msg_t *msg,
                                    turbo_flow_fmq_metadata_policy_t policy, int identity,
                                    tstr_v *out) {
  flow_fmq_message_context_t *message_context;
  if (!adapter || !out) return TURBO_EINVAL;
  if (policy == TURBO_FLOW_FMQ_METADATA_STATIC) {
    *out = identity ? tstr_to_v(adapter->identity) : tstr_to_v(adapter->topic);
    return TURBO_OK;
  }
  if (policy == TURBO_FLOW_FMQ_METADATA_CONTENT) {
    const turbo_flow_content_descriptor_t *descriptor;
    if (!msg || !(descriptor = turbo_flow_msg_content_descriptor(msg))) return TURBO_ENOENT;
    *out = tstr_v_from_buf(descriptor->identity, strlen(descriptor->identity));
    return TURBO_OK;
  }
  if (policy != TURBO_FLOW_FMQ_METADATA_INHERIT || !msg) return TURBO_EINVAL;
  message_context = (flow_fmq_message_context_t *)msg->transport_context;
  if (!message_context || message_context->magic != FLOW_FMQ_MESSAGE_CONTEXT_MAGIC) {
    return TURBO_ENOENT;
  }
  *out = identity ? message_context->identity : message_context->topic;
  return TURBO_OK;
}

static int flow_fmq_payload_within_buffer(const turbo_flow_msg_t *msg) {
  uintptr_t payload_address;
  uintptr_t buffer_address;
  size_t offset;
  size_t used;
  if (!msg) return 0;
  if (msg->payload.len == 0u) return 1;
  if (!msg->buffer || !msg->payload.data) return 0;
  payload_address = (uintptr_t)msg->payload.data;
  buffer_address = (uintptr_t)mem_buffer_const_data(msg->buffer);
  if (payload_address < buffer_address) return 0;
  offset = (size_t)(payload_address - buffer_address);
  used = mem_buffer_used(msg->buffer);
  return offset <= used && msg->payload.len <= used - offset;
}

static int flow_fmq_consume_with_delivery(void *ctx, turbo_flow_t *flow,
                                          const turbo_flow_stage_plan_t *stage,
                                          turbo_flow_msg_t *msg,
                                          flow_fmq_delivery_stage_t *delivery_stage) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  flow_fmq_message_context_t *message_context = NULL;
  flow_fmq_frame_t frame;
  flow_fmq_send_request_t *request = NULL;
  flow_fmq_send_batch_t *batch = NULL;
  flow_fmq_segmented_frame_t segmented = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
  mem_buffer_t *payload_buffer = NULL;
  tstr_v encoded_topic = {0};
  tstr_t encoded = NULL;
  size_t encoded_size;
  size_t batch_frame_offset = 0u;
  size_t batch_frame_size = 0u;
  uint64_t correlation_id = 0u;
  uint64_t correlation_generation = 0u;
  flow_fmq_route_token_t send_route = {0u, 0u};
  flow_fmq_delivery_stage_t current_delivery_stage = FLOW_FMQ_DELIVERY_NOT_SUBMITTED;
  uint64_t profile_consume_started_ns = 0u;
  int req_state_acquired = 0;
  int rc;
  (void)flow;
  (void)stage;
  if (delivery_stage) *delivery_stage = FLOW_FMQ_DELIVERY_NOT_SUBMITTED;
  if (!adapter || !msg || (msg->payload.len > 0 && !msg->payload.data)) {
    return TURBO_EINVAL;
  }
  if (flow_fmq_active_send_batch &&
      flow_fmq_active_send_batch->owner == (flow ? (const void *)flow : (const void *)adapter)) {
    batch = flow_fmq_active_send_batch;
    profile_consume_started_ns = flow_fmq_profile_batch_step_begin(batch);
  }
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  if (adapter->pattern == TURBO_FLOW_FMQ_SUB || adapter->pattern == TURBO_FLOW_FMQ_PULL) {
    return TURBO_ENOTSUP;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_XSUB) {
    message_context = (flow_fmq_message_context_t *)msg->transport_context;
    if (!message_context || message_context->magic != FLOW_FMQ_MESSAGE_CONTEXT_MAGIC ||
        message_context->subscription == 0) {
      return TURBO_EINVAL;
    }
  }
  memset(&frame, 0, sizeof(frame));
  frame.kind = FLOW_FMQ_FRAME_DATA;
  frame.pattern = adapter->pattern;
  frame.message_id =
      atomic_fetch_add_explicit(&adapter->next_message_id, 1u, memory_order_relaxed) + 1u;
  if (frame.message_id == 0u) {
    frame.message_id =
        atomic_fetch_add_explicit(&adapter->next_message_id, 1u, memory_order_relaxed) + 1u;
  }
  correlation_id = frame.message_id;
  if (adapter->pattern == TURBO_FLOW_FMQ_REQ) {
    rc = adapter->connect_endpoint
             ? flowmq_connect_endpoint_request_begin(adapter->connect_endpoint, correlation_id,
                                                     &correlation_generation)
             : TURBO_ENOTCONN;
    if (rc != TURBO_OK) return rc;
    req_state_acquired = 1;
  }
  frame.payload = msg->payload;
  if (adapter->pattern == TURBO_FLOW_FMQ_XSUB) {
    frame.kind =
        message_context->subscription > 0 ? FLOW_FMQ_FRAME_SUBSCRIBE : FLOW_FMQ_FRAME_UNSUBSCRIBE;
    frame.message_id = 0u;
    correlation_id = 0u;
    frame.payload = (tstr_v){0};
    frame.topic = message_context->topic;
    frame.identity = (tstr_v){0};
    goto frame_ready;
  }
  rc = flow_fmq_select_metadata(adapter, msg, adapter->topic_policy, 0, &frame.topic);
  if (rc != TURBO_OK) goto local_failure;
  rc = flow_fmq_select_metadata(adapter, msg, adapter->identity_policy, 1, &frame.identity);
  if (rc != TURBO_OK) goto local_failure;
  if (adapter->pattern == TURBO_FLOW_FMQ_ROUTER ||
      (adapter->pattern == TURBO_FLOW_FMQ_PAIR && adapter->mode == TURBO_FLOW_FMQ_BIND)) {
    const turbo_flow_protocol_route_t *route = turbo_flow_msg_protocol_route(msg);
    if (!route || route->protocol != TURBO_FLOW_PROTOCOL_FMQ ||
        route->owner_instance_id != adapter->instance_id) {
      rc = TURBO_EINVAL;
      goto local_failure;
    }
    if (route->session_generation !=
        atomic_load_explicit(&adapter->generation, memory_order_acquire)) {
      rc = TURBO_ENOTCONN;
      goto local_failure;
    }
    send_route.session_id = route->session_id;
    send_route.generation = route->session_generation;
  } else if (adapter->pattern == TURBO_FLOW_FMQ_REP) {
    message_context = (flow_fmq_message_context_t *)msg->transport_context;
    if (!message_context || message_context->magic != FLOW_FMQ_MESSAGE_CONTEXT_MAGIC ||
        message_context->adapter != adapter || !message_context->peer) {
      rc = TURBO_EBUSY;
      goto local_failure;
    }
    send_route = message_context->peer->route;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_REP) {
    if (message_context->correlation_id == 0u ||
        flowmq_peer_session_match(
            &message_context->peer->exchange, message_context->correlation_generation,
            FLOW_FMQ_PATTERN_PROCESSING_REQUEST, message_context->correlation_id) != TURBO_OK) {
      rc = TURBO_EBUSY;
      goto local_failure;
    }
    frame.message_id = message_context->correlation_id;
    correlation_id = frame.message_id;
  }
frame_ready:
  rc = flow_fmq_encoded_size(&frame, adapter->max_frame_size, &encoded_size);
  if (rc != TURBO_OK) goto local_failure;
  turbo_mutex_lock(&adapter->send_admission_mutex);
  rc = flow_fmq_reserve_send_budget(adapter, encoded_size);
  if (rc != TURBO_OK) {
    turbo_mutex_unlock(&adapter->send_admission_mutex);
    goto local_failure;
  }
  if (adapter->transport == TURBO_FLOW_FMQ_TCP &&
      adapter->pattern != TURBO_FLOW_FMQ_XSUB &&
      msg->payload.len >= FLOW_FMQ_ZERO_COPY_MIN_PAYLOAD_SIZE &&
      flow_fmq_payload_within_buffer(msg)) {
    rc = flow_fmq_encode_frame_segmented(&frame, adapter->max_frame_size, &segmented);
    if (rc == TURBO_OK && msg->payload.len > 0u) {
      payload_buffer = mem_buffer_retain(msg->buffer);
      if (!payload_buffer) rc = TURBO_ENOMEM;
    }
    if (rc == TURBO_OK) {
      encoded_topic = tstr_v_from_buf(
          (const char *)segmented.segments[0].data + FLOW_FMQ_HEADER_SIZE + frame.identity.len,
          frame.topic.len);
    }
  } else if (batch &&
             flow_fmq_send_batch_mode_for_adapter(adapter, batch->capacity) !=
                 FLOW_FMQ_SEND_BATCH_NONE) {
    rc = flow_fmq_send_batch_encode_frame(batch, &frame, adapter->max_frame_size, encoded_size,
                                          &batch_frame_offset);
    if (rc == TURBO_OK) batch_frame_size = encoded_size;
  } else {
    rc = flow_fmq_encode_frame(&frame, adapter->max_frame_size, &encoded);
  }
  if (rc != TURBO_OK) {
    flow_fmq_segmented_frame_cleanup(&segmented);
    mem_buffer_release(payload_buffer);
    flow_fmq_release_send_budget(adapter, encoded_size);
    turbo_mutex_unlock(&adapter->send_admission_mutex);
    goto local_failure;
  }
  rc = flow_fmq_submit_send(adapter, send_route, encoded, &segmented, payload_buffer,
                            encoded_topic, encoded_size, batch != NULL, batch, &request);
  payload_buffer = NULL;
  if (rc == TURBO_OK && batch_frame_size != 0u) {
    request->batch_frame_offset = batch_frame_offset;
    request->batch_frame_size = batch_frame_size;
  }
  turbo_mutex_unlock(&adapter->send_admission_mutex);
  if (rc != TURBO_OK) goto transport_failure;
  if (batch) {
    rc = flow_fmq_send_batch_append(batch, request);
    if (rc != TURBO_OK) {
      flow_fmq_discard_unsubmitted_request(request, rc);
      request = NULL;
      goto transport_failure;
    }
    flow_fmq_request_release(request);
    flow_fmq_profile_batch_adapter_consumed(batch, profile_consume_started_ns);
    return TURBO_OK;
  }
  rc = flow_fmq_wait_send_request(adapter, request, &current_delivery_stage);
  flow_fmq_request_release(request);
  if (delivery_stage) *delivery_stage = current_delivery_stage;
  if (rc != TURBO_OK) goto transport_failure;
  if (adapter->pattern == TURBO_FLOW_FMQ_REP) {
    rc = flowmq_peer_session_finish(&message_context->peer->exchange,
                                    message_context->correlation_generation,
                                    FLOW_FMQ_PATTERN_PROCESSING_REQUEST,
                                    message_context->correlation_id, FLOW_FMQ_PATTERN_READY);
  }
  return rc;

local_failure:
  if (req_state_acquired) {
    int finish_rc =
        flowmq_connect_endpoint_request_finish(adapter->connect_endpoint, correlation_generation,
                                               correlation_id, FLOWMQ_PEER_EXCHANGE_READY);
    if (finish_rc != TURBO_OK) return finish_rc;
  }
  return rc;

transport_failure:
  if (req_state_acquired) {
    flow_fmq_pattern_state_t terminal =
        flow_fmq_delivery_requires_session_reset(current_delivery_stage)
            ? FLOW_FMQ_PATTERN_RESETTING
            : FLOW_FMQ_PATTERN_READY;
    int finish_rc = flowmq_connect_endpoint_request_finish(
        adapter->connect_endpoint, correlation_generation, correlation_id, terminal);
    if (terminal == FLOW_FMQ_PATTERN_RESETTING) {
      if (adapter->connect_endpoint)
        (void)flowmq_connect_endpoint_interrupt(adapter->connect_endpoint, rc);
    }
    if (finish_rc != TURBO_OK) return finish_rc;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_REP && message_context && message_context->peer) {
    if (flow_fmq_delivery_requires_session_reset(current_delivery_stage)) {
      (void)flowmq_peer_session_mark_resetting(&message_context->peer->exchange);
      (void)coro_socket_interrupt_wait(message_context->peer->socket, rc);
    }
  }
  return rc;
}

static int flow_fmq_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                            turbo_flow_msg_t *msg) {
  return flow_fmq_consume_with_delivery(ctx, flow, stage, msg, NULL);
}

static int flow_fmq_consume_batch(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_stage_plan_t *stage,
                                  const turbo_flow_adapter_batch_t *batch,
                                  size_t *consumed) {
  int rc = TURBO_OK;
  if (consumed) *consumed = 0u;
  if (!ctx || !batch || batch->size < sizeof(*batch) || batch->message_count == 0u ||
      !batch->next || !consumed) {
    return TURBO_EINVAL;
  }
  for (size_t i = 0u; i < batch->message_count; ++i) {
    turbo_flow_msg_t message;
    turbo_flow_msg_init(&message);
    rc = batch->next(batch->ctx, i, &message);
    if (rc == TURBO_OK) {
      rc = flow_fmq_consume_with_delivery(ctx, flow, stage, &message, NULL);
    }
    turbo_flow_msg_cleanup(&message);
    if (rc != TURBO_OK) break;
    *consumed = i + 1u;
  }
  return rc;
}

typedef struct flow_fmq_retry_context_s {
  flow_fmq_adapter_t *adapter;
  flow_fmq_delivery_stage_t delivery_stage;
} flow_fmq_retry_context_t;

static int flow_fmq_retry_attempt(void *ctx, turbo_flow_msg_t *msg, uint32_t attempt) {
  flow_fmq_retry_context_t *retry = (flow_fmq_retry_context_t *)ctx;
  (void)attempt;
  retry->delivery_stage = FLOW_FMQ_DELIVERY_NOT_SUBMITTED;
  return flow_fmq_consume_with_delivery(retry->adapter, NULL, NULL, msg, &retry->delivery_stage);
}

static int flow_fmq_retryable(void *ctx, int status) {
  flow_fmq_retry_context_t *retry = (flow_fmq_retry_context_t *)ctx;
  return retry && flow_fmq_delivery_retryable(retry->delivery_stage, status);
}

static int flow_fmq_retry_wait(void *ctx, uint32_t delay_ms) {
  flow_fmq_retry_context_t *retry = (flow_fmq_retry_context_t *)ctx;
  flow_fmq_adapter_t *adapter = retry ? retry->adapter : NULL;
  int rc;
  if (!adapter || !adapter->retry_wait_initialized) return TURBO_EINVAL;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  rc = tf_timer_wait_for_ms(&adapter->retry_wait, delay_ms);
  if (rc == TURBO_ETIMEDOUT) return TURBO_OK;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  return rc;
}

static int flow_fmq_consume_retry(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg,
                                  const turbo_flow_retry_policy_t *policy) {
  turbo_flow_retry_ops_t ops;
  flow_fmq_retry_context_t retry;
  (void)flow;
  (void)stage;
  memset(&ops, 0, sizeof(ops));
  memset(&retry, 0, sizeof(retry));
  retry.adapter = (flow_fmq_adapter_t *)ctx;
  ops.size = sizeof(ops);
  ops.attempt = flow_fmq_retry_attempt;
  ops.retryable = flow_fmq_retryable;
  ops.wait = flow_fmq_retry_wait;
  return turbo_flow_retry_execute(policy, msg, &ops, &retry);
}

static void flow_fmq_stop_resources(flow_fmq_adapter_t *adapter) {
  uint64_t timeout_ns;
  if (!adapter || !atomic_load_explicit(&adapter->started, memory_order_acquire)) return;
  flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_CLOSING, TURBO_ESHUTDOWN,
                      atomic_load_explicit(&adapter->connections_current, memory_order_relaxed));
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  tf_io_budget_close(&adapter->send_budget);
  if (adapter->retry_wait_initialized) tf_timer_stop(&adapter->retry_wait);
  /* Close admission first so a BLOCK waiter wakes, then serialize against the
   * reserve-to-enqueue critical section before draining accepted requests. */
  turbo_mutex_lock(&adapter->send_admission_mutex);
  if (adapter->frame_linger_ms == 0) flow_fmq_fail_send_queue(adapter, TURBO_ESHUTDOWN);
  flow_fmq_wait_send_drain(adapter);
  turbo_mutex_unlock(&adapter->send_admission_mutex);
  timeout_ns = flow_fmq_connect_timeout_ns(adapter);
  if (adapter->connect_endpoint) {
    flowmq_connect_endpoint_stop(adapter->connect_endpoint);
  } else {
    (void)flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_CLOSE_RESOURCES, timeout_ns);
  }
  flow_fmq_wait_lane_tasks(adapter);
  if (adapter->mode == TURBO_FLOW_FMQ_BIND) {
    (void)flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_CLEAR_PEERS, timeout_ns);
    tf_coronet_execution_stop(&adapter->execution);
  }
  flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_STOPPED, TURBO_ESHUTDOWN, 0u);
}

static void flow_fmq_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  (void)flow;
  (void)stage;
  if (!adapter || adapter->start_refs <= 0) return;
  adapter->start_refs -= 1;
  if (adapter->start_refs > 0) return;
  atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
  flow_fmq_stop_resources(adapter);
  adapter->flow = NULL;
  adapter->ingress_dispatch = NULL;
  adapter->ingress_dispatch_ctx = NULL;
}

static int flow_fmq_endpoint_prepare(flow_fmq_adapter_t *adapter,
                                     const turbo_flow_adapter_endpoint_t *endpoint, tstr_t *host,
                                     tstr_t *path) {
  if (!adapter || !endpoint || !host || !path || *host || *path) return TURBO_EINVAL;
  if (adapter->transport == TURBO_FLOW_FMQ_PIPE) {
    if (!endpoint->path || endpoint->path[0] == '\0') return TURBO_EINVAL;
    *host = tstr_new();
    *path = tstr_dup(endpoint->path);
  } else {
    if (!endpoint->host || endpoint->host[0] == '\0' || endpoint->port < 1 ||
        endpoint->port > 65535) {
      return TURBO_EINVAL;
    }
    *host = tstr_dup(endpoint->host);
    *path = endpoint->path ? tstr_dup(endpoint->path) : tstr_new();
  }
  if (!*host || !*path) {
    tstr_freep(host);
    tstr_freep(path);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static int flow_fmq_replace_endpoint(flow_fmq_adapter_t *adapter,
                                     const turbo_flow_adapter_endpoint_t *endpoint) {
  tstr_t next_host = NULL;
  tstr_t next_path = NULL;
  tstr_t old_host;
  tstr_t old_path;
  int old_port;
  int was_started;
  int rc;
  int rollback_rc;

  rc = flow_fmq_endpoint_prepare(adapter, endpoint, &next_host, &next_path);
  if (rc != TURBO_OK) return rc;
  old_host = adapter->host;
  old_path = adapter->path;
  old_port = adapter->port;
  was_started = atomic_load_explicit(&adapter->started, memory_order_acquire);
  if (was_started) flow_fmq_stop_resources(adapter);
  if (adapter->connect_endpoint) {
    rc = flowmq_connect_endpoint_update_endpoint(
        adapter->connect_endpoint, next_host,
        adapter->transport == TURBO_FLOW_FMQ_PIPE ? 0 : endpoint->port, next_path);
    if (rc != TURBO_OK) {
      if (was_started) (void)flow_fmq_start_resources(adapter);
      tstr_free(next_host);
      tstr_free(next_path);
      return rc;
    }
  }
  adapter->host = next_host;
  adapter->path = next_path;
  adapter->port = adapter->transport == TURBO_FLOW_FMQ_PIPE ? 0 : endpoint->port;
  if (!was_started) {
    tstr_free(old_host);
    tstr_free(old_path);
    return TURBO_OK;
  }

  rc = flow_fmq_start_resources(adapter);
  if (rc == TURBO_OK) {
    tstr_free(old_host);
    tstr_free(old_path);
    return TURBO_OK;
  }
  flow_fmq_stop_resources(adapter);
  next_host = adapter->host;
  next_path = adapter->path;
  adapter->host = old_host;
  adapter->path = old_path;
  adapter->port = old_port;
  if (adapter->connect_endpoint) {
    rollback_rc = flowmq_connect_endpoint_update_endpoint(adapter->connect_endpoint, old_host,
                                                          old_port, old_path);
    if (rollback_rc != TURBO_OK) {
      tstr_free(next_host);
      tstr_free(next_path);
      return rollback_rc;
    }
  }
  rollback_rc = flow_fmq_start_resources(adapter);
  tstr_free(next_host);
  tstr_free(next_path);
  return rollback_rc == TURBO_OK ? rc : rollback_rc;
}

static int flow_fmq_command(void *ctx, turbo_flow_t *flow,
                            const turbo_flow_adapter_command_t *command) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  (void)flow;
  if (!adapter || !command || adapter->start_refs <= 0) return TURBO_EINVAL;
  switch (command->kind) {
  case TURBO_FLOW_ADAPTER_QUIESCE:
    if (atomic_exchange_explicit(&adapter->quiesced, 1, memory_order_acq_rel) == 0) {
      flow_fmq_stop_resources(adapter);
    }
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_RESUME:
    if (!atomic_load_explicit(&adapter->quiesced, memory_order_acquire)) return TURBO_OK;
    {
      int rc = flow_fmq_start_resources(adapter);
      if (rc != TURBO_OK) return rc;
    }
    atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT:
    return flow_fmq_replace_endpoint(adapter, &command->endpoint);
  default:
    return TURBO_EINVAL;
  }
}

static int flow_fmq_resource_command(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_resource_command_t *command) {
  flow_fmq_resource_context_t *resource = (flow_fmq_resource_context_t *)ctx;
  turbo_flow_adapter_command_t owner_command;
  if (!resource || !resource->adapter || !command ||
      resource->kind != TURBO_FLOW_RESOURCE_CONNECTION) {
    return resource && command ? TURBO_ENOTSUP : TURBO_EINVAL;
  }
  memset(&owner_command, 0, sizeof(owner_command));
  owner_command.size = sizeof(owner_command);
  switch (command->kind) {
  case TURBO_FLOW_RESOURCE_COMMAND_QUIESCE:
    owner_command.kind = TURBO_FLOW_ADAPTER_QUIESCE;
    break;
  case TURBO_FLOW_RESOURCE_COMMAND_RESUME:
    owner_command.kind = TURBO_FLOW_ADAPTER_RESUME;
    break;
  case TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT:
    owner_command.kind = TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT;
    owner_command.endpoint.host = command->endpoint_host;
    owner_command.endpoint.path = command->endpoint_path;
    owner_command.endpoint.port = command->endpoint_port;
    break;
  default:
    return TURBO_ENOTSUP;
  }
  return flow_fmq_command(resource->adapter, flow, &owner_command);
}

static void flow_fmq_shutdown(void *ctx) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  if (!adapter) return;
  if (adapter->start_refs > 0) {
    adapter->start_refs = 1;
    flow_fmq_stop(adapter, NULL, NULL);
  }
  flowmq_connect_endpoint_destroy(adapter->connect_endpoint);
  adapter->connect_endpoint = NULL;
  if (adapter->send_queue_initialized) {
    flow_fmq_fail_send_queue(adapter, TURBO_ESHUTDOWN);
    flow_fmq_send_requests_destroy(&adapter->send_queue);
    turbo_mutex_destroy(&adapter->send_queue_mutex);
    turbo_mutex_destroy(&adapter->send_admission_mutex);
    adapter->send_queue_initialized = 0;
  }
  tf_io_budget_destroy(&adapter->send_budget);
  if (adapter->retry_wait_initialized) {
    tf_timer_destroy(&adapter->retry_wait);
    adapter->retry_wait_initialized = 0;
  }
  if (adapter->lane_task_sync_initialized) {
    turbo_cond_destroy(&adapter->lane_task_cond);
    turbo_mutex_destroy(&adapter->lane_task_mutex);
    adapter->lane_task_sync_initialized = 0;
  }
  if (adapter->reactor_actor_initialized) {
    tf_coronet_actor_close(&adapter->reactor_actor);
    (void)tf_coronet_actor_drain(&adapter->reactor_actor, UINT64_MAX);
    (void)tf_coronet_actor_destroy(&adapter->reactor_actor);
    adapter->reactor_actor_initialized = 0;
  }
  tf_coronet_execution_destroy(&adapter->execution);
  flowmq_security_binding_destroy(&adapter->security);
  if (adapter->security_selection_initialized) {
    turbo_vec_destroy(&adapter->security_selection);
    adapter->security_selection_initialized = 0;
  }
  turbo_vec_destroy(&adapter->peers);
  tstr_freep(&adapter->host);
  tstr_freep(&adapter->path);
  tstr_freep(&adapter->topic);
  tstr_freep(&adapter->identity);
  tstr_freep(&adapter->source_name);
  tstr_freep(&adapter->resource_owner);
  tstr_freep(&adapter->connection_uid);
  tstr_freep(&adapter->queue_uid);
  tstr_freep(&adapter->protocol_uid);
  tstr_freep(&adapter->udp_multicast_group);
  tstr_freep(&adapter->udp_multicast_interface);
  tstr_freep(&adapter->tls_ca_file);
  tstr_freep(&adapter->tls_cert_file);
  tstr_freep(&adapter->tls_key_file);
  tstr_freep(&adapter->tls_key_password);
  tstr_freep(&adapter->tls_server_name);
  turbo_kcp_config_wipe(&adapter->kcp_config);
  free(adapter);
}

static uint32_t flow_fmq_roles(turbo_flow_fmq_pattern_t pattern) {
  switch (pattern) {
  case TURBO_FLOW_FMQ_PUB:
  case TURBO_FLOW_FMQ_PUSH:
    return TURBO_FLOW_ADAPTER_SINK;
  case TURBO_FLOW_FMQ_SUB:
  case TURBO_FLOW_FMQ_PULL:
    return TURBO_FLOW_ADAPTER_SOURCE;
  case TURBO_FLOW_FMQ_ROUTER:
  case TURBO_FLOW_FMQ_DEALER:
  case TURBO_FLOW_FMQ_PAIR:
  case TURBO_FLOW_FMQ_REQ:
  case TURBO_FLOW_FMQ_REP:
  case TURBO_FLOW_FMQ_XPUB:
  case TURBO_FLOW_FMQ_XSUB:
    return TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK;
  default:
    return 0;
  }
}

typedef struct flow_fmq_operation_contract_s {
  const char *name;
  uint32_t role;
} flow_fmq_operation_contract_t;

static const flow_fmq_operation_contract_t FLOW_FMQ_OPERATION_CONTRACTS[] = {
    {TURBO_FLOW_FMQ_PUB_SEND_OPERATION, TURBO_FLOW_OPERATION_STAGE},
    {TURBO_FLOW_FMQ_SUB_RECEIVE_OPERATION, TURBO_FLOW_OPERATION_SOURCE},
    {TURBO_FLOW_FMQ_PUSH_SEND_OPERATION, TURBO_FLOW_OPERATION_STAGE},
    {TURBO_FLOW_FMQ_PULL_RECEIVE_OPERATION, TURBO_FLOW_OPERATION_SOURCE},
    {TURBO_FLOW_FMQ_ROUTER_RECEIVE_OPERATION, TURBO_FLOW_OPERATION_SOURCE},
    {TURBO_FLOW_FMQ_ROUTER_SEND_OPERATION, TURBO_FLOW_OPERATION_STAGE},
    {TURBO_FLOW_FMQ_DEALER_RECEIVE_OPERATION, TURBO_FLOW_OPERATION_SOURCE},
    {TURBO_FLOW_FMQ_DEALER_SEND_OPERATION, TURBO_FLOW_OPERATION_STAGE},
    {TURBO_FLOW_FMQ_PAIR_RECEIVE_OPERATION, TURBO_FLOW_OPERATION_SOURCE},
    {TURBO_FLOW_FMQ_PAIR_SEND_OPERATION, TURBO_FLOW_OPERATION_STAGE},
    {TURBO_FLOW_FMQ_REQ_REQUEST_OPERATION, TURBO_FLOW_OPERATION_STAGE},
    {TURBO_FLOW_FMQ_REQ_REPLY_OPERATION, TURBO_FLOW_OPERATION_SOURCE},
    {TURBO_FLOW_FMQ_REP_REQUEST_OPERATION, TURBO_FLOW_OPERATION_SOURCE},
    {TURBO_FLOW_FMQ_REP_REPLY_OPERATION, TURBO_FLOW_OPERATION_STAGE},
    {TURBO_FLOW_FMQ_XPUB_RECEIVE_OPERATION, TURBO_FLOW_OPERATION_SOURCE},
    {TURBO_FLOW_FMQ_XPUB_SEND_OPERATION, TURBO_FLOW_OPERATION_STAGE},
    {TURBO_FLOW_FMQ_XSUB_RECEIVE_OPERATION, TURBO_FLOW_OPERATION_SOURCE},
    {TURBO_FLOW_FMQ_XSUB_SEND_OPERATION, TURBO_FLOW_OPERATION_STAGE}};

static int flow_fmq_register_contract(turbo_flow_t *flow) {
  static const char *const primitive_types[] = {"FmqConnection", "FmqQueue",
                                                "FmqProtocolAggregate"};
  const size_t count =
      sizeof(FLOW_FMQ_OPERATION_CONTRACTS) / sizeof(FLOW_FMQ_OPERATION_CONTRACTS[0]);
  const char *operation_names[sizeof(FLOW_FMQ_OPERATION_CONTRACTS) /
                              sizeof(FLOW_FMQ_OPERATION_CONTRACTS[0])];
  turbo_flow_operation_descriptor_t
      operations[sizeof(FLOW_FMQ_OPERATION_CONTRACTS) / sizeof(FLOW_FMQ_OPERATION_CONTRACTS[0])];
  turbo_flow_module_descriptor_t module;
  memset(operations, 0, sizeof(operations));
  memset(&module, 0, sizeof(module));
  for (size_t i = 0; i < count; ++i) {
    const uint32_t role = FLOW_FMQ_OPERATION_CONTRACTS[i].role;
    operation_names[i] = FLOW_FMQ_OPERATION_CONTRACTS[i].name;
    operations[i].size = sizeof(operations[i]);
    operations[i].name = operation_names[i];
    operations[i].version = 1u;
    operations[i].domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
    operations[i].scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
    operations[i].scope.state = TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER;
    operations[i].scope.lifetime = role == TURBO_FLOW_OPERATION_SOURCE
                                       ? TURBO_FLOW_LIFETIME_DISPATCH
                                       : TURBO_FLOW_LIFETIME_CALL;
    operations[i].scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
    operations[i].scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
    operations[i].flags = role | TURBO_FLOW_OPERATION_BRIDGE;
    operations[i].execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
    if (role == TURBO_FLOW_OPERATION_SOURCE) {
      operations[i].output_domain = TURBO_FLOW_DOMAIN_DATA;
      operations[i].output_type = "Message";
    } else {
      operations[i].input_domain = TURBO_FLOW_DOMAIN_DATA;
      operations[i].input_type = "Message";
    }
  }
  module.size = sizeof(module);
  module.name = TURBO_FLOW_FMQ_MODULE;
  module.version = 1u;
  module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                            TURBO_FLOW_MODULE_MANAGED_RESOURCES | TURBO_FLOW_MODULE_NATIVE_API;
  module.primitive_types = primitive_types;
  module.primitive_type_count = 3u;
  module.operation_names = operation_names;
  module.operation_count = count;
  return turbo_flow_register_module_contract(flow, &module, operations, count);
}

static size_t flow_fmq_pattern_operation_names(turbo_flow_fmq_pattern_t pattern,
                                               const char **operations) {
  switch (pattern) {
  case TURBO_FLOW_FMQ_PUB:
    operations[0] = TURBO_FLOW_FMQ_PUB_SEND_OPERATION;
    return 1u;
  case TURBO_FLOW_FMQ_SUB:
    operations[0] = TURBO_FLOW_FMQ_SUB_RECEIVE_OPERATION;
    return 1u;
  case TURBO_FLOW_FMQ_PUSH:
    operations[0] = TURBO_FLOW_FMQ_PUSH_SEND_OPERATION;
    return 1u;
  case TURBO_FLOW_FMQ_PULL:
    operations[0] = TURBO_FLOW_FMQ_PULL_RECEIVE_OPERATION;
    return 1u;
  case TURBO_FLOW_FMQ_ROUTER:
    operations[0] = TURBO_FLOW_FMQ_ROUTER_RECEIVE_OPERATION;
    operations[1] = TURBO_FLOW_FMQ_ROUTER_SEND_OPERATION;
    return 2u;
  case TURBO_FLOW_FMQ_DEALER:
    operations[0] = TURBO_FLOW_FMQ_DEALER_RECEIVE_OPERATION;
    operations[1] = TURBO_FLOW_FMQ_DEALER_SEND_OPERATION;
    return 2u;
  case TURBO_FLOW_FMQ_PAIR:
    operations[0] = TURBO_FLOW_FMQ_PAIR_RECEIVE_OPERATION;
    operations[1] = TURBO_FLOW_FMQ_PAIR_SEND_OPERATION;
    return 2u;
  case TURBO_FLOW_FMQ_REQ:
    operations[0] = TURBO_FLOW_FMQ_REQ_REPLY_OPERATION;
    operations[1] = TURBO_FLOW_FMQ_REQ_REQUEST_OPERATION;
    return 2u;
  case TURBO_FLOW_FMQ_REP:
    operations[0] = TURBO_FLOW_FMQ_REP_REQUEST_OPERATION;
    operations[1] = TURBO_FLOW_FMQ_REP_REPLY_OPERATION;
    return 2u;
  case TURBO_FLOW_FMQ_XPUB:
    operations[0] = TURBO_FLOW_FMQ_XPUB_RECEIVE_OPERATION;
    operations[1] = TURBO_FLOW_FMQ_XPUB_SEND_OPERATION;
    return 2u;
  case TURBO_FLOW_FMQ_XSUB:
    operations[0] = TURBO_FLOW_FMQ_XSUB_RECEIVE_OPERATION;
    operations[1] = TURBO_FLOW_FMQ_XSUB_SEND_OPERATION;
    return 2u;
  default:
    return 0u;
  }
}

static int flow_fmq_fanout_config_validate(const turbo_flow_fmq_config_t *config,
                                           const turbo_flow_fmq_fanout_config_t *fanout) {
  if (!config || !fanout || fanout->size != sizeof(*fanout) ||
      (fanout->peer_hwm_messages == 0u && fanout->peer_hwm_bytes == 0u) ||
      fanout->slow_peer_policy < TURBO_FLOW_FMQ_SLOW_PEER_FAIL ||
      fanout->slow_peer_policy > TURBO_FLOW_FMQ_SLOW_PEER_DISCONNECT) {
    return TURBO_EINVAL;
  }
  if ((config->pattern != TURBO_FLOW_FMQ_PUB && config->pattern != TURBO_FLOW_FMQ_XPUB) ||
      config->mode != TURBO_FLOW_FMQ_BIND) {
    return TURBO_ENOTSUP;
  }
  if (config->transport == TURBO_FLOW_FMQ_UDP || config->transport == TURBO_FLOW_FMQ_KCP) {
    return TURBO_ENOTSUP;
  }
  return TURBO_OK;
}

static int
flow_fmq_register_adapter_internal(turbo_flow_t *flow, const char *name,
                                   const turbo_flow_fmq_config_t *config,
                                   const turbo_flow_fmq_fanout_config_t *fanout,
                                   const turbo_flow_coronet_execution_binding_t *execution,
                                   const turbo_flow_fmq_security_binding_t *security,
                                   flow_fmq_adapter_t **direct_out) {
  flow_fmq_adapter_t *adapter;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_adapter_schema_t schema;
  turbo_flow_resource_provider_registration_t resources[3];
  turbo_flow_module_adapter_registration_t registration =
      TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
  const char *operation_names[2];
  size_t operation_count;
  uint32_t roles;
  int rc;
  char host_buf[FLOW_FMQ_ENDPOINT_HOST_MAX + 1u];
  flow_fmq_endpoint_effective_t endpoint_effective;
  turbo_flow_fmq_config_t normalized;
  if (direct_out) *direct_out = NULL;
  if ((!flow && !direct_out) || (flow && direct_out) || !name || name[0] == '\0')
    return TURBO_EINVAL;
  if (!config) return TURBO_EINVAL;
  /* A "<scheme>://" prefix on config->host selects the transport and may carry
     the port. Resolve it once so validation, security checks and the adapter
     all observe the same effective transport/host/port. */
  rc = flow_fmq_endpoint_effective(config, host_buf, sizeof(host_buf),
                                   &endpoint_effective);
  if (rc != TURBO_OK) return rc;
  normalized = *config;
  normalized.transport = endpoint_effective.transport;
  normalized.host = endpoint_effective.host;
  normalized.port = endpoint_effective.port;
  config = &normalized;
  rc = turbo_flow_coronet_execution_binding_validate(execution);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_config_validate(config);
  if (rc != TURBO_OK) return rc;
  if (config->stream_recv_buffer_bytes != 0u &&
      execution->kind != TURBO_FLOW_CORONET_EXECUTION_PRIVATE)
    return TURBO_ENOTSUP;
  if (security && config->mode == TURBO_FLOW_FMQ_CONNECT &&
      (!config->identity || config->identity[0] == '\0')) {
    return TURBO_EINVAL;
  }
  if ((security || endpoint_effective.scheme_used) &&
      (config->transport == TURBO_FLOW_FMQ_TLS ||
       config->transport == TURBO_FLOW_FMQ_WSS) &&
      !config->tls) {
    return TURBO_EINVAL;
  }
  if (security && security->verify_peer_certificate_identity &&
      (config->mode != TURBO_FLOW_FMQ_BIND || !config->tls ||
       !config->tls->require_client_certificate)) {
    return TURBO_EINVAL;
  }
  if (fanout) {
    rc = flow_fmq_fanout_config_validate(config, fanout);
    if (rc != TURBO_OK) return rc;
  }
  roles = flow_fmq_roles(config->pattern);
  if (roles == 0) return TURBO_EINVAL;
  operation_count = flow_fmq_pattern_operation_names(config->pattern, operation_names);
  if (operation_count == 0u) return TURBO_EINVAL;
  if (flow) {
    rc = flow_fmq_register_contract(flow);
    if (rc != TURBO_OK) return rc;
  }
  adapter = (flow_fmq_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  rc = flow_fmq_allocate_adapter_instance_id(&adapter->instance_id);
  if (rc != TURBO_OK) {
    free(adapter);
    return rc;
  }
  if (turbo_vec_init(&adapter->peers, sizeof(flow_fmq_peer_t *)) != TURBO_OK) {
    free(adapter);
    return TURBO_ENOMEM;
  }
  rc = flowmq_security_binding_init(&adapter->security, config->mode, config->transport, security);
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&adapter->peers);
    free(adapter);
    return rc;
  }
  if (config->mode == TURBO_FLOW_FMQ_BIND) {
    rc = tf_coronet_execution_init(&adapter->execution, execution);
    if (rc != TURBO_OK) {
      flowmq_security_binding_destroy(&adapter->security);
      turbo_vec_destroy(&adapter->peers);
      free(adapter);
      return rc;
    }
    if (config->stream_recv_buffer_bytes != 0u) {
      rc = coro_context_set_stream_recv_buffer_size(adapter->execution.context,
                                                    config->stream_recv_buffer_bytes);
      if (rc != TURBO_OK) {
        tf_coronet_execution_destroy(&adapter->execution);
        flowmq_security_binding_destroy(&adapter->security);
        turbo_vec_destroy(&adapter->peers);
        free(adapter);
        return rc;
      }
    }
    rc = tf_coronet_actor_init(&adapter->reactor_actor, &adapter->execution,
                               flow_fmq_reactor_command, adapter, FLOW_FMQ_REACTOR_COMMAND_CAPACITY,
                               sizeof(flow_fmq_reactor_command_t));
    if (rc != TURBO_OK) {
      tf_coronet_execution_destroy(&adapter->execution);
      flowmq_security_binding_destroy(&adapter->security);
      turbo_vec_destroy(&adapter->peers);
      free(adapter);
      return rc;
    }
    adapter->reactor_actor_initialized = 1;
    adapter->ctx = adapter->execution.context;
  }
  turbo_mutex_init(&adapter->lane_task_mutex);
  turbo_cond_init(&adapter->lane_task_cond);
  adapter->lane_task_sync_initialized = 1;
  adapter->host = config->host ? tstr_dup(config->host) : tstr_new();
  adapter->path = config->path ? tstr_dup(config->path) : tstr_new();
  adapter->topic = config->topic ? tstr_dup(config->topic) : tstr_new();
  adapter->identity = config->identity ? tstr_dup(config->identity) : tstr_new();
  if (config->tls) {
    adapter->tls_ca_file = config->tls->ca_file ? tstr_dup(config->tls->ca_file) : NULL;
    adapter->tls_cert_file = config->tls->cert_file ? tstr_dup(config->tls->cert_file) : NULL;
    adapter->tls_key_file = config->tls->key_file ? tstr_dup(config->tls->key_file) : NULL;
    adapter->tls_key_password =
        config->tls->key_password ? tstr_dup(config->tls->key_password) : NULL;
    adapter->tls_server_name =
        config->tls->server_name ? tstr_dup(config->tls->server_name) : NULL;
    if ((config->tls->ca_file && !adapter->tls_ca_file) ||
        (config->tls->cert_file && !adapter->tls_cert_file) ||
        (config->tls->key_file && !adapter->tls_key_file) ||
        (config->tls->key_password && !adapter->tls_key_password) ||
        (config->tls->server_name && !adapter->tls_server_name)) {
      flow_fmq_shutdown(adapter);
      return TURBO_ENOMEM;
    }
    adapter->tls = *config->tls;
    adapter->tls.ca_file = adapter->tls_ca_file;
    adapter->tls.cert_file = adapter->tls_cert_file;
    adapter->tls.key_file = adapter->tls_key_file;
    adapter->tls.key_password = adapter->tls_key_password;
    adapter->tls.server_name = adapter->tls_server_name;
    adapter->tls_configured = 1;
  }
  adapter->resource_owner = tstr_dup(name);
  adapter->connection_uid = tstr_format("fmq:{}:connection", name);
  adapter->queue_uid = tstr_format("fmq:{}:queue", name);
  adapter->protocol_uid = tstr_format("fmq:{}:protocol", name);
  adapter->pattern = config->pattern;
  adapter->initial_subscription_configured = config->topic != NULL;
  adapter->mode = config->mode;
  adapter->transport = config->transport;
  rc = turbo_flow_content_descriptor_init(
      &adapter->control_descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
      TURBO_FLOW_CONTENT_PROFILE_FMQ_CONTROL, TURBO_FLOW_DATA_ENCODING_OPAQUE,
      "application/octet-stream", config->topic);
  if (rc != TURBO_OK) {
    flow_fmq_shutdown(adapter);
    return rc;
  }
  adapter->control_descriptor.flags |= TURBO_FLOW_CONTENT_PROTOCOL_CONTROL;
  if (config->content_type) {
    rc = turbo_flow_content_descriptor_from_media(
        &adapter->data_descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
        TURBO_FLOW_CONTENT_PROFILE_FMQ_DATA, config->content_type, config->topic,
        config->content_binding);
    if (rc == TURBO_OK) {
      adapter->has_data_descriptor = 1;
    } else if (rc != TURBO_ENOENT ||
               (config->content_binding && config->content_binding->schema.schema_version != 0u)) {
      flow_fmq_shutdown(adapter);
      return rc == TURBO_ENOENT ? TURBO_EPROTO : rc;
    }
  }
  adapter->topic_policy =
      config->topic_policy ? config->topic_policy : TURBO_FLOW_FMQ_METADATA_STATIC;
  adapter->identity_policy =
      config->identity_policy ? config->identity_policy : TURBO_FLOW_FMQ_METADATA_STATIC;
  adapter->port = config->port;
  adapter->max_frame_size =
      config->max_frame_size ? config->max_frame_size : TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE;
  adapter->max_connections =
      config->max_connections ? config->max_connections : TURBO_FLOW_FMQ_DEFAULT_MAX_CONNECTIONS;
  flow_fmq_timeout_config_resolve(&adapter->timeouts, config);
  adapter->heartbeat_interval_ms = config->heartbeat_interval_ms;
  adapter->heartbeat_timeout_ms = config->heartbeat_timeout_ms;
  adapter->event_callback = config->event_callback;
  adapter->event_ctx = config->event_ctx;
  {
    rc = flow_fmq_kcp_config_resolve(config, &adapter->kcp_config,
                                     &adapter->kcp_configured);
    if (rc != TURBO_OK) {
      flow_fmq_shutdown(adapter);
      return rc;
    }
  }
  adapter->reuse_port = config->reuse_port ? 1 : 0;
  adapter->socket_options.tcp_keepalive = config->tcp_keepalive;
  adapter->socket_options.tcp_keepalive_idle_ms = config->tcp_keepalive_idle_ms;
  adapter->socket_options.tcp_keepalive_interval_ms = config->tcp_keepalive_interval_ms;
  adapter->socket_options.tcp_keepalive_count = config->tcp_keepalive_count;
  adapter->socket_options.linger = config->linger;
  adapter->socket_options.linger_ms = config->linger_ms;
  adapter->socket_options.send_hwm_bytes = config->send_hwm_bytes;
  adapter->stream_recv_buffer_bytes = config->stream_recv_buffer_bytes;
  adapter->socket_options.socket_recv_buffer_bytes = config->socket_recv_buffer_bytes;
  adapter->socket_options.socket_send_buffer_bytes = config->socket_send_buffer_bytes;
  adapter->udp_options.option_flags = config->udp_option_flags;
  adapter->udp_options.multicast_loop = config->udp_multicast_loop;
  adapter->udp_options.multicast_ttl = config->udp_multicast_ttl;
  adapter->udp_options.broadcast = config->udp_broadcast;
  if (config->udp_multicast_group) {
    adapter->udp_multicast_group = tstr_dup(config->udp_multicast_group);
    if (!adapter->udp_multicast_group) {
      flow_fmq_shutdown(adapter);
      return TURBO_ENOMEM;
    }
    adapter->udp_options.multicast_group = adapter->udp_multicast_group;
  }
  if (config->udp_multicast_interface) {
    adapter->udp_multicast_interface = tstr_dup(config->udp_multicast_interface);
    if (!adapter->udp_multicast_interface) {
      flow_fmq_shutdown(adapter);
      return TURBO_ENOMEM;
    }
    adapter->udp_options.multicast_interface = adapter->udp_multicast_interface;
  }
  adapter->frame_hwm_messages = config->frame_hwm_messages;
  adapter->frame_hwm_bytes = config->frame_hwm_bytes;
  adapter->frame_admission_policy = config->frame_admission_policy;
  adapter->frame_linger_ms = config->frame_linger_ms;
  if (fanout) {
    adapter->fanout = *fanout;
    adapter->fanout.size = sizeof(adapter->fanout);
    adapter->fanout_enabled = 1;
    rc = turbo_vec_init(&adapter->security_selection, sizeof(uint8_t));
    if (rc == TURBO_OK) {
      adapter->security_selection_initialized = 1;
      rc = turbo_vec_resize(&adapter->security_selection, adapter->max_connections);
    }
    if (rc != TURBO_OK) {
      flow_fmq_shutdown(adapter);
      return rc;
    }
  }
  atomic_init(&adapter->started, 0);
  atomic_init(&adapter->connect_status, TURBO_ENOTCONN);
  atomic_init(&adapter->connection_state, TURBO_FLOW_CONNECTION_STOPPED);
  atomic_init(&adapter->quiesced, 0);
  atomic_init(&adapter->connections_current, 0);
  atomic_init(&adapter->generation, 1u);
  rc = turbo_flow_pattern_selector_init(&adapter->selector);
  if (rc != TURBO_OK) {
    flow_fmq_shutdown(adapter);
    return rc;
  }
  atomic_init(&adapter->next_message_id, 0u);
  if (adapter->mode == TURBO_FLOW_FMQ_CONNECT) {
    flowmq_connect_endpoint_config_t endpoint_config;
    memset(&endpoint_config, 0, sizeof(endpoint_config));
    endpoint_config.transport = (flowmq_coronet_transport_t)adapter->transport;
    endpoint_config.pattern = (flowmq_protocol_pattern_t)adapter->pattern;
    endpoint_config.host = adapter->host;
    endpoint_config.path = adapter->path;
    endpoint_config.topic = adapter->topic;
    endpoint_config.identity = adapter->identity;
    endpoint_config.tls = adapter->tls_configured ? &adapter->tls : NULL;
    endpoint_config.port = adapter->port;
    endpoint_config.max_frame_size = adapter->max_frame_size;
    endpoint_config.stream_recv_buffer_bytes = adapter->stream_recv_buffer_bytes;
    endpoint_config.timeouts = adapter->timeouts;
    endpoint_config.socket_options = adapter->socket_options;
    endpoint_config.udp_options = adapter->udp_options;
    endpoint_config.kcp_config = adapter->kcp_config;
    endpoint_config.kcp_configured = adapter->kcp_configured;
    endpoint_config.initial_subscription_configured = adapter->initial_subscription_configured;
    endpoint_config.reconnect_initial_ms =
        config->reconnect_initial_ms == TURBO_FLOW_FMQ_RECONNECT_DISABLED
            ? 0u
            : (config->reconnect_initial_ms ? config->reconnect_initial_ms
                                            : FLOW_FMQ_RECONNECT_INITIAL_MS);
    endpoint_config.reconnect_max_ms =
        config->reconnect_max_ms == TURBO_FLOW_FMQ_RECONNECT_MAX_UNBOUNDED
            ? 0u
            : (config->reconnect_max_ms ? config->reconnect_max_ms : FLOW_FMQ_RECONNECT_MAX_MS);
    endpoint_config.heartbeat_interval_ms = adapter->heartbeat_interval_ms;
    endpoint_config.heartbeat_timeout_ms = adapter->heartbeat_timeout_ms;
    rc = flow_fmq_connect_endpoint_execution_resolve(&endpoint_config, execution);
    if (rc != TURBO_OK) {
      flow_fmq_shutdown(adapter);
      return rc;
    }
    endpoint_config.on_frame = flow_fmq_connect_endpoint_frame;
    endpoint_config.receive_ready = flow_fmq_connect_endpoint_receive_ready;
    endpoint_config.on_state = flow_fmq_connect_endpoint_state;
    endpoint_config.on_event = flow_fmq_connect_endpoint_event;
    endpoint_config.callback_ctx = adapter;
    endpoint_config.security = adapter->security.enabled ? &adapter->security : NULL;
    rc = flowmq_connect_endpoint_create(&endpoint_config, &adapter->connect_endpoint);
    if (rc != TURBO_OK) {
      flow_fmq_shutdown(adapter);
      return rc;
    }
    adapter->ctx = flowmq_connect_endpoint_context(adapter->connect_endpoint);
  }
  if (flow_fmq_send_requests_init(&adapter->send_queue) != TURBO_OK) {
    flow_fmq_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  turbo_mutex_init(&adapter->send_queue_mutex);
  turbo_mutex_init(&adapter->send_admission_mutex);
  adapter->send_queue_initialized = 1;
  {
    const tf_io_budget_config_t budget_config = {
        adapter->frame_hwm_messages, adapter->frame_hwm_bytes,
        config->frame_admission_policy == TURBO_FLOW_FMQ_FRAME_ADMISSION_BLOCK
            ? TF_IO_ADMISSION_BLOCK
            : TF_IO_ADMISSION_FAIL,
        config->frame_admission_timeout_ms};
    if (tf_io_budget_init(&adapter->send_budget, &budget_config) != TURBO_OK) {
      flow_fmq_shutdown(adapter);
      return TURBO_ENOMEM;
    }
  }
  if (tf_timer_init(&adapter->retry_wait) != TURBO_OK) {
    flow_fmq_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  adapter->retry_wait_initialized = 1;
  if (!adapter->ctx || !adapter->host || !adapter->path || !adapter->topic || !adapter->identity ||
      !adapter->resource_owner || !adapter->connection_uid || !adapter->queue_uid ||
      !adapter->protocol_uid || tstr_len(adapter->resource_owner) > TURBO_FLOW_RESOURCE_OWNER_MAX ||
      tstr_len(adapter->connection_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      tstr_len(adapter->queue_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      tstr_len(adapter->protocol_uid) > TURBO_FLOW_RESOURCE_UID_MAX) {
    int identity_missing = !adapter->resource_owner || !adapter->connection_uid ||
                           !adapter->queue_uid || !adapter->protocol_uid;
    flow_fmq_shutdown(adapter);
    return identity_missing ? TURBO_ENOMEM : TURBO_ENAMETOOLONG;
  }
  if (direct_out) {
    *direct_out = adapter;
    return TURBO_OK;
  }
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_fmq_start;
  ops.consume = flow_fmq_consume;
  ops.consume_retry = flow_fmq_consume_retry;
  ops.stop = flow_fmq_stop;
  ops.shutdown = flow_fmq_shutdown;
  ops.connection_snapshot = flow_fmq_connection_snapshot;
  ops.command = flow_fmq_command;
  memset(&schema, 0, sizeof(schema));
  schema.kind = TURBO_FLOW_ADAPTER_KIND_FMQ;
  schema.roles = roles;
  schema.direction = roles == TURBO_FLOW_ADAPTER_SOURCE ? TURBO_FLOW_ADAPTER_INPUT
                     : roles == TURBO_FLOW_ADAPTER_SINK ? TURBO_FLOW_ADAPTER_OUTPUT
                                                        : TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  schema.fields = FLOW_FMQ_OPTION_FIELDS;
  schema.field_count = sizeof(FLOW_FMQ_OPTION_FIELDS) / sizeof(FLOW_FMQ_OPTION_FIELDS[0]);
  for (size_t i = 0; i < 3u; ++i) {
    resources[i] =
        (turbo_flow_resource_provider_registration_t)TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
    resources[i].owner_name = adapter->resource_owner;
    resources[i].ops.metadata = flow_fmq_resource_metadata;
    resources[i].ops.snapshot = flow_fmq_resource_snapshot;
    resources[i].ops.document = flow_fmq_resource_document;
    resources[i].ctx = &adapter->resource_contexts[i];
    adapter->resource_contexts[i].adapter = adapter;
  }
  adapter->resource_contexts[0].kind = TURBO_FLOW_RESOURCE_CONNECTION;
  adapter->resource_contexts[1].kind = TURBO_FLOW_RESOURCE_QUEUE_BUFFER;
  adapter->resource_contexts[2].kind = TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE;
  resources[0].ops.command = flow_fmq_resource_command;
  registration.module_name = TURBO_FLOW_FMQ_MODULE;
  registration.adapter_name = name;
  registration.ops = &ops;
  registration.ctx = adapter;
  registration.schema = &schema;
  registration.operation_names = operation_names;
  registration.operation_count = operation_count;
  registration.resources = resources;
  registration.resource_count = 3u;
  registration.consume_batch = flow_fmq_consume_batch;
  rc = turbo_flow_register_module_adapter(flow, &registration);
  if (rc != TURBO_OK) flow_fmq_shutdown(adapter);
  return rc;
}

int turbo_flow_fmq_register_adapter_ex(turbo_flow_t *flow, const char *name,
                                       const turbo_flow_fmq_config_t *config,
                                       const turbo_flow_coronet_execution_binding_t *execution) {
  return flow_fmq_register_adapter_internal(flow, name, config, NULL, execution, NULL, NULL);
}

int turbo_flow_fmq_register_secure_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_fmq_config_t *config,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security) {
  if (!security) return TURBO_EINVAL;
  return flow_fmq_register_adapter_internal(flow, name, config, NULL, execution, security, NULL);
}

int turbo_flow_fmq_register_fanout_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_fmq_config_t *config,
    const turbo_flow_fmq_fanout_config_t *fanout,
    const turbo_flow_coronet_execution_binding_t *execution) {
  return flow_fmq_register_adapter_internal(flow, name, config, fanout, execution, NULL, NULL);
}

int turbo_flow_fmq_register_secure_fanout_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_fmq_config_t *config,
    const turbo_flow_fmq_fanout_config_t *fanout,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security) {
  if (!security) return TURBO_EINVAL;
  return flow_fmq_register_adapter_internal(flow, name, config, fanout, execution, security, NULL);
}

static int flow_fmq_message_context(const turbo_flow_msg_t *msg, flow_fmq_message_context_t **out) {
  flow_fmq_message_context_t *context;
  if (!msg || !out) return TURBO_EINVAL;
  context = (flow_fmq_message_context_t *)msg->transport_context;
  if (!context || context->magic != FLOW_FMQ_MESSAGE_CONTEXT_MAGIC) return TURBO_ENOENT;
  *out = context;
  return TURBO_OK;
}

int turbo_flow_fmq_message_topic(const turbo_flow_msg_t *msg, tstr_v *topic) {
  flow_fmq_message_context_t *context;
  int rc;
  if (!topic) return TURBO_EINVAL;
  rc = flow_fmq_message_context(msg, &context);
  if (rc != TURBO_OK) return rc;
  *topic = context->topic;
  return TURBO_OK;
}

int turbo_flow_fmq_message_identity(const turbo_flow_msg_t *msg, tstr_v *identity) {
  flow_fmq_message_context_t *context;
  int rc;
  if (!identity) return TURBO_EINVAL;
  rc = flow_fmq_message_context(msg, &context);
  if (rc != TURBO_OK) return rc;
  *identity = context->identity;
  return TURBO_OK;
}

int turbo_flow_fmq_message_correlation_id(const turbo_flow_msg_t *msg, uint64_t *correlation_id) {
  flow_fmq_message_context_t *context;
  int rc;
  if (!correlation_id) return TURBO_EINVAL;
  rc = flow_fmq_message_context(msg, &context);
  if (rc != TURBO_OK) return rc;
  *correlation_id = context->correlation_id;
  return TURBO_OK;
}

int turbo_flow_fmq_message_detach_router_route(turbo_flow_msg_t *msg) {
  flow_fmq_message_context_t *context;
  const turbo_flow_protocol_route_t *route;
  int rc;
  if (!msg) return TURBO_EINVAL;
  rc = flow_fmq_message_context(msg, &context);
  if (rc != TURBO_OK) return rc;
  route = turbo_flow_msg_protocol_route(msg);
  if (context->pattern != TURBO_FLOW_FMQ_ROUTER || !route ||
      route->protocol != TURBO_FLOW_PROTOCOL_FMQ ||
      route->owner_instance_id != context->owner_instance_id || route->session_id == 0u ||
      route->session_generation == 0u) {
    return TURBO_ENOTSUP;
  }
  if (turbo_flow_msg_content_descriptor(msg) && !turbo_flow_msg_content_descriptor_owned(msg)) {
    rc = turbo_flow_msg_copy_content_descriptor(msg, turbo_flow_msg_content_descriptor(msg));
    if (rc != TURBO_OK) return rc;
  }
  msg->transport_context = NULL;
  return TURBO_OK;
}

int turbo_flow_fmq_message_subscription(const turbo_flow_msg_t *msg, int *subscribe,
                                         tstr_v *topic) {
  flow_fmq_message_context_t *context;
  int rc;
  if (!subscribe || !topic) return TURBO_EINVAL;
  rc = flow_fmq_message_context(msg, &context);
  if (rc != TURBO_OK) return rc;
  if (context->subscription == 0) return TURBO_ENOENT;
  *subscribe = context->subscription > 0;
  *topic = context->topic;
  return TURBO_OK;
}

typedef struct flow_fmq_app_async_request_s {
  mem_buffer_t *buffer;
  size_t data_size;
  turbo_flow_fmq_app_send_completion_fn completion;
  void *completion_ctx;
} flow_fmq_app_async_request_t;

TURBO_DEQUE_DEFINE(flow_fmq_app_async_requests, flow_fmq_app_async_request_t)

struct turbo_flow_fmq_app_s {
  flow_fmq_adapter_t *endpoint;
  turbo_flow_fmq_pattern_t pattern;
  turbo_flow_fmq_app_message_fn on_message;
  void *message_ctx;
  flow_fmq_app_async_requests async_requests;
  flow_fmq_app_async_request_t *async_batch_requests;
  turbo_mutex_t async_mutex;
  turbo_cond_t async_changed;
  turbo_thread_t async_thread;
  size_t async_queue_capacity;
  size_t async_queue_capacity_bytes;
  size_t async_queued_bytes;
  size_t async_batch_size;
  uint64_t async_linger_ns;
  int async_initialized;
  int async_configured;
  int async_accepting;
  int async_thread_started;
  int can_send;
  int started;
  int started_once;
};

static int flow_fmq_app_send_batch_internal(
    turbo_flow_fmq_app_t *app, const turbo_flow_fmq_app_send_item_t *copied_items,
    const flow_fmq_app_async_request_t *retained_items, size_t item_count,
    size_t *submitted);

static int flow_fmq_app_async_pattern_supported(turbo_flow_fmq_pattern_t pattern) {
  return pattern == TURBO_FLOW_FMQ_PUB || pattern == TURBO_FLOW_FMQ_PUSH ||
         pattern == TURBO_FLOW_FMQ_DEALER;
}

static int flow_fmq_app_async_config_validate(
    const turbo_flow_fmq_app_async_send_config_t *config) {
  return config && config->size == sizeof(*config) &&
         config->queue_capacity > 0u &&
         config->queue_capacity <= TURBO_FLOW_FMQ_APP_ASYNC_SEND_MAX_QUEUE_ITEMS &&
         config->queue_capacity_bytes > 0u && config->batch_size > 0u &&
         config->batch_size <= config->queue_capacity &&
         config->batch_size <= TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_ITEMS &&
         config->linger_ns <= TURBO_FLOW_FMQ_APP_ASYNC_SEND_MAX_LINGER_NS;
}

static void flow_fmq_app_async_complete(flow_fmq_app_async_request_t *request, int status) {
  turbo_flow_fmq_app_send_completion_fn completion;
  void *completion_ctx;
  if (!request) return;
  completion = request->completion;
  completion_ctx = request->completion_ctx;
  mem_buffer_release(request->buffer);
  memset(request, 0, sizeof(*request));
  if (completion) completion(completion_ctx, status);
}

static size_t flow_fmq_app_async_take_batch(turbo_flow_fmq_app_t *app) {
  size_t batch_count = 0u;
  size_t batch_bytes = 0u;
  while (batch_count < app->async_batch_size) {
    const flow_fmq_app_async_request_t *front =
        flow_fmq_app_async_requests_front_const(&app->async_requests);
    if (!front || front->data_size >
                      TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES - batch_bytes) {
      break;
    }
    if (!flow_fmq_app_async_requests_pop_front(
            &app->async_requests, &app->async_batch_requests[batch_count])) {
      break;
    }
    batch_bytes += app->async_batch_requests[batch_count].data_size;
    app->async_queued_bytes -= app->async_batch_requests[batch_count].data_size;
    ++batch_count;
  }
  return batch_count;
}

static void flow_fmq_app_async_worker(void *arg) {
  turbo_flow_fmq_app_t *app = (turbo_flow_fmq_app_t *)arg;
  if (!app) return;
  for (;;) {
    size_t batch_count;
    size_t submitted = 0u;
    int rc;
    turbo_mutex_lock(&app->async_mutex);
    while (flow_fmq_app_async_requests_empty(&app->async_requests) &&
           app->async_accepting) {
      turbo_cond_wait(&app->async_changed, &app->async_mutex);
    }
    if (flow_fmq_app_async_requests_empty(&app->async_requests) &&
        !app->async_accepting) {
      turbo_mutex_unlock(&app->async_mutex);
      break;
    }
    if (app->async_accepting && app->async_linger_ns > 0u &&
        flow_fmq_app_async_requests_size(&app->async_requests) < app->async_batch_size) {
      const uint64_t started_ns = turbo_hrtime();
      const uint64_t deadline_ns =
          app->async_linger_ns > UINT64_MAX - started_ns
              ? UINT64_MAX
              : started_ns + app->async_linger_ns;
      while (app->async_accepting &&
             flow_fmq_app_async_requests_size(&app->async_requests) <
                 app->async_batch_size) {
        const uint64_t now_ns = turbo_hrtime();
        if (now_ns >= deadline_ns) break;
        (void)turbo_cond_timedwait(&app->async_changed, &app->async_mutex,
                                   deadline_ns - now_ns);
      }
    }
    batch_count = flow_fmq_app_async_take_batch(app);
    turbo_mutex_unlock(&app->async_mutex);

    rc = flow_fmq_app_send_batch_internal(app, NULL, app->async_batch_requests,
                                          batch_count, &submitted);
    if (rc == TURBO_OK && submitted != batch_count) rc = TURBO_EPROTO;
    for (size_t i = 0u; i < batch_count; ++i)
      flow_fmq_app_async_complete(&app->async_batch_requests[i], rc);
  }
}

static void flow_fmq_app_async_destroy(turbo_flow_fmq_app_t *app) {
  flow_fmq_app_async_request_t request;
  if (!app || !app->async_initialized) return;
  while (flow_fmq_app_async_requests_pop_front(&app->async_requests, &request))
    flow_fmq_app_async_complete(&request, TURBO_ESHUTDOWN);
  flow_fmq_app_async_requests_destroy(&app->async_requests);
  free(app->async_batch_requests);
  turbo_cond_destroy(&app->async_changed);
  turbo_mutex_destroy(&app->async_mutex);
  app->async_initialized = 0;
}

static int flow_fmq_app_options_validate(const turbo_flow_fmq_app_options_t *options) {
  return options && options->size == sizeof(*options);
}

static int flow_fmq_app_pattern_contract(turbo_flow_fmq_pattern_t pattern,
                                         const char **receive_operation,
                                         const char **send_operation, int *auto_reply) {
  if (!receive_operation || !send_operation || !auto_reply) return TURBO_EINVAL;
  *receive_operation = NULL;
  *send_operation = NULL;
  *auto_reply = 0;
  switch (pattern) {
  case TURBO_FLOW_FMQ_PUB:
    *send_operation = TURBO_FLOW_FMQ_PUB_SEND_OPERATION;
    break;
  case TURBO_FLOW_FMQ_SUB:
    *receive_operation = TURBO_FLOW_FMQ_SUB_RECEIVE_OPERATION;
    break;
  case TURBO_FLOW_FMQ_PUSH:
    *send_operation = TURBO_FLOW_FMQ_PUSH_SEND_OPERATION;
    break;
  case TURBO_FLOW_FMQ_PULL:
    *receive_operation = TURBO_FLOW_FMQ_PULL_RECEIVE_OPERATION;
    break;
  case TURBO_FLOW_FMQ_ROUTER:
    *receive_operation = TURBO_FLOW_FMQ_ROUTER_RECEIVE_OPERATION;
    *send_operation = TURBO_FLOW_FMQ_ROUTER_SEND_OPERATION;
    break;
  case TURBO_FLOW_FMQ_DEALER:
    *receive_operation = TURBO_FLOW_FMQ_DEALER_RECEIVE_OPERATION;
    *send_operation = TURBO_FLOW_FMQ_DEALER_SEND_OPERATION;
    break;
  case TURBO_FLOW_FMQ_PAIR:
    *receive_operation = TURBO_FLOW_FMQ_PAIR_RECEIVE_OPERATION;
    *send_operation = TURBO_FLOW_FMQ_PAIR_SEND_OPERATION;
    break;
  case TURBO_FLOW_FMQ_REQ:
    *receive_operation = TURBO_FLOW_FMQ_REQ_REPLY_OPERATION;
    *send_operation = TURBO_FLOW_FMQ_REQ_REQUEST_OPERATION;
    break;
  case TURBO_FLOW_FMQ_REP:
    *receive_operation = TURBO_FLOW_FMQ_REP_REQUEST_OPERATION;
    *send_operation = TURBO_FLOW_FMQ_REP_REPLY_OPERATION;
    *auto_reply = 1;
    break;
  case TURBO_FLOW_FMQ_XPUB:
    *receive_operation = TURBO_FLOW_FMQ_XPUB_RECEIVE_OPERATION;
    *send_operation = TURBO_FLOW_FMQ_XPUB_SEND_OPERATION;
    break;
  case TURBO_FLOW_FMQ_XSUB:
    *receive_operation = TURBO_FLOW_FMQ_XSUB_RECEIVE_OPERATION;
    *send_operation = TURBO_FLOW_FMQ_XSUB_SEND_OPERATION;
    break;
  default:
    return TURBO_ENOTSUP;
  }
  return TURBO_OK;
}

static int flow_fmq_app_receive(turbo_flow_msg_t *message, void *ctx) {
  turbo_flow_fmq_app_t *app = (turbo_flow_fmq_app_t *)ctx;
  if (!app || !message) return TURBO_EINVAL;
  return app->on_message ? app->on_message(app, message, app->message_ctx) : TURBO_OK;
}

static int flow_fmq_app_ingress_dispatch(void *ctx, turbo_flow_msg_t *message) {
  turbo_flow_fmq_app_t *app = (turbo_flow_fmq_app_t *)ctx;
  int rc = flow_fmq_app_receive(message, ctx);
  if (rc == TURBO_OK && app && app->pattern == TURBO_FLOW_FMQ_REP) {
    rc = flow_fmq_consume_with_delivery(app->endpoint, NULL, NULL, message, NULL);
  }
  return rc;
}

static int flow_fmq_app_contract_configure(turbo_flow_fmq_app_t *app) {
  const char *receive_operation;
  const char *send_operation;
  int auto_reply;
  int rc;
  if (!app) return TURBO_EINVAL;
  rc = flow_fmq_app_pattern_contract(app->pattern, &receive_operation, &send_operation,
                                     &auto_reply);
  if (rc != TURBO_OK) return rc;
  if (receive_operation && !send_operation && !app->on_message) return TURBO_EINVAL;
  if (!receive_operation && app->on_message) return TURBO_EINVAL;
  if (auto_reply && !app->on_message) return TURBO_EINVAL;
  app->can_send = send_operation != NULL && !auto_reply;
  return TURBO_OK;
}

static int flow_fmq_app_allocate(turbo_flow_fmq_pattern_t pattern,
                                 const turbo_flow_fmq_app_options_t *options,
                                 turbo_flow_fmq_app_t **out) {
  turbo_flow_fmq_app_t *app;
  if (out) *out = NULL;
  if (!out || !flow_fmq_app_options_validate(options)) return TURBO_EINVAL;
  app = (turbo_flow_fmq_app_t *)calloc(1, sizeof(*app));
  if (!app) return TURBO_ENOMEM;
  if (flow_fmq_app_async_requests_init(&app->async_requests) != TURBO_OK) {
    free(app);
    return TURBO_ENOMEM;
  }
  turbo_mutex_init(&app->async_mutex);
  turbo_cond_init(&app->async_changed);
  app->async_initialized = 1;
  app->pattern = pattern;
  app->on_message = options->on_message;
  app->message_ctx = options->message_ctx;
  if (flow_fmq_app_contract_configure(app) != TURBO_OK) {
    flow_fmq_app_async_requests_destroy(&app->async_requests);
    turbo_cond_destroy(&app->async_changed);
    turbo_mutex_destroy(&app->async_mutex);
    free(app);
    return TURBO_EINVAL;
  }
  *out = app;
  return TURBO_OK;
}

int flow_fmq_app_create_endpoint_internal(
    const char *name, const turbo_flow_fmq_config_t *endpoint,
    const turbo_flow_fmq_fanout_config_t *fanout,
    const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out) {
  turbo_flow_fmq_app_t *app = NULL;
  int rc;
  if (out) *out = NULL;
  if (!name || !name[0] || !endpoint || !execution) return TURBO_EINVAL;
  rc = turbo_flow_coronet_execution_binding_validate(execution);
  if (rc != TURBO_OK) return rc;
  if (execution->kind == TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT)
    return TURBO_EINVAL;
  rc = flow_fmq_app_allocate(endpoint->pattern, options, &app);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_register_adapter_internal(NULL, name, endpoint, fanout, execution, security,
                                          &app->endpoint);
  if (rc != TURBO_OK) {
    turbo_flow_fmq_app_destroy(app);
    return rc;
  }
  app->endpoint->ingress_dispatch = flow_fmq_app_ingress_dispatch;
  app->endpoint->ingress_dispatch_ctx = app;
  *out = app;
  return TURBO_OK;
}

static int flow_fmq_app_create_internal(
    const turbo_flow_fmq_config_t *endpoint, const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out) {
  static const char *const endpoint_name = "fmq.app.endpoint";
  const turbo_flow_coronet_execution_binding_t private_execution = {
      sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
  return flow_fmq_app_create_endpoint_internal(endpoint_name, endpoint, NULL, options,
                                               execution ? execution : &private_execution,
                                               security, out);
}

int turbo_flow_fmq_app_create(const turbo_flow_fmq_config_t *endpoint,
                              const turbo_flow_fmq_app_options_t *options,
                              turbo_flow_fmq_app_t **out) {
  return flow_fmq_app_create_internal(endpoint, options, NULL, NULL, out);
}

int turbo_flow_fmq_app_create_ex(
    const turbo_flow_fmq_config_t *endpoint, const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_coronet_execution_binding_t *execution, turbo_flow_fmq_app_t **out) {
  if (!execution) {
    if (out) *out = NULL;
    return TURBO_EINVAL;
  }
  return flow_fmq_app_create_internal(endpoint, options, execution, NULL, out);
}

int turbo_flow_fmq_app_create_secure(
    const turbo_flow_fmq_config_t *endpoint, const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out) {
  if (!security) {
    if (out) *out = NULL;
    return TURBO_EINVAL;
  }
  return flow_fmq_app_create_internal(endpoint, options, NULL, security, out);
}

int turbo_flow_fmq_app_create_secure_ex(
    const turbo_flow_fmq_config_t *endpoint, const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out) {
  if (!execution || !security) {
    if (out) *out = NULL;
    return TURBO_EINVAL;
  }
  return flow_fmq_app_create_internal(endpoint, options, execution, security, out);
}

static int flow_fmq_app_create_resolved_internal(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out,
    turbo_flow_config_error_t *error) {
  if (out) *out = NULL;
  if (!resolved || !adapter_name || !adapter_name[0] || !out || !error ||
      error->size < sizeof(*error) || !flow_fmq_app_options_validate(options))
    return TURBO_EINVAL;
  return flow_fmq_app_create_resolved_endpoint_internal(resolved, adapter_name, options, security,
                                                        out, error);
}

int turbo_flow_fmq_app_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_app_options_t *options, turbo_flow_fmq_app_t **out,
    turbo_flow_config_error_t *error) {
  return flow_fmq_app_create_resolved_internal(resolved, adapter_name, options, NULL, out, error);
}

int turbo_flow_fmq_app_create_resolved_secure(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out,
    turbo_flow_config_error_t *error) {
  if (!security) return TURBO_EINVAL;
  return flow_fmq_app_create_resolved_internal(resolved, adapter_name, options, security, out,
                                               error);
}

int turbo_flow_fmq_app_start(turbo_flow_fmq_app_t *app) {
  int rc;
  if (!app || !app->endpoint) return TURBO_EINVAL;
  if (app->started) return TURBO_EALREADY;
  app->endpoint->ingress_dispatch = flow_fmq_app_ingress_dispatch;
  app->endpoint->ingress_dispatch_ctx = app;
  app->endpoint->start_refs = 1;
  atomic_store_explicit(&app->endpoint->quiesced, 0, memory_order_release);
  rc = flow_fmq_start_resources(app->endpoint);
  if (rc != TURBO_OK) {
    app->endpoint->start_refs = 0;
    return rc;
  }
  app->started = 1;
  app->started_once = 1;
  if (!app->async_configured) return TURBO_OK;
  turbo_mutex_lock(&app->async_mutex);
  app->async_accepting = 1;
  turbo_mutex_unlock(&app->async_mutex);
  rc = turbo_thread_create(&app->async_thread, flow_fmq_app_async_worker, app);
  if (rc == TURBO_OK) {
    app->async_thread_started = 1;
    return TURBO_OK;
  }
  turbo_mutex_lock(&app->async_mutex);
  app->async_accepting = 0;
  turbo_mutex_unlock(&app->async_mutex);
  flow_fmq_stop(app->endpoint, NULL, NULL);
  app->started = 0;
  return rc;
}

int turbo_flow_fmq_app_stop(turbo_flow_fmq_app_t *app) {
  int rc;
  if (!app || !app->endpoint) return TURBO_EINVAL;
  if (!app->started) return TURBO_OK;
  if (app->async_configured) {
    turbo_mutex_lock(&app->async_mutex);
    app->async_accepting = 0;
    turbo_cond_broadcast(&app->async_changed);
    turbo_mutex_unlock(&app->async_mutex);
    if (app->async_thread_started) {
      rc = turbo_thread_join(&app->async_thread);
      if (rc != TURBO_OK) return rc;
      turbo_thread_destroy(&app->async_thread);
      app->async_thread_started = 0;
    }
  }
  flow_fmq_stop(app->endpoint, NULL, NULL);
  app->started = 0;
  return TURBO_OK;
}

int turbo_flow_fmq_app_send_message(turbo_flow_fmq_app_t *app,
                                    const turbo_flow_msg_t *message) {
  if (!app || !app->endpoint || !message) return TURBO_EINVAL;
  if (!app->can_send) return TURBO_ENOTSUP;
  if (!app->started) return TURBO_EBUSY;
  return flow_fmq_consume_with_delivery(app->endpoint, NULL, NULL,
                                        (turbo_flow_msg_t *)message, NULL);
}

int turbo_flow_fmq_app_send(turbo_flow_fmq_app_t *app, const void *data, size_t data_size) {
  turbo_flow_msg_t message;
  int rc;
  if (!app || (!data && data_size > 0u)) return TURBO_EINVAL;
  turbo_flow_msg_init(&message);
  message.buffer = mem_get_buffer(mem_global(), data_size);
  if (!message.buffer) return TURBO_ENOMEM;
  if (data_size > 0u) memcpy(mem_buffer_data(message.buffer), data, data_size);
  mem_set_used(message.buffer, data_size);
  message.payload = tstr_v_from_buf(mem_buffer_data(message.buffer), data_size);
  rc = turbo_flow_fmq_app_send_message(app, &message);
  turbo_flow_msg_cleanup(&message);
  return rc;
}

typedef struct flow_fmq_app_batch_prepare_context_s {
  const turbo_flow_fmq_app_send_item_t *copied_items;
  const flow_fmq_app_async_request_t *retained_items;
  flow_fmq_send_batch_t *batch;
  mem_buffer_t *shared_payload;
  size_t prepared_payload_bytes;
} flow_fmq_app_batch_prepare_context_t;

static void flow_fmq_app_prepare_shared_batch_payload(
    flow_fmq_app_batch_prepare_context_t *prepare, size_t item_count) {
  uint64_t profile_started_ns;
  size_t total_size = 0u;
  unsigned char *destination;
  if (!prepare || !prepare->batch || !prepare->copied_items || item_count == 0u) return;
  for (size_t i = 0u; i < item_count; ++i) {
    const turbo_flow_fmq_app_send_item_t *item = &prepare->copied_items[i];
    if ((!item->data && item->data_size > 0u) ||
        item->data_size > TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES - total_size) {
      return;
    }
    total_size += item->data_size;
  }
  if (total_size == 0u) return;

  profile_started_ns = flow_fmq_profile_batch_step_begin(prepare->batch);
  prepare->shared_payload = mem_get_buffer(mem_global(), total_size);
  if (!prepare->shared_payload) return;
  destination = (unsigned char *)mem_buffer_data(prepare->shared_payload);
  for (size_t i = 0u; i < item_count; ++i) {
    const turbo_flow_fmq_app_send_item_t *item = &prepare->copied_items[i];
    if (item->data_size > 0u) {
      memcpy(destination, item->data, item->data_size);
      destination += item->data_size;
    }
  }
  mem_set_used(prepare->shared_payload, total_size);
  flow_fmq_profile_batch_payload_prepared(prepare->batch, profile_started_ns);
}

static int flow_fmq_app_prepare_batch_message(void *ctx, size_t index,
                                              turbo_flow_msg_t *message) {
  flow_fmq_app_batch_prepare_context_t *prepare =
      (flow_fmq_app_batch_prepare_context_t *)ctx;
  const void *data;
  size_t payload_offset;
  size_t data_size;
  uint64_t profile_started_ns;
  if (!prepare || !prepare->batch || !message) return TURBO_EINVAL;
  data = prepare->copied_items ? prepare->copied_items[index].data : NULL;
  data_size = prepare->copied_items ? prepare->copied_items[index].data_size
                                    : prepare->retained_items[index].data_size;
  if ((prepare->copied_items && !data && data_size > 0u) ||
      (prepare->retained_items && !prepare->retained_items[index].buffer)) {
    return TURBO_EINVAL;
  }
  if (data_size > TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES -
                      prepare->prepared_payload_bytes) {
    return TURBO_EMSGSIZE;
  }
  profile_started_ns = flow_fmq_profile_batch_step_begin(prepare->batch);
  payload_offset = prepare->prepared_payload_bytes;
  prepare->prepared_payload_bytes += data_size;
  message->buffer =
      prepare->shared_payload
          ? mem_buffer_retain(prepare->shared_payload)
          : (prepare->copied_items
                 ? mem_get_buffer(mem_global(), data_size)
                 : mem_buffer_retain(prepare->retained_items[index].buffer));
  if (!message->buffer) return TURBO_ENOMEM;
  if (prepare->copied_items && !prepare->shared_payload) {
    if (data_size > 0u) memcpy(mem_buffer_data(message->buffer), data, data_size);
    mem_set_used(message->buffer, data_size);
  }
  message->payload = tstr_v_from_buf(
      mem_buffer_data(message->buffer) + (prepare->shared_payload ? payload_offset : 0u),
      data_size);
  flow_fmq_profile_batch_payload_prepared(prepare->batch, profile_started_ns);
  return TURBO_OK;
}

static int flow_fmq_app_send_batch_internal(
    turbo_flow_fmq_app_t *app, const turbo_flow_fmq_app_send_item_t *copied_items,
    const flow_fmq_app_async_request_t *retained_items, size_t item_count,
    size_t *submitted) {
  flow_fmq_app_batch_prepare_context_t prepare_context;
  flow_fmq_send_batch_t *batch;
  flow_fmq_send_batch_t *previous_batch;
  int prepare_status = TURBO_OK;
  int commit_status;
  int wait_status = TURBO_OK;
  size_t prepared_messages = 0u;
  if (submitted) *submitted = 0u;
  if (!app || !app->endpoint || ((copied_items == NULL) == (retained_items == NULL)) ||
      item_count == 0u) {
    return TURBO_EINVAL;
  }
  if (!app->can_send) return TURBO_ENOTSUP;
  if (!app->started) return TURBO_EBUSY;
  if (app->pattern != TURBO_FLOW_FMQ_PUB && app->pattern != TURBO_FLOW_FMQ_PUSH &&
      app->pattern != TURBO_FLOW_FMQ_DEALER) {
    return TURBO_ENOTSUP;
  }
  if (item_count > TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_ITEMS) return TURBO_ERANGE;
  batch = flow_fmq_send_batch_create(app->endpoint, item_count);
  if (!batch) return TURBO_ENOMEM;
  prepare_context = (flow_fmq_app_batch_prepare_context_t){
      copied_items, retained_items, batch, NULL, 0u};
  flow_fmq_app_prepare_shared_batch_payload(&prepare_context, item_count);
  previous_batch = flow_fmq_active_send_batch;
  flow_fmq_active_send_batch = batch;
  {
    const uint64_t profile_started_ns = flow_fmq_profile_batch_step_begin(batch);
    for (size_t i = 0u; i < item_count; ++i) {
      turbo_flow_msg_t message;
      turbo_flow_msg_init(&message);
      prepare_status = flow_fmq_app_prepare_batch_message(&prepare_context, i, &message);
      if (prepare_status == TURBO_OK) {
        prepare_status =
            flow_fmq_consume_with_delivery(app->endpoint, NULL, NULL, &message, NULL);
      }
      turbo_flow_msg_cleanup(&message);
      if (prepare_status != TURBO_OK) break;
      prepared_messages = i + 1u;
    }
    flow_fmq_profile_batch_submitted(batch, profile_started_ns);
  }
  flow_fmq_active_send_batch = previous_batch;
  mem_buffer_release(prepare_context.shared_payload);
  prepare_context.shared_payload = NULL;
  if (prepared_messages != batch->count && prepare_status == TURBO_OK) {
    prepare_status = TURBO_EPROTO;
  }
  if (batch->count == 0u) {
    flow_fmq_send_batch_release(batch);
    return prepare_status;
  }
  commit_status = flow_fmq_enqueue_send_batch(batch);
  if (!batch->committed) {
    flow_fmq_discard_deferred_batch(batch, commit_status);
    flow_fmq_send_batch_release(batch);
    return prepare_status != TURBO_OK ? prepare_status : commit_status;
  }
  if (submitted) *submitted = batch->count;
  wait_status = flow_fmq_wait_send_batch(batch);
  flow_fmq_send_batch_release(batch);
  if (prepare_status != TURBO_OK) return prepare_status;
  if (commit_status != TURBO_OK) return commit_status;
  return wait_status;
}

int turbo_flow_fmq_app_send_batch(turbo_flow_fmq_app_t *app,
                                  const turbo_flow_fmq_app_send_item_t *items,
                                  size_t item_count, size_t *submitted) {
  return flow_fmq_app_send_batch_internal(app, items, NULL, item_count, submitted);
}

int turbo_flow_fmq_app_configure_async_send(
    turbo_flow_fmq_app_t *app, const turbo_flow_fmq_app_async_send_config_t *config) {
  flow_fmq_app_async_request_t *batch_requests;
  int rc;
  if (!app || !app->endpoint || !flow_fmq_app_async_config_validate(config))
    return TURBO_EINVAL;
  if (!flow_fmq_app_async_pattern_supported(app->pattern)) return TURBO_ENOTSUP;
  if (app->started || app->started_once) return TURBO_EBUSY;
  if (app->async_configured) return TURBO_EALREADY;
  batch_requests =
      (flow_fmq_app_async_request_t *)calloc(config->batch_size, sizeof(*batch_requests));
  if (!batch_requests) return TURBO_ENOMEM;
  rc = flow_fmq_app_async_requests_reserve(&app->async_requests, config->queue_capacity);
  if (rc != TURBO_OK) {
    free(batch_requests);
    return rc;
  }
  app->async_batch_requests = batch_requests;
  app->async_queue_capacity = config->queue_capacity;
  app->async_queue_capacity_bytes = config->queue_capacity_bytes;
  app->async_batch_size = config->batch_size;
  app->async_linger_ns = config->linger_ns;
  app->async_configured = 1;
  return TURBO_OK;
}

int turbo_flow_fmq_app_send_async(turbo_flow_fmq_app_t *app, const void *data,
                                  size_t data_size,
                                  turbo_flow_fmq_app_send_completion_fn completion, void *ctx) {
  flow_fmq_app_async_request_t request;
  size_t queued_items_before;
  size_t queued_items_after;
  int rc;
  if (!app || (!data && data_size > 0u)) return TURBO_EINVAL;
  if (!app->can_send || !flow_fmq_app_async_pattern_supported(app->pattern)) return TURBO_ENOTSUP;
  if (!app->async_configured) return TURBO_ENOTSUP;
  if (data_size > TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES) return TURBO_EMSGSIZE;
  if (data_size > app->async_queue_capacity_bytes) return TURBO_ENOSPC;
  memset(&request, 0, sizeof(request));
  request.buffer = mem_get_buffer(mem_global(), data_size);
  if (!request.buffer) return TURBO_ENOMEM;
  if (data_size > 0u) memcpy(mem_buffer_data(request.buffer), data, data_size);
  mem_set_used(request.buffer, data_size);
  request.data_size = data_size;
  request.completion = completion;
  request.completion_ctx = ctx;

  turbo_mutex_lock(&app->async_mutex);
  queued_items_before = flow_fmq_app_async_requests_size(&app->async_requests);
  if (!app->async_accepting) {
    rc = TURBO_EBUSY;
  } else if (queued_items_before >= app->async_queue_capacity ||
             data_size > app->async_queue_capacity_bytes - app->async_queued_bytes) {
    rc = TURBO_ENOSPC;
  } else {
    rc = flow_fmq_app_async_requests_push_back(&app->async_requests, request);
    if (rc == TURBO_OK) {
      app->async_queued_bytes += data_size;
      queued_items_after = flow_fmq_app_async_requests_size(&app->async_requests);
      /*
       * The worker waits on queue state, not on individual messages. Notify only
       * when a previously empty queue becomes visible or a partial batch reaches
       * its configured size; linger covers messages added between those edges.
       */
      if (queued_items_before == 0u ||
          (queued_items_before < app->async_batch_size &&
           queued_items_after >= app->async_batch_size))
        turbo_cond_signal(&app->async_changed);
    }
  }
  turbo_mutex_unlock(&app->async_mutex);
  if (rc != TURBO_OK) mem_buffer_release(request.buffer);
  return rc;
}

int turbo_flow_fmq_app_message_set_payload_copy(turbo_flow_msg_t *message, const void *data,
                                                size_t data_size) {
  tstr_t payload;
  if (!message || (!data && data_size > 0u)) return TURBO_EINVAL;
  payload = tstr_new_len(data, data_size);
  if (!payload) return TURBO_ENOMEM;
  tstr_freep(&message->owned_payload);
  message->owned_payload = payload;
  message->payload = tstr_to_v(payload);
  return TURBO_OK;
}

void turbo_flow_fmq_app_destroy(turbo_flow_fmq_app_t *app) {
  if (!app) return;
  if (app->started) (void)turbo_flow_fmq_app_stop(app);
  if (app->async_thread_started) {
    turbo_mutex_lock(&app->async_mutex);
    app->async_accepting = 0;
    turbo_cond_broadcast(&app->async_changed);
    turbo_mutex_unlock(&app->async_mutex);
    if (turbo_thread_join(&app->async_thread) != TURBO_OK) return;
    turbo_thread_destroy(&app->async_thread);
    app->async_thread_started = 0;
  }
  flow_fmq_shutdown(app->endpoint);
  app->endpoint = NULL;
  flow_fmq_app_async_destroy(app);
  free(app);
}
