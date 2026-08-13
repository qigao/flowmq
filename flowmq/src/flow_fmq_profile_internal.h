#ifndef FLOW_FMQ_PROFILE_INTERNAL_H
#define FLOW_FMQ_PROFILE_INTERNAL_H

#include "turbo_flow_fmq.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Benchmark-only aggregate for the synchronous FlowMQ send handoff. */
typedef struct flow_fmq_send_profile_snapshot_s {
  size_t size;
  uint64_t samples;
  uint64_t post_samples;
  uint64_t socket_samples;
  uint64_t socket_cpu_samples;
  uint64_t enqueue_sum_ns;
  uint64_t owner_wait_sum_ns;
  uint64_t post_call_sum_ns;
  uint64_t owner_dispatch_sum_ns;
  uint64_t socket_send_sum_ns;
  uint64_t socket_send_max_ns;
  uint64_t socket_send_slow_samples;
  uint64_t socket_thread_cpu_sum_ns;
  uint64_t socket_estimated_off_cpu_sum_ns;
  uint64_t completion_sum_ns;
  uint64_t waiter_wake_sum_ns;
  uint64_t total_sum_ns;
  uint64_t total_max_ns;
  uint64_t total_slow_samples;
  uint64_t batch_socket_calls;
  uint64_t batch_socket_cpu_samples;
  uint64_t batch_socket_frames;
  uint64_t batch_socket_iov_segments;
  uint64_t batch_socket_send_sum_ns;
  uint64_t batch_socket_send_max_ns;
  uint64_t batch_socket_send_slow_samples;
  uint64_t batch_socket_thread_cpu_sum_ns;
  uint64_t batch_socket_estimated_off_cpu_sum_ns;
  uint64_t batch_samples;
  uint64_t batch_post_samples;
  uint64_t batch_build_sum_ns;
  uint64_t batch_build_max_ns;
  uint64_t batch_build_slow_samples;
  uint64_t batch_payload_prepare_sum_ns;
  uint64_t batch_payload_prepare_max_ns;
  uint64_t batch_payload_prepare_slow_samples;
  uint64_t batch_submit_sum_ns;
  uint64_t batch_adapter_consume_sum_ns;
  uint64_t batch_enqueue_prepare_sum_ns;
  uint64_t batch_owner_wait_sum_ns;
  uint64_t batch_owner_wait_max_ns;
  uint64_t batch_owner_wait_slow_samples;
  uint64_t batch_post_call_sum_ns;
  uint64_t batch_owner_work_sum_ns;
  uint64_t batch_owner_work_max_ns;
  uint64_t batch_owner_work_slow_samples;
  uint64_t batch_waiter_wake_sum_ns;
  uint64_t batch_waiter_wake_max_ns;
  uint64_t batch_waiter_wake_slow_samples;
  uint64_t batch_total_sum_ns;
  uint64_t batch_total_max_ns;
  uint64_t batch_total_slow_samples;
} flow_fmq_send_profile_snapshot_t;

#define FLOW_FMQ_SEND_PROFILE_SNAPSHOT_INIT \
  {sizeof(flow_fmq_send_profile_snapshot_t), 0u}

/**
 * Reset process-wide benchmark counters. The benchmark must ensure no profiled send is in flight.
 */
CXX_C_API void flow_fmq_send_profile_reset(void);

/** Enable or disable timestamp capture for subsequently submitted send requests. */
CXX_C_API void flow_fmq_send_profile_set_enabled(int enabled);

/** Copy the current process-wide aggregate into caller-owned storage. */
CXX_C_API int flow_fmq_send_profile_snapshot(flow_fmq_send_profile_snapshot_t *out);

#ifdef __cplusplus
}
#endif

#endif /* FLOW_FMQ_PROFILE_INTERNAL_H */
