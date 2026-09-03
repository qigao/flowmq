#ifndef FLOWMQ_FLOW_CONTROL_H
#define FLOWMQ_FLOW_CONTROL_H

#include "flowmq_protocol.h"

#include <stddef.h>
#include <stdint.h>

#define FLOWMQ_FLOW_CONTROL_NO_UPDATE 1
#define FLOWMQ_FLOW_CONTROL_MIN_QUANTUM (64u * 1024u)
#define FLOWMQ_FLOW_CONTROL_DEFAULT_UPDATE_INTERVAL_MS 10u

typedef struct flowmq_flow_control_s {
  uint64_t local_generation;
  uint64_t receive_window;
  uint64_t received_data;
  uint64_t consumed_data;
  uint64_t advertised_consumed_data;
  uint64_t advertised_max_data;
  uint64_t update_deadline_ns;
  uint64_t remote_generation;
  uint64_t sent_data;
  uint64_t remote_consumed_data;
  uint64_t remote_max_data;
  uint64_t remote_receive_window;
  uint32_t flow_update_quantum;
  uint32_t flow_update_interval_ms;
  uint32_t remote_max_frame_size;
  uint32_t remote_flow_update_quantum;
  uint32_t remote_flow_update_interval_ms;
  uint8_t local_initialized;
  uint8_t remote_initialized;
  uint8_t update_pending;
} flowmq_flow_control_t;

#define FLOWMQ_FLOW_CONTROL_INIT {0}

uint32_t flowmq_flow_control_default_quantum(uint64_t receive_window);
int flowmq_flow_control_init_local(flowmq_flow_control_t *state,
                                   uint64_t session_generation,
                                   uint64_t receive_window,
                                   uint32_t flow_update_quantum,
                                   uint32_t flow_update_interval_ms);
int flowmq_flow_control_make_settings(const flowmq_flow_control_t *state,
                                      uint32_t max_frame_size,
                                      flowmq_protocol_settings_t *settings);
int flowmq_flow_control_apply_settings(flowmq_flow_control_t *state,
                                       const flowmq_protocol_settings_t *settings);
int flowmq_flow_control_send_check(const flowmq_flow_control_t *state,
                                   size_t payload_size);
int flowmq_flow_control_send_credit_check(const flowmq_flow_control_t *state,
                                          size_t payload_size);
int flowmq_flow_control_send_commit(flowmq_flow_control_t *state,
                                    size_t payload_size);
int flowmq_flow_control_send_credit_commit(flowmq_flow_control_t *state,
                                           size_t payload_size);
int flowmq_flow_control_receive_check(const flowmq_flow_control_t *state,
                                      size_t payload_size);
int flowmq_flow_control_receive_commit(flowmq_flow_control_t *state,
                                       size_t payload_size);
int flowmq_flow_control_consume(flowmq_flow_control_t *state,
                                size_t payload_size, uint64_t now_ns);
int flowmq_flow_control_next_update(const flowmq_flow_control_t *state,
                                    uint64_t now_ns,
                                    flowmq_protocol_flow_update_t *update);
int flowmq_flow_control_mark_update_sent(
    flowmq_flow_control_t *state,
    const flowmq_protocol_flow_update_t *update);
int flowmq_flow_control_apply_remote_update(
    flowmq_flow_control_t *state,
    const flowmq_protocol_flow_update_t *update);

#endif /* FLOWMQ_FLOW_CONTROL_H */
