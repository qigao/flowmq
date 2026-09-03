#include "flowmq_flow_control.h"

#include "turbo_error.h"

#include <limits.h>
#include <string.h>

#define FLOWMQ_FLOW_CONTROL_NS_PER_MS UINT64_C(1000000)

static uint64_t flowmq_flow_control_deadline(uint64_t now_ns,
                                             uint32_t interval_ms) {
  uint64_t interval_ns = (uint64_t)interval_ms * FLOWMQ_FLOW_CONTROL_NS_PER_MS;
  return now_ns > UINT64_MAX - interval_ns ? UINT64_MAX : now_ns + interval_ns;
}

uint32_t flowmq_flow_control_default_quantum(uint64_t receive_window) {
  uint64_t quantum;
  if (receive_window == 0u) return 0u;
  quantum = receive_window / 4u;
  if (quantum < FLOWMQ_FLOW_CONTROL_MIN_QUANTUM)
    quantum = FLOWMQ_FLOW_CONTROL_MIN_QUANTUM;
  if (quantum > receive_window) quantum = receive_window;
  if (quantum > UINT32_MAX) quantum = UINT32_MAX;
  return (uint32_t)quantum;
}

int flowmq_flow_control_init_local(flowmq_flow_control_t *state,
                                   uint64_t session_generation,
                                   uint64_t receive_window,
                                   uint32_t flow_update_quantum,
                                   uint32_t flow_update_interval_ms) {
  if (!state) return TURBO_EINVAL;
  if (state->local_initialized || state->remote_initialized) return TURBO_EALREADY;
  if (session_generation == 0u || receive_window == 0u ||
      flow_update_quantum == 0u || flow_update_quantum > receive_window ||
      flow_update_interval_ms == 0u) {
    return TURBO_EINVAL;
  }
  memset(state, 0, sizeof(*state));
  state->local_generation = session_generation;
  state->receive_window = receive_window;
  state->advertised_max_data = receive_window;
  state->flow_update_quantum = flow_update_quantum;
  state->flow_update_interval_ms = flow_update_interval_ms;
  state->local_initialized = 1u;
  return TURBO_OK;
}

int flowmq_flow_control_make_settings(const flowmq_flow_control_t *state,
                                      uint32_t max_frame_size,
                                      flowmq_protocol_settings_t *settings) {
  if (!state || !settings || max_frame_size == 0u) return TURBO_EINVAL;
  if (!state->local_initialized) return TURBO_EBUSY;
  *settings = (flowmq_protocol_settings_t){
      .capabilities = FLOWMQ_PROTOCOL_CAP_FLOW_CREDIT,
      .max_frame_size = max_frame_size,
      .session_generation = state->local_generation,
      .initial_max_data = state->advertised_max_data,
      .flow_update_quantum = state->flow_update_quantum,
      .flow_update_interval_ms = state->flow_update_interval_ms};
  return TURBO_OK;
}

int flowmq_flow_control_apply_settings(flowmq_flow_control_t *state,
                                       const flowmq_protocol_settings_t *settings) {
  unsigned char payload[FLOWMQ_PROTOCOL_SETTINGS_PAYLOAD_SIZE];
  int status;
  if (!state || !settings) return TURBO_EINVAL;
  if (!state->local_initialized) return TURBO_EBUSY;
  if (state->remote_initialized) return TURBO_EPROTO;
  status = flowmq_protocol_settings_encode(settings, payload);
  if (status != TURBO_OK) return status;
  state->remote_generation = settings->session_generation;
  state->remote_max_data = settings->initial_max_data;
  state->remote_receive_window = settings->initial_max_data;
  state->remote_max_frame_size = settings->max_frame_size;
  state->remote_flow_update_quantum = settings->flow_update_quantum;
  state->remote_flow_update_interval_ms = settings->flow_update_interval_ms;
  state->remote_initialized = 1u;
  return TURBO_OK;
}

int flowmq_flow_control_send_check(const flowmq_flow_control_t *state,
                                   size_t payload_size) {
  if (!state) return TURBO_EINVAL;
  if (!state->remote_initialized) return TURBO_EBUSY;
  if ((uint64_t)payload_size > state->remote_max_frame_size) return TURBO_EMSGSIZE;
  return flowmq_flow_control_send_credit_check(state, payload_size);
}

int flowmq_flow_control_send_credit_check(const flowmq_flow_control_t *state,
                                          size_t payload_size) {
  uint64_t size = (uint64_t)payload_size;
  if (!state) return TURBO_EINVAL;
  if (!state->remote_initialized) return TURBO_EBUSY;
  if (state->sent_data > state->remote_max_data ||
      size > state->remote_max_data - state->sent_data) {
    return TURBO_ENOBUFS;
  }
  return TURBO_OK;
}

int flowmq_flow_control_send_commit(flowmq_flow_control_t *state,
                                    size_t payload_size) {
  int status = flowmq_flow_control_send_check(state, payload_size);
  if (status != TURBO_OK) return status;
  state->sent_data += (uint64_t)payload_size;
  return TURBO_OK;
}

int flowmq_flow_control_send_credit_commit(flowmq_flow_control_t *state,
                                           size_t payload_size) {
  int status = flowmq_flow_control_send_credit_check(state, payload_size);
  if (status != TURBO_OK) return status;
  state->sent_data += (uint64_t)payload_size;
  return TURBO_OK;
}

int flowmq_flow_control_receive_check(const flowmq_flow_control_t *state,
                                      size_t payload_size) {
  uint64_t size = (uint64_t)payload_size;
  if (!state) return TURBO_EINVAL;
  if (!state->local_initialized) return TURBO_EBUSY;
  if (state->received_data > state->advertised_max_data ||
      size > state->advertised_max_data - state->received_data) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

int flowmq_flow_control_receive_commit(flowmq_flow_control_t *state,
                                       size_t payload_size) {
  int status = flowmq_flow_control_receive_check(state, payload_size);
  if (status != TURBO_OK) return status;
  state->received_data += (uint64_t)payload_size;
  return TURBO_OK;
}

int flowmq_flow_control_consume(flowmq_flow_control_t *state,
                                size_t payload_size, uint64_t now_ns) {
  uint64_t size = (uint64_t)payload_size;
  if (!state) return TURBO_EINVAL;
  if (!state->local_initialized) return TURBO_EBUSY;
  if (state->consumed_data > state->received_data ||
      size > state->received_data - state->consumed_data) {
    return TURBO_EPROTO;
  }
  if (size == 0u) return TURBO_OK;
  state->consumed_data += size;
  if (!state->update_pending) {
    state->update_pending = 1u;
    state->update_deadline_ns = flowmq_flow_control_deadline(
        now_ns, state->flow_update_interval_ms);
  }
  return TURBO_OK;
}

int flowmq_flow_control_next_update(const flowmq_flow_control_t *state,
                                    uint64_t now_ns,
                                    flowmq_protocol_flow_update_t *update) {
  uint64_t unadvertised;
  if (!state || !update) return TURBO_EINVAL;
  if (!state->local_initialized) return TURBO_EBUSY;
  if (!state->update_pending) return FLOWMQ_FLOW_CONTROL_NO_UPDATE;
  if (state->consumed_data < state->advertised_consumed_data) return TURBO_EPROTO;
  unadvertised = state->consumed_data - state->advertised_consumed_data;
  if (unadvertised < state->flow_update_quantum &&
      now_ns < state->update_deadline_ns) {
    return FLOWMQ_FLOW_CONTROL_NO_UPDATE;
  }
  if (state->consumed_data > UINT64_MAX - state->receive_window)
    return TURBO_ERANGE;
  *update = (flowmq_protocol_flow_update_t){
      .session_generation = state->local_generation,
      .consumed_data = state->consumed_data,
      .max_data = state->consumed_data + state->receive_window};
  return TURBO_OK;
}

int flowmq_flow_control_mark_update_sent(
    flowmq_flow_control_t *state,
    const flowmq_protocol_flow_update_t *update) {
  uint64_t expected_max;
  if (!state || !update) return TURBO_EINVAL;
  if (!state->local_initialized || !state->update_pending) return TURBO_EBUSY;
  if (state->consumed_data > UINT64_MAX - state->receive_window)
    return TURBO_ERANGE;
  expected_max = state->consumed_data + state->receive_window;
  if (update->session_generation != state->local_generation ||
      update->consumed_data != state->consumed_data ||
      update->max_data != expected_max ||
      update->max_data < state->advertised_max_data) {
    return TURBO_EPROTO;
  }
  state->advertised_consumed_data = update->consumed_data;
  state->advertised_max_data = update->max_data;
  state->update_deadline_ns = 0u;
  state->update_pending = 0u;
  return TURBO_OK;
}

int flowmq_flow_control_apply_remote_update(
    flowmq_flow_control_t *state,
    const flowmq_protocol_flow_update_t *update) {
  unsigned char payload[FLOWMQ_PROTOCOL_FLOW_UPDATE_PAYLOAD_SIZE];
  int status;
  if (!state || !update) return TURBO_EINVAL;
  if (!state->remote_initialized) return TURBO_EBUSY;
  status = flowmq_protocol_flow_update_encode(update, payload);
  if (status != TURBO_OK) return status;
  if (update->session_generation != state->remote_generation ||
      update->consumed_data < state->remote_consumed_data ||
      update->consumed_data > state->sent_data ||
      update->consumed_data > UINT64_MAX - state->remote_receive_window ||
      update->max_data !=
          update->consumed_data + state->remote_receive_window ||
      update->max_data < state->remote_max_data) {
    return TURBO_EPROTO;
  }
  state->remote_consumed_data = update->consumed_data;
  state->remote_max_data = update->max_data;
  return TURBO_OK;
}
