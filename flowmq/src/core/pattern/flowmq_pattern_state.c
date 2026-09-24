#include "flowmq_pattern_state.h"

#include "salts_error.h"

#include <string.h>

int flowmq_pattern_can_send(flowmq_protocol_pattern_t pattern) {
  return flowmq_pattern_has_capability(pattern, FLOWMQ_PATTERN_CAP_API_SEND);
}

int flowmq_pattern_can_receive(flowmq_protocol_pattern_t pattern) {
  return flowmq_pattern_has_capability(pattern, FLOWMQ_PATTERN_CAP_API_RECV);
}

int flowmq_pattern_state_init(flowmq_pattern_state_t *state,
                              flowmq_protocol_pattern_t pattern) {
  const flowmq_pattern_desc_t *desc = flowmq_pattern_descriptor(pattern);
  if (state == NULL || desc == NULL) return SALTS_EINVAL;
  memset(state, 0, sizeof(*state));
  state->desc = desc;
  state->pattern = pattern;
  return SALTS_OK;
}

int flowmq_pattern_state_send_validate(const flowmq_pattern_state_t *state) {
  if (state == NULL || state->desc == NULL ||
      (state->desc->capabilities & FLOWMQ_PATTERN_CAP_API_SEND) == 0u)
    return SALTS_ENOTSUP;
  if (state->receiving_multipart) return SALTS_EPROTO;
  if (state->sending_multipart) return SALTS_OK;
  if (state->desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ &&
      state->phase != FLOWMQ_PATTERN_PHASE_READY)
    return SALTS_EPROTO;
  if (state->desc->fsm_class == FLOWMQ_PATTERN_FSM_REP &&
      state->phase != FLOWMQ_PATTERN_PHASE_REP_SEND_REPLY)
    return SALTS_EPROTO;
  return SALTS_OK;
}

void flowmq_pattern_state_send_commit(flowmq_pattern_state_t *state, int more) {
  if (state == NULL || state->desc == NULL) return;
  state->sending_multipart = more != 0;
  if (more) return;
  if (state->desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ)
    state->phase = FLOWMQ_PATTERN_PHASE_REQ_WAIT_REPLY;
  else if (state->desc->fsm_class == FLOWMQ_PATTERN_FSM_REP)
    state->phase = FLOWMQ_PATTERN_PHASE_READY;
}

int flowmq_pattern_state_receive_validate(const flowmq_pattern_state_t *state) {
  if (state == NULL || state->desc == NULL ||
      (state->desc->capabilities & FLOWMQ_PATTERN_CAP_API_RECV) == 0u)
    return SALTS_ENOTSUP;
  if (state->sending_multipart) return SALTS_EPROTO;
  if (state->receiving_multipart) return SALTS_OK;
  if (state->desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ &&
      state->phase != FLOWMQ_PATTERN_PHASE_REQ_WAIT_REPLY)
    return SALTS_EPROTO;
  if (state->desc->fsm_class == FLOWMQ_PATTERN_FSM_REP &&
      state->phase != FLOWMQ_PATTERN_PHASE_READY)
    return SALTS_EPROTO;
  return SALTS_OK;
}

void flowmq_pattern_state_receive_commit(flowmq_pattern_state_t *state, int more) {
  if (state == NULL || state->desc == NULL) return;
  state->receiving_multipart = more != 0;
  if (more) return;
  if (state->desc->fsm_class == FLOWMQ_PATTERN_FSM_REQ)
    state->phase = FLOWMQ_PATTERN_PHASE_READY;
  else if (state->desc->fsm_class == FLOWMQ_PATTERN_FSM_REP)
    state->phase = FLOWMQ_PATTERN_PHASE_REP_SEND_REPLY;
}

void flowmq_pattern_state_cancel_transaction(flowmq_pattern_state_t *state) {
  if (state == NULL) return;
  state->sending_multipart = 0u;
  state->receiving_multipart = 0u;
  state->phase = FLOWMQ_PATTERN_PHASE_READY;
}
