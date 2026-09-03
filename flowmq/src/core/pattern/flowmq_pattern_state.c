#include "flowmq_pattern_state.h"

#include "turbo_error.h"

#include <string.h>

int flowmq_pattern_can_send(flowmq_protocol_pattern_t pattern) {
  if (pattern < FLOWMQ_PROTOCOL_PUB || pattern > FLOWMQ_PROTOCOL_XSUB) return 0;
  return pattern != FLOWMQ_PROTOCOL_SUB && pattern != FLOWMQ_PROTOCOL_PULL;
}

int flowmq_pattern_can_receive(flowmq_protocol_pattern_t pattern) {
  if (pattern < FLOWMQ_PROTOCOL_PUB || pattern > FLOWMQ_PROTOCOL_XSUB) return 0;
  return pattern != FLOWMQ_PROTOCOL_PUB && pattern != FLOWMQ_PROTOCOL_PUSH;
}

int flowmq_pattern_state_init(flowmq_pattern_state_t *state,
                              flowmq_protocol_pattern_t pattern) {
  if (state == NULL || pattern < FLOWMQ_PROTOCOL_PUB ||
      pattern > FLOWMQ_PROTOCOL_XSUB)
    return TURBO_EINVAL;
  memset(state, 0, sizeof(*state));
  state->pattern = pattern;
  return TURBO_OK;
}

int flowmq_pattern_state_send_validate(const flowmq_pattern_state_t *state) {
  if (state == NULL || !flowmq_pattern_can_send(state->pattern))
    return TURBO_ENOTSUP;
  if (state->receiving_multipart) return TURBO_EPROTO;
  if (state->sending_multipart) return TURBO_OK;
  if (state->pattern == FLOWMQ_PROTOCOL_REQ &&
      state->phase != FLOWMQ_PATTERN_PHASE_READY)
    return TURBO_EPROTO;
  if (state->pattern == FLOWMQ_PROTOCOL_REP &&
      state->phase != FLOWMQ_PATTERN_PHASE_REP_SEND_REPLY)
    return TURBO_EPROTO;
  return TURBO_OK;
}

void flowmq_pattern_state_send_commit(flowmq_pattern_state_t *state, int more) {
  if (state == NULL) return;
  state->sending_multipart = more != 0;
  if (more) return;
  if (state->pattern == FLOWMQ_PROTOCOL_REQ)
    state->phase = FLOWMQ_PATTERN_PHASE_REQ_WAIT_REPLY;
  else if (state->pattern == FLOWMQ_PROTOCOL_REP)
    state->phase = FLOWMQ_PATTERN_PHASE_READY;
}

int flowmq_pattern_state_receive_validate(const flowmq_pattern_state_t *state) {
  if (state == NULL || !flowmq_pattern_can_receive(state->pattern))
    return TURBO_ENOTSUP;
  if (state->sending_multipart) return TURBO_EPROTO;
  if (state->receiving_multipart) return TURBO_OK;
  if (state->pattern == FLOWMQ_PROTOCOL_REQ &&
      state->phase != FLOWMQ_PATTERN_PHASE_REQ_WAIT_REPLY)
    return TURBO_EPROTO;
  if (state->pattern == FLOWMQ_PROTOCOL_REP &&
      state->phase != FLOWMQ_PATTERN_PHASE_READY)
    return TURBO_EPROTO;
  return TURBO_OK;
}

void flowmq_pattern_state_receive_commit(flowmq_pattern_state_t *state, int more) {
  if (state == NULL) return;
  state->receiving_multipart = more != 0;
  if (more) return;
  if (state->pattern == FLOWMQ_PROTOCOL_REQ)
    state->phase = FLOWMQ_PATTERN_PHASE_READY;
  else if (state->pattern == FLOWMQ_PROTOCOL_REP)
    state->phase = FLOWMQ_PATTERN_PHASE_REP_SEND_REPLY;
}

void flowmq_pattern_state_cancel_transaction(flowmq_pattern_state_t *state) {
  if (state == NULL) return;
  state->sending_multipart = 0u;
  state->receiving_multipart = 0u;
  state->phase = FLOWMQ_PATTERN_PHASE_READY;
}
