#ifndef FLOWMQ_PATTERN_STATE_H
#define FLOWMQ_PATTERN_STATE_H

#include "flowmq_pattern.h"

#include <stdint.h>

typedef enum flowmq_pattern_phase_e {
  FLOWMQ_PATTERN_PHASE_READY = 0,
  FLOWMQ_PATTERN_PHASE_REQ_WAIT_REPLY,
  FLOWMQ_PATTERN_PHASE_REP_SEND_REPLY
} flowmq_pattern_phase_t;

typedef struct flowmq_pattern_state_s {
  const flowmq_pattern_desc_t *desc;
  flowmq_protocol_pattern_t pattern;
  uint8_t phase;
  uint8_t sending_multipart;
  uint8_t receiving_multipart;
} flowmq_pattern_state_t;

#define FLOWMQ_PATTERN_STATE_INIT {NULL, 0u, 0u, 0u, 0u}

int flowmq_pattern_can_send(flowmq_protocol_pattern_t pattern);
int flowmq_pattern_can_receive(flowmq_protocol_pattern_t pattern);
int flowmq_pattern_state_init(flowmq_pattern_state_t *state,
                              flowmq_protocol_pattern_t pattern);
int flowmq_pattern_state_send_validate(const flowmq_pattern_state_t *state);
void flowmq_pattern_state_send_commit(flowmq_pattern_state_t *state, int more);
int flowmq_pattern_state_receive_validate(const flowmq_pattern_state_t *state);
void flowmq_pattern_state_receive_commit(flowmq_pattern_state_t *state, int more);
void flowmq_pattern_state_cancel_transaction(flowmq_pattern_state_t *state);

#endif /* FLOWMQ_PATTERN_STATE_H */
