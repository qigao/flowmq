#include "flowmq_peer_state.h"
#include "flowmq_peer_state_schema.h"

#include "salts_error.h"

#include <string.h>

typedef struct flowmq_peer_lifecycle_desc_s {
  uint8_t state;
  uint8_t allowed_next_mask;
} flowmq_peer_lifecycle_desc_t;

typedef struct flowmq_peer_handshake_desc_s {
  uint8_t event;
  uint8_t prerequisite_mask;
} flowmq_peer_handshake_desc_t;

typedef struct flowmq_peer_write_desc_s {
  uint8_t lane;
  uint8_t completion_event;
} flowmq_peer_write_desc_t;

#define FLOWMQ_PEER_LIFECYCLE_ROW(state_value, next_mask_value)   [state_value] = {state_value, next_mask_value},
static const flowmq_peer_lifecycle_desc_t
    FLOWMQ_PEER_LIFECYCLE_DESCRIPTORS[FLOWMQ_PEER_LIFECYCLE_COUNT] = {
        Replay(FLOWMQ_PEER_LIFECYCLE_SCHEMA, FLOWMQ_PEER_LIFECYCLE_ROW)};
#undef FLOWMQ_PEER_LIFECYCLE_ROW

#define FLOWMQ_PEER_HANDSHAKE_ROW(event_value, prerequisite_value)   {event_value, prerequisite_value},
static const flowmq_peer_handshake_desc_t FLOWMQ_PEER_HANDSHAKE_DESCRIPTORS[] = {
    Replay(FLOWMQ_PEER_HANDSHAKE_SCHEMA, FLOWMQ_PEER_HANDSHAKE_ROW)};
#undef FLOWMQ_PEER_HANDSHAKE_ROW

#define FLOWMQ_PEER_WRITE_ROW(lane_value, completion_value)   [lane_value] = {lane_value, completion_value},
static const flowmq_peer_write_desc_t
    FLOWMQ_PEER_WRITE_DESCRIPTORS[FLOWMQ_PEER_WRITE_COUNT] = {
        Replay(FLOWMQ_PEER_WRITE_SCHEMA, FLOWMQ_PEER_WRITE_ROW)};
#undef FLOWMQ_PEER_WRITE_ROW

#define FLOWMQ_PEER_LIFECYCLE_COUNT_ROW(state_value, next_mask_value) +1
#define FLOWMQ_PEER_HANDSHAKE_COUNT_ROW(event_value, prerequisite_value) +1
#define FLOWMQ_PEER_WRITE_COUNT_ROW(lane_value, completion_value) +1
enum {
  FLOWMQ_PEER_LIFECYCLE_SCHEMA_COUNT =
      0 Replay(FLOWMQ_PEER_LIFECYCLE_SCHEMA,
               FLOWMQ_PEER_LIFECYCLE_COUNT_ROW),
  FLOWMQ_PEER_HANDSHAKE_SCHEMA_COUNT =
      0 Replay(FLOWMQ_PEER_HANDSHAKE_SCHEMA,
               FLOWMQ_PEER_HANDSHAKE_COUNT_ROW),
  FLOWMQ_PEER_WRITE_SCHEMA_COUNT =
      0 Replay(FLOWMQ_PEER_WRITE_SCHEMA, FLOWMQ_PEER_WRITE_COUNT_ROW)
};
#undef FLOWMQ_PEER_LIFECYCLE_COUNT_ROW
#undef FLOWMQ_PEER_HANDSHAKE_COUNT_ROW
#undef FLOWMQ_PEER_WRITE_COUNT_ROW

_Static_assert(FLOWMQ_PEER_LIFECYCLE_SCHEMA_COUNT ==
                   FLOWMQ_PEER_LIFECYCLE_COUNT,
               "peer lifecycle schema must cover every lifecycle state");
_Static_assert(FLOWMQ_PEER_HANDSHAKE_SCHEMA_COUNT == 4,
               "peer handshake schema must cover all four progress facts");
_Static_assert(FLOWMQ_PEER_WRITE_SCHEMA_COUNT == FLOWMQ_PEER_WRITE_COUNT,
               "peer write schema must cover every write lane");

static const flowmq_peer_handshake_desc_t *
flowmq_peer_handshake_descriptor(uint8_t event) {
  for (size_t i = 0u;
       i < sizeof(FLOWMQ_PEER_HANDSHAKE_DESCRIPTORS) /
               sizeof(FLOWMQ_PEER_HANDSHAKE_DESCRIPTORS[0]);
       ++i) {
    if (FLOWMQ_PEER_HANDSHAKE_DESCRIPTORS[i].event == event)
      return &FLOWMQ_PEER_HANDSHAKE_DESCRIPTORS[i];
  }
  return NULL;
}

static int flowmq_peer_handshake_can_mark(const flowmq_peer_state_t *state,
                                          uint8_t event) {
  const flowmq_peer_handshake_desc_t *desc;
  if (state == NULL) return SALTS_EINVAL;
  desc = flowmq_peer_handshake_descriptor(event);
  if (desc == NULL) return SALTS_EINVAL;
  if ((state->handshake & event) != 0u) return SALTS_EALREADY;
  return (state->handshake & desc->prerequisite_mask) ==
                 desc->prerequisite_mask
             ? SALTS_OK
             : SALTS_EPROTO;
}

int flowmq_peer_state_transition(flowmq_peer_state_t *state,
                                 flowmq_peer_lifecycle_t next) {
  const flowmq_peer_lifecycle_desc_t *desc;
  uint8_t bit;
  if (state == NULL || state->lifecycle >= FLOWMQ_PEER_LIFECYCLE_COUNT ||
      next >= FLOWMQ_PEER_LIFECYCLE_COUNT)
    return SALTS_EINVAL;
  if (state->lifecycle == (uint8_t)next) return SALTS_EALREADY;
  desc = &FLOWMQ_PEER_LIFECYCLE_DESCRIPTORS[state->lifecycle];
  bit = FLOWMQ_PEER_LIFECYCLE_BIT(next);
  if ((desc->allowed_next_mask & bit) == 0u) return SALTS_EPROTO;
  state->lifecycle = (uint8_t)next;
  return SALTS_OK;
}

int flowmq_peer_state_allocate(flowmq_peer_state_t *state) {
  int status;
  if (state == NULL) return SALTS_EINVAL;
  if (state->lifecycle != FLOWMQ_PEER_LIFECYCLE_FREE)
    return SALTS_EALREADY;
  state->handshake = 0u;
  state->write_lane = FLOWMQ_PEER_WRITE_IDLE;
  status =
      flowmq_peer_state_transition(state, FLOWMQ_PEER_LIFECYCLE_ALLOCATED);
  return status;
}

int flowmq_peer_state_release(flowmq_peer_state_t *state) {
  int status;
  if (state == NULL) return SALTS_EINVAL;
  status = flowmq_peer_state_transition(state, FLOWMQ_PEER_LIFECYCLE_FREE);
  if (status != SALTS_OK) return status;
  state->handshake = 0u;
  state->write_lane = FLOWMQ_PEER_WRITE_IDLE;
  return SALTS_OK;
}

int flowmq_peer_state_is_used(const flowmq_peer_state_t *state) {
  return state != NULL &&
         state->lifecycle != FLOWMQ_PEER_LIFECYCLE_FREE;
}

int flowmq_peer_state_is_connected(const flowmq_peer_state_t *state) {
  return state != NULL &&
         state->lifecycle == FLOWMQ_PEER_LIFECYCLE_CONNECTED;
}

int flowmq_peer_state_is_retired(const flowmq_peer_state_t *state) {
  return state != NULL &&
         state->lifecycle == FLOWMQ_PEER_LIFECYCLE_RETIRED;
}

int flowmq_peer_state_needs_close_retry(const flowmq_peer_state_t *state) {
  return state != NULL &&
         state->lifecycle == FLOWMQ_PEER_LIFECYCLE_CLOSE_RETRY;
}

int flowmq_peer_state_handshake_mark(flowmq_peer_state_t *state,
                                     flowmq_peer_handshake_t event) {
  int status = flowmq_peer_handshake_can_mark(state, (uint8_t)event);
  if (status != SALTS_OK) return status;
  state->handshake |= (uint8_t)event;
  return SALTS_OK;
}

int flowmq_peer_state_handshake_has(const flowmq_peer_state_t *state,
                                    uint8_t mask) {
  return state != NULL && (state->handshake & mask) == mask;
}

int flowmq_peer_state_ready(const flowmq_peer_state_t *state) {
  return flowmq_peer_state_is_connected(state) &&
         state->handshake == FLOWMQ_PEER_HANDSHAKE_READY;
}

int flowmq_peer_state_write_begin(flowmq_peer_state_t *state,
                                  flowmq_peer_write_lane_t lane) {
  const flowmq_peer_write_desc_t *desc;
  int status;
  if (state == NULL || lane <= FLOWMQ_PEER_WRITE_IDLE ||
      lane >= FLOWMQ_PEER_WRITE_COUNT)
    return SALTS_EINVAL;
  if (!flowmq_peer_state_is_connected(state)) return SALTS_EBUSY;
  if (state->write_lane != FLOWMQ_PEER_WRITE_IDLE) return SALTS_EBUSY;
  desc = &FLOWMQ_PEER_WRITE_DESCRIPTORS[lane];
  if (desc->completion_event != 0u) {
    status = flowmq_peer_handshake_can_mark(state, desc->completion_event);
    if (status != SALTS_OK) return status;
  }
  state->write_lane = (uint8_t)lane;
  return SALTS_OK;
}

int flowmq_peer_state_write_complete(flowmq_peer_state_t *state,
                                     flowmq_peer_write_lane_t *completed_lane) {
  const flowmq_peer_write_desc_t *desc;
  flowmq_peer_write_lane_t lane;
  int status = SALTS_OK;
  if (state == NULL || state->write_lane == FLOWMQ_PEER_WRITE_IDLE ||
      state->write_lane >= FLOWMQ_PEER_WRITE_COUNT)
    return SALTS_EPROTO;
  lane = (flowmq_peer_write_lane_t)state->write_lane;
  desc = &FLOWMQ_PEER_WRITE_DESCRIPTORS[lane];
  state->write_lane = FLOWMQ_PEER_WRITE_IDLE;
  if (completed_lane != NULL) *completed_lane = lane;
  if (desc->completion_event != 0u)
    status = flowmq_peer_state_handshake_mark(
        state, (flowmq_peer_handshake_t)desc->completion_event);
  return status;
}

void flowmq_peer_state_write_cancel(flowmq_peer_state_t *state) {
  if (state != NULL) state->write_lane = FLOWMQ_PEER_WRITE_IDLE;
}

int flowmq_peer_state_write_idle(const flowmq_peer_state_t *state) {
  return state != NULL && state->write_lane == FLOWMQ_PEER_WRITE_IDLE;
}
