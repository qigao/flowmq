#ifndef FLOWMQ_PEER_STATE_H
#define FLOWMQ_PEER_STATE_H

#include <stdint.h>

typedef enum flowmq_peer_lifecycle_e {
  FLOWMQ_PEER_LIFECYCLE_FREE = 0,
  FLOWMQ_PEER_LIFECYCLE_ALLOCATED,
  FLOWMQ_PEER_LIFECYCLE_CONNECTED,
  FLOWMQ_PEER_LIFECYCLE_CLOSE_RETRY,
  FLOWMQ_PEER_LIFECYCLE_CLOSING,
  FLOWMQ_PEER_LIFECYCLE_RETIRED,
  FLOWMQ_PEER_LIFECYCLE_COUNT
} flowmq_peer_lifecycle_t;

typedef enum flowmq_peer_handshake_e {
  FLOWMQ_PEER_HANDSHAKE_HELLO_TX = 1u << 0,
  FLOWMQ_PEER_HANDSHAKE_HELLO_RX = 1u << 1,
  FLOWMQ_PEER_HANDSHAKE_SETTINGS_TX = 1u << 2,
  FLOWMQ_PEER_HANDSHAKE_SETTINGS_RX = 1u << 3,
  FLOWMQ_PEER_HANDSHAKE_READY =
      FLOWMQ_PEER_HANDSHAKE_HELLO_TX |
      FLOWMQ_PEER_HANDSHAKE_HELLO_RX |
      FLOWMQ_PEER_HANDSHAKE_SETTINGS_TX |
      FLOWMQ_PEER_HANDSHAKE_SETTINGS_RX
} flowmq_peer_handshake_t;

typedef enum flowmq_peer_write_lane_e {
  FLOWMQ_PEER_WRITE_IDLE = 0,
  FLOWMQ_PEER_WRITE_HELLO,
  FLOWMQ_PEER_WRITE_SETTINGS,
  FLOWMQ_PEER_WRITE_CONTROL,
  FLOWMQ_PEER_WRITE_DATA,
  FLOWMQ_PEER_WRITE_COUNT
} flowmq_peer_write_lane_t;

typedef struct flowmq_peer_state_s {
  uint8_t lifecycle;
  uint8_t handshake;
  uint8_t write_lane;
} flowmq_peer_state_t;

#define FLOWMQ_PEER_STATE_INIT   { FLOWMQ_PEER_LIFECYCLE_FREE, 0u, FLOWMQ_PEER_WRITE_IDLE }

int flowmq_peer_state_transition(flowmq_peer_state_t *state,
                                 flowmq_peer_lifecycle_t next);
int flowmq_peer_state_allocate(flowmq_peer_state_t *state);
int flowmq_peer_state_release(flowmq_peer_state_t *state);

int flowmq_peer_state_is_used(const flowmq_peer_state_t *state);
int flowmq_peer_state_is_connected(const flowmq_peer_state_t *state);
int flowmq_peer_state_is_retired(const flowmq_peer_state_t *state);
int flowmq_peer_state_needs_close_retry(const flowmq_peer_state_t *state);

int flowmq_peer_state_handshake_mark(flowmq_peer_state_t *state,
                                     flowmq_peer_handshake_t event);
int flowmq_peer_state_handshake_has(const flowmq_peer_state_t *state,
                                    uint8_t mask);
int flowmq_peer_state_ready(const flowmq_peer_state_t *state);

int flowmq_peer_state_write_begin(flowmq_peer_state_t *state,
                                  flowmq_peer_write_lane_t lane);
int flowmq_peer_state_write_complete(flowmq_peer_state_t *state,
                                     flowmq_peer_write_lane_t *completed_lane);
void flowmq_peer_state_write_cancel(flowmq_peer_state_t *state);
int flowmq_peer_state_write_idle(const flowmq_peer_state_t *state);

#endif /* FLOWMQ_PEER_STATE_H */
