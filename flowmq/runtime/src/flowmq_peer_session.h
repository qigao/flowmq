#ifndef FLOWMQ_PEER_SESSION_H
#define FLOWMQ_PEER_SESSION_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

typedef enum flowmq_peer_exchange_state_e {
  FLOWMQ_PEER_EXCHANGE_READY = 0,
  FLOWMQ_PEER_EXCHANGE_WAIT_REPLY,
  FLOWMQ_PEER_EXCHANGE_PROCESSING_REQUEST,
  FLOWMQ_PEER_EXCHANGE_RESETTING
} flowmq_peer_exchange_state_t;

typedef struct flowmq_peer_session_s {
  size_t size;
  uint32_t api_version;
  atomic_int state;
  atomic_uint_fast64_t generation;
  atomic_uint_fast64_t correlation_generation;
  atomic_uint_fast64_t correlation_id;
} flowmq_peer_session_t;

int flowmq_peer_session_init(flowmq_peer_session_t *session, uint64_t initial_generation);
int flowmq_peer_session_snapshot(const flowmq_peer_session_t *session,
                                 flowmq_peer_exchange_state_t *state, uint64_t *generation,
                                 uint64_t *correlation_id);
int flowmq_peer_session_handshake_complete(flowmq_peer_session_t *session, uint64_t *generation);
int flowmq_peer_session_begin(flowmq_peer_session_t *session,
                              flowmq_peer_exchange_state_t active_state, uint64_t correlation_id,
                              uint64_t *generation);
int flowmq_peer_session_match(const flowmq_peer_session_t *session, uint64_t generation,
                              flowmq_peer_exchange_state_t expected_state, uint64_t correlation_id);
int flowmq_peer_session_finish(flowmq_peer_session_t *session, uint64_t generation,
                               flowmq_peer_exchange_state_t expected_state, uint64_t correlation_id,
                               flowmq_peer_exchange_state_t terminal_state);
int flowmq_peer_session_mark_resetting(flowmq_peer_session_t *session);

#endif /* FLOWMQ_PEER_SESSION_H */
