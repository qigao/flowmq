#include "flowmq_peer_session.h"

#include "turbo_error.h"

#define FLOWMQ_PEER_SESSION_API_VERSION 1u
#define FLOWMQ_PEER_EXCHANGE_TRANSITION 4

static int flowmq_peer_session_valid(const flowmq_peer_session_t *session) {
  return session && session->size >= sizeof(*session) &&
         session->api_version == FLOWMQ_PEER_SESSION_API_VERSION;
}

static int flowmq_peer_exchange_active(flowmq_peer_exchange_state_t state) {
  return state == FLOWMQ_PEER_EXCHANGE_WAIT_REPLY ||
         state == FLOWMQ_PEER_EXCHANGE_PROCESSING_REQUEST;
}

int flowmq_peer_session_init(flowmq_peer_session_t *session, uint64_t initial_generation) {
  if (!session || initial_generation == 0u) return TURBO_EINVAL;
  session->size = sizeof(*session);
  session->api_version = FLOWMQ_PEER_SESSION_API_VERSION;
  atomic_init(&session->state, FLOWMQ_PEER_EXCHANGE_READY);
  atomic_init(&session->generation, initial_generation);
  atomic_init(&session->correlation_generation, 0u);
  atomic_init(&session->correlation_id, 0u);
  return TURBO_OK;
}

int flowmq_peer_session_snapshot(const flowmq_peer_session_t *session,
                                 flowmq_peer_exchange_state_t *state, uint64_t *generation,
                                 uint64_t *correlation_id) {
  int value;
  if (!flowmq_peer_session_valid(session) || !state || !generation || !correlation_id)
    return TURBO_EINVAL;
  value = atomic_load_explicit(&session->state, memory_order_acquire);
  if (value < FLOWMQ_PEER_EXCHANGE_READY || value > FLOWMQ_PEER_EXCHANGE_RESETTING)
    return TURBO_EPROTO;
  *state = (flowmq_peer_exchange_state_t)value;
  *generation = atomic_load_explicit(&session->generation, memory_order_acquire);
  *correlation_id = atomic_load_explicit(&session->correlation_id, memory_order_acquire);
  return *generation == 0u ? TURBO_EPROTO : TURBO_OK;
}

int flowmq_peer_session_handshake_complete(flowmq_peer_session_t *session, uint64_t *generation) {
  uint_fast64_t current;
  uint_fast64_t next;
  if (!flowmq_peer_session_valid(session) || !generation) return TURBO_EINVAL;
  for (;;) {
    int state = atomic_load_explicit(&session->state, memory_order_acquire);
    if (state == FLOWMQ_PEER_EXCHANGE_TRANSITION) continue;
    if (atomic_compare_exchange_weak_explicit(&session->state, &state,
                                              FLOWMQ_PEER_EXCHANGE_TRANSITION, memory_order_acq_rel,
                                              memory_order_acquire))
      break;
  }
  current = atomic_load_explicit(&session->generation, memory_order_acquire);
  for (;;) {
    if (current == 0u) {
      atomic_store_explicit(&session->state, FLOWMQ_PEER_EXCHANGE_RESETTING, memory_order_release);
      return TURBO_EPROTO;
    }
    next = current == UINT64_MAX ? 1u : current + 1u;
    if (atomic_compare_exchange_weak_explicit(&session->generation, &current, next,
                                              memory_order_acq_rel, memory_order_acquire))
      break;
  }
  atomic_store_explicit(&session->correlation_id, 0u, memory_order_release);
  atomic_store_explicit(&session->correlation_generation, 0u, memory_order_release);
  atomic_store_explicit(&session->state, FLOWMQ_PEER_EXCHANGE_READY, memory_order_release);
  *generation = next;
  return TURBO_OK;
}

int flowmq_peer_session_begin(flowmq_peer_session_t *session,
                              flowmq_peer_exchange_state_t active_state, uint64_t correlation_id,
                              uint64_t *generation) {
  uint64_t current_generation;
  int expected = FLOWMQ_PEER_EXCHANGE_READY;
  if (!flowmq_peer_session_valid(session) || !flowmq_peer_exchange_active(active_state) ||
      correlation_id == 0u || !generation)
    return TURBO_EINVAL;
  current_generation = atomic_load_explicit(&session->generation, memory_order_acquire);
  if (current_generation == 0u) return TURBO_EPROTO;
  if (!atomic_compare_exchange_strong_explicit(&session->state, &expected,
                                               FLOWMQ_PEER_EXCHANGE_TRANSITION,
                                               memory_order_acq_rel, memory_order_acquire))
    return TURBO_EBUSY;
  current_generation = atomic_load_explicit(&session->generation, memory_order_acquire);
  if (current_generation == 0u) {
    atomic_store_explicit(&session->state, FLOWMQ_PEER_EXCHANGE_RESETTING, memory_order_release);
    return TURBO_EPROTO;
  }
  atomic_store_explicit(&session->correlation_id, correlation_id, memory_order_release);
  atomic_store_explicit(&session->correlation_generation, current_generation, memory_order_release);
  atomic_store_explicit(&session->state, active_state, memory_order_release);
  *generation = current_generation;
  return TURBO_OK;
}

int flowmq_peer_session_match(const flowmq_peer_session_t *session, uint64_t generation,
                              flowmq_peer_exchange_state_t expected_state,
                              uint64_t correlation_id) {
  if (!flowmq_peer_session_valid(session) || generation == 0u ||
      !flowmq_peer_exchange_active(expected_state) || correlation_id == 0u)
    return TURBO_EINVAL;
  if (atomic_load_explicit(&session->generation, memory_order_acquire) != generation ||
      atomic_load_explicit(&session->correlation_generation, memory_order_acquire) != generation)
    return TURBO_ENOTCONN;
  if (atomic_load_explicit(&session->state, memory_order_acquire) != (int)expected_state)
    return TURBO_EBUSY;
  return atomic_load_explicit(&session->correlation_id, memory_order_acquire) == correlation_id
             ? TURBO_OK
             : TURBO_EPROTO;
}

int flowmq_peer_session_finish(flowmq_peer_session_t *session, uint64_t generation,
                               flowmq_peer_exchange_state_t expected_state, uint64_t correlation_id,
                               flowmq_peer_exchange_state_t terminal_state) {
  int expected = expected_state;
  if (terminal_state != FLOWMQ_PEER_EXCHANGE_READY &&
      terminal_state != FLOWMQ_PEER_EXCHANGE_RESETTING)
    return TURBO_EINVAL;
  if (!flowmq_peer_session_valid(session) || generation == 0u ||
      !flowmq_peer_exchange_active(expected_state) || correlation_id == 0u)
    return TURBO_EINVAL;
  if (!atomic_compare_exchange_strong_explicit(&session->state, &expected,
                                               FLOWMQ_PEER_EXCHANGE_TRANSITION,
                                               memory_order_acq_rel, memory_order_acquire))
    return TURBO_EBUSY;
  if (atomic_load_explicit(&session->generation, memory_order_acquire) != generation ||
      atomic_load_explicit(&session->correlation_generation, memory_order_acquire) != generation) {
    atomic_store_explicit(&session->state, expected_state, memory_order_release);
    return TURBO_ENOTCONN;
  }
  if (atomic_load_explicit(&session->correlation_id, memory_order_acquire) != correlation_id) {
    atomic_store_explicit(&session->state, expected_state, memory_order_release);
    return TURBO_EPROTO;
  }
  if (terminal_state == FLOWMQ_PEER_EXCHANGE_READY) {
    atomic_store_explicit(&session->correlation_id, 0u, memory_order_release);
    atomic_store_explicit(&session->correlation_generation, 0u, memory_order_release);
  }
  atomic_store_explicit(&session->state, terminal_state, memory_order_release);
  return TURBO_OK;
}

int flowmq_peer_session_mark_resetting(flowmq_peer_session_t *session) {
  int state;
  if (!flowmq_peer_session_valid(session)) return TURBO_EINVAL;
  for (;;) {
    state = atomic_load_explicit(&session->state, memory_order_acquire);
    if (state == FLOWMQ_PEER_EXCHANGE_RESETTING) return TURBO_OK;
    if (!flowmq_peer_exchange_active((flowmq_peer_exchange_state_t)state)) return TURBO_EBUSY;
    if (atomic_compare_exchange_weak_explicit(&session->state, &state,
                                              FLOWMQ_PEER_EXCHANGE_TRANSITION, memory_order_acq_rel,
                                              memory_order_acquire))
      break;
  }
  atomic_store_explicit(&session->state, FLOWMQ_PEER_EXCHANGE_RESETTING, memory_order_release);
  return TURBO_OK;
}
