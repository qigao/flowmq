#include "flowmq_reconnect.h"

#include "turbo_error.h"

#include <string.h>

#define FLOWMQ_CONNECT_ENDPOINT_JITTER_GAMMA UINT64_C(0x9e3779b97f4a7c15)
#define FLOWMQ_CONNECT_ENDPOINT_JITTER_MIX_A UINT64_C(0xbf58476d1ce4e5b9)
#define FLOWMQ_CONNECT_ENDPOINT_JITTER_MIX_B UINT64_C(0x94d049bb133111eb)

int flowmq_reconnect_init(flowmq_reconnect_t *reconnect, uint64_t initial_delay_ms,
                          uint64_t max_delay_ms, uint64_t jitter_seed) {
  if (!reconnect || (max_delay_ms != 0u && initial_delay_ms > max_delay_ms)) return TURBO_EINVAL;
  memset(reconnect, 0, sizeof(*reconnect));
  reconnect->initial_delay_ms = initial_delay_ms;
  reconnect->max_delay_ms = max_delay_ms;
  reconnect->jitter_state = jitter_seed;
  return TURBO_OK;
}

void flowmq_reconnect_reset(flowmq_reconnect_t *reconnect) {
  if (reconnect) reconnect->current_delay_ms = 0u;
}

int flowmq_reconnect_next(flowmq_reconnect_t *reconnect, uint64_t *delay_ms) {
  uint64_t value;
  uint64_t minimum_delay_ms;
  uint64_t span_ms;
  if (!reconnect || !delay_ms) return TURBO_EINVAL;
  if (reconnect->initial_delay_ms == 0u) return TURBO_ENOENT;
  if (reconnect->current_delay_ms == 0u) {
    reconnect->current_delay_ms = reconnect->initial_delay_ms;
  } else if (reconnect->max_delay_ms == 0u) {
    reconnect->current_delay_ms = reconnect->current_delay_ms > UINT64_MAX / 2u
                                      ? UINT64_MAX
                                      : reconnect->current_delay_ms * 2u;
  } else if (reconnect->current_delay_ms >= reconnect->max_delay_ms ||
             reconnect->current_delay_ms > reconnect->max_delay_ms / 2u) {
    reconnect->current_delay_ms = reconnect->max_delay_ms;
  } else {
    reconnect->current_delay_ms *= 2u;
  }
  if (reconnect->current_delay_ms <= 1u) {
    *delay_ms = reconnect->current_delay_ms;
    return TURBO_OK;
  }

  reconnect->jitter_state += FLOWMQ_CONNECT_ENDPOINT_JITTER_GAMMA;
  value = reconnect->jitter_state;
  value = (value ^ (value >> 30u)) * FLOWMQ_CONNECT_ENDPOINT_JITTER_MIX_A;
  value = (value ^ (value >> 27u)) * FLOWMQ_CONNECT_ENDPOINT_JITTER_MIX_B;
  value ^= value >> 31u;
  minimum_delay_ms = reconnect->current_delay_ms / 2u + reconnect->current_delay_ms % 2u;
  span_ms = reconnect->current_delay_ms - minimum_delay_ms;
  *delay_ms = minimum_delay_ms + value % span_ms;
  return TURBO_OK;
}
