#ifndef FLOWMQ_RECONNECT_H
#define FLOWMQ_RECONNECT_H

#include <stdint.h>

typedef struct flowmq_reconnect_s {
  uint64_t initial_delay_ms;
  uint64_t max_delay_ms;
  uint64_t current_delay_ms;
  uint64_t jitter_state;
} flowmq_reconnect_t;

int flowmq_reconnect_init(flowmq_reconnect_t *reconnect, uint64_t initial_delay_ms,
                          uint64_t max_delay_ms, uint64_t jitter_seed);
void flowmq_reconnect_reset(flowmq_reconnect_t *reconnect);
int flowmq_reconnect_next(flowmq_reconnect_t *reconnect, uint64_t *delay_ms);

#endif /* FLOWMQ_RECONNECT_H */
