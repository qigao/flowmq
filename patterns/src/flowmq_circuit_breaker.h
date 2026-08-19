#ifndef FLOWMQ_CIRCUIT_BREAKER_H
#define FLOWMQ_CIRCUIT_BREAKER_H

/**
 * @file flowmq_circuit_breaker.h
 * Circuit breaker for service protection.
 * 
 * Circuit breaker prevents cascading failures by monitoring error rates
 * and temporarily blocking requests when failures exceed threshold.
 * 
 * State machine:
 *   CLOSED (normal) → OPEN (tripped) → HALF_OPEN (testing) → CLOSED/OPEN
 * 
 * - CLOSED: Requests pass through, failures counted
 * - OPEN: Requests rejected immediately (fail fast)
 * - HALF_OPEN: Limited requests allowed to test recovery
 * 
 * Transitions:
 * - CLOSED → OPEN: failure_count >= failure_threshold within window
 * - OPEN → HALF_OPEN: after timeout_ms elapsed
 * - HALF_OPEN → CLOSED: success_count >= success_threshold
 * - HALF_OPEN → OPEN: any failure detected
 */

#include "turbo_error.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Circuit breaker state */
typedef enum flowmq_circuit_breaker_state_e {
  FLOWMQ_CB_CLOSED = 0,      /* Normal operation */
  FLOWMQ_CB_OPEN = 1,        /* Circuit tripped, rejecting requests */
  FLOWMQ_CB_HALF_OPEN = 2,   /* Testing recovery */
} flowmq_circuit_breaker_state_t;

/**
 * Circuit breaker instance.
 * Tracks service health and manages state transitions.
 */
typedef struct flowmq_circuit_breaker_s {
  flowmq_circuit_breaker_state_t state;
  
  /* Configuration */
  uint32_t failure_threshold;      /* Max failures before opening */
  uint32_t success_threshold;      /* Min successes to close from half-open */
  uint64_t timeout_ms;             /* Time to wait before half-open */
  uint64_t window_ms;              /* Sliding window for failure counting */
  
  /* State tracking */
  uint32_t failure_count;          /* Recent failures */
  uint32_t success_count;          /* Recent successes (in half-open) */
  uint32_t total_requests;         /* Total requests in current window */
  uint64_t last_failure_ns;        /* Last failure timestamp */
  uint64_t opened_at_ns;           /* When circuit opened */
  uint64_t window_start_ns;        /* Current window start */
  
  /* Statistics */
  uint64_t total_opened;           /* Total times opened */
  uint64_t total_rejected;         /* Total requests rejected */
} flowmq_circuit_breaker_t;

/* Initialize circuit breaker */
int flowmq_circuit_breaker_init(flowmq_circuit_breaker_t *cb,
                               uint32_t failure_threshold,
                               uint32_t success_threshold,
                               uint64_t timeout_ms,
                               uint64_t window_ms);

/* Reset circuit breaker (move to CLOSED, clear counters) */
void flowmq_circuit_breaker_reset(flowmq_circuit_breaker_t *cb);

/* Check if request should be allowed (fail fast if OPEN) */
int flowmq_circuit_breaker_allow_request(flowmq_circuit_breaker_t *cb,
                                        uint64_t now_ns);

/* Record successful request */
int flowmq_circuit_breaker_record_success(flowmq_circuit_breaker_t *cb,
                                         uint64_t now_ns);

/* Record failed request */
int flowmq_circuit_breaker_record_failure(flowmq_circuit_breaker_t *cb,
                                         uint64_t now_ns);

/* Get current state */
flowmq_circuit_breaker_state_t flowmq_circuit_breaker_get_state(
    flowmq_circuit_breaker_t *cb);

/* Get failure rate (0.0 - 1.0) in current window */
double flowmq_circuit_breaker_get_failure_rate(flowmq_circuit_breaker_t *cb);

/* Get statistics */
void flowmq_circuit_breaker_stats(flowmq_circuit_breaker_t *cb,
                                 uint32_t *out_failure_count,
                                 uint32_t *out_success_count,
                                 uint64_t *out_total_opened,
                                 uint64_t *out_total_rejected);

/* Force state transition (for testing/manual control) */
int flowmq_circuit_breaker_force_open(flowmq_circuit_breaker_t *cb,
                                     uint64_t now_ns);

int flowmq_circuit_breaker_force_half_open(flowmq_circuit_breaker_t *cb);

int flowmq_circuit_breaker_force_close(flowmq_circuit_breaker_t *cb);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_CIRCUIT_BREAKER_H */
