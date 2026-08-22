/**
 * @file flowmq_circuit_breaker.c
 * Circuit breaker implementation.
 */

#include "flowmq_circuit_breaker.h"
#include "tlog.h"
#include "platform.h"

#include <string.h>

int flowmq_circuit_breaker_init(flowmq_circuit_breaker_t *cb,
                               uint32_t failure_threshold,
                               uint32_t success_threshold,
                               uint64_t timeout_ms,
                               uint64_t window_ms) {
  if (!cb) return TURBO_EINVAL;
  if (failure_threshold == 0 || success_threshold == 0) return TURBO_EINVAL;
  if (timeout_ms == 0 || window_ms == 0) return TURBO_EINVAL;
  
  memset(cb, 0, sizeof(*cb));
  
  cb->state = FLOWMQ_CB_CLOSED;
  cb->failure_threshold = failure_threshold;
  cb->success_threshold = success_threshold;
  cb->timeout_ms = timeout_ms;
  cb->window_ms = window_ms;
  cb->window_start_ns = 0;
  
  return TURBO_OK;
}

void flowmq_circuit_breaker_reset(flowmq_circuit_breaker_t *cb) {
  if (!cb) return;
  
  cb->state = FLOWMQ_CB_CLOSED;
  cb->failure_count = 0;
  cb->success_count = 0;
  cb->total_requests = 0;
  cb->last_failure_ns = 0;
  cb->opened_at_ns = 0;
  cb->window_start_ns = 0;
  
  TLOG_INFO("Circuit breaker reset to CLOSED");
}

/* Check if sliding window has expired */
static void check_window_expiry(flowmq_circuit_breaker_t *cb, uint64_t now_ns) {
  if (cb->window_start_ns == 0) {
    cb->window_start_ns = now_ns;
    return;
  }

  uint64_t window_ns = cb->window_ms * 1000000ULL;
  if (now_ns >= cb->window_start_ns + window_ns) {
    /* Window expired, reset counters */
    cb->failure_count = 0;
    cb->success_count = 0;
    cb->total_requests = 0;
    cb->window_start_ns = now_ns;
  }
}

int flowmq_circuit_breaker_allow_request(flowmq_circuit_breaker_t *cb,
                                        uint64_t now_ns) {
  if (!cb) return TURBO_EINVAL;
  
  switch (cb->state) {
    case FLOWMQ_CB_CLOSED:
      /* Allow all requests */
      check_window_expiry(cb, now_ns);
      return TURBO_OK;
    
    case FLOWMQ_CB_OPEN: {
      /* Check if timeout elapsed */
      uint64_t timeout_ns = cb->timeout_ms * 1000000ULL;
      if (now_ns >= cb->opened_at_ns + timeout_ns) {
        /* Move to HALF_OPEN */
        cb->state = FLOWMQ_CB_HALF_OPEN;
        cb->success_count = 0;
        cb->failure_count = 0;
        TLOG_INFO("Circuit breaker → HALF_OPEN (timeout elapsed)");
        return TURBO_OK;
      }
      
      /* Still open, reject request */
      cb->total_rejected++;
      return TURBO_ECONNREFUSED;
    }
    
    case FLOWMQ_CB_HALF_OPEN:
      /* Allow limited requests for testing */
      return TURBO_OK;
    
    default:
      return TURBO_EINVAL;
  }
}

int flowmq_circuit_breaker_record_success(flowmq_circuit_breaker_t *cb,
                                         uint64_t now_ns) {
  if (!cb) return TURBO_EINVAL;
  
  switch (cb->state) {
    case FLOWMQ_CB_CLOSED:
      /* Normal operation, no action needed */
      check_window_expiry(cb, now_ns);
      cb->total_requests++;
      break;
    
    case FLOWMQ_CB_OPEN:
      /* Should not receive success in OPEN state (request should be rejected) */
      TLOG_WARN("Received success in OPEN state (unexpected)");
      break;
    
    case FLOWMQ_CB_HALF_OPEN:
      cb->success_count++;
      
      /* Check if enough successes to close */
      if (cb->success_count >= cb->success_threshold) {
        cb->state = FLOWMQ_CB_CLOSED;
        cb->failure_count = 0;
        cb->success_count = 0;
        cb->window_start_ns = now_ns;
        TLOG_INFOF("Circuit breaker → CLOSED (success threshold met: {}/{})",
                   cb->success_count, cb->success_threshold);
      }
      break;
  }
  
  return TURBO_OK;
}

int flowmq_circuit_breaker_record_failure(flowmq_circuit_breaker_t *cb,
                                         uint64_t now_ns) {
  if (!cb) return TURBO_EINVAL;
  
  check_window_expiry(cb, now_ns);
  cb->total_requests++;
  cb->failure_count++;
  cb->last_failure_ns = now_ns;
  
  switch (cb->state) {
    case FLOWMQ_CB_CLOSED:
      /* Check if failure threshold exceeded */
      if (cb->failure_count >= cb->failure_threshold) {
        cb->state = FLOWMQ_CB_OPEN;
        cb->opened_at_ns = now_ns;
        cb->total_opened++;
        TLOG_WARNF("Circuit breaker → OPEN (failure threshold: {}/{})",
                   cb->failure_count, cb->failure_threshold);
      }
      break;
    
    case FLOWMQ_CB_OPEN:
      /* Already open, no state change */
      break;
    
    case FLOWMQ_CB_HALF_OPEN:
      /* Any failure in HALF_OPEN → back to OPEN */
      cb->state = FLOWMQ_CB_OPEN;
      cb->opened_at_ns = now_ns;
      cb->total_opened++;
      cb->success_count = 0;
      TLOG_WARN("Circuit breaker → OPEN (failure in HALF_OPEN)");
      break;
  }
  
  return TURBO_OK;
}

flowmq_circuit_breaker_state_t flowmq_circuit_breaker_get_state(
    flowmq_circuit_breaker_t *cb) {
  if (!cb) return FLOWMQ_CB_CLOSED;
  return cb->state;
}

double flowmq_circuit_breaker_get_failure_rate(flowmq_circuit_breaker_t *cb) {
  if (!cb || cb->total_requests == 0) return 0.0;
  return (double)cb->failure_count / (double)cb->total_requests;
}

void flowmq_circuit_breaker_stats(flowmq_circuit_breaker_t *cb,
                                 uint32_t *out_failure_count,
                                 uint32_t *out_success_count,
                                 uint64_t *out_total_opened,
                                 uint64_t *out_total_rejected) {
  if (!cb) return;
  if (out_failure_count) *out_failure_count = cb->failure_count;
  if (out_success_count) *out_success_count = cb->success_count;
  if (out_total_opened) *out_total_opened = cb->total_opened;
  if (out_total_rejected) *out_total_rejected = cb->total_rejected;
}

int flowmq_circuit_breaker_force_open(flowmq_circuit_breaker_t *cb,
                                     uint64_t now_ns) {
  if (!cb) return TURBO_EINVAL;
  
  cb->state = FLOWMQ_CB_OPEN;
  cb->opened_at_ns = now_ns;
  cb->total_opened++;
  
  TLOG_INFO("Circuit breaker forced → OPEN");
  return TURBO_OK;
}

int flowmq_circuit_breaker_force_half_open(flowmq_circuit_breaker_t *cb) {
  if (!cb) return TURBO_EINVAL;
  
  cb->state = FLOWMQ_CB_HALF_OPEN;
  cb->success_count = 0;
  cb->failure_count = 0;
  
  TLOG_INFO("Circuit breaker forced → HALF_OPEN");
  return TURBO_OK;
}

int flowmq_circuit_breaker_force_close(flowmq_circuit_breaker_t *cb) {
  if (!cb) return TURBO_EINVAL;
  
  cb->state = FLOWMQ_CB_CLOSED;
  cb->failure_count = 0;
  cb->success_count = 0;
  cb->window_start_ns = 0;
  
  TLOG_INFO("Circuit breaker forced → CLOSED");
  return TURBO_OK;
}
