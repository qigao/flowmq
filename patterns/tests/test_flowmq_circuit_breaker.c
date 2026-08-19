/**
 * @file test_flowmq_circuit_breaker.c
 * Unit tests for circuit breaker.
 */

#include "flowmq_circuit_breaker.h"
#include "tinytest.h"

#include <string.h>

spec("flowmq_circuit_breaker") {
  it("initializes in CLOSED state") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 5, 3, 1000, 5000), TURBO_OK);
    
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_CLOSED);
    
    uint32_t failures = 0, successes = 0;
    uint64_t total_opened = 0, total_rejected = 0;
    flowmq_circuit_breaker_stats(&cb, &failures, &successes, &total_opened, &total_rejected);
    check_int_eq(failures, 0);
    check_int_eq(successes, 0);
    check_ull_eq(total_opened, 0);
    check_ull_eq(total_rejected, 0);
  }
  
  it("rejects invalid initialization parameters") {
    flowmq_circuit_breaker_t cb;
    
    /* NULL pointer */
    check_int_eq(flowmq_circuit_breaker_init(NULL, 5, 3, 1000, 5000), TURBO_EINVAL);
    
    /* Zero failure_threshold */
    check_int_eq(flowmq_circuit_breaker_init(&cb, 0, 3, 1000, 5000), TURBO_EINVAL);
    
    /* Zero success_threshold */
    check_int_eq(flowmq_circuit_breaker_init(&cb, 5, 0, 1000, 5000), TURBO_EINVAL);
    
    /* Zero timeout_ms */
    check_int_eq(flowmq_circuit_breaker_init(&cb, 5, 3, 0, 5000), TURBO_EINVAL);
    
    /* Zero window_ms */
    check_int_eq(flowmq_circuit_breaker_init(&cb, 5, 3, 1000, 0), TURBO_EINVAL);
  }
  
  it("allows requests in CLOSED state") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 5, 3, 1000, 5000), TURBO_OK);
    
    uint64_t now_ns = 1000000000ULL;
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, now_ns), TURBO_OK);
  }
  
  it("transitions CLOSED → OPEN on failure threshold") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 3, 2, 1000, 5000), TURBO_OK);
    /* failure_threshold = 3 */
    
    uint64_t now_ns = 1000000000ULL;
    
    /* Record 2 failures */
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_CLOSED);
    
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_CLOSED);
    
    /* Third failure → OPEN */
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_OPEN);
    
    uint64_t total_opened = 0;
    flowmq_circuit_breaker_stats(&cb, NULL, NULL, &total_opened, NULL);
    check_ull_eq(total_opened, 1);
  }
  
  it("rejects requests in OPEN state (fail fast)") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 2, 2, 1000, 5000), TURBO_OK);
    
    uint64_t now_ns = 1000000000ULL;
    
    /* Trigger OPEN */
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_OPEN);
    
    /* Requests should be rejected */
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, now_ns), TURBO_ECONNREFUSED);
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, now_ns), TURBO_ECONNREFUSED);
    
    uint64_t total_rejected = 0;
    flowmq_circuit_breaker_stats(&cb, NULL, NULL, NULL, &total_rejected);
    check_ull_eq(total_rejected, 2);
  }
  
  it("transitions OPEN → HALF_OPEN after timeout") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 2, 2, 1000, 5000), TURBO_OK);
    /* timeout_ms = 1000 (1 second) */
    
    uint64_t now_ns = 1000000000ULL;
    
    /* Trigger OPEN */
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_OPEN);
    
    /* Try request before timeout (rejected) */
    uint64_t before_timeout_ns = now_ns + 500000000ULL;  /* 0.5 seconds */
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, before_timeout_ns),
                 TURBO_ECONNREFUSED);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_OPEN);
    
    /* Try request after timeout (allowed, → HALF_OPEN) */
    uint64_t after_timeout_ns = now_ns + 1000000001ULL;  /* >1 second */
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, after_timeout_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_HALF_OPEN);
  }
  
  it("transitions HALF_OPEN → CLOSED on success threshold") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 2, 3, 1000, 5000), TURBO_OK);
    /* success_threshold = 3 */
    
    uint64_t now_ns = 1000000000ULL;
    
    /* Force to HALF_OPEN */
    check_int_eq(flowmq_circuit_breaker_force_half_open(&cb), TURBO_OK);
    
    /* Record 2 successes */
    check_int_eq(flowmq_circuit_breaker_record_success(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_HALF_OPEN);
    
    check_int_eq(flowmq_circuit_breaker_record_success(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_HALF_OPEN);
    
    /* Third success → CLOSED */
    check_int_eq(flowmq_circuit_breaker_record_success(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_CLOSED);
  }
  
  it("transitions HALF_OPEN → OPEN on any failure") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 2, 3, 1000, 5000), TURBO_OK);
    
    uint64_t now_ns = 1000000000ULL;
    
    /* Force to HALF_OPEN */
    check_int_eq(flowmq_circuit_breaker_force_half_open(&cb), TURBO_OK);
    
    /* Record 1 success */
    check_int_eq(flowmq_circuit_breaker_record_success(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_HALF_OPEN);
    
    /* Any failure → back to OPEN */
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_OPEN);
  }
  
  it("calculates failure rate correctly") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 10, 3, 1000, 5000), TURBO_OK);
    
    uint64_t now_ns = 1000000000ULL;
    
    /* 3 failures, 7 successes = 30% failure rate */
    for (int i = 0; i < 3; i++) {
      check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    }
    for (int i = 0; i < 7; i++) {
      check_int_eq(flowmq_circuit_breaker_record_success(&cb, now_ns), TURBO_OK);
    }
    
    double failure_rate = flowmq_circuit_breaker_get_failure_rate(&cb);
    check(failure_rate >= 0.29 && failure_rate <= 0.31);  /* ~0.30 */
  }
  
  it("resets sliding window on expiry") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 5, 3, 1000, 1000), TURBO_OK);
    /* window_ms = 1000 (1 second) */
    
    uint64_t now_ns = 1000000000ULL;
    
    /* Record 3 failures */
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    
    uint32_t failures = 0;
    flowmq_circuit_breaker_stats(&cb, &failures, NULL, NULL, NULL);
    check_int_eq(failures, 3);
    
    /* Advance time past window */
    uint64_t after_window_ns = now_ns + 1000000001ULL;  /* >1 second */
    
    /* Allow request (triggers window check) */
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, after_window_ns), TURBO_OK);
    
    /* Window should reset, failures cleared */
    flowmq_circuit_breaker_stats(&cb, &failures, NULL, NULL, NULL);
    check_int_eq(failures, 0);
  }
  
  it("resets circuit breaker") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 2, 2, 1000, 5000), TURBO_OK);
    
    uint64_t now_ns = 1000000000ULL;
    
    /* Trigger OPEN */
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_OPEN);
    
    /* Reset */
    flowmq_circuit_breaker_reset(&cb);
    
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_CLOSED);
    
    uint32_t failures = 0;
    flowmq_circuit_breaker_stats(&cb, &failures, NULL, NULL, NULL);
    check_int_eq(failures, 0);
  }
  
  it("forces state transitions manually") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 5, 3, 1000, 5000), TURBO_OK);
    
    uint64_t now_ns = 1000000000ULL;
    
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_CLOSED);
    
    /* Force OPEN */
    check_int_eq(flowmq_circuit_breaker_force_open(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_OPEN);
    
    /* Force HALF_OPEN */
    check_int_eq(flowmq_circuit_breaker_force_half_open(&cb), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_HALF_OPEN);
    
    /* Force CLOSED */
    check_int_eq(flowmq_circuit_breaker_force_close(&cb), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_get_state(&cb), FLOWMQ_CB_CLOSED);
  }
  
  it("tracks total opened and rejected counts") {
    flowmq_circuit_breaker_t cb;
    check_int_eq(flowmq_circuit_breaker_init(&cb, 2, 2, 1000, 5000), TURBO_OK);
    
    uint64_t now_ns = 1000000000ULL;
    
    /* First open */
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, now_ns), TURBO_OK);
    
    /* Reject 3 requests */
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, now_ns), TURBO_ECONNREFUSED);
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, now_ns), TURBO_ECONNREFUSED);
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, now_ns), TURBO_ECONNREFUSED);
    
    /* Move to HALF_OPEN, then fail back to OPEN */
    uint64_t after_timeout_ns = now_ns + 1000000001ULL;
    check_int_eq(flowmq_circuit_breaker_allow_request(&cb, after_timeout_ns), TURBO_OK);
    check_int_eq(flowmq_circuit_breaker_record_failure(&cb, after_timeout_ns), TURBO_OK);
    
    /* Check stats */
    uint64_t total_opened = 0, total_rejected = 0;
    flowmq_circuit_breaker_stats(&cb, NULL, NULL, &total_opened, &total_rejected);
    check_ull_eq(total_opened, 2);  /* Opened twice */
    check_ull_eq(total_rejected, 3);
  }
}
