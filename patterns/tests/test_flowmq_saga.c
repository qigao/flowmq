/**
 * @file test_flowmq_saga.c
 * Unit tests for SAGA distributed transaction coordinator.
 */

#include "flowmq_saga.h"
#include "tinytest.h"
#include "platform.h"

#include <string.h>

spec("flowmq_saga") {
  it("initializes and destroys coordinator") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    uint32_t active = 0, capacity = 0;
    flowmq_saga_coordinator_stats(&coordinator, &active, &capacity);
    check_int_eq(active, 0);
    check_int_eq(capacity, 10);

    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("rejects invalid coordinator initialization") {
    flowmq_saga_coordinator_t coordinator;

    /* NULL coordinator */
    check_int_eq(flowmq_saga_coordinator_init(NULL, 10), TURBO_EINVAL);

    /* Zero capacity */
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 0), TURBO_EINVAL);

    /* Exceeds maximum */
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 1000), TURBO_EINVAL);
  }

  it("creates SAGA transaction") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    tstr_t saga_id = NULL;
    uint64_t deadline_ns = turbo_hrtime() + 10000000000ULL;  /* 10 seconds */

    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 3, deadline_ns, NULL, &saga_id),
                 TURBO_OK);
    check(tstr_len(saga_id) > 0);
    check_str_starts_with(saga_id, "saga-");

    /* Check state */
    flowmq_saga_state_t state;
    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_OK);
    check_int_eq(state, FLOWMQ_SAGA_RUNTIME_PENDING);

    uint32_t active = 0;
    flowmq_saga_coordinator_stats(&coordinator, &active, NULL);
    check_int_eq(active, 1);

    tstr_free(saga_id);
    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("adds steps to SAGA transaction") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    tstr_t saga_id = NULL;
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 2, 2000000000ULL, NULL, &saga_id),
                 TURBO_OK);

    /* Add step 1 */
    tstr_t service1 = NULL, tx1 = NULL, comp1 = NULL;
    service1 = tstr_dup("order-service");
    tx1 = tstr_dup("{\"action\":\"create_order\"}");
    comp1 = tstr_dup("{\"action\":\"cancel_order\"}");

    check_int_eq(flowmq_saga_coordinator_add_step(&coordinator, &saga_id, &service1, &tx1, &comp1),
                 TURBO_OK);

    /* Add step 2 */
    tstr_t service2 = NULL, tx2 = NULL, comp2 = NULL;
    service2 = tstr_dup("payment-service");
    tx2 = tstr_dup("{\"action\":\"charge\"}");
    comp2 = tstr_dup("{\"action\":\"refund\"}");

    check_int_eq(flowmq_saga_coordinator_add_step(&coordinator, &saga_id, &service2, &tx2, &comp2),
                 TURBO_OK);

    /* Try to add third step (exceeds total_steps=2) */
    tstr_t service3 = NULL, tx3 = NULL, comp3 = NULL;
    service3 = tstr_dup("inventory-service");
    tx3 = tstr_dup("{\"action\":\"reserve\"}");
    comp3 = tstr_dup("{\"action\":\"release\"}");

    check_int_eq(flowmq_saga_coordinator_add_step(&coordinator, &saga_id, &service3, &tx3, &comp3),
                 TURBO_EINVAL);
    tstr_free(service3);
    tstr_free(tx3);
    tstr_free(comp3);

    tstr_free(saga_id);
    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("starts SAGA execution") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    tstr_t saga_id = NULL;
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 1, 2000000000ULL, NULL, &saga_id),
                 TURBO_OK);

    tstr_t service = NULL, tx = NULL, comp = NULL;
    service = tstr_dup("test-service");
    tx = tstr_dup("{}");
    comp = tstr_dup("{}");
    check_int_eq(flowmq_saga_coordinator_add_step(&coordinator, &saga_id, &service, &tx, &comp),
                 TURBO_OK);

    /* Start execution */
    check_int_eq(flowmq_saga_coordinator_start(&coordinator, &saga_id), TURBO_OK);

    flowmq_saga_state_t state;
    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_OK);
    check_int_eq(state, FLOWMQ_SAGA_RUNTIME_EXECUTING);

    uint32_t current_step = 999;
    check_int_eq(flowmq_saga_coordinator_get_current_step(&coordinator, &saga_id, &current_step),
                 TURBO_OK);
    check_int_eq(current_step, 0);

    tstr_free(saga_id);
    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("commits SAGA when all steps succeed") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    tstr_t saga_id = NULL;
    uint64_t now_ns = 1000000000ULL;
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 3, now_ns + 10000000000ULL,
                                                NULL, &saga_id),
                 TURBO_OK);

    /* Add 3 steps */
    for (int i = 0; i < 3; i++) {
      tstr_t service = NULL, tx = NULL, comp = NULL;
      char buf[32];
      snprintf(buf, sizeof(buf), "service-%d", i);
      service = tstr_dup(buf);
      tx = tstr_dup("tx");
      comp = tstr_dup("comp");
      check_int_eq(flowmq_saga_coordinator_add_step(&coordinator, &saga_id, &service, &tx, &comp),
                   TURBO_OK);
    }

    check_int_eq(flowmq_saga_coordinator_start(&coordinator, &saga_id), TURBO_OK);

    /* Execute all steps successfully */
    for (uint32_t i = 0; i < 3; i++) {
      check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, i, 1,
                                                              TURBO_OK, now_ns),
                   TURBO_OK);
    }

    /* Check committed */
    flowmq_saga_state_t state;
    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_OK);
    check_int_eq(state, FLOWMQ_SAGA_RUNTIME_COMMITTED);

    tstr_free(saga_id);
    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("triggers compensation on step failure") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    tstr_t saga_id = NULL;
    uint64_t now_ns = 1000000000ULL;
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 3, now_ns + 10000000000ULL,
                                                NULL, &saga_id),
                 TURBO_OK);

    /* Add 3 steps */
    for (int i = 0; i < 3; i++) {
      tstr_t service = NULL, tx = NULL, comp = NULL;
      service = tstr_dup("service");
      tx = tstr_dup("tx");
      comp = tstr_dup("comp");
      check_int_eq(flowmq_saga_coordinator_add_step(&coordinator, &saga_id, &service, &tx, &comp),
                   TURBO_OK);
    }

    check_int_eq(flowmq_saga_coordinator_start(&coordinator, &saga_id), TURBO_OK);

    /* Step 0 succeeds */
    check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, 0, 1,
                                                            TURBO_OK, now_ns),
                 TURBO_OK);

    /* Step 1 fails */
    check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, 1, 0,
                                                            TURBO_ECONNREFUSED, now_ns),
                 TURBO_OK);

    /* Check compensating */
    flowmq_saga_state_t state;
    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_OK);
    check_int_eq(state, FLOWMQ_SAGA_RUNTIME_COMPENSATING);

    tstr_free(saga_id);
    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("completes compensation and aborts") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    tstr_t saga_id = NULL;
    uint64_t now_ns = 1000000000ULL;
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 2, now_ns + 10000000000ULL,
                                                NULL, &saga_id),
                 TURBO_OK);

    /* Add 2 steps */
    for (int i = 0; i < 2; i++) {
      tstr_t service = NULL, tx = NULL, comp = NULL;
      service = tstr_dup("service");
      tx = tstr_dup("tx");
      comp = tstr_dup("comp");
      check_int_eq(flowmq_saga_coordinator_add_step(&coordinator, &saga_id, &service, &tx, &comp),
                   TURBO_OK);
    }

    check_int_eq(flowmq_saga_coordinator_start(&coordinator, &saga_id), TURBO_OK);

    /* Step 0 succeeds, step 1 fails → COMPENSATING */
    check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, 0, 1,
                                                            TURBO_OK, now_ns),
                 TURBO_OK);
    check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, 1, 0,
                                                            TURBO_EIO, now_ns),
                 TURBO_OK);

    /* Compensate step 0 (only step 0 executed before failure) */
    check_int_eq(flowmq_saga_coordinator_record_compensation(&coordinator, &saga_id, 0, now_ns),
                 TURBO_OK);

    /* Check aborted */
    flowmq_saga_state_t state;
    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_OK);
    check_int_eq(state, FLOWMQ_SAGA_RUNTIME_ABORTED);

    tstr_free(saga_id);
    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("peeks compensation target without moving cursor") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    tstr_t saga_id = NULL;
    uint64_t now_ns = 1000000000ULL;
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 3, now_ns + 10000000000ULL,
                                                NULL, &saga_id),
                 TURBO_OK);

    for (int i = 0; i < 3; i++) {
      tstr_t service = NULL, tx = NULL, comp = NULL;
      char service_name[32];
      char comp_name[32];
      snprintf(service_name, sizeof(service_name), "service-%d", i);
      snprintf(comp_name, sizeof(comp_name), "{\"action\":\"comp%d\"}", i);
      service = tstr_dup(service_name);
      tx = tstr_dup("tx");
      comp = tstr_dup(comp_name);
      check_int_eq(flowmq_saga_coordinator_add_step(&coordinator, &saga_id, &service, &tx, &comp),
                   TURBO_OK);
    }

    check_int_eq(flowmq_saga_coordinator_start(&coordinator, &saga_id), TURBO_OK);

    tstr_t unused_service = NULL;
    tstr_t unused_payload = NULL;
    uint32_t unused_step = 0u;
    check_int_eq(flowmq_saga_coordinator_peek_compensation(&coordinator, &saga_id, &unused_step,
                                                          &unused_service, &unused_payload),
                 TURBO_EINVAL);
    tstr_free(unused_service);
    tstr_free(unused_payload);

    /* Step 0,1 succeed; step 2 fails, so compensation should start from step 1. */
    check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, 0, 1,
                                                           TURBO_OK, now_ns),
                 TURBO_OK);
    check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, 1, 1,
                                                           TURBO_OK, now_ns),
                 TURBO_OK);
    check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, 2, 0,
                                                           TURBO_EIO, now_ns),
                 TURBO_OK);

    flowmq_saga_state_t state = FLOWMQ_SAGA_RUNTIME_PENDING;
    uint32_t current_step = 999u;
    tstr_t peeked_service = NULL;
    tstr_t peeked_payload = NULL;
    uint32_t peeked_step = 0u;
    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_OK);
    check_int_eq(state, FLOWMQ_SAGA_RUNTIME_COMPENSATING);
    check_int_eq(flowmq_saga_coordinator_get_current_step(&coordinator, &saga_id, &current_step),
                 TURBO_OK);
    check_int_eq(current_step, 1u);

    check_int_eq(flowmq_saga_coordinator_peek_compensation(&coordinator, &saga_id, &peeked_step,
                                                          &peeked_service, &peeked_payload),
                 TURBO_OK);
    check_int_eq(peeked_step, 1u);
    check_str_eq(peeked_service, "service-1");
    check_str_eq(peeked_payload, "{\"action\":\"comp1\"}");
    check_int_eq(current_step, 1u); /* peek should not move cursor */

    /* Apply compensation for current target; cursor moves to previous compensable step. */
    check_int_eq(flowmq_saga_coordinator_record_compensation(&coordinator, &saga_id, 1u, now_ns),
                 TURBO_OK);
    check_int_eq(flowmq_saga_coordinator_get_current_step(&coordinator, &saga_id, &current_step),
                 TURBO_OK);
    check_int_eq(current_step, 0u);

    check_int_eq(flowmq_saga_coordinator_peek_compensation(&coordinator, &saga_id, &peeked_step,
                                                          &peeked_service, &peeked_payload),
                 TURBO_OK);
    check_int_eq(peeked_step, 0u);
    check_str_eq(peeked_service, "service-0");
    check_str_eq(peeked_payload, "{\"action\":\"comp0\"}");

    check_int_eq(flowmq_saga_coordinator_record_compensation(&coordinator, &saga_id, 0u, now_ns),
                 TURBO_OK);
    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_OK);
    check_int_eq(state, FLOWMQ_SAGA_RUNTIME_ABORTED);

    tstr_free(peeked_service);
    tstr_free(peeked_payload);
    tstr_free(saga_id);
    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("advances compensation cursor in reverse step order") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    tstr_t saga_id = NULL;
    uint64_t now_ns = 1000000000ULL;
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 3, now_ns + 10000000000ULL,
                                                NULL, &saga_id),
                 TURBO_OK);

    for (int i = 0; i < 3; i++) {
      tstr_t service = NULL, tx = NULL, comp = NULL;
      char service_name[32];
      snprintf(service_name, sizeof(service_name), "service-%d", i);
      service = tstr_dup(service_name);
      tx = tstr_dup("tx");
      comp = tstr_dup("comp");
      check_int_eq(flowmq_saga_coordinator_add_step(&coordinator, &saga_id, &service, &tx, &comp),
                   TURBO_OK);
    }

    check_int_eq(flowmq_saga_coordinator_start(&coordinator, &saga_id), TURBO_OK);

    check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, 0, 1,
                                                           TURBO_OK, now_ns),
                 TURBO_OK);
    check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, 1, 1,
                                                           TURBO_OK, now_ns),
                 TURBO_OK);
    check_int_eq(flowmq_saga_coordinator_record_step_result(&coordinator, &saga_id, 2, 0,
                                                           TURBO_EIO, now_ns),
                 TURBO_OK);

    flowmq_saga_state_t state;
    uint32_t current_step = 0;
    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_OK);
    check_int_eq(state, FLOWMQ_SAGA_RUNTIME_COMPENSATING);
    check_int_eq(flowmq_saga_coordinator_get_current_step(&coordinator, &saga_id, &current_step),
                 TURBO_OK);
    check_int_eq(current_step, 1u);

    check_int_eq(flowmq_saga_coordinator_compensate(&coordinator, &saga_id), TURBO_OK);
    check_int_eq(flowmq_saga_coordinator_get_current_step(&coordinator, &saga_id, &current_step),
                 TURBO_OK);
    check_int_eq(current_step, 1u);

    /* Wrong step should be rejected: must compensate current step first. */
    check_int_eq(flowmq_saga_coordinator_record_compensation(&coordinator, &saga_id, 0u, now_ns),
                 TURBO_EINVAL);

    check_int_eq(flowmq_saga_coordinator_record_compensation(&coordinator, &saga_id, 1u, now_ns),
                 TURBO_OK);
    check_int_eq(flowmq_saga_coordinator_get_current_step(&coordinator, &saga_id, &current_step),
                 TURBO_OK);
    check_int_eq(current_step, 0u);

    check_int_eq(flowmq_saga_coordinator_record_compensation(&coordinator, &saga_id, 0u, now_ns),
                 TURBO_OK);

    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_OK);
    check_int_eq(state, FLOWMQ_SAGA_RUNTIME_ABORTED);

    tstr_free(saga_id);
    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("processes transaction timeouts") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    tstr_t saga_id = NULL;
    uint64_t now_ns = 1000000000ULL;
    uint64_t deadline_ns = now_ns + 1000000000ULL;  /* 1 second */

    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 1, deadline_ns, NULL, &saga_id),
                 TURBO_OK);

    tstr_t service = NULL, tx = NULL, comp = NULL;
    service = tstr_dup("service");
    tx = tstr_dup("tx");
    comp = tstr_dup("comp");
    check_int_eq(flowmq_saga_coordinator_add_step(&coordinator, &saga_id, &service, &tx, &comp),
                 TURBO_OK);
    check_int_eq(flowmq_saga_coordinator_start(&coordinator, &saga_id), TURBO_OK);

    /* Advance time past deadline */
    uint64_t expired_ns = deadline_ns + 1;
    uint32_t timed_out = flowmq_saga_coordinator_process_timeouts(&coordinator, expired_ns);
    check_int_eq(timed_out, 1);

    /* Check aborted */
    flowmq_saga_state_t state;
    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_OK);
    check_int_eq(state, FLOWMQ_SAGA_RUNTIME_ABORTED);

    tstr_free(saga_id);
    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("aborts SAGA transaction") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 10), TURBO_OK);

    tstr_t saga_id = NULL;
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 1, 2000000000ULL, NULL, &saga_id),
                 TURBO_OK);

    /* Abort immediately */
    check_int_eq(flowmq_saga_coordinator_abort(&coordinator, &saga_id), TURBO_OK);

    /* Transaction should not exist */
    flowmq_saga_state_t state;
    check_int_eq(flowmq_saga_coordinator_get_state(&coordinator, &saga_id, &state), TURBO_ENOENT);

    uint32_t active = 0;
    flowmq_saga_coordinator_stats(&coordinator, &active, NULL);
    check_int_eq(active, 0);

    tstr_free(saga_id);
    flowmq_saga_coordinator_destroy(&coordinator);
  }

  it("enforces capacity limit") {
    flowmq_saga_coordinator_t coordinator;
    check_int_eq(flowmq_saga_coordinator_init(&coordinator, 2), TURBO_OK);

    tstr_t id1 = NULL, id2 = NULL, id3 = NULL;

    /* Create first transaction */
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 1, 2000000000ULL, NULL, &id1),
                 TURBO_OK);

    /* Create second transaction */
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 1, 2000000000ULL, NULL, &id2),
                 TURBO_OK);

    /* Third should fail (capacity reached) */
    check_int_eq(flowmq_saga_coordinator_create(&coordinator, 1, 2000000000ULL, NULL, &id3),
                 TURBO_ENOSPC);

    tstr_free(id1);
    tstr_free(id2);
    flowmq_saga_coordinator_destroy(&coordinator);
  }
}


