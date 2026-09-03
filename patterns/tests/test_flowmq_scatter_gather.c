/**
 * @file test_flowmq_scatter_gather.c
 * Unit tests for Scatter/Gather session management.
 */

#include "flowmq_scatter_gather.h"
#include "tinytest.h"

#include <string.h>

spec("flowmq_scatter_gather") {
  it("initializes and destroys manager") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 5000), TURBO_OK);

    uint32_t active = 0, capacity = 0;
    flowmq_scatter_gather_stats(&manager, &active, &capacity);
    check_equal(active, 0);
    check_equal(capacity, 10);

    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("rejects invalid manager initialization") {
    flowmq_scatter_gather_manager_t manager;

    /* NULL manager */
    check_equal(flowmq_scatter_gather_manager_init(NULL, 10, 5000), TURBO_EINVAL);

    /* Zero capacity */
    check_equal(flowmq_scatter_gather_manager_init(&manager, 0, 5000), TURBO_EINVAL);

    /* Exceeds maximum */
    check_equal(flowmq_scatter_gather_manager_init(&manager, 1000, 5000), TURBO_EINVAL);
  }

  it("creates scatter session with ALL policy") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 5000), TURBO_OK);

    uint64_t correlation_id = 0;
    uint64_t now_ns = 1000000000ULL;
    uint64_t deadline_ns = now_ns + 5000000000ULL; /* 5 seconds */

    check_equal(flowmq_scatter_gather_create_session(&manager, 3, FLOWMQ_ESB_GATHER_ALL,
                                                     deadline_ns, NULL, &correlation_id),
                TURBO_OK);
    check(correlation_id > 0);

    /* Check session exists */
    flowmq_scatter_session_state_t state;
    check_equal(flowmq_scatter_gather_check_session(&manager, correlation_id, &state), TURBO_OK);
    check_equal(state, FLOWMQ_SCATTER_PENDING);

    uint32_t active = 0;
    flowmq_scatter_gather_stats(&manager, &active, NULL);
    check_equal(active, 1);

    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("records partial responses and completes with ALL policy") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 5000), TURBO_OK);

    uint64_t correlation_id = 0;
    uint64_t now_ns = 1000000000ULL;

    check_equal(flowmq_scatter_gather_create_session(&manager, 3, FLOWMQ_ESB_GATHER_ALL,
                                                     now_ns + 5000000000ULL, NULL, &correlation_id),
                TURBO_OK);

    /* Record first response */
    tstr payload1 = tstr_dup("response 1");
    check_equal(flowmq_scatter_gather_record_response(&manager, correlation_id, 0, &payload1,
                                                      TURBO_OK, now_ns),
                TURBO_OK);

    /* Session still pending (need 3 responses) */
    flowmq_scatter_session_state_t state;
    check_equal(flowmq_scatter_gather_check_session(&manager, correlation_id, &state), TURBO_OK);
    check_equal(state, FLOWMQ_SCATTER_PENDING);

    /* Record second response */
    tstr payload2 = tstr_dup("response 2");
    check_equal(flowmq_scatter_gather_record_response(&manager, correlation_id, 1, &payload2,
                                                      TURBO_OK, now_ns),
                TURBO_OK);

    /* Still pending */
    check_equal(flowmq_scatter_gather_check_session(&manager, correlation_id, &state), TURBO_OK);
    check_equal(state, FLOWMQ_SCATTER_PENDING);

    /* Record third response */
    tstr payload3 = tstr_dup("response 3");
    check_equal(flowmq_scatter_gather_record_response(&manager, correlation_id, 2, &payload3,
                                                      TURBO_OK, now_ns),
                TURBO_OK);

    /* Now completed */
    check_equal(flowmq_scatter_gather_check_session(&manager, correlation_id, &state), TURBO_OK);
    check_equal(state, FLOWMQ_SCATTER_COMPLETED);

    /* Finalize session */
    flowmq_scatter_session_t session;
    check_equal(flowmq_scatter_gather_finalize_session(&manager, correlation_id, &session),
                TURBO_OK);
    check_equal(session.received_responses, 3);
    check_equal(session.success_responses, 3);
    check_equal(session.error_responses, 0);

    flowmq_scatter_session_cleanup(&session);
    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("completes with QUORUM policy") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 5000), TURBO_OK);

    uint64_t correlation_id = 0;
    uint64_t now_ns = 1000000000ULL;

    /* Need 5 responses, quorum is 3 (5/2 + 1) */
    check_equal(flowmq_scatter_gather_create_session(&manager, 5, FLOWMQ_ESB_GATHER_QUORUM,
                                                     now_ns + 5000000000ULL, NULL, &correlation_id),
                TURBO_OK);

    /* Record 3 responses - should complete */
    for (uint32_t i = 0; i < 3; i++) {
      tstr payload = tstr_dup("response");
      check_equal(flowmq_scatter_gather_record_response(&manager, correlation_id, i, &payload,
                                                        TURBO_OK, now_ns),
                  TURBO_OK);
    }

    /* Check completed */
    flowmq_scatter_session_state_t state;
    check_equal(flowmq_scatter_gather_check_session(&manager, correlation_id, &state), TURBO_OK);
    check_equal(state, FLOWMQ_SCATTER_COMPLETED);

    flowmq_scatter_session_t session;
    check_equal(flowmq_scatter_gather_finalize_session(&manager, correlation_id, &session),
                TURBO_OK);
    check_equal(session.received_responses, 3);

    flowmq_scatter_session_cleanup(&session);
    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("handles error responses") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 5000), TURBO_OK);

    uint64_t correlation_id = 0;
    uint64_t now_ns = 1000000000ULL;

    check_equal(flowmq_scatter_gather_create_session(&manager, 2, FLOWMQ_ESB_GATHER_ALL,
                                                     now_ns + 5000000000ULL, NULL, &correlation_id),
                TURBO_OK);

    /* First response successful */
    tstr payload1 = tstr_dup("ok");
    check_equal(flowmq_scatter_gather_record_response(&manager, correlation_id, 0, &payload1,
                                                      TURBO_OK, now_ns),
                TURBO_OK);

    /* Second response failed */
    tstr payload2 = tstr_dup("error");
    check_equal(flowmq_scatter_gather_record_response(&manager, correlation_id, 1, &payload2,
                                                      TURBO_ECONNREFUSED, now_ns),
                TURBO_OK);

    /* Should complete (all responses received) */
    flowmq_scatter_session_t session;
    check_equal(flowmq_scatter_gather_finalize_session(&manager, correlation_id, &session),
                TURBO_OK);
    check_equal(session.success_responses, 1);
    check_equal(session.error_responses, 1);

    flowmq_scatter_session_cleanup(&session);
    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("rejects duplicate response for same index") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 5000), TURBO_OK);

    uint64_t correlation_id = 0;
    uint64_t now_ns = 1000000000ULL;

    check_equal(flowmq_scatter_gather_create_session(&manager, 2, FLOWMQ_ESB_GATHER_ALL,
                                                     now_ns + 5000000000ULL, NULL, &correlation_id),
                TURBO_OK);

    tstr payload1 = tstr_dup("first");
    check_equal(flowmq_scatter_gather_record_response(&manager, correlation_id, 0, &payload1,
                                                      TURBO_OK, now_ns),
                TURBO_OK);

    /* Try to record duplicate */
    tstr payload2 = tstr_dup("duplicate");
    check_equal(flowmq_scatter_gather_record_response(&manager, correlation_id, 0, &payload2,
                                                      TURBO_OK, now_ns),
                TURBO_EALREADY);
    tstr_free(payload2); /* Must cleanup since rejected */

    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("processes session timeouts") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 5000), TURBO_OK);

    uint64_t correlation_id = 0;
    uint64_t now_ns = 1000000000ULL;
    uint64_t deadline_ns = now_ns + 1000000000ULL; /* 1 second */

    check_equal(flowmq_scatter_gather_create_session(&manager, 3, FLOWMQ_ESB_GATHER_ALL,
                                                     deadline_ns, NULL, &correlation_id),
                TURBO_OK);

    /* Record partial response */
    tstr payload = tstr_dup("partial");
    check_equal(flowmq_scatter_gather_record_response(&manager, correlation_id, 0, &payload,
                                                      TURBO_OK, now_ns),
                TURBO_OK);

    /* Advance time past deadline */
    uint64_t expired_ns = deadline_ns + 1;
    uint32_t timed_out = flowmq_scatter_gather_process_timeouts(&manager, expired_ns, NULL, NULL);
    check_equal(timed_out, 1);

    /* Check session is timed out */
    flowmq_scatter_session_state_t state;
    check_equal(flowmq_scatter_gather_check_session(&manager, correlation_id, &state), TURBO_OK);
    check_equal(state, FLOWMQ_SCATTER_TIMEOUT);

    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("uses manager default timeout when timeout_ms is zero") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 50), TURBO_OK);

    uint64_t correlation_id = 0;
    uint64_t now_ns = turbo_hrtime();

    /* timeout_ns == 0 should be converted with manager.default_timeout_ms (50 ms). */
    check_equal(flowmq_scatter_gather_create_session(&manager, 2, FLOWMQ_ESB_GATHER_ALL, 0ULL, NULL,
                                                     &correlation_id),
                TURBO_OK);

    uint32_t timed_out = flowmq_scatter_gather_process_timeouts(
        &manager, now_ns + FLOWMQ_SCATTER_GATHER_NS_PER_MS / 2, NULL, NULL);
    check_equal(timed_out, 0);

    timed_out = flowmq_scatter_gather_process_timeouts(
        &manager, now_ns + FLOWMQ_SCATTER_GATHER_NS_PER_MS * 100ULL, NULL, NULL);
    check_equal(timed_out, 1);

    flowmq_scatter_session_state_t state;
    check_equal(flowmq_scatter_gather_check_session(&manager, correlation_id, &state), TURBO_OK);
    check_equal(state, FLOWMQ_SCATTER_TIMEOUT);

    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("creates session with relative timeout in milliseconds explicitly") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 50), TURBO_OK);

    uint64_t correlation_id = 0;
    check_equal(flowmq_scatter_gather_create_session_ms(&manager, 1, FLOWMQ_ESB_GATHER_ALL, 2u,
                                                        NULL, &correlation_id),
                TURBO_OK);

    uint64_t now_ns = turbo_hrtime();
    uint32_t timed_out = flowmq_scatter_gather_process_timeouts(
        &manager, now_ns + FLOWMQ_SCATTER_GATHER_NS_PER_MS / 2, NULL, NULL);
    check_equal(timed_out, 0);

    timed_out = flowmq_scatter_gather_process_timeouts(
        &manager, now_ns + FLOWMQ_SCATTER_GATHER_NS_PER_MS * 3, NULL, NULL);
    check_equal(timed_out, 1);

    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("rejects overflow for relative timeout conversion in milliseconds") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 50), TURBO_OK);
    check_equal(flowmq_scatter_gather_create_session_ms(&manager, 1, FLOWMQ_ESB_GATHER_ALL,
                                                        FLOWMQ_SCATTER_GATHER_MAX_TIMEOUT_MS + 1,
                                                        NULL, NULL),
                TURBO_ERANGE);
    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("cancels active session") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 10, 5000), TURBO_OK);

    uint64_t correlation_id = 0;
    check_equal(flowmq_scatter_gather_create_session(&manager, 2, FLOWMQ_ESB_GATHER_ALL,
                                                     2000000000ULL, NULL, &correlation_id),
                TURBO_OK);

    /* Cancel session */
    check_equal(flowmq_scatter_gather_cancel_session(&manager, correlation_id), TURBO_OK);

    /* Session should not exist */
    flowmq_scatter_session_state_t state;
    check_equal(flowmq_scatter_gather_check_session(&manager, correlation_id, &state),
                TURBO_ENOENT);

    uint32_t active = 0;
    flowmq_scatter_gather_stats(&manager, &active, NULL);
    check_equal(active, 0);

    flowmq_scatter_gather_manager_destroy(&manager);
  }

  it("enforces capacity limit") {
    flowmq_scatter_gather_manager_t manager;
    check_equal(flowmq_scatter_gather_manager_init(&manager, 2, 5000), TURBO_OK);

    uint64_t id1 = 0, id2 = 0, id3 = 0;

    /* Create first session */
    check_equal(flowmq_scatter_gather_create_session(&manager, 1, FLOWMQ_ESB_GATHER_ALL,
                                                     2000000000ULL, NULL, &id1),
                TURBO_OK);

    /* Create second session */
    check_equal(flowmq_scatter_gather_create_session(&manager, 1, FLOWMQ_ESB_GATHER_ALL,
                                                     2000000000ULL, NULL, &id2),
                TURBO_OK);

    /* Third should fail (capacity reached) */
    check_equal(flowmq_scatter_gather_create_session(&manager, 1, FLOWMQ_ESB_GATHER_ALL,
                                                     2000000000ULL, NULL, &id3),
                TURBO_ENOSPC);

    flowmq_scatter_gather_manager_destroy(&manager);
  }
}
