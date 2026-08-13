#include "flowmq_peer_session.h"
#include "tinytest.h"
#include "turbo_error.h"

spec("flowmq_peer_session") {
  it("fences late replies when a new HELLO establishes a session") {
    flowmq_peer_session_t session;
    flowmq_peer_exchange_state_t state = FLOWMQ_PEER_EXCHANGE_RESETTING;
    uint64_t first_generation = 0u;
    uint64_t second_generation = 0u;
    uint64_t snapshot_generation = 0u;
    uint64_t correlation_id = 0u;

    check_int_eq(flowmq_peer_session_init(&session, 1u), TURBO_OK);
    check_int_eq(flowmq_peer_session_handshake_complete(&session, &first_generation), TURBO_OK);
    check_uint_eq(first_generation, 2u);
    check_int_eq(flowmq_peer_session_begin(&session, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY, 41u,
                                           &snapshot_generation),
                 TURBO_OK);
    check_uint_eq(snapshot_generation, first_generation);
    check_int_eq(flowmq_peer_session_finish(&session, first_generation,
                                            FLOWMQ_PEER_EXCHANGE_WAIT_REPLY, 41u,
                                            FLOWMQ_PEER_EXCHANGE_RESETTING),
                 TURBO_OK);

    check_int_eq(flowmq_peer_session_handshake_complete(&session, &second_generation), TURBO_OK);
    check_uint_eq(second_generation, 3u);
    check_int_eq(
        flowmq_peer_session_match(&session, first_generation, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY, 41u),
        TURBO_ENOTCONN);
    check_int_eq(
        flowmq_peer_session_snapshot(&session, &state, &snapshot_generation, &correlation_id),
        TURBO_OK);
    check_int_eq(state, FLOWMQ_PEER_EXCHANGE_READY);
    check_uint_eq(snapshot_generation, second_generation);
    check_uint_eq(correlation_id, 0u);
  }

  it("enforces one synchronous request correlation") {
    flowmq_peer_session_t session;
    uint64_t generation = 0u;
    uint64_t ignored_generation = 0u;

    check_int_eq(flowmq_peer_session_init(&session, 7u), TURBO_OK);
    check_int_eq(
        flowmq_peer_session_begin(&session, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY, 51u, &generation),
        TURBO_OK);
    check_int_eq(flowmq_peer_session_begin(&session, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY, 52u,
                                           &ignored_generation),
                 TURBO_EBUSY);
    check_int_eq(
        flowmq_peer_session_match(&session, generation, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY, 52u),
        TURBO_EPROTO);
    check_int_eq(flowmq_peer_session_finish(&session, generation, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY,
                                            51u, FLOWMQ_PEER_EXCHANGE_READY),
                 TURBO_OK);
  }

  it("keeps REP processing synchronous and resettable") {
    flowmq_peer_session_t session;
    uint64_t generation = 0u;

    check_int_eq(flowmq_peer_session_init(&session, 11u), TURBO_OK);
    check_int_eq(flowmq_peer_session_begin(&session, FLOWMQ_PEER_EXCHANGE_PROCESSING_REQUEST, 61u,
                                           &generation),
                 TURBO_OK);
    check_int_eq(flowmq_peer_session_mark_resetting(&session), TURBO_OK);
    check_int_eq(flowmq_peer_session_finish(&session, generation,
                                            FLOWMQ_PEER_EXCHANGE_PROCESSING_REQUEST, 61u,
                                            FLOWMQ_PEER_EXCHANGE_READY),
                 TURBO_EBUSY);
  }
}
