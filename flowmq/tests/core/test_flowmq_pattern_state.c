#include "flowmq_pattern_state.h"
#include "tinytest.h"
#include "salts_error.h"

spec("flowmq_pattern_state") {
  it("enforces the REQ send then receive FSM across multipart messages") {
    flowmq_pattern_state_t state = FLOWMQ_PATTERN_STATE_INIT;

    check_equal(flowmq_pattern_state_init(&state, FLOWMQ_PROTOCOL_REQ), SALTS_OK);
    check_equal(state.desc->fsm_class, FLOWMQ_PATTERN_FSM_REQ);
    check_equal(flowmq_pattern_state_send_validate(&state), SALTS_OK);
    flowmq_pattern_state_send_commit(&state, 1);
    check_equal(flowmq_pattern_state_receive_validate(&state), SALTS_EPROTO);
    check_equal(flowmq_pattern_state_send_validate(&state), SALTS_OK);
    flowmq_pattern_state_send_commit(&state, 0);

    check_equal(flowmq_pattern_state_send_validate(&state), SALTS_EPROTO);
    check_equal(flowmq_pattern_state_receive_validate(&state), SALTS_OK);
    flowmq_pattern_state_receive_commit(&state, 1);
    check_equal(flowmq_pattern_state_send_validate(&state), SALTS_EPROTO);
    check_equal(flowmq_pattern_state_receive_validate(&state), SALTS_OK);
    flowmq_pattern_state_receive_commit(&state, 0);
    check_equal(flowmq_pattern_state_send_validate(&state), SALTS_OK);
  }

  it("enforces the REP receive then send FSM across multipart messages") {
    flowmq_pattern_state_t state = FLOWMQ_PATTERN_STATE_INIT;

    check_equal(flowmq_pattern_state_init(&state, FLOWMQ_PROTOCOL_REP), SALTS_OK);
    check_equal(state.desc->fsm_class, FLOWMQ_PATTERN_FSM_REP);
    check_equal(flowmq_pattern_state_send_validate(&state), SALTS_EPROTO);
    check_equal(flowmq_pattern_state_receive_validate(&state), SALTS_OK);
    flowmq_pattern_state_receive_commit(&state, 1);
    check_equal(flowmq_pattern_state_send_validate(&state), SALTS_EPROTO);
    flowmq_pattern_state_receive_commit(&state, 0);

    check_equal(flowmq_pattern_state_receive_validate(&state), SALTS_EPROTO);
    check_equal(flowmq_pattern_state_send_validate(&state), SALTS_OK);
    flowmq_pattern_state_send_commit(&state, 1);
    check_equal(flowmq_pattern_state_receive_validate(&state), SALTS_EPROTO);
    flowmq_pattern_state_send_commit(&state, 0);
    check_equal(flowmq_pattern_state_receive_validate(&state), SALTS_OK);
  }

  it("derives send and receive capability for every public pattern") {
    static const unsigned char expected_send[FLOWMQ_PROTOCOL_XSUB + 1u] = {
        [FLOWMQ_PROTOCOL_PUB] = 1u,
        [FLOWMQ_PROTOCOL_SUB] = 0u,
        [FLOWMQ_PROTOCOL_PUSH] = 1u,
        [FLOWMQ_PROTOCOL_PULL] = 0u,
        [FLOWMQ_PROTOCOL_ROUTER] = 1u,
        [FLOWMQ_PROTOCOL_DEALER] = 1u,
        [FLOWMQ_PROTOCOL_PAIR] = 1u,
        [FLOWMQ_PROTOCOL_REQ] = 1u,
        [FLOWMQ_PROTOCOL_REP] = 1u,
        [FLOWMQ_PROTOCOL_XPUB] = 1u,
        [FLOWMQ_PROTOCOL_XSUB] = 1u};
    static const unsigned char expected_receive[FLOWMQ_PROTOCOL_XSUB + 1u] = {
        [FLOWMQ_PROTOCOL_PUB] = 0u,
        [FLOWMQ_PROTOCOL_SUB] = 1u,
        [FLOWMQ_PROTOCOL_PUSH] = 0u,
        [FLOWMQ_PROTOCOL_PULL] = 1u,
        [FLOWMQ_PROTOCOL_ROUTER] = 1u,
        [FLOWMQ_PROTOCOL_DEALER] = 1u,
        [FLOWMQ_PROTOCOL_PAIR] = 1u,
        [FLOWMQ_PROTOCOL_REQ] = 1u,
        [FLOWMQ_PROTOCOL_REP] = 1u,
        [FLOWMQ_PROTOCOL_XPUB] = 1u,
        [FLOWMQ_PROTOCOL_XSUB] = 1u};

    for (flowmq_protocol_pattern_t pattern = FLOWMQ_PROTOCOL_PUB;
         pattern <= FLOWMQ_PROTOCOL_XSUB; ++pattern) {
      check_equal(flowmq_pattern_can_send(pattern), expected_send[pattern]);
      check_equal(flowmq_pattern_can_receive(pattern),
                  expected_receive[pattern]);
    }
    check_false(flowmq_pattern_can_send(0u));
    check_false(flowmq_pattern_can_receive(0u));
    check_false(flowmq_pattern_can_send((flowmq_protocol_pattern_t)12u));
    check_false(flowmq_pattern_can_receive((flowmq_protocol_pattern_t)12u));
  }

  it("does not permit direction interleaving inside a multipart message") {
    flowmq_pattern_state_t state = FLOWMQ_PATTERN_STATE_INIT;

    check_equal(flowmq_pattern_state_init(&state, FLOWMQ_PROTOCOL_DEALER),
                SALTS_OK);
    flowmq_pattern_state_send_commit(&state, 1);
    check_equal(flowmq_pattern_state_receive_validate(&state), SALTS_EPROTO);
    flowmq_pattern_state_send_commit(&state, 0);
    check_equal(flowmq_pattern_state_receive_validate(&state), SALTS_OK);
    flowmq_pattern_state_receive_commit(&state, 1);
    check_equal(flowmq_pattern_state_send_validate(&state), SALTS_EPROTO);
    flowmq_pattern_state_receive_commit(&state, 0);
    check_equal(flowmq_pattern_state_send_validate(&state), SALTS_OK);
  }
}
