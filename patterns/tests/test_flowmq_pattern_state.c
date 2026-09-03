#include "flowmq_pattern_state.h"
#include "tinytest.h"
#include "turbo_error.h"

spec("flowmq_pattern_state") {
  it("enforces the REQ send then receive FSM across multipart messages") {
    flowmq_pattern_state_t state = FLOWMQ_PATTERN_STATE_INIT;

    check_equal(flowmq_pattern_state_init(&state, FLOWMQ_PROTOCOL_REQ), TURBO_OK);
    check_equal(flowmq_pattern_state_send_validate(&state), TURBO_OK);
    flowmq_pattern_state_send_commit(&state, 1);
    check_equal(flowmq_pattern_state_receive_validate(&state), TURBO_EBUSY);
    check_equal(flowmq_pattern_state_send_validate(&state), TURBO_OK);
    flowmq_pattern_state_send_commit(&state, 0);

    check_equal(flowmq_pattern_state_send_validate(&state), TURBO_EBUSY);
    check_equal(flowmq_pattern_state_receive_validate(&state), TURBO_OK);
    flowmq_pattern_state_receive_commit(&state, 1);
    check_equal(flowmq_pattern_state_send_validate(&state), TURBO_EBUSY);
    check_equal(flowmq_pattern_state_receive_validate(&state), TURBO_OK);
    flowmq_pattern_state_receive_commit(&state, 0);
    check_equal(flowmq_pattern_state_send_validate(&state), TURBO_OK);
  }

  it("enforces the REP receive then send FSM across multipart messages") {
    flowmq_pattern_state_t state = FLOWMQ_PATTERN_STATE_INIT;

    check_equal(flowmq_pattern_state_init(&state, FLOWMQ_PROTOCOL_REP), TURBO_OK);
    check_equal(flowmq_pattern_state_send_validate(&state), TURBO_EBUSY);
    check_equal(flowmq_pattern_state_receive_validate(&state), TURBO_OK);
    flowmq_pattern_state_receive_commit(&state, 1);
    check_equal(flowmq_pattern_state_send_validate(&state), TURBO_EBUSY);
    flowmq_pattern_state_receive_commit(&state, 0);

    check_equal(flowmq_pattern_state_receive_validate(&state), TURBO_EBUSY);
    check_equal(flowmq_pattern_state_send_validate(&state), TURBO_OK);
    flowmq_pattern_state_send_commit(&state, 1);
    check_equal(flowmq_pattern_state_receive_validate(&state), TURBO_EBUSY);
    flowmq_pattern_state_send_commit(&state, 0);
    check_equal(flowmq_pattern_state_receive_validate(&state), TURBO_OK);
  }

  it("defines send and receive capabilities for all classic patterns") {
    check_true(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_PAIR));
    check_true(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_PAIR));
    check_true(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_PUB));
    check_false(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_PUB));
    check_false(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_SUB));
    check_true(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_SUB));
    check_true(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_PUSH));
    check_false(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_PUSH));
    check_false(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_PULL));
    check_true(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_PULL));
    check_true(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_REQ));
    check_true(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_REQ));
    check_true(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_REP));
    check_true(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_REP));
    check_true(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_DEALER));
    check_true(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_DEALER));
    check_true(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_ROUTER));
    check_true(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_ROUTER));
    check_true(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_XPUB));
    check_true(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_XPUB));
    check_true(flowmq_pattern_can_send(FLOWMQ_PROTOCOL_XSUB));
    check_true(flowmq_pattern_can_receive(FLOWMQ_PROTOCOL_XSUB));
    check_false(flowmq_pattern_can_send((flowmq_protocol_pattern_t)12u));
    check_false(flowmq_pattern_can_receive((flowmq_protocol_pattern_t)12u));
  }

  it("does not permit direction interleaving inside a multipart message") {
    flowmq_pattern_state_t state = FLOWMQ_PATTERN_STATE_INIT;

    check_equal(flowmq_pattern_state_init(&state, FLOWMQ_PROTOCOL_DEALER), TURBO_OK);
    flowmq_pattern_state_send_commit(&state, 1);
    check_equal(flowmq_pattern_state_receive_validate(&state), TURBO_EBUSY);
    flowmq_pattern_state_send_commit(&state, 0);
    check_equal(flowmq_pattern_state_receive_validate(&state), TURBO_OK);
    flowmq_pattern_state_receive_commit(&state, 1);
    check_equal(flowmq_pattern_state_send_validate(&state), TURBO_EBUSY);
    flowmq_pattern_state_receive_commit(&state, 0);
    check_equal(flowmq_pattern_state_send_validate(&state), TURBO_OK);
  }
}
