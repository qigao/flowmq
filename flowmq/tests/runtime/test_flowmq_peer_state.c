#include "flowmq_peer_state.h"

#include "salts_error.h"
#include "tinytest.h"

spec("flowmq_peer_state") {
  it("enforces lifecycle transitions without conflating retirement and free") {
    flowmq_peer_state_t state = FLOWMQ_PEER_STATE_INIT;

    check_false(flowmq_peer_state_is_used(&state));
    check_equal(flowmq_peer_state_allocate(&state), SALTS_OK);
    check_true(flowmq_peer_state_is_used(&state));
    check_false(flowmq_peer_state_is_connected(&state));

    check_equal(flowmq_peer_state_transition(
                    &state, FLOWMQ_PEER_LIFECYCLE_CONNECTED),
                SALTS_OK);
    check_true(flowmq_peer_state_is_connected(&state));
    check_equal(flowmq_peer_state_transition(
                    &state, FLOWMQ_PEER_LIFECYCLE_FREE),
                SALTS_EPROTO);

    check_equal(flowmq_peer_state_transition(
                    &state, FLOWMQ_PEER_LIFECYCLE_CLOSE_RETRY),
                SALTS_OK);
    check_true(flowmq_peer_state_needs_close_retry(&state));
    check_equal(flowmq_peer_state_transition(
                    &state, FLOWMQ_PEER_LIFECYCLE_CLOSING),
                SALTS_OK);
    check_equal(flowmq_peer_state_transition(
                    &state, FLOWMQ_PEER_LIFECYCLE_RETIRED),
                SALTS_OK);
    check_true(flowmq_peer_state_is_retired(&state));
    check_true(flowmq_peer_state_is_used(&state));

    check_equal(flowmq_peer_state_release(&state), SALTS_OK);
    check_false(flowmq_peer_state_is_used(&state));
  }

  it("models TX and RX handshake chains as independent partial orders") {
    flowmq_peer_state_t state = FLOWMQ_PEER_STATE_INIT;

    check_equal(flowmq_peer_state_allocate(&state), SALTS_OK);
    check_equal(flowmq_peer_state_transition(
                    &state, FLOWMQ_PEER_LIFECYCLE_CONNECTED),
                SALTS_OK);

    check_equal(flowmq_peer_state_handshake_mark(
                    &state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_TX),
                SALTS_EPROTO);
    check_equal(flowmq_peer_state_handshake_mark(
                    &state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_RX),
                SALTS_EPROTO);

    check_equal(flowmq_peer_state_handshake_mark(
                    &state, FLOWMQ_PEER_HANDSHAKE_HELLO_RX),
                SALTS_OK);
    check_equal(flowmq_peer_state_handshake_mark(
                    &state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_RX),
                SALTS_OK);
    check_false(flowmq_peer_state_ready(&state));

    check_equal(flowmq_peer_state_handshake_mark(
                    &state, FLOWMQ_PEER_HANDSHAKE_HELLO_TX),
                SALTS_OK);
    check_false(flowmq_peer_state_ready(&state));
    check_equal(flowmq_peer_state_handshake_mark(
                    &state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_TX),
                SALTS_OK);
    check_true(flowmq_peer_state_ready(&state));
  }

  it("serializes one write lane and derives TX handshake completion") {
    flowmq_peer_state_t state = FLOWMQ_PEER_STATE_INIT;
    flowmq_peer_write_lane_t completed = FLOWMQ_PEER_WRITE_IDLE;

    check_equal(flowmq_peer_state_allocate(&state), SALTS_OK);
    check_equal(flowmq_peer_state_transition(
                    &state, FLOWMQ_PEER_LIFECYCLE_CONNECTED),
                SALTS_OK);

    check_equal(flowmq_peer_state_write_begin(
                    &state, FLOWMQ_PEER_WRITE_HELLO),
                SALTS_OK);
    check_false(flowmq_peer_state_write_idle(&state));
    check_equal(flowmq_peer_state_write_begin(
                    &state, FLOWMQ_PEER_WRITE_CONTROL),
                SALTS_EBUSY);
    check_equal(flowmq_peer_state_write_complete(&state, &completed),
                SALTS_OK);
    check_equal(completed, FLOWMQ_PEER_WRITE_HELLO);
    check_true(flowmq_peer_state_handshake_has(
        &state, FLOWMQ_PEER_HANDSHAKE_HELLO_TX));
    check_true(flowmq_peer_state_write_idle(&state));

    check_equal(flowmq_peer_state_write_begin(
                    &state, FLOWMQ_PEER_WRITE_SETTINGS),
                SALTS_OK);
    check_equal(flowmq_peer_state_write_complete(&state, &completed),
                SALTS_OK);
    check_equal(completed, FLOWMQ_PEER_WRITE_SETTINGS);
    check_true(flowmq_peer_state_handshake_has(
        &state, FLOWMQ_PEER_HANDSHAKE_SETTINGS_TX));

    check_equal(flowmq_peer_state_write_begin(
                    &state, FLOWMQ_PEER_WRITE_CONTROL),
                SALTS_OK);
    check_equal(flowmq_peer_state_write_complete(&state, &completed),
                SALTS_OK);
    check_equal(completed, FLOWMQ_PEER_WRITE_CONTROL);
  }

  it("rejects duplicate handshake facts and illegal write completion") {
    flowmq_peer_state_t state = FLOWMQ_PEER_STATE_INIT;

    check_equal(flowmq_peer_state_allocate(&state), SALTS_OK);
    check_equal(flowmq_peer_state_transition(
                    &state, FLOWMQ_PEER_LIFECYCLE_CONNECTED),
                SALTS_OK);
    check_equal(flowmq_peer_state_handshake_mark(
                    &state, FLOWMQ_PEER_HANDSHAKE_HELLO_RX),
                SALTS_OK);
    check_equal(flowmq_peer_state_handshake_mark(
                    &state, FLOWMQ_PEER_HANDSHAKE_HELLO_RX),
                SALTS_EALREADY);
    check_equal(flowmq_peer_state_write_complete(&state, NULL),
                SALTS_EPROTO);
  }

  it("permits failed allocation to return directly to free") {
    flowmq_peer_state_t state = FLOWMQ_PEER_STATE_INIT;

    check_equal(flowmq_peer_state_allocate(&state), SALTS_OK);
    check_equal(flowmq_peer_state_release(&state), SALTS_OK);
    check_equal(state.lifecycle, FLOWMQ_PEER_LIFECYCLE_FREE);
    check_equal(state.handshake, 0u);
    check_equal(state.write_lane, FLOWMQ_PEER_WRITE_IDLE);
  }
}
