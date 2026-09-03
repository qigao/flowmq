#include "flowmq_flow_control.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <stdint.h>

static flowmq_protocol_settings_t flowmq_flow_control_test_settings(
    uint64_t generation, uint64_t initial_max_data) {
  flowmq_protocol_settings_t settings = {
      .capabilities = FLOWMQ_PROTOCOL_CAP_FLOW_CREDIT,
      .max_frame_size = 8u,
      .session_generation = generation,
      .initial_max_data = initial_max_data,
      .flow_update_quantum = 2u,
      .flow_update_interval_ms = 10u};
  return settings;
}

spec("flowmq_flow_control") {
  it("admits sends only inside negotiated frame and cumulative credit limits") {
    flowmq_flow_control_t state = FLOWMQ_FLOW_CONTROL_INIT;
    flowmq_protocol_settings_t remote = flowmq_flow_control_test_settings(7u, 10u);

    check_equal(flowmq_flow_control_init_local(&state, 11u, 100u, 25u, 10u),
                TURBO_OK);
    check_equal(flowmq_flow_control_send_check(&state, 1u), TURBO_EBUSY);
    check_equal(flowmq_flow_control_apply_settings(&state, &remote), TURBO_OK);
    check_equal(flowmq_flow_control_send_commit(&state, 8u), TURBO_OK);
    check_equal(flowmq_flow_control_send_commit(&state, 2u), TURBO_OK);
    check_equal(flowmq_flow_control_send_check(&state, 1u), TURBO_ENOBUFS);
    check_equal(flowmq_flow_control_send_check(&state, 9u), TURBO_EMSGSIZE);
    check_equal(state.sent_data, 10u);
  }

  it("coalesces receive credit by quantum and advances it after send acceptance") {
    flowmq_flow_control_t state = FLOWMQ_FLOW_CONTROL_INIT;
    flowmq_protocol_flow_update_t update = {0};

    check_equal(flowmq_flow_control_init_local(&state, 11u, 100u, 25u, 10u),
                TURBO_OK);
    check_equal(flowmq_flow_control_receive_commit(&state, 100u), TURBO_OK);
    check_equal(flowmq_flow_control_receive_commit(&state, 1u), TURBO_EPROTO);
    check_equal(flowmq_flow_control_consume(&state, 25u, 1000u), TURBO_OK);
    check_equal(flowmq_flow_control_next_update(&state, 1000u, &update), TURBO_OK);
    check_equal(update.session_generation, 11u);
    check_equal(update.consumed_data, 25u);
    check_equal(update.max_data, 125u);
    check_equal(state.advertised_max_data, 100u);
    check_equal(flowmq_flow_control_mark_update_sent(&state, &update), TURBO_OK);
    check_equal(state.advertised_max_data, 125u);
    check_equal(flowmq_flow_control_next_update(&state, 1000u, &update),
                FLOWMQ_FLOW_CONTROL_NO_UPDATE);
  }

  it("emits a small pending update at the negotiated deadline") {
    flowmq_flow_control_t state = FLOWMQ_FLOW_CONTROL_INIT;
    flowmq_protocol_flow_update_t update = {0};
    const uint64_t start_ns = UINT64_C(1000000000);

    check_equal(flowmq_flow_control_init_local(&state, 3u, 100u, 25u, 10u),
                TURBO_OK);
    check_equal(flowmq_flow_control_receive_commit(&state, 1u), TURBO_OK);
    check_equal(flowmq_flow_control_consume(&state, 1u, start_ns), TURBO_OK);
    check_equal(flowmq_flow_control_next_update(
                    &state, start_ns + UINT64_C(9999999), &update),
                FLOWMQ_FLOW_CONTROL_NO_UPDATE);
    check_equal(flowmq_flow_control_next_update(
                    &state, start_ns + UINT64_C(10000000), &update),
                TURBO_OK);
    check_equal(update.consumed_data, 1u);
    check_equal(update.max_data, 101u);
  }

  it("rejects stale generations and decreasing remote cumulative credit") {
    flowmq_flow_control_t state = FLOWMQ_FLOW_CONTROL_INIT;
    flowmq_protocol_settings_t remote = flowmq_flow_control_test_settings(7u, 10u);
    flowmq_protocol_flow_update_t update = {
        .session_generation = 8u, .consumed_data = 1u, .max_data = 11u};

    check_equal(flowmq_flow_control_init_local(&state, 11u, 100u, 25u, 10u),
                TURBO_OK);
    check_equal(flowmq_flow_control_apply_settings(&state, &remote), TURBO_OK);
    check_equal(flowmq_flow_control_apply_remote_update(&state, &update), TURBO_EPROTO);
    update.session_generation = 7u;
    check_equal(flowmq_flow_control_apply_remote_update(&state, &update), TURBO_EPROTO);
    check_equal(flowmq_flow_control_send_commit(&state, 1u), TURBO_OK);
    check_equal(flowmq_flow_control_apply_remote_update(&state, &update), TURBO_OK);
    update.consumed_data = 0u;
    check_equal(flowmq_flow_control_apply_remote_update(&state, &update), TURBO_EPROTO);
    update.consumed_data = 1u;
    update.max_data = 10u;
    check_equal(flowmq_flow_control_apply_remote_update(&state, &update), TURBO_EPROTO);
  }

  it("fails fast when a receive-window update would overflow uint64") {
    flowmq_flow_control_t state = FLOWMQ_FLOW_CONTROL_INIT;
    flowmq_protocol_flow_update_t update = {0};

    check_equal(flowmq_flow_control_init_local(&state, 1u, UINT64_MAX, 1u, 10u),
                TURBO_OK);
    check_equal(flowmq_flow_control_receive_commit(&state, 1u), TURBO_OK);
    check_equal(flowmq_flow_control_consume(&state, 1u, 0u), TURBO_OK);
    check_equal(flowmq_flow_control_next_update(&state, 0u, &update), TURBO_ERANGE);
  }
}
