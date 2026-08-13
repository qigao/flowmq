#include "flowmq_coronet.h"
#include "tinytest.h"
#include "turbo_error.h"

spec("flowmq_coronet_transport") {
  it("establishes the process-wide TLS 1.3-only profile") {
    check_int_eq(flowmq_coronet_tls_require_tls13(), TURBO_OK);
    check_true(flowmq_coronet_tls_is_tls13_only());
  }

  it("validates every standalone transport value") {
    check_int_eq(flowmq_coronet_transport_validate(FLOWMQ_TRANSPORT_TCP), TURBO_OK);
    check_int_eq(flowmq_coronet_transport_validate(FLOWMQ_TRANSPORT_TLS), TURBO_OK);
    check_int_eq(flowmq_coronet_transport_validate(FLOWMQ_TRANSPORT_UDP), TURBO_OK);
    check_int_eq(flowmq_coronet_transport_validate(FLOWMQ_TRANSPORT_KCP), TURBO_OK);
    check_int_eq(flowmq_coronet_transport_validate(FLOWMQ_TRANSPORT_PIPE), TURBO_OK);
    check_int_eq(flowmq_coronet_transport_validate(FLOWMQ_TRANSPORT_WS), TURBO_OK);
    check_int_eq(flowmq_coronet_transport_validate(FLOWMQ_TRANSPORT_WSS), TURBO_OK);
    check_int_eq(flowmq_coronet_transport_validate((flowmq_coronet_transport_t)0), TURBO_EINVAL);
  }

  it("resolves operation timeouts from one explicit default") {
    flowmq_coronet_timeout_config_t timeouts = {0};
    timeouts.timeout_ms = 250u;
    timeouts.set_flags = FLOWMQ_TIMEOUT_SET_DEFAULT;
    flowmq_coronet_timeouts_resolve(&timeouts, 1000u);
    check_uint_eq(timeouts.connect_timeout_ms, 250u);
    check_uint_eq(timeouts.send_timeout_ms, 250u);
    check_uint_eq(timeouts.recv_timeout_ms, 250u);
    check_uint_eq(timeouts.handshake_timeout_ms, 250u);
    check_uint_eq(timeouts.explicit_flags, FLOWMQ_TIMEOUT_SET_DEFAULT);
  }
}
