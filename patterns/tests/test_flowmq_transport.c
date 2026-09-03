#include "flowmq_transport.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("flowmq_transport") {
  it("admits only the TCP and TLS phase-one transports") {
    check_equal(flowmq_transport_validate(FLOWMQ_TRANSPORT_TCP), TURBO_OK);
    check_equal(flowmq_transport_validate(FLOWMQ_TRANSPORT_TLS), TURBO_OK);
    check_equal(flowmq_transport_validate((flowmq_transport_t)0), TURBO_EINVAL);
    check_equal(flowmq_transport_validate((flowmq_transport_t)3), TURBO_EINVAL);
  }

  it("resolves operation timeouts from one explicit default") {
    flowmq_timeout_config_t timeouts = {0};
    timeouts.timeout_ms = 250u;
    timeouts.set_flags = FLOWMQ_TIMEOUT_SET_DEFAULT;
    flowmq_timeouts_resolve(&timeouts, 1000u);
    check_equal(timeouts.connect_timeout_ms, 250u);
    check_equal(timeouts.send_timeout_ms, 250u);
    check_equal(timeouts.recv_timeout_ms, 250u);
    check_equal(timeouts.handshake_timeout_ms, 250u);
    check_equal(timeouts.explicit_flags, FLOWMQ_TIMEOUT_SET_DEFAULT);
  }

  it("provides valid bounded CNet runtime defaults") {
    flowmq_io_config_t io;
    memset(&io, 0, sizeof(io));
    flowmq_io_config_init(&io);
    check_true(io.command_capacity != 0u);
    check_equal(io.command_capacity & (io.command_capacity - 1u), (size_t)0u);
    check_true(io.request_capacity != 0u);
    check_true(io.completion_batch_capacity != 0u);
    check_true(io.completion_batch_capacity <= io.request_capacity);
    check_true(io.event_capacity >= 2u);
    check_equal(io.event_capacity & (io.event_capacity - 1u), (size_t)0u);
    check_true(io.receive_buffer_bytes != 0u);
    check_true(io.tls_io_buffer_bytes >= 17u * 1024u);
    check_equal(flowmq_io_config_validate(&io), TURBO_OK);
    io.command_capacity = 3u;
    check_equal(flowmq_io_config_validate(&io), TURBO_EINVAL);
  }
}
