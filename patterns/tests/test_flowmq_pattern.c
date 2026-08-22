#include "flowmq_core.h"
#include "flowmq_pattern.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("flowmq_pattern") {
  it("defines all FMQ v3 peer relationships without TurboFlow") {
    check_equal(flowmq_core_pattern_validate(FLOWMQ_PROTOCOL_REQ), TURBO_OK);
    check_equal(flowmq_core_pattern_validate(0u), TURBO_EINVAL);
    check_true(flowmq_core_patterns_compatible(FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_REP));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_PUB, FLOWMQ_PROTOCOL_SUB));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_PUB, FLOWMQ_PROTOCOL_XSUB));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_XPUB, FLOWMQ_PROTOCOL_SUB));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_XPUB, FLOWMQ_PROTOCOL_XSUB));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_REP));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_ROUTER, FLOWMQ_PROTOCOL_DEALER));
    check_false(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_DEALER));
  }

  it("requires a compatible HELLO and DEALER identity") {
    flowmq_protocol_frame_t hello;
    memset(&hello, 0, sizeof(hello));
    hello.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
    hello.pattern = FLOWMQ_PROTOCOL_DEALER;

    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello), TURBO_EPROTO);
    hello.identity = vstr_from_cstr("worker-1");
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello), TURBO_OK);
    hello.payload = vstr_from_cstr("not-allowed");
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello), TURBO_EPROTO);
  }

  it("rejects data received by send-only roles") {
    flowmq_protocol_frame_t frame;
    memset(&frame, 0, sizeof(frame));
    frame.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    frame.pattern = FLOWMQ_PROTOCOL_SUB;

    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PUB, &frame), TURBO_EPROTO);
    frame.pattern = FLOWMQ_PROTOCOL_PUB;
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_SUB, &frame), TURBO_OK);
  }

  it("encodes client handshake and control commands") {
    flowmq_protocol_frame_t frame;
    tstr encoded = NULL;
    size_t consumed = 0u;

    check_equal(flowmq_pattern_encode_hello(FLOWMQ_PROTOCOL_DEALER, vstr_from_cstr("worker-1"),
                                             (vstr){0}, 1024u, &encoded),
                 TURBO_OK);
    check_equal(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &frame, &consumed),
                 TURBO_OK);
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &frame), TURBO_OK);
    flowmq_protocol_frame_cleanup(&frame);
    tstr_freep(&encoded);

    check_equal(flowmq_pattern_encode_heartbeat(FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_FRAME_PING,
                                                 1024u, &encoded),
                 TURBO_OK);
    tstr_freep(&encoded);
    check_equal(flowmq_pattern_encode_subscription(FLOWMQ_PROTOCOL_XSUB,
                                                    FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
                                                    vstr_from_cstr("orders."), 1024u, &encoded),
                 TURBO_OK);
    tstr_freep(&encoded);
    check_equal(flowmq_pattern_encode_subscription(FLOWMQ_PROTOCOL_PUB,
                                                    FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
                                                    vstr_from_cstr("orders."), 1024u, &encoded),
                 TURBO_EINVAL);
  }
}
