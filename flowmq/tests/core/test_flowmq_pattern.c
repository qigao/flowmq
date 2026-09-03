#include "flowmq_core.h"
#include "flowmq_pattern.h"
#include "flowmq_security.h"
#include "tinytest.h"
#include "salts_error.h"

#include <string.h>

spec("flowmq_pattern") {
  it("defines all FMQ/6 peer relationships") {
    check_equal(flowmq_core_pattern_validate(FLOWMQ_PROTOCOL_REQ), SALTS_OK);
    check_equal(flowmq_core_pattern_validate(0u), SALTS_EINVAL);
    check_equal(flowmq_core_pattern_validate(12u), SALTS_EINVAL);
    check_equal(flowmq_pattern_validate(12u), SALTS_EINVAL);
    check_true(flowmq_core_patterns_compatible(FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_REP));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_PUB, FLOWMQ_PROTOCOL_SUB));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_PUB, FLOWMQ_PROTOCOL_XSUB));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_XPUB, FLOWMQ_PROTOCOL_SUB));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_XPUB, FLOWMQ_PROTOCOL_XSUB));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_REP));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_ROUTER));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_REP, FLOWMQ_PROTOCOL_DEALER));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_ROUTER, FLOWMQ_PROTOCOL_DEALER));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_ROUTER, FLOWMQ_PROTOCOL_ROUTER));
    check_true(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_DEALER, FLOWMQ_PROTOCOL_DEALER));
    check_false(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_DEALER));
  }

  it("requires a compatible HELLO and DEALER identity") {
    flowmq_protocol_frame_t hello;
    memset(&hello, 0, sizeof(hello));
    hello.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
    hello.pattern = FLOWMQ_PROTOCOL_DEALER;

    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello), SALTS_EPROTO);
    hello.identity = vstr_from_cstr("worker-1");
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello), SALTS_OK);
    hello.payload = vstr_from_cstr("not-allowed");
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello), SALTS_EPROTO);
  }

  it("rejects an unbound FMS/3 envelope at the socket handshake boundary") {
    flowmq_security_t security = {.mode = FLOWMQ_SECURITY_AUTH,
                                  .identity = vstr_from_cstr("worker-1"),
                                  .method = vstr_from_cstr("token"),
                                  .secret = vstr_from_cstr("unverified")};
    flowmq_protocol_frame_t hello = {.kind = FLOWMQ_PROTOCOL_FRAME_HELLO,
                                     .pattern = FLOWMQ_PROTOCOL_DEALER,
                                     .identity = {.data = "worker-1", .len = 8u}};
    tstr payload = NULL;

    check_equal(flowmq_security_encode(&security, &payload), SALTS_OK);
    hello.payload = tstr_to_v(payload);
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello), SALTS_OK);
    check_equal(flowmq_pattern_socket_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello), SALTS_EPROTO);
    tstr_free(payload);
  }

  it("rejects data received by send-only roles") {
    flowmq_protocol_frame_t frame;
    memset(&frame, 0, sizeof(frame));
    frame.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    frame.pattern = FLOWMQ_PROTOCOL_SUB;

    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PUB, &frame), SALTS_EPROTO);
    frame.pattern = FLOWMQ_PROTOCOL_PUB;
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_SUB, &frame), SALTS_OK);
    frame.pattern = FLOWMQ_PROTOCOL_XSUB;
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_XPUB, &frame), SALTS_EPROTO);
  }

  it("accepts subscription controls only on PUB and XPUB peers") {
    flowmq_protocol_frame_t frame = {0};
    frame.kind = FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE;
    frame.pattern = FLOWMQ_PROTOCOL_SUB;
    frame.topic = vstr_from_cstr("orders.");

    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PUB, &frame), SALTS_OK);
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_XPUB, &frame), SALTS_OK);
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PULL, &frame), SALTS_EPROTO);
    frame.pattern = FLOWMQ_PROTOCOL_XSUB;
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_XPUB, &frame), SALTS_OK);
  }

  it("accepts FMQ/6 negotiation controls for compatible peers") {
    flowmq_protocol_frame_t settings = {.kind = FLOWMQ_PROTOCOL_FRAME_SETTINGS,
                                        .pattern = FLOWMQ_PROTOCOL_PAIR};
    flowmq_protocol_frame_t update = {.kind = FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE,
                                      .pattern = FLOWMQ_PROTOCOL_PAIR};

    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PAIR, &settings), SALTS_OK);
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PAIR, &update), SALTS_OK);
    settings.pattern = FLOWMQ_PROTOCOL_PUSH;
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PAIR, &settings),
                SALTS_EPROTO);
  }

  it("encodes client handshake and control commands") {
    flowmq_protocol_frame_t frame;
    tstr encoded = NULL;
    size_t consumed = 0u;

    check_equal(flowmq_pattern_encode_hello(FLOWMQ_PROTOCOL_DEALER, vstr_from_cstr("worker-1"),
                                            (vstr){0}, 1024u, &encoded),
                SALTS_OK);
    check_equal(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &frame, &consumed),
                SALTS_OK);
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &frame), SALTS_OK);
    flowmq_protocol_frame_cleanup(&frame);
    tstr_freep(&encoded);

    check_equal(flowmq_pattern_encode_heartbeat(FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_FRAME_PING,
                                                1024u, &encoded),
                SALTS_OK);
    tstr_freep(&encoded);
    check_equal(flowmq_pattern_encode_subscription(FLOWMQ_PROTOCOL_XSUB,
                                                   FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
                                                   vstr_from_cstr("orders."), 1024u, &encoded),
                SALTS_OK);
    tstr_freep(&encoded);
    check_equal(flowmq_pattern_encode_subscription(FLOWMQ_PROTOCOL_PUB,
                                                   FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
                                                   vstr_from_cstr("orders."), 1024u, &encoded),
                SALTS_EINVAL);
  }
}
