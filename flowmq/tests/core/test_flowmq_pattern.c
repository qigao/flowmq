#include "flowmq_core.h"
#include "flowmq_pattern.h"
#include "flowmq_security.h"
#include "tinytest.h"
#include "salts_error.h"

#include <stdint.h>
#include <string.h>

#define TEST_PATTERN_BIT(pattern_value) \
  ((uint16_t)(UINT16_C(1) << (pattern_value)))

static const uint16_t EXPECTED_COMPATIBILITY[FLOWMQ_PROTOCOL_XSUB + 1u] = {
    [FLOWMQ_PROTOCOL_PUB] =
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_SUB) |
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_XSUB),
    [FLOWMQ_PROTOCOL_SUB] =
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_PUB) |
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_XPUB),
    [FLOWMQ_PROTOCOL_PUSH] = TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_PULL),
    [FLOWMQ_PROTOCOL_PULL] = TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_PUSH),
    [FLOWMQ_PROTOCOL_ROUTER] =
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_DEALER) |
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_REQ) |
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_ROUTER),
    [FLOWMQ_PROTOCOL_DEALER] =
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_ROUTER) |
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_REP) |
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_DEALER),
    [FLOWMQ_PROTOCOL_PAIR] = TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_PAIR),
    [FLOWMQ_PROTOCOL_REQ] =
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_REP) |
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_ROUTER),
    [FLOWMQ_PROTOCOL_REP] =
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_REQ) |
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_DEALER),
    [FLOWMQ_PROTOCOL_XPUB] =
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_SUB) |
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_XSUB),
    [FLOWMQ_PROTOCOL_XSUB] =
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_PUB) |
        TEST_PATTERN_BIT(FLOWMQ_PROTOCOL_XPUB)};

spec("flowmq_pattern") {
  it("defines all FMQ/6 peer relationships from one descriptor table") {
    check_equal(flowmq_core_pattern_validate(FLOWMQ_PROTOCOL_REQ), SALTS_OK);
    check_equal(flowmq_core_pattern_validate(0u), SALTS_EINVAL);
    check_equal(flowmq_core_pattern_validate(12u), SALTS_EINVAL);
    check_equal(flowmq_pattern_validate(12u), SALTS_EINVAL);
    check_true(flowmq_pattern_descriptor(0u) == NULL);
    check_true(flowmq_pattern_descriptor(12u) == NULL);

    for (flowmq_protocol_pattern_t local = FLOWMQ_PROTOCOL_PUB;
         local <= FLOWMQ_PROTOCOL_XSUB; ++local) {
      const flowmq_pattern_desc_t *desc = flowmq_pattern_descriptor(local);
      check_true(desc != NULL);
      check_equal(desc->pattern, local);
      check_equal(desc->compatible_mask, EXPECTED_COMPATIBILITY[local]);

      for (flowmq_protocol_pattern_t remote = FLOWMQ_PROTOCOL_PUB;
           remote <= FLOWMQ_PROTOCOL_XSUB; ++remote) {
        int expected =
            (EXPECTED_COMPATIBILITY[local] & TEST_PATTERN_BIT(remote)) != 0u;
        check_equal(flowmq_patterns_compatible(local, remote), expected);
      }
    }

    check_false(flowmq_patterns_compatible(0u, FLOWMQ_PROTOCOL_PUB));
    check_false(flowmq_patterns_compatible(FLOWMQ_PROTOCOL_PUB, 12u));
  }

  it("defines routing subscription mute and FSM policy in the same schema") {
    const flowmq_pattern_desc_t *pub =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_PUB);
    const flowmq_pattern_desc_t *xpub =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_XPUB);
    const flowmq_pattern_desc_t *sub =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_SUB);
    const flowmq_pattern_desc_t *router =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_ROUTER);
    const flowmq_pattern_desc_t *dealer =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_DEALER);
    const flowmq_pattern_desc_t *pair =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_PAIR);
    const flowmq_pattern_desc_t *req =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_REQ);
    const flowmq_pattern_desc_t *rep =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_REP);

    check_equal(pub->routing_class, FLOWMQ_PATTERN_ROUTE_FANOUT);
    check_equal(pub->subscription_class, FLOWMQ_PATTERN_SUB_PUBLISHER);
    check_equal(pub->mute_class, FLOWMQ_PATTERN_MUTE_DROP);
    check_equal(xpub->routing_class, FLOWMQ_PATTERN_ROUTE_FANOUT);
    check_true((xpub->capabilities & FLOWMQ_PATTERN_CAP_SUB_EVENTS) != 0u);
    check_equal(sub->subscription_class, FLOWMQ_PATTERN_SUB_SUBSCRIBER);
    check_equal(router->routing_class, FLOWMQ_PATTERN_ROUTE_IDENTITY);
    check_equal(dealer->routing_class, FLOWMQ_PATTERN_ROUTE_ROUND_ROBIN);
    check_equal(pair->routing_class, FLOWMQ_PATTERN_ROUTE_SINGLE);
    check_equal(req->fsm_class, FLOWMQ_PATTERN_FSM_REQ);
    check_equal(rep->routing_class, FLOWMQ_PATTERN_ROUTE_REPLY_PEER);
    check_equal(rep->fsm_class, FLOWMQ_PATTERN_FSM_REP);

    /*
     * Reply-peer routing and REP transaction semantics are distinct fields,
     * but in the single canonical pattern schema they intentionally identify
     * the same one public pattern. Hot routing may therefore consume the
     * routing fact without changing transaction/FSM ownership.
     */
    for (flowmq_protocol_pattern_t pattern = FLOWMQ_PROTOCOL_PUB;
         pattern <= FLOWMQ_PROTOCOL_XSUB; ++pattern) {
      const flowmq_pattern_desc_t *desc = flowmq_pattern_descriptor(pattern);
      check_equal(desc->routing_class == FLOWMQ_PATTERN_ROUTE_REPLY_PEER,
                  desc->fsm_class == FLOWMQ_PATTERN_FSM_REP);
    }
  }

  it("requires a compatible HELLO and DEALER identity") {
    flowmq_protocol_frame_t hello;
    memset(&hello, 0, sizeof(hello));
    hello.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
    hello.pattern = FLOWMQ_PROTOCOL_DEALER;

    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello),
                SALTS_EPROTO);
    hello.identity = vstr_from_cstr("worker-1");
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello),
                SALTS_OK);
    hello.payload = vstr_from_cstr("not-allowed");
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello),
                SALTS_EPROTO);
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
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello),
                SALTS_OK);
    check_equal(
        flowmq_pattern_socket_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &hello),
        SALTS_EPROTO);
    tstr_free(payload);
  }

  it("derives DATA direction separately from application send/receive capability") {
    flowmq_protocol_frame_t frame;
    memset(&frame, 0, sizeof(frame));
    frame.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    frame.pattern = FLOWMQ_PROTOCOL_SUB;

    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PUB,
                                                       &frame),
                SALTS_EPROTO);
    frame.pattern = FLOWMQ_PROTOCOL_PUB;
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_SUB,
                                                       &frame),
                SALTS_OK);
    frame.pattern = FLOWMQ_PROTOCOL_XSUB;
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_XPUB,
                                                       &frame),
                SALTS_EPROTO);
    frame.pattern = FLOWMQ_PROTOCOL_XPUB;
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_XSUB,
                                                       &frame),
                SALTS_OK);
  }

  it("accepts subscription controls only for schema publisher/subscriber peers") {
    flowmq_protocol_frame_t frame = {0};
    frame.kind = FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE;
    frame.pattern = FLOWMQ_PROTOCOL_SUB;
    frame.topic = vstr_from_cstr("orders.");

    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PUB,
                                                       &frame),
                SALTS_OK);
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_XPUB,
                                                       &frame),
                SALTS_OK);
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PULL,
                                                       &frame),
                SALTS_EPROTO);
    frame.pattern = FLOWMQ_PROTOCOL_XSUB;
    check_equal(flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_XPUB,
                                                       &frame),
                SALTS_OK);
  }

  it("accepts FMQ/6 negotiation controls for compatible peers") {
    flowmq_protocol_frame_t settings = {.kind = FLOWMQ_PROTOCOL_FRAME_SETTINGS,
                                        .pattern = FLOWMQ_PROTOCOL_PAIR};
    flowmq_protocol_frame_t update = {.kind = FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE,
                                      .pattern = FLOWMQ_PROTOCOL_PAIR};

    check_equal(
        flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PAIR, &settings),
        SALTS_OK);
    check_equal(
        flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PAIR, &update),
        SALTS_OK);
    settings.pattern = FLOWMQ_PROTOCOL_PUSH;
    check_equal(
        flowmq_pattern_data_direction_validate(FLOWMQ_PROTOCOL_PAIR, &settings),
        SALTS_EPROTO);
  }

  it("encodes client handshake and control commands") {
    flowmq_protocol_frame_t frame;
    tstr encoded = NULL;
    size_t consumed = 0u;

    check_equal(flowmq_pattern_encode_hello(FLOWMQ_PROTOCOL_DEALER,
                                            vstr_from_cstr("worker-1"),
                                            (vstr){0}, 1024u, &encoded),
                SALTS_OK);
    check_equal(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u,
                                             &frame, &consumed),
                SALTS_OK);
    check_equal(flowmq_pattern_hello_validate(FLOWMQ_PROTOCOL_ROUTER, &frame),
                SALTS_OK);
    flowmq_protocol_frame_cleanup(&frame);
    tstr_freep(&encoded);

    check_equal(flowmq_pattern_encode_heartbeat(
                    FLOWMQ_PROTOCOL_REQ, FLOWMQ_PROTOCOL_FRAME_PING, 1024u,
                    &encoded),
                SALTS_OK);
    tstr_freep(&encoded);
    check_equal(flowmq_pattern_encode_subscription(
                    FLOWMQ_PROTOCOL_XSUB, FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
                    vstr_from_cstr("orders."), 1024u, &encoded),
                SALTS_OK);
    tstr_freep(&encoded);
    check_equal(flowmq_pattern_encode_subscription(
                    FLOWMQ_PROTOCOL_PUB, FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
                    vstr_from_cstr("orders."), 1024u, &encoded),
                SALTS_EINVAL);
  }
}
