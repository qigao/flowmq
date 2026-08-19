/**
 * @file test_flowmq_protocol_esb.c
 * Unit tests for FlowMQ ESB protocol extensions.
 */

#include "flowmq_protocol_esb.h"
#include "tinytest.h"

#include <stdlib.h>
#include <string.h>

/* Test ESB pattern validation */
spec("flowmq_protocol_esb") {
  it("validates ESB patterns") {
    /* Valid ESB patterns */
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_SCATTER), TURBO_OK);
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_GATHER), TURBO_OK);
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_SAGA_COORDINATOR),
                 TURBO_OK);
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_SAGA_PARTICIPANT),
                 TURBO_OK);
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_STREAM_PRODUCER),
                 TURBO_OK);
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_STREAM_CONSUMER),
                 TURBO_OK);
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_PRIORITY_PRODUCER),
                 TURBO_OK);
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_PRIORITY_CONSUMER),
                 TURBO_OK);
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_CB_CLIENT), TURBO_OK);
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_CB_SERVICE), TURBO_OK);

    /* Invalid patterns */
    check_int_eq(flowmq_protocol_esb_pattern_validate(0), TURBO_EINVAL);
    check_int_eq(flowmq_protocol_esb_pattern_validate(FLOWMQ_PROTOCOL_PUB), TURBO_EINVAL);
    check_int_eq(flowmq_protocol_esb_pattern_validate(32), TURBO_EINVAL);
    check_int_eq(flowmq_protocol_esb_pattern_validate(255), TURBO_EINVAL);
  }

  it("checks ESB pattern compatibility") {
    /* SCATTER/GATHER compatibility */
    check_int_eq(
        flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_SCATTER, FLOWMQ_PROTOCOL_GATHER),
        1);
    check_int_eq(
        flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_GATHER, FLOWMQ_PROTOCOL_SCATTER),
        1);
    check_int_eq(
        flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_SCATTER, FLOWMQ_PROTOCOL_SCATTER),
        0);

    /* SAGA compatibility */
    check_int_eq(flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_SAGA_COORDINATOR,
                                                         FLOWMQ_PROTOCOL_SAGA_PARTICIPANT),
                 1);
    check_int_eq(flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_SAGA_PARTICIPANT,
                                                         FLOWMQ_PROTOCOL_SAGA_COORDINATOR),
                 1);

    /* STREAM compatibility */
    check_int_eq(flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_STREAM_PRODUCER,
                                                         FLOWMQ_PROTOCOL_STREAM_CONSUMER),
                 1);
    check_int_eq(flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_STREAM_CONSUMER,
                                                         FLOWMQ_PROTOCOL_STREAM_PRODUCER),
                 1);
    /* Stream consumers can connect to each other (consumer group) */
    check_int_eq(flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_STREAM_CONSUMER,
                                                         FLOWMQ_PROTOCOL_STREAM_CONSUMER),
                 1);

    /* PRIORITY_QUEUE compatibility */
    check_int_eq(flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_PRIORITY_PRODUCER,
                                                         FLOWMQ_PROTOCOL_PRIORITY_CONSUMER),
                 1);

    /* CIRCUIT_BREAKER compatibility */
    check_int_eq(flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_CB_CLIENT,
                                                         FLOWMQ_PROTOCOL_CB_SERVICE),
                 1);

    /* Cross-pattern incompatibility */
    check_int_eq(
        flowmq_protocol_esb_patterns_compatible(FLOWMQ_PROTOCOL_SCATTER, FLOWMQ_PROTOCOL_STREAM_PRODUCER),
        0);
  }

  it("encodes and decodes scatter/gather frame") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_SCATTER_REQUEST;
    frame.base.pattern = FLOWMQ_PROTOCOL_SCATTER;
    frame.base.message_id = 12345;
    frame.base.identity = tstr_v_from_cstr("scatter-client");
    frame.base.topic = tstr_v_from_cstr("order.process");
    frame.expected_responses = 5;
    frame.aggregation_policy = FLOWMQ_GATHER_ALL;

    /* Encode */
    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 65536, &encoded), TURBO_OK);
    check_int_eq(tstr_len(encoded) > 0, 1);

    /* Decode */
    flowmq_protocol_esb_frame_t decoded = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    size_t consumed = 0;
    size_t encoded_len = tstr_len(encoded);
    check_int_eq(flowmq_protocol_esb_decode_frame(encoded, encoded_len, 65536, &decoded, &consumed),
                 TURBO_OK);
    check_int_eq(consumed, encoded_len);

    /* Verify fields */
    check_int_eq(decoded.base.kind, FLOWMQ_PROTOCOL_FRAME_SCATTER_REQUEST);
    check_int_eq(decoded.base.pattern, FLOWMQ_PROTOCOL_SCATTER);
    check_int_eq(decoded.base.message_id, 12345);
    check_int_eq(tstr_v_eq(decoded.base.identity, tstr_v_from_cstr("scatter-client")), 1);
    check_int_eq(tstr_v_eq(decoded.base.topic, tstr_v_from_cstr("order.process")), 1);
    check_int_eq(decoded.expected_responses, 5);
    check_int_eq(decoded.aggregation_policy, FLOWMQ_GATHER_ALL);

    /* Cleanup */
    tstr_free(encoded);
    flowmq_protocol_esb_frame_cleanup(&decoded);
  }

  it("encodes and decodes partial response frame") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_PARTIAL_RESPONSE;
    frame.base.pattern = FLOWMQ_PROTOCOL_GATHER;
    frame.base.message_id = 12345;
    frame.base.payload = tstr_v_from_cstr("{\"result\":\"ok\"}");
    frame.partial_index = 2;

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 65536, &encoded), TURBO_OK);

    flowmq_protocol_esb_frame_t decoded = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    size_t consumed = 0;
    size_t encoded_len = tstr_len(encoded);
    check_int_eq(flowmq_protocol_esb_decode_frame(encoded, encoded_len, 65536, &decoded, &consumed),
                 TURBO_OK);

    check_int_eq(decoded.base.kind, FLOWMQ_PROTOCOL_FRAME_PARTIAL_RESPONSE);
    check_int_eq(decoded.partial_index, 2);

    tstr_free(encoded);
    flowmq_protocol_esb_frame_cleanup(&decoded);
  }

  it("encodes and decodes stream frame with consumer group") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_STREAM_PUBLISH;
    frame.base.pattern = FLOWMQ_PROTOCOL_STREAM_PRODUCER;
    frame.base.message_id = 99999;
    frame.base.topic = tstr_v_from_cstr("events.log");
    frame.base.payload = tstr_v_from_cstr("event data");
    frame.partition_id = 7;
    frame.offset = 123456789;
    frame.consumer_group = tstr_v_from_cstr("log-consumers");

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 65536, &encoded), TURBO_OK);

    flowmq_protocol_esb_frame_t decoded = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    size_t consumed = 0;
    size_t encoded_len = tstr_len(encoded);
    check_int_eq(flowmq_protocol_esb_decode_frame(encoded, encoded_len, 65536, &decoded, &consumed),
                 TURBO_OK);

    check_int_eq(decoded.base.kind, FLOWMQ_PROTOCOL_FRAME_STREAM_PUBLISH);
    check_int_eq(decoded.base.pattern, FLOWMQ_PROTOCOL_STREAM_PRODUCER);
    check_int_eq(decoded.partition_id, 7);
    check_ull_eq(decoded.offset, 123456789ULL);
    check_int_eq(tstr_v_eq(decoded.consumer_group, tstr_v_from_cstr("log-consumers")), 1);
    check_int_eq(tstr_v_eq(decoded.base.payload, tstr_v_from_cstr("event data")), 1);

    tstr_free(encoded);
    flowmq_protocol_esb_frame_cleanup(&decoded);
  }

  it("supports strict decode mode for unknown TLV fields") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_STREAM_SUBSCRIBE;
    frame.base.pattern = FLOWMQ_PROTOCOL_STREAM_CONSUMER;
    frame.base.message_id = 11111;

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 65536, &encoded), TURBO_OK);
    check_int_eq(tstr_len(encoded) > 0, 1);

    /* Append one unknown TLV type (0x7Fu) with 1-byte payload. */
    size_t encoded_len = tstr_len(encoded);
    size_t extended_len = encoded_len + 6u;
    char *extended = (char *)malloc(extended_len);
    check(extended != NULL);
    memcpy(extended, encoded, encoded_len);
    extended[encoded_len + 0u] = 0x7Fu;
    extended[encoded_len + 1u] = 1u;
    extended[encoded_len + 2u] = 0u;
    extended[encoded_len + 3u] = 0u;
    extended[encoded_len + 4u] = 0u;
    extended[encoded_len + 5u] = 0xABu;

    flowmq_protocol_esb_frame_t relaxed = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    size_t consumed = 0;
    check_int_eq(flowmq_protocol_esb_decode_frame(extended, extended_len, 65536, &relaxed,
                                                 &consumed),
                 TURBO_OK);
    check_int_eq(consumed, extended_len);
    flowmq_protocol_esb_frame_cleanup(&relaxed);

    flowmq_protocol_esb_frame_t strict = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    check_int_eq(flowmq_protocol_esb_decode_frame_ex(
                  extended, extended_len, 65536, FLOWMQ_PROTOCOL_ESB_DECODE_STRICT, &strict,
                  &consumed),
                 TURBO_EPROTO);
    tstr_free(encoded);
    flowmq_protocol_esb_frame_cleanup(&strict);
    free(extended);
  }

  it("decodes ESB frame from already-decoded base frame") {
    flowmq_protocol_esb_frame_t esb = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    esb.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_STREAM_SUBSCRIBE;
    esb.base.pattern = FLOWMQ_PROTOCOL_STREAM_CONSUMER;
    esb.base.message_id = 20002;
    esb.base.identity = tstr_v_from_cstr("consumer-A");
    esb.base.topic = tstr_v_from_cstr("orders");
    esb.partition_id = 3;
    esb.offset = 42;
    esb.consumer_group = tstr_v_from_cstr("group-1");

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&esb, 65536, &encoded), TURBO_OK);
    size_t encoded_len = tstr_len(encoded);

    flowmq_protocol_frame_t base = {0};
    size_t consumed = 0;
    check_int_eq(flowmq_protocol_decode_frame(encoded, encoded_len, 65536, &base, &consumed),
                 TURBO_OK);
    check_int_eq(consumed, encoded_len);

    flowmq_protocol_esb_frame_t decoded = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    check_int_eq(flowmq_protocol_esb_decode_frame_from_base(&base, &decoded), TURBO_OK);
    check_int_eq(decoded.base.kind, FLOWMQ_PROTOCOL_FRAME_STREAM_SUBSCRIBE);
    check_int_eq(decoded.base.pattern, FLOWMQ_PROTOCOL_STREAM_CONSUMER);
    check_int_eq(decoded.partition_id, 3);
    check_ull_eq(decoded.offset, 42ULL);
    check_int_eq(tstr_v_eq(decoded.consumer_group, tstr_v_from_cstr("group-1")), 1);

    flowmq_protocol_esb_frame_cleanup(&decoded);
    flowmq_protocol_frame_cleanup(&base);
    tstr_free(encoded);
  }

  it("respects strict mode for unknown TLV on base-decoded input") {
    flowmq_protocol_esb_frame_t esb = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    esb.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_STREAM_PUBLISH;
    esb.base.pattern = FLOWMQ_PROTOCOL_STREAM_PRODUCER;
    esb.base.message_id = 20003;
    esb.base.topic = tstr_v_from_cstr("topic-A");
    esb.base.identity = tstr_v_from_cstr("pub-A");

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&esb, 65536, &encoded), TURBO_OK);
    check_int_eq(tstr_len(encoded) > 0, 1);

    size_t encoded_len = tstr_len(encoded);
    size_t extended_len = encoded_len + 6u;
    char *extended = (char *)malloc(extended_len);
    check(extended != NULL);
    memcpy(extended, encoded, encoded_len);
    extended[encoded_len + 0u] = 0x7Fu;
    extended[encoded_len + 1u] = 1u;
    extended[encoded_len + 2u] = 0u;
    extended[encoded_len + 3u] = 0u;
    extended[encoded_len + 4u] = 0u;
    extended[encoded_len + 5u] = 0x01u;

    flowmq_protocol_frame_t base = {0};
    size_t consumed = 0;
    check_int_eq(flowmq_protocol_decode_frame(extended, extended_len, 65536, &base, &consumed),
                 TURBO_OK);
    check_int_eq(consumed, extended_len);

    flowmq_protocol_esb_frame_t relaxed = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    check_int_eq(flowmq_protocol_esb_decode_frame_from_base(&base, &relaxed), TURBO_OK);

    flowmq_protocol_esb_frame_t strict = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    check_int_eq(flowmq_protocol_esb_decode_frame_from_base_ex(
                     &base, FLOWMQ_PROTOCOL_ESB_DECODE_STRICT, &strict),
                 TURBO_EPROTO);

    flowmq_protocol_esb_frame_cleanup(&relaxed);
    flowmq_protocol_esb_frame_cleanup(&strict);
    flowmq_protocol_frame_cleanup(&base);
    tstr_free(encoded);
    free(extended);
  }

  it("copies borrowed consumer_group into owned storage") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_STREAM_PUBLISH;
    frame.base.pattern = FLOWMQ_PROTOCOL_STREAM_PRODUCER;
    frame.base.message_id = 99999;
    frame.base.topic = tstr_v_from_cstr("events.log");
    frame.base.payload = tstr_v_from_cstr("event data");
    frame.partition_id = 7;
    frame.offset = 123456789;
    frame.consumer_group = tstr_v_from_cstr("log-consumers");

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 65536, &encoded), TURBO_OK);

    flowmq_protocol_esb_frame_t decoded = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    size_t consumed = 0;
    size_t encoded_len = tstr_len(encoded);
    check_int_eq(flowmq_protocol_esb_decode_frame(encoded, encoded_len, 65536, &decoded, &consumed),
                 TURBO_OK);

    tstr_t copied = NULL;
    check_int_eq(flowmq_protocol_esb_frame_consumer_group_copy(&decoded, &copied), TURBO_OK);
    flowmq_protocol_esb_frame_cleanup(&decoded);
    check_int_eq(tstr_cmp_v(copied, tstr_v_from_cstr("log-consumers")), 0);

    tstr_free(copied);
    tstr_free(encoded);
  }

  it("encodes and decodes saga frame") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_SAGA_EXECUTE;
    frame.base.pattern = FLOWMQ_PROTOCOL_SAGA_COORDINATOR;
    frame.base.message_id = 555;
    frame.saga_id = 0x0123456789ABCDEF;
    frame.saga_step = 3;
    frame.saga_state = FLOWMQ_PROTOCOL_SAGA_EXECUTING;

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 65536, &encoded), TURBO_OK);

    flowmq_protocol_esb_frame_t decoded = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    size_t consumed = 0;
    size_t encoded_len = tstr_len(encoded);
    check_int_eq(flowmq_protocol_esb_decode_frame(encoded, encoded_len, 65536, &decoded, &consumed),
                 TURBO_OK);

    check_int_eq(decoded.base.kind, FLOWMQ_PROTOCOL_FRAME_SAGA_EXECUTE);
    check_ull_eq(decoded.saga_id, 0x0123456789ABCDEFULL);
    check_int_eq(decoded.saga_step, 3);
    check_int_eq(decoded.saga_state, FLOWMQ_PROTOCOL_SAGA_EXECUTING);

    tstr_free(encoded);
    flowmq_protocol_esb_frame_cleanup(&decoded);
  }

  it("encodes and decodes priority queue frame") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_PRIORITY_PUBLISH;
    frame.base.pattern = FLOWMQ_PROTOCOL_PRIORITY_PRODUCER;
    frame.base.message_id = 777;
    frame.base.payload = tstr_v_from_cstr("urgent message");
    frame.priority = 200;  /* High priority */

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 65536, &encoded), TURBO_OK);

    flowmq_protocol_esb_frame_t decoded = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    size_t consumed = 0;
    size_t encoded_len = tstr_len(encoded);
    check_int_eq(flowmq_protocol_esb_decode_frame(encoded, encoded_len, 65536, &decoded, &consumed),
                 TURBO_OK);

    check_int_eq(decoded.priority, 200);
    check_int_eq(tstr_v_eq(decoded.base.payload, tstr_v_from_cstr("urgent message")), 1);

    tstr_free(encoded);
    flowmq_protocol_esb_frame_cleanup(&decoded);
  }

  it("encodes and decodes circuit breaker frame") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_CB_STATUS;
    frame.base.pattern = FLOWMQ_PROTOCOL_CB_SERVICE;
    frame.base.message_id = 888;
    frame.cb_state = FLOWMQ_CB_HALF_OPEN;
    frame.failure_count = 15;

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 65536, &encoded), TURBO_OK);

    flowmq_protocol_esb_frame_t decoded = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    size_t consumed = 0;
    size_t encoded_len = tstr_len(encoded);
    check_int_eq(flowmq_protocol_esb_decode_frame(encoded, encoded_len, 65536, &decoded, &consumed),
                 TURBO_OK);

    check_int_eq(decoded.cb_state, FLOWMQ_CB_HALF_OPEN);
    check_int_eq(decoded.failure_count, 15);

    tstr_free(encoded);
    flowmq_protocol_esb_frame_cleanup(&decoded);
  }

  it("rejects frame size exceeding max_frame_size") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_SCATTER_REQUEST;
    frame.base.pattern = FLOWMQ_PROTOCOL_SCATTER;
    frame.base.message_id = 1;
    frame.expected_responses = 1000;

    tstr_t encoded = NULL;
    /* Max frame size too small */
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 10, &encoded), TURBO_EMSGSIZE);
  }

  it("rejects invalid expected_responses count") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_SCATTER_REQUEST;
    frame.base.pattern = FLOWMQ_PROTOCOL_SCATTER;
    frame.base.message_id = 1;
    frame.expected_responses = FLOWMQ_PROTOCOL_ESB_MAX_FANOUT_COUNT + 1;  /* Too large */

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 65536, &encoded), TURBO_EINVAL);
  }

  it("handles empty ESB frame (no TLV fields)") {
    flowmq_protocol_esb_frame_t frame = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    frame.base.kind = (flowmq_protocol_frame_kind_t)FLOWMQ_PROTOCOL_FRAME_DATA;
    frame.base.pattern = FLOWMQ_PROTOCOL_SCATTER;
    frame.base.message_id = 1;
    /* No ESB-specific fields set */

    tstr_t encoded = NULL;
    check_int_eq(flowmq_protocol_esb_encode_frame(&frame, 65536, &encoded), TURBO_OK);

    flowmq_protocol_esb_frame_t decoded = FLOWMQ_PROTOCOL_ESB_FRAME_INIT;
    size_t consumed = 0;
    size_t encoded_len = tstr_len(encoded);
    check_int_eq(flowmq_protocol_esb_decode_frame(encoded, encoded_len, 65536, &decoded, &consumed),
                 TURBO_OK);

    check_int_eq(decoded.expected_responses, 0);
    check_int_eq(decoded.partition_id, 0);
    check_int_eq(decoded.priority, 0);

    tstr_free(encoded);
    flowmq_protocol_esb_frame_cleanup(&decoded);
  }
}
