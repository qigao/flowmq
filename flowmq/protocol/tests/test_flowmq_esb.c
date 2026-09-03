#include "flowmq_esb.h"
#include "flowmq_protocol.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("flowmq_esb") {
  it("round trips a strict FES/1 scatter request") {
    flowmq_esb_message_t input = {.kind = FLOWMQ_ESB_SCATTER_REQUEST,
                                  .expected_responses = 5u,
                                  .aggregation_policy = FLOWMQ_ESB_GATHER_ALL,
                                  .payload = {.data = "request", .len = 7u}};
    flowmq_esb_message_t output;
    tstr encoded = NULL;
    size_t encoded_size = 0u;

    check_equal(flowmq_esb_encoded_size(&input, 1024u, &encoded_size), TURBO_OK);
    check_equal(flowmq_esb_encode(&input, 1024u, &encoded), TURBO_OK);
    check_equal(tstr_len(encoded), encoded_size);
    check_equal(encoded, "FES1", 4u);
    check_equal(flowmq_esb_decode(tstr_to_v(encoded), 1024u, &output), TURBO_OK);
    check_equal(output.kind, FLOWMQ_ESB_SCATTER_REQUEST);
    check_equal(output.expected_responses, 5u);
    check_equal(output.aggregation_policy, FLOWMQ_ESB_GATHER_ALL);
    check_true(vstr_eq(output.payload, vstr_from_cstr("request")));
    encoded[3] = '0';
    check_equal(flowmq_esb_decode(tstr_to_v(encoded), 1024u, &output), TURBO_EPROTO);
    tstr_free(encoded);
  }

  it("round trips stream commit metadata as borrowed views") {
    flowmq_esb_message_t input = {.kind = FLOWMQ_ESB_STREAM_COMMIT,
                                  .partition_id = 0u,
                                  .offset = UINT64_C(42),
                                  .consumer_group = {.data = "workers", .len = 7u}};
    flowmq_esb_message_t output;
    tstr encoded = NULL;

    check_equal(flowmq_esb_encode(&input, 1024u, &encoded), TURBO_OK);
    check_equal(flowmq_esb_decode(tstr_to_v(encoded), 1024u, &output), TURBO_OK);
    check_equal(output.partition_id, 0u);
    check_equal(output.offset, UINT64_C(42));
    check_true(vstr_eq(output.consumer_group, vstr_from_cstr("workers")));
    check_true(output.consumer_group.data >= encoded);
    check_true(output.consumer_group.data < encoded + tstr_len(encoded));
    tstr_free(encoded);
  }

  it("round trips saga priority and circuit-breaker messages") {
    flowmq_esb_message_t message = {.kind = FLOWMQ_ESB_SAGA_COMPENSATE,
                                    .saga_id = UINT64_C(0x0102030405060708),
                                    .saga_step = 3u,
                                    .saga_state = FLOWMQ_ESB_SAGA_COMPENSATING};
    flowmq_esb_message_t output;
    tstr encoded = NULL;

    check_equal(flowmq_esb_encode(&message, 1024u, &encoded), TURBO_OK);
    check_equal(flowmq_esb_decode(tstr_to_v(encoded), 1024u, &output), TURBO_OK);
    check_equal(output.saga_id, message.saga_id);
    check_equal(output.saga_step, 3u);
    check_equal(output.saga_state, FLOWMQ_ESB_SAGA_COMPENSATING);
    tstr_freep(&encoded);

    message = (flowmq_esb_message_t){
        .kind = FLOWMQ_ESB_PRIORITY_PUBLISH, .priority = 0u, .payload = {.data = "low", .len = 3u}};
    check_equal(flowmq_esb_encode(&message, 1024u, &encoded), TURBO_OK);
    check_equal(flowmq_esb_decode(tstr_to_v(encoded), 1024u, &output), TURBO_OK);
    check_equal(output.priority, 0u);
    tstr_freep(&encoded);

    message = (flowmq_esb_message_t){.kind = FLOWMQ_ESB_CIRCUIT_STATUS,
                                     .circuit_state = FLOWMQ_ESB_CIRCUIT_HALF_OPEN,
                                     .failure_count = 9u};
    check_equal(flowmq_esb_encode(&message, 1024u, &encoded), TURBO_OK);
    check_equal(flowmq_esb_decode(tstr_to_v(encoded), 1024u, &output), TURBO_OK);
    check_equal(output.circuit_state, FLOWMQ_ESB_CIRCUIT_HALF_OPEN);
    check_equal(output.failure_count, 9u);
    tstr_free(encoded);
  }

  it("carries FES/1 unchanged inside an ordinary FMQ/6 DATA payload") {
    flowmq_esb_message_t esb = {.kind = FLOWMQ_ESB_STREAM_PUBLISH,
                                .partition_id = 7u,
                                .offset = UINT64_C(123),
                                .payload = {.data = "event", .len = 5u}};
    flowmq_protocol_frame_t frame = {.kind = FLOWMQ_PROTOCOL_FRAME_DATA,
                                     .pattern = FLOWMQ_PROTOCOL_PUSH,
                                     .message_id = UINT64_C(99)};
    flowmq_protocol_frame_t decoded_frame;
    flowmq_esb_message_t decoded_esb;
    tstr esb_bytes = NULL;
    tstr fmq_bytes = NULL;
    size_t consumed = 0u;

    check_equal(flowmq_esb_encode(&esb, 1024u, &esb_bytes), TURBO_OK);
    frame.payload = tstr_to_v(esb_bytes);
    check_equal(flowmq_protocol_encode_frame(&frame, 2048u, &fmq_bytes), TURBO_OK);
    check_equal(flowmq_protocol_decode_frame(fmq_bytes, tstr_len(fmq_bytes), 2048u, &decoded_frame,
                                             &consumed),
                TURBO_OK);
    check_equal(decoded_frame.kind, FLOWMQ_PROTOCOL_FRAME_DATA);
    check_equal(flowmq_esb_decode(decoded_frame.payload, 1024u, &decoded_esb), TURBO_OK);
    check_equal(decoded_esb.kind, FLOWMQ_ESB_STREAM_PUBLISH);
    check_true(vstr_eq(decoded_esb.payload, vstr_from_cstr("event")));
    flowmq_protocol_frame_cleanup(&decoded_frame);
    tstr_free(fmq_bytes);
    tstr_free(esb_bytes);
  }

  it("rejects unknown TLV types and trailing bytes without compatibility mode") {
    flowmq_esb_message_t message = {.kind = FLOWMQ_ESB_PARTIAL_RESPONSE, .partial_index = 2u};
    flowmq_esb_message_t output;
    tstr encoded = NULL;
    tstr extended = NULL;
    size_t size;

    check_equal(flowmq_esb_encode(&message, 1024u, &encoded), TURBO_OK);
    size = tstr_len(encoded);
    encoded[FLOWMQ_ESB_HEADER_SIZE] = (char)0x7f;
    check_equal(flowmq_esb_decode(tstr_to_v(encoded), 1024u, &output), TURBO_EPROTO);
    encoded[FLOWMQ_ESB_HEADER_SIZE] = FLOWMQ_ESB_FIELD_PARTIAL_INDEX;
    extended = tstr_new_len(NULL, size + 1u);
    check_not_null(extended);
    memcpy(extended, encoded, size);
    extended[size] = 0;
    check_equal(flowmq_esb_decode(tstr_to_v(extended), 1024u, &output), TURBO_EPROTO);
    tstr_free(extended);
    tstr_free(encoded);
  }

  it("fails fast on invalid kind fields and configured bounds") {
    flowmq_esb_message_t message = {.kind = FLOWMQ_ESB_SCATTER_REQUEST,
                                    .expected_responses = FLOWMQ_ESB_MAX_FANOUT_COUNT + 1u,
                                    .aggregation_policy = FLOWMQ_ESB_GATHER_ALL};
    tstr encoded = NULL;

    check_equal(flowmq_esb_encode(&message, 1024u, &encoded), TURBO_EINVAL);
    message.expected_responses = 1u;
    message.consumer_group = vstr_from_cstr("not-a-scatter-field");
    check_equal(flowmq_esb_encode(&message, 1024u, &encoded), TURBO_EPROTO);
    message.consumer_group = (vstr){0};
    check_equal(flowmq_esb_encode(&message, FLOWMQ_ESB_HEADER_SIZE, &encoded), TURBO_EMSGSIZE);
    check_null(encoded);
  }
}
