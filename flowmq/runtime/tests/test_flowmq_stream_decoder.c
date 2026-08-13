#include "flowmq_stream_decoder.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("flowmq_stream_decoder") {
  it("assembles fragmented socket bytes into one complete FMQ frame") {
    flowmq_stream_decoder_t stream;
    flowmq_protocol_frame_t input;
    flowmq_protocol_frame_t output;
    tstr_t encoded = NULL;
    size_t consumed = 0u;
    size_t split;

    memset(&stream, 0, sizeof(stream));
    memset(&input, 0, sizeof(input));
    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_SUB;
    input.message_id = 17u;
    input.topic = tstr_v_from_cstr("orders.created");
    input.payload = tstr_v_from_cstr("accepted");
    check_int_eq(flowmq_protocol_encode_frame(&input, 1024u, &encoded), TURBO_OK);
    check_int_eq(flowmq_stream_decoder_prepare(&stream, 1024u), TURBO_OK);

    split = FLOWMQ_PROTOCOL_HEADER_SIZE - 1u;
    check_int_eq(flowmq_stream_decoder_append(&stream, encoded, split), TURBO_OK);
    check_int_eq(flowmq_stream_decoder_next(&stream, &output, &consumed),
                 FLOWMQ_PROTOCOL_INCOMPLETE);
    check_int_eq(flowmq_stream_decoder_append(&stream, encoded + split, tstr_len(encoded) - split),
                 TURBO_OK);
    check_int_eq(flowmq_stream_decoder_next(&stream, &output, &consumed), TURBO_OK);
    check_uint_eq(output.message_id, 17u);
    check_size_eq(output.topic.len, 14u);
    check_mem_eq(output.topic.data, "orders.created", 14u);
    check_size_eq(output.payload.len, 8u);
    check_mem_eq(output.payload.data, "accepted", 8u);
    check_int_eq(flowmq_stream_decoder_consume(&stream, consumed), TURBO_OK);

    flowmq_protocol_frame_cleanup(&output);
    flowmq_stream_decoder_destroy(&stream);
    tstr_free(encoded);
  }

  it("enforces one immutable frame limit per stream") {
    flowmq_stream_decoder_t stream;
    memset(&stream, 0, sizeof(stream));

    check_int_eq(flowmq_stream_decoder_prepare(&stream, 128u), TURBO_OK);
    check_int_eq(flowmq_stream_decoder_prepare(&stream, 128u), TURBO_OK);
    check_int_eq(flowmq_stream_decoder_prepare(&stream, 256u), TURBO_EALREADY);
    check_int_eq(
        flowmq_stream_decoder_append(&stream, NULL, flowmq_stream_decoder_available(&stream) + 1u),
        TURBO_EINVAL);

    flowmq_stream_decoder_destroy(&stream);
  }
}
