#include "flowmq_protocol.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static tstr_t flowmq_protocol_test_flatten(
    const flowmq_protocol_segmented_frame_t *segmented) {
  tstr_t flat;
  size_t offset = 0u;
  if (!segmented || segmented->encoded_size == 0u) return NULL;
  flat = tstr_new_len(NULL, segmented->encoded_size);
  if (!flat) return NULL;
  for (size_t i = 0u; i < segmented->segment_count; ++i) {
    if (segmented->segments[i].size > segmented->encoded_size - offset) {
      tstr_free(flat);
      return NULL;
    }
    memcpy(flat + offset, segmented->segments[i].data, segmented->segments[i].size);
    offset += segmented->segments[i].size;
  }
  if (offset != segmented->encoded_size) {
    tstr_free(flat);
    return NULL;
  }
  return flat;
}

spec("flowmq_protocol") {
  it("segments one packet without copying payload bytes") {
    static const char payload[] = {'z', '\0', 'c'};
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_segmented_frame_t segmented = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
    tstr_t contiguous = NULL;
    tstr_t flat = NULL;

    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUB;
    input.message_id = 7u;
    input.identity = tstr_v_from_cstr("publisher");
    input.topic = tstr_v_from_cstr("events.created");
    input.payload = tstr_v_from_buf(payload, sizeof(payload));
    check_int_eq(flowmq_protocol_encode_frame(&input, 1024u, &contiguous), TURBO_OK);
    check_int_eq(flowmq_protocol_encode_frame_segmented(&input, 1024u, &segmented), TURBO_OK);
    check_size_eq(segmented.segment_count, 2u);
    check_true(segmented.segments[1].data == payload);
    check_size_eq(segmented.encoded_size, tstr_len(contiguous));
    flat = flowmq_protocol_test_flatten(&segmented);
    check_not_null(flat);
    check_mem_eq(flat, contiguous, tstr_len(contiguous));

    tstr_free(flat);
    tstr_free(contiguous);
    flowmq_protocol_segmented_frame_cleanup(&segmented);
  }

  it("segments fragmented payload with byte-identical wire framing") {
    static char payload[FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 19u];
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_segmented_frame_t segmented = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
    tstr_t contiguous = NULL;
    tstr_t flat = NULL;

    for (size_t i = 0u; i < sizeof(payload); ++i) payload[i] = (char)(i % 239u);
    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUSH;
    input.message_id = 9u;
    input.topic = tstr_v_from_cstr("bulk");
    input.payload = tstr_v_from_buf(payload, sizeof(payload));
    check_int_eq(flowmq_protocol_encode_frame(&input, sizeof(payload) + 4u, &contiguous),
                 TURBO_OK);
    check_int_eq(flowmq_protocol_encode_frame_segmented(
                     &input, sizeof(payload) + 4u, &segmented),
                 TURBO_OK);
    check_size_eq(segmented.segment_count, 4u);
    check_true(segmented.segments[1].data == payload);
    check_true(segmented.segments[3].data == payload + FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE);
    flat = flowmq_protocol_test_flatten(&segmented);
    check_not_null(flat);
    check_mem_eq(flat, contiguous, tstr_len(contiguous));

    tstr_free(flat);
    tstr_free(contiguous);
    flowmq_protocol_segmented_frame_cleanup(&segmented);
  }

  it("segments zero-length payload as one complete framing segment") {
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_segmented_frame_t segmented = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
    tstr_t contiguous = NULL;
    tstr_t flat = NULL;

    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_DEALER;
    input.message_id = 11u;
    input.identity = tstr_v_from_cstr("client");
    input.topic = tstr_v_from_cstr("empty");
    check_int_eq(flowmq_protocol_encode_frame(&input, 1024u, &contiguous), TURBO_OK);
    check_int_eq(flowmq_protocol_encode_frame_segmented(&input, 1024u, &segmented), TURBO_OK);
    check_size_eq(segmented.segment_count, 1u);
    flat = flowmq_protocol_test_flatten(&segmented);
    check_not_null(flat);
    check_mem_eq(flat, contiguous, tstr_len(contiguous));

    flowmq_protocol_segmented_frame_cleanup(&segmented);
    flowmq_protocol_segmented_frame_cleanup(&segmented);
    tstr_free(flat);
    tstr_free(contiguous);
  }

  it("rejects invalid segmented output state and frame limits") {
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_segmented_frame_t segmented = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;

    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUB;
    input.message_id = 13u;
    input.payload = tstr_v_from_cstr("payload");
    segmented.encoded_size = 1u;
    check_int_eq(flowmq_protocol_encode_frame_segmented(&input, 1024u, &segmented),
                 TURBO_EINVAL);
    segmented.encoded_size = 0u;
    check_int_eq(flowmq_protocol_encode_frame_segmented(&input, 3u, &segmented),
                 TURBO_EMSGSIZE);
    flowmq_protocol_segmented_frame_cleanup(NULL);
  }

  it("round trips binary data without a TurboFlow dependency") {
    static const char payload[] = {'a', '\0', 'b'};
    flowmq_protocol_frame_t input;
    flowmq_protocol_frame_t output;
    tstr_t encoded = NULL;
    size_t consumed = 0u;

    memset(&input, 0, sizeof(input));
    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUB;
    input.message_id = UINT64_C(42);
    input.identity = tstr_v_from_cstr("publisher");
    input.topic = tstr_v_from_cstr("orders.created");
    input.payload = tstr_v_from_buf(payload, sizeof(payload));

    check_int_eq(flowmq_protocol_encode_frame(&input, 1024u, &encoded), TURBO_OK);
    check_int_eq(flowmq_protocol_decode_frame(encoded, FLOWMQ_PROTOCOL_HEADER_SIZE - 1u, 1024u,
                                              &output, &consumed),
                 FLOWMQ_PROTOCOL_INCOMPLETE);
    check_int_eq(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &output,
                                              &consumed),
                 TURBO_OK);
    check_int_eq(output.kind, FLOWMQ_PROTOCOL_FRAME_DATA);
    check_int_eq(output.pattern, FLOWMQ_PROTOCOL_PUB);
    check_size_eq(output.payload.len, sizeof(payload));
    check_mem_eq(output.payload.data, payload, sizeof(payload));
    check_size_eq(consumed, tstr_len(encoded));

    flowmq_protocol_frame_cleanup(&output);
    tstr_free(encoded);
  }

  it("reassembles a fragmented v3 payload into owned storage") {
    static char payload[FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 17u];
    flowmq_protocol_frame_t input;
    flowmq_protocol_frame_t output;
    tstr_t encoded = NULL;
    size_t consumed = 0u;

    for (size_t i = 0u; i < sizeof(payload); ++i) payload[i] = (char)(i % 251u);
    memset(&input, 0, sizeof(input));
    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUSH;
    input.message_id = UINT64_C(0x1020304050607080);
    input.payload = tstr_v_from_buf(payload, sizeof(payload));

    check_int_eq(flowmq_protocol_encode_frame(&input, sizeof(payload), &encoded), TURBO_OK);
    check_int_eq(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), sizeof(payload), &output,
                                              &consumed),
                 TURBO_OK);
    check_not_null(output.owned_payload);
    check_size_eq(output.payload.len, sizeof(payload));
    check_mem_eq(output.payload.data, payload, sizeof(payload));

    flowmq_protocol_frame_cleanup(&output);
    tstr_free(encoded);
  }

  it("encodes and decodes v3 HELLO security with optional transport binding") {
    static const char binding[FLOWMQ_PROTOCOL_CHANNEL_BINDING_SIZE] = {1};
    flowmq_protocol_security_t input = {0};
    flowmq_protocol_security_t output;
    tstr_t payload = NULL;

    input.mode = FLOWMQ_PROTOCOL_SECURITY_AUTH;
    input.identity = tstr_v_from_cstr("client-a");
    input.method = tstr_v_from_cstr("token");
    input.secret = tstr_v_from_cstr("credential");
    input.channel_binding = tstr_v_from_buf(binding, sizeof(binding));
    check_int_eq(flowmq_protocol_security_encode(&input, &payload), TURBO_OK);
    check_int_eq(flowmq_protocol_security_decode(tstr_to_v(payload), &output), TURBO_OK);
    check_int_eq(output.mode, FLOWMQ_PROTOCOL_SECURITY_AUTH);
    check_size_eq(output.identity.len, sizeof("client-a") - 1u);
    check_mem_eq(output.identity.data, "client-a", sizeof("client-a") - 1u);
    check_size_eq(output.method.len, sizeof("token") - 1u);
    check_mem_eq(output.method.data, "token", sizeof("token") - 1u);
    check_size_eq(output.channel_binding.len, sizeof(binding));
    check_mem_eq(output.secret.data, "credential", sizeof("credential") - 1u);
    tstr_free(payload);

    payload = NULL;
    input.channel_binding = tstr_v_from_buf(NULL, 0u);
    check_int_eq(flowmq_protocol_security_encode(&input, &payload), TURBO_OK);
    check_int_eq(flowmq_protocol_security_decode(tstr_to_v(payload), &output), TURBO_OK);
    check_size_eq(output.channel_binding.len, 0u);
    tstr_free(payload);
  }

  it("round trips the secure server acceptance HELLO") {
    static const char binding[FLOWMQ_PROTOCOL_CHANNEL_BINDING_SIZE] = {2};
    flowmq_protocol_security_t security = {0};
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_frame_t output;
    tstr_t payload = NULL;
    tstr_t encoded = NULL;
    size_t consumed = 0u;

    security.mode = FLOWMQ_PROTOCOL_SECURITY_ACCEPTED;
    security.channel_binding = tstr_v_from_buf(binding, sizeof(binding));
    check_int_eq(flowmq_protocol_security_encode(&security, &payload), TURBO_OK);
    input.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
    input.pattern = FLOWMQ_PROTOCOL_PUB;
    input.topic = tstr_v_from_cstr("secure");
    input.payload = tstr_to_v(payload);
    check_int_eq(flowmq_protocol_encode_frame(&input, 1024u, &encoded), TURBO_OK);
    check_int_eq(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &output,
                                              &consumed),
                 TURBO_OK);
    check_int_eq(output.kind, FLOWMQ_PROTOCOL_FRAME_HELLO);
    check_int_eq(flowmq_protocol_security_decode(output.payload, &security), TURBO_OK);
    check_int_eq(security.mode, FLOWMQ_PROTOCOL_SECURITY_ACCEPTED);
    flowmq_protocol_frame_cleanup(&output);
    tstr_free(encoded);
    tstr_free(payload);
  }

  it("rejects unknown wire versions and malformed v3 security envelopes") {
    static const char invalid_identity[] = {'c', 'l', 'i', 'e', 'n', 't', '\0', 'x'};
    static const char binding[FLOWMQ_PROTOCOL_CHANNEL_BINDING_SIZE] = {3};
    flowmq_protocol_security_t security = {0};
    flowmq_protocol_frame_t frame = {0};
    flowmq_protocol_frame_t decoded;
    tstr_t encoded = NULL;
    tstr_t security_payload = NULL;
    size_t consumed = 0u;

    frame.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
    frame.pattern = FLOWMQ_PROTOCOL_DEALER;
    frame.identity = tstr_v_from_cstr("client-a");
    check_int_eq(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_OK);
    encoded[4] = FLOWMQ_PROTOCOL_WIRE_VERSION + 1u;
    check_int_eq(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &decoded,
                                              &consumed),
                 TURBO_EPROTO);
    encoded[4] = FLOWMQ_PROTOCOL_WIRE_VERSION;
    encoded[24] = 0;
    encoded[25] = 0;
    encoded[26] = 0;
    encoded[27] = 1;
    check_int_eq(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &decoded,
                                              &consumed),
                 TURBO_EPROTO);
    tstr_free(encoded);

    security.mode = FLOWMQ_PROTOCOL_SECURITY_AUTH;
    security.identity = tstr_v_from_buf(invalid_identity, sizeof(invalid_identity));
    security.method = tstr_v_from_cstr("token");
    security.secret = tstr_v_from_cstr("secret");
    security.channel_binding = tstr_v_from_buf(binding, sizeof(binding));
    check_int_eq(flowmq_protocol_security_encode(&security, &security_payload), TURBO_EPROTO);
    check_null(security_payload);

    security.identity = tstr_v_from_cstr("client-a");
    check_int_eq(flowmq_protocol_security_encode(&security, &security_payload), TURBO_OK);
    security_payload[FLOWMQ_PROTOCOL_SECURITY_HEADER_SIZE + 2u] = '\0';
    check_int_eq(flowmq_protocol_security_decode(tstr_to_v(security_payload), &security),
                 TURBO_EPROTO);
    tstr_free(security_payload);

    security_payload = NULL;
    security.identity = tstr_v_from_cstr("client-a");
    security.method = tstr_v_from_cstr("token");
    security.secret = tstr_v_from_cstr("secret");
    security.channel_binding = tstr_v_from_buf(binding, 1u);
    check_int_eq(flowmq_protocol_security_encode(&security, &security_payload), TURBO_EPROTO);
    check_null(security_payload);
  }

  it("rejects invalid control-frame metadata") {
    flowmq_protocol_frame_t frame;
    tstr_t encoded = NULL;

    memset(&frame, 0, sizeof(frame));
    frame.kind = FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE;
    frame.pattern = FLOWMQ_PROTOCOL_XSUB;
    frame.identity = tstr_v_from_cstr("peer");
    check_int_eq(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_EPROTO);
    check_null(encoded);
  }

  it("keeps receive and heartbeat deadlines independent") {
    flowmq_protocol_heartbeat_deadlines_t heartbeat;
    uint64_t wait_deadline_ns = 0u;
    const uint64_t start_ns = UINT64_C(1000000000);

    flowmq_protocol_heartbeat_deadlines_init(&heartbeat, start_ns, 20u, 500u, 55u);
    flowmq_protocol_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(20000000));
    flowmq_protocol_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(40000000));
    check_int_eq(flowmq_protocol_heartbeat_deadlines_next(
                     &heartbeat, start_ns + UINT64_C(55000000), &wait_deadline_ns),
                 FLOWMQ_PROTOCOL_HEARTBEAT_RECV_EXPIRED);
  }
}
