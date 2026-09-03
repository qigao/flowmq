#include "flowmq_protocol.h"
#include "flowmq_protocol_catalog.h"
#include "flowmq_protocol_internal.h"
#include "flowmq_security.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static tstr flowmq_protocol_test_flatten(const flowmq_protocol_segmented_frame_t *segmented) {
  tstr flat;
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
  it("publishes one immutable current-version descriptor per protocol family") {
    const flowmq_protocol_descriptor_t *fmq =
        flowmq_protocol_catalog_get(FLOWMQ_PROTOCOL_FAMILY_FMQ);
    const flowmq_protocol_descriptor_t *fms =
        flowmq_protocol_catalog_get(FLOWMQ_PROTOCOL_FAMILY_FMS);
    const flowmq_protocol_descriptor_t *fes =
        flowmq_protocol_catalog_get(FLOWMQ_PROTOCOL_FAMILY_FES);
    const flowmq_protocol_descriptor_t *fmp =
        flowmq_protocol_catalog_get(FLOWMQ_PROTOCOL_FAMILY_FMP);

    check_equal(flowmq_protocol_catalog_count(), 4u);
    check_not_null(fmq);
    check_equal(fmq->version, FLOWMQ_PROTOCOL_FMQ_VERSION);
    check_equal(fmq->layer, FLOWMQ_PROTOCOL_LAYER_TRANSPORT);
    check_equal(fmq->magic, "TFMQ", 4u);
    check_not_null(fms);
    check_equal(fms->version, FLOWMQ_PROTOCOL_FMS_VERSION);
    check_equal(fms->magic, "FMS3", 4u);
    check_not_null(fes);
    check_equal(fes->version, FLOWMQ_PROTOCOL_FES_VERSION);
    check_equal(fes->magic, "FES1", 4u);
    check_not_null(fmp);
    check_equal(fmp->version, FLOWMQ_PROTOCOL_FMP_VERSION);
    check_equal(fmp->magic_size, 0u);
    check_null(flowmq_protocol_catalog_get(FLOWMQ_PROTOCOL_FAMILY_COUNT));
    check_null(flowmq_protocol_catalog_at(flowmq_protocol_catalog_count()));
  }

  it("keeps FMQ/6 transport namespaces limited to ZeroMQ-style patterns") {
    flowmq_protocol_frame_t frame = {.kind = FLOWMQ_PROTOCOL_FRAME_DATA,
                                     .pattern = (flowmq_protocol_pattern_t)12u,
                                     .message_id = 1u};
    tstr encoded = NULL;

    check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_EINVAL);
    check_null(encoded);
    frame.pattern = FLOWMQ_PROTOCOL_PAIR;
    frame.kind = (flowmq_protocol_frame_kind_t)7u;
    check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_EINVAL);
    check_null(encoded);
  }

  it("round trips FMQ/6 SETTINGS and FLOW_UPDATE payloads") {
    flowmq_protocol_settings_t settings = {.capabilities = FLOWMQ_PROTOCOL_CAP_FLOW_CREDIT,
                                           .max_frame_size = 1024u * 1024u,
                                           .session_generation = UINT64_C(0x0102030405060708),
                                           .initial_max_data = UINT64_C(16) * 1024u * 1024u,
                                           .flow_update_quantum = 64u * 1024u,
                                           .flow_update_interval_ms = 10u};
    flowmq_protocol_settings_t decoded_settings = {0};
    flowmq_protocol_flow_update_t update = {.session_generation = UINT64_C(0x0102030405060708),
                                            .consumed_data = UINT64_C(0x1112131415161718),
                                            .max_data = UINT64_C(0x2122232425262728)};
    flowmq_protocol_flow_update_t decoded_update = {0};
    unsigned char settings_payload[FLOWMQ_PROTOCOL_SETTINGS_PAYLOAD_SIZE];
    unsigned char update_payload[FLOWMQ_PROTOCOL_FLOW_UPDATE_PAYLOAD_SIZE];

    check_equal(flowmq_protocol_settings_encode(&settings, settings_payload), TURBO_OK);
    check_equal(settings_payload[0], 0u);
    check_equal(settings_payload[3], FLOWMQ_PROTOCOL_CAP_FLOW_CREDIT);
    check_equal(settings_payload[8], 0x01u);
    check_equal(settings_payload[15], 0x08u);
    check_equal(flowmq_protocol_settings_decode(
                    vstr_from_buf((const char *)settings_payload, sizeof(settings_payload)),
                    &decoded_settings),
                TURBO_OK);
    check_equal(decoded_settings.capabilities, settings.capabilities);
    check_equal(decoded_settings.max_frame_size, settings.max_frame_size);
    check_equal(decoded_settings.session_generation, settings.session_generation);
    check_equal(decoded_settings.initial_max_data, settings.initial_max_data);
    check_equal(decoded_settings.flow_update_quantum, settings.flow_update_quantum);
    check_equal(decoded_settings.flow_update_interval_ms, settings.flow_update_interval_ms);

    check_equal(flowmq_protocol_flow_update_encode(&update, update_payload), TURBO_OK);
    check_equal(update_payload[0], 0x01u);
    check_equal(update_payload[7], 0x08u);
    check_equal(
        flowmq_protocol_flow_update_decode(
            vstr_from_buf((const char *)update_payload, sizeof(update_payload)), &decoded_update),
        TURBO_OK);
    check_equal(decoded_update.session_generation, update.session_generation);
    check_equal(decoded_update.consumed_data, update.consumed_data);
    check_equal(decoded_update.max_data, update.max_data);
  }

  it("rejects invalid FMQ/6 flow-control payloads") {
    flowmq_protocol_settings_t settings = {.capabilities = FLOWMQ_PROTOCOL_CAP_FLOW_CREDIT,
                                           .max_frame_size = 1024u,
                                           .session_generation = 1u,
                                           .initial_max_data = 4096u,
                                           .flow_update_quantum = 1024u,
                                           .flow_update_interval_ms = 10u};
    flowmq_protocol_flow_update_t update = {
        .session_generation = 1u, .consumed_data = 20u, .max_data = 19u};
    unsigned char settings_payload[FLOWMQ_PROTOCOL_SETTINGS_PAYLOAD_SIZE] = {0};
    unsigned char update_payload[FLOWMQ_PROTOCOL_FLOW_UPDATE_PAYLOAD_SIZE] = {0};

    settings.capabilities = 0u;
    check_equal(flowmq_protocol_settings_encode(&settings, settings_payload), TURBO_EPROTO);
    settings.capabilities = FLOWMQ_PROTOCOL_CAP_FLOW_CREDIT << 1u;
    check_equal(flowmq_protocol_settings_encode(&settings, settings_payload), TURBO_EPROTO);
    settings.capabilities = FLOWMQ_PROTOCOL_CAP_FLOW_CREDIT;
    settings.flow_update_quantum = (uint32_t)settings.initial_max_data + 1u;
    check_equal(flowmq_protocol_settings_encode(&settings, settings_payload), TURBO_EPROTO);
    settings.flow_update_quantum = 1024u;
    settings.session_generation = 0u;
    check_equal(flowmq_protocol_settings_encode(&settings, settings_payload), TURBO_EPROTO);
    check_equal(flowmq_protocol_settings_decode(vstr_from_buf((const char *)settings_payload, 31u),
                                                &settings),
                TURBO_EPROTO);

    check_equal(flowmq_protocol_flow_update_encode(&update, update_payload), TURBO_EPROTO);
    update.max_data = update.consumed_data;
    update.session_generation = 0u;
    check_equal(flowmq_protocol_flow_update_encode(&update, update_payload), TURBO_EPROTO);
    check_equal(flowmq_protocol_flow_update_decode(vstr_from_buf((const char *)update_payload, 23u),
                                                   &update),
                TURBO_EPROTO);
  }

  it("enforces FMQ/6 control-frame metadata") {
    flowmq_protocol_settings_t settings = {.capabilities = FLOWMQ_PROTOCOL_CAP_FLOW_CREDIT,
                                           .max_frame_size = 1024u,
                                           .session_generation = 1u,
                                           .initial_max_data = 4096u,
                                           .flow_update_quantum = 1024u,
                                           .flow_update_interval_ms = 10u};
    flowmq_protocol_frame_t frame = {0};
    flowmq_protocol_frame_t decoded = {0};
    unsigned char payload[FLOWMQ_PROTOCOL_SETTINGS_PAYLOAD_SIZE];
    tstr encoded = NULL;
    size_t consumed = 0u;

    check_equal(flowmq_protocol_settings_encode(&settings, payload), TURBO_OK);
    frame.kind = FLOWMQ_PROTOCOL_FRAME_SETTINGS;
    frame.pattern = FLOWMQ_PROTOCOL_PAIR;
    frame.payload = vstr_from_buf((const char *)payload, sizeof(payload));
    check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_OK);
    check_equal(
        flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &decoded, &consumed),
        TURBO_OK);
    check_equal(decoded.kind, FLOWMQ_PROTOCOL_FRAME_SETTINGS);
    flowmq_protocol_frame_cleanup(&decoded);
    tstr_freep(&encoded);

    frame.message_id = 1u;
    check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_EPROTO);
    frame.message_id = 0u;
    frame.topic = vstr_from_cstr("forbidden");
    check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_EPROTO);
    frame.topic = vstr_from_buf(NULL, 0u);
    frame.payload.len--;
    check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_EPROTO);
  }

  it("preserves the multipart-more bit across encode and decode") {
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_frame_t output = {0};
    tstr encoded = NULL;
    size_t consumed = 0u;

    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_DEALER;
    input.message_id = 1u;
    input.more = 1;
    input.payload = vstr_from_cstr("part-1");

    check_equal(flowmq_protocol_encode_frame(&input, 1024u, &encoded), TURBO_OK);
    check_equal(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &output, &consumed),
                TURBO_OK);
    check_equal(output.more, 1);
    check_equal(consumed, tstr_len(encoded));
    check_true(vstr_eq(output.payload, input.payload));

    flowmq_protocol_frame_cleanup(&output);
    tstr_freep(&encoded);
  }

  it("rejects multipart on protocol control frames") {
    flowmq_protocol_frame_t input = {0};
    tstr encoded = NULL;

    input.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
    input.pattern = FLOWMQ_PROTOCOL_PAIR;
    input.more = 1;
    check_equal(flowmq_protocol_encode_frame(&input, 1024u, &encoded), TURBO_EPROTO);
    check_null(encoded);
  }

  it("segments one packet without copying payload bytes") {
    static const char payload[] = {'z', '\0', 'c'};
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_segmented_frame_t segmented = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
    tstr contiguous = NULL;
    tstr flat = NULL;

    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUB;
    input.message_id = 7u;
    input.identity = vstr_from_cstr("publisher");
    input.topic = vstr_from_cstr("events.created");
    input.payload = vstr_from_buf(payload, sizeof(payload));
    check_equal(flowmq_protocol_encode_frame(&input, 1024u, &contiguous), TURBO_OK);
    check_equal(flowmq_protocol_encode_frame_segmented(&input, 1024u, &segmented), TURBO_OK);
    check_equal(segmented.segment_count, 2u);
    check_true(segmented.segments[1].data == payload);
    check_equal(segmented.encoded_size, tstr_len(contiguous));
    flat = flowmq_protocol_test_flatten(&segmented);
    check_not_null(flat);
    check_equal(flat, contiguous, tstr_len(contiguous));

    tstr_free(flat);
    tstr_free(contiguous);
    flowmq_protocol_segmented_frame_cleanup(&segmented);
  }

  it("segments fragmented payload with byte-identical wire framing") {
    static char payload[FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 19u];
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_segmented_frame_t segmented = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
    tstr contiguous = NULL;
    tstr flat = NULL;

    for (size_t i = 0u; i < sizeof(payload); ++i)
      payload[i] = (char)(i % 239u);
    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUSH;
    input.message_id = 9u;
    input.topic = vstr_from_cstr("bulk");
    input.payload = vstr_from_buf(payload, sizeof(payload));
    check_equal(flowmq_protocol_encode_frame(&input, sizeof(payload) + 4u, &contiguous), TURBO_OK);
    check_equal(flowmq_protocol_encode_frame_segmented(&input, sizeof(payload) + 4u, &segmented),
                TURBO_OK);
    check_equal(segmented.segment_count, 4u);
    check_true(segmented.segments[1].data == payload);
    check_true(segmented.segments[3].data == payload + FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE);
    flat = flowmq_protocol_test_flatten(&segmented);
    check_not_null(flat);
    check_equal(flat, contiguous, tstr_len(contiguous));

    tstr_free(flat);
    tstr_free(contiguous);
    flowmq_protocol_segmented_frame_cleanup(&segmented);
  }

  it("writes segmented framing into caller-provided bounded storage") {
    static char payload[FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 19u];
    enum {
      SEGMENT_CAPACITY = 4u,
      FRAMING_CAPACITY = 2u * FLOWMQ_PROTOCOL_HEADER_SIZE + 4u + 5u
    };
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_segment_t segments[SEGMENT_CAPACITY];
    unsigned char framing[FRAMING_CAPACITY];
    flowmq_protocol_segmented_frame_t view = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
    tstr contiguous = NULL;
    tstr flat = NULL;
    size_t segment_count = 99u;
    size_t encoded_size = 99u;

    for (size_t i = 0u; i < sizeof(payload); ++i)
      payload[i] = (char)(i % 239u);
    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUSH;
    input.message_id = 10u;
    input.identity = vstr_from_cstr("node");
    input.topic = vstr_from_cstr("event");
    input.payload = vstr_from_buf(payload, sizeof(payload));
    check_equal(flowmq_protocol_encode_frame(&input, sizeof(payload) + 9u,
                                             &contiguous),
                TURBO_OK);
    check_equal(flowmq_protocol_encode_frame_segmented_into_internal(
                    &input, sizeof(payload) + 9u, segments, SEGMENT_CAPACITY,
                    framing, sizeof(framing), &segment_count, &encoded_size),
                TURBO_OK);
    check_equal(segment_count, (size_t)SEGMENT_CAPACITY);
    check_equal(encoded_size, tstr_len(contiguous));
    check_true(segments[1].data == payload);
    check_true(segments[3].data ==
               payload + FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE);
    view.segments = segments;
    view.segment_count = segment_count;
    view.encoded_size = encoded_size;
    flat = flowmq_protocol_test_flatten(&view);
    check_not_null(flat);
    check_equal(flat, contiguous, tstr_len(contiguous));

    segment_count = 99u;
    encoded_size = 99u;
    check_equal(flowmq_protocol_encode_frame_segmented_into_internal(
                    &input, sizeof(payload) + 9u, segments,
                    SEGMENT_CAPACITY - 1u, framing, sizeof(framing),
                    &segment_count, &encoded_size),
                TURBO_ENOSPC);
    check_equal(segment_count, 0u);
    check_equal(encoded_size, 0u);
    check_equal(flowmq_protocol_encode_frame_segmented_into_internal(
                    &input, sizeof(payload) + 9u, segments, SEGMENT_CAPACITY,
                    framing, sizeof(framing) - 1u, &segment_count,
                    &encoded_size),
                TURBO_ENOSPC);
    check_equal(segment_count, 0u);
    check_equal(encoded_size, 0u);

    tstr_free(flat);
    tstr_free(contiguous);
  }

  it("segments zero-length payload as one complete framing segment") {
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_segmented_frame_t segmented = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
    tstr contiguous = NULL;
    tstr flat = NULL;

    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_DEALER;
    input.message_id = 11u;
    input.identity = vstr_from_cstr("client");
    input.topic = vstr_from_cstr("empty");
    check_equal(flowmq_protocol_encode_frame(&input, 1024u, &contiguous), TURBO_OK);
    check_equal(flowmq_protocol_encode_frame_segmented(&input, 1024u, &segmented), TURBO_OK);
    check_equal(segmented.segment_count, 1u);
    flat = flowmq_protocol_test_flatten(&segmented);
    check_not_null(flat);
    check_equal(flat, contiguous, tstr_len(contiguous));

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
    input.payload = vstr_from_cstr("payload");
    segmented.encoded_size = 1u;
    check_equal(flowmq_protocol_encode_frame_segmented(&input, 1024u, &segmented), TURBO_EINVAL);
    segmented.encoded_size = 0u;
    check_equal(flowmq_protocol_encode_frame_segmented(&input, 3u, &segmented), TURBO_EMSGSIZE);
    flowmq_protocol_segmented_frame_cleanup(NULL);
  }

  it("round trips binary data through the public protocol API") {
    static const char payload[] = {'a', '\0', 'b'};
    flowmq_protocol_frame_t input;
    flowmq_protocol_frame_t output;
    tstr encoded = NULL;
    size_t consumed = 0u;

    memset(&input, 0, sizeof(input));
    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUB;
    input.message_id = UINT64_C(42);
    input.identity = vstr_from_cstr("publisher");
    input.topic = vstr_from_cstr("orders.created");
    input.payload = vstr_from_buf(payload, sizeof(payload));

    check_equal(flowmq_protocol_encode_frame(&input, 1024u, &encoded), TURBO_OK);
    check_equal(flowmq_protocol_decode_frame(encoded, FLOWMQ_PROTOCOL_HEADER_SIZE - 1u, 1024u,
                                             &output, &consumed),
                FLOWMQ_PROTOCOL_INCOMPLETE);
    check_equal(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &output, &consumed),
                TURBO_OK);
    check_equal(output.kind, FLOWMQ_PROTOCOL_FRAME_DATA);
    check_equal(output.pattern, FLOWMQ_PROTOCOL_PUB);
    check_equal(output.payload.len, sizeof(payload));
    check_equal(output.payload.data, payload, sizeof(payload));
    check_equal(consumed, tstr_len(encoded));

    flowmq_protocol_frame_cleanup(&output);
    tstr_free(encoded);
  }

  it("reassembles a fragmented FMQ/6 payload into owned storage") {
    static char payload[FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 17u];
    flowmq_protocol_frame_t input;
    flowmq_protocol_frame_t output;
    tstr encoded = NULL;
    size_t consumed = 0u;

    for (size_t i = 0u; i < sizeof(payload); ++i)
      payload[i] = (char)(i % 251u);
    memset(&input, 0, sizeof(input));
    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUSH;
    input.message_id = UINT64_C(0x1020304050607080);
    input.payload = vstr_from_buf(payload, sizeof(payload));

    check_equal(flowmq_protocol_encode_frame(&input, sizeof(payload), &encoded), TURBO_OK);
    check_equal(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), sizeof(payload), &output,
                                             &consumed),
                TURBO_OK);
    check_not_null(output.owned_payload);
    check_equal(output.payload.len, sizeof(payload));
    check_equal(output.payload.data, payload, sizeof(payload));

    flowmq_protocol_frame_cleanup(&output);
    tstr_free(encoded);
  }

  it("encodes and decodes FMS/3 HELLO security with optional transport binding") {
    static const char binding[FLOWMQ_SECURITY_CHANNEL_BINDING_SIZE] = {1};
    flowmq_security_t input = {0};
    flowmq_security_t output;
    tstr payload = NULL;

    input.mode = FLOWMQ_SECURITY_AUTH;
    input.identity = vstr_from_cstr("client-a");
    input.method = vstr_from_cstr("token");
    input.secret = vstr_from_cstr("credential");
    input.channel_binding = vstr_from_buf(binding, sizeof(binding));
    check_equal(flowmq_security_encode(&input, &payload), TURBO_OK);
    check_equal(flowmq_security_decode(tstr_to_v(payload), &output), TURBO_OK);
    check_equal(output.mode, FLOWMQ_SECURITY_AUTH);
    check_equal(output.identity.len, sizeof("client-a") - 1u);
    check_equal(output.identity.data, "client-a", sizeof("client-a") - 1u);
    check_equal(output.method.len, sizeof("token") - 1u);
    check_equal(output.method.data, "token", sizeof("token") - 1u);
    check_equal(output.channel_binding.len, sizeof(binding));
    check_equal(output.secret.data, "credential", sizeof("credential") - 1u);
    tstr_free(payload);

    payload = NULL;
    input.channel_binding = vstr_from_buf(NULL, 0u);
    check_equal(flowmq_security_encode(&input, &payload), TURBO_OK);
    check_equal(flowmq_security_decode(tstr_to_v(payload), &output), TURBO_OK);
    check_equal(output.channel_binding.len, 0u);
    tstr_free(payload);
  }

  it("round trips the secure server acceptance HELLO") {
    static const char binding[FLOWMQ_SECURITY_CHANNEL_BINDING_SIZE] = {2};
    flowmq_security_t security = {0};
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_frame_t output;
    tstr payload = NULL;
    tstr encoded = NULL;
    size_t consumed = 0u;

    security.mode = FLOWMQ_SECURITY_ACCEPTED;
    security.channel_binding = vstr_from_buf(binding, sizeof(binding));
    check_equal(flowmq_security_encode(&security, &payload), TURBO_OK);
    input.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
    input.pattern = FLOWMQ_PROTOCOL_PUB;
    input.topic = vstr_from_cstr("secure");
    input.payload = tstr_to_v(payload);
    check_equal(flowmq_protocol_encode_frame(&input, 1024u, &encoded), TURBO_OK);
    check_equal(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &output, &consumed),
                TURBO_OK);
    check_equal(output.kind, FLOWMQ_PROTOCOL_FRAME_HELLO);
    check_equal(flowmq_security_decode(output.payload, &security), TURBO_OK);
    check_equal(security.mode, FLOWMQ_SECURITY_ACCEPTED);
    flowmq_protocol_frame_cleanup(&output);
    tstr_free(encoded);
    tstr_free(payload);
  }

  it("rejects unknown wire versions and malformed FMS/3 security envelopes") {
    static const char invalid_identity[] = {'c', 'l', 'i', 'e', 'n', 't', '\0', 'x'};
    static const char binding[FLOWMQ_SECURITY_CHANNEL_BINDING_SIZE] = {3};
    flowmq_security_t security = {0};
    flowmq_protocol_frame_t frame = {0};
    flowmq_protocol_frame_t decoded;
    tstr encoded = NULL;
    tstr security_payload = NULL;
    size_t consumed = 0u;

    frame.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
    frame.pattern = FLOWMQ_PROTOCOL_DEALER;
    frame.identity = vstr_from_cstr("client-a");
    check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_OK);
    encoded[4] = 5u;
    check_equal(
        flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &decoded, &consumed),
        TURBO_EPROTO);
    encoded[4] = FLOWMQ_PROTOCOL_WIRE_VERSION + 1u;
    check_equal(
        flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &decoded, &consumed),
        TURBO_EPROTO);
    encoded[4] = FLOWMQ_PROTOCOL_WIRE_VERSION;
    encoded[24] = 0;
    encoded[25] = 0;
    encoded[26] = 0;
    encoded[27] = 1;
    check_equal(
        flowmq_protocol_decode_frame(encoded, tstr_len(encoded), 1024u, &decoded, &consumed),
        TURBO_EPROTO);
    tstr_free(encoded);

    security.mode = FLOWMQ_SECURITY_AUTH;
    security.identity = vstr_from_buf(invalid_identity, sizeof(invalid_identity));
    security.method = vstr_from_cstr("token");
    security.secret = vstr_from_cstr("secret");
    security.channel_binding = vstr_from_buf(binding, sizeof(binding));
    check_equal(flowmq_security_encode(&security, &security_payload), TURBO_EPROTO);
    check_null(security_payload);

    security.identity = vstr_from_cstr("client-a");
    check_equal(flowmq_security_encode(&security, &security_payload), TURBO_OK);
    security_payload[3] = '2';
    check_equal(flowmq_security_decode(tstr_to_v(security_payload), &security), TURBO_EPROTO);
    security_payload[3] = '3';
    security_payload[FLOWMQ_SECURITY_HEADER_SIZE + 2u] = '\0';
    check_equal(flowmq_security_decode(tstr_to_v(security_payload), &security), TURBO_EPROTO);
    tstr_free(security_payload);

    security_payload = NULL;
    security.identity = vstr_from_cstr("client-a");
    security.method = vstr_from_cstr("token");
    security.secret = vstr_from_cstr("secret");
    security.channel_binding = vstr_from_buf(binding, 1u);
    check_equal(flowmq_security_encode(&security, &security_payload), TURBO_EPROTO);
    check_null(security_payload);
  }

  it("rejects invalid control-frame metadata") {
    flowmq_protocol_frame_t frame;
    tstr encoded = NULL;

    memset(&frame, 0, sizeof(frame));
    frame.kind = FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE;
    frame.pattern = FLOWMQ_PROTOCOL_XSUB;
    frame.identity = vstr_from_cstr("peer");
    check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded), TURBO_EPROTO);
    check_null(encoded);
  }

  it("keeps receive and heartbeat deadlines independent") {
    flowmq_protocol_heartbeat_deadlines_t heartbeat;
    uint64_t wait_deadline_ns = 0u;
    const uint64_t start_ns = UINT64_C(1000000000);

    flowmq_protocol_heartbeat_deadlines_init(&heartbeat, start_ns, 20u, 500u, 55u);
    flowmq_protocol_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(20000000));
    flowmq_protocol_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(40000000));
    check_equal(flowmq_protocol_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(55000000),
                                                         &wait_deadline_ns),
                FLOWMQ_PROTOCOL_HEARTBEAT_RECV_EXPIRED);
  }

  it("starts the heartbeat timeout only after a ping is sent") {
    flowmq_protocol_heartbeat_deadlines_t heartbeat;
    uint64_t wait_deadline_ns = 0u;
    const uint64_t start_ns = UINT64_C(1000000000);

    flowmq_protocol_heartbeat_deadlines_init(&heartbeat, start_ns, 20u, 30u, 0u);
    check_equal(flowmq_protocol_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(20000000),
                                                         &wait_deadline_ns),
                FLOWMQ_PROTOCOL_HEARTBEAT_SEND_PING);
    flowmq_protocol_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(20000000));
    check_equal(flowmq_protocol_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(40000000),
                                                         &wait_deadline_ns),
                FLOWMQ_PROTOCOL_HEARTBEAT_SEND_PING);
    flowmq_protocol_heartbeat_deadlines_on_ping(&heartbeat, start_ns + UINT64_C(40000000));
    check_equal(flowmq_protocol_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(49000000),
                                                         &wait_deadline_ns),
                FLOWMQ_PROTOCOL_HEARTBEAT_WAIT);
    check_equal(flowmq_protocol_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(50000000),
                                                         &wait_deadline_ns),
                FLOWMQ_PROTOCOL_HEARTBEAT_EXPIRED);

    flowmq_protocol_heartbeat_deadlines_on_receive(&heartbeat, start_ns + UINT64_C(55000000));
    check_equal(flowmq_protocol_heartbeat_deadlines_next(&heartbeat, start_ns + UINT64_C(74000000),
                                                         &wait_deadline_ns),
                FLOWMQ_PROTOCOL_HEARTBEAT_WAIT);
  }
}
