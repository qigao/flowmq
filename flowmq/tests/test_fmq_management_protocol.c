#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_fmq_management_protocol.h"

#include <string.h>

static turbo_flow_tfmp_envelope_t tfmp_request(uint16_t kind, uint64_t correlation_id,
                                               const uint8_t *body, size_t body_size) {
  turbo_flow_tfmp_envelope_t envelope = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  envelope.kind = kind;
  envelope.correlation_id = correlation_id;
  envelope.body = body;
  envelope.body_size = body_size;
  return envelope;
}

static turbo_flow_tfmp_envelope_t tfmp_response(uint16_t kind, uint16_t status, const uint8_t *body,
                                                size_t body_size) {
  turbo_flow_tfmp_envelope_t envelope = tfmp_request(kind, 1u, body, body_size);
  envelope.flags = TURBO_FLOW_TFMP_FLAG_RESPONSE;
  envelope.status = status;
  envelope.disposition = status == TURBO_FLOW_TFMP_STATUS_OK ? TURBO_FLOW_TFMP_DISPOSITION_COMPLETED
                                                             : TURBO_FLOW_TFMP_DISPOSITION_FAILED;
  return envelope;
}

static int tfmp_append_response_identity(turbo_flow_tfmp_body_builder_t *builder) {
  static const uint8_t incarnation[16] = {1u};
  int rc = turbo_flow_tfmp_body_builder_append_utf8(builder, 120u, 1, "authority-a", 11u);
  if (rc != TURBO_OK) return rc;
  return turbo_flow_tfmp_body_builder_append(builder, 121u, 1, incarnation, sizeof(incarnation));
}

static int tfmp_build_target(uint8_t *body, size_t capacity, size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  int rc = turbo_flow_tfmp_body_builder_init(&builder, body, capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, "target-a", 8u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 2u, 1, 1u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 3u, 1, 7u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 4u, 1, 7u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 5u, 1, 2u);
  if (rc == TURBO_OK && body_size) *body_size = builder.length;
  return rc;
}

static int tfmp_build_pool_command(uint8_t *body, size_t capacity, uint16_t command_type,
                                   int include_parallelism, size_t *body_size) {
  turbo_flow_tfmp_body_builder_t payload_builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  uint8_t payload[32];
  int rc = turbo_flow_tfmp_body_builder_init(&payload_builder, payload, sizeof(payload));
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&payload_builder, 1u, 1, 1u);
  if (rc == TURBO_OK && include_parallelism)
    rc = turbo_flow_tfmp_body_builder_append_u32(&payload_builder, 2u, 1, 4u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_init(&builder, body, capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, "client-a", 8u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 2u, 1, "key-a", 5u);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 3u, 1, "target-a", 8u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 4u, 1, command_type);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 5u, 1, 1u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 6u, 1, 1u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 8u, 1, 100u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 9u, 1, 1000u);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 10u, 0, payload, payload_builder.length);
  if (rc == TURBO_OK && body_size) *body_size = builder.length;
  return rc;
}

static void tfmp_set_body_size(uint8_t *wire, uint32_t body_size) {
  wire[24] = (uint8_t)(body_size >> 24u);
  wire[25] = (uint8_t)(body_size >> 16u);
  wire[26] = (uint8_t)(body_size >> 8u);
  wire[27] = (uint8_t)body_size;
}

spec("fmq_management_protocol") {
  it("matches the TFMP request header golden vector") {
    static const uint8_t expected[TURBO_FLOW_TFMP_HEADER_SIZE] = {
        'T',  'F',  'M',  'P',  0x00, 0x01, 0x00, 0x00, 0x00, 0x28, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    turbo_flow_tfmp_envelope_t envelope =
        tfmp_request(TURBO_FLOW_TFMP_CAPABILITIES_GET, 42u, NULL, 0u);
    turbo_flow_tfmp_envelope_t decoded = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    uint8_t wire[TURBO_FLOW_TFMP_HEADER_SIZE];
    size_t wire_size = 0u;

    check_int_eq(turbo_flow_tfmp_envelope_encode(&envelope, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    check_size_eq(wire_size, sizeof(expected));
    check_mem_eq(wire, expected, sizeof(expected));
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &decoded), TURBO_OK);
    check_int_eq(decoded.kind, TURBO_FLOW_TFMP_CAPABILITIES_GET);
    check_uint_eq(decoded.correlation_id, 42u);
    check_null(decoded.body);
    check_size_eq(decoded.body_size, 0u);
  }

  it("builds and iterates ordered canonical LTV fields") {
    static const uint8_t expected[] = {0x03, 0x81, 0x00, 0x01, 0x02, 0x02, 'x'};
    const uint8_t version[] = {0x00, 0x01};
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_field_iterator_t iterator = TURBO_FLOW_TFMP_FIELD_ITERATOR_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    uint8_t body[32];

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 1u, 1, version, sizeof(version)),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 2u, 0, (const uint8_t *)"x", 1u),
                 TURBO_OK);
    check_size_eq(builder.length, sizeof(expected));
    check_mem_eq(body, expected, sizeof(expected));
    check_int_eq(turbo_flow_tfmp_field_iterator_init(&iterator, body, builder.length), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_iterator_next(&iterator, &field), TURBO_OK);
    check_int_eq(field.id, 1);
    check_true(field.critical);
    check_size_eq(field.value_size, 2u);
    check_mem_eq(field.value, version, sizeof(version));
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(turbo_flow_tfmp_field_iterator_next(&iterator, &field), TURBO_OK);
    check_int_eq(field.id, 2);
    check_false(field.critical);
    check_mem_eq(field.value, "x", 1u);
    check_int_eq(turbo_flow_tfmp_field_iterator_next(&iterator, &field), TURBO_ENOENT);
  }

  it("round trips response and event envelope semantics") {
    turbo_flow_tfmp_envelope_t response = tfmp_request(TURBO_FLOW_TFMP_HEALTH_GET, 7u, NULL, 0u);
    turbo_flow_tfmp_envelope_t decoded = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    uint8_t wire[TURBO_FLOW_TFMP_HEADER_SIZE];
    size_t wire_size = 0u;

    response.flags = TURBO_FLOW_TFMP_FLAG_RESPONSE | TURBO_FLOW_TFMP_FLAG_REPLAYED;
    response.status = TURBO_FLOW_TFMP_STATUS_OK;
    response.disposition = TURBO_FLOW_TFMP_DISPOSITION_COMPLETED;
    check_int_eq(turbo_flow_tfmp_envelope_encode(&response, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &decoded), TURBO_OK);
    check_bits(decoded.flags, TURBO_FLOW_TFMP_FLAG_RESPONSE | TURBO_FLOW_TFMP_FLAG_REPLAYED);
    check_int_eq(decoded.disposition, TURBO_FLOW_TFMP_DISPOSITION_COMPLETED);

    response.flags = TURBO_FLOW_TFMP_FLAG_EVENT;
    response.status = TURBO_FLOW_TFMP_STATUS_OK;
    response.disposition = TURBO_FLOW_TFMP_DISPOSITION_NONE;
    check_int_eq(turbo_flow_tfmp_envelope_encode(&response, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    decoded = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &decoded), TURBO_OK);
    check_bits(decoded.flags, TURBO_FLOW_TFMP_FLAG_EVENT);
  }

  it("rejects malformed headers without changing output") {
    turbo_flow_tfmp_envelope_t envelope =
        tfmp_request(TURBO_FLOW_TFMP_CAPABILITIES_GET, 42u, NULL, 0u);
    turbo_flow_tfmp_envelope_t decoded = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    uint8_t wire[TURBO_FLOW_TFMP_HEADER_SIZE + 8u];
    size_t wire_size = 0u;
    check_int_eq(turbo_flow_tfmp_envelope_encode(&envelope, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    decoded.kind = 0x7777u;

    wire[0] = 'X';
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &decoded), TURBO_EPROTO);
    check_int_eq(decoded.kind, 0x7777);
    wire[0] = 'T';
    wire[5] = 2u;
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &decoded), TURBO_ENOTSUP);
    wire[5] = 1u;
    wire[15] = 0x08u;
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &decoded), TURBO_EPROTO);
    wire[15] = 0u;
    wire[39] = 1u;
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &decoded), TURBO_EPROTO);
    wire[39] = 0u;
    tfmp_set_body_size(wire, 1u);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &decoded), TURBO_EPROTO);
    tfmp_set_body_size(wire, 0u);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size + 1u, &decoded), TURBO_EPROTO);
    memset(wire + 16u, 0, 8u);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &decoded), TURBO_EPROTO);
  }

  it("rejects noncanonical and descending LTV fields") {
    turbo_flow_tfmp_envelope_t envelope =
        tfmp_request(TURBO_FLOW_TFMP_CAPABILITIES_GET, 1u, NULL, 0u);
    turbo_flow_tfmp_envelope_t decoded = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    uint8_t wire[TURBO_FLOW_TFMP_HEADER_SIZE + 8u];
    size_t wire_size = 0u;
    check_int_eq(turbo_flow_tfmp_envelope_encode(&envelope, wire, sizeof(wire), &wire_size),
                 TURBO_OK);

    wire[40] = 0x81u;
    wire[41] = 0x00u;
    wire[42] = 0x81u;
    tfmp_set_body_size(wire, 3u);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, 43u, &decoded), TURBO_EPROTO);

    wire[40] = 0x01u;
    wire[41] = 0x82u;
    wire[42] = 0x01u;
    wire[43] = 0x81u;
    tfmp_set_body_size(wire, 4u);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, 44u, &decoded), TURBO_EPROTO);
  }

  it("keeps builder state unchanged on order and capacity failures") {
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    uint8_t body[4];
    const uint8_t value[] = {1u, 2u};
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 2u, 0, NULL, 0u), TURBO_OK);
    check_size_eq(builder.length, 2u);
    check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 1u, 0, NULL, 0u), TURBO_EINVAL);
    check_size_eq(builder.length, 2u);
    check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 3u, 0, value, sizeof(value)),
                 TURBO_ENOSPC);
    check_size_eq(builder.length, 2u);
    check_int_eq(builder.previous_id, 2);
  }

  it("enforces repeated-field critical bits and the field-count limit") {
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    uint8_t body[2u * (TURBO_FLOW_TFMP_MAX_FIELDS + 1u)];
    size_t validated_count = 0u;

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    for (size_t i = 0u; i < TURBO_FLOW_TFMP_MAX_FIELDS; ++i)
      check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 1u, 1, NULL, 0u), TURBO_OK);
    check_size_eq(builder.field_count, TURBO_FLOW_TFMP_MAX_FIELDS);
    check_int_eq(turbo_flow_tfmp_body_validate(body, builder.length, &validated_count), TURBO_OK);
    check_size_eq(validated_count, TURBO_FLOW_TFMP_MAX_FIELDS);
    check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 1u, 1, NULL, 0u), TURBO_EMSGSIZE);
    check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 1u, 0, NULL, 0u), TURBO_EINVAL);

    body[builder.length] = 0x01u;
    body[builder.length + 1u] = 0x81u;
    check_int_eq(turbo_flow_tfmp_body_validate(body, builder.length + 2u, &validated_count),
                 TURBO_EMSGSIZE);
  }

  it("round trips typed scalar and UTF-8 fields in network byte order") {
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_field_iterator_t iterator = TURBO_FLOW_TFMP_FIELD_ITERATOR_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    uint8_t body[96];
    uint16_t u16 = 0u;
    uint32_t u32 = 0u;
    uint64_t u64 = 0u;
    int32_t i32 = 0;
    int boolean = 0;

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 1u, 1, 0x1234u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u32(&builder, 2u, 1, 0x89abcdefu), TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_body_builder_append_u64(&builder, 3u, 1, UINT64_C(0x0123456789abcdef)),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_i32(&builder, 4u, 1, -42), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_bool(&builder, 5u, 1, 1), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_utf8(&builder, 6u, 1, "node-a", 6u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_iterator_init(&iterator, body, builder.length), TURBO_OK);

    check_int_eq(turbo_flow_tfmp_field_iterator_next(&iterator, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &u16), TURBO_OK);
    check_uint_eq(u16, 0x1234u);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(turbo_flow_tfmp_field_iterator_next(&iterator, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u32(&field, &u32), TURBO_OK);
    check_uint_eq(u32, 0x89abcdefu);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(turbo_flow_tfmp_field_iterator_next(&iterator, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&field, &u64), TURBO_OK);
    check_hex64_eq(u64, UINT64_C(0x0123456789abcdef));
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(turbo_flow_tfmp_field_iterator_next(&iterator, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_i32(&field, &i32), TURBO_OK);
    check_int_eq(i32, -42);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(turbo_flow_tfmp_field_iterator_next(&iterator, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_bool(&field, &boolean), TURBO_OK);
    check_true(boolean);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(turbo_flow_tfmp_field_iterator_next(&iterator, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_validate_utf8(&field), TURBO_OK);
    check_mem_eq(field.value, "node-a", 6u);
  }

  it("rejects invalid BOOL and UTF-8 typed values") {
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    const uint8_t invalid_bool = 2u;
    const uint8_t invalid_utf8[] = {0xc0u, 0x80u};
    const char embedded_nul[] = {'a', '\0', 'b'};
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    uint8_t body[16];
    int value = 7;

    field.value = &invalid_bool;
    field.value_size = 1u;
    check_int_eq(turbo_flow_tfmp_field_read_bool(&field, &value), TURBO_EPROTO);
    check_int_eq(value, 7);
    field.value = invalid_utf8;
    field.value_size = sizeof(invalid_utf8);
    check_int_eq(turbo_flow_tfmp_field_validate_utf8(&field), TURBO_EPROTO);
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_bool(&builder, 1u, 1, 2), TURBO_EINVAL);
    check_int_eq(turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, embedded_nul,
                                                          sizeof(embedded_nul)),
                 TURBO_EINVAL);
  }

  it("validates required fields and forward-compatible optional fields") {
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_envelope_t envelope;
    uint8_t body[64];

    envelope = tfmp_request(TURBO_FLOW_TFMP_TARGET_GET, 1u, NULL, 0u);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_EPROTO);

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, "target-a", 8u),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 2u, 0, 9u), TURBO_OK);
    envelope = tfmp_request(TURBO_FLOW_TFMP_TARGET_GET, 1u, body, builder.length);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);

    body[builder.length - 3u] |= TURBO_FLOW_TFMP_FIELD_CRITICAL;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_ENOTSUP);

    builder = (turbo_flow_tfmp_body_builder_t)TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 0, "target-a", 8u),
                 TURBO_OK);
    envelope = tfmp_request(TURBO_FLOW_TFMP_TARGET_GET, 1u, body, builder.length);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_EPROTO);
  }

  it("requires common response identity and omits success fields on errors") {
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_envelope_t envelope;
    uint8_t body[128];

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 1u, 1, 2u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u64(&builder, 2u, 1, 50u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u64(&builder, 3u, 1, 7u), TURBO_OK);
    envelope =
        tfmp_response(TURBO_FLOW_TFMP_HEALTH_GET, TURBO_FLOW_TFMP_STATUS_OK, body, builder.length);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_EPROTO);
    check_int_eq(tfmp_append_response_identity(&builder), TURBO_OK);
    envelope.body_size = builder.length;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);

    builder = (turbo_flow_tfmp_body_builder_t)TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(tfmp_append_response_identity(&builder), TURBO_OK);
    envelope = tfmp_response(TURBO_FLOW_TFMP_HEALTH_GET, TURBO_FLOW_TFMP_STATUS_UNAVAILABLE, body,
                             builder.length);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);
  }

  it("validates repeated nested targets and rejects duplicate singular fields") {
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_envelope_t envelope;
    uint8_t target[96];
    uint8_t body[384];
    size_t target_size = 0u;

    check_int_eq(tfmp_build_target(target, sizeof(target), &target_size), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u64(&builder, 1u, 1, 7u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 2u, 0, target, target_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 2u, 0, target, target_size),
                 TURBO_OK);
    check_int_eq(tfmp_append_response_identity(&builder), TURBO_OK);
    envelope =
        tfmp_response(TURBO_FLOW_TFMP_TARGET_LIST, TURBO_FLOW_TFMP_STATUS_OK, body, builder.length);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);

    builder = (turbo_flow_tfmp_body_builder_t)TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u64(&builder, 1u, 1, 7u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u64(&builder, 1u, 1, 8u), TURBO_OK);
    check_int_eq(tfmp_append_response_identity(&builder), TURBO_OK);
    envelope.body_size = builder.length;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_EPROTO);
  }

  it("dispatches command payload schemas without fallback") {
    turbo_flow_tfmp_envelope_t envelope;
    uint8_t body[256];
    size_t body_size = 0u;

    check_int_eq(tfmp_build_pool_command(body, sizeof(body), TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE, 1,
                                         &body_size),
                 TURBO_OK);
    envelope = tfmp_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 1u, body, body_size);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);

    check_int_eq(tfmp_build_pool_command(body, sizeof(body), TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE, 0,
                                         &body_size),
                 TURBO_OK);
    envelope.body_size = body_size;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_EPROTO);

    check_int_eq(tfmp_build_pool_command(body, sizeof(body), 0x7777u, 1, &body_size), TURBO_OK);
    envelope.body_size = body_size;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_ENOTSUP);
  }

  it("validates the dedicated event kind and body registry") {
    static const uint8_t incarnation[16] = {2u};
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_envelope_t envelope;
    uint8_t body[160];

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, "authority-a", 11u),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_body_builder_append(&builder, 2u, 1, incarnation, sizeof(incarnation)),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 3u, 1, 1u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 4u, 1, 1u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u64(&builder, 9u, 1, 100u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 10u, 1, 1u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 11u, 1, 1u), TURBO_OK);
    envelope = tfmp_request(TURBO_FLOW_TFMP_EVENT, 1u, body, builder.length);
    envelope.flags = TURBO_FLOW_TFMP_FLAG_EVENT;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);
    envelope.kind = TURBO_FLOW_TFMP_HEALTH_GET;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_EPROTO);
  }

  it("routes every RPC kind through one frozen schema registry") {
    static const uint16_t kinds[] = {
        TURBO_FLOW_TFMP_CAPABILITIES_GET,
        TURBO_FLOW_TFMP_HEALTH_GET,
        TURBO_FLOW_TFMP_TARGET_LIST,
        TURBO_FLOW_TFMP_TARGET_GET,
        TURBO_FLOW_TFMP_RESOURCE_LIST,
        TURBO_FLOW_TFMP_RESOURCE_GET,
        TURBO_FLOW_TFMP_RESOURCE_DOCUMENT_GET,
        TURBO_FLOW_TFMP_COMMAND_SUBMIT,
        TURBO_FLOW_TFMP_OPERATION_GET,
        TURBO_FLOW_TFMP_OPERATION_CANCEL,
        TURBO_FLOW_TFMP_EVENTS_GET,
    };
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_envelope_t envelope;
    uint8_t body[64];
    size_t i;

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(tfmp_append_response_identity(&builder), TURBO_OK);
    for (i = 0u; i < sizeof(kinds) / sizeof(kinds[0]); ++i) {
      envelope = tfmp_response(kinds[i], TURBO_FLOW_TFMP_STATUS_UNAVAILABLE, body, builder.length);
      check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);
    }

    envelope = tfmp_request(TURBO_FLOW_TFMP_CAPABILITIES_GET, 1u, NULL, 0u);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);
    envelope.kind = TURBO_FLOW_TFMP_HEALTH_GET;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);
    envelope.kind = TURBO_FLOW_TFMP_RESOURCE_DOCUMENT_GET;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_EPROTO);
    envelope.kind = 0x7777u;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_ENOTSUP);
  }

  it("accepts read-only capabilities without command descriptors") {
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_envelope_t envelope;
    uint8_t body[192];

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 1u, 1, 0u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 2u, 1, 0u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append(&builder, 3u, 1, NULL, 0u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u32(&builder, 5u, 1, 65536u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u32(&builder, 6u, 1, 65536u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 7u, 1, 64u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 8u, 1, 4u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u32(&builder, 9u, 1, 256u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&builder, 10u, 1, 1u), TURBO_OK);
    check_int_eq(tfmp_append_response_identity(&builder), TURBO_OK);
    envelope = tfmp_response(TURBO_FLOW_TFMP_CAPABILITIES_GET, TURBO_FLOW_TFMP_STATUS_OK, body,
                             builder.length);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);
  }

  it("encodes response-only protocol errors with zero correlation") {
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_envelope_t envelope;
    turbo_flow_tfmp_envelope_t decoded = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    uint8_t body[96];
    uint8_t wire[160];
    size_t wire_size = 0u;

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(tfmp_append_response_identity(&builder), TURBO_OK);
    envelope = tfmp_response(TURBO_FLOW_TFMP_PROTOCOL_ERROR,
                             TURBO_FLOW_TFMP_STATUS_FAILED_PRECONDITION, body, builder.length);
    envelope.correlation_id = 0u;
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_encode(&envelope, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &decoded), TURBO_OK);
    check_int_eq(decoded.status, TURBO_FLOW_TFMP_STATUS_FAILED_PRECONDITION);
    envelope = tfmp_request(TURBO_FLOW_TFMP_PROTOCOL_ERROR, 1u, NULL, 0u);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&envelope), TURBO_ENOTSUP);
  }

  it("recovers only trustworthy malformed request identity") {
    turbo_flow_tfmp_envelope_t envelope = tfmp_request(TURBO_FLOW_TFMP_HEALTH_GET, 99u, NULL, 0u);
    uint8_t wire[64];
    size_t wire_size = 0u;
    uint16_t kind = 0u;
    uint64_t correlation_id = 0u;

    check_int_eq(turbo_flow_tfmp_envelope_encode(&envelope, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    wire[32] = 1u;
    turbo_flow_tfmp_envelope_peek_request_identity(wire, wire_size, &kind, &correlation_id);
    check_int_eq(kind, TURBO_FLOW_TFMP_HEALTH_GET);
    check_int_eq(correlation_id, 99u);
    turbo_flow_tfmp_envelope_peek_request_identity(wire, 3u, &kind, &correlation_id);
    check_int_eq(kind, TURBO_FLOW_TFMP_PROTOCOL_ERROR);
    check_int_eq(correlation_id, 0u);
  }

  it("maps process errors to stable protocol status") {
    check_int_eq(turbo_flow_tfmp_status_from_error(TURBO_OK), TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(turbo_flow_tfmp_status_from_error(TURBO_EPROTO),
                 TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT);
    check_int_eq(turbo_flow_tfmp_status_from_error(TURBO_ENOTSUP),
                 TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY);
    check_int_eq(turbo_flow_tfmp_status_from_error(TURBO_ENOENT), TURBO_FLOW_TFMP_STATUS_NOT_FOUND);
    check_int_eq(turbo_flow_tfmp_status_from_error(TURBO_EALREADY),
                 TURBO_FLOW_TFMP_STATUS_CONFLICT);
    check_int_eq(turbo_flow_tfmp_status_from_error(TURBO_ETIMEDOUT),
                 TURBO_FLOW_TFMP_STATUS_DEADLINE_EXCEEDED);
    check_int_eq(turbo_flow_tfmp_status_from_error(TURBO_ENOSPC),
                 TURBO_FLOW_TFMP_STATUS_RESOURCE_EXHAUSTED);
    check_int_eq(turbo_flow_tfmp_status_from_error(TURBO_ECANCELED),
                 TURBO_FLOW_TFMP_STATUS_CANCELED);
    check_int_eq(turbo_flow_tfmp_status_from_error(TURBO_ESHUTDOWN),
                 TURBO_FLOW_TFMP_STATUS_UNAVAILABLE);
  }
}
