#include "turbo_flow_fmq_management_protocol.h"

#include "turbo_error.h"

#include <stddef.h>

typedef enum flow_tfmp_value_type_e {
  FLOW_TFMP_VALUE_U16,
  FLOW_TFMP_VALUE_U32,
  FLOW_TFMP_VALUE_U64,
  FLOW_TFMP_VALUE_I32,
  FLOW_TFMP_VALUE_BOOL,
  FLOW_TFMP_VALUE_UTF8,
  FLOW_TFMP_VALUE_BYTES,
  FLOW_TFMP_VALUE_BYTES16,
  FLOW_TFMP_VALUE_NESTED,
  FLOW_TFMP_VALUE_NESTED_COMMAND,
  FLOW_TFMP_VALUE_PACKED_U16
} flow_tfmp_value_type_t;

typedef struct flow_tfmp_schema_s flow_tfmp_schema_t;

typedef struct flow_tfmp_field_descriptor_s {
  uint8_t id;
  uint8_t required;
  uint8_t repeated;
  flow_tfmp_value_type_t type;
  const flow_tfmp_schema_t *nested;
} flow_tfmp_field_descriptor_t;

struct flow_tfmp_schema_s {
  const flow_tfmp_field_descriptor_t *fields;
  size_t field_count;
};

#define FLOW_TFMP_FIELD(ID, REQUIRED, REPEATED, TYPE, NESTED)                                      \
  {(ID), (REQUIRED), (REPEATED), (TYPE), (NESTED)}
#define FLOW_TFMP_SCHEMA(FIELDS) {(FIELDS), sizeof(FIELDS) / sizeof((FIELDS)[0])}
#define FLOW_TFMP_EMPTY_SCHEMA {NULL, 0u}

static const flow_tfmp_field_descriptor_t FLOW_TFMP_COMMON_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(120u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(121u, 1u, 0u, FLOW_TFMP_VALUE_BYTES16, NULL),
    FLOW_TFMP_FIELD(122u, 0u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(123u, 0u, 0u, FLOW_TFMP_VALUE_I32, NULL),
    FLOW_TFMP_FIELD(124u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_COMMON_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_COMMON_RESPONSE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_CONDITION_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_BOOL, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(4u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(5u, 0u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_CONDITION_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_CONDITION_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_TARGET_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(4u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(5u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_TARGET_SCHEMA = FLOW_TFMP_SCHEMA(FLOW_TFMP_TARGET_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_RESOURCE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(4u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(5u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(6u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(7u, 0u, 1u, FLOW_TFMP_VALUE_NESTED, &FLOW_TFMP_CONDITION_SCHEMA),
};
static const flow_tfmp_schema_t FLOW_TFMP_RESOURCE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_RESOURCE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_COMMAND_DESCRIPTOR_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(4u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(5u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_COMMAND_DESCRIPTOR_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_COMMAND_DESCRIPTOR_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_EVENT_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_BYTES16, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(4u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(5u, 0u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(6u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(7u, 0u, 0u, FLOW_TFMP_VALUE_BYTES16, NULL),
    FLOW_TFMP_FIELD(8u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(9u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(10u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(11u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(12u, 0u, 0u, FLOW_TFMP_VALUE_BYTES, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_EVENT_SCHEMA = FLOW_TFMP_SCHEMA(FLOW_TFMP_EVENT_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_ENDPOINT_PAYLOAD_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 0u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(2u, 0u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(3u, 0u, 0u, FLOW_TFMP_VALUE_U16, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_ENDPOINT_PAYLOAD_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_ENDPOINT_PAYLOAD_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_POOL_PAYLOAD_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U32, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_POOL_PAYLOAD_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_POOL_PAYLOAD_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_CAPABILITIES_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_PACKED_U16, NULL),
    FLOW_TFMP_FIELD(4u, 0u, 1u, FLOW_TFMP_VALUE_NESTED, &FLOW_TFMP_COMMAND_DESCRIPTOR_SCHEMA),
    FLOW_TFMP_FIELD(5u, 1u, 0u, FLOW_TFMP_VALUE_U32, NULL),
    FLOW_TFMP_FIELD(6u, 1u, 0u, FLOW_TFMP_VALUE_U32, NULL),
    FLOW_TFMP_FIELD(7u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(8u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(9u, 1u, 0u, FLOW_TFMP_VALUE_U32, NULL),
    FLOW_TFMP_FIELD(10u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(11u, 0u, 0u, FLOW_TFMP_VALUE_U32, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_CAPABILITIES_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_CAPABILITIES_RESPONSE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_HEALTH_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_HEALTH_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_HEALTH_RESPONSE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_TARGET_LIST_REQUEST_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_U32, NULL),
    FLOW_TFMP_FIELD(2u, 0u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(3u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_TARGET_LIST_REQUEST_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_TARGET_LIST_REQUEST_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_TARGET_LIST_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(2u, 0u, 1u, FLOW_TFMP_VALUE_NESTED, &FLOW_TFMP_TARGET_SCHEMA),
    FLOW_TFMP_FIELD(3u, 0u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_TARGET_LIST_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_TARGET_LIST_RESPONSE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_TARGET_GET_REQUEST_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_TARGET_GET_REQUEST_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_TARGET_GET_REQUEST_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_TARGET_GET_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_NESTED, &FLOW_TFMP_TARGET_SCHEMA),
};
static const flow_tfmp_schema_t FLOW_TFMP_TARGET_GET_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_TARGET_GET_RESPONSE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_RESOURCE_LIST_REQUEST_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U32, NULL),
    FLOW_TFMP_FIELD(3u, 0u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(4u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_RESOURCE_LIST_REQUEST_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_RESOURCE_LIST_REQUEST_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_RESOURCE_LIST_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(2u, 0u, 1u, FLOW_TFMP_VALUE_NESTED, &FLOW_TFMP_RESOURCE_SCHEMA),
    FLOW_TFMP_FIELD(3u, 0u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_RESOURCE_LIST_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_RESOURCE_LIST_RESPONSE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_RESOURCE_GET_REQUEST_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_RESOURCE_GET_REQUEST_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_RESOURCE_GET_REQUEST_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_RESOURCE_GET_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_NESTED, &FLOW_TFMP_RESOURCE_SCHEMA),
};
static const flow_tfmp_schema_t FLOW_TFMP_RESOURCE_GET_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_RESOURCE_GET_RESPONSE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_RESOURCE_DOCUMENT_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_NESTED, &FLOW_TFMP_RESOURCE_SCHEMA),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(4u, 1u, 0u, FLOW_TFMP_VALUE_BYTES, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_RESOURCE_DOCUMENT_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_RESOURCE_DOCUMENT_RESPONSE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_COMMAND_REQUEST_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(4u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(5u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(6u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(7u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(8u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(9u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(10u, 0u, 0u, FLOW_TFMP_VALUE_NESTED_COMMAND, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_COMMAND_REQUEST_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_COMMAND_REQUEST_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_COMMAND_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(2u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(3u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(4u, 0u, 0u, FLOW_TFMP_VALUE_BYTES16, NULL),
    FLOW_TFMP_FIELD(5u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_COMMAND_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_COMMAND_RESPONSE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_OPERATION_GET_REQUEST_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_BYTES16, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_OPERATION_GET_REQUEST_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_OPERATION_GET_REQUEST_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_OPERATION_CANCEL_REQUEST_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_BYTES16, NULL),
    FLOW_TFMP_FIELD(4u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_OPERATION_CANCEL_REQUEST_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_OPERATION_CANCEL_REQUEST_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_OPERATION_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_BYTES16, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U16, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(4u, 1u, 0u, FLOW_TFMP_VALUE_UTF8, NULL),
    FLOW_TFMP_FIELD(5u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(6u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(7u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(8u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(9u, 0u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(10u, 0u, 0u, FLOW_TFMP_VALUE_U16, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_OPERATION_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_OPERATION_RESPONSE_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_EVENTS_GET_REQUEST_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 1u, 0u, FLOW_TFMP_VALUE_BYTES16, NULL),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
    FLOW_TFMP_FIELD(3u, 1u, 0u, FLOW_TFMP_VALUE_U32, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_EVENTS_GET_REQUEST_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_EVENTS_GET_REQUEST_FIELDS);

static const flow_tfmp_field_descriptor_t FLOW_TFMP_EVENTS_GET_RESPONSE_FIELDS[] = {
    FLOW_TFMP_FIELD(1u, 0u, 1u, FLOW_TFMP_VALUE_NESTED, &FLOW_TFMP_EVENT_SCHEMA),
    FLOW_TFMP_FIELD(2u, 1u, 0u, FLOW_TFMP_VALUE_U64, NULL),
};
static const flow_tfmp_schema_t FLOW_TFMP_EVENTS_GET_RESPONSE_SCHEMA =
    FLOW_TFMP_SCHEMA(FLOW_TFMP_EVENTS_GET_RESPONSE_FIELDS);

static const flow_tfmp_schema_t FLOW_TFMP_EMPTY = FLOW_TFMP_EMPTY_SCHEMA;

static const flow_tfmp_field_descriptor_t *flow_tfmp_schema_find(const flow_tfmp_schema_t *schema,
                                                                 uint8_t id, size_t *index) {
  size_t i;
  if (!schema) return NULL;
  for (i = 0u; i < schema->field_count; ++i) {
    if (schema->fields[i].id == id) {
      if (index) *index = i;
      return &schema->fields[i];
    }
  }
  return NULL;
}

static int flow_tfmp_validate_body_schema(const uint8_t *body, size_t body_size,
                                          const flow_tfmp_schema_t *schema,
                                          const flow_tfmp_schema_t *additional, int require_schema,
                                          int require_additional, unsigned int depth);

static int flow_tfmp_validate_field_value(const turbo_flow_tfmp_field_t *field,
                                          const flow_tfmp_field_descriptor_t *descriptor,
                                          unsigned int depth) {
  uint16_t u16;
  uint32_t u32;
  uint64_t u64;
  int32_t i32;
  int boolean;
  switch (descriptor->type) {
  case FLOW_TFMP_VALUE_U16:
    return turbo_flow_tfmp_field_read_u16(field, &u16);
  case FLOW_TFMP_VALUE_U32:
    return turbo_flow_tfmp_field_read_u32(field, &u32);
  case FLOW_TFMP_VALUE_U64:
    return turbo_flow_tfmp_field_read_u64(field, &u64);
  case FLOW_TFMP_VALUE_I32:
    return turbo_flow_tfmp_field_read_i32(field, &i32);
  case FLOW_TFMP_VALUE_BOOL:
    return turbo_flow_tfmp_field_read_bool(field, &boolean);
  case FLOW_TFMP_VALUE_UTF8:
    return turbo_flow_tfmp_field_validate_utf8(field);
  case FLOW_TFMP_VALUE_BYTES:
    return TURBO_OK;
  case FLOW_TFMP_VALUE_BYTES16:
    return field->value_size == 16u ? TURBO_OK : TURBO_EPROTO;
  case FLOW_TFMP_VALUE_PACKED_U16:
    return (field->value_size % 2u) == 0u ? TURBO_OK : TURBO_EPROTO;
  case FLOW_TFMP_VALUE_NESTED_COMMAND:
    if (depth >= TURBO_FLOW_TFMP_MAX_NESTING) return TURBO_EMSGSIZE;
    return turbo_flow_tfmp_body_validate(field->value, field->value_size, NULL);
  case FLOW_TFMP_VALUE_NESTED:
    if (depth >= TURBO_FLOW_TFMP_MAX_NESTING) return TURBO_EMSGSIZE;
    return flow_tfmp_validate_body_schema(field->value, field->value_size, descriptor->nested, NULL,
                                          1, 0, depth + 1u);
  default:
    return TURBO_EPROTO;
  }
}

static int flow_tfmp_validate_required(const flow_tfmp_schema_t *schema, uint64_t seen,
                                       int required) {
  size_t i;
  if (!schema || !required) return TURBO_OK;
  if (schema->field_count > 64u) return TURBO_EMSGSIZE;
  for (i = 0u; i < schema->field_count; ++i) {
    if (schema->fields[i].required && (seen & (UINT64_C(1) << i)) == 0u) return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flow_tfmp_validate_body_schema(const uint8_t *body, size_t body_size,
                                          const flow_tfmp_schema_t *schema,
                                          const flow_tfmp_schema_t *additional, int require_schema,
                                          int require_additional, unsigned int depth) {
  turbo_flow_tfmp_field_iterator_t iterator = TURBO_FLOW_TFMP_FIELD_ITERATOR_INIT;
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  uint64_t seen = 0u;
  uint64_t seen_additional = 0u;
  uint8_t previous_known_id = 0u;
  const flow_tfmp_field_descriptor_t *descriptor;
  size_t descriptor_index;
  int rc;

  if (depth == 0u || depth > TURBO_FLOW_TFMP_MAX_NESTING) return TURBO_EMSGSIZE;
  rc = turbo_flow_tfmp_field_iterator_init(&iterator, body, body_size);
  if (rc != TURBO_OK) return rc;
  for (;;) {
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    rc = turbo_flow_tfmp_field_iterator_next(&iterator, &field);
    if (rc == TURBO_ENOENT) break;
    if (rc != TURBO_OK) return rc;

    descriptor_index = 0u;
    descriptor = flow_tfmp_schema_find(schema, field.id, &descriptor_index);
    if (descriptor) {
      seen |= UINT64_C(1) << descriptor_index;
    } else {
      descriptor = flow_tfmp_schema_find(additional, field.id, &descriptor_index);
      if (descriptor) seen_additional |= UINT64_C(1) << descriptor_index;
    }
    if (!descriptor) {
      if (field.critical) return TURBO_ENOTSUP;
      continue;
    }
    if ((field.critical != 0) != (descriptor->required != 0)) return TURBO_EPROTO;
    if (field.id == previous_known_id && !descriptor->repeated) return TURBO_EPROTO;
    rc = flow_tfmp_validate_field_value(&field, descriptor, depth);
    if (rc != TURBO_OK) return rc;
    previous_known_id = field.id;
  }

  rc = flow_tfmp_validate_required(schema, seen, require_schema);
  if (rc != TURBO_OK) return rc;
  return flow_tfmp_validate_required(additional, seen_additional, require_additional);
}

static int flow_tfmp_select_schema(uint16_t kind, int response, const flow_tfmp_schema_t **schema) {
  if (!schema) return TURBO_EINVAL;
  switch (kind) {
  case TURBO_FLOW_TFMP_PROTOCOL_ERROR:
    if (!response) return TURBO_ENOTSUP;
    *schema = &FLOW_TFMP_EMPTY;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_CAPABILITIES_GET:
    *schema = response ? &FLOW_TFMP_CAPABILITIES_RESPONSE_SCHEMA : &FLOW_TFMP_EMPTY;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_HEALTH_GET:
    *schema = response ? &FLOW_TFMP_HEALTH_RESPONSE_SCHEMA : &FLOW_TFMP_EMPTY;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_TARGET_LIST:
    *schema =
        response ? &FLOW_TFMP_TARGET_LIST_RESPONSE_SCHEMA : &FLOW_TFMP_TARGET_LIST_REQUEST_SCHEMA;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_TARGET_GET:
    *schema =
        response ? &FLOW_TFMP_TARGET_GET_RESPONSE_SCHEMA : &FLOW_TFMP_TARGET_GET_REQUEST_SCHEMA;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_RESOURCE_LIST:
    *schema = response ? &FLOW_TFMP_RESOURCE_LIST_RESPONSE_SCHEMA
                       : &FLOW_TFMP_RESOURCE_LIST_REQUEST_SCHEMA;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_RESOURCE_GET:
    *schema =
        response ? &FLOW_TFMP_RESOURCE_GET_RESPONSE_SCHEMA : &FLOW_TFMP_RESOURCE_GET_REQUEST_SCHEMA;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_RESOURCE_DOCUMENT_GET:
    *schema = response ? &FLOW_TFMP_RESOURCE_DOCUMENT_RESPONSE_SCHEMA
                       : &FLOW_TFMP_RESOURCE_GET_REQUEST_SCHEMA;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_COMMAND_SUBMIT:
    *schema = response ? &FLOW_TFMP_COMMAND_RESPONSE_SCHEMA : &FLOW_TFMP_COMMAND_REQUEST_SCHEMA;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_OPERATION_GET:
    *schema =
        response ? &FLOW_TFMP_OPERATION_RESPONSE_SCHEMA : &FLOW_TFMP_OPERATION_GET_REQUEST_SCHEMA;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_OPERATION_CANCEL:
    *schema = response ? &FLOW_TFMP_OPERATION_RESPONSE_SCHEMA
                       : &FLOW_TFMP_OPERATION_CANCEL_REQUEST_SCHEMA;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_EVENTS_GET:
    *schema =
        response ? &FLOW_TFMP_EVENTS_GET_RESPONSE_SCHEMA : &FLOW_TFMP_EVENTS_GET_REQUEST_SCHEMA;
    return TURBO_OK;
  default:
    return TURBO_ENOTSUP;
  }
}

static int flow_tfmp_validate_command_payload(const turbo_flow_tfmp_envelope_t *envelope) {
  turbo_flow_tfmp_field_iterator_t iterator = TURBO_FLOW_TFMP_FIELD_ITERATOR_INIT;
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  const uint8_t *payload = NULL;
  size_t payload_size = 0u;
  uint16_t command_type = 0u;
  int rc;

  rc = turbo_flow_tfmp_field_iterator_init(&iterator, envelope->body, envelope->body_size);
  if (rc != TURBO_OK) return rc;
  while ((rc = turbo_flow_tfmp_field_iterator_next(&iterator, &field)) == TURBO_OK) {
    if (field.id == 4u) {
      rc = turbo_flow_tfmp_field_read_u16(&field, &command_type);
      if (rc != TURBO_OK) return rc;
    } else if (field.id == 10u) {
      payload = field.value;
      payload_size = field.value_size;
    }
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  }
  if (rc != TURBO_ENOENT) return rc;

  switch (command_type) {
  case TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE:
  case TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME:
  case TURBO_FLOW_TFMP_COMMAND_FLOW_DRAIN:
  case TURBO_FLOW_TFMP_COMMAND_RESOURCE_QUIESCE:
  case TURBO_FLOW_TFMP_COMMAND_RESOURCE_RESUME:
    return payload ? TURBO_EPROTO : TURBO_OK;
  case TURBO_FLOW_TFMP_COMMAND_ENDPOINT_REPLACE:
    if (!payload) return TURBO_EPROTO;
    return flow_tfmp_validate_body_schema(payload, payload_size, &FLOW_TFMP_ENDPOINT_PAYLOAD_SCHEMA,
                                          NULL, 1, 0, 2u);
  case TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE:
    if (!payload) return TURBO_EPROTO;
    return flow_tfmp_validate_body_schema(payload, payload_size, &FLOW_TFMP_POOL_PAYLOAD_SCHEMA,
                                          NULL, 1, 0, 2u);
  default:
    return TURBO_ENOTSUP;
  }
}

int turbo_flow_tfmp_envelope_validate_schema(const turbo_flow_tfmp_envelope_t *envelope) {
  const flow_tfmp_schema_t *schema = NULL;
  int response;
  int event;
  int rc;

  if (!envelope || envelope->size < sizeof(*envelope) ||
      envelope->major != TURBO_FLOW_TFMP_PROTOCOL_MAJOR ||
      (!envelope->body && envelope->body_size > 0u))
    return TURBO_EINVAL;
  response = (envelope->flags & TURBO_FLOW_TFMP_FLAG_RESPONSE) != 0u;
  event = (envelope->flags & TURBO_FLOW_TFMP_FLAG_EVENT) != 0u;
  if (event) {
    if (response || envelope->kind != TURBO_FLOW_TFMP_EVENT) return TURBO_EPROTO;
    return flow_tfmp_validate_body_schema(envelope->body, envelope->body_size,
                                          &FLOW_TFMP_EVENT_SCHEMA, NULL, 1, 0, 1u);
  }
  if (envelope->kind == TURBO_FLOW_TFMP_EVENT) return TURBO_EPROTO;

  rc = flow_tfmp_select_schema(envelope->kind, response, &schema);
  if (rc != TURBO_OK) {
    if (!response || envelope->status == TURBO_FLOW_TFMP_STATUS_OK) return rc;
    schema = &FLOW_TFMP_EMPTY;
  }
  rc = flow_tfmp_validate_body_schema(envelope->body, envelope->body_size, schema,
                                      response ? &FLOW_TFMP_COMMON_RESPONSE_SCHEMA : NULL,
                                      !response || envelope->status == TURBO_FLOW_TFMP_STATUS_OK,
                                      response, 1u);
  if (rc != TURBO_OK) return rc;
  if (!response && envelope->kind == TURBO_FLOW_TFMP_COMMAND_SUBMIT)
    return flow_tfmp_validate_command_payload(envelope);
  return TURBO_OK;
}
