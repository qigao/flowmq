#include "flowmq_esb.h"

#include "salts_error.h"

#include <limits.h>
#include <string.h>

enum flowmq_esb_field_mask_e {
  FLOWMQ_ESB_HAS_EXPECTED = 1u << 0,
  FLOWMQ_ESB_HAS_POLICY = 1u << 1,
  FLOWMQ_ESB_HAS_PARTIAL = 1u << 2,
  FLOWMQ_ESB_HAS_SAGA_ID = 1u << 3,
  FLOWMQ_ESB_HAS_SAGA_STEP = 1u << 4,
  FLOWMQ_ESB_HAS_SAGA_STATE = 1u << 5,
  FLOWMQ_ESB_HAS_PARTITION = 1u << 6,
  FLOWMQ_ESB_HAS_OFFSET = 1u << 7,
  FLOWMQ_ESB_HAS_GROUP = 1u << 8,
  FLOWMQ_ESB_HAS_PRIORITY = 1u << 9,
  FLOWMQ_ESB_HAS_CIRCUIT_STATE = 1u << 10,
  FLOWMQ_ESB_HAS_FAILURE_COUNT = 1u << 11
};

typedef char flowmq_esb_single_digit_wire_version[FLOWMQ_PROTOCOL_FES_VERSION <= 9u ? 1 : -1];

static const unsigned char FLOWMQ_ESB_MAGIC[4] = {
    'F', 'E', 'S', (unsigned char)('0' + FLOWMQ_PROTOCOL_FES_VERSION)};

static void flowmq_esb_write_u32(unsigned char *out, uint32_t value) {
  out[0] = (unsigned char)(value >> 24);
  out[1] = (unsigned char)(value >> 16);
  out[2] = (unsigned char)(value >> 8);
  out[3] = (unsigned char)value;
}

static void flowmq_esb_write_u64(unsigned char *out, uint64_t value) {
  unsigned int index;
  for (index = 0u; index < 8u; ++index) {
    out[index] = (unsigned char)(value >> (56u - index * 8u));
  }
}

static uint32_t flowmq_esb_read_u32(const unsigned char *data) {
  return ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) | ((uint32_t)data[2] << 8) | data[3];
}

static uint64_t flowmq_esb_read_u64(const unsigned char *data) {
  uint64_t value = 0u;
  unsigned int index;
  for (index = 0u; index < 8u; ++index)
    value = (value << 8u) | data[index];
  return value;
}

static uint32_t flowmq_esb_present_fields(const flowmq_esb_message_t *message) {
  uint32_t fields = 0u;
  if (message->expected_responses != 0u) fields |= FLOWMQ_ESB_HAS_EXPECTED;
  if (message->aggregation_policy != FLOWMQ_ESB_GATHER_ALL) fields |= FLOWMQ_ESB_HAS_POLICY;
  if (message->partial_index != 0u) fields |= FLOWMQ_ESB_HAS_PARTIAL;
  if (message->saga_id != 0u) fields |= FLOWMQ_ESB_HAS_SAGA_ID;
  if (message->saga_step != 0u) fields |= FLOWMQ_ESB_HAS_SAGA_STEP;
  if (message->saga_state != FLOWMQ_ESB_SAGA_PENDING) fields |= FLOWMQ_ESB_HAS_SAGA_STATE;
  if (message->partition_id != 0u) fields |= FLOWMQ_ESB_HAS_PARTITION;
  if (message->offset != 0u) fields |= FLOWMQ_ESB_HAS_OFFSET;
  if (message->consumer_group.len != 0u) fields |= FLOWMQ_ESB_HAS_GROUP;
  if (message->priority != 0u) fields |= FLOWMQ_ESB_HAS_PRIORITY;
  if (message->circuit_state != FLOWMQ_ESB_CIRCUIT_CLOSED) fields |= FLOWMQ_ESB_HAS_CIRCUIT_STATE;
  if (message->failure_count != 0u) fields |= FLOWMQ_ESB_HAS_FAILURE_COUNT;
  return fields;
}

static int flowmq_esb_metadata_shape(const flowmq_esb_message_t *message, uint32_t *allowed_fields,
                                     size_t *metadata_size) {
  const size_t u8_field = FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint8_t);
  const size_t u32_field = FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint32_t);
  const size_t u64_field = FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint64_t);
  if (!message || !allowed_fields || !metadata_size) return SALTS_EINVAL;
  *allowed_fields = 0u;
  *metadata_size = 0u;
  switch (message->kind) {
  case FLOWMQ_ESB_SCATTER_REQUEST:
    *allowed_fields = FLOWMQ_ESB_HAS_EXPECTED | FLOWMQ_ESB_HAS_POLICY;
    *metadata_size = u32_field + u8_field;
    break;
  case FLOWMQ_ESB_GATHER_RESPONSE:
    break;
  case FLOWMQ_ESB_PARTIAL_RESPONSE:
    *allowed_fields = FLOWMQ_ESB_HAS_PARTIAL;
    *metadata_size = u32_field;
    break;
  case FLOWMQ_ESB_SAGA_EXECUTE:
  case FLOWMQ_ESB_SAGA_COMMIT:
  case FLOWMQ_ESB_SAGA_COMPENSATE:
  case FLOWMQ_ESB_SAGA_ABORT:
    *allowed_fields = FLOWMQ_ESB_HAS_SAGA_ID | FLOWMQ_ESB_HAS_SAGA_STEP | FLOWMQ_ESB_HAS_SAGA_STATE;
    *metadata_size = u64_field + u32_field + u8_field;
    break;
  case FLOWMQ_ESB_STREAM_PUBLISH:
    *allowed_fields = FLOWMQ_ESB_HAS_PARTITION | FLOWMQ_ESB_HAS_OFFSET;
    *metadata_size = u32_field + u64_field;
    break;
  case FLOWMQ_ESB_STREAM_SUBSCRIBE:
  case FLOWMQ_ESB_STREAM_REBALANCE:
    *allowed_fields = FLOWMQ_ESB_HAS_GROUP;
    *metadata_size = FLOWMQ_ESB_TLV_HEADER_SIZE + message->consumer_group.len;
    break;
  case FLOWMQ_ESB_STREAM_COMMIT:
    *allowed_fields = FLOWMQ_ESB_HAS_PARTITION | FLOWMQ_ESB_HAS_OFFSET | FLOWMQ_ESB_HAS_GROUP;
    *metadata_size =
        u32_field + u64_field + FLOWMQ_ESB_TLV_HEADER_SIZE + message->consumer_group.len;
    break;
  case FLOWMQ_ESB_PRIORITY_PUBLISH:
    *allowed_fields = FLOWMQ_ESB_HAS_PRIORITY;
    *metadata_size = u8_field;
    break;
  case FLOWMQ_ESB_CIRCUIT_STATUS:
    *allowed_fields = FLOWMQ_ESB_HAS_CIRCUIT_STATE | FLOWMQ_ESB_HAS_FAILURE_COUNT;
    *metadata_size = u8_field + u32_field;
    break;
  default:
    return SALTS_EINVAL;
  }
  return SALTS_OK;
}

static int flowmq_esb_validate(const flowmq_esb_message_t *message, size_t *metadata_size) {
  uint32_t allowed_fields;
  uint32_t present_fields;
  int rc;
  if (!message || !metadata_size) return SALTS_EINVAL;
  if ((message->consumer_group.len != 0u && !message->consumer_group.data) ||
      (message->payload.len != 0u && !message->payload.data)) {
    return SALTS_EINVAL;
  }
  if (message->consumer_group.len > FLOWMQ_ESB_MAX_CONSUMER_GROUP_SIZE ||
      message->payload.len > UINT32_MAX) {
    return SALTS_EMSGSIZE;
  }
  if (message->expected_responses > FLOWMQ_ESB_MAX_FANOUT_COUNT ||
      message->saga_step >= FLOWMQ_ESB_MAX_SAGA_STEPS ||
      message->partition_id >= FLOWMQ_ESB_MAX_STREAM_PARTITIONS) {
    return SALTS_EINVAL;
  }
  if (message->aggregation_policy > FLOWMQ_ESB_GATHER_QUORUM ||
      message->saga_state > FLOWMQ_ESB_SAGA_PARTIALLY_COMPENSATED ||
      message->circuit_state > FLOWMQ_ESB_CIRCUIT_HALF_OPEN) {
    return SALTS_EINVAL;
  }
  rc = flowmq_esb_metadata_shape(message, &allowed_fields, metadata_size);
  if (rc != SALTS_OK) return rc;
  present_fields = flowmq_esb_present_fields(message);
  if ((present_fields & ~allowed_fields) != 0u) return SALTS_EPROTO;
  if (message->kind == FLOWMQ_ESB_SCATTER_REQUEST && message->expected_responses == 0u) {
    return SALTS_EPROTO;
  }
  if ((message->kind == FLOWMQ_ESB_SAGA_EXECUTE || message->kind == FLOWMQ_ESB_SAGA_COMMIT ||
       message->kind == FLOWMQ_ESB_SAGA_COMPENSATE || message->kind == FLOWMQ_ESB_SAGA_ABORT) &&
      message->saga_id == 0u) {
    return SALTS_EPROTO;
  }
  if ((message->kind == FLOWMQ_ESB_STREAM_SUBSCRIBE || message->kind == FLOWMQ_ESB_STREAM_COMMIT ||
       message->kind == FLOWMQ_ESB_STREAM_REBALANCE) &&
      message->consumer_group.len == 0u) {
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

int flowmq_esb_encoded_size(const flowmq_esb_message_t *message, size_t max_message_size,
                            size_t *encoded_size) {
  size_t metadata_size;
  size_t total;
  int rc;
  if (!encoded_size || max_message_size == 0u) return SALTS_EINVAL;
  *encoded_size = 0u;
  rc = flowmq_esb_validate(message, &metadata_size);
  if (rc != SALTS_OK) return rc;
  if (FLOWMQ_ESB_HEADER_SIZE > SIZE_MAX - metadata_size ||
      FLOWMQ_ESB_HEADER_SIZE + metadata_size > SIZE_MAX - message->payload.len) {
    return SALTS_ERANGE;
  }
  total = FLOWMQ_ESB_HEADER_SIZE + metadata_size + message->payload.len;
  if (total > max_message_size) return SALTS_EMSGSIZE;
  if (metadata_size > UINT32_MAX) return SALTS_ERANGE;
  *encoded_size = total;
  return SALTS_OK;
}

static void flowmq_esb_encode_field_header(unsigned char **cursor, flowmq_esb_field_t field,
                                           uint32_t value_size) {
  (*cursor)[0] = (unsigned char)field;
  flowmq_esb_write_u32(*cursor + 1u, value_size);
  *cursor += FLOWMQ_ESB_TLV_HEADER_SIZE;
}

static void flowmq_esb_encode_u8(unsigned char **cursor, flowmq_esb_field_t field, uint8_t value) {
  flowmq_esb_encode_field_header(cursor, field, sizeof(value));
  **cursor = value;
  *cursor += sizeof(value);
}

static void flowmq_esb_encode_u32(unsigned char **cursor, flowmq_esb_field_t field,
                                  uint32_t value) {
  flowmq_esb_encode_field_header(cursor, field, sizeof(value));
  flowmq_esb_write_u32(*cursor, value);
  *cursor += sizeof(value);
}

static void flowmq_esb_encode_u64(unsigned char **cursor, flowmq_esb_field_t field,
                                  uint64_t value) {
  flowmq_esb_encode_field_header(cursor, field, sizeof(value));
  flowmq_esb_write_u64(*cursor, value);
  *cursor += sizeof(value);
}

static void flowmq_esb_encode_bytes(unsigned char **cursor, flowmq_esb_field_t field, vstr value) {
  flowmq_esb_encode_field_header(cursor, field, (uint32_t)value.len);
  if (value.len != 0u) memcpy(*cursor, value.data, value.len);
  *cursor += value.len;
}

static void flowmq_esb_encode_metadata(const flowmq_esb_message_t *message,
                                       unsigned char **cursor) {
  switch (message->kind) {
  case FLOWMQ_ESB_SCATTER_REQUEST:
    flowmq_esb_encode_u32(cursor, FLOWMQ_ESB_FIELD_EXPECTED_RESPONSES, message->expected_responses);
    flowmq_esb_encode_u8(cursor, FLOWMQ_ESB_FIELD_AGGREGATION_POLICY,
                         (uint8_t)message->aggregation_policy);
    break;
  case FLOWMQ_ESB_PARTIAL_RESPONSE:
    flowmq_esb_encode_u32(cursor, FLOWMQ_ESB_FIELD_PARTIAL_INDEX, message->partial_index);
    break;
  case FLOWMQ_ESB_SAGA_EXECUTE:
  case FLOWMQ_ESB_SAGA_COMMIT:
  case FLOWMQ_ESB_SAGA_COMPENSATE:
  case FLOWMQ_ESB_SAGA_ABORT:
    flowmq_esb_encode_u64(cursor, FLOWMQ_ESB_FIELD_SAGA_ID, message->saga_id);
    flowmq_esb_encode_u32(cursor, FLOWMQ_ESB_FIELD_SAGA_STEP, message->saga_step);
    flowmq_esb_encode_u8(cursor, FLOWMQ_ESB_FIELD_SAGA_STATE, (uint8_t)message->saga_state);
    break;
  case FLOWMQ_ESB_STREAM_PUBLISH:
    flowmq_esb_encode_u32(cursor, FLOWMQ_ESB_FIELD_PARTITION_ID, message->partition_id);
    flowmq_esb_encode_u64(cursor, FLOWMQ_ESB_FIELD_OFFSET, message->offset);
    break;
  case FLOWMQ_ESB_STREAM_SUBSCRIBE:
  case FLOWMQ_ESB_STREAM_REBALANCE:
    flowmq_esb_encode_bytes(cursor, FLOWMQ_ESB_FIELD_CONSUMER_GROUP, message->consumer_group);
    break;
  case FLOWMQ_ESB_STREAM_COMMIT:
    flowmq_esb_encode_u32(cursor, FLOWMQ_ESB_FIELD_PARTITION_ID, message->partition_id);
    flowmq_esb_encode_u64(cursor, FLOWMQ_ESB_FIELD_OFFSET, message->offset);
    flowmq_esb_encode_bytes(cursor, FLOWMQ_ESB_FIELD_CONSUMER_GROUP, message->consumer_group);
    break;
  case FLOWMQ_ESB_PRIORITY_PUBLISH:
    flowmq_esb_encode_u8(cursor, FLOWMQ_ESB_FIELD_PRIORITY, message->priority);
    break;
  case FLOWMQ_ESB_CIRCUIT_STATUS:
    flowmq_esb_encode_u8(cursor, FLOWMQ_ESB_FIELD_CIRCUIT_STATE, (uint8_t)message->circuit_state);
    flowmq_esb_encode_u32(cursor, FLOWMQ_ESB_FIELD_FAILURE_COUNT, message->failure_count);
    break;
  case FLOWMQ_ESB_GATHER_RESPONSE:
    break;
  default:
    break;
  }
}

int flowmq_esb_encode(const flowmq_esb_message_t *message, size_t max_message_size, tstr *encoded) {
  size_t metadata_size;
  size_t encoded_size;
  unsigned char *cursor;
  int rc;
  if (!encoded || *encoded) return SALTS_EINVAL;
  rc = flowmq_esb_encoded_size(message, max_message_size, &encoded_size);
  if (rc != SALTS_OK) return rc;
  rc = flowmq_esb_validate(message, &metadata_size);
  if (rc != SALTS_OK) return rc;
  *encoded = tstr_new_len(NULL, encoded_size);
  if (!*encoded) return SALTS_ENOMEM;
  cursor = (unsigned char *)*encoded;
  memcpy(cursor, FLOWMQ_ESB_MAGIC, sizeof(FLOWMQ_ESB_MAGIC));
  cursor[4] = (unsigned char)message->kind;
  cursor[5] = 0u;
  cursor[6] = 0u;
  cursor[7] = 0u;
  flowmq_esb_write_u32(cursor + 8u, (uint32_t)metadata_size);
  flowmq_esb_write_u32(cursor + 12u, (uint32_t)message->payload.len);
  cursor += FLOWMQ_ESB_HEADER_SIZE;
  flowmq_esb_encode_metadata(message, &cursor);
  if (message->payload.len != 0u) {
    memcpy(cursor, message->payload.data, message->payload.len);
    cursor += message->payload.len;
  }
  if ((size_t)(cursor - (unsigned char *)*encoded) != encoded_size) {
    tstr_freep(encoded);
    return SALTS_ERANGE;
  }
  return SALTS_OK;
}

static int flowmq_esb_decode_field(const unsigned char **cursor, const unsigned char *end,
                                   flowmq_esb_field_t expected, uint32_t expected_size,
                                   const unsigned char **value) {
  uint32_t size;
  if ((size_t)(end - *cursor) < FLOWMQ_ESB_TLV_HEADER_SIZE) return SALTS_EPROTO;
  if ((*cursor)[0] != (unsigned char)expected) return SALTS_EPROTO;
  size = flowmq_esb_read_u32(*cursor + 1u);
  if (size != expected_size || (size_t)(end - *cursor) < FLOWMQ_ESB_TLV_HEADER_SIZE + size) {
    return SALTS_EPROTO;
  }
  *value = *cursor + FLOWMQ_ESB_TLV_HEADER_SIZE;
  *cursor += FLOWMQ_ESB_TLV_HEADER_SIZE + size;
  return SALTS_OK;
}

static int flowmq_esb_decode_metadata(flowmq_esb_message_t *message, const unsigned char *data,
                                      const unsigned char *end) {
  const unsigned char *cursor = data;
  const unsigned char *value;
  uint32_t group_size;
  int rc;
#define FLOWMQ_ESB_DECODE_FIXED(field, width)                                                      \
  do {                                                                                             \
    rc = flowmq_esb_decode_field(&cursor, end, field, width, &value);                              \
    if (rc != SALTS_OK) return rc;                                                                 \
  } while (0)
  switch (message->kind) {
  case FLOWMQ_ESB_SCATTER_REQUEST:
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_EXPECTED_RESPONSES, 4u);
    message->expected_responses = flowmq_esb_read_u32(value);
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_AGGREGATION_POLICY, 1u);
    message->aggregation_policy = (flowmq_esb_gather_policy_t)value[0];
    break;
  case FLOWMQ_ESB_GATHER_RESPONSE:
    break;
  case FLOWMQ_ESB_PARTIAL_RESPONSE:
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_PARTIAL_INDEX, 4u);
    message->partial_index = flowmq_esb_read_u32(value);
    break;
  case FLOWMQ_ESB_SAGA_EXECUTE:
  case FLOWMQ_ESB_SAGA_COMMIT:
  case FLOWMQ_ESB_SAGA_COMPENSATE:
  case FLOWMQ_ESB_SAGA_ABORT:
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_SAGA_ID, 8u);
    message->saga_id = flowmq_esb_read_u64(value);
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_SAGA_STEP, 4u);
    message->saga_step = flowmq_esb_read_u32(value);
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_SAGA_STATE, 1u);
    message->saga_state = (flowmq_esb_saga_state_t)value[0];
    break;
  case FLOWMQ_ESB_STREAM_PUBLISH:
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_PARTITION_ID, 4u);
    message->partition_id = flowmq_esb_read_u32(value);
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_OFFSET, 8u);
    message->offset = flowmq_esb_read_u64(value);
    break;
  case FLOWMQ_ESB_STREAM_COMMIT:
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_PARTITION_ID, 4u);
    message->partition_id = flowmq_esb_read_u32(value);
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_OFFSET, 8u);
    message->offset = flowmq_esb_read_u64(value);
    /* fall through to the shared consumer-group field */
  case FLOWMQ_ESB_STREAM_SUBSCRIBE:
  case FLOWMQ_ESB_STREAM_REBALANCE:
    if ((size_t)(end - cursor) < FLOWMQ_ESB_TLV_HEADER_SIZE ||
        cursor[0] != FLOWMQ_ESB_FIELD_CONSUMER_GROUP) {
      return SALTS_EPROTO;
    }
    group_size = flowmq_esb_read_u32(cursor + 1u);
    if (group_size == 0u || group_size > FLOWMQ_ESB_MAX_CONSUMER_GROUP_SIZE) {
      return SALTS_EPROTO;
    }
    rc = flowmq_esb_decode_field(&cursor, end, FLOWMQ_ESB_FIELD_CONSUMER_GROUP, group_size, &value);
    if (rc != SALTS_OK) return rc;
    message->consumer_group = vstr_from_buf((const char *)value, group_size);
    break;
  case FLOWMQ_ESB_PRIORITY_PUBLISH:
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_PRIORITY, 1u);
    message->priority = value[0];
    break;
  case FLOWMQ_ESB_CIRCUIT_STATUS:
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_CIRCUIT_STATE, 1u);
    message->circuit_state = (flowmq_esb_circuit_state_t)value[0];
    FLOWMQ_ESB_DECODE_FIXED(FLOWMQ_ESB_FIELD_FAILURE_COUNT, 4u);
    message->failure_count = flowmq_esb_read_u32(value);
    break;
  default:
    return SALTS_EPROTO;
  }
#undef FLOWMQ_ESB_DECODE_FIXED
  return cursor == end ? SALTS_OK : SALTS_EPROTO;
}

int flowmq_esb_decode(vstr encoded, size_t max_message_size, flowmq_esb_message_t *message) {
  const unsigned char *header = (const unsigned char *)encoded.data;
  const unsigned char *metadata;
  size_t metadata_size;
  size_t payload_size;
  size_t total;
  size_t validated_metadata_size;
  int rc;
  if (!message || (encoded.len != 0u && !encoded.data) || max_message_size == 0u) {
    return SALTS_EINVAL;
  }
  memset(message, 0, sizeof(*message));
  if (encoded.len > max_message_size) return SALTS_EMSGSIZE;
  if (encoded.len < FLOWMQ_ESB_HEADER_SIZE) return SALTS_EPROTO;
  if (memcmp(header, FLOWMQ_ESB_MAGIC, sizeof(FLOWMQ_ESB_MAGIC)) != 0 || header[5] != 0u ||
      header[6] != 0u || header[7] != 0u) {
    return SALTS_EPROTO;
  }
  metadata_size = flowmq_esb_read_u32(header + 8u);
  payload_size = flowmq_esb_read_u32(header + 12u);
  if (FLOWMQ_ESB_HEADER_SIZE > SIZE_MAX - metadata_size ||
      FLOWMQ_ESB_HEADER_SIZE + metadata_size > SIZE_MAX - payload_size) {
    return SALTS_ERANGE;
  }
  total = FLOWMQ_ESB_HEADER_SIZE + metadata_size + payload_size;
  if (total != encoded.len) return SALTS_EPROTO;
  message->kind = (flowmq_esb_message_kind_t)header[4];
  metadata = header + FLOWMQ_ESB_HEADER_SIZE;
  rc = flowmq_esb_decode_metadata(message, metadata, metadata + metadata_size);
  if (rc != SALTS_OK) {
    memset(message, 0, sizeof(*message));
    return rc;
  }
  message->payload = vstr_from_buf((const char *)(metadata + metadata_size), payload_size);
  rc = flowmq_esb_validate(message, &validated_metadata_size);
  if (rc != SALTS_OK || validated_metadata_size != metadata_size) {
    memset(message, 0, sizeof(*message));
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}
