/**
 * @file flowmq_protocol_esb.c
 * FlowMQ ESB protocol implementation.
 *
 * ESB-specific fields are encoded as TLV (Type-Length-Value) sections in the
 * frame payload. This maintains wire compatibility with FMQ v3 while allowing
 * pattern-specific extensions.
 *
 * TLV encoding format:
 * - Type: uint8_t (field identifier)
 * - Length: uint32_t (value byte count, little-endian)
 * - Value: variable-length data
 */

#include "flowmq_protocol_esb.h"
#include "turbo_error.h"

#include <string.h>

/* ESB TLV field type identifiers */
enum flowmq_esb_tlv_type_e {
  FLOWMQ_ESB_TLV_EXPECTED_RESPONSES = 1,
  FLOWMQ_ESB_TLV_AGGREGATION_POLICY = 2,
  FLOWMQ_ESB_TLV_PARTIAL_INDEX = 3,
  FLOWMQ_ESB_TLV_SAGA_ID = 4,
  FLOWMQ_ESB_TLV_SAGA_STEP = 5,
  FLOWMQ_ESB_TLV_SAGA_STATE = 6,
  FLOWMQ_ESB_TLV_PARTITION_ID = 7,
  FLOWMQ_ESB_TLV_OFFSET = 8,
  FLOWMQ_ESB_TLV_CONSUMER_GROUP = 9,
  FLOWMQ_ESB_TLV_PRIORITY = 10,
  FLOWMQ_ESB_TLV_CB_STATE = 11,
  FLOWMQ_ESB_TLV_FAILURE_COUNT = 12
};

/* TLV header size: 1 byte type + 4 bytes length */
#define FLOWMQ_ESB_TLV_HEADER_SIZE 5u

/* Write little-endian uint32_t */
static void flowmq_esb_write_u32(unsigned char *out, uint32_t value) {
  out[0] = (unsigned char)(value & 0xFFu);
  out[1] = (unsigned char)((value >> 8) & 0xFFu);
  out[2] = (unsigned char)((value >> 16) & 0xFFu);
  out[3] = (unsigned char)((value >> 24) & 0xFFu);
}

/* Read little-endian uint32_t */
static uint32_t flowmq_esb_read_u32(const unsigned char *data) {
  return (uint32_t)data[0] | ((uint32_t)data[1] << 8) | ((uint32_t)data[2] << 16) |
         ((uint32_t)data[3] << 24);
}

/* Write little-endian uint64_t */
static void flowmq_esb_write_u64(unsigned char *out, uint64_t value) {
  flowmq_esb_write_u32(out, (uint32_t)(value & 0xFFFFFFFFu));
  flowmq_esb_write_u32(out + 4, (uint32_t)(value >> 32));
}

/* Read little-endian uint64_t */
static uint64_t flowmq_esb_read_u64(const unsigned char *data) {
  uint32_t low = flowmq_esb_read_u32(data);
  uint32_t high = flowmq_esb_read_u32(data + 4);
  return ((uint64_t)high << 32) | (uint64_t)low;
}

int flowmq_protocol_esb_pattern_validate(flowmq_protocol_pattern_t pattern) {
  if (pattern < FLOWMQ_PROTOCOL_ESB_PATTERN_MIN) {
    return TURBO_EINVAL;
  }
  if (pattern > FLOWMQ_PROTOCOL_ESB_PATTERN_MAX) {
    return TURBO_EINVAL;
  }

  /* Validate known ESB patterns */
  switch (pattern) {
    case FLOWMQ_PROTOCOL_SCATTER:
    case FLOWMQ_PROTOCOL_GATHER:
    case FLOWMQ_PROTOCOL_SAGA_COORDINATOR:
    case FLOWMQ_PROTOCOL_SAGA_PARTICIPANT:
    case FLOWMQ_PROTOCOL_STREAM_PRODUCER:
    case FLOWMQ_PROTOCOL_STREAM_CONSUMER:
    case FLOWMQ_PROTOCOL_PRIORITY_PRODUCER:
    case FLOWMQ_PROTOCOL_PRIORITY_CONSUMER:
    case FLOWMQ_PROTOCOL_CB_CLIENT:
    case FLOWMQ_PROTOCOL_CB_SERVICE:
      return TURBO_OK;
    default:
      return TURBO_EINVAL;
  }
}

int flowmq_protocol_esb_patterns_compatible(flowmq_protocol_pattern_t local,
                                            flowmq_protocol_pattern_t remote) {
  /* Validate both patterns are ESB patterns */
  if (flowmq_protocol_esb_pattern_validate(local) != TURBO_OK) {
    return 0;
  }
  if (flowmq_protocol_esb_pattern_validate(remote) != TURBO_OK) {
    return 0;
  }

  /* Check compatibility pairs */
  if (local == FLOWMQ_PROTOCOL_SCATTER && remote == FLOWMQ_PROTOCOL_GATHER) {
    return 1;
  }
  if (local == FLOWMQ_PROTOCOL_GATHER && remote == FLOWMQ_PROTOCOL_SCATTER) {
    return 1;
  }

  if (local == FLOWMQ_PROTOCOL_SAGA_COORDINATOR && remote == FLOWMQ_PROTOCOL_SAGA_PARTICIPANT) {
    return 1;
  }
  if (local == FLOWMQ_PROTOCOL_SAGA_PARTICIPANT && remote == FLOWMQ_PROTOCOL_SAGA_COORDINATOR) {
    return 1;
  }

  if (local == FLOWMQ_PROTOCOL_STREAM_PRODUCER && remote == FLOWMQ_PROTOCOL_STREAM_CONSUMER) {
    return 1;
  }
  if (local == FLOWMQ_PROTOCOL_STREAM_CONSUMER && remote == FLOWMQ_PROTOCOL_STREAM_PRODUCER) {
    return 1;
  }

  if (local == FLOWMQ_PROTOCOL_PRIORITY_PRODUCER && remote == FLOWMQ_PROTOCOL_PRIORITY_CONSUMER) {
    return 1;
  }
  if (local == FLOWMQ_PROTOCOL_PRIORITY_CONSUMER && remote == FLOWMQ_PROTOCOL_PRIORITY_PRODUCER) {
    return 1;
  }

  if (local == FLOWMQ_PROTOCOL_CB_CLIENT && remote == FLOWMQ_PROTOCOL_CB_SERVICE) {
    return 1;
  }
  if (local == FLOWMQ_PROTOCOL_CB_SERVICE && remote == FLOWMQ_PROTOCOL_CB_CLIENT) {
    return 1;
  }

  /* Same pattern can connect to itself for some patterns */
  if (local == remote) {
    switch (local) {
      case FLOWMQ_PROTOCOL_STREAM_CONSUMER:  /* Consumer group members */
        return 1;
      default:
        return 0;
    }
  }

  return 0;
}

/* Calculate size needed for ESB TLV section */
static int flowmq_esb_calculate_tlv_size(const flowmq_protocol_esb_frame_t *frame,
                                         size_t *tlv_size) {
  if (!frame || !tlv_size) {
    return TURBO_EINVAL;
  }

  if (frame->expected_responses > FLOWMQ_PROTOCOL_ESB_MAX_FANOUT_COUNT) {
    return TURBO_EINVAL;
  }

  size_t size = 0;

  /* SCATTER/GATHER fields */
  if (frame->expected_responses > 0) {
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint32_t);
  }
  if (frame->aggregation_policy > 0) {
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint8_t);
  }
  if (frame->partial_index > 0) {
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint32_t);
  }

  /* SAGA fields */
  if (frame->saga_id > 0) {
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint64_t);
  }
  if (frame->saga_step > 0 || frame->saga_state > 0) {
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint32_t);  /* saga_step */
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint8_t);   /* saga_state */
  }

  /* STREAM fields */
  if (frame->partition_id > 0 || frame->offset > 0) {
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint32_t);  /* partition_id */
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint64_t);  /* offset */
  }
  if (frame->consumer_group.data && frame->consumer_group.len > 0) {
    if (frame->consumer_group.len > FLOWMQ_PROTOCOL_ESB_MAX_CONSUMER_GROUP_SIZE) {
      return TURBO_EINVAL;
    }
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + frame->consumer_group.len;
  }

  /* PRIORITY_QUEUE fields */
  if (frame->priority > 0) {
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint8_t);
  }

  /* CIRCUIT_BREAKER fields */
  if (frame->cb_state > 0 || frame->failure_count > 0) {
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint8_t);   /* cb_state */
    size += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint32_t);  /* failure_count */
  }

  *tlv_size = size;
  return TURBO_OK;
}

int flowmq_protocol_esb_encoded_size(const flowmq_protocol_esb_frame_t *frame,
                                     size_t max_frame_size, size_t *size) {
  if (!frame || !size) {
    return TURBO_EINVAL;
  }

  /* Calculate base frame size */
  int rc = flowmq_protocol_encoded_size(&frame->base, max_frame_size, size);
  if (rc != TURBO_OK) {
    return rc;
  }

  /* Add ESB TLV section size */
  size_t tlv_size = 0;
  rc = flowmq_esb_calculate_tlv_size(frame, &tlv_size);
  if (rc != TURBO_OK) {
    return rc;
  }

  if (*size + tlv_size > max_frame_size) {
    return TURBO_EMSGSIZE;
  }

  *size += tlv_size;
  return TURBO_OK;
}

/* Encode TLV field: type (1 byte) + length (4 bytes LE) + value */
static int flowmq_esb_encode_tlv_u32(unsigned char **cursor, unsigned char *end,
                                     uint8_t type, uint32_t value) {
  if (!cursor || !*cursor || !end) {
    return TURBO_EINVAL;
  }
  if (*cursor + FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint32_t) > end) {
    return TURBO_EMSGSIZE;
  }

  (*cursor)[0] = type;
  flowmq_esb_write_u32(*cursor + 1, sizeof(uint32_t));
  flowmq_esb_write_u32(*cursor + 5, value);
  *cursor += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint32_t);
  return TURBO_OK;
}

static int flowmq_esb_encode_tlv_u64(unsigned char **cursor, unsigned char *end,
                                     uint8_t type, uint64_t value) {
  if (!cursor || !*cursor || !end) {
    return TURBO_EINVAL;
  }
  if (*cursor + FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint64_t) > end) {
    return TURBO_EMSGSIZE;
  }

  (*cursor)[0] = type;
  flowmq_esb_write_u32(*cursor + 1, sizeof(uint64_t));
  flowmq_esb_write_u64(*cursor + 5, value);
  *cursor += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint64_t);
  return TURBO_OK;
}

static int flowmq_esb_encode_tlv_u8(unsigned char **cursor, unsigned char *end,
                                    uint8_t type, uint8_t value) {
  if (!cursor || !*cursor || !end) {
    return TURBO_EINVAL;
  }
  if (*cursor + FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint8_t) > end) {
    return TURBO_EMSGSIZE;
  }

  (*cursor)[0] = type;
  flowmq_esb_write_u32(*cursor + 1, sizeof(uint8_t));
  (*cursor)[5] = value;
  *cursor += FLOWMQ_ESB_TLV_HEADER_SIZE + sizeof(uint8_t);
  return TURBO_OK;
}

static int flowmq_esb_encode_tlv_bytes(unsigned char **cursor, unsigned char *end,
                                       uint8_t type, const void *data, size_t len) {
  if (!cursor || !*cursor || !end || !data) {
    return TURBO_EINVAL;
  }
  if (len == 0) {
    return TURBO_OK;
  }
  if (*cursor + FLOWMQ_ESB_TLV_HEADER_SIZE + len > end) {
    return TURBO_EMSGSIZE;
  }

  (*cursor)[0] = type;
  flowmq_esb_write_u32(*cursor + 1, (uint32_t)len);
  memcpy(*cursor + 5, data, len);
  *cursor += FLOWMQ_ESB_TLV_HEADER_SIZE + len;
  return TURBO_OK;
}

int flowmq_protocol_esb_encode_frame(const flowmq_protocol_esb_frame_t *frame,
                                     size_t max_frame_size, tstr *out) {
  if (!frame || !out) {
    return TURBO_EINVAL;
  }

  /* Validate ESB pattern */
  int rc = flowmq_protocol_esb_pattern_validate(frame->base.pattern);
  if (rc != TURBO_OK) {
    return rc;
  }

  /* Calculate TLV size */
  size_t tlv_size = 0;
  rc = flowmq_esb_calculate_tlv_size(frame, &tlv_size);
  if (rc != TURBO_OK) {
    return rc;
  }

  /* Calculate base frame encoded size */
  size_t base_size = 0u;
  rc = flowmq_protocol_encoded_size(&frame->base, max_frame_size, &base_size);
  if (rc != TURBO_OK) {
    return rc;
  }
  if (base_size + tlv_size > max_frame_size) {
    return TURBO_EMSGSIZE;
  }

  tstr base_encoded = NULL;
  rc = flowmq_protocol_encode_frame(&frame->base, max_frame_size, &base_encoded);
  if (rc != TURBO_OK) {
    return rc;
  }
  if (tstr_len(base_encoded) != base_size) {
    tstr_free(base_encoded);
    return TURBO_ERANGE;
  }
  *out = tstr_new_len(NULL, base_size + tlv_size);
  if (!*out) {
    tstr_free(base_encoded);
    return TURBO_ENOMEM;
  }
  memcpy(*out, base_encoded, base_size);
  tstr_free(base_encoded);
  tstr_set_len(*out, base_size + tlv_size);

  unsigned char *cursor = (unsigned char *)(*out + base_size);
  unsigned char *end = cursor + tlv_size;

  if (tlv_size > 0u) {
    /* Encode SCATTER/GATHER fields */
    if (frame->expected_responses > 0) {
      rc = flowmq_esb_encode_tlv_u32(&cursor, end, FLOWMQ_ESB_TLV_EXPECTED_RESPONSES,
                                     frame->expected_responses);
      if (rc != TURBO_OK) goto encode_fail;
    }
    if (frame->aggregation_policy > 0) {
      rc = flowmq_esb_encode_tlv_u8(&cursor, end, FLOWMQ_ESB_TLV_AGGREGATION_POLICY,
                                    frame->aggregation_policy);
      if (rc != TURBO_OK) goto encode_fail;
    }
    if (frame->partial_index > 0) {
      rc = flowmq_esb_encode_tlv_u32(&cursor, end, FLOWMQ_ESB_TLV_PARTIAL_INDEX,
                                     frame->partial_index);
      if (rc != TURBO_OK) goto encode_fail;
    }

    /* Encode SAGA fields */
    if (frame->saga_id > 0) {
      rc = flowmq_esb_encode_tlv_u64(&cursor, end, FLOWMQ_ESB_TLV_SAGA_ID, frame->saga_id);
      if (rc != TURBO_OK) goto encode_fail;
    }
    if (frame->saga_step > 0 || frame->saga_state > 0) {
      rc = flowmq_esb_encode_tlv_u32(&cursor, end, FLOWMQ_ESB_TLV_SAGA_STEP, frame->saga_step);
      if (rc != TURBO_OK) goto encode_fail;
      rc = flowmq_esb_encode_tlv_u8(&cursor, end, FLOWMQ_ESB_TLV_SAGA_STATE, frame->saga_state);
      if (rc != TURBO_OK) goto encode_fail;
    }

    /* Encode STREAM fields */
    if (frame->partition_id > 0 || frame->offset > 0) {
      rc = flowmq_esb_encode_tlv_u32(&cursor, end, FLOWMQ_ESB_TLV_PARTITION_ID,
                                     frame->partition_id);
      if (rc != TURBO_OK) goto encode_fail;
      rc = flowmq_esb_encode_tlv_u64(&cursor, end, FLOWMQ_ESB_TLV_OFFSET, frame->offset);
      if (rc != TURBO_OK) goto encode_fail;
    }
    if (frame->consumer_group.data && frame->consumer_group.len > 0) {
      rc = flowmq_esb_encode_tlv_bytes(&cursor, end, FLOWMQ_ESB_TLV_CONSUMER_GROUP,
                                       frame->consumer_group.data, frame->consumer_group.len);
      if (rc != TURBO_OK) goto encode_fail;
    }

    /* Encode PRIORITY_QUEUE fields */
    if (frame->priority > 0) {
      rc = flowmq_esb_encode_tlv_u8(&cursor, end, FLOWMQ_ESB_TLV_PRIORITY, frame->priority);
      if (rc != TURBO_OK) goto encode_fail;
    }

    /* Encode CIRCUIT_BREAKER fields */
    if (frame->cb_state > 0 || frame->failure_count > 0) {
      rc = flowmq_esb_encode_tlv_u8(&cursor, end, FLOWMQ_ESB_TLV_CB_STATE, frame->cb_state);
      if (rc != TURBO_OK) goto encode_fail;
      rc = flowmq_esb_encode_tlv_u32(&cursor, end, FLOWMQ_ESB_TLV_FAILURE_COUNT,
                                     frame->failure_count);
      if (rc != TURBO_OK) goto encode_fail;
    }
  }

  if (cursor != end) {
    tstr_free(*out);
    *out = NULL;
    return TURBO_ERANGE;
  }

  return rc;

encode_fail:
  tstr_free(*out);
  *out = NULL;
  return rc;
}

/* Decode one TLV field */
static int flowmq_esb_decode_tlv_field(const unsigned char **cursor, const unsigned char *end,
                                       uint8_t *type, const unsigned char **value,
                                       uint32_t *value_len) {
  if (!cursor || !*cursor || !end || !type || !value || !value_len) {
    return TURBO_EINVAL;
  }

  /* Need at least header */
  if (*cursor + FLOWMQ_ESB_TLV_HEADER_SIZE > end) {
    return FLOWMQ_PROTOCOL_INCOMPLETE;
  }

  *type = (*cursor)[0];
  *value_len = flowmq_esb_read_u32(*cursor + 1);

  /* Check if full value is available */
  if (*cursor + FLOWMQ_ESB_TLV_HEADER_SIZE + *value_len > end) {
    return FLOWMQ_PROTOCOL_INCOMPLETE;
  }

  *value = *cursor + FLOWMQ_ESB_TLV_HEADER_SIZE;
  *cursor += FLOWMQ_ESB_TLV_HEADER_SIZE + *value_len;

  return TURBO_OK;
}

static int flowmq_esb_apply_tlv_payload(const vstr *payload,
                                        flowmq_protocol_esb_decode_mode_t mode,
                                        flowmq_protocol_esb_frame_t *out) {
  if (!payload || !out) {
    return TURBO_EINVAL;
  }
  if (payload->data && payload->len > 0u) {
    const unsigned char *cursor = (const unsigned char *)payload->data;
    const unsigned char *end = cursor + payload->len;

    while (cursor < end) {
      uint8_t type = 0;
      const unsigned char *value = NULL;
      uint32_t value_len = 0;
      int rc = flowmq_esb_decode_tlv_field(&cursor, end, &type, &value, &value_len);
      if (rc == FLOWMQ_PROTOCOL_INCOMPLETE) {
        return TURBO_EPROTO;
      }
      if (rc != TURBO_OK) {
        return rc;
      }

      /* Decode based on type */
      switch (type) {
        case FLOWMQ_ESB_TLV_EXPECTED_RESPONSES:
          if (value_len != sizeof(uint32_t)) {
            return TURBO_EPROTO;
          }
          out->expected_responses = flowmq_esb_read_u32(value);
          if (out->expected_responses > FLOWMQ_PROTOCOL_ESB_MAX_FANOUT_COUNT) {
            return TURBO_EPROTO;
          }
          break;

        case FLOWMQ_ESB_TLV_AGGREGATION_POLICY:
          if (value_len != sizeof(uint8_t)) {
            return TURBO_EPROTO;
          }
          out->aggregation_policy = value[0];
          break;

        case FLOWMQ_ESB_TLV_PARTIAL_INDEX:
          if (value_len != sizeof(uint32_t)) {
            return TURBO_EPROTO;
          }
          out->partial_index = flowmq_esb_read_u32(value);
          break;

        case FLOWMQ_ESB_TLV_SAGA_ID:
          if (value_len != sizeof(uint64_t)) {
            return TURBO_EPROTO;
          }
          out->saga_id = flowmq_esb_read_u64(value);
          break;

        case FLOWMQ_ESB_TLV_SAGA_STEP:
          if (value_len != sizeof(uint32_t)) {
            return TURBO_EPROTO;
          }
          out->saga_step = flowmq_esb_read_u32(value);
          if (out->saga_step >= FLOWMQ_PROTOCOL_ESB_MAX_SAGA_STEPS) {
            return TURBO_EPROTO;
          }
          break;

        case FLOWMQ_ESB_TLV_SAGA_STATE:
          if (value_len != sizeof(uint8_t)) {
            return TURBO_EPROTO;
          }
          out->saga_state = value[0];
          break;

        case FLOWMQ_ESB_TLV_PARTITION_ID:
          if (value_len != sizeof(uint32_t)) {
            return TURBO_EPROTO;
          }
          out->partition_id = flowmq_esb_read_u32(value);
          break;

        case FLOWMQ_ESB_TLV_OFFSET:
          if (value_len != sizeof(uint64_t)) {
            return TURBO_EPROTO;
          }
          out->offset = flowmq_esb_read_u64(value);
          break;

        case FLOWMQ_ESB_TLV_CONSUMER_GROUP:
          if (value_len > FLOWMQ_PROTOCOL_ESB_MAX_CONSUMER_GROUP_SIZE) {
            return TURBO_EPROTO;
          }
          /* Borrow view from base frame payload */
          out->consumer_group.data = (const char *)value;
          out->consumer_group.len = value_len;
          break;

        case FLOWMQ_ESB_TLV_PRIORITY:
          if (value_len != sizeof(uint8_t)) {
            return TURBO_EPROTO;
          }
          out->priority = value[0];
          break;

        case FLOWMQ_ESB_TLV_CB_STATE:
          if (value_len != sizeof(uint8_t)) {
            return TURBO_EPROTO;
          }
          out->cb_state = value[0];
          break;

        case FLOWMQ_ESB_TLV_FAILURE_COUNT:
          if (value_len != sizeof(uint32_t)) {
            return TURBO_EPROTO;
          }
          out->failure_count = flowmq_esb_read_u32(value);
          break;

        default:
          if (mode == FLOWMQ_PROTOCOL_ESB_DECODE_STRICT) {
            return TURBO_EPROTO;
          }
          break;
      }
    }
  }

  return TURBO_OK;
}

static int flowmq_protocol_esb_decode_frame_internal(const char *data,
                                                    size_t data_len,
                                                    size_t max_frame_size,
                                                    flowmq_protocol_esb_decode_mode_t mode,
                                                    flowmq_protocol_esb_frame_t *out,
                                                    size_t *consumed) {
  if (!data || !out || !consumed) {
    return TURBO_EINVAL;
  }
  if (mode != FLOWMQ_PROTOCOL_ESB_DECODE_RELAXED &&
      mode != FLOWMQ_PROTOCOL_ESB_DECODE_STRICT) {
    return TURBO_EINVAL;
  }

  /* Initialize output */
  memset(out, 0, sizeof(*out));

  /* Decode base frame first */
  int rc = flowmq_protocol_decode_frame(data, data_len, max_frame_size, &out->base, consumed);
  if (rc != TURBO_OK) {
    return rc;
  }

  /* Validate ESB pattern */
  rc = flowmq_protocol_esb_pattern_validate(out->base.pattern);
  if (rc != TURBO_OK) {
    flowmq_protocol_esb_frame_cleanup(out);
    return rc;
  }

  vstr extension_payload = {0};
  if (out->base.owned_payload) {
    size_t owned_payload_len = tstr_len(out->base.owned_payload);
    if (owned_payload_len < out->base.payload.len) {
      flowmq_protocol_esb_frame_cleanup(out);
      return TURBO_EPROTO;
    }
    extension_payload = vstr_from_buf(
        (const char *)out->base.owned_payload + out->base.payload.len,
        owned_payload_len - out->base.payload.len);
  }
  rc = flowmq_esb_apply_tlv_payload(&extension_payload, mode, out);
  if (rc != TURBO_OK) {
    flowmq_protocol_esb_frame_cleanup(out);
    return rc;
  }

  return TURBO_OK;
}

int flowmq_protocol_esb_decode_frame(const char *data, size_t data_len,
                                     size_t max_frame_size,
                                     flowmq_protocol_esb_frame_t *out, size_t *consumed) {
  return flowmq_protocol_esb_decode_frame_ex(data, data_len, max_frame_size,
                                            FLOWMQ_PROTOCOL_ESB_DECODE_RELAXED, out,
                                            consumed);
}

int flowmq_protocol_esb_decode_frame_ex(const char *data,
                                        size_t data_len,
                                        size_t max_frame_size,
                                        flowmq_protocol_esb_decode_mode_t mode,
                                        flowmq_protocol_esb_frame_t *out,
                                        size_t *consumed) {
  return flowmq_protocol_esb_decode_frame_internal(data, data_len, max_frame_size, mode, out,
                                                  consumed);
}

int flowmq_protocol_esb_decode_frame_from_base_ex(
    const flowmq_protocol_frame_t *base,
    flowmq_protocol_esb_decode_mode_t mode,
    flowmq_protocol_esb_frame_t *out) {
  int rc;
  if (!base || !out) return TURBO_EINVAL;
  if (mode != FLOWMQ_PROTOCOL_ESB_DECODE_RELAXED && mode != FLOWMQ_PROTOCOL_ESB_DECODE_STRICT) {
    return TURBO_EINVAL;
  }

  memset(out, 0, sizeof(*out));

  /* Borrow from base view, do not take base owned payload ownership. */
  const tstr borrowed_owned_payload = base->owned_payload;
  out->base = *base;
  out->base.owned_payload = NULL;

  rc = flowmq_protocol_esb_pattern_validate(out->base.pattern);
  if (rc != TURBO_OK) {
    flowmq_protocol_esb_frame_cleanup(out);
    return rc;
  }

  vstr extension_payload = {0};
  if (borrowed_owned_payload) {
    size_t owned_payload_len = tstr_len(borrowed_owned_payload);
    if (owned_payload_len < out->base.payload.len) {
      flowmq_protocol_esb_frame_cleanup(out);
      return TURBO_EPROTO;
    }
    extension_payload = vstr_from_buf((const char *)borrowed_owned_payload +
                                            out->base.payload.len,
                                        owned_payload_len - out->base.payload.len);
  }
  rc = flowmq_esb_apply_tlv_payload(&extension_payload, mode, out);
  if (rc != TURBO_OK) {
    flowmq_protocol_esb_frame_cleanup(out);
    return rc;
  }

  return TURBO_OK;
}

int flowmq_protocol_esb_decode_frame_from_base(const flowmq_protocol_frame_t *base,
                                              flowmq_protocol_esb_frame_t *out) {
  return flowmq_protocol_esb_decode_frame_from_base_ex(base,
                                                      FLOWMQ_PROTOCOL_ESB_DECODE_RELAXED,
                                                      out);
}

void flowmq_protocol_esb_frame_cleanup(flowmq_protocol_esb_frame_t *frame) {
  if (!frame) {
    return;
  }

  /* Clean up base frame resources */
  flowmq_protocol_frame_cleanup(&frame->base);

  /* ESB-specific cleanup (consumer_group is borrowed view, no cleanup needed) */

  /* Zero out the structure */
  memset(frame, 0, sizeof(*frame));
}

int flowmq_protocol_esb_frame_consumer_group_copy(
    const flowmq_protocol_esb_frame_t *frame,
    tstr *out) {
  if (!frame || !out) {
    return TURBO_EINVAL;
  }

  tstr_free(*out);
  if (frame->consumer_group.len == 0) {
    *out = NULL;
    return TURBO_OK;
  }

  *out = tstr_from_v(frame->consumer_group);
  if (!*out) {
    return TURBO_ENOMEM;
  }

  return TURBO_OK;
}
