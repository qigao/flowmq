#include "turbo_flow_fmq_management_protocol.h"

#include "turbo_error.h"
#include "turbo_parser.h"
#include "turbo_str_view.h"

#include <limits.h>
#include <string.h>

static const uint8_t FLOW_TFMP_MAGIC[4] = {'T', 'F', 'M', 'P'};
static const uint32_t FLOW_TFMP_KNOWN_FLAGS =
    TURBO_FLOW_TFMP_FLAG_RESPONSE | TURBO_FLOW_TFMP_FLAG_REPLAYED | TURBO_FLOW_TFMP_FLAG_EVENT;

static void flow_tfmp_write_u16(uint8_t *out, uint16_t value) {
  out[0] = (uint8_t)(value >> 8u);
  out[1] = (uint8_t)value;
}

static void flow_tfmp_write_u32(uint8_t *out, uint32_t value) {
  out[0] = (uint8_t)(value >> 24u);
  out[1] = (uint8_t)(value >> 16u);
  out[2] = (uint8_t)(value >> 8u);
  out[3] = (uint8_t)value;
}

static void flow_tfmp_write_u64(uint8_t *out, uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    out[i] = (uint8_t)(value >> (56u - 8u * i));
}

static uint16_t flow_tfmp_read_u16(const uint8_t *data) {
  return (uint16_t)(((uint16_t)data[0] << 8u) | data[1]);
}

static uint32_t flow_tfmp_read_u32(const uint8_t *data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) | ((uint32_t)data[2] << 8u) |
         data[3];
}

static uint64_t flow_tfmp_read_u64(const uint8_t *data) {
  uint64_t value = 0u;
  for (size_t i = 0u; i < 8u; ++i)
    value = (value << 8u) | data[i];
  return value;
}

static size_t flow_tfmp_varint_size(uint32_t value) {
  if (value < (1u << 7u)) return 1u;
  if (value < (1u << 14u)) return 2u;
  if (value < (1u << 21u)) return 3u;
  if (value < (1u << 28u)) return 4u;
  return 5u;
}

static int flow_tfmp_envelope_semantics(const turbo_flow_tfmp_envelope_t *envelope,
                                        int wire_error) {
  int error = wire_error ? TURBO_EPROTO : TURBO_EINVAL;
  int response;
  int event;
  if (!envelope || envelope->kind == 0u || (envelope->flags & ~FLOW_TFMP_KNOWN_FLAGS) != 0u ||
      envelope->status > TURBO_FLOW_TFMP_STATUS_FAILED_PRECONDITION ||
      envelope->disposition > TURBO_FLOW_TFMP_DISPOSITION_FAILED) {
    return error;
  }
  response = (envelope->flags & TURBO_FLOW_TFMP_FLAG_RESPONSE) != 0u;
  event = (envelope->flags & TURBO_FLOW_TFMP_FLAG_EVENT) != 0u;
  if (response && event) return error;
  if ((envelope->flags & TURBO_FLOW_TFMP_FLAG_REPLAYED) != 0u && !response) return error;
  if (event) {
    return envelope->correlation_id != 0u && envelope->status == 0u && envelope->disposition == 0u
               ? TURBO_OK
               : error;
  }
  if (!response) {
    return envelope->flags == 0u && envelope->correlation_id != 0u && envelope->status == 0u &&
                   envelope->disposition == 0u
               ? TURBO_OK
               : error;
  }
  if (envelope->correlation_id == 0u && envelope->status == TURBO_FLOW_TFMP_STATUS_OK) return error;
  if (envelope->status == TURBO_FLOW_TFMP_STATUS_OK) {
    return envelope->disposition >= TURBO_FLOW_TFMP_DISPOSITION_COMPLETED &&
                   envelope->disposition <= TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_DURABLE
               ? TURBO_OK
               : error;
  }
  return envelope->disposition == TURBO_FLOW_TFMP_DISPOSITION_NONE ||
                 envelope->disposition == TURBO_FLOW_TFMP_DISPOSITION_FAILED
             ? TURBO_OK
             : error;
}

int turbo_flow_tfmp_field_iterator_init(turbo_flow_tfmp_field_iterator_t *iterator,
                                        const uint8_t *body, size_t body_size) {
  if (!iterator || iterator->size < sizeof(*iterator) || (!body && body_size > 0u))
    return TURBO_EINVAL;
  if (body_size > TURBO_FLOW_TFMP_MAX_BODY_SIZE) return TURBO_EMSGSIZE;
  iterator->body = body;
  iterator->body_size = body_size;
  iterator->offset = 0u;
  iterator->field_count = 0u;
  iterator->previous_id = 0u;
  iterator->previous_type = 0u;
  return TURBO_OK;
}

int turbo_flow_tfmp_field_iterator_next(turbo_flow_tfmp_field_iterator_t *iterator,
                                        turbo_flow_tfmp_field_t *out) {
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  uint32_t length;
  size_t header_size;
  size_t remaining;
  uint8_t type;
  if (!iterator || iterator->size < sizeof(*iterator) || !out || out->size < sizeof(*out))
    return TURBO_EINVAL;
  if (iterator->offset == iterator->body_size) return TURBO_ENOENT;
  if (iterator->body_size > TURBO_FLOW_TFMP_MAX_BODY_SIZE) return TURBO_EMSGSIZE;
  if (!iterator->body || iterator->offset > iterator->body_size) return TURBO_EPROTO;
  if (iterator->field_count >= TURBO_FLOW_TFMP_MAX_FIELDS) return TURBO_EMSGSIZE;
  remaining = iterator->body_size - iterator->offset;
  if (turbo_ltv_peek_size(iterator->body + iterator->offset, remaining, &length, &header_size) !=
          0 ||
      length == 0u || header_size != flow_tfmp_varint_size(length) || header_size > remaining ||
      length > remaining - header_size) {
    return TURBO_EPROTO;
  }
  type = iterator->body[iterator->offset + header_size];
  field.id = type & TURBO_FLOW_TFMP_FIELD_ID_MASK;
  if (field.id == 0u || field.id < iterator->previous_id ||
      (field.id == iterator->previous_id && type != iterator->previous_type))
    return TURBO_EPROTO;
  field.critical = (type & TURBO_FLOW_TFMP_FIELD_CRITICAL) != 0u;
  field.value = iterator->body + iterator->offset + header_size + 1u;
  field.value_size = (size_t)length - 1u;
  iterator->offset += header_size + length;
  iterator->field_count++;
  iterator->previous_id = field.id;
  iterator->previous_type = type;
  *out = field;
  return TURBO_OK;
}

int turbo_flow_tfmp_body_validate(const uint8_t *body, size_t body_size, size_t *field_count) {
  turbo_flow_tfmp_field_iterator_t iterator = TURBO_FLOW_TFMP_FIELD_ITERATOR_INIT;
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  int rc = turbo_flow_tfmp_field_iterator_init(&iterator, body, body_size);
  if (field_count) *field_count = 0u;
  if (rc != TURBO_OK) return rc;
  while ((rc = turbo_flow_tfmp_field_iterator_next(&iterator, &field)) == TURBO_OK) {
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  }
  if (rc != TURBO_ENOENT) return rc;
  if (field_count) *field_count = iterator.field_count;
  return TURBO_OK;
}

int turbo_flow_tfmp_body_builder_init(turbo_flow_tfmp_body_builder_t *builder, uint8_t *data,
                                      size_t capacity) {
  if (!builder || builder->size < sizeof(*builder) || (!data && capacity > 0u)) return TURBO_EINVAL;
  builder->data = data;
  builder->capacity = capacity;
  builder->length = 0u;
  builder->field_count = 0u;
  builder->previous_id = 0u;
  builder->previous_type = 0u;
  return TURBO_OK;
}

int turbo_flow_tfmp_body_builder_append(turbo_flow_tfmp_body_builder_t *builder, uint8_t field_id,
                                        int critical, const uint8_t *value, size_t value_size) {
  size_t wire_size;
  size_t written;
  uint8_t type;
  type = field_id | (critical ? TURBO_FLOW_TFMP_FIELD_CRITICAL : 0u);
  if (!builder || builder->size < sizeof(*builder) || !builder->data || field_id == 0u ||
      field_id > TURBO_FLOW_TFMP_FIELD_ID_MASK || field_id < builder->previous_id ||
      (field_id == builder->previous_id && type != builder->previous_type) ||
      (value_size > 0u && !value)) {
    return TURBO_EINVAL;
  }
  if (builder->length > TURBO_FLOW_TFMP_MAX_BODY_SIZE) return TURBO_EMSGSIZE;
  if (builder->length > builder->capacity) return TURBO_EINVAL;
  if (builder->field_count >= TURBO_FLOW_TFMP_MAX_FIELDS) return TURBO_EMSGSIZE;
  wire_size = turbo_ltv_wire_size(value_size);
  if (wire_size == 0u || wire_size > TURBO_FLOW_TFMP_MAX_BODY_SIZE - builder->length)
    return TURBO_EMSGSIZE;
  if (wire_size > builder->capacity - builder->length) return TURBO_ENOSPC;
  written = turbo_ltv_build(type, value, value_size, builder->data + builder->length,
                            builder->capacity - builder->length);
  if (written != wire_size) return TURBO_EPROTO;
  builder->length += written;
  builder->field_count++;
  builder->previous_id = field_id;
  builder->previous_type = type;
  return TURBO_OK;
}

int turbo_flow_tfmp_body_builder_append_u16(turbo_flow_tfmp_body_builder_t *builder,
                                            uint8_t field_id, int critical, uint16_t value) {
  uint8_t bytes[2];
  flow_tfmp_write_u16(bytes, value);
  return turbo_flow_tfmp_body_builder_append(builder, field_id, critical, bytes, sizeof(bytes));
}

int turbo_flow_tfmp_body_builder_append_u32(turbo_flow_tfmp_body_builder_t *builder,
                                            uint8_t field_id, int critical, uint32_t value) {
  uint8_t bytes[4];
  flow_tfmp_write_u32(bytes, value);
  return turbo_flow_tfmp_body_builder_append(builder, field_id, critical, bytes, sizeof(bytes));
}

int turbo_flow_tfmp_body_builder_append_u64(turbo_flow_tfmp_body_builder_t *builder,
                                            uint8_t field_id, int critical, uint64_t value) {
  uint8_t bytes[8];
  flow_tfmp_write_u64(bytes, value);
  return turbo_flow_tfmp_body_builder_append(builder, field_id, critical, bytes, sizeof(bytes));
}

int turbo_flow_tfmp_body_builder_append_i32(turbo_flow_tfmp_body_builder_t *builder,
                                            uint8_t field_id, int critical, int32_t value) {
  return turbo_flow_tfmp_body_builder_append_u32(builder, field_id, critical, (uint32_t)value);
}

int turbo_flow_tfmp_body_builder_append_bool(turbo_flow_tfmp_body_builder_t *builder,
                                             uint8_t field_id, int critical, int value) {
  uint8_t byte;
  if (value != 0 && value != 1) return TURBO_EINVAL;
  byte = (uint8_t)value;
  return turbo_flow_tfmp_body_builder_append(builder, field_id, critical, &byte, sizeof(byte));
}

int turbo_flow_tfmp_body_builder_append_utf8(turbo_flow_tfmp_body_builder_t *builder,
                                             uint8_t field_id, int critical, const char *value,
                                             size_t value_size) {
  tstr_v view;
  if ((!value && value_size > 0u) || (value_size > 0u && memchr(value, '\0', value_size) != NULL))
    return TURBO_EINVAL;
  view = tstr_v_from_buf(value, value_size);
  if (!tstr_v_utf8_valid(view)) return TURBO_EINVAL;
  return turbo_flow_tfmp_body_builder_append(builder, field_id, critical, (const uint8_t *)value,
                                             value_size);
}

static int flow_tfmp_field_width(const turbo_flow_tfmp_field_t *field, size_t width) {
  if (!field || field->size < sizeof(*field) || (!field->value && field->value_size > 0u))
    return TURBO_EINVAL;
  return field->value_size == width ? TURBO_OK : TURBO_EPROTO;
}

int turbo_flow_tfmp_field_read_u16(const turbo_flow_tfmp_field_t *field, uint16_t *out) {
  uint16_t value;
  int rc;
  if (!out) return TURBO_EINVAL;
  rc = flow_tfmp_field_width(field, 2u);
  if (rc != TURBO_OK) return rc;
  value = flow_tfmp_read_u16(field->value);
  *out = value;
  return TURBO_OK;
}

int turbo_flow_tfmp_field_read_u32(const turbo_flow_tfmp_field_t *field, uint32_t *out) {
  uint32_t value;
  int rc;
  if (!out) return TURBO_EINVAL;
  rc = flow_tfmp_field_width(field, 4u);
  if (rc != TURBO_OK) return rc;
  value = flow_tfmp_read_u32(field->value);
  *out = value;
  return TURBO_OK;
}

int turbo_flow_tfmp_field_read_u64(const turbo_flow_tfmp_field_t *field, uint64_t *out) {
  uint64_t value;
  int rc;
  if (!out) return TURBO_EINVAL;
  rc = flow_tfmp_field_width(field, 8u);
  if (rc != TURBO_OK) return rc;
  value = flow_tfmp_read_u64(field->value);
  *out = value;
  return TURBO_OK;
}

int turbo_flow_tfmp_field_read_i32(const turbo_flow_tfmp_field_t *field, int32_t *out) {
  uint32_t value;
  int rc;
  if (!out) return TURBO_EINVAL;
  rc = turbo_flow_tfmp_field_read_u32(field, &value);
  if (rc != TURBO_OK) return rc;
  *out = (int32_t)value;
  return TURBO_OK;
}

int turbo_flow_tfmp_field_read_bool(const turbo_flow_tfmp_field_t *field, int *out) {
  int rc;
  if (!out) return TURBO_EINVAL;
  rc = flow_tfmp_field_width(field, 1u);
  if (rc != TURBO_OK) return rc;
  if (field->value[0] > 1u) return TURBO_EPROTO;
  *out = field->value[0] != 0u;
  return TURBO_OK;
}

int turbo_flow_tfmp_field_validate_utf8(const turbo_flow_tfmp_field_t *field) {
  tstr_v view;
  if (!field || field->size < sizeof(*field) || (!field->value && field->value_size > 0u))
    return TURBO_EINVAL;
  if (field->value_size > 0u && memchr(field->value, '\0', field->value_size) != NULL)
    return TURBO_EPROTO;
  view = tstr_v_from_buf((const char *)field->value, field->value_size);
  return tstr_v_utf8_valid(view) ? TURBO_OK : TURBO_EPROTO;
}

int turbo_flow_tfmp_envelope_encode(const turbo_flow_tfmp_envelope_t *envelope, uint8_t *out,
                                    size_t capacity, size_t *out_len) {
  size_t required;
  int rc;
  if (!out_len) return TURBO_EINVAL;
  *out_len = 0u;
  if (!envelope || envelope->size < sizeof(*envelope) ||
      envelope->major != TURBO_FLOW_TFMP_PROTOCOL_MAJOR ||
      (!envelope->body && envelope->body_size > 0u)) {
    return TURBO_EINVAL;
  }
  rc = flow_tfmp_envelope_semantics(envelope, 0);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_tfmp_body_validate(envelope->body, envelope->body_size, NULL);
  if (rc != TURBO_OK) return rc;
  required = TURBO_FLOW_TFMP_HEADER_SIZE + envelope->body_size;
  *out_len = required;
  if (!out || capacity < required) return TURBO_ENOSPC;
  memcpy(out, FLOW_TFMP_MAGIC, sizeof(FLOW_TFMP_MAGIC));
  flow_tfmp_write_u16(out + 4u, envelope->major);
  flow_tfmp_write_u16(out + 6u, envelope->minor);
  flow_tfmp_write_u16(out + 8u, TURBO_FLOW_TFMP_HEADER_SIZE);
  flow_tfmp_write_u16(out + 10u, envelope->kind);
  flow_tfmp_write_u32(out + 12u, envelope->flags);
  flow_tfmp_write_u64(out + 16u, envelope->correlation_id);
  flow_tfmp_write_u32(out + 24u, (uint32_t)envelope->body_size);
  flow_tfmp_write_u16(out + 28u, envelope->status);
  flow_tfmp_write_u16(out + 30u, envelope->disposition);
  memset(out + 32u, 0, 8u);
  if (envelope->body_size > 0u)
    memcpy(out + TURBO_FLOW_TFMP_HEADER_SIZE, envelope->body, envelope->body_size);
  return TURBO_OK;
}

int turbo_flow_tfmp_envelope_decode(const uint8_t *data, size_t data_len,
                                    turbo_flow_tfmp_envelope_t *out) {
  turbo_flow_tfmp_envelope_t decoded = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  size_t body_size;
  int rc;
  if (!data || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  if (data_len > TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE) return TURBO_EMSGSIZE;
  if (data_len < TURBO_FLOW_TFMP_HEADER_SIZE ||
      memcmp(data, FLOW_TFMP_MAGIC, sizeof(FLOW_TFMP_MAGIC)) != 0)
    return TURBO_EPROTO;
  decoded.major = flow_tfmp_read_u16(data + 4u);
  if (decoded.major != TURBO_FLOW_TFMP_PROTOCOL_MAJOR) return TURBO_ENOTSUP;
  if (flow_tfmp_read_u16(data + 8u) != TURBO_FLOW_TFMP_HEADER_SIZE ||
      flow_tfmp_read_u64(data + 32u) != 0u)
    return TURBO_EPROTO;
  decoded.minor = flow_tfmp_read_u16(data + 6u);
  decoded.kind = flow_tfmp_read_u16(data + 10u);
  decoded.flags = flow_tfmp_read_u32(data + 12u);
  decoded.correlation_id = flow_tfmp_read_u64(data + 16u);
  body_size = flow_tfmp_read_u32(data + 24u);
  decoded.status = flow_tfmp_read_u16(data + 28u);
  decoded.disposition = flow_tfmp_read_u16(data + 30u);
  if (body_size > TURBO_FLOW_TFMP_MAX_BODY_SIZE) return TURBO_EMSGSIZE;
  if (data_len != TURBO_FLOW_TFMP_HEADER_SIZE + body_size) return TURBO_EPROTO;
  decoded.body = body_size > 0u ? data + TURBO_FLOW_TFMP_HEADER_SIZE : NULL;
  decoded.body_size = body_size;
  rc = flow_tfmp_envelope_semantics(&decoded, 1);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_tfmp_body_validate(decoded.body, decoded.body_size, NULL);
  if (rc != TURBO_OK) return rc;
  *out = decoded;
  return TURBO_OK;
}

void turbo_flow_tfmp_envelope_peek_request_identity(const uint8_t *data, size_t data_len,
                                                    uint16_t *kind, uint64_t *correlation_id) {
  uint16_t recovered_kind = TURBO_FLOW_TFMP_PROTOCOL_ERROR;
  uint64_t recovered_correlation_id = 0u;
  int trusted_magic = data && data_len >= sizeof(FLOW_TFMP_MAGIC) &&
                      memcmp(data, FLOW_TFMP_MAGIC, sizeof(FLOW_TFMP_MAGIC)) == 0;

  if (trusted_magic && data_len >= 12u) {
    uint16_t candidate = flow_tfmp_read_u16(data + 10u);
    if (candidate != 0u && candidate != TURBO_FLOW_TFMP_EVENT) recovered_kind = candidate;
  }
  if (trusted_magic && data_len >= 24u) recovered_correlation_id = flow_tfmp_read_u64(data + 16u);
  if (kind) *kind = recovered_kind;
  if (correlation_id) *correlation_id = recovered_correlation_id;
}

turbo_flow_tfmp_status_t turbo_flow_tfmp_status_from_error(int error) {
  switch (error) {
  case TURBO_OK:
    return TURBO_FLOW_TFMP_STATUS_OK;
  case TURBO_EINVAL:
  case TURBO_ERANGE:
  case TURBO_EPROTO:
  case TURBO_ECHARSET:
    return TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT;
  case TURBO_ENOTSUP:
  case TURBO_ENOSYS:
  case TURBO_EPROTONOSUPPORT:
  case TURBO_ENOPROTOOPT:
    return TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY;
  case TURBO_ENOENT:
  case TURBO_ENODEV:
  case TURBO_ESRCH:
    return TURBO_FLOW_TFMP_STATUS_NOT_FOUND;
  case TURBO_EALREADY:
    return TURBO_FLOW_TFMP_STATUS_CONFLICT;
  case TURBO_EBUSY:
  case TURBO_ETXTBSY:
    return TURBO_FLOW_TFMP_STATUS_BUSY;
  case TURBO_ETIMEDOUT:
    return TURBO_FLOW_TFMP_STATUS_DEADLINE_EXCEEDED;
  case TURBO_ENOSPC:
  case TURBO_ENOMEM:
  case TURBO_EMSGSIZE:
  case TURBO_ENOBUFS:
  case TURBO_EMFILE:
  case TURBO_ENFILE:
  case TURBO_EFBIG:
    return TURBO_FLOW_TFMP_STATUS_RESOURCE_EXHAUSTED;
  case TURBO_ECANCELED:
    return TURBO_FLOW_TFMP_STATUS_CANCELED;
  case TURBO_ESHUTDOWN:
  case TURBO_ENOTCONN:
  case TURBO_ECONNABORTED:
  case TURBO_ECONNREFUSED:
  case TURBO_ECONNRESET:
  case TURBO_ENETDOWN:
  case TURBO_ENETUNREACH:
  case TURBO_EHOSTUNREACH:
  case TURBO_EPIPE:
  case TURBO_EIO:
    return TURBO_FLOW_TFMP_STATUS_UNAVAILABLE;
  default:
    return TURBO_FLOW_TFMP_STATUS_INTERNAL;
  }
}
