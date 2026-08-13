#include "turbo_flow_fmq_broker_protocol.h"

#include "turbo_error.h"

#include <string.h>

static const uint8_t FLOW_FMQ_BROKER_ENVELOPE_MAGIC[4] = {'T', 'F', 'B', 'R'};

static void flow_fmq_broker_write_u16(uint8_t *out, uint16_t value) {
  out[0] = (uint8_t)(value >> 8u);
  out[1] = (uint8_t)value;
}

static void flow_fmq_broker_write_u32(uint8_t *out, uint32_t value) {
  out[0] = (uint8_t)(value >> 24u);
  out[1] = (uint8_t)(value >> 16u);
  out[2] = (uint8_t)(value >> 8u);
  out[3] = (uint8_t)value;
}

static void flow_fmq_broker_write_u64(uint8_t *out, uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    out[i] = (uint8_t)(value >> (56u - 8u * i));
}

static uint16_t flow_fmq_broker_read_u16(const uint8_t *data) {
  return (uint16_t)(((uint16_t)data[0] << 8u) | data[1]);
}

static uint32_t flow_fmq_broker_read_u32(const uint8_t *data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) | ((uint32_t)data[2] << 8u) |
         data[3];
}

static uint64_t flow_fmq_broker_read_u64(const uint8_t *data) {
  uint64_t value = 0u;
  for (size_t i = 0u; i < 8u; ++i)
    value = (value << 8u) | data[i];
  return value;
}

static int flow_fmq_broker_logical_id_length(const char *id, size_t *length) {
  const char *terminator;
  if (!id || !id[0] || !length) return TURBO_EINVAL;
  terminator = (const char *)memchr(id, '\0', TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX + 1u);
  if (!terminator) return TURBO_ENAMETOOLONG;
  *length = (size_t)(terminator - id);
  return *length > 0u && *length <= TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX ? TURBO_OK : TURBO_EINVAL;
}

int turbo_flow_fmq_broker_logical_address_validate(
    const turbo_flow_fmq_broker_logical_address_t *address) {
  size_t broker_len;
  size_t client_len;
  if (!address || address->size < sizeof(*address) ||
      address->version != TURBO_FLOW_FMQ_BROKER_PROTOCOL_VERSION || address->request_id == 0u) {
    return TURBO_EINVAL;
  }
  if (flow_fmq_broker_logical_id_length(address->origin_broker_id, &broker_len) != TURBO_OK ||
      flow_fmq_broker_logical_id_length(address->client_id, &client_len) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

int turbo_flow_fmq_broker_logical_address_encode(
    const turbo_flow_fmq_broker_logical_address_t *address, uint8_t *out, size_t capacity,
    size_t *out_len) {
  size_t broker_len;
  size_t client_len;
  size_t required;
  int rc;
  if (!out_len) return TURBO_EINVAL;
  *out_len = 0u;
  rc = turbo_flow_fmq_broker_logical_address_validate(address);
  if (rc != TURBO_OK) return rc;
  (void)flow_fmq_broker_logical_id_length(address->origin_broker_id, &broker_len);
  (void)flow_fmq_broker_logical_id_length(address->client_id, &client_len);
  required = TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE + broker_len + client_len;
  *out_len = required;
  if (!out || capacity < required) return TURBO_ENOSPC;

  memcpy(out, FLOW_FMQ_BROKER_ENVELOPE_MAGIC, sizeof(FLOW_FMQ_BROKER_ENVELOPE_MAGIC));
  flow_fmq_broker_write_u16(out + 4u, TURBO_FLOW_FMQ_BROKER_PROTOCOL_VERSION);
  flow_fmq_broker_write_u16(out + 6u, TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE);
  flow_fmq_broker_write_u16(out + 8u, (uint16_t)broker_len);
  flow_fmq_broker_write_u16(out + 10u, (uint16_t)client_len);
  flow_fmq_broker_write_u32(out + 12u, 0u);
  flow_fmq_broker_write_u64(out + 16u, address->request_id);
  memcpy(out + TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE, address->origin_broker_id, broker_len);
  memcpy(out + TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE + broker_len, address->client_id,
         client_len);
  return TURBO_OK;
}

int turbo_flow_fmq_broker_logical_address_decode(const uint8_t *data, size_t data_len,
                                                 turbo_flow_fmq_broker_logical_address_t *out) {
  turbo_flow_fmq_broker_logical_address_t decoded = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
  size_t broker_len;
  size_t client_len;
  size_t expected;
  if (!data || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  if (data_len < TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE ||
      memcmp(data, FLOW_FMQ_BROKER_ENVELOPE_MAGIC, sizeof(FLOW_FMQ_BROKER_ENVELOPE_MAGIC)) != 0 ||
      flow_fmq_broker_read_u16(data + 4u) != TURBO_FLOW_FMQ_BROKER_PROTOCOL_VERSION ||
      flow_fmq_broker_read_u16(data + 6u) != TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE ||
      flow_fmq_broker_read_u32(data + 12u) != 0u) {
    return TURBO_EPROTO;
  }
  broker_len = flow_fmq_broker_read_u16(data + 8u);
  client_len = flow_fmq_broker_read_u16(data + 10u);
  if (broker_len == 0u || broker_len > TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX || client_len == 0u ||
      client_len > TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX) {
    return TURBO_EPROTO;
  }
  expected = TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE + broker_len + client_len;
  if (data_len != expected || flow_fmq_broker_read_u64(data + 16u) == 0u ||
      memchr(data + TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE, '\0', broker_len) ||
      memchr(data + TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE + broker_len, '\0', client_len)) {
    return TURBO_EPROTO;
  }
  memcpy(decoded.origin_broker_id, data + TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE, broker_len);
  memcpy(decoded.client_id, data + TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE + broker_len,
         client_len);
  decoded.request_id = flow_fmq_broker_read_u64(data + 16u);
  *out = decoded;
  return TURBO_OK;
}
