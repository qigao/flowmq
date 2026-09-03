#include "flowmq_security.h"

#include "salts_error.h"

#include <stdint.h>
#include <string.h>

typedef char flowmq_security_single_digit_wire_version[FLOWMQ_PROTOCOL_FMS_VERSION <= 9u ? 1 : -1];

static const unsigned char FLOWMQ_SECURITY_MAGIC[4] = {
    'F', 'M', 'S', (unsigned char)('0' + FLOWMQ_PROTOCOL_FMS_VERSION)};

static void flowmq_security_write_u32(unsigned char *out, uint32_t value) {
  out[0] = (unsigned char)(value >> 24);
  out[1] = (unsigned char)(value >> 16);
  out[2] = (unsigned char)(value >> 8);
  out[3] = (unsigned char)value;
}

static uint32_t flowmq_security_read_u32(const unsigned char *data) {
  return ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) | ((uint32_t)data[2] << 8) | data[3];
}

static int flowmq_security_validate(const flowmq_security_t *security) {
  if (!security) return SALTS_EINVAL;
  if ((security->identity.len > 0u && !security->identity.data) ||
      (security->method.len > 0u && !security->method.data) ||
      (security->secret.len > 0u && !security->secret.data) ||
      (security->channel_binding.len > 0u && !security->channel_binding.data)) {
    return SALTS_EINVAL;
  }
  if (security->identity.len > FLOWMQ_SECURITY_MAX_IDENTITY_SIZE ||
      security->method.len > FLOWMQ_SECURITY_MAX_AUTH_METHOD_SIZE ||
      security->secret.len > FLOWMQ_SECURITY_MAX_AUTH_SECRET_SIZE) {
    return SALTS_EMSGSIZE;
  }
  if ((security->identity.len != 0u &&
       memchr(security->identity.data, '\0', security->identity.len) != NULL) ||
      (security->method.len != 0u &&
       memchr(security->method.data, '\0', security->method.len) != NULL)) {
    return SALTS_EPROTO;
  }
  switch (security->mode) {
  case FLOWMQ_SECURITY_NONE:
    return security->identity.len == 0u && security->method.len == 0u &&
                   security->secret.len == 0u && security->channel_binding.len == 0u
               ? SALTS_OK
               : SALTS_EPROTO;
  case FLOWMQ_SECURITY_AUTH:
    return security->identity.len != 0u && security->method.len != 0u &&
                   security->secret.len != 0u &&
                   (security->channel_binding.len == 0u ||
                    security->channel_binding.len == FLOWMQ_SECURITY_CHANNEL_BINDING_SIZE)
               ? SALTS_OK
               : SALTS_EPROTO;
  case FLOWMQ_SECURITY_ACCEPTED:
    return security->identity.len == 0u && security->method.len == 0u &&
                   security->secret.len == 0u &&
                   (security->channel_binding.len == 0u ||
                    security->channel_binding.len == FLOWMQ_SECURITY_CHANNEL_BINDING_SIZE)
               ? SALTS_OK
               : SALTS_EPROTO;
  default:
    return SALTS_EPROTO;
  }
}

int flowmq_security_encode(const flowmq_security_t *security, tstr *payload) {
  size_t total;
  size_t offset;
  unsigned char *header;
  int rc;
  if (!payload || *payload) return SALTS_EINVAL;
  rc = flowmq_security_validate(security);
  if (rc != SALTS_OK) return rc;
  if (security->mode == FLOWMQ_SECURITY_NONE) {
    *payload = tstr_new_len(NULL, 0u);
    return *payload ? SALTS_OK : SALTS_ENOMEM;
  }
  total = FLOWMQ_SECURITY_HEADER_SIZE + security->identity.len + security->method.len +
          security->channel_binding.len + security->secret.len;
  *payload = tstr_new_len(NULL, total);
  if (!*payload) return SALTS_ENOMEM;
  header = (unsigned char *)*payload;
  memcpy(header, FLOWMQ_SECURITY_MAGIC, sizeof(FLOWMQ_SECURITY_MAGIC));
  header[4] = (unsigned char)security->mode;
  header[5] = (unsigned char)security->identity.len;
  header[6] = (unsigned char)security->method.len;
  header[7] = (unsigned char)security->channel_binding.len;
  flowmq_security_write_u32(header + 8u, (uint32_t)security->secret.len);
  offset = FLOWMQ_SECURITY_HEADER_SIZE;
  if (security->identity.len != 0u) {
    memcpy(*payload + offset, security->identity.data, security->identity.len);
    offset += security->identity.len;
  }
  if (security->method.len != 0u) {
    memcpy(*payload + offset, security->method.data, security->method.len);
    offset += security->method.len;
  }
  if (security->channel_binding.len != 0u) {
    memcpy(*payload + offset, security->channel_binding.data, security->channel_binding.len);
    offset += security->channel_binding.len;
  }
  if (security->secret.len != 0u) {
    memcpy(*payload + offset, security->secret.data, security->secret.len);
  }
  return SALTS_OK;
}

int flowmq_security_decode(vstr payload, flowmq_security_t *security) {
  const unsigned char *header = (const unsigned char *)payload.data;
  size_t identity_len;
  size_t method_len;
  size_t binding_len;
  size_t secret_len;
  size_t fields_size;
  size_t total;
  size_t offset;
  int rc;
  if (!security || (payload.len > 0u && !payload.data)) return SALTS_EINVAL;
  memset(security, 0, sizeof(*security));
  if (payload.len == 0u) return SALTS_OK;
  if (payload.len < FLOWMQ_SECURITY_HEADER_SIZE ||
      memcmp(header, FLOWMQ_SECURITY_MAGIC, sizeof(FLOWMQ_SECURITY_MAGIC)) != 0) {
    return SALTS_EPROTO;
  }
  security->mode = (flowmq_security_mode_t)header[4];
  if (security->mode == FLOWMQ_SECURITY_NONE) return SALTS_EPROTO;
  identity_len = header[5];
  method_len = header[6];
  binding_len = header[7];
  secret_len = flowmq_security_read_u32(header + 8u);
  if (identity_len > SIZE_MAX - method_len || identity_len + method_len > SIZE_MAX - binding_len ||
      identity_len + method_len + binding_len > SIZE_MAX - secret_len) {
    return SALTS_ERANGE;
  }
  fields_size = identity_len + method_len + binding_len + secret_len;
  if (FLOWMQ_SECURITY_HEADER_SIZE > SIZE_MAX - fields_size) return SALTS_ERANGE;
  total = FLOWMQ_SECURITY_HEADER_SIZE + fields_size;
  if (total != payload.len) return SALTS_EPROTO;
  offset = FLOWMQ_SECURITY_HEADER_SIZE;
  security->identity = vstr_from_buf(payload.data + offset, identity_len);
  offset += identity_len;
  security->method = vstr_from_buf(payload.data + offset, method_len);
  offset += method_len;
  security->channel_binding = vstr_from_buf(payload.data + offset, binding_len);
  offset += binding_len;
  security->secret = vstr_from_buf(payload.data + offset, secret_len);
  rc = flowmq_security_validate(security);
  if (rc != SALTS_OK) memset(security, 0, sizeof(*security));
  return rc;
}
