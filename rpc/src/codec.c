#include "rpc_internal.h"

bool rpc_string_is(const json_value_t *value, const char *text) {
  size_t len = strlen(text);
  return value && json_type(value) == JSON_STRING && json_string_len(value) == len &&
         !memcmp(json_string(value), text, len);
}

bool rpc_valid_name(const char *name) {
  return name && strncmp(name, "rpc.", 4) &&
         vstr_utf8_valid((vstr){.data = (char *)name, .len = strlen(name)});
}

bool rpc_valid_params(const json_value_t *p) {
  return !p || json_type(p) == JSON_OBJECT || json_type(p) == JSON_ARRAY;
}

bool rpc_valid_id(const json_value_t *id) {
  return id && (json_type(id) == JSON_STRING || json_type(id) == JSON_NUMBER ||
                json_type(id) == JSON_NULL);
}

bool rpc_same_id(const json_value_t *left, const json_value_t *right) {
  if (!rpc_valid_id(left) || !right || json_type(left) != json_type(right)) return false;
  if (json_type(left) == JSON_NULL) return true;
  if (json_type(left) == JSON_STRING)
    return json_string_len(left) == json_string_len(right) &&
           !memcmp(json_string(left), json_string(right), json_string_len(left));
  /* Preserve the decimal token, including values beyond IEEE-754 precision. */
  size_t an = 0, bn = 0;
  const char *a = json_number_text(left, &an), *b = json_number_text(right, &bn);
  return a && b && an == bn && !memcmp(a, b, an);
}

bool rpc_add(json_value_t *object, const char *key, json_value_t *value) {
  if (value && json_object_add_checked(object, key, value)) return true;
  json_free(value);
  return false;
}

json_value_t *rpc_envelope(const json_value_t *id) {
  json_value_t *obj = json_create_object();
  if (!obj) return NULL;
  if (!rpc_add(obj, "jsonrpc", json_create_string("2.0")) ||
      (id && !rpc_add(obj, "id", json_clone(id)))) {
    json_free(obj);
    return NULL;
  }
  return obj;
}

int rpc_append(flowmq_mdp_body *body, const char *data, size_t len) {
  if (len > sizeof(body->bytes) - body->size) return SALTS_EMSGSIZE;
  memcpy(body->bytes + body->size, data, len);
  body->size += (uint32_t)len;
  body->lengths[0] = body->size;
  body->count = 1;
  return SALTS_OK;
}

/* Bound the input to the SDK's allocating serializer. Escaping may expand a
 * string by up to six bytes per input byte; rpc_append checks the exact size. */
static int charge(size_t *remaining, size_t bytes) {
  if (bytes > *remaining) return SALTS_EMSGSIZE;
  *remaining -= bytes;
  return SALTS_OK;
}
static int check_text(vstr text, size_t *remaining) {
  int rc = charge(remaining, text.len);
  if (rc != SALTS_OK) return rc;
  return vstr_utf8_valid(text) ? SALTS_OK : SALTS_EINVAL;
}
static int check_value(const json_value_t *value, size_t depth, size_t *remaining) {
  if (depth > FLOWMQ_RPC_DEPTH_MAX || charge(remaining, 1) != SALTS_OK) return SALTS_EMSGSIZE;
  json_type_t type = json_type(value);
  if (type == JSON_STRING) return check_text(json_string_v(value), remaining);
  if (type == JSON_NUMBER) {
    size_t len = 0;
    const char *token = json_number_text(value, &len);
    if (!token || !len) return SALTS_EINVAL;
    int rc = charge(remaining, len - 1);
    if (rc != SALTS_OK) return rc;
    size_t first = token[0] == '-' ? 1u : 0u;
    return first < len && token[first] >= '0' && token[first] <= '9' ? SALTS_OK : SALTS_EINVAL;
  }
  if (type != JSON_ARRAY && type != JSON_OBJECT) return SALTS_OK;
  size_t n = type == JSON_ARRAY ? json_array_size(value) : json_object_size(value);
  if (n > *remaining) return SALTS_EMSGSIZE;
  for (size_t i = 0; i < n; ++i) {
    const json_value_t *child;
    if (type == JSON_OBJECT) {
      int rc = check_text(json_object_key_v(value, i), remaining);
      if (rc != SALTS_OK) return rc;
      child = json_object_value(value, i);
    } else child = json_array_get(value, i);
    int rc = check_value(child, depth + 1, remaining);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}

int rpc_append_json(flowmq_mdp_body *body, const json_value_t *value) {
  size_t remaining = sizeof(body->bytes) - body->size;
  int rc = check_value(value, 0, &remaining);
  if (rc != SALTS_OK) return rc;
  size_t len;
  char *text = json_serialize(value, &len);
  if (!text) return SALTS_ENOMEM;
  rc = rpc_append(body, text, len);
  json_serialize_free(text);
  return rc;
}

json_value_t *rpc_parse_part(const flowmq_mdp_body *body) {
  if (body->count != 1 || body->size == 0 || body->size > sizeof(body->bytes) ||
      body->lengths[0] != body->size || memchr(body->bytes, 0, body->size))
    return NULL;
  const char *data = (const char *)body->bytes;
  if (!vstr_utf8_valid((vstr){.data = (char *)data, .len = body->size})) return NULL;
  return json_parse(data, body->size);
}
