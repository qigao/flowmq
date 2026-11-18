#include "rpc_internal.h"

static int validate_set(const flowmq_rpc_request *requests, size_t count, bool batch) {
  if (!requests || !count || (!batch && count != 1)) return SALTS_EINVAL;
  if (count > FLOWMQ_RPC_BATCH_MAX) return SALTS_EMSGSIZE;
  for (size_t i = 0; i < count; ++i) {
    if (!requests[i].id) continue;
    if (!rpc_valid_id(requests[i].id)) return SALTS_EINVAL;
    for (size_t j = 0; j < i; ++j)
      if (rpc_same_id(requests[i].id, requests[j].id)) return SALTS_EINVAL;
  }
  return SALTS_OK;
}

static int encode_request(flowmq_mdp_body *body, const flowmq_rpc_request *request) {
  if (!rpc_valid_name(request->method) || !rpc_valid_params(request->params)) return SALTS_EINVAL;
  const char prefix[] = "{\"jsonrpc\":\"2.0\",\"method\":";
  if (strlen(request->method) > sizeof(body->bytes)) return SALTS_EMSGSIZE;
  int rc = rpc_append(body, prefix, sizeof(prefix) - 1);
  if (rc != SALTS_OK) return rc;
  json_value_t *name = json_create_string(request->method);
  if (!name) return SALTS_ENOMEM;
  rc = rpc_append_json(body, name);
  json_free(name);
  if (rc == SALTS_OK && request->id) {
    rc = rpc_append(body, ",\"id\":", 6);
    if (rc == SALTS_OK) rc = rpc_append_json(body, request->id);
  }
  if (rc == SALTS_OK && request->params) {
    rc = rpc_append(body, ",\"params\":", 10);
    if (rc == SALTS_OK) rc = rpc_append_json(body, request->params);
  }
  if (rc == SALTS_OK) rc = rpc_append(body, "}", 1);
  return rc;
}

int flowmq_rpc_call(flowmq_mdp_client *client, const char *service,
                    const flowmq_rpc_request *requests, size_t count, bool batch, uint64_t now,
                    uint32_t budget, flowmq_mdp_id *handle) {
  int rc = validate_set(requests, count, batch);
  if (rc != SALTS_OK) return rc;
  flowmq_mdp_body body = {0};
  if (batch) rc = rpc_append(&body, "[", 1);
  for (size_t i = 0; i < count && rc == SALTS_OK; ++i) {
    if (i) rc = rpc_append(&body, ",", 1);
    if (rc == SALTS_OK) rc = encode_request(&body, &requests[i]);
  }
  if (rc == SALTS_OK && batch) rc = rpc_append(&body, "]", 1);
  if (rc != SALTS_OK) return rc;
  return flowmq_mdp_call(client, service, &body, now, budget, handle);
}

static bool valid_response(const json_value_t *obj) {
  if (!obj || json_type(obj) != JSON_OBJECT ||
      !rpc_string_is(json_object_get(obj, "jsonrpc"), "2.0") ||
      !rpc_valid_id(json_object_get(obj, "id")))
    return false;
  const json_value_t *value = json_object_get(obj, "result"),
                     *error = json_object_get(obj, "error");
  if (value) return !error;
  if (!error || json_type(error) != JSON_OBJECT ||
      json_type(json_object_get(error, "message")) != JSON_STRING)
    return false;
  size_t len = 0;
  const char *code = json_number_text(json_object_get(error, "code"), &len);
  if (!code || !len) return false;
  for (size_t i = code[0] == '-' ? 1u : 0u; i < len; ++i)
    if (code[i] < '0' || code[i] > '9') return false;
  return true;
}

int flowmq_rpc_decode(const flowmq_mdp_result *result, const flowmq_rpc_request *requests,
                      size_t count, bool batch, json_value_t **out) {
  if (!result || !out) return SALTS_EINVAL;
  *out = NULL;
  int rc = validate_set(requests, count, batch);
  if (rc != SALTS_OK) return rc;
  if (result->outcome != FLOWMQ_MDP_RESULT) return SALTS_EPROTO;
  if (result->status != SALTS_OK) return result->status;
  size_t expected = 0;
  for (size_t i = 0; i < count; ++i)
    if (requests[i].id) ++expected;
  if (!expected) return result->body.count == 0 && result->body.size == 0 ? SALTS_OK : SALTS_EPROTO;
  json_value_t *root = rpc_parse_part(&result->body);
  if (!root) return SALTS_EPROTO;
  bool seen[FLOWMQ_RPC_BATCH_MAX] = {0};
  bool valid = !batch || (json_type(root) == JSON_ARRAY && json_array_size(root) == expected);
  for (size_t i = 0; valid && i < expected; ++i) {
    const json_value_t *obj = batch ? json_array_get(root, i) : root;
    valid = false;
    if (!valid_response(obj)) break;
    for (size_t j = 0; j < count; ++j) {
      if (!seen[j] && rpc_same_id(requests[j].id, json_object_get(obj, "id"))) {
        seen[j] = true;
        valid = true;
        break;
      }
    }
  }
  if (!valid) {
    json_free(root);
    return SALTS_EPROTO;
  }
  *out = root;
  return SALTS_OK;
}
