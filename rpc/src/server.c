#include "rpc_internal.h"
#include <stdlib.h>

struct flowmq_rpc_server {
  const flowmq_rpc_method *methods;
  size_t count;
};

int flowmq_rpc_server_open(const flowmq_rpc_method *methods, size_t count,
                           flowmq_rpc_server **out) {
  if (!out || !methods || !count || count > FLOWMQ_RPC_METHODS_MAX) return SALTS_EINVAL;
  for (size_t i = 0; i < count; ++i) {
    if (!rpc_valid_name(methods[i].name) || !methods[i].handler) return SALTS_EINVAL;
    for (size_t j = 0; j < i; ++j)
      if (!strcmp(methods[i].name, methods[j].name)) return SALTS_EINVAL;
  }
  flowmq_rpc_server *server = malloc(sizeof(*server));
  if (!server) return SALTS_ENOMEM;
  *server = (flowmq_rpc_server){methods, count};
  *out = server;
  return SALTS_OK;
}

void flowmq_rpc_server_close(flowmq_rpc_server *server) { free(server); }

static const char *error_message(int32_t code) {
  switch (code) {
  case FLOWMQ_RPC_PARSE_ERROR:
    return "Parse error";
  case FLOWMQ_RPC_INVALID_REQUEST:
    return "Invalid Request";
  case FLOWMQ_RPC_METHOD_NOT_FOUND:
    return "Method not found";
  case FLOWMQ_RPC_INVALID_PARAMS:
    return "Invalid params";
  case FLOWMQ_RPC_INTERNAL_ERROR:
    return "Internal error";
  case FLOWMQ_RPC_OVERLOADED:
    return "Request capacity exceeded";
  default:
    return "Application error";
  }
}

/* Consumes output.value even when a response cannot be constructed. */
static int emit_response(flowmq_mdp_body *body, bool batch, const json_value_t *id, int32_t code,
                         flowmq_rpc_output output) {
  json_value_t *obj = rpc_envelope(id);
  json_value_t *error = NULL;
  int rc = SALTS_ENOMEM;
  if (!obj || (!id && !rpc_add(obj, "id", json_create_null()))) goto done;
  if (code) {
    if (output.message && strlen(output.message) > sizeof(body->bytes)) {
      rc = SALTS_EMSGSIZE;
      goto done;
    }
    error = json_create_object();
    if (!error || !rpc_add(error, "code", json_create_int64(code)) ||
        !rpc_add(error, "message",
                 json_create_string(output.message ? output.message : error_message(code))))
      goto done;
    if (output.value && !json_object_add_checked(error, "data", output.value)) goto done;
    output.value = NULL;
    if (!json_object_add_checked(obj, "error", error)) goto done;
    error = NULL;
  } else {
    if (!json_object_add_checked(obj, "result", output.value)) goto done;
    output.value = NULL;
  }
  rc = SALTS_OK;
  if (batch) rc = rpc_append(body, body->size ? "," : "[", 1);
  if (rc == SALTS_OK) rc = rpc_append_json(body, obj);
done:
  json_free(output.value);
  json_free(error);
  json_free(obj);
  return rc;
}

const flowmq_rpc_method *rpc_find_method(const flowmq_rpc_server *server,
                                         const json_value_t *name) {
  if (server)
    for (size_t i = 0; i < server->count; ++i)
      if (rpc_string_is(name, server->methods[i].name)) return &server->methods[i];
  return NULL;
}

int rpc_document_open(const flowmq_mdp_task *task, rpc_document *doc, flowmq_mdp_body *reply) {
  if (task->body.count != 1) return SALTS_EPROTO;
  doc->root = rpc_parse_part(&task->body);
  if (!doc->root)
    return emit_response(reply, false, NULL, FLOWMQ_RPC_PARSE_ERROR, (flowmq_rpc_output){0});
  doc->batch = json_type(doc->root) == JSON_ARRAY;
  size_t n = doc->batch ? json_array_size(doc->root) : 1;
  if (!n)
    return emit_response(reply, false, NULL, FLOWMQ_RPC_INVALID_REQUEST, (flowmq_rpc_output){0});
  if (n > FLOWMQ_RPC_BATCH_MAX) return SALTS_EMSGSIZE;
  doc->count = n;
  return SALTS_OK;
}

bool rpc_document_member(const rpc_document *doc, size_t index, flowmq_mdp_id request,
                         rpc_member *member) {
  const json_value_t *item = doc->batch ? json_array_get(doc->root, index) : doc->root;
  member->method = json_object_get(item, "method");
  member->input = (flowmq_rpc_invocation){json_object_get(item, "params"),
                                          json_object_get(item, "id"), request};
  return json_type(item) == JSON_OBJECT && rpc_string_is(json_object_get(item, "jsonrpc"), "2.0") &&
         member->method && json_type(member->method) == JSON_STRING &&
         (!member->input.id || rpc_valid_id(member->input.id)) &&
         rpc_valid_params(member->input.params);
}

int rpc_member_reply(const rpc_member *member, bool batch, flowmq_mdp_body *body, int32_t code,
                     flowmq_rpc_output output) {
  if (member && !member->input.id) {
    json_free(output.value);
    return SALTS_OK;
  }
  if (!code && !output.value) code = FLOWMQ_RPC_INTERNAL_ERROR;
  return emit_response(body, batch, member ? member->input.id : NULL, code, output);
}

static int dispatch(const flowmq_rpc_server *server, const flowmq_mdp_task *task,
                    flowmq_mdp_body *body) {
  rpc_document doc = {0};
  int rc = rpc_document_open(task, &doc, body);
  for (size_t i = 0; i < doc.count; ++i) {
    rpc_member member;
    bool valid = rpc_document_member(&doc, i, task->id, &member);
    const flowmq_rpc_method *method = valid ? rpc_find_method(server, member.method) : NULL;
    flowmq_rpc_output output = {0};
    int32_t code = valid ? FLOWMQ_RPC_METHOD_NOT_FOUND : FLOWMQ_RPC_INVALID_REQUEST;
    if (method) code = method->handler(method->context, &member.input, &output);
    int status = rpc_member_reply(valid ? &member : NULL, doc.batch, body, code, output);
    if (rc == SALTS_OK) rc = status;
  }
  if (doc.count && doc.batch && rc == SALTS_OK && body->size) rc = rpc_append(body, "]", 1);
  json_free(doc.root);
  return rc;
}

int flowmq_rpc_execute(const flowmq_rpc_server *server, flowmq_mdp_worker *worker) {
  if (!server || !worker) return SALTS_EINVAL;
  flowmq_mdp_task task;
  int rc = flowmq_mdp_take_task(worker, &task);
  if (rc != SALTS_OK) return rc;
  flowmq_mdp_body reply = {0};
  rc = dispatch(server, &task, &reply);
  if (rc != SALTS_OK) reply = (flowmq_mdp_body){0};
  return flowmq_mdp_complete(worker, &task, rc, &reply);
}
