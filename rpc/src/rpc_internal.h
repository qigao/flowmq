#ifndef FLOWMQ_RPC_INTERNAL_H
#define FLOWMQ_RPC_INTERNAL_H
#include "flowmq_rpc.h"
#include <core/error.h>
#include <string.h>

bool rpc_string_is(const json_value_t *value, const char *text);
bool rpc_valid_id(const json_value_t *id);
bool rpc_same_id(const json_value_t *left, const json_value_t *right);
bool rpc_valid_params(const json_value_t *params);
bool rpc_valid_name(const char *name);
/* add consumes value on every path */
bool rpc_add(json_value_t *object, const char *key, json_value_t *value);
json_value_t *rpc_envelope(const json_value_t *id);
/* A nonempty document is one UTF-8 MDP part. */
int rpc_append(flowmq_mdp_body *body, const char *data, size_t length);
int rpc_append_json(flowmq_mdp_body *body, const json_value_t *value);
json_value_t *rpc_parse_part(const flowmq_mdp_body *body);

typedef struct rpc_document {
  json_value_t *root;
  size_t count;
  bool batch;
} rpc_document;
typedef struct rpc_member {
  const json_value_t *method;
  flowmq_rpc_invocation input;
} rpc_member;
/* A count of zero indicates a terminal parse/shape response or MDP error. */
int rpc_document_open(const flowmq_mdp_task *task, rpc_document *doc, flowmq_mdp_body *reply);
bool rpc_document_member(const rpc_document *doc, size_t index, flowmq_mdp_id id,
                         rpc_member *member);
const flowmq_rpc_method *rpc_find_method(const flowmq_rpc_server *server, const json_value_t *name);
/* Consumes output.value. Invalid request has a null member. */
int rpc_member_reply(const rpc_member *member, bool batch, flowmq_mdp_body *body, int32_t code,
                     flowmq_rpc_output output);
#endif
