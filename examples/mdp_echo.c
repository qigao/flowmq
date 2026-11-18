/* One finite loopback exchange through the public client, broker and worker. */
#include "flowmq_mdp.h"
#include "flowmq_rpc.h"
#include <core/clock.h>
#include <core/error.h>
#include <stdio.h>
#include <string.h>

static int echo_allowed(void *context, int worker, const void *identity, size_t size,
                        const char *service) {
  (void)context;
  const char *expected = worker ? "echo-worker" : "echo-client";
  return !strcmp(service, "echo") && size == strlen(expected) && !memcmp(identity, expected, size);
}

static int link_peer(flowmq_socket_t *router, flowmq_socket_t *dealer, const char *identity,
                     const char *endpoint) {
  char address[512];
  size_t length = 0;
  int rc = flowmq_bind(router, endpoint);
  if (rc == SALTS_OK) rc = flowmq_last_endpoint(router, address, sizeof(address), &length);
  if (rc == SALTS_OK) rc = flowmq_setsockopt(dealer, FLOWMQ_IDENTITY, identity, strlen(identity));
  if (rc == SALTS_OK) rc = flowmq_connect(dealer, address);
  return rc;
}

static int32_t echo_text(void *context, const flowmq_rpc_invocation *request,
                         flowmq_rpc_output *out) {
  (void)context;
  const json_value_t *text = json_array_get(request->params, 0);
  if (json_array_size(request->params) != 1 || !text || json_type(text) != JSON_STRING)
    return FLOWMQ_RPC_INVALID_PARAMS;
  out->value = json_clone(text);
  return out->value ? 0 : FLOWMQ_RPC_INTERNAL_ERROR;
}

static int call_echo(flowmq_mdp_client *client, flowmq_rpc_request *request, const char *text,
                     flowmq_mdp_id *handle) {
  json_value_t *args = json_create_array();
  json_value_t *value = json_create_string(text);
  int rc = SALTS_ENOMEM;
  if (args && value && json_array_add_checked(args, value)) {
    request->params = args;
    rc = flowmq_rpc_call(client, "echo", request, 1, false, monotonic_ms(), 5000, handle);
    request->params = NULL;
  } else json_free(value);
  json_free(args);
  return rc;
}

static int print_rpc_result(const flowmq_mdp_result *result, const flowmq_rpc_request *request,
                            const char *expected) {
  json_value_t *response = NULL;
  if (result->outcome != FLOWMQ_MDP_RESULT) return SALTS_EPROTO;
  int rc = flowmq_rpc_decode(result, request, 1, false, &response);
  if (rc == SALTS_OK) {
    const json_value_t *value = json_object_get(response, "result");
    if (json_type(value) == JSON_STRING && json_string_len(value) == strlen(expected) &&
        !memcmp(json_string(value), expected, strlen(expected)))
      printf("%s\n", json_string(value));
    else rc = SALTS_EPROTO;
  }
  json_free(response);
  return rc;
}

int main(int argc, char **argv) {
  bool rpc = (argc == 3 || argc == 5) && !strcmp(argv[1], "--rpc");
  if (rpc) {
    ++argv;
    --argc;
  }
  if (argc > 2 && argc != 4) {
    fprintf(stderr, "usage: flowmq_mdp_echo [[--rpc] text [client-endpoint worker-endpoint]]\n");
    return 2;
  }
  const char *text = argc >= 2 ? argv[1] : "hello";
  const char *front = argc == 4 ? argv[2] : "tcp://127.0.0.1:0";
  const char *back = argc == 4 ? argv[3] : "tcp://127.0.0.1:0";
  flowmq_ctx_t *ctx = flowmq_ctx_new();
  if (!ctx) return 1;
  flowmq_socket_t *sockets[4] = {0};
  flowmq_pollitem_t items[4] = {0};
  flowmq_mdp_client *client = NULL;
  flowmq_mdp_worker *worker = NULL;
  flowmq_mdp_broker *broker = NULL;
  flowmq_mdp_body input = {0};
  flowmq_mdp_id id = {0};
  flowmq_rpc_server *server = NULL;
  const flowmq_rpc_method method = {"echo.text", echo_text, NULL};
  json_value_t *rpc_id = rpc ? json_create_int64(1) : NULL;
  flowmq_rpc_request request = {method.name, NULL, rpc_id};
  /* This example owns one ephemeral deployment per process invocation. */
  const flowmq_mdp_broker_config config = {1, 8, 1, 4, 1000, echo_allowed, NULL, 8};
  int rc = SALTS_OK;
  if (rpc) rc = rpc_id ? flowmq_rpc_server_open(&method, 1, &server) : SALTS_ENOMEM;
  else rc = flowmq_mdp_add_part(&input, text, strlen(text));
  for (size_t i = 0; rc == SALTS_OK && i < 4; ++i) {
    sockets[i] = flowmq_socket(ctx, i < 2 ? FLOWMQ_ROUTER : FLOWMQ_DEALER);
    if (!sockets[i]) rc = SALTS_ENOMEM;
    items[i] = (flowmq_pollitem_t){sockets[i], FLOWMQ_POLLIN, 0};
  }
  if (rc == SALTS_OK) rc = link_peer(sockets[0], sockets[2], "echo-client", front);
  if (rc == SALTS_OK) rc = link_peer(sockets[1], sockets[3], "echo-worker", back);
  if (rc == SALTS_OK) rc = flowmq_mdp_broker_open(sockets[0], sockets[1], &config, &broker);
  if (rc == SALTS_OK) rc = flowmq_mdp_worker_open(sockets[3], "echo", 2, 1, 100, &worker);
  if (rc == SALTS_OK) rc = flowmq_mdp_client_open(sockets[2], 3, 1, &client);
  if (rc == SALTS_OK)
    rc = rpc ? call_echo(client, &request, text, &id)
             : flowmq_mdp_call(client, "echo", &input, monotonic_ms(), 5000, &id);
  while (rc == SALTS_OK) {
    size_t ready = 0;
    rc = flowmq_poll(items, 4, 1, &ready);
    uint64_t now = monotonic_ms();
    if (rc == SALTS_OK) rc = flowmq_mdp_client_step(client, now);
    if (rc == SALTS_OK) rc = flowmq_mdp_worker_step(worker, now);
    if (rc == SALTS_OK) rc = flowmq_mdp_broker_step(broker, now);
    if (rc != SALTS_OK) break;
    flowmq_mdp_task task;
    if (rpc) rc = flowmq_rpc_execute(server, worker);
    else {
      rc = flowmq_mdp_take_task(worker, &task);
      if (rc == SALTS_OK) rc = flowmq_mdp_complete(worker, &task, 0, &task.body);
    }
    if (rc == SALTS_EBUSY) rc = SALTS_OK;
    if (rc != SALTS_OK) break;
    flowmq_mdp_result result;
    rc = flowmq_mdp_take_result(client, id, &result);
    if (rc == SALTS_EBUSY) {
      rc = SALTS_OK;
      continue;
    }
    if (rc == SALTS_OK && rpc) rc = print_rpc_result(&result, &request, text);
    else if (rc == SALTS_OK) {
      if (result.outcome != FLOWMQ_MDP_RESULT || result.status != 0 || result.body.count != 1 ||
          result.body.size != input.size || memcmp(result.body.bytes, input.bytes, input.size))
        rc = SALTS_EPROTO;
      else printf("%.*s\n", (int)result.body.size, (const char *)result.body.bytes);
    }
    break;
  }
  json_free(rpc_id);
  flowmq_rpc_server_close(server);
  flowmq_mdp_worker_retire(worker);
  if (worker) {
    int close = flowmq_mdp_worker_close(worker);
    if (rc == SALTS_OK) rc = close;
  }
  flowmq_mdp_client_close(client);
  flowmq_mdp_broker_close(broker);
  for (size_t i = 0; i < 4; ++i)
    if (sockets[i]) {
      int close = flowmq_close(sockets[i]);
      if (rc == SALTS_OK) rc = close;
    }
  int close = flowmq_ctx_term(ctx);
  if (rc == SALTS_OK) rc = close;
  if (rc != SALTS_OK) fprintf(stderr, "MDP echo failed (%d)\n", rc);
  return rc == SALTS_OK ? 0 : 1;
}
