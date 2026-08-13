#include "zmq_style_common.h"

#include <inttypes.h>
#include <stdio.h>

#define FMQ_REQ_REP_DEFAULT_PORT 7712u

typedef struct reply_state_s {
  atomic_int received;
} reply_state_t;

static int on_request(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *message, void *ctx) {
  static const char reply[] = "world";
  (void)app;
  (void)ctx;
  if (!message) return TURBO_EINVAL;
  printf("REP received: ");
  if (message->payload.len > 0u)
    (void)fwrite(message->payload.data, 1u, message->payload.len, stdout);
  printf("\n");
  return turbo_flow_fmq_app_message_set_payload_copy(message, reply, sizeof(reply) - 1u);
}

static int on_reply(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *message, void *ctx) {
  reply_state_t *state = (reply_state_t *)ctx;
  uint64_t correlation_id;
  int rc;
  (void)app;
  if (!message || !state) return TURBO_EINVAL;
  rc = turbo_flow_fmq_message_correlation_id(message, &correlation_id);
  if (rc != TURBO_OK) return rc;
  printf("REQ received correlation=%" PRIu64 " payload=", correlation_id);
  if (message->payload.len > 0u)
    (void)fwrite(message->payload.data, 1u, message->payload.len, stdout);
  printf("\n");
  atomic_fetch_add_explicit(&state->received, 1, memory_order_release);
  return TURBO_OK;
}

int main(int argc, char **argv) {
  static const char request[] = "hello";
  turbo_flow_fmq_config_t rep_endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_config_t req_endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_app_options_t rep_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_options_t req_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_t *server = NULL;
  turbo_flow_fmq_app_t *client = NULL;
  reply_state_t state;
  unsigned short port;
  int server_started = 0;
  int client_started = 0;
  int rc;

  atomic_init(&state.received, 0);
  rc = fmq_example_parse_port(argc, argv, FMQ_REQ_REP_DEFAULT_PORT, &port);
  if (rc != TURBO_OK) {
    fprintf(stderr, "usage: %s [port]\n", argv[0]);
    return 2;
  }

  rep_endpoint.pattern = TURBO_FLOW_FMQ_REP;
  rep_endpoint.mode = TURBO_FLOW_FMQ_BIND;
  rep_endpoint.transport = TURBO_FLOW_FMQ_TCP;
  rep_endpoint.host = "127.0.0.1";
  rep_endpoint.port = (int)port;
  rep_options.on_message = on_request;

  req_endpoint.pattern = TURBO_FLOW_FMQ_REQ;
  req_endpoint.mode = TURBO_FLOW_FMQ_CONNECT;
  req_endpoint.transport = TURBO_FLOW_FMQ_TCP;
  req_endpoint.host = "127.0.0.1";
  req_endpoint.port = (int)port;
  req_options.on_message = on_reply;
  req_options.message_ctx = &state;

  rc = turbo_flow_fmq_app_create(&rep_endpoint, &rep_options, &server);
  if (rc == TURBO_OK) rc = turbo_flow_fmq_app_create(&req_endpoint, &req_options, &client);
  if (rc == TURBO_OK) {
    rc = turbo_flow_fmq_app_start(server);
    server_started = rc == TURBO_OK;
  }
  if (rc == TURBO_OK) {
    rc = turbo_flow_fmq_app_start(client);
    client_started = rc == TURBO_OK;
  }
  if (rc == TURBO_OK)
    rc = fmq_example_send_when_connected(client, request, sizeof(request) - 1u);
  if (rc == TURBO_OK) rc = fmq_example_wait_for(&state.received, 1);

  if (client_started) {
    int stop_rc = turbo_flow_fmq_app_stop(client);
    if (rc == TURBO_OK) rc = stop_rc;
  }
  if (server_started) {
    int stop_rc = turbo_flow_fmq_app_stop(server);
    if (rc == TURBO_OK) rc = stop_rc;
  }
  turbo_flow_fmq_app_destroy(client);
  turbo_flow_fmq_app_destroy(server);
  if (rc != TURBO_OK) {
    fprintf(stderr, "REQ/REP example failed: %d\n", rc);
    return 1;
  }
  return 0;
}
