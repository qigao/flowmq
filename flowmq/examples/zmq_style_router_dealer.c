#include "zmq_style_common.h"

#include <stdio.h>

#define FMQ_ROUTER_DEALER_DEFAULT_PORT 7713u

typedef struct router_state_s {
  turbo_flow_msg_t pending;
  atomic_int ready;
  atomic_int capture_status;
} router_state_t;

typedef struct dealer_state_s {
  atomic_int received;
} dealer_state_t;

static int on_router_request(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *message, void *ctx) {
  router_state_t *state = (router_state_t *)ctx;
  int rc;
  (void)app;
  if (!message || !state) return TURBO_EINVAL;
  rc = turbo_flow_fmq_message_detach_router_route(message);
  if (rc == TURBO_OK) rc = turbo_flow_msg_clone(&state->pending, message);
  atomic_store_explicit(&state->capture_status, rc, memory_order_relaxed);
  atomic_store_explicit(&state->ready, 1, memory_order_release);
  return rc;
}

static int on_dealer_reply(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *message, void *ctx) {
  dealer_state_t *state = (dealer_state_t *)ctx;
  (void)app;
  if (!message || !state) return TURBO_EINVAL;
  printf("DEALER received delayed reply: ");
  if (message->payload.len > 0u)
    (void)fwrite(message->payload.data, 1u, message->payload.len, stdout);
  printf("\n");
  atomic_fetch_add_explicit(&state->received, 1, memory_order_release);
  return TURBO_OK;
}

int main(int argc, char **argv) {
  static const char request[] = "job-17";
  static const char reply[] = "job-17-complete";
  turbo_flow_fmq_config_t router_endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_config_t dealer_endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_app_options_t router_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_options_t dealer_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_t *router = NULL;
  turbo_flow_fmq_app_t *dealer = NULL;
  router_state_t router_state;
  dealer_state_t dealer_state;
  unsigned short port;
  int router_started = 0;
  int dealer_started = 0;
  int rc;

  turbo_flow_msg_init(&router_state.pending);
  atomic_init(&router_state.ready, 0);
  atomic_init(&router_state.capture_status, TURBO_EBUSY);
  atomic_init(&dealer_state.received, 0);
  rc = fmq_example_parse_port(argc, argv, FMQ_ROUTER_DEALER_DEFAULT_PORT, &port);
  if (rc != TURBO_OK) {
    fprintf(stderr, "usage: %s [port]\n", argv[0]);
    turbo_flow_msg_cleanup(&router_state.pending);
    return 2;
  }

  router_endpoint.pattern = TURBO_FLOW_FMQ_ROUTER;
  router_endpoint.mode = TURBO_FLOW_FMQ_BIND;
  router_endpoint.transport = TURBO_FLOW_FMQ_TCP;
  router_endpoint.host = "127.0.0.1";
  router_endpoint.port = (int)port;
  router_options.on_message = on_router_request;
  router_options.message_ctx = &router_state;

  dealer_endpoint.pattern = TURBO_FLOW_FMQ_DEALER;
  dealer_endpoint.mode = TURBO_FLOW_FMQ_CONNECT;
  dealer_endpoint.transport = TURBO_FLOW_FMQ_TCP;
  dealer_endpoint.host = "127.0.0.1";
  dealer_endpoint.port = (int)port;
  dealer_endpoint.identity = "worker-17";
  dealer_options.on_message = on_dealer_reply;
  dealer_options.message_ctx = &dealer_state;

  rc = turbo_flow_fmq_app_create(&router_endpoint, &router_options, &router);
  if (rc == TURBO_OK)
    rc = turbo_flow_fmq_app_create(&dealer_endpoint, &dealer_options, &dealer);
  if (rc == TURBO_OK) {
    rc = turbo_flow_fmq_app_start(router);
    router_started = rc == TURBO_OK;
  }
  if (rc == TURBO_OK) {
    rc = turbo_flow_fmq_app_start(dealer);
    dealer_started = rc == TURBO_OK;
  }
  if (rc == TURBO_OK)
    rc = fmq_example_send_when_connected(dealer, request, sizeof(request) - 1u);
  if (rc == TURBO_OK) rc = fmq_example_wait_for(&router_state.ready, 1);
  if (rc == TURBO_OK)
    rc = atomic_load_explicit(&router_state.capture_status, memory_order_acquire);
  if (rc == TURBO_OK)
    rc = turbo_flow_fmq_app_message_set_payload_copy(&router_state.pending, reply,
                                                     sizeof(reply) - 1u);
  if (rc == TURBO_OK) rc = turbo_flow_fmq_app_send_message(router, &router_state.pending);
  if (rc == TURBO_OK) rc = fmq_example_wait_for(&dealer_state.received, 1);

  if (dealer_started) {
    int stop_rc = turbo_flow_fmq_app_stop(dealer);
    if (rc == TURBO_OK) rc = stop_rc;
  }
  if (router_started) {
    int stop_rc = turbo_flow_fmq_app_stop(router);
    if (rc == TURBO_OK) rc = stop_rc;
  }
  turbo_flow_msg_cleanup(&router_state.pending);
  turbo_flow_fmq_app_destroy(dealer);
  turbo_flow_fmq_app_destroy(router);
  if (rc != TURBO_OK) {
    fprintf(stderr, "ROUTER/DEALER example failed: %d\n", rc);
    return 1;
  }
  return 0;
}
