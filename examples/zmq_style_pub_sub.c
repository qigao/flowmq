#include "zmq_style_common.h"

#include <stdio.h>

#define FMQ_PUB_SUB_DEFAULT_PORT 7711u

typedef struct subscriber_state_s {
  atomic_int received;
} subscriber_state_t;

static int on_publication(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *message, void *ctx) {
  subscriber_state_t *state = (subscriber_state_t *)ctx;
  tstr_v topic = {0};
  int rc;
  (void)app;
  if (!message || !state) return TURBO_EINVAL;
  rc = turbo_flow_fmq_message_topic(message, &topic);
  if (rc != TURBO_OK) return rc;
  printf("SUB received topic=");
  if (topic.len > 0u) (void)fwrite(topic.data, 1u, topic.len, stdout);
  printf(" payload=");
  if (message->payload.len > 0u)
    (void)fwrite(message->payload.data, 1u, message->payload.len, stdout);
  printf("\n");
  atomic_fetch_add_explicit(&state->received, 1, memory_order_release);
  return TURBO_OK;
}

int main(int argc, char **argv) {
  static const char payload[] = "order-42";
  turbo_flow_fmq_config_t pub_endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_config_t sub_endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_app_options_t pub_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_options_t sub_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_t *publisher = NULL;
  turbo_flow_fmq_app_t *subscriber = NULL;
  subscriber_state_t state;
  unsigned short port;
  int publisher_started = 0;
  int subscriber_started = 0;
  int rc;

  atomic_init(&state.received, 0);
  rc = fmq_example_parse_port(argc, argv, FMQ_PUB_SUB_DEFAULT_PORT, &port);
  if (rc != TURBO_OK) {
    fprintf(stderr, "usage: %s [port]\n", argv[0]);
    return 2;
  }

  pub_endpoint.pattern = TURBO_FLOW_FMQ_PUB;
  pub_endpoint.mode = TURBO_FLOW_FMQ_BIND;
  pub_endpoint.transport = TURBO_FLOW_FMQ_TCP;
  pub_endpoint.host = "127.0.0.1";
  pub_endpoint.port = (int)port;
  pub_endpoint.topic = "orders.created";

  sub_endpoint.pattern = TURBO_FLOW_FMQ_SUB;
  sub_endpoint.mode = TURBO_FLOW_FMQ_CONNECT;
  sub_endpoint.transport = TURBO_FLOW_FMQ_TCP;
  sub_endpoint.host = "127.0.0.1";
  sub_endpoint.port = (int)port;
  sub_endpoint.topic = "orders.";
  sub_options.on_message = on_publication;
  sub_options.message_ctx = &state;

  rc = turbo_flow_fmq_app_create(&pub_endpoint, &pub_options, &publisher);
  if (rc == TURBO_OK)
    rc = turbo_flow_fmq_app_create(&sub_endpoint, &sub_options, &subscriber);
  if (rc == TURBO_OK) {
    rc = turbo_flow_fmq_app_start(publisher);
    publisher_started = rc == TURBO_OK;
  }
  if (rc == TURBO_OK) {
    rc = turbo_flow_fmq_app_start(subscriber);
    subscriber_started = rc == TURBO_OK;
  }
  if (rc == TURBO_OK)
    rc = fmq_example_send_when_connected(publisher, payload, sizeof(payload) - 1u);
  if (rc == TURBO_OK) rc = fmq_example_wait_for(&state.received, 1);

  if (subscriber_started) {
    int stop_rc = turbo_flow_fmq_app_stop(subscriber);
    if (rc == TURBO_OK) rc = stop_rc;
  }
  if (publisher_started) {
    int stop_rc = turbo_flow_fmq_app_stop(publisher);
    if (rc == TURBO_OK) rc = stop_rc;
  }
  turbo_flow_fmq_app_destroy(subscriber);
  turbo_flow_fmq_app_destroy(publisher);
  if (rc != TURBO_OK) {
    fprintf(stderr, "PUB/SUB example failed: %d\n", rc);
    return 1;
  }
  return 0;
}
