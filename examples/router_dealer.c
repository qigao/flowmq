#include "flowmq_connect_endpoint.h"
#include "flowmq_router_endpoint.h"

#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

enum { EXAMPLE_WAIT_ATTEMPTS = 1000, EXAMPLE_WAIT_STEP_MS = 5 };
static const uint64_t EXAMPLE_START_TIMEOUT_NS = UINT64_C(5000000000);

typedef struct example_state_s {
  flowmq_router_endpoint_t *router;
  atomic_int reply_received;
  atomic_int status;
} example_state_t;

static unsigned short example_loopback_port(void) {
  struct sockaddr_in address;
#ifdef _WIN32
  WSADATA data;
  SOCKET handle;
  int address_size = (int)sizeof(address);
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0u;
  handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (handle == INVALID_SOCKET) {
    WSACleanup();
    return 0u;
  }
#else
  int handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  socklen_t address_size = (socklen_t)sizeof(address);
  if (handle < 0) return 0u;
#endif
  unsigned short port = 0u;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (bind(handle, (struct sockaddr *)&address, sizeof(address)) == 0 &&
      getsockname(handle, (struct sockaddr *)&address, &address_size) == 0)
    port = ntohs(address.sin_port);
#ifdef _WIN32
  closesocket(handle);
  WSACleanup();
#else
  close(handle);
#endif
  return port;
}

static int example_router_frame(void *ctx, const flowmq_router_route_t *route,
                                tstr_v identity, tstr_v topic,
                                const flowmq_protocol_frame_t *request) {
  example_state_t *state = (example_state_t *)ctx;
  flowmq_protocol_frame_t reply;
  tstr_t encoded = NULL;
  int rc;
  (void)identity;
  (void)topic;
  memset(&reply, 0, sizeof(reply));
  reply.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
  reply.pattern = FLOWMQ_PROTOCOL_ROUTER;
  reply.message_id = request->message_id;
  reply.payload = tstr_v_from_cstr("pong");
  rc = flowmq_protocol_encode_frame(&reply,
                                    FLOWMQ_ROUTER_ENDPOINT_DEFAULT_MAX_FRAME_SIZE,
                                    &encoded);
  if (rc == TURBO_OK)
    rc = flowmq_router_endpoint_send(state->router, *route, encoded,
                                     tstr_len(encoded));
  tstr_free(encoded);
  atomic_store_explicit(&state->status, rc, memory_order_release);
  return rc;
}

static int example_dealer_frame(void *ctx, const flowmq_protocol_frame_t *reply,
                                uint64_t generation) {
  example_state_t *state = (example_state_t *)ctx;
  (void)generation;
  if (reply->message_id != 1u ||
      !tstr_v_eq(reply->payload, tstr_v_from_cstr("pong")))
    return TURBO_EPROTO;
  atomic_store_explicit(&state->reply_received, 1, memory_order_release);
  return TURBO_OK;
}

static int example_wait_for_reply(const example_state_t *state) {
  for (int attempt = 0; attempt < EXAMPLE_WAIT_ATTEMPTS; ++attempt) {
    if (atomic_load_explicit(&state->reply_received, memory_order_acquire))
      return TURBO_OK;
    turbo_sleep_ms(EXAMPLE_WAIT_STEP_MS);
  }
  return TURBO_ETIMEDOUT;
}

int main(void) {
  example_state_t state;
  flowmq_router_endpoint_config_t router_config;
  flowmq_connect_endpoint_config_t dealer_config;
  flowmq_connect_endpoint_t *dealer = NULL;
  flowmq_protocol_frame_t request;
  tstr_t encoded = NULL;
  unsigned short port = example_loopback_port();
  int rc = port == 0u ? TURBO_EIO : TURBO_OK;

  memset(&state, 0, sizeof(state));
  atomic_init(&state.status, TURBO_OK);
  flowmq_router_endpoint_config_init(&router_config);
  router_config.transport = FLOWMQ_TRANSPORT_TCP;
  router_config.host = "127.0.0.1";
  router_config.path = "";
  router_config.topic = "examples";
  router_config.identity = "router-a";
  router_config.port = (int)port;
  router_config.context = NULL;
  router_config.drive_context = 1;
  router_config.own_context = 1;
  router_config.on_frame = example_router_frame;
  router_config.callback_ctx = &state;
  if (rc == TURBO_OK)
    rc = flowmq_router_endpoint_create(&router_config, &state.router);
  if (rc == TURBO_OK)
    rc = flowmq_router_endpoint_start(state.router, EXAMPLE_START_TIMEOUT_NS);

  flowmq_connect_endpoint_config_init(&dealer_config);
  dealer_config.transport = FLOWMQ_TRANSPORT_TCP;
  dealer_config.pattern = FLOWMQ_PROTOCOL_DEALER;
  dealer_config.host = "127.0.0.1";
  dealer_config.path = "";
  dealer_config.topic = "examples";
  dealer_config.identity = "dealer-a";
  dealer_config.port = (int)port;
  dealer_config.context = NULL;
  dealer_config.drive_context = 1;
  dealer_config.own_context = 1;
  dealer_config.on_frame = example_dealer_frame;
  dealer_config.callback_ctx = &state;
  if (rc == TURBO_OK)
    rc = flowmq_connect_endpoint_create(&dealer_config, &dealer);
  if (rc == TURBO_OK)
    rc = flowmq_connect_endpoint_start(dealer, EXAMPLE_START_TIMEOUT_NS);

  memset(&request, 0, sizeof(request));
  request.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
  request.pattern = FLOWMQ_PROTOCOL_DEALER;
  request.message_id = 1u;
  request.payload = tstr_v_from_cstr("ping");
  if (rc == TURBO_OK)
    rc = flowmq_protocol_encode_frame(&request,
                                      FLOWMQ_CONNECT_ENDPOINT_DEFAULT_MAX_FRAME_SIZE,
                                      &encoded);
  if (rc == TURBO_OK)
    rc = flowmq_connect_endpoint_send_copy(dealer, 1u, encoded,
                                           tstr_len(encoded));
  if (rc == TURBO_OK) rc = example_wait_for_reply(&state);
  if (rc == TURBO_OK)
    rc = atomic_load_explicit(&state.status, memory_order_acquire);

  tstr_free(encoded);
  flowmq_connect_endpoint_destroy(dealer);
  flowmq_router_endpoint_destroy(state.router);
  if (rc != TURBO_OK) {
    fprintf(stderr, "FlowMQ ROUTER/DEALER example failed: %d\n", rc);
    return 1;
  }
  puts("ROUTER received ping; DEALER received pong");
  return 0;
}
