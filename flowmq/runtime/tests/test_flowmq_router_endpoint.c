#include "flowmq_connect_endpoint.h"
#include "flowmq_router_endpoint.h"

#include "CoroNet/turbo_coro_context.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
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

enum {
  ROUTER_TEST_WAIT_ATTEMPTS = 1000,
  ROUTER_TEST_WAIT_STEP_MS = 5
};

static const uint64_t ROUTER_TEST_START_TIMEOUT_NS = UINT64_C(5000000000);

static int router_verify_peer_identity(void *ctx, const char *certificate_sha256,
                                       tstr_v claimed_identity) {
  (void)ctx;
  (void)certificate_sha256;
  (void)claimed_identity;
  return TURBO_OK;
}

typedef struct router_roundtrip_s {
  flowmq_router_endpoint_t *router;
  flowmq_connect_endpoint_t *dealer;
  atomic_int router_frames;
  atomic_int dealer_frames;
  atomic_int send_status;
  atomic_int send_completion_entered;
  atomic_int send_completion_release;
  atomic_int send_completion_done;
  atomic_uint_fast64_t send_completion_id;
  atomic_int stale_done;
  atomic_int stale_status;
  atomic_int router_status;
  flowmq_router_route_t route;
  char reply[16];
  size_t reply_size;
  tstr_t request;
} router_roundtrip_t;

static unsigned short router_test_port(void) {
  struct sockaddr_in address;
#ifdef _WIN32
  SOCKET socket_handle = INVALID_SOCKET;
  int address_size = (int)sizeof(address);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0u;
  socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle == INVALID_SOCKET) {
    WSACleanup();
    return 0u;
  }
#else
  int socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  socklen_t address_size = (socklen_t)sizeof(address);
  if (socket_handle < 0) return 0u;
#endif
  unsigned short port = 0u;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(socket_handle, (struct sockaddr *)&address, sizeof(address)) == 0 &&
      getsockname(socket_handle, (struct sockaddr *)&address, &address_size) == 0)
    port = ntohs(address.sin_port);
#ifdef _WIN32
  closesocket(socket_handle);
  WSACleanup();
#else
  close(socket_handle);
#endif
  return port;
}

static int router_on_frame(void *ctx, const flowmq_router_route_t *route,
                           tstr_v peer_identity, tstr_v peer_topic,
                           const flowmq_protocol_frame_t *frame) {
  router_roundtrip_t *roundtrip = (router_roundtrip_t *)ctx;
  flowmq_protocol_frame_t reply;
  tstr_t encoded = NULL;
  int rc;
  (void)peer_topic;
  if (!tstr_v_eq(peer_identity, tstr_v_from_cstr("dealer-a")) ||
      frame->message_id != 41u || !tstr_v_eq(frame->payload, tstr_v_from_cstr("ping"))) {
    atomic_store_explicit(&roundtrip->router_status, TURBO_EPROTO, memory_order_release);
    return TURBO_EPROTO;
  }
  roundtrip->route = *route;
  memset(&reply, 0, sizeof(reply));
  reply.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
  reply.pattern = FLOWMQ_PROTOCOL_ROUTER;
  reply.message_id = frame->message_id;
  reply.payload = tstr_v_from_cstr("pong");
  rc = flowmq_protocol_encode_frame(&reply, FLOWMQ_ROUTER_ENDPOINT_DEFAULT_MAX_FRAME_SIZE,
                                    &encoded);
  if (rc == TURBO_OK)
    rc = flowmq_router_endpoint_send(roundtrip->router, *route, encoded, tstr_len(encoded));
  tstr_freep(&encoded);
  atomic_store_explicit(&roundtrip->router_status, rc, memory_order_release);
  atomic_fetch_add_explicit(&roundtrip->router_frames, 1, memory_order_acq_rel);
  return rc;
}

static int dealer_on_frame(void *ctx, const flowmq_protocol_frame_t *frame,
                           uint64_t generation) {
  router_roundtrip_t *roundtrip = (router_roundtrip_t *)ctx;
  size_t copy_size = frame->payload.len < sizeof(roundtrip->reply)
                         ? frame->payload.len
                         : sizeof(roundtrip->reply);
  (void)generation;
  memcpy(roundtrip->reply, frame->payload.data, copy_size);
  roundtrip->reply_size = copy_size;
  atomic_fetch_add_explicit(&roundtrip->dealer_frames, 1, memory_order_acq_rel);
  return TURBO_OK;
}

static void dealer_send_complete(void *ctx, uint64_t completion_id, int status) {
  router_roundtrip_t *roundtrip = (router_roundtrip_t *)ctx;
  atomic_store_explicit(&roundtrip->send_completion_entered, 1,
                        memory_order_release);
  while (!atomic_load_explicit(&roundtrip->send_completion_release,
                               memory_order_acquire))
    turbo_sleep_ms(1u);
  atomic_store_explicit(&roundtrip->send_completion_id, completion_id,
                        memory_order_release);
  atomic_store_explicit(&roundtrip->send_status, status, memory_order_release);
  atomic_store_explicit(&roundtrip->send_completion_done, 1,
                        memory_order_release);
}

static void router_stale_send_post(void *arg1, void *arg2) {
  router_roundtrip_t *roundtrip = (router_roundtrip_t *)arg1;
  int rc = flowmq_router_endpoint_send(roundtrip->router, roundtrip->route,
                                       roundtrip->request, tstr_len(roundtrip->request));
  (void)arg2;
  atomic_store_explicit(&roundtrip->stale_status, rc, memory_order_release);
  atomic_store_explicit(&roundtrip->stale_done, 1, memory_order_release);
}

static int router_wait_for(const atomic_int *value, int expected) {
  for (int attempt = 0; attempt < ROUTER_TEST_WAIT_ATTEMPTS; ++attempt) {
    if (atomic_load_explicit(value, memory_order_acquire) == expected) return TURBO_OK;
    turbo_sleep_ms(ROUTER_TEST_WAIT_STEP_MS);
  }
  return TURBO_ETIMEDOUT;
}

static int router_wait_for_connections(flowmq_router_endpoint_t *router, size_t expected) {
  for (int attempt = 0; attempt < ROUTER_TEST_WAIT_ATTEMPTS; ++attempt) {
    if (flowmq_router_endpoint_connections(router) == expected) return TURBO_OK;
    turbo_sleep_ms(ROUTER_TEST_WAIT_STEP_MS);
  }
  return TURBO_ETIMEDOUT;
}

static void router_run_roundtrip(flowmq_coronet_transport_t transport, const char *path) {
  router_roundtrip_t roundtrip;
  flowmq_router_endpoint_config_t router_config;
  flowmq_connect_endpoint_config_t dealer_config;
  flowmq_protocol_frame_t request_frame;
  unsigned short port = router_test_port();
  memset(&roundtrip, 0, sizeof(roundtrip));
  atomic_init(&roundtrip.send_status, TURBO_EALREADY);
  atomic_init(&roundtrip.stale_status, TURBO_EALREADY);
  atomic_init(&roundtrip.router_status, TURBO_EALREADY);
  check_uint_ne(port, 0u);

  flowmq_router_endpoint_config_init(&router_config);
  router_config.transport = transport;
  router_config.host = "127.0.0.1";
  router_config.path = path;
  router_config.topic = "raft";
  router_config.identity = "router-a";
  router_config.port = (int)port;
  router_config.max_connections = 8u;
  router_config.context = NULL;
  router_config.drive_context = 1;
  router_config.own_context = 1;
  router_config.on_frame = router_on_frame;
  router_config.callback_ctx = &roundtrip;
  check_int_eq(flowmq_router_endpoint_create(&router_config, &roundtrip.router), TURBO_OK);
  check_int_eq(flowmq_router_endpoint_start(roundtrip.router, ROUTER_TEST_START_TIMEOUT_NS),
               TURBO_OK);

  flowmq_connect_endpoint_config_init(&dealer_config);
  dealer_config.transport = transport;
  dealer_config.pattern = FLOWMQ_PROTOCOL_DEALER;
  dealer_config.host = "127.0.0.1";
  dealer_config.path = path;
  dealer_config.topic = "raft";
  dealer_config.identity = "dealer-a";
  dealer_config.port = (int)port;
  dealer_config.reconnect_initial_ms = 0u;
  dealer_config.reconnect_max_ms = 0u;
  dealer_config.context = NULL;
  dealer_config.drive_context = 1;
  dealer_config.own_context = 1;
  dealer_config.on_frame = dealer_on_frame;
  dealer_config.send_admission.capacity = 1u;
  dealer_config.send_admission.capacity_bytes = 4096u;
  dealer_config.send_admission.on_complete = dealer_send_complete;
  dealer_config.send_admission.completion_ctx = &roundtrip;
  dealer_config.callback_ctx = &roundtrip;
  check_int_eq(flowmq_connect_endpoint_create(&dealer_config, &roundtrip.dealer), TURBO_OK);
  check_int_eq(flowmq_connect_endpoint_start(roundtrip.dealer, ROUTER_TEST_START_TIMEOUT_NS),
               TURBO_OK);
  check_size_eq(flowmq_router_endpoint_connections(roundtrip.router), 1u);

  memset(&request_frame, 0, sizeof(request_frame));
  request_frame.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
  request_frame.pattern = FLOWMQ_PROTOCOL_DEALER;
  request_frame.message_id = 41u;
  request_frame.payload = tstr_v_from_cstr("ping");
  check_int_eq(flowmq_protocol_encode_frame(&request_frame,
                                            FLOWMQ_ROUTER_ENDPOINT_DEFAULT_MAX_FRAME_SIZE,
                                            &roundtrip.request),
               TURBO_OK);
  check_int_eq(flowmq_connect_endpoint_send_copy(
                   roundtrip.dealer, 73u, roundtrip.request,
                   tstr_len(roundtrip.request)),
               TURBO_OK);
  check_int_eq(router_wait_for(&roundtrip.send_completion_entered, 1), TURBO_OK);
  check_int_eq(flowmq_connect_endpoint_send_copy(
                   roundtrip.dealer, 74u, roundtrip.request,
                   tstr_len(roundtrip.request)),
               TURBO_ENOSPC);
  atomic_store_explicit(&roundtrip.send_completion_release, 1,
                        memory_order_release);
  check_int_eq(router_wait_for(&roundtrip.send_completion_done, 1), TURBO_OK);
  check_int_eq(atomic_load_explicit(&roundtrip.send_status, memory_order_acquire), TURBO_OK);
  check_uint_eq(atomic_load_explicit(&roundtrip.send_completion_id,
                                     memory_order_acquire),
                73u);
  check_int_eq(router_wait_for(&roundtrip.dealer_frames, 1), TURBO_OK);
  check_int_eq(atomic_load_explicit(&roundtrip.router_status, memory_order_acquire), TURBO_OK);
  check_int_eq(atomic_load_explicit(&roundtrip.router_frames, memory_order_acquire), 1);
  check_size_eq(roundtrip.reply_size, 4u);
  check_mem_eq(roundtrip.reply, "pong", 4u);
  {
    flowmq_send_admission_stats_t stats;
    flowmq_connect_endpoint_send_stats(roundtrip.dealer, &stats);
    check_size_eq(stats.pending, 0u);
    check_size_eq(stats.high_water, 1u);
    check_uint_eq(stats.rejected_full, 1u);
    check_uint_eq(stats.completed, 1u);
  }

  flowmq_connect_endpoint_stop(roundtrip.dealer);
  check_int_eq(flowmq_connect_endpoint_send_copy(
                   roundtrip.dealer, 75u, roundtrip.request,
                   tstr_len(roundtrip.request)),
               TURBO_ESHUTDOWN);
  flowmq_connect_endpoint_destroy(roundtrip.dealer);
  check_int_eq(router_wait_for_connections(roundtrip.router, 0u), TURBO_OK);
  check_int_eq(coro_post(flowmq_router_endpoint_context(roundtrip.router),
                         router_stale_send_post, &roundtrip, NULL),
               TURBO_OK);
  check_int_eq(router_wait_for(&roundtrip.stale_done, 1), TURBO_OK);
  check_int_eq(atomic_load_explicit(&roundtrip.stale_status, memory_order_acquire),
               TURBO_ENOTCONN);
  tstr_freep(&roundtrip.request);
  flowmq_router_endpoint_destroy(roundtrip.router);
}

spec("flowmq_router_endpoint owner") {
  it("rejects unbounded and incomplete listener configurations") {
    flowmq_router_endpoint_config_t config;
    flowmq_router_endpoint_t *router = NULL;
    memset(&config, 0, sizeof(config));
    check_int_eq(flowmq_router_endpoint_create(&config, &router), TURBO_EINVAL);
    check_null(router);

    flowmq_router_endpoint_config_init(&config);
    config.host = "127.0.0.1";
    config.path = "";
    config.topic = "";
    config.identity = "router";
    config.port = 70000;
    config.context = coro_context_create(NULL);
    config.max_connections = FLOWMQ_ROUTER_ENDPOINT_MAX_CONNECTIONS + 1u;
    check_int_eq(flowmq_router_endpoint_create(&config, &router), TURBO_EINVAL);
    check_null(router);
    coro_context_destroy(config.context);
  }

  it("requires explicit TLS listener credentials") {
    flowmq_router_endpoint_config_t config;
    flowmq_router_endpoint_t *router = NULL;
    coro_context_t *context = coro_context_create(NULL);
    flowmq_router_endpoint_config_init(&config);
    config.transport = FLOWMQ_TRANSPORT_WSS;
    config.host = "127.0.0.1";
    config.path = "/fmq";
    config.topic = "";
    config.identity = "router";
    config.port = 9443;
    config.context = context;
    check_int_eq(flowmq_router_endpoint_create(&config, &router), TURBO_EINVAL);
    check_null(router);
    coro_context_destroy(context);
  }

  it("requires mandatory mTLS when peer identity verification is configured") {
    flowmq_router_endpoint_config_t config;
    flowmq_router_endpoint_t *router = NULL;
    coro_context_t *context = coro_context_create(NULL);
    flowmq_coronet_tls_server_config_t tls = {0};
    flowmq_router_endpoint_config_init(&config);
    config.transport = FLOWMQ_TRANSPORT_TLS;
    config.host = "127.0.0.1";
    config.path = "";
    config.topic = "provider";
    config.identity = "iris";
    config.port = 9443;
    config.context = context;
    config.verify_peer_identity = router_verify_peer_identity;
    check_int_eq(flowmq_router_endpoint_create(&config, &router), TURBO_EINVAL);
    check_null(router);

    tls.cert_file = "server.crt";
    tls.key_file = "server.key";
    tls.ca_file = "providers-ca.crt";
    tls.require_client_certificate = 0;
    config.tls = &tls;
    check_int_eq(flowmq_router_endpoint_create(&config, &router), TURBO_EINVAL);
    check_null(router);
    coro_context_destroy(context);
  }

  it("accepts the complete v3 prefix with legacy verifier context semantics") {
    flowmq_router_endpoint_config_t config;
    flowmq_router_endpoint_t *router = NULL;
    coro_context_t *context = coro_context_create(NULL);
    flowmq_coronet_tls_server_config_t tls = {
        "providers-ca.crt", "server.crt", "server.key", NULL, 1};
    flowmq_router_endpoint_config_init(&config);
    config.transport = FLOWMQ_TRANSPORT_TLS;
    config.host = "127.0.0.1";
    config.path = "";
    config.topic = "provider";
    config.identity = "iris";
    config.port = 9443;
    config.context = context;
    config.tls = &tls;
    config.verify_peer_identity = router_verify_peer_identity;
    config.callback_ctx = &config;
    config.size = FLOWMQ_ROUTER_ENDPOINT_CONFIG_V3_SIZE;
    check_int_eq(flowmq_router_endpoint_create(&config, &router), TURBO_OK);
    check_not_null(router);
    flowmq_router_endpoint_destroy(router);
    coro_context_destroy(context);
  }

  it("routes one DEALER request and reply over TCP") {
    router_run_roundtrip(FLOWMQ_TRANSPORT_TCP, "");
  }

  it("routes binary FMQ frames over an exact WebSocket path") {
    router_run_roundtrip(FLOWMQ_TRANSPORT_WS, "/fmq");
  }
}
