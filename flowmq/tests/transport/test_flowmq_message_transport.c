#include "flowmq_owner.h"
#include "flowmq_protocol.h"
#include "flowmq_socket.h"
#include "flowmq_socket_external_internal.h"
#include "flowmq_tls_test_material.h"
#include "flowmq_transport.h"
#include "tinytest.h"

#include <core/clock.h>
#include <core/error.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int wait_for(flowmq_owner_t *owner, flowmq_socket_t *first,
                    flowmq_socket_t *second, int events) {
  flowmq_pollitem_t items[] = {
      {.socket = first, .events = events}, {.socket = second}};
  if (owner != NULL) {
    size_t ready;
    const int status = flowmq_owner_poll(owner, items, 1u, 5000u, &ready);
    if (status != SALTS_OK) return status;
    if (items[0].revents & FLOWMQ_POLLERR) return SALTS_EIO;
    return (items[0].revents & events) != 0 ? SALTS_OK : SALTS_ETIMEDOUT;
  }
  const uint64_t start = monotonic_ms();
  do {
    size_t ready;
    const int status = flowmq_poll(items, 2u, 1u, &ready);
    if (status != SALTS_OK) return status;
    if ((items[0].revents | items[1].revents) & FLOWMQ_POLLERR) return SALTS_EIO;
    if (items[0].revents & events) return SALTS_OK;
  } while (monotonic_ms() - start < 5000u);
  return SALTS_ETIMEDOUT;
}

static void receive_part(flowmq_owner_t *owner, flowmq_socket_t *receiver,
                         flowmq_socket_t *sender, const void *expected,
                         size_t size, int more) {
  unsigned char *received = malloc(size == 0u ? 1u : size);
  size_t actual;
  size_t option_size = sizeof(int);
  int actual_more;
  check_not_null(received);
  check_equal(wait_for(owner, receiver, sender, FLOWMQ_POLLIN), SALTS_OK);
  check_equal(flowmq_recv(receiver, received, size, &actual, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(actual, size);
  check_equal(memcmp(received, expected, size), 0);
  check_equal(flowmq_getsockopt(receiver, FLOWMQ_RCVMORE, &actual_more, &option_size),
              SALTS_OK);
  check_equal(actual_more, more);
  free(received);
}

static void round_trip(const char *uri, flowmq_transport_t transport, bool shared) {
  const bool kcp = transport == FLOWMQ_TRANSPORT_KCP;
  flowmq_ctx_t *ctx = flowmq_ctx_new();
  flowmq_owner_t *owner = shared ? flowmq_owner_new(ctx, NULL) : NULL;
  if (shared) check_not_null(owner);
  flowmq_socket_t *server = shared ? flowmq_owner_socket(owner, FLOWMQ_PAIR)
                                  : flowmq_socket(ctx, FLOWMQ_PAIR);
  flowmq_socket_t *client = shared ? flowmq_owner_socket(owner, FLOWMQ_PAIR)
                                  : flowmq_socket(ctx, FLOWMQ_PAIR);
  flowmq_socket_t *tcp_receiver = NULL;
  flowmq_socket_t *tcp_sender = NULL;
  char *ca = NULL;
  char *cert = NULL;
  char *key = NULL;
  char endpoint[320u];
  size_t endpoint_size;
  check_not_null(server);
  check_not_null(client);
  if (transport == FLOWMQ_TRANSPORT_WSS) {
    ca = tt_make_temp_file("flowmq-wss-ca", ".pem");
    cert = tt_make_temp_file("flowmq-wss-cert", ".pem");
    key = tt_make_temp_file("flowmq-wss-key", ".pem");
    check_not_null(ca);
    check_not_null(cert);
    check_not_null(key);
    check_equal(tt_write_file(ca, FLOWMQ_TLS_TEST_ROOT_CA,
                              sizeof(FLOWMQ_TLS_TEST_ROOT_CA) - 1u), 0);
    check_equal(tt_write_file(cert, FLOWMQ_TLS_TEST_CERTIFICATE,
                              sizeof(FLOWMQ_TLS_TEST_CERTIFICATE) - 1u), 0);
    check_equal(tt_write_file(key, FLOWMQ_TLS_TEST_KEY,
                              sizeof(FLOWMQ_TLS_TEST_KEY) - 1u), 0);
    check_equal(flowmq_setsockopt(server, FLOWMQ_TLS_CERT_FILE, cert, strlen(cert)), SALTS_OK);
    check_equal(flowmq_setsockopt(server, FLOWMQ_TLS_KEY_FILE, key, strlen(key)), SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_CA_FILE, ca, strlen(ca)), SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_SERVER_NAME, "localhost", 9u), SALTS_OK);
  }
  if (kcp) {
    const int reuse_port = 1;
    check_equal(flowmq_setsockopt(client, FLOWMQ_REUSE_PORT, &reuse_port,
                                 sizeof(reuse_port)), SALTS_OK);
  }
  if (shared) {
    check_equal(flowmq_socket_internal_bind_external(server, uri), SALTS_OK);
    tcp_receiver = flowmq_owner_socket(owner, FLOWMQ_PULL);
    tcp_sender = flowmq_owner_socket(owner, FLOWMQ_PUSH);
    check_not_null(tcp_receiver);
    check_not_null(tcp_sender);
    check_equal(flowmq_socket_internal_bind_external(tcp_receiver, "tcp://127.0.0.1:0"),
                SALTS_OK);
    check_equal(flowmq_last_endpoint(tcp_receiver, endpoint, sizeof(endpoint),
                                    &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(tcp_sender, endpoint), SALTS_OK);
    check_equal(wait_for(owner, tcp_sender, tcp_receiver, FLOWMQ_POLLOUT), SALTS_OK);
  } else {
    check_equal(flowmq_bind(server, uri), SALTS_OK);
  }
  check_equal(flowmq_last_endpoint(server, endpoint, sizeof(endpoint), &endpoint_size),
              SALTS_OK);
  check_equal(flowmq_connect(client, endpoint), SALTS_OK);
  check_equal(wait_for(owner, client, server, FLOWMQ_POLLOUT), SALTS_OK);

  const size_t large_size = FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + (kcp ? 0u : 123u);
  unsigned char *large = malloc(large_size + 1u);
  check_not_null(large);
  for (size_t i = 0u; i <= large_size; ++i) large[i] = (unsigned char)(i * 31u);
  if (kcp)
    check_equal(flowmq_send(client, large, large_size + 1u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE),
                SALTS_EMSGSIZE);
  check_equal(flowmq_send(client, large, large_size, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE),
              SALTS_OK);
  check_equal(flowmq_send(client, "tail", 4u, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(flowmq_send(client, "", 0u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(owner, server, client, large, large_size, 1);
  receive_part(owner, server, client, "tail", 4u, 0);
  receive_part(owner, server, client, "", 0u, 0);

  check_equal(wait_for(owner, server, client, FLOWMQ_POLLOUT), SALTS_OK);
  check_equal(flowmq_send(server, "reply", 5u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(owner, client, server, "reply", 5u, 0);

  check_equal(flowmq_send(client, large, large_size, FLOWMQ_DONTWAIT), SALTS_OK);
  flowmq_pollitem_t pending = {.socket = client};
  size_t ready;
  if (shared) {
    check_equal(flowmq_send(tcp_sender, "concurrent", 10u, FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_owner_poll(owner, &pending, 1u, 0u, &ready), SALTS_OK);
    check_equal(flowmq_owner_close_socket(owner, client), SALTS_OK);
    check_equal(flowmq_owner_close_socket(owner, server), SALTS_OK);
    receive_part(owner, tcp_receiver, tcp_sender, "concurrent", 10u, 0);
    check_equal(flowmq_send(tcp_sender, "live", 4u, FLOWMQ_DONTWAIT), SALTS_OK);
    receive_part(owner, tcp_receiver, tcp_sender, "live", 4u, 0);
    check_equal(flowmq_owner_close_socket(owner, tcp_sender), SALTS_OK);
    check_equal(flowmq_owner_close_socket(owner, tcp_receiver), SALTS_OK);
    check_equal(flowmq_owner_term(owner), SALTS_OK);
  } else {
    /* The other endpoint cannot ACK the pending KCP write until driven. */
    check_equal(flowmq_poll(&pending, 1u, 0u, &ready), SALTS_OK);
    check_equal(flowmq_close(client), SALTS_OK);
    check_equal(flowmq_close(server), SALTS_OK);
  }
  check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  free(large);
  free(ca);
  free(cert);
  free(key);
}

static void check_wrong_route(const char *bind_uri, const char *correct,
                              const char *wrong) {
  flowmq_ctx_t *ctx = flowmq_ctx_new();
  flowmq_socket_t *server = flowmq_socket(ctx, FLOWMQ_PAIR);
  flowmq_socket_t *bad = flowmq_socket(ctx, FLOWMQ_PAIR);
  flowmq_socket_t *good = flowmq_socket(ctx, FLOWMQ_PAIR);
  char endpoint[320u];
  char rejected[320u];
  size_t endpoint_size;
  const int reconnect = -1;
  check_not_null(server);
  check_not_null(bad);
  check_not_null(good);
  check_equal(flowmq_setsockopt(bad, FLOWMQ_RECONNECT_IVL, &reconnect,
                               sizeof(reconnect)), SALTS_OK);
  check_equal(flowmq_bind(server, bind_uri), SALTS_OK);
  check_equal(flowmq_last_endpoint(server, endpoint, sizeof(endpoint), &endpoint_size),
              SALTS_OK);
  check(endpoint_size > strlen(correct) + 1u);
  const size_t prefix_size = endpoint_size - strlen(correct) - 1u;
  check_equal(strcmp(endpoint + prefix_size, correct), 0);
  memcpy(rejected, endpoint, prefix_size);
  memcpy(rejected + prefix_size, wrong, strlen(wrong) + 1u);
  check_equal(flowmq_connect(bad, rejected), SALTS_OK);
  flowmq_pollitem_t items[] = {
      {.socket = server, .events = FLOWMQ_POLLOUT},
      {.socket = bad, .events = FLOWMQ_POLLOUT}};
  size_t ready;
  check_equal(flowmq_poll(items, 2u, 100u, &ready), SALTS_OK);
  check_equal(ready, 0u);
  check_equal(flowmq_close(bad), SALTS_OK);
  check_equal(flowmq_connect(good, endpoint), SALTS_OK);
  check_equal(wait_for(NULL, good, server, FLOWMQ_POLLOUT), SALTS_OK);
  check_equal(flowmq_send(good, "correct", 7u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(NULL, server, good, "correct", 7u, 0);
  check_equal(flowmq_close(good), SALTS_OK);
  check_equal(flowmq_close(server), SALTS_OK);
  check_equal(flowmq_ctx_term(ctx), SALTS_OK);
}

spec("FlowMQ message transports") {
  it("exchanges fragmented binary FMQ messages over WebSocket") {
    round_trip("ws://127.0.0.1:0/fmq?room=1", FLOWMQ_TRANSPORT_WS, false);
  }

  it("exchanges binary FMQ messages over verified WSS") {
    round_trip("wss://127.0.0.1:0/fmq", FLOWMQ_TRANSPORT_WSS, false);
  }

  it("drives fragmented WebSocket messages and a TCP neighbor on one Owner") {
    round_trip("ws://127.0.0.1:0/fmq?room=1", FLOWMQ_TRANSPORT_WS, true);
  }

  it("drives TLS, WebSocket Upgrade and a TCP neighbor on one Owner") {
    round_trip("wss://127.0.0.1:0/fmq", FLOWMQ_TRANSPORT_WSS, true);
  }

  it("caps the Owner wait at an incomplete WebSocket Upgrade deadline") {
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_owner_t *owner = flowmq_owner_new(ctx, NULL);
    check_not_null(owner);
    flowmq_socket_t *server = flowmq_owner_socket(owner, FLOWMQ_PAIR);
    flowmq_socket_t *incomplete = flowmq_socket(ctx, FLOWMQ_PAIR);
    char uri[320u];
    char tcp_uri[320u];
    size_t uri_size;
    size_t ready;
    uint32_t wait_ms = 5000u;
    const int reconnect = -1;
    check_not_null(server);
    check_not_null(incomplete);
    check_equal(flowmq_socket_internal_bind_external(server, "ws://127.0.0.1:0/"),
                SALTS_OK);
    check_equal(flowmq_last_endpoint(server, uri, sizeof(uri), &uri_size), SALTS_OK);
    /* A TCP peer sends FMQ HELLO bytes and leaves the HTTP request incomplete. */
    uri[uri_size - 2u] = '\0'; /* Remove the root path. */
    check_true(snprintf(tcp_uri, sizeof(tcp_uri), "tcp://%s",
                        uri + sizeof("ws://") - 1u) > 0);
    check_equal(flowmq_setsockopt(incomplete, FLOWMQ_RECONNECT_IVL, &reconnect,
                                 sizeof(reconnect)), SALTS_OK);
    check_equal(flowmq_connect(incomplete, tcp_uri), SALTS_OK);
    flowmq_pollitem_t ws = {.socket = server};
    flowmq_pollitem_t tcp = {.socket = incomplete};
    const uint64_t started = monotonic_ms();
    do {
      check_equal(flowmq_poll(&tcp, 1u, 1u, &ready), SALTS_OK);
      check_equal(flowmq_owner_poll(owner, &ws, 1u, 0u, &ready), SALTS_OK);
      check_equal(flowmq_socket_internal_external_timeout(server, 5000u, &wait_ms),
                  SALTS_OK);
    } while ((wait_ms == 0u || wait_ms > 1000u) &&
             monotonic_ms() - started < 2000u);
    check_true(wait_ms > 0u && wait_ms <= 1000u);

    check_equal(flowmq_owner_poll(owner, &ws, 1u, 1500u, &ready), SALTS_OK);
    check_equal(ready, 0u);
    check_equal(flowmq_socket_internal_external_timeout(server, 5000u, &wait_ms),
                SALTS_OK);
    check_equal(wait_ms, 5000u);
    check_equal(flowmq_close(incomplete), SALTS_OK);
    check_equal(flowmq_owner_close_socket(owner, server), SALTS_OK);
    check_equal(flowmq_owner_term(owner), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("exchanges ordered multipart FMQ messages over real UDP/KCP") {
    round_trip("kcp://127.0.0.1:0?conv=4281", FLOWMQ_TRANSPORT_KCP, false);
  }

  it("drives KCP acknowledgements and closes beside a TCP neighbor on one Owner") {
    round_trip("kcp://127.0.0.1:0?conv=4281", FLOWMQ_TRANSPORT_KCP, true);
  }

  it("rejects a different WebSocket resource and leaves the listener available") {
    check_wrong_route("ws://127.0.0.1:0/fmq", "/fmq", "/other");
  }

  it("rejects a different KCP conversation and leaves the listener available") {
    check_wrong_route("kcp://127.0.0.1:0?conv=4281", "?conv=4281", "?conv=4282");
  }

  it("rejects invalid conversation and WebSocket endpoints before opening IO") {
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *socket = flowmq_socket(ctx, FLOWMQ_PAIR);
    check_not_null(socket);
    check_equal(flowmq_bind(socket, "kcp://127.0.0.1:0"), SALTS_EINVAL);
    check_equal(flowmq_bind(socket, "kcp://127.0.0.1:0?conv=0"), SALTS_EINVAL);
    check_equal(flowmq_connect(socket, "kcp://127.0.0.1:1?conv=4294967296"), SALTS_EINVAL);
    check_equal(flowmq_connect(socket, "ws://127.0.0.1:1/fmq\r\n"), SALTS_EINVAL);
    check_equal(flowmq_bind(socket, "ws://127.0.0.1:0/fmq#fragment"), SALTS_EINVAL);
    check_equal(flowmq_close(socket), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }
}
