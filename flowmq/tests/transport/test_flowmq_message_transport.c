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

typedef struct tls_files {
  char *ca;
  char *cert;
  char *key;
} tls_files;

static void tls_files_init(tls_files *files) {
  files->ca = tt_make_temp_file("flowmq-wss-ca", ".pem");
  files->cert = tt_make_temp_file("flowmq-wss-cert", ".pem");
  files->key = tt_make_temp_file("flowmq-wss-key", ".pem");
  check_not_null(files->ca);
  check_not_null(files->cert);
  check_not_null(files->key);
  check_equal(tt_write_file(files->ca, FLOWMQ_TLS_TEST_ROOT_CA,
                            sizeof(FLOWMQ_TLS_TEST_ROOT_CA) - 1u), 0);
  check_equal(tt_write_file(files->cert, FLOWMQ_TLS_TEST_CERTIFICATE,
                            sizeof(FLOWMQ_TLS_TEST_CERTIFICATE) - 1u), 0);
  check_equal(tt_write_file(files->key, FLOWMQ_TLS_TEST_KEY,
                            sizeof(FLOWMQ_TLS_TEST_KEY) - 1u), 0);
}

static void tls_server_configure(flowmq_socket_t *server, const tls_files *files) {
  check_equal(flowmq_setsockopt(server, FLOWMQ_TLS_CERT_FILE, files->cert,
                               strlen(files->cert)), SALTS_OK);
  check_equal(flowmq_setsockopt(server, FLOWMQ_TLS_KEY_FILE, files->key,
                               strlen(files->key)), SALTS_OK);
}

static void tls_client_configure(flowmq_socket_t *client, const tls_files *files) {
  check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_CA_FILE, files->ca,
                               strlen(files->ca)), SALTS_OK);
  check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_SERVER_NAME, "localhost", 9u), SALTS_OK);
}

static void tls_files_free(tls_files *files) {
  if (files->ca != NULL) check_equal(tt_remove_file(files->ca), 0);
  if (files->cert != NULL) check_equal(tt_remove_file(files->cert), 0);
  if (files->key != NULL) check_equal(tt_remove_file(files->key), 0);
  free(files->ca);
  free(files->cert);
  free(files->key);
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
  tls_files files = {0};
  char endpoint[320u];
  size_t endpoint_size;
  check_not_null(server);
  check_not_null(client);
  if (transport == FLOWMQ_TRANSPORT_WSS) {
    tls_files_init(&files);
    tls_server_configure(server, &files);
    tls_client_configure(client, &files);
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
  tls_files_free(&files);
}

typedef struct pattern_fixture {
  flowmq_ctx_t *ctx;
  flowmq_owner_t *owner;
  tls_files tls;
  bool kcp;
} pattern_fixture;

static void pattern_fixture_init(pattern_fixture *fixture, const char *uri, bool shared) {
  fixture->ctx = flowmq_ctx_new();
  check_not_null(fixture->ctx);
  if (shared) {
    fixture->owner = flowmq_owner_new(fixture->ctx, NULL);
    check_not_null(fixture->owner);
  }
  fixture->kcp = strncmp(uri, "kcp://", sizeof("kcp://") - 1u) == 0;
  if (strncmp(uri, "wss://", sizeof("wss://") - 1u) == 0)
    tls_files_init(&fixture->tls);
}

static flowmq_socket_t *pattern_socket(pattern_fixture *fixture, int type, bool bind) {
  flowmq_socket_t *socket = fixture->owner != NULL
      ? flowmq_owner_socket(fixture->owner, type)
      : flowmq_socket(fixture->ctx, type);
  check_not_null(socket);
  if (fixture->tls.ca != NULL) {
    if (bind) tls_server_configure(socket, &fixture->tls);
    else tls_client_configure(socket, &fixture->tls);
  }
  return socket;
}

static void pattern_bind(pattern_fixture *fixture, flowmq_socket_t *server, const char *uri) {
  check_equal(fixture->owner != NULL ? flowmq_socket_internal_bind_external(server, uri)
                                     : flowmq_bind(server, uri), SALTS_OK);
}

static void pattern_close(pattern_fixture *fixture, flowmq_socket_t *socket) {
  check_equal(fixture->owner != NULL ? flowmq_owner_close_socket(fixture->owner, socket)
                                     : flowmq_close(socket), SALTS_OK);
}

static void pattern_fixture_term(pattern_fixture *fixture) {
  if (fixture->owner != NULL) check_equal(flowmq_owner_term(fixture->owner), SALTS_OK);
  check_equal(flowmq_ctx_term(fixture->ctx), SALTS_OK);
  tls_files_free(&fixture->tls);
}

static void request_reply(pattern_fixture *fixture, const char *uri) {
  flowmq_socket_t *req = pattern_socket(fixture, FLOWMQ_REQ, false);
  flowmq_socket_t *rep = pattern_socket(fixture, FLOWMQ_REP, true);
  char endpoint[320u];
  size_t endpoint_size;
  const size_t window = 8u;
  check_equal(flowmq_setsockopt(req, FLOWMQ_SNDHWM_BYTES, &window, sizeof(window)), SALTS_OK);
  pattern_bind(fixture, rep, uri);
  check_equal(flowmq_last_endpoint(rep, endpoint, sizeof(endpoint), &endpoint_size), SALTS_OK);
  check_equal(flowmq_connect(req, endpoint), SALTS_OK);
  check_equal(wait_for(fixture->owner, req, rep, FLOWMQ_POLLOUT), SALTS_OK);

  check_equal(flowmq_send(req, "head", 4u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
  check_equal(flowmq_send(req, "large", 5u, FLOWMQ_DONTWAIT), SALTS_EMSGSIZE);
  check_equal(flowmq_send(req, "body", 4u, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(flowmq_send(req, "body", 4u, FLOWMQ_DONTWAIT), SALTS_EPROTO);
  receive_part(fixture->owner, rep, req, "head", 4u, 1);
  check_equal(flowmq_send(rep, "reply", 5u, FLOWMQ_DONTWAIT), SALTS_EPROTO);
  receive_part(fixture->owner, rep, req, "body", 4u, 0);
  check_equal(flowmq_send(rep, "reply", 5u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture->owner, req, rep, "reply", 5u, 0);

  pattern_close(fixture, req);
  pattern_close(fixture, rep);
}

static void route_dealers(pattern_fixture *fixture, const char *uri) {
  flowmq_socket_t *router = pattern_socket(fixture, FLOWMQ_ROUTER, true);
  flowmq_socket_t *slow = pattern_socket(fixture, FLOWMQ_DEALER, false);
  flowmq_socket_t *fast = pattern_socket(fixture, FLOWMQ_DEALER, false);
  char endpoint[320u];
  size_t endpoint_size;
  const size_t window = 5u;
  flowmq_router_peer_status_t peer = FLOWMQ_ROUTER_PEER_STATUS_INIT;
  check_equal(flowmq_setsockopt(slow, FLOWMQ_IDENTITY, "slow", 4u), SALTS_OK);
  check_equal(flowmq_setsockopt(fast, FLOWMQ_IDENTITY, "fast", 4u), SALTS_OK);
  check_equal(flowmq_setsockopt(slow, FLOWMQ_RCVHWM_BYTES, &window, sizeof(window)), SALTS_OK);
  pattern_bind(fixture, router, uri);
  check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint), &endpoint_size),
              SALTS_OK);
  check_equal(flowmq_connect(slow, endpoint), SALTS_OK);
  check_equal(wait_for(fixture->owner, slow, router, FLOWMQ_POLLOUT), SALTS_OK);
  check_equal(flowmq_send(slow, "one", 3u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture->owner, router, slow, "slow", 4u, 1);
  receive_part(fixture->owner, router, slow, "one", 3u, 0);

  check_equal(flowmq_connect(fast, endpoint), SALTS_OK);
  check_equal(wait_for(fixture->owner, fast, router, FLOWMQ_POLLOUT), SALTS_OK);
  check_equal(flowmq_send(fast, "two", 3u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture->owner, router, fast, "fast", 4u, 1);
  receive_part(fixture->owner, router, fast, "two", 3u, 0);

  check_equal(flowmq_send(router, "slow", 4u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
  check_equal(flowmq_send(router, "reply", 5u, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(wait_for(fixture->owner, slow, router, FLOWMQ_POLLIN), SALTS_OK);
  check_equal(flowmq_router_peer_status(router, "slow", 4u, &peer), SALTS_OK);
  check_equal(peer.send_credit_bytes, 0u);
  check_equal(flowmq_send(router, "slow", 4u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
  check_equal(flowmq_send(router, "reply", 5u, FLOWMQ_DONTWAIT), SALTS_ENOBUFS);
  check_equal(flowmq_send(router, "fast", 4u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
  check_equal(flowmq_send(router, "reply", 5u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture->owner, fast, router, "reply", 5u, 0);
  receive_part(fixture->owner, slow, router, "reply", 5u, 0);

  pattern_close(fixture, fast);
  pattern_close(fixture, slow);
  pattern_close(fixture, router);
}

static void check_subscription(pattern_fixture *fixture, const char *uri) {
  static const unsigned char subscribed[] = {1u, 'f', 'm', 'q', '.'};
  const int retry_ms = 5;
  flowmq_socket_t *xpub = pattern_socket(fixture, FLOWMQ_XPUB, true);
  flowmq_socket_t *sub = pattern_socket(fixture, FLOWMQ_SUB, false);
  char endpoint[320u];
  size_t endpoint_size;
  check_equal(flowmq_setsockopt(sub, FLOWMQ_SUBSCRIBE, "fmq.", 4u), SALTS_OK);
  check_equal(flowmq_setsockopt(sub, FLOWMQ_RECONNECT_IVL, &retry_ms, sizeof(retry_ms)),
              SALTS_OK);
  pattern_bind(fixture, xpub, uri);
  check_equal(flowmq_last_endpoint(xpub, endpoint, sizeof(endpoint), &endpoint_size), SALTS_OK);
  check_equal(flowmq_connect(sub, endpoint), SALTS_OK);
  receive_part(fixture->owner, xpub, sub, subscribed, sizeof(subscribed), 0);
  check_equal(flowmq_send(xpub, "other", 5u, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(flowmq_send(xpub, "fmq.before", 10u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture->owner, sub, xpub, "fmq.before", 10u, 0);

  if (!fixture->kcp) {
    pattern_close(fixture, xpub);
    xpub = pattern_socket(fixture, FLOWMQ_XPUB, true);
    pattern_bind(fixture, xpub, endpoint);
    receive_part(fixture->owner, xpub, sub, subscribed, sizeof(subscribed), 0);
    check_equal(flowmq_send(xpub, "other", 5u, FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_send(xpub, "fmq.after", 9u, FLOWMQ_DONTWAIT), SALTS_OK);
    receive_part(fixture->owner, sub, xpub, "fmq.after", 9u, 0);
  }

  pattern_close(fixture, sub);
  pattern_close(fixture, xpub);
}

static void messaging_patterns(const char *uri, bool shared) {
  pattern_fixture fixture = {0};
  pattern_fixture_init(&fixture, uri, shared);
  request_reply(&fixture, uri);
  route_dealers(&fixture, uri);
  check_subscription(&fixture, uri);
  pattern_fixture_term(&fixture);
}

static void expire_unacknowledged_kcp_peer(bool shared) {
  const char uri[] = "kcp://127.0.0.1:0?conv=4281";
  const int heartbeat_ms = 25;
  const int timeout_ms = 500;
  pattern_fixture fixture = {0};
  char endpoint[320u];
  size_t endpoint_size;
  flowmq_router_peer_status_t peer = FLOWMQ_ROUTER_PEER_STATUS_INIT;
  pattern_fixture_init(&fixture, uri, shared);
  flowmq_socket_t *router = pattern_socket(&fixture, FLOWMQ_ROUTER, true);
  flowmq_socket_t *dealer = pattern_socket(&fixture, FLOWMQ_DEALER, false);
  check_equal(flowmq_setsockopt(router, FLOWMQ_HEARTBEAT_IVL, &heartbeat_ms,
                               sizeof(heartbeat_ms)), SALTS_OK);
  check_equal(flowmq_setsockopt(router, FLOWMQ_HEARTBEAT_TIMEOUT, &timeout_ms,
                               sizeof(timeout_ms)), SALTS_OK);
  check_equal(flowmq_setsockopt(dealer, FLOWMQ_IDENTITY, "gone", 4u), SALTS_OK);
  pattern_bind(&fixture, router, uri);
  check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint), &endpoint_size),
              SALTS_OK);
  check_equal(flowmq_connect(dealer, endpoint), SALTS_OK);
  check_equal(wait_for(fixture.owner, dealer, router, FLOWMQ_POLLOUT), SALTS_OK);
  check_equal(wait_for(fixture.owner, router, dealer, FLOWMQ_POLLOUT), SALTS_OK);

  check_equal(flowmq_send(router, "gone", 4u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
  check_equal(flowmq_send(router, "no ack", 6u, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(flowmq_router_peer_status(router, "gone", 4u, &peer), SALTS_OK);
  check_equal(peer.outstanding_messages, 1u);
  pattern_close(&fixture, dealer);
  flowmq_pollitem_t item = {.socket = router};
  const uint64_t deadline = monotonic_ms() + 5000u;
  int status;
  do {
    size_t ready;
    check_equal(shared ? flowmq_owner_poll(fixture.owner, &item, 1u, 5u, &ready)
                       : flowmq_poll(&item, 1u, 5u, &ready), SALTS_OK);
    status = flowmq_router_peer_status(router, "gone", 4u, &peer);
  } while (status == SALTS_OK && monotonic_ms() < deadline);
  check_equal(status, SALTS_ENOENT);
  pattern_close(&fixture, router);
  pattern_fixture_term(&fixture);
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
  it("preserves request, routing and subscription semantics over WS") {
    messaging_patterns("ws://127.0.0.1:0/fmq", false);
    messaging_patterns("ws://127.0.0.1:0/fmq", true);
  }

  it("preserves request, routing and subscription semantics over verified WSS") {
    messaging_patterns("wss://127.0.0.1:0/fmq", false);
    messaging_patterns("wss://127.0.0.1:0/fmq", true);
  }

  it("preserves request, routing and subscription semantics over KCP") {
    messaging_patterns("kcp://127.0.0.1:0?conv=4281", false);
    messaging_patterns("kcp://127.0.0.1:0?conv=4281", true);
  }

  it("expires a KCP peer with DATA waiting for an acknowledgement") {
    expire_unacknowledged_kcp_peer(false);
    expire_unacknowledged_kcp_peer(true);
  }

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
