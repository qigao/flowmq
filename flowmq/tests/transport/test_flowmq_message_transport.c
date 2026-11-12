#include "flowmq_owner.h"
#include "flowmq_protocol.h"
#include "flowmq_socket.h"
#include "flowmq_socket_external_internal.h"
#include "flowmq_socket_packet_test.h"
#include "flowmq_tls_identity_map.h"
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

static void check_received_part(flowmq_socket_t *receiver, const void *expected,
                                size_t size, int more) {
  unsigned char *received = malloc(size == 0u ? 1u : size);
  size_t actual;
  size_t option_size = sizeof(int);
  int actual_more;
  check_not_null(received);
  check_equal(flowmq_recv(receiver, received, size, &actual, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(actual, size);
  check_equal(memcmp(received, expected, size), 0);
  check_equal(flowmq_getsockopt(receiver, FLOWMQ_RCVMORE, &actual_more, &option_size),
              SALTS_OK);
  check_equal(actual_more, more);
  free(received);
}

static void receive_part(flowmq_owner_t *owner, flowmq_socket_t *receiver,
                         flowmq_socket_t *sender, const void *expected,
                         size_t size, int more) {
  check_equal(wait_for(owner, receiver, sender, FLOWMQ_POLLIN), SALTS_OK);
  check_received_part(receiver, expected, size, more);
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

static void set_test_kcp_psk(flowmq_socket_t *socket, unsigned char value) {
  /* Test-only keys for loopback peers. */
  const unsigned char key[FLOWMQ_KCP_PSK_BYTES] = {value};
  check_equal(flowmq_setsockopt(socket, FLOWMQ_KCP_PSK, key, sizeof(key)), SALTS_OK);
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
    if (strstr(uri, "?auth=psk") != NULL) {
      set_test_kcp_psk(server, 1u);
      set_test_kcp_psk(client, 1u);
    }
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
  bool kcp_psk;
} pattern_fixture;

static void pattern_fixture_init(pattern_fixture *fixture, const char *uri, bool shared) {
  fixture->ctx = flowmq_ctx_new();
  check_not_null(fixture->ctx);
  if (shared) {
    fixture->owner = flowmq_owner_new(fixture->ctx, NULL);
    check_not_null(fixture->owner);
  }
  fixture->kcp = strncmp(uri, "kcp://", sizeof("kcp://") - 1u) == 0;
  fixture->kcp_psk = fixture->kcp && strstr(uri, "?auth=psk") != NULL;
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
  if (fixture->kcp_psk) set_test_kcp_psk(socket, 1u);
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

static int send_routed(flowmq_socket_t *router, const char *identity,
                       const void *data, size_t size) {
  const int status = flowmq_send(router, identity, strlen(identity),
                                 FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE);
  if (status != SALTS_OK) return status;
  return flowmq_send(router, data, size, FLOWMQ_DONTWAIT);
}

static int wait_for_kcp_peer(flowmq_owner_t *owner, flowmq_socket_t *router,
                             flowmq_socket_t *dealer, const char *identity,
                             uint64_t completed, uint64_t credit) {
  flowmq_pollitem_t local = {.socket = router};
  flowmq_pollitem_t remote = {.socket = dealer, .events = FLOWMQ_POLLOUT};
  const size_t identity_size = strlen(identity);
  const uint64_t deadline = monotonic_ms() + 5000u;
  do {
    size_t ready;
    int status = flowmq_poll(&remote, 1u, 0u, &ready);
    if (status != SALTS_OK) return status;
    status = flowmq_owner_poll(owner, &local, 1u, 1u, &ready);
    if (status != SALTS_OK) return status;
    if ((local.revents | remote.revents) & FLOWMQ_POLLERR) return SALTS_EIO;
    flowmq_router_peer_status_t peer = FLOWMQ_ROUTER_PEER_STATUS_INIT;
    status = flowmq_router_peer_status(router, identity, identity_size, &peer);
    if (status != SALTS_OK && status != SALTS_ENOENT) return status;
    if (status == SALTS_OK && peer.ready && (remote.revents & FLOWMQ_POLLOUT) &&
        peer.completed_messages >= completed && peer.send_credit_bytes >= credit)
      return SALTS_OK;
  } while (monotonic_ms() < deadline);
  return SALTS_ETIMEDOUT;
}

static void check_kcp_peer_backpressure(const char *uri) {
  static const char slow_id[] = "slow";
  static const char fast_id[] = "fast";
  const int send_hwm = 2;
  const size_t send_hwm_bytes = FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 1u;
  const size_t receive_credit = 2u * FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE;
  const size_t remaining_credit = FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE - 1u;
  unsigned char payload[FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE];
  pattern_fixture fixture = {0};
  char endpoint[320u];
  size_t endpoint_size;
  flowmq_router_peer_status_t peer = FLOWMQ_ROUTER_PEER_STATUS_INIT;
  memset(payload, 0x65, sizeof(payload));

  pattern_fixture_init(&fixture, uri, true);
  flowmq_socket_t *router = pattern_socket(&fixture, FLOWMQ_ROUTER, true);
  flowmq_socket_t *fast = pattern_socket(&fixture, FLOWMQ_DEALER, false);
  /* A standalone peer can stop driving KCP while the shared Owner keeps polling. */
  flowmq_socket_t *slow = flowmq_socket(fixture.ctx, FLOWMQ_DEALER);
  check_not_null(slow);
  if (fixture.kcp_psk) set_test_kcp_psk(slow, 1u);
  check_equal(flowmq_setsockopt(slow, FLOWMQ_IDENTITY, slow_id, sizeof(slow_id) - 1u), SALTS_OK);
  check_equal(flowmq_setsockopt(fast, FLOWMQ_IDENTITY, fast_id, sizeof(fast_id) - 1u), SALTS_OK);
  check_equal(flowmq_setsockopt(router, FLOWMQ_SNDHWM, &send_hwm, sizeof(send_hwm)), SALTS_OK);
  check_equal(flowmq_setsockopt(router, FLOWMQ_SNDHWM_BYTES, &send_hwm_bytes,
                                sizeof(send_hwm_bytes)), SALTS_OK);
  check_equal(flowmq_setsockopt(slow, FLOWMQ_RCVHWM_BYTES, &receive_credit,
                                sizeof(receive_credit)), SALTS_OK);
  pattern_bind(&fixture, router, uri);
  check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint), &endpoint_size), SALTS_OK);
  check_equal(flowmq_connect(fast, endpoint), SALTS_OK);
  check_equal(flowmq_connect(slow, endpoint), SALTS_OK);
  check_equal(wait_for_kcp_peer(fixture.owner, router, slow, slow_id, 0u, receive_credit), SALTS_OK);
  check_equal(wait_for(fixture.owner, fast, router, FLOWMQ_POLLOUT), SALTS_OK);

  check_equal(send_routed(router, slow_id, payload, sizeof(payload)), SALTS_OK);
  /* A second large part fits the remote credit and exceeds the local byte HWM. */
  check_equal(flowmq_router_peer_status(router, slow_id, sizeof(slow_id) - 1u, &peer), SALTS_OK);
  check_equal(peer.send_credit_bytes, sizeof(payload));
  check_equal(peer.outstanding_messages, 1u);
  check_equal(send_routed(router, slow_id, payload, sizeof(payload)), SALTS_ENOBUFS);
  check_equal(send_routed(router, slow_id, "s", 1u), SALTS_OK);
  /* An empty message fits the byte budgets and exceeds the message HWM. */
  check_equal(send_routed(router, slow_id, "", 0u), SALTS_ENOBUFS);

  check_equal(send_routed(router, fast_id, "live", 4u), SALTS_OK);
  receive_part(fixture.owner, fast, router, "live", 4u, 0);
  check_equal(flowmq_send(fast, "reply", 5u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture.owner, router, fast, fast_id, sizeof(fast_id) - 1u, 1);
  receive_part(fixture.owner, router, fast, "reply", 5u, 0);
  check_equal(flowmq_router_peer_status(router, slow_id, sizeof(slow_id) - 1u, &peer), SALTS_OK);
  check_equal(peer.outstanding_messages, 2u);
  check_equal(peer.outstanding_bytes, send_hwm_bytes);
  check_equal(peer.completed_messages, 0u);
  check_equal(peer.send_credit_bytes, remaining_credit);

  /* KCP ACKs release the local HWM before the application reads any DATA. */
  check_equal(wait_for_kcp_peer(fixture.owner, router, slow, slow_id, 2u, remaining_credit), SALTS_OK);
  check_equal(flowmq_router_peer_status(router, slow_id, sizeof(slow_id) - 1u, &peer), SALTS_OK);
  check_equal(peer.completed_messages, 2u);
  check_equal(peer.completed_bytes, send_hwm_bytes);
  check_equal(peer.outstanding_messages, 0u);
  check_equal(peer.outstanding_bytes, 0u);
  check_equal(peer.send_credit_bytes, remaining_credit);
  check_equal(send_routed(router, slow_id, payload, sizeof(payload)), SALTS_ENOBUFS);

  check_received_part(slow, payload, sizeof(payload), 0);
  check_equal(wait_for_kcp_peer(fixture.owner, router, slow, slow_id, 2u, sizeof(payload)), SALTS_OK);
  payload[0] = 0x66;
  check_equal(send_routed(router, slow_id, payload, sizeof(payload)), SALTS_OK);
  check_equal(wait_for_kcp_peer(fixture.owner, router, slow, slow_id, 3u, remaining_credit), SALTS_OK);
  check_received_part(slow, "s", 1u, 0);
  check_received_part(slow, payload, sizeof(payload), 0);
  check_equal(flowmq_router_peer_status(router, slow_id, sizeof(slow_id) - 1u, &peer), SALTS_OK);
  check_equal(peer.admitted_messages, 3u);
  check_equal(peer.completed_messages, 3u);
  check_equal(peer.completed_bytes, receive_credit + 1u);
  check_equal(peer.rejected_messages, 3u);
  check_equal(peer.outstanding_messages, 0u);
  check_equal(peer.outstanding_bytes, 0u);
  unsigned char unexpected;
  size_t received_size;
  check_equal(flowmq_recv(slow, &unexpected, sizeof(unexpected), &received_size, FLOWMQ_DONTWAIT),
              SALTS_EBUSY);

  check_equal(flowmq_close(slow), SALTS_OK);
  pattern_close(&fixture, fast);
  pattern_close(&fixture, router);
  pattern_fixture_term(&fixture);
}

typedef struct kcp_terminal_trace {
  cnet_packet_session retired;
  cnet_packet_session replacement;
  size_t retired_index;
  size_t replacement_index;
  size_t size;
  uint64_t tag;
  size_t cancellations;
} kcp_terminal_trace;

static void trace_kcp_terminal(void *user, size_t peer_index,
                               cnet_packet_session session, size_t size,
                               int status, uint64_t tag) {
  kcp_terminal_trace *trace = user;
  if (status == SALTS_ECANCELED) {
    ++trace->cancellations;
    trace->retired = session;
    trace->retired_index = peer_index;
    trace->size = size;
    trace->tag = tag;
  } else if (status == SALTS_OK && trace->cancellations == 1u &&
             session.slot == trace->retired.slot) {
    trace->replacement = session;
    trace->replacement_index = peer_index;
  }
}

static void check_kcp_peer_generation(const char *uri) {
  static const char identity[] = "reused";
  static const char neighbor_id[] = "neighbor";
  const int heartbeat_interval = 25;
  const int heartbeat_timeout = 500;
  const int send_hwm = 1;
  const size_t credit = 16u;
  pattern_fixture fixture = {0};
  kcp_terminal_trace trace = {0};
  flowmq_router_peer_status_t peer = FLOWMQ_ROUTER_PEER_STATUS_INIT;
  char endpoint[320u];
  size_t endpoint_size;
  size_t ready;

  pattern_fixture_init(&fixture, uri, true);
  flowmq_socket_t *router = pattern_socket(&fixture, FLOWMQ_ROUTER, true);
  flowmq_socket_t *neighbor = pattern_socket(&fixture, FLOWMQ_DEALER, false);
  flowmq_socket_t *old = flowmq_socket(fixture.ctx, FLOWMQ_DEALER);
  flowmq_socket_t *replacement = flowmq_socket(fixture.ctx, FLOWMQ_DEALER);
  check_not_null(old);
  check_not_null(replacement);
  if (fixture.kcp_psk) {
    set_test_kcp_psk(old, 1u);
    set_test_kcp_psk(replacement, 1u);
  }
  check_equal(flowmq_setsockopt(old, FLOWMQ_IDENTITY, identity, strlen(identity)), SALTS_OK);
  check_equal(flowmq_setsockopt(replacement, FLOWMQ_IDENTITY, identity, strlen(identity)),
              SALTS_OK);
  check_equal(flowmq_setsockopt(replacement, FLOWMQ_RCVHWM_BYTES, &credit, sizeof(credit)),
              SALTS_OK);
  check_equal(flowmq_setsockopt(neighbor, FLOWMQ_IDENTITY, neighbor_id, strlen(neighbor_id)),
              SALTS_OK);
  check_equal(flowmq_setsockopt(router, FLOWMQ_SNDHWM, &send_hwm, sizeof(send_hwm)),
              SALTS_OK);
  check_equal(flowmq_setsockopt(router, FLOWMQ_HEARTBEAT_IVL, &heartbeat_interval,
                               sizeof(heartbeat_interval)), SALTS_OK);
  check_equal(flowmq_setsockopt(router, FLOWMQ_HEARTBEAT_TIMEOUT, &heartbeat_timeout,
                               sizeof(heartbeat_timeout)), SALTS_OK);
  pattern_bind(&fixture, router, uri);
  check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint), &endpoint_size), SALTS_OK);
  check_equal(flowmq_connect(neighbor, endpoint), SALTS_OK);
  check_equal(wait_for(fixture.owner, neighbor, router, FLOWMQ_POLLOUT), SALTS_OK);
  check_equal(flowmq_connect(old, endpoint), SALTS_OK);
  check_equal(wait_for_kcp_peer(fixture.owner, router, old, identity, 0u, 1u), SALTS_OK);

  cnet_packet_send_terminal_fn terminal =
      flowmq_socket_test_observe_packet_sends(router, trace_kcp_terminal, &trace);
  check_equal(send_routed(router, identity, "old", 3u), SALTS_OK);
  check_equal(flowmq_router_peer_status(router, identity, strlen(identity), &peer), SALTS_OK);
  check_equal(peer.outstanding_messages, 1u);
  check_equal(peer.completed_messages, 0u);

  /* Stop polling the old DEALER so its DATA remains unacknowledged at expiry. */
  flowmq_pollitem_t item = {.socket = router};
  const uint64_t deadline = monotonic_ms() + 5000u;
  int status;
  do {
    check_equal(flowmq_owner_poll(fixture.owner, &item, 1u, 5u, &ready), SALTS_OK);
    status = flowmq_router_peer_status(router, identity, strlen(identity), &peer);
  } while ((status == SALTS_OK || trace.cancellations == 0u) && monotonic_ms() < deadline);
  check_equal(status, SALTS_ENOENT);
  check_equal(trace.cancellations, 1u);
  check_true(cnet_packet_session_valid(trace.retired));

  check_equal(flowmq_connect(replacement, endpoint), SALTS_OK);
  check_equal(wait_for_kcp_peer(fixture.owner, router, replacement, identity, 0u, credit),
              SALTS_OK);
  check_equal(trace.replacement.slot, trace.retired.slot);
  check_true(trace.replacement.generation != trace.retired.generation);
  check_equal(trace.replacement_index, trace.retired_index);

  check_equal(flowmq_send(router, identity, strlen(identity),
                          FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
  check_equal(flowmq_send(router, "new", 3u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
  check_equal(flowmq_send(router, "tail", 4u, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(flowmq_owner_poll(fixture.owner, &item, 1u, 0u, &ready), SALTS_OK);

  /* CNet drains before CLOSED. Inject both stale terminal outcomes into the
   * actual FlowMQ callback while replacement DATA waits for its own ACK. */
  terminal(router, NULL, trace.retired, trace.size, SALTS_OK, trace.tag);
  terminal(router, NULL, trace.retired, trace.size, SALTS_ECANCELED, trace.tag);
  check_equal(trace.cancellations, 1u);
  check_equal(flowmq_router_peer_status(router, identity, strlen(identity), &peer), SALTS_OK);
  check_true(peer.ready);
  check_equal(peer.admitted_messages, 1u);
  check_equal(peer.completed_messages, 0u);
  check_equal(peer.completed_bytes, 0u);
  check_equal(peer.outstanding_messages, 1u);
  check_equal(peer.outstanding_bytes, 7u);
  check_equal(peer.send_credit_bytes, credit - 7u);
  check_equal(send_routed(router, identity, "hwm", 3u), SALTS_ENOBUFS);

  check_equal(send_routed(router, neighbor_id, "live", 4u), SALTS_OK);
  receive_part(fixture.owner, neighbor, router, "live", 4u, 0);
  check_equal(flowmq_send(neighbor, "reply", 5u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture.owner, router, neighbor, neighbor_id, strlen(neighbor_id), 1);
  receive_part(fixture.owner, router, neighbor, "reply", 5u, 0);

  check_equal(wait_for_kcp_peer(fixture.owner, router, replacement, identity, 1u, credit - 7u),
              SALTS_OK);
  check_received_part(replacement, "new", 3u, 1);
  check_received_part(replacement, "tail", 4u, 0);
  check_equal(wait_for_kcp_peer(fixture.owner, router, replacement, identity, 1u, credit),
              SALTS_OK);
  check_equal(flowmq_router_peer_status(router, identity, strlen(identity), &peer), SALTS_OK);
  check_equal(peer.completed_messages, 1u);
  check_equal(peer.completed_bytes, 7u);
  check_equal(peer.outstanding_messages, 0u);
  check_equal(peer.outstanding_bytes, 0u);
  check_equal(peer.rejected_messages, 1u);
  unsigned char unexpected;
  size_t received_size;
  check_equal(flowmq_recv(replacement, &unexpected, sizeof(unexpected), &received_size,
                          FLOWMQ_DONTWAIT), SALTS_EBUSY);

  flowmq_socket_test_observe_packet_sends(router, NULL, NULL);
  check_equal(flowmq_close(old), SALTS_OK);
  check_equal(flowmq_close(replacement), SALTS_OK);
  pattern_close(&fixture, neighbor);
  pattern_close(&fixture, router);
  pattern_fixture_term(&fixture);
}

static void check_subscription(pattern_fixture *fixture, const char *uri) {
  static const unsigned char subscribed[] = {1u, 'f', 'm', 'q', '.'};
  const int retry_ms = 5;
  const int heartbeat_ms = 25;
  const int timeout_ms = 500;
  flowmq_socket_t *xpub = pattern_socket(fixture, FLOWMQ_XPUB, true);
  flowmq_socket_t *sub = pattern_socket(fixture, FLOWMQ_SUB, false);
  char endpoint[320u];
  size_t endpoint_size;
  check_equal(flowmq_setsockopt(sub, FLOWMQ_SUBSCRIBE, "fmq.", 4u), SALTS_OK);
  check_equal(flowmq_setsockopt(sub, FLOWMQ_RECONNECT_IVL, &retry_ms, sizeof(retry_ms)),
              SALTS_OK);
  if (fixture->kcp) {
    check_equal(flowmq_setsockopt(sub, FLOWMQ_HEARTBEAT_IVL, &heartbeat_ms,
                                 sizeof(heartbeat_ms)), SALTS_OK);
    check_equal(flowmq_setsockopt(sub, FLOWMQ_HEARTBEAT_TIMEOUT, &timeout_ms,
                                 sizeof(timeout_ms)), SALTS_OK);
  }
  pattern_bind(fixture, xpub, uri);
  check_equal(flowmq_last_endpoint(xpub, endpoint, sizeof(endpoint), &endpoint_size), SALTS_OK);
  check_equal(flowmq_connect(sub, endpoint), SALTS_OK);
  receive_part(fixture->owner, xpub, sub, subscribed, sizeof(subscribed), 0);
  check_equal(flowmq_send(xpub, "other", 5u, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(flowmq_send(xpub, "fmq.before", 10u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture->owner, sub, xpub, "fmq.before", 10u, 0);

  pattern_close(fixture, xpub);
  if (fixture->kcp && fixture->owner != NULL) {
    /* Drive the subscriber through a closed UDP port before rebinding. */
    flowmq_pollitem_t item = {.socket = sub};
    const uint64_t offline_until = monotonic_ms() + 150u;
    do {
      size_t ready;
      check_equal(flowmq_owner_poll(fixture->owner, &item, 1u, 5u, &ready), SALTS_OK);
    } while (monotonic_ms() < offline_until);
  }
  xpub = pattern_socket(fixture, FLOWMQ_XPUB, true);
  pattern_bind(fixture, xpub, endpoint);
  receive_part(fixture->owner, xpub, sub, subscribed, sizeof(subscribed), 0);
  check_equal(flowmq_send(xpub, "other", 5u, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(flowmq_send(xpub, "fmq.after", 9u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture->owner, sub, xpub, "fmq.after", 9u, 0);

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
  flowmq_socket_t *dealer = flowmq_socket(fixture.ctx, FLOWMQ_DEALER);
  check_not_null(dealer);
  check_equal(flowmq_setsockopt(router, FLOWMQ_HEARTBEAT_IVL, &heartbeat_ms,
                               sizeof(heartbeat_ms)), SALTS_OK);
  check_equal(flowmq_setsockopt(router, FLOWMQ_HEARTBEAT_TIMEOUT, &timeout_ms,
                               sizeof(timeout_ms)), SALTS_OK);
  check_equal(flowmq_setsockopt(dealer, FLOWMQ_IDENTITY, "gone", 4u), SALTS_OK);
  pattern_bind(&fixture, router, uri);
  check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint), &endpoint_size),
              SALTS_OK);
  check_equal(flowmq_connect(dealer, endpoint), SALTS_OK);
  flowmq_pollitem_t receiver = {.socket = dealer, .events = FLOWMQ_POLLOUT};
  flowmq_pollitem_t sender = {.socket = router, .events = FLOWMQ_POLLOUT};
  const uint64_t connect_deadline = monotonic_ms() + 5000u;
  do {
    size_t ready;
    check_equal(flowmq_poll(&receiver, 1u, 1u, &ready), SALTS_OK);
    check_equal(shared ? flowmq_owner_poll(fixture.owner, &sender, 1u, 1u, &ready)
                       : flowmq_poll(&sender, 1u, 1u, &ready), SALTS_OK);
    check_equal((receiver.revents | sender.revents) & FLOWMQ_POLLERR, 0);
  } while ((receiver.revents & sender.revents & FLOWMQ_POLLOUT) == 0 &&
           monotonic_ms() < connect_deadline);
  check_equal(receiver.revents & sender.revents & FLOWMQ_POLLOUT, FLOWMQ_POLLOUT);

  /* The receiver stays bound but is no longer driven, so it cannot ACK. */
  check_equal(flowmq_send(router, "gone", 4u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
  check_equal(flowmq_send(router, "no ack", 6u, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(flowmq_router_peer_status(router, "gone", 4u, &peer), SALTS_OK);
  check_equal(peer.outstanding_messages, 1u);
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
  check_equal(flowmq_close(dealer), SALTS_OK);
  pattern_close(&fixture, router);
  pattern_fixture_term(&fixture);
}

static void wss_authentication(bool shared) {
  static const char uri[] = "wss://127.0.0.1:0/fmq";
  static const char identity[] = "trusted";
  static const struct {
    const char *name;
    const char *identity;
    bool trust_server;
    bool present_certificate;
    bool rejected_hello;
  } rejected[] = {
      {"localhost", identity, false, true, false},
      {"wrong.example", identity, true, true, false},
      {"localhost", identity, true, false, false},
      {"localhost", "forged", true, true, true}};
  const int required = 1;
  const int reconnect = -1;
  const flowmq_tls_identity_binding_t binding = {
      sizeof(binding), "sha256:" FLOWMQ_TLS_TEST_CERTIFICATE_SHA256, identity};
  flowmq_tls_identity_map_config_t policy = FLOWMQ_TLS_IDENTITY_MAP_CONFIG_INIT;
  pattern_fixture fixture = {0};
  char endpoint[320u];
  size_t endpoint_size;
  pattern_fixture_init(&fixture, uri, shared);
  flowmq_socket_t *router = pattern_socket(&fixture, FLOWMQ_ROUTER, true);
  policy.bindings = &binding;
  policy.binding_count = 1u;
  check_equal(flowmq_setsockopt(router, FLOWMQ_TLS_CA_FILE, fixture.tls.ca,
                               strlen(fixture.tls.ca)), SALTS_OK);
  check_equal(flowmq_setsockopt(router, FLOWMQ_TLS_REQUIRE_CLIENT_CERTIFICATE, &required,
                               sizeof(required)), SALTS_OK);
  check_equal(flowmq_setsockopt(router, FLOWMQ_TLS_IDENTITY_POLICY, &policy, sizeof(policy)),
              SALTS_OK);
  pattern_bind(&fixture, router, uri);
  check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint), &endpoint_size), SALTS_OK);

  for (size_t i = 0u; i < sizeof(rejected) / sizeof(rejected[0]); ++i) {
    flowmq_socket_t *client = shared ? flowmq_owner_socket(fixture.owner, FLOWMQ_DEALER)
                                    : flowmq_socket(fixture.ctx, FLOWMQ_DEALER);
    check_not_null(client);
    if (rejected[i].trust_server)
      check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_CA_FILE, fixture.tls.ca,
                                   strlen(fixture.tls.ca)), SALTS_OK);
    if (rejected[i].present_certificate) {
      check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_CERT_FILE, fixture.tls.cert,
                                   strlen(fixture.tls.cert)), SALTS_OK);
      check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_KEY_FILE, fixture.tls.key,
                                   strlen(fixture.tls.key)), SALTS_OK);
    }
    check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_SERVER_NAME, rejected[i].name,
                                 strlen(rejected[i].name)), SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_IDENTITY, rejected[i].identity,
                                 strlen(rejected[i].identity)), SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_RECONNECT_IVL, &reconnect,
                                 sizeof(reconnect)), SALTS_OK);
    check_equal(flowmq_connect(client, endpoint), SALTS_OK);
    flowmq_pollitem_t items[] = {
        {.socket = client, .events = FLOWMQ_POLLOUT},
        {.socket = router, .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT}};
    const uint64_t deadline = monotonic_ms() + (rejected[i].rejected_hello ? 5000u : 250u);
    uint64_t rejections;
    do {
      size_t ready;
      size_t size = sizeof(rejections);
      check_equal(shared ? flowmq_owner_poll(fixture.owner, items, 2u, 10u, &ready)
                         : flowmq_poll(items, 2u, 10u, &ready), SALTS_OK);
      check((items[0].revents & FLOWMQ_POLLOUT) == 0,
            "Rejected WSS client became writable (case %zu, shared %d, events %d)",
            i, (int)shared, (int)items[0].revents);
      check(items[1].revents == 0,
            "WSS ROUTER reported events for a rejected client (case %zu, shared %d, events %d)",
            i, (int)shared, (int)items[1].revents);
      check_equal(flowmq_getsockopt(router, FLOWMQ_TLS_IDENTITY_REJECTIONS, &rejections,
                                   &size), SALTS_OK);
    } while (rejections == 0u && monotonic_ms() < deadline);
    check_equal(rejections, rejected[i].rejected_hello ? 1u : 0u);
    pattern_close(&fixture, client);
  }

  flowmq_socket_t *authorized = pattern_socket(&fixture, FLOWMQ_DEALER, false);
  check_equal(flowmq_setsockopt(authorized, FLOWMQ_TLS_CERT_FILE, fixture.tls.cert,
                               strlen(fixture.tls.cert)), SALTS_OK);
  check_equal(flowmq_setsockopt(authorized, FLOWMQ_TLS_KEY_FILE, fixture.tls.key,
                               strlen(fixture.tls.key)), SALTS_OK);
  check_equal(flowmq_setsockopt(authorized, FLOWMQ_IDENTITY, identity, sizeof(identity) - 1u),
              SALTS_OK);
  check_equal(flowmq_connect(authorized, endpoint), SALTS_OK);
  check_equal(wait_for(fixture.owner, authorized, router, FLOWMQ_POLLOUT), SALTS_OK);
  check_equal(flowmq_send(authorized, "authorized", 10u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture.owner, router, authorized, identity, sizeof(identity) - 1u, 1);
  receive_part(fixture.owner, router, authorized, "authorized", 10u, 0);
  pattern_close(&fixture, authorized);
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

static void reject_kcp_authentication(bool shared) {
  const char uri[] = "kcp://127.0.0.1:0?auth=psk";
  const char psk_query[] = "?auth=psk";
  const int reconnect = -1;
  pattern_fixture fixture = {0};
  char endpoint[320u], plain_endpoint[320u];
  size_t endpoint_size, ready;
  pattern_fixture_init(&fixture, uri, shared);
  flowmq_socket_t *server = pattern_socket(&fixture, FLOWMQ_ROUTER, true);
  flowmq_socket_t *bad = pattern_socket(&fixture, FLOWMQ_DEALER, false);
  set_test_kcp_psk(bad, 2u);
  check_equal(flowmq_setsockopt(bad, FLOWMQ_RECONNECT_IVL, &reconnect,
                               sizeof(reconnect)), SALTS_OK);
  pattern_bind(&fixture, server, uri);
  check_equal(flowmq_last_endpoint(server, endpoint, sizeof(endpoint), &endpoint_size),
              SALTS_OK);
  check_true(endpoint_size > sizeof(psk_query));
  const size_t query_offset = endpoint_size - sizeof(psk_query);
  check_equal(strcmp(endpoint + query_offset, psk_query), 0);
  memcpy(plain_endpoint, endpoint, query_offset);
  memcpy(plain_endpoint + query_offset, "?conv=42", sizeof("?conv=42"));

  flowmq_socket_t *plain = shared ? flowmq_owner_socket(fixture.owner, FLOWMQ_DEALER)
                                   : flowmq_socket(fixture.ctx, FLOWMQ_DEALER);
  check_not_null(plain);
  check_equal(flowmq_connect(bad, endpoint), SALTS_OK);
  check_equal(flowmq_connect(bad, endpoint), SALTS_EALREADY);
  check_equal(flowmq_connect(plain, plain_endpoint), SALTS_OK);
  flowmq_pollitem_t items[] = {
      {.socket = server, .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT},
      {.socket = bad, .events = FLOWMQ_POLLOUT},
      {.socket = plain, .events = FLOWMQ_POLLOUT}};
  /* In the shared case also cross the bounded authentication deadline and
   * prove the old CNet handle no longer owns this destination. */
  check_equal(shared ? flowmq_owner_poll(fixture.owner, items, 3u, 5300u, &ready)
                     : flowmq_poll(items, 3u, 300u, &ready), SALTS_OK);
  check_equal(ready, 0u);
  if (shared) check_equal(flowmq_connect(bad, endpoint), SALTS_OK);
  pattern_close(&fixture, bad);
  pattern_close(&fixture, plain);

  flowmq_socket_t *good = pattern_socket(&fixture, FLOWMQ_DEALER, false);
  check_equal(flowmq_setsockopt(good, FLOWMQ_IDENTITY, "psk", 3u), SALTS_OK);
  check_equal(flowmq_connect(good, endpoint), SALTS_OK);
  check_equal(wait_for(fixture.owner, good, server, FLOWMQ_POLLOUT), SALTS_OK);
  check_equal(flowmq_send(good, "authenticated", 13u, FLOWMQ_DONTWAIT), SALTS_OK);
  receive_part(fixture.owner, server, good, "psk", 3u, 1);
  receive_part(fixture.owner, server, good, "authenticated", 13u, 0);
  pattern_close(&fixture, good);
  pattern_close(&fixture, server);
  pattern_fixture_term(&fixture);
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

  it("preserves request, routing and subscription recovery over authenticated KCP") {
    messaging_patterns("kcp://127.0.0.1:0?auth=psk", false);
    messaging_patterns("kcp://127.0.0.1:0?auth=psk", true);
  }

  it("rejects plain and wrong-key KCP peers and retires an incomplete handshake") {
    reject_kcp_authentication(false);
    reject_kcp_authentication(true);
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

  it("enforces WSS trust, hostname and mutual TLS ROUTER identity before READY") {
    wss_authentication(false);
    wss_authentication(true);
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

  it("isolates a KCP peer stalled at its message and byte HWM from a healthy neighbor") {
    check_kcp_peer_backpressure("kcp://127.0.0.1:0?conv=4281");
    check_kcp_peer_backpressure("kcp://127.0.0.1:0?auth=psk");
  }

  it("isolates stale KCP terminals after a ROUTER peer slot is reused") {
    check_kcp_peer_generation("kcp://127.0.0.1:0?conv=4281");
    check_kcp_peer_generation("kcp://127.0.0.1:0?auth=psk");
  }

  it("carries the full KCP part size with PSK overhead on a shared Owner") {
    round_trip("kcp://127.0.0.1:0?auth=psk", FLOWMQ_TRANSPORT_KCP, true);
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

  it("requires a write-only nonzero key and an explicit authenticated endpoint") {
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *socket = flowmq_socket(ctx, FLOWMQ_PAIR);
    const unsigned char zero[FLOWMQ_KCP_PSK_BYTES] = {0};
    size_t size = sizeof(zero);
    unsigned char value[FLOWMQ_KCP_PSK_BYTES];
    check_not_null(socket);
    check_equal(flowmq_bind(socket, "kcp://127.0.0.1:0?auth=psk"), SALTS_EINVAL);
    check_equal(flowmq_connect(socket, "kcp://127.0.0.1:1?auth=psk"), SALTS_EINVAL);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_KCP_PSK, zero, size - 1u), SALTS_EINVAL);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_KCP_PSK, zero, size), SALTS_EINVAL);
    set_test_kcp_psk(socket, 1u);
    check_equal(flowmq_getsockopt(socket, FLOWMQ_KCP_PSK, value, &size), SALTS_ENOTSUP);
    check_equal(flowmq_connect(socket, "kcp://127.0.0.1:1?conv=42"), SALTS_EINVAL);
    check_equal(flowmq_bind(socket, "tcp://127.0.0.1:0"), SALTS_EINVAL);
    check_equal(flowmq_bind(socket, "kcp://127.0.0.1:0?auth=psk&conv=42"), SALTS_EINVAL);
    check_equal(flowmq_bind(socket, "kcp://127.0.0.1:0?auth=psk"), SALTS_OK);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_KCP_PSK, zero, size), SALTS_EBUSY);
    check_equal(flowmq_close(socket), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }
}
