#include "flowmq_owner.h"
#include "tinytest.h"
#include "salts_error.h"

#include <stdbool.h>
#include <stddef.h>
#include <string.h>

enum { OWNER_TEST_PROGRESS_LIMIT = 20000u, OWNER_TEST_LANES = 2 };

static int owner_test_progress(flowmq_owner_t *owner,
                               flowmq_socket_t *clients[OWNER_TEST_LANES],
                               flowmq_socket_t *peers[OWNER_TEST_LANES],
                               size_t lane_count) {
  flowmq_pollitem_t owner_items[OWNER_TEST_LANES] = {0};
  flowmq_pollitem_t peer_items[OWNER_TEST_LANES] = {0};
  size_t owner_ready = 0u;
  size_t peer_ready = 0u;
  int status;

  for (size_t lane = 0u; lane < lane_count; ++lane) {
    owner_items[lane] = (flowmq_pollitem_t){
        .socket = clients[lane],
        .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT | FLOWMQ_POLLERR};
    peer_items[lane] = (flowmq_pollitem_t){
        .socket = peers[lane],
        .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT | FLOWMQ_POLLERR};
  }

  status = flowmq_owner_poll(
      owner, owner_items, lane_count, 0u, &owner_ready);
  if (status != SALTS_OK) return status;
  status = flowmq_poll(peer_items, lane_count, 0u, &peer_ready);
  if (status != SALTS_OK) return status;
  (void)owner_ready;
  (void)peer_ready;
  return SALTS_OK;
}

static int owner_test_roundtrip(
    flowmq_owner_t *owner,
    flowmq_socket_t *clients[OWNER_TEST_LANES],
    flowmq_socket_t *peers[OWNER_TEST_LANES],
    size_t lane_count,
    unsigned char seed) {
  unsigned char payload[OWNER_TEST_LANES][32];
  unsigned char peer_received[OWNER_TEST_LANES][32];
  unsigned char client_received[OWNER_TEST_LANES][32];
  bool sent[OWNER_TEST_LANES] = {false, false};
  bool peer_got[OWNER_TEST_LANES] = {false, false};
  bool peer_replied[OWNER_TEST_LANES] = {false, false};
  bool client_got[OWNER_TEST_LANES] = {false, false};

  for (size_t lane = 0u; lane < lane_count; ++lane) {
    for (size_t i = 0u; i < sizeof(payload[lane]); ++i)
      payload[lane][i] =
          (unsigned char)(seed + (unsigned char)(lane * 37u + i));
  }

  for (size_t progress = 0u;
       progress < OWNER_TEST_PROGRESS_LIMIT; ++progress) {
    int status = owner_test_progress(owner, clients, peers, lane_count);
    if (status != SALTS_OK) return status;

    for (size_t lane = 0u; lane < lane_count; ++lane) {
      if (!sent[lane]) {
        status = flowmq_send(clients[lane], payload[lane],
                             sizeof(payload[lane]), FLOWMQ_DONTWAIT);
        if (status == SALTS_OK)
          sent[lane] = true;
        else if (status != SALTS_EBUSY && status != SALTS_ENOBUFS)
          return status;
      }

      if (!peer_got[lane]) {
        size_t received = 0u;
        status = flowmq_recv(peers[lane], peer_received[lane],
                             sizeof(peer_received[lane]), &received,
                             FLOWMQ_DONTWAIT);
        if (status == SALTS_OK) {
          if (received != sizeof(payload[lane]) ||
              memcmp(peer_received[lane], payload[lane],
                     sizeof(payload[lane])) != 0)
            return SALTS_EPROTO;
          peer_got[lane] = true;
        } else if (status != SALTS_EBUSY) {
          return status;
        }
      }

      if (peer_got[lane] && !peer_replied[lane]) {
        status = flowmq_send(peers[lane], peer_received[lane],
                             sizeof(peer_received[lane]), FLOWMQ_DONTWAIT);
        if (status == SALTS_OK)
          peer_replied[lane] = true;
        else if (status != SALTS_EBUSY && status != SALTS_ENOBUFS)
          return status;
      }

      if (!client_got[lane]) {
        size_t received = 0u;
        status = flowmq_recv(clients[lane], client_received[lane],
                             sizeof(client_received[lane]), &received,
                             FLOWMQ_DONTWAIT);
        if (status == SALTS_OK) {
          if (received != sizeof(payload[lane]) ||
              memcmp(client_received[lane], payload[lane],
                     sizeof(payload[lane])) != 0)
            return SALTS_EPROTO;
          client_got[lane] = true;
        } else if (status != SALTS_EBUSY) {
          return status;
        }
      }
    }

    {
      bool complete = true;
      for (size_t lane = 0u; lane < lane_count; ++lane)
        complete = complete && client_got[lane];
      if (complete) return SALTS_OK;
    }
  }
  return SALTS_ETIMEDOUT;
}

spec("FlowMQ explicit owner lane") {
  it("bounds owner capacity and leases the context") {
    flowmq_owner_config_t config = FLOWMQ_OWNER_CONFIG_INIT;
    flowmq_owner_config_t invalid = FLOWMQ_OWNER_CONFIG_INIT;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_owner_t *owner;
    flowmq_socket_t *first;
    flowmq_socket_t *second;

    check_not_null(ctx);

    invalid.size = sizeof(invalid) - 1u;
    check_null(flowmq_owner_new(ctx, &invalid));
    invalid = (flowmq_owner_config_t)FLOWMQ_OWNER_CONFIG_INIT;
    invalid.socket_capacity = FLOWMQ_OWNER_MAX_SOCKET_CAPACITY + 1u;
    check_null(flowmq_owner_new(ctx, &invalid));

    config.socket_capacity = 2u;
    owner = flowmq_owner_new(ctx, &config);
    check_not_null(owner);
    check_equal(flowmq_ctx_term(ctx), SALTS_EBUSY);

    first = flowmq_owner_socket(owner, FLOWMQ_PAIR);
    second = flowmq_owner_socket(owner, FLOWMQ_PAIR);
    check_not_null(first);
    check_not_null(second);
    check_null(flowmq_owner_socket(owner, FLOWMQ_PAIR));
    check_equal(flowmq_owner_term(owner), SALTS_EBUSY);

    check_equal(flowmq_close(first), SALTS_EBUSY);
    check_equal(flowmq_owner_close_socket(owner, first), SALTS_OK);
    check_equal(flowmq_owner_close_socket(owner, second), SALTS_OK);
    check_equal(flowmq_owner_term(owner), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("shares one wait across two clients and isolates owner close") {
    flowmq_owner_config_t config = FLOWMQ_OWNER_CONFIG_INIT;
    flowmq_owner_config_t other_config = FLOWMQ_OWNER_CONFIG_INIT;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_owner_t *owner;
    flowmq_owner_t *other;
    flowmq_socket_t *clients[OWNER_TEST_LANES] = {0};
    flowmq_socket_t *peers[OWNER_TEST_LANES] = {0};
    flowmq_socket_t *other_socket;
    char endpoints[OWNER_TEST_LANES][128] = {{0}};
    size_t endpoint_size = 0u;
    flowmq_pollitem_t ordinary_item;
    flowmq_pollitem_t wrong_item;
    size_t ready = 0u;

    check_not_null(ctx);
    config.socket_capacity = OWNER_TEST_LANES;
    other_config.socket_capacity = 1u;
    owner = flowmq_owner_new(ctx, &config);
    other = flowmq_owner_new(ctx, &other_config);
    check_not_null(owner);
    check_not_null(other);

    for (size_t lane = 0u; lane < OWNER_TEST_LANES; ++lane) {
      clients[lane] = flowmq_owner_socket(owner, FLOWMQ_PAIR);
      peers[lane] = flowmq_socket(ctx, FLOWMQ_PAIR);
      check_not_null(clients[lane]);
      check_not_null(peers[lane]);
      check_equal(flowmq_bind(peers[lane], "tcp://127.0.0.1:0"), SALTS_OK);
      check_equal(flowmq_last_endpoint(
                      peers[lane], endpoints[lane],
                      sizeof(endpoints[lane]), &endpoint_size),
                  SALTS_OK);
      check_equal(flowmq_connect(clients[lane], endpoints[lane]), SALTS_OK);
    }

    /* Owner sockets are client-only in the first product slice. */
    check_equal(flowmq_bind(clients[0], "tcp://127.0.0.1:0"),
                SALTS_ENOTSUP);

    /* Ordinary poll must not silently fall back to the 1 ms sleep loop. */
    ordinary_item = (flowmq_pollitem_t){
        .socket = clients[0],
        .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT | FLOWMQ_POLLERR};
    check_equal(flowmq_poll(&ordinary_item, 1u, 0u, &ready),
                SALTS_ENOTSUP);

    /* Items from another execution domain are rejected by owner poll. */
    wrong_item = (flowmq_pollitem_t){
        .socket = peers[0],
        .events = FLOWMQ_POLLIN};
    check_equal(flowmq_owner_poll(owner, &wrong_item, 1u, 0u, &ready),
                SALTS_EINVAL);

    other_socket = flowmq_owner_socket(other, FLOWMQ_PAIR);
    check_not_null(other_socket);
    check_equal(flowmq_owner_close_socket(other, clients[0]), SALTS_EINVAL);

    check_equal(owner_test_roundtrip(
                    owner, clients, peers, OWNER_TEST_LANES, 0x21u),
                SALTS_OK);

    /*
     * Retire lane 0 while lane 1 remains live. Close the ordinary peer first
     * so owner close can observe the terminal path without another thread.
     */
    check_equal(flowmq_close(peers[0]), SALTS_OK);
    peers[0] = NULL;
    check_equal(flowmq_owner_close_socket(owner, clients[0]), SALTS_OK);
    clients[0] = NULL;

    {
      flowmq_socket_t *single_client[OWNER_TEST_LANES] = {
          clients[1], NULL};
      flowmq_socket_t *single_peer[OWNER_TEST_LANES] = {
          peers[1], NULL};
      check_equal(owner_test_roundtrip(
                      owner, single_client, single_peer, 1u, 0x71u),
                  SALTS_OK);
    }

    check_equal(flowmq_close(peers[1]), SALTS_OK);
    peers[1] = NULL;
    check_equal(flowmq_owner_close_socket(owner, clients[1]), SALTS_OK);
    clients[1] = NULL;
    check_equal(flowmq_owner_term(owner), SALTS_OK);

    check_equal(flowmq_owner_close_socket(other, other_socket), SALTS_OK);
    check_equal(flowmq_owner_term(other), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }
}
