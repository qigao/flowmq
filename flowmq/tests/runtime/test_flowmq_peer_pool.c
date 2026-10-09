#include "flowmq_peer_pool.h"
#include "flowmq_owner.h"
#include "tinytest.h"
#include "cmeta_error.h"

#include <string.h>

enum { POOL_TEST_PROGRESS_LIMIT = 10000, POOL_TEST_SOCKETS = 4 };

static flowmq_peer_pool_snapshot_t pool_snapshot(flowmq_socket_t *socket) {
  flowmq_peer_pool_snapshot_t state = FLOWMQ_PEER_POOL_SNAPSHOT_INIT;
  check_equal(flowmq_socket_get_peer_pool(socket, &state), SALTS_OK);
  return state;
}

static void pool_configure(flowmq_socket_t *socket, size_t peers, size_t connecting) {
  flowmq_peer_pool_config_t config = FLOWMQ_PEER_POOL_CONFIG_INIT;
  const int reconnect = -1;
  config.max_peers = peers;
  config.max_connecting = connecting;
  check_equal(flowmq_socket_set_peer_pool(socket, &config), SALTS_OK);
  check_equal(flowmq_setsockopt(socket, FLOWMQ_RECONNECT_IVL,
                               &reconnect, sizeof(reconnect)), SALTS_OK);
}

static void pool_progress(flowmq_socket_t *a, flowmq_socket_t *b) {
  flowmq_pollitem_t items[] = {{.socket = a}, {.socket = b}};
  size_t ready = 0u;
  check_equal(flowmq_poll(items, b == NULL ? 1u : 2u, 1u, &ready), SALTS_OK);
}

static void pool_wait_ready(flowmq_socket_t *a, size_t count, flowmq_socket_t *b) {
  for (size_t i = 0; i < POOL_TEST_PROGRESS_LIMIT; ++i) {
    if (pool_snapshot(a).ready == count && pool_snapshot(b).ready != 0u) return;
    pool_progress(a, b);
  }
  check_equal(pool_snapshot(a).ready, count);
  check_true(pool_snapshot(b).ready != 0u);
}

spec("bounded long-lived peer pool") {
  static flowmq_ctx_t *ctx;
  static flowmq_owner_t *owner;
  static flowmq_socket_t *sockets[POOL_TEST_SOCKETS];

  before_each() {
    memset(sockets, 0, sizeof(sockets));
    ctx = flowmq_ctx_new();
    owner = NULL;
    check_not_null(ctx);
  }

  after_each() {
    for (size_t i = 0; i < POOL_TEST_SOCKETS; ++i) {
      if (sockets[i] == NULL) continue;
      if (owner != NULL && i == 1u)
        check_equal(flowmq_owner_close_socket(owner, sockets[i]), SALTS_OK);
      else
        check_equal(flowmq_close(sockets[i]), SALTS_OK);
    }
    if (owner != NULL) check_equal(flowmq_owner_term(owner), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("is opt-in, validates zero and bounds, and freezes config at startup") {
    flowmq_peer_pool_config_t config = FLOWMQ_PEER_POOL_CONFIG_INIT;
    flowmq_peer_pool_snapshot_t snapshot = FLOWMQ_PEER_POOL_SNAPSHOT_INIT;
    sockets[0] = flowmq_socket(ctx, FLOWMQ_PULL);
    check_not_null(sockets[0]);
    check_equal(pool_snapshot(sockets[0]).enabled, 0);
    check_equal(pool_snapshot(sockets[0]).drained, 1);
    check_equal(flowmq_socket_set_peer_pool(NULL, &config), SALTS_EINVAL);
    check_equal(flowmq_socket_set_peer_pool(sockets[0], NULL), SALTS_EINVAL);
    config.size--;
    check_equal(flowmq_socket_set_peer_pool(sockets[0], &config), SALTS_EINVAL);
    config = (flowmq_peer_pool_config_t)FLOWMQ_PEER_POOL_CONFIG_INIT;
    config.version++;
    check_equal(flowmq_socket_set_peer_pool(sockets[0], &config), SALTS_EINVAL);
    config = (flowmq_peer_pool_config_t)FLOWMQ_PEER_POOL_CONFIG_INIT;
    config.max_peers = 0u;
    check_equal(flowmq_socket_set_peer_pool(sockets[0], &config), SALTS_EINVAL);
    config.max_peers = FLOWMQ_PEER_POOL_MAX_PEERS + 1u;
    check_equal(flowmq_socket_set_peer_pool(sockets[0], &config), SALTS_EINVAL);
    config.max_peers = 1u;
    config.max_connecting = 0u;
    check_equal(flowmq_socket_set_peer_pool(sockets[0], &config), SALTS_EINVAL);
    config.max_connecting = 2u;
    check_equal(flowmq_socket_set_peer_pool(sockets[0], &config), SALTS_EINVAL);
    config.max_connecting = 1u;
    check_equal(flowmq_socket_set_peer_pool(sockets[0], &config), SALTS_OK);
    config.max_peers = 4u; /* API copied the input. */
    check_equal(pool_snapshot(sockets[0]).max_peers, 1u);
    snapshot.size--;
    check_equal(flowmq_socket_get_peer_pool(sockets[0], &snapshot), SALTS_EINVAL);
    check_equal(flowmq_socket_get_peer_pool(sockets[0], NULL), SALTS_EINVAL);
    check_equal(flowmq_socket_get_peer_pool(NULL, &snapshot), SALTS_EINVAL);
    check_equal(flowmq_bind(sockets[0], "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_socket_set_peer_pool(sockets[0], &config), SALTS_EBUSY);
    check_equal(pool_snapshot(sockets[0]).enabled, 1);
    check_equal(pool_snapshot(sockets[0]).physical_in_use, 0u);
  }

  it("reserves connecting before dial and recovers physical credit after terminal") {
    char endpoints[3][128] = {{0}};
    size_t size = 0u;
    sockets[0] = flowmq_socket(ctx, FLOWMQ_PUSH);
    check_not_null(sockets[0]);
    pool_configure(sockets[0], 2u, 1u);
    for (size_t i = 1; i < POOL_TEST_SOCKETS; ++i) {
      sockets[i] = flowmq_socket(ctx, FLOWMQ_PULL);
      check_not_null(sockets[i]);
      pool_configure(sockets[i], 1u, 1u);
      check_equal(flowmq_bind(sockets[i], "tcp://127.0.0.1:0"), SALTS_OK);
      check_equal(flowmq_last_endpoint(sockets[i], endpoints[i - 1],
                                      sizeof(endpoints[0]), &size), SALTS_OK);
    }
    check_equal(flowmq_connect(sockets[0], endpoints[0]), SALTS_OK);
    check_equal(pool_snapshot(sockets[0]).connecting, 1u);
    check_equal(pool_snapshot(sockets[0]).active_leases, 0u);
    check_equal(flowmq_connect(sockets[0], endpoints[1]), SALTS_ENOBUFS);
    /* Advancing TCP without the listener's HELLO/SETTINGS cannot create a lease. */
    for (size_t i = 0; i < 8u; ++i) pool_progress(sockets[0], NULL);
    check_equal(pool_snapshot(sockets[0]).connecting, 1u);
    check_equal(pool_snapshot(sockets[0]).ready, 0u);
    check_equal(pool_snapshot(sockets[0]).active_leases, 0u);
    pool_wait_ready(sockets[0], 1u, sockets[1]);
    check_equal(pool_snapshot(sockets[0]).active_leases, 1u);
    check_equal(flowmq_connect(sockets[0], endpoints[1]), SALTS_OK);
    check_equal(pool_snapshot(sockets[0]).connecting, 1u);
    pool_wait_ready(sockets[0], 2u, sockets[2]);
    check_equal(pool_snapshot(sockets[0]).active_leases, 2u);
    check_equal(flowmq_connect(sockets[0], endpoints[2]), SALTS_ENOBUFS);
    check_equal(flowmq_close(sockets[1]), SALTS_OK);
    sockets[1] = NULL;
    for (size_t i = 0; i < POOL_TEST_PROGRESS_LIMIT &&
         pool_snapshot(sockets[0]).physical_in_use != 1u; ++i)
      pool_progress(sockets[0], sockets[2]);
    check_equal(pool_snapshot(sockets[0]).physical_in_use, 1u);
    check_equal(pool_snapshot(sockets[0]).active_leases, 1u);
    check_equal(flowmq_connect(sockets[0], endpoints[2]), SALTS_OK);
    pool_wait_ready(sockets[0], 2u, sockets[3]);
    check_equal(pool_snapshot(sockets[0]).active_leases, 2u);
  }

  for (size_t external = 0u; external < 2u; ++external) {
    it("drains shutdown at connecting and handshake stages, external=%zu", external) {
      if (external != 0u) {
        flowmq_owner_config_t config = FLOWMQ_OWNER_CONFIG_INIT;
        config.socket_capacity = 1u;
        owner = flowmq_owner_new(ctx, &config);
        check_not_null(owner);
      }
      for (size_t turns = 0u; turns < 8u; ++turns) {
        char endpoint[128] = {0};
        size_t size = 0u;
        size_t ready = 0u;
        sockets[0] = flowmq_socket(ctx, FLOWMQ_PULL);
        sockets[1] = owner != NULL ? flowmq_owner_socket(owner, FLOWMQ_PUSH)
                                   : flowmq_socket(ctx, FLOWMQ_PUSH);
        check_not_null(sockets[0]);
        check_not_null(sockets[1]);
        pool_configure(sockets[0], 1u, 1u);
        pool_configure(sockets[1], 1u, 1u);
        check_equal(flowmq_bind(sockets[0], "tcp://127.0.0.1:0"), SALTS_OK);
        check_equal(flowmq_last_endpoint(sockets[0], endpoint, sizeof(endpoint), &size), SALTS_OK);
        check_equal(flowmq_connect(sockets[1], endpoint), SALTS_OK);
        for (size_t i = 0u; i < turns; ++i) {
          if (owner != NULL) {
            flowmq_pollitem_t item = {.socket = sockets[1]};
            check_equal(flowmq_owner_poll(owner, &item, 1u, 0u, &ready), SALTS_OK);
            pool_progress(sockets[0], NULL);
          } else {
            pool_progress(sockets[0], sockets[1]);
          }
        }
        if (owner != NULL)
          check_equal(flowmq_owner_close_socket(owner, sockets[1]), SALTS_OK);
        else
          check_equal(flowmq_close(sockets[1]), SALTS_OK);
        sockets[1] = NULL;
        check_equal(flowmq_close(sockets[0]), SALTS_OK);
        sockets[0] = NULL;
      }
    }
  }

  it("holds a retired generation until queued receive parts are consumed") {
    char endpoint[128] = {0};
    size_t size = 0u;
    size_t ready = 0u;
    unsigned char byte = 0u;
    flowmq_pollitem_t item;
    sockets[0] = flowmq_socket(ctx, FLOWMQ_PULL);
    sockets[1] = flowmq_socket(ctx, FLOWMQ_PUSH);
    sockets[2] = flowmq_socket(ctx, FLOWMQ_PUSH);
    for (size_t i = 0; i < 3u; ++i) {
      check_not_null(sockets[i]);
      pool_configure(sockets[i], 1u, 1u);
    }
    check_equal(flowmq_bind(sockets[0], "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(sockets[0], endpoint, sizeof(endpoint), &size), SALTS_OK);
    check_equal(flowmq_connect(sockets[1], endpoint), SALTS_OK);
    pool_wait_ready(sockets[0], 1u, sockets[1]);
    check_equal(flowmq_send(sockets[1], "A", 1u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
    check_equal(flowmq_send(sockets[1], "B", 1u, FLOWMQ_DONTWAIT), SALTS_OK);
    item = (flowmq_pollitem_t){.socket = sockets[0], .events = FLOWMQ_POLLIN};
    for (size_t i = 0; i < POOL_TEST_PROGRESS_LIMIT && ready == 0u; ++i) {
      pool_progress(sockets[0], sockets[1]);
      check_equal(flowmq_poll(&item, 1u, 0u, &ready), SALTS_OK);
    }
    check_equal(ready, 1u);
    check_equal(flowmq_close(sockets[1]), SALTS_OK);
    sockets[1] = NULL;
    for (size_t i = 0; i < POOL_TEST_PROGRESS_LIMIT &&
         pool_snapshot(sockets[0]).terminal_waiting_for_leases == 0u; ++i)
      pool_progress(sockets[0], NULL);
    check_equal(pool_snapshot(sockets[0]).terminal_waiting_for_leases, 1u);
    check_equal(pool_snapshot(sockets[0]).active_leases, 1u);
    check_equal(flowmq_connect(sockets[2], endpoint), SALTS_OK);
    for (size_t i = 0; i < 8u; ++i) pool_progress(sockets[0], sockets[2]);
    check_equal(pool_snapshot(sockets[2]).active_leases, 0u);
    check_equal(flowmq_recv(sockets[0], &byte, 1u, &size, FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(byte, 'A');
    check_equal(pool_snapshot(sockets[0]).active_leases, 1u);
    check_equal(flowmq_recv(sockets[0], &byte, 1u, &size, FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(byte, 'B');
    check_equal(pool_snapshot(sockets[0]).active_leases, 0u);
    check_equal(pool_snapshot(sockets[0]).physical_in_use, 0u);
    pool_wait_ready(sockets[0], 1u, sockets[2]);
    check_equal(pool_snapshot(sockets[0]).active_leases, 1u);
    check_equal(flowmq_send(sockets[2], "C", 1u, FLOWMQ_DONTWAIT), SALTS_OK);
    int status = SALTS_EBUSY;
    for (size_t i = 0; i < POOL_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      pool_progress(sockets[0], sockets[2]);
      status = flowmq_recv(sockets[0], &byte, 1u, &size, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(byte, 'C');
  }
}
