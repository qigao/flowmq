#include "flowmq_owner.h"
#include "flowmq_peer_pool.h"
#include "flowmq_socket_external_internal.h"
#include "tinytest.h"

#include <salts/clock.h>
#include <salts/error_codes.h>
#include <string.h>

enum { LISTENER_OWNED = 2, LISTENER_CLIENTS = 5, LISTENER_DEADLINE_MS = 3000 };

typedef struct listener_fixture_s {
  flowmq_ctx_t *ctx;
  flowmq_owner_t *owner;
  flowmq_socket_t *owned[LISTENER_OWNED];
  flowmq_socket_t *clients[LISTENER_CLIENTS];
} listener_fixture;

static void listener_configure(flowmq_socket_t *socket) {
  const int reconnect = -1;
  check_not_null(socket);
  check_equal(flowmq_setsockopt(socket, FLOWMQ_RECONNECT_IVL,
                               &reconnect, sizeof(reconnect)), SALTS_OK);
#if defined(FLOWMQ_TEST_PEER_POOL)
  const flowmq_peer_pool_config_t config = FLOWMQ_PEER_POOL_CONFIG_INIT;
  check_equal(flowmq_socket_set_peer_pool(socket, &config), SALTS_OK);
#endif
}

static void listener_owned(listener_fixture *f, size_t slot, int pattern) {
  f->owned[slot] = flowmq_owner_socket(f->owner, pattern);
  listener_configure(f->owned[slot]);
}

static void listener_client(listener_fixture *f, size_t slot, int pattern) {
  f->clients[slot] = flowmq_socket(f->ctx, pattern);
  listener_configure(f->clients[slot]);
}

static void listener_connect_to(flowmq_socket_t *client, flowmq_socket_t *server) {
  char endpoint[128];
  size_t size = 0u;
  check_equal(flowmq_last_endpoint(server, endpoint, sizeof(endpoint), &size), SALTS_OK);
  check_equal(flowmq_connect(client, endpoint), SALTS_OK);
}

static void listener_bind(flowmq_socket_t *server) {
  check_equal(flowmq_socket_internal_bind_external(server, "tcp://127.0.0.1:0"), SALTS_OK);
}

static flowmq_peer_pool_snapshot_t listener_pool(flowmq_socket_t *socket) {
  flowmq_peer_pool_snapshot_t snapshot = FLOWMQ_PEER_POOL_SNAPSHOT_INIT;
  check_equal(flowmq_socket_get_peer_pool(socket, &snapshot), SALTS_OK);
  return snapshot;
}

static void listener_progress(listener_fixture *f) {
  flowmq_pollitem_t items[LISTENER_CLIENTS] = {0};
  size_t count = 0u;
  size_t ready;
  for (size_t i = 0u; i < LISTENER_OWNED; ++i)
    if (f->owned[i] != NULL)
      items[count++] = (flowmq_pollitem_t){.socket = f->owned[i]};
  if (count != 0u)
    check_equal(flowmq_owner_poll(f->owner, items, count, 0u, &ready), SALTS_OK);
  count = 0u;
  for (size_t i = 0u; i < LISTENER_CLIENTS; ++i)
    if (f->clients[i] != NULL)
      items[count++] = (flowmq_pollitem_t){.socket = f->clients[i]};
  if (count != 0u)
    check_equal(flowmq_poll(items, count, 0u, &ready), SALTS_OK);
}

static void listener_exchange(listener_fixture *f, flowmq_socket_t *from,
                               flowmq_socket_t *to, unsigned char seed) {
  unsigned char payload[64], received[64];
  const uint64_t deadline = cmeta_monotonic_ms() + LISTENER_DEADLINE_MS;
  bool sent = false;
  bool got = false;
  for (size_t i = 0u; i < sizeof(payload); ++i)
    payload[i] = (unsigned char)(seed + i * 3u);
  while (!got && cmeta_monotonic_ms() < deadline) {
    size_t size = 0u;
    int status;
    listener_progress(f);
    if (!sent) {
      status = flowmq_send(from, payload, sizeof(payload), FLOWMQ_DONTWAIT);
      check_true(status == SALTS_OK || status == SALTS_EBUSY || status == SALTS_ENOBUFS);
      sent = status == SALTS_OK;
    }
    status = flowmq_recv(to, received, sizeof(received), &size, FLOWMQ_DONTWAIT);
    check_true(status == SALTS_OK || status == SALTS_EBUSY);
    if (status == SALTS_OK) {
      check_true(sent);
      check_equal(size, sizeof(payload));
      check_equal(received, payload, sizeof(payload));
      got = true;
    }
  }
  check_true(got);
}

spec("FlowMQ internal shared listener integration") {
  static listener_fixture f;
  before_each() {
    flowmq_owner_config_t config = FLOWMQ_OWNER_CONFIG_INIT;
    memset(&f, 0, sizeof(f));
    f.ctx = flowmq_ctx_new();
    check_not_null(f.ctx);
    config.socket_capacity = LISTENER_OWNED;
    f.owner = flowmq_owner_new(f.ctx, &config);
    check_not_null(f.owner);
  }
  after_each() {
    for (size_t i = 0u; i < LISTENER_CLIENTS; ++i)
      if (f.clients[i] != NULL) check_equal(flowmq_close(f.clients[i]), SALTS_OK);
    for (size_t i = 0u; i < LISTENER_OWNED; ++i)
      if (f.owned[i] != NULL)
        check_equal(flowmq_owner_close_socket(f.owner, f.owned[i]), SALTS_OK);
    if (f.owner != NULL) check_equal(flowmq_owner_term(f.owner), SALTS_OK);
    if (f.ctx != NULL) check_equal(flowmq_ctx_term(f.ctx), SALTS_OK);
  }

  it("keeps bind private and checks the expanded backend budget") {
    native_io_backend_config config;
    listener_owned(&f, 0u, FLOWMQ_PULL);
    listener_client(&f, 0u, FLOWMQ_PUSH);
    check_equal(flowmq_bind(f.owned[0], "tcp://127.0.0.1:0"), SALTS_ENOTSUP);
    check_equal(flowmq_socket_internal_bind_external(f.clients[0], "tcp://127.0.0.1:0"),
                SALTS_EINVAL);
    check_equal(flowmq_socket_internal_bind_external(f.owned[0], "tls://127.0.0.1:0"),
                SALTS_ENOTSUP);
    check_equal(flowmq_socket_internal_owner_backend_config(SIZE_MAX, &config), SALTS_ERANGE);
    check_equal(flowmq_socket_internal_owner_backend_config(0u, &config), SALTS_EINVAL);
    listener_bind(f.owned[0]);
    check_equal(flowmq_socket_internal_bind_external(f.owned[0], "tcp://127.0.0.1:0"),
                SALTS_EALREADY);
  }

  it("waits idle and repeatedly drains a pending accept before reusing the owner slot") {
    for (size_t round = 0u; round < 8u; ++round) {
      flowmq_pollitem_t item;
      size_t ready = 1u;
      listener_owned(&f, 0u, FLOWMQ_PULL);
      listener_bind(f.owned[0]);
      item = (flowmq_pollitem_t){.socket = f.owned[0], .events = FLOWMQ_POLLIN};
      check_equal(flowmq_owner_poll(f.owner, &item, 1u, 5u, &ready), SALTS_OK);
      check_equal(ready, 0u);
#if defined(FLOWMQ_TEST_PEER_POOL)
      const flowmq_peer_pool_snapshot_t snapshot = listener_pool(f.owned[0]);
      check_equal(snapshot.connecting, 1u);
      check_equal(snapshot.physical_in_use, 1u);
      check_equal(snapshot.active_leases, 0u);
#endif
      check_equal(flowmq_owner_close_socket(f.owner, f.owned[0]), SALTS_OK);
      f.owned[0] = NULL;
    }
  }

  it("shares one lane between listening and connecting sockets and isolates close") {
    listener_owned(&f, 0u, FLOWMQ_PAIR);
    listener_owned(&f, 1u, FLOWMQ_PAIR);
    listener_client(&f, 0u, FLOWMQ_PAIR);
    listener_client(&f, 1u, FLOWMQ_PAIR);
    listener_bind(f.owned[0]);
    check_equal(flowmq_bind(f.clients[1], "tcp://127.0.0.1:0"), SALTS_OK);
    listener_connect_to(f.clients[0], f.owned[0]);
    listener_connect_to(f.owned[1], f.clients[1]);
    for (unsigned char round = 0u; round < 4u; ++round) {
      listener_exchange(&f, f.clients[0], f.owned[0], round);
      listener_exchange(&f, f.owned[0], f.clients[0], (unsigned char)(round + 11u));
      listener_exchange(&f, f.owned[1], f.clients[1], (unsigned char)(round + 22u));
    }
    check_equal(flowmq_owner_close_socket(f.owner, f.owned[0]), SALTS_OK);
    f.owned[0] = NULL;
    check_equal(flowmq_close(f.clients[0]), SALTS_OK);
    f.clients[0] = NULL;
    listener_exchange(&f, f.owned[1], f.clients[1], 77u);
    listener_exchange(&f, f.clients[1], f.owned[1], 88u);
  }

  it("accepts and exchanges when both endpoints belong to the same owner") {
    listener_owned(&f, 0u, FLOWMQ_PAIR);
    listener_owned(&f, 1u, FLOWMQ_PAIR);
    listener_bind(f.owned[0]);
    listener_connect_to(f.owned[1], f.owned[0]);
    listener_exchange(&f, f.owned[1], f.owned[0], 33u);
    listener_exchange(&f, f.owned[0], f.owned[1], 44u);
  }

  it("leaves a fifth peer in backlog and admits it after a live peer retires") {
    listener_owned(&f, 0u, FLOWMQ_PULL);
    listener_bind(f.owned[0]);
    for (size_t i = 0u; i < 4u; ++i) {
      listener_client(&f, i, FLOWMQ_PUSH);
      listener_connect_to(f.clients[i], f.owned[0]);
      listener_exchange(&f, f.clients[i], f.owned[0], (unsigned char)i);
    }
    listener_client(&f, 4u, FLOWMQ_PUSH);
    listener_connect_to(f.clients[4], f.owned[0]);
    for (size_t step = 0u; step < 128u; ++step) listener_progress(&f);
    check_equal(flowmq_send(f.clients[4], "x", 1u, FLOWMQ_DONTWAIT), SALTS_EBUSY);
    check_equal(flowmq_close(f.clients[0]), SALTS_OK);
    f.clients[0] = NULL;
    listener_exchange(&f, f.clients[4], f.owned[0], 55u);
    listener_exchange(&f, f.clients[3], f.owned[0], 66u);
  }

  it("bounds a pool reservation and restores admission after its only session closes") {
    flowmq_peer_pool_config_t config = FLOWMQ_PEER_POOL_CONFIG_INIT;
    flowmq_peer_pool_snapshot_t snapshot;
    listener_owned(&f, 0u, FLOWMQ_PULL);
    config.max_peers = config.max_connecting = 1u;
    check_equal(flowmq_socket_set_peer_pool(f.owned[0], &config), SALTS_OK);
    listener_bind(f.owned[0]);
    listener_progress(&f);
    snapshot = listener_pool(f.owned[0]);
    check_equal(snapshot.connecting, 1u);
    check_equal(snapshot.physical_in_use, 1u);
    listener_client(&f, 0u, FLOWMQ_PUSH);
    listener_connect_to(f.clients[0], f.owned[0]);
    listener_exchange(&f, f.clients[0], f.owned[0], 10u);
    snapshot = listener_pool(f.owned[0]);
    check_equal(snapshot.ready, 1u);
    check_equal(snapshot.connecting, 0u);
    listener_client(&f, 1u, FLOWMQ_PUSH);
    listener_connect_to(f.clients[1], f.owned[0]);
    for (size_t step = 0u; step < 128u; ++step) listener_progress(&f);
    check_equal(flowmq_send(f.clients[1], "x", 1u, FLOWMQ_DONTWAIT), SALTS_EBUSY);
    check_equal(flowmq_close(f.clients[0]), SALTS_OK);
    f.clients[0] = NULL;
    listener_exchange(&f, f.clients[1], f.owned[0], 20u);
    snapshot = listener_pool(f.owned[0]);
    check_equal(snapshot.ready, 1u);
    check_equal(snapshot.physical_in_use, 1u);
    check_equal(snapshot.active_leases, 1u);
  }

  it("progresses a burst across two listeners on the same lane") {
    for (size_t i = 0u; i < LISTENER_OWNED; ++i) {
      listener_owned(&f, i, FLOWMQ_PULL);
      listener_bind(f.owned[i]);
    }
    for (size_t i = 0u; i < 4u; ++i) {
      listener_client(&f, i, FLOWMQ_PUSH);
      listener_connect_to(f.clients[i], f.owned[i % LISTENER_OWNED]);
    }
    for (unsigned char round = 0u; round < 4u; ++round)
      for (size_t i = 0u; i < 4u; ++i)
        listener_exchange(&f, f.clients[i], f.owned[i % LISTENER_OWNED],
                           (unsigned char)(round * 8u + i));
  }

  it("counts pending accept against max_connecting until protocol READY") {
    flowmq_peer_pool_config_t config = FLOWMQ_PEER_POOL_CONFIG_INIT;
    flowmq_peer_pool_snapshot_t snapshot;
    flowmq_pollitem_t owner_item, client_item;
    size_t ready;
    listener_owned(&f, 0u, FLOWMQ_PULL);
    config.max_peers = 2u;
    config.max_connecting = 1u;
    check_equal(flowmq_socket_set_peer_pool(f.owned[0], &config), SALTS_OK);
    listener_bind(f.owned[0]);
    listener_client(&f, 0u, FLOWMQ_PUSH);
    listener_connect_to(f.clients[0], f.owned[0]);
    client_item = (flowmq_pollitem_t){.socket = f.clients[0]};
    check_equal(flowmq_poll(&client_item, 1u, 0u, &ready), SALTS_OK);
    /* Leave the first peer's protocol handshake unprogressed. The owner may
     * accept it, but TCP establishment must not create a READY pool lease. */
    listener_client(&f, 1u, FLOWMQ_PUSH);
    listener_connect_to(f.clients[1], f.owned[0]);
    client_item = (flowmq_pollitem_t){.socket = f.clients[1]};
    owner_item = (flowmq_pollitem_t){.socket = f.owned[0]};
    for (size_t step = 0u; step < 128u; ++step) {
      check_equal(flowmq_owner_poll(f.owner, &owner_item, 1u, 0u, &ready), SALTS_OK);
      check_equal(flowmq_poll(&client_item, 1u, 0u, &ready), SALTS_OK);
    }
    snapshot = listener_pool(f.owned[0]);
    check_equal(snapshot.connecting, 1u);
    check_equal(snapshot.physical_in_use, 1u);
    check_equal(snapshot.ready, 0u);
    check_equal(snapshot.active_leases, 0u);
    check_equal(flowmq_send(f.clients[1], "x", 1u, FLOWMQ_DONTWAIT), SALTS_EBUSY);
    listener_exchange(&f, f.clients[0], f.owned[0], 81u);
    listener_exchange(&f, f.clients[1], f.owned[0], 82u);
    snapshot = listener_pool(f.owned[0]);
    check_equal(snapshot.ready, 2u);
    check_equal(snapshot.physical_in_use, 2u);
    check_equal(snapshot.active_leases, 2u);
  }
}
