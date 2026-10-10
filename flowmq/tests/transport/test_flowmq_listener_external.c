#include "flowmq_cnet_transport.h"
#include "flowmq_owner_batch.h"
#include "tinytest.h"

#include <cnet/manager.h>
#include <salts/clock.h>
#include <salts/error_codes.h>
#include <string.h>

/* Qualify the released SDK boundary before opening FlowMQ owner bind. These
 * are real loopback accepts and NativeIO completions, without a second wait
 * owner or fabricated successful completions. */
enum { ACCEPT_LISTENERS = 2, ACCEPT_CLIENTS = 2, ACCEPT_BATCH = 17,
       ACCEPT_DEADLINE_MS = 2000 };

typedef struct accept_observer_s {
  size_t connected;
  size_t terminal;
  size_t recycled;
  unsigned char bytes[64];
  size_t received;
  bool overflow;
} accept_observer;

typedef struct accept_fixture_s {
  native_io_backend backend;
  cnet_listener listeners[ACCEPT_LISTENERS];
  cnet_client clients[ACCEPT_CLIENTS];
  cnet_connection outgoing;
  cnet_connection incoming;
  cnet_manager manager;
  cnet_managed_connection managed;
  cnet_accepted_stream detached;
  accept_observer observers[ACCEPT_CLIENTS];
  native_io_completion last_accept;
  size_t accepts;
  size_t client_completions;
  size_t routed;
  bool context_held;
  bool inject_error;
  bool stopped[ACCEPT_CLIENTS];
  mem_buffer_t *buffer;
  unsigned char payload[64];
  size_t releases;
} accept_fixture;

static void accept_state(void *user, cnet_connection connection,
                         cnet_connection_state state, const cnet_error *error) {
  accept_observer *observer = user;
  (void)connection;
  (void)error;
  if (state == CNET_CONNECTION_CONNECTED) ++observer->connected;
  if (state == CNET_CONNECTION_CLOSED || state == CNET_CONNECTION_FAILED)
    ++observer->terminal;
}

static void accept_recycle(void *user) {
  ++((accept_observer *)user)->recycled;
}

static void accept_receive(void *user, cnet_connection connection,
                           const cnet_receive_view *view) {
  accept_observer *observer = user;
  (void)connection;
  if (view->size > sizeof(observer->bytes) - observer->received) {
    observer->overflow = true;
    return;
  }
  memcpy(observer->bytes + observer->received, view->data, view->size);
  observer->received += view->size;
}

static void accept_payload_release(void *data, void *user) {
  (void)data;
  ++*(size_t *)user;
}

static int accept_route(void *user, size_t slot,
                        const native_io_completion *completion,
                        bool *consumed, size_t *events) {
  accept_fixture *f = user;
  int status = SALTS_OK;
  *consumed = false;
  *events = 0u;
  if (slot < ACCEPT_LISTENERS) {
    if (f->listeners[slot].impl != NULL)
      status = cnet_listener_route_external_completion(
          &f->listeners[slot], completion, consumed);
    if (*consumed) {
      f->last_accept = *completion;
      ++f->accepts;
    }
  } else {
    const size_t client = slot - ACCEPT_LISTENERS;
    if (f->clients[client].impl != NULL)
      status = cnet_client_route_external_completion(
          &f->clients[client], completion, consumed, events);
    if (*consumed) ++f->client_completions;
  }
  if (*consumed) {
    ++f->routed;
    if (status == SALTS_OK && f->inject_error) {
      /* Report an error only AFTER the real router settles ownership. */
      f->inject_error = false;
      return SALTS_EPERM;
    }
  }
  return status;
}

static int accept_progress(accept_fixture *f, uint32_t wait_ms) {
  native_io_completion completions[ACCEPT_BATCH];
  size_t count = 0u;
  int status;
  for (size_t i = 0u; i < ACCEPT_CLIENTS; ++i) {
    size_t events;
    if (f->clients[i].impl == NULL || f->stopped[i]) continue;
    status = cnet_client_advance_external(&f->clients[i], &events);
    if (status != SALTS_OK) return status;
  }
  status = native_io_backend_observe(
      &f->backend, completions, ACCEPT_BATCH, wait_ms, &count);
  if (status == SALTS_ETIMEDOUT) return SALTS_OK;
  if (status != SALTS_OK) return status;
  return flowmq_owner_batch_route(completions, count,
      ACCEPT_LISTENERS + ACCEPT_CLIENTS, accept_route, f);
}

static void accept_backend(accept_fixture *f, size_t endpoints, size_t requests) {
  const native_io_backend_config config = {
      .kind = flowmq_cnet_backend(), .endpoint_capacity = endpoints,
      .request_capacity = requests, .completion_batch_capacity = requests};
  check_equal(native_io_backend_init(&f->backend, &config), SALTS_OK);
}

static void accept_listener(accept_fixture *f, size_t slot) {
  const cnet_listener_config config = {.backend = flowmq_cnet_backend(),
      .host = "127.0.0.1", .port = 0u, .backlog = 4u};
  check_equal(cnet_listener_init(&f->listeners[slot], &config), SALTS_OK);
}

static void accept_attach(accept_fixture *f, size_t slot) {
  accept_listener(f, slot);
  check_equal(cnet_listener_attach_external(&f->listeners[slot], &f->backend),
              SALTS_OK);
}

static native_io_request accept_submit(accept_fixture *f, size_t slot) {
  native_io_request request = {0};
  check_equal(cnet_listener_submit_external_accept(&f->listeners[slot], &request),
              SALTS_OK);
  check_true(native_io_request_valid(request));
  return request;
}

static void accept_wait_terminal(accept_fixture *f, size_t expected) {
  const uint64_t deadline = cmeta_monotonic_ms() + ACCEPT_DEADLINE_MS;
  while (f->accepts < expected && cmeta_monotonic_ms() < deadline)
    check_equal(accept_progress(f, 10u), SALTS_OK);
  check_equal(f->accepts, expected);
}

static void accept_clients(accept_fixture *f) {
  flowmq_io_config_t io;
  flowmq_timeout_config_t timeouts = {0};
  cnet_client_config config;
  cnet_manager_config manager_config = {.size = sizeof(manager_config),
      .version = CNET_MANAGER_VERSION, .client = &f->clients[0],
      .record_capacity = 1u, .connection_capacity = 1u};
  flowmq_io_config_init(&io);
  check_equal(flowmq_cnet_client_config(&io, &timeouts, FLOWMQ_TRANSPORT_TCP,
                                       1u, 64u, &config), SALTS_OK);
  for (size_t i = 0u; i < ACCEPT_CLIENTS; ++i)
    check_equal(cnet_client_init_external(&f->clients[i], &config, &f->backend),
                SALTS_OK);
  check_equal(cnet_manager_init(&f->manager, &manager_config), SALTS_OK);
}

static void accept_reserve(accept_fixture *f) {
  const cnet_manager_attachment attachment = {
      .observer = {.on_state = accept_state, .on_receive = accept_receive,
                   .user = &f->observers[0]},
      .on_recycle = accept_recycle, .hold_context = true};
  check_equal(cnet_manager_reserve(&f->manager, &attachment, &f->managed), SALTS_OK);
  f->context_held = true;
}

static void accept_connect(accept_fixture *f) {
  cnet_stream_peer peer = {0};
  const cnet_observer observer = {
      .on_state = accept_state, .user = &f->observers[1]};
  check_equal(cnet_listener_local(&f->listeners[0], &peer), SALTS_OK);
  check_equal(cnet_connect_peer(&f->clients[1], &peer, NULL, &observer, &f->outgoing),
              SALTS_OK);
}

static void accept_cleanup(accept_fixture *f) {
  const uint64_t deadline = cmeta_monotonic_ms() + ACCEPT_DEADLINE_MS;
  bool listeners_done = false;
  bool clients_done = false;
  size_t work;
  if (f->backend.impl == NULL) return;
  mem_buffer_release(f->buffer);
  f->buffer = NULL;
  {
    const int status = cnet_accepted_stream_close(&f->detached);
    check_true(status == SALTS_OK || status == SALTS_EALREADY);
  }
  if (f->manager.impl != NULL) {
    check_equal(cnet_manager_request_close(&f->manager), SALTS_OK);
    if (f->context_held) {
      check_equal(cnet_manager_release_context(&f->manager, f->managed), SALTS_OK);
      f->context_held = false;
    }
  }
  if (f->outgoing.generation != 0u && f->observers[1].terminal == 0u)
    check_equal(cnet_close(&f->clients[1], f->outgoing), SALTS_OK);
  do {
    listeners_done = true;
    clients_done = true;
    for (size_t i = 0u; i < ACCEPT_LISTENERS; ++i) {
      int status;
      if (f->listeners[i].impl == NULL) continue;
      status = cnet_listener_close(&f->listeners[i]);
      if (status == SALTS_EBUSY) { listeners_done = false; continue; }
      check_true(status == SALTS_OK || status == SALTS_EALREADY);
      check_equal(cnet_listener_destroy(&f->listeners[i]), SALTS_OK);
    }
    if (f->manager.impl != NULL)
      check_equal(cnet_manager_advance(&f->manager, 1u, &work), SALTS_OK);
    for (size_t i = 0u; i < ACCEPT_CLIENTS; ++i) {
      int status;
      if (f->clients[i].impl == NULL || f->stopped[i]) continue;
      status = cnet_client_stop_external(&f->clients[i]);
      check_true(status == SALTS_OK || status == SALTS_EBUSY);
      f->stopped[i] = status == SALTS_OK;
      clients_done = clients_done && f->stopped[i];
    }
    if (listeners_done && clients_done) break;
    check_equal(accept_progress(f, 10u), SALTS_OK);
  } while (cmeta_monotonic_ms() < deadline);
  check_true(listeners_done && clients_done);
  if (f->manager.impl != NULL) {
    check_equal(cnet_manager_advance(&f->manager, 1u, &work), SALTS_OK);
    check_equal(cnet_manager_destroy(&f->manager), SALTS_OK);
  }
  for (size_t i = 0u; i < ACCEPT_CLIENTS; ++i)
    check_equal(cnet_client_destroy(&f->clients[i]), SALTS_OK);
  {
    native_io_backend_stats stats;
    check_true(native_io_backend_get_stats(&f->backend, &stats));
    check_equal(stats.active_requests, 0u);
    check_equal(stats.endpoint_count, 0u);
  }
  check_equal(native_io_backend_close(&f->backend), SALTS_OK);
  check_equal(native_io_backend_destroy(&f->backend), SALTS_OK);
}

static void accept_observe_connect_pair(accept_fixture *f,
                                        native_io_completion completions[2]) {
  const uint64_t deadline = cmeta_monotonic_ms() + ACCEPT_DEADLINE_MS;
  size_t count = 0u;
  /* Collect the connect and accept terminals before either is routed. Keeping
   * the two actual events makes the close race and mixed batch deterministic. */
  while (count < 2u && cmeta_monotonic_ms() < deadline) {
    size_t events = 0u;
    size_t observed = 0u;
    int status;
    check_equal(cnet_client_advance_external(&f->clients[1], &events), SALTS_OK);
    status = native_io_backend_observe(
        &f->backend, completions + count, 2u - count, 10u, &observed);
    check_true(status == SALTS_OK || status == SALTS_ETIMEDOUT);
    count += observed;
  }
  check_equal(count, 2u);
}

spec("FlowMQ external listener SDK qualification") {
  static accept_fixture f;
  before_each() { memset(&f, 0, sizeof(f)); }
  after_each() { f.inject_error = false; accept_cleanup(&f); }

  it("keeps one accept identity and drains pending close before destroy") {
    native_io_request first, repeated;
    native_io_backend_stats stats;
    int ready = 1;
    accept_backend(&f, 2u, 1u);
    accept_attach(&f, 0u);
    check_equal(cnet_listener_attach_external(&f.listeners[0], &f.backend), SALTS_EALREADY);
    check_equal(cnet_listener_wait(&f.listeners[0], 0u, &ready), SALTS_ENOTSUP);
    check_equal(ready, 0);
    first = accept_submit(&f, 0u);
    repeated = accept_submit(&f, 0u);
    check_equal(first.slot, repeated.slot);
    check_equal(first.generation, repeated.generation);
    check_true(native_io_backend_get_stats(&f.backend, &stats));
    check_equal(stats.submitted, 1u);
    check_equal(accept_progress(&f, 5u), SALTS_OK);
    check_equal(f.accepts, 0u);
    check_equal(cnet_listener_close(&f.listeners[0]), SALTS_EBUSY);
    check_equal(cnet_listener_destroy(&f.listeners[0]), SALTS_EBUSY);
    accept_wait_terminal(&f, 1u);
    check_equal(f.last_accept.kind, NATIVE_IO_COMPLETION_CANCELLED);
  }

  it("rejects old generations after request-slot reuse") {
    native_io_completion stale;
    native_io_request first, next;
    bool consumed = true;
    accept_backend(&f, 2u, 1u);
    accept_attach(&f, 0u);
    first = accept_submit(&f, 0u);
    check_equal(native_io_backend_cancel(&f.backend, first), SALTS_OK);
    accept_wait_terminal(&f, 1u);
    stale = f.last_accept;
    next = accept_submit(&f, 0u);
    check_equal(next.slot, first.slot);
    check_not_equal(next.generation, first.generation);
    check_equal(cnet_listener_route_external_completion(&f.listeners[0], &stale,
                                                        &consumed), SALTS_OK);
    check_false(consumed);
    check_equal(native_io_backend_cancel(&f.backend, next), SALTS_OK);
    accept_wait_terminal(&f, 2u);
    check_equal(f.last_accept.request.generation, next.generation);
  }

  it("recovers an attach rejected by endpoint capacity") {
    accept_backend(&f, 1u, 1u);
    accept_attach(&f, 0u);
    accept_listener(&f, 1u);
    check_equal(cnet_listener_attach_external(&f.listeners[1], &f.backend), SALTS_ENOBUFS);
    check_equal(cnet_listener_close(&f.listeners[0]), SALTS_OK);
    check_equal(cnet_listener_destroy(&f.listeners[0]), SALTS_OK);
    check_equal(cnet_listener_attach_external(&f.listeners[1], &f.backend), SALTS_OK);
    (void)accept_submit(&f, 1u);
  }

  it("recovers accept admission after the shared request budget is exhausted") {
    native_io_request first, rejected;
    accept_backend(&f, 4u, 1u);
    accept_attach(&f, 0u);
    accept_attach(&f, 1u);
    first = accept_submit(&f, 0u);
    check_equal(cnet_listener_submit_external_accept(&f.listeners[1], &rejected),
                SALTS_ENOBUFS);
    check_false(native_io_request_valid(rejected));
    check_equal(native_io_backend_cancel(&f.backend, first), SALTS_OK);
    accept_wait_terminal(&f, 1u);
    (void)accept_submit(&f, 1u);
  }

  it("rolls back a reserved manager record only after its context hold ends") {
    cnet_manager_snapshot snapshot;
    cnet_manager_entry entry;
    cnet_managed_connection first, rejected;
    const cnet_manager_attachment attachment = {
        .observer = {.on_state = accept_state, .user = &f.observers[0]}};
    size_t work;
    accept_backend(&f, 6u, ACCEPT_BATCH);
    accept_clients(&f);
    accept_reserve(&f);
    first = f.managed;
    check_equal(cnet_manager_reserve(&f.manager, &attachment, &rejected), SALTS_ENOBUFS);
    check_equal(cnet_manager_cancel(&f.manager, first), SALTS_OK);
    check_equal(cnet_manager_advance(&f.manager, 1u, &work), SALTS_OK);
    check_equal(f.observers[0].recycled, 0u);
    check_equal(cnet_manager_get_snapshot(&f.manager, &snapshot), SALTS_OK);
    check_equal(snapshot.reserved, 0u);
    check_equal(snapshot.retired, 1u);
    check_equal(snapshot.context_holds, 1u);
    check_equal(f.observers[0].connected, 0u);
    check_equal(f.observers[0].terminal, 0u);
    check_equal(cnet_manager_release_context(&f.manager, first), SALTS_OK);
    f.context_held = false;
    check_equal(cnet_manager_advance(&f.manager, 1u, &work), SALTS_OK);
    check_equal(f.observers[0].recycled, 1u);
    accept_reserve(&f);
    check_equal(f.managed.slot, first.slot);
    check_not_equal(f.managed.generation, first.generation);
    check_equal(cnet_manager_lookup(&f.manager, first, &entry), SALTS_ENOENT);
  }

  it("moves one accepted child to the manager while sharing client completions") {
    native_io_request rejected;
    uint64_t deadline;
    accept_backend(&f, 6u, ACCEPT_BATCH);
    accept_attach(&f, 0u);
    accept_clients(&f);
    accept_reserve(&f);
    (void)accept_submit(&f, 0u);
    accept_connect(&f);
    accept_wait_terminal(&f, 1u);
    check_equal(cnet_listener_submit_external_accept(&f.listeners[0], &rejected), SALTS_EALREADY);
    check_equal(cnet_listener_accept_detached(&f.listeners[0], &f.detached), SALTS_OK);
    check_equal(cnet_manager_adopt(&f.manager, f.managed, &f.detached, NULL,
                                   &f.incoming), SALTS_OK);
    check_equal(cnet_accepted_stream_close(&f.detached), SALTS_EALREADY);
    deadline = cmeta_monotonic_ms() + ACCEPT_DEADLINE_MS;
    while ((f.observers[0].connected == 0u || f.observers[1].connected == 0u) &&
           cmeta_monotonic_ms() < deadline)
      check_equal(accept_progress(&f, 10u), SALTS_OK);
    check_equal(f.observers[0].connected, 1u);
    check_equal(f.observers[1].connected, 1u);
    check_true(f.client_completions != 0u);
    check_equal(cnet_listener_close(&f.listeners[0]), SALTS_OK);
    check_equal(cnet_listener_destroy(&f.listeners[0]), SALTS_OK);
    check_equal(f.observers[0].terminal, 0u);
    check_equal(f.observers[1].terminal, 0u);
    /* Closing admission must leave an adopted session usable, including its
     * borrowed backend and retained payload completion obligations. */
    for (size_t i = 0u; i < sizeof(f.payload); ++i)
      f.payload[i] = (unsigned char)(i * 7u + 3u);
    f.buffer = mem_wrap_external(f.payload, sizeof(f.payload),
                                 accept_payload_release, &f.releases);
    check_not_null(f.buffer);
    check_equal(cnet_receive(&f.clients[0], f.incoming, sizeof(f.payload)), SALTS_OK);
    check_equal(cnet_send_buffer(&f.clients[1], f.outgoing, f.buffer), SALTS_OK);
    mem_buffer_release(f.buffer);
    f.buffer = NULL;
    deadline = cmeta_monotonic_ms() + ACCEPT_DEADLINE_MS;
    while ((f.observers[0].received != sizeof(f.payload) || f.releases == 0u) &&
           cmeta_monotonic_ms() < deadline)
      check_equal(accept_progress(&f, 10u), SALTS_OK);
    check_false(f.observers[0].overflow);
    check_equal(f.observers[0].received, sizeof(f.payload));
    check_equal(f.observers[0].bytes, f.payload, sizeof(f.payload));
    check_equal(f.releases, 1u);
  }

  it("drains a successful accept observed before close and destroys its unmoved child") {
    native_io_completion completions[2];
    accept_backend(&f, 6u, ACCEPT_BATCH);
    accept_attach(&f, 0u);
    accept_clients(&f);
    (void)accept_submit(&f, 0u);
    accept_connect(&f);
    accept_observe_connect_pair(&f, completions);
    check_equal(cnet_listener_close(&f.listeners[0]), SALTS_EBUSY);
    check_equal(cnet_listener_destroy(&f.listeners[0]), SALTS_EBUSY);
    check_equal(flowmq_owner_batch_route(completions, 2u,
        ACCEPT_LISTENERS + ACCEPT_CLIENTS, accept_route, &f), SALTS_OK);
    check_equal(f.accepts, 1u);
    check_equal(f.last_accept.kind, NATIVE_IO_COMPLETION_OK);
    check_equal(f.last_accept.status, SALTS_OK);
    check_equal(cnet_listener_close(&f.listeners[0]), SALTS_OK);
    check_equal(cnet_listener_destroy(&f.listeners[0]), SALTS_OK);
    check_equal(f.observers[0].connected, 0u);
  }

  it("routes both real accept and client completions despite a first-event error") {
    native_io_completion completions[2];
    accept_backend(&f, 6u, ACCEPT_BATCH);
    accept_attach(&f, 0u);
    accept_clients(&f);
    (void)accept_submit(&f, 0u);
    accept_connect(&f);
    accept_observe_connect_pair(&f, completions);
    f.inject_error = true;
    check_equal(flowmq_owner_batch_route(completions, 2u,
        ACCEPT_LISTENERS + ACCEPT_CLIENTS, accept_route, &f), SALTS_EPERM);
    check_equal(f.routed, 2u);
    check_equal(f.accepts, 1u);
    check_equal(f.client_completions, 1u);
  }

  it("consumes a detached child on sealed-manager adoption failure") {
    size_t work;
    accept_backend(&f, 6u, ACCEPT_BATCH);
    accept_attach(&f, 0u);
    accept_clients(&f);
    accept_reserve(&f);
    (void)accept_submit(&f, 0u);
    accept_connect(&f);
    accept_wait_terminal(&f, 1u);
    check_equal(cnet_listener_accept_detached(&f.listeners[0], &f.detached), SALTS_OK);
    check_equal(cnet_manager_seal(&f.manager), SALTS_OK);
    check_equal(cnet_manager_adopt(&f.manager, f.managed, &f.detached, NULL,
                                   &f.incoming), SALTS_ESHUTDOWN);
    check_equal(cnet_accepted_stream_close(&f.detached), SALTS_EALREADY);
    check_equal(f.incoming.generation, 0u);
    check_equal(f.observers[0].connected, 0u);
    check_equal(f.observers[0].terminal, 0u);
    check_equal(cnet_manager_release_context(&f.manager, f.managed), SALTS_OK);
    f.context_held = false;
    check_equal(cnet_manager_advance(&f.manager, 1u, &work), SALTS_OK);
    check_equal(f.observers[0].recycled, 1u);
  }

  it("drains the rest of a real accept batch after a consumed event reports an error") {
    native_io_completion completions[2];
    native_io_request requests[2];
    size_t count = 0u;
    const uint64_t deadline = cmeta_monotonic_ms() + ACCEPT_DEADLINE_MS;
    accept_backend(&f, 4u, 2u);
    for (size_t i = 0u; i < 2u; ++i) {
      accept_attach(&f, i);
      requests[i] = accept_submit(&f, i);
      check_equal(native_io_backend_cancel(&f.backend, requests[i]), SALTS_OK);
    }
    /* Accumulate real observations without routing; two terminal events have
     * no payload borrow and fit the declared bounded batch. */
    while (count < 2u && cmeta_monotonic_ms() < deadline) {
      size_t observed = 0u;
      const int status = native_io_backend_observe(
          &f.backend, completions + count, 2u - count, 10u, &observed);
      check_true(status == SALTS_OK || status == SALTS_ETIMEDOUT);
      count += observed;
    }
    check_equal(count, 2u);
    f.inject_error = true;
    check_equal(flowmq_owner_batch_route(completions, count,
        ACCEPT_LISTENERS + ACCEPT_CLIENTS, accept_route, &f), SALTS_EPERM);
    check_equal(f.routed, 2u);
    check_equal(f.accepts, 2u);
  }
}
