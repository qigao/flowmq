#include "flowmq_owner_internal.h"
#include "tinytest.h"
#include "cmeta_error.h"

#include <stdbool.h>
#include <stddef.h>
#include <string.h>

enum {
  FAULT_LANES = 3,
  FAULT_MESSAGES = 2,
  FAULT_BYTES = 32,
  FAULT_PROGRESS_LIMIT = 20000,
  FAULT_ROUND_LIMIT = 16
};

static const char peer_identity[] = "peer";

typedef struct owner_fault_fixture_s {
  flowmq_ctx_t *ctx;
  flowmq_owner_t *owner;
  flowmq_socket_t *clients[FAULT_LANES];
  flowmq_socket_t *peers[FAULT_LANES];
  unsigned char payload[FAULT_LANES][FAULT_MESSAGES][FAULT_BYTES];
  size_t releases[FAULT_LANES];
  size_t received[FAULT_LANES];
  mem_buffer_t *buffer;
  mem_slice_t slice;
  bool armed;
  size_t error_index;
  size_t error_slot;
  size_t injections;
  size_t fault_batch_count;
  size_t settled;
  size_t settled_after_error;
  const native_io_completion *batch;
  flowmq_owner_batch_route_fn route;
  void *route_user;
} owner_fault_fixture;

static void owner_fault_release(void *data, void *user) {
  (void)data;
  ++*(size_t *)user;
}

static int owner_fault_route(
    void *user, size_t slot, const native_io_completion *completion,
    bool *consumed, size_t *events) {
  owner_fault_fixture *f = user;
  int status;
  /* A consumed event must never be offered to another slot or repeated. */
  check_true(f->settled < f->fault_batch_count);
  check_true(completion == &f->batch[f->settled]);
  check_true(native_io_request_valid(completion->request));
  status = f->route(f->route_user, slot, completion, consumed, events);
  check_equal(status, SALTS_OK);
  if (*consumed) {
    const size_t index = f->settled++;
    if (index > f->error_index) ++f->settled_after_error;
    if (index == f->error_index) {
      f->error_slot = slot;
      ++f->injections;
      /* The actual CNet router has already consumed this actual NativeIO
       * completion. Inject only the subsequent error report, never a second
       * completion or fabricated request/generation. */
      return SALTS_EPERM;
    }
  }
  return status;
}

static int owner_fault_batch(
    void *user, const native_io_completion *completions, size_t count,
    size_t socket_capacity, flowmq_owner_batch_route_fn route, void *route_user) {
  owner_fault_fixture *f = user;
  int status;
  if (!f->armed || count < FAULT_LANES)
    return flowmq_owner_batch_route(completions, count, socket_capacity,
                                   route, route_user);
  f->armed = false;
  f->batch = completions;
  f->fault_batch_count = count;
  f->route = route;
  f->route_user = route_user;
  status = flowmq_owner_batch_route(completions, count, socket_capacity,
                                   owner_fault_route, f);
  check_equal(status, SALTS_EPERM);
  check_equal(f->settled, count);
  check_true(f->settled_after_error > 0u);
  f->batch = NULL;
  f->route = NULL;
  f->route_user = NULL;
  return status;
}

static void owner_fault_progress(owner_fault_fixture *f, bool inject) {
  flowmq_pollitem_t items[FAULT_LANES] = {0};
  size_t count = 0u;
  size_t ready = 0u;
  const size_t previous_injections = f->injections;
  int status = flowmq_owner_internal_progress_once(
      f->owner, 0u, inject ? owner_fault_batch : NULL, inject ? f : NULL);
  check_equal(status, f->injections != previous_injections ? SALTS_EPERM : SALTS_OK);
  for (size_t lane = 0u; lane < FAULT_LANES; ++lane) {
    if (f->peers[lane] != NULL)
      items[count++] = (flowmq_pollitem_t){.socket = f->peers[lane]};
  }
  check_equal(flowmq_poll(items, count, 0u, &ready), SALTS_OK);
}

static void owner_fault_open(owner_fault_fixture *f) {
  flowmq_owner_config_t config = FLOWMQ_OWNER_CONFIG_INIT;
  const int hwm = FAULT_MESSAGES;
  const int reconnect_disabled = -1;
  const size_t window = FAULT_MESSAGES * FAULT_BYTES;
  const size_t quantum = FAULT_BYTES;
  const int update_ms = 1;
  bool all_ready = false;
  config.socket_capacity = FAULT_LANES;
  f->ctx = flowmq_ctx_new();
  check_not_null(f->ctx);
  f->owner = flowmq_owner_new(f->ctx, &config);
  check_not_null(f->owner);
  for (size_t lane = 0u; lane < FAULT_LANES; ++lane) {
    char endpoint[128] = {0};
    size_t endpoint_size = 0u;
    f->clients[lane] = flowmq_owner_socket(f->owner, FLOWMQ_ROUTER);
    f->peers[lane] = flowmq_socket(f->ctx, FLOWMQ_DEALER);
    check_not_null(f->clients[lane]);
    check_not_null(f->peers[lane]);
    check_equal(flowmq_setsockopt(f->clients[lane], FLOWMQ_RECONNECT_IVL,
                                 &reconnect_disabled, sizeof(reconnect_disabled)),
                SALTS_OK);
    check_equal(flowmq_setsockopt(f->clients[lane], FLOWMQ_SNDHWM,
                                 &hwm, sizeof(hwm)), SALTS_OK);
    check_equal(flowmq_setsockopt(f->clients[lane], FLOWMQ_SNDHWM_BYTES,
                                 &window, sizeof(window)), SALTS_OK);
    check_equal(flowmq_setsockopt(f->peers[lane], FLOWMQ_IDENTITY, peer_identity,
                                 sizeof(peer_identity) - 1u), SALTS_OK);
    check_equal(flowmq_setsockopt(f->peers[lane], FLOWMQ_RCVHWM_BYTES,
                                 &window, sizeof(window)), SALTS_OK);
    check_equal(flowmq_setsockopt(f->peers[lane], FLOWMQ_FLOW_UPDATE_QUANTUM,
                                 &quantum, sizeof(quantum)), SALTS_OK);
    check_equal(flowmq_setsockopt(f->peers[lane], FLOWMQ_FLOW_UPDATE_IVL,
                                 &update_ms, sizeof(update_ms)), SALTS_OK);
    check_equal(flowmq_bind(f->peers[lane], "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(f->peers[lane], endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(f->clients[lane], endpoint), SALTS_OK);
  }
  for (size_t step = 0u; step < FAULT_PROGRESS_LIMIT && !all_ready; ++step) {
    owner_fault_progress(f, false);
    all_ready = true;
    for (size_t lane = 0u; lane < FAULT_LANES; ++lane) {
      flowmq_router_peer_status_t peer = FLOWMQ_ROUTER_PEER_STATUS_INIT;
      int status = flowmq_router_peer_status(f->clients[lane], peer_identity,
                                             sizeof(peer_identity) - 1u, &peer);
      check_true(status == SALTS_OK || status == SALTS_ENOENT);
      all_ready = all_ready && status == SALTS_OK && peer.ready;
    }
  }
  check_true(all_ready);
}

static void owner_fault_round(owner_fault_fixture *f, size_t round, bool inject) {
  bool complete = false;
  for (size_t lane = 0u; lane < FAULT_LANES; ++lane) {
    if (f->clients[lane] == NULL) continue;
    f->received[lane] = 0u;
    f->releases[lane] = 0u;
    for (size_t message = 0u; message < FAULT_MESSAGES; ++message) {
      memset(f->payload[lane][message], (int)(round * 7u + lane * 2u + message),
             FAULT_BYTES);
      check_equal(flowmq_send(f->clients[lane], peer_identity,
                             sizeof(peer_identity) - 1u,
                             FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
      if (message != 0u) {
        /* Retained single-peer sends are immediate-only. The following
         * copied message uses the real bounded outbound queue and needs the
         * Owner's post-route local flush after the retained write settles. */
        check_equal(flowmq_send(f->clients[lane], f->payload[lane][message],
                               FAULT_BYTES, FLOWMQ_DONTWAIT), SALTS_OK);
        continue;
      }
      f->buffer = mem_wrap_external(f->payload[lane][message], FAULT_BYTES,
                                    owner_fault_release, &f->releases[lane]);
      check_not_null(f->buffer);
      f->slice = mem_slice(f->buffer, 0u, FAULT_BYTES);
      check_not_null(f->slice.buffer);
      check_equal(flowmq_send_slice(f->clients[lane], &f->slice, FLOWMQ_DONTWAIT),
                  SALTS_OK);
      mem_slice_release(&f->slice);
      mem_buffer_release(f->buffer);
      f->buffer = NULL;
      check_equal(f->releases[lane], 0u);
    }
  }
  /* All three lanes have bounded retained work before the one Owner observe.
   * Injection positions refer to that actual batch, never OS arrival order. */
  for (size_t step = 0u; step < FAULT_PROGRESS_LIMIT && !complete; ++step) {
    owner_fault_progress(f, inject);
    complete = true;
    for (size_t lane = 0u; lane < FAULT_LANES; ++lane) {
      flowmq_router_peer_status_t peer = FLOWMQ_ROUTER_PEER_STATUS_INIT;
      unsigned char received[FAULT_BYTES];
      size_t received_size = 0u;
      if (f->clients[lane] == NULL) continue;
      while (f->received[lane] < FAULT_MESSAGES) {
        int status = flowmq_recv(f->peers[lane], received, sizeof(received),
                                 &received_size, FLOWMQ_DONTWAIT);
        if (status == SALTS_EBUSY) break;
        check_equal(status, SALTS_OK);
        check_equal(received_size, (size_t)FAULT_BYTES);
        check_equal(received, f->payload[lane][f->received[lane]], received_size);
        ++f->received[lane];
      }
      check_equal(flowmq_router_peer_status(f->clients[lane], peer_identity,
                                           sizeof(peer_identity) - 1u, &peer), SALTS_OK);
      check_true(peer.completed_messages <= peer.admitted_messages);
      check_true(peer.completed_bytes <= peer.admitted_bytes);
      complete = complete && f->received[lane] == FAULT_MESSAGES &&
          peer.completed_messages == peer.admitted_messages &&
          peer.outstanding_messages == 0u && peer.outstanding_bytes == 0u &&
          peer.send_credit_bytes == FAULT_MESSAGES * FAULT_BYTES;
      complete = complete && f->releases[lane] == 1u;
    }
  }
  check_true(complete);
  for (size_t lane = 0u; lane < FAULT_LANES; ++lane) {
    unsigned char received[FAULT_BYTES];
    size_t received_size = 0u;
    if (f->clients[lane] == NULL) continue;
    check_equal(flowmq_recv(f->peers[lane], received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT), SALTS_EBUSY);
    check_equal(f->releases[lane], 1u);
  }
}

spec("FlowMQ actual NativeIO batch routing faults") {
  static owner_fault_fixture fixture;

  before_each() {
    memset(&fixture, 0, sizeof(fixture));
    owner_fault_open(&fixture);
  }

  after_each() {
    mem_slice_release(&fixture.slice);
    mem_buffer_release(fixture.buffer);
    for (size_t lane = 0u; lane < FAULT_LANES; ++lane) {
      if (fixture.peers[lane] != NULL)
        check_equal(flowmq_close(fixture.peers[lane]), SALTS_OK);
      if (fixture.clients[lane] != NULL)
        check_equal(flowmq_owner_close_socket(fixture.owner, fixture.clients[lane]),
                    SALTS_OK);
    }
    if (fixture.owner != NULL) check_equal(flowmq_owner_term(fixture.owner), SALTS_OK);
    if (fixture.ctx != NULL) check_equal(flowmq_ctx_term(fixture.ctx), SALTS_OK);
  }

  for (size_t error_index = 0u; error_index < 2u; ++error_index) {
    it("settles real TCP completions after error at batch index %zu", error_index) {
      fixture.error_index = error_index;
      fixture.armed = true;
      for (size_t round = 0u; round < FAULT_ROUND_LIMIT && fixture.armed; ++round)
        owner_fault_round(&fixture, round, true);
      check_false(fixture.armed);
      check_equal(fixture.injections, 1u);
      check_true(fixture.fault_batch_count >= FAULT_LANES);
      check_equal(fixture.settled, fixture.fault_batch_count);
      check_equal(fixture.settled_after_error,
                  fixture.fault_batch_count - error_index - 1u);

      /* Closing the socket whose router reported the error must neither
       * strand retained sends nor stop healthy neighbors sharing its Owner. */
      check_equal(flowmq_close(fixture.peers[fixture.error_slot]), SALTS_OK);
      fixture.peers[fixture.error_slot] = NULL;
      check_equal(flowmq_owner_close_socket(
                      fixture.owner, fixture.clients[fixture.error_slot]), SALTS_OK);
      fixture.clients[fixture.error_slot] = NULL;
      owner_fault_round(&fixture, FAULT_ROUND_LIMIT, false);
      check_equal(fixture.injections, 1u);
    }
  }
}
