#include "flowmq_socket.h"
#include "tinytest.h"
#include "cmeta_error.h"
#include <salts/clock.h>

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#if defined(FLOWMQ_BENCH_SHARED_LISTENER)
#include "flowmq_owner.h"
#include "flowmq_socket_external_internal.h"
#endif

#if defined(FLOWMQ_BATCH_PROBE)
#include "flowmq_socket_batch_probe.h"
#include <stdio.h>

static flowmq_socket_t *bench_probe_socket(flowmq_ctx_t *ctx, int type) {
  flowmq_socket_t *socket = flowmq_socket(ctx, type);
  if (socket != NULL && flowmq_socket_batch_probe_coalesce(socket, 3) != SALTS_OK) {
    (void)flowmq_close(socket);
    return NULL;
  }
  return socket;
}
#define flowmq_socket(ctx, type) bench_probe_socket(ctx, type)
#endif

enum {
  BENCH_PATTERN_PAYLOAD_BYTES = 64u,
  BENCH_PATTERN_SAMPLES = 20000u,
  /* Readiness is sub-microsecond in some cases; 20k samples cover only about
   * 10ms. Use a longer window to reduce short scheduler/frequency transients. */
  BENCH_PATTERN_POLLOUT_SAMPLES = 1000000u,
  BENCH_PATTERN_PEERS = 4u,
  BENCH_PATTERN_PROGRESS_LIMIT = 100000u,
  BENCH_PATTERN_PROGRESS_TIMEOUT_MS = 250u
};

static size_t bench_pattern_samples(size_t full_samples) {
  const char *smoke = getenv("FLOWMQ_BENCH_SMOKE");
  return smoke != NULL && strcmp(smoke, "0") != 0
             ? (size_t)1u
             : full_samples;
}

typedef struct bench_socket_group_s {
  flowmq_ctx_t *ctx;
  flowmq_socket_t *sockets[BENCH_PATTERN_PEERS + 1u];
  size_t count;
#if defined(FLOWMQ_BENCH_SHARED_LISTENER)
  flowmq_owner_t *owner;
#endif
#if defined(FLOWMQ_BATCH_PROBE)
  flowmq_socket_batch_probe_t before;
  uint64_t started_ns;
  int omit_listener;
#endif
} bench_socket_group_t;

static flowmq_socket_t *bench_group_socket(bench_socket_group_t *group, int type) {
#if defined(FLOWMQ_BENCH_SHARED_LISTENER)
  if (group->owner == NULL) {
    flowmq_owner_config_t config = FLOWMQ_OWNER_CONFIG_INIT;
    config.socket_capacity = BENCH_PATTERN_PEERS + 1u;
    group->owner = flowmq_owner_new(group->ctx, &config);
    if (group->owner == NULL) return NULL;
  }
  return flowmq_owner_socket(group->owner, type);
#else
  return flowmq_socket(group->ctx, type);
#endif
}

static int bench_group_bind(flowmq_socket_t *socket) {
#if defined(FLOWMQ_BENCH_SHARED_LISTENER)
  return flowmq_socket_internal_bind_external(socket, "tcp://127.0.0.1:0");
#else
  return flowmq_bind(socket, "tcp://127.0.0.1:0");
#endif
}

static int bench_group_progress(bench_socket_group_t *group) {
  flowmq_pollitem_t items[BENCH_PATTERN_PEERS + 1u] = {0};
  size_t ready = 0u;
  if (group == NULL || group->count == 0u ||
      group->count > BENCH_PATTERN_PEERS + 1u)
    return SALTS_EINVAL;
  for (size_t i = 0u; i < group->count; ++i)
    items[i].socket = group->sockets[i];
#if defined(FLOWMQ_BENCH_SHARED_LISTENER)
  return flowmq_owner_poll(group->owner, items, group->count, 0u, &ready);
#else
  return flowmq_poll(items, group->count, 0u, &ready);
#endif
}

static int bench_group_progress_many(bench_socket_group_t *group,
                                     size_t iterations) {
  int status = SALTS_OK;
  for (size_t i = 0u; i < iterations && status == SALTS_OK; ++i)
    status = bench_group_progress(group);
  return status;
}

#if defined(FLOWMQ_BATCH_PROBE)
/* Owner-thread snapshots only, outside the sample loop. No borrowed payloads
 * or reset of live counters. Fixed groups and sample counts bound all sums. */
#define BENCH_PROBE_FIELDS(M) \
  M(direct_writes) M(queued_writes) M(messages) M(payload_bytes) \
  M(queued_ranges) M(submitted_ranges) M(coalesced_writes) M(coalesced_ranges) M(drive_calls) \
  M(listener_checks) M(listener_ready) M(manager_calls) M(manager_work) \
  M(local_slots) M(local_used_peers) M(listener_ns) M(listener_wait_ns) \
  M(client_poll_ns) M(local_progress_ns) M(receive_callback_ns) \
  M(listener_manager_ns) M(local_manager_ns) M(reconnect_ns)

static int bench_probe_snapshot(const bench_socket_group_t *group,
                                 flowmq_socket_batch_probe_t *total) {
  memset(total, 0, sizeof(*total));
  for (size_t i = 0u; i < group->count; ++i) {
    flowmq_socket_batch_probe_t socket_stats;
    int status = flowmq_socket_batch_probe_read(group->sockets[i], &socket_stats);
    if (status != SALTS_OK) return status;
#define BENCH_PROBE_ADD(field) total->field += socket_stats.field;
    BENCH_PROBE_FIELDS(BENCH_PROBE_ADD)
#undef BENCH_PROBE_ADD
    for (size_t n = 0u; n <= FLOWMQ_BATCH_PROBE_MAX_FRAMES; ++n)
      total->queued_range_histogram[n] += socket_stats.queued_range_histogram[n];
  }
  return SALTS_OK;
}

static int bench_probe_begin(bench_socket_group_t *group) {
  const char *omit = getenv("FLOWMQ_PATTERN_OMIT_LISTENER");
  if (omit != NULL && strcmp(omit, "0") != 0 && strcmp(omit, "1") != 0)
    return SALTS_EINVAL;
  group->omit_listener = omit != NULL && strcmp(omit, "1") == 0;
  /* Finish fixture setup/control completions before taking the baseline. */
  int status = bench_group_progress_many(group, 32u);
  /* Only this diagnostic window has a fixed established topology. Mode 2
   * preserves manager progress but cannot accept new connections. */
  for (size_t i = 0u; status == SALTS_OK && i < group->count; ++i)
    status = flowmq_socket_batch_probe_progress(
        group->sockets[i], group->omit_listener ? 2 : 0);
  if (status == SALTS_OK) status = bench_probe_snapshot(group, &group->before);
  group->started_ns = cmeta_hrtime();
  return status;
}

static int bench_probe_end(const bench_socket_group_t *group, const char *name,
                            size_t samples, size_t messages_per_sample) {
  const uint64_t wall_ns = cmeta_hrtime() - group->started_ns;
  const uint64_t expected = (uint64_t)samples * messages_per_sample;
  flowmq_socket_batch_probe_t delta;
  uint64_t histogram_writes = 0u, histogram_ranges = 0u;
  int status = bench_probe_snapshot(group, &delta);
  if (status != SALTS_OK) return status;
#define BENCH_PROBE_SUBTRACT(field) delta.field -= group->before.field;
  BENCH_PROBE_FIELDS(BENCH_PROBE_SUBTRACT)
#undef BENCH_PROBE_SUBTRACT
  for (size_t n = 1u; n <= FLOWMQ_BATCH_PROBE_MAX_FRAMES; ++n) {
    const uint64_t writes = delta.queued_range_histogram[n] -
                            group->before.queued_range_histogram[n];
    histogram_writes += writes;
    histogram_ranges += n * writes;
    if (writes != 0u)
      printf("PATTERN_HIST,%s,%zu,%llu\n", name, n, (unsigned long long)writes);
  }
  /* These cases send one wire DATA frame per logical message, including ROUTER
   * whose identity envelope is local. Control writes are outside these counters.
   * Admission counts and byte totals must match the verified received payloads. */
  if (delta.messages != expected ||
      delta.payload_bytes != expected * BENCH_PATTERN_PAYLOAD_BYTES ||
      delta.direct_writes + histogram_ranges != expected ||
      histogram_writes != delta.queued_writes || histogram_ranges != delta.queued_ranges ||
      delta.submitted_ranges + delta.coalesced_ranges !=
          delta.direct_writes + delta.queued_ranges + delta.coalesced_writes ||
      delta.listener_ready != 0u || delta.manager_work != 0u ||
      (group->omit_listener && delta.listener_checks != 0u))
    return SALTS_EPROTO;
  printf("PATTERN_PROBE,%s,%d,%zu,%llu,%llu", name, group->omit_listener, samples,
         (unsigned long long)expected, (unsigned long long)wall_ns);
#define BENCH_PROBE_PRINT(field) printf(",%llu", (unsigned long long)delta.field);
  BENCH_PROBE_FIELDS(BENCH_PROBE_PRINT)
#undef BENCH_PROBE_PRINT
  printf("\n");
  return SALTS_OK;
}

#define BENCH_PROBE_BEGIN(group) check_equal(bench_probe_begin(group), SALTS_OK)
#define BENCH_PROBE_END(group, name, samples, messages) \
  check_equal(bench_probe_end(group, name, samples, messages), SALTS_OK)
#else
#define BENCH_PROBE_BEGIN(group) ((void)0)
#define BENCH_PROBE_END(group, name, samples, messages) ((void)0)
#endif

static void bench_group_close(bench_socket_group_t *group) {
  if (group == NULL) return;
  for (size_t i = 0u; i < group->count; ++i) {
    if (group->sockets[i] != NULL) {
#if defined(FLOWMQ_BENCH_SHARED_LISTENER)
      check_equal(flowmq_owner_close_socket(group->owner, group->sockets[i]), SALTS_OK);
#else
      check_equal(flowmq_close(group->sockets[i]), SALTS_OK);
#endif
    }
  }
#if defined(FLOWMQ_BENCH_SHARED_LISTENER)
  if (group->owner != NULL) check_equal(flowmq_owner_term(group->owner), SALTS_OK);
#endif
  if (group->ctx != NULL) check_equal(flowmq_ctx_term(group->ctx), SALTS_OK);
  memset(group, 0, sizeof(*group));
}

static int bench_recv_exact(bench_socket_group_t *group,
                            flowmq_socket_t *receiver,
                            const void *expected,
                            size_t expected_size) {
  unsigned char buffer[BENCH_PATTERN_PAYLOAD_BYTES + 32u];
  size_t received = 0u;
  const uint64_t started_ms = cmeta_monotonic_ms();
  int status = SALTS_EBUSY;
  for (size_t i = 0u;
       i < BENCH_PATTERN_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
    status = bench_group_progress(group);
    if (status == SALTS_OK)
      status = flowmq_recv(receiver, buffer, sizeof(buffer), &received,
                           FLOWMQ_DONTWAIT);
    if (status == SALTS_EBUSY &&
        cmeta_monotonic_ms() - started_ms >=
            BENCH_PATTERN_PROGRESS_TIMEOUT_MS)
      break;
  }
  if (status != SALTS_OK) return status;
  if (received != expected_size ||
      (expected_size != 0u &&
       memcmp(buffer, expected, expected_size) != 0))
    return SALTS_EPROTO;

  /*
   * The receiver can observe DATA before the sender-side owner loop has
   * consumed the corresponding CNet send-completion/control work. Drain one
   * additional caller-driven progress turn so the next benchmark sample
   * starts from a stable write lane and current credit state.
   */
  return bench_group_progress(group);
}

static int bench_send_retry(bench_socket_group_t *group,
                            flowmq_socket_t *sender,
                            const void *payload,
                            size_t payload_size,
                            int flags) {
  const uint64_t started_ms = cmeta_monotonic_ms();
  int status = SALTS_EBUSY;
  for (size_t i = 0u;
       i < BENCH_PATTERN_PROGRESS_LIMIT &&
       (status == SALTS_EBUSY || status == SALTS_ENOBUFS); ++i) {
    status = bench_group_progress(group);
    if (status == SALTS_OK)
      status = flowmq_send(sender, payload, payload_size,
                           flags | FLOWMQ_DONTWAIT);
    if ((status == SALTS_EBUSY || status == SALTS_ENOBUFS) &&
        cmeta_monotonic_ms() - started_ms >=
            BENCH_PATTERN_PROGRESS_TIMEOUT_MS)
      break;
  }
  return status;
}

/* PUB --------------------------------------------------------------------- */

typedef struct bench_pub_s {
  bench_socket_group_t group;
  flowmq_socket_t *pub;
  flowmq_socket_t *subs[BENCH_PATTERN_PEERS];
} bench_pub_t;

static int bench_pub_open(bench_pub_t *fixture) {
  char endpoint[128] = {0};
  size_t endpoint_size = 0u;
  static const char payload[BENCH_PATTERN_PAYLOAD_BYTES] = {0};
  memset(fixture, 0, sizeof(*fixture));
  fixture->group.ctx = flowmq_ctx_new();
  if (fixture->group.ctx == NULL) return SALTS_ENOMEM;

  fixture->pub = bench_group_socket(&fixture->group, FLOWMQ_PUB);
  if (fixture->pub == NULL) return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->pub;

  if (bench_group_bind(fixture->pub) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_last_endpoint(fixture->pub, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    return SALTS_EIO;

  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; ++i) {
    fixture->subs[i] = bench_group_socket(&fixture->group, FLOWMQ_SUB);
    if (fixture->subs[i] == NULL) return SALTS_ENOMEM;
    fixture->group.sockets[fixture->group.count++] = fixture->subs[i];
    if (flowmq_setsockopt(fixture->subs[i], FLOWMQ_SUBSCRIBE, NULL, 0u) !=
        SALTS_OK)
      return SALTS_EIO;
    if (flowmq_connect(fixture->subs[i], endpoint) != SALTS_OK)
      return SALTS_EIO;
  }

  if (bench_group_progress_many(&fixture->group, 256u) != SALTS_OK)
    return SALTS_EIO;

  /* Prove all four subscription snapshots reached PUB before timing. */
  for (size_t attempt = 0u; attempt < BENCH_PATTERN_PROGRESS_LIMIT; ++attempt) {
    unsigned char received_peer[BENCH_PATTERN_PEERS] = {0};
    size_t received_count = 0u;
    if (flowmq_send(fixture->pub, payload, sizeof(payload), FLOWMQ_DONTWAIT) !=
        SALTS_OK) {
      if (bench_group_progress(&fixture->group) != SALTS_OK) return SALTS_EIO;
      continue;
    }
    for (size_t spin = 0u;
         spin < BENCH_PATTERN_PROGRESS_LIMIT &&
         received_count < BENCH_PATTERN_PEERS; ++spin) {
      if (bench_group_progress(&fixture->group) != SALTS_OK) return SALTS_EIO;
      for (size_t peer = 0u; peer < BENCH_PATTERN_PEERS; ++peer) {
        if (received_peer[peer]) continue;
        {
          unsigned char buffer[BENCH_PATTERN_PAYLOAD_BYTES];
          size_t received = 0u;
          int status = flowmq_recv(fixture->subs[peer], buffer, sizeof(buffer),
                                   &received, FLOWMQ_DONTWAIT);
          if (status == SALTS_OK) {
            if (received != sizeof(payload)) return SALTS_EPROTO;
            received_peer[peer] = 1u;
            ++received_count;
          } else if (status != SALTS_EBUSY) {
            return status;
          }
        }
      }
    }
    if (received_count == BENCH_PATTERN_PEERS) return SALTS_OK;
  }
  return SALTS_ETIMEDOUT;
}

static int bench_pub_exchange(bench_pub_t *fixture,
                              const void *payload,
                              size_t payload_size) {
  int status = flowmq_send(fixture->pub, payload, payload_size,
                           FLOWMQ_DONTWAIT);
  if (status != SALTS_OK) return status;
  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; ++i) {
    status = bench_recv_exact(&fixture->group, fixture->subs[i],
                              payload, payload_size);
    if (status != SALTS_OK) return status;
  }
  return SALTS_OK;
}

/* PUSH -------------------------------------------------------------------- */

typedef struct bench_push_s {
  bench_socket_group_t group;
  flowmq_socket_t *push;
  flowmq_socket_t *pulls[BENCH_PATTERN_PEERS];
} bench_push_t;

static int bench_push_open(bench_push_t *fixture) {
  memset(fixture, 0, sizeof(*fixture));
  fixture->group.ctx = flowmq_ctx_new();
  if (fixture->group.ctx == NULL) return SALTS_ENOMEM;
  fixture->push = bench_group_socket(&fixture->group, FLOWMQ_PUSH);
  if (fixture->push == NULL) return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->push;

  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; ++i) {
    char endpoint[128] = {0};
    size_t endpoint_size = 0u;
    fixture->pulls[i] = bench_group_socket(&fixture->group, FLOWMQ_PULL);
    if (fixture->pulls[i] == NULL) return SALTS_ENOMEM;
    fixture->group.sockets[fixture->group.count++] = fixture->pulls[i];
    if (bench_group_bind(fixture->pulls[i]) != SALTS_OK)
      return SALTS_EIO;
    if (flowmq_last_endpoint(fixture->pulls[i], endpoint, sizeof(endpoint),
                             &endpoint_size) != SALTS_OK)
      return SALTS_EIO;
    if (flowmq_connect(fixture->push, endpoint) != SALTS_OK)
      return SALTS_EIO;
  }
  return bench_group_progress_many(&fixture->group, 512u);
}

static int bench_push_cycle(bench_push_t *fixture,
                            const void *payload,
                            size_t payload_size) {
  int status;
  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; ++i) {
    status = bench_send_retry(&fixture->group, fixture->push,
                              payload, payload_size, 0);
    if (status != SALTS_OK) return status;
  }
  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; ++i) {
    status = bench_recv_exact(&fixture->group, fixture->pulls[i],
                              payload, payload_size);
    if (status != SALTS_OK) return status;
  }
  return SALTS_OK;
}

/* ROUTER ------------------------------------------------------------------ */

typedef struct bench_router_s {
  bench_socket_group_t group;
  flowmq_socket_t *router;
  flowmq_socket_t *dealer;
  char identity[16];
  size_t identity_size;
} bench_router_t;

static int bench_router_open(bench_router_t *fixture) {
  char endpoint[128] = {0};
  size_t endpoint_size = 0u;
  static const char warmup[] = "warmup";
  unsigned char received[32] = {0};
  size_t received_size = 0u;
  int status = SALTS_EBUSY;

  memset(fixture, 0, sizeof(*fixture));
  memcpy(fixture->identity, "bench-dealer", sizeof("bench-dealer") - 1u);
  fixture->identity_size = sizeof("bench-dealer") - 1u;

  fixture->group.ctx = flowmq_ctx_new();
  if (fixture->group.ctx == NULL) return SALTS_ENOMEM;
  fixture->router = bench_group_socket(&fixture->group, FLOWMQ_ROUTER);
  fixture->dealer = bench_group_socket(&fixture->group, FLOWMQ_DEALER);
  if (fixture->router == NULL || fixture->dealer == NULL) return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->router;
  fixture->group.sockets[fixture->group.count++] = fixture->dealer;

  if (flowmq_setsockopt(fixture->dealer, FLOWMQ_IDENTITY,
                        fixture->identity, fixture->identity_size) != SALTS_OK)
    return SALTS_EIO;
  if (bench_group_bind(fixture->router) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_last_endpoint(fixture->router, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_connect(fixture->dealer, endpoint) != SALTS_OK)
    return SALTS_EIO;

  /* Warm the identity table from a real DEALER -> ROUTER message. */
  for (size_t i = 0u;
       i < BENCH_PATTERN_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
    status = bench_group_progress(&fixture->group);
    if (status == SALTS_OK)
      status = flowmq_send(fixture->dealer, warmup, sizeof(warmup) - 1u,
                           FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;

  status = SALTS_EBUSY;
  for (size_t i = 0u;
       i < BENCH_PATTERN_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
    status = bench_group_progress(&fixture->group);
    if (status == SALTS_OK)
      status = flowmq_recv(fixture->router, received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK || received_size != fixture->identity_size)
    return SALTS_EPROTO;

  status = flowmq_recv(fixture->router, received, sizeof(received),
                       &received_size, FLOWMQ_DONTWAIT);
  if (status != SALTS_OK || received_size != sizeof(warmup) - 1u)
    return SALTS_EPROTO;
  return SALTS_OK;
}

static int bench_router_exchange(bench_router_t *fixture,
                                 const void *payload,
                                 size_t payload_size) {
  int status = bench_send_retry(&fixture->group, fixture->router,
                                fixture->identity, fixture->identity_size,
                                FLOWMQ_SNDMORE);
  if (status != SALTS_OK) return status;
  status = bench_send_retry(&fixture->group, fixture->router,
                            payload, payload_size, 0);
  if (status != SALTS_OK) return status;
  return bench_recv_exact(&fixture->group, fixture->dealer,
                          payload, payload_size);
}

/* REQ/REP ----------------------------------------------------------------- */

typedef struct bench_reqrep_s {
  bench_socket_group_t group;
  flowmq_socket_t *req;
  flowmq_socket_t *rep;
} bench_reqrep_t;

static int bench_reqrep_open(bench_reqrep_t *fixture) {
  char endpoint[128] = {0};
  size_t endpoint_size = 0u;
  memset(fixture, 0, sizeof(*fixture));
  fixture->group.ctx = flowmq_ctx_new();
  if (fixture->group.ctx == NULL) return SALTS_ENOMEM;
  fixture->req = bench_group_socket(&fixture->group, FLOWMQ_REQ);
  fixture->rep = bench_group_socket(&fixture->group, FLOWMQ_REP);
  if (fixture->req == NULL || fixture->rep == NULL) return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->req;
  fixture->group.sockets[fixture->group.count++] = fixture->rep;
  if (bench_group_bind(fixture->rep) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_last_endpoint(fixture->rep, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_connect(fixture->req, endpoint) != SALTS_OK)
    return SALTS_EIO;
  return bench_group_progress_many(&fixture->group, 256u);
}

static int bench_reqrep_roundtrip(bench_reqrep_t *fixture,
                                  const void *payload,
                                  size_t payload_size) {
  int status = bench_send_retry(&fixture->group, fixture->req,
                                payload, payload_size, 0);
  if (status != SALTS_OK) return status;
  status = bench_recv_exact(&fixture->group, fixture->rep,
                            payload, payload_size);
  if (status != SALTS_OK) return status;
  status = bench_send_retry(&fixture->group, fixture->rep,
                            payload, payload_size, 0);
  if (status != SALTS_OK) return status;
  return bench_recv_exact(&fixture->group, fixture->req,
                          payload, payload_size);
}


/* Adversarial policy edges ------------------------------------------------ */

static int bench_pollout(bench_socket_group_t *group, flowmq_socket_t *socket,
                         int expected_ready) {
  flowmq_pollitem_t item = {.socket = socket, .events = FLOWMQ_POLLOUT};
  size_t ready = 0u;
#if defined(FLOWMQ_BENCH_SHARED_LISTENER)
  /* Owner poll always progresses the entire lane, including sockets outside
   * items. Its readiness cost is not an isolated one-socket ordinary poll. */
  int status = flowmq_owner_poll(group->owner, &item, 1u, 0u, &ready);
#else
  int status = flowmq_poll(&item, 1u, 0u, &ready);
  (void)group;
#endif
  if (status != SALTS_OK) return status;
  if (expected_ready)
    return ready == 1u && item.revents == FLOWMQ_POLLOUT
               ? SALTS_OK
               : SALTS_EPROTO;
  return ready == 0u && item.revents == 0u ? SALTS_OK : SALTS_EPROTO;
}

typedef struct bench_pub_filtered_s {
  bench_socket_group_t group;
  flowmq_socket_t *pub;
  flowmq_socket_t *subs[BENCH_PATTERN_PEERS];
} bench_pub_filtered_t;

static int bench_pub_filtered_open(bench_pub_filtered_t *fixture,
                                   const void *matching_payload,
                                   size_t payload_size) {
  static const char orders[] = "orders.";
  static const char payments[] = "payments.";
  char endpoint[128] = {0};
  size_t endpoint_size = 0u;
  int status;

  memset(fixture, 0, sizeof(*fixture));
  fixture->group.ctx = flowmq_ctx_new();
  if (fixture->group.ctx == NULL) return SALTS_ENOMEM;
  fixture->pub = bench_group_socket(&fixture->group, FLOWMQ_PUB);
  if (fixture->pub == NULL) return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->pub;
  if (bench_group_bind(fixture->pub) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_last_endpoint(fixture->pub, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    return SALTS_EIO;

  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; ++i) {
    const char *prefix = (i & 1u) == 0u ? orders : payments;
    size_t prefix_size = (i & 1u) == 0u ? sizeof(orders) - 1u
                                         : sizeof(payments) - 1u;
    fixture->subs[i] = bench_group_socket(&fixture->group, FLOWMQ_SUB);
    if (fixture->subs[i] == NULL) return SALTS_ENOMEM;
    fixture->group.sockets[fixture->group.count++] = fixture->subs[i];
    if (flowmq_setsockopt(fixture->subs[i], FLOWMQ_SUBSCRIBE,
                          prefix, prefix_size) != SALTS_OK)
      return SALTS_EIO;
    if (flowmq_connect(fixture->subs[i], endpoint) != SALTS_OK)
      return SALTS_EIO;
  }

  if (bench_group_progress_many(&fixture->group, 256u) != SALTS_OK)
    return SALTS_EIO;

  /* Prove the subscription snapshot before timing. */
  status = flowmq_send(fixture->pub, matching_payload, payload_size,
                       FLOWMQ_DONTWAIT);
  if (status != SALTS_OK) return status;
  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; i += 2u) {
    status = bench_recv_exact(&fixture->group, fixture->subs[i],
                              matching_payload, payload_size);
    if (status != SALTS_OK) return status;
  }
  if (bench_group_progress_many(&fixture->group, 32u) != SALTS_OK)
    return SALTS_EIO;
  for (size_t i = 1u; i < BENCH_PATTERN_PEERS; i += 2u) {
    unsigned char buffer[BENCH_PATTERN_PAYLOAD_BYTES + 32u];
    size_t received = 0u;
    status = flowmq_recv(fixture->subs[i], buffer, sizeof(buffer), &received,
                         FLOWMQ_DONTWAIT);
    if (status != SALTS_EBUSY) return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int bench_pub_filtered_exchange(bench_pub_filtered_t *fixture,
                                       const void *payload,
                                       size_t payload_size) {
  int status = flowmq_send(fixture->pub, payload, payload_size,
                           FLOWMQ_DONTWAIT);
  if (status != SALTS_OK) return status;
  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; i += 2u) {
    status = bench_recv_exact(&fixture->group, fixture->subs[i],
                              payload, payload_size);
    if (status != SALTS_OK) return status;
  }
  return SALTS_OK;
}

typedef struct bench_router_isolation_s {
  bench_socket_group_t group;
  flowmq_socket_t *router;
  flowmq_socket_t *slow;
  flowmq_socket_t *healthy;
  char slow_identity[16];
  char healthy_identity[16];
  size_t slow_identity_size;
  size_t healthy_identity_size;
} bench_router_isolation_t;

static int bench_router_learn_identity(bench_socket_group_t *group,
                                       flowmq_socket_t *router,
                                       flowmq_socket_t *dealer,
                                       const char *identity,
                                       size_t identity_size) {
  static const char warmup[] = "warm";
  int status = bench_send_retry(group, dealer, warmup,
                                sizeof(warmup) - 1u, 0);
  if (status != SALTS_OK) return status;
  status = bench_recv_exact(group, router, identity, identity_size);
  if (status != SALTS_OK) return status;
  return bench_recv_exact(group, router, warmup, sizeof(warmup) - 1u);
}

static int bench_router_isolation_open(bench_router_isolation_t *fixture,
                                       const void *payload,
                                       size_t payload_size) {
  char endpoint[128] = {0};
  size_t endpoint_size = 0u;
  size_t slow_window = payload_size;
  int status;

  memset(fixture, 0, sizeof(*fixture));
  memcpy(fixture->slow_identity, "slow", sizeof("slow") - 1u);
  memcpy(fixture->healthy_identity, "healthy", sizeof("healthy") - 1u);
  fixture->slow_identity_size = sizeof("slow") - 1u;
  fixture->healthy_identity_size = sizeof("healthy") - 1u;

  fixture->group.ctx = flowmq_ctx_new();
  if (fixture->group.ctx == NULL) return SALTS_ENOMEM;
  fixture->router = bench_group_socket(&fixture->group, FLOWMQ_ROUTER);
  fixture->slow = bench_group_socket(&fixture->group, FLOWMQ_DEALER);
  fixture->healthy = bench_group_socket(&fixture->group, FLOWMQ_DEALER);
  if (fixture->router == NULL || fixture->slow == NULL ||
      fixture->healthy == NULL)
    return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->router;
  fixture->group.sockets[fixture->group.count++] = fixture->slow;
  fixture->group.sockets[fixture->group.count++] = fixture->healthy;

  if (flowmq_setsockopt(fixture->slow, FLOWMQ_IDENTITY,
                        fixture->slow_identity,
                        fixture->slow_identity_size) != SALTS_OK ||
      flowmq_setsockopt(fixture->healthy, FLOWMQ_IDENTITY,
                        fixture->healthy_identity,
                        fixture->healthy_identity_size) != SALTS_OK ||
      flowmq_setsockopt(fixture->slow, FLOWMQ_RCVHWM_BYTES,
                        &slow_window, sizeof(slow_window)) != SALTS_OK)
    return SALTS_EIO;

  if (bench_group_bind(fixture->router) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_last_endpoint(fixture->router, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_connect(fixture->slow, endpoint) != SALTS_OK ||
      flowmq_connect(fixture->healthy, endpoint) != SALTS_OK)
    return SALTS_EIO;

  status = bench_router_learn_identity(
      &fixture->group, fixture->router, fixture->slow,
      fixture->slow_identity, fixture->slow_identity_size);
  if (status != SALTS_OK) return status;
  status = bench_router_learn_identity(
      &fixture->group, fixture->router, fixture->healthy,
      fixture->healthy_identity, fixture->healthy_identity_size);
  if (status != SALTS_OK) return status;

  /* Consume the slow peer's entire advertised receive-credit window once. */
  status = bench_send_retry(&fixture->group, fixture->router,
                            fixture->slow_identity,
                            fixture->slow_identity_size, FLOWMQ_SNDMORE);
  if (status != SALTS_OK) return status;
  status = bench_send_retry(&fixture->group, fixture->router,
                            payload, payload_size, 0);
  if (status != SALTS_OK) return status;
  return bench_group_progress_many(&fixture->group, 32u);
}

static int bench_router_isolation_cycle(bench_router_isolation_t *fixture,
                                        const void *payload,
                                        size_t payload_size) {
  int status;
  status = flowmq_send(fixture->router, fixture->slow_identity,
                       fixture->slow_identity_size,
                       FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE);
  if (status != SALTS_OK) return status;
  status = flowmq_send(fixture->router, payload, payload_size,
                       FLOWMQ_DONTWAIT);
  if (status != SALTS_ENOBUFS) return SALTS_EPROTO;

  status = bench_pollout(&fixture->group, fixture->router, 1);
  if (status != SALTS_OK) return status;

  status = flowmq_send(fixture->router, fixture->healthy_identity,
                       fixture->healthy_identity_size,
                       FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE);
  if (status != SALTS_OK) return status;
  status = bench_send_retry(&fixture->group, fixture->router,
                            payload, payload_size, 0);
  if (status != SALTS_OK) return status;
  return bench_recv_exact(&fixture->group, fixture->healthy,
                          payload, payload_size);
}

typedef struct bench_pair_blocked_s {
  bench_socket_group_t group;
  flowmq_socket_t *sender;
  flowmq_socket_t *receiver;
} bench_pair_blocked_t;

static int bench_pair_blocked_open(bench_pair_blocked_t *fixture,
                                   const void *payload,
                                   size_t payload_size) {
  char endpoint[128] = {0};
  size_t endpoint_size = 0u;
  size_t receive_window = payload_size;
  int status;

  memset(fixture, 0, sizeof(*fixture));
  fixture->group.ctx = flowmq_ctx_new();
  if (fixture->group.ctx == NULL) return SALTS_ENOMEM;
  fixture->sender = bench_group_socket(&fixture->group, FLOWMQ_PAIR);
  fixture->receiver = bench_group_socket(&fixture->group, FLOWMQ_PAIR);
  if (fixture->sender == NULL || fixture->receiver == NULL)
    return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->sender;
  fixture->group.sockets[fixture->group.count++] = fixture->receiver;

  if (flowmq_setsockopt(fixture->receiver, FLOWMQ_RCVHWM_BYTES,
                        &receive_window, sizeof(receive_window)) != SALTS_OK)
    return SALTS_EIO;
  if (bench_group_bind(fixture->receiver) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_last_endpoint(fixture->receiver, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_connect(fixture->sender, endpoint) != SALTS_OK)
    return SALTS_EIO;

  status = bench_send_retry(&fixture->group, fixture->sender,
                            payload, payload_size, 0);
  if (status != SALTS_OK) return status;
  if (bench_group_progress_many(&fixture->group, 32u) != SALTS_OK)
    return SALTS_EIO;
  return bench_pollout(&fixture->group, fixture->sender, 0);
}

static int bench_reqrep_prepare_reply(bench_reqrep_t *fixture,
                                      const void *payload,
                                      size_t payload_size) {
  int status = bench_send_retry(&fixture->group, fixture->req,
                                payload, payload_size, 0);
  if (status != SALTS_OK) return status;
  return bench_recv_exact(&fixture->group, fixture->rep,
                          payload, payload_size);
}

spec("FlowMQ pattern dispatch benchmark") {
  bench("descriptor-driven pattern policies") {
    static unsigned char payload[BENCH_PATTERN_PAYLOAD_BYTES];
    bench_pub_t pub;
    bench_push_t push;
    bench_router_t router;
    bench_reqrep_t reqrep;
    const size_t samples = bench_pattern_samples(BENCH_PATTERN_SAMPLES);
    const size_t poll_samples = bench_pattern_samples(BENCH_PATTERN_POLLOUT_SAMPLES);
    int status;

#if defined(FLOWMQ_BATCH_PROBE)
    printf("PATTERN_PROBE_HEADER,scenario,omit_listener,samples,expected_messages,wall_ns");
#define BENCH_PROBE_HEADER(field) printf("," #field);
    BENCH_PROBE_FIELDS(BENCH_PROBE_HEADER)
#undef BENCH_PROBE_HEADER
    printf("\nPATTERN_HIST_HEADER,scenario,frames_per_write,writes\n");
#endif
    memset(payload, 0x5a, sizeof(payload));

    status = bench_pub_open(&pub);
    check_equal(status, SALTS_OK);
    BENCH_PROBE_BEGIN(&pub.group);
    benchmark_io("PUB 4-peer fanout", samples,
                 BENCH_PATTERN_PEERS,
                 BENCH_PATTERN_PEERS * BENCH_PATTERN_PAYLOAD_BYTES) {
      if (status == SALTS_OK)
        status = bench_pub_exchange(&pub, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);
    BENCH_PROBE_END(&pub.group, "pub_fanout", samples, BENCH_PATTERN_PEERS);
    bench_group_close(&pub.group);

    status = bench_push_open(&push);
    check_equal(status, SALTS_OK);
    check_equal(bench_push_cycle(&push, payload, sizeof(payload)), SALTS_OK);
    BENCH_PROBE_BEGIN(&push.group);
    benchmark_io("PUSH 4-peer round-robin", samples,
                 BENCH_PATTERN_PEERS,
                 BENCH_PATTERN_PEERS * BENCH_PATTERN_PAYLOAD_BYTES) {
      if (status == SALTS_OK)
        status = bench_push_cycle(&push, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);
    BENCH_PROBE_END(&push.group, "push_round_robin", samples, BENCH_PATTERN_PEERS);
    bench_group_close(&push.group);

    status = bench_router_open(&router);
    check_equal(status, SALTS_OK);
    check_equal(bench_router_exchange(&router, payload, sizeof(payload)),
                SALTS_OK);
    BENCH_PROBE_BEGIN(&router.group);
    benchmark_io("ROUTER identity one-way", samples,
                 1u, BENCH_PATTERN_PAYLOAD_BYTES) {
      if (status == SALTS_OK)
        status = bench_router_exchange(&router, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);
    BENCH_PROBE_END(&router.group, "router_identity", samples, 1u);
    bench_group_close(&router.group);

    status = bench_reqrep_open(&reqrep);
    check_equal(status, SALTS_OK);
    check_equal(bench_reqrep_roundtrip(&reqrep, payload, sizeof(payload)),
                SALTS_OK);
    BENCH_PROBE_BEGIN(&reqrep.group);
    benchmark_io("REQ/REP roundtrip", samples,
                 2u, 2u * BENCH_PATTERN_PAYLOAD_BYTES) {
      if (status == SALTS_OK)
        status = bench_reqrep_roundtrip(&reqrep, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);
    BENCH_PROBE_END(&reqrep.group, "reqrep", samples, 2u);
    bench_group_close(&reqrep.group);

    {
      static unsigned char filtered_payload[BENCH_PATTERN_PAYLOAD_BYTES];
      bench_pub_filtered_t filtered;
      memset(filtered_payload, 0x5a, sizeof(filtered_payload));
      memcpy(filtered_payload, "orders.", sizeof("orders.") - 1u);
      status = bench_pub_filtered_open(&filtered, filtered_payload,
                                       sizeof(filtered_payload));
      check_equal(status, SALTS_OK);
      BENCH_PROBE_BEGIN(&filtered.group);
      benchmark_io("PUB 2-of-4 filtered fanout", samples, 2u,
                   2u * BENCH_PATTERN_PAYLOAD_BYTES) {
        if (status == SALTS_OK)
          status = bench_pub_filtered_exchange(
              &filtered, filtered_payload, sizeof(filtered_payload));
      }
      check_equal(status, SALTS_OK);
      BENCH_PROBE_END(&filtered.group, "pub_filtered", samples, 2u);
      bench_group_close(&filtered.group);
    }

    {
      bench_router_isolation_t isolation;
      status = bench_router_isolation_open(&isolation, payload,
                                           sizeof(payload));
      check_equal(status, SALTS_OK);
      check_equal(bench_router_isolation_cycle(
                      &isolation, payload, sizeof(payload)),
                  SALTS_OK);
      BENCH_PROBE_BEGIN(&isolation.group);
      benchmark_io("ROUTER slow-peer isolation", samples, 1u,
                   BENCH_PATTERN_PAYLOAD_BYTES) {
        if (status == SALTS_OK)
          status = bench_router_isolation_cycle(
              &isolation, payload, sizeof(payload));
      }
      check_equal(status, SALTS_OK);
      BENCH_PROBE_END(&isolation.group, "router_isolation", samples, 1u);
      bench_group_close(&isolation.group);
    }

    {
      bench_push_t poll_push;
      status = bench_push_open(&poll_push);
      check_equal(status, SALTS_OK);
      check_equal(bench_pollout(&poll_push.group, poll_push.push, 1), SALTS_OK);
      BENCH_PROBE_BEGIN(&poll_push.group);
      benchmark_ops("POLLOUT PUSH ready", poll_samples, 1u) {
        if (status == SALTS_OK)
          status = bench_pollout(&poll_push.group, poll_push.push, 1);
      }
      check_equal(status, SALTS_OK);
      BENCH_PROBE_END(&poll_push.group, "pollout_push", poll_samples, 0u);
      bench_group_close(&poll_push.group);
    }

    {
      bench_pair_blocked_t blocked;
      status = bench_pair_blocked_open(&blocked, payload, sizeof(payload));
      check_equal(status, SALTS_OK);
      BENCH_PROBE_BEGIN(&blocked.group);
      benchmark_ops("POLLOUT PAIR credit-exhausted", poll_samples, 1u) {
        if (status == SALTS_OK)
          status = bench_pollout(&blocked.group, blocked.sender, 0);
      }
      check_equal(status, SALTS_OK);
      BENCH_PROBE_END(&blocked.group, "pollout_blocked_pair", poll_samples, 0u);
      bench_group_close(&blocked.group);
    }

    {
      bench_reqrep_t reply_ready;
      status = bench_reqrep_open(&reply_ready);
      check_equal(status, SALTS_OK);
      status = bench_reqrep_prepare_reply(&reply_ready, payload,
                                          sizeof(payload));
      check_equal(status, SALTS_OK);
      check_equal(bench_pollout(&reply_ready.group, reply_ready.rep, 1), SALTS_OK);
      BENCH_PROBE_BEGIN(&reply_ready.group);
      benchmark_ops("POLLOUT REP reply-peer ready", poll_samples, 1u) {
        if (status == SALTS_OK)
          status = bench_pollout(&reply_ready.group, reply_ready.rep, 1);
      }
      check_equal(status, SALTS_OK);
      BENCH_PROBE_END(&reply_ready.group, "pollout_rep", poll_samples, 0u);
      status = bench_send_retry(&reply_ready.group, reply_ready.rep,
                                payload, sizeof(payload), 0);
      check_equal(status, SALTS_OK);
      check_equal(bench_recv_exact(&reply_ready.group, reply_ready.req,
                                   payload, sizeof(payload)), SALTS_OK);
      bench_group_close(&reply_ready.group);
    }
  }
}
