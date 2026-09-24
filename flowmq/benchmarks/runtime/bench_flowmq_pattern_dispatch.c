#include "flowmq_socket.h"
#include "tinytest.h"
#include "salts_error.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

enum {
  BENCH_PATTERN_PAYLOAD_BYTES = 64u,
  BENCH_PATTERN_SAMPLES = 20000u,
  BENCH_PATTERN_PEERS = 4u,
  BENCH_PATTERN_PROGRESS_LIMIT = 10000u
};

static size_t bench_pattern_samples(void) {
  const char *smoke = getenv("FLOWMQ_BENCH_SMOKE");
  return smoke != NULL && strcmp(smoke, "0") != 0
             ? (size_t)1u
             : (size_t)BENCH_PATTERN_SAMPLES;
}

typedef struct bench_socket_group_s {
  flowmq_ctx_t *ctx;
  flowmq_socket_t *sockets[BENCH_PATTERN_PEERS + 1u];
  size_t count;
} bench_socket_group_t;

static int bench_group_progress(bench_socket_group_t *group) {
  flowmq_pollitem_t items[BENCH_PATTERN_PEERS + 1u] = {0};
  size_t ready = 0u;
  if (group == NULL || group->count == 0u ||
      group->count > BENCH_PATTERN_PEERS + 1u)
    return SALTS_EINVAL;
  for (size_t i = 0u; i < group->count; ++i)
    items[i].socket = group->sockets[i];
  return flowmq_poll(items, group->count, 0u, &ready);
}

static int bench_group_progress_many(bench_socket_group_t *group,
                                     size_t iterations) {
  int status = SALTS_OK;
  for (size_t i = 0u; i < iterations && status == SALTS_OK; ++i)
    status = bench_group_progress(group);
  return status;
}

static void bench_group_close(bench_socket_group_t *group) {
  if (group == NULL) return;
  for (size_t i = 0u; i < group->count; ++i) {
    if (group->sockets[i] != NULL)
      (void)flowmq_close(group->sockets[i]);
  }
  if (group->ctx != NULL) (void)flowmq_ctx_term(group->ctx);
  memset(group, 0, sizeof(*group));
}

static int bench_recv_exact(bench_socket_group_t *group,
                            flowmq_socket_t *receiver,
                            const void *expected,
                            size_t expected_size) {
  unsigned char buffer[BENCH_PATTERN_PAYLOAD_BYTES + 32u];
  size_t received = 0u;
  int status = SALTS_EBUSY;
  for (size_t i = 0u;
       i < BENCH_PATTERN_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
    status = bench_group_progress(group);
    if (status == SALTS_OK)
      status = flowmq_recv(receiver, buffer, sizeof(buffer), &received,
                           FLOWMQ_DONTWAIT);
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
  int status = SALTS_EBUSY;
  for (size_t i = 0u;
       i < BENCH_PATTERN_PROGRESS_LIMIT &&
       (status == SALTS_EBUSY || status == SALTS_ENOBUFS); ++i) {
    status = bench_group_progress(group);
    if (status == SALTS_OK)
      status = flowmq_send(sender, payload, payload_size,
                           flags | FLOWMQ_DONTWAIT);
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

  fixture->pub = flowmq_socket(fixture->group.ctx, FLOWMQ_PUB);
  if (fixture->pub == NULL) return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->pub;

  if (flowmq_bind(fixture->pub, "tcp://127.0.0.1:0") != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_last_endpoint(fixture->pub, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    return SALTS_EIO;

  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; ++i) {
    fixture->subs[i] = flowmq_socket(fixture->group.ctx, FLOWMQ_SUB);
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
  fixture->push = flowmq_socket(fixture->group.ctx, FLOWMQ_PUSH);
  if (fixture->push == NULL) return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->push;

  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; ++i) {
    char endpoint[128] = {0};
    size_t endpoint_size = 0u;
    fixture->pulls[i] = flowmq_socket(fixture->group.ctx, FLOWMQ_PULL);
    if (fixture->pulls[i] == NULL) return SALTS_ENOMEM;
    fixture->group.sockets[fixture->group.count++] = fixture->pulls[i];
    if (flowmq_bind(fixture->pulls[i], "tcp://127.0.0.1:0") != SALTS_OK)
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
  fixture->router = flowmq_socket(fixture->group.ctx, FLOWMQ_ROUTER);
  fixture->dealer = flowmq_socket(fixture->group.ctx, FLOWMQ_DEALER);
  if (fixture->router == NULL || fixture->dealer == NULL) return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->router;
  fixture->group.sockets[fixture->group.count++] = fixture->dealer;

  if (flowmq_setsockopt(fixture->dealer, FLOWMQ_IDENTITY,
                        fixture->identity, fixture->identity_size) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_bind(fixture->router, "tcp://127.0.0.1:0") != SALTS_OK)
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
  int status = SALTS_EBUSY;

  /*
   * ROUTER routing-id + payload is one admission transaction. If target
   * capacity/credit rejects the final payload, FlowMQ intentionally rolls
   * that route transaction back. Retry must therefore restart from the
   * routing-id; retrying only the payload would no longer name a target.
   */
  for (size_t i = 0u; i < BENCH_PATTERN_PROGRESS_LIMIT; ++i) {
    status = flowmq_send(fixture->router, fixture->identity,
                         fixture->identity_size,
                         FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE);
    if (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
      status = bench_group_progress(&fixture->group);
      if (status != SALTS_OK) return status;
      continue;
    }
    if (status != SALTS_OK) return status;

    status = flowmq_send(fixture->router, payload, payload_size,
                         FLOWMQ_DONTWAIT);
    if (status == SALTS_OK) break;
    if (status != SALTS_EBUSY && status != SALTS_ENOBUFS) return status;

    status = bench_group_progress(&fixture->group);
    if (status != SALTS_OK) return status;
  }

  if (status != SALTS_OK) return status;
  status = bench_recv_exact(&fixture->group, fixture->dealer,
                            payload, payload_size);
  if (status != SALTS_OK) return status;

  /*
   * Give the receiver a caller-driven progress turn after consumption so its
   * cumulative credit update/control lane is not deferred into the next timed
   * sample. This keeps the benchmark focused on routing policy rather than
   * interval-dependent credit publication.
   */
  return bench_group_progress(&fixture->group);
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
  fixture->req = flowmq_socket(fixture->group.ctx, FLOWMQ_REQ);
  fixture->rep = flowmq_socket(fixture->group.ctx, FLOWMQ_REP);
  if (fixture->req == NULL || fixture->rep == NULL) return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->req;
  fixture->group.sockets[fixture->group.count++] = fixture->rep;
  if (flowmq_bind(fixture->rep, "tcp://127.0.0.1:0") != SALTS_OK)
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

static int bench_pollout(flowmq_socket_t *socket, int expected_ready) {
  flowmq_pollitem_t item = {.socket = socket, .events = FLOWMQ_POLLOUT};
  size_t ready = 0u;
  int status = flowmq_poll(&item, 1u, 0u, &ready);
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
  fixture->pub = flowmq_socket(fixture->group.ctx, FLOWMQ_PUB);
  if (fixture->pub == NULL) return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->pub;
  if (flowmq_bind(fixture->pub, "tcp://127.0.0.1:0") != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_last_endpoint(fixture->pub, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    return SALTS_EIO;

  for (size_t i = 0u; i < BENCH_PATTERN_PEERS; ++i) {
    const char *prefix = (i & 1u) == 0u ? orders : payments;
    size_t prefix_size = (i & 1u) == 0u ? sizeof(orders) - 1u
                                         : sizeof(payments) - 1u;
    fixture->subs[i] = flowmq_socket(fixture->group.ctx, FLOWMQ_SUB);
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
  fixture->router = flowmq_socket(fixture->group.ctx, FLOWMQ_ROUTER);
  fixture->slow = flowmq_socket(fixture->group.ctx, FLOWMQ_DEALER);
  fixture->healthy = flowmq_socket(fixture->group.ctx, FLOWMQ_DEALER);
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

  if (flowmq_bind(fixture->router, "tcp://127.0.0.1:0") != SALTS_OK)
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

  status = bench_pollout(fixture->router, 1);
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
  fixture->sender = flowmq_socket(fixture->group.ctx, FLOWMQ_PAIR);
  fixture->receiver = flowmq_socket(fixture->group.ctx, FLOWMQ_PAIR);
  if (fixture->sender == NULL || fixture->receiver == NULL)
    return SALTS_ENOMEM;
  fixture->group.sockets[fixture->group.count++] = fixture->sender;
  fixture->group.sockets[fixture->group.count++] = fixture->receiver;

  if (flowmq_setsockopt(fixture->receiver, FLOWMQ_RCVHWM_BYTES,
                        &receive_window, sizeof(receive_window)) != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_bind(fixture->receiver, "tcp://127.0.0.1:0") != SALTS_OK)
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
  return bench_pollout(fixture->sender, 0);
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
    const size_t samples = bench_pattern_samples();
    int status;

    memset(payload, 0x5a, sizeof(payload));

    status = bench_pub_open(&pub);
    check_equal(status, SALTS_OK);
    benchmark_io("PUB 4-peer fanout", samples,
                 BENCH_PATTERN_PEERS,
                 BENCH_PATTERN_PEERS * BENCH_PATTERN_PAYLOAD_BYTES) {
      if (status == SALTS_OK)
        status = bench_pub_exchange(&pub, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);
    bench_group_close(&pub.group);

    status = bench_push_open(&push);
    check_equal(status, SALTS_OK);
    check_equal(bench_push_cycle(&push, payload, sizeof(payload)), SALTS_OK);
    benchmark_io("PUSH 4-peer round-robin", samples,
                 BENCH_PATTERN_PEERS,
                 BENCH_PATTERN_PEERS * BENCH_PATTERN_PAYLOAD_BYTES) {
      if (status == SALTS_OK)
        status = bench_push_cycle(&push, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);
    bench_group_close(&push.group);

    status = bench_router_open(&router);
    check_equal(status, SALTS_OK);
    check_equal(bench_router_exchange(&router, payload, sizeof(payload)),
                SALTS_OK);
    benchmark_io("ROUTER identity one-way", samples,
                 1u, BENCH_PATTERN_PAYLOAD_BYTES) {
      if (status == SALTS_OK)
        status = bench_router_exchange(&router, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);
    bench_group_close(&router.group);

    status = bench_reqrep_open(&reqrep);
    check_equal(status, SALTS_OK);
    check_equal(bench_reqrep_roundtrip(&reqrep, payload, sizeof(payload)),
                SALTS_OK);
    benchmark_io("REQ/REP roundtrip", samples,
                 2u, 2u * BENCH_PATTERN_PAYLOAD_BYTES) {
      if (status == SALTS_OK)
        status = bench_reqrep_roundtrip(&reqrep, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);
    bench_group_close(&reqrep.group);

    {
      static unsigned char filtered_payload[BENCH_PATTERN_PAYLOAD_BYTES];
      bench_pub_filtered_t filtered;
      memset(filtered_payload, 0x5a, sizeof(filtered_payload));
      memcpy(filtered_payload, "orders.", sizeof("orders.") - 1u);
      status = bench_pub_filtered_open(&filtered, filtered_payload,
                                       sizeof(filtered_payload));
      check_equal(status, SALTS_OK);
      benchmark_io("PUB 2-of-4 filtered fanout", samples, 2u,
                   2u * BENCH_PATTERN_PAYLOAD_BYTES) {
        if (status == SALTS_OK)
          status = bench_pub_filtered_exchange(
              &filtered, filtered_payload, sizeof(filtered_payload));
      }
      check_equal(status, SALTS_OK);
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
      benchmark_io("ROUTER slow-peer isolation", samples, 1u,
                   BENCH_PATTERN_PAYLOAD_BYTES) {
        if (status == SALTS_OK)
          status = bench_router_isolation_cycle(
              &isolation, payload, sizeof(payload));
      }
      check_equal(status, SALTS_OK);
      bench_group_close(&isolation.group);
    }

    {
      bench_push_t poll_push;
      status = bench_push_open(&poll_push);
      check_equal(status, SALTS_OK);
      check_equal(bench_pollout(poll_push.push, 1), SALTS_OK);
      benchmark_ops("POLLOUT PUSH ready", samples, 1u) {
        if (status == SALTS_OK)
          status = bench_pollout(poll_push.push, 1);
      }
      check_equal(status, SALTS_OK);
      bench_group_close(&poll_push.group);
    }

    {
      bench_pair_blocked_t blocked;
      status = bench_pair_blocked_open(&blocked, payload, sizeof(payload));
      check_equal(status, SALTS_OK);
      benchmark_ops("POLLOUT PAIR credit-exhausted", samples, 1u) {
        if (status == SALTS_OK)
          status = bench_pollout(blocked.sender, 0);
      }
      check_equal(status, SALTS_OK);
      bench_group_close(&blocked.group);
    }

    {
      bench_reqrep_t reply_ready;
      status = bench_reqrep_open(&reply_ready);
      check_equal(status, SALTS_OK);
      status = bench_reqrep_prepare_reply(&reply_ready, payload,
                                          sizeof(payload));
      check_equal(status, SALTS_OK);
      check_equal(bench_pollout(reply_ready.rep, 1), SALTS_OK);
      benchmark_ops("POLLOUT REP reply-peer ready", samples, 1u) {
        if (status == SALTS_OK)
          status = bench_pollout(reply_ready.rep, 1);
      }
      check_equal(status, SALTS_OK);
      status = bench_send_retry(&reply_ready.group, reply_ready.rep,
                                payload, sizeof(payload), 0);
      check_equal(status, SALTS_OK);
      check_equal(bench_recv_exact(&reply_ready.group, reply_ready.req,
                                   payload, sizeof(payload)), SALTS_OK);
      bench_group_close(&reply_ready.group);
    }
  }
}
