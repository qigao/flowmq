#include "flowmq_socket.h"
#include "flowmq_owner.h"
#include "flowmq_socket_external_internal.h"
#include "flowmq_peer_pool.h"
#include "flowmq_bench_metrics.h"
#include "flowmq_tls_test_material.h"
#include "tinytest.h"
#include <salts/clock.h>
#include <salts/thread.h>
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
#include <zmq.h>
#endif

#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  OWNER_PAIRS = 4,
  OWNER_MAX_THREADS = 4,
  OWNER_LANE_PAIRS = 8,
  OWNER_LANE_THREADS = 8,
  OWNER_LANE_WINDOW = 128,
  OWNER_LANE_ROUNDS = 2048,
  OWNER_LANE_WARMUPS = 8,
  OWNER_LANE_REPEATS = 8,
  OWNER_LANE_STEPS = 4,
  OWNER_PACED_PERIOD_MS = 5,
  OWNER_PACED_ROUNDS = 256,
  OWNER_PACED_REPEATS = 6,
  OWNER_WINDOW = 16,
  OWNER_SMALL_BYTES = 64,
  OWNER_LARGE_BYTES = 64 * 1024,
  OWNER_SMALL_ROUNDS = 256,
  OWNER_TCP_SMALL_ROUNDS = 4096,
  OWNER_LARGE_ROUNDS = 32,
  OWNER_TCP_LARGE_ROUNDS = 256,
  OWNER_MAX_ROUNDS = 4096,
  OWNER_REPEATS = 3,
  OWNER_MAX_REPEATS = 9,
  OWNER_CHECK_ROUNDS = 2,
  OWNER_TIMEOUT_MS = 30000,
  OWNER_ENDPOINT_BYTES = 128,
  OWNER_PERCENTILE = 99,
  OWNER_PERCENT_SCALE = 100,
  OWNER_SCALE_STEPS = 3,
  OWNER_NS_PER_US = 1000,
  OWNER_NS_PER_SECOND = 1000000000,
  OWNER_MIB = 1024 * 1024
};

typedef struct owner_tls_files_s {
  char *ca;
  char *cert;
  char *key;
} owner_tls_files_t;

typedef enum owner_engine_e {
  OWNER_ENGINE_ORDINARY = 0,
  OWNER_ENGINE_ZMQ = 1,
  OWNER_ENGINE_SHARED = 2
} owner_engine_t;

typedef struct owner_pair_s {
  flowmq_socket_t *sender;
  flowmq_socket_t *receiver;
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  void *zmq_sender;
  void *zmq_receiver;
#endif
  uint64_t sequence;
  uint64_t identity;
  uint64_t burst_started_ns;
} owner_pair_t;

typedef struct owner_run_s {
  cmeta_mutex_t mutex;
  cmeta_cond_t changed;
  atomic_int error;
  size_t ready;
  size_t done;
  int started;
  int cleanup;
  int tls;
  int peer_pool;
  int lane_scaling;
  int use_zmq;
  int shared_owner;
  uint32_t period_ms;
  int idle_wait;
  size_t payload_bytes;
  size_t rounds;
  const owner_tls_files_t *files;
} owner_run_t;

typedef struct owner_worker_s {
  owner_run_t *run;
  size_t first_pair;
  size_t pair_count;
  flowmq_ctx_t *ctx;
  flowmq_owner_t *owner;
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  void *zmq_ctx;
#endif
  owner_pair_t pairs[OWNER_LANE_PAIRS];
  unsigned char *payload;
  unsigned char *received;
  uint64_t *latencies;
  uint64_t *scheduled_latencies;
  uint64_t release_ns;
  size_t sample_count;
  size_t retries;
  size_t poll_calls;
  uint64_t max_release_lag_ns;
  uint64_t started_ns;
  uint64_t finished_ns;
  int workload_status;
  const char *phase;
  const char *operation;
} owner_worker_t;

static void owner_error(owner_run_t *run, int status) {
  int expected = SALTS_OK;
  if (status != SALTS_OK) (void)atomic_compare_exchange_strong(&run->error, &expected, status);
}

static int owner_progress(owner_worker_t *worker, owner_pair_t *pair, uint64_t deadline_ms) {
  flowmq_pollitem_t items[OWNER_LANE_PAIRS * 2u] = {0};
  size_t count = 2u;
  items[0].socket = pair->sender;
  items[1].socket = pair->receiver;
  if (worker->run->lane_scaling) {
    count = worker->pair_count * 2u;
    for (size_t i = 0u; i < worker->pair_count; ++i) {
      items[i * 2u].socket = worker->pairs[i].sender;
      items[i * 2u + 1u].socket = worker->pairs[i].receiver;
    }
  }
  size_t ready = 0u;
  int status = atomic_load_explicit(&worker->run->error, memory_order_relaxed);
  if (status != SALTS_OK) return status;
  if (cmeta_monotonic_ms() >= deadline_ms) return SALTS_ETIMEDOUT;
  worker->operation = "poll";
  ++worker->poll_calls;
  if (worker->owner != NULL)
    return flowmq_owner_poll(worker->owner, items, count, 0u, &ready);
  return flowmq_poll(items, count, 0u, &ready);
}

static int owner_socket_option(flowmq_socket_t *socket, int option, const char *value) {
  return flowmq_setsockopt(socket, option, value, strlen(value));
}

/* Fixed scheduled releases, not a sleep added after each burst. Both policies
 * offer the same work on the same cadence; late releases catch up without loss.
 * Only receivers are interesting while idle. POLLOUT would keep a healthy lane
 * awake. Other sockets and their control/credit deadlines still progress. */
static int owner_wait_release(owner_worker_t *worker, size_t round) {
  const uint64_t release = worker->started_ns +
      (uint64_t)(round + 1u) * worker->run->period_ms * 1000000u;
  worker->release_ns = release;
  flowmq_pollitem_t items[OWNER_LANE_PAIRS] = {0};
  for (size_t i = 0u; i < worker->pair_count; ++i)
    items[i] = (flowmq_pollitem_t){worker->pairs[i].receiver, FLOWMQ_POLLIN, 0};
  worker->operation = "scheduled release";
  for (;;) {
    const uint64_t now = cmeta_hrtime();
    size_t ready = 0u;
    int status = atomic_load_explicit(&worker->run->error, memory_order_relaxed);
    if (status != SALTS_OK) return status;
    if (now >= release) {
      const uint64_t lag = now - release;
      if (lag > worker->max_release_lag_ns) worker->max_release_lag_ns = lag;
      return SALTS_OK;
    }
    /* Round up the remaining fraction of a millisecond. Overshoot is recorded
     * as release lag, never excluded from the total measurement interval. */
    const uint32_t wait_ms = worker->run->idle_wait
        ? (uint32_t)((release - now + 999999u) / 1000000u) : 0u;
    ++worker->poll_calls;
    status = flowmq_owner_poll(worker->owner, items, worker->pair_count, wait_ms, &ready);
    if (status != SALTS_OK) return status;
    /* Previous round consumed every expected message. Unexpected data/errors
     * must fail, rather than turning a ready socket into an idle spin loop. */
    if (ready != 0u) return SALTS_EPROTO;
  }
}

#if defined(FLOWMQ_BENCH_WITH_ZMQ)
static int owner_zmq_pair_open(owner_worker_t *worker, owner_pair_t *pair) {
  char endpoint[OWNER_ENDPOINT_BYTES];
  size_t endpoint_size = sizeof(endpoint);
  const int hwm = OWNER_LANE_WINDOW;
  const int linger = 0;
  const int timeout = OWNER_TIMEOUT_MS;
  pair->zmq_sender = zmq_socket(worker->zmq_ctx, ZMQ_PAIR);
  pair->zmq_receiver = zmq_socket(worker->zmq_ctx, ZMQ_PAIR);
  if (pair->zmq_sender == NULL || pair->zmq_receiver == NULL) return SALTS_ENOMEM;
  void *sockets[] = {pair->zmq_sender, pair->zmq_receiver};
  for (size_t i = 0u; i < 2u; ++i) {
    if (zmq_setsockopt(sockets[i], ZMQ_LINGER, &linger, sizeof(linger)) != 0 ||
        zmq_setsockopt(sockets[i], ZMQ_SNDTIMEO, &timeout, sizeof(timeout)) != 0 ||
        zmq_setsockopt(sockets[i], ZMQ_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
        zmq_setsockopt(sockets[i], ZMQ_SNDHWM, &hwm, sizeof(hwm)) != 0 ||
        zmq_setsockopt(sockets[i], ZMQ_RCVHWM, &hwm, sizeof(hwm)) != 0)
      return SALTS_EIO;
  }
  if (zmq_bind(pair->zmq_receiver, "tcp://127.0.0.1:*") != 0 ||
      zmq_getsockopt(pair->zmq_receiver, ZMQ_LAST_ENDPOINT, endpoint, &endpoint_size) != 0 ||
      zmq_connect(pair->zmq_sender, endpoint) != 0)
    return SALTS_EIO;
  return SALTS_OK;
}
#endif

static int owner_pair_open(owner_worker_t *worker, owner_pair_t *pair) {
  const owner_run_t *run = worker->run;
  char endpoint[OWNER_ENDPOINT_BYTES];
  size_t endpoint_size = 0u;
  int hwm = run->lane_scaling ? OWNER_LANE_WINDOW : OWNER_WINDOW;
  int status;
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  if (run->use_zmq) return owner_zmq_pair_open(worker, pair);
#endif
  pair->sender = run->shared_owner
      ? flowmq_owner_socket(worker->owner, FLOWMQ_PAIR)
      : flowmq_socket(worker->ctx, FLOWMQ_PAIR);
  pair->receiver = run->shared_owner
      ? flowmq_owner_socket(worker->owner, FLOWMQ_PAIR)
      : flowmq_socket(worker->ctx, FLOWMQ_PAIR);
  if (pair->sender == NULL || pair->receiver == NULL) return SALTS_ENOMEM;
  if (run->peer_pool) {
    flowmq_peer_pool_config_t config = FLOWMQ_PEER_POOL_CONFIG_INIT;
    config.max_peers = config.max_connecting = 1u;
    status = flowmq_socket_set_peer_pool(pair->sender, &config);
    if (status == SALTS_OK)
      status = flowmq_socket_set_peer_pool(pair->receiver, &config);
    if (status != SALTS_OK) return status;
  }
  status = flowmq_setsockopt(pair->sender, FLOWMQ_SNDHWM, &hwm, sizeof(hwm));
  if (status == SALTS_OK && run->lane_scaling)
    status = flowmq_setsockopt(pair->receiver, FLOWMQ_RCVHWM, &hwm, sizeof(hwm));
  if (status == SALTS_OK && run->tls) {
    status = owner_socket_option(pair->sender, FLOWMQ_TLS_CA_FILE, run->files->ca);
    if (status == SALTS_OK)
      status = owner_socket_option(pair->sender, FLOWMQ_TLS_SERVER_NAME, "localhost");
    if (status == SALTS_OK)
      status = owner_socket_option(pair->receiver, FLOWMQ_TLS_CERT_FILE, run->files->cert);
    if (status == SALTS_OK)
      status = owner_socket_option(pair->receiver, FLOWMQ_TLS_KEY_FILE, run->files->key);
  }
  if (status == SALTS_OK) {
    /* The shared-listener path is a private qualification seam. It does not
     * change the public owner socket's bind contract. */
    status = run->shared_owner
        ? flowmq_socket_internal_bind_external(pair->receiver, "tcp://127.0.0.1:0")
        : flowmq_bind(pair->receiver, run->tls ? "tls://127.0.0.1:0" : "tcp://127.0.0.1:0");
  }
  if (status == SALTS_OK)
    status = flowmq_last_endpoint(pair->receiver, endpoint, sizeof(endpoint), &endpoint_size);
  if (status == SALTS_OK) status = flowmq_connect(pair->sender, endpoint);
  return status;
}

static void owner_payload(owner_worker_t *worker, const owner_pair_t *pair, uint64_t sequence) {
  memcpy(worker->payload, &pair->identity, sizeof(pair->identity));
  memcpy(worker->payload + sizeof(pair->identity), &sequence, sizeof(sequence));
}

/* Fixed-size bursts preserve per-connection order. Each latency spans first
 * send attempt through the final received part, including admission pressure.
 * The reused sender storage must not affect already admitted multipart data. */
static int owner_batch(owner_worker_t *worker, owner_pair_t *pair, int check_full, int measured) {
  const size_t bytes = worker->run->payload_bytes;
  const size_t part_bytes = bytes / 2u;
  const uint64_t deadline = cmeta_monotonic_ms() + OWNER_TIMEOUT_MS;
  uint64_t started[OWNER_WINDOW];
  int status;
  for (size_t message = 0u; message < OWNER_WINDOW; ++message) {
    owner_payload(worker, pair, pair->sequence + message);
    started[message] = cmeta_hrtime();
    for (size_t part = 0u; part < 2u; ++part) {
      const int flags = FLOWMQ_DONTWAIT | (part == 0u ? FLOWMQ_SNDMORE : 0);
      for (;;) {
        worker->operation = "send";
        status = flowmq_send(pair->sender, worker->payload + part * part_bytes, part_bytes, flags);
        if (status != SALTS_EBUSY && status != SALTS_ENOBUFS) break;
        ++worker->retries;
        status = owner_progress(worker, pair, deadline);
        if (status != SALTS_OK) return status;
      }
      if (status != SALTS_OK) return status;
    }
  }
  if (check_full) {
    worker->operation = "HWM rejection";
    status = flowmq_send(pair->sender, worker->payload, bytes, FLOWMQ_DONTWAIT);
    if (status != SALTS_ENOBUFS) return status == SALTS_OK ? SALTS_EPROTO : status;
  }
  for (size_t message = 0u; message < OWNER_WINDOW; ++message) {
    for (size_t part = 0u; part < 2u; ++part) {
      size_t received_size = 0u;
      int more = 0;
      size_t option_size = sizeof(more);
      for (;;) {
        worker->operation = "recv";
        status = flowmq_recv(pair->receiver, worker->received + part * part_bytes, part_bytes,
                             &received_size, FLOWMQ_DONTWAIT);
        if (status != SALTS_EBUSY) break;
        status = owner_progress(worker, pair, deadline);
        if (status != SALTS_OK) return status;
      }
      if (status != SALTS_OK) return status;
      worker->operation = "part size";
      if (received_size != part_bytes) return SALTS_EPROTO;
      worker->operation = "RCVMORE";
      status = flowmq_getsockopt(pair->receiver, FLOWMQ_RCVMORE, &more, &option_size);
      if (status != SALTS_OK) return status;
      if (more != (part == 0u ? 1 : 0)) return SALTS_EPROTO;
    }
    if (measured) worker->latencies[worker->sample_count++] = cmeta_hrtime() - started[message];
    owner_payload(worker, pair, pair->sequence + message);
    worker->operation = "payload/order";
    if (memcmp(worker->received, worker->payload, bytes) != 0) return SALTS_EPROTO;
  }
  pair->sequence += OWNER_WINDOW;
  return SALTS_OK;
}

/* Fixed total connections and per-connection window. Admit one burst on ALL
 * lane-local connections before draining them; otherwise the 1-lane control
 * would expose only one connection's window. Progress always includes every
 * local socket. No shared mutable payload or hot-path worker barrier. */
static int owner_lane_round(owner_worker_t *worker, int measured) {
  const size_t bytes = worker->run->payload_bytes;
  const uint64_t deadline = cmeta_monotonic_ms() + OWNER_TIMEOUT_MS;
  int status;
  for (size_t i = 0u; i < worker->pair_count; ++i) {
    owner_pair_t *pair = &worker->pairs[i];
    pair->burst_started_ns = cmeta_hrtime();
    for (size_t message = 0u; message < OWNER_LANE_WINDOW; ++message) {
      owner_payload(worker, pair, pair->sequence + message);
      for (;;) {
        worker->operation = "lane send";
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
        if (worker->run->use_zmq) {
          const int sent = zmq_send(pair->zmq_sender, worker->payload, bytes, 0);
          status = sent == (int)bytes ? SALTS_OK
              : sent < 0 && zmq_errno() == EAGAIN ? SALTS_ETIMEDOUT : SALTS_EIO;
        } else
#endif
          status = flowmq_send(pair->sender, worker->payload, bytes, FLOWMQ_DONTWAIT);
        if (status != SALTS_EBUSY && status != SALTS_ENOBUFS) break;
        ++worker->retries;
        status = owner_progress(worker, pair, deadline);
        if (status != SALTS_OK) return status;
      }
      if (status != SALTS_OK) return status;
    }
  }
  for (size_t i = 0u; i < worker->pair_count; ++i) {
    owner_pair_t *pair = &worker->pairs[i];
    for (size_t message = 0u; message < OWNER_LANE_WINDOW; ++message) {
      size_t received_size = 0u;
      for (;;) {
        worker->operation = "lane receive";
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
        if (worker->run->use_zmq) {
          const int received = zmq_recv(pair->zmq_receiver, worker->received, bytes, 0);
          status = received >= 0 ? SALTS_OK
              : zmq_errno() == EAGAIN ? SALTS_ETIMEDOUT : SALTS_EIO;
          if (received >= 0) received_size = (size_t)received;
        } else
#endif
          status = flowmq_recv(pair->receiver, worker->received, bytes,
                               &received_size, FLOWMQ_DONTWAIT);
        if (status != SALTS_EBUSY) break;
        status = owner_progress(worker, pair, deadline);
        if (status != SALTS_OK) return status;
      }
      if (status != SALTS_OK) return status;
      owner_payload(worker, pair, pair->sequence + message);
      worker->operation = "lane payload/order";
      if (received_size != bytes || memcmp(worker->received, worker->payload, bytes) != 0)
        return SALTS_EPROTO;
    }
    if (measured) {
      const uint64_t completed_ns = cmeta_hrtime();
      if (worker->scheduled_latencies != NULL)
        worker->scheduled_latencies[worker->sample_count] = completed_ns - worker->release_ns;
      worker->latencies[worker->sample_count++] = completed_ns - pair->burst_started_ns;
    }
    pair->sequence += OWNER_LANE_WINDOW;
  }
  /* libzmq's I/O thread drives transport and credit in the background. */
  if (worker->run->use_zmq) return SALTS_OK;
  /* Return credit on every connection before the next fixed-window round. */
  status = owner_progress(worker, &worker->pairs[0], deadline);
  if (status == SALTS_OK) status = owner_progress(worker, &worker->pairs[0], deadline);
  return status;
}

static void owner_worker_close(owner_worker_t *worker) {
  for (size_t i = 0u; i < worker->pair_count; ++i) {
    if (worker->pairs[i].sender != NULL)
      owner_error(worker->run, worker->owner != NULL
          ? flowmq_owner_close_socket(worker->owner, worker->pairs[i].sender)
          : flowmq_close(worker->pairs[i].sender));
    if (worker->pairs[i].receiver != NULL)
      owner_error(worker->run, worker->owner != NULL
          ? flowmq_owner_close_socket(worker->owner, worker->pairs[i].receiver)
          : flowmq_close(worker->pairs[i].receiver));
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
    if (worker->pairs[i].zmq_sender != NULL && zmq_close(worker->pairs[i].zmq_sender) != 0)
      owner_error(worker->run, SALTS_EIO);
    if (worker->pairs[i].zmq_receiver != NULL && zmq_close(worker->pairs[i].zmq_receiver) != 0)
      owner_error(worker->run, SALTS_EIO);
#endif
  }
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  if (worker->zmq_ctx != NULL && zmq_ctx_term(worker->zmq_ctx) != 0)
    owner_error(worker->run, SALTS_EIO);
#endif
  if (worker->owner != NULL) owner_error(worker->run, flowmq_owner_term(worker->owner));
  if (worker->ctx != NULL) owner_error(worker->run, flowmq_ctx_term(worker->ctx));
  free(worker->payload);
  free(worker->received);
}

/* Observe the real session leases outside measurement. The workload keeps
 * exactly one fully READY pipe per socket; HWM remains a separate budget. */
static int owner_pool_check(owner_worker_t *worker, owner_pair_t *pair) {
  if (worker->run->use_zmq) return SALTS_OK;
  flowmq_socket_t *sockets[] = {pair->sender, pair->receiver};
  for (size_t i = 0u; i < sizeof(sockets) / sizeof(sockets[0]); ++i) {
    flowmq_peer_pool_snapshot_t pool = FLOWMQ_PEER_POOL_SNAPSHOT_INIT;
    int status = flowmq_socket_get_peer_pool(sockets[i], &pool);
    if (status != SALTS_OK) return status;
    if (pool.enabled != worker->run->peer_pool) return SALTS_EPROTO;
    if (pool.enabled && (pool.ready != 1u || pool.active_leases != 1u ||
                         pool.physical_in_use != 1u || pool.connecting != 0u ||
                         pool.draining != 0u || pool.sealed)) return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static void owner_worker_entry(void *arg) {
  owner_worker_t *worker = arg;
  owner_run_t *run = worker->run;
  int status = SALTS_OK;
  worker->phase = "setup";
  worker->operation = "init";
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  if (run->use_zmq) {
    worker->zmq_ctx = zmq_ctx_new();
    if (worker->zmq_ctx == NULL) status = SALTS_ENOMEM;
    else if (zmq_ctx_set(worker->zmq_ctx, ZMQ_IO_THREADS, 1) != 0 ||
             zmq_ctx_get(worker->zmq_ctx, ZMQ_IO_THREADS) != 1) status = SALTS_EIO;
  } else
#endif
  {
    worker->ctx = flowmq_ctx_new();
    if (worker->ctx == NULL) status = SALTS_ENOMEM;
    if (status == SALTS_OK && run->shared_owner) {
      flowmq_owner_config_t config = FLOWMQ_OWNER_CONFIG_INIT;
      config.socket_capacity = worker->pair_count * 2u;
      worker->owner = flowmq_owner_new(worker->ctx, &config);
      if (worker->owner == NULL) status = SALTS_ENOMEM;
    }
  }
  worker->payload = malloc(run->payload_bytes);
  worker->received = malloc(run->payload_bytes);
  if (worker->payload == NULL || worker->received == NULL)
    status = SALTS_ENOMEM;
  if (status == SALTS_OK) memset(worker->payload, 0xa5, run->payload_bytes);
  for (size_t i = 0u; status == SALTS_OK && i < worker->pair_count; ++i) {
    worker->pairs[i].identity = worker->first_pair + i;
    status = owner_pair_open(worker, &worker->pairs[i]);
    worker->phase = "warmup";
    if (status == SALTS_OK && !run->lane_scaling) status = owner_batch(worker, &worker->pairs[i], 1, 0);
    if (status == SALTS_OK && !run->lane_scaling) status = owner_batch(worker, &worker->pairs[i], 0, 0);
    if (status == SALTS_OK && !run->lane_scaling) status = owner_pool_check(worker, &worker->pairs[i]);
  }
  if (run->lane_scaling) {
    for (size_t warmup = 0u; status == SALTS_OK && warmup < OWNER_LANE_WARMUPS; ++warmup)
      status = owner_lane_round(worker, 0);
    for (size_t i = 0u; status == SALTS_OK && i < worker->pair_count; ++i)
      status = owner_pool_check(worker, &worker->pairs[i]);
  }
  owner_error(run, status);
  cmeta_mutex_lock(&run->mutex);
  ++run->ready;
  cmeta_cond_broadcast(&run->changed);
  while (!run->started)
    cmeta_cond_wait(&run->changed, &run->mutex);
  cmeta_mutex_unlock(&run->mutex);

  worker->retries = 0u;
  worker->poll_calls = 0u;
  worker->started_ns = cmeta_hrtime();
  if (status == SALTS_OK) worker->phase = "measured";
  for (size_t round = 0u; status == SALTS_OK && round < run->rounds; ++round) {
    status = atomic_load_explicit(&run->error, memory_order_relaxed);
    if (status == SALTS_OK && run->period_ms != 0u)
      status = owner_wait_release(worker, round);
    if (status == SALTS_OK && run->lane_scaling) status = owner_lane_round(worker, 1);
    if (!run->lane_scaling)
      for (size_t i = 0u; status == SALTS_OK && i < worker->pair_count; ++i)
        status = owner_batch(worker, &worker->pairs[i], 0, 1);
  }
  worker->finished_ns = cmeta_hrtime();
  worker->workload_status = status;
  owner_error(run, status);
  cmeta_mutex_lock(&run->mutex);
  ++run->done;
  cmeta_cond_broadcast(&run->changed);
  /* Keep completed workers asleep until measurement ends: spinning or freeing
   * their sockets here would contaminate other owners' CPU and wall time. */
  while (!run->cleanup)
    cmeta_cond_wait(&run->changed, &run->mutex);
  cmeta_mutex_unlock(&run->mutex);
  if (status == SALTS_OK) {
    for (size_t i = 0u; i < worker->pair_count; ++i)
      owner_error(run, owner_pool_check(worker, &worker->pairs[i]));
  }
  owner_worker_close(worker);
}

static int owner_compare_ns(const void *left, const void *right) {
  const uint64_t a = *(const uint64_t *)left;
  const uint64_t b = *(const uint64_t *)right;
  return (a > b) - (a < b);
}

static int owner_run_policy(size_t owners, size_t payload_bytes, size_t rounds, int tls,
                     const owner_tls_files_t *files, int measured, size_t repetition,
                     int peer_pool, int lane_scaling, size_t order, owner_engine_t engine,
                     size_t engine_order, uint32_t period_ms, int idle_wait) {
  owner_run_t run = {0};
  owner_worker_t workers[OWNER_LANE_THREADS] = {0};
  cmeta_thread_t threads[OWNER_LANE_THREADS] = {0};
  const size_t pairs = lane_scaling ? OWNER_LANE_PAIRS : OWNER_PAIRS;
  const size_t window = lane_scaling ? OWNER_LANE_WINDOW : OWNER_WINDOW;
  size_t messages, expected_samples;
  uint64_t *latencies;
  uint64_t *scheduled_latencies = NULL;
  flowmq_bench_metrics_t before = {0}, after = {0};
  uint64_t started_ns = 0u, finished_ns = 0u;
  size_t created = 0u, samples = 0u, retries = 0u;
  int status = SALTS_OK;
  const int use_zmq = engine == OWNER_ENGINE_ZMQ;
  const int shared_owner = engine == OWNER_ENGINE_SHARED;
  const char *engine_name = use_zmq ? "libzmq" : shared_owner ? "flowmq_shared" : "flowmq";
  if (engine != OWNER_ENGINE_ORDINARY && engine != OWNER_ENGINE_ZMQ &&
      engine != OWNER_ENGINE_SHARED) return SALTS_EINVAL;
#if !defined(FLOWMQ_BENCH_WITH_ZMQ)
  if (use_zmq) return SALTS_ENOTSUP;
#endif
  if (use_zmq && (!lane_scaling || tls || peer_pool)) return SALTS_EINVAL;
  if (shared_owner && (!lane_scaling || tls)) return SALTS_EINVAL;
  if (period_ms != 0u && (!shared_owner || period_ms > OWNER_TIMEOUT_MS)) return SALTS_EINVAL;
  if (owners == 0u || owners > OWNER_LANE_THREADS || pairs % owners != 0u ||
      rounds == 0u || rounds > OWNER_MAX_ROUNDS || payload_bytes < 2u * sizeof(uint64_t))
    return SALTS_EINVAL;
  if (rounds > SIZE_MAX / pairs / window) return SALTS_ERANGE;
  messages = pairs * rounds * window;
  expected_samples = lane_scaling ? pairs * rounds : messages;
  if (messages > SIZE_MAX / payload_bytes || expected_samples > SIZE_MAX / sizeof(*latencies))
    return SALTS_ERANGE;
  latencies = calloc(expected_samples, sizeof(*latencies));
  if (latencies == NULL) return SALTS_ENOMEM;
  if (period_ms != 0u) {
    scheduled_latencies = calloc(expected_samples, sizeof(*scheduled_latencies));
    if (scheduled_latencies == NULL) {
      free(latencies);
      return SALTS_ENOMEM;
    }
  }
  atomic_init(&run.error, SALTS_OK);
  cmeta_mutex_init(&run.mutex);
  cmeta_cond_init(&run.changed);
  run.tls = tls;
  run.peer_pool = peer_pool;
  run.lane_scaling = lane_scaling;
  run.use_zmq = use_zmq;
  run.shared_owner = shared_owner;
  run.period_ms = period_ms;
  run.idle_wait = idle_wait;
  run.payload_bytes = payload_bytes;
  run.rounds = rounds;
  run.files = files;
  for (size_t i = 0u; i < owners; ++i) {
    workers[i].run = &run;
    workers[i].first_pair = i * (pairs / owners);
    workers[i].pair_count = pairs / owners;
    workers[i].latencies = latencies + i * (expected_samples / owners);
    if (scheduled_latencies != NULL)
      workers[i].scheduled_latencies = scheduled_latencies + i * (expected_samples / owners);
    status = cmeta_thread_create(&threads[i], owner_worker_entry, &workers[i]);
    if (status != SALTS_OK) break;
    ++created;
  }
  owner_error(&run, status);
  cmeta_mutex_lock(&run.mutex);
  while (run.ready != created)
    cmeta_cond_wait(&run.changed, &run.mutex);
  cmeta_mutex_unlock(&run.mutex);
  owner_error(&run, flowmq_bench_metrics_read(&before));
  /* TinyTest reports the wall cost of the parallel workload once. The CSV
   * interval ends at the last worker, excluding controller wake latency. */
  benchmark_io("fixed-total owner workload", 1u, messages, messages * payload_bytes) {
    cmeta_mutex_lock(&run.mutex);
    started_ns = cmeta_hrtime();
    run.started = 1;
    cmeta_cond_broadcast(&run.changed);
    while (run.done != created)
      cmeta_cond_wait(&run.changed, &run.mutex);
    cmeta_mutex_unlock(&run.mutex);
    /* Do not print a nominal full-work throughput for a failed/short run, or
     * performance numbers for the deliberately tiny correctness workload. */
    if (!measured || atomic_load(&run.error) != SALTS_OK) goto measurement_finished;
  }
measurement_finished:
  owner_error(&run, flowmq_bench_metrics_read(&after));
  cmeta_mutex_lock(&run.mutex);
  run.cleanup = 1;
  cmeta_cond_broadcast(&run.changed);
  cmeta_mutex_unlock(&run.mutex);
  for (size_t i = 0u; i < created; ++i) {
    status = cmeta_thread_join(&threads[i]);
    /* A failed join cannot establish that stack-owned synchronization and
     * worker storage are quiescent; terminating this test process is required. */
    if (status != SALTS_OK) {
      fprintf(stderr, "owner join failed: %d\n", status);
      abort();
    }
    if (workers[i].finished_ns > finished_ns) finished_ns = workers[i].finished_ns;
    samples += workers[i].sample_count;
    retries += workers[i].retries;
  }
  status = atomic_load(&run.error);
  if (status != SALTS_OK) {
    fprintf(stderr, "OWNER_FAILED_CASE,transport=%s,bytes=%zu,owners=%zu,repeat=%zu,pool=%d,engine=%s\n",
            tls ? "tls" : "tcp", payload_bytes, owners, repetition, peer_pool, engine_name);
    for (size_t i = 0u; i < created; ++i)
      fprintf(stderr,
              "OWNER_ERROR,owner=%zu,phase=%s,operation=%s,samples=%zu,"
              "workload_status=%d,first_error=%d\n",
              i, workers[i].phase, workers[i].operation, workers[i].sample_count,
              workers[i].workload_status, status);
  }
  if (status == SALTS_OK && (samples != expected_samples || finished_ns <= started_ns))
    status = SALTS_EPROTO;
  if (status == SALTS_OK && measured) {
    const double seconds = (double)(finished_ns - started_ns) / OWNER_NS_PER_SECOND;
    const double cpu_seconds = (double)(after.cpu_ns - before.cpu_ns) / OWNER_NS_PER_SECOND;
    const size_t p99 =
        (samples * OWNER_PERCENTILE + OWNER_PERCENT_SCALE - 1u) / OWNER_PERCENT_SCALE - 1u;
    if (lane_scaling) {
      size_t polls = 0u;
      uint64_t max_release_lag_ns = 0u;
      for (size_t i = 0u; i < created; ++i) {
        owner_worker_t *worker = &workers[i];
        if (worker->sample_count != expected_samples / owners) {
          status = SALTS_EPROTO;
          break;
        }
        qsort(worker->latencies, worker->sample_count, sizeof(*latencies), owner_compare_ns);
        const size_t lane_p99 =
            (worker->sample_count * OWNER_PERCENTILE + OWNER_PERCENT_SCALE - 1u) /
                OWNER_PERCENT_SCALE - 1u;
        if (measured == 3)
          printf("LANE_CPU_WORKER,%s,%u,%zu,", idle_wait ? "wait" : "spin", period_ms, engine_order);
        else if (measured == 2)
          printf("LANE_COMPARE_WORKER,%s,%zu,", engine_name, engine_order);
        else printf("LANE_WORKER,");
        printf("%zu,%zu,%zu,%zu,%zu,%zu,%zu,%llu,%llu,%llu,%zu,%zu\n",
               payload_bytes, owners, repetition, order, i, worker->pair_count,
               worker->sample_count * window,
               (unsigned long long)(worker->started_ns - started_ns),
               (unsigned long long)(worker->finished_ns - started_ns),
               (unsigned long long)worker->latencies[lane_p99], worker->poll_calls, worker->retries);
        polls += worker->poll_calls;
        if (worker->max_release_lag_ns > max_release_lag_ns)
          max_release_lag_ns = worker->max_release_lag_ns;
      }
      if (status == SALTS_OK) {
        qsort(latencies, samples, sizeof(*latencies), owner_compare_ns);
        if (measured == 3) {
          qsort(scheduled_latencies, samples, sizeof(*scheduled_latencies), owner_compare_ns);
          printf("LANE_CPU_RESULT,%s,%u,%zu,%llu,%llu,%d,%.3f,", idle_wait ? "wait" : "spin",
                 period_ms, engine_order, (unsigned long long)max_release_lag_ns,
                 (unsigned long long)scheduled_latencies[p99], after.cpu_cycles_available,
                 (double)(after.cpu_cycles - before.cpu_cycles) / messages);
        }
        else if (measured == 2)
          printf("LANE_COMPARE_RESULT,%s,%zu,", engine_name, engine_order);
        else printf("LANE_RESULT,");
        printf("%zu,%zu,%zu,%zu,%zu,%zu,%zu,%llu,%.3f,%.3f,%llu,%llu,%zu,%zu,%llu\n",
               payload_bytes, owners, repetition, order, pairs, window, messages,
               (unsigned long long)(finished_ns - started_ns), (double)messages / seconds,
               (double)(after.cpu_ns - before.cpu_ns) / messages,
               (unsigned long long)latencies[(samples * 95u + 99u) / 100u - 1u],
               (unsigned long long)latencies[p99], polls, retries,
               (unsigned long long)after.peak_rss_bytes);
      }
    } else {
      qsort(latencies, samples, sizeof(*latencies), owner_compare_ns);
      printf("OWNER_RESULT,%s,%zu,%zu,%zu,%zu,%.6f,%.0f,%.2f,%.2f,%.3f,%.2f,%zu,%d\n",
             tls ? "tls" : "tcp", payload_bytes, owners, repetition, messages, seconds,
             (double)messages / seconds, (double)messages * payload_bytes / seconds / OWNER_MIB,
             (double)latencies[p99] / OWNER_NS_PER_US, cpu_seconds,
             (double)after.peak_rss_bytes / OWNER_MIB, retries, peer_pool);
    }
  }
  cmeta_cond_destroy(&run.changed);
  cmeta_mutex_destroy(&run.mutex);
  free(latencies);
  free(scheduled_latencies);
  return status;
}

static int owner_run_configured(size_t owners, size_t payload_bytes, size_t rounds, int tls,
                     const owner_tls_files_t *files, int measured, size_t repetition,
                     int peer_pool, int lane_scaling, size_t order, owner_engine_t engine,
                     size_t engine_order) {
  return owner_run_policy(owners, payload_bytes, rounds, tls, files, measured, repetition,
                          peer_pool, lane_scaling, order, engine, engine_order, 0u, 0);
}

static int owner_run(size_t owners, size_t payload_bytes, size_t rounds, int tls,
                     const owner_tls_files_t *files, int measured, size_t repetition,
                     int peer_pool) {
  return owner_run_configured(owners, payload_bytes, rounds, tls, files, measured,
                               repetition, peer_pool, 0, 0u, OWNER_ENGINE_ORDINARY, 0u);
}

static int owner_env_count(const char *name, size_t default_value, size_t maximum, size_t *out) {
  const char *value = getenv(name);
  char *end = NULL;
  unsigned long parsed;
  *out = default_value;
  if (value == NULL) return SALTS_OK;
  if (*value < '0' || *value > '9') return SALTS_EINVAL;
  errno = 0;
  parsed = strtoul(value, &end, 10);
  if (errno != 0 || *end != '\0' || parsed == 0u || parsed > maximum) return SALTS_EINVAL;
  *out = (size_t)parsed;
  return SALTS_OK;
}

static void owner_scaling(int tls, size_t bytes, const owner_tls_files_t *files,
                           int compare_pool) {
  const size_t default_rounds = bytes == OWNER_SMALL_BYTES
                                    ? (tls ? OWNER_SMALL_ROUNDS : OWNER_TCP_SMALL_ROUNDS)
                                    : (tls ? OWNER_LARGE_ROUNDS : OWNER_TCP_LARGE_ROUNDS);
  size_t rounds, repeats;
  check_equal(owner_env_count("FLOWMQ_OWNER_BENCH_REPEATS", OWNER_REPEATS,
                              OWNER_MAX_REPEATS, &repeats), SALTS_OK);
  check_equal(owner_env_count("FLOWMQ_OWNER_BENCH_ROUNDS", default_rounds,
                              OWNER_MAX_ROUNDS, &rounds), SALTS_OK);
  printf("OWNER_CONFIG,pairs=%d,window=%d,parts=2,logical_cpus=%d,affinity=unbound,"
         "pool_comparison=%d,rounds=%zu,repeats=%zu\n",
         OWNER_PAIRS, OWNER_WINDOW, cmeta_cpu_count(), compare_pool, rounds, repeats);
  printf("OWNER_COLUMNS,transport,payload_bytes,owners,repeat,messages,seconds,"
         "messages_per_second,MiB_per_second,p99_us,process_cpu_seconds,"
         "process_lifetime_peak_RSS_MiB,admission_retries,pool_enabled\n");
  for (size_t repeat = 0u; repeat < repeats; ++repeat) {
    for (size_t lane = 0u; lane < OWNER_SCALE_STEPS; ++lane) {
      /* Rotate owner count and alternate paired pool modes to reduce order
       * bias. Total pairs, messages, payloads and HWM are identical. */
      const size_t owners = (size_t)1u << ((lane + repeat) % OWNER_SCALE_STEPS);
      for (size_t mode = 0u; mode < (compare_pool ? 2u : 1u); ++mode) {
        const int pool = compare_pool ? (int)((repeat + mode) % 2u) : 0;
        check_equal(owner_run(owners, bytes, rounds, tls, files, 1, repeat, pool), SALTS_OK);
      }
    }
  }
}

static void owner_lane_compare_header(void) {
  printf("LANE_COMPARE_HEADER,engine,engine_order,payload_bytes,lanes,repeat,order,connections,"
         "batch,messages,wall_ns,messages_per_second,cpu_ns_per_message,burst_p95_ns,"
         "burst_p99_ns,poll_calls,send_retries,process_peak_rss_bytes\n");
  printf("LANE_COMPARE_WORKER_HEADER,engine,engine_order,payload_bytes,lanes,repeat,order,lane,"
         "connections,messages,start_offset_ns,finish_offset_ns,burst_p99_ns,poll_calls,send_retries\n");
}

spec("FlowMQ independent owners") {
  static owner_tls_files_t files;

  before_each() {
    files.ca = tt_make_temp_file("flowmq-owner-ca", ".pem");
    files.cert = tt_make_temp_file("flowmq-owner-cert", ".pem");
    files.key = tt_make_temp_file("flowmq-owner-key", ".pem");
    check_not_null(files.ca);
    check_not_null(files.cert);
    check_not_null(files.key);
    check_equal(tt_write_file(files.ca, FLOWMQ_TLS_TEST_ROOT_CA, sizeof(FLOWMQ_TLS_TEST_ROOT_CA) - 1u), 0);
    check_equal(tt_write_file(files.cert, FLOWMQ_TLS_TEST_CERTIFICATE,
                              sizeof(FLOWMQ_TLS_TEST_CERTIFICATE) - 1u),
                0);
    check_equal(tt_write_file(files.key, FLOWMQ_TLS_TEST_KEY, sizeof(FLOWMQ_TLS_TEST_KEY) - 1u), 0);
  }

  after_each() {
    if (files.ca != NULL) {
      check_equal_warn(tt_remove_file(files.ca), 0);
      free(files.ca);
    }
    if (files.cert != NULL) {
      check_equal_warn(tt_remove_file(files.cert), 0);
      free(files.cert);
    }
    if (files.key != NULL) {
      check_equal_warn(tt_remove_file(files.key), 0);
      free(files.key);
    }
    memset(&files, 0, sizeof(files));
  }

  it("correctness: concurrent multipart order, HWM recovery and owner shutdown") {
    const size_t sizes[] = {OWNER_SMALL_BYTES, OWNER_LARGE_BYTES};
    for (int tls = 0; tls <= 1; ++tls)
      for (size_t size = 0u; size < sizeof(sizes) / sizeof(sizes[0]); ++size)
        for (size_t owners = 1u; owners <= OWNER_MAX_THREADS; owners *= 2u) {
          for (int pool = 0; pool <= 1; ++pool)
            check_equal(owner_run(owners, sizes[size], OWNER_CHECK_ROUNDS, tls,
                                  &files, 0, 0u, pool), SALTS_OK);
        }
  }

  it("correctness: eight fixed connections preserve copy bursts across 1/2/4/8 lanes") {
    const size_t sizes[] = {64u, 1024u};
    for (size_t size = 0u; size < 2u; ++size)
      for (size_t owners = 1u; owners <= OWNER_LANE_THREADS; owners *= 2u)
        check_equal(owner_run_configured(owners, sizes[size], OWNER_CHECK_ROUNDS,
                                          0, &files, 0, 0u, 0, 1, 0u,
                                          OWNER_ENGINE_ORDINARY, 0u), SALTS_OK);
  }

  it("correctness: shared owner lanes preserve fixed bursts and drain on their own threads") {
    const size_t sizes[] = {64u, 1024u};
    for (size_t size = 0u; size < 2u; ++size)
      for (size_t owners = 1u; owners <= OWNER_LANE_THREADS; owners *= 2u)
        for (int pool = 0; pool <= 1; ++pool)
          check_equal(owner_run_configured(owners, sizes[size], OWNER_CHECK_ROUNDS,
                                            0, &files, 0, 0u, pool, 1, 0u,
                                            OWNER_ENGINE_SHARED, 0u), SALTS_OK);
  }

  it("correctness: paced shared lanes preserve content and order across idle waits") {
    const size_t sizes[] = {64u, 1024u};
    for (size_t size = 0u; size < 2u; ++size)
      for (size_t owners = 1u; owners <= OWNER_LANE_THREADS; owners *= OWNER_LANE_THREADS)
        for (int wait = 0; wait <= 1; ++wait)
          check_equal(owner_run_policy(owners, sizes[size], OWNER_CHECK_ROUNDS, 0, &files,
                                        0, 0u, 0, 1, 0u, OWNER_ENGINE_SHARED, 0u,
                                        OWNER_PACED_PERIOD_MS, wait), SALTS_OK);
  }

  bench("paced lanes: shared owner idle spin versus deadline wait") {
    const size_t sizes[] = {64u, 1024u};
    printf("LANE_CPU_CONFIG,pairs=%d,batch=%d,period_ms=%d,rounds=%d,repeats=%d,"
           "active_poll_timeout_ms=0,release=fixed-schedule,logical_cpus=%d,affinity=unbound\n",
           OWNER_LANE_PAIRS, OWNER_LANE_WINDOW, OWNER_PACED_PERIOD_MS,
           OWNER_PACED_ROUNDS, OWNER_PACED_REPEATS, cmeta_cpu_count());
    printf("LANE_CPU_HEADER,policy,period_ms,policy_order,max_release_lag_ns,"
           "scheduled_completion_p99_ns,cpu_cycles_available,cpu_cycles_per_message,payload_bytes,"
           "lanes,repeat,order,connections,batch,messages,wall_ns,messages_per_second,"
           "cpu_ns_per_message,burst_p95_ns,burst_p99_ns,poll_calls,send_retries,process_peak_rss_bytes\n");
    printf("LANE_CPU_WORKER_HEADER,policy,period_ms,policy_order,payload_bytes,lanes,repeat,"
           "order,lane,connections,messages,start_offset_ns,finish_offset_ns,burst_p99_ns,"
           "poll_calls,send_retries\n");
    for (size_t repeat = 0u; repeat < OWNER_PACED_REPEATS; ++repeat)
      for (size_t size = 0u; size < 2u; ++size)
        for (size_t order = 0u; order < 2u; ++order) {
          const size_t owners = (order + repeat) % 2u == 0u ? 1u : OWNER_LANE_THREADS;
          for (size_t policy = 0u; policy < 2u; ++policy)
            check_equal(owner_run_policy(owners, sizes[(size + repeat) % 2u],
                                          OWNER_PACED_ROUNDS, 0, &files, 3, repeat + 1u,
                                          0, 1, order, OWNER_ENGINE_SHARED, policy,
                                          OWNER_PACED_PERIOD_MS, (int)((repeat + policy) % 2u)),
                        SALTS_OK);
        }
  }

  bench("shared lanes: ordinary and shared owners on independent threads") {
    const size_t sizes[] = {64u, 1024u};
    size_t rounds, repeats;
    check_equal(owner_env_count("FLOWMQ_LANE_BENCH_ROUNDS", OWNER_LANE_ROUNDS,
                                OWNER_MAX_ROUNDS, &rounds), SALTS_OK);
    check_equal(owner_env_count("FLOWMQ_LANE_BENCH_REPEATS", OWNER_LANE_REPEATS,
                                OWNER_MAX_REPEATS, &repeats), SALTS_OK);
    printf("LANE_COMPARE_CONFIG,model=ordinary-vs-shared,threads_per_lane=1,"
           "contexts_per_lane=1,shared_owners_per_lane=1,shared_socket_capacity=2*pairs_per_lane,"
           "private_tcp_listener=1,pairs=%d,window=%d,send_receive_hwm=%d,parts=1,"
           "logical_cpus=%d,affinity=unbound,rounds=%zu,repeats=%zu\n",
           OWNER_LANE_PAIRS, OWNER_LANE_WINDOW, OWNER_LANE_WINDOW,
           cmeta_cpu_count(), rounds, repeats);
    owner_lane_compare_header();
    for (size_t repeat = 0u; repeat < repeats; ++repeat)
      for (size_t size = 0u; size < 2u; ++size) {
        const size_t bytes = sizes[(size + repeat) % 2u];
        for (size_t order = 0u; order < OWNER_LANE_STEPS; ++order) {
          const size_t owners = (size_t)1u << ((order + repeat) % OWNER_LANE_STEPS);
          /* Eight repeats balance both engine positions at each lane-order
           * position. Both domains execute the same data and credit loop. */
          for (size_t engine_order = 0u; engine_order < 2u; ++engine_order) {
            const int shared = (int)((repeat / OWNER_LANE_STEPS + order + engine_order) % 2u);
            check_equal(owner_run_configured(owners, bytes, rounds, 0, &files, 2,
                                              repeat + 1u, 0, 1, order,
                                              shared ? OWNER_ENGINE_SHARED : OWNER_ENGINE_ORDINARY,
                                              engine_order), SALTS_OK);
          }
        }
      }
  }

  bench("lane scaling: fixed eight TCP connections and 128-message copy bursts") {
    const size_t sizes[] = {64u, 1024u};
    size_t rounds, repeats;
    check_equal(owner_env_count("FLOWMQ_LANE_BENCH_ROUNDS", OWNER_LANE_ROUNDS,
                                OWNER_MAX_ROUNDS, &rounds), SALTS_OK);
    check_equal(owner_env_count("FLOWMQ_LANE_BENCH_REPEATS", OWNER_LANE_REPEATS,
                                OWNER_MAX_REPEATS, &repeats), SALTS_OK);
    printf("LANE_CONFIG,model=ordinary-socket-fixed-owner,pairs=%d,window=%d,parts=1,"
           "logical_cpus=%d,affinity=unbound,rounds=%zu,repeats=%zu\n",
           OWNER_LANE_PAIRS, OWNER_LANE_WINDOW, cmeta_cpu_count(), rounds, repeats);
    printf("LANE_HEADER,payload_bytes,lanes,repeat,order,connections,batch,messages,wall_ns,"
           "messages_per_second,cpu_ns_per_message,burst_p95_ns,burst_p99_ns,poll_calls,"
           "send_retries,process_peak_rss_bytes\n");
    printf("LANE_WORKER_HEADER,payload_bytes,lanes,repeat,order,lane,connections,messages,"
           "start_offset_ns,finish_offset_ns,burst_p99_ns,poll_calls,send_retries\n");
    for (size_t repeat = 0u; repeat < repeats; ++repeat)
      for (size_t size = 0u; size < 2u; ++size) {
        const size_t bytes = sizes[(size + repeat) % 2u];
        for (size_t order = 0u; order < OWNER_LANE_STEPS; ++order) {
          const size_t owners = (size_t)1u << ((order + repeat) % OWNER_LANE_STEPS);
          check_equal(owner_run_configured(owners, bytes, rounds, 0, &files, 1,
                                            repeat + 1u, 0, 1, order,
                                            OWNER_ENGINE_ORDINARY, 0u), SALTS_OK);
        }
      }
  }

#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  it("correctness: libzmq preserves the same eight-connection lane workload") {
    const size_t sizes[] = {64u, 1024u};
    for (size_t size = 0u; size < 2u; ++size)
      for (size_t owners = 1u; owners <= OWNER_LANE_THREADS; owners *= 2u)
        check_equal(owner_run_configured(owners, sizes[size], OWNER_CHECK_ROUNDS,
                                          0, &files, 0, 0u, 0, 1, 0u,
                                          OWNER_ENGINE_ZMQ, 0u), SALTS_OK);
  }

  bench("lane comparison: FlowMQ and libzmq fixed eight-connection bursts") {
    const size_t sizes[] = {64u, 1024u};
    size_t rounds, repeats;
    int major, minor, patch;
    check_equal(owner_env_count("FLOWMQ_LANE_BENCH_ROUNDS", OWNER_LANE_ROUNDS,
                                OWNER_MAX_ROUNDS, &rounds), SALTS_OK);
    check_equal(owner_env_count("FLOWMQ_LANE_BENCH_REPEATS", OWNER_LANE_REPEATS,
                                OWNER_MAX_REPEATS, &repeats), SALTS_OK);
    zmq_version(&major, &minor, &patch);
    printf("LANE_COMPARE_CONFIG,libzmq=%d.%d.%d,contexts_per_lane=1,zmq_io_threads_per_context=1,"
           "pairs=%d,window=%d,send_receive_hwm=%d,parts=1,logical_cpus=%d,affinity=unbound,"
           "rounds=%zu,repeats=%zu\n", major, minor, patch, OWNER_LANE_PAIRS,
           OWNER_LANE_WINDOW, OWNER_LANE_WINDOW, cmeta_cpu_count(), rounds, repeats);
    owner_lane_compare_header();
    for (size_t repeat = 0u; repeat < repeats; ++repeat)
      for (size_t size = 0u; size < 2u; ++size) {
        const size_t bytes = sizes[(size + repeat) % 2u];
        for (size_t order = 0u; order < OWNER_LANE_STEPS; ++order) {
          const size_t owners = (size_t)1u << ((order + repeat) % OWNER_LANE_STEPS);
          /* Each engine leads once at each lane-order position over 8 repeats. */
          for (size_t engine_order = 0u; engine_order < 2u; ++engine_order) {
            const int use_zmq = (int)((repeat / OWNER_LANE_STEPS + order + engine_order) % 2u);
            check_equal(owner_run_configured(owners, bytes, rounds, 0, &files, 2,
                                              repeat + 1u, 0, 1, order,
                                              use_zmq ? OWNER_ENGINE_ZMQ : OWNER_ENGINE_ORDINARY,
                                              engine_order), SALTS_OK);
          }
        }
      }
  }
#endif

  for (int tls = 0; tls <= 1; ++tls) {
    const size_t sizes[] = {OWNER_SMALL_BYTES, OWNER_LARGE_BYTES};
    for (size_t size = 0u; size < sizeof(sizes) / sizeof(sizes[0]); ++size) {
      bench("scaling: %s %zu-byte fixed-total workload", tls ? "tls" : "tcp", sizes[size]) {
        owner_scaling(tls, sizes[size], &files, 0);
      }
      bench("peer pool: %s %zu-byte fixed-total workload", tls ? "tls" : "tcp", sizes[size]) {
        owner_scaling(tls, sizes[size], &files, 1);
      }
    }
  }
}
