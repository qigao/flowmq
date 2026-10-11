#include "flowmq_socket.h"
#include "flowmq_owner.h"
#include "flowmq_bench_metrics.h"
#include "tinytest.h"
#include <salts/clock.h>
#include <salts/thread.h>

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  WAKE_BYTES = 64, WAKE_MESSAGES = 256, WAKE_WARMUP = 16,
  WAKE_REPEATS = 5, WAKE_POLICIES = 4, WAKE_DEADLINE_MS = 30000
};

/* Two fixed socket owners; controller storage outlives both joins. Only the
 * receiver writes latency arrays. No application data crosses this barrier. */
typedef struct wake_run_s {
  cmeta_mutex_t mutex;
  cmeta_cond_t changed;
  atomic_int error;
  atomic_size_t received;
  char endpoint[128];
  int endpoint_ready, ready, started, done, cleanup;
  size_t messages, fail_after;
  unsigned policy, period_ms;
  uint64_t started_ns;
  uint64_t latency[WAKE_MESSAGES], scheduled_latency[WAKE_MESSAGES];
  uint64_t release_lag[WAKE_MESSAGES];
} wake_run;

typedef struct wake_worker_s {
  wake_run *run;
  int sender;
  flowmq_ctx_t *ctx;
  flowmq_owner_t *owner;
  flowmq_socket_t *socket;
  uint64_t deadline_ms, finished_ns;
  size_t polls, send_busy, samples;
} wake_worker;

static void wake_error(wake_run *run, int status) {
  int expected = SALTS_OK;
  if (status == SALTS_OK) return;
  (void)atomic_compare_exchange_strong(&run->error, &expected, status);
  cmeta_mutex_lock(&run->mutex);
  cmeta_cond_broadcast(&run->changed);
  cmeta_mutex_unlock(&run->mutex);
}

static int wake_status(wake_worker *worker) {
  const int status = atomic_load(&worker->run->error);
  if (status != SALTS_OK) return status;
  return cmeta_monotonic_ms() >= worker->deadline_ms ? SALTS_ETIMEDOUT : SALTS_OK;
}

static int wake_poll(wake_worker *worker, uint32_t wait_ms, short events) {
  flowmq_pollitem_t item = {.socket = worker->socket, .events = events};
  size_t ready = 0u;
  int status = wake_status(worker);
  if (status != SALTS_OK) return status;
  ++worker->polls;
  status = worker->owner != NULL
      ? flowmq_owner_poll(worker->owner, &item, 1u, wait_ms, &ready)
      : flowmq_poll(&item, 1u, wait_ms, &ready);
  if (status == SALTS_OK && (item.revents & FLOWMQ_POLLERR) != 0)
    status = SALTS_EIO;
  return status;
}

static void wake_payload(unsigned char out[WAKE_BYTES], uint64_t sequence,
                         uint64_t scheduled, uint64_t attempted) {
  const uint64_t header[] = {sequence, scheduled, attempted};
  memset(out, (int)(sequence % 251u), WAKE_BYTES);
  memcpy(out, header, sizeof(header));
}

static int wake_send(wake_worker *worker, uint64_t sequence,
                     uint64_t scheduled, uint64_t attempted) {
  unsigned char payload[WAKE_BYTES];
  wake_payload(payload, sequence, scheduled, attempted);
  for (;;) {
    int status = wake_status(worker);
    if (status != SALTS_OK) return status;
    status = flowmq_send(worker->socket, payload, sizeof(payload), FLOWMQ_DONTWAIT);
    if (status != SALTS_EBUSY && status != SALTS_ENOBUFS) return status;
    ++worker->send_busy;
    status = wake_poll(worker, 0u, FLOWMQ_POLLOUT);
    if (status != SALTS_OK) return status;
  }
}

static int wake_receive(wake_worker *worker, size_t sequence, int measured) {
  wake_run *run = worker->run;
  unsigned char payload[WAKE_BYTES], expected[WAKE_BYTES];
  uint64_t header[3];
  const uint32_t wait_ms = !measured || run->policy % 2u == 0u
      ? 0u : run->policy == 1u ? 1u : 20u;
  const uint64_t scheduled = measured
      ? run->started_ns + (sequence + 1u) * (uint64_t)run->period_ms * 1000000u : 0u;
  size_t size = 0u;
  int status;
  for (;;) {
    status = wake_poll(worker, wait_ms, FLOWMQ_POLLIN);
    if (status != SALTS_OK) return status;
    status = flowmq_recv(worker->socket, payload, sizeof(payload), &size, FLOWMQ_DONTWAIT);
    if (status != SALTS_EBUSY) break;
  }
  if (status != SALTS_OK) return status;
  if (size != sizeof(payload)) return SALTS_EPROTO;
  memcpy(header, payload, sizeof(header));
  wake_payload(expected, sequence, scheduled, header[2]);
  if (memcmp(payload, expected, sizeof(payload)) != 0 || header[2] < scheduled)
    return SALTS_EPROTO;
  if (measured) {
    const uint64_t now = cmeta_hrtime();
    if (now < header[2]) return SALTS_EPROTO;
    run->latency[sequence] = now - header[2];
    run->scheduled_latency[sequence] = now - scheduled;
    run->release_lag[sequence] = header[2] - scheduled;
    ++worker->samples;
  }
  atomic_store(&run->received, sequence + 1u);
  return SALTS_OK;
}

static int wake_open(wake_worker *worker) {
  wake_run *run = worker->run;
  int status, hwm = WAKE_MESSAGES, reconnect = -1;
  worker->ctx = flowmq_ctx_new();
  if (worker->ctx == NULL) return SALTS_ENOMEM;
  if (!worker->sender && run->policy >= 2u) {
    const flowmq_owner_config_t config = {sizeof(config), 1u};
    worker->owner = flowmq_owner_new(worker->ctx, &config);
    if (worker->owner == NULL) return SALTS_ENOMEM;
  }
  worker->socket = worker->owner != NULL
      ? flowmq_owner_socket(worker->owner, FLOWMQ_PAIR)
      : flowmq_socket(worker->ctx, FLOWMQ_PAIR);
  if (worker->socket == NULL) return SALTS_ENOMEM;
  status = flowmq_setsockopt(worker->socket, FLOWMQ_SNDHWM, &hwm, sizeof(hwm));
  if (status == SALTS_OK)
    status = flowmq_setsockopt(worker->socket, FLOWMQ_RCVHWM, &hwm, sizeof(hwm));
  if (status == SALTS_OK)
    status = flowmq_setsockopt(worker->socket, FLOWMQ_RECONNECT_IVL, &reconnect, sizeof(reconnect));
  if (status != SALTS_OK) return status;
  if (worker->sender) {
    size_t size = 0u;
    status = flowmq_bind(worker->socket, "tcp://127.0.0.1:0");
    if (status == SALTS_OK)
      status = flowmq_last_endpoint(worker->socket, run->endpoint, sizeof(run->endpoint), &size);
    if (status != SALTS_OK) return status;
    cmeta_mutex_lock(&run->mutex);
    run->endpoint_ready = 1;
    cmeta_cond_broadcast(&run->changed);
    cmeta_mutex_unlock(&run->mutex);
    return SALTS_OK;
  }
  cmeta_mutex_lock(&run->mutex);
  while (!run->endpoint_ready && atomic_load(&run->error) == SALTS_OK)
    cmeta_cond_wait(&run->changed, &run->mutex);
  status = atomic_load(&run->error);
  cmeta_mutex_unlock(&run->mutex);
  return status != SALTS_OK ? status : flowmq_connect(worker->socket, run->endpoint);
}

static int wake_sender_work(wake_worker *worker, size_t messages, int measured) {
  wake_run *run = worker->run;
  for (size_t i = 0u; i < messages; ++i) {
    uint64_t scheduled = 0u, attempted = 0u;
    int status;
    if (measured) {
      scheduled = run->started_ns + (i + 1u) * (uint64_t)run->period_ms * 1000000u;
      while ((attempted = cmeta_hrtime()) < scheduled) {
        status = wake_status(worker);
        if (status != SALTS_OK) return status;
        cmeta_sleep_ms((uint32_t)((scheduled - attempted + 999999u) / 1000000u));
      }
    }
    status = wake_send(worker, i, scheduled, attempted);
    if (status != SALTS_OK) return status;
    status = wake_poll(worker, 0u, 0);
    if (status != SALTS_OK) return status;
  }
  /* Admission is not delivery. Keep this owner alive and progressing until
   * the receiver validates every message, or publishes the first failure. */
  while (atomic_load(&run->received) != messages) {
    const int status = wake_poll(worker, 0u, 0);
    if (status != SALTS_OK) return status;
  }
  return SALTS_OK;
}

static void wake_entry(void *arg) {
  wake_worker *worker = arg;
  wake_run *run = worker->run;
  worker->deadline_ms = cmeta_monotonic_ms() + WAKE_DEADLINE_MS;
  int status = wake_open(worker);
  if (status == SALTS_OK && worker->sender)
    status = wake_sender_work(worker, WAKE_WARMUP, 0);
  if (!worker->sender)
    for (size_t i = 0u; status == SALTS_OK && i < WAKE_WARMUP; ++i)
      status = wake_receive(worker, i, 0);
  wake_error(run, status);
  cmeta_mutex_lock(&run->mutex);
  ++run->ready;
  cmeta_cond_broadcast(&run->changed);
  while (!run->started) cmeta_cond_wait(&run->changed, &run->mutex);
  cmeta_mutex_unlock(&run->mutex);
  worker->deadline_ms = cmeta_monotonic_ms() + WAKE_DEADLINE_MS;
  worker->polls = worker->send_busy = 0u;
  status = wake_status(worker);
  if (status == SALTS_OK && worker->sender)
    status = wake_sender_work(worker, run->messages, 1);
  if (!worker->sender)
    for (size_t i = 0u; status == SALTS_OK && i < run->messages; ++i) {
      status = wake_receive(worker, i, 1);
      if (status == SALTS_OK && run->fail_after != 0u && i + 1u == run->fail_after)
        status = SALTS_ECANCELED;
    }
  worker->finished_ns = cmeta_hrtime();
  wake_error(run, status);
  cmeta_mutex_lock(&run->mutex);
  ++run->done;
  cmeta_cond_broadcast(&run->changed);
  while (!run->cleanup) cmeta_cond_wait(&run->changed, &run->mutex);
  cmeta_mutex_unlock(&run->mutex);
  if (worker->socket != NULL)
    wake_error(run, worker->owner != NULL ? flowmq_owner_close_socket(worker->owner, worker->socket)
                                        : flowmq_close(worker->socket));
  if (worker->owner != NULL) wake_error(run, flowmq_owner_term(worker->owner));
  if (worker->ctx != NULL) wake_error(run, flowmq_ctx_term(worker->ctx));
}

static int wake_compare(const void *a, const void *b) {
  const uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
  return (x > y) - (x < y);
}

static void wake_start(wake_run *run, size_t created) {
  cmeta_mutex_lock(&run->mutex);
  atomic_store(&run->received, 0u);
  run->started_ns = cmeta_hrtime();
  run->started = 1;
  cmeta_cond_broadcast(&run->changed);
  while ((size_t)run->done != created) cmeta_cond_wait(&run->changed, &run->mutex);
  cmeta_mutex_unlock(&run->mutex);
}

static int wake_measure(unsigned policy, unsigned period_ms, size_t messages,
                        size_t fail_after, size_t repeat, size_t order, int measured) {
  wake_run run = {0};
  wake_worker workers[2] = {{.run = &run, .sender = 1}, {.run = &run}};
  cmeta_thread_t threads[2] = {0};
  flowmq_bench_metrics_t before = {0}, after = {0};
  size_t created = 0u;
  int status;
  if (policy >= WAKE_POLICIES || period_ms == 0u || period_ms > 20u ||
      messages == 0u || messages > WAKE_MESSAGES || fail_after > messages)
    return SALTS_EINVAL;
  run.policy = policy; run.period_ms = period_ms; run.messages = messages;
  run.fail_after = fail_after;
  atomic_init(&run.error, SALTS_OK);
  atomic_init(&run.received, 0u);
  cmeta_mutex_init(&run.mutex);
  cmeta_cond_init(&run.changed);
  for (size_t i = 0u; i < 2u; ++i) {
    status = cmeta_thread_create(&threads[i], wake_entry, &workers[i]);
    if (status != SALTS_OK) { wake_error(&run, status); break; }
    ++created;
  }
  cmeta_mutex_lock(&run.mutex);
  while ((size_t)run.ready != created) cmeta_cond_wait(&run.changed, &run.mutex);
  cmeta_mutex_unlock(&run.mutex);
  wake_error(&run, flowmq_bench_metrics_read(&before));
  if (measured) {
    benchmark_io("independent sender wake", 1u, messages, messages * WAKE_BYTES) {
      wake_start(&run, created);
    }
  } else wake_start(&run, created);
  wake_error(&run, flowmq_bench_metrics_read(&after));
  cmeta_mutex_lock(&run.mutex);
  run.cleanup = 1;
  cmeta_cond_broadcast(&run.changed);
  cmeta_mutex_unlock(&run.mutex);
  for (size_t i = 0u; i < created; ++i)
    if (cmeta_thread_join(&threads[i]) != SALTS_OK) abort();
  status = atomic_load(&run.error);
  if (status == SALTS_OK && (workers[1].samples != messages ||
      workers[1].finished_ns <= run.started_ns)) status = SALTS_EPROTO;
  if (status == SALTS_OK && measured) {
    static const char *names[] = {"ordinary_spin", "ordinary_sleep", "owner_spin", "owner_wait"};
    const uint64_t finished = workers[0].finished_ns > workers[1].finished_ns
        ? workers[0].finished_ns : workers[1].finished_ns;
    const uint64_t elapsed = finished - run.started_ns;
    const uint64_t cpu_ns = after.cpu_ns - before.cpu_ns;
    const size_t p50 = (messages * 50u + 99u) / 100u - 1u;
    const size_t p95 = (messages * 95u + 99u) / 100u - 1u;
    const size_t p99 = (messages * 99u + 99u) / 100u - 1u;
    qsort(run.latency, messages, sizeof(uint64_t), wake_compare);
    qsort(run.scheduled_latency, messages, sizeof(uint64_t), wake_compare);
    qsort(run.release_lag, messages, sizeof(uint64_t), wake_compare);
    printf("WAKE_RESULT,%s,%u,%zu,%zu,%zu,%llu,%.3f,%llu,%.6f,%llu,%llu,%llu,%llu,%llu,%zu,%zu,%d,%.3f\n",
           names[policy], period_ms, repeat, order, messages,
           (unsigned long long)elapsed, (double)messages * 1e9 / elapsed,
           (unsigned long long)cpu_ns, (double)cpu_ns / elapsed,
           (unsigned long long)run.latency[p50], (unsigned long long)run.latency[p95],
           (unsigned long long)run.latency[p99], (unsigned long long)run.scheduled_latency[p99],
           (unsigned long long)run.release_lag[p99], workers[1].polls, workers[0].send_busy,
           after.cpu_cycles_available, (double)(after.cpu_cycles - before.cpu_cycles) / messages);
  }
  cmeta_cond_destroy(&run.changed);
  cmeta_mutex_destroy(&run.mutex);
  return status;
}

spec("FlowMQ independent sender wake") {
  it("wake correctness: ordinary and owner waits preserve payload and FIFO") {
    for (unsigned policy = 0u; policy < WAKE_POLICIES; ++policy)
      check_equal(wake_measure(policy, 1u, 8u, 0u, 0u, policy, 0), SALTS_OK);
  }
  it("wake correctness: receive failure stops and joins both socket owners") {
    for (unsigned policy = 0u; policy < WAKE_POLICIES; ++policy)
      check_equal(wake_measure(policy, 1u, 8u, 2u, 0u, policy, 0), SALTS_ECANCELED);
  }
  bench("wake comparison: independent paced sender and receiving owner") {
    const unsigned periods[] = {5u, 20u};
    printf("WAKE_CONFIG,threads=2,connections=1,payload_bytes=%u,hwm=%u,warmup=%u,repeats=%u,affinity=unbound,sender=fixed-schedule-sleep,receiver_wait_ms=ordinary:1-owner:20\n",
           WAKE_BYTES, WAKE_MESSAGES, WAKE_WARMUP, WAKE_REPEATS);
    printf("WAKE_HEADER,policy,period_ms,repeat,order,messages,wall_ns,messages_per_second,cpu_ns,cpu_cores,p50_ns,p95_ns,p99_ns,scheduled_p99_ns,release_lag_p99_ns,receiver_polls,send_busy,cpu_cycles_available,cpu_cycles_per_message\n");
    for (size_t repeat = 0u; repeat < WAKE_REPEATS; ++repeat)
      for (size_t period = 0u; period < 2u; ++period)
        for (size_t order = 0u; order < WAKE_POLICIES; ++order)
          check_equal(wake_measure((unsigned)((order + repeat) % WAKE_POLICIES),
                                    periods[(period + repeat) % 2u], WAKE_MESSAGES,
                                    0u, repeat + 1u, order, 1), SALTS_OK);
  }
}
