#include "flowmq_socket.h"
#include "flowmq_bench_metrics.h"
#include "flowmq_tls_test_material.h"
#include "tinytest.h"
#include <salts/clock.h>
#include <salts/thread.h>

#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  OWNER_PAIRS = 4,
  OWNER_MAX_THREADS = 4,
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

typedef struct owner_pair_s {
  flowmq_socket_t *sender;
  flowmq_socket_t *receiver;
  uint64_t sequence;
  uint64_t identity;
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
  size_t payload_bytes;
  size_t rounds;
  const owner_tls_files_t *files;
} owner_run_t;

typedef struct owner_worker_s {
  owner_run_t *run;
  size_t first_pair;
  size_t pair_count;
  flowmq_ctx_t *ctx;
  owner_pair_t pairs[OWNER_PAIRS];
  unsigned char *payload;
  unsigned char *received;
  uint64_t *latencies;
  size_t sample_count;
  size_t retries;
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
  flowmq_pollitem_t items[] = {{.socket = pair->sender}, {.socket = pair->receiver}};
  size_t ready = 0u;
  int status = atomic_load_explicit(&worker->run->error, memory_order_relaxed);
  if (status != SALTS_OK) return status;
  if (cmeta_monotonic_ms() >= deadline_ms) return SALTS_ETIMEDOUT;
  worker->operation = "poll";
  return flowmq_poll(items, sizeof(items) / sizeof(items[0]), 0u, &ready);
}

static int owner_socket_option(flowmq_socket_t *socket, int option, const char *value) {
  return flowmq_setsockopt(socket, option, value, strlen(value));
}

static int owner_pair_open(owner_worker_t *worker, owner_pair_t *pair) {
  const owner_run_t *run = worker->run;
  char endpoint[OWNER_ENDPOINT_BYTES];
  size_t endpoint_size = 0u;
  int hwm = OWNER_WINDOW;
  int status;
  pair->sender = flowmq_socket(worker->ctx, FLOWMQ_PAIR);
  pair->receiver = flowmq_socket(worker->ctx, FLOWMQ_PAIR);
  if (pair->sender == NULL || pair->receiver == NULL) return SALTS_ENOMEM;
  status = flowmq_setsockopt(pair->sender, FLOWMQ_SNDHWM, &hwm, sizeof(hwm));
  if (status == SALTS_OK && run->tls) {
    status = owner_socket_option(pair->sender, FLOWMQ_TLS_CA_FILE, run->files->ca);
    if (status == SALTS_OK)
      status = owner_socket_option(pair->sender, FLOWMQ_TLS_SERVER_NAME, "localhost");
    if (status == SALTS_OK)
      status = owner_socket_option(pair->receiver, FLOWMQ_TLS_CERT_FILE, run->files->cert);
    if (status == SALTS_OK)
      status = owner_socket_option(pair->receiver, FLOWMQ_TLS_KEY_FILE, run->files->key);
  }
  if (status == SALTS_OK)
    status = flowmq_bind(pair->receiver, run->tls ? "tls://127.0.0.1:0" : "tcp://127.0.0.1:0");
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

static void owner_worker_close(owner_worker_t *worker) {
  for (size_t i = 0u; i < worker->pair_count; ++i) {
    if (worker->pairs[i].sender != NULL)
      owner_error(worker->run, flowmq_close(worker->pairs[i].sender));
    if (worker->pairs[i].receiver != NULL)
      owner_error(worker->run, flowmq_close(worker->pairs[i].receiver));
  }
  if (worker->ctx != NULL) owner_error(worker->run, flowmq_ctx_term(worker->ctx));
  free(worker->payload);
  free(worker->received);
}

static void owner_worker_entry(void *arg) {
  owner_worker_t *worker = arg;
  owner_run_t *run = worker->run;
  int status = SALTS_OK;
  worker->phase = "setup";
  worker->operation = "init";
  worker->ctx = flowmq_ctx_new();
  worker->payload = malloc(run->payload_bytes);
  worker->received = malloc(run->payload_bytes);
  if (worker->ctx == NULL || worker->payload == NULL || worker->received == NULL)
    status = SALTS_ENOMEM;
  if (status == SALTS_OK) memset(worker->payload, 0xa5, run->payload_bytes);
  for (size_t i = 0u; status == SALTS_OK && i < worker->pair_count; ++i) {
    worker->pairs[i].identity = worker->first_pair + i;
    status = owner_pair_open(worker, &worker->pairs[i]);
    worker->phase = "warmup";
    if (status == SALTS_OK) status = owner_batch(worker, &worker->pairs[i], 1, 0);
    if (status == SALTS_OK) status = owner_batch(worker, &worker->pairs[i], 0, 0);
  }
  owner_error(run, status);
  cmeta_mutex_lock(&run->mutex);
  ++run->ready;
  cmeta_cond_broadcast(&run->changed);
  while (!run->started)
    cmeta_cond_wait(&run->changed, &run->mutex);
  cmeta_mutex_unlock(&run->mutex);

  worker->retries = 0u;
  if (status == SALTS_OK) worker->phase = "measured";
  for (size_t round = 0u; status == SALTS_OK && round < run->rounds; ++round) {
    status = atomic_load_explicit(&run->error, memory_order_relaxed);
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
  owner_worker_close(worker);
}

static int owner_compare_ns(const void *left, const void *right) {
  const uint64_t a = *(const uint64_t *)left;
  const uint64_t b = *(const uint64_t *)right;
  return (a > b) - (a < b);
}

static int owner_run(size_t owners, size_t payload_bytes, size_t rounds, int tls,
                     const owner_tls_files_t *files, int measured, size_t repetition) {
  owner_run_t run = {0};
  owner_worker_t workers[OWNER_MAX_THREADS] = {0};
  cmeta_thread_t threads[OWNER_MAX_THREADS] = {0};
  const size_t messages = OWNER_PAIRS * rounds * OWNER_WINDOW;
  uint64_t *latencies;
  flowmq_bench_metrics_t before = {0}, after = {0};
  uint64_t started_ns = 0u, finished_ns = 0u;
  size_t created = 0u, samples = 0u, retries = 0u;
  int status = SALTS_OK;
  if (messages > SIZE_MAX / payload_bytes) return SALTS_ERANGE;
  latencies = calloc(messages, sizeof(*latencies));
  if (latencies == NULL) return SALTS_ENOMEM;
  atomic_init(&run.error, SALTS_OK);
  cmeta_mutex_init(&run.mutex);
  cmeta_cond_init(&run.changed);
  run.tls = tls;
  run.payload_bytes = payload_bytes;
  run.rounds = rounds;
  run.files = files;
  for (size_t i = 0u; i < owners; ++i) {
    workers[i].run = &run;
    workers[i].first_pair = i * (OWNER_PAIRS / owners);
    workers[i].pair_count = OWNER_PAIRS / owners;
    workers[i].latencies = latencies + i * (messages / owners);
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
    fprintf(stderr, "OWNER_FAILED_CASE,transport=%s,bytes=%zu,owners=%zu,repeat=%zu\n",
            tls ? "tls" : "tcp", payload_bytes, owners, repetition);
    for (size_t i = 0u; i < created; ++i)
      fprintf(stderr,
              "OWNER_ERROR,owner=%zu,phase=%s,operation=%s,samples=%zu,"
              "workload_status=%d,first_error=%d\n",
              i, workers[i].phase, workers[i].operation, workers[i].sample_count,
              workers[i].workload_status, status);
  }
  if (status == SALTS_OK && (samples != messages || finished_ns <= started_ns))
    status = SALTS_EPROTO;
  if (status == SALTS_OK && measured) {
    const double seconds = (double)(finished_ns - started_ns) / OWNER_NS_PER_SECOND;
    const double cpu_seconds = (double)(after.cpu_ns - before.cpu_ns) / OWNER_NS_PER_SECOND;
    const size_t p99 =
        (samples * OWNER_PERCENTILE + OWNER_PERCENT_SCALE - 1u) / OWNER_PERCENT_SCALE - 1u;
    qsort(latencies, samples, sizeof(*latencies), owner_compare_ns);
    printf("OWNER_RESULT,%s,%zu,%zu,%zu,%zu,%.6f,%.0f,%.2f,%.2f,%.3f,%.2f,%zu\n",
           tls ? "tls" : "tcp", payload_bytes, owners, repetition, messages, seconds,
           (double)messages / seconds, (double)messages * payload_bytes / seconds / OWNER_MIB,
           (double)latencies[p99] / OWNER_NS_PER_US, cpu_seconds,
           (double)after.peak_rss_bytes / OWNER_MIB, retries);
  }
  cmeta_cond_destroy(&run.changed);
  cmeta_mutex_destroy(&run.mutex);
  free(latencies);
  return status;
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
          check_equal(owner_run(owners, sizes[size], OWNER_CHECK_ROUNDS, tls, &files, 0, 0u),
                      SALTS_OK);
        }
  }

  for (int tls = 0; tls <= 1; ++tls) {
    const size_t sizes[] = {OWNER_SMALL_BYTES, OWNER_LARGE_BYTES};
    for (size_t size = 0u; size < sizeof(sizes) / sizeof(sizes[0]); ++size) {
      bench("scaling: %s %zu-byte fixed-total workload", tls ? "tls" : "tcp", sizes[size]) {
        const size_t default_rounds = size == 0u
                                          ? (tls ? OWNER_SMALL_ROUNDS : OWNER_TCP_SMALL_ROUNDS)
                                          : (tls ? OWNER_LARGE_ROUNDS : OWNER_TCP_LARGE_ROUNDS);
        size_t rounds, repeats;
        check_equal(owner_env_count("FLOWMQ_OWNER_BENCH_REPEATS", OWNER_REPEATS, OWNER_MAX_REPEATS,
                                    &repeats),
                    SALTS_OK);
        printf("OWNER_CONFIG,pairs=%d,window=%d,parts=2,logical_cpus=%d,affinity=unbound\n",
               OWNER_PAIRS, OWNER_WINDOW, cmeta_cpu_count());
        printf("OWNER_COLUMNS,transport,payload_bytes,owners,repeat,messages,seconds,"
               "messages_per_second,MiB_per_second,p99_us,process_cpu_seconds,"
               "process_lifetime_peak_RSS_MiB,admission_retries\n");
        check_equal(
            owner_env_count("FLOWMQ_OWNER_BENCH_ROUNDS", default_rounds, OWNER_MAX_ROUNDS, &rounds),
            SALTS_OK);
        for (size_t repeat = 0u; repeat < repeats; ++repeat)
          for (size_t lane = 0u; lane < OWNER_SCALE_STEPS; ++lane) {
            /* Rotate 1/2/4 order to reduce systematic thermal/order bias. */
            const size_t owners = (size_t)1u << ((lane + repeat) % OWNER_SCALE_STEPS);
            check_equal(owner_run(owners, sizes[size], rounds, tls, &files, 1, repeat), SALTS_OK);
          }
      }
    }
  }
}
