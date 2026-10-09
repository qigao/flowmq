#include "flowmq_socket.h"
#include "flowmq_bench_metrics.h"
#include "tinytest.h"
#include <salts/clock.h>
#include <zmq.h>

#if defined(FLOWMQ_BATCH_PROBE)
#include "flowmq_socket_batch_probe.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  BATCH_PAYLOAD_BYTES = 64,
  BATCH_MAX_MESSAGES = 130,
  BATCH_MEASURE_MESSAGES = 262144,
  BATCH_PROBE_MESSAGES = BATCH_MEASURE_MESSAGES,
  BATCH_REPEATS = 6,
  BATCH_WARMUP_ROUNDS = 8,
  BATCH_TIMEOUT_MS = 10000
};
static const size_t batch_sizes[] = {1u, 8u, 16u, 32u, 64u, 128u};

typedef struct batch_pair_s {
  flowmq_ctx_t *ctx;
  flowmq_socket_t *sender;
  flowmq_socket_t *receiver;
  void *zmq_ctx;
  void *zmq_sender;
  void *zmq_receiver;
  uint64_t sequence;
  uint64_t poll_calls;
  uint64_t send_retries;
  size_t payload_bytes;
  unsigned char *payload;
  unsigned char *received;
#if defined(FLOWMQ_BATCH_PROBE)
  uint64_t send_ns;
  uint64_t recv_ns;
  uint64_t poll_ns;
#endif
  int use_zmq;
} batch_pair_t;

static int batch_pair_close(batch_pair_t *pair) {
  int first = SALTS_OK;
  int status;
  if (pair->sender != NULL) {
    status = flowmq_close(pair->sender);
    if (first == SALTS_OK) first = status;
  }
  if (pair->receiver != NULL) {
    status = flowmq_close(pair->receiver);
    if (first == SALTS_OK) first = status;
  }
  if (pair->ctx != NULL) {
    status = flowmq_ctx_term(pair->ctx);
    if (first == SALTS_OK) first = status;
  }
  if (pair->zmq_sender != NULL && zmq_close(pair->zmq_sender) != 0)
    first = SALTS_EIO;
  if (pair->zmq_receiver != NULL && zmq_close(pair->zmq_receiver) != 0)
    first = SALTS_EIO;
  if (pair->zmq_ctx != NULL && zmq_ctx_term(pair->zmq_ctx) != 0)
    first = SALTS_EIO;
  free(pair->payload);
  free(pair->received);
  memset(pair, 0, sizeof(*pair));
  return first;
}

static int batch_pair_open(batch_pair_t *pair, int use_zmq, int coalesce,
                           size_t payload_bytes) {
  char endpoint[256];
  size_t endpoint_size = sizeof(endpoint);
  int status;
  memset(pair, 0, sizeof(*pair));
  pair->use_zmq = use_zmq;
  if (payload_bytes < sizeof(uint64_t) || payload_bytes > 65536u)
    return SALTS_EINVAL;
  pair->payload_bytes = payload_bytes;
  pair->payload = malloc(BATCH_MAX_MESSAGES * payload_bytes);
  pair->received = malloc(payload_bytes);
  if (pair->payload == NULL || pair->received == NULL) return SALTS_ENOMEM;
#if !defined(FLOWMQ_BATCH_PROBE)
  (void)coalesce;
#endif
  if (!use_zmq) {
    pair->ctx = flowmq_ctx_new();
    if (pair->ctx == NULL) return SALTS_ENOMEM;
    pair->sender = flowmq_socket(pair->ctx, FLOWMQ_PAIR);
    pair->receiver = flowmq_socket(pair->ctx, FLOWMQ_PAIR);
    if (pair->sender == NULL || pair->receiver == NULL) return SALTS_ENOMEM;
#if defined(FLOWMQ_BATCH_PROBE)
    status = flowmq_socket_batch_probe_coalesce(pair->sender, coalesce);
    if (status != SALTS_OK) return status;
#endif
    status = flowmq_bind(pair->receiver, "tcp://127.0.0.1:0");
    if (status != SALTS_OK) return status;
    status = flowmq_last_endpoint(pair->receiver, endpoint, sizeof(endpoint),
                                 &endpoint_size);
    if (status != SALTS_OK) return status;
    return flowmq_connect(pair->sender, endpoint);
  }

  pair->zmq_ctx = zmq_ctx_new();
  if (pair->zmq_ctx == NULL) return SALTS_ENOMEM;
  pair->zmq_sender = zmq_socket(pair->zmq_ctx, ZMQ_PAIR);
  pair->zmq_receiver = zmq_socket(pair->zmq_ctx, ZMQ_PAIR);
  if (pair->zmq_sender == NULL || pair->zmq_receiver == NULL) return SALTS_ENOMEM;
  {
    const int linger = 0;
    const int timeout = BATCH_TIMEOUT_MS;
    void *sockets[] = {pair->zmq_sender, pair->zmq_receiver};
    for (size_t i = 0u; i < 2u; ++i) {
      if (zmq_setsockopt(sockets[i], ZMQ_LINGER, &linger, sizeof(linger)) != 0 ||
          zmq_setsockopt(sockets[i], ZMQ_SNDTIMEO, &timeout, sizeof(timeout)) != 0 ||
          zmq_setsockopt(sockets[i], ZMQ_RCVTIMEO, &timeout, sizeof(timeout)) != 0)
        return SALTS_EIO;
    }
  }
  if (zmq_bind(pair->zmq_receiver, "tcp://127.0.0.1:*") != 0 ||
      zmq_getsockopt(pair->zmq_receiver, ZMQ_LAST_ENDPOINT, endpoint,
                    &endpoint_size) != 0 ||
      zmq_connect(pair->zmq_sender, endpoint) != 0)
    return SALTS_EIO;
  return SALTS_OK;
}

static int batch_progress(batch_pair_t *pair, uint64_t deadline) {
  flowmq_pollitem_t items[] = {{.socket = pair->sender}, {.socket = pair->receiver}};
  size_t ready = 0u;
  if (cmeta_monotonic_ms() >= deadline) return SALTS_ETIMEDOUT;
  ++pair->poll_calls;
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
  {
    const uint64_t started = cmeta_hrtime();
    const int status = flowmq_poll(items, 2u, 0u, &ready);
    pair->poll_ns += cmeta_hrtime() - started;
    return status;
  }
#else
  return flowmq_poll(items, 2u, 0u, &ready);
#endif
}

/* Identical FIFO payload validation for both engines. Reusing these buffers is
 * permitted by their copy admission contracts. Measurements use up to 128
 * messages; boundary tests use 130, below the default message/byte HWM.
 * No producer thread, unbounded retry or hidden benchmark queue is introduced.
 */
static int batch_exchange(batch_pair_t *pair, size_t count) {
  unsigned char *payload = pair->payload;
  unsigned char *received = pair->received;
  const size_t bytes = pair->payload_bytes;
  const uint64_t now = cmeta_monotonic_ms();
  uint64_t deadline;
  int status;
  if (count == 0u || count > BATCH_MAX_MESSAGES ||
      pair->sequence > UINT64_MAX - count || now > UINT64_MAX - BATCH_TIMEOUT_MS)
    return SALTS_EINVAL;
  deadline = now + BATCH_TIMEOUT_MS;
  memset(payload, 0x5a, count * bytes);
  for (size_t i = 0u; i < count; ++i) {
    const uint64_t sequence = pair->sequence + i;
    memcpy(payload + i * bytes, &sequence, sizeof(sequence));
    if (pair->use_zmq) {
      if (zmq_send(pair->zmq_sender, payload + i * bytes, bytes, 0) != (int)bytes)
        return SALTS_EIO;
    } else {
      for (;;) {
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
        const uint64_t started = cmeta_hrtime();
#endif
        status = flowmq_send(pair->sender, payload + i * bytes, bytes,
                             FLOWMQ_DONTWAIT);
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
        pair->send_ns += cmeta_hrtime() - started;
#endif
        if (status != SALTS_EBUSY && status != SALTS_ENOBUFS) break;
        ++pair->send_retries;
        status = batch_progress(pair, deadline);
        if (status != SALTS_OK) return status;
      }
      if (status != SALTS_OK) return status;
    }
  }
  for (size_t i = 0u; i < count; ++i) {
    if (pair->use_zmq) {
      if (zmq_recv(pair->zmq_receiver, received, bytes, 0) != (int)bytes)
        return SALTS_EIO;
    } else {
      size_t received_size = 0u;
      for (;;) {
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
        const uint64_t started = cmeta_hrtime();
#endif
        status = flowmq_recv(pair->receiver, received, bytes,
                             &received_size, FLOWMQ_DONTWAIT);
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
        pair->recv_ns += cmeta_hrtime() - started;
#endif
        if (status != SALTS_EBUSY) break;
        status = batch_progress(pair, deadline);
        if (status != SALTS_OK) return status;
      }
      if (status != SALTS_OK) return status;
      if (received_size != bytes) return SALTS_EPROTO;
    }
    if (memcmp(received, payload + i * bytes, bytes) != 0) return SALTS_EPROTO;
  }
  pair->sequence += count;
  if (pair->use_zmq) return SALTS_OK;
  /* Preserve the existing queued benchmark's credit-progress policy for all
   * burst sizes, including one; no 1-ms wait is inserted for that case. */
  status = batch_progress(pair, deadline);
  return status == SALTS_OK ? batch_progress(pair, deadline) : status;
}

#if defined(FLOWMQ_BATCH_PROBE)
static int batch_probe_result(const flowmq_socket_batch_probe_t *before,
                              const flowmq_socket_batch_probe_t *after,
                              size_t batch, size_t messages, size_t bytes, int report) {
  const uint64_t direct = after->direct_writes - before->direct_writes;
  const uint64_t queued = after->queued_writes - before->queued_writes;
  const uint64_t ranges = after->queued_ranges - before->queued_ranges;
  uint64_t histogram_writes = 0u;
  uint64_t histogram_ranges = 0u;
  for (size_t i = 1u; i <= FLOWMQ_BATCH_PROBE_MAX_FRAMES; ++i) {
    const uint64_t writes =
        after->queued_range_histogram[i] - before->queued_range_histogram[i];
    histogram_writes += writes;
    histogram_ranges += i * writes;
    if (report && writes != 0u)
      printf("BATCH_HIST,%zu,%zu,%llu\n", batch, i, (unsigned long long)writes);
  }
  if (after->messages - before->messages != messages ||
      after->payload_bytes - before->payload_bytes != messages * bytes ||
      histogram_writes != queued || histogram_ranges != ranges ||
      direct + ranges != messages || direct + queued == 0u)
    return SALTS_EPROTO;
  if (report)
    printf("BATCH_PROBE,%zu,%zu,%llu,%llu,%llu,%.6f,unavailable\n",
           batch, messages, (unsigned long long)direct, (unsigned long long)queued,
           (unsigned long long)ranges, (double)messages / (double)(direct + queued));
  return SALTS_OK;
}
#endif

#if defined(FLOWMQ_BATCH_PROBE)
static int batch_compare_ns(const void *left, const void *right) {
  const uint64_t a = *(const uint64_t *)left, b = *(const uint64_t *)right;
  return (a > b) - (a < b);
}
#endif

static int batch_run_sized(int use_zmq, size_t batch, size_t messages,
                           size_t repetition, size_t order, int measured,
                           int coalesce, size_t bytes, int progress) {
  batch_pair_t pair = {0};
  flowmq_bench_metrics_t before = {0}, after = {0};
  uint64_t started = 0u, finished = 0u;
  size_t completed = 0u;
  uint64_t *burst_ns = NULL;
  int status = batch_pair_open(&pair, use_zmq, coalesce, bytes);
#if defined(FLOWMQ_BATCH_PROBE)
  flowmq_socket_batch_probe_t probe_before = {0}, probe_after = {0};
  flowmq_socket_batch_probe_t rx_before = {0}, rx_after = {0};
#else
  (void)progress;
#endif
  if (status != SALTS_OK) goto cleanup;
  if (batch == 0u || batch > BATCH_MAX_MESSAGES || messages == 0u ||
      messages % batch != 0u || messages > SIZE_MAX / bytes) {
    status = SALTS_EINVAL;
    goto cleanup;
  }
  if (measured == 3 || measured == 4) {
    burst_ns = calloc(messages / batch, sizeof(*burst_ns));
    if (burst_ns == NULL) { status = SALTS_ENOMEM; goto cleanup; }
  }
  for (size_t i = 0u; i < BATCH_WARMUP_ROUNDS; ++i) {
    status = batch_exchange(&pair, batch);
    if (status != SALTS_OK) goto cleanup;
  }
  pair.poll_calls = 0u;
  pair.send_retries = 0u;
#if defined(FLOWMQ_BATCH_PROBE)
  if (!use_zmq) {
    status = flowmq_socket_batch_probe_progress(pair.sender, progress);
    if (status == SALTS_OK)
      status = flowmq_socket_batch_probe_progress(pair.receiver, progress);
    if (status != SALTS_OK) goto cleanup;
    pair.send_ns = pair.recv_ns = pair.poll_ns = 0u;
    status = flowmq_socket_batch_probe_read(pair.sender, &probe_before);
    if (status == SALTS_OK)
      status = flowmq_socket_batch_probe_read(pair.receiver, &rx_before);
    if (status != SALTS_OK) goto cleanup;
  }
#endif
  status = flowmq_bench_metrics_read(&before);
  if (status != SALTS_OK) goto cleanup;
#if !defined(FLOWMQ_BATCH_PROBE)
  benchmark_io("fixed-total TCP batch workload", 1u, messages,
               messages * bytes) {
#endif
    started = cmeta_hrtime();
    for (; completed < messages; completed += batch) {
      const uint64_t burst_start = burst_ns == NULL ? 0u : cmeta_hrtime();
      status = batch_exchange(&pair, batch);
      if (burst_ns != NULL) burst_ns[completed / batch] = cmeta_hrtime() - burst_start;
      if (status != SALTS_OK) goto measurement_finished;
    }
    finished = cmeta_hrtime();
    status = flowmq_bench_metrics_read(&after);
    if (status != SALTS_OK) goto measurement_finished;
#if !defined(FLOWMQ_BATCH_PROBE)
    if (!measured) goto measurement_finished;
  }
#endif
measurement_finished:
  if (status != SALTS_OK) goto cleanup;
  if (completed != messages || finished <= started || after.cpu_ns < before.cpu_ns) {
    status = SALTS_EPROTO;
    goto cleanup;
  }
#if defined(FLOWMQ_BATCH_PROBE)
  if (!use_zmq) {
    status = flowmq_socket_batch_probe_read(pair.sender, &probe_after);
    if (status == SALTS_OK)
      status = flowmq_socket_batch_probe_read(pair.receiver, &rx_after);
    if (status == SALTS_OK)
      status = batch_probe_result(&probe_before, &probe_after, batch, messages, bytes, measured == 1);
    if (status != SALTS_OK) goto cleanup;
    {
      const uint64_t direct = probe_after.direct_writes - probe_before.direct_writes;
      const uint64_t queued = probe_after.queued_writes - probe_before.queued_writes;
      const uint64_t ranges = probe_after.queued_ranges - probe_before.queued_ranges;
      const uint64_t copied = probe_after.coalesced_writes - probe_before.coalesced_writes;
      const uint64_t copied_ranges = probe_after.coalesced_ranges - probe_before.coalesced_ranges;
      if (copied > queued || copied_ranges > ranges ||
          probe_after.submitted_ranges - probe_before.submitted_ranges !=
              direct + copied + ranges - copied_ranges ||
          (coalesce != 3 && copied != (coalesce ? queued : 0u)) ||
          (coalesce == 3 && probe_after.coalesced_bytes_peak > 16384u)) {
        status = SALTS_EPROTO;
        goto cleanup;
      }
    }
    if (measured == 3) {
      const size_t bursts = messages / batch;
      const double seconds = (double)(finished - started) / 1e9;
      qsort(burst_ns, bursts, sizeof(*burst_ns), batch_compare_ns);
      printf("BATCH_POLICY,%s,%zu,%zu,%zu,%zu,%zu,%.9f,%.3f,%.3f,%llu,%llu,%llu,%llu,%llu,%llu,%llu\n",
             coalesce == 3 ? "bounded" : "sg32", bytes, batch, repetition, order,
             messages, seconds, (double)messages / seconds,
             (double)(after.cpu_ns - before.cpu_ns) / messages,
             (unsigned long long)burst_ns[(bursts * 95u + 99u) / 100u - 1u],
             (unsigned long long)burst_ns[(bursts * 99u + 99u) / 100u - 1u],
             (unsigned long long)after.peak_rss_bytes,
             (unsigned long long)(probe_after.direct_writes - probe_before.direct_writes +
                                  probe_after.queued_writes - probe_before.queued_writes),
             (unsigned long long)(probe_after.submitted_ranges - probe_before.submitted_ranges),
             (unsigned long long)pair.poll_calls, (unsigned long long)pair.send_retries);
      fflush(stdout);
    }
#define BATCH_PHASE_SUM(member) \
      ((unsigned long long)(probe_after.member - probe_before.member + \
                            rx_after.member - rx_before.member))
    if (measured == 4) {
      const size_t bursts = messages / batch;
      /* The interventions apply only after both ends finish warmup. No accept
       * or retirement work is allowed inside this fixed-topology workload. */
      if (BATCH_PHASE_SUM(listener_ready) != 0u || BATCH_PHASE_SUM(manager_work) != 0u) {
        status = SALTS_EPROTO;
        goto cleanup;
      }
      qsort(burst_ns, bursts, sizeof(*burst_ns), batch_compare_ns);
      printf("PROGRESS_RESULT,%s,%zu,%zu,%zu,%zu,%zu,%llu,%.3f,%llu,%llu,%llu,%llu,"
             "%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu\n",
             progress == 1 ? "no_pre_manager" : progress == 2 ? "no_listener_check" : "baseline",
             bytes, batch, repetition, order, messages,
             (unsigned long long)(finished - started),
             (double)(after.cpu_ns - before.cpu_ns) / messages,
             (unsigned long long)burst_ns[(bursts * 95u + 99u) / 100u - 1u],
             (unsigned long long)burst_ns[(bursts * 99u + 99u) / 100u - 1u],
             (unsigned long long)pair.poll_calls,
             (unsigned long long)(probe_after.direct_writes - probe_before.direct_writes +
                                  probe_after.queued_writes - probe_before.queued_writes),
             BATCH_PHASE_SUM(drive_calls), BATCH_PHASE_SUM(listener_ns),
             BATCH_PHASE_SUM(client_poll_ns), BATCH_PHASE_SUM(local_progress_ns),
             BATCH_PHASE_SUM(listener_manager_ns), BATCH_PHASE_SUM(local_manager_ns),
             BATCH_PHASE_SUM(listener_wait_ns), BATCH_PHASE_SUM(reconnect_ns),
             BATCH_PHASE_SUM(manager_calls), BATCH_PHASE_SUM(manager_work),
             BATCH_PHASE_SUM(listener_checks), BATCH_PHASE_SUM(listener_ready),
             BATCH_PHASE_SUM(local_slots), BATCH_PHASE_SUM(local_used_peers));
      fflush(stdout);
    }
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
    if (measured == 1) {
      printf("BATCH_PHASE,%zu,%zu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu\n",
             batch, messages, (unsigned long long)(finished - started),
             (unsigned long long)pair.send_ns, (unsigned long long)pair.recv_ns,
             (unsigned long long)pair.poll_ns,
             BATCH_PHASE_SUM(listener_ns), BATCH_PHASE_SUM(client_poll_ns),
             BATCH_PHASE_SUM(local_progress_ns), BATCH_PHASE_SUM(receive_callback_ns),
             BATCH_PHASE_SUM(receive_callbacks), BATCH_PHASE_SUM(receive_bytes),
             BATCH_PHASE_SUM(drive_calls), BATCH_PHASE_SUM(receive_fast_slices),
             BATCH_PHASE_SUM(receive_stream_slices), BATCH_PHASE_SUM(receive_decoder_slices));
    }
#endif
    if (measured == 2) {
      const double seconds = (double)(finished - started) / 1e9;
      printf("BATCH_AB,%s,%zu,%zu,%zu,%zu,%.9f,%.3f,%.9f,%.3f,%llu,%llu,%llu,"
             "%llu,%llu,%llu,%llu,%llu,%llu,%llu\n",
             coalesce == 2 ? "coalesce128" : coalesce == 1 ? "coalesce32" : "sg32",
             batch, repetition, order, messages,
             seconds, (double)messages / seconds,
             (double)(after.cpu_ns - before.cpu_ns) / 1e9,
             (double)(after.cpu_ns - before.cpu_ns) / messages,
             (unsigned long long)(probe_after.direct_writes - probe_before.direct_writes +
                                  probe_after.queued_writes - probe_before.queued_writes),
             (unsigned long long)(probe_after.submitted_ranges - probe_before.submitted_ranges),
             (unsigned long long)pair.poll_calls,
             (unsigned long long)pair.send_ns, (unsigned long long)pair.recv_ns,
             (unsigned long long)pair.poll_ns, BATCH_PHASE_SUM(listener_ns),
             BATCH_PHASE_SUM(client_poll_ns), BATCH_PHASE_SUM(local_progress_ns),
             BATCH_PHASE_SUM(receive_callback_ns));
      fflush(stdout);
    }
#undef BATCH_PHASE_SUM
  }
#else
  if (measured) {
    const double seconds = (double)(finished - started) / 1e9;
    const double cpu_seconds = (double)(after.cpu_ns - before.cpu_ns) / 1e9;
    printf("BATCH_RESULT,%s,%zu,%zu,%zu,%zu,%.9f,%.3f,%.9f,%.3f,%llu,%llu\n",
           use_zmq ? "libzmq" : "flowmq", batch, repetition, order, messages,
           seconds, (double)messages / seconds, cpu_seconds,
           (double)(after.cpu_ns - before.cpu_ns) / messages,
           (unsigned long long)pair.poll_calls, (unsigned long long)pair.send_retries);
    fflush(stdout);
  }
#endif
cleanup:
  free(burst_ns);
  {
    const int close_status = batch_pair_close(&pair);
    if (status == SALTS_OK) status = close_status;
  }
  return status;
}

static int batch_run(int use_zmq, size_t batch, size_t messages,
                     size_t repetition, size_t order, int measured, int coalesce) {
  return batch_run_sized(use_zmq, batch, messages, repetition, order, measured,
                         coalesce, BATCH_PAYLOAD_BYTES, 0);
}

#if defined(FLOWMQ_BATCH_PROBE)
static void batch_progress_header(void) {
  printf("PROGRESS_HEADER,variant,payload_bytes,batch,repeat,order,messages,wall_ns,"
         "cpu_ns_per_message,burst_p95_ns,burst_p99_ns,poll_calls,logical_writes,"
         "drive_calls,listener_ns,client_poll_ns,local_progress_ns,listener_manager_ns,"
         "local_manager_ns,listener_wait_ns,reconnect_ns,manager_calls,manager_work,"
         "listener_checks,listener_ready,local_slots,local_used_peers\n");
}
#endif

spec("FlowMQ and libzmq batch sweep") {
  it("correctness: copy bursts preserve payloads and FIFO") {
    for (size_t i = 0u; i < sizeof(batch_sizes) / sizeof(batch_sizes[0]); ++i) {
      check_equal(batch_run(0, batch_sizes[i], batch_sizes[i] * 3u, 0u, 0u, 0, 0), SALTS_OK);
      check_equal(batch_run(1, batch_sizes[i], batch_sizes[i] * 3u, 0u, 1u, 0, 0), SALTS_OK);
#if defined(FLOWMQ_BATCH_PROBE)
      check_equal(batch_run(0, batch_sizes[i], batch_sizes[i] * 3u, 0u, 0u, 0, 1), SALTS_OK);
      check_equal(batch_run(0, batch_sizes[i], batch_sizes[i] * 3u, 0u, 0u, 0, 2), SALTS_OK);
      check_equal(batch_run(0, batch_sizes[i], batch_sizes[i] * 3u, 0u, 0u, 0, 3), SALTS_OK);
#endif
    }
  }
#if defined(FLOWMQ_BATCH_PROBE)
  it("correctness: fixed-topology progress interventions preserve copy delivery") {
    for (int mode = 0; mode < 3; ++mode) {
      check_equal(batch_run_sized(0, 128u, 384u, 0u, 0u, 0, 3, 64u, mode), SALTS_OK);
      check_equal(batch_run_sized(0, 128u, 384u, 0u, 0u, 0, 3, 1024u, mode), SALTS_OK);
    }
  }
  bench("progress-phases: production copy progress attribution") {
    static const struct { size_t bytes, batch, messages; } cases[] = {
      {64u, 1u, 65536u}, {64u, 128u, BATCH_MEASURE_MESSAGES},
      {1024u, 128u, BATCH_MEASURE_MESSAGES}};
    batch_progress_header();
    for (size_t repetition = 0u; repetition < 3u; ++repetition)
      for (size_t i = 0u; i < 3u; ++i) {
        const size_t index = (i + repetition) % 3u;
        check_equal(batch_run_sized(0, cases[index].batch, cases[index].messages,
                                    repetition + 1u, 0u, 4, 3, cases[index].bytes, 0), SALTS_OK);
      }
  }
  bench("progress-ablation: fixed-topology progress costs") {
    static const struct { size_t bytes, batch, messages; } cases[] = {
      {64u, 1u, 65536u}, {64u, 128u, BATCH_MEASURE_MESSAGES},
      {1024u, 128u, BATCH_MEASURE_MESSAGES}};
    batch_progress_header();
    for (size_t repetition = 0u; repetition < BATCH_REPEATS; ++repetition)
      for (size_t i = 0u; i < 3u; ++i) {
        const size_t index = (i + repetition) % 3u;
        for (size_t order = 0u; order < 3u; ++order) {
          const int mode = (int)((order + repetition + index) % 3u);
          check_equal(batch_run_sized(0, cases[index].batch, cases[index].messages,
                                      repetition + 1u, order, 4, 3, cases[index].bytes, mode), SALTS_OK);
        }
      }
  }
  it("correctness: production copy policy respects frame and byte boundaries") {
    static const struct { size_t bytes, batch, copies, peak; } cases[] = {
      {64u, 1u, 0u, 0u}, {64u, 2u, 0u, 0u}, {64u, 3u, 0u, 0u},
      {64u, 17u, 0u, 0u}, {64u, 18u, 1u, 1632u},
      {64u, 128u, 1u, 12192u}, {64u, 129u, 1u, 12288u},
      {64u, 130u, 1u, 12288u}, {224u, 65u, 1u, 16384u},
      {224u, 66u, 1u, 16384u}, {224u, 128u, 2u, 16384u},
      {225u, 128u, 0u, 0u}, {1024u, 128u, 0u, 0u}};
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      batch_pair_t pair = {0};
      flowmq_socket_batch_probe_t stats = {0};
      int status = batch_pair_open(&pair, 0, 3, cases[i].bytes);
      if (status == SALTS_OK) status = batch_exchange(&pair, cases[i].batch);
      if (status == SALTS_OK) status = flowmq_socket_batch_probe_read(pair.sender, &stats);
      const int close_status = batch_pair_close(&pair);
      check_equal(status, SALTS_OK);
      check_equal(close_status, SALTS_OK);
      check_equal(stats.coalesced_writes, cases[i].copies);
      check_equal(stats.coalesced_bytes_peak, cases[i].peak);
      check_equal(stats.messages, cases[i].batch);
    }
  }
  bench("policy: bounded copy versus historical SG") {
    static const struct { size_t bytes, batch; } cases[] = {
      {64u, 1u}, {64u, 8u}, {64u, 32u}, {64u, 128u},
      {224u, 128u}, {225u, 128u}, {1024u, 128u}, {65536u, 16u}};
    const size_t count = sizeof(cases) / sizeof(cases[0]);
    printf("BATCH_POLICY_HEADER,variant,payload_bytes,batch,repeat,order,messages,seconds,"
           "messages_per_second,cpu_ns_per_message,burst_p95_ns,burst_p99_ns,"
           "process_peak_rss_bytes,logical_writes,submitted_ranges,poll_calls,send_retries\n");
    for (size_t repetition = 0u; repetition < BATCH_REPEATS; ++repetition) {
      for (size_t position = 0u; position < count; ++position) {
        const size_t index = (position + repetition) % count;
        /* Keep the short 225 B/1 KiB cases above a handful of Windows CPU
         * timer ticks. The 64 KiB control transfers 1 GiB per sample. */
        const size_t messages = cases[index].bytes == 65536u
            ? 16384u : BATCH_MEASURE_MESSAGES;
        for (size_t order = 0u; order < 2u; ++order) {
          const int mode = ((order + repetition + index) % 2u) == 0u ? 0 : 3;
          check_equal(batch_run_sized(0, cases[index].batch, messages,
                                      repetition + 1u, order, 3, mode,
                                      cases[index].bytes, 0), SALTS_OK);
        }
      }
    }
  }
  bench("diagnostic: actual FlowMQ copy admission batches") {
    printf("BATCH_PROBE_HEADER,batch,messages,direct_writes,queued_writes,queued_ranges,"
           "messages_per_logical_write,native_write_count\n");
    printf("BATCH_HIST_HEADER,batch,ranges_per_queued_write,writes\n");
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
    printf("BATCH_PHASE_HEADER,batch,messages,wall_ns,send_ns,recv_ns,poll_ns,listener_ns,"
           "client_poll_ns,local_progress_ns,receive_callback_ns,receive_callbacks,"
           "receive_bytes,drive_calls,fast_slices,stream_slices,decoder_slices\n");
#endif
    for (size_t i = 0u; i < sizeof(batch_sizes) / sizeof(batch_sizes[0]); ++i)
      check_equal(batch_run(0, batch_sizes[i], BATCH_PROBE_MESSAGES, 0u, 0u, 1, 0), SALTS_OK);
  }
  bench("ablation: SG and coalesced copy batch limits") {
    printf("BATCH_AB_HEADER,variant,batch,repeat,order,messages,seconds,messages_per_second,"
           "process_cpu_seconds,cpu_ns_per_message,logical_writes,submitted_ranges,poll_calls,"
           "send_ns,recv_ns,poll_ns,listener_ns,client_poll_ns,local_progress_ns,receive_callback_ns\n");
    const size_t count = sizeof(batch_sizes) / sizeof(batch_sizes[0]) - 2u;
    for (size_t repetition = 0u; repetition < BATCH_REPEATS; ++repetition) {
      for (size_t position = 0u; position < count; ++position) {
        const size_t index = (position + repetition) % count + 2u;
        for (size_t order = 0u; order < 3u; ++order) {
          const int mode = (int)((order + repetition + index) % 3u);
          check_equal(batch_run(0, batch_sizes[index], BATCH_MEASURE_MESSAGES,
                                repetition + 1u, order, 2, mode), SALTS_OK);
        }
      }
    }
  }
#else
  bench("performance: fixed-total batch sweep") {
    const size_t count = sizeof(batch_sizes) / sizeof(batch_sizes[0]);
    printf("BATCH_RESULT_HEADER,engine,batch,repeat,order,messages,seconds,"
           "messages_per_second,process_cpu_seconds,cpu_ns_per_message,poll_calls,send_retries\n");
    for (size_t repetition = 0u; repetition < BATCH_REPEATS; ++repetition) {
      for (size_t position = 0u; position < count; ++position) {
        const size_t index = (position + repetition) % count;
        const int zmq_first = (int)((repetition + index) % 2u);
        check_equal(batch_run(zmq_first, batch_sizes[index], BATCH_MEASURE_MESSAGES,
                              repetition + 1u, 0u, 1, 0), SALTS_OK);
        check_equal(batch_run(!zmq_first, batch_sizes[index], BATCH_MEASURE_MESSAGES,
                              repetition + 1u, 1u, 1, 0), SALTS_OK);
      }
    }
  }
#endif
}
