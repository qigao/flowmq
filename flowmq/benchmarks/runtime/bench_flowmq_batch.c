#include "flowmq_socket.h"
#include "flowmq_bench_metrics.h"
#include "tinytest.h"
#include <salts/clock.h>
#include <zmq.h>

#if defined(FLOWMQ_BATCH_PROBE)
#include "flowmq_socket_batch_probe.h"
#endif

#include <stdio.h>
#include <string.h>

enum {
  BATCH_PAYLOAD_BYTES = 64,
  BATCH_MAX_MESSAGES = 128,
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
  memset(pair, 0, sizeof(*pair));
  return first;
}

static int batch_pair_open(batch_pair_t *pair, int use_zmq, int coalesce) {
  char endpoint[256];
  size_t endpoint_size = sizeof(endpoint);
  int status;
  memset(pair, 0, sizeof(*pair));
  pair->use_zmq = use_zmq;
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
 * permitted by their copy admission contracts. At most 128 messages are in
 * one application burst, below both libraries' default message/byte HWM.
 * No producer thread, unbounded retry or hidden benchmark queue is introduced.
 */
static int batch_exchange(batch_pair_t *pair, size_t count) {
  unsigned char payload[BATCH_MAX_MESSAGES][BATCH_PAYLOAD_BYTES];
  unsigned char received[BATCH_PAYLOAD_BYTES];
  const uint64_t now = cmeta_monotonic_ms();
  uint64_t deadline;
  int status;
  if (count == 0u || count > BATCH_MAX_MESSAGES ||
      pair->sequence > UINT64_MAX - count || now > UINT64_MAX - BATCH_TIMEOUT_MS)
    return SALTS_EINVAL;
  deadline = now + BATCH_TIMEOUT_MS;
  memset(payload, 0x5a, count * BATCH_PAYLOAD_BYTES);
  for (size_t i = 0u; i < count; ++i) {
    const uint64_t sequence = pair->sequence + i;
    memcpy(payload[i], &sequence, sizeof(sequence));
    if (pair->use_zmq) {
      if (zmq_send(pair->zmq_sender, payload[i], BATCH_PAYLOAD_BYTES, 0) !=
          BATCH_PAYLOAD_BYTES)
        return SALTS_EIO;
    } else {
      for (;;) {
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
        const uint64_t started = cmeta_hrtime();
#endif
        status = flowmq_send(pair->sender, payload[i], BATCH_PAYLOAD_BYTES,
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
      if (zmq_recv(pair->zmq_receiver, received, sizeof(received), 0) !=
          BATCH_PAYLOAD_BYTES)
        return SALTS_EIO;
    } else {
      size_t received_size = 0u;
      for (;;) {
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
        const uint64_t started = cmeta_hrtime();
#endif
        status = flowmq_recv(pair->receiver, received, sizeof(received),
                             &received_size, FLOWMQ_DONTWAIT);
#if defined(FLOWMQ_BATCH_PHASE_TIMING)
        pair->recv_ns += cmeta_hrtime() - started;
#endif
        if (status != SALTS_EBUSY) break;
        status = batch_progress(pair, deadline);
        if (status != SALTS_OK) return status;
      }
      if (status != SALTS_OK) return status;
      if (received_size != BATCH_PAYLOAD_BYTES) return SALTS_EPROTO;
    }
    if (memcmp(received, payload[i], sizeof(received)) != 0) return SALTS_EPROTO;
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
                              size_t batch, size_t messages, int report) {
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
      after->payload_bytes - before->payload_bytes != messages * BATCH_PAYLOAD_BYTES ||
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

static int batch_run(int use_zmq, size_t batch, size_t messages,
                     size_t repetition, size_t order, int measured, int coalesce) {
  batch_pair_t pair = {0};
  flowmq_bench_metrics_t before = {0}, after = {0};
  uint64_t started = 0u, finished = 0u;
  size_t completed = 0u;
  int status = batch_pair_open(&pair, use_zmq, coalesce);
#if defined(FLOWMQ_BATCH_PROBE)
  flowmq_socket_batch_probe_t probe_before = {0}, probe_after = {0};
  flowmq_socket_batch_probe_t rx_before = {0}, rx_after = {0};
#endif
  if (status != SALTS_OK) goto cleanup;
  if (batch == 0u || batch > BATCH_MAX_MESSAGES || messages == 0u ||
      messages % batch != 0u || messages > SIZE_MAX / BATCH_PAYLOAD_BYTES) {
    status = SALTS_EINVAL;
    goto cleanup;
  }
  for (size_t i = 0u; i < BATCH_WARMUP_ROUNDS; ++i) {
    status = batch_exchange(&pair, batch);
    if (status != SALTS_OK) goto cleanup;
  }
  pair.poll_calls = 0u;
  pair.send_retries = 0u;
#if defined(FLOWMQ_BATCH_PROBE)
  if (!use_zmq) {
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
               messages * BATCH_PAYLOAD_BYTES) {
#endif
    started = cmeta_hrtime();
    for (; completed < messages; completed += batch) {
      status = batch_exchange(&pair, batch);
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
      status = batch_probe_result(&probe_before, &probe_after, batch, messages, measured == 1);
    if (status != SALTS_OK) goto cleanup;
    {
      const uint64_t direct = probe_after.direct_writes - probe_before.direct_writes;
      const uint64_t queued = probe_after.queued_writes - probe_before.queued_writes;
      const uint64_t ranges = probe_after.queued_ranges - probe_before.queued_ranges;
      if (probe_after.submitted_ranges - probe_before.submitted_ranges !=
              direct + (coalesce ? queued : ranges) ||
          probe_after.coalesced_writes - probe_before.coalesced_writes !=
              (coalesce ? queued : 0u)) {
        status = SALTS_EPROTO;
        goto cleanup;
      }
    }
#define BATCH_PHASE_SUM(member) \
      ((unsigned long long)(probe_after.member - probe_before.member + \
                            rx_after.member - rx_before.member))
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
  {
    const int close_status = batch_pair_close(&pair);
    if (status == SALTS_OK) status = close_status;
  }
  return status;
}

spec("FlowMQ and libzmq batch sweep") {
  it("correctness: copy bursts preserve payloads and FIFO") {
    for (size_t i = 0u; i < sizeof(batch_sizes) / sizeof(batch_sizes[0]); ++i) {
      check_equal(batch_run(0, batch_sizes[i], batch_sizes[i] * 3u, 0u, 0u, 0, 0), SALTS_OK);
      check_equal(batch_run(1, batch_sizes[i], batch_sizes[i] * 3u, 0u, 1u, 0, 0), SALTS_OK);
#if defined(FLOWMQ_BATCH_PROBE)
      check_equal(batch_run(0, batch_sizes[i], batch_sizes[i] * 3u, 0u, 0u, 0, 1), SALTS_OK);
      check_equal(batch_run(0, batch_sizes[i], batch_sizes[i] * 3u, 0u, 0u, 0, 2), SALTS_OK);
#endif
    }
  }
#if defined(FLOWMQ_BATCH_PROBE)
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
