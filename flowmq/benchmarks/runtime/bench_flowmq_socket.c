#include "flowmq_socket.h"
#include "flowmq_protocol.h"
#include "flowmq_tls_test_material.h"
#include "tinytest.h"
#include "cmeta_error.h"
#include <salts/clock.h>

#include <stdlib.h>
#include <string.h>

#if defined(FLOWMQ_BENCH_WITH_ZMQ)
#include <zmq.h>
#endif

enum {
  BENCH_PAYLOAD_BYTES = 64u,
  BENCH_LARGE_PAYLOAD_BYTES = 64u * 1024u,
  BENCH_RETAINED_LARGE_PAYLOAD_BYTES = 1024u * 1024u,
  BENCH_SAMPLES = 100000u,
  BENCH_LARGE_SAMPLES = 5000u,
  BENCH_RETAINED_LARGE_SAMPLES = 512u,
  BENCH_MULTIPART_SMALL_PARTS = 4u,
  BENCH_MULTIPART_SMALL_PART_BYTES = 64u,
  BENCH_MULTIPART_LARGE_PARTS = 2u,
  BENCH_MULTIPART_LARGE_PART_BYTES = 256u * 1024u,
  BENCH_MULTIPART_SMALL_SAMPLES = 25000u,
  BENCH_MULTIPART_LARGE_SAMPLES = 512u,
  BENCH_BATCH_SAMPLES = 10000u,
  BENCH_BATCH_MESSAGES = 64u,
  BENCH_PROGRESS_LIMIT = 10000u,
  BENCH_PROGRESS_TIMEOUT_MS = 10000u,
  BENCH_PROGRESS_WAIT_MS = 1u
};

static size_t bench_socket_samples(size_t regular, size_t smoke_samples) {
  const char *smoke = getenv("FLOWMQ_BENCH_SMOKE");
  const char *ci = getenv("FLOWMQ_BENCH_CI");
  size_t ci_samples;
  if (smoke != NULL && strcmp(smoke, "0") != 0) return smoke_samples;
  if (ci == NULL || strcmp(ci, "0") == 0) return regular;
  ci_samples = regular / 50u;
  if (ci_samples < smoke_samples * 32u) ci_samples = smoke_samples * 32u;
  if (ci_samples > regular) ci_samples = regular;
  return ci_samples;
}

typedef struct bench_pair_s {
  flowmq_ctx_t *ctx;
  flowmq_socket_t *sender;
  flowmq_socket_t *receiver;
  char *ca_path;
  char *cert_path;
  char *key_path;
} bench_pair_t;

static int bench_progress(bench_pair_t *pair) {
  flowmq_pollitem_t items[] = {
      {.socket = pair->sender}, {.socket = pair->receiver}};
  size_t ready = 0u;
  return flowmq_poll(items, 2u, 0u, &ready);
}

static uint64_t bench_progress_deadline_ms(void) {
  const uint64_t now = cmeta_monotonic_ms();
  return now > UINT64_MAX - BENCH_PROGRESS_TIMEOUT_MS
             ? UINT64_MAX
             : now + BENCH_PROGRESS_TIMEOUT_MS;
}

static int bench_progress_wait(bench_pair_t *pair, uint64_t deadline_ms,
                               int waiting_for_send) {
  flowmq_pollitem_t items[] = {
      {.socket = pair->sender,
       .events = (short)(FLOWMQ_POLLERR |
                         (waiting_for_send ? FLOWMQ_POLLOUT : 0))},
      {.socket = pair->receiver,
       .events = (short)(FLOWMQ_POLLERR |
                         (waiting_for_send ? 0 : FLOWMQ_POLLIN))}};
  const uint64_t now = cmeta_monotonic_ms();
  uint64_t remaining_ms;
  uint32_t wait_ms;
  size_t ready = 0u;
  int status;
  if (now >= deadline_ms) return SALTS_ETIMEDOUT;
  remaining_ms = deadline_ms - now;
  wait_ms =
      remaining_ms < BENCH_PROGRESS_WAIT_MS
          ? (uint32_t)remaining_ms
          : BENCH_PROGRESS_WAIT_MS;
  if (wait_ms == 0u) wait_ms = 1u;
  status = flowmq_poll(items, 2u, wait_ms, &ready);
  if (status != SALTS_OK) return status;
  if ((items[0].revents & FLOWMQ_POLLERR) != 0 ||
      (items[1].revents & FLOWMQ_POLLERR) != 0)
    return SALTS_EIO;
  return SALTS_OK;
}

static int bench_pair_open(bench_pair_t *pair) {
  char endpoint[128];
  size_t endpoint_size = 0u;
  memset(pair, 0, sizeof(*pair));
  pair->ctx = flowmq_ctx_new();
  if (pair->ctx == NULL) return SALTS_ENOMEM;
  pair->sender = flowmq_socket(pair->ctx, FLOWMQ_PAIR);
  pair->receiver = flowmq_socket(pair->ctx, FLOWMQ_PAIR);
  if (pair->sender == NULL || pair->receiver == NULL) return SALTS_ENOMEM;
  if (flowmq_bind(pair->receiver, "tcp://127.0.0.1:0") != SALTS_OK)
    return SALTS_EIO;
  if (flowmq_last_endpoint(pair->receiver, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    return SALTS_EIO;
  return flowmq_connect(pair->sender, endpoint);
}

static int bench_pair_close(bench_pair_t *pair) {
  int result = SALTS_OK;
  int status;
  if (pair == NULL) return SALTS_EINVAL;
  if (pair->sender != NULL) {
    status = flowmq_close(pair->sender);
    if (result == SALTS_OK && status != SALTS_OK) result = status;
  }
  if (pair->receiver != NULL) {
    status = flowmq_close(pair->receiver);
    if (result == SALTS_OK && status != SALTS_OK) result = status;
  }
  if (pair->ctx != NULL) {
    status = flowmq_ctx_term(pair->ctx);
    if (result == SALTS_OK && status != SALTS_OK) result = status;
  }
  if (pair->ca_path != NULL) {
    if (tt_remove_file(pair->ca_path) != 0 && result == SALTS_OK)
      result = SALTS_EIO;
    free(pair->ca_path);
  }
  if (pair->cert_path != NULL) {
    if (tt_remove_file(pair->cert_path) != 0 && result == SALTS_OK)
      result = SALTS_EIO;
    free(pair->cert_path);
  }
  if (pair->key_path != NULL) {
    if (tt_remove_file(pair->key_path) != 0 && result == SALTS_OK)
      result = SALTS_EIO;
    free(pair->key_path);
  }
  memset(pair, 0, sizeof(*pair));
  return result;
}

static int bench_pair_warm_tls(bench_pair_t *pair) {
  static const unsigned char probe[] = {0x5au};
  unsigned char received[sizeof(probe)] = {0};
  size_t received_size = 0u;
  int status = SALTS_EBUSY;

  if (pair == NULL || pair->sender == NULL || pair->receiver == NULL)
    return SALTS_EINVAL;

  for (size_t i = 0u;
       (status == SALTS_EBUSY || status == SALTS_ENOBUFS) &&
       i < BENCH_PROGRESS_LIMIT; ++i) {
    flowmq_pollitem_t items[] = {
        {.socket = pair->sender}, {.socket = pair->receiver}};
    size_t ready = 0u;
    status = flowmq_poll(items, 2u, 1u, &ready);
    if (status == SALTS_OK)
      status = flowmq_send(pair->sender, probe, sizeof(probe),
                           FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;

  status = SALTS_EBUSY;
  for (size_t i = 0u; status == SALTS_EBUSY &&
                      i < BENCH_PROGRESS_LIMIT; ++i) {
    flowmq_pollitem_t items[] = {
        {.socket = pair->sender}, {.socket = pair->receiver}};
    size_t ready = 0u;
    status = flowmq_poll(items, 2u, 1u, &ready);
    if (status == SALTS_OK)
      status = flowmq_recv(pair->receiver, received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;
  return received_size == sizeof(probe) &&
                 memcmp(received, probe, sizeof(probe)) == 0
             ? SALTS_OK
             : SALTS_EPROTO;
}

static int bench_pair_open_tls(bench_pair_t *pair) {
  char endpoint[128];
  size_t endpoint_size = 0u;
  int status;
  memset(pair, 0, sizeof(*pair));
  pair->ca_path = tt_make_temp_file("flowmq-bench-ca", ".pem");
  pair->cert_path = tt_make_temp_file("flowmq-bench-cert", ".pem");
  pair->key_path = tt_make_temp_file("flowmq-bench-key", ".pem");
  if (pair->ca_path == NULL || pair->cert_path == NULL ||
      pair->key_path == NULL) {
    (void)bench_pair_close(pair);
    return SALTS_ENOMEM;
  }
  if (tt_write_file(pair->ca_path, FLOWMQ_TLS_TEST_ROOT_CA,
                    sizeof(FLOWMQ_TLS_TEST_ROOT_CA) - 1u) != 0 ||
      tt_write_file(pair->cert_path, FLOWMQ_TLS_TEST_CERTIFICATE,
                    sizeof(FLOWMQ_TLS_TEST_CERTIFICATE) - 1u) != 0 ||
      tt_write_file(pair->key_path, FLOWMQ_TLS_TEST_KEY,
                    sizeof(FLOWMQ_TLS_TEST_KEY) - 1u) != 0) {
    (void)bench_pair_close(pair);
    return SALTS_EIO;
  }

  pair->ctx = flowmq_ctx_new();
  if (pair->ctx == NULL) {
    (void)bench_pair_close(pair);
    return SALTS_ENOMEM;
  }
  pair->sender = flowmq_socket(pair->ctx, FLOWMQ_PAIR);
  pair->receiver = flowmq_socket(pair->ctx, FLOWMQ_PAIR);
  if (pair->sender == NULL || pair->receiver == NULL) {
    (void)bench_pair_close(pair);
    return SALTS_ENOMEM;
  }

  status = flowmq_setsockopt(pair->receiver, FLOWMQ_TLS_CERT_FILE,
                             pair->cert_path, strlen(pair->cert_path));
  if (status == SALTS_OK)
    status = flowmq_setsockopt(pair->receiver, FLOWMQ_TLS_KEY_FILE,
                               pair->key_path, strlen(pair->key_path));
  if (status == SALTS_OK)
    status = flowmq_setsockopt(pair->sender, FLOWMQ_TLS_CA_FILE,
                               pair->ca_path, strlen(pair->ca_path));
  if (status == SALTS_OK)
    status = flowmq_setsockopt(pair->sender, FLOWMQ_TLS_SERVER_NAME,
                               "localhost", strlen("localhost"));
  if (status == SALTS_OK)
    status = flowmq_bind(pair->receiver, "tls://127.0.0.1:0");
  if (status == SALTS_OK)
    status = flowmq_last_endpoint(pair->receiver, endpoint, sizeof(endpoint),
                                  &endpoint_size);
  if (status == SALTS_OK)
    status = flowmq_connect(pair->sender, endpoint);
  if (status == SALTS_OK)
    status = bench_pair_warm_tls(pair);
  if (status != SALTS_OK) {
    (void)bench_pair_close(pair);
    return status;
  }
  return SALTS_OK;
}

static int bench_exchange(bench_pair_t *pair, const void *payload,
                          size_t payload_size) {
  static unsigned char received[BENCH_RETAINED_LARGE_PAYLOAD_BYTES];
  const uint64_t deadline_ms = bench_progress_deadline_ms();
  size_t received_size = 0u;
  int status = flowmq_send(pair->sender, payload, payload_size,
                           FLOWMQ_DONTWAIT);
  while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    status = bench_progress_wait(pair, deadline_ms, 1);
    if (status != SALTS_OK) break;
    status = flowmq_send(pair->sender, payload, payload_size,
                         FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) {
    fprintf(stderr, "bench_exchange send/progress failed payload=%zu status=%d\n",
            payload_size, status);
    return status;
  }

  status = SALTS_EBUSY;
  while (status == SALTS_EBUSY) {
    status = bench_progress_wait(pair, deadline_ms, 0);
    if (status != SALTS_OK) break;
    status = flowmq_recv(pair->receiver, received, sizeof(received),
                         &received_size, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) {
    fprintf(stderr,
            "bench_exchange recv/progress failed payload=%zu status=%d received=%zu\n",
            payload_size, status, received_size);
    return status;
  }
  if (received_size != payload_size ||
      memcmp(received, payload, payload_size) != 0) {
    fprintf(stderr, "bench_exchange payload mismatch expected=%zu received=%zu\n",
            payload_size, received_size);
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int bench_exchange_owned_recv(bench_pair_t *pair,
                                     const void *payload,
                                     size_t payload_size) {
  mem_slice_t received = {0};
  const uint64_t deadline_ms = bench_progress_deadline_ms();
  int result = SALTS_OK;
  int status = flowmq_send(pair->sender, payload, payload_size,
                           FLOWMQ_DONTWAIT);
  while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    status = bench_progress_wait(pair, deadline_ms, 1);
    if (status != SALTS_OK) break;
    status = flowmq_send(pair->sender, payload, payload_size,
                         FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;

  status = SALTS_EBUSY;
  while (status == SALTS_EBUSY) {
    status = bench_progress_wait(pair, deadline_ms, 0);
    if (status != SALTS_OK) break;
    status = flowmq_recv_slice(pair->receiver, &received, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;
  if (received.length != payload_size ||
      (payload_size != 0u &&
       memcmp(received.data, payload, payload_size) != 0))
    result = SALTS_EPROTO;
  mem_slice_release(&received);
  return result;
}

static int bench_exchange_retained(bench_pair_t *pair,
                                   const mem_slice_t *payload) {
  static unsigned char received[BENCH_RETAINED_LARGE_PAYLOAD_BYTES];
  const uint64_t deadline_ms = bench_progress_deadline_ms();
  size_t received_size = 0u;
  int status;
  if (payload == NULL || payload->data == NULL || payload->length == 0u)
    return SALTS_EINVAL;
  if (payload->length > sizeof(received)) return SALTS_EMSGSIZE;

  status = flowmq_send_slice(pair->sender, payload, FLOWMQ_DONTWAIT);
  while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    status = bench_progress_wait(pair, deadline_ms, 1);
    if (status != SALTS_OK) break;
    status = flowmq_send_slice(pair->sender, payload, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;

  status = SALTS_EBUSY;
  while (status == SALTS_EBUSY) {
    status = bench_progress_wait(pair, deadline_ms, 0);
    if (status != SALTS_OK) break;
    status = flowmq_recv(pair->receiver, received, sizeof(received),
                         &received_size, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;
  if (received_size != payload->length ||
      memcmp(received, payload->data, payload->length) != 0)
    return SALTS_EPROTO;
  return SALTS_OK;
}

static int bench_receive_part(bench_pair_t *pair, const void *expected,
                              size_t expected_size, int expected_more) {
  static unsigned char received[BENCH_RETAINED_LARGE_PAYLOAD_BYTES];
  const uint64_t deadline_ms = bench_progress_deadline_ms();
  size_t received_size = 0u;
  size_t option_size = sizeof(int);
  int more = -1;
  int status = SALTS_EBUSY;

  if (expected == NULL || expected_size == 0u ||
      expected_size > sizeof(received))
    return SALTS_EINVAL;
  while (status == SALTS_EBUSY) {
    status = bench_progress_wait(pair, deadline_ms, 0);
    if (status != SALTS_OK) break;
    status = flowmq_recv(pair->receiver, received, sizeof(received),
                         &received_size, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;
  if (received_size != expected_size ||
      memcmp(received, expected, expected_size) != 0)
    return SALTS_EPROTO;
  status = flowmq_getsockopt(pair->receiver, FLOWMQ_RCVMORE, &more,
                             &option_size);
  if (status != SALTS_OK) return status;
  return more == expected_more ? SALTS_OK : SALTS_EPROTO;
}

static int bench_exchange_multipart_copy(bench_pair_t *pair,
                                         const unsigned char *payload,
                                         size_t part_size,
                                         size_t part_count) {
  const uint64_t deadline_ms = bench_progress_deadline_ms();
  int status;
  if (pair == NULL || payload == NULL || part_size == 0u || part_count < 2u)
    return SALTS_EINVAL;

  for (size_t part = 0u; part < part_count; ++part) {
    const int flags =
        FLOWMQ_DONTWAIT | (part + 1u < part_count ? FLOWMQ_SNDMORE : 0);
    status = flowmq_send(pair->sender, payload + part * part_size,
                         part_size, flags);
    while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
      status = bench_progress_wait(pair, deadline_ms, 1);
      if (status != SALTS_OK) break;
      status = flowmq_send(pair->sender, payload + part * part_size,
                           part_size, flags);
    }
    if (status != SALTS_OK) return status;
  }

  for (size_t part = 0u; part < part_count; ++part) {
    status = bench_receive_part(pair, payload + part * part_size, part_size,
                                part + 1u < part_count);
    if (status != SALTS_OK) return status;
  }
  return SALTS_OK;
}

static int bench_exchange_multipart_retained(bench_pair_t *pair,
                                             const mem_slice_t *parts,
                                             size_t part_count) {
  const uint64_t deadline_ms = bench_progress_deadline_ms();
  int status;
  if (pair == NULL || parts == NULL || part_count < 2u) return SALTS_EINVAL;

  for (size_t part = 0u; part < part_count; ++part) {
    const int flags =
        FLOWMQ_DONTWAIT | (part + 1u < part_count ? FLOWMQ_SNDMORE : 0);
    status = flowmq_send_slice(pair->sender, &parts[part], flags);
    while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
      status = bench_progress_wait(pair, deadline_ms, 1);
      if (status != SALTS_OK) break;
      status = flowmq_send_slice(pair->sender, &parts[part], flags);
    }
    if (status != SALTS_OK) return status;
  }

  for (size_t part = 0u; part < part_count; ++part) {
    status = bench_receive_part(pair, parts[part].data, parts[part].length,
                                part + 1u < part_count);
    if (status != SALTS_OK) return status;
  }
  return SALTS_OK;
}

static int bench_exchange_batch(bench_pair_t *pair, const void *payload,
                                size_t payload_size) {
  static unsigned char received[BENCH_LARGE_PAYLOAD_BYTES];
  size_t received_size = 0u;
  int status;
  for (size_t i = 0u; i < BENCH_BATCH_MESSAGES; ++i) {
    status = flowmq_send(pair->sender, payload, payload_size, FLOWMQ_DONTWAIT);
    if (status != SALTS_OK) return status;
  }
  for (size_t message = 0u; message < BENCH_BATCH_MESSAGES; ++message) {
    status = flowmq_recv(pair->receiver, received, sizeof(received),
                         &received_size, FLOWMQ_DONTWAIT);
    for (size_t i = 0u; status == SALTS_EBUSY && i < BENCH_PROGRESS_LIMIT;
         ++i) {
      status = bench_progress(pair);
      if (status == SALTS_OK)
        status = flowmq_recv(pair->receiver, received, sizeof(received),
                             &received_size, FLOWMQ_DONTWAIT);
    }
    if (status != SALTS_OK || received_size != payload_size ||
        memcmp(received, payload, payload_size) != 0)
      return status == SALTS_OK ? SALTS_EPROTO : status;
  }

  /*
   * Flow credit is receiver-driven and may become interval-eligible only
   * after the application consumes the final message. Two owner passes let
   * the receiver publish a pending FLOW_UPDATE and then let the sender consume
   * it before the next 64-message admission burst.
   */
  status = bench_progress(pair);
  if (status != SALTS_OK) return status;
  return bench_progress(pair);
}

typedef enum bench_tls_evidence_kind {
  BENCH_TLS_EVIDENCE_COPY = 0,
  BENCH_TLS_EVIDENCE_RETAINED,
  BENCH_TLS_EVIDENCE_MULTIPART_COPY,
  BENCH_TLS_EVIDENCE_MULTIPART_RETAINED
} bench_tls_evidence_kind;

typedef struct bench_tls_evidence_workload {
  const char *name;
  bench_tls_evidence_kind kind;
  const unsigned char *payload;
  const mem_slice_t *slice;
  const mem_slice_t *parts;
  size_t payload_bytes;
  size_t part_size;
  size_t part_count;
  size_t samples;
} bench_tls_evidence_workload;

typedef struct bench_tls_evidence_result {
  const char *name;
  size_t payload_bytes;
  size_t samples;
  uint64_t p50_ns;
  uint64_t p95_ns;
  double mib_per_second;
} bench_tls_evidence_result;

static size_t bench_tls_evidence_samples(size_t regular, size_t ci_samples) {
  const char *smoke = getenv("FLOWMQ_BENCH_SMOKE");
  const char *ci = getenv("FLOWMQ_BENCH_CI");
  if (smoke != NULL && strcmp(smoke, "0") != 0) return 3u;
  if (ci != NULL && strcmp(ci, "0") != 0) return ci_samples;
  return regular;
}

static int bench_tls_evidence_u64_compare(const void *left, const void *right) {
  const uint64_t a = *(const uint64_t *)left;
  const uint64_t b = *(const uint64_t *)right;
  return a < b ? -1 : a > b ? 1 : 0;
}

static uint64_t bench_tls_evidence_percentile(uint64_t *values, size_t count,
                                              unsigned percentile) {
  size_t index;
  qsort(values, count, sizeof(*values), bench_tls_evidence_u64_compare);
  index = ((count - 1u) * (size_t)percentile + 50u) / 100u;
  return values[index];
}

static int bench_tls_evidence_exchange(
    bench_pair_t *pair, const bench_tls_evidence_workload *workload) {
  switch (workload->kind) {
    case BENCH_TLS_EVIDENCE_COPY:
      return bench_exchange(pair, workload->payload, workload->payload_bytes);
    case BENCH_TLS_EVIDENCE_RETAINED:
      return bench_exchange_retained(pair, workload->slice);
    case BENCH_TLS_EVIDENCE_MULTIPART_COPY:
      return bench_exchange_multipart_copy(
          pair, workload->payload, workload->part_size, workload->part_count);
    case BENCH_TLS_EVIDENCE_MULTIPART_RETAINED:
      return bench_exchange_multipart_retained(
          pair, workload->parts, workload->part_count);
  }
  return SALTS_EINVAL;
}

static int bench_tls_evidence_measure(
    const bench_tls_evidence_workload *workload,
    bench_tls_evidence_result *out) {
  enum { BENCH_TLS_EVIDENCE_MAX_SAMPLES = 128u, BENCH_TLS_EVIDENCE_WARMUPS = 3u };
  bench_pair_t pair;
  uint64_t latencies[BENCH_TLS_EVIDENCE_MAX_SAMPLES];
  uint64_t elapsed_total = 0u;
  int status;

  if (workload == NULL || out == NULL || workload->name == NULL ||
      workload->payload_bytes == 0u || workload->samples == 0u ||
      workload->samples > BENCH_TLS_EVIDENCE_MAX_SAMPLES)
    return SALTS_EINVAL;

  memset(&pair, 0, sizeof(pair));
  memset(latencies, 0, sizeof(latencies));
  status = bench_pair_open_tls(&pair);
  if (status != SALTS_OK) return status;

  for (size_t warmup = 0u; warmup < BENCH_TLS_EVIDENCE_WARMUPS; ++warmup) {
    status = bench_tls_evidence_exchange(&pair, workload);
    if (status != SALTS_OK) goto cleanup;
  }

  for (size_t sample = 0u; sample < workload->samples; ++sample) {
    const uint64_t started = cmeta_hrtime();
    uint64_t elapsed;
    status = bench_tls_evidence_exchange(&pair, workload);
    if (status != SALTS_OK) {
      fprintf(stderr,
              "TLS_EVIDENCE_FAIL name=%s sample=%zu status=%d\n",
              workload->name, sample, status);
      goto cleanup;
    }
    elapsed = cmeta_hrtime() - started;
    if (elapsed == 0u || elapsed > UINT64_MAX - elapsed_total) {
      status = SALTS_ERANGE;
      goto cleanup;
    }
    latencies[sample] = elapsed;
    elapsed_total += elapsed;
  }

  *out = (bench_tls_evidence_result){
      .name = workload->name,
      .payload_bytes = workload->payload_bytes,
      .samples = workload->samples,
      .p50_ns = bench_tls_evidence_percentile(
          latencies, workload->samples, 50u),
      .p95_ns = bench_tls_evidence_percentile(
          latencies, workload->samples, 95u),
      .mib_per_second =
          ((double)workload->payload_bytes * (double)workload->samples /
           (1024.0 * 1024.0)) *
          1.0e9 / (double)elapsed_total};

cleanup:
  {
    const int close_status = bench_pair_close(&pair);
    if (status == SALTS_OK && close_status != SALTS_OK) status = close_status;
  }
  return status;
}

static void bench_tls_evidence_print(const bench_tls_evidence_result *result) {
  printf("TLS_EVIDENCE name=%s payload_bytes=%zu samples=%zu "
         "p50_us=%.3f p95_us=%.3f mib_per_second=%.2f\n",
         result->name, result->payload_bytes, result->samples,
         (double)result->p50_ns / 1000.0,
         (double)result->p95_ns / 1000.0,
         result->mib_per_second);
}

typedef enum bench_owned_rx_mode_e {
  BENCH_OWNED_RX_COPY = 0,
  BENCH_OWNED_RX_SLICE = 1,
  BENCH_OWNED_RX_SLICEV = 2
} bench_owned_rx_mode_t;

static const char BENCH_RX_COPIED_FALLBACK[] = "copied_fallback";

static const char BENCH_RX_MIXED_FALLBACK[] = "mixed_or_fallback";

typedef struct bench_owned_rx_result_s {
  const char *mode;
  const char *contract_basis;
  size_t payload_bytes;
  size_t samples;
  uint64_t p50_ns;
  uint64_t p95_ns;
  double mib_per_second;
  size_t subrange_hits;
  size_t base_aligned_hits;
  size_t multi_range_hits;
  size_t range_count_min;
  size_t range_count_max;
  int payload_copy_contract;
  int shape_observable;
} bench_owned_rx_result_t;

static const char *bench_owned_rx_mode_name(bench_owned_rx_mode_t mode) {
  switch (mode) {
    case BENCH_OWNED_RX_COPY:
      return "copy";
    case BENCH_OWNED_RX_SLICE:
      return "slice";
    case BENCH_OWNED_RX_SLICEV:
      return "slicev";
  }
  return "invalid";
}

static size_t bench_owned_rx_samples(size_t payload_bytes) {
  if (payload_bytes <= BENCH_PAYLOAD_BYTES)
    return bench_tls_evidence_samples(101u, 31u);
  if (payload_bytes <= BENCH_LARGE_PAYLOAD_BYTES)
    return bench_tls_evidence_samples(61u, 31u);
  return bench_tls_evidence_samples(31u, 15u);
}

static int bench_owned_rx_exchange(
    bench_pair_t *pair, const void *payload, size_t payload_size,
    bench_owned_rx_mode_t mode, int *out_direct, size_t *out_range_count) {
  enum { BENCH_OWNED_RX_VECTOR_CAPACITY = 64u };
  static unsigned char received_copy[BENCH_RETAINED_LARGE_PAYLOAD_BYTES];
  mem_slice_t received_slice = {0};
  mem_slice_t received_segments[BENCH_OWNED_RX_VECTOR_CAPACITY] = {{0}};
  const uint64_t deadline_ms = bench_progress_deadline_ms();
  size_t received_size = 0u;
  size_t range_count = 0u;
  int status;

  if (pair == NULL || payload == NULL || payload_size == 0u ||
      payload_size > sizeof(received_copy) || out_direct == NULL ||
      out_range_count == NULL)
    return SALTS_EINVAL;
  *out_direct = 0;
  *out_range_count = 0u;

  status = flowmq_send(pair->sender, payload, payload_size, FLOWMQ_DONTWAIT);
  while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    status = bench_progress_wait(pair, deadline_ms, 1);
    if (status != SALTS_OK) return status;
    status = flowmq_send(pair->sender, payload, payload_size, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;

  if (mode == BENCH_OWNED_RX_COPY) {
    status = SALTS_EBUSY;
    while (status == SALTS_EBUSY) {
      status = bench_progress_wait(pair, deadline_ms, 0);
      if (status != SALTS_OK) return status;
      status = flowmq_recv(pair->receiver, received_copy,
                           sizeof(received_copy), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    if (status != SALTS_OK) return status;
    if (received_size != payload_size ||
        memcmp(received_copy, payload, payload_size) != 0)
      return SALTS_EPROTO;
    return SALTS_OK;
  }

  if (mode == BENCH_OWNED_RX_SLICE) {
    status = SALTS_EBUSY;
    while (status == SALTS_EBUSY) {
      status = bench_progress_wait(pair, deadline_ms, 0);
      if (status != SALTS_OK) return status;
      status =
          flowmq_recv_slice(pair->receiver, &received_slice, FLOWMQ_DONTWAIT);
    }
    if (status != SALTS_OK) return status;
    if (received_slice.buffer == NULL || received_slice.data == NULL ||
        received_slice.length != payload_size ||
        memcmp(received_slice.data, payload, payload_size) != 0) {
      mem_slice_release(&received_slice);
      return SALTS_EPROTO;
    }
    *out_direct =
        received_slice.data !=
        (const void *)mem_buffer_data(received_slice.buffer);
    *out_range_count = 1u;
    mem_slice_release(&received_slice);
    return SALTS_OK;
  }

  if (mode != BENCH_OWNED_RX_SLICEV) return SALTS_EINVAL;

  status = SALTS_EBUSY;
  while (status == SALTS_EBUSY) {
    status = bench_progress_wait(pair, deadline_ms, 0);
    if (status != SALTS_OK) return status;
    range_count = 0u;
    status = flowmq_recv_slicev(
        pair->receiver, received_segments, BENCH_OWNED_RX_VECTOR_CAPACITY,
        &range_count, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;
  if (range_count == 0u || range_count > BENCH_OWNED_RX_VECTOR_CAPACITY)
    status = SALTS_EPROTO;

  {
    size_t offset = 0u;
    for (size_t i = 0u; status == SALTS_OK && i < range_count; ++i) {
      if (received_segments[i].buffer == NULL ||
          received_segments[i].data == NULL ||
          received_segments[i].length == 0u ||
          received_segments[i].length > payload_size - offset ||
          memcmp(received_segments[i].data,
                 (const unsigned char *)payload + offset,
                 received_segments[i].length) != 0) {
        status = SALTS_EPROTO;
        break;
      }
      offset += received_segments[i].length;
    }
    if (status == SALTS_OK && offset != payload_size)
      status = SALTS_EPROTO;
  }

  if (status == SALTS_OK) {
    *out_range_count = range_count;
    *out_direct =
        range_count > 1u ||
        received_segments[0].data !=
            (const void *)mem_buffer_data(received_segments[0].buffer);
  }
  for (size_t i = 0u; i < range_count; ++i)
    mem_slice_release(&received_segments[i]);
  return status;
}

static int bench_owned_rx_measure(
    const void *payload, size_t payload_size, bench_owned_rx_mode_t mode,
    bench_owned_rx_result_t *out) {
  enum {
    BENCH_OWNED_RX_MAX_SAMPLES = 128u,
    BENCH_OWNED_RX_WARMUPS = 3u
  };
  bench_pair_t pair;
  uint64_t latencies[BENCH_OWNED_RX_MAX_SAMPLES];
  size_t samples = bench_owned_rx_samples(payload_size);
  uint64_t elapsed_total = 0u;
  size_t subrange_hits = 0u;
  size_t base_aligned_hits = 0u;
  size_t multi_range_hits = 0u;
  size_t range_count_min = SIZE_MAX;
  size_t range_count_max = 0u;
  int status;

  if (payload == NULL || payload_size == 0u || out == NULL)
    return SALTS_EINVAL;
  if (mode == BENCH_OWNED_RX_SLICEV &&
      payload_size > FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE)
    samples = bench_tls_evidence_samples(128u, 128u);
  if (samples == 0u || samples > BENCH_OWNED_RX_MAX_SAMPLES)
    return SALTS_EINVAL;

  memset(&pair, 0, sizeof(pair));
  memset(latencies, 0, sizeof(latencies));
  status = bench_pair_open(&pair);
  if (status != SALTS_OK) return status;

  for (size_t warmup = 0u; warmup < BENCH_OWNED_RX_WARMUPS; ++warmup) {
    int direct = 0;
    size_t range_count = 0u;
    status = bench_owned_rx_exchange(
        &pair, payload, payload_size, mode, &direct, &range_count);
    if (status != SALTS_OK) goto cleanup;
  }

  for (size_t sample = 0u; sample < samples; ++sample) {
    const uint64_t started = cmeta_hrtime();
    uint64_t elapsed;
    int direct = 0;
    size_t range_count = 0u;
    status = bench_owned_rx_exchange(
        &pair, payload, payload_size, mode, &direct, &range_count);
    if (status != SALTS_OK) {
      fprintf(stderr,
              "OWNED_RX_EVIDENCE_FAIL mode=%s payload_bytes=%zu "
              "sample=%zu status=%d\n",
              bench_owned_rx_mode_name(mode), payload_size, sample, status);
      goto cleanup;
    }
    elapsed = cmeta_hrtime() - started;
    if (elapsed == 0u || elapsed > UINT64_MAX - elapsed_total) {
      status = SALTS_ERANGE;
      goto cleanup;
    }
    latencies[sample] = elapsed;
    elapsed_total += elapsed;

    if (mode != BENCH_OWNED_RX_COPY) {
      if (range_count == 0u) {
        status = SALTS_EPROTO;
        goto cleanup;
      }
      if (range_count < range_count_min) range_count_min = range_count;
      if (range_count > range_count_max) range_count_max = range_count;
      if (range_count > 1u)
        ++multi_range_hits;
      else if (direct)
        ++subrange_hits;
      else
        ++base_aligned_hits;
    }
  }

  {
    const int slice_mode = mode == BENCH_OWNED_RX_SLICE;
    const int slicev_mode = mode == BENCH_OWNED_RX_SLICEV;
    const int all_subrange =
        (slice_mode || slicev_mode) &&
        subrange_hits == samples && base_aligned_hits == 0u &&
        multi_range_hits == 0u;
    const int all_vector =
        slicev_mode && multi_range_hits == samples &&
        subrange_hits == 0u && base_aligned_hits == 0u;
    const int segmented_legacy =
        slice_mode &&
        payload_size > FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE &&
        base_aligned_hits == samples;
    const int zero_copy_vector =
        slicev_mode && base_aligned_hits == 0u &&
        subrange_hits + multi_range_hits == samples;
    const int copied_fallback =
        (slice_mode || slicev_mode) && base_aligned_hits == samples &&
        subrange_hits == 0u && multi_range_hits == 0u;

    const char *contract_basis = BENCH_RX_MIXED_FALLBACK;
    if (mode == BENCH_OWNED_RX_COPY)
      contract_basis = "caller_copy";
    else if (slice_mode && all_subrange)
      contract_basis = "owned_subrange";
    else if (segmented_legacy)
      contract_basis = "segmented_legacy_coalesce";
    else if (copied_fallback)
      contract_basis = BENCH_RX_COPIED_FALLBACK;
    else if (all_vector)
      contract_basis = "owned_vector";
    else if (slicev_mode && all_subrange)
      contract_basis = "owned_single_range";
    else if (slicev_mode && multi_range_hits != 0u && base_aligned_hits != 0u)
      contract_basis = "mixed_vector_or_fallback";
    else if (zero_copy_vector)
      contract_basis = "owned_vector_mixed_ranges";

    *out = (bench_owned_rx_result_t){
        .mode = bench_owned_rx_mode_name(mode),
        .contract_basis = contract_basis,
        .payload_bytes = payload_size,
        .samples = samples,
        .p50_ns = bench_tls_evidence_percentile(latencies, samples, 50u),
        .p95_ns = bench_tls_evidence_percentile(latencies, samples, 95u),
        .mib_per_second =
            ((double)payload_size * (double)samples /
             (1024.0 * 1024.0)) *
            1.0e9 / (double)elapsed_total,
        .subrange_hits = mode == BENCH_OWNED_RX_COPY ? 0u : subrange_hits,
        .base_aligned_hits =
            mode == BENCH_OWNED_RX_COPY ? 0u : base_aligned_hits,
        .multi_range_hits =
            mode == BENCH_OWNED_RX_COPY ? 0u : multi_range_hits,
        .range_count_min =
            mode == BENCH_OWNED_RX_COPY ? 0u : range_count_min,
        .range_count_max =
            mode == BENCH_OWNED_RX_COPY ? 0u : range_count_max,
        .payload_copy_contract =
            mode == BENCH_OWNED_RX_COPY
                ? 1
                : (slice_mode
                       ? (all_subrange ? 0 : (copied_fallback ? 1 : -1))
                       : (zero_copy_vector ? 0 : (copied_fallback ? 1 : -1))),
        .shape_observable = mode == BENCH_OWNED_RX_COPY ? 0 : 1};
  }

cleanup:
  {
    const int close_status = bench_pair_close(&pair);
    if (status == SALTS_OK && close_status != SALTS_OK) status = close_status;
  }
  return status;
}

static int bench_owned_rx_contract_validate(
    const bench_owned_rx_result_t *result) {
  const char *expected_basis;
  int expected_copy;
  if (result == NULL || result->mode == NULL || result->contract_basis == NULL ||
      result->samples == 0u)
    return SALTS_EINVAL;

  if (strcmp(result->mode, "copy") == 0)
    return result->payload_copy_contract == 1 &&
                   result->range_count_min == 0u &&
                   result->range_count_max == 0u &&
                   strcmp(result->contract_basis, "caller_copy") == 0
               ? SALTS_OK
               : SALTS_EPROTO;

  if (result->range_count_min == 0u ||
      result->range_count_max < result->range_count_min ||
      result->subrange_hits > result->samples ||
      result->base_aligned_hits > result->samples - result->subrange_hits ||
      result->multi_range_hits !=
          result->samples - result->subrange_hits - result->base_aligned_hits)
    return SALTS_EPROTO;

  expected_copy = result->base_aligned_hits == 0u
                      ? 0
                      : (result->base_aligned_hits == result->samples ? 1 : -1);
  /* TCP fragmentation determines storage shape at every payload size. Validate
   * the observed ownership accounting, never require a particular hit rate. */
  if (strcmp(result->mode, "slice") == 0) {
    if (result->range_count_min != 1u || result->range_count_max != 1u ||
        result->multi_range_hits != 0u)
      return SALTS_EPROTO;
    if (result->subrange_hits == result->samples)
      expected_basis = "owned_subrange";
    else if (result->base_aligned_hits == result->samples)
      expected_basis = result->payload_bytes > FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE
                           ? "segmented_legacy_coalesce"
                           : BENCH_RX_COPIED_FALLBACK;
    else
      expected_basis = BENCH_RX_MIXED_FALLBACK;
  } else if (strcmp(result->mode, "slicev") == 0) {
    if (result->multi_range_hits == 0u) {
      if (result->range_count_min != 1u || result->range_count_max != 1u)
        return SALTS_EPROTO;
    } else if (result->multi_range_hits == result->samples) {
      if (result->range_count_min <= 1u) return SALTS_EPROTO;
    } else if (result->range_count_min != 1u || result->range_count_max <= 1u) {
      return SALTS_EPROTO;
    }

    if (result->base_aligned_hits == result->samples)
      expected_basis = BENCH_RX_COPIED_FALLBACK;
    else if (result->multi_range_hits == result->samples)
      expected_basis = "owned_vector";
    else if (result->subrange_hits == result->samples)
      expected_basis = "owned_single_range";
    else if (result->base_aligned_hits == 0u)
      expected_basis = "owned_vector_mixed_ranges";
    else if (result->multi_range_hits != 0u)
      expected_basis = "mixed_vector_or_fallback";
    else
      expected_basis = BENCH_RX_MIXED_FALLBACK;
  } else {
    return SALTS_EPROTO;
  }

  return result->payload_copy_contract == expected_copy &&
                 strcmp(result->contract_basis, expected_basis) == 0
             ? SALTS_OK
             : SALTS_EPROTO;
}

static void bench_owned_rx_print(const bench_owned_rx_result_t *result) {
  printf(
      "OWNED_RX_EVIDENCE mode=%s payload_bytes=%zu samples=%zu "
      "p50_us=%.3f p95_us=%.3f mib_per_second=%.2f "
      "shape_observable=%d subrange_hits=%zu base_aligned_hits=%zu "
      "multi_range_hits=%zu range_count_min=%zu range_count_max=%zu "
      "direct_hits=%zu payload_copy_contract=%d contract_basis=%s\n",
      result->mode, result->payload_bytes, result->samples,
      (double)result->p50_ns / 1000.0,
      (double)result->p95_ns / 1000.0,
      result->mib_per_second, result->shape_observable,
      result->subrange_hits, result->base_aligned_hits,
      result->multi_range_hits, result->range_count_min,
      result->range_count_max,
      result->subrange_hits + result->multi_range_hits,
      result->payload_copy_contract, result->contract_basis);
}

#if defined(FLOWMQ_BENCH_WITH_ZMQ)
typedef struct bench_zmq_pair_s {
  void *ctx;
  void *sender;
  void *receiver;
} bench_zmq_pair_t;

static int bench_zmq_open(bench_zmq_pair_t *pair) {
  char endpoint[256];
  size_t endpoint_size = sizeof(endpoint);
  int linger = 0;
  memset(pair, 0, sizeof(*pair));
  pair->ctx = zmq_ctx_new();
  if (pair->ctx == NULL) return -1;
  pair->sender = zmq_socket(pair->ctx, ZMQ_PAIR);
  pair->receiver = zmq_socket(pair->ctx, ZMQ_PAIR);
  if (pair->sender == NULL || pair->receiver == NULL) return -1;
  if (zmq_setsockopt(pair->sender, ZMQ_LINGER, &linger, sizeof(linger)) != 0 ||
      zmq_setsockopt(pair->receiver, ZMQ_LINGER, &linger, sizeof(linger)) != 0 ||
      zmq_bind(pair->receiver, "tcp://127.0.0.1:*") != 0 ||
      zmq_getsockopt(pair->receiver, ZMQ_LAST_ENDPOINT, endpoint,
                     &endpoint_size) != 0 ||
      zmq_connect(pair->sender, endpoint) != 0)
    return -1;
  return 0;
}

static int bench_zmq_exchange(bench_zmq_pair_t *pair, const void *payload,
                              size_t payload_size) {
  static unsigned char received[BENCH_LARGE_PAYLOAD_BYTES];
  int sent = zmq_send(pair->sender, payload, payload_size, 0);
  int read;
  if (sent != (int)payload_size) return -1;
  read = zmq_recv(pair->receiver, received, sizeof(received), 0);
  if (read != (int)payload_size ||
      memcmp(received, payload, payload_size) != 0)
    return -1;
  return 0;
}

static int bench_zmq_exchange_batch(bench_zmq_pair_t *pair,
                                    const void *payload,
                                    size_t payload_size) {
  static unsigned char received[BENCH_LARGE_PAYLOAD_BYTES];
  for (size_t i = 0u; i < BENCH_BATCH_MESSAGES; ++i) {
    if (zmq_send(pair->sender, payload, payload_size, 0) != (int)payload_size)
      return -1;
  }
  for (size_t i = 0u; i < BENCH_BATCH_MESSAGES; ++i) {
    int read = zmq_recv(pair->receiver, received, sizeof(received), 0);
    if (read != (int)payload_size ||
        memcmp(received, payload, payload_size) != 0)
      return -1;
  }
  return 0;
}

static void bench_zmq_close(bench_zmq_pair_t *pair) {
  if (pair->sender != NULL) (void)zmq_close(pair->sender);
  if (pair->receiver != NULL) (void)zmq_close(pair->receiver);
  if (pair->ctx != NULL) (void)zmq_ctx_term(pair->ctx);
  memset(pair, 0, sizeof(*pair));
}
#endif

spec("FlowMQ direct socket benchmark") {
  it("receive evidence: accepts fragmentation without claiming zero-copy") {
    enum { EVIDENCE_SAMPLES = 31u, EVIDENCE_DIRECT_SAMPLES = 30u };
    bench_owned_rx_result_t observed[] = {
        {.mode = bench_owned_rx_mode_name(BENCH_OWNED_RX_SLICE),
         .contract_basis = BENCH_RX_MIXED_FALLBACK,
         .payload_bytes = BENCH_LARGE_PAYLOAD_BYTES,
         .samples = EVIDENCE_SAMPLES, .subrange_hits = EVIDENCE_DIRECT_SAMPLES,
         .base_aligned_hits = 1u, .range_count_min = 1u, .range_count_max = 1u,
         .payload_copy_contract = -1},
        {.mode = bench_owned_rx_mode_name(BENCH_OWNED_RX_SLICEV),
         .contract_basis = BENCH_RX_COPIED_FALLBACK,
         .payload_bytes = BENCH_RETAINED_LARGE_PAYLOAD_BYTES,
         .samples = EVIDENCE_SAMPLES, .base_aligned_hits = EVIDENCE_SAMPLES,
         .range_count_min = 1u, .range_count_max = 1u,
         .payload_copy_contract = 1}};
    for (size_t i = 0u; i < sizeof(observed) / sizeof(observed[0]); ++i) {
      check_equal(bench_owned_rx_contract_validate(&observed[i]), SALTS_OK);
      observed[i].payload_copy_contract = 0;
      check_equal(bench_owned_rx_contract_validate(&observed[i]), SALTS_EPROTO);
    }
  }

  bench("caller-driven loopback TCP") {
    static unsigned char payload[BENCH_PAYLOAD_BYTES];
    static unsigned char large_payload[BENCH_LARGE_PAYLOAD_BYTES];
    static unsigned char retained_large_payload[
        BENCH_RETAINED_LARGE_PAYLOAD_BYTES];
    static unsigned char multipart_small[
        BENCH_MULTIPART_SMALL_PARTS * BENCH_MULTIPART_SMALL_PART_BYTES];
    static unsigned char multipart_large[
        BENCH_MULTIPART_LARGE_PARTS * BENCH_MULTIPART_LARGE_PART_BYTES];
    const size_t samples = bench_socket_samples(BENCH_SAMPLES, 8u);
    const size_t large_samples =
        bench_socket_samples(BENCH_LARGE_SAMPLES, 4u);
    const size_t retained_large_samples =
        bench_socket_samples(BENCH_RETAINED_LARGE_SAMPLES, 2u);
    const size_t multipart_small_samples =
        bench_socket_samples(BENCH_MULTIPART_SMALL_SAMPLES, 2u);
    const size_t multipart_large_samples =
        bench_socket_samples(BENCH_MULTIPART_LARGE_SAMPLES, 1u);
    mem_buffer_t *payload_buffer;
    mem_buffer_t *large_buffer;
    mem_buffer_t *retained_large_buffer;
    mem_buffer_t *multipart_small_buffer;
    mem_buffer_t *multipart_large_buffer;
    mem_slice_t payload_slice;
    mem_slice_t large_slice;
    mem_slice_t retained_large_slice;
    mem_slice_t multipart_small_slices[BENCH_MULTIPART_SMALL_PARTS] = {0};
    mem_slice_t multipart_large_slices[BENCH_MULTIPART_LARGE_PARTS] = {0};
    bench_pair_t pair;
    int status;

    memset(payload, 0x5a, sizeof(payload));
    memset(large_payload, 0xa5, sizeof(large_payload));
    memset(retained_large_payload, 0x3c, sizeof(retained_large_payload));
    for (size_t i = 0u; i < sizeof(multipart_small); ++i)
      multipart_small[i] = (unsigned char)((i * 11u + 5u) & 0xffu);
    for (size_t i = 0u; i < sizeof(multipart_large); ++i)
      multipart_large[i] = (unsigned char)((i * 29u + 9u) & 0xffu);

    payload_buffer =
        mem_wrap_external(payload, sizeof(payload), NULL, NULL);
    large_buffer =
        mem_wrap_external(large_payload, sizeof(large_payload), NULL, NULL);
    retained_large_buffer =
        mem_wrap_external(retained_large_payload,
                          sizeof(retained_large_payload), NULL, NULL);
    multipart_small_buffer =
        mem_wrap_external(multipart_small, sizeof(multipart_small), NULL, NULL);
    multipart_large_buffer =
        mem_wrap_external(multipart_large, sizeof(multipart_large), NULL, NULL);
    check_not_null(payload_buffer);
    check_not_null(large_buffer);
    check_not_null(retained_large_buffer);
    check_not_null(multipart_small_buffer);
    check_not_null(multipart_large_buffer);
    payload_slice = mem_slice(payload_buffer, 0u, sizeof(payload));
    large_slice = mem_slice(large_buffer, 0u, sizeof(large_payload));
    retained_large_slice =
        mem_slice(retained_large_buffer, 0u, sizeof(retained_large_payload));
    check_not_null(payload_slice.buffer);
    check_not_null(large_slice.buffer);
    check_not_null(retained_large_slice.buffer);
    for (size_t part = 0u; part < BENCH_MULTIPART_SMALL_PARTS; ++part) {
      multipart_small_slices[part] =
          mem_slice(multipart_small_buffer,
                    part * BENCH_MULTIPART_SMALL_PART_BYTES,
                    BENCH_MULTIPART_SMALL_PART_BYTES);
      check_not_null(multipart_small_slices[part].buffer);
    }
    for (size_t part = 0u; part < BENCH_MULTIPART_LARGE_PARTS; ++part) {
      multipart_large_slices[part] =
          mem_slice(multipart_large_buffer,
                    part * BENCH_MULTIPART_LARGE_PART_BYTES,
                    BENCH_MULTIPART_LARGE_PART_BYTES);
      check_not_null(multipart_large_slices[part].buffer);
    }

    status = bench_pair_open(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange(&pair, payload, sizeof(payload)), SALTS_OK);

    benchmark_bytes("PAIR 64-byte copy immediate", samples,
                    BENCH_PAYLOAD_BYTES) {
      status = bench_exchange(&pair, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange_owned_recv(&pair, payload, sizeof(payload)),
                SALTS_OK);
    benchmark_bytes("PAIR 64-byte owned recv", samples,
                    BENCH_PAYLOAD_BYTES) {
      status = bench_exchange_owned_recv(&pair, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange_retained(&pair, &payload_slice), SALTS_OK);
    benchmark_bytes("PAIR 64-byte retained immediate", samples,
                    BENCH_PAYLOAD_BYTES) {
      status = bench_exchange_retained(&pair, &payload_slice);
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange(&pair, large_payload, sizeof(large_payload)),
                SALTS_OK);
    benchmark_bytes("PAIR 64-KiB copy one-way", large_samples,
                    BENCH_LARGE_PAYLOAD_BYTES) {
      status = bench_exchange(&pair, large_payload, sizeof(large_payload));
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange_owned_recv(
                    &pair, large_payload, sizeof(large_payload)), SALTS_OK);
    benchmark_bytes("PAIR 64-KiB owned recv", large_samples,
                    BENCH_LARGE_PAYLOAD_BYTES) {
      status = bench_exchange_owned_recv(
          &pair, large_payload, sizeof(large_payload));
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange(
                    &pair, retained_large_payload,
                    sizeof(retained_large_payload)), SALTS_OK);
    benchmark_bytes("PAIR 1-MiB copy immediate", retained_large_samples,
                    BENCH_RETAINED_LARGE_PAYLOAD_BYTES) {
      status = bench_exchange(&pair, retained_large_payload,
                              sizeof(retained_large_payload));
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange_owned_recv(
                    &pair, retained_large_payload,
                    sizeof(retained_large_payload)), SALTS_OK);
    benchmark_bytes("PAIR 1-MiB owned recv", retained_large_samples,
                    BENCH_RETAINED_LARGE_PAYLOAD_BYTES) {
      status = bench_exchange_owned_recv(
          &pair, retained_large_payload, sizeof(retained_large_payload));
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange_retained(&pair, &retained_large_slice),
                SALTS_OK);
    benchmark_bytes("PAIR 1-MiB retained immediate", retained_large_samples,
                    BENCH_RETAINED_LARGE_PAYLOAD_BYTES) {
      status = bench_exchange_retained(&pair, &retained_large_slice);
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_pair_close(&pair), SALTS_OK);
    status = bench_pair_open(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange_multipart_copy(
                    &pair, multipart_small, BENCH_MULTIPART_SMALL_PART_BYTES,
                    BENCH_MULTIPART_SMALL_PARTS), SALTS_OK);
    benchmark_bytes("PAIR 4x64-byte copy multipart", multipart_small_samples,
                    BENCH_MULTIPART_SMALL_PARTS *
                        BENCH_MULTIPART_SMALL_PART_BYTES) {
      status = bench_exchange_multipart_copy(
          &pair, multipart_small, BENCH_MULTIPART_SMALL_PART_BYTES,
          BENCH_MULTIPART_SMALL_PARTS);
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_pair_close(&pair), SALTS_OK);
    status = bench_pair_open(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange_multipart_retained(
                    &pair, multipart_small_slices,
                    BENCH_MULTIPART_SMALL_PARTS), SALTS_OK);
    benchmark_bytes("PAIR 4x64-byte retained multipart",
                    multipart_small_samples,
                    BENCH_MULTIPART_SMALL_PARTS *
                        BENCH_MULTIPART_SMALL_PART_BYTES) {
      status = bench_exchange_multipart_retained(
          &pair, multipart_small_slices, BENCH_MULTIPART_SMALL_PARTS);
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_pair_close(&pair), SALTS_OK);
    status = bench_pair_open(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange_multipart_copy(
                    &pair, multipart_large, BENCH_MULTIPART_LARGE_PART_BYTES,
                    BENCH_MULTIPART_LARGE_PARTS), SALTS_OK);
    benchmark_bytes("PAIR 2x256-KiB copy multipart", multipart_large_samples,
                    BENCH_MULTIPART_LARGE_PARTS *
                        BENCH_MULTIPART_LARGE_PART_BYTES) {
      status = bench_exchange_multipart_copy(
          &pair, multipart_large, BENCH_MULTIPART_LARGE_PART_BYTES,
          BENCH_MULTIPART_LARGE_PARTS);
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_pair_close(&pair), SALTS_OK);
    status = bench_pair_open(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange_multipart_retained(
                    &pair, multipart_large_slices,
                    BENCH_MULTIPART_LARGE_PARTS), SALTS_OK);
    benchmark_bytes("PAIR 2x256-KiB retained multipart",
                    multipart_large_samples,
                    BENCH_MULTIPART_LARGE_PARTS *
                        BENCH_MULTIPART_LARGE_PART_BYTES) {
      status = bench_exchange_multipart_retained(
          &pair, multipart_large_slices, BENCH_MULTIPART_LARGE_PARTS);
    }
    check_equal(status, SALTS_OK);

    /*
     * Keep the queued-batch workload independent from the high-volume
     * immediate benchmarks above. Those runs intentionally exercise flow
     * credit and terminal timing for hundreds of thousands of messages; the
     * batch benchmark measures fresh-session queue admission instead of
     * inheriting transient credit/update state from a different workload.
     */
    check_equal(bench_pair_close(&pair), SALTS_OK);
    status = bench_pair_open(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange(&pair, payload, sizeof(payload)), SALTS_OK);

    benchmark_io("PAIR 64-message queued batch",
                 bench_socket_samples(BENCH_BATCH_SAMPLES, 2u),
                 BENCH_BATCH_MESSAGES,
                 BENCH_BATCH_MESSAGES * BENCH_PAYLOAD_BYTES) {
      status = bench_exchange_batch(&pair, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);

    {
      const struct {
        const void *payload;
        size_t payload_size;
      } workloads[] = {
          {payload, sizeof(payload)},
          {large_payload, sizeof(large_payload)},
          {retained_large_payload, sizeof(retained_large_payload)}};

      printf("OWNED_RX_EVIDENCE_BEGIN transport=tcp\n");
      for (size_t index = 0u;
           index < sizeof(workloads) / sizeof(workloads[0]); ++index) {
        for (size_t mode = 0u; mode < 3u; ++mode) {
          bench_owned_rx_result_t result = {0};
          status = bench_owned_rx_measure(
              workloads[index].payload, workloads[index].payload_size,
              (bench_owned_rx_mode_t)mode, &result);
          check_equal(status, SALTS_OK);
          if (status != SALTS_OK) break;
          bench_owned_rx_print(&result);
          status = bench_owned_rx_contract_validate(&result);
          if (status != SALTS_OK) {
            fprintf(stderr,
                    "OWNED_RX_EVIDENCE_FAIL mode=%s payload_bytes=%zu "
                    "contract_status=%d\n",
                    result.mode, result.payload_bytes, status);
            break;
          }
        }
        if (status != SALTS_OK) break;
      }
      check_equal(status, SALTS_OK);
      printf("OWNED_RX_EVIDENCE_END transport=tcp\n");
    }

    {
      const bench_tls_evidence_workload workloads[] = {
          {.name = "copy_64b",
           .kind = BENCH_TLS_EVIDENCE_COPY,
           .payload = payload,
           .payload_bytes = sizeof(payload),
           .samples = bench_tls_evidence_samples(101u, 31u)},
          {.name = "retained_64b",
           .kind = BENCH_TLS_EVIDENCE_RETAINED,
           .slice = &payload_slice,
           .payload_bytes = sizeof(payload),
           .samples = bench_tls_evidence_samples(101u, 31u)},
          {.name = "copy_64k",
           .kind = BENCH_TLS_EVIDENCE_COPY,
           .payload = large_payload,
           .payload_bytes = sizeof(large_payload),
           .samples = bench_tls_evidence_samples(61u, 31u)},
          {.name = "retained_64k",
           .kind = BENCH_TLS_EVIDENCE_RETAINED,
           .slice = &large_slice,
           .payload_bytes = sizeof(large_payload),
           .samples = bench_tls_evidence_samples(61u, 31u)},
          {.name = "copy_1m",
           .kind = BENCH_TLS_EVIDENCE_COPY,
           .payload = retained_large_payload,
           .payload_bytes = sizeof(retained_large_payload),
           .samples = bench_tls_evidence_samples(31u, 15u)},
          {.name = "retained_1m",
           .kind = BENCH_TLS_EVIDENCE_RETAINED,
           .slice = &retained_large_slice,
           .payload_bytes = sizeof(retained_large_payload),
           .samples = bench_tls_evidence_samples(31u, 15u)},
          {.name = "copy_2x256k",
           .kind = BENCH_TLS_EVIDENCE_MULTIPART_COPY,
           .payload = multipart_large,
           .payload_bytes = sizeof(multipart_large),
           .part_size = BENCH_MULTIPART_LARGE_PART_BYTES,
           .part_count = BENCH_MULTIPART_LARGE_PARTS,
           .samples = bench_tls_evidence_samples(31u, 15u)},
          {.name = "retained_2x256k",
           .kind = BENCH_TLS_EVIDENCE_MULTIPART_RETAINED,
           .parts = multipart_large_slices,
           .payload_bytes = sizeof(multipart_large),
           .part_size = BENCH_MULTIPART_LARGE_PART_BYTES,
           .part_count = BENCH_MULTIPART_LARGE_PARTS,
           .samples = bench_tls_evidence_samples(31u, 15u)}};

      printf("TLS_EVIDENCE_BEGIN layout=natural_discontiguous\n");
      for (size_t index = 0u;
           index < sizeof(workloads) / sizeof(workloads[0]); ++index) {
        bench_tls_evidence_result result = {0};
        status = bench_tls_evidence_measure(&workloads[index], &result);
        check_equal(status, SALTS_OK);
        if (status != SALTS_OK) break;
        bench_tls_evidence_print(&result);
      }
      check_equal(status, SALTS_OK);
      printf("TLS_EVIDENCE_END layout=natural_discontiguous\n");
    }

    for (size_t part = 0u; part < BENCH_MULTIPART_LARGE_PARTS; ++part)
      mem_slice_release(&multipart_large_slices[part]);
    for (size_t part = 0u; part < BENCH_MULTIPART_SMALL_PARTS; ++part)
      mem_slice_release(&multipart_small_slices[part]);
    mem_slice_release(&retained_large_slice);
    mem_slice_release(&large_slice);
    mem_slice_release(&payload_slice);
    mem_buffer_release(multipart_large_buffer);
    mem_buffer_release(multipart_small_buffer);
    mem_buffer_release(retained_large_buffer);
    mem_buffer_release(large_buffer);
    mem_buffer_release(payload_buffer);
    check_equal(bench_pair_close(&pair), SALTS_OK);
  }

  bench("caller-driven loopback TLS natural framing") {
    static unsigned char payload[BENCH_PAYLOAD_BYTES];
    static unsigned char large_payload[BENCH_LARGE_PAYLOAD_BYTES];
    static unsigned char retained_large_payload[
        BENCH_RETAINED_LARGE_PAYLOAD_BYTES];
    static unsigned char multipart_large[
        BENCH_MULTIPART_LARGE_PARTS * BENCH_MULTIPART_LARGE_PART_BYTES];
    const size_t samples = bench_socket_samples(BENCH_SAMPLES, 4u);
    const size_t large_samples =
        bench_socket_samples(BENCH_LARGE_SAMPLES, 2u);
    const size_t retained_large_samples =
        bench_socket_samples(BENCH_RETAINED_LARGE_SAMPLES, 1u);
    const size_t multipart_large_samples =
        bench_socket_samples(BENCH_MULTIPART_LARGE_SAMPLES, 1u);
    mem_buffer_t *payload_buffer;
    mem_buffer_t *large_buffer;
    mem_buffer_t *retained_large_buffer;
    mem_buffer_t *multipart_large_buffer;
    mem_slice_t payload_slice;
    mem_slice_t large_slice;
    mem_slice_t retained_large_slice;
    mem_slice_t multipart_large_slices[BENCH_MULTIPART_LARGE_PARTS] = {0};
    bench_pair_t pair;
    int status;

    memset(payload, 0x6a, sizeof(payload));
    memset(large_payload, 0xb6, sizeof(large_payload));
    memset(retained_large_payload, 0x4d, sizeof(retained_large_payload));
    for (size_t i = 0u; i < sizeof(multipart_large); ++i)
      multipart_large[i] = (unsigned char)((i * 17u + 7u) & 0xffu);

    payload_buffer = mem_wrap_external(payload, sizeof(payload), NULL, NULL);
    large_buffer =
        mem_wrap_external(large_payload, sizeof(large_payload), NULL, NULL);
    retained_large_buffer =
        mem_wrap_external(retained_large_payload,
                          sizeof(retained_large_payload), NULL, NULL);
    multipart_large_buffer =
        mem_wrap_external(multipart_large, sizeof(multipart_large), NULL, NULL);
    check_not_null(payload_buffer);
    check_not_null(large_buffer);
    check_not_null(retained_large_buffer);
    check_not_null(multipart_large_buffer);

    payload_slice = mem_slice(payload_buffer, 0u, sizeof(payload));
    large_slice = mem_slice(large_buffer, 0u, sizeof(large_payload));
    retained_large_slice =
        mem_slice(retained_large_buffer, 0u, sizeof(retained_large_payload));
    check_not_null(payload_slice.buffer);
    check_not_null(large_slice.buffer);
    check_not_null(retained_large_slice.buffer);
    for (size_t part = 0u; part < BENCH_MULTIPART_LARGE_PARTS; ++part) {
      multipart_large_slices[part] =
          mem_slice(multipart_large_buffer,
                    part * BENCH_MULTIPART_LARGE_PART_BYTES,
                    BENCH_MULTIPART_LARGE_PART_BYTES);
      check_not_null(multipart_large_slices[part].buffer);
    }

    status = bench_pair_open_tls(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange(&pair, payload, sizeof(payload)), SALTS_OK);
    benchmark_bytes("TLS natural framing 64-byte copy immediate", samples,
                    BENCH_PAYLOAD_BYTES) {
      status = bench_exchange(&pair, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange_retained(&pair, &payload_slice), SALTS_OK);
    benchmark_bytes("TLS natural framing 64-byte retained immediate", samples,
                    BENCH_PAYLOAD_BYTES) {
      status = bench_exchange_retained(&pair, &payload_slice);
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange(&pair, large_payload, sizeof(large_payload)),
                SALTS_OK);
    benchmark_bytes("TLS natural framing 64-KiB copy one-way", large_samples,
                    BENCH_LARGE_PAYLOAD_BYTES) {
      status = bench_exchange(&pair, large_payload, sizeof(large_payload));
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange_retained(&pair, &large_slice), SALTS_OK);
    benchmark_bytes("TLS natural framing 64-KiB retained immediate",
                    large_samples, BENCH_LARGE_PAYLOAD_BYTES) {
      status = bench_exchange_retained(&pair, &large_slice);
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_pair_close(&pair), SALTS_OK);
    status = bench_pair_open_tls(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange(
                    &pair, retained_large_payload,
                    sizeof(retained_large_payload)), SALTS_OK);
    benchmark_bytes("TLS natural framing 1-MiB copy immediate",
                    retained_large_samples,
                    BENCH_RETAINED_LARGE_PAYLOAD_BYTES) {
      status = bench_exchange(&pair, retained_large_payload,
                              sizeof(retained_large_payload));
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_pair_close(&pair), SALTS_OK);
    status = bench_pair_open_tls(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange_retained(&pair, &retained_large_slice),
                SALTS_OK);
    benchmark_bytes("TLS natural framing 1-MiB retained immediate",
                    retained_large_samples,
                    BENCH_RETAINED_LARGE_PAYLOAD_BYTES) {
      status = bench_exchange_retained(&pair, &retained_large_slice);
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_pair_close(&pair), SALTS_OK);
    status = bench_pair_open_tls(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange_multipart_copy(
                    &pair, multipart_large, BENCH_MULTIPART_LARGE_PART_BYTES,
                    BENCH_MULTIPART_LARGE_PARTS), SALTS_OK);
    benchmark_bytes("TLS natural framing 2x256-KiB copy multipart",
                    multipart_large_samples,
                    BENCH_MULTIPART_LARGE_PARTS *
                        BENCH_MULTIPART_LARGE_PART_BYTES) {
      status = bench_exchange_multipart_copy(
          &pair, multipart_large, BENCH_MULTIPART_LARGE_PART_BYTES,
          BENCH_MULTIPART_LARGE_PARTS);
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_pair_close(&pair), SALTS_OK);
    status = bench_pair_open_tls(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange_multipart_retained(
                    &pair, multipart_large_slices,
                    BENCH_MULTIPART_LARGE_PARTS), SALTS_OK);
    benchmark_bytes("TLS natural framing 2x256-KiB retained multipart",
                    multipart_large_samples,
                    BENCH_MULTIPART_LARGE_PARTS *
                        BENCH_MULTIPART_LARGE_PART_BYTES) {
      status = bench_exchange_multipart_retained(
          &pair, multipart_large_slices, BENCH_MULTIPART_LARGE_PARTS);
    }
    check_equal(status, SALTS_OK);

    for (size_t part = 0u; part < BENCH_MULTIPART_LARGE_PARTS; ++part)
      mem_slice_release(&multipart_large_slices[part]);
    mem_slice_release(&retained_large_slice);
    mem_slice_release(&large_slice);
    mem_slice_release(&payload_slice);
    mem_buffer_release(multipart_large_buffer);
    mem_buffer_release(retained_large_buffer);
    mem_buffer_release(large_buffer);
    mem_buffer_release(payload_buffer);
    check_equal(bench_pair_close(&pair), SALTS_OK);
  }

#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  group("paired TCP comparison") {
    static bench_pair_t flow_pair;
    static bench_zmq_pair_t zmq_pair;
    static unsigned char payload[BENCH_PAYLOAD_BYTES];
    static unsigned char large_payload[BENCH_LARGE_PAYLOAD_BYTES];

    before_each() {
      memset(&flow_pair, 0, sizeof(flow_pair));
      memset(&zmq_pair, 0, sizeof(zmq_pair));
      memset(payload, 0x5a, sizeof(payload));
      memset(large_payload, 0xa5, sizeof(large_payload));
    }
    after_each() {
      const int status = bench_pair_close(&flow_pair);
      bench_zmq_close(&zmq_pair);
      check_equal(status, SALTS_OK);
    }

    bench("FlowMQ copy") {
      check_equal(bench_pair_open(&flow_pair), SALTS_OK);
      check_equal(bench_exchange(&flow_pair, payload, sizeof(payload)), SALTS_OK);
      benchmark_bytes("PAIR 64-byte one-way", BENCH_SAMPLES,
                      BENCH_PAYLOAD_BYTES) {
        check_equal(bench_exchange(&flow_pair, payload, sizeof(payload)), SALTS_OK);
      }
      check_equal(bench_exchange(&flow_pair, large_payload, sizeof(large_payload)),
                  SALTS_OK);
      benchmark_bytes("PAIR 64-KiB one-way", BENCH_LARGE_SAMPLES,
                      BENCH_LARGE_PAYLOAD_BYTES) {
        check_equal(bench_exchange(&flow_pair, large_payload, sizeof(large_payload)),
                    SALTS_OK);
      }
      /* Start the admission bursts with fresh flow credit, as in the full suite. */
      check_equal(bench_pair_close(&flow_pair), SALTS_OK);
      check_equal(bench_pair_open(&flow_pair), SALTS_OK);
      check_equal(bench_exchange(&flow_pair, payload, sizeof(payload)), SALTS_OK);
      check_equal(bench_exchange_batch(&flow_pair, payload, sizeof(payload)), SALTS_OK);
      benchmark_io("PAIR 64-message queued batch", BENCH_BATCH_SAMPLES,
                   BENCH_BATCH_MESSAGES, BENCH_BATCH_MESSAGES * BENCH_PAYLOAD_BYTES) {
        check_equal(bench_exchange_batch(&flow_pair, payload, sizeof(payload)), SALTS_OK);
      }
    }

    bench("libzmq copy") {
      check_equal(bench_zmq_open(&zmq_pair), 0);
      check_equal(bench_zmq_exchange(&zmq_pair, payload, sizeof(payload)), 0);
      benchmark_bytes("PAIR 64-byte one-way", BENCH_SAMPLES,
                      BENCH_PAYLOAD_BYTES) {
        check_equal(bench_zmq_exchange(&zmq_pair, payload, sizeof(payload)), 0);
      }
      check_equal(bench_zmq_exchange(&zmq_pair, large_payload, sizeof(large_payload)), 0);
      benchmark_bytes("PAIR 64-KiB one-way", BENCH_LARGE_SAMPLES,
                      BENCH_LARGE_PAYLOAD_BYTES) {
        check_equal(bench_zmq_exchange(&zmq_pair, large_payload, sizeof(large_payload)), 0);
      }
      bench_zmq_close(&zmq_pair);
      check_equal(bench_zmq_open(&zmq_pair), 0);
      check_equal(bench_zmq_exchange(&zmq_pair, payload, sizeof(payload)), 0);
      check_equal(bench_zmq_exchange_batch(&zmq_pair, payload, sizeof(payload)), 0);
      benchmark_io("PAIR 64-message queued batch", BENCH_BATCH_SAMPLES,
                   BENCH_BATCH_MESSAGES, BENCH_BATCH_MESSAGES * BENCH_PAYLOAD_BYTES) {
        check_equal(bench_zmq_exchange_batch(&zmq_pair, payload, sizeof(payload)), 0);
      }
    }
  }

  bench("libzmq loopback TCP reference") {
    static unsigned char payload[BENCH_PAYLOAD_BYTES];
    static unsigned char large_payload[BENCH_LARGE_PAYLOAD_BYTES];
    bench_zmq_pair_t pair;
    int status;
    memset(payload, 0x5a, sizeof(payload));
    memset(large_payload, 0xa5, sizeof(large_payload));
    status = bench_zmq_open(&pair);
    check_equal(status, 0);
    check_equal(bench_zmq_exchange(&pair, payload, sizeof(payload)), 0);

    benchmark_bytes("PAIR 64-byte one-way", BENCH_SAMPLES,
                    BENCH_PAYLOAD_BYTES) {
      status = bench_zmq_exchange(&pair, payload, sizeof(payload));
    }
    check_equal(status, 0);

    check_equal(bench_zmq_exchange(&pair, large_payload,
                                   sizeof(large_payload)), 0);
    benchmark_bytes("PAIR 64-KiB one-way", BENCH_LARGE_SAMPLES,
                    BENCH_LARGE_PAYLOAD_BYTES) {
      status = bench_zmq_exchange(&pair, large_payload,
                                  sizeof(large_payload));
    }
    check_equal(status, 0);

    benchmark_io("PAIR 64-message queued batch", BENCH_BATCH_SAMPLES,
                 BENCH_BATCH_MESSAGES,
                 BENCH_BATCH_MESSAGES * BENCH_PAYLOAD_BYTES) {
      status = bench_zmq_exchange_batch(&pair, payload, sizeof(payload));
    }
    check_equal(status, 0);
    bench_zmq_close(&pair);
  }
#endif
}
