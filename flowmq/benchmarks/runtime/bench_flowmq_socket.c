#include "flowmq_socket.h"
#include "flowmq_tls_test_material.h"
#include "tinytest.h"
#include "salts_error.h"
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
  BENCH_PROGRESS_LIMIT = 10000u
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
  char *cert_path;
  char *key_path;
} bench_pair_t;

static int bench_progress(bench_pair_t *pair) {
  flowmq_pollitem_t items[] = {
      {.socket = pair->sender}, {.socket = pair->receiver}};
  size_t ready = 0u;
  return flowmq_poll(items, 2u, 0u, &ready);
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
  pair->cert_path = tt_make_temp_file("flowmq-bench-cert", ".pem");
  pair->key_path = tt_make_temp_file("flowmq-bench-key", ".pem");
  if (pair->cert_path == NULL || pair->key_path == NULL) {
    (void)bench_pair_close(pair);
    return SALTS_ENOMEM;
  }
  if (tt_write_file(pair->cert_path, FLOWMQ_TLS_TEST_CERTIFICATE,
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
                               pair->cert_path, strlen(pair->cert_path));
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
  size_t received_size = 0u;
  int status = flowmq_send(pair->sender, payload, payload_size,
                           FLOWMQ_DONTWAIT);
  for (size_t i = 0u;
       (status == SALTS_EBUSY || status == SALTS_ENOBUFS) &&
       i < BENCH_PROGRESS_LIMIT; ++i) {
    status = bench_progress(pair);
    if (status == SALTS_OK)
      status = flowmq_send(pair->sender, payload, payload_size,
                           FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) {
    fprintf(stderr, "bench_exchange send/progress failed payload=%zu status=%d\n",
            payload_size, status);
    return status;
  }
  status = SALTS_EBUSY;
  for (size_t i = 0u; status == SALTS_EBUSY && i < BENCH_PROGRESS_LIMIT; ++i) {
    status = bench_progress(pair);
    if (status == SALTS_OK)
      status = flowmq_recv(pair->receiver, received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) {
    fprintf(stderr, "bench_exchange recv/progress failed payload=%zu status=%d received=%zu\n",
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
  int result = SALTS_OK;
  int status = flowmq_send(pair->sender, payload, payload_size,
                           FLOWMQ_DONTWAIT);
  for (size_t i = 0u; status == SALTS_EBUSY && i < BENCH_PROGRESS_LIMIT; ++i) {
    status = bench_progress(pair);
    if (status == SALTS_OK)
      status = flowmq_send(pair->sender, payload, payload_size,
                           FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;

  status = SALTS_EBUSY;
  for (size_t i = 0u; status == SALTS_EBUSY && i < BENCH_PROGRESS_LIMIT; ++i) {
    status = bench_progress(pair);
    if (status == SALTS_OK)
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
  size_t received_size = 0u;
  int status;
  if (payload == NULL || payload->data == NULL || payload->length == 0u)
    return SALTS_EINVAL;
  if (payload->length > sizeof(received)) return SALTS_EMSGSIZE;

  status = flowmq_send_slice(pair->sender, payload, FLOWMQ_DONTWAIT);
  for (size_t i = 0u;
       (status == SALTS_EBUSY || status == SALTS_ENOBUFS) &&
       i < BENCH_PROGRESS_LIMIT; ++i) {
    status = bench_progress(pair);
    if (status == SALTS_OK)
      status = flowmq_send_slice(pair->sender, payload, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;

  status = SALTS_EBUSY;
  for (size_t i = 0u; status == SALTS_EBUSY && i < BENCH_PROGRESS_LIMIT; ++i) {
    status = bench_progress(pair);
    if (status == SALTS_OK)
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
  size_t received_size = 0u;
  size_t option_size = sizeof(int);
  int more = -1;
  int status = SALTS_EBUSY;

  if (expected == NULL || expected_size == 0u ||
      expected_size > sizeof(received))
    return SALTS_EINVAL;
  for (size_t i = 0u; status == SALTS_EBUSY && i < BENCH_PROGRESS_LIMIT; ++i) {
    status = bench_progress(pair);
    if (status == SALTS_OK)
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
  int status;
  if (pair == NULL || payload == NULL || part_size == 0u || part_count < 2u)
    return SALTS_EINVAL;

  for (size_t part = 0u; part < part_count; ++part) {
    const int flags =
        FLOWMQ_DONTWAIT | (part + 1u < part_count ? FLOWMQ_SNDMORE : 0);
    status = flowmq_send(pair->sender, payload + part * part_size,
                         part_size, flags);
    for (size_t i = 0u;
         (status == SALTS_EBUSY || status == SALTS_ENOBUFS) &&
         i < BENCH_PROGRESS_LIMIT; ++i) {
      status = bench_progress(pair);
      if (status == SALTS_OK)
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
  int status;
  if (pair == NULL || parts == NULL || part_count < 2u) return SALTS_EINVAL;

  for (size_t part = 0u; part < part_count; ++part) {
    const int flags =
        FLOWMQ_DONTWAIT | (part + 1u < part_count ? FLOWMQ_SNDMORE : 0);
    status = flowmq_send_slice(pair->sender, &parts[part], flags);
    for (size_t i = 0u;
         (status == SALTS_EBUSY || status == SALTS_ENOBUFS) &&
         i < BENCH_PROGRESS_LIMIT; ++i) {
      status = bench_progress(pair);
      if (status == SALTS_OK)
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
    const uint64_t started = salts_hrtime();
    uint64_t elapsed;
    status = bench_tls_evidence_exchange(&pair, workload);
    if (status != SALTS_OK) {
      fprintf(stderr,
              "TLS_EVIDENCE_FAIL name=%s sample=%zu status=%d\n",
              workload->name, sample, status);
      goto cleanup;
    }
    elapsed = salts_hrtime() - started;
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
    mem_buffer_t *retained_large_buffer;
    mem_buffer_t *multipart_small_buffer;
    mem_buffer_t *multipart_large_buffer;
    mem_slice_t payload_slice;
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
    retained_large_buffer =
        mem_wrap_external(retained_large_payload,
                          sizeof(retained_large_payload), NULL, NULL);
    multipart_small_buffer =
        mem_wrap_external(multipart_small, sizeof(multipart_small), NULL, NULL);
    multipart_large_buffer =
        mem_wrap_external(multipart_large, sizeof(multipart_large), NULL, NULL);
    check_not_null(payload_buffer);
    check_not_null(retained_large_buffer);
    check_not_null(multipart_small_buffer);
    check_not_null(multipart_large_buffer);
    payload_slice = mem_slice(payload_buffer, 0u, sizeof(payload));
    retained_large_slice =
        mem_slice(retained_large_buffer, 0u, sizeof(retained_large_payload));
    check_not_null(payload_slice.buffer);
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

    for (size_t part = 0u; part < BENCH_MULTIPART_LARGE_PARTS; ++part)
      mem_slice_release(&multipart_large_slices[part]);
    for (size_t part = 0u; part < BENCH_MULTIPART_SMALL_PARTS; ++part)
      mem_slice_release(&multipart_small_slices[part]);
    mem_slice_release(&retained_large_slice);
    mem_slice_release(&payload_slice);
    mem_buffer_release(multipart_large_buffer);
    mem_buffer_release(multipart_small_buffer);
    mem_buffer_release(retained_large_buffer);
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
