#include "flowmq_socket.h"
#include "tinytest.h"
#include "salts_error.h"

#include <string.h>

#if defined(FLOWMQ_BENCH_WITH_ZMQ)
#include <zmq.h>
#endif

enum {
  BENCH_PAYLOAD_BYTES = 64u,
  BENCH_LARGE_PAYLOAD_BYTES = 64u * 1024u,
  BENCH_SAMPLES = 100000u,
  BENCH_LARGE_SAMPLES = 5000u,
  BENCH_BATCH_SAMPLES = 10000u,
  BENCH_BATCH_MESSAGES = 64u,
  BENCH_PROGRESS_LIMIT = 10000u
};

typedef struct bench_pair_s {
  flowmq_ctx_t *ctx;
  flowmq_socket_t *sender;
  flowmq_socket_t *receiver;
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

static void bench_pair_close(bench_pair_t *pair) {
  if (pair->sender != NULL) (void)flowmq_close(pair->sender);
  if (pair->receiver != NULL) (void)flowmq_close(pair->receiver);
  if (pair->ctx != NULL) (void)flowmq_ctx_term(pair->ctx);
  memset(pair, 0, sizeof(*pair));
}

static int bench_exchange(bench_pair_t *pair, const void *payload,
                          size_t payload_size) {
  static unsigned char received[BENCH_LARGE_PAYLOAD_BYTES];
  size_t received_size = 0u;
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
      status = flowmq_recv(pair->receiver, received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT);
  }
  if (status != SALTS_OK) return status;
  if (received_size != payload_size ||
      memcmp(received, payload, payload_size) != 0)
    return SALTS_EPROTO;
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
  return SALTS_OK;
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
    bench_pair_t pair;
    int status;
    memset(payload, 0x5a, sizeof(payload));
    memset(large_payload, 0xa5, sizeof(large_payload));
    status = bench_pair_open(&pair);
    check_equal(status, SALTS_OK);
    check_equal(bench_exchange(&pair, payload, sizeof(payload)), SALTS_OK);

    benchmark_bytes("PAIR 64-byte one-way", BENCH_SAMPLES,
                    BENCH_PAYLOAD_BYTES) {
      status = bench_exchange(&pair, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);

    check_equal(bench_exchange(&pair, large_payload, sizeof(large_payload)),
                SALTS_OK);
    benchmark_bytes("PAIR 64-KiB one-way", BENCH_LARGE_SAMPLES,
                    BENCH_LARGE_PAYLOAD_BYTES) {
      status = bench_exchange(&pair, large_payload, sizeof(large_payload));
    }
    check_equal(status, SALTS_OK);

    benchmark_io("PAIR 64-message queued batch", BENCH_BATCH_SAMPLES,
                 BENCH_BATCH_MESSAGES,
                 BENCH_BATCH_MESSAGES * BENCH_PAYLOAD_BYTES) {
      status = bench_exchange_batch(&pair, payload, sizeof(payload));
    }
    check_equal(status, SALTS_OK);
    bench_pair_close(&pair);
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
