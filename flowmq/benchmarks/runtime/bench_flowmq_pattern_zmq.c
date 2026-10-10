#include "tinytest.h"
#include <salts/clock.h>
#include <zmq.h>

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum {
  PATTERN_BYTES = 64,
  PATTERN_PEERS = 4,
  PATTERN_SAMPLES = 20000,
  PATTERN_HWM = 1000,
  PATTERN_TIMEOUT_MS = 3000,
  PATTERN_WARM_POLL_MS = 10
};

typedef enum pattern_kind_e {
  PATTERN_PUB, PATTERN_PUSH, PATTERN_ROUTER, PATTERN_REQREP,
  PATTERN_FILTERED, PATTERN_COUNT
} pattern_kind_t;

typedef struct pattern_fixture_s {
  void *context;
  /* Main socket, up to four peers, and a setup-only monitor. */
  void *sockets[PATTERN_PEERS + 2];
  size_t count;
  pattern_kind_t kind;
} pattern_fixture_t;

static const char *const pattern_names[PATTERN_COUNT] = {
  "PUB 4-peer fanout", "PUSH 4-peer round-robin",
  "ROUTER identity one-way", "REQ/REP roundtrip",
  "PUB 2-of-4 filtered fanout"
};
static const size_t pattern_messages[PATTERN_COUNT] = {4, 4, 1, 2, 2};
static const char pattern_identity[] = "bench-dealer";
static int pattern_spin;

static int pattern_close(pattern_fixture_t *fixture) {
  int status = 0;
  for (size_t i = 0; i < fixture->count; ++i) {
    if (fixture->sockets[i] != NULL && zmq_close(fixture->sockets[i]) != 0)
      status = -1;
  }
  if (fixture->context != NULL && zmq_ctx_term(fixture->context) != 0)
    status = -1;
  memset(fixture, 0, sizeof(*fixture));
  return status;
}

static void *pattern_socket(pattern_fixture_t *fixture, int type) {
  const int zero = 0, one = 1, reconnect = -1;
  const int hwm = PATTERN_HWM, timeout = PATTERN_TIMEOUT_MS;
  void *socket;
  if (fixture->count == PATTERN_PEERS + 2) return NULL;
  socket = zmq_socket(fixture->context, type);
  if (socket == NULL) return NULL;
  fixture->sockets[fixture->count++] = socket;
  if (zmq_setsockopt(socket, ZMQ_LINGER, &zero, sizeof(zero)) != 0 ||
      zmq_setsockopt(socket, ZMQ_SNDHWM, &hwm, sizeof(hwm)) != 0 ||
      zmq_setsockopt(socket, ZMQ_RCVHWM, &hwm, sizeof(hwm)) != 0 ||
      zmq_setsockopt(socket, ZMQ_SNDTIMEO, &timeout, sizeof(timeout)) != 0 ||
      zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
      zmq_setsockopt(socket, ZMQ_RECONNECT_IVL, &reconnect,
                    sizeof(reconnect)) != 0)
    return NULL;
  if ((type == ZMQ_PUSH || type == ZMQ_DEALER || type == ZMQ_REQ) &&
      zmq_setsockopt(socket, ZMQ_IMMEDIATE, &one, sizeof(one)) != 0)
    return NULL;
  return socket;
}

static int pattern_bind(void *socket, char *endpoint, size_t capacity) {
  if (zmq_bind(socket, "tcp://127.0.0.1:*") != 0) return -1;
  return zmq_getsockopt(socket, ZMQ_LAST_ENDPOINT, endpoint, &capacity);
}

static int pattern_send(void *socket, const void *data, size_t size, int flags) {
  uint64_t start = 0;
  for (;;) {
    const int sent = zmq_send(socket, data, size,
                              flags | (pattern_spin ? ZMQ_DONTWAIT : 0));
    if (sent == (int)size) return 0;
    if (!pattern_spin || sent != -1 || zmq_errno() != EAGAIN) return -1;
    const uint64_t now = cmeta_monotonic_ms();
    if (start == 0) start = now;
    if (now - start >= PATTERN_TIMEOUT_MS) return -1;
  }
}

static int pattern_receive(void *socket, void *data, size_t capacity) {
  uint64_t start = 0;
  for (;;) {
    const int received = zmq_recv(socket, data, capacity,
                                  pattern_spin ? ZMQ_DONTWAIT : 0);
    if (received >= 0) return received;
    if (!pattern_spin || zmq_errno() != EAGAIN) return -1;
    const uint64_t now = cmeta_monotonic_ms();
    if (start == 0) start = now;
    if (now - start >= PATTERN_TIMEOUT_MS) return -1;
  }
}

static int pattern_recv(void *socket, const void *expected, size_t size,
                        int more_expected) {
  unsigned char bytes[PATTERN_BYTES];
  int more = 0;
  size_t more_size = sizeof(more);
  if (size > sizeof(bytes) ||
      pattern_receive(socket, bytes, sizeof(bytes)) != (int)size ||
      memcmp(bytes, expected, size) != 0 ||
      zmq_getsockopt(socket, ZMQ_RCVMORE, &more, &more_size) != 0 ||
      more != more_expected)
    return -1;
  return 0;
}

/* A monitor handshake proves transport readiness, not SUB propagation. PUB
 * therefore has its own end-to-end, tagged subscription barrier below. */
static int pattern_push_ready(void *monitor) {
  unsigned ready = 0;
  const uint64_t start = cmeta_monotonic_ms();
  while (ready < PATTERN_PEERS &&
         cmeta_monotonic_ms() - start < PATTERN_TIMEOUT_MS) {
    unsigned char event_bytes[6];
    char endpoint[256];
    uint16_t event;
    int more = 0;
    size_t more_size = sizeof(more);
    zmq_pollitem_t item = {monitor, 0, ZMQ_POLLIN, 0};
    const int polled = zmq_poll(&item, 1, PATTERN_WARM_POLL_MS);
    if (polled < 0) return -1;
    if (polled == 0) continue;
    if (zmq_recv(monitor, event_bytes, sizeof(event_bytes), 0) != 6 ||
        zmq_getsockopt(monitor, ZMQ_RCVMORE, &more, &more_size) != 0 ||
        more != 1)
      return -1;
    memcpy(&event, event_bytes, sizeof(event));
    const int received = zmq_recv(monitor, endpoint, sizeof(endpoint), 0);
    if (received < 0 || received > (int)sizeof(endpoint) ||
        zmq_getsockopt(monitor, ZMQ_RCVMORE, &more, &more_size) != 0 ||
        more != 0 || event != ZMQ_EVENT_HANDSHAKE_SUCCEEDED)
      return -1;
    ++ready;
  }
  return ready == PATTERN_PEERS ? 0 : -1;
}

static int pattern_pub_ready(pattern_fixture_t *fixture, const char *topic,
                             unsigned mask) {
  const uint64_t start = cmeta_monotonic_ms();
  uint64_t sequence = 0;
  unsigned char warm[PATTERN_BYTES];
  while (cmeta_monotonic_ms() - start < PATTERN_TIMEOUT_MS) {
    unsigned received_mask = 0;
    const uint64_t attempt = cmeta_monotonic_ms();
    memset(warm, 0xa5, sizeof(warm));
    memcpy(warm, topic, strlen(topic));
    ++sequence;
    memcpy(warm + sizeof(warm) - sizeof(sequence), &sequence, sizeof(sequence));
    if (pattern_send(fixture->sockets[0], warm, sizeof(warm), 0) != 0)
      return -1;
    while (received_mask != mask &&
           cmeta_monotonic_ms() - attempt < PATTERN_WARM_POLL_MS) {
      zmq_pollitem_t items[PATTERN_PEERS] = {0};
      for (size_t i = 0; i < PATTERN_PEERS; ++i) {
        items[i].socket = fixture->sockets[i + 1];
        items[i].events = ZMQ_POLLIN;
      }
      if (zmq_poll(items, PATTERN_PEERS, 1) < 0) return -1;
      for (size_t i = 0; i < PATTERN_PEERS; ++i) {
        if (items[i].revents & ZMQ_POLLIN) {
          unsigned char bytes[PATTERN_BYTES];
          if (!(mask & (1u << i)) ||
              zmq_recv(items[i].socket, bytes, sizeof(bytes), 0) != sizeof(bytes))
            return -1;
          if (memcmp(bytes, warm, sizeof(warm)) == 0)
            received_mask |= 1u << i;
        }
      }
    }
    /* Each peer has consumed the last published frame in FIFO order. There
     * are no later warm frames to leak into the measured workload. */
    if (received_mask == mask) return 0;
  }
  return -1;
}

static int pattern_cycle(pattern_fixture_t *fixture, const unsigned char *data) {
  void *main_socket = fixture->sockets[0];
  if (fixture->kind == PATTERN_PUB || fixture->kind == PATTERN_FILTERED) {
    if (pattern_send(main_socket, data, PATTERN_BYTES, 0) != 0) return -1;
    for (size_t i = 0; i < PATTERN_PEERS; ++i) {
      if (fixture->kind == PATTERN_FILTERED && (i & 1u)) continue;
      if (pattern_recv(fixture->sockets[i + 1], data, PATTERN_BYTES, 0) != 0)
        return -1;
    }
  } else if (fixture->kind == PATTERN_PUSH) {
    for (size_t i = 0; i < PATTERN_PEERS; ++i)
      if (pattern_send(main_socket, data, PATTERN_BYTES, 0) != 0) return -1;
    for (size_t i = 0; i < PATTERN_PEERS; ++i)
      if (pattern_recv(fixture->sockets[i + 1], data, PATTERN_BYTES, 0) != 0)
        return -1;
  } else if (fixture->kind == PATTERN_ROUTER) {
    if (pattern_send(main_socket, pattern_identity, sizeof(pattern_identity) - 1,
                     ZMQ_SNDMORE) != 0 ||
        pattern_send(main_socket, data, PATTERN_BYTES, 0) != 0 ||
        pattern_recv(fixture->sockets[1], data, PATTERN_BYTES, 0) != 0)
      return -1;
  } else if (fixture->kind == PATTERN_REQREP) {
    if (pattern_send(main_socket, data, PATTERN_BYTES, 0) != 0 ||
        pattern_recv(fixture->sockets[1], data, PATTERN_BYTES, 0) != 0 ||
        pattern_send(fixture->sockets[1], data, PATTERN_BYTES, 0) != 0 ||
        pattern_recv(main_socket, data, PATTERN_BYTES, 0) != 0)
      return -1;
  } else {
    return -1;
  }
  return 0;
}

static int pattern_open(pattern_fixture_t *fixture, pattern_kind_t kind,
                        unsigned char *data) {
  static const int main_types[PATTERN_COUNT] = {
    ZMQ_PUB, ZMQ_PUSH, ZMQ_ROUTER, ZMQ_REQ, ZMQ_PUB
  };
  static const int peer_types[PATTERN_COUNT] = {
    ZMQ_SUB, ZMQ_PULL, ZMQ_DEALER, ZMQ_REP, ZMQ_SUB
  };
  char endpoint[256];
  const int one = 1;
  const size_t peers = kind == PATTERN_ROUTER || kind == PATTERN_REQREP
                           ? 1 : PATTERN_PEERS;
  void *monitor = NULL;
  fixture->kind = kind;
  memset(data, 0x5a, PATTERN_BYTES);
  if (kind == PATTERN_FILTERED) memcpy(data, "orders.", 7);
  fixture->context = zmq_ctx_new();
  if (fixture->context == NULL ||
      zmq_ctx_set(fixture->context, ZMQ_IO_THREADS, 1) != 0 ||
      pattern_socket(fixture, main_types[kind]) == NULL)
    return -1;
  for (size_t i = 0; i < peers; ++i)
    if (pattern_socket(fixture, peer_types[kind]) == NULL) return -1;
  if (kind == PATTERN_PUSH) {
    if (zmq_socket_monitor(fixture->sockets[0], "inproc://pattern-monitor",
                           ZMQ_EVENT_HANDSHAKE_SUCCEEDED |
                           ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL |
                           ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL |
                           ZMQ_EVENT_HANDSHAKE_FAILED_AUTH |
                           ZMQ_EVENT_DISCONNECTED) != 0)
      return -1;
    monitor = pattern_socket(fixture, ZMQ_PAIR);
    if (monitor == NULL || zmq_connect(monitor, "inproc://pattern-monitor") != 0)
      return -1;
  }
  if (kind == PATTERN_ROUTER &&
      (zmq_setsockopt(fixture->sockets[0], ZMQ_ROUTER_MANDATORY,
                      &one, sizeof(one)) != 0 ||
       zmq_setsockopt(fixture->sockets[1], ZMQ_ROUTING_ID, pattern_identity,
                      sizeof(pattern_identity) - 1) != 0))
    return -1;
  if (kind != PATTERN_PUSH && kind != PATTERN_REQREP &&
      pattern_bind(fixture->sockets[0], endpoint, sizeof(endpoint)) != 0)
    return -1;
  for (size_t i = 0; i < peers; ++i) {
    void *peer = fixture->sockets[i + 1];
    if (kind == PATTERN_PUSH || kind == PATTERN_REQREP) {
      if (pattern_bind(peer, endpoint, sizeof(endpoint)) != 0 ||
          zmq_connect(fixture->sockets[0], endpoint) != 0)
        return -1;
    } else {
      if (kind == PATTERN_PUB || kind == PATTERN_FILTERED) {
        const char *topic = kind == PATTERN_PUB ? "" :
                               (i & 1u) ? "payments." : "orders.";
        if (zmq_setsockopt(peer, ZMQ_SUBSCRIBE, topic, strlen(topic)) != 0)
          return -1;
      }
      if (zmq_connect(peer, endpoint) != 0) return -1;
    }
  }
  if (kind == PATTERN_PUSH) {
    if (pattern_push_ready(monitor) != 0 ||
        zmq_socket_monitor(fixture->sockets[0], NULL, 0) != 0 ||
        zmq_close(monitor) != 0)
      return -1;
    fixture->sockets[--fixture->count] = NULL;
  } else if (kind == PATTERN_ROUTER) {
    if (pattern_send(fixture->sockets[1], data, PATTERN_BYTES, 0) != 0 ||
        pattern_recv(fixture->sockets[0], pattern_identity,
                      sizeof(pattern_identity) - 1, 1) != 0 ||
        pattern_recv(fixture->sockets[0], data, PATTERN_BYTES, 0) != 0)
      return -1;
  } else if (kind == PATTERN_PUB) {
    if (pattern_pub_ready(fixture, "", 15u) != 0) return -1;
  } else if (kind == PATTERN_FILTERED) {
    if (pattern_pub_ready(fixture, "payments.", 10u) != 0 ||
        pattern_pub_ready(fixture, "orders.", 5u) != 0)
      return -1;
  }
  return pattern_cycle(fixture, data);
}

static int pattern_empty(const pattern_fixture_t *fixture) {
  unsigned char bytes[PATTERN_BYTES];
  /* All expected deliveries have been consumed; also check filtered-out SUBs. */
  for (size_t i = 1; i < fixture->count; ++i) {
    if (fixture->kind == PATTERN_REQREP) break; /* REP is in its send/recv FSM. */
    if (zmq_recv(fixture->sockets[i], bytes, sizeof(bytes), ZMQ_DONTWAIT) != -1 ||
        zmq_errno() != EAGAIN)
      return -1;
  }
  return 0;
}

spec("ZeroMQ pattern comparison") {
  static pattern_fixture_t fixture;
  static unsigned char data[PATTERN_BYTES];
  before_each() {
    const char *spin = getenv("FLOWMQ_PATTERN_ZMQ_SPIN");
    memset(&fixture, 0, sizeof(fixture));
    check(spin == NULL || strcmp(spin, "0") == 0 || strcmp(spin, "1") == 0);
    pattern_spin = spin != NULL && strcmp(spin, "1") == 0;
  }
  after_each() { check_equal(pattern_close(&fixture), 0); }

  for (int kind = 0; kind < PATTERN_COUNT; ++kind) {
    it("correctness: %s", pattern_names[kind]) {
      check_equal(pattern_open(&fixture, (pattern_kind_t)kind, data), 0);
      for (uint64_t sequence = 1; sequence <= 128; ++sequence) {
        memcpy(data + PATTERN_BYTES - sizeof(sequence), &sequence, sizeof(sequence));
        check_equal(pattern_cycle(&fixture, data), 0);
      }
      check_equal(pattern_empty(&fixture), 0);
    }
    bench("performance: %s", pattern_names[kind]) {
      int status = pattern_open(&fixture, (pattern_kind_t)kind, data);
      check_equal(status, 0);
      benchmark_io(pattern_names[kind], PATTERN_SAMPLES, pattern_messages[kind],
                    pattern_messages[kind] * PATTERN_BYTES) {
        if (status == 0) status = pattern_cycle(&fixture, data);
      }
      check_equal(status, 0);
      check_equal(pattern_empty(&fixture), 0);
    }
  }
}
