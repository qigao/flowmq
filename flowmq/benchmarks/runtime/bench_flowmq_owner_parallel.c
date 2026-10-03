#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "flowmq_socket.h"
#include "salts_error.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum {
  OWNER_PARALLEL_LANES = 2,
  OWNER_PARALLEL_WARMUPS = 8,
  OWNER_PARALLEL_SAMPLES = 64,
  OWNER_PARALLEL_REPEATS = 7,
  OWNER_PARALLEL_PAYLOAD_COUNT = 2,
  OWNER_PARALLEL_PATH_COUNT = 2,
  OWNER_PARALLEL_WAIT_MS = 10,
  OWNER_PARALLEL_TIMEOUT_MS = 10000
};

static const size_t OWNER_PARALLEL_PAYLOADS[] = {1024u, 65536u};

typedef enum owner_parallel_mode_e {
  OWNER_PARALLEL_SERIAL = 0,
  OWNER_PARALLEL_PARALLEL = 1,
  OWNER_PARALLEL_MODE_COUNT
} owner_parallel_mode_t;

typedef enum owner_parallel_path_e {
  OWNER_PARALLEL_COPY = 0,
  OWNER_PARALLEL_RETAINED = 1
} owner_parallel_path_t;

typedef struct owner_parallel_peer_s {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  pthread_t thread;
  size_t payload_size;
  size_t cycles;
  size_t echoed_cycles;
  owner_parallel_path_t path;
  int status;
  bool mutex_initialized;
  bool cond_initialized;
  bool thread_started;
  bool ready;
  bool completed;
  bool stop;
  char endpoint[128];
} owner_parallel_peer_t;

typedef struct owner_parallel_lane_s {
  flowmq_ctx_t *ctx;
  flowmq_socket_t *socket;
  unsigned char *payload;
  unsigned char *received;
  mem_buffer_t *payload_buffer;
  mem_slice_t payload_slice;
  size_t payload_size;
  size_t progress_calls;
  size_t send_admissions;
  size_t receive_deliveries;
  owner_parallel_path_t path;
  bool measuring;
} owner_parallel_lane_t;

typedef struct owner_parallel_gate_s {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  size_t ready;
  size_t done;
  bool start;
  bool cleanup;
  int failure;
} owner_parallel_gate_t;

typedef struct owner_parallel_thread_arg_s {
  owner_parallel_gate_t *gate;
  owner_parallel_peer_t *peers[OWNER_PARALLEL_LANES];
  owner_parallel_lane_t lanes[OWNER_PARALLEL_LANES];
  size_t lane_ids[OWNER_PARALLEL_LANES];
  uint64_t latencies[OWNER_PARALLEL_LANES][OWNER_PARALLEL_SAMPLES];
  size_t lane_count;
  size_t payload_size;
  size_t progress_calls;
  size_t send_admissions;
  size_t receive_deliveries;
  owner_parallel_path_t path;
  int cpu;
  int observed_cpu;
  uint64_t cpu_ns;
  int status;
} owner_parallel_thread_arg_t;

typedef struct owner_parallel_sample_s {
  const char *mode;
  const char *path;
  const char *topology;
  size_t payload_size;
  size_t repeat;
  size_t logical_operations;
  int cpu_a;
  int cpu_b;
  uint64_t wall_ns;
  uint64_t owner_cpu_ns;
  uint64_t p50_ns;
  uint64_t p95_ns;
  uint64_t p99_ns;
  double operations_per_second;
  double mib_per_second;
  double owner_cpu_us_per_op;
  size_t progress_calls;
  size_t send_admissions;
  size_t receive_deliveries;
} owner_parallel_sample_t;

static uint64_t owner_parallel_clock_ns(clockid_t clock_id) {
  struct timespec value;
  if (clock_gettime(clock_id, &value) != 0) return 0u;
  return (uint64_t)value.tv_sec * UINT64_C(1000000000) +
         (uint64_t)value.tv_nsec;
}

static uint64_t owner_parallel_deadline_ns(void) {
  const uint64_t now = owner_parallel_clock_ns(CLOCK_MONOTONIC);
  return now + (uint64_t)OWNER_PARALLEL_TIMEOUT_MS * UINT64_C(1000000);
}

static bool owner_parallel_deadline_expired(uint64_t deadline) {
  const uint64_t now = owner_parallel_clock_ns(CLOCK_MONOTONIC);
  return now == 0u || now >= deadline;
}

static int owner_parallel_progress_socket(flowmq_socket_t *socket,
                                          uint32_t timeout_ms) {
  flowmq_pollitem_t item = {
      .socket = socket,
      .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT | FLOWMQ_POLLERR};
  size_t ready = 0u;
  return flowmq_poll(&item, 1u, timeout_ms, &ready);
}

static bool owner_parallel_peer_stopping(owner_parallel_peer_t *peer) {
  bool stopping;
  (void)pthread_mutex_lock(&peer->mutex);
  stopping = peer->stop;
  (void)pthread_mutex_unlock(&peer->mutex);
  return stopping;
}

static int owner_parallel_peer_recv_copy(owner_parallel_peer_t *peer,
                                         flowmq_socket_t *socket,
                                         unsigned char *buffer) {
  const uint64_t deadline = owner_parallel_deadline_ns();
  size_t received = 0u;
  int status = SALTS_EBUSY;

  while (status == SALTS_EBUSY) {
    if (owner_parallel_peer_stopping(peer)) return SALTS_EIO;
    status = flowmq_recv(socket, buffer, peer->payload_size, &received,
                         FLOWMQ_DONTWAIT);
    if (status == SALTS_OK) break;
    if (status != SALTS_EBUSY) return status;
    status = owner_parallel_progress_socket(socket, OWNER_PARALLEL_WAIT_MS);
    if (status != SALTS_OK) return status;
    status = SALTS_EBUSY;
    if (owner_parallel_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }
  return received == peer->payload_size ? SALTS_OK : SALTS_EPROTO;
}

static int owner_parallel_peer_send_copy(owner_parallel_peer_t *peer,
                                         flowmq_socket_t *socket,
                                         const unsigned char *buffer) {
  const uint64_t deadline = owner_parallel_deadline_ns();
  int status = flowmq_send(socket, buffer, peer->payload_size,
                           FLOWMQ_DONTWAIT);

  while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    if (owner_parallel_peer_stopping(peer)) return SALTS_EIO;
    status = owner_parallel_progress_socket(socket, OWNER_PARALLEL_WAIT_MS);
    if (status != SALTS_OK) return status;
    status = flowmq_send(socket, buffer, peer->payload_size,
                         FLOWMQ_DONTWAIT);
    if (owner_parallel_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }
  return status;
}

static int owner_parallel_peer_recv_slice(owner_parallel_peer_t *peer,
                                          flowmq_socket_t *socket,
                                          mem_slice_t *slice) {
  const uint64_t deadline = owner_parallel_deadline_ns();
  int status = SALTS_EBUSY;

  memset(slice, 0, sizeof(*slice));
  while (status == SALTS_EBUSY) {
    if (owner_parallel_peer_stopping(peer)) return SALTS_EIO;
    status = flowmq_recv_slice(socket, slice, FLOWMQ_DONTWAIT);
    if (status == SALTS_OK) break;
    if (status != SALTS_EBUSY) return status;
    status = owner_parallel_progress_socket(socket, OWNER_PARALLEL_WAIT_MS);
    if (status != SALTS_OK) return status;
    status = SALTS_EBUSY;
    if (owner_parallel_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }
  if (slice->length != peer->payload_size) {
    mem_slice_release(slice);
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int owner_parallel_peer_send_slice(owner_parallel_peer_t *peer,
                                          flowmq_socket_t *socket,
                                          const mem_slice_t *slice) {
  const uint64_t deadline = owner_parallel_deadline_ns();
  int status = flowmq_send_slice(socket, slice, FLOWMQ_DONTWAIT);

  while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    if (owner_parallel_peer_stopping(peer)) return SALTS_EIO;
    status = owner_parallel_progress_socket(socket, OWNER_PARALLEL_WAIT_MS);
    if (status != SALTS_OK) return status;
    status = flowmq_send_slice(socket, slice, FLOWMQ_DONTWAIT);
    if (owner_parallel_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }
  return status;
}

static void *owner_parallel_peer_entry(void *user) {
  owner_parallel_peer_t *peer = (owner_parallel_peer_t *)user;
  flowmq_ctx_t *ctx = NULL;
  flowmq_socket_t *socket = NULL;
  unsigned char *buffer = NULL;
  size_t endpoint_size = 0u;
  int status = SALTS_OK;

  ctx = flowmq_ctx_new();
  if (ctx == NULL) status = SALTS_ENOMEM;
  if (status == SALTS_OK) {
    socket = flowmq_socket(ctx, FLOWMQ_PAIR);
    if (socket == NULL) status = SALTS_ENOMEM;
  }
  if (status == SALTS_OK &&
      flowmq_bind(socket, "tcp://127.0.0.1:0") != SALTS_OK)
    status = SALTS_EIO;
  if (status == SALTS_OK) {
    status = flowmq_last_endpoint(socket, peer->endpoint,
                                  sizeof(peer->endpoint), &endpoint_size);
  }
  if (status == SALTS_OK && peer->path == OWNER_PARALLEL_COPY) {
    buffer = (unsigned char *)malloc(peer->payload_size);
    if (buffer == NULL) status = SALTS_ENOMEM;
  }

  (void)pthread_mutex_lock(&peer->mutex);
  peer->status = status;
  peer->ready = true;
  (void)pthread_cond_broadcast(&peer->changed);
  (void)pthread_mutex_unlock(&peer->mutex);

  for (size_t cycle = 0u;
       cycle < peer->cycles && status == SALTS_OK; ++cycle) {
    if (peer->path == OWNER_PARALLEL_COPY) {
      status = owner_parallel_peer_recv_copy(peer, socket, buffer);
      if (status == SALTS_OK)
        status = owner_parallel_peer_send_copy(peer, socket, buffer);
    } else {
      mem_slice_t slice = {0};
      status = owner_parallel_peer_recv_slice(peer, socket, &slice);
      if (status == SALTS_OK)
        status = owner_parallel_peer_send_slice(peer, socket, &slice);
      mem_slice_release(&slice);
    }
    if (status == SALTS_OK) {
      (void)pthread_mutex_lock(&peer->mutex);
      ++peer->echoed_cycles;
      (void)pthread_mutex_unlock(&peer->mutex);
    }
  }

  (void)pthread_mutex_lock(&peer->mutex);
  peer->status = status;
  peer->completed = status == SALTS_OK && peer->echoed_cycles == peer->cycles;
  (void)pthread_cond_broadcast(&peer->changed);
  (void)pthread_mutex_unlock(&peer->mutex);

  /*
   * A successful FlowMQ send is local admission, not remote completion.
   * Keep the peer owner progressing after its final echo admission until the
   * controller confirms every measured owner received its final reply.
   */
  while (status == SALTS_OK && !owner_parallel_peer_stopping(peer)) {
    status = owner_parallel_progress_socket(socket, 1u);
  }
  if (owner_parallel_peer_stopping(peer)) status = SALTS_OK;

  free(buffer);
  if (socket != NULL) {
    const int close_status = flowmq_close(socket);
    if (status == SALTS_OK && close_status != SALTS_OK) status = close_status;
  }
  if (ctx != NULL) {
    const int term_status = flowmq_ctx_term(ctx);
    if (status == SALTS_OK && term_status != SALTS_OK) status = term_status;
  }

  (void)pthread_mutex_lock(&peer->mutex);
  peer->status = status;
  (void)pthread_cond_broadcast(&peer->changed);
  (void)pthread_mutex_unlock(&peer->mutex);
  return NULL;
}

static int owner_parallel_peer_init(owner_parallel_peer_t *peer,
                                    size_t payload_size,
                                    size_t cycles,
                                    owner_parallel_path_t path) {
  int status;
  memset(peer, 0, sizeof(*peer));
  peer->payload_size = payload_size;
  peer->cycles = cycles;
  peer->path = path;
  peer->status = SALTS_OK;

  status = pthread_mutex_init(&peer->mutex, NULL);
  if (status != 0) return -status;
  peer->mutex_initialized = true;
  status = pthread_cond_init(&peer->changed, NULL);
  if (status != 0) {
    (void)pthread_mutex_destroy(&peer->mutex);
    peer->mutex_initialized = false;
    return -status;
  }
  peer->cond_initialized = true;
  status = pthread_create(&peer->thread, NULL, owner_parallel_peer_entry, peer);
  if (status != 0) {
    (void)pthread_cond_destroy(&peer->changed);
    (void)pthread_mutex_destroy(&peer->mutex);
    peer->cond_initialized = false;
    peer->mutex_initialized = false;
    return -status;
  }
  peer->thread_started = true;

  (void)pthread_mutex_lock(&peer->mutex);
  while (!peer->ready)
    (void)pthread_cond_wait(&peer->changed, &peer->mutex);
  status = peer->status;
  (void)pthread_mutex_unlock(&peer->mutex);
  return status;
}

static int owner_parallel_peer_destroy(owner_parallel_peer_t *peer,
                                       bool abort_peer) {
  int result = SALTS_OK;
  if (peer == NULL) return SALTS_EINVAL;

  if (peer->mutex_initialized) {
    (void)pthread_mutex_lock(&peer->mutex);
    peer->stop = true;
    (void)pthread_cond_broadcast(&peer->changed);
    (void)pthread_mutex_unlock(&peer->mutex);
  }

  if (peer->thread_started) {
    const int join_status = pthread_join(peer->thread, NULL);
    if (join_status != 0) result = -join_status;
    peer->thread_started = false;
  }

  if (!abort_peer && result == SALTS_OK) {
    if (peer->status != SALTS_OK)
      result = peer->status;
    else if (!peer->completed || peer->echoed_cycles != peer->cycles)
      result = SALTS_EPROTO;
  }

  if (peer->cond_initialized) {
    (void)pthread_cond_destroy(&peer->changed);
    peer->cond_initialized = false;
  }
  if (peer->mutex_initialized) {
    (void)pthread_mutex_destroy(&peer->mutex);
    peer->mutex_initialized = false;
  }
  return result;
}

static int owner_parallel_lane_progress(owner_parallel_lane_t *lane) {
  const int status =
      owner_parallel_progress_socket(lane->socket, OWNER_PARALLEL_WAIT_MS);
  if (lane->measuring) ++lane->progress_calls;
  return status;
}

static int owner_parallel_lane_send(owner_parallel_lane_t *lane) {
  const uint64_t deadline = owner_parallel_deadline_ns();
  int status =
      lane->path == OWNER_PARALLEL_COPY
          ? flowmq_send(lane->socket, lane->payload, lane->payload_size,
                        FLOWMQ_DONTWAIT)
          : flowmq_send_slice(lane->socket, &lane->payload_slice,
                              FLOWMQ_DONTWAIT);

  while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    status = owner_parallel_lane_progress(lane);
    if (status != SALTS_OK) return status;
    status =
        lane->path == OWNER_PARALLEL_COPY
            ? flowmq_send(lane->socket, lane->payload, lane->payload_size,
                          FLOWMQ_DONTWAIT)
            : flowmq_send_slice(lane->socket, &lane->payload_slice,
                                FLOWMQ_DONTWAIT);
    if (owner_parallel_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }
  if (status == SALTS_OK && lane->measuring) ++lane->send_admissions;
  return status;
}

static int owner_parallel_lane_recv(owner_parallel_lane_t *lane) {
  const uint64_t deadline = owner_parallel_deadline_ns();
  int status = SALTS_EBUSY;

  if (lane->path == OWNER_PARALLEL_COPY) {
    size_t received = 0u;
    while (status == SALTS_EBUSY) {
      status = flowmq_recv(lane->socket, lane->received, lane->payload_size,
                           &received, FLOWMQ_DONTWAIT);
      if (status == SALTS_OK) break;
      if (status != SALTS_EBUSY) return status;
      status = owner_parallel_lane_progress(lane);
      if (status != SALTS_OK) return status;
      status = SALTS_EBUSY;
      if (owner_parallel_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
    }
    if (received != lane->payload_size ||
        memcmp(lane->received, lane->payload, lane->payload_size) != 0)
      return SALTS_EPROTO;
  } else {
    mem_slice_t received = {0};
    while (status == SALTS_EBUSY) {
      status =
          flowmq_recv_slice(lane->socket, &received, FLOWMQ_DONTWAIT);
      if (status == SALTS_OK) break;
      if (status != SALTS_EBUSY) return status;
      status = owner_parallel_lane_progress(lane);
      if (status != SALTS_OK) return status;
      status = SALTS_EBUSY;
      if (owner_parallel_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
    }
    if (received.length != lane->payload_size ||
        memcmp(received.data, lane->payload, lane->payload_size) != 0) {
      mem_slice_release(&received);
      return SALTS_EPROTO;
    }
    mem_slice_release(&received);
  }

  if (lane->measuring) ++lane->receive_deliveries;
  return SALTS_OK;
}

static int owner_parallel_lane_roundtrip(owner_parallel_lane_t *lane,
                                         uint64_t *latency_out) {
  const uint64_t started = owner_parallel_clock_ns(CLOCK_MONOTONIC);
  int status;
  if (started == 0u) return SALTS_EIO;

  status = owner_parallel_lane_send(lane);
  if (status == SALTS_OK) status = owner_parallel_lane_recv(lane);
  if (status != SALTS_OK) return status;

  if (latency_out != NULL) {
    const uint64_t finished = owner_parallel_clock_ns(CLOCK_MONOTONIC);
    if (finished <= started) return SALTS_EIO;
    *latency_out = finished - started;
  }
  return SALTS_OK;
}

static int owner_parallel_lane_init(owner_parallel_lane_t *lane,
                                    const char *endpoint,
                                    size_t payload_size,
                                    owner_parallel_path_t path,
                                    size_t lane_index) {
  int status = SALTS_OK;
  memset(lane, 0, sizeof(*lane));
  lane->payload_size = payload_size;
  lane->path = path;

  lane->payload = (unsigned char *)malloc(payload_size);
  lane->received = (unsigned char *)malloc(payload_size);
  if (lane->payload == NULL || lane->received == NULL) status = SALTS_ENOMEM;
  if (status == SALTS_OK) {
    for (size_t index = 0u; index < payload_size; ++index) {
      lane->payload[index] =
          (unsigned char)((index * 29u + lane_index * 53u + 11u) & 0xffu);
    }
  }

  if (status == SALTS_OK && path == OWNER_PARALLEL_RETAINED) {
    lane->payload_buffer =
        mem_wrap_external(lane->payload, payload_size, NULL, NULL);
    if (lane->payload_buffer == NULL) status = SALTS_ENOMEM;
    if (status == SALTS_OK) {
      lane->payload_slice = mem_slice(lane->payload_buffer, 0u, payload_size);
      if (lane->payload_slice.buffer == NULL) status = SALTS_ENOMEM;
    }
  }

  if (status == SALTS_OK) {
    lane->ctx = flowmq_ctx_new();
    if (lane->ctx == NULL) status = SALTS_ENOMEM;
  }
  if (status == SALTS_OK) {
    lane->socket = flowmq_socket(lane->ctx, FLOWMQ_PAIR);
    if (lane->socket == NULL) status = SALTS_ENOMEM;
  }
  if (status == SALTS_OK) status = flowmq_connect(lane->socket, endpoint);
  return status;
}

static int owner_parallel_lane_destroy(owner_parallel_lane_t *lane) {
  int result = SALTS_OK;
  if (lane == NULL) return SALTS_EINVAL;

  if (lane->socket != NULL) {
    const int status = flowmq_close(lane->socket);
    if (result == SALTS_OK && status != SALTS_OK) result = status;
    lane->socket = NULL;
  }
  if (lane->ctx != NULL) {
    const int status = flowmq_ctx_term(lane->ctx);
    if (result == SALTS_OK && status != SALTS_OK) result = status;
    lane->ctx = NULL;
  }

  mem_slice_release(&lane->payload_slice);
  if (lane->payload_buffer != NULL) {
    mem_buffer_release(lane->payload_buffer);
    lane->payload_buffer = NULL;
  }
  free(lane->received);
  free(lane->payload);
  lane->received = NULL;
  lane->payload = NULL;
  return result;
}

static int owner_parallel_gate_init(owner_parallel_gate_t *gate) {
  int status;
  memset(gate, 0, sizeof(*gate));
  status = pthread_mutex_init(&gate->mutex, NULL);
  if (status != 0) return -status;
  status = pthread_cond_init(&gate->changed, NULL);
  if (status != 0) {
    (void)pthread_mutex_destroy(&gate->mutex);
    return -status;
  }
  gate->failure = SALTS_OK;
  return SALTS_OK;
}

static void owner_parallel_gate_destroy(owner_parallel_gate_t *gate) {
  (void)pthread_cond_destroy(&gate->changed);
  (void)pthread_mutex_destroy(&gate->mutex);
}

static void owner_parallel_gate_fail(owner_parallel_gate_t *gate, int status) {
  (void)pthread_mutex_lock(&gate->mutex);
  if (gate->failure == SALTS_OK) gate->failure = status;
  gate->start = true;
  (void)pthread_cond_broadcast(&gate->changed);
  (void)pthread_mutex_unlock(&gate->mutex);
}

static int owner_parallel_gate_ready(owner_parallel_gate_t *gate) {
  int status;
  (void)pthread_mutex_lock(&gate->mutex);
  ++gate->ready;
  (void)pthread_cond_broadcast(&gate->changed);
  while (!gate->start && gate->failure == SALTS_OK)
    (void)pthread_cond_wait(&gate->changed, &gate->mutex);
  status = gate->failure;
  (void)pthread_mutex_unlock(&gate->mutex);
  return status;
}

static void owner_parallel_gate_done(owner_parallel_gate_t *gate, int status) {
  (void)pthread_mutex_lock(&gate->mutex);
  if (status != SALTS_OK && gate->failure == SALTS_OK) gate->failure = status;
  ++gate->done;
  (void)pthread_cond_broadcast(&gate->changed);
  (void)pthread_mutex_unlock(&gate->mutex);
}

static int owner_parallel_pin_cpu(int cpu) {
  cpu_set_t set;
  int status;
  if (cpu < 0 || cpu >= CPU_SETSIZE) return SALTS_EINVAL;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  status = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
  return status == 0 ? SALTS_OK : -status;
}

static void *owner_parallel_owner_entry(void *user) {
  owner_parallel_thread_arg_t *arg =
      (owner_parallel_thread_arg_t *)user;
  size_t initialized = 0u;
  uint64_t cpu_started = 0u;
  int status;

  arg->observed_cpu = -1;
  arg->status = SALTS_OK;
  status = owner_parallel_pin_cpu(arg->cpu);
  if (status != SALTS_OK) goto fail;

  arg->observed_cpu = sched_getcpu();
  if (arg->observed_cpu != arg->cpu) {
    status = SALTS_EPROTO;
    goto fail;
  }

  for (size_t lane = 0u; lane < arg->lane_count; ++lane) {
    status = owner_parallel_lane_init(
        &arg->lanes[lane], arg->peers[lane]->endpoint,
        arg->payload_size, arg->path, arg->lane_ids[lane]);
    if (status != SALTS_OK) goto fail;
    ++initialized;

    for (size_t warmup = 0u;
         warmup < OWNER_PARALLEL_WARMUPS; ++warmup) {
      status = owner_parallel_lane_roundtrip(&arg->lanes[lane], NULL);
      if (status != SALTS_OK) goto fail;
    }
  }

  status = owner_parallel_gate_ready(arg->gate);
  if (status != SALTS_OK) goto fail_after_gate;

  cpu_started = owner_parallel_clock_ns(CLOCK_THREAD_CPUTIME_ID);
  if (cpu_started == 0u) {
    status = SALTS_EIO;
    goto finish_measurement;
  }

  for (size_t lane = 0u;
       lane < arg->lane_count && status == SALTS_OK; ++lane) {
    owner_parallel_lane_t *entry = &arg->lanes[lane];
    entry->measuring = true;
    for (size_t sample = 0u;
         sample < OWNER_PARALLEL_SAMPLES; ++sample) {
      status = owner_parallel_lane_roundtrip(
          entry, &arg->latencies[lane][sample]);
      if (status != SALTS_OK) break;
    }
    entry->measuring = false;
    arg->progress_calls += entry->progress_calls;
    arg->send_admissions += entry->send_admissions;
    arg->receive_deliveries += entry->receive_deliveries;
  }

finish_measurement:
  if (cpu_started != 0u) {
    const uint64_t cpu_finished =
        owner_parallel_clock_ns(CLOCK_THREAD_CPUTIME_ID);
    if (cpu_finished > cpu_started)
      arg->cpu_ns = cpu_finished - cpu_started;
    else if (status == SALTS_OK)
      status = SALTS_EIO;
  }

  arg->status = status;
  owner_parallel_gate_done(arg->gate, status);

  /*
   * Keep the measured owner socket alive while the peer drains its final
   * admitted reply. The controller stops peers first, then releases owners
   * into teardown.
   */
  (void)pthread_mutex_lock(&arg->gate->mutex);
  while (!arg->gate->cleanup)
    (void)pthread_cond_wait(&arg->gate->changed, &arg->gate->mutex);
  (void)pthread_mutex_unlock(&arg->gate->mutex);
  goto cleanup;

fail:
  owner_parallel_gate_fail(arg->gate, status);
fail_after_gate:
  arg->status = status;

cleanup:
  for (size_t lane = 0u; lane < initialized; ++lane) {
    const int destroy_status = owner_parallel_lane_destroy(&arg->lanes[lane]);
    if (arg->status == SALTS_OK && destroy_status != SALTS_OK)
      arg->status = destroy_status;
  }
  return NULL;
}

static int owner_parallel_u64_compare(const void *left, const void *right) {
  const uint64_t a = *(const uint64_t *)left;
  const uint64_t b = *(const uint64_t *)right;
  return a < b ? -1 : a > b ? 1 : 0;
}

static uint64_t owner_parallel_percentile(uint64_t *values,
                                          size_t count,
                                          unsigned percentile) {
  size_t index;
  qsort(values, count, sizeof(*values), owner_parallel_u64_compare);
  index = ((count - 1u) * (size_t)percentile + 50u) / 100u;
  return values[index];
}

static const char *owner_parallel_path_name(owner_parallel_path_t path) {
  return path == OWNER_PARALLEL_RETAINED ? "retained" : "copy";
}

static int owner_parallel_run_mode(owner_parallel_mode_t mode,
                                   const char *topology,
                                   owner_parallel_path_t path,
                                   size_t payload_size,
                                   size_t repeat,
                                   int cpu_a,
                                   int cpu_b,
                                   owner_parallel_sample_t *out) {
  owner_parallel_peer_t peers[OWNER_PARALLEL_LANES];
  owner_parallel_gate_t gate;
  owner_parallel_thread_arg_t args[OWNER_PARALLEL_LANES];
  pthread_t threads[OWNER_PARALLEL_LANES];
  bool thread_started[OWNER_PARALLEL_LANES] = {false, false};
  bool peer_destroyed[OWNER_PARALLEL_LANES] = {false, false};
  uint64_t latencies[OWNER_PARALLEL_LANES * OWNER_PARALLEL_SAMPLES];
  const size_t cycles = OWNER_PARALLEL_WARMUPS + OWNER_PARALLEL_SAMPLES;
  const size_t thread_count =
      mode == OWNER_PARALLEL_SERIAL ? 1u : OWNER_PARALLEL_LANES;
  uint64_t wall_started = 0u;
  uint64_t wall_ns = 0u;
  uint64_t owner_cpu_ns = 0u;
  size_t latency_count = 0u;
  size_t progress_calls = 0u;
  size_t send_admissions = 0u;
  size_t receive_deliveries = 0u;
  int status = SALTS_OK;

  if (out == NULL || topology == NULL) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  memset(peers, 0, sizeof(peers));
  memset(args, 0, sizeof(args));

  status = owner_parallel_gate_init(&gate);
  if (status != SALTS_OK) return status;

  for (size_t lane = 0u;
       lane < OWNER_PARALLEL_LANES && status == SALTS_OK; ++lane)
    status = owner_parallel_peer_init(
        &peers[lane], payload_size, cycles, path);
  if (status != SALTS_OK) goto cleanup;

  if (mode == OWNER_PARALLEL_SERIAL) {
    args[0].gate = &gate;
    args[0].peers[0] = &peers[0];
    args[0].peers[1] = &peers[1];
    args[0].lane_ids[0] = 0u;
    args[0].lane_ids[1] = 1u;
    args[0].lane_count = OWNER_PARALLEL_LANES;
    args[0].payload_size = payload_size;
    args[0].path = path;
    args[0].cpu = cpu_a;
  } else {
    for (size_t lane = 0u; lane < OWNER_PARALLEL_LANES; ++lane) {
      args[lane].gate = &gate;
      args[lane].peers[0] = &peers[lane];
      args[lane].lane_ids[0] = lane;
      args[lane].lane_count = 1u;
      args[lane].payload_size = payload_size;
      args[lane].path = path;
      args[lane].cpu = lane == 0u ? cpu_a : cpu_b;
    }
  }

  for (size_t index = 0u;
       index < thread_count && status == SALTS_OK; ++index) {
    const int create_status =
        pthread_create(&threads[index], NULL,
                       owner_parallel_owner_entry, &args[index]);
    if (create_status != 0) {
      status = -create_status;
      break;
    }
    thread_started[index] = true;
  }
  if (status != SALTS_OK) {
    owner_parallel_gate_fail(&gate, status);
    goto join_threads;
  }

  (void)pthread_mutex_lock(&gate.mutex);
  while (gate.ready < thread_count && gate.failure == SALTS_OK)
    (void)pthread_cond_wait(&gate.changed, &gate.mutex);
  if (gate.failure != SALTS_OK) {
    status = gate.failure;
    gate.start = true;
    (void)pthread_cond_broadcast(&gate.changed);
    (void)pthread_mutex_unlock(&gate.mutex);
    goto join_threads;
  }

  wall_started = owner_parallel_clock_ns(CLOCK_MONOTONIC);
  if (wall_started == 0u) {
    status = SALTS_EIO;
    gate.failure = status;
    gate.start = true;
    (void)pthread_cond_broadcast(&gate.changed);
    (void)pthread_mutex_unlock(&gate.mutex);
    goto join_threads;
  }
  gate.start = true;
  (void)pthread_cond_broadcast(&gate.changed);
  while (gate.done < thread_count)
    (void)pthread_cond_wait(&gate.changed, &gate.mutex);
  if (wall_started != 0u) {
    const uint64_t wall_finished =
        owner_parallel_clock_ns(CLOCK_MONOTONIC);
    if (wall_finished > wall_started)
      wall_ns = wall_finished - wall_started;
    else if (status == SALTS_OK)
      status = SALTS_EIO;
  }
  if (gate.failure != SALTS_OK) status = gate.failure;
  (void)pthread_mutex_unlock(&gate.mutex);

  /*
   * All measured owners are now parked before teardown. Stop and join the
   * echo peers while the owner sockets still exist, then permit owner cleanup.
   */
  for (size_t lane = 0u; lane < OWNER_PARALLEL_LANES; ++lane) {
    const int peer_status =
        owner_parallel_peer_destroy(&peers[lane], status != SALTS_OK);
    peer_destroyed[lane] = true;
    if (status == SALTS_OK && peer_status != SALTS_OK) status = peer_status;
  }
  (void)pthread_mutex_lock(&gate.mutex);
  gate.cleanup = true;
  (void)pthread_cond_broadcast(&gate.changed);
  (void)pthread_mutex_unlock(&gate.mutex);

join_threads:
  for (size_t index = 0u; index < thread_count; ++index) {
    if (thread_started[index]) {
      const int join_status = pthread_join(threads[index], NULL);
      if (status == SALTS_OK && join_status != 0) status = -join_status;
    }
    if (status == SALTS_OK && args[index].status != SALTS_OK)
      status = args[index].status;
  }
  if (status != SALTS_OK) goto cleanup;

  for (size_t index = 0u; index < thread_count; ++index) {
    if (args[index].observed_cpu != args[index].cpu) {
      status = SALTS_EPROTO;
      goto cleanup;
    }
    owner_cpu_ns += args[index].cpu_ns;
    progress_calls += args[index].progress_calls;
    send_admissions += args[index].send_admissions;
    receive_deliveries += args[index].receive_deliveries;
    for (size_t lane = 0u; lane < args[index].lane_count; ++lane) {
      for (size_t sample = 0u;
           sample < OWNER_PARALLEL_SAMPLES; ++sample)
        latencies[latency_count++] = args[index].latencies[lane][sample];
    }
  }

  if (latency_count != OWNER_PARALLEL_LANES * OWNER_PARALLEL_SAMPLES ||
      send_admissions != latency_count ||
      receive_deliveries != latency_count ||
      wall_ns == 0u || owner_cpu_ns == 0u) {
    status = SALTS_EPROTO;
    goto cleanup;
  }

  out->mode =
      mode == OWNER_PARALLEL_SERIAL
          ? "one_owner_serial"
          : "two_owner_parallel";
  out->path = owner_parallel_path_name(path);
  out->topology = topology;
  out->payload_size = payload_size;
  out->repeat = repeat;
  out->logical_operations = latency_count;
  out->cpu_a = cpu_a;
  out->cpu_b = mode == OWNER_PARALLEL_SERIAL ? cpu_a : cpu_b;
  out->wall_ns = wall_ns;
  out->owner_cpu_ns = owner_cpu_ns;
  out->p50_ns = owner_parallel_percentile(latencies, latency_count, 50u);
  out->p95_ns = owner_parallel_percentile(latencies, latency_count, 95u);
  out->p99_ns = owner_parallel_percentile(latencies, latency_count, 99u);
  out->operations_per_second =
      (double)latency_count * 1.0e9 / (double)wall_ns;
  out->mib_per_second =
      ((double)latency_count * (double)payload_size /
       (1024.0 * 1024.0)) *
      1.0e9 / (double)wall_ns;
  out->owner_cpu_us_per_op =
      (double)owner_cpu_ns / 1000.0 / (double)latency_count;
  out->progress_calls = progress_calls;
  out->send_admissions = send_admissions;
  out->receive_deliveries = receive_deliveries;

cleanup:
  for (size_t lane = 0u; lane < OWNER_PARALLEL_LANES; ++lane) {
    if (!peer_destroyed[lane]) {
      const int peer_status =
          owner_parallel_peer_destroy(&peers[lane], status != SALTS_OK);
      if (status == SALTS_OK && peer_status != SALTS_OK) status = peer_status;
    }
  }
  owner_parallel_gate_destroy(&gate);
  return status;
}

static int owner_parallel_parse_cpu(const char *name, int *out_cpu) {
  const char *value = getenv(name);
  char *end = NULL;
  long parsed;

  if (out_cpu == NULL || value == NULL || *value == '\0')
    return SALTS_EINVAL;
  errno = 0;
  parsed = strtol(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' ||
      parsed < 0 || parsed >= CPU_SETSIZE)
    return SALTS_EINVAL;
  *out_cpu = (int)parsed;
  return SALTS_OK;
}

static int owner_parallel_double_compare(const void *left,
                                         const void *right) {
  const double a = *(const double *)left;
  const double b = *(const double *)right;
  return a < b ? -1 : a > b ? 1 : 0;
}

static double owner_parallel_double_median(double *values, size_t count) {
  qsort(values, count, sizeof(*values), owner_parallel_double_compare);
  return values[count / 2u];
}

static FILE *owner_parallel_open_csv(void) {
  const char *prefix = getenv("FLOWMQ_OWNER_PARALLEL_OUTPUT");
  char path[1024];
  if (prefix == NULL || *prefix == '\0') return NULL;
  if (snprintf(path, sizeof(path), "%s.csv", prefix) < 0) return NULL;
  return fopen(path, "w");
}

static int owner_parallel_write_csv(FILE *csv,
                                    const owner_parallel_sample_t *sample) {
  if (csv == NULL || sample == NULL) return SALTS_OK;
  return fprintf(
             csv,
             "%s,%s,%s,%zu,%zu,%zu,%d,%d,%" PRIu64 ",%" PRIu64
             ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
             ",%.6f,%.6f,%.6f,%zu,%zu,%zu\n",
             sample->topology, sample->path, sample->mode,
             sample->payload_size, sample->repeat,
             sample->logical_operations, sample->cpu_a, sample->cpu_b,
             sample->wall_ns, sample->owner_cpu_ns,
             sample->p50_ns, sample->p95_ns, sample->p99_ns,
             sample->operations_per_second,
             sample->mib_per_second,
             sample->owner_cpu_us_per_op,
             sample->progress_calls,
             sample->send_admissions,
             sample->receive_deliveries) < 0
             ? SALTS_EIO
             : SALTS_OK;
}

int main(void) {
  const char *topology = getenv("FLOWMQ_OWNER_PARALLEL_TOPOLOGY");
  const char *output_prefix = getenv("FLOWMQ_OWNER_PARALLEL_OUTPUT");
  owner_parallel_sample_t
      results[OWNER_PARALLEL_PATH_COUNT]
             [OWNER_PARALLEL_PAYLOAD_COUNT]
             [OWNER_PARALLEL_MODE_COUNT]
             [OWNER_PARALLEL_REPEATS];
  FILE *csv = NULL;
  int cpu_a = -1;
  int cpu_b = -1;
  int status;

  if (topology == NULL || *topology == '\0') topology = "unspecified";
  status = owner_parallel_parse_cpu(
      "FLOWMQ_OWNER_PARALLEL_CPU_A", &cpu_a);
  if (status == SALTS_OK)
    status = owner_parallel_parse_cpu(
        "FLOWMQ_OWNER_PARALLEL_CPU_B", &cpu_b);
  if (status != SALTS_OK) {
    fprintf(stderr, "invalid FlowMQ owner CPU affinity environment\n");
    return 2;
  }

  memset(results, 0, sizeof(results));
  csv = owner_parallel_open_csv();
  if (output_prefix != NULL && *output_prefix != '\0' && csv == NULL) {
    fprintf(stderr, "failed to create FlowMQ owner-parallel CSV output\n");
    return 2;
  }
  if (csv != NULL) {
    fprintf(
        csv,
        "topology,path,mode,payload_bytes,repeat,logical_operations,"
        "cpu_a,cpu_b,wall_ns,owner_cpu_ns,p50_ns,p95_ns,p99_ns,"
        "operations_per_second,mib_per_second,owner_cpu_us_per_op,"
        "progress_calls,send_admissions,receive_deliveries\n");
  }

  for (size_t path = 0u; path < OWNER_PARALLEL_PATH_COUNT; ++path) {
    for (size_t payload = 0u;
         payload < OWNER_PARALLEL_PAYLOAD_COUNT; ++payload) {
      for (size_t repeat = 0u;
           repeat < OWNER_PARALLEL_REPEATS; ++repeat) {
        for (size_t offset = 0u;
             offset < OWNER_PARALLEL_MODE_COUNT; ++offset) {
          const owner_parallel_mode_t mode =
              (owner_parallel_mode_t)(
                  (path + payload + repeat + offset) %
                  OWNER_PARALLEL_MODE_COUNT);
          owner_parallel_sample_t *sample =
              &results[path][payload][mode][repeat];
          status = owner_parallel_run_mode(
              mode, topology, (owner_parallel_path_t)path,
              OWNER_PARALLEL_PAYLOADS[payload], repeat + 1u,
              cpu_a, cpu_b, sample);
          if (status != SALTS_OK) {
            fprintf(stderr,
                    "FlowMQ owner-parallel benchmark failed: "
                    "topology=%s path=%s payload=%zu repeat=%zu "
                    "mode=%u status=%d\n",
                    topology,
                    owner_parallel_path_name((owner_parallel_path_t)path),
                    OWNER_PARALLEL_PAYLOADS[payload], repeat + 1u,
                    (unsigned)mode, status);
            goto cleanup;
          }
          status = owner_parallel_write_csv(csv, sample);
          if (status != SALTS_OK) goto cleanup;
        }
      }
    }
  }

  printf("# FlowMQ owner-affine multicore scaling control\n\n");
  printf("Topology: %s; owner CPU A=%d; owner CPU B=%d.\n\n",
         topology, cpu_a, cpu_b);
  printf(
      "Each repeat executes equal total work: two independent FlowMQ PAIR "
      "lanes, one outstanding request/reply per lane, and %u measured "
      "round trips per lane. Serial mode drives both client lanes on one "
      "pinned owner thread; parallel mode drives one client lane per pinned "
      "owner thread. Echo peers are independent FlowMQ sockets on "
      "scheduler-managed threads. No performance threshold is enforced.\n\n",
      (unsigned)OWNER_PARALLEL_SAMPLES);
  printf("| path | payload | mode | ops/s median | MiB/s median | "
         "p50 us median | p99 us median | owner CPU us/op median | "
         "progress calls median |\n");
  printf("| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");

  for (size_t path = 0u; path < OWNER_PARALLEL_PATH_COUNT; ++path) {
    for (size_t payload = 0u;
         payload < OWNER_PARALLEL_PAYLOAD_COUNT; ++payload) {
      double rate[OWNER_PARALLEL_MODE_COUNT][OWNER_PARALLEL_REPEATS];
      double mib[OWNER_PARALLEL_MODE_COUNT][OWNER_PARALLEL_REPEATS];
      double p50[OWNER_PARALLEL_MODE_COUNT][OWNER_PARALLEL_REPEATS];
      double p99[OWNER_PARALLEL_MODE_COUNT][OWNER_PARALLEL_REPEATS];
      double cpu[OWNER_PARALLEL_MODE_COUNT][OWNER_PARALLEL_REPEATS];
      double progress[OWNER_PARALLEL_MODE_COUNT][OWNER_PARALLEL_REPEATS];
      double speedup[OWNER_PARALLEL_REPEATS];

      for (size_t mode = 0u;
           mode < OWNER_PARALLEL_MODE_COUNT; ++mode) {
        for (size_t repeat = 0u;
             repeat < OWNER_PARALLEL_REPEATS; ++repeat) {
          const owner_parallel_sample_t *sample =
              &results[path][payload][mode][repeat];
          rate[mode][repeat] = sample->operations_per_second;
          mib[mode][repeat] = sample->mib_per_second;
          p50[mode][repeat] = (double)sample->p50_ns / 1000.0;
          p99[mode][repeat] = (double)sample->p99_ns / 1000.0;
          cpu[mode][repeat] = sample->owner_cpu_us_per_op;
          progress[mode][repeat] = (double)sample->progress_calls;
        }
        printf("| %s | %zu | %s | %.0f | %.3f | %.3f | %.3f | %.3f | %.0f |\n",
               owner_parallel_path_name((owner_parallel_path_t)path),
               OWNER_PARALLEL_PAYLOADS[payload],
               mode == OWNER_PARALLEL_SERIAL
                   ? "one_owner_serial"
                   : "two_owner_parallel",
               owner_parallel_double_median(
                   rate[mode], OWNER_PARALLEL_REPEATS),
               owner_parallel_double_median(
                   mib[mode], OWNER_PARALLEL_REPEATS),
               owner_parallel_double_median(
                   p50[mode], OWNER_PARALLEL_REPEATS),
               owner_parallel_double_median(
                   p99[mode], OWNER_PARALLEL_REPEATS),
               owner_parallel_double_median(
                   cpu[mode], OWNER_PARALLEL_REPEATS),
               owner_parallel_double_median(
                   progress[mode], OWNER_PARALLEL_REPEATS));
      }

      for (size_t repeat = 0u;
           repeat < OWNER_PARALLEL_REPEATS; ++repeat) {
        speedup[repeat] =
            results[path][payload][OWNER_PARALLEL_PARALLEL][repeat]
                .operations_per_second /
            results[path][payload][OWNER_PARALLEL_SERIAL][repeat]
                .operations_per_second;
      }
      printf("\n%s payload %zu parallel speedup median: %.3fx\n\n",
             owner_parallel_path_name((owner_parallel_path_t)path),
             OWNER_PARALLEL_PAYLOADS[payload],
             owner_parallel_double_median(
                 speedup, OWNER_PARALLEL_REPEATS));
    }
  }

  status = SALTS_OK;

cleanup:
  if (csv != NULL && fclose(csv) != 0 && status == SALTS_OK)
    status = SALTS_EIO;
  return status == SALTS_OK ? 0 : 1;
}
