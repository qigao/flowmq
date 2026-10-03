#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "flowmq_socket.h"
#include "flowmq_socket_external_internal.h"
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
  SHARED_WAIT_LANES = 2,
  SHARED_WAIT_WARMUPS = 8,
  SHARED_WAIT_SAMPLES = 64,
  SHARED_WAIT_REPEATS = 7,
  SHARED_WAIT_PAYLOAD_COUNT = 2,
  SHARED_WAIT_PATH_COUNT = 2,
  SHARED_WAIT_TIMEOUT_MS = 10000,
  SHARED_WAIT_MAX_WAIT_MS = 10,
  SHARED_WAIT_COMPLETION_CAPACITY = 64,
  SHARED_WAIT_BACKEND_ENDPOINT_CAPACITY = 32,
  SHARED_WAIT_BACKEND_REQUEST_CAPACITY = 32
};

static const size_t SHARED_WAIT_PAYLOADS[] = {1024u, 65536u};

typedef enum shared_wait_mode_e {
  SHARED_WAIT_CONTROL = 0,
  SHARED_WAIT_CANDIDATE = 1,
  SHARED_WAIT_MODE_COUNT
} shared_wait_mode_t;

typedef enum shared_wait_path_e {
  SHARED_WAIT_COPY = 0,
  SHARED_WAIT_RETAINED = 1
} shared_wait_path_t;

typedef struct shared_wait_peer_s {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  pthread_t thread;
  size_t payload_size;
  size_t cycles;
  size_t echoed_cycles;
  shared_wait_path_t path;
  int status;
  bool mutex_initialized;
  bool cond_initialized;
  bool thread_started;
  bool ready;
  bool completed;
  bool stop;
  char endpoint[128];
} shared_wait_peer_t;

typedef struct shared_wait_fixture_s {
  flowmq_ctx_t *ctx;
  flowmq_socket_t *sockets[SHARED_WAIT_LANES];
  native_io_backend backend;
  unsigned char *payload[SHARED_WAIT_LANES];
  unsigned char *received[SHARED_WAIT_LANES];
  mem_buffer_t *payload_buffer[SHARED_WAIT_LANES];
  mem_slice_t payload_slice[SHARED_WAIT_LANES];
  size_t payload_size;
  shared_wait_mode_t mode;
  shared_wait_path_t path;
  size_t control_poll_calls;
  size_t advance_calls;
  size_t observe_calls;
  size_t route_attempts;
  size_t routed_completions;
  size_t unrelated_route_attempts;
  size_t send_admissions;
  size_t receive_deliveries;
  bool backend_initialized;
  bool measuring;
} shared_wait_fixture_t;

typedef struct shared_wait_sample_s {
  const char *mode;
  const char *path;
  size_t payload_size;
  size_t repeat;
  size_t logical_operations;
  int cpu;
  uint64_t wall_ns;
  uint64_t owner_cpu_ns;
  uint64_t p50_ns;
  uint64_t p95_ns;
  uint64_t p99_ns;
  double operations_per_second;
  double mib_per_second;
  double owner_cpu_us_per_op;
  size_t control_poll_calls;
  size_t advance_calls;
  size_t observe_calls;
  size_t route_attempts;
  size_t routed_completions;
  size_t unrelated_route_attempts;
  size_t send_admissions;
  size_t receive_deliveries;
} shared_wait_sample_t;

static uint64_t shared_wait_clock_ns(clockid_t clock_id) {
  struct timespec value;
  if (clock_gettime(clock_id, &value) != 0) return 0u;
  return (uint64_t)value.tv_sec * UINT64_C(1000000000) +
         (uint64_t)value.tv_nsec;
}

static uint64_t shared_wait_deadline_ns(void) {
  const uint64_t now = shared_wait_clock_ns(CLOCK_MONOTONIC);
  return now + (uint64_t)SHARED_WAIT_TIMEOUT_MS * UINT64_C(1000000);
}

static bool shared_wait_deadline_expired(uint64_t deadline) {
  const uint64_t now = shared_wait_clock_ns(CLOCK_MONOTONIC);
  return now == 0u || now >= deadline;
}

static bool shared_wait_peer_stopping(shared_wait_peer_t *peer) {
  bool stopping;
  (void)pthread_mutex_lock(&peer->mutex);
  stopping = peer->stop;
  (void)pthread_mutex_unlock(&peer->mutex);
  return stopping;
}

static int shared_wait_peer_progress(flowmq_socket_t *socket,
                                     uint32_t timeout_ms) {
  flowmq_pollitem_t item = {
      .socket = socket,
      .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT | FLOWMQ_POLLERR};
  size_t ready = 0u;
  return flowmq_poll(&item, 1u, timeout_ms, &ready);
}

static int shared_wait_peer_recv_copy(shared_wait_peer_t *peer,
                                      flowmq_socket_t *socket,
                                      unsigned char *buffer) {
  const uint64_t deadline = shared_wait_deadline_ns();
  size_t received = 0u;
  int status = SALTS_EBUSY;

  while (status == SALTS_EBUSY) {
    if (shared_wait_peer_stopping(peer)) return SALTS_EIO;
    status = flowmq_recv(socket, buffer, peer->payload_size, &received,
                         FLOWMQ_DONTWAIT);
    if (status == SALTS_OK) break;
    if (status != SALTS_EBUSY) return status;
    status = shared_wait_peer_progress(socket, SHARED_WAIT_MAX_WAIT_MS);
    if (status != SALTS_OK) return status;
    status = SALTS_EBUSY;
    if (shared_wait_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }
  return received == peer->payload_size ? SALTS_OK : SALTS_EPROTO;
}

static int shared_wait_peer_send_copy(shared_wait_peer_t *peer,
                                      flowmq_socket_t *socket,
                                      const unsigned char *buffer) {
  const uint64_t deadline = shared_wait_deadline_ns();
  int status =
      flowmq_send(socket, buffer, peer->payload_size, FLOWMQ_DONTWAIT);

  while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    if (shared_wait_peer_stopping(peer)) return SALTS_EIO;
    status = shared_wait_peer_progress(socket, SHARED_WAIT_MAX_WAIT_MS);
    if (status != SALTS_OK) return status;
    status = flowmq_send(socket, buffer, peer->payload_size, FLOWMQ_DONTWAIT);
    if (shared_wait_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }
  return status;
}

static int shared_wait_peer_recv_slice(shared_wait_peer_t *peer,
                                       flowmq_socket_t *socket,
                                       mem_slice_t *slice) {
  const uint64_t deadline = shared_wait_deadline_ns();
  int status = SALTS_EBUSY;

  memset(slice, 0, sizeof(*slice));
  while (status == SALTS_EBUSY) {
    if (shared_wait_peer_stopping(peer)) return SALTS_EIO;
    status = flowmq_recv_slice(socket, slice, FLOWMQ_DONTWAIT);
    if (status == SALTS_OK) break;
    if (status != SALTS_EBUSY) return status;
    status = shared_wait_peer_progress(socket, SHARED_WAIT_MAX_WAIT_MS);
    if (status != SALTS_OK) return status;
    status = SALTS_EBUSY;
    if (shared_wait_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }
  if (slice->length != peer->payload_size) {
    mem_slice_release(slice);
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int shared_wait_peer_send_slice(shared_wait_peer_t *peer,
                                       flowmq_socket_t *socket,
                                       const mem_slice_t *slice) {
  const uint64_t deadline = shared_wait_deadline_ns();
  int status = flowmq_send_slice(socket, slice, FLOWMQ_DONTWAIT);

  while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    if (shared_wait_peer_stopping(peer)) return SALTS_EIO;
    status = shared_wait_peer_progress(socket, SHARED_WAIT_MAX_WAIT_MS);
    if (status != SALTS_OK) return status;
    status = flowmq_send_slice(socket, slice, FLOWMQ_DONTWAIT);
    if (shared_wait_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }
  return status;
}

static void *shared_wait_peer_entry(void *user) {
  shared_wait_peer_t *peer = (shared_wait_peer_t *)user;
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
  if (status == SALTS_OK && peer->path == SHARED_WAIT_COPY) {
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
    if (peer->path == SHARED_WAIT_COPY) {
      status = shared_wait_peer_recv_copy(peer, socket, buffer);
      if (status == SALTS_OK)
        status = shared_wait_peer_send_copy(peer, socket, buffer);
    } else {
      mem_slice_t slice = {0};
      status = shared_wait_peer_recv_slice(peer, socket, &slice);
      if (status == SALTS_OK)
        status = shared_wait_peer_send_slice(peer, socket, &slice);
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

  /* Keep driving the final admitted echo until the measured owner is done. */
  while (status == SALTS_OK && !shared_wait_peer_stopping(peer))
    status = shared_wait_peer_progress(socket, 1u);
  if (shared_wait_peer_stopping(peer)) status = SALTS_OK;

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

static int shared_wait_peer_init(shared_wait_peer_t *peer,
                                 size_t payload_size,
                                 size_t cycles,
                                 shared_wait_path_t path) {
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
  status = pthread_create(&peer->thread, NULL, shared_wait_peer_entry, peer);
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

static int shared_wait_peer_destroy(shared_wait_peer_t *peer,
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

static int shared_wait_control_progress(shared_wait_fixture_t *fixture,
                                        uint32_t timeout_ms) {
  flowmq_pollitem_t items[SHARED_WAIT_LANES];
  size_t ready = 0u;
  int status;

  for (size_t lane = 0u; lane < SHARED_WAIT_LANES; ++lane) {
    items[lane] = (flowmq_pollitem_t){
        .socket = fixture->sockets[lane],
        .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT | FLOWMQ_POLLERR};
  }
  status = flowmq_poll(items, SHARED_WAIT_LANES, timeout_ms, &ready);
  if (fixture->measuring) ++fixture->control_poll_calls;
  if (status != SALTS_OK) return status;
  for (size_t lane = 0u; lane < SHARED_WAIT_LANES; ++lane) {
    if ((items[lane].revents & FLOWMQ_POLLERR) != 0) return SALTS_EIO;
  }
  return SALTS_OK;
}

static int shared_wait_candidate_progress(shared_wait_fixture_t *fixture,
                                          uint32_t max_wait_ms) {
  native_io_completion completions[SHARED_WAIT_COMPLETION_CAPACITY];
  uint32_t wait_ms = max_wait_ms;
  size_t completion_count = 0u;
  int status;

  for (size_t lane = 0u; lane < SHARED_WAIT_LANES; ++lane) {
    size_t events = 0u;
    uint32_t lane_wait = max_wait_ms;
    status = flowmq_socket_internal_advance_external(
        fixture->sockets[lane], &events);
    if (fixture->measuring) ++fixture->advance_calls;
    if (status != SALTS_OK) return status;
    status = flowmq_socket_internal_external_timeout(
        fixture->sockets[lane], max_wait_ms, &lane_wait);
    if (status != SALTS_OK) return status;
    if (lane_wait < wait_ms) wait_ms = lane_wait;
  }

  status = native_io_backend_observe(
      &fixture->backend, completions, SHARED_WAIT_COMPLETION_CAPACITY,
      wait_ms, &completion_count);
  if (fixture->measuring) ++fixture->observe_calls;
  if (status == SALTS_ETIMEDOUT) return SALTS_OK;
  if (status != SALTS_OK) return status;

  for (size_t index = 0u; index < completion_count; ++index) {
    bool consumed = false;
    for (size_t lane = 0u; lane < SHARED_WAIT_LANES && !consumed; ++lane) {
      size_t events = 0u;
      bool lane_consumed = false;
      status = flowmq_socket_internal_route_external_completion(
          fixture->sockets[lane], &completions[index],
          &lane_consumed, &events);
      if (fixture->measuring) ++fixture->route_attempts;
      if (status != SALTS_OK) return status;
      if (lane_consumed) {
        consumed = true;
        if (fixture->measuring) ++fixture->routed_completions;
      } else if (fixture->measuring) {
        ++fixture->unrelated_route_attempts;
      }
    }
    if (!consumed) return SALTS_EPROTO;
  }

  /* No second CNet advance after a routed batch: #713 canonical host loop. */
  return SALTS_OK;
}

static int shared_wait_progress(shared_wait_fixture_t *fixture,
                                uint32_t timeout_ms) {
  return fixture->mode == SHARED_WAIT_CONTROL
             ? shared_wait_control_progress(fixture, timeout_ms)
             : shared_wait_candidate_progress(fixture, timeout_ms);
}

static int shared_wait_try_send(shared_wait_fixture_t *fixture,
                                size_t lane) {
  int status;
  if (fixture->path == SHARED_WAIT_COPY) {
    status = flowmq_send(fixture->sockets[lane], fixture->payload[lane],
                         fixture->payload_size, FLOWMQ_DONTWAIT);
  } else {
    status = flowmq_send_slice(fixture->sockets[lane],
                               &fixture->payload_slice[lane],
                               FLOWMQ_DONTWAIT);
  }
  if (status == SALTS_OK && fixture->measuring) ++fixture->send_admissions;
  return status;
}

static int shared_wait_try_receive(shared_wait_fixture_t *fixture,
                                   size_t lane) {
  int status;
  if (fixture->path == SHARED_WAIT_COPY) {
    size_t received = 0u;
    status = flowmq_recv(fixture->sockets[lane], fixture->received[lane],
                         fixture->payload_size, &received, FLOWMQ_DONTWAIT);
    if (status == SALTS_OK &&
        (received != fixture->payload_size ||
         memcmp(fixture->received[lane], fixture->payload[lane],
                fixture->payload_size) != 0))
      return SALTS_EPROTO;
  } else {
    mem_slice_t received = {0};
    status = flowmq_recv_slice(
        fixture->sockets[lane], &received, FLOWMQ_DONTWAIT);
    if (status == SALTS_OK &&
        (received.length != fixture->payload_size ||
         memcmp(received.data, fixture->payload[lane],
                fixture->payload_size) != 0)) {
      mem_slice_release(&received);
      return SALTS_EPROTO;
    }
    if (status == SALTS_OK) mem_slice_release(&received);
  }
  if (status == SALTS_OK && fixture->measuring)
    ++fixture->receive_deliveries;
  return status;
}

static int shared_wait_round(shared_wait_fixture_t *fixture,
                             uint64_t latency_out[SHARED_WAIT_LANES]) {
  bool sent[SHARED_WAIT_LANES] = {false, false};
  bool received[SHARED_WAIT_LANES] = {false, false};
  uint64_t started[SHARED_WAIT_LANES] = {0u, 0u};
  const uint64_t deadline = shared_wait_deadline_ns();

  if (latency_out != NULL)
    memset(latency_out, 0, SHARED_WAIT_LANES * sizeof(*latency_out));

  for (;;) {
    bool all_sent = true;
    for (size_t lane = 0u; lane < SHARED_WAIT_LANES; ++lane) {
      int status;
      uint64_t attempted;
      if (sent[lane]) continue;
      all_sent = false;
      attempted = shared_wait_clock_ns(CLOCK_MONOTONIC);
      if (attempted == 0u) return SALTS_EIO;
      status = shared_wait_try_send(fixture, lane);
      if (status == SALTS_OK) {
        sent[lane] = true;
        started[lane] = attempted;
      } else if (status != SALTS_EBUSY && status != SALTS_ENOBUFS) {
        return status;
      }
    }
    if (all_sent ||
        (sent[0] && sent[1]))
      break;
    {
      const int status =
          shared_wait_progress(fixture, SHARED_WAIT_MAX_WAIT_MS);
      if (status != SALTS_OK) return status;
    }
    if (shared_wait_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }

  while (!received[0] || !received[1]) {
    for (size_t lane = 0u; lane < SHARED_WAIT_LANES; ++lane) {
      int status;
      if (received[lane]) continue;
      status = shared_wait_try_receive(fixture, lane);
      if (status == SALTS_OK) {
        const uint64_t finished = shared_wait_clock_ns(CLOCK_MONOTONIC);
        if (finished <= started[lane]) return SALTS_EIO;
        received[lane] = true;
        if (latency_out != NULL)
          latency_out[lane] = finished - started[lane];
      } else if (status != SALTS_EBUSY) {
        return status;
      }
    }
    if (received[0] && received[1]) break;
    {
      const int status =
          shared_wait_progress(fixture, SHARED_WAIT_MAX_WAIT_MS);
      if (status != SALTS_OK) return status;
    }
    if (shared_wait_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }

  return SALTS_OK;
}

static int shared_wait_fixture_init(
    shared_wait_fixture_t *fixture,
    shared_wait_peer_t peers[SHARED_WAIT_LANES],
    size_t payload_size,
    shared_wait_path_t path,
    shared_wait_mode_t mode) {
  int status = SALTS_OK;
  memset(fixture, 0, sizeof(*fixture));
  fixture->payload_size = payload_size;
  fixture->path = path;
  fixture->mode = mode;

  if (mode == SHARED_WAIT_CANDIDATE) {
    const native_io_backend_config config = {
        NATIVE_IO_BACKEND_EPOLL,
        SHARED_WAIT_BACKEND_ENDPOINT_CAPACITY,
        SHARED_WAIT_BACKEND_REQUEST_CAPACITY,
        SHARED_WAIT_COMPLETION_CAPACITY};
    status = native_io_backend_init(&fixture->backend, &config);
    if (status != SALTS_OK) return status;
    fixture->backend_initialized = true;
  }

  fixture->ctx = flowmq_ctx_new();
  if (fixture->ctx == NULL) return SALTS_ENOMEM;

  for (size_t lane = 0u;
       lane < SHARED_WAIT_LANES && status == SALTS_OK; ++lane) {
    fixture->payload[lane] = (unsigned char *)malloc(payload_size);
    fixture->received[lane] = (unsigned char *)malloc(payload_size);
    if (fixture->payload[lane] == NULL || fixture->received[lane] == NULL) {
      status = SALTS_ENOMEM;
      break;
    }
    for (size_t index = 0u; index < payload_size; ++index) {
      fixture->payload[lane][index] =
          (unsigned char)((index * 29u + lane * 53u + 11u) & 0xffu);
    }
    if (path == SHARED_WAIT_RETAINED) {
      fixture->payload_buffer[lane] =
          mem_wrap_external(fixture->payload[lane], payload_size, NULL, NULL);
      if (fixture->payload_buffer[lane] == NULL) {
        status = SALTS_ENOMEM;
        break;
      }
      fixture->payload_slice[lane] =
          mem_slice(fixture->payload_buffer[lane], 0u, payload_size);
      if (fixture->payload_slice[lane].buffer == NULL) {
        status = SALTS_ENOMEM;
        break;
      }
    }

    fixture->sockets[lane] = flowmq_socket(fixture->ctx, FLOWMQ_PAIR);
    if (fixture->sockets[lane] == NULL) {
      status = SALTS_ENOMEM;
      break;
    }
    if (mode == SHARED_WAIT_CANDIDATE) {
      status = flowmq_socket_internal_attach_external_backend(
          fixture->sockets[lane], &fixture->backend);
      if (status != SALTS_OK) break;
    }
    status = flowmq_connect(fixture->sockets[lane], peers[lane].endpoint);
  }

  return status;
}

static int shared_wait_fixture_stop_candidate(shared_wait_fixture_t *fixture) {
  bool stopped[SHARED_WAIT_LANES] = {false, false};
  const uint64_t deadline = shared_wait_deadline_ns();

  while (!stopped[0] || !stopped[1]) {
    for (size_t lane = 0u; lane < SHARED_WAIT_LANES; ++lane) {
      int status;
      if (stopped[lane] || fixture->sockets[lane] == NULL) {
        stopped[lane] = true;
        continue;
      }
      status =
          flowmq_socket_internal_stop_external(fixture->sockets[lane]);
      if (status == SALTS_OK) {
        stopped[lane] = true;
      } else if (status != SALTS_EBUSY) {
        return status;
      }
    }
    if (stopped[0] && stopped[1]) break;
    {
      const int status =
          shared_wait_candidate_progress(fixture, SHARED_WAIT_MAX_WAIT_MS);
      if (status != SALTS_OK) return status;
    }
    if (shared_wait_deadline_expired(deadline)) return SALTS_ETIMEDOUT;
  }
  return SALTS_OK;
}

static int shared_wait_fixture_destroy(shared_wait_fixture_t *fixture) {
  int result = SALTS_OK;

  if (fixture->mode == SHARED_WAIT_CANDIDATE &&
      fixture->backend_initialized) {
    const int stop_status = shared_wait_fixture_stop_candidate(fixture);
    if (result == SALTS_OK && stop_status != SALTS_OK) result = stop_status;
  }

  for (size_t lane = 0u; lane < SHARED_WAIT_LANES; ++lane) {
    if (fixture->sockets[lane] != NULL) {
      const int status = flowmq_close(fixture->sockets[lane]);
      if (result == SALTS_OK && status != SALTS_OK) result = status;
      fixture->sockets[lane] = NULL;
    }
  }
  if (fixture->ctx != NULL) {
    const int status = flowmq_ctx_term(fixture->ctx);
    if (result == SALTS_OK && status != SALTS_OK) result = status;
    fixture->ctx = NULL;
  }

  if (fixture->backend_initialized) {
    int status = native_io_backend_close(&fixture->backend);
    if (result == SALTS_OK && status != SALTS_OK &&
        status != SALTS_EALREADY)
      result = status;
    status = native_io_backend_destroy(&fixture->backend);
    if (result == SALTS_OK && status != SALTS_OK) result = status;
    fixture->backend_initialized = false;
  }

  for (size_t lane = 0u; lane < SHARED_WAIT_LANES; ++lane) {
    mem_slice_release(&fixture->payload_slice[lane]);
    if (fixture->payload_buffer[lane] != NULL) {
      if (result == SALTS_OK &&
          mem_buffer_ref_count(fixture->payload_buffer[lane]) != 1u)
        result = SALTS_EPROTO;
      mem_buffer_release(fixture->payload_buffer[lane]);
      fixture->payload_buffer[lane] = NULL;
    }
    free(fixture->received[lane]);
    free(fixture->payload[lane]);
    fixture->received[lane] = NULL;
    fixture->payload[lane] = NULL;
  }
  return result;
}

static int shared_wait_u64_compare(const void *left, const void *right) {
  const uint64_t a = *(const uint64_t *)left;
  const uint64_t b = *(const uint64_t *)right;
  return a < b ? -1 : a > b ? 1 : 0;
}

static uint64_t shared_wait_percentile(uint64_t *values,
                                       size_t count,
                                       unsigned percentile) {
  size_t index;
  qsort(values, count, sizeof(*values), shared_wait_u64_compare);
  index = ((count - 1u) * (size_t)percentile + 50u) / 100u;
  return values[index];
}

static int shared_wait_set_affinity(int cpu) {
  cpu_set_t set;
  int status;
  if (cpu < 0 || cpu >= CPU_SETSIZE) return SALTS_EINVAL;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  status = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
  return status == 0 ? SALTS_OK : -status;
}

static int shared_wait_restore_affinity(const cpu_set_t *set) {
  const int status =
      pthread_setaffinity_np(pthread_self(), sizeof(*set), set);
  return status == 0 ? SALTS_OK : -status;
}

static int shared_wait_run_mode(
    shared_wait_mode_t mode,
    shared_wait_path_t path,
    size_t payload_size,
    size_t repeat,
    int cpu,
    const cpu_set_t *original_affinity,
    shared_wait_sample_t *out) {
  shared_wait_peer_t peers[SHARED_WAIT_LANES];
  shared_wait_fixture_t fixture;
  uint64_t latencies[SHARED_WAIT_LANES * SHARED_WAIT_SAMPLES];
  const size_t cycles = SHARED_WAIT_WARMUPS + SHARED_WAIT_SAMPLES;
  uint64_t wall_started = 0u;
  uint64_t wall_ns = 0u;
  uint64_t cpu_started = 0u;
  uint64_t owner_cpu_ns = 0u;
  size_t latency_count = 0u;
  int status = SALTS_OK;
  bool fixture_initialized = false;

  if (out == NULL || original_affinity == NULL) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  memset(peers, 0, sizeof(peers));
  memset(&fixture, 0, sizeof(fixture));

  status = shared_wait_restore_affinity(original_affinity);
  if (status != SALTS_OK) return status;

  for (size_t lane = 0u;
       lane < SHARED_WAIT_LANES && status == SALTS_OK; ++lane)
    status = shared_wait_peer_init(&peers[lane], payload_size, cycles, path);
  if (status != SALTS_OK) goto cleanup;

  status = shared_wait_set_affinity(cpu);
  if (status != SALTS_OK) goto cleanup;
  if (sched_getcpu() != cpu) {
    status = SALTS_EPROTO;
    goto cleanup;
  }

  status = shared_wait_fixture_init(
      &fixture, peers, payload_size, path, mode);
  fixture_initialized = true;
  if (status != SALTS_OK) goto cleanup;

  for (size_t warmup = 0u;
       warmup < SHARED_WAIT_WARMUPS; ++warmup) {
    status = shared_wait_round(&fixture, NULL);
    if (status != SALTS_OK) goto cleanup;
  }

  fixture.control_poll_calls = 0u;
  fixture.advance_calls = 0u;
  fixture.observe_calls = 0u;
  fixture.route_attempts = 0u;
  fixture.routed_completions = 0u;
  fixture.unrelated_route_attempts = 0u;
  fixture.send_admissions = 0u;
  fixture.receive_deliveries = 0u;
  fixture.measuring = true;

  wall_started = shared_wait_clock_ns(CLOCK_MONOTONIC);
  cpu_started = shared_wait_clock_ns(CLOCK_THREAD_CPUTIME_ID);
  if (wall_started == 0u || cpu_started == 0u) {
    status = SALTS_EIO;
    goto cleanup;
  }

  for (size_t sample = 0u;
       sample < SHARED_WAIT_SAMPLES; ++sample) {
    uint64_t lane_latencies[SHARED_WAIT_LANES] = {0u, 0u};
    status = shared_wait_round(&fixture, lane_latencies);
    if (status != SALTS_OK) goto cleanup;
    for (size_t lane = 0u; lane < SHARED_WAIT_LANES; ++lane)
      latencies[latency_count++] = lane_latencies[lane];
  }

  {
    const uint64_t cpu_finished =
        shared_wait_clock_ns(CLOCK_THREAD_CPUTIME_ID);
    const uint64_t wall_finished =
        shared_wait_clock_ns(CLOCK_MONOTONIC);
    if (cpu_finished <= cpu_started || wall_finished <= wall_started) {
      status = SALTS_EIO;
      goto cleanup;
    }
    owner_cpu_ns = cpu_finished - cpu_started;
    wall_ns = wall_finished - wall_started;
  }
  fixture.measuring = false;

  if (latency_count != SHARED_WAIT_LANES * SHARED_WAIT_SAMPLES ||
      fixture.send_admissions != latency_count ||
      fixture.receive_deliveries != latency_count ||
      wall_ns == 0u || owner_cpu_ns == 0u) {
    status = SALTS_EPROTO;
    goto cleanup;
  }
  if (mode == SHARED_WAIT_CANDIDATE &&
      (fixture.observe_calls == 0u ||
       fixture.routed_completions == 0u ||
       fixture.unrelated_route_attempts == 0u)) {
    status = SALTS_EPROTO;
    goto cleanup;
  }

  out->mode = mode == SHARED_WAIT_CONTROL ? "public_poll" : "shared_wait";
  out->path = path == SHARED_WAIT_COPY ? "copy" : "retained";
  out->payload_size = payload_size;
  out->repeat = repeat;
  out->logical_operations = latency_count;
  out->cpu = cpu;
  out->wall_ns = wall_ns;
  out->owner_cpu_ns = owner_cpu_ns;
  out->p50_ns = shared_wait_percentile(latencies, latency_count, 50u);
  out->p95_ns = shared_wait_percentile(latencies, latency_count, 95u);
  out->p99_ns = shared_wait_percentile(latencies, latency_count, 99u);
  out->operations_per_second =
      (double)latency_count * 1.0e9 / (double)wall_ns;
  out->mib_per_second =
      ((double)latency_count * (double)payload_size /
       (1024.0 * 1024.0)) *
      1.0e9 / (double)wall_ns;
  out->owner_cpu_us_per_op =
      (double)owner_cpu_ns / 1000.0 / (double)latency_count;
  out->control_poll_calls = fixture.control_poll_calls;
  out->advance_calls = fixture.advance_calls;
  out->observe_calls = fixture.observe_calls;
  out->route_attempts = fixture.route_attempts;
  out->routed_completions = fixture.routed_completions;
  out->unrelated_route_attempts = fixture.unrelated_route_attempts;
  out->send_admissions = fixture.send_admissions;
  out->receive_deliveries = fixture.receive_deliveries;

cleanup:
  fixture.measuring = false;
  if (fixture_initialized) {
    const int destroy_status = shared_wait_fixture_destroy(&fixture);
    if (status == SALTS_OK && destroy_status != SALTS_OK)
      status = destroy_status;
  }
  for (size_t lane = 0u; lane < SHARED_WAIT_LANES; ++lane) {
    const int peer_status =
        shared_wait_peer_destroy(&peers[lane], status != SALTS_OK);
    if (status == SALTS_OK && peer_status != SALTS_OK) status = peer_status;
  }
  {
    const int restore_status =
        shared_wait_restore_affinity(original_affinity);
    if (status == SALTS_OK && restore_status != SALTS_OK)
      status = restore_status;
  }
  return status;
}

static int shared_wait_parse_cpu(int *out_cpu) {
  const char *value = getenv("FLOWMQ_SHARED_WAIT_CPU");
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

static int shared_wait_double_compare(const void *left, const void *right) {
  const double a = *(const double *)left;
  const double b = *(const double *)right;
  return a < b ? -1 : a > b ? 1 : 0;
}

static double shared_wait_double_median(double *values, size_t count) {
  qsort(values, count, sizeof(*values), shared_wait_double_compare);
  return values[count / 2u];
}

static const char *shared_wait_mode_name(shared_wait_mode_t mode) {
  return mode == SHARED_WAIT_CONTROL ? "public_poll" : "shared_wait";
}

static const char *shared_wait_path_name(shared_wait_path_t path) {
  return path == SHARED_WAIT_COPY ? "copy" : "retained";
}

static FILE *shared_wait_open_csv(void) {
  const char *prefix = getenv("FLOWMQ_SHARED_WAIT_OUTPUT");
  char path[1024];
  if (prefix == NULL || *prefix == '\0') return NULL;
  if (snprintf(path, sizeof(path), "%s.csv", prefix) < 0) return NULL;
  return fopen(path, "w");
}

static int shared_wait_write_csv(FILE *csv,
                                 const shared_wait_sample_t *sample) {
  if (csv == NULL || sample == NULL) return SALTS_OK;
  return fprintf(
             csv,
             "%s,%s,%zu,%zu,%zu,%d,%" PRIu64 ",%" PRIu64
             ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
             ",%.6f,%.6f,%.6f,%zu,%zu,%zu,%zu,%zu,%zu,%zu,%zu\n",
             sample->path, sample->mode, sample->payload_size,
             sample->repeat, sample->logical_operations, sample->cpu,
             sample->wall_ns, sample->owner_cpu_ns,
             sample->p50_ns, sample->p95_ns, sample->p99_ns,
             sample->operations_per_second, sample->mib_per_second,
             sample->owner_cpu_us_per_op,
             sample->control_poll_calls, sample->advance_calls,
             sample->observe_calls, sample->route_attempts,
             sample->routed_completions, sample->unrelated_route_attempts,
             sample->send_admissions, sample->receive_deliveries) < 0
             ? SALTS_EIO
             : SALTS_OK;
}

int main(void) {
  shared_wait_sample_t
      results[SHARED_WAIT_PATH_COUNT]
             [SHARED_WAIT_PAYLOAD_COUNT]
             [SHARED_WAIT_MODE_COUNT]
             [SHARED_WAIT_REPEATS];
  cpu_set_t original_affinity;
  FILE *csv = NULL;
  const char *output_prefix = getenv("FLOWMQ_SHARED_WAIT_OUTPUT");
  int cpu = -1;
  int status;

  if (pthread_getaffinity_np(
          pthread_self(), sizeof(original_affinity),
          &original_affinity) != 0) {
    fprintf(stderr, "failed to read original CPU affinity\n");
    return 2;
  }
  status = shared_wait_parse_cpu(&cpu);
  if (status != SALTS_OK) {
    fprintf(stderr, "invalid FLOWMQ_SHARED_WAIT_CPU\n");
    return 2;
  }

  memset(results, 0, sizeof(results));
  csv = shared_wait_open_csv();
  if (output_prefix != NULL && *output_prefix != '\0' && csv == NULL) {
    fprintf(stderr, "failed to create shared-wait CSV output\n");
    return 2;
  }
  if (csv != NULL) {
    fprintf(
        csv,
        "path,mode,payload_bytes,repeat,logical_operations,cpu,"
        "wall_ns,owner_cpu_ns,p50_ns,p95_ns,p99_ns,"
        "operations_per_second,mib_per_second,owner_cpu_us_per_op,"
        "control_poll_calls,advance_calls,observe_calls,route_attempts,"
        "routed_completions,unrelated_route_attempts,"
        "send_admissions,receive_deliveries\n");
  }

  for (size_t path = 0u; path < SHARED_WAIT_PATH_COUNT; ++path) {
    for (size_t payload = 0u;
         payload < SHARED_WAIT_PAYLOAD_COUNT; ++payload) {
      for (size_t repeat = 0u;
           repeat < SHARED_WAIT_REPEATS; ++repeat) {
        for (size_t offset = 0u;
             offset < SHARED_WAIT_MODE_COUNT; ++offset) {
          const shared_wait_mode_t mode =
              (shared_wait_mode_t)(
                  (path + payload + repeat + offset) %
                  SHARED_WAIT_MODE_COUNT);
          shared_wait_sample_t *sample =
              &results[path][payload][mode][repeat];
          status = shared_wait_run_mode(
              mode, (shared_wait_path_t)path,
              SHARED_WAIT_PAYLOADS[payload], repeat + 1u,
              cpu, &original_affinity, sample);
          if (status != SALTS_OK) {
            fprintf(stderr,
                    "shared-wait benchmark failed: "
                    "mode=%s path=%s payload=%zu repeat=%zu status=%d\n",
                    shared_wait_mode_name(mode),
                    shared_wait_path_name((shared_wait_path_t)path),
                    SHARED_WAIT_PAYLOADS[payload], repeat + 1u, status);
            goto cleanup;
          }
          status = shared_wait_write_csv(csv, sample);
          if (status != SALTS_OK) goto cleanup;
        }
      }
    }
  }

  printf("# FlowMQ one-owner shared-wait prototype\n\n");
  printf("Owner CPU: %d. Two client sockets are concurrently active in every "
         "measured round; echo-peer threads remain scheduler-managed.\n\n", cpu);
  printf("| path | payload | mode | ops/s median | p50 us | p99 us | "
         "owner CPU us/op | owner wait calls |\n");
  printf("| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |\n");

  for (size_t path = 0u; path < SHARED_WAIT_PATH_COUNT; ++path) {
    for (size_t payload = 0u;
         payload < SHARED_WAIT_PAYLOAD_COUNT; ++payload) {
      double rate[SHARED_WAIT_MODE_COUNT][SHARED_WAIT_REPEATS];
      double p50[SHARED_WAIT_MODE_COUNT][SHARED_WAIT_REPEATS];
      double p99[SHARED_WAIT_MODE_COUNT][SHARED_WAIT_REPEATS];
      double cpu_us[SHARED_WAIT_MODE_COUNT][SHARED_WAIT_REPEATS];
      double waits[SHARED_WAIT_MODE_COUNT][SHARED_WAIT_REPEATS];
      double speedup[SHARED_WAIT_REPEATS];
      double p99_ratio[SHARED_WAIT_REPEATS];
      double cpu_ratio[SHARED_WAIT_REPEATS];
      double wait_ratio[SHARED_WAIT_REPEATS];

      for (size_t mode = 0u;
           mode < SHARED_WAIT_MODE_COUNT; ++mode) {
        for (size_t repeat = 0u;
             repeat < SHARED_WAIT_REPEATS; ++repeat) {
          const shared_wait_sample_t *sample =
              &results[path][payload][mode][repeat];
          rate[mode][repeat] = sample->operations_per_second;
          p50[mode][repeat] = (double)sample->p50_ns / 1000.0;
          p99[mode][repeat] = (double)sample->p99_ns / 1000.0;
          cpu_us[mode][repeat] = sample->owner_cpu_us_per_op;
          waits[mode][repeat] =
              mode == SHARED_WAIT_CONTROL
                  ? (double)sample->control_poll_calls
                  : (double)sample->observe_calls;
        }
        printf("| %s | %zu | %s | %.0f | %.3f | %.3f | %.3f | %.0f |\n",
               shared_wait_path_name((shared_wait_path_t)path),
               SHARED_WAIT_PAYLOADS[payload],
               shared_wait_mode_name((shared_wait_mode_t)mode),
               shared_wait_double_median(
                   rate[mode], SHARED_WAIT_REPEATS),
               shared_wait_double_median(
                   p50[mode], SHARED_WAIT_REPEATS),
               shared_wait_double_median(
                   p99[mode], SHARED_WAIT_REPEATS),
               shared_wait_double_median(
                   cpu_us[mode], SHARED_WAIT_REPEATS),
               shared_wait_double_median(
                   waits[mode], SHARED_WAIT_REPEATS));
      }

      for (size_t repeat = 0u;
           repeat < SHARED_WAIT_REPEATS; ++repeat) {
        const shared_wait_sample_t *control =
            &results[path][payload][SHARED_WAIT_CONTROL][repeat];
        const shared_wait_sample_t *candidate =
            &results[path][payload][SHARED_WAIT_CANDIDATE][repeat];
        speedup[repeat] =
            candidate->operations_per_second /
            control->operations_per_second;
        p99_ratio[repeat] =
            (double)candidate->p99_ns / (double)control->p99_ns;
        cpu_ratio[repeat] =
            candidate->owner_cpu_us_per_op /
            control->owner_cpu_us_per_op;
        wait_ratio[repeat] =
            (double)candidate->observe_calls /
            (double)control->control_poll_calls;
      }

      printf("\n%s %zu candidate/control medians: throughput %.3fx, "
             "p99 %.3fx, owner CPU %.3fx, wait calls %.3fx\n\n",
             shared_wait_path_name((shared_wait_path_t)path),
             SHARED_WAIT_PAYLOADS[payload],
             shared_wait_double_median(speedup, SHARED_WAIT_REPEATS),
             shared_wait_double_median(p99_ratio, SHARED_WAIT_REPEATS),
             shared_wait_double_median(cpu_ratio, SHARED_WAIT_REPEATS),
             shared_wait_double_median(wait_ratio, SHARED_WAIT_REPEATS));
    }
  }

  status = SALTS_OK;

cleanup:
  if (csv != NULL && fclose(csv) != 0 && status == SALTS_OK)
    status = SALTS_EIO;
  (void)shared_wait_restore_affinity(&original_affinity);
  return status == SALTS_OK ? 0 : 1;
}
