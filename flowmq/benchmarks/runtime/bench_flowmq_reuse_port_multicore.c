#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "flowmq_socket.h"
#include "salts_error.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum {
  REUSE_PORT_CLIENTS = 4,
  REUSE_PORT_SERVERS = 2,
  REUSE_PORT_WARMUPS = 8,
  REUSE_PORT_SAMPLES = 64,
  REUSE_PORT_REPEATS = 7,
  REUSE_PORT_PAYLOAD_COUNT = 2,
  REUSE_PORT_TIMEOUT_MS = 10000
};

static const size_t REUSE_PORT_PAYLOADS[] = {1024u, 65536u};

typedef enum reuse_port_mode_e {
  REUSE_PORT_ONE_OWNER = 0,
  REUSE_PORT_TWO_OWNERS = 1,
  REUSE_PORT_MODE_COUNT
} reuse_port_mode_t;

typedef struct reuse_port_shared_s {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  char endpoint[128];
  size_t server_ready;
  size_t client_ready;
  size_t clients_done;
  atomic_int start;
  atomic_int stop;
  atomic_int failure;
} reuse_port_shared_t;

typedef struct reuse_port_server_arg_s {
  reuse_port_shared_t *shared;
  size_t index;
  int cpu;
  int observed_cpu;
  size_t warmup_requests;
  size_t measured_requests;
  uint64_t cpu_ns;
  int status;
} reuse_port_server_arg_t;

typedef struct reuse_port_client_arg_s {
  reuse_port_shared_t *shared;
  size_t client_id;
  size_t payload_size;
  uint64_t latencies[REUSE_PORT_SAMPLES];
  int status;
} reuse_port_client_arg_t;

typedef struct reuse_port_sample_s {
  const char *mode;
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
  size_t server0_requests;
  size_t server1_requests;
} reuse_port_sample_t;

static uint64_t reuse_port_clock_ns(clockid_t clock_id) {
  struct timespec value;
  if (clock_gettime(clock_id, &value) != 0) return 0u;
  return (uint64_t)value.tv_sec * UINT64_C(1000000000) +
         (uint64_t)value.tv_nsec;
}

static uint64_t reuse_port_thread_cpu_ns(void) {
  return reuse_port_clock_ns(CLOCK_THREAD_CPUTIME_ID);
}

static int reuse_port_set_affinity(int cpu) {
  cpu_set_t set;
  int status;
  if (cpu < 0 || cpu >= CPU_SETSIZE) return SALTS_EINVAL;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  status = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
  return status == 0 ? SALTS_OK : -status;
}

static void reuse_port_fail(reuse_port_shared_t *shared, int status) {
  int expected = SALTS_OK;
  if (status == SALTS_OK) status = SALTS_EIO;
  (void)atomic_compare_exchange_strong(&shared->failure, &expected, status);
  atomic_store(&shared->start, 1);
  atomic_store(&shared->stop, 1);
  (void)pthread_mutex_lock(&shared->mutex);
  (void)pthread_cond_broadcast(&shared->changed);
  (void)pthread_mutex_unlock(&shared->mutex);
}

static int reuse_port_shared_init(reuse_port_shared_t *shared) {
  int status;
  memset(shared, 0, sizeof(*shared));
  atomic_init(&shared->start, 0);
  atomic_init(&shared->stop, 0);
  atomic_init(&shared->failure, SALTS_OK);
  status = pthread_mutex_init(&shared->mutex, NULL);
  if (status != 0) return -status;
  status = pthread_cond_init(&shared->changed, NULL);
  if (status != 0) {
    (void)pthread_mutex_destroy(&shared->mutex);
    return -status;
  }
  return SALTS_OK;
}

static void reuse_port_shared_destroy(reuse_port_shared_t *shared) {
  (void)pthread_cond_destroy(&shared->changed);
  (void)pthread_mutex_destroy(&shared->mutex);
}

static int reuse_port_socket_progress(flowmq_socket_t *socket) {
  flowmq_pollitem_t item = {
      .socket = socket,
      .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT | FLOWMQ_POLLERR};
  size_t ready = 0u;
  int status = flowmq_poll(&item, 1u, 0u, &ready);
  if (status != SALTS_OK) return status;
  if ((item.revents & FLOWMQ_POLLERR) != 0) return SALTS_EIO;
  return SALTS_OK;
}

static int reuse_port_server_send(flowmq_socket_t *socket,
                                  const unsigned char *payload,
                                  size_t payload_size,
                                  reuse_port_shared_t *shared) {
  int status = flowmq_send(socket, payload, payload_size, FLOWMQ_DONTWAIT);
  while (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
    if (atomic_load(&shared->failure) != SALTS_OK) return SALTS_EIO;
    status = reuse_port_socket_progress(socket);
    if (status != SALTS_OK) return status;
    sched_yield();
    status = flowmq_send(socket, payload, payload_size, FLOWMQ_DONTWAIT);
  }
  return status;
}

static void *reuse_port_server_entry(void *user) {
  reuse_port_server_arg_t *arg = (reuse_port_server_arg_t *)user;
  reuse_port_shared_t *shared = arg->shared;
  flowmq_ctx_t *ctx = NULL;
  flowmq_socket_t *socket = NULL;
  unsigned char *payload = NULL;
  char endpoint[128] = {0};
  size_t endpoint_size = 0u;
  size_t received = 0u;
  uint64_t cpu_started = 0u;
  bool measuring = false;
  int reuse_port = 1;
  int status;

  arg->observed_cpu = -1;
  arg->status = SALTS_OK;

  status = reuse_port_set_affinity(arg->cpu);
  if (status != SALTS_OK) goto fail;
  arg->observed_cpu = sched_getcpu();
  if (arg->observed_cpu != arg->cpu) {
    status = SALTS_EPROTO;
    goto fail;
  }

  payload = (unsigned char *)malloc(REUSE_PORT_PAYLOADS[REUSE_PORT_PAYLOAD_COUNT - 1u]);
  if (payload == NULL) {
    status = SALTS_ENOMEM;
    goto fail;
  }
  ctx = flowmq_ctx_new();
  if (ctx == NULL) {
    status = SALTS_ENOMEM;
    goto fail;
  }
  socket = flowmq_socket(ctx, FLOWMQ_REP);
  if (socket == NULL) {
    status = SALTS_ENOMEM;
    goto fail;
  }
  status = flowmq_setsockopt(socket, FLOWMQ_REUSE_PORT,
                             &reuse_port, sizeof(reuse_port));
  if (status != SALTS_OK) goto fail;

  if (arg->index == 0u) {
    status = flowmq_bind(socket, "tcp://127.0.0.1:0");
    if (status != SALTS_OK) goto fail;
    status = flowmq_last_endpoint(socket, endpoint, sizeof(endpoint),
                                  &endpoint_size);
    if (status != SALTS_OK) goto fail;

    (void)pthread_mutex_lock(&shared->mutex);
    memcpy(shared->endpoint, endpoint, endpoint_size);
    ++shared->server_ready;
    (void)pthread_cond_broadcast(&shared->changed);
    (void)pthread_mutex_unlock(&shared->mutex);
  } else {
    (void)pthread_mutex_lock(&shared->mutex);
    while (shared->server_ready == 0u &&
           atomic_load(&shared->failure) == SALTS_OK)
      (void)pthread_cond_wait(&shared->changed, &shared->mutex);
    if (atomic_load(&shared->failure) != SALTS_OK) {
      (void)pthread_mutex_unlock(&shared->mutex);
      status = SALTS_EIO;
      goto fail;
    }
    memcpy(endpoint, shared->endpoint, sizeof(endpoint));
    (void)pthread_mutex_unlock(&shared->mutex);

    status = flowmq_bind(socket, endpoint);
    if (status != SALTS_OK) goto fail;
    (void)pthread_mutex_lock(&shared->mutex);
    ++shared->server_ready;
    (void)pthread_cond_broadcast(&shared->changed);
    (void)pthread_mutex_unlock(&shared->mutex);
  }

  for (;;) {
    int failure = atomic_load(&shared->failure);
    if (failure != SALTS_OK) {
      status = failure;
      break;
    }

    if (!measuring && atomic_load(&shared->start)) {
      cpu_started = reuse_port_thread_cpu_ns();
      if (cpu_started == 0u) {
        status = SALTS_EIO;
        break;
      }
      measuring = true;
    }

    received = 0u;
    status = flowmq_recv(socket, payload,
                         REUSE_PORT_PAYLOADS[REUSE_PORT_PAYLOAD_COUNT - 1u],
                         &received, FLOWMQ_DONTWAIT);
    if (status == SALTS_EBUSY) {
      if (atomic_load(&shared->stop)) {
        status = SALTS_OK;
        break;
      }
      status = reuse_port_socket_progress(socket);
      if (status != SALTS_OK) break;
      sched_yield();
      continue;
    }
    if (status != SALTS_OK) break;
    if (received < 2u) {
      status = SALTS_EPROTO;
      break;
    }

    if (payload[0] == 0u)
      ++arg->warmup_requests;
    else if (payload[0] == 1u)
      ++arg->measured_requests;
    else {
      status = SALTS_EPROTO;
      break;
    }

    status = reuse_port_server_send(socket, payload, received, shared);
    if (status != SALTS_OK) break;
  }

  if (measuring) {
    const uint64_t cpu_finished = reuse_port_thread_cpu_ns();
    if (cpu_finished <= cpu_started) {
      if (status == SALTS_OK) status = SALTS_EIO;
    } else {
      arg->cpu_ns = cpu_finished - cpu_started;
    }
  }
  arg->status = status;
  if (status != SALTS_OK) reuse_port_fail(shared, status);

  if (socket != NULL) {
    const int close_status = flowmq_close(socket);
    if (arg->status == SALTS_OK && close_status != SALTS_OK)
      arg->status = close_status;
  }
  if (ctx != NULL) {
    const int term_status = flowmq_ctx_term(ctx);
    if (arg->status == SALTS_OK && term_status != SALTS_OK)
      arg->status = term_status;
  }
  free(payload);
  return NULL;

fail:
  arg->status = status;
  reuse_port_fail(shared, status);
  if (socket != NULL) (void)flowmq_close(socket);
  if (ctx != NULL) (void)flowmq_ctx_term(ctx);
  free(payload);
  return NULL;
}

static int reuse_port_client_cycle(flowmq_socket_t *socket,
                                   unsigned char *payload,
                                   unsigned char *received_buffer,
                                   size_t payload_size,
                                   uint64_t *latency_out) {
  size_t received = 0u;
  uint64_t started = 0u;
  int status;

  if (latency_out != NULL) {
    started = reuse_port_clock_ns(CLOCK_MONOTONIC);
    if (started == 0u) return SALTS_EIO;
  }

  status = flowmq_send(socket, payload, payload_size, 0);
  if (status != SALTS_OK) return status;
  status = flowmq_recv(socket, received_buffer, payload_size, &received, 0);
  if (status != SALTS_OK) return status;
  if (received != payload_size ||
      memcmp(payload, received_buffer, payload_size) != 0)
    return SALTS_EPROTO;

  if (latency_out != NULL) {
    const uint64_t finished = reuse_port_clock_ns(CLOCK_MONOTONIC);
    if (finished <= started) return SALTS_EIO;
    *latency_out = finished - started;
  }
  return SALTS_OK;
}

static void *reuse_port_client_entry(void *user) {
  reuse_port_client_arg_t *arg = (reuse_port_client_arg_t *)user;
  reuse_port_shared_t *shared = arg->shared;
  flowmq_ctx_t *ctx = NULL;
  flowmq_socket_t *socket = NULL;
  unsigned char *payload = NULL;
  unsigned char *received = NULL;
  char endpoint[128] = {0};
  int status = SALTS_OK;

  payload = (unsigned char *)malloc(arg->payload_size);
  received = (unsigned char *)malloc(arg->payload_size);
  if (payload == NULL || received == NULL) {
    status = SALTS_ENOMEM;
    goto fail;
  }
  for (size_t i = 0u; i < arg->payload_size; ++i)
    payload[i] =
        (unsigned char)((i * 29u + arg->client_id * 53u + 11u) & 0xffu);
  payload[1] = (unsigned char)arg->client_id;

  (void)pthread_mutex_lock(&shared->mutex);
  memcpy(endpoint, shared->endpoint, sizeof(endpoint));
  (void)pthread_mutex_unlock(&shared->mutex);

  ctx = flowmq_ctx_new();
  if (ctx == NULL) {
    status = SALTS_ENOMEM;
    goto fail;
  }
  socket = flowmq_socket(ctx, FLOWMQ_REQ);
  if (socket == NULL) {
    status = SALTS_ENOMEM;
    goto fail;
  }
  status = flowmq_connect(socket, endpoint);
  if (status != SALTS_OK) goto fail;

  payload[0] = 0u;
  for (size_t warmup = 0u; warmup < REUSE_PORT_WARMUPS; ++warmup) {
    status = reuse_port_client_cycle(
        socket, payload, received, arg->payload_size, NULL);
    if (status != SALTS_OK) goto fail;
  }

  (void)pthread_mutex_lock(&shared->mutex);
  ++shared->client_ready;
  (void)pthread_cond_broadcast(&shared->changed);
  while (!atomic_load(&shared->start) &&
         atomic_load(&shared->failure) == SALTS_OK)
    (void)pthread_cond_wait(&shared->changed, &shared->mutex);
  (void)pthread_mutex_unlock(&shared->mutex);
  if (atomic_load(&shared->failure) != SALTS_OK) {
    status = SALTS_EIO;
    goto fail;
  }

  payload[0] = 1u;
  for (size_t sample = 0u; sample < REUSE_PORT_SAMPLES; ++sample) {
    if (arg->payload_size >= 10u) {
      const uint64_t value =
          ((uint64_t)arg->client_id << 56u) | (uint64_t)sample;
      memcpy(payload + 2u, &value, sizeof(value));
    }
    status = reuse_port_client_cycle(
        socket, payload, received, arg->payload_size,
        &arg->latencies[sample]);
    if (status != SALTS_OK) goto fail;
  }

  (void)pthread_mutex_lock(&shared->mutex);
  ++shared->clients_done;
  (void)pthread_cond_broadcast(&shared->changed);
  (void)pthread_mutex_unlock(&shared->mutex);

  arg->status = SALTS_OK;
  if (socket != NULL) {
    const int close_status = flowmq_close(socket);
    if (arg->status == SALTS_OK && close_status != SALTS_OK)
      arg->status = close_status;
  }
  if (ctx != NULL) {
    const int term_status = flowmq_ctx_term(ctx);
    if (arg->status == SALTS_OK && term_status != SALTS_OK)
      arg->status = term_status;
  }
  free(received);
  free(payload);
  return NULL;

fail:
  arg->status = status;
  reuse_port_fail(shared, status);
  if (socket != NULL) (void)flowmq_close(socket);
  if (ctx != NULL) (void)flowmq_ctx_term(ctx);
  free(received);
  free(payload);
  return NULL;
}

static int reuse_port_u64_compare(const void *left, const void *right) {
  const uint64_t a = *(const uint64_t *)left;
  const uint64_t b = *(const uint64_t *)right;
  return a < b ? -1 : a > b ? 1 : 0;
}

static uint64_t reuse_port_percentile(uint64_t *values,
                                      size_t count,
                                      unsigned percentile) {
  size_t index;
  qsort(values, count, sizeof(*values), reuse_port_u64_compare);
  index = ((count - 1u) * (size_t)percentile + 50u) / 100u;
  return values[index];
}

static int reuse_port_run_mode(reuse_port_mode_t mode,
                               size_t payload_size,
                               size_t repeat,
                               int cpu_a,
                               int cpu_b,
                               reuse_port_sample_t *out) {
  reuse_port_shared_t shared;
  reuse_port_server_arg_t servers[REUSE_PORT_SERVERS];
  reuse_port_client_arg_t clients[REUSE_PORT_CLIENTS];
  pthread_t server_threads[REUSE_PORT_SERVERS];
  pthread_t client_threads[REUSE_PORT_CLIENTS];
  bool server_started[REUSE_PORT_SERVERS] = {false, false};
  bool client_started[REUSE_PORT_CLIENTS] = {false, false, false, false};
  uint64_t latencies[REUSE_PORT_CLIENTS * REUSE_PORT_SAMPLES];
  const size_t server_count =
      mode == REUSE_PORT_ONE_OWNER ? 1u : REUSE_PORT_SERVERS;
  const size_t expected_warmups =
      REUSE_PORT_CLIENTS * REUSE_PORT_WARMUPS;
  const size_t expected_samples =
      REUSE_PORT_CLIENTS * REUSE_PORT_SAMPLES;
  size_t latency_count = 0u;
  uint64_t wall_started = 0u;
  uint64_t wall_ns = 0u;
  uint64_t owner_cpu_ns = 0u;
  int status = reuse_port_shared_init(&shared);

  if (out == NULL) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  memset(servers, 0, sizeof(servers));
  memset(clients, 0, sizeof(clients));
  if (status != SALTS_OK) return status;

  for (size_t index = 0u;
       index < server_count && status == SALTS_OK; ++index) {
    servers[index].shared = &shared;
    servers[index].index = index;
    servers[index].cpu = index == 0u ? cpu_a : cpu_b;
    status = pthread_create(
        &server_threads[index], NULL,
        reuse_port_server_entry, &servers[index]);
    if (status != 0) status = -status;
    else server_started[index] = true;
  }
  if (status != SALTS_OK) {
    reuse_port_fail(&shared, status);
    goto cleanup;
  }

  (void)pthread_mutex_lock(&shared.mutex);
  while (shared.server_ready < server_count &&
         atomic_load(&shared.failure) == SALTS_OK)
    (void)pthread_cond_wait(&shared.changed, &shared.mutex);
  status = atomic_load(&shared.failure);
  (void)pthread_mutex_unlock(&shared.mutex);
  if (status != SALTS_OK) goto cleanup;

  for (size_t index = 0u;
       index < REUSE_PORT_CLIENTS && status == SALTS_OK; ++index) {
    clients[index].shared = &shared;
    clients[index].client_id = index;
    clients[index].payload_size = payload_size;
    status = pthread_create(
        &client_threads[index], NULL,
        reuse_port_client_entry, &clients[index]);
    if (status != 0) status = -status;
    else client_started[index] = true;
  }
  if (status != SALTS_OK) {
    reuse_port_fail(&shared, status);
    goto cleanup;
  }

  (void)pthread_mutex_lock(&shared.mutex);
  while (shared.client_ready < REUSE_PORT_CLIENTS &&
         atomic_load(&shared.failure) == SALTS_OK)
    (void)pthread_cond_wait(&shared.changed, &shared.mutex);
  status = atomic_load(&shared.failure);
  if (status == SALTS_OK) {
    wall_started = reuse_port_clock_ns(CLOCK_MONOTONIC);
    if (wall_started == 0u)
      status = SALTS_EIO;
    else {
      atomic_store(&shared.start, 1);
      (void)pthread_cond_broadcast(&shared.changed);
    }
  }
  (void)pthread_mutex_unlock(&shared.mutex);
  if (status != SALTS_OK) {
    reuse_port_fail(&shared, status);
    goto cleanup;
  }

  (void)pthread_mutex_lock(&shared.mutex);
  while (shared.clients_done < REUSE_PORT_CLIENTS &&
         atomic_load(&shared.failure) == SALTS_OK)
    (void)pthread_cond_wait(&shared.changed, &shared.mutex);
  status = atomic_load(&shared.failure);
  if (status == SALTS_OK) {
    const uint64_t wall_finished =
        reuse_port_clock_ns(CLOCK_MONOTONIC);
    if (wall_finished <= wall_started)
      status = SALTS_EIO;
    else
      wall_ns = wall_finished - wall_started;
  }
  atomic_store(&shared.stop, 1);
  (void)pthread_cond_broadcast(&shared.changed);
  (void)pthread_mutex_unlock(&shared.mutex);

cleanup:
  if (status != SALTS_OK) reuse_port_fail(&shared, status);
  else atomic_store(&shared.stop, 1);

  for (size_t index = 0u; index < REUSE_PORT_CLIENTS; ++index) {
    if (client_started[index]) {
      const int join_status = pthread_join(client_threads[index], NULL);
      if (status == SALTS_OK && join_status != 0) status = -join_status;
      if (status == SALTS_OK && clients[index].status != SALTS_OK)
        status = clients[index].status;
    }
  }
  for (size_t index = 0u; index < server_count; ++index) {
    if (server_started[index]) {
      const int join_status = pthread_join(server_threads[index], NULL);
      if (status == SALTS_OK && join_status != 0) status = -join_status;
      if (status == SALTS_OK && servers[index].status != SALTS_OK)
        status = servers[index].status;
    }
  }

  if (status == SALTS_OK) {
    size_t warmup_total = 0u;
    size_t measured_total = 0u;
    for (size_t index = 0u; index < server_count; ++index) {
      if (servers[index].observed_cpu != servers[index].cpu ||
          servers[index].cpu_ns == 0u) {
        status = SALTS_EPROTO;
        break;
      }
      owner_cpu_ns += servers[index].cpu_ns;
      warmup_total += servers[index].warmup_requests;
      measured_total += servers[index].measured_requests;
    }
    if (status == SALTS_OK &&
        (warmup_total != expected_warmups ||
         measured_total != expected_samples ||
         wall_ns == 0u))
      status = SALTS_EPROTO;

    if (status == SALTS_OK) {
      for (size_t client = 0u; client < REUSE_PORT_CLIENTS; ++client)
        for (size_t sample = 0u; sample < REUSE_PORT_SAMPLES; ++sample)
          latencies[latency_count++] = clients[client].latencies[sample];
      if (latency_count != expected_samples) status = SALTS_EPROTO;
    }
  }

  if (status == SALTS_OK) {
    out->mode =
        mode == REUSE_PORT_ONE_OWNER
            ? "one_owner"
            : "two_owner_reuse_port";
    out->payload_size = payload_size;
    out->repeat = repeat;
    out->logical_operations = expected_samples;
    out->cpu_a = cpu_a;
    out->cpu_b =
        mode == REUSE_PORT_ONE_OWNER ? cpu_a : cpu_b;
    out->wall_ns = wall_ns;
    out->owner_cpu_ns = owner_cpu_ns;
    out->p50_ns =
        reuse_port_percentile(latencies, latency_count, 50u);
    out->p95_ns =
        reuse_port_percentile(latencies, latency_count, 95u);
    out->p99_ns =
        reuse_port_percentile(latencies, latency_count, 99u);
    out->operations_per_second =
        (double)expected_samples * 1.0e9 / (double)wall_ns;
    out->mib_per_second =
        ((double)expected_samples * (double)payload_size /
         (1024.0 * 1024.0)) *
        1.0e9 / (double)wall_ns;
    out->owner_cpu_us_per_op =
        (double)owner_cpu_ns / 1000.0 / (double)expected_samples;
    out->server0_requests = servers[0].measured_requests;
    out->server1_requests =
        mode == REUSE_PORT_TWO_OWNERS
            ? servers[1].measured_requests
            : 0u;
  }

  reuse_port_shared_destroy(&shared);
  return status;
}

static int reuse_port_parse_cpu(const char *name, int *out_cpu) {
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

static int reuse_port_double_compare(const void *left,
                                     const void *right) {
  const double a = *(const double *)left;
  const double b = *(const double *)right;
  return a < b ? -1 : a > b ? 1 : 0;
}

static double reuse_port_double_median(double *values, size_t count) {
  qsort(values, count, sizeof(*values), reuse_port_double_compare);
  return values[count / 2u];
}

static FILE *reuse_port_open_csv(void) {
  const char *prefix = getenv("FLOWMQ_REUSE_PORT_OUTPUT");
  char path[1024];
  if (prefix == NULL || *prefix == '\0') return NULL;
  if (snprintf(path, sizeof(path), "%s.csv", prefix) < 0) return NULL;
  return fopen(path, "w");
}

static int reuse_port_write_csv(FILE *csv,
                                const reuse_port_sample_t *sample) {
  if (csv == NULL || sample == NULL) return SALTS_OK;
  return fprintf(
             csv,
             "%s,%zu,%zu,%zu,%d,%d,%" PRIu64 ",%" PRIu64
             ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
             ",%.6f,%.6f,%.6f,%zu,%zu\n",
             sample->mode, sample->payload_size, sample->repeat,
             sample->logical_operations, sample->cpu_a, sample->cpu_b,
             sample->wall_ns, sample->owner_cpu_ns,
             sample->p50_ns, sample->p95_ns, sample->p99_ns,
             sample->operations_per_second,
             sample->mib_per_second,
             sample->owner_cpu_us_per_op,
             sample->server0_requests,
             sample->server1_requests) < 0
             ? SALTS_EIO
             : SALTS_OK;
}

int main(void) {
  reuse_port_sample_t
      results[REUSE_PORT_PAYLOAD_COUNT]
             [REUSE_PORT_MODE_COUNT]
             [REUSE_PORT_REPEATS];
  FILE *csv = NULL;
  const char *output_prefix = getenv("FLOWMQ_REUSE_PORT_OUTPUT");
  int cpu_a = -1;
  int cpu_b = -1;
  int status;

  status = reuse_port_parse_cpu("FLOWMQ_REUSE_PORT_CPU_A", &cpu_a);
  if (status == SALTS_OK)
    status = reuse_port_parse_cpu("FLOWMQ_REUSE_PORT_CPU_B", &cpu_b);
  if (status != SALTS_OK || cpu_a == cpu_b) {
    fprintf(stderr, "invalid distinct-core reuse-port CPU environment\n");
    return 2;
  }

  memset(results, 0, sizeof(results));
  csv = reuse_port_open_csv();
  if (output_prefix != NULL && *output_prefix != '\0' && csv == NULL) {
    fprintf(stderr, "failed to create reuse-port CSV output\n");
    return 2;
  }
  if (csv != NULL) {
    fprintf(
        csv,
        "mode,payload_bytes,repeat,logical_operations,cpu_a,cpu_b,"
        "wall_ns,owner_cpu_ns,p50_ns,p95_ns,p99_ns,"
        "operations_per_second,mib_per_second,owner_cpu_us_per_op,"
        "server0_requests,server1_requests\n");
  }

  for (size_t payload = 0u;
       payload < REUSE_PORT_PAYLOAD_COUNT; ++payload) {
    for (size_t repeat = 0u;
         repeat < REUSE_PORT_REPEATS; ++repeat) {
      for (size_t offset = 0u;
           offset < REUSE_PORT_MODE_COUNT; ++offset) {
        const reuse_port_mode_t mode =
            (reuse_port_mode_t)(
                (payload + repeat + offset) %
                REUSE_PORT_MODE_COUNT);
        reuse_port_sample_t *sample =
            &results[payload][mode][repeat];
        status = reuse_port_run_mode(
            mode, REUSE_PORT_PAYLOADS[payload], repeat + 1u,
            cpu_a, cpu_b, sample);
        if (status != SALTS_OK) {
          fprintf(stderr,
                  "reuse-port benchmark failed: mode=%u payload=%zu "
                  "repeat=%zu status=%d\n",
                  (unsigned)mode, REUSE_PORT_PAYLOADS[payload],
                  repeat + 1u, status);
          goto cleanup;
        }
        status = reuse_port_write_csv(csv, sample);
        if (status != SALTS_OK) goto cleanup;
      }
    }
  }

  printf("# FlowMQ same-endpoint reuse-port multicore service\n\n");
  printf(
      "Four scheduler-managed REQ clients execute equal request/reply work. "
      "Control uses one pinned REP owner; candidate uses two REP owners on "
      "distinct physical CPUs %d and %d bound to the same TCP endpoint with "
      "FLOWMQ_REUSE_PORT=1. Connections never migrate. No relative "
      "performance threshold is enforced.\n\n",
      cpu_a, cpu_b);
  printf("| payload | mode | ops/s median | MiB/s median | p50 us | "
         "p99 us | service CPU us/op | owner0/owner1 measured requests |\n");
  printf("| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |\n");

  for (size_t payload = 0u;
       payload < REUSE_PORT_PAYLOAD_COUNT; ++payload) {
    double rate[REUSE_PORT_MODE_COUNT][REUSE_PORT_REPEATS];
    double mib[REUSE_PORT_MODE_COUNT][REUSE_PORT_REPEATS];
    double p50[REUSE_PORT_MODE_COUNT][REUSE_PORT_REPEATS];
    double p99[REUSE_PORT_MODE_COUNT][REUSE_PORT_REPEATS];
    double cpu[REUSE_PORT_MODE_COUNT][REUSE_PORT_REPEATS];
    double s0[REUSE_PORT_REPEATS];
    double s1[REUSE_PORT_REPEATS];
    double speedup[REUSE_PORT_REPEATS];
    double p99_ratio[REUSE_PORT_REPEATS];
    double cpu_ratio[REUSE_PORT_REPEATS];

    for (size_t mode = 0u;
         mode < REUSE_PORT_MODE_COUNT; ++mode) {
      for (size_t repeat = 0u;
           repeat < REUSE_PORT_REPEATS; ++repeat) {
        const reuse_port_sample_t *sample =
            &results[payload][mode][repeat];
        rate[mode][repeat] = sample->operations_per_second;
        mib[mode][repeat] = sample->mib_per_second;
        p50[mode][repeat] = (double)sample->p50_ns / 1000.0;
        p99[mode][repeat] = (double)sample->p99_ns / 1000.0;
        cpu[mode][repeat] = sample->owner_cpu_us_per_op;
      }
      if (mode == REUSE_PORT_TWO_OWNERS) {
        for (size_t repeat = 0u;
             repeat < REUSE_PORT_REPEATS; ++repeat) {
          s0[repeat] =
              (double)results[payload][mode][repeat].server0_requests;
          s1[repeat] =
              (double)results[payload][mode][repeat].server1_requests;
        }
      }
      printf("| %zu | %s | %.0f | %.3f | %.3f | %.3f | %.3f | ",
             REUSE_PORT_PAYLOADS[payload],
             mode == REUSE_PORT_ONE_OWNER
                 ? "one_owner"
                 : "two_owner_reuse_port",
             reuse_port_double_median(rate[mode], REUSE_PORT_REPEATS),
             reuse_port_double_median(mib[mode], REUSE_PORT_REPEATS),
             reuse_port_double_median(p50[mode], REUSE_PORT_REPEATS),
             reuse_port_double_median(p99[mode], REUSE_PORT_REPEATS),
             reuse_port_double_median(cpu[mode], REUSE_PORT_REPEATS));
      if (mode == REUSE_PORT_ONE_OWNER)
        printf("%zu/0 |\n",
               (size_t)(REUSE_PORT_CLIENTS * REUSE_PORT_SAMPLES));
      else
        printf("%.0f/%.0f |\n",
               reuse_port_double_median(s0, REUSE_PORT_REPEATS),
               reuse_port_double_median(s1, REUSE_PORT_REPEATS));
    }

    for (size_t repeat = 0u;
         repeat < REUSE_PORT_REPEATS; ++repeat) {
      const reuse_port_sample_t *control =
          &results[payload][REUSE_PORT_ONE_OWNER][repeat];
      const reuse_port_sample_t *candidate =
          &results[payload][REUSE_PORT_TWO_OWNERS][repeat];
      speedup[repeat] =
          candidate->operations_per_second /
          control->operations_per_second;
      p99_ratio[repeat] =
          (double)candidate->p99_ns / (double)control->p99_ns;
      cpu_ratio[repeat] =
          candidate->owner_cpu_us_per_op /
          control->owner_cpu_us_per_op;
    }
    printf(
        "\n%zu-byte two-owner/control paired medians: throughput %.3fx, "
        "p99 %.3fx, service CPU/op %.3fx\n\n",
        REUSE_PORT_PAYLOADS[payload],
        reuse_port_double_median(speedup, REUSE_PORT_REPEATS),
        reuse_port_double_median(p99_ratio, REUSE_PORT_REPEATS),
        reuse_port_double_median(cpu_ratio, REUSE_PORT_REPEATS));
  }

  status = SALTS_OK;

cleanup:
  if (csv != NULL && fclose(csv) != 0 && status == SALTS_OK)
    status = SALTS_EIO;
  return status == SALTS_OK ? 0 : 1;
}
