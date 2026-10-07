#include "flowmq_socket.h"

#include <errno.h>
#include <salts/clock.h>
#include <salts/thread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  EXAMPLE_DEFAULT_WORKERS = 4,
  EXAMPLE_MAX_WORKERS = 16,
  EXAMPLE_DEFAULT_REQUESTS = 100,
  EXAMPLE_MAX_REQUESTS = 10000,
  EXAMPLE_DEFAULT_TIMEOUT_MS = 5000,
  EXAMPLE_MAX_TIMEOUT_MS = 60000,
  EXAMPLE_POLL_MS = 1,
  EXAMPLE_ENDPOINT_BYTES = 128,
  EXAMPLE_MESSAGE_BYTES = 64,
  EXAMPLE_HWM = 1,
  EXAMPLE_RECONNECT_DISABLED = -1
};

typedef struct example_run_s {
  cmeta_mutex_t mutex;
  cmeta_cond_t changed;
  atomic_int error;
  atomic_bool stop;
  size_t ready;
  size_t exited;
  size_t requests;
  uint32_t timeout_ms;
} example_run_t;

typedef struct example_worker_s {
  example_run_t *run;
  size_t id;
  /* Published once under run->mutex; main reads only after ready. */
  char endpoint[EXAMPLE_ENDPOINT_BYTES];
  size_t completed;
  const char *phase;
  int status;
} example_worker_t;

static void example_error(example_run_t *run, int status) {
  int expected = SALTS_OK;
  if (status != SALTS_OK) atomic_compare_exchange_strong(&run->error, &expected, status);
}

static int example_socket_options(flowmq_socket_t *socket) {
  const int hwm = EXAMPLE_HWM;
  const int reconnect = EXAMPLE_RECONNECT_DISABLED;
  int status = flowmq_setsockopt(socket, FLOWMQ_SNDHWM, &hwm, sizeof(hwm));
  if (status == SALTS_OK) status = flowmq_setsockopt(socket, FLOWMQ_RCVHWM, &hwm, sizeof(hwm));
  if (status == SALTS_OK)
    status = flowmq_setsockopt(socket, FLOWMQ_RECONNECT_IVL, &reconnect, sizeof(reconnect));
  return status;
}

static int example_progress(example_run_t *run, flowmq_socket_t *socket, short events,
                            uint64_t deadline) {
  flowmq_pollitem_t item = {.socket = socket, .events = events};
  size_t ready = 0u;
  int status = atomic_load(&run->error);
  if (status != SALTS_OK) return status;
  if (cmeta_monotonic_ms() >= deadline) return SALTS_ETIMEDOUT;
  status = flowmq_poll(&item, 1u, EXAMPLE_POLL_MS, &ready);
  if (status == SALTS_OK && (item.revents & FLOWMQ_POLLERR)) return SALTS_ENOTCONN;
  return status;
}

static int example_send(example_run_t *run, flowmq_socket_t *socket, const char *message,
                        size_t size) {
  const uint64_t deadline = cmeta_monotonic_ms() + run->timeout_ms;
  for (;;) {
    int status = atomic_load(&run->error);
    if (status != SALTS_OK) return status;
    status = flowmq_send(socket, message, size, FLOWMQ_DONTWAIT);
    if (status != SALTS_EBUSY && status != SALTS_ENOBUFS) return status;
    status = example_progress(run, socket, FLOWMQ_POLLOUT, deadline);
    if (status != SALTS_OK) return status;
  }
}

static int example_receive(example_run_t *run, flowmq_socket_t *socket, char *message,
                           size_t capacity, size_t *size) {
  const uint64_t deadline = cmeta_monotonic_ms() + run->timeout_ms;
  for (;;) {
    int status = atomic_load(&run->error);
    if (status != SALTS_OK) return status;
    status = flowmq_recv(socket, message, capacity, size, FLOWMQ_DONTWAIT);
    if (status != SALTS_EBUSY) {
      int more = 0;
      size_t option_size = sizeof(more);
      if (status != SALTS_OK) return status;
      status = flowmq_getsockopt(socket, FLOWMQ_RCVMORE, &more, &option_size);
      return status != SALTS_OK ? status : (more ? SALTS_EPROTO : SALTS_OK);
    }
    status = example_progress(run, socket, FLOWMQ_POLLIN, deadline);
    if (status != SALTS_OK) return status;
  }
}

static int example_message(char *message, size_t capacity, size_t worker, size_t sequence) {
  int size = snprintf(message, capacity, "worker=%zu sequence=%zu", worker, sequence);
  return size < 0 || (size_t)size >= capacity ? SALTS_EMSGSIZE : SALTS_OK;
}

static void example_worker_entry(void *arg) {
  example_worker_t *worker = arg;
  example_run_t *run = worker->run;
  flowmq_ctx_t *ctx = flowmq_ctx_new();
  flowmq_socket_t *socket = ctx != NULL ? flowmq_socket(ctx, FLOWMQ_REP) : NULL;
  size_t endpoint_size = 0u;
  int status = socket != NULL ? SALTS_OK : SALTS_ENOMEM;
  worker->phase = "bind";
  if (status == SALTS_OK) status = example_socket_options(socket);
  if (status == SALTS_OK) status = flowmq_bind(socket, "tcp://127.0.0.1:0");
  if (status == SALTS_OK)
    status =
        flowmq_last_endpoint(socket, worker->endpoint, sizeof(worker->endpoint), &endpoint_size);
  example_error(run, status);
  cmeta_mutex_lock(&run->mutex);
  ++run->ready;
  cmeta_cond_signal(&run->changed);
  cmeta_mutex_unlock(&run->mutex);

  for (size_t sequence = 0u; status == SALTS_OK && sequence < run->requests; ++sequence) {
    char expected[EXAMPLE_MESSAGE_BYTES];
    char message[EXAMPLE_MESSAGE_BYTES];
    size_t size = 0u;
    worker->phase = "request";
    status = example_message(expected, sizeof(expected), worker->id, sequence);
    if (status == SALTS_OK) status = example_receive(run, socket, message, sizeof(message), &size);
    if (status == SALTS_OK && (size != strlen(expected) || memcmp(message, expected, size) != 0))
      status = SALTS_EPROTO;
    if (status == SALTS_OK) {
      worker->phase = "reply";
      status = example_send(run, socket, message, size);
      if (status == SALTS_OK) ++worker->completed;
    }
  }
  /* Send admission does not establish delivery. Keep progressing the last
   * reply until the client has consumed every reply and explicitly stops us. */
  if (status == SALTS_OK) {
    const uint64_t deadline = cmeta_monotonic_ms() + run->timeout_ms;
    worker->phase = "drain";
    while (status == SALTS_OK && !atomic_load(&run->stop))
      status = example_progress(run, socket, 0, deadline);
  }
  worker->status = status;
  example_error(run, status);
  if (socket != NULL) example_error(run, flowmq_close(socket));
  if (ctx != NULL) example_error(run, flowmq_ctx_term(ctx));
  cmeta_mutex_lock(&run->mutex);
  ++run->exited;
  cmeta_cond_signal(&run->changed);
  cmeta_mutex_unlock(&run->mutex);
}

static int example_round(example_run_t *run, flowmq_pollitem_t *items, size_t count,
                         size_t sequence) {
  char expected[EXAMPLE_MAX_WORKERS][EXAMPLE_MESSAGE_BYTES];
  bool sent[EXAMPLE_MAX_WORKERS] = {false};
  bool received[EXAMPLE_MAX_WORKERS] = {false};
  const uint64_t deadline = cmeta_monotonic_ms() + run->timeout_ms;
  size_t completed = 0u;
  for (size_t i = 0u; i < count; ++i) {
    int status = example_message(expected[i], sizeof(expected[i]), i, sequence);
    if (status != SALTS_OK) return status;
    items[i].events = FLOWMQ_POLLOUT;
  }
  while (completed != count) {
    size_t ready = 0u;
    int status = atomic_load(&run->error);
    if (status != SALTS_OK) return status;
    if (cmeta_monotonic_ms() >= deadline) return SALTS_ETIMEDOUT;
    for (size_t i = 0u; i < count; ++i) {
      char message[EXAMPLE_MESSAGE_BYTES];
      size_t size = 0u;
      int more = 0;
      size_t option_size = sizeof(more);
      if (received[i]) continue;
      if (!sent[i]) {
        status = flowmq_send(items[i].socket, expected[i], strlen(expected[i]), FLOWMQ_DONTWAIT);
        if (status == SALTS_EBUSY || status == SALTS_ENOBUFS) continue;
        if (status != SALTS_OK) return status;
        sent[i] = true;
        items[i].events = FLOWMQ_POLLIN;
      }
      status = flowmq_recv(items[i].socket, message, sizeof(message), &size, FLOWMQ_DONTWAIT);
      if (status == SALTS_EBUSY) continue;
      if (status != SALTS_OK) return status;
      status = flowmq_getsockopt(items[i].socket, FLOWMQ_RCVMORE, &more, &option_size);
      if (status != SALTS_OK) return status;
      if (more || size != strlen(expected[i]) || memcmp(message, expected[i], size) != 0)
        return SALTS_EPROTO;
      received[i] = true;
      items[i].events = 0;
      ++completed;
    }
    if (completed == count) break;
    /* Progress every client: send admission alone does not flush its bytes. */
    status = flowmq_poll(items, count, EXAMPLE_POLL_MS, &ready);
    if (status != SALTS_OK) return status;
    for (size_t i = 0u; i < count; ++i)
      if (items[i].revents & FLOWMQ_POLLERR) return SALTS_ENOTCONN;
  }
  return SALTS_OK;
}

static int example_exchange(example_run_t *run, example_worker_t *workers, size_t count) {
  flowmq_ctx_t *ctx = flowmq_ctx_new();
  flowmq_pollitem_t clients[EXAMPLE_MAX_WORKERS] = {0};
  int status = ctx != NULL ? SALTS_OK : SALTS_ENOMEM;
  for (size_t i = 0u; status == SALTS_OK && i < count; ++i) {
    clients[i].socket = flowmq_socket(ctx, FLOWMQ_REQ);
    status = clients[i].socket != NULL ? example_socket_options(clients[i].socket) : SALTS_ENOMEM;
    if (status == SALTS_OK) status = flowmq_connect(clients[i].socket, workers[i].endpoint);
  }
  /* O(workers * requests) exchanges, O(workers) state. One request per worker
   * is in flight, so work executes concurrently without a shared message queue. */
  for (size_t sequence = 0u; status == SALTS_OK && sequence < run->requests; ++sequence)
    status = example_round(run, clients, count, sequence);
  example_error(run, status);
  atomic_store(&run->stop, true);
  /* Keep clients alive until workers finish progress, avoiding a peer-close
   * race with the successful final reply's transport completion. */
  cmeta_mutex_lock(&run->mutex);
  while (run->exited != count)
    cmeta_cond_wait(&run->changed, &run->mutex);
  cmeta_mutex_unlock(&run->mutex);
  for (size_t i = 0u; i < count; ++i)
    if (clients[i].socket != NULL) example_error(run, flowmq_close(clients[i].socket));
  if (ctx != NULL) example_error(run, flowmq_ctx_term(ctx));
  return atomic_load(&run->error);
}

static int example_count(const char *value, size_t maximum, size_t *out) {
  char *end = NULL;
  unsigned long parsed;
  if (value[0] < '0' || value[0] > '9') return SALTS_EINVAL;
  errno = 0;
  parsed = strtoul(value, &end, 10);
  if (errno != 0 || *end != '\0' || parsed == 0u || parsed > maximum) return SALTS_EINVAL;
  *out = (size_t)parsed;
  return SALTS_OK;
}

int main(int argc, char **argv) {
  example_run_t run = {0};
  example_worker_t workers[EXAMPLE_MAX_WORKERS] = {0};
  cmeta_thread_t threads[EXAMPLE_MAX_WORKERS] = {0};
  size_t count = EXAMPLE_DEFAULT_WORKERS;
  size_t requests = EXAMPLE_DEFAULT_REQUESTS;
  size_t timeout = EXAMPLE_DEFAULT_TIMEOUT_MS;
  size_t created = 0u;
  size_t completed = 0u;
  int status = SALTS_OK;
  if (argc > 4 || (argc > 1 && example_count(argv[1], EXAMPLE_MAX_WORKERS, &count) != SALTS_OK) ||
      (argc > 2 && example_count(argv[2], EXAMPLE_MAX_REQUESTS, &requests) != SALTS_OK) ||
      (argc > 3 && example_count(argv[3], EXAMPLE_MAX_TIMEOUT_MS, &timeout) != SALTS_OK)) {
    fprintf(stderr, "usage: %s [workers:1-%d] [requests-per-worker:1-%d] [timeout-ms:1-%d]\n",
            argv[0], EXAMPLE_MAX_WORKERS, EXAMPLE_MAX_REQUESTS, EXAMPLE_MAX_TIMEOUT_MS);
    return EXIT_FAILURE;
  }
  run.requests = requests;
  run.timeout_ms = (uint32_t)timeout;
  atomic_init(&run.error, SALTS_OK);
  atomic_init(&run.stop, false);
  cmeta_mutex_init(&run.mutex);
  cmeta_cond_init(&run.changed);
  for (size_t i = 0u; status == SALTS_OK && i < count; ++i) {
    workers[i].run = &run;
    workers[i].id = i;
    status = cmeta_thread_create(&threads[i], example_worker_entry, &workers[i]);
    if (status == SALTS_OK) ++created;
  }
  example_error(&run, status);
  cmeta_mutex_lock(&run.mutex);
  while (run.ready != created)
    cmeta_cond_wait(&run.changed, &run.mutex);
  cmeta_mutex_unlock(&run.mutex);
  if (atomic_load(&run.error) == SALTS_OK)
    example_error(&run, example_exchange(&run, workers, count));
  atomic_store(&run.stop, true);
  for (size_t i = 0u; i < created; ++i) {
    status = cmeta_thread_join(&threads[i]);
    if (status != SALTS_OK) {
      /* A failed join cannot establish that stack-owned state is quiescent. */
      fprintf(stderr, "worker=%zu join failed: %d\n", i, status);
      abort();
    }
    if (workers[i].status != SALTS_OK)
      fprintf(stderr, "worker=%zu phase=%s status=%d\n", i, workers[i].phase, workers[i].status);
    completed += workers[i].completed;
  }
  status = atomic_load(&run.error);
  if (status == SALTS_OK && completed != count * requests) status = SALTS_EPROTO;
  cmeta_cond_destroy(&run.changed);
  cmeta_mutex_destroy(&run.mutex);
  if (status != SALTS_OK) {
    fprintf(stderr, "multicore exchange failed: status=%d\n", status);
    return EXIT_FAILURE;
  }
  printf("workers=%zu replies=%zu verified; all owners closed\n", count, completed);
  return EXIT_SUCCESS;
}
