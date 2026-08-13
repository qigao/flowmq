#include "flowmq_posted_send.h"

#include "turbo_error.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct flowmq_posted_send_request_s {
  flowmq_posted_send_t *send;
  uint64_t completion_id;
  size_t route_size;
  size_t encoded_size;
  unsigned char data[];
} flowmq_posted_send_request_t;

static void flowmq_posted_send_release(flowmq_posted_send_request_t *request,
                                       int status) {
  flowmq_posted_send_t *send = request->send;
  if (send->config.on_complete)
    send->config.on_complete(send->config.completion_ctx,
                             request->completion_id, status);
  turbo_mutex_lock(&send->mutex);
  send->stats.pending--;
  send->stats.pending_bytes -= request->encoded_size;
  send->stats.completed++;
  if (status != TURBO_OK) send->stats.failed++;
  turbo_cond_broadcast(&send->changed);
  turbo_mutex_unlock(&send->mutex);
  free(request);
}

static void flowmq_posted_send_run(void *arg1, void *arg2) {
  flowmq_posted_send_request_t *request =
      (flowmq_posted_send_request_t *)arg1;
  const void *route = request->route_size ? request->data : NULL;
  const char *encoded =
      (const char *)(request->data + request->route_size);
  int status;
  (void)arg2;
  status = request->send->execute(
      request->send->owner, route, request->route_size, encoded,
      request->encoded_size);
  flowmq_posted_send_release(request, status);
}

int flowmq_posted_send_init(flowmq_posted_send_t *send,
                            coro_context_t *context, void *owner,
                            flowmq_posted_send_execute_fn execute,
                            const flowmq_send_admission_config_t *config) {
  if (!send || !context || !owner || !execute || !config ||
      config->capacity == 0u || config->capacity_bytes == 0u)
    return TURBO_EINVAL;
  memset(send, 0, sizeof(*send));
  send->context = context;
  send->owner = owner;
  send->execute = execute;
  send->config = *config;
  turbo_mutex_init(&send->mutex);
  turbo_cond_init(&send->changed);
  send->initialized = 1;
  return TURBO_OK;
}

void flowmq_posted_send_start(flowmq_posted_send_t *send) {
  if (!send || !send->initialized) return;
  turbo_mutex_lock(&send->mutex);
  if (send->stats.pending == 0u && send->posting == 0u)
    send->accepting = 1;
  turbo_mutex_unlock(&send->mutex);
}

void flowmq_posted_send_stop(flowmq_posted_send_t *send) {
  if (!send || !send->initialized) return;
  turbo_mutex_lock(&send->mutex);
  send->accepting = 0;
  while (send->posting != 0u || send->stats.pending != 0u)
    turbo_cond_wait(&send->changed, &send->mutex);
  turbo_mutex_unlock(&send->mutex);
}

void flowmq_posted_send_destroy(flowmq_posted_send_t *send) {
  if (!send || !send->initialized) return;
  flowmq_posted_send_stop(send);
  turbo_cond_destroy(&send->changed);
  turbo_mutex_destroy(&send->mutex);
  memset(send, 0, sizeof(*send));
}

int flowmq_posted_send_copy(flowmq_posted_send_t *send, const void *route,
                            size_t route_size, uint64_t completion_id,
                            const char *encoded, size_t encoded_size) {
  flowmq_posted_send_request_t *request;
  size_t allocation_size;
  int status;
  if (!send || !send->initialized || !encoded || encoded_size == 0u ||
      (!route && route_size != 0u))
    return TURBO_EINVAL;
  if (encoded_size > send->config.capacity_bytes ||
      route_size > SIZE_MAX - encoded_size ||
      sizeof(*request) > SIZE_MAX - route_size - encoded_size)
    return TURBO_EMSGSIZE;
  allocation_size = sizeof(*request) + route_size + encoded_size;
  request = (flowmq_posted_send_request_t *)malloc(allocation_size);
  if (!request) return TURBO_ENOMEM;
  request->send = send;
  request->completion_id = completion_id;
  request->route_size = route_size;
  request->encoded_size = encoded_size;
  if (route_size) memcpy(request->data, route, route_size);
  memcpy(request->data + route_size, encoded, encoded_size);

  turbo_mutex_lock(&send->mutex);
  if (!send->accepting) {
    turbo_mutex_unlock(&send->mutex);
    free(request);
    return TURBO_ESHUTDOWN;
  }
  if (send->stats.pending >= send->config.capacity ||
      send->stats.pending_bytes >
          send->config.capacity_bytes - encoded_size) {
    send->stats.rejected_full++;
    turbo_mutex_unlock(&send->mutex);
    free(request);
    return TURBO_ENOSPC;
  }
  send->stats.pending++;
  send->stats.pending_bytes += encoded_size;
  if (send->stats.pending > send->stats.high_water)
    send->stats.high_water = send->stats.pending;
  if (send->stats.pending_bytes > send->stats.high_water_bytes)
    send->stats.high_water_bytes = send->stats.pending_bytes;
  send->posting++;
  turbo_mutex_unlock(&send->mutex);

  status = coro_post(send->context, flowmq_posted_send_run, request, NULL);

  turbo_mutex_lock(&send->mutex);
  send->posting--;
  if (status != TURBO_OK) {
    send->stats.pending--;
    send->stats.pending_bytes -= encoded_size;
  }
  turbo_cond_broadcast(&send->changed);
  turbo_mutex_unlock(&send->mutex);
  if (status != TURBO_OK) free(request);
  return status;
}

void flowmq_posted_send_get_stats(flowmq_posted_send_t *send,
                                  flowmq_send_admission_stats_t *stats) {
  if (!stats) return;
  memset(stats, 0, sizeof(*stats));
  if (!send || !send->initialized) return;
  turbo_mutex_lock(&send->mutex);
  *stats = send->stats;
  turbo_mutex_unlock(&send->mutex);
}
