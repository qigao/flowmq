#include "turbo_flow_fmq_broker.h"

#include "turbo_error.h"
#include "turbo_hash.h"
#include "turbo_parser.h"
#include "turbo_str.h"
#include "turbo_str_view.h"
#include "turbo_vec.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef enum flow_fmq_broker_worker_state_e {
  FLOW_FMQ_BROKER_WORKER_IDLE = 1,
  FLOW_FMQ_BROKER_WORKER_BUSY
} flow_fmq_broker_worker_state_t;

typedef struct flow_fmq_broker_worker_s {
  tstr_t worker_id;
  tstr_t service;
  turbo_flow_protocol_route_t route;
  flow_fmq_broker_worker_state_t state;
  uint64_t available_order;
  uint64_t last_seen_ms;
} flow_fmq_broker_worker_t;

typedef struct flow_fmq_broker_inflight_s {
  uint64_t request_id;
  size_t worker_index;
  char service[TURBO_FLOW_FMQ_BROKER_SERVICE_MAX + 1u];
  turbo_flow_protocol_route_t client_route;
} flow_fmq_broker_inflight_t;

typedef struct flow_fmq_broker_accepted_s {
  uint64_t request_id;
  char service[TURBO_FLOW_FMQ_BROKER_SERVICE_MAX + 1u];
  turbo_flow_protocol_route_t client_route;
} flow_fmq_broker_accepted_t;

typedef struct flow_fmq_broker_worker_key_s {
  char value[TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX + 1u];
} flow_fmq_broker_worker_key_t;

TURBO_VEC_DEFINE(flow_fmq_broker_workers, flow_fmq_broker_worker_t)
TURBO_VEC_DEFINE(flow_fmq_broker_inflight, flow_fmq_broker_inflight_t)
TURBO_VEC_DEFINE(flow_fmq_broker_accepted, flow_fmq_broker_accepted_t)
TURBO_HASH_MAP_DEFINE(flow_fmq_broker_worker_index, flow_fmq_broker_worker_key_t, size_t)
TURBO_HASH_MAP_DEFINE(flow_fmq_broker_request_index, uint64_t, size_t)
TURBO_HASH_MAP_DEFINE(flow_fmq_broker_accepted_index, uint64_t, size_t)

struct turbo_flow_fmq_broker_s {
  flow_fmq_broker_workers workers;
  flow_fmq_broker_inflight inflight;
  flow_fmq_broker_accepted accepted;
  flow_fmq_broker_worker_index worker_index;
  flow_fmq_broker_request_index request_index;
  flow_fmq_broker_accepted_index accepted_index;
  size_t max_workers;
  size_t max_inflight;
  turbo_flow_fmq_broker_scheduler_t scheduler;
  turbo_flow_fmq_broker_reliability_t reliability;
  uint64_t worker_lease_ms;
  uint64_t order;
  uint64_t dispatched;
  uint64_t completed;
  uint64_t canceled;
  uint64_t expired_workers;
  uint64_t expired_drops;
  uint64_t expired_requeues;
  uint64_t accept_acks;
  uint64_t worker_completion_acks;
};

static int flow_fmq_broker_config_valid(const turbo_flow_fmq_broker_config_t *config) {
  int reliability_valid;
  if (!config) return 0;
  reliability_valid = (config->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE &&
                       config->worker_lease_ms == 0u) ||
                      ((config->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE ||
                        config->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE) &&
                       config->worker_lease_ms > 0u);
  return config->size >= sizeof(*config) && config->version == TURBO_FLOW_FMQ_BROKER_API_VERSION &&
         config->max_workers > 0u && config->max_workers <= TURBO_FLOW_FMQ_BROKER_MAX_WORKERS &&
         config->max_inflight > 0u && config->max_inflight <= TURBO_FLOW_FMQ_BROKER_MAX_INFLIGHT &&
         config->scheduler == TURBO_FLOW_FMQ_BROKER_SCHEDULER_LRU && reliability_valid;
}

static int flow_fmq_broker_name_valid(const char *value, size_t maximum) {
  size_t length;
  if (!value || !value[0]) return 0;
  length = strlen(value);
  return length <= maximum;
}

static int flow_fmq_broker_route_valid(const turbo_flow_protocol_route_t *route) {
  return route && route->size >= sizeof(*route) &&
         route->contract_version == TURBO_FLOW_PROTOCOL_CONTRACT_VERSION &&
         route->protocol == TURBO_FLOW_PROTOCOL_FMQ && route->reserved == 0u &&
         route->owner_instance_id != 0u && route->session_id != 0u &&
         route->session_generation != 0u;
}

static turbo_flow_protocol_route_t
flow_fmq_broker_route_copy(const turbo_flow_protocol_route_t *route) {
  turbo_flow_protocol_route_t copy = *route;
  copy.size = sizeof(copy);
  return copy;
}

static int flow_fmq_broker_route_equal(const turbo_flow_protocol_route_t *left,
                                       const turbo_flow_protocol_route_t *right) {
  return left && right && left->contract_version == right->contract_version &&
         left->protocol == right->protocol && left->reserved == right->reserved &&
         left->owner_instance_id == right->owner_instance_id &&
         left->session_id == right->session_id &&
         left->session_generation == right->session_generation;
}

static flow_fmq_broker_worker_key_t flow_fmq_broker_worker_key(const char *worker_id) {
  flow_fmq_broker_worker_key_t key;
  memset(&key, 0, sizeof(key));
  if (worker_id) memcpy(key.value, worker_id, strlen(worker_id));
  return key;
}

static int flow_fmq_broker_worker_find(const turbo_flow_fmq_broker_t *broker,
                                       const char *worker_id) {
  flow_fmq_broker_worker_key_t key = flow_fmq_broker_worker_key(worker_id);
  const size_t *index = flow_fmq_broker_worker_index_get_const(&broker->worker_index, key);
  return index ? (int)*index : -1;
}

static int flow_fmq_broker_inflight_find(const turbo_flow_fmq_broker_t *broker,
                                         uint64_t request_id) {
  const size_t *index = flow_fmq_broker_request_index_get_const(&broker->request_index, request_id);
  return index ? (int)*index : -1;
}

static int flow_fmq_broker_accepted_find(const turbo_flow_fmq_broker_t *broker,
                                         uint64_t request_id) {
  const size_t *index =
      flow_fmq_broker_accepted_index_get_const(&broker->accepted_index, request_id);
  return index ? (int)*index : -1;
}

static size_t flow_fmq_broker_request_count(const turbo_flow_fmq_broker_t *broker) {
  return flow_fmq_broker_accepted_size(&broker->accepted) +
         flow_fmq_broker_inflight_size(&broker->inflight);
}

static int flow_fmq_broker_worker_live(const turbo_flow_fmq_broker_t *broker,
                                       const flow_fmq_broker_worker_t *worker, uint64_t now_ms) {
  if (broker->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE) return 1;
  return now_ms >= worker->last_seen_ms && now_ms - worker->last_seen_ms < broker->worker_lease_ms;
}

static int flow_fmq_broker_worker_expired(const turbo_flow_fmq_broker_t *broker,
                                          const flow_fmq_broker_worker_t *worker, uint64_t now_ms) {
  return now_ms >= worker->last_seen_ms && now_ms - worker->last_seen_ms >= broker->worker_lease_ms;
}

static int flow_fmq_broker_idle_worker(const turbo_flow_fmq_broker_t *broker, const char *service,
                                       uint64_t now_ms) {
  size_t selected = flow_fmq_broker_workers_size(&broker->workers);
  uint64_t oldest = UINT64_MAX;
  for (size_t i = 0; i < flow_fmq_broker_workers_size(&broker->workers); ++i) {
    const flow_fmq_broker_worker_t *worker = flow_fmq_broker_workers_at_const(&broker->workers, i);
    if (!worker || worker->state != FLOW_FMQ_BROKER_WORKER_IDLE ||
        !flow_fmq_broker_worker_live(broker, worker, now_ms) ||
        strcmp(worker->service, service) != 0) {
      continue;
    }
    if (selected == flow_fmq_broker_workers_size(&broker->workers) ||
        worker->available_order < oldest) {
      selected = i;
      oldest = worker->available_order;
    }
  }
  return selected == flow_fmq_broker_workers_size(&broker->workers) ? -1 : (int)selected;
}

static void flow_fmq_broker_worker_cleanup(flow_fmq_broker_worker_t *worker) {
  if (!worker) return;
  tstr_freep(&worker->worker_id);
  tstr_freep(&worker->service);
  memset(worker, 0, sizeof(*worker));
}

static void flow_fmq_broker_completion_fill(turbo_flow_fmq_broker_completion_result_t *result,
                                            uint64_t request_id,
                                            const flow_fmq_broker_worker_t *worker,
                                            const turbo_flow_protocol_route_t *client_route) {
  memset(result, 0, sizeof(*result));
  result->size = sizeof(*result);
  result->request_id = request_id;
  memcpy(result->worker_id, worker->worker_id, tstr_len(worker->worker_id));
  result->client_route = *client_route;
  result->client_route.size = sizeof(result->client_route);
}

static int flow_fmq_broker_inflight_remove(turbo_flow_fmq_broker_t *broker, size_t index) {
  size_t count = flow_fmq_broker_inflight_size(&broker->inflight);
  flow_fmq_broker_inflight_t *slot = flow_fmq_broker_inflight_at(&broker->inflight, index);
  uint64_t removed_request_id;
  if (!slot || index >= count) return TURBO_EINVAL;
  removed_request_id = slot->request_id;
  if (index + 1u < count) {
    const flow_fmq_broker_inflight_t *last =
        flow_fmq_broker_inflight_at_const(&broker->inflight, count - 1u);
    size_t old_index = count - 1u;
    if (!last || flow_fmq_broker_request_index_put(&broker->request_index, last->request_id,
                                                   index) != TURBO_OK) {
      return TURBO_EPROTO;
    }
    if (!flow_fmq_broker_request_index_remove(&broker->request_index, removed_request_id, NULL)) {
      (void)flow_fmq_broker_request_index_put(&broker->request_index, last->request_id, old_index);
      return TURBO_EPROTO;
    }
    *slot = *last;
  } else if (!flow_fmq_broker_request_index_remove(&broker->request_index, removed_request_id,
                                                   NULL)) {
    return TURBO_EPROTO;
  }
  if (turbo_vec_resize(&broker->inflight.raw, count - 1u) != TURBO_OK) return TURBO_EPROTO;
  return TURBO_OK;
}

static int flow_fmq_broker_accepted_remove(turbo_flow_fmq_broker_t *broker, size_t index) {
  size_t count = flow_fmq_broker_accepted_size(&broker->accepted);
  flow_fmq_broker_accepted_t *slot = flow_fmq_broker_accepted_at(&broker->accepted, index);
  uint64_t removed_request_id;
  if (!slot || index >= count) return TURBO_EINVAL;
  removed_request_id = slot->request_id;
  if (index + 1u < count) {
    const flow_fmq_broker_accepted_t *last =
        flow_fmq_broker_accepted_at_const(&broker->accepted, count - 1u);
    size_t old_index = count - 1u;
    if (!last || flow_fmq_broker_accepted_index_put(&broker->accepted_index, last->request_id,
                                                    index) != TURBO_OK) {
      return TURBO_EPROTO;
    }
    if (!flow_fmq_broker_accepted_index_remove(&broker->accepted_index, removed_request_id, NULL)) {
      (void)flow_fmq_broker_accepted_index_put(&broker->accepted_index, last->request_id,
                                               old_index);
      return TURBO_EPROTO;
    }
    *slot = *last;
  } else if (!flow_fmq_broker_accepted_index_remove(&broker->accepted_index, removed_request_id,
                                                    NULL)) {
    return TURBO_EPROTO;
  }
  if (turbo_vec_resize(&broker->accepted.raw, count - 1u) != TURBO_OK) return TURBO_EPROTO;
  return TURBO_OK;
}

static int flow_fmq_broker_dispatch_store(turbo_flow_fmq_broker_t *broker, size_t worker_index,
                                          uint64_t request_id,
                                          const turbo_flow_protocol_route_t *client_route,
                                          turbo_flow_fmq_broker_dispatch_result_t *result) {
  flow_fmq_broker_inflight_t entry;
  flow_fmq_broker_worker_t *worker = flow_fmq_broker_workers_at(&broker->workers, worker_index);
  size_t stored_index;
  if (!worker) return TURBO_ENOTCONN;
  memset(&entry, 0, sizeof(entry));
  entry.request_id = request_id;
  entry.worker_index = worker_index;
  memcpy(entry.service, worker->service, tstr_len(worker->service));
  entry.client_route = flow_fmq_broker_route_copy(client_route);
  if (flow_fmq_broker_inflight_push(&broker->inflight, entry) != TURBO_OK) return TURBO_ENOMEM;
  stored_index = flow_fmq_broker_inflight_size(&broker->inflight) - 1u;
  if (flow_fmq_broker_request_index_put(&broker->request_index, request_id, stored_index) !=
      TURBO_OK) {
    (void)turbo_vec_resize(&broker->inflight.raw, stored_index);
    return TURBO_ENOMEM;
  }
  worker->state = FLOW_FMQ_BROKER_WORKER_BUSY;
  broker->dispatched += 1u;
  result->request_id = request_id;
  memcpy(result->worker_id, worker->worker_id, tstr_len(worker->worker_id));
  memcpy(result->service, worker->service, tstr_len(worker->service));
  result->worker_route = worker->route;
  result->worker_route.size = sizeof(result->worker_route);
  return TURBO_OK;
}

turbo_flow_fmq_broker_t *
turbo_flow_fmq_broker_create(const turbo_flow_fmq_broker_config_t *config) {
  turbo_flow_fmq_broker_t *broker;
  if (!flow_fmq_broker_config_valid(config)) return NULL;
  broker = (turbo_flow_fmq_broker_t *)calloc(1, sizeof(*broker));
  if (!broker) return NULL;
  if (flow_fmq_broker_workers_init(&broker->workers) != TURBO_OK ||
      flow_fmq_broker_inflight_init(&broker->inflight) != TURBO_OK ||
      flow_fmq_broker_accepted_init(&broker->accepted) != TURBO_OK ||
      flow_fmq_broker_worker_index_init(&broker->worker_index) != TURBO_OK ||
      flow_fmq_broker_request_index_init(&broker->request_index) != TURBO_OK ||
      flow_fmq_broker_accepted_index_init(&broker->accepted_index) != TURBO_OK ||
      flow_fmq_broker_workers_reserve(&broker->workers, config->max_workers) != TURBO_OK ||
      flow_fmq_broker_inflight_reserve(&broker->inflight, config->max_inflight) != TURBO_OK ||
      flow_fmq_broker_accepted_reserve(&broker->accepted, config->max_inflight) != TURBO_OK ||
      turbo_hash_map_reserve(&broker->worker_index.raw, config->max_workers) != TURBO_OK ||
      turbo_hash_map_reserve(&broker->request_index.raw, config->max_inflight) != TURBO_OK ||
      turbo_hash_map_reserve(&broker->accepted_index.raw, config->max_inflight) != TURBO_OK) {
    turbo_flow_fmq_broker_destroy(broker);
    return NULL;
  }
  broker->max_workers = config->max_workers;
  broker->max_inflight = config->max_inflight;
  broker->scheduler = config->scheduler;
  broker->reliability = config->reliability;
  broker->worker_lease_ms = config->worker_lease_ms;
  return broker;
}

void turbo_flow_fmq_broker_destroy(turbo_flow_fmq_broker_t *broker) {
  if (!broker) return;
  for (size_t i = 0; i < flow_fmq_broker_workers_size(&broker->workers); ++i) {
    flow_fmq_broker_worker_cleanup(flow_fmq_broker_workers_at(&broker->workers, i));
  }
  flow_fmq_broker_workers_destroy(&broker->workers);
  flow_fmq_broker_inflight_destroy(&broker->inflight);
  flow_fmq_broker_accepted_destroy(&broker->accepted);
  flow_fmq_broker_worker_index_destroy(&broker->worker_index);
  flow_fmq_broker_request_index_destroy(&broker->request_index);
  flow_fmq_broker_accepted_index_destroy(&broker->accepted_index);
  free(broker);
}

static int flow_fmq_broker_worker_ready_at(turbo_flow_fmq_broker_t *broker, const char *worker_id,
                                           const char *service,
                                           const turbo_flow_protocol_route_t *worker_route,
                                           uint64_t now_ms) {
  flow_fmq_broker_worker_t worker;
  int index;
  if (!broker || !flow_fmq_broker_name_valid(worker_id, TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX) ||
      !flow_fmq_broker_name_valid(service, TURBO_FLOW_FMQ_BROKER_SERVICE_MAX) ||
      !flow_fmq_broker_route_valid(worker_route)) {
    return TURBO_EINVAL;
  }
  index = flow_fmq_broker_worker_find(broker, worker_id);
  if (index >= 0) {
    flow_fmq_broker_worker_t *current = flow_fmq_broker_workers_at(&broker->workers, (size_t)index);
    if (!current || strcmp(current->service, service) != 0) return TURBO_EPROTO;
    if (current->state != FLOW_FMQ_BROKER_WORKER_IDLE) return TURBO_EBUSY;
    if (broker->reliability != TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE &&
        now_ms < current->last_seen_ms) {
      return TURBO_EALREADY;
    }
    if (!flow_fmq_broker_route_equal(&current->route, worker_route))
      current->route = flow_fmq_broker_route_copy(worker_route);
    current->last_seen_ms = now_ms;
    return TURBO_OK;
  }
  if (flow_fmq_broker_workers_size(&broker->workers) >= broker->max_workers) return TURBO_ENOSPC;
  memset(&worker, 0, sizeof(worker));
  worker.worker_id = tstr_dup(worker_id);
  worker.service = tstr_dup(service);
  if (!worker.worker_id || !worker.service) {
    flow_fmq_broker_worker_cleanup(&worker);
    return TURBO_ENOMEM;
  }
  worker.route = flow_fmq_broker_route_copy(worker_route);
  worker.state = FLOW_FMQ_BROKER_WORKER_IDLE;
  worker.available_order = ++broker->order;
  worker.last_seen_ms = now_ms;
  if (flow_fmq_broker_workers_push(&broker->workers, worker) != TURBO_OK) {
    flow_fmq_broker_worker_cleanup(&worker);
    return TURBO_ENOMEM;
  }
  {
    size_t stored_index = flow_fmq_broker_workers_size(&broker->workers) - 1u;
    flow_fmq_broker_worker_key_t key = flow_fmq_broker_worker_key(worker_id);
    if (flow_fmq_broker_worker_index_put(&broker->worker_index, key, stored_index) != TURBO_OK) {
      flow_fmq_broker_worker_t *stored = flow_fmq_broker_workers_at(&broker->workers, stored_index);
      flow_fmq_broker_worker_cleanup(stored);
      (void)turbo_vec_resize(&broker->workers.raw, stored_index);
      return TURBO_ENOMEM;
    }
  }
  return TURBO_OK;
}

int turbo_flow_fmq_broker_worker_ready(turbo_flow_fmq_broker_t *broker, const char *worker_id,
                                       const char *service,
                                       const turbo_flow_protocol_route_t *worker_route) {
  if (!broker) return TURBO_EINVAL;
  if (broker->reliability != TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE) return TURBO_ENOTSUP;
  return flow_fmq_broker_worker_ready_at(broker, worker_id, service, worker_route, 0u);
}

int turbo_flow_fmq_broker_worker_ready_at(turbo_flow_fmq_broker_t *broker, const char *worker_id,
                                          const char *service,
                                          const turbo_flow_protocol_route_t *worker_route,
                                          uint64_t now_ms) {
  if (!broker) return TURBO_EINVAL;
  if (broker->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE) return TURBO_ENOTSUP;
  return flow_fmq_broker_worker_ready_at(broker, worker_id, service, worker_route, now_ms);
}

int turbo_flow_fmq_broker_worker_heartbeat(turbo_flow_fmq_broker_t *broker, const char *worker_id,
                                           const turbo_flow_protocol_route_t *worker_route,
                                           uint64_t now_ms) {
  flow_fmq_broker_worker_t *worker;
  int found;
  if (!broker || !flow_fmq_broker_name_valid(worker_id, TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX) ||
      !flow_fmq_broker_route_valid(worker_route)) {
    return TURBO_EINVAL;
  }
  if (broker->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE) return TURBO_ENOTSUP;
  found = flow_fmq_broker_worker_find(broker, worker_id);
  if (found < 0) return TURBO_ENOENT;
  worker = flow_fmq_broker_workers_at(&broker->workers, (size_t)found);
  if (!worker) return TURBO_EPROTO;
  if (now_ms < worker->last_seen_ms) return TURBO_EALREADY;
  if (!flow_fmq_broker_route_equal(&worker->route, worker_route)) {
    if (worker->state == FLOW_FMQ_BROKER_WORKER_BUSY) return TURBO_EBUSY;
    worker->route = flow_fmq_broker_route_copy(worker_route);
  }
  worker->last_seen_ms = now_ms;
  return TURBO_OK;
}

int turbo_flow_fmq_broker_worker_remove(turbo_flow_fmq_broker_t *broker, const char *worker_id) {
  size_t count;
  size_t index;
  flow_fmq_broker_worker_t *worker;
  if (!broker || !flow_fmq_broker_name_valid(worker_id, TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX))
    return TURBO_EINVAL;
  {
    int found = flow_fmq_broker_worker_find(broker, worker_id);
    if (found < 0) return TURBO_ENOENT;
    index = (size_t)found;
  }
  worker = flow_fmq_broker_workers_at(&broker->workers, index);
  if (!worker) return TURBO_ENOENT;
  if (worker->state != FLOW_FMQ_BROKER_WORKER_IDLE) return TURBO_EBUSY;
  count = flow_fmq_broker_workers_size(&broker->workers);
  if (index + 1u < count) {
    const flow_fmq_broker_worker_t *last =
        flow_fmq_broker_workers_at_const(&broker->workers, count - 1u);
    flow_fmq_broker_worker_key_t moved_key;
    flow_fmq_broker_worker_key_t removed_key = flow_fmq_broker_worker_key(worker_id);
    size_t old_index = count - 1u;
    if (!last) return TURBO_EPROTO;
    moved_key = flow_fmq_broker_worker_key(last->worker_id);
    if (flow_fmq_broker_worker_index_put(&broker->worker_index, moved_key, index) != TURBO_OK)
      return TURBO_EPROTO;
    if (!flow_fmq_broker_worker_index_remove(&broker->worker_index, removed_key, NULL)) {
      (void)flow_fmq_broker_worker_index_put(&broker->worker_index, moved_key, old_index);
      return TURBO_EPROTO;
    }
    flow_fmq_broker_worker_cleanup(worker);
    *worker = *last;
    for (size_t i = 0; i < flow_fmq_broker_inflight_size(&broker->inflight); ++i) {
      flow_fmq_broker_inflight_t *entry = flow_fmq_broker_inflight_at(&broker->inflight, i);
      if (entry && entry->worker_index == count - 1u) entry->worker_index = index;
    }
  } else {
    flow_fmq_broker_worker_key_t removed_key = flow_fmq_broker_worker_key(worker_id);
    if (!flow_fmq_broker_worker_index_remove(&broker->worker_index, removed_key, NULL))
      return TURBO_EPROTO;
    flow_fmq_broker_worker_cleanup(worker);
  }
  if (turbo_vec_resize(&broker->workers.raw, count - 1u) != TURBO_OK) return TURBO_EPROTO;
  return TURBO_OK;
}

static int flow_fmq_broker_dispatch_at(turbo_flow_fmq_broker_t *broker, const char *service,
                                       uint64_t request_id,
                                       const turbo_flow_protocol_route_t *client_route,
                                       uint64_t now_ms,
                                       turbo_flow_fmq_broker_dispatch_result_t *result) {
  int worker_index;
  if (!broker || !flow_fmq_broker_name_valid(service, TURBO_FLOW_FMQ_BROKER_SERVICE_MAX) ||
      request_id == 0u || !flow_fmq_broker_route_valid(client_route) || !result ||
      result->size < sizeof(*result)) {
    return TURBO_EINVAL;
  }
  memset(result, 0, sizeof(*result));
  result->size = sizeof(*result);
  if (flow_fmq_broker_inflight_find(broker, request_id) >= 0 ||
      flow_fmq_broker_accepted_find(broker, request_id) >= 0) {
    return TURBO_EALREADY;
  }
  if (flow_fmq_broker_request_count(broker) >= broker->max_inflight) return TURBO_ENOSPC;
  worker_index = flow_fmq_broker_idle_worker(broker, service, now_ms);
  if (worker_index < 0) return TURBO_ENOTCONN;
  return flow_fmq_broker_dispatch_store(broker, (size_t)worker_index, request_id, client_route,
                                        result);
}

int turbo_flow_fmq_broker_dispatch(turbo_flow_fmq_broker_t *broker, const char *service,
                                   uint64_t request_id,
                                   const turbo_flow_protocol_route_t *client_route,
                                   turbo_flow_fmq_broker_dispatch_result_t *result) {
  if (!broker) return TURBO_EINVAL;
  if (broker->reliability != TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE) return TURBO_ENOTSUP;
  return flow_fmq_broker_dispatch_at(broker, service, request_id, client_route, 0u, result);
}

int turbo_flow_fmq_broker_dispatch_at(turbo_flow_fmq_broker_t *broker, const char *service,
                                      uint64_t request_id,
                                      const turbo_flow_protocol_route_t *client_route,
                                      uint64_t now_ms,
                                      turbo_flow_fmq_broker_dispatch_result_t *result) {
  if (!broker) return TURBO_EINVAL;
  if (broker->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE) return TURBO_ENOTSUP;
  return flow_fmq_broker_dispatch_at(broker, service, request_id, client_route, now_ms, result);
}

int turbo_flow_fmq_broker_record_accept_commit(turbo_flow_fmq_broker_t *broker, const char *service,
                                               uint64_t request_id,
                                               const turbo_flow_protocol_route_t *client_route,
                                               turbo_flow_fmq_broker_ack_result_t *ack) {
  flow_fmq_broker_accepted_t entry;
  int found;
  if (!broker || !flow_fmq_broker_name_valid(service, TURBO_FLOW_FMQ_BROKER_SERVICE_MAX) ||
      request_id == 0u || !flow_fmq_broker_route_valid(client_route) || !ack ||
      ack->size < sizeof(*ack)) {
    return TURBO_EINVAL;
  }
  memset(ack, 0, sizeof(*ack));
  ack->size = sizeof(*ack);
  if (broker->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE) return TURBO_ENOTSUP;
  found = flow_fmq_broker_accepted_find(broker, request_id);
  if (found >= 0) {
    const flow_fmq_broker_accepted_t *current =
        flow_fmq_broker_accepted_at_const(&broker->accepted, (size_t)found);
    if (!current || strcmp(current->service, service) != 0 ||
        !flow_fmq_broker_route_equal(&current->client_route, client_route)) {
      return TURBO_EPROTO;
    }
    ack->kind = TURBO_FLOW_FMQ_BROKER_ACK_ACCEPT;
    ack->request_id = request_id;
    ack->client_route = current->client_route;
    ack->client_route.size = sizeof(ack->client_route);
    return TURBO_OK;
  }
  found = flow_fmq_broker_inflight_find(broker, request_id);
  if (found >= 0) {
    const flow_fmq_broker_inflight_t *current =
        flow_fmq_broker_inflight_at_const(&broker->inflight, (size_t)found);
    if (!current || strcmp(current->service, service) != 0 ||
        !flow_fmq_broker_route_equal(&current->client_route, client_route)) {
      return TURBO_EPROTO;
    }
    ack->kind = TURBO_FLOW_FMQ_BROKER_ACK_ACCEPT;
    ack->request_id = request_id;
    ack->client_route = current->client_route;
    ack->client_route.size = sizeof(ack->client_route);
    return TURBO_OK;
  }
  if (flow_fmq_broker_request_count(broker) >= broker->max_inflight) return TURBO_ENOSPC;
  memset(&entry, 0, sizeof(entry));
  entry.request_id = request_id;
  memcpy(entry.service, service, strlen(service));
  entry.client_route = flow_fmq_broker_route_copy(client_route);
  if (flow_fmq_broker_accepted_push(&broker->accepted, entry) != TURBO_OK) return TURBO_ENOMEM;
  {
    size_t stored_index = flow_fmq_broker_accepted_size(&broker->accepted) - 1u;
    if (flow_fmq_broker_accepted_index_put(&broker->accepted_index, request_id, stored_index) !=
        TURBO_OK) {
      (void)turbo_vec_resize(&broker->accepted.raw, stored_index);
      return TURBO_ENOMEM;
    }
  }
  broker->accept_acks += 1u;
  ack->kind = TURBO_FLOW_FMQ_BROKER_ACK_ACCEPT;
  ack->request_id = request_id;
  ack->client_route = entry.client_route;
  ack->client_route.size = sizeof(ack->client_route);
  return TURBO_OK;
}

int turbo_flow_fmq_broker_dispatch_accepted(turbo_flow_fmq_broker_t *broker, uint64_t request_id,
                                            uint64_t now_ms,
                                            turbo_flow_fmq_broker_dispatch_result_t *result) {
  const flow_fmq_broker_accepted_t *accepted;
  turbo_flow_protocol_route_t client_route;
  char service[TURBO_FLOW_FMQ_BROKER_SERVICE_MAX + 1u];
  flow_fmq_broker_worker_t *worker;
  int accepted_index;
  int worker_index;
  int rc;
  if (!broker || request_id == 0u || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  memset(result, 0, sizeof(*result));
  result->size = sizeof(*result);
  if (broker->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE) return TURBO_ENOTSUP;
  accepted_index = flow_fmq_broker_accepted_find(broker, request_id);
  if (accepted_index < 0) return TURBO_ENOENT;
  accepted = flow_fmq_broker_accepted_at_const(&broker->accepted, (size_t)accepted_index);
  if (!accepted) return TURBO_EPROTO;
  memcpy(service, accepted->service, sizeof(service));
  client_route = accepted->client_route;
  worker_index = flow_fmq_broker_idle_worker(broker, service, now_ms);
  if (worker_index < 0) return TURBO_ENOTCONN;
  rc = flow_fmq_broker_dispatch_store(broker, (size_t)worker_index, request_id, &client_route,
                                      result);
  if (rc != TURBO_OK) return rc;
  if (flow_fmq_broker_accepted_remove(broker, (size_t)accepted_index) != TURBO_OK) {
    int inflight_index = flow_fmq_broker_inflight_find(broker, request_id);
    worker = flow_fmq_broker_workers_at(&broker->workers, (size_t)worker_index);
    if (inflight_index >= 0) (void)flow_fmq_broker_inflight_remove(broker, (size_t)inflight_index);
    if (worker) worker->state = FLOW_FMQ_BROKER_WORKER_IDLE;
    if (broker->dispatched > 0u) broker->dispatched -= 1u;
    memset(result, 0, sizeof(*result));
    result->size = sizeof(*result);
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

int turbo_flow_fmq_broker_cancel(turbo_flow_fmq_broker_t *broker, uint64_t request_id,
                                 turbo_flow_fmq_broker_completion_result_t *result) {
  flow_fmq_broker_inflight_t *entry;
  flow_fmq_broker_worker_t *worker;
  int found;
  if (!broker || request_id == 0u || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  found = flow_fmq_broker_inflight_find(broker, request_id);
  if (found < 0) return TURBO_ENOENT;
  entry = flow_fmq_broker_inflight_at(&broker->inflight, (size_t)found);
  worker = entry ? flow_fmq_broker_workers_at(&broker->workers, entry->worker_index) : NULL;
  if (!entry || !worker || worker->state != FLOW_FMQ_BROKER_WORKER_BUSY) return TURBO_EPROTO;
  flow_fmq_broker_completion_fill(result, request_id, worker, &entry->client_route);
  if (flow_fmq_broker_inflight_remove(broker, (size_t)found) != TURBO_OK) return TURBO_EPROTO;
  worker->state = FLOW_FMQ_BROKER_WORKER_IDLE;
  worker->available_order = ++broker->order;
  broker->canceled += 1u;
  return TURBO_OK;
}

int turbo_flow_fmq_broker_complete(turbo_flow_fmq_broker_t *broker, const char *worker_id,
                                   uint64_t request_id,
                                   turbo_flow_fmq_broker_completion_result_t *result) {
  flow_fmq_broker_inflight_t *entry;
  flow_fmq_broker_worker_t *worker;
  int found;
  if (!broker || !flow_fmq_broker_name_valid(worker_id, TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX) ||
      request_id == 0u || !result || result->size < sizeof(*result)) {
    return TURBO_EINVAL;
  }
  found = flow_fmq_broker_inflight_find(broker, request_id);
  if (found < 0) return TURBO_ENOENT;
  entry = flow_fmq_broker_inflight_at(&broker->inflight, (size_t)found);
  worker = entry ? flow_fmq_broker_workers_at(&broker->workers, entry->worker_index) : NULL;
  if (!entry || !worker || worker->state != FLOW_FMQ_BROKER_WORKER_BUSY ||
      strcmp(worker->worker_id, worker_id) != 0) {
    return TURBO_EPROTO;
  }
  flow_fmq_broker_completion_fill(result, request_id, worker, &entry->client_route);
  if (flow_fmq_broker_inflight_remove(broker, (size_t)found) != TURBO_OK) return TURBO_EPROTO;
  worker->state = FLOW_FMQ_BROKER_WORKER_IDLE;
  worker->available_order = ++broker->order;
  broker->completed += 1u;
  broker->worker_completion_acks += 1u;
  return TURBO_OK;
}

int turbo_flow_fmq_broker_completion_ack(
    const turbo_flow_fmq_broker_completion_result_t *completion,
    turbo_flow_fmq_broker_ack_result_t *ack) {
  if (!completion || completion->size < sizeof(*completion) || completion->request_id == 0u ||
      !flow_fmq_broker_route_valid(&completion->client_route) || !ack || ack->size < sizeof(*ack)) {
    return TURBO_EINVAL;
  }
  memset(ack, 0, sizeof(*ack));
  ack->size = sizeof(*ack);
  ack->kind = TURBO_FLOW_FMQ_BROKER_ACK_WORKER_COMPLETION;
  ack->request_id = completion->request_id;
  ack->client_route = completion->client_route;
  ack->client_route.size = sizeof(ack->client_route);
  return TURBO_OK;
}

int turbo_flow_fmq_broker_expire(turbo_flow_fmq_broker_t *broker, uint64_t now_ms,
                                 turbo_flow_fmq_broker_expire_result_t *result) {
  flow_fmq_broker_worker_t *worker = NULL;
  size_t worker_index = 0u;
  int remove_status;
  if (!broker || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  memset(result, 0, sizeof(*result));
  result->size = sizeof(*result);
  result->client_route = (turbo_flow_protocol_route_t)TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  if (broker->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE) return TURBO_ENOTSUP;
  for (size_t i = 0; i < flow_fmq_broker_workers_size(&broker->workers); ++i) {
    flow_fmq_broker_worker_t *candidate = flow_fmq_broker_workers_at(&broker->workers, i);
    if (candidate && flow_fmq_broker_worker_expired(broker, candidate, now_ms)) {
      worker = candidate;
      worker_index = i;
      break;
    }
  }
  if (!worker) return TURBO_ENOENT;

  memcpy(result->worker_id, worker->worker_id, tstr_len(worker->worker_id));
  memcpy(result->service, worker->service, tstr_len(worker->service));
  if (worker->state == FLOW_FMQ_BROKER_WORKER_IDLE) {
    result->disposition = TURBO_FLOW_FMQ_BROKER_EXPIRED_IDLE;
  } else {
    flow_fmq_broker_inflight_t *entry = NULL;
    size_t inflight_index = 0u;
    for (size_t i = 0; i < flow_fmq_broker_inflight_size(&broker->inflight); ++i) {
      flow_fmq_broker_inflight_t *candidate = flow_fmq_broker_inflight_at(&broker->inflight, i);
      if (candidate && candidate->worker_index == worker_index) {
        entry = candidate;
        inflight_index = i;
        break;
      }
    }
    if (!entry) return TURBO_EPROTO;
    result->request_id = entry->request_id;
    result->client_route = entry->client_route;
    result->client_route.size = sizeof(result->client_route);
    result->disposition = broker->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE
                              ? TURBO_FLOW_FMQ_BROKER_EXPIRED_REQUEUE
                              : TURBO_FLOW_FMQ_BROKER_EXPIRED_DROP;
    if (flow_fmq_broker_inflight_remove(broker, inflight_index) != TURBO_OK) return TURBO_EPROTO;
    worker->state = FLOW_FMQ_BROKER_WORKER_IDLE;
  }

  remove_status = turbo_flow_fmq_broker_worker_remove(broker, result->worker_id);
  if (remove_status != TURBO_OK) return TURBO_EPROTO;
  broker->expired_workers += 1u;
  if (result->disposition == TURBO_FLOW_FMQ_BROKER_EXPIRED_DROP) broker->expired_drops += 1u;
  else if (result->disposition == TURBO_FLOW_FMQ_BROKER_EXPIRED_REQUEUE)
    broker->expired_requeues += 1u;
  return TURBO_OK;
}

int turbo_flow_fmq_broker_snapshot(const turbo_flow_fmq_broker_t *broker,
                                   turbo_flow_fmq_broker_snapshot_t *out) {
  if (!broker || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  memset(out, 0, sizeof(*out));
  out->size = sizeof(*out);
  out->workers = flow_fmq_broker_workers_size(&broker->workers);
  out->accepted_requests = flow_fmq_broker_accepted_size(&broker->accepted);
  out->inflight = flow_fmq_broker_inflight_size(&broker->inflight);
  out->dispatched = broker->dispatched;
  out->completed = broker->completed;
  out->canceled = broker->canceled;
  out->expired_workers = broker->expired_workers;
  out->expired_drops = broker->expired_drops;
  out->expired_requeues = broker->expired_requeues;
  out->accept_acks = broker->accept_acks;
  out->worker_completion_acks = broker->worker_completion_acks;
  for (size_t i = 0; i < flow_fmq_broker_workers_size(&broker->workers); ++i) {
    const flow_fmq_broker_worker_t *worker = flow_fmq_broker_workers_at_const(&broker->workers, i);
    if (worker && worker->state == FLOW_FMQ_BROKER_WORKER_IDLE) out->idle_workers += 1u;
    else if (worker && worker->state == FLOW_FMQ_BROKER_WORKER_BUSY) out->busy_workers += 1u;
  }
  return TURBO_OK;
}

static const uint8_t FLOW_TFCW_MAGIC[4] = {'T', 'F', 'C', 'W'};

static void flow_tfcw_write_u16(uint8_t *out, uint16_t value) {
  out[0] = (uint8_t)(value >> 8u);
  out[1] = (uint8_t)value;
}

static void flow_tfcw_write_u32(uint8_t *out, uint32_t value) {
  out[0] = (uint8_t)(value >> 24u);
  out[1] = (uint8_t)(value >> 16u);
  out[2] = (uint8_t)(value >> 8u);
  out[3] = (uint8_t)value;
}

static void flow_tfcw_write_u64(uint8_t *out, uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    out[i] = (uint8_t)(value >> (56u - 8u * i));
}

static uint16_t flow_tfcw_read_u16(const uint8_t *data) {
  return (uint16_t)(((uint16_t)data[0] << 8u) | data[1]);
}

static uint32_t flow_tfcw_read_u32(const uint8_t *data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) | ((uint32_t)data[2] << 8u) |
         data[3];
}

static uint64_t flow_tfcw_read_u64(const uint8_t *data) {
  uint64_t value = 0u;
  for (size_t i = 0u; i < 8u; ++i)
    value = (value << 8u) | data[i];
  return value;
}

static size_t flow_tfcw_varint_size(uint32_t value) {
  if (value < (1u << 7u)) return 1u;
  if (value < (1u << 14u)) return 2u;
  if (value < (1u << 21u)) return 3u;
  if (value < (1u << 28u)) return 4u;
  return 5u;
}

static int flow_tfcw_field_allowed(turbo_flow_tfcw_kind_t kind, uint8_t field_id) {
  switch (kind) {
  case TURBO_FLOW_TFCW_READY:
  case TURBO_FLOW_TFCW_CREDIT:
    return field_id >= TURBO_FLOW_TFCW_FIELD_WORKER_ID &&
           field_id <= TURBO_FLOW_TFCW_FIELD_GRANT_BYTES;
  case TURBO_FLOW_TFCW_HEARTBEAT:
    return field_id == TURBO_FLOW_TFCW_FIELD_WORKER_ID;
  case TURBO_FLOW_TFCW_JOB:
    return field_id >= TURBO_FLOW_TFCW_FIELD_LOGICAL_ADDRESS &&
           field_id <= TURBO_FLOW_TFCW_FIELD_PAYLOAD;
  case TURBO_FLOW_TFCW_COMPLETE:
    return field_id == TURBO_FLOW_TFCW_FIELD_WORKER_ID ||
           field_id == TURBO_FLOW_TFCW_FIELD_METADATA || field_id == TURBO_FLOW_TFCW_FIELD_PAYLOAD;
  case TURBO_FLOW_TFCW_FAIL:
    return field_id == TURBO_FLOW_TFCW_FIELD_WORKER_ID ||
           field_id == TURBO_FLOW_TFCW_FIELD_METADATA ||
           field_id == TURBO_FLOW_TFCW_FIELD_FAILURE_CODE ||
           field_id == TURBO_FLOW_TFCW_FIELD_RETRYABLE;
  default:
    return 0;
  }
}

static int flow_tfcw_field_value_valid(uint8_t field_id, const uint8_t *value, size_t value_size) {
  switch (field_id) {
  case TURBO_FLOW_TFCW_FIELD_WORKER_ID:
    return value_size > 0u && value_size <= TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX &&
           memchr(value, '\0', value_size) == NULL &&
           tstr_v_utf8_valid(tstr_v_from_buf((const char *)value, value_size));
  case TURBO_FLOW_TFCW_FIELD_SERVICE:
    return value_size > 0u && value_size <= TURBO_FLOW_FMQ_BROKER_SERVICE_MAX &&
           memchr(value, '\0', value_size) == NULL &&
           tstr_v_utf8_valid(tstr_v_from_buf((const char *)value, value_size));
  case TURBO_FLOW_TFCW_FIELD_GRANT_MESSAGES:
  case TURBO_FLOW_TFCW_FIELD_GRANT_BYTES:
    return value_size == 8u && flow_tfcw_read_u64(value) > 0u;
  case TURBO_FLOW_TFCW_FIELD_LOGICAL_ADDRESS:
    return value_size > 0u;
  case TURBO_FLOW_TFCW_FIELD_FAILURE_CODE:
    return value_size == 4u && flow_tfcw_read_u32(value) > 0u;
  case TURBO_FLOW_TFCW_FIELD_RETRYABLE:
    return value_size == 1u && value[0] <= 1u;
  case TURBO_FLOW_TFCW_FIELD_METADATA:
  case TURBO_FLOW_TFCW_FIELD_PAYLOAD:
    return 1;
  default:
    return 1;
  }
}

static int flow_tfcw_body_validate(turbo_flow_tfcw_kind_t kind, const uint8_t *body,
                                   size_t body_size, int wire_error,
                                   turbo_flow_tfcw_fields_t *decoded_fields) {
  uint8_t seen[TURBO_FLOW_TFCW_FIELD_ID_MASK + 1u] = {0};
  turbo_flow_tfcw_fields_t fields = TURBO_FLOW_TFCW_FIELDS_INIT;
  uint8_t previous_id = 0u;
  size_t offset = 0u;
  size_t field_count = 0u;
  int error = wire_error ? TURBO_EPROTO : TURBO_EINVAL;
  if ((!body && body_size > 0u) || body_size > TURBO_FLOW_TFCW_MAX_BODY_SIZE) return error;
  while (offset < body_size) {
    uint32_t length = 0u;
    size_t header_size = 0u;
    size_t remaining = body_size - offset;
    uint8_t type;
    uint8_t field_id;
    const uint8_t *value;
    size_t value_size;
    if (field_count >= TURBO_FLOW_TFCW_MAX_FIELDS ||
        turbo_ltv_peek_size(body + offset, remaining, &length, &header_size) != 0 || length == 0u ||
        header_size != flow_tfcw_varint_size(length) || header_size > remaining ||
        length > remaining - header_size) {
      return error;
    }
    type = body[offset + header_size];
    field_id = type & TURBO_FLOW_TFCW_FIELD_ID_MASK;
    if (field_id == 0u || field_id <= previous_id || seen[field_id]) return error;
    value = body + offset + header_size + 1u;
    value_size = (size_t)length - 1u;
    if (field_id <= TURBO_FLOW_TFCW_FIELD_RETRYABLE) {
      if (!flow_tfcw_field_allowed(kind, field_id) ||
          !flow_tfcw_field_value_valid(field_id, value, value_size)) {
        return error;
      }
    } else if ((type & TURBO_FLOW_TFCW_FIELD_CRITICAL) != 0u) {
      return error;
    }
    if (field_id <= TURBO_FLOW_TFCW_FIELD_RETRYABLE)
      fields.present |= TURBO_FLOW_TFCW_FIELD_PRESENT(field_id);
    switch (field_id) {
    case TURBO_FLOW_TFCW_FIELD_WORKER_ID:
      fields.worker_id = tstr_v_from_buf((const char *)value, value_size);
      break;
    case TURBO_FLOW_TFCW_FIELD_SERVICE:
      fields.service = tstr_v_from_buf((const char *)value, value_size);
      break;
    case TURBO_FLOW_TFCW_FIELD_GRANT_MESSAGES:
      fields.grant_messages = flow_tfcw_read_u64(value);
      break;
    case TURBO_FLOW_TFCW_FIELD_GRANT_BYTES:
      fields.grant_bytes = flow_tfcw_read_u64(value);
      break;
    case TURBO_FLOW_TFCW_FIELD_LOGICAL_ADDRESS:
      fields.logical_address = tstr_v_from_buf((const char *)value, value_size);
      break;
    case TURBO_FLOW_TFCW_FIELD_METADATA:
      fields.metadata = tstr_v_from_buf((const char *)value, value_size);
      break;
    case TURBO_FLOW_TFCW_FIELD_PAYLOAD:
      fields.payload = tstr_v_from_buf((const char *)value, value_size);
      break;
    case TURBO_FLOW_TFCW_FIELD_FAILURE_CODE:
      fields.failure_code = flow_tfcw_read_u32(value);
      break;
    case TURBO_FLOW_TFCW_FIELD_RETRYABLE:
      fields.retryable = value[0] != 0u;
      break;
    default:
      break;
    }
    seen[field_id] = 1u;
    previous_id = field_id;
    offset += header_size + length;
    field_count++;
  }
  switch (kind) {
  case TURBO_FLOW_TFCW_READY:
  case TURBO_FLOW_TFCW_CREDIT:
    if (!seen[TURBO_FLOW_TFCW_FIELD_WORKER_ID] || !seen[TURBO_FLOW_TFCW_FIELD_SERVICE] ||
        !seen[TURBO_FLOW_TFCW_FIELD_GRANT_MESSAGES] || !seen[TURBO_FLOW_TFCW_FIELD_GRANT_BYTES]) {
      return error;
    }
    break;
  case TURBO_FLOW_TFCW_HEARTBEAT:
  case TURBO_FLOW_TFCW_COMPLETE:
    if (!seen[TURBO_FLOW_TFCW_FIELD_WORKER_ID]) return error;
    break;
  case TURBO_FLOW_TFCW_JOB:
    if (!seen[TURBO_FLOW_TFCW_FIELD_LOGICAL_ADDRESS] || !seen[TURBO_FLOW_TFCW_FIELD_PAYLOAD]) {
      return error;
    }
    break;
  case TURBO_FLOW_TFCW_FAIL:
    if (!seen[TURBO_FLOW_TFCW_FIELD_WORKER_ID] || !seen[TURBO_FLOW_TFCW_FIELD_FAILURE_CODE]) {
      return error;
    }
    break;
  default:
    return error;
  }
  if (decoded_fields) *decoded_fields = fields;
  return TURBO_OK;
}

static int flow_tfcw_envelope_validate(const turbo_flow_tfcw_envelope_t *envelope, int wire_error) {
  int error = wire_error ? TURBO_EPROTO : TURBO_EINVAL;
  if (!envelope || envelope->kind < TURBO_FLOW_TFCW_READY ||
      envelope->kind > TURBO_FLOW_TFCW_FAIL || envelope->flags != 0u ||
      (!envelope->body && envelope->body_size > 0u)) {
    return error;
  }
  switch (envelope->kind) {
  case TURBO_FLOW_TFCW_READY:
  case TURBO_FLOW_TFCW_CREDIT:
    if (envelope->request_id != 0u || envelope->credit_sequence == 0u) return error;
    break;
  case TURBO_FLOW_TFCW_HEARTBEAT:
    if (envelope->request_id != 0u || envelope->credit_sequence != 0u) return error;
    break;
  case TURBO_FLOW_TFCW_JOB:
  case TURBO_FLOW_TFCW_COMPLETE:
  case TURBO_FLOW_TFCW_FAIL:
    if (envelope->request_id == 0u || envelope->credit_sequence != 0u) return error;
    break;
  default:
    return error;
  }
  return flow_tfcw_body_validate(envelope->kind, envelope->body, envelope->body_size, wire_error,
                                 NULL);
}

int turbo_flow_tfcw_encode(const turbo_flow_tfcw_envelope_t *envelope, uint8_t *out,
                           size_t out_capacity, size_t *out_size) {
  size_t required;
  int rc;
  if (!out_size) return TURBO_EINVAL;
  *out_size = 0u;
  if (!envelope || envelope->size < sizeof(*envelope)) return TURBO_EINVAL;
  rc = flow_tfcw_envelope_validate(envelope, 0);
  if (rc != TURBO_OK) return rc;
  required = TURBO_FLOW_TFCW_HEADER_SIZE + envelope->body_size;
  *out_size = required;
  if (!out || out_capacity < required) return TURBO_ENOSPC;
  memcpy(out, FLOW_TFCW_MAGIC, sizeof(FLOW_TFCW_MAGIC));
  out[4] = TURBO_FLOW_TFCW_PROTOCOL_MAJOR;
  out[5] = TURBO_FLOW_TFCW_PROTOCOL_MINOR;
  flow_tfcw_write_u16(out + 6u, (uint16_t)envelope->kind);
  flow_tfcw_write_u16(out + 8u, envelope->flags);
  flow_tfcw_write_u16(out + 10u, 0u);
  flow_tfcw_write_u32(out + 12u, (uint32_t)envelope->body_size);
  flow_tfcw_write_u64(out + 16u, envelope->request_id);
  flow_tfcw_write_u64(out + 24u, envelope->credit_sequence);
  flow_tfcw_write_u64(out + 32u, envelope->sender_timestamp_ms);
  if (envelope->body_size > 0u)
    memcpy(out + TURBO_FLOW_TFCW_HEADER_SIZE, envelope->body, envelope->body_size);
  return TURBO_OK;
}

int turbo_flow_tfcw_decode(const uint8_t *data, size_t data_size, turbo_flow_tfcw_envelope_t *out) {
  turbo_flow_tfcw_envelope_t decoded = TURBO_FLOW_TFCW_ENVELOPE_INIT;
  size_t body_size;
  int rc;
  if (!data || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  if (data_size > TURBO_FLOW_TFCW_HEADER_SIZE + TURBO_FLOW_TFCW_MAX_BODY_SIZE)
    return TURBO_EMSGSIZE;
  if (data_size < TURBO_FLOW_TFCW_HEADER_SIZE ||
      memcmp(data, FLOW_TFCW_MAGIC, sizeof(FLOW_TFCW_MAGIC)) != 0) {
    return TURBO_EPROTO;
  }
  if (data[4] != TURBO_FLOW_TFCW_PROTOCOL_MAJOR || data[5] > TURBO_FLOW_TFCW_PROTOCOL_MINOR) {
    return TURBO_ENOTSUP;
  }
  if (flow_tfcw_read_u16(data + 10u) != 0u) return TURBO_EPROTO;
  decoded.kind = (turbo_flow_tfcw_kind_t)flow_tfcw_read_u16(data + 6u);
  decoded.flags = flow_tfcw_read_u16(data + 8u);
  body_size = flow_tfcw_read_u32(data + 12u);
  decoded.request_id = flow_tfcw_read_u64(data + 16u);
  decoded.credit_sequence = flow_tfcw_read_u64(data + 24u);
  decoded.sender_timestamp_ms = flow_tfcw_read_u64(data + 32u);
  if (body_size > TURBO_FLOW_TFCW_MAX_BODY_SIZE) return TURBO_EMSGSIZE;
  if (data_size != TURBO_FLOW_TFCW_HEADER_SIZE + body_size) return TURBO_EPROTO;
  decoded.body = body_size > 0u ? data + TURBO_FLOW_TFCW_HEADER_SIZE : NULL;
  decoded.body_size = body_size;
  rc = flow_tfcw_envelope_validate(&decoded, 1);
  if (rc != TURBO_OK) return rc;
  *out = decoded;
  return TURBO_OK;
}

int turbo_flow_tfcw_fields_decode(const turbo_flow_tfcw_envelope_t *envelope,
                                  turbo_flow_tfcw_fields_t *out) {
  turbo_flow_tfcw_fields_t decoded = TURBO_FLOW_TFCW_FIELDS_INIT;
  int rc;
  if (!envelope || envelope->size < sizeof(*envelope) || !out || out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flow_tfcw_envelope_validate(envelope, 0);
  if (rc != TURBO_OK) return rc;
  rc = flow_tfcw_body_validate(envelope->kind, envelope->body, envelope->body_size, 0, &decoded);
  if (rc != TURBO_OK) return rc;
  *out = decoded;
  return TURBO_OK;
}

typedef struct flow_fmq_credit_worker_record_s {
  tstr_t worker_id;
  tstr_t service;
  turbo_flow_protocol_route_t route;
  uint64_t last_sequence;
  size_t last_grant_messages;
  size_t last_grant_bytes;
  size_t available_messages;
  size_t available_bytes;
  size_t inflight_count;
  uint64_t available_order;
  uint64_t last_seen_ms;
} flow_fmq_credit_worker_record_t;

typedef struct flow_fmq_credit_inflight_s {
  uint64_t request_id;
  size_t worker_index;
  size_t encoded_job_bytes;
  turbo_flow_protocol_route_t client_route;
} flow_fmq_credit_inflight_t;

TURBO_VEC_DEFINE(flow_fmq_credit_workers, flow_fmq_credit_worker_record_t)
TURBO_VEC_DEFINE(flow_fmq_credit_inflight, flow_fmq_credit_inflight_t)
TURBO_HASH_MAP_DEFINE(flow_fmq_credit_worker_index, flow_fmq_broker_worker_key_t, size_t)
TURBO_HASH_MAP_DEFINE(flow_fmq_credit_request_index, uint64_t, size_t)

struct turbo_flow_fmq_credit_worker_s {
  flow_fmq_credit_workers workers;
  flow_fmq_credit_inflight inflight;
  flow_fmq_credit_worker_index worker_index;
  flow_fmq_credit_request_index request_index;
  size_t max_workers;
  size_t max_inflight;
  uint64_t worker_lease_ms;
  size_t max_credit_messages_per_worker;
  size_t max_credit_bytes_per_worker;
  size_t max_job_bytes;
  turbo_flow_fmq_broker_reliability_t reliability;
  uint64_t order;
  uint64_t grants;
  uint64_t dispatched;
  uint64_t completed;
  uint64_t canceled;
  uint64_t expired_workers;
  uint64_t expired_requests;
};

static int flow_fmq_credit_config_valid(const turbo_flow_fmq_credit_worker_config_t *config) {
  return config && config->size >= sizeof(*config) &&
         config->version == TURBO_FLOW_FMQ_CREDIT_WORKER_API_VERSION && config->max_workers > 0u &&
         config->max_workers <= TURBO_FLOW_FMQ_BROKER_MAX_WORKERS && config->max_inflight > 0u &&
         config->max_inflight <= TURBO_FLOW_FMQ_BROKER_MAX_INFLIGHT &&
         config->worker_lease_ms > 0u && config->max_credit_messages_per_worker > 0u &&
         config->max_credit_bytes_per_worker > 0u && config->max_job_bytes > 0u &&
         config->max_job_bytes <= config->max_credit_bytes_per_worker &&
         config->max_credit_messages_per_worker <= SIZE_MAX / config->max_workers &&
         config->max_credit_bytes_per_worker <= SIZE_MAX / config->max_workers &&
         (config->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE ||
          config->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE);
}

static void flow_fmq_credit_worker_record_cleanup(flow_fmq_credit_worker_record_t *worker) {
  if (!worker) return;
  tstr_freep(&worker->worker_id);
  tstr_freep(&worker->service);
  memset(worker, 0, sizeof(*worker));
}

static int flow_fmq_credit_worker_find(const turbo_flow_fmq_credit_worker_t *owner,
                                       const char *worker_id) {
  flow_fmq_broker_worker_key_t key = flow_fmq_broker_worker_key(worker_id);
  const size_t *index = flow_fmq_credit_worker_index_get_const(&owner->worker_index, key);
  return index ? (int)*index : -1;
}

static int flow_fmq_credit_request_find(const turbo_flow_fmq_credit_worker_t *owner,
                                        uint64_t request_id) {
  const size_t *index = flow_fmq_credit_request_index_get_const(&owner->request_index, request_id);
  return index ? (int)*index : -1;
}

static int flow_fmq_credit_worker_live(const turbo_flow_fmq_credit_worker_t *owner,
                                       const flow_fmq_credit_worker_record_t *worker,
                                       uint64_t now_ms) {
  return now_ms >= worker->last_seen_ms && now_ms - worker->last_seen_ms < owner->worker_lease_ms;
}

static void flow_fmq_credit_completion_fill(turbo_flow_fmq_broker_completion_result_t *result,
                                            uint64_t request_id,
                                            const flow_fmq_credit_worker_record_t *worker,
                                            const turbo_flow_protocol_route_t *client_route) {
  memset(result, 0, sizeof(*result));
  result->size = sizeof(*result);
  result->request_id = request_id;
  memcpy(result->worker_id, worker->worker_id, tstr_len(worker->worker_id));
  result->client_route = flow_fmq_broker_route_copy(client_route);
}

static int flow_fmq_credit_inflight_remove(turbo_flow_fmq_credit_worker_t *owner, size_t index) {
  size_t count = flow_fmq_credit_inflight_size(&owner->inflight);
  flow_fmq_credit_inflight_t *slot = flow_fmq_credit_inflight_at(&owner->inflight, index);
  uint64_t removed_request_id;
  if (!slot || index >= count) return TURBO_EINVAL;
  removed_request_id = slot->request_id;
  if (index + 1u < count) {
    const flow_fmq_credit_inflight_t *last =
        flow_fmq_credit_inflight_at_const(&owner->inflight, count - 1u);
    if (!last || flow_fmq_credit_request_index_put(&owner->request_index, last->request_id,
                                                   index) != TURBO_OK) {
      return TURBO_EPROTO;
    }
    if (!flow_fmq_credit_request_index_remove(&owner->request_index, removed_request_id, NULL))
      return TURBO_EPROTO;
    *slot = *last;
  } else if (!flow_fmq_credit_request_index_remove(&owner->request_index, removed_request_id,
                                                   NULL)) {
    return TURBO_EPROTO;
  }
  return turbo_vec_resize(&owner->inflight.raw, count - 1u) == TURBO_OK ? TURBO_OK : TURBO_EPROTO;
}

static int flow_fmq_credit_worker_remove(turbo_flow_fmq_credit_worker_t *owner, size_t index) {
  size_t count = flow_fmq_credit_workers_size(&owner->workers);
  flow_fmq_credit_worker_record_t *worker = flow_fmq_credit_workers_at(&owner->workers, index);
  flow_fmq_broker_worker_key_t removed_key;
  if (!worker || index >= count || worker->inflight_count != 0u) return TURBO_EBUSY;
  removed_key = flow_fmq_broker_worker_key(worker->worker_id);
  if (index + 1u < count) {
    const flow_fmq_credit_worker_record_t *last =
        flow_fmq_credit_workers_at_const(&owner->workers, count - 1u);
    flow_fmq_broker_worker_key_t moved_key;
    if (!last) return TURBO_EPROTO;
    moved_key = flow_fmq_broker_worker_key(last->worker_id);
    if (flow_fmq_credit_worker_index_put(&owner->worker_index, moved_key, index) != TURBO_OK)
      return TURBO_EPROTO;
    if (!flow_fmq_credit_worker_index_remove(&owner->worker_index, removed_key, NULL))
      return TURBO_EPROTO;
    flow_fmq_credit_worker_record_cleanup(worker);
    *worker = *last;
    for (size_t i = 0u; i < flow_fmq_credit_inflight_size(&owner->inflight); ++i) {
      flow_fmq_credit_inflight_t *entry = flow_fmq_credit_inflight_at(&owner->inflight, i);
      if (entry && entry->worker_index == count - 1u) entry->worker_index = index;
    }
  } else {
    if (!flow_fmq_credit_worker_index_remove(&owner->worker_index, removed_key, NULL))
      return TURBO_EPROTO;
    flow_fmq_credit_worker_record_cleanup(worker);
  }
  return turbo_vec_resize(&owner->workers.raw, count - 1u) == TURBO_OK ? TURBO_OK : TURBO_EPROTO;
}

turbo_flow_fmq_credit_worker_t *
turbo_flow_fmq_credit_worker_create(const turbo_flow_fmq_credit_worker_config_t *config) {
  turbo_flow_fmq_credit_worker_t *owner;
  if (!flow_fmq_credit_config_valid(config)) return NULL;
  owner = (turbo_flow_fmq_credit_worker_t *)calloc(1, sizeof(*owner));
  if (!owner) return NULL;
  if (flow_fmq_credit_workers_init(&owner->workers) != TURBO_OK ||
      flow_fmq_credit_inflight_init(&owner->inflight) != TURBO_OK ||
      flow_fmq_credit_worker_index_init(&owner->worker_index) != TURBO_OK ||
      flow_fmq_credit_request_index_init(&owner->request_index) != TURBO_OK ||
      flow_fmq_credit_workers_reserve(&owner->workers, config->max_workers) != TURBO_OK ||
      flow_fmq_credit_inflight_reserve(&owner->inflight, config->max_inflight) != TURBO_OK ||
      turbo_hash_map_reserve(&owner->worker_index.raw, config->max_workers) != TURBO_OK ||
      turbo_hash_map_reserve(&owner->request_index.raw, config->max_inflight) != TURBO_OK) {
    turbo_flow_fmq_credit_worker_destroy(owner);
    return NULL;
  }
  owner->max_workers = config->max_workers;
  owner->max_inflight = config->max_inflight;
  owner->worker_lease_ms = config->worker_lease_ms;
  owner->max_credit_messages_per_worker = config->max_credit_messages_per_worker;
  owner->max_credit_bytes_per_worker = config->max_credit_bytes_per_worker;
  owner->max_job_bytes = config->max_job_bytes;
  owner->reliability = config->reliability;
  return owner;
}

void turbo_flow_fmq_credit_worker_destroy(turbo_flow_fmq_credit_worker_t *owner) {
  if (!owner) return;
  for (size_t i = 0u; i < flow_fmq_credit_workers_size(&owner->workers); ++i)
    flow_fmq_credit_worker_record_cleanup(flow_fmq_credit_workers_at(&owner->workers, i));
  flow_fmq_credit_workers_destroy(&owner->workers);
  flow_fmq_credit_inflight_destroy(&owner->inflight);
  flow_fmq_credit_worker_index_destroy(&owner->worker_index);
  flow_fmq_credit_request_index_destroy(&owner->request_index);
  free(owner);
}

int turbo_flow_fmq_credit_worker_grant(turbo_flow_fmq_credit_worker_t *owner,
                                       const turbo_flow_fmq_credit_grant_t *grant) {
  flow_fmq_credit_worker_record_t record;
  flow_fmq_credit_worker_record_t *worker;
  int found;
  if (!owner || !grant || grant->size < sizeof(*grant) ||
      !flow_fmq_broker_name_valid(grant->worker_id, TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX) ||
      !flow_fmq_broker_name_valid(grant->service, TURBO_FLOW_FMQ_BROKER_SERVICE_MAX) ||
      !flow_fmq_broker_route_valid(&grant->worker_route) || grant->sequence == 0u ||
      grant->grant_messages == 0u || grant->grant_bytes == 0u) {
    return TURBO_EINVAL;
  }
  if (grant->grant_messages > owner->max_credit_messages_per_worker ||
      grant->grant_bytes > owner->max_credit_bytes_per_worker) {
    return TURBO_ERANGE;
  }
  found = flow_fmq_credit_worker_find(owner, grant->worker_id);
  if (found >= 0) {
    worker = flow_fmq_credit_workers_at(&owner->workers, (size_t)found);
    if (!worker || strcmp(worker->service, grant->service) != 0) return TURBO_EPROTO;
    if (grant->now_ms < worker->last_seen_ms) return TURBO_EALREADY;
    if (!flow_fmq_broker_route_equal(&worker->route, &grant->worker_route)) {
      if (worker->inflight_count > 0u) return TURBO_EBUSY;
      if (grant->sequence != 1u) return TURBO_EPROTO;
      worker->route = flow_fmq_broker_route_copy(&grant->worker_route);
      worker->last_sequence = 1u;
      worker->last_grant_messages = grant->grant_messages;
      worker->last_grant_bytes = grant->grant_bytes;
      worker->available_messages = grant->grant_messages;
      worker->available_bytes = grant->grant_bytes;
      worker->available_order = ++owner->order;
      worker->last_seen_ms = grant->now_ms;
      owner->grants += 1u;
      return TURBO_OK;
    }
    if (grant->sequence < worker->last_sequence) return TURBO_EALREADY;
    if (grant->sequence == worker->last_sequence) {
      if (grant->grant_messages != worker->last_grant_messages ||
          grant->grant_bytes != worker->last_grant_bytes) {
        return TURBO_EPROTO;
      }
      worker->last_seen_ms = grant->now_ms;
      return TURBO_OK;
    }
    if (worker->last_sequence == UINT64_MAX || grant->sequence != worker->last_sequence + 1u)
      return TURBO_EPROTO;
    if (grant->grant_messages >
            owner->max_credit_messages_per_worker - worker->available_messages ||
        grant->grant_bytes > owner->max_credit_bytes_per_worker - worker->available_bytes) {
      return TURBO_ERANGE;
    }
    worker->last_sequence = grant->sequence;
    worker->last_grant_messages = grant->grant_messages;
    worker->last_grant_bytes = grant->grant_bytes;
    worker->available_messages += grant->grant_messages;
    worker->available_bytes += grant->grant_bytes;
    worker->available_order = ++owner->order;
    worker->last_seen_ms = grant->now_ms;
    owner->grants += 1u;
    return TURBO_OK;
  }
  if (grant->sequence != 1u) return TURBO_EPROTO;
  if (flow_fmq_credit_workers_size(&owner->workers) >= owner->max_workers) return TURBO_ENOSPC;
  memset(&record, 0, sizeof(record));
  record.worker_id = tstr_dup(grant->worker_id);
  record.service = tstr_dup(grant->service);
  if (!record.worker_id || !record.service) {
    flow_fmq_credit_worker_record_cleanup(&record);
    return TURBO_ENOMEM;
  }
  record.route = flow_fmq_broker_route_copy(&grant->worker_route);
  record.last_sequence = grant->sequence;
  record.last_grant_messages = grant->grant_messages;
  record.last_grant_bytes = grant->grant_bytes;
  record.available_messages = grant->grant_messages;
  record.available_bytes = grant->grant_bytes;
  record.available_order = ++owner->order;
  record.last_seen_ms = grant->now_ms;
  if (flow_fmq_credit_workers_push(&owner->workers, record) != TURBO_OK) {
    flow_fmq_credit_worker_record_cleanup(&record);
    return TURBO_ENOMEM;
  }
  {
    size_t index = flow_fmq_credit_workers_size(&owner->workers) - 1u;
    flow_fmq_broker_worker_key_t key = flow_fmq_broker_worker_key(grant->worker_id);
    if (flow_fmq_credit_worker_index_put(&owner->worker_index, key, index) != TURBO_OK) {
      flow_fmq_credit_worker_record_cleanup(flow_fmq_credit_workers_at(&owner->workers, index));
      (void)turbo_vec_resize(&owner->workers.raw, index);
      return TURBO_ENOMEM;
    }
  }
  owner->grants += 1u;
  return TURBO_OK;
}

int turbo_flow_fmq_credit_worker_heartbeat(turbo_flow_fmq_credit_worker_t *owner,
                                           const char *worker_id,
                                           const turbo_flow_protocol_route_t *worker_route,
                                           uint64_t now_ms) {
  flow_fmq_credit_worker_record_t *worker;
  int found;
  if (!owner || !flow_fmq_broker_name_valid(worker_id, TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX) ||
      !flow_fmq_broker_route_valid(worker_route)) {
    return TURBO_EINVAL;
  }
  found = flow_fmq_credit_worker_find(owner, worker_id);
  if (found < 0) return TURBO_ENOENT;
  worker = flow_fmq_credit_workers_at(&owner->workers, (size_t)found);
  if (!worker) return TURBO_EPROTO;
  if (!flow_fmq_broker_route_equal(&worker->route, worker_route)) return TURBO_EPROTO;
  if (now_ms < worker->last_seen_ms) return TURBO_EALREADY;
  worker->last_seen_ms = now_ms;
  return TURBO_OK;
}

int turbo_flow_fmq_credit_worker_dispatch(turbo_flow_fmq_credit_worker_t *owner,
                                          const char *service, uint64_t request_id,
                                          size_t encoded_job_bytes,
                                          const turbo_flow_protocol_route_t *client_route,
                                          uint64_t now_ms,
                                          turbo_flow_fmq_broker_dispatch_result_t *result) {
  size_t selected;
  uint64_t oldest = UINT64_MAX;
  int live_service = 0;
  flow_fmq_credit_inflight_t entry;
  flow_fmq_credit_worker_record_t *worker;
  if (!owner || !flow_fmq_broker_name_valid(service, TURBO_FLOW_FMQ_BROKER_SERVICE_MAX) ||
      request_id == 0u || encoded_job_bytes == 0u || !flow_fmq_broker_route_valid(client_route) ||
      !result || result->size < sizeof(*result)) {
    return TURBO_EINVAL;
  }
  memset(result, 0, sizeof(*result));
  result->size = sizeof(*result);
  if (encoded_job_bytes > owner->max_job_bytes) return TURBO_EMSGSIZE;
  if (flow_fmq_credit_request_find(owner, request_id) >= 0) return TURBO_EALREADY;
  if (flow_fmq_credit_inflight_size(&owner->inflight) >= owner->max_inflight) return TURBO_ENOSPC;
  selected = flow_fmq_credit_workers_size(&owner->workers);
  for (size_t i = 0u; i < flow_fmq_credit_workers_size(&owner->workers); ++i) {
    const flow_fmq_credit_worker_record_t *candidate =
        flow_fmq_credit_workers_at_const(&owner->workers, i);
    if (!candidate || strcmp(candidate->service, service) != 0 ||
        !flow_fmq_credit_worker_live(owner, candidate, now_ms)) {
      continue;
    }
    live_service = 1;
    if (candidate->available_messages == 0u || candidate->available_bytes < encoded_job_bytes) {
      continue;
    }
    if (selected == flow_fmq_credit_workers_size(&owner->workers) ||
        candidate->available_order < oldest) {
      selected = i;
      oldest = candidate->available_order;
    }
  }
  if (selected == flow_fmq_credit_workers_size(&owner->workers))
    return live_service ? TURBO_FLOW_FMQ_EAGAIN : TURBO_ENOTCONN;
  memset(&entry, 0, sizeof(entry));
  entry.request_id = request_id;
  entry.worker_index = selected;
  entry.encoded_job_bytes = encoded_job_bytes;
  entry.client_route = flow_fmq_broker_route_copy(client_route);
  if (flow_fmq_credit_inflight_push(&owner->inflight, entry) != TURBO_OK) return TURBO_ENOMEM;
  {
    size_t index = flow_fmq_credit_inflight_size(&owner->inflight) - 1u;
    if (flow_fmq_credit_request_index_put(&owner->request_index, request_id, index) != TURBO_OK) {
      (void)turbo_vec_resize(&owner->inflight.raw, index);
      return TURBO_ENOMEM;
    }
  }
  worker = flow_fmq_credit_workers_at(&owner->workers, selected);
  if (!worker || worker->available_messages == 0u || worker->available_bytes < encoded_job_bytes)
    return TURBO_EPROTO;
  worker->available_messages -= 1u;
  worker->available_bytes -= encoded_job_bytes;
  worker->inflight_count += 1u;
  worker->available_order = ++owner->order;
  owner->dispatched += 1u;
  result->request_id = request_id;
  memcpy(result->worker_id, worker->worker_id, tstr_len(worker->worker_id));
  memcpy(result->service, worker->service, tstr_len(worker->service));
  result->worker_route = flow_fmq_broker_route_copy(&worker->route);
  return TURBO_OK;
}

int turbo_flow_fmq_credit_worker_complete(turbo_flow_fmq_credit_worker_t *owner,
                                          const char *worker_id,
                                          const turbo_flow_protocol_route_t *worker_route,
                                          uint64_t request_id,
                                          turbo_flow_fmq_broker_completion_result_t *result) {
  flow_fmq_credit_inflight_t *entry;
  flow_fmq_credit_worker_record_t *worker;
  int found;
  if (!owner || !flow_fmq_broker_name_valid(worker_id, TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX) ||
      !flow_fmq_broker_route_valid(worker_route) || request_id == 0u || !result ||
      result->size < sizeof(*result)) {
    return TURBO_EINVAL;
  }
  found = flow_fmq_credit_request_find(owner, request_id);
  if (found < 0) return TURBO_ENOENT;
  entry = flow_fmq_credit_inflight_at(&owner->inflight, (size_t)found);
  worker = entry ? flow_fmq_credit_workers_at(&owner->workers, entry->worker_index) : NULL;
  if (!entry || !worker || strcmp(worker->worker_id, worker_id) != 0 ||
      !flow_fmq_broker_route_equal(&worker->route, worker_route) || worker->inflight_count == 0u) {
    return TURBO_EPROTO;
  }
  flow_fmq_credit_completion_fill(result, request_id, worker, &entry->client_route);
  if (flow_fmq_credit_inflight_remove(owner, (size_t)found) != TURBO_OK) return TURBO_EPROTO;
  worker->inflight_count -= 1u;
  owner->completed += 1u;
  return TURBO_OK;
}

int turbo_flow_fmq_credit_worker_cancel(turbo_flow_fmq_credit_worker_t *owner, uint64_t request_id,
                                        turbo_flow_fmq_broker_completion_result_t *result) {
  flow_fmq_credit_inflight_t *entry;
  flow_fmq_credit_worker_record_t *worker;
  int found;
  if (!owner || request_id == 0u || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  found = flow_fmq_credit_request_find(owner, request_id);
  if (found < 0) return TURBO_ENOENT;
  entry = flow_fmq_credit_inflight_at(&owner->inflight, (size_t)found);
  worker = entry ? flow_fmq_credit_workers_at(&owner->workers, entry->worker_index) : NULL;
  if (!entry || !worker || worker->inflight_count == 0u) return TURBO_EPROTO;
  flow_fmq_credit_completion_fill(result, request_id, worker, &entry->client_route);
  if (flow_fmq_credit_inflight_remove(owner, (size_t)found) != TURBO_OK) return TURBO_EPROTO;
  worker->inflight_count -= 1u;
  owner->canceled += 1u;
  return TURBO_OK;
}

int turbo_flow_fmq_credit_worker_expire(turbo_flow_fmq_credit_worker_t *owner, uint64_t now_ms,
                                        turbo_flow_fmq_broker_expire_result_t *result) {
  flow_fmq_credit_worker_record_t *worker = NULL;
  size_t worker_index = 0u;
  if (!owner || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  memset(result, 0, sizeof(*result));
  result->size = sizeof(*result);
  result->client_route = (turbo_flow_protocol_route_t)TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  for (size_t i = 0u; i < flow_fmq_credit_workers_size(&owner->workers); ++i) {
    flow_fmq_credit_worker_record_t *candidate = flow_fmq_credit_workers_at(&owner->workers, i);
    if (candidate && now_ms >= candidate->last_seen_ms &&
        now_ms - candidate->last_seen_ms >= owner->worker_lease_ms) {
      worker = candidate;
      worker_index = i;
      break;
    }
  }
  if (!worker) return TURBO_ENOENT;
  memcpy(result->worker_id, worker->worker_id, tstr_len(worker->worker_id));
  memcpy(result->service, worker->service, tstr_len(worker->service));
  if (worker->inflight_count == 0u) {
    result->disposition = TURBO_FLOW_FMQ_BROKER_EXPIRED_IDLE;
  } else {
    flow_fmq_credit_inflight_t *entry = NULL;
    size_t inflight_index = 0u;
    for (size_t i = 0u; i < flow_fmq_credit_inflight_size(&owner->inflight); ++i) {
      flow_fmq_credit_inflight_t *candidate = flow_fmq_credit_inflight_at(&owner->inflight, i);
      if (candidate && candidate->worker_index == worker_index) {
        entry = candidate;
        inflight_index = i;
        break;
      }
    }
    if (!entry) return TURBO_EPROTO;
    result->request_id = entry->request_id;
    result->client_route = flow_fmq_broker_route_copy(&entry->client_route);
    result->disposition = owner->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE
                              ? TURBO_FLOW_FMQ_BROKER_EXPIRED_REQUEUE
                              : TURBO_FLOW_FMQ_BROKER_EXPIRED_DROP;
    if (flow_fmq_credit_inflight_remove(owner, inflight_index) != TURBO_OK) return TURBO_EPROTO;
    worker->inflight_count -= 1u;
    owner->expired_requests += 1u;
  }
  if (worker->inflight_count == 0u) {
    if (flow_fmq_credit_worker_remove(owner, worker_index) != TURBO_OK) return TURBO_EPROTO;
    owner->expired_workers += 1u;
  }
  return TURBO_OK;
}

int turbo_flow_fmq_credit_worker_snapshot(const turbo_flow_fmq_credit_worker_t *owner,
                                          turbo_flow_fmq_credit_worker_snapshot_t *out) {
  if (!owner || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  memset(out, 0, sizeof(*out));
  out->size = sizeof(*out);
  out->workers = flow_fmq_credit_workers_size(&owner->workers);
  out->inflight = flow_fmq_credit_inflight_size(&owner->inflight);
  out->grants = owner->grants;
  out->dispatched = owner->dispatched;
  out->completed = owner->completed;
  out->canceled = owner->canceled;
  out->expired_workers = owner->expired_workers;
  out->expired_requests = owner->expired_requests;
  for (size_t i = 0u; i < flow_fmq_credit_workers_size(&owner->workers); ++i) {
    const flow_fmq_credit_worker_record_t *worker =
        flow_fmq_credit_workers_at_const(&owner->workers, i);
    if (!worker) continue;
    out->available_messages += worker->available_messages;
    out->available_bytes += worker->available_bytes;
    if (worker->available_messages > 0u && worker->available_bytes > 0u)
      out->available_workers += 1u;
  }
  return TURBO_OK;
}

typedef enum flow_fmq_credit_settlement_state_e {
  FLOW_FMQ_CREDIT_SETTLEMENT_DISPATCHED = 1,
  FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_ACK,
  FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_REQUEUE,
  FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_DROP
} flow_fmq_credit_settlement_state_t;

typedef struct flow_fmq_credit_settlement_record_s {
  uint64_t request_id;
  uint64_t claim_token;
  flow_fmq_credit_settlement_state_t state;
  turbo_flow_fmq_broker_completion_result_t completion;
} flow_fmq_credit_settlement_record_t;

TURBO_VEC_DEFINE(flow_fmq_credit_settlement_records, flow_fmq_credit_settlement_record_t)
TURBO_HASH_MAP_DEFINE(flow_fmq_credit_settlement_index, uint64_t, size_t)

struct turbo_flow_fmq_credit_settlement_s {
  turbo_flow_fmq_credit_worker_t *credit_owner;
  turbo_flow_claim_settler_t settler;
  flow_fmq_credit_settlement_records records;
  flow_fmq_credit_settlement_index request_index;
  flow_fmq_credit_settlement_index claim_index;
  size_t capacity;
  uint64_t settled_acks;
  uint64_t settled_requeues;
  uint64_t settled_drops;
  uint64_t settlement_failures;
};

static int flow_fmq_credit_settlement_remove(turbo_flow_fmq_credit_settlement_t *owner,
                                             size_t index) {
  size_t count = flow_fmq_credit_settlement_records_size(&owner->records);
  flow_fmq_credit_settlement_record_t *record =
      flow_fmq_credit_settlement_records_at(&owner->records, index);
  uint64_t request_id;
  uint64_t claim_token;
  if (!record || index >= count) return TURBO_EINVAL;
  request_id = record->request_id;
  claim_token = record->claim_token;
  if (index + 1u < count) {
    const flow_fmq_credit_settlement_record_t *last =
        flow_fmq_credit_settlement_records_at_const(&owner->records, count - 1u);
    if (!last ||
        flow_fmq_credit_settlement_index_put(&owner->request_index, last->request_id, index) !=
            TURBO_OK ||
        flow_fmq_credit_settlement_index_put(&owner->claim_index, last->claim_token, index) !=
            TURBO_OK) {
      return TURBO_EPROTO;
    }
    if (!flow_fmq_credit_settlement_index_remove(&owner->request_index, request_id, NULL) ||
        !flow_fmq_credit_settlement_index_remove(&owner->claim_index, claim_token, NULL)) {
      return TURBO_EPROTO;
    }
    *record = *last;
  } else if (!flow_fmq_credit_settlement_index_remove(&owner->request_index, request_id, NULL) ||
             !flow_fmq_credit_settlement_index_remove(&owner->claim_index, claim_token, NULL)) {
    return TURBO_EPROTO;
  }
  return turbo_vec_resize(&owner->records.raw, count - 1u) == TURBO_OK ? TURBO_OK : TURBO_EPROTO;
}

static int flow_fmq_credit_settlement_add(turbo_flow_fmq_credit_settlement_t *owner,
                                          uint64_t request_id, uint64_t claim_token,
                                          size_t *stored_index) {
  flow_fmq_credit_settlement_record_t record;
  size_t index;
  if (flow_fmq_credit_settlement_index_get_const(&owner->request_index, request_id) ||
      flow_fmq_credit_settlement_index_get_const(&owner->claim_index, claim_token)) {
    return TURBO_EALREADY;
  }
  if (flow_fmq_credit_settlement_records_size(&owner->records) >= owner->capacity)
    return TURBO_ENOSPC;
  memset(&record, 0, sizeof(record));
  record.request_id = request_id;
  record.claim_token = claim_token;
  record.state = FLOW_FMQ_CREDIT_SETTLEMENT_DISPATCHED;
  record.completion =
      (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
  if (flow_fmq_credit_settlement_records_push(&owner->records, record) != TURBO_OK)
    return TURBO_ENOMEM;
  index = flow_fmq_credit_settlement_records_size(&owner->records) - 1u;
  if (flow_fmq_credit_settlement_index_put(&owner->request_index, request_id, index) != TURBO_OK) {
    (void)turbo_vec_resize(&owner->records.raw, index);
    return TURBO_ENOMEM;
  }
  if (flow_fmq_credit_settlement_index_put(&owner->claim_index, claim_token, index) != TURBO_OK) {
    if (!flow_fmq_credit_settlement_index_remove(&owner->request_index, request_id, NULL))
      return TURBO_EPROTO;
    (void)turbo_vec_resize(&owner->records.raw, index);
    return TURBO_ENOMEM;
  }
  *stored_index = index;
  return TURBO_OK;
}

static int flow_fmq_credit_settlement_apply(turbo_flow_fmq_credit_settlement_t *owner, size_t index,
                                            turbo_flow_fmq_credit_settlement_result_t *result) {
  flow_fmq_credit_settlement_record_t *record =
      flow_fmq_credit_settlement_records_at(&owner->records, index);
  turbo_flow_fmq_credit_settlement_result_t settled = TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
  turbo_flow_claim_settle_fn settle;
  uint64_t *counter;
  int rc;
  if (!record || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  settled.request_id = record->request_id;
  settled.claim_token = record->claim_token;
  switch (record->state) {
  case FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_ACK:
    settled.action = TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_ACK;
    rc = turbo_flow_fmq_broker_completion_ack(&record->completion, &settled.completion_ack);
    if (rc != TURBO_OK) return TURBO_EPROTO;
    settle = owner->settler.ack;
    counter = &owner->settled_acks;
    break;
  case FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_REQUEUE:
    settled.action = TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_REQUEUE;
    settle = owner->settler.requeue;
    counter = &owner->settled_requeues;
    break;
  case FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_DROP:
    settled.action = TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_DROP;
    settle = owner->settler.drop;
    counter = &owner->settled_drops;
    break;
  default:
    return TURBO_EBUSY;
  }
  rc = settle(owner->settler.ctx, record->claim_token);
  if (rc != TURBO_OK) {
    owner->settlement_failures += 1u;
    return rc;
  }
  if (flow_fmq_credit_settlement_remove(owner, index) != TURBO_OK) return TURBO_EPROTO;
  *counter += 1u;
  *result = settled;
  return TURBO_OK;
}

turbo_flow_fmq_credit_settlement_t *
turbo_flow_fmq_credit_settlement_create(turbo_flow_fmq_credit_worker_t *credit_owner,
                                        const turbo_flow_claim_settler_t *settler,
                                        const turbo_flow_fmq_credit_settlement_config_t *config) {
  turbo_flow_fmq_credit_settlement_t *owner;
  if (!credit_owner || !settler || settler->size < sizeof(*settler) || !settler->ack ||
      !settler->requeue || !settler->drop || !config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_API_VERSION || config->capacity == 0u ||
      config->capacity > TURBO_FLOW_FMQ_BROKER_MAX_INFLIGHT) {
    return NULL;
  }
  owner = (turbo_flow_fmq_credit_settlement_t *)calloc(1, sizeof(*owner));
  if (!owner) return NULL;
  if (flow_fmq_credit_settlement_records_init(&owner->records) != TURBO_OK ||
      flow_fmq_credit_settlement_index_init(&owner->request_index) != TURBO_OK ||
      flow_fmq_credit_settlement_index_init(&owner->claim_index) != TURBO_OK ||
      flow_fmq_credit_settlement_records_reserve(&owner->records, config->capacity) != TURBO_OK ||
      turbo_hash_map_reserve(&owner->request_index.raw, config->capacity) != TURBO_OK ||
      turbo_hash_map_reserve(&owner->claim_index.raw, config->capacity) != TURBO_OK) {
    flow_fmq_credit_settlement_records_destroy(&owner->records);
    flow_fmq_credit_settlement_index_destroy(&owner->request_index);
    flow_fmq_credit_settlement_index_destroy(&owner->claim_index);
    free(owner);
    return NULL;
  }
  owner->credit_owner = credit_owner;
  owner->settler = *settler;
  owner->settler.size = sizeof(owner->settler);
  owner->capacity = config->capacity;
  return owner;
}

int turbo_flow_fmq_credit_settlement_destroy(turbo_flow_fmq_credit_settlement_t *owner) {
  if (!owner) return TURBO_OK;
  if (flow_fmq_credit_settlement_records_size(&owner->records) != 0u) return TURBO_EBUSY;
  flow_fmq_credit_settlement_records_destroy(&owner->records);
  flow_fmq_credit_settlement_index_destroy(&owner->request_index);
  flow_fmq_credit_settlement_index_destroy(&owner->claim_index);
  free(owner);
  return TURBO_OK;
}

int turbo_flow_fmq_credit_settlement_dispatch(turbo_flow_fmq_credit_settlement_t *owner,
                                              uint64_t claim_token, const char *service,
                                              uint64_t request_id, size_t encoded_job_bytes,
                                              const turbo_flow_protocol_route_t *client_route,
                                              uint64_t now_ms,
                                              turbo_flow_fmq_broker_dispatch_result_t *dispatch) {
  size_t index;
  int rc;
  if (!owner || claim_token == 0u) return TURBO_EINVAL;
  rc = flow_fmq_credit_settlement_add(owner, request_id, claim_token, &index);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_fmq_credit_worker_dispatch(owner->credit_owner, service, request_id,
                                             encoded_job_bytes, client_route, now_ms, dispatch);
  if (rc != TURBO_OK) {
    if (flow_fmq_credit_settlement_remove(owner, index) != TURBO_OK) return TURBO_EPROTO;
  }
  return rc;
}

int turbo_flow_fmq_credit_settlement_complete(turbo_flow_fmq_credit_settlement_t *owner,
                                              const char *worker_id,
                                              const turbo_flow_protocol_route_t *worker_route,
                                              uint64_t request_id,
                                              turbo_flow_fmq_credit_settlement_result_t *result) {
  const size_t *found;
  flow_fmq_credit_settlement_record_t *record;
  turbo_flow_fmq_broker_completion_result_t completion =
      TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
  int rc;
  if (!owner || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  found = flow_fmq_credit_settlement_index_get_const(&owner->request_index, request_id);
  if (!found) return TURBO_ENOENT;
  record = flow_fmq_credit_settlement_records_at(&owner->records, *found);
  if (!record || record->state != FLOW_FMQ_CREDIT_SETTLEMENT_DISPATCHED) return TURBO_EALREADY;
  rc = turbo_flow_fmq_credit_worker_complete(owner->credit_owner, worker_id, worker_route,
                                             request_id, &completion);
  if (rc != TURBO_OK) return rc;
  record->completion = completion;
  record->state = FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_ACK;
  return flow_fmq_credit_settlement_apply(owner, *found, result);
}

int turbo_flow_fmq_credit_settlement_cancel(turbo_flow_fmq_credit_settlement_t *owner,
                                            uint64_t request_id,
                                            turbo_flow_fmq_credit_settlement_action_t action,
                                            turbo_flow_fmq_credit_settlement_result_t *result) {
  const size_t *found;
  flow_fmq_credit_settlement_record_t *record;
  turbo_flow_fmq_broker_completion_result_t completion =
      TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
  int rc;
  if (!owner || !result || result->size < sizeof(*result) ||
      (action != TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_REQUEUE &&
       action != TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_DROP)) {
    return TURBO_EINVAL;
  }
  found = flow_fmq_credit_settlement_index_get_const(&owner->request_index, request_id);
  if (!found) return TURBO_ENOENT;
  record = flow_fmq_credit_settlement_records_at(&owner->records, *found);
  if (!record || record->state != FLOW_FMQ_CREDIT_SETTLEMENT_DISPATCHED) return TURBO_EALREADY;
  rc = turbo_flow_fmq_credit_worker_cancel(owner->credit_owner, request_id, &completion);
  if (rc != TURBO_OK) return rc;
  record->state = action == TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_REQUEUE
                      ? FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_REQUEUE
                      : FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_DROP;
  return flow_fmq_credit_settlement_apply(owner, *found, result);
}

int turbo_flow_fmq_credit_settlement_expire(turbo_flow_fmq_credit_settlement_t *owner,
                                            uint64_t now_ms,
                                            turbo_flow_fmq_broker_expire_result_t *expired,
                                            turbo_flow_fmq_credit_settlement_result_t *result) {
  turbo_flow_fmq_broker_expire_result_t local_expired = TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;
  turbo_flow_fmq_credit_settlement_result_t local_result =
      TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
  const size_t *found;
  flow_fmq_credit_settlement_record_t *record;
  int rc;
  if (!owner || !expired || expired->size < sizeof(*expired) || !result ||
      result->size < sizeof(*result)) {
    return TURBO_EINVAL;
  }
  rc = turbo_flow_fmq_credit_worker_expire(owner->credit_owner, now_ms, &local_expired);
  if (rc != TURBO_OK) return rc;
  *expired = local_expired;
  if (local_expired.disposition == TURBO_FLOW_FMQ_BROKER_EXPIRED_IDLE) {
    *result = local_result;
    return TURBO_OK;
  }
  found =
      flow_fmq_credit_settlement_index_get_const(&owner->request_index, local_expired.request_id);
  if (!found) return TURBO_EPROTO;
  record = flow_fmq_credit_settlement_records_at(&owner->records, *found);
  if (!record || record->state != FLOW_FMQ_CREDIT_SETTLEMENT_DISPATCHED) return TURBO_EPROTO;
  record->state = local_expired.disposition == TURBO_FLOW_FMQ_BROKER_EXPIRED_REQUEUE
                      ? FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_REQUEUE
                      : FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_DROP;
  return flow_fmq_credit_settlement_apply(owner, *found, result);
}

int turbo_flow_fmq_credit_settlement_retry_one(turbo_flow_fmq_credit_settlement_t *owner,
                                               turbo_flow_fmq_credit_settlement_result_t *result) {
  if (!owner || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  for (size_t i = 0u; i < flow_fmq_credit_settlement_records_size(&owner->records); ++i) {
    const flow_fmq_credit_settlement_record_t *record =
        flow_fmq_credit_settlement_records_at_const(&owner->records, i);
    if (record && record->state != FLOW_FMQ_CREDIT_SETTLEMENT_DISPATCHED)
      return flow_fmq_credit_settlement_apply(owner, i, result);
  }
  return TURBO_ENOENT;
}

int turbo_flow_fmq_credit_settlement_snapshot(const turbo_flow_fmq_credit_settlement_t *owner,
                                              turbo_flow_fmq_credit_settlement_snapshot_t *out) {
  if (!owner || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  memset(out, 0, sizeof(*out));
  out->size = sizeof(*out);
  out->tracked = flow_fmq_credit_settlement_records_size(&owner->records);
  out->settled_acks = owner->settled_acks;
  out->settled_requeues = owner->settled_requeues;
  out->settled_drops = owner->settled_drops;
  out->settlement_failures = owner->settlement_failures;
  for (size_t i = 0u; i < flow_fmq_credit_settlement_records_size(&owner->records); ++i) {
    const flow_fmq_credit_settlement_record_t *record =
        flow_fmq_credit_settlement_records_at_const(&owner->records, i);
    if (!record) continue;
    switch (record->state) {
    case FLOW_FMQ_CREDIT_SETTLEMENT_DISPATCHED:
      out->dispatched += 1u;
      break;
    case FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_ACK:
      out->pending_ack += 1u;
      break;
    case FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_REQUEUE:
      out->pending_requeue += 1u;
      break;
    case FLOW_FMQ_CREDIT_SETTLEMENT_PENDING_DROP:
      out->pending_drop += 1u;
      break;
    default:
      return TURBO_EPROTO;
    }
  }
  return TURBO_OK;
}

typedef enum flow_fmq_credit_durable_state_e {
  FLOW_FMQ_CREDIT_DURABLE_PENDING = 1,
  FLOW_FMQ_CREDIT_DURABLE_INFLIGHT,
  FLOW_FMQ_CREDIT_DURABLE_PENDING_ACK,
  FLOW_FMQ_CREDIT_DURABLE_PENDING_REQUEUE,
  FLOW_FMQ_CREDIT_DURABLE_PENDING_DROP,
  FLOW_FMQ_CREDIT_DURABLE_COMPLETION_OUTBOX,
  FLOW_FMQ_CREDIT_DURABLE_COMPLETED,
  FLOW_FMQ_CREDIT_DURABLE_POISONED
} flow_fmq_credit_durable_state_t;

typedef struct flow_fmq_credit_durable_key_s {
  char origin_broker_id[TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX + 1u];
  char client_id[TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX + 1u];
  uint64_t request_id;
} flow_fmq_credit_durable_key_t;

typedef struct flow_fmq_credit_durable_record_s {
  turbo_flow_fmq_broker_logical_address_t address;
  uint64_t runtime_request_id;
  uint64_t claim_token;
  uint64_t updated_at_ms;
  uint32_t attempts;
  flow_fmq_credit_durable_state_t state;
  int expire_pending;
  turbo_flow_fmq_broker_completion_result_t completion;
} flow_fmq_credit_durable_record_t;

TURBO_VEC_DEFINE(flow_fmq_credit_durable_records, flow_fmq_credit_durable_record_t)
TURBO_HASH_MAP_DEFINE(flow_fmq_credit_durable_address_index, flow_fmq_credit_durable_key_t, size_t)
TURBO_HASH_MAP_DEFINE(flow_fmq_credit_durable_runtime_index, uint64_t, size_t)

struct turbo_flow_fmq_credit_durable_s {
  turbo_flow_fmq_credit_worker_t *credit_owner;
  turbo_flow_claim_settler_t settler;
  flow_fmq_credit_durable_records records;
  flow_fmq_credit_durable_address_index address_index;
  flow_fmq_credit_durable_runtime_index runtime_index;
  size_t capacity;
  uint32_t max_attempts;
  uint64_t terminal_ttl_ms;
  char state_key[TURBO_FLOW_FMQ_CREDIT_DURABLE_STATE_KEY_MAX + 1u];
  uint8_t *state_buffer;
  size_t state_capacity;
  turbo_flow_fmq_credit_shutdown_policy_t shutdown_policy;
  size_t shutdown_max_steps;
  int quiesced;
  uint64_t retries;
  uint64_t duplicate_dispatches;
  uint64_t settlement_failures;
};

#define FLOW_FMQ_CREDIT_DURABLE_HEADER_SIZE 8u
#define FLOW_FMQ_CREDIT_DURABLE_METADATA_SIZE 13u
#define FLOW_FMQ_CREDIT_DURABLE_RECORD_BUFFER_SIZE                                                 \
  (TURBO_FLOW_FMQ_BROKER_ENVELOPE_MAX_SIZE + FLOW_FMQ_CREDIT_DURABLE_METADATA_SIZE + 32u)

static flow_fmq_credit_durable_key_t
flow_fmq_credit_durable_key(const turbo_flow_fmq_broker_logical_address_t *address) {
  flow_fmq_credit_durable_key_t key;
  memset(&key, 0, sizeof(key));
  if (address) {
    memcpy(key.origin_broker_id, address->origin_broker_id, strlen(address->origin_broker_id));
    memcpy(key.client_id, address->client_id, strlen(address->client_id));
    key.request_id = address->request_id;
  }
  return key;
}

static void flow_fmq_credit_durable_write_u32(uint8_t *data, uint32_t value) {
  data[0] = (uint8_t)(value >> 24u);
  data[1] = (uint8_t)(value >> 16u);
  data[2] = (uint8_t)(value >> 8u);
  data[3] = (uint8_t)value;
}

static void flow_fmq_credit_durable_write_u64(uint8_t *data, uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    data[7u - i] = (uint8_t)(value >> (i * 8u));
}

static uint32_t flow_fmq_credit_durable_read_u32(const uint8_t *data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) | ((uint32_t)data[2] << 8u) |
         data[3];
}

static uint64_t flow_fmq_credit_durable_read_u64(const uint8_t *data) {
  uint64_t value = 0u;
  for (size_t i = 0u; i < 8u; ++i)
    value = (value << 8u) | data[i];
  return value;
}

static int flow_fmq_credit_durable_append(uint8_t *out, size_t capacity, size_t *length,
                                          uint8_t type, const uint8_t *value, size_t value_size) {
  size_t wire_size;
  size_t written;
  if (!out || !length || type == 0u || (!value && value_size > 0u) || *length > capacity)
    return TURBO_EINVAL;
  wire_size = turbo_ltv_wire_size(value_size);
  if (wire_size == 0u || wire_size > capacity - *length) return TURBO_ENOSPC;
  written = turbo_ltv_build(type, value, value_size, out + *length, capacity - *length);
  if (written != wire_size) return TURBO_EPROTO;
  *length += written;
  return TURBO_OK;
}

static int flow_fmq_credit_durable_record_encode(const flow_fmq_credit_durable_record_t *record,
                                                 flow_fmq_credit_durable_state_t state,
                                                 uint8_t *out, size_t capacity, size_t *out_size) {
  uint8_t address[TURBO_FLOW_FMQ_BROKER_ENVELOPE_MAX_SIZE];
  uint8_t metadata[FLOW_FMQ_CREDIT_DURABLE_METADATA_SIZE];
  size_t address_size = 0u;
  size_t length = 0u;
  int rc;
  if (!record || !out || !out_size) return TURBO_EINVAL;
  rc = turbo_flow_fmq_broker_logical_address_encode(&record->address, address, sizeof(address),
                                                    &address_size);
  if (rc != TURBO_OK) return rc;
  metadata[0] = (uint8_t)state;
  flow_fmq_credit_durable_write_u32(metadata + 1u, record->attempts);
  flow_fmq_credit_durable_write_u64(metadata + 5u, record->updated_at_ms);
  rc = flow_fmq_credit_durable_append(out, capacity, &length, 1u, address, address_size);
  if (rc == TURBO_OK)
    rc = flow_fmq_credit_durable_append(out, capacity, &length, 2u, metadata, sizeof(metadata));
  if (rc == TURBO_OK) *out_size = length;
  return rc;
}

static int flow_fmq_credit_durable_encode(const turbo_flow_fmq_credit_durable_t *owner,
                                          size_t override_index,
                                          flow_fmq_credit_durable_state_t override_state,
                                          size_t *out_size) {
  static const uint8_t header[FLOW_FMQ_CREDIT_DURABLE_HEADER_SIZE] = {
      'T', 'F',
      'C', 'S',
      0u,  TURBO_FLOW_FMQ_CREDIT_DURABLE_SCHEMA_MAJOR,
      0u,  TURBO_FLOW_FMQ_CREDIT_DURABLE_SCHEMA_MINOR};
  uint8_t record_buffer[FLOW_FMQ_CREDIT_DURABLE_RECORD_BUFFER_SIZE];
  size_t length = 0u;
  int rc;
  if (!owner || !out_size) return TURBO_EINVAL;
  rc = flow_fmq_credit_durable_append(owner->state_buffer, owner->state_capacity, &length, 1u,
                                      header, sizeof(header));
  for (size_t i = 0u; rc == TURBO_OK && i < flow_fmq_credit_durable_records_size(&owner->records);
       ++i) {
    const flow_fmq_credit_durable_record_t *record =
        flow_fmq_credit_durable_records_at_const(&owner->records, i);
    flow_fmq_credit_durable_state_t state = i == override_index ? override_state : record->state;
    size_t record_size = 0u;
    if (!record) return TURBO_EPROTO;
    if (record->expire_pending || (i == override_index && override_state == 0)) continue;
    rc = flow_fmq_credit_durable_record_encode(record, state, record_buffer, sizeof(record_buffer),
                                               &record_size);
    if (rc == TURBO_OK)
      rc = flow_fmq_credit_durable_append(owner->state_buffer, owner->state_capacity, &length, 2u,
                                          record_buffer, record_size);
  }
  if (rc == TURBO_OK) *out_size = length;
  return rc;
}

static int flow_fmq_credit_durable_parse_record(const uint8_t *data, size_t data_size,
                                                flow_fmq_credit_durable_record_t *out) {
  const uint8_t *values[3] = {0};
  size_t sizes[3] = {0};
  size_t offset = 0u;
  uint8_t previous_type = 0u;
  flow_fmq_credit_durable_record_t record;
  if (!data || data_size == 0u || !out) return TURBO_EINVAL;
  memset(&record, 0, sizeof(record));
  record.address =
      (turbo_flow_fmq_broker_logical_address_t)TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
  record.completion =
      (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
  while (offset < data_size) {
    turbo_ltv_message_t *message = NULL;
    uint32_t payload_size = 0u;
    size_t header_size = 0u;
    size_t wire_size;
    uint8_t type;
    int rc = turbo_ltv_peek_size(data + offset, data_size - offset, &payload_size, &header_size);
    if (rc != TURBO_OK || payload_size == 0u || header_size > data_size - offset ||
        payload_size > data_size - offset - header_size)
      return TURBO_EPROTO;
    wire_size = header_size + payload_size;
    if (turbo_parse_ltv(data + offset, wire_size, &message) != TURBO_OK || !message) {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    type = turbo_ltv_type(message);
    if (type == 0u || type > 2u || type <= previous_type) {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    values[type] = data + offset + header_size + 1u;
    sizes[type] = turbo_ltv_value_len(message);
    if (sizes[type] != (size_t)payload_size - 1u) {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    previous_type = type;
    turbo_free_ltv(&message);
    offset += wire_size;
  }
  if (!values[1] || !values[2] || sizes[2] != FLOW_FMQ_CREDIT_DURABLE_METADATA_SIZE ||
      turbo_flow_fmq_broker_logical_address_decode(values[1], sizes[1], &record.address) !=
          TURBO_OK)
    return TURBO_EPROTO;
  record.state = (flow_fmq_credit_durable_state_t)values[2][0];
  record.attempts = flow_fmq_credit_durable_read_u32(values[2] + 1u);
  record.updated_at_ms = flow_fmq_credit_durable_read_u64(values[2] + 5u);
  if (record.state < FLOW_FMQ_CREDIT_DURABLE_PENDING ||
      record.state > FLOW_FMQ_CREDIT_DURABLE_POISONED)
    return TURBO_EPROTO;
  *out = record;
  return TURBO_OK;
}

static int flow_fmq_credit_durable_add_loaded(turbo_flow_fmq_credit_durable_t *owner,
                                              const flow_fmq_credit_durable_record_t *record) {
  flow_fmq_credit_durable_key_t key = flow_fmq_credit_durable_key(&record->address);
  size_t index;
  if (flow_fmq_credit_durable_records_size(&owner->records) >= owner->capacity) return TURBO_ENOSPC;
  if (flow_fmq_credit_durable_address_index_get_const(&owner->address_index, key))
    return TURBO_EPROTO;
  if (flow_fmq_credit_durable_records_push(&owner->records, *record) != TURBO_OK)
    return TURBO_ENOMEM;
  index = flow_fmq_credit_durable_records_size(&owner->records) - 1u;
  if (flow_fmq_credit_durable_address_index_put(&owner->address_index, key, index) != TURBO_OK) {
    (void)turbo_vec_resize(&owner->records.raw, index);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static int flow_fmq_credit_durable_load(turbo_flow_fmq_credit_durable_t *owner) {
  size_t loaded_size = 0u;
  size_t offset = 0u;
  int header_seen = 0;
  int dirty = 0;
  int rc = owner->settler.load_state(owner->settler.ctx, owner->state_key, owner->state_buffer,
                                     owner->state_capacity, &loaded_size);
  if (rc == TURBO_ENOENT) return TURBO_OK;
  if (rc != TURBO_OK) return rc;
  if (loaded_size == 0u) return TURBO_EPROTO;
  while (offset < loaded_size) {
    turbo_ltv_message_t *message = NULL;
    uint32_t payload_size = 0u;
    size_t header_size = 0u;
    size_t wire_size;
    uint8_t type;
    const uint8_t *value;
    size_t value_size;
    rc = turbo_ltv_peek_size(owner->state_buffer + offset, loaded_size - offset, &payload_size,
                             &header_size);
    if (rc != TURBO_OK || payload_size == 0u || header_size > loaded_size - offset ||
        payload_size > loaded_size - offset - header_size)
      return TURBO_EPROTO;
    wire_size = header_size + payload_size;
    if (turbo_parse_ltv(owner->state_buffer + offset, wire_size, &message) != TURBO_OK ||
        !message) {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    type = turbo_ltv_type(message);
    value = turbo_ltv_value(message);
    value_size = turbo_ltv_value_len(message);
    if (value_size != (size_t)payload_size - 1u) {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    if (type == 1u) {
      if (header_seen || offset != 0u || value_size != FLOW_FMQ_CREDIT_DURABLE_HEADER_SIZE ||
          memcmp(value, "TFCS", 4u) != 0 || value[4] != 0u ||
          value[5] != TURBO_FLOW_FMQ_CREDIT_DURABLE_SCHEMA_MAJOR || value[6] != 0u ||
          value[7] != TURBO_FLOW_FMQ_CREDIT_DURABLE_SCHEMA_MINOR) {
        turbo_free_ltv(&message);
        return TURBO_EPROTO;
      }
      header_seen = 1;
    } else if (type == 2u) {
      flow_fmq_credit_durable_record_t record;
      if (!header_seen ||
          flow_fmq_credit_durable_parse_record(value, value_size, &record) != TURBO_OK) {
        turbo_free_ltv(&message);
        return TURBO_EPROTO;
      }
      if (record.attempts > owner->max_attempts) {
        turbo_free_ltv(&message);
        return TURBO_EPROTO;
      }
      if (record.state == FLOW_FMQ_CREDIT_DURABLE_INFLIGHT ||
          record.state == FLOW_FMQ_CREDIT_DURABLE_PENDING_ACK ||
          record.state == FLOW_FMQ_CREDIT_DURABLE_PENDING_REQUEUE ||
          record.state == FLOW_FMQ_CREDIT_DURABLE_PENDING_DROP) {
        record.state = FLOW_FMQ_CREDIT_DURABLE_PENDING;
        dirty = 1;
      }
      rc = flow_fmq_credit_durable_add_loaded(owner, &record);
      if (rc != TURBO_OK) {
        turbo_free_ltv(&message);
        return rc;
      }
    } else {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    turbo_free_ltv(&message);
    offset += wire_size;
  }
  if (!header_seen) return TURBO_EPROTO;
  if (dirty) {
    size_t state_size = 0u;
    rc = flow_fmq_credit_durable_encode(owner, SIZE_MAX, FLOW_FMQ_CREDIT_DURABLE_PENDING,
                                        &state_size);
    if (rc == TURBO_OK)
      rc = owner->settler.commit_state(owner->settler.ctx, 0u, TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY,
                                       owner->state_key, owner->state_buffer, state_size);
  }
  return rc;
}

static int flow_fmq_credit_durable_remove(turbo_flow_fmq_credit_durable_t *owner, size_t index) {
  size_t count = flow_fmq_credit_durable_records_size(&owner->records);
  flow_fmq_credit_durable_record_t *record =
      flow_fmq_credit_durable_records_at(&owner->records, index);
  flow_fmq_credit_durable_key_t removed_key;
  if (!record || index >= count) return TURBO_EINVAL;
  removed_key = flow_fmq_credit_durable_key(&record->address);
  if (!flow_fmq_credit_durable_address_index_remove(&owner->address_index, removed_key, NULL))
    return TURBO_EPROTO;
  if (record->runtime_request_id != 0u)
    (void)flow_fmq_credit_durable_runtime_index_remove(&owner->runtime_index,
                                                       record->runtime_request_id, NULL);
  if (index + 1u < count) {
    const flow_fmq_credit_durable_record_t *last =
        flow_fmq_credit_durable_records_at_const(&owner->records, count - 1u);
    flow_fmq_credit_durable_key_t last_key;
    if (!last) return TURBO_EPROTO;
    last_key = flow_fmq_credit_durable_key(&last->address);
    *record = *last;
    if (flow_fmq_credit_durable_address_index_put(&owner->address_index, last_key, index) !=
        TURBO_OK)
      return TURBO_EPROTO;
    if (record->runtime_request_id != 0u &&
        flow_fmq_credit_durable_runtime_index_put(&owner->runtime_index, record->runtime_request_id,
                                                  index) != TURBO_OK)
      return TURBO_EPROTO;
  }
  return turbo_vec_resize(&owner->records.raw, count - 1u) == TURBO_OK ? TURBO_OK : TURBO_EPROTO;
}

static int flow_fmq_credit_durable_commit(turbo_flow_fmq_credit_durable_t *owner, size_t index,
                                          flow_fmq_credit_durable_state_t target,
                                          turbo_flow_claim_commit_action_t action) {
  flow_fmq_credit_durable_record_t *record =
      flow_fmq_credit_durable_records_at(&owner->records, index);
  size_t state_size = 0u;
  uint64_t token;
  int rc;
  if (!record) return TURBO_EPROTO;
  token = action == TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY ? 0u : record->claim_token;
  rc = flow_fmq_credit_durable_encode(owner, index, target, &state_size);
  if (rc == TURBO_OK)
    rc = owner->settler.commit_state(owner->settler.ctx, token, action, owner->state_key,
                                     owner->state_buffer, state_size);
  if (rc != TURBO_OK) {
    owner->settlement_failures += 1u;
    return rc;
  }
  record->state = target;
  if (target != FLOW_FMQ_CREDIT_DURABLE_INFLIGHT) {
    if (record->runtime_request_id != 0u)
      (void)flow_fmq_credit_durable_runtime_index_remove(&owner->runtime_index,
                                                         record->runtime_request_id, NULL);
    record->runtime_request_id = 0u;
    record->claim_token = 0u;
  }
  return TURBO_OK;
}

static int flow_fmq_credit_durable_apply(turbo_flow_fmq_credit_durable_t *owner, size_t index,
                                         turbo_flow_fmq_credit_settlement_result_t *result);

int turbo_flow_fmq_credit_durable_create(turbo_flow_fmq_credit_worker_t *credit_owner,
                                         const turbo_flow_claim_settler_t *settler,
                                         const turbo_flow_fmq_credit_durable_config_t *config,
                                         turbo_flow_fmq_credit_durable_t **out) {
  turbo_flow_fmq_credit_durable_t *owner;
  size_t record_value_max;
  size_t record_wire_max;
  size_t required_state_size;
  size_t key_size;
  int rc;
  if (out) *out = NULL;
  if (!out || !credit_owner || !settler || settler->size < sizeof(*settler) ||
      !settler->load_state || !settler->commit_state || settler->max_state_size == 0u || !config ||
      config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_FMQ_CREDIT_DURABLE_API_VERSION || config->capacity == 0u ||
      config->capacity > TURBO_FLOW_FMQ_BROKER_MAX_INFLIGHT || config->max_attempts == 0u ||
      config->terminal_ttl_ms == 0u || !config->state_key || !config->state_key[0] ||
      (config->shutdown_policy != TURBO_FLOW_FMQ_CREDIT_SHUTDOWN_REQUEUE &&
       config->shutdown_policy != TURBO_FLOW_FMQ_CREDIT_SHUTDOWN_PRESERVE) ||
      config->shutdown_max_steps == 0u)
    return TURBO_EINVAL;
  key_size = strlen(config->state_key);
  if (key_size > TURBO_FLOW_FMQ_CREDIT_DURABLE_STATE_KEY_MAX) return TURBO_EMSGSIZE;
  record_value_max = turbo_ltv_wire_size(TURBO_FLOW_FMQ_BROKER_ENVELOPE_MAX_SIZE) +
                     turbo_ltv_wire_size(FLOW_FMQ_CREDIT_DURABLE_METADATA_SIZE);
  record_wire_max = turbo_ltv_wire_size(record_value_max);
  required_state_size = turbo_ltv_wire_size(FLOW_FMQ_CREDIT_DURABLE_HEADER_SIZE);
  if (record_value_max == 0u || record_wire_max == 0u || required_state_size == 0u ||
      config->capacity > (SIZE_MAX - required_state_size) / record_wire_max)
    return TURBO_ERANGE;
  required_state_size += config->capacity * record_wire_max;
  if (required_state_size > settler->max_state_size) return TURBO_ENOSPC;
  owner = (turbo_flow_fmq_credit_durable_t *)calloc(1, sizeof(*owner));
  if (!owner) return TURBO_ENOMEM;
  owner->state_buffer = (uint8_t *)malloc(settler->max_state_size);
  if (!owner->state_buffer) {
    free(owner);
    return TURBO_ENOMEM;
  }
  owner->credit_owner = credit_owner;
  owner->settler = *settler;
  owner->settler.size = sizeof(owner->settler);
  owner->capacity = config->capacity;
  owner->max_attempts = config->max_attempts;
  owner->terminal_ttl_ms = config->terminal_ttl_ms;
  owner->shutdown_policy = config->shutdown_policy;
  owner->shutdown_max_steps = config->shutdown_max_steps;
  owner->state_capacity = settler->max_state_size;
  memcpy(owner->state_key, config->state_key, key_size + 1u);
  rc = flow_fmq_credit_durable_records_init(&owner->records);
  if (rc == TURBO_OK) rc = flow_fmq_credit_durable_address_index_init(&owner->address_index);
  if (rc == TURBO_OK) rc = flow_fmq_credit_durable_runtime_index_init(&owner->runtime_index);
  if (rc == TURBO_OK)
    rc = flow_fmq_credit_durable_records_reserve(&owner->records, config->capacity);
  if (rc == TURBO_OK) rc = turbo_hash_map_reserve(&owner->address_index.raw, config->capacity);
  if (rc == TURBO_OK) rc = turbo_hash_map_reserve(&owner->runtime_index.raw, config->capacity);
  if (rc == TURBO_OK) rc = flow_fmq_credit_durable_load(owner);
  if (rc != TURBO_OK) {
    turbo_flow_fmq_credit_durable_destroy(owner);
    return rc;
  }
  *out = owner;
  return TURBO_OK;
}

void turbo_flow_fmq_credit_durable_destroy(turbo_flow_fmq_credit_durable_t *owner) {
  if (!owner) return;
  flow_fmq_credit_durable_records_destroy(&owner->records);
  flow_fmq_credit_durable_address_index_destroy(&owner->address_index);
  flow_fmq_credit_durable_runtime_index_destroy(&owner->runtime_index);
  free(owner->state_buffer);
  free(owner);
}

int turbo_flow_fmq_credit_durable_shutdown(turbo_flow_fmq_credit_durable_t *owner, uint64_t now_ms,
                                           turbo_flow_fmq_credit_durable_snapshot_t *out) {
  size_t steps = 0u;
  int rc = TURBO_OK;
  if (!owner || (out && out->size < sizeof(*out))) return TURBO_EINVAL;
  owner->quiesced = 1;
  if (owner->shutdown_policy == TURBO_FLOW_FMQ_CREDIT_SHUTDOWN_PRESERVE)
    return out ? turbo_flow_fmq_credit_durable_snapshot(owner, out) : TURBO_OK;
  while (steps < owner->shutdown_max_steps) {
    size_t pending = SIZE_MAX;
    size_t inflight = SIZE_MAX;
    turbo_flow_fmq_credit_settlement_result_t settled =
        TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
    for (size_t i = 0u; i < flow_fmq_credit_durable_records_size(&owner->records); ++i) {
      const flow_fmq_credit_durable_record_t *record =
          flow_fmq_credit_durable_records_at_const(&owner->records, i);
      if (!record) return TURBO_EPROTO;
      if (record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_ACK ||
          record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_REQUEUE ||
          record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_DROP) {
        pending = i;
        break;
      }
      if (inflight == SIZE_MAX && record->state == FLOW_FMQ_CREDIT_DURABLE_INFLIGHT) inflight = i;
    }
    if (pending != SIZE_MAX) {
      rc = flow_fmq_credit_durable_apply(owner, pending, &settled);
      if (rc != TURBO_OK) break;
      steps += 1u;
      continue;
    }
    if (inflight != SIZE_MAX) {
      flow_fmq_credit_durable_record_t *record =
          flow_fmq_credit_durable_records_at(&owner->records, inflight);
      turbo_flow_fmq_broker_completion_result_t canceled =
          TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
      rc = turbo_flow_fmq_credit_worker_cancel(owner->credit_owner, record->runtime_request_id,
                                               &canceled);
      if (rc != TURBO_OK) break;
      record->updated_at_ms = now_ms;
      record->state = record->attempts < owner->max_attempts
                          ? FLOW_FMQ_CREDIT_DURABLE_PENDING_REQUEUE
                          : FLOW_FMQ_CREDIT_DURABLE_PENDING_DROP;
      rc = flow_fmq_credit_durable_apply(owner, inflight, &settled);
      if (rc != TURBO_OK) break;
      steps += 1u;
      continue;
    }
    break;
  }
  if (rc == TURBO_OK) {
    for (size_t i = 0u; i < flow_fmq_credit_durable_records_size(&owner->records); ++i) {
      const flow_fmq_credit_durable_record_t *record =
          flow_fmq_credit_durable_records_at_const(&owner->records, i);
      if (record && (record->state == FLOW_FMQ_CREDIT_DURABLE_INFLIGHT ||
                     record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_ACK ||
                     record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_REQUEUE ||
                     record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_DROP)) {
        rc = TURBO_EBUSY;
        break;
      }
    }
  }
  if (out) {
    int snapshot_rc = turbo_flow_fmq_credit_durable_snapshot(owner, out);
    if (rc == TURBO_OK && snapshot_rc != TURBO_OK) rc = snapshot_rc;
  }
  return rc;
}

int turbo_flow_fmq_credit_durable_dispatch(
    turbo_flow_fmq_credit_durable_t *owner, uint64_t claim_token,
    const turbo_flow_fmq_broker_logical_address_t *address, uint64_t runtime_request_id,
    const char *service, size_t encoded_job_bytes, const turbo_flow_protocol_route_t *client_route,
    uint64_t now_ms, turbo_flow_fmq_broker_dispatch_result_t *dispatch) {
  flow_fmq_credit_durable_record_t record;
  flow_fmq_credit_durable_record_t *stored;
  flow_fmq_credit_durable_key_t key;
  const size_t *found;
  size_t index;
  int added = 0;
  int rc;
  if (!owner || claim_token == 0u || runtime_request_id == 0u ||
      turbo_flow_fmq_broker_logical_address_validate(address) != TURBO_OK || !dispatch ||
      dispatch->size < sizeof(*dispatch))
    return TURBO_EINVAL;
  if (owner->quiesced) return TURBO_ESHUTDOWN;
  if (flow_fmq_credit_durable_runtime_index_get_const(&owner->runtime_index, runtime_request_id))
    return TURBO_EALREADY;
  key = flow_fmq_credit_durable_key(address);
  found = flow_fmq_credit_durable_address_index_get_const(&owner->address_index, key);
  if (found) {
    index = *found;
    stored = flow_fmq_credit_durable_records_at(&owner->records, index);
    if (!stored || stored->state != FLOW_FMQ_CREDIT_DURABLE_PENDING) {
      owner->duplicate_dispatches += 1u;
      return TURBO_EALREADY;
    }
    if (stored->attempts >= owner->max_attempts) {
      stored->claim_token = claim_token;
      stored->updated_at_ms = now_ms;
      stored->state = FLOW_FMQ_CREDIT_DURABLE_PENDING_DROP;
      rc = flow_fmq_credit_durable_commit(owner, index, FLOW_FMQ_CREDIT_DURABLE_POISONED,
                                          TURBO_FLOW_CLAIM_COMMIT_DROP);
      return rc == TURBO_OK ? TURBO_EALREADY : rc;
    }
  } else {
    memset(&record, 0, sizeof(record));
    record.address = *address;
    record.address.size = sizeof(record.address);
    record.completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    record.state = FLOW_FMQ_CREDIT_DURABLE_PENDING;
    rc = flow_fmq_credit_durable_add_loaded(owner, &record);
    if (rc != TURBO_OK) return rc;
    index = flow_fmq_credit_durable_records_size(&owner->records) - 1u;
    stored = flow_fmq_credit_durable_records_at(&owner->records, index);
    added = 1;
  }
  rc = turbo_flow_fmq_credit_worker_dispatch(owner->credit_owner, service, runtime_request_id,
                                             encoded_job_bytes, client_route, now_ms, dispatch);
  if (rc != TURBO_OK) {
    if (added && flow_fmq_credit_durable_remove(owner, index) != TURBO_OK) return TURBO_EPROTO;
    return rc;
  }
  stored->runtime_request_id = runtime_request_id;
  stored->claim_token = claim_token;
  stored->attempts += 1u;
  stored->updated_at_ms = now_ms;
  stored->state = FLOW_FMQ_CREDIT_DURABLE_INFLIGHT;
  if (flow_fmq_credit_durable_runtime_index_put(&owner->runtime_index, runtime_request_id, index) !=
      TURBO_OK) {
    turbo_flow_fmq_broker_completion_result_t canceled =
        TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    (void)turbo_flow_fmq_credit_worker_cancel(owner->credit_owner, runtime_request_id, &canceled);
    stored->state = FLOW_FMQ_CREDIT_DURABLE_PENDING;
    stored->runtime_request_id = 0u;
    stored->claim_token = 0u;
    if (added) (void)flow_fmq_credit_durable_remove(owner, index);
    return TURBO_ENOMEM;
  }
  rc = flow_fmq_credit_durable_commit(owner, index, FLOW_FMQ_CREDIT_DURABLE_INFLIGHT,
                                      TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY);
  if (rc != TURBO_OK) {
    turbo_flow_fmq_broker_completion_result_t canceled =
        TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    (void)turbo_flow_fmq_credit_worker_cancel(owner->credit_owner, runtime_request_id, &canceled);
    (void)flow_fmq_credit_durable_runtime_index_remove(&owner->runtime_index, runtime_request_id,
                                                       NULL);
    stored->state = FLOW_FMQ_CREDIT_DURABLE_PENDING;
    stored->runtime_request_id = 0u;
    stored->claim_token = 0u;
    if (stored->attempts > 0u) stored->attempts -= 1u;
    {
      size_t corrected_size = 0u;
      if (flow_fmq_credit_durable_encode(owner, SIZE_MAX, FLOW_FMQ_CREDIT_DURABLE_PENDING,
                                         &corrected_size) == TURBO_OK)
        (void)owner->settler.commit_state(owner->settler.ctx, 0u,
                                          TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY, owner->state_key,
                                          owner->state_buffer, corrected_size);
    }
    return rc;
  }
  if (stored->attempts > 1u) owner->retries += 1u;
  return TURBO_OK;
}

static int flow_fmq_credit_durable_apply(turbo_flow_fmq_credit_durable_t *owner, size_t index,
                                         turbo_flow_fmq_credit_settlement_result_t *result) {
  flow_fmq_credit_durable_record_t *record =
      flow_fmq_credit_durable_records_at(&owner->records, index);
  turbo_flow_claim_commit_action_t action;
  flow_fmq_credit_durable_state_t target;
  turbo_flow_fmq_credit_settlement_result_t settled = TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
  int rc;
  if (!record || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  settled.request_id = record->runtime_request_id;
  settled.claim_token = record->claim_token;
  if (record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_ACK) {
    action = TURBO_FLOW_CLAIM_COMMIT_ACK;
    target = FLOW_FMQ_CREDIT_DURABLE_COMPLETION_OUTBOX;
    settled.action = TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_ACK;
    rc = turbo_flow_fmq_broker_completion_ack(&record->completion, &settled.completion_ack);
    if (rc != TURBO_OK) return TURBO_EPROTO;
  } else if (record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_REQUEUE) {
    action = TURBO_FLOW_CLAIM_COMMIT_REQUEUE;
    target = FLOW_FMQ_CREDIT_DURABLE_PENDING;
    settled.action = TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_REQUEUE;
  } else if (record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_DROP) {
    action = TURBO_FLOW_CLAIM_COMMIT_DROP;
    target = FLOW_FMQ_CREDIT_DURABLE_POISONED;
    settled.action = TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_DROP;
  } else {
    return TURBO_EBUSY;
  }
  rc = flow_fmq_credit_durable_commit(owner, index, target, action);
  if (rc != TURBO_OK) return rc;
  *result = settled;
  return TURBO_OK;
}

int turbo_flow_fmq_credit_durable_complete(turbo_flow_fmq_credit_durable_t *owner,
                                           const char *worker_id,
                                           const turbo_flow_protocol_route_t *worker_route,
                                           uint64_t runtime_request_id, uint64_t now_ms,
                                           turbo_flow_fmq_credit_settlement_result_t *result) {
  const size_t *found;
  flow_fmq_credit_durable_record_t *record;
  turbo_flow_fmq_broker_completion_result_t completion =
      TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
  int rc;
  if (!owner || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  found =
      flow_fmq_credit_durable_runtime_index_get_const(&owner->runtime_index, runtime_request_id);
  if (!found) return TURBO_ENOENT;
  record = flow_fmq_credit_durable_records_at(&owner->records, *found);
  if (!record || record->state != FLOW_FMQ_CREDIT_DURABLE_INFLIGHT) return TURBO_EALREADY;
  rc = turbo_flow_fmq_credit_worker_complete(owner->credit_owner, worker_id, worker_route,
                                             runtime_request_id, &completion);
  if (rc != TURBO_OK) return rc;
  record->completion = completion;
  record->updated_at_ms = now_ms;
  record->state = FLOW_FMQ_CREDIT_DURABLE_PENDING_ACK;
  return flow_fmq_credit_durable_apply(owner, *found, result);
}

int turbo_flow_fmq_credit_durable_expire(turbo_flow_fmq_credit_durable_t *owner, uint64_t now_ms,
                                         turbo_flow_fmq_broker_expire_result_t *expired,
                                         turbo_flow_fmq_credit_settlement_result_t *result) {
  turbo_flow_fmq_broker_expire_result_t local = TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;
  const size_t *found;
  flow_fmq_credit_durable_record_t *record;
  int rc;
  if (!owner || !expired || expired->size < sizeof(*expired) || !result ||
      result->size < sizeof(*result))
    return TURBO_EINVAL;
  rc = turbo_flow_fmq_credit_worker_expire(owner->credit_owner, now_ms, &local);
  if (rc != TURBO_OK) return rc;
  *expired = local;
  if (local.disposition == TURBO_FLOW_FMQ_BROKER_EXPIRED_IDLE) {
    *result =
        (turbo_flow_fmq_credit_settlement_result_t)TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
    return TURBO_OK;
  }
  found = flow_fmq_credit_durable_runtime_index_get_const(&owner->runtime_index, local.request_id);
  if (!found) return TURBO_EPROTO;
  record = flow_fmq_credit_durable_records_at(&owner->records, *found);
  if (!record || record->state != FLOW_FMQ_CREDIT_DURABLE_INFLIGHT) return TURBO_EPROTO;
  record->updated_at_ms = now_ms;
  record->state = local.disposition == TURBO_FLOW_FMQ_BROKER_EXPIRED_REQUEUE &&
                          record->attempts < owner->max_attempts
                      ? FLOW_FMQ_CREDIT_DURABLE_PENDING_REQUEUE
                      : FLOW_FMQ_CREDIT_DURABLE_PENDING_DROP;
  return flow_fmq_credit_durable_apply(owner, *found, result);
}

int turbo_flow_fmq_credit_durable_retry_one(turbo_flow_fmq_credit_durable_t *owner,
                                            turbo_flow_fmq_credit_settlement_result_t *result) {
  if (!owner || !result || result->size < sizeof(*result)) return TURBO_EINVAL;
  for (size_t i = 0u; i < flow_fmq_credit_durable_records_size(&owner->records); ++i) {
    const flow_fmq_credit_durable_record_t *record =
        flow_fmq_credit_durable_records_at_const(&owner->records, i);
    if (record && (record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_ACK ||
                   record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_REQUEUE ||
                   record->state == FLOW_FMQ_CREDIT_DURABLE_PENDING_DROP))
      return flow_fmq_credit_durable_apply(owner, i, result);
  }
  for (size_t i = 0u; i < flow_fmq_credit_durable_records_size(&owner->records); ++i) {
    flow_fmq_credit_durable_record_t *record =
        flow_fmq_credit_durable_records_at(&owner->records, i);
    size_t state_size = 0u;
    int rc;
    if (!record || !record->expire_pending) continue;
    rc = flow_fmq_credit_durable_encode(owner, i, 0, &state_size);
    if (rc == TURBO_OK)
      rc = owner->settler.commit_state(owner->settler.ctx, 0u, TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY,
                                       owner->state_key, owner->state_buffer, state_size);
    if (rc != TURBO_OK) {
      owner->settlement_failures += 1u;
      return rc;
    }
    if (flow_fmq_credit_durable_remove(owner, i) != TURBO_OK) return TURBO_EPROTO;
    *result =
        (turbo_flow_fmq_credit_settlement_result_t)TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
    return TURBO_OK;
  }
  return TURBO_ENOENT;
}

int turbo_flow_fmq_credit_durable_outbox_next(const turbo_flow_fmq_credit_durable_t *owner,
                                              turbo_flow_fmq_broker_logical_address_t *address) {
  if (!owner || !address || address->size < sizeof(*address)) return TURBO_EINVAL;
  for (size_t i = 0u; i < flow_fmq_credit_durable_records_size(&owner->records); ++i) {
    const flow_fmq_credit_durable_record_t *record =
        flow_fmq_credit_durable_records_at_const(&owner->records, i);
    if (record && record->state == FLOW_FMQ_CREDIT_DURABLE_COMPLETION_OUTBOX) {
      *address = record->address;
      return TURBO_OK;
    }
  }
  return TURBO_ENOENT;
}

int turbo_flow_fmq_credit_durable_outbox_confirm(
    turbo_flow_fmq_credit_durable_t *owner, const turbo_flow_fmq_broker_logical_address_t *address,
    uint64_t now_ms) {
  flow_fmq_credit_durable_key_t key;
  const size_t *found;
  flow_fmq_credit_durable_record_t *record;
  if (!owner || turbo_flow_fmq_broker_logical_address_validate(address) != TURBO_OK)
    return TURBO_EINVAL;
  key = flow_fmq_credit_durable_key(address);
  found = flow_fmq_credit_durable_address_index_get_const(&owner->address_index, key);
  if (!found) return TURBO_ENOENT;
  record = flow_fmq_credit_durable_records_at(&owner->records, *found);
  if (!record) return TURBO_EPROTO;
  if (record->state == FLOW_FMQ_CREDIT_DURABLE_COMPLETED) return TURBO_OK;
  if (record->state != FLOW_FMQ_CREDIT_DURABLE_COMPLETION_OUTBOX) return TURBO_EBUSY;
  record->updated_at_ms = now_ms;
  return flow_fmq_credit_durable_commit(owner, *found, FLOW_FMQ_CREDIT_DURABLE_COMPLETED,
                                        TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY);
}

int turbo_flow_fmq_credit_durable_expire_terminal(
    turbo_flow_fmq_credit_durable_t *owner, uint64_t now_ms,
    turbo_flow_fmq_broker_logical_address_t *address) {
  if (!owner || !address || address->size < sizeof(*address)) return TURBO_EINVAL;
  for (size_t i = 0u; i < flow_fmq_credit_durable_records_size(&owner->records); ++i) {
    flow_fmq_credit_durable_record_t *record =
        flow_fmq_credit_durable_records_at(&owner->records, i);
    size_t state_size = 0u;
    int rc;
    if (!record ||
        (record->state != FLOW_FMQ_CREDIT_DURABLE_COMPLETED &&
         record->state != FLOW_FMQ_CREDIT_DURABLE_POISONED) ||
        now_ms < record->updated_at_ms || now_ms - record->updated_at_ms < owner->terminal_ttl_ms)
      continue;
    record->expire_pending = 1;
    rc = flow_fmq_credit_durable_encode(owner, i, 0, &state_size);
    if (rc == TURBO_OK)
      rc = owner->settler.commit_state(owner->settler.ctx, 0u, TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY,
                                       owner->state_key, owner->state_buffer, state_size);
    if (rc != TURBO_OK) {
      owner->settlement_failures += 1u;
      return rc;
    }
    *address = record->address;
    return flow_fmq_credit_durable_remove(owner, i);
  }
  return TURBO_ENOENT;
}

int turbo_flow_fmq_credit_durable_snapshot(const turbo_flow_fmq_credit_durable_t *owner,
                                           turbo_flow_fmq_credit_durable_snapshot_t *out) {
  if (!owner || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  memset(out, 0, sizeof(*out));
  out->size = sizeof(*out);
  out->records = flow_fmq_credit_durable_records_size(&owner->records);
  out->retries = owner->retries;
  out->duplicate_dispatches = owner->duplicate_dispatches;
  out->settlement_failures = owner->settlement_failures;
  out->quiesced = owner->quiesced;
  for (size_t i = 0u; i < out->records; ++i) {
    const flow_fmq_credit_durable_record_t *record =
        flow_fmq_credit_durable_records_at_const(&owner->records, i);
    if (!record) return TURBO_EPROTO;
    switch (record->state) {
    case FLOW_FMQ_CREDIT_DURABLE_PENDING:
    case FLOW_FMQ_CREDIT_DURABLE_PENDING_REQUEUE:
      out->pending += 1u;
      break;
    case FLOW_FMQ_CREDIT_DURABLE_INFLIGHT:
    case FLOW_FMQ_CREDIT_DURABLE_PENDING_ACK:
    case FLOW_FMQ_CREDIT_DURABLE_PENDING_DROP:
      out->inflight += 1u;
      break;
    case FLOW_FMQ_CREDIT_DURABLE_COMPLETION_OUTBOX:
      out->completion_outbox += 1u;
      break;
    case FLOW_FMQ_CREDIT_DURABLE_COMPLETED:
      out->completed += 1u;
      break;
    case FLOW_FMQ_CREDIT_DURABLE_POISONED:
      out->poisoned += 1u;
      break;
    default:
      return TURBO_EPROTO;
    }
  }
  return TURBO_OK;
}
