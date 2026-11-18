#include "mdp_internal.h"

enum request_state { UNUSED, QUEUED, ACTIVE, REPLY };
enum { CACHE_RING_POINTS = 32 };
typedef struct broker_request {
  enum request_state state;
  mdp_route client;
  mdp_packet packet;
  uint64_t deadline;
  size_t worker_index;
  const flowmq_mdp_service_policy *policy;
  uint64_t key_hash;
} broker_request;

typedef struct broker_worker {
  size_t listener;
  mdp_route route;
  uint64_t session, last_seen;
  char service[FLOWMQ_MDP_SERVICE_MAX + 1];
  uint32_t slots;
  int registered;
  uint64_t busy[(FLOWMQ_MDP_CAPACITY_MAX + 63) / 64];
  uint64_t ring[CACHE_RING_POINTS];
  uint32_t occupied;
} broker_worker;

typedef struct worker_choice {
  size_t index, rotation;
  const broker_worker *peer;
  uint64_t distance;
} worker_choice;

typedef struct broker_listener {
  flowmq_mdp_worker_listener config;
  mdp_reader reader;
} broker_listener;

struct flowmq_mdp_broker {
  flowmq_socket_t *front;
  flowmq_mdp_broker_config config;
  mdp_reader clients;
  size_t listener_count, listener_cursor;
  broker_request *calls;
  broker_worker *peers;
  size_t request_cursor, worker_cursor;
  const flowmq_mdp_service_policy *policies;
  size_t policy_count;
  bool *paused;
  worker_choice *choices;
  bool started;
  int stopping;
  broker_request *rejections;
  broker_listener listeners[];
};

int flowmq_mdp_broker_open(flowmq_socket_t *front, flowmq_socket_t *back,
                           const flowmq_mdp_broker_config *cfg, flowmq_mdp_broker **out) {
  if (!cfg) return SALTS_EINVAL;
  const flowmq_mdp_worker_listener listener = {back, cfg->authorize, cfg->context};
  return flowmq_mdp_broker_open_listeners(front, &listener, 1, cfg, out);
}

int flowmq_mdp_broker_open_listeners(flowmq_socket_t *front,
                                     const flowmq_mdp_worker_listener *listeners, size_t count,
                                     const flowmq_mdp_broker_config *cfg, flowmq_mdp_broker **out) {
  if (!front || !listeners || !count || count > FLOWMQ_MDP_CAPACITY_MAX || !cfg || !out ||
      !cfg->incarnation || !cfg->requests || cfg->requests > FLOWMQ_MDP_CAPACITY_MAX ||
      !cfg->workers || cfg->workers > FLOWMQ_MDP_CAPACITY_MAX || !cfg->requests_per_client ||
      cfg->requests_per_client > cfg->requests || cfg->requests_per_service > cfg->requests ||
      !cfg->worker_lease_ms || !cfg->authorize)
    return SALTS_EINVAL;
  for (size_t i = 0; i < count; ++i) {
    if (!listeners[i].router || !listeners[i].authorize || listeners[i].router == front)
      return SALTS_EINVAL;
    for (size_t j = 0; j < i; ++j)
      if (listeners[j].router == listeners[i].router) return SALTS_EINVAL;
  }
  flowmq_mdp_broker *b = calloc(1, sizeof(*b) + count * sizeof(broker_listener));
  if (!b) return SALTS_ENOMEM;
  b->calls = calloc(cfg->requests, sizeof(*b->calls));
  b->rejections = calloc(cfg->requests, sizeof(*b->rejections));
  b->peers = calloc(cfg->workers, sizeof(*b->peers));
  if (!b->calls || !b->peers || !b->rejections) {
    flowmq_mdp_broker_close(b);
    return SALTS_ENOMEM;
  }
  b->front = front;
  b->listener_count = count;
  for (size_t i = 0; i < count; ++i)
    b->listeners[i].config = listeners[i];
  b->config = *cfg;
  if (!b->config.requests_per_service) b->config.requests_per_service = cfg->requests;
  *out = b;
  return SALTS_OK;
}
void flowmq_mdp_broker_close(flowmq_mdp_broker *b) {
  if (b) {
    free(b->peers);
    free(b->calls);
    free(b->rejections);
    free(b->paused);
    free(b->choices);
    free(b);
  }
}

int flowmq_mdp_broker_set_policies(flowmq_mdp_broker *b, const flowmq_mdp_service_policy *policies,
                                   size_t count) {
  if (!b || (count && !policies) || count > FLOWMQ_MDP_CAPACITY_MAX) return SALTS_EINVAL;
  if (b->started) return SALTS_EBUSY;
  bool ranked = false;
  for (size_t i = 0; i < count; ++i) {
    const flowmq_mdp_service_policy *p = &policies[i];
    if (!mdp_valid_service(p->service) || (unsigned)p->placement > FLOWMQ_MDP_EXCLUSIVE)
      return SALTS_EINVAL;
    if (p->placement == FLOWMQ_MDP_CACHE_RING || p->placement == FLOWMQ_MDP_LEAST_LOAD)
      ranked = true;
    if (p->placement == FLOWMQ_MDP_CACHE_RING) {
      if (!p->route_key || !p->max_load_percent || p->max_load_percent > 100) return SALTS_EINVAL;
    } else if (p->route_key || p->max_load_percent) return SALTS_EINVAL;
    for (size_t j = 0; j < i; ++j)
      if (!strcmp(p->service, policies[j].service)) return SALTS_EINVAL;
  }
  bool *paused = count ? calloc(count, sizeof(*paused)) : NULL;
  if (count && !paused) return SALTS_ENOMEM;
  worker_choice *choices = ranked ? calloc(b->config.workers, sizeof(*choices)) : NULL;
  if (ranked && !choices) {
    free(paused);
    return SALTS_ENOMEM;
  }
  free(b->paused);
  free(b->choices);
  b->choices = choices;
  b->policies = policies;
  b->policy_count = count;
  b->paused = paused;
  return SALTS_OK;
}

static const flowmq_mdp_service_policy *policy_for(const flowmq_mdp_broker *b,
                                                   const char *service) {
  for (size_t i = 0; i < b->policy_count; ++i)
    if (!strcmp(service, b->policies[i].service)) return &b->policies[i];
  return NULL;
}

int flowmq_mdp_broker_resume_service(flowmq_mdp_broker *b, const char *service) {
  if (!b || !mdp_valid_service(service)) return SALTS_EINVAL;
  const flowmq_mdp_service_policy *p = policy_for(b, service);
  if (!p || p->placement != FLOWMQ_MDP_EXCLUSIVE) return SALTS_EINVAL;
  if (!b->paused[p - b->policies]) return SALTS_EBUSY;
  b->paused[p - b->policies] = false;
  return SALTS_OK;
}

static void reply(broker_request *q, enum mdp_command command, int status) {
  q->state = REPLY;
  q->packet.command = command;
  q->packet.status = status;
  if (command != RESULT) q->packet.body = (flowmq_mdp_body){0};
}
static int flush_reply(flowmq_mdp_broker *b, broker_request *q, uint64_t now) {
  int rc = mdp_send(b->front, &q->client, &q->packet);
  if (mdp_busy(rc)) {
    if (now >= q->deadline) q->state = UNUSED;
    return SALTS_OK;
  }
  if (rc != SALTS_OK && rc != SALTS_ENOENT && rc != SALTS_ENOTCONN) return rc;
  q->state = UNUSED;
  return SALTS_OK;
}
static void retire(flowmq_mdp_broker *b, size_t index) {
  const flowmq_mdp_service_policy *p = policy_for(b, b->peers[index].service);
  if (p && p->placement == FLOWMQ_MDP_EXCLUSIVE) b->paused[p - b->policies] = true;
  for (size_t i = 0; i < b->config.requests; ++i) {
    broker_request *q = &b->calls[i];
    if (q->state == ACTIVE && q->worker_index == index) reply(q, UNKNOWN, SALTS_ENOTCONN);
  }
  memset(&b->peers[index], 0, sizeof(b->peers[index]));
}

static int admit(flowmq_mdp_broker *b, uint64_t now) {
  broker_request *reserve = NULL;
  for (size_t i = 0; i < b->config.requests; ++i) {
    broker_request *q = &b->rejections[i];
    if (q->state == REPLY) {
      int status = flush_reply(b, q, now);
      if (status != SALTS_OK) return status;
    }
    if (q->state == UNUSED) reserve = q;
  }
  if (!reserve) return SALTS_OK;
  mdp_packet p;
  mdp_route route;
  int rc = mdp_receive(b->front, &b->clients, 1, &route, &p);
  if (rc == SALTS_EBUSY || rc == SALTS_ENOTCONN) return SALTS_OK;
  if (rc != SALTS_OK) return rc;
  if (p.command == CANCEL) {
    for (size_t i = 0; i < b->config.requests; ++i) {
      broker_request *q = &b->calls[i];
      if (q->state == QUEUED && mdp_same_route(&route, &q->client) &&
          mdp_same_id(p.id, q->packet.id) && !strcmp(p.service, q->packet.service))
        q->state = UNUSED;
    }
    return SALTS_OK;
  }
  if (p.command != CALL) return SALTS_EPROTO;
  size_t client_count = 0, service_count = 0;
  broker_request *q = NULL;
  for (size_t i = 0; i < b->config.requests; ++i) {
    broker_request *old = &b->calls[i];
    if (old->state == UNUSED) {
      if (!q) q = old;
      continue;
    }
    if (!strcmp(old->packet.service, p.service)) ++service_count;
    if (mdp_same_route(&old->client, &route)) {
      ++client_count;
      /* A second copy of the same live call must never trigger a second
       * dispatch or issue a false pre-dispatch rejection. */
      if (mdp_same_id(old->packet.id, p.id)) return SALTS_OK;
    }
  }
  rc = SALTS_OK;
  if (!b->config.authorize(b->config.context, 0, route.bytes, route.size, p.service))
    rc = SALTS_EPERM;
  else if (b->stopping) rc = SALTS_ESHUTDOWN;
  else if (client_count >= b->config.requests_per_client ||
           service_count >= b->config.requests_per_service || !q)
    rc = SALTS_ENOBUFS;
  if (now > UINT64_MAX - p.budget) return SALTS_ERANGE;
  const flowmq_mdp_service_policy *policy = policy_for(b, p.service);
  uint64_t key_hash = 0;
  if (rc == SALTS_OK && policy) {
    if (b->paused[policy - b->policies]) rc = SALTS_ENOTCONN;
    else if (policy->route_key) rc = policy->route_key(policy->context, &p.body, &key_hash);
  }
  if (rc != SALTS_OK) q = reserve;
  q->policy = policy;
  q->key_hash = key_hash;
  q->client = route;
  q->packet = p;
  q->packet.broker = b->config.incarnation;
  q->deadline = now + p.budget;
  q->state = QUEUED;
  if (rc != SALTS_OK) reply(q, REJECT, rc);
  return SALTS_OK;
}

/* Stable byte-oriented peer identity; vnode positions use the SplitMix64
 * finalizer to spread adjacent point numbers around the full ring. */
static void build_ring_points(broker_worker *w) {
  uint64_t seed = UINT64_C(14695981039346656037) ^ w->listener;
  for (size_t i = 0; i < w->route.size; ++i)
    seed = (seed ^ w->route.bytes[i]) * UINT64_C(1099511628211);
  for (size_t i = 0; i < CACHE_RING_POINTS; ++i) {
    seed += UINT64_C(0x9e3779b97f4a7c15);
    uint64_t x = seed;
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    w->ring[i] = x ^ (x >> 31);
  }
}

static bool exclusive_available(const flowmq_mdp_broker *b,
                                const flowmq_mdp_service_policy *policy) {
  if (b->paused[policy - b->policies]) return false;
  for (size_t i = 0; i < b->config.workers; ++i)
    if (b->peers[i].session && !strcmp(policy->service, b->peers[i].service)) return false;
  return true;
}

static int worker_message(flowmq_mdp_broker *b, size_t ingress, uint64_t now) {
  broker_listener *listener = &b->listeners[ingress];
  mdp_packet p;
  mdp_route route;
  int rc = mdp_receive(listener->config.router, &listener->reader, 1, &route, &p);
  if (rc == SALTS_EBUSY || rc == SALTS_ENOTCONN) return SALTS_OK;
  if (rc != SALTS_OK) return rc;
  size_t capacity = b->config.workers, index = capacity, unused = capacity;
  for (size_t i = 0; i < capacity; ++i) {
    if (!b->peers[i].session) {
      if (unused == capacity) unused = i;
    } else if (b->peers[i].listener == ingress && mdp_same_route(&b->peers[i].route, &route)) {
      index = i;
      break;
    }
  }
  if (p.command == REGISTER) {
    int allowed =
        listener->config.authorize(listener->config.context, 1, route.bytes, route.size, p.service);
    const flowmq_mdp_service_policy *rule = policy_for(b, p.service);
    bool duplicate = index < capacity && b->peers[index].session == p.worker &&
                     !strcmp(b->peers[index].service, p.service);
    bool exclusive_busy = rule && rule->placement == FLOWMQ_MDP_EXCLUSIVE && !duplicate &&
                          !exclusive_available(b, rule);
    if (!allowed || exclusive_busy || (index == capacity && unused == capacity)) {
      p.command = LEAVE;
      p.status = allowed ? SALTS_ENOBUFS : SALTS_EPERM;
      p.broker = b->config.incarnation;
      rc = mdp_send(listener->config.router, &route, &p);
      return mdp_busy(rc) || rc == SALTS_ENOENT || rc == SALTS_ENOTCONN ? SALTS_OK : rc;
    }
    if (index == capacity) index = unused;
    broker_worker *w = &b->peers[index];
    if (w->session != p.worker) {
      retire(b, index);
      w->route = route;
      w->listener = ingress;
      w->session = p.worker;
      w->slots = p.slot;
      strcpy(w->service, p.service);
      if (rule && rule->placement == FLOWMQ_MDP_CACHE_RING) build_ring_points(w);
    } else if (w->slots != p.slot || strcmp(w->service, p.service)) return SALTS_EPROTO;
    w->last_seen = now;
    w->registered = 0;
    return SALTS_OK;
  }
  if (p.command != HEARTBEAT && p.command != COMPLETE && p.command != LEAVE) return SALTS_EPROTO;
  if (index == capacity || b->peers[index].session != p.worker ||
      p.broker != b->config.incarnation) {
    /* Tell a still-live old session to drain its business slots and register
     * a fresh incarnation. A stale receiver ignores the mismatched session. */
    if (p.command != HEARTBEAT) return SALTS_OK;
    p.command = LEAVE;
    p.broker = b->config.incarnation;
    rc = mdp_send(listener->config.router, &route, &p);
    return mdp_busy(rc) || rc == SALTS_ENOENT || rc == SALTS_ENOTCONN ? SALTS_OK : rc;
  }
  broker_worker *w = &b->peers[index];
  if (strcmp(w->service, p.service)) return SALTS_EPROTO;
  w->last_seen = now;
  if (p.command == LEAVE) {
    retire(b, index);
    return SALTS_OK;
  }
  if (p.command == HEARTBEAT) return SALTS_OK;
  for (size_t i = 0; i < b->config.requests; ++i) {
    broker_request *q = &b->calls[i];
    if (q->state != ACTIVE || q->worker_index != index || q->packet.slot != p.slot ||
        !mdp_same_id(q->packet.id, p.id))
      continue;
    w->busy[p.slot / 64] &= ~(UINT64_C(1) << (p.slot % 64));
    --w->occupied;
    q->packet.body = p.body;
    reply(q, RESULT, p.status);
    break;
  }
  return SALTS_OK;
}

static int compare_load(const void *left, const void *right) {
  const worker_choice *a = left, *b = right;
  /* Both factors are bounded by FLOWMQ_MDP_CAPACITY_MAX. */
  uint32_t x = a->peer->occupied * b->peer->slots;
  uint32_t y = b->peer->occupied * a->peer->slots;
  if (x != y) return x < y ? -1 : 1;
  return (a->rotation > b->rotation) - (a->rotation < b->rotation);
}

static int compare_ring(const void *left, const void *right) {
  const worker_choice *a = left, *b = right;
  if (a->distance != b->distance) return a->distance < b->distance ? -1 : 1;
  const broker_worker *x = a->peer, *y = b->peer;
  if (x->listener != y->listener) return x->listener < y->listener ? -1 : 1;
  if (x->route.size != y->route.size) return x->route.size < y->route.size ? -1 : 1;
  return memcmp(x->route.bytes, y->route.bytes, x->route.size);
}

static size_t rank_workers(const flowmq_mdp_broker *b, const broker_request *q,
                           worker_choice *choices) {
  size_t count = 0;
  bool ring = q->policy->placement == FLOWMQ_MDP_CACHE_RING;
  for (size_t i = 0; i < b->config.workers; ++i) {
    size_t index = (b->worker_cursor + i) % b->config.workers;
    const broker_worker *w = &b->peers[index];
    if (!w->registered || w->occupied == w->slots || strcmp(w->service, q->packet.service))
      continue;
    worker_choice choice = {index, i, w, UINT64_MAX};
    if (ring) {
      /* Round the configured capacity up to a whole slot; a one-slot peer
       * must still admit its first request. Try each eligible peer once. */
      if (w->occupied * 100 >= q->policy->max_load_percent * w->slots) continue;
      for (size_t j = 0; j < CACHE_RING_POINTS; ++j) {
        uint64_t distance = w->ring[j] - q->key_hash;
        if (distance < choice.distance) choice.distance = distance;
      }
    }
    choices[count++] = choice;
  }
  qsort(choices, count, sizeof(*choices), ring ? compare_ring : compare_load);
  return count;
}

static int schedule(flowmq_mdp_broker *b, broker_request *q, uint64_t now) {
  if (b->stopping) {
    reply(q, REJECT, SALTS_ESHUTDOWN);
    return SALTS_OK;
  }
  if (now >= q->deadline) {
    reply(q, REJECT, SALTS_ETIMEDOUT);
    return SALTS_OK;
  }
  bool ranked = q->policy && (q->policy->placement == FLOWMQ_MDP_LEAST_LOAD ||
                              q->policy->placement == FLOWMQ_MDP_CACHE_RING);
  size_t candidates = ranked ? rank_workers(b, q, b->choices) : b->config.workers;
  for (size_t i = 0; i < candidates; ++i) {
    size_t index = ranked ? b->choices[i].index : (b->worker_cursor + i) % b->config.workers;
    broker_worker *w = &b->peers[index];
    if (!w->registered || strcmp(w->service, q->packet.service)) continue;
    uint32_t slot = 0;
    while (slot < w->slots && (w->busy[slot / 64] & (UINT64_C(1) << (slot % 64))))
      ++slot;
    if (slot == w->slots) continue;
    q->packet.command = DISPATCH;
    q->packet.worker = w->session;
    q->packet.slot = slot;
    q->packet.budget = (uint32_t)(q->deadline - now);
    int rc = mdp_send(b->listeners[w->listener].config.router, &w->route, &q->packet);
    if (mdp_busy(rc)) continue;
    if (rc == SALTS_ENOENT || rc == SALTS_ENOTCONN) {
      retire(b, index);
      continue;
    }
    if (rc != SALTS_OK) return rc;
    w->busy[slot / 64] |= UINT64_C(1) << (slot % 64);
    ++w->occupied;
    q->state = ACTIVE;
    q->worker_index = index;
    b->worker_cursor = (index + 1) % b->config.workers;
    break;
  }
  return SALTS_OK;
}

void flowmq_mdp_broker_stop(flowmq_mdp_broker *b) {
  if (b) b->stopping = 1;
}

int flowmq_mdp_broker_drained(const flowmq_mdp_broker *b) {
  if (!b || !b->stopping) return 0;
  for (size_t i = 0; i < b->config.requests; ++i)
    if (b->calls[i].state != UNUSED || b->rejections[i].state != UNUSED) return 0;
  return 1;
}

int flowmq_mdp_broker_step(flowmq_mdp_broker *b, uint64_t now) {
  if (!b) return SALTS_EINVAL;
  b->started = true;
  for (size_t i = 0; i < b->config.workers; ++i) {
    broker_worker *w = &b->peers[i];
    if (!w->session) continue;
    if (now - w->last_seen >= b->config.worker_lease_ms) {
      retire(b, i);
      continue;
    }
    if (!w->registered) {
      mdp_packet p = {.command = REGISTERED, .broker = b->config.incarnation, .worker = w->session};
      strcpy(p.service, w->service);
      int rc = mdp_send(b->listeners[w->listener].config.router, &w->route, &p);
      if (rc == SALTS_OK) w->registered = 1;
      else if (rc == SALTS_ENOENT || rc == SALTS_ENOTCONN) retire(b, i);
      else if (!mdp_busy(rc)) return rc;
    }
  }
  size_t ingress = b->listener_cursor;
  b->listener_cursor = (ingress + 1) % b->listener_count;
  int rc = worker_message(b, ingress, now);
  if (rc == SALTS_OK) rc = admit(b, now);
  if (rc != SALTS_OK) return rc;
  for (size_t i = 0; i < b->config.requests; ++i) {
    size_t index = (b->request_cursor + i) % b->config.requests;
    broker_request *q = &b->calls[index];
    if (q->state == QUEUED) rc = schedule(b, q, now);
    if (rc == SALTS_OK && q->state == REPLY) rc = flush_reply(b, q, now);
    if (rc != SALTS_OK) return rc;
  }
  b->request_cursor = (b->request_cursor + 1) % b->config.requests;
  return SALTS_OK;
}
