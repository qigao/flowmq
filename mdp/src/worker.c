#include "mdp_internal.h"
#include <threads/threads.h>

enum job_state { IDLE, READY, RUNNING, FINISHED };
typedef struct worker_slot {
  enum job_state state;
  flowmq_mdp_task task;
  uint64_t deadline;
  size_t next_ready;
  int32_t status;
} worker_slot;

struct flowmq_mdp_worker {
  flowmq_socket_t *socket;
  mdp_reader reader;
  sync_mutex_t lock;
  uint64_t session, broker, last_ping;
  uint32_t interval;
  size_t capacity, cursor;
  size_t ready_head, ready_tail; /* capacity is the end-of-queue sentinel */
  int retired, registering;
  char service[FLOWMQ_MDP_SERVICE_MAX + 1];
  worker_slot slots[];
};

int flowmq_mdp_worker_open(flowmq_socket_t *s, const char *service, uint64_t session, size_t slots,
                           uint32_t interval, flowmq_mdp_worker **out) {
  if (!s || !session || !mdp_valid_service(service) || !slots || slots > FLOWMQ_MDP_CAPACITY_MAX ||
      !interval || !out)
    return SALTS_EINVAL;
  flowmq_mdp_worker *w = calloc(1, sizeof(*w) + slots * sizeof(worker_slot));
  if (!w) return SALTS_ENOMEM;
  sync_mutex_init(&w->lock);
  w->socket = s;
  w->session = session;
  w->capacity = slots;
  w->ready_head = w->ready_tail = slots;
  w->interval = interval;
  strcpy(w->service, service);
  *out = w;
  return SALTS_OK;
}

void flowmq_mdp_worker_retire(flowmq_mdp_worker *w) {
  if (!w) return;
  sync_mutex_lock(&w->lock);
  w->retired = 1;
  w->ready_head = w->ready_tail = w->capacity;
  for (size_t i = 0; i < w->capacity; ++i)
    if (w->slots[i].state != RUNNING) w->slots[i].state = IDLE;
  sync_mutex_unlock(&w->lock);
}

bool flowmq_mdp_worker_is_retired(flowmq_mdp_worker *w) {
  if (!w) return false;
  sync_mutex_lock(&w->lock);
  bool retired = w->retired != 0;
  sync_mutex_unlock(&w->lock);
  return retired;
}

int flowmq_mdp_worker_close(flowmq_mdp_worker *w) {
  if (!w) return SALTS_EINVAL;
  sync_mutex_lock(&w->lock);
  for (size_t i = 0; i < w->capacity; ++i) {
    if (w->slots[i].state != IDLE) {
      sync_mutex_unlock(&w->lock);
      return SALTS_EBUSY;
    }
  }
  sync_mutex_unlock(&w->lock);
  if (w->broker && !w->retired) {
    mdp_packet p = {.command = LEAVE, .broker = w->broker, .worker = w->session};
    strcpy(p.service, w->service);
    int rc = mdp_send(w->socket, NULL, &p);
    if (rc != SALTS_OK) return rc;
  }
  sync_mutex_destroy(&w->lock);
  free(w);
  return SALTS_OK;
}

int flowmq_mdp_take_task(flowmq_mdp_worker *w, flowmq_mdp_task *out) {
  uint64_t deadline;
  return flowmq_mdp_take_task_until(w, out, &deadline);
}

int flowmq_mdp_take_task_until(flowmq_mdp_worker *w, flowmq_mdp_task *out, uint64_t *deadline) {
  if (!w || !out || !deadline) return SALTS_EINVAL;
  int rc = SALTS_EBUSY;
  sync_mutex_lock(&w->lock);
  if (w->retired) rc = SALTS_ESHUTDOWN;
  else if (w->ready_head != w->capacity) {
    worker_slot *s = &w->slots[w->ready_head];
    w->ready_head = s->next_ready;
    if (w->ready_head == w->capacity) w->ready_tail = w->capacity;
    *out = s->task;
    *deadline = s->deadline;
    s->state = RUNNING;
    rc = SALTS_OK;
  }
  sync_mutex_unlock(&w->lock);
  return rc;
}
int flowmq_mdp_complete(flowmq_mdp_worker *w, const flowmq_mdp_task *task, int32_t status,
                        const flowmq_mdp_body *body) {
  if (!w || !task || task->slot >= w->capacity || !mdp_valid_body(body)) return SALTS_EINVAL;
  int rc = SALTS_ENOENT;
  sync_mutex_lock(&w->lock);
  worker_slot *s = &w->slots[task->slot];
  if (s->state == RUNNING && s->task.broker == task->broker && s->task.worker == task->worker &&
      mdp_same_id(s->task.id, task->id)) {
    s->task.body = *body;
    s->status = status;
    s->state = w->retired ? IDLE : FINISHED;
    rc = SALTS_OK;
  }
  sync_mutex_unlock(&w->lock);
  return rc;
}

int flowmq_mdp_worker_step(flowmq_mdp_worker *w, uint64_t now) {
  if (!w) return SALTS_EINVAL;
  if (w->retired) return SALTS_ESHUTDOWN;
  mdp_packet p;
  int rc = mdp_receive(w->socket, &w->reader, 0, NULL, &p);
  if (rc == SALTS_OK && p.worker == w->session && !strcmp(w->service, p.service)) {
    if (p.command == LEAVE || (w->broker && p.broker != w->broker)) {
      flowmq_mdp_worker_retire(w);
      if (p.command == LEAVE && !w->broker &&
          (p.status == SALTS_ENOBUFS || p.status == SALTS_EPERM))
        return p.status;
      return SALTS_ENOTCONN;
    }
    if (p.command == REGISTERED) w->broker = p.broker;
    else if (p.command == DISPATCH && w->broker && p.slot < w->capacity) {
      if (now > UINT64_MAX - p.budget) return SALTS_ERANGE;
      sync_mutex_lock(&w->lock);
      worker_slot *s = &w->slots[p.slot];
      if (s->state == IDLE) {
        s->task = (flowmq_mdp_task){p.id, p.broker, p.worker, p.slot, p.budget, p.body};
        s->deadline = now + p.budget;
        s->next_ready = w->capacity;
        if (w->ready_tail == w->capacity) w->ready_head = p.slot;
        else w->slots[w->ready_tail].next_ready = p.slot;
        w->ready_tail = p.slot;
        s->state = READY;
      } else rc = SALTS_EPROTO;
      sync_mutex_unlock(&w->lock);
      if (rc != SALTS_OK) return rc;
    } else return SALTS_EPROTO;
  } else if (rc == SALTS_ENOTCONN) {
    flowmq_mdp_worker_retire(w);
    return rc;
  } else if (rc != SALTS_OK && rc != SALTS_EBUSY) return rc;

  if (!w->registering || now - w->last_ping >= w->interval) {
    p = (mdp_packet){.command = w->broker ? HEARTBEAT : REGISTER,
                     .worker = w->session,
                     .broker = w->broker,
                     .slot = (uint32_t)w->capacity};
    strcpy(p.service, w->service);
    rc = mdp_send(w->socket, NULL, &p);
    if (rc == SALTS_OK) {
      w->last_ping = now;
      w->registering = 1;
    } else if (!mdp_busy(rc)) return rc;
  }
  for (size_t i = 0; i < w->capacity; ++i) {
    size_t index = (w->cursor + i) % w->capacity;
    worker_slot *s = &w->slots[index];
    sync_mutex_lock(&w->lock);
    if (s->state != FINISHED) {
      sync_mutex_unlock(&w->lock);
      continue;
    }
    p = (mdp_packet){.command = COMPLETE,
                     .id = s->task.id,
                     .worker = w->session,
                     .broker = w->broker,
                     .slot = (uint32_t)index,
                     .status = s->status,
                     .body = s->task.body};
    sync_mutex_unlock(&w->lock);
    strcpy(p.service, w->service);
    rc = mdp_send(w->socket, NULL, &p);
    if (rc == SALTS_OK) {
      sync_mutex_lock(&w->lock);
      s->state = IDLE;
      sync_mutex_unlock(&w->lock);
    }
    w->cursor = (index + 1) % w->capacity;
    return mdp_busy(rc) ? SALTS_OK : rc;
  }
  return SALTS_OK;
}
