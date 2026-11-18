#include "rpc_internal.h"
#include <stdlib.h>

typedef struct engine_call {
  flowmq_mdp_task task;
  rpc_document doc;
  rpc_member member;
  const char *name;
  const flowmq_rpc_method *sync_method;
  uint64_t generation, deadline;
  size_t position, children;
  flowmq_mdp_body reply;
  int encoding_status, cancel_status;
  bool active, scheduled, ready, started, cancel_sent;
} engine_call;

typedef enum branch_state { BRANCH_FREE, BRANCH_CALLING, BRANCH_DONE } branch_state;
typedef struct child_call {
  branch_state state;
  size_t parent;
  uint32_t tag;
  flowmq_mdp_id id;
  flowmq_mdp_result result;
} child_call;

struct flowmq_rpc_engine {
  flowmq_rpc_engine_config config;
  flowmq_mdp_worker *worker;
  const flowmq_rpc_server *sync;
  flowmq_mdp_client *client;
  flowmq_rpc_scheduler *scheduler;
  child_call *branches;
  uint64_t generation;
  bool draining, stopping;
  engine_call calls[];
};

static const char *deferred_name(const flowmq_rpc_engine *e, const json_value_t *method) {
  for (size_t i = 0; i < e->config.deferred_count; ++i)
    if (rpc_string_is(method, e->config.deferred_methods[i])) return e->config.deferred_methods[i];
  return NULL;
}

int flowmq_rpc_engine_open(flowmq_mdp_worker *worker, const flowmq_rpc_server *sync,
                           flowmq_mdp_client *client, const flowmq_rpc_engine_config *cfg,
                           flowmq_rpc_engine **out) {
  if (!worker || !cfg || !out || !cfg->now_ms || !cfg->schedule.capacity ||
      cfg->schedule.capacity > FLOWMQ_MDP_CAPACITY_MAX || (!sync && !cfg->deferred_count) ||
      (cfg->deferred_count && !cfg->deferred_methods) ||
      cfg->deferred_count > FLOWMQ_RPC_METHODS_MAX ||
      cfg->child_capacity > FLOWMQ_MDP_CAPACITY_MAX ||
      (client != NULL) != (cfg->child_capacity != 0) ||
      (client != NULL) != (cfg->children_per_request != 0) ||
      cfg->children_per_request > cfg->child_capacity)
    return SALTS_EINVAL;
  for (size_t i = 0; i < cfg->deferred_count; ++i) {
    const char *name = cfg->deferred_methods[i];
    if (!rpc_valid_name(name)) return SALTS_EINVAL;
    for (size_t j = 0; j < i; ++j)
      if (!strcmp(name, cfg->deferred_methods[j])) return SALTS_EINVAL;
    if (sync) {
      json_value_t *probe = json_create_string(name);
      if (!probe) return SALTS_ENOMEM;
      const flowmq_rpc_method *duplicate = rpc_find_method(sync, probe);
      json_free(probe);
      if (duplicate) return SALTS_EINVAL;
    }
  }
  flowmq_rpc_engine *e = calloc(1, sizeof(*e) + cfg->schedule.capacity * sizeof(engine_call));
  if (!e) return SALTS_ENOMEM;
  int rc = flowmq_rpc_scheduler_open(&cfg->schedule, &e->scheduler);
  if (rc != SALTS_OK) {
    free(e);
    return rc;
  }
  if (client) {
    e->branches = calloc(cfg->child_capacity, sizeof(child_call));
    if (!e->branches) {
      flowmq_rpc_scheduler_close(e->scheduler);
      free(e);
      return SALTS_ENOMEM;
    }
  }
  e->worker = worker;
  e->sync = sync;
  e->client = client;
  e->config = *cfg;
  *out = e;
  return SALTS_OK;
}

static engine_call *lookup(flowmq_rpc_engine *e, flowmq_rpc_ticket t) {
  if (t.slot >= e->config.schedule.capacity) return NULL;
  engine_call *q = &e->calls[t.slot];
  const flowmq_mdp_id id = q->task.id;
  return q->active && q->started && q->generation == t.generation && q->position == t.member &&
                 t.broker == q->task.broker && t.worker == q->task.worker &&
                 id.client == t.request.client && id.sequence == t.request.sequence &&
                 id.attempt == t.request.attempt
             ? q
             : NULL;
}

static int cancel_children(flowmq_rpc_engine *e, size_t slot) {
  engine_call *q = &e->calls[slot];
  int rc = SALTS_OK;
  for (size_t i = 0; i < e->config.child_capacity && q->children; ++i) {
    child_call *c = &e->branches[i];
    if (c->state == BRANCH_FREE || c->parent != slot) continue;
    if (c->state == BRANCH_CALLING) {
      int status = flowmq_mdp_cancel(e->client, c->id);
      if (rc == SALTS_OK) rc = status;
    }
    c->state = BRANCH_FREE;
    --q->children;
  }
  return rc;
}

/* All external work is unclaimed or acknowledged by its application here. */
static int settle(flowmq_rpc_engine *e, size_t slot, int status) {
  engine_call *q = &e->calls[slot];
  if (q->scheduled) {
    int rc = flowmq_rpc_scheduler_finish(e->scheduler, slot);
    if (rc != SALTS_OK) return rc;
    q->scheduled = false;
  }
  if (status != SALTS_OK) q->reply = (flowmq_mdp_body){0};
  int rc = flowmq_mdp_complete(e->worker, &q->task, status, &q->reply);
  if (rc != SALTS_OK) return rc;
  json_free(q->doc.root);
  *q = (engine_call){0};
  return SALTS_OK;
}

static int finish_member(flowmq_rpc_engine *e, size_t slot, bool valid, int32_t code,
                         flowmq_rpc_output output) {
  engine_call *q = &e->calls[slot];
  int status = rpc_member_reply(valid ? &q->member : NULL, q->doc.batch, &q->reply, code, output);
  if (q->encoding_status == SALTS_OK) q->encoding_status = status;
  if (q->scheduled) {
    int rc = flowmq_rpc_scheduler_finish(e->scheduler, slot);
    if (rc != SALTS_OK) return rc;
  }
  q->scheduled = false;
  q->ready = false;
  q->started = false;
  if (++q->position < q->doc.count) return SALTS_OK;
  if (q->encoding_status == SALTS_OK && q->doc.batch && q->reply.size)
    q->encoding_status = rpc_append(&q->reply, "]", 1);
  return settle(e, slot, q->encoding_status);
}

static int prepare(flowmq_rpc_engine *e, size_t slot) {
  engine_call *q = &e->calls[slot];
  if (!rpc_document_member(&q->doc, q->position, q->task.id, &q->member))
    return finish_member(e, slot, false, FLOWMQ_RPC_INVALID_REQUEST, (flowmq_rpc_output){0});
  q->sync_method = rpc_find_method(e->sync, q->member.method);
  q->name = q->sync_method ? q->sync_method->name : deferred_name(e, q->member.method);
  if (!q->name)
    return finish_member(e, slot, true, FLOWMQ_RPC_METHOD_NOT_FOUND, (flowmq_rpc_output){0});
  flowmq_rpc_work_class work = {0};
  if (e->config.classify) {
    int32_t code = e->config.classify(e->config.context, q->name, &q->member.input, &work);
    if (code) return finish_member(e, slot, true, code, (flowmq_rpc_output){0});
  }
  int rc = flowmq_rpc_scheduler_admit(e->scheduler, slot, &work);
  if (rc == SALTS_ENOBUFS)
    return finish_member(e, slot, true, FLOWMQ_RPC_OVERLOADED, (flowmq_rpc_output){0});
  if (rc != SALTS_OK) return settle(e, slot, rc);
  q->scheduled = true;
  q->ready = true;
  return SALTS_OK;
}

static int admit(flowmq_rpc_engine *e, bool *progress) {
  if (e->draining || e->stopping) {
    flowmq_mdp_task rejected;
    int rc = flowmq_mdp_take_task(e->worker, &rejected);
    if (rc == SALTS_EBUSY || rc == SALTS_ESHUTDOWN) return SALTS_OK;
    if (rc != SALTS_OK) return rc;
    *progress = true;
    return flowmq_mdp_complete(e->worker, &rejected, SALTS_ESHUTDOWN, &(flowmq_mdp_body){0});
  }
  size_t slot = 0;
  while (slot < e->config.schedule.capacity && e->calls[slot].active)
    ++slot;
  if (slot == e->config.schedule.capacity) return SALTS_OK;
  if (e->generation == UINT64_MAX) return SALTS_ERANGE;
  engine_call *q = &e->calls[slot];
  int rc = flowmq_mdp_take_task_until(e->worker, &q->task, &q->deadline);
  if (rc == SALTS_EBUSY) return SALTS_OK;
  if (rc == SALTS_ESHUTDOWN) {
    e->stopping = true;
    return SALTS_OK;
  }
  if (rc != SALTS_OK) return rc;
  q->active = true;
  *progress = true;
  q->generation = ++e->generation;
  if (e->config.now_ms(e->config.context) >= q->deadline) return settle(e, slot, SALTS_ETIMEDOUT);
  rc = rpc_document_open(&q->task, &q->doc, &q->reply);
  if (!q->doc.count) return settle(e, slot, rc);
  return SALTS_OK;
}

static void make_event(const engine_call *q, size_t slot, flowmq_rpc_event_kind kind,
                       flowmq_rpc_event *event) {
  *event = (flowmq_rpc_event){
      .kind = kind,
      .ticket = {q->task.id, q->task.broker, q->task.worker, q->generation, slot, q->position},
      .deadline_ms = q->deadline,
      .method = q->name,
      .input = q->member.input,
      .cancel_status = q->cancel_status};
}

static int harvest(flowmq_rpc_engine *e) {
  for (size_t i = 0; i < e->config.child_capacity; ++i) {
    child_call *c = &e->branches[i];
    if (c->state != BRANCH_CALLING) continue;
    int rc = flowmq_mdp_take_result(e->client, c->id, &c->result);
    if (rc == SALTS_EBUSY) continue;
    if (rc != SALTS_OK) return rc;
    c->state = BRANCH_DONE;
  }
  for (size_t i = 0; i < e->config.child_capacity; ++i) {
    child_call *c = &e->branches[i];
    if (c->state != BRANCH_DONE) continue;
    engine_call *q = &e->calls[c->parent];
    if (!q->ready && !q->cancel_status) {
      int rc = flowmq_rpc_scheduler_ready(e->scheduler, c->parent);
      if (rc != SALTS_OK) return rc;
      q->ready = true;
    }
  }
  return SALTS_OK;
}

static int abort_status(const flowmq_rpc_engine *e, const engine_call *q) {
  if (q->cancel_status) return q->cancel_status;
  if (e->stopping) return SALTS_ESHUTDOWN;
  if (e->config.now_ms(e->config.context) >= q->deadline) return SALTS_ETIMEDOUT;
  return SALTS_OK;
}

int flowmq_rpc_engine_next_deadline(const flowmq_rpc_engine *e, uint64_t *deadline) {
  if (!e || !deadline) return SALTS_EINVAL;
  bool found = false;
  uint64_t minimum = UINT64_MAX;
  for (size_t i = 0; i < e->config.schedule.capacity; ++i) {
    const engine_call *q = &e->calls[i];
    if (!q->active || q->cancel_status) continue;
    found = true;
    if (q->deadline < minimum) minimum = q->deadline;
  }
  if (!found) return SALTS_EBUSY;
  *deadline = minimum;
  return SALTS_OK;
}

int flowmq_rpc_engine_poll(flowmq_rpc_engine *e, flowmq_rpc_event *event) {
  if (!e || !event) return SALTS_EINVAL;
  if (flowmq_mdp_worker_is_retired(e->worker)) e->stopping = true;
  bool progress = false;
  int rc = admit(e, &progress);
  *event = (flowmq_rpc_event){.kind = FLOWMQ_RPC_PROGRESS};
  if (rc != SALTS_OK) return rc;
  for (size_t slot = 0; slot < e->config.schedule.capacity; ++slot) {
    engine_call *q = &e->calls[slot];
    if (!q->active) continue;
    int status = abort_status(e, q);
    if (status) {
      q->cancel_status = status;
      rc = cancel_children(e, slot);
      if (rc != SALTS_OK) return rc;
      if (!q->started) {
        rc = settle(e, slot, status);
        progress = true;
      } else if (!q->cancel_sent) {
        q->cancel_sent = true;
        make_event(q, slot, FLOWMQ_RPC_CANCEL, event);
        return SALTS_OK;
      }
    } else if (!q->scheduled) {
      rc = prepare(e, slot);
      progress = true;
    }
    if (rc != SALTS_OK) return rc;
  }
  rc = harvest(e);
  if (rc != SALTS_OK) return rc;
  size_t slot = 0;
  rc = flowmq_rpc_scheduler_next(e->scheduler, &slot);
  if (rc == SALTS_EBUSY && progress) return SALTS_OK;
  if (rc != SALTS_OK) return rc;
  engine_call *q = &e->calls[slot];
  q->ready = false;
  if (q->cancel_sent) return SALTS_OK;
  /* Queue selection can cross the deadline after the maintenance scan. */
  int status = abort_status(e, q);
  if (status) {
    if (q->started) {
      q->cancel_status = status;
      rc = cancel_children(e, slot);
      if (rc != SALTS_OK) return rc;
      q->cancel_sent = true;
      make_event(q, slot, FLOWMQ_RPC_CANCEL, event);
      return SALTS_OK;
    }
    return settle(e, slot, status);
  }
  if (q->started) {
    for (size_t i = 0; i < e->config.child_capacity; ++i) {
      child_call *c = &e->branches[i];
      if (c->state != BRANCH_DONE || c->parent != slot) continue;
      make_event(q, slot, FLOWMQ_RPC_CHILD, event);
      event->child_tag = c->tag;
      event->child = c->result;
      c->state = BRANCH_FREE;
      --q->children;
      return SALTS_OK;
    }
    return SALTS_EPROTO;
  }
  if (q->sync_method) {
    flowmq_rpc_output output = {0};
    int32_t code = q->sync_method->handler(q->sync_method->context, &q->member.input, &output);
    if (flowmq_mdp_worker_is_retired(e->worker)) e->stopping = true;
    status = abort_status(e, q);
    if (status) {
      json_free(output.value);
      rc = settle(e, slot, status);
    } else rc = finish_member(e, slot, true, code, output);
    return rc;
  }
  q->started = true;
  make_event(q, slot, FLOWMQ_RPC_START, event);
  return SALTS_OK;
}

int flowmq_rpc_engine_fork(flowmq_rpc_engine *e, flowmq_rpc_ticket t,
                           const flowmq_rpc_branch *branches, size_t n) {
  if (!e || !branches || !n || !e->client) return SALTS_EINVAL;
  engine_call *q = lookup(e, t);
  if (!q) return SALTS_ENOENT;
  uint64_t now = e->config.now_ms(e->config.context);
  if (e->stopping || flowmq_mdp_worker_is_retired(e->worker)) return SALTS_ESHUTDOWN;
  if (q->cancel_status || now >= q->deadline) return SALTS_ETIMEDOUT;
  if (n > e->config.children_per_request - q->children) return SALTS_ENOBUFS;
  size_t free_count = 0;
  for (size_t i = 0; i < e->config.child_capacity; ++i) {
    child_call *c = &e->branches[i];
    if (c->state == BRANCH_FREE) ++free_count;
    else if (c->parent == t.slot)
      for (size_t j = 0; j < n; ++j)
        if (branches[j].tag == c->tag) return SALTS_EINVAL;
  }
  if (free_count < n) return SALTS_ENOBUFS;
  for (size_t i = 0; i < n; ++i)
    for (size_t j = 0; j < i; ++j)
      if (branches[i].tag == branches[j].tag) return SALTS_EINVAL;
  /* New children are the first n free slots, all submissions on this
   * serialized client are UNSENT until the owner next drives its socket. */
  size_t allocated[FLOWMQ_MDP_CAPACITY_MAX];
  size_t used = 0;
  for (size_t i = 0; used < n; ++i) {
    child_call *c = &e->branches[i];
    if (c->state != BRANCH_FREE) continue;
    const flowmq_rpc_branch *branch = &branches[used];
    int rc = flowmq_rpc_call(e->client, branch->service, &branch->request, 1, false, now,
                             (uint32_t)(q->deadline - now), &c->id);
    if (rc != SALTS_OK) {
      for (size_t j = 0; j < used; ++j) {
        child_call *rollback = &e->branches[allocated[j]];
        int undo = flowmq_mdp_cancel(e->client, rollback->id);
        if (undo != SALTS_OK) return undo;
        rollback->state = BRANCH_FREE;
        --q->children;
      }
      return rc;
    }
    allocated[used++] = i;
    c->state = BRANCH_CALLING;
    c->parent = t.slot;
    c->tag = branch->tag;
    ++q->children;
  }
  return SALTS_OK;
}

int flowmq_rpc_engine_finish(flowmq_rpc_engine *e, flowmq_rpc_ticket t, int32_t code,
                             flowmq_rpc_output out) {
  if (!e) {
    json_free(out.value);
    return SALTS_EINVAL;
  }
  engine_call *q = lookup(e, t);
  if (!q) {
    json_free(out.value);
    return SALTS_ENOENT;
  }
  if (flowmq_mdp_worker_is_retired(e->worker)) e->stopping = true;
  int rc = cancel_children(e, t.slot);
  int status = abort_status(e, q);
  if (rc != SALTS_OK || status) {
    json_free(out.value);
    int settled = settle(e, t.slot, rc != SALTS_OK ? rc : status);
    return settled != SALTS_OK ? settled : rc;
  }
  return finish_member(e, t.slot, true, code, out);
}

void flowmq_rpc_engine_drain(flowmq_rpc_engine *e) {
  if (e) e->draining = true;
}
void flowmq_rpc_engine_stop(flowmq_rpc_engine *e) {
  if (e) e->stopping = true;
}
int flowmq_rpc_engine_close(flowmq_rpc_engine *e) {
  if (!e) return SALTS_EINVAL;
  for (size_t i = 0; i < e->config.schedule.capacity; ++i)
    if (e->calls[i].active) return SALTS_EBUSY;
  flowmq_rpc_scheduler_close(e->scheduler);
  free(e->branches);
  free(e);
  return SALTS_OK;
}
