#include "mdp_internal.h"

enum client_state { FREE, UNSENT, SENT, DONE };
typedef struct pending_call {
  enum client_state state;
  uint64_t deadline;
  mdp_packet packet;
  flowmq_mdp_body original_body;
} pending_call;

struct flowmq_mdp_client {
  flowmq_socket_t *socket;
  uint64_t session, sequence;
  size_t capacity, cursor;
  mdp_reader reader;
  pending_call calls[];
};

int flowmq_mdp_client_open(flowmq_socket_t *s, uint64_t session, size_t cap,
                           flowmq_mdp_client **out) {
  if (!s || !session || !out || !cap || cap > FLOWMQ_MDP_CAPACITY_MAX) return SALTS_EINVAL;
  flowmq_mdp_client *c = calloc(1, sizeof(*c) + cap * sizeof(pending_call));
  if (!c) return SALTS_ENOMEM;
  c->socket = s;
  c->session = session;
  c->capacity = cap;
  *out = c;
  return SALTS_OK;
}
void flowmq_mdp_client_close(flowmq_mdp_client *c) { free(c); }

int flowmq_mdp_call(flowmq_mdp_client *c, const char *service, const flowmq_mdp_body *body,
                    uint64_t now, uint32_t budget, flowmq_mdp_id *out) {
  if (!c || !out || !mdp_valid_service(service) || !mdp_valid_body(body) || !budget)
    return SALTS_EINVAL;
  if (now > UINT64_MAX - budget || c->sequence == UINT64_MAX) return SALTS_ERANGE;
  for (size_t i = 0; i < c->capacity; ++i) {
    pending_call *q = &c->calls[i];
    if (q->state != FREE) continue;
    q->packet = (mdp_packet){.command = CALL, .id = {c->session, ++c->sequence, 1}, .body = *body};
    strcpy(q->packet.service, service);
    q->deadline = now + budget;
    q->original_body = *body;
    q->state = UNSENT;
    *out = q->packet.id;
    return SALTS_OK;
  }
  return SALTS_ENOBUFS;
}

static pending_call *lookup(flowmq_mdp_client *c, flowmq_mdp_id id) {
  for (size_t i = 0; i < c->capacity; ++i)
    if (c->calls[i].state != FREE && mdp_same_id(c->calls[i].packet.id, id)) return &c->calls[i];
  return NULL;
}
int flowmq_mdp_retry(flowmq_mdp_client *c, flowmq_mdp_id id, uint64_t now, flowmq_mdp_id *next) {
  if (!c || !next) return SALTS_EINVAL;
  pending_call *q = lookup(c, id);
  if (!q) return SALTS_ENOENT;
  if (q->state != DONE) return SALTS_EBUSY;
  if (q->packet.command != REJECT) return SALTS_EPROTO;
  if (now >= q->deadline) return SALTS_ETIMEDOUT;
  if (id.attempt == UINT32_MAX) return SALTS_ERANGE;
  q->packet.command = CALL;
  q->packet.broker = q->packet.worker = 0;
  q->packet.slot = 0;
  q->packet.status = 0;
  q->packet.body = q->original_body;
  q->packet.id.attempt++;
  q->state = UNSENT;
  *next = q->packet.id;
  return SALTS_OK;
}
int flowmq_mdp_cancel(flowmq_mdp_client *c, flowmq_mdp_id id) {
  if (!c) return SALTS_EINVAL;
  pending_call *q = lookup(c, id);
  if (!q) return SALTS_ENOENT;
  int rc = SALTS_OK;
  if (q->state == SENT) {
    mdp_packet p = {.command = CANCEL, .id = id};
    strcpy(p.service, q->packet.service);
    rc = mdp_send(c->socket, NULL, &p);
  }
  q->state = FREE;
  return mdp_busy(rc) || rc == SALTS_ENOTCONN ? SALTS_OK : rc;
}
static int read_result(flowmq_mdp_client *c, flowmq_mdp_id id, flowmq_mdp_result *out,
                       int consume, uint64_t *worker_session) {
  if (!c || !out) return SALTS_EINVAL;
  pending_call *q = lookup(c, id);
  if (!q) return SALTS_ENOENT;
  if (q->state != DONE) return SALTS_EBUSY;
  flowmq_mdp_outcome outcome = q->packet.command == RESULT   ? FLOWMQ_MDP_RESULT
                               : q->packet.command == REJECT ? FLOWMQ_MDP_REJECTED
                                                             : FLOWMQ_MDP_UNKNOWN;
  *out = (flowmq_mdp_result){id, outcome, q->packet.status, q->packet.body};
  if (worker_session) *worker_session = q->packet.worker;
  if (consume) q->state = FREE;
  return SALTS_OK;
}

int flowmq_mdp_peek_result(flowmq_mdp_client *c, flowmq_mdp_id id, flowmq_mdp_result *out) {
  return read_result(c, id, out, 0, NULL);
}
int flowmq_mdp_take_result(flowmq_mdp_client *c, flowmq_mdp_id id, flowmq_mdp_result *out) {
  return read_result(c, id, out, 1, NULL);
}
int mdp_take_member_result(flowmq_mdp_client *c, flowmq_mdp_id id,
                           flowmq_mdp_result *out, uint64_t *worker_session) {
  return read_result(c, id, out, 1, worker_session);
}

int flowmq_mdp_client_step(flowmq_mdp_client *c, uint64_t now) {
  if (!c) return SALTS_EINVAL;
  mdp_packet p;
  int rc = mdp_receive(c->socket, &c->reader, 0, NULL, &p);
  if (rc == SALTS_OK) {
    if (p.command != RESULT && p.command != REJECT && p.command != UNKNOWN) return SALTS_EPROTO;
    pending_call *q = lookup(c, p.id);
    if (q && q->state == SENT && now < q->deadline && !strcmp(q->packet.service, p.service)) {
      q->packet = p;
      q->state = DONE;
    }
  } else if (rc != SALTS_EBUSY && rc != SALTS_ENOTCONN) return rc;

  int sent = 0;
  size_t start = c->cursor;
  for (size_t i = 0; i < c->capacity; ++i) {
    size_t index = (start + i) % c->capacity;
    pending_call *q = &c->calls[index];
    if (q->state != SENT && q->state != UNSENT) continue;
    if (now >= q->deadline) {
      q->packet.command = q->state == UNSENT ? REJECT : UNKNOWN;
      q->packet.status = SALTS_ETIMEDOUT;
      q->packet.body = (flowmq_mdp_body){0};
      q->state = DONE;
    } else if (!sent && q->state == UNSENT) {
      q->packet.budget = (uint32_t)(q->deadline - now);
      rc = mdp_send(c->socket, NULL, &q->packet);
      if (rc == SALTS_OK) q->state = SENT;
      else if (!mdp_busy(rc) && rc != SALTS_ENOTCONN) return rc;
      sent = 1;
      c->cursor = (index + 1) % c->capacity;
    }
  }
  return SALTS_OK;
}
