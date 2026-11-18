#include "flowmq_mdp_fanout.h"
#include "mdp_internal.h"
#include <inttypes.h>
#include <stdio.h>

enum { MEMBER_HEADER_SIZE = 3 * sizeof(uint64_t) };

typedef struct member_call {
  flowmq_mdp_member member;
  /* A zero outcome identifies an outstanding MDP handle in result.id. */
  flowmq_mdp_result result;
} member_call;

struct flowmq_mdp_fanout {
  flowmq_mdp_client *client;
  size_t count;
  flowmq_mdp_fanout_summary summary;
  member_call calls[];
};

int flowmq_mdp_member_service(flowmq_mdp_member member, char service[FLOWMQ_MDP_SERVICE_MAX + 1]) {
  if (!member.id || !member.session || !service) return SALTS_EINVAL;
  int n = snprintf(service, FLOWMQ_MDP_SERVICE_MAX + 1, "member.%016" PRIx64 ".%016" PRIx64,
                   member.id, member.session);
  return n > 0 && n <= FLOWMQ_MDP_SERVICE_MAX ? SALTS_OK : SALTS_ERANGE;
}

static void encode_id(unsigned char *p, uint64_t id) {
  for (size_t i = sizeof(id); i; --i) {
    p[i - 1] = (unsigned char)id;
    id >>= 8;
  }
}

static uint64_t decode_id(const unsigned char *p) {
  uint64_t id = 0;
  for (size_t i = 0; i < sizeof(id); ++i)
    id = (id << 8) | p[i];
  return id;
}

/* Both layouts have been validated. Copy the full business body including
 * zero-length parts. Keep application metadata outside the business data. */
static int make_request(const flowmq_mdp_body *body, uint64_t id, flowmq_mdp_body *wire) {
  if (body->count == FLOWMQ_MDP_PARTS_MAX ||
      body->size > FLOWMQ_MDP_PAYLOAD_MAX - MEMBER_HEADER_SIZE)
    return SALTS_EMSGSIZE;
  wire->count = body->count + 1;
  wire->size = body->size + MEMBER_HEADER_SIZE;
  wire->lengths[0] = MEMBER_HEADER_SIZE;
  memcpy(wire->lengths + 1, body->lengths, body->count * sizeof(*body->lengths));
  memcpy(wire->bytes + MEMBER_HEADER_SIZE, body->bytes, body->size);
  encode_id(wire->bytes, id);
  return SALTS_OK;
}

int flowmq_mdp_fanout_start(flowmq_mdp_client *client, uint64_t operation_id,
                            const flowmq_mdp_member *members, size_t count,
                            const flowmq_mdp_body *body, uint64_t now, uint32_t budget,
                            flowmq_mdp_fanout **out) {
  if (!out) return SALTS_EINVAL;
  *out = NULL;
  if (!client || !operation_id || !members || !count || count > FLOWMQ_MDP_CAPACITY_MAX ||
      !mdp_valid_body(body) || !budget)
    return SALTS_EINVAL;
  if (now > UINT64_MAX - budget) return SALTS_ERANGE;
  for (size_t i = 0; i < count; ++i) {
    if (!members[i].id || !members[i].session) return SALTS_EINVAL;
    for (size_t j = 0; j < i; ++j)
      if (members[j].id == members[i].id) return SALTS_EINVAL;
  }
  flowmq_mdp_body wire = {0};
  int rc = make_request(body, operation_id, &wire);
  if (rc != SALTS_OK) return rc;
  flowmq_mdp_fanout *op = calloc(1, sizeof(*op) + count * sizeof(*op->calls));
  if (!op) return SALTS_ENOMEM;
  op->client = client;
  for (size_t i = 0; i < count; ++i) {
    member_call *call = &op->calls[i];
    call->member = members[i];
    char service[FLOWMQ_MDP_SERVICE_MAX + 1];
    rc = flowmq_mdp_member_service(call->member, service);
    if (rc != SALTS_OK) break;
    encode_id(wire.bytes + sizeof(uint64_t), call->member.id);
    encode_id(wire.bytes + 2 * sizeof(uint64_t), call->member.session);
    rc = flowmq_mdp_call(client, service, &wire, now, budget, &call->result.id);
    if (rc != SALTS_OK) break;
    ++op->count;
  }
  if (rc != SALTS_OK) {
    /* Same I/O owner throughout start. Each reservation is still UNSENT. */
    flowmq_mdp_fanout_close(op);
    return rc;
  }
  op->summary.pending = count;
  *out = op;
  return SALTS_OK;
}

int flowmq_mdp_fanout_poll(flowmq_mdp_fanout *op, flowmq_mdp_fanout_summary *summary) {
  if (!op || !summary) return SALTS_EINVAL;
  for (size_t i = 0; i < op->count && op->summary.pending; ++i) {
    member_call *call = &op->calls[i];
    if (call->result.outcome) continue;
    uint64_t session = 0;
    int rc = mdp_take_member_result(op->client, call->result.id, &call->result, &session);
    if (rc == SALTS_EBUSY) continue;
    if (rc != SALTS_OK) return rc;
    flowmq_mdp_result *result = &call->result;
    if (result->outcome == FLOWMQ_MDP_RESULT && session != call->member.session) {
      /* The Broker validates the registered session and slot on COMPLETE.
       * A replacement cannot acknowledge its predecessor's invocation. */
      result->outcome = FLOWMQ_MDP_UNKNOWN;
      result->status = SALTS_EPROTO;
      result->body = (flowmq_mdp_body){0};
    }
    --op->summary.pending;
    if (result->outcome == FLOWMQ_MDP_RESULT && result->status == SALTS_OK) ++op->summary.succeeded;
    else if (result->outcome == FLOWMQ_MDP_UNKNOWN ||
             (result->outcome == FLOWMQ_MDP_REJECTED && result->status == SALTS_ETIMEDOUT))
      ++op->summary.missing;
    else ++op->summary.failed;
  }
  *summary = op->summary;
  return summary->pending ? SALTS_EBUSY : SALTS_OK;
}

int flowmq_mdp_fanout_result_at(const flowmq_mdp_fanout *op, size_t index,
                                flowmq_mdp_member *member, flowmq_mdp_result *result) {
  if (!op || index >= op->count || !member || !result) return SALTS_EINVAL;
  if (!op->calls[index].result.outcome) return SALTS_EBUSY;
  *member = op->calls[index].member;
  *result = op->calls[index].result;
  return SALTS_OK;
}

int flowmq_mdp_fanout_close(flowmq_mdp_fanout *op) {
  if (!op) return SALTS_OK;
  int rc = SALTS_OK;
  for (size_t i = 0; i < op->count; ++i) {
    if (op->calls[i].result.outcome) continue;
    int status = flowmq_mdp_cancel(op->client, op->calls[i].result.id);
    if (rc == SALTS_OK) rc = status;
  }
  free(op);
  return rc;
}

int flowmq_mdp_fanout_execute(flowmq_mdp_worker *worker, flowmq_mdp_member self,
                              flowmq_mdp_member_handler handler, void *context) {
  if (!worker || !self.id || !self.session || !handler) return SALTS_EINVAL;
  flowmq_mdp_task task;
  int rc = flowmq_mdp_take_task(worker, &task);
  if (rc != SALTS_OK) return rc;
  const flowmq_mdp_body *wire = &task.body;
  flowmq_mdp_body reply = {0};
  int32_t status = SALTS_EPROTO;
  if (wire->count && wire->lengths[0] == MEMBER_HEADER_SIZE && self.session == task.worker &&
      decode_id(wire->bytes + sizeof(uint64_t)) == self.id &&
      decode_id(wire->bytes + 2 * sizeof(uint64_t)) == self.session) {
    uint64_t operation_id = decode_id(wire->bytes);
    if (operation_id) {
      flowmq_mdp_body input = {.count = wire->count - 1, .size = wire->size - MEMBER_HEADER_SIZE};
      memcpy(input.lengths, wire->lengths + 1, input.count * sizeof(*wire->lengths));
      memcpy(input.bytes, wire->bytes + MEMBER_HEADER_SIZE, input.size);
      status = handler(context, operation_id, self, &input, &reply);
      if (!mdp_valid_body(&reply)) {
        status = SALTS_EINVAL;
        reply = (flowmq_mdp_body){0};
      }
    }
  }
  return flowmq_mdp_complete(worker, &task, status, &reply);
}
