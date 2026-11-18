#ifndef FLOWMQ_MDP_FANOUT_H
#define FLOWMQ_MDP_FANOUT_H

#include "flowmq_mdp.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A trusted application member directory supplies a stable member identity
 * and its current, nonzero Worker session. Start a fresh session on reconnect.
 * Both values are scoped to one Broker deployment. */
typedef struct flowmq_mdp_member {
  uint64_t id;
  uint64_t session;
} flowmq_mdp_member;

/* Format the dedicated MDP service for exactly this member incarnation. The
 * caller supplies FLOWMQ_MDP_SERVICE_MAX + 1 bytes. On the worker listener
 * authorize only the authenticated member for its generated service. */
int flowmq_mdp_member_service(flowmq_mdp_member member, char service[FLOWMQ_MDP_SERVICE_MAX + 1]);

typedef struct flowmq_mdp_fanout flowmq_mdp_fanout;
typedef struct flowmq_mdp_fanout_summary {
  size_t pending, succeeded, failed, missing;
} flowmq_mdp_fanout_summary;

/* Begin an all-member operation on the client I/O owner. Copies the snapshot
 * of 1..FLOWMQ_MDP_CAPACITY_MAX distinct member IDs and enqueues a request for
 * each on the same client, sharing one absolute deadline. If local admission
 * fails, releases every reservation before returning; no sends have occurred.
 * Use a unique nonzero application operation_id (persist it with external
 * effects when idempotence is needed). The request envelope consumes one
 * part and 24 payload bytes of the normal MDP limits. Each reply retains the
 * full MDP body budget. The operation borrows client until close.
 * EINVAL for invalid snapshot/body/budget, EMSGSIZE for an oversized body,
 * ENOBUFS for insufficient client slots, or the underlying MDP error. */
int flowmq_mdp_fanout_start(flowmq_mdp_client *client, uint64_t operation_id,
                            const flowmq_mdp_member *members, size_t count,
                            const flowmq_mdp_body *body, uint64_t now_ms, uint32_t budget_ms,
                            flowmq_mdp_fanout **out);

/* Drive the socket and flowmq_mdp_client_step on the client owner first.
 * Collect available replies; updates *summary on success or EBUSY. Returns
 * EBUSY while pending and OK once settled, including partial results.
 * succeeded counts matching status-zero worker replies; failed counts worker
 * errors and definitive rejections. missing counts timed-out rejections and
 * unknown outcomes. Repeated polls never count a member twice. */
int flowmq_mdp_fanout_poll(flowmq_mdp_fanout *operation, flowmq_mdp_fanout_summary *summary);
/* Copy one settled MDP result and original member by snapshot index. EBUSY
 * while pending. UNKNOWN means execution may have happened; EPROTO with that
 * outcome indicates a reply from a different Worker incarnation. */
int flowmq_mdp_fanout_result_at(const flowmq_mdp_fanout *operation, size_t index,
                                flowmq_mdp_member *member, flowmq_mdp_result *result);
/* I/O owner only. Abandon any outstanding waits and release local slots.
 * Already dispatched work may finish later. The operation is destroyed even
 * when a best-effort MDP cancellation reports a transport error. */
int flowmq_mdp_fanout_close(flowmq_mdp_fanout *operation);

/* Worker-side application action. Input is borrowed for the duration of the
 * call. Fill the initially empty reply using flowmq_mdp_add_part and return
 * the per-member completion status. */
typedef int32_t (*flowmq_mdp_member_handler)(void *context, uint64_t operation_id,
                                             flowmq_mdp_member self, const flowmq_mdp_body *input,
                                             flowmq_mdp_body *reply);
/* Open a normal MDP Worker on member_service(self) with session self.session.
 * On its business thread call execute to claim and settle one member task.
 * It verifies the addressed identity/session before invoking the handler.
 * Returns EBUSY when no task is ready or the MDP claim/completion status. */
int flowmq_mdp_fanout_execute(flowmq_mdp_worker *worker, flowmq_mdp_member self,
                              flowmq_mdp_member_handler handler, void *context);

/* Example (after checking start and while polling the shared client):
 *   flowmq_mdp_member members[] = {{7, 101}, {9, 205}};
 *   rc = flowmq_mdp_fanout_start(client, op_id, members, 2, &command, now, 500, &op);
 *   // Each I/O turn after flowmq_mdp_client_step(client, now):
 *   rc = flowmq_mdp_fanout_poll(op, &summary);
 *   // On OK inspect both result_at entries, then close(op).
 * Worker setup uses flowmq_mdp_member_service(self, name) followed by
 * flowmq_mdp_worker_open(dealer, name, self.session, slots, heartbeat, &worker).
 */
#ifdef __cplusplus
}
#endif
#endif
