#ifndef FLOWMQ_MDP_H
#define FLOWMQ_MDP_H
#include "flowmq_socket.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Single-node broker. Socket handles are borrowed, already configured, and
 * exclusively driven by their I/O owner. Client and worker each use a DEALER
 * connected to one broker. Broker uses one client ROUTER and one or more
 * worker ROUTERs, with independent endpoint and ACL per worker listener.
 * Supply fresh nonzero incarnation/session IDs on every start or reconnect. */
enum {
  FLOWMQ_MDP_SERVICE_MAX = 63,
  FLOWMQ_MDP_PARTS_MAX = 16,
  FLOWMQ_MDP_PAYLOAD_MAX = 4096,
  FLOWMQ_MDP_CAPACITY_MAX = 1024
};
typedef struct flowmq_mdp_body {
  uint32_t size;
  uint32_t count;
  uint32_t lengths[FLOWMQ_MDP_PARTS_MAX];
  unsigned char bytes[FLOWMQ_MDP_PAYLOAD_MAX];
} flowmq_mdp_body;
int flowmq_mdp_add_part(flowmq_mdp_body *body, const void *data, size_t size);

typedef struct flowmq_mdp_id {
  uint64_t client;
  uint64_t sequence;
  uint32_t attempt;
} flowmq_mdp_id;
typedef enum flowmq_mdp_outcome {
  FLOWMQ_MDP_RESULT = 1,
  FLOWMQ_MDP_REJECTED,
  FLOWMQ_MDP_UNKNOWN
} flowmq_mdp_outcome;
typedef struct flowmq_mdp_result {
  flowmq_mdp_id id;
  flowmq_mdp_outcome outcome;
  int32_t status;
  flowmq_mdp_body body;
} flowmq_mdp_result;
typedef struct flowmq_mdp_task {
  flowmq_mdp_id id;
  uint64_t broker;
  uint64_t worker;
  uint32_t slot;
  uint32_t remaining_ms;
  flowmq_mdp_body body;
} flowmq_mdp_task;
typedef struct flowmq_mdp_client flowmq_mdp_client;
typedef struct flowmq_mdp_worker flowmq_mdp_worker;
typedef struct flowmq_mdp_broker flowmq_mdp_broker;

/* Required immutable service ACL. Bind identity to an authenticated peer at
 * the transport boundary in untrusted deployments. Return nonzero to allow.
 * The callback borrows the identity and NUL-terminated service for this call. */
typedef int (*flowmq_mdp_authorize_fn)(void *context, int worker, const void *identity,
                                       size_t identity_size, const char *service);
typedef struct flowmq_mdp_broker_config {
  uint64_t incarnation;
  size_t requests;
  size_t workers;
  size_t requests_per_client;
  uint32_t worker_lease_ms;
  flowmq_mdp_authorize_fn authorize;
  void *context;
  size_t requests_per_service; /* zero uses the total request capacity */
} flowmq_mdp_broker_config;

int flowmq_mdp_broker_open(flowmq_socket_t *clients, flowmq_socket_t *workers,
                           const flowmq_mdp_broker_config *config, flowmq_mdp_broker **out);
typedef struct flowmq_mdp_worker_listener {
  flowmq_socket_t *router;
  flowmq_mdp_authorize_fn authorize;
  void *context;
} flowmq_mdp_worker_listener;
/* Open one broker over 1..FLOWMQ_MDP_CAPACITY_MAX distinct worker ROUTERs.
 * Copy listener descriptors; borrow each bound socket and ACL context until close.
 * Poll every listener on the same I/O owner. Config.workers is a shared limit.
 * Each listener authorizes its own workers; config.authorize handles clients.
 * EINVAL for invalid/duplicate inputs, ENOMEM on allocation failure. */
int flowmq_mdp_broker_open_listeners(flowmq_socket_t *clients,
                                     const flowmq_mdp_worker_listener *listeners, size_t count,
                                     const flowmq_mdp_broker_config *config,
                                     flowmq_mdp_broker **out);
typedef enum flowmq_mdp_placement {
  FLOWMQ_MDP_ROUND_ROBIN,
  FLOWMQ_MDP_LEAST_LOAD,
  FLOWMQ_MDP_CACHE_RING,
  FLOWMQ_MDP_EXCLUSIVE
} flowmq_mdp_placement;
/* Called once after authorization and admission checks. Supply a stable hash
 * of the validated business key or return an error to reject before dispatch. */
typedef int (*flowmq_mdp_route_key_fn)(void *context, const flowmq_mdp_body *body,
                                       uint64_t *key_hash);
typedef struct flowmq_mdp_service_policy {
  const char *service;
  flowmq_mdp_placement placement;
  unsigned max_load_percent;         /* cache ring only, 1..100 */
  flowmq_mdp_route_key_fn route_key; /* cache ring only */
  void *context;
} flowmq_mdp_service_policy;
/* On the I/O owner before the first step, install immutable service policies.
 * Borrows descriptors, names and callback contexts until close. Unlisted
 * services use round robin. EINVAL for malformed/duplicate rules, EBUSY once
 * started. Ring placement uses stable listener ordinal and peer identity.
 * Exclusive ownership pauses after loss; resume only after external fencing
 * or confirmation that the previous owner has stopped. */
int flowmq_mdp_broker_set_policies(flowmq_mdp_broker *broker,
                                   const flowmq_mdp_service_policy *policies, size_t count);
int flowmq_mdp_broker_resume_service(flowmq_mdp_broker *broker, const char *service);
/* One bounded nonblocking turn. Drive underlying socket polling separately.
 * Use monotonic milliseconds from one local clock for all operations on an
 * object. BUSY/full peers are retried on later turns. Other errors propagate. */
int flowmq_mdp_broker_step(flowmq_mdp_broker *broker, uint64_t now_ms);
/* Stop dispatching queued calls. Keep stepping until all dispatched calls and
 * replies settle, or explicitly abandon their results at a caller deadline. */
void flowmq_mdp_broker_stop(flowmq_mdp_broker *broker);
int flowmq_mdp_broker_drained(const flowmq_mdp_broker *broker);
void flowmq_mdp_broker_close(flowmq_mdp_broker *broker);

int flowmq_mdp_client_open(flowmq_socket_t *dealer, uint64_t session, size_t capacity,
                           flowmq_mdp_client **out);
/* Enqueue a copy with one total time budget. A queued item that expires before
 * local socket admission is definitively rejected. A deadline after admission
 * yields UNKNOWN. The SDK never retries admitted RPCs automatically. */
int flowmq_mdp_call(flowmq_mdp_client *client, const char *service, const flowmq_mdp_body *body,
                    uint64_t now_ms, uint32_t budget_ms, flowmq_mdp_id *out);
int flowmq_mdp_client_step(flowmq_mdp_client *client, uint64_t now_ms);
/* Copies the result, retaining the pending slot for a retry decision.
 * EBUSY means waiting, ENOENT means the handle is no longer live. */
int flowmq_mdp_peek_result(flowmq_mdp_client *client, flowmq_mdp_id id, flowmq_mdp_result *out);
/* Takes a completed result and releases its pending slot. EBUSY means waiting. */
int flowmq_mdp_take_result(flowmq_mdp_client *client, flowmq_mdp_id id, flowmq_mdp_result *out);
/* Explicitly reattempt a definitive broker rejection before taking its result.
 * Preserves service, payload and total deadline. EBUSY while still pending,
 * EPROTO for a completed business result or unknown outcome. */
int flowmq_mdp_retry(flowmq_mdp_client *client, flowmq_mdp_id id, uint64_t now_ms,
                     flowmq_mdp_id *next);
/* Local cancellation releases the slot. For a submitted call also attempts
 * one nonblocking withdrawal. Already dispatched work may complete later. */
int flowmq_mdp_cancel(flowmq_mdp_client *client, flowmq_mdp_id id);
void flowmq_mdp_client_close(flowmq_mdp_client *client);

/* Choose a heartbeat interval shorter than the broker lease and step often
 * enough for the heartbeat to reach the broker before expiry. */
int flowmq_mdp_worker_open(flowmq_socket_t *dealer, const char *service, uint64_t session,
                           size_t slots, uint32_t heartbeat_ms, flowmq_mdp_worker **out);
/* On refused registration returns EPERM (ACL) or ENOBUFS (worker limit).
 * A retired instance must drain its running tasks before close. */
int flowmq_mdp_worker_step(flowmq_mdp_worker *worker, uint64_t now_ms);
/* Business threads take owned task copies in local dispatch arrival order
 * and may submit completed results in any order.
 * EBUSY means no task. Only an exact active task may complete a slot once. */
int flowmq_mdp_take_task(flowmq_mdp_worker *worker, flowmq_mdp_task *out);
/* Same claim, also return the absolute deadline derived when the worker
 * received DISPATCH. Both outputs are valid only on success. */
int flowmq_mdp_take_task_until(flowmq_mdp_worker *worker, flowmq_mdp_task *out,
                               uint64_t *deadline_ms);
int flowmq_mdp_complete(flowmq_mdp_worker *worker, const flowmq_mdp_task *task, int32_t status,
                        const flowmq_mdp_body *body);
/* Retire a lost session on its I/O owner. Ready tasks are withdrawn; running
 * tasks keep their slots until complete. Re-open with a new session after
 * close succeeds. Quiesce external callers before destroying the object. */
void flowmq_mdp_worker_retire(flowmq_mdp_worker *worker);
/* Thread-safe observation while the caller owns a live worker reference.
 * A retired session accepts no new business work. */
bool flowmq_mdp_worker_is_retired(flowmq_mdp_worker *worker);
int flowmq_mdp_worker_close(flowmq_mdp_worker *worker);

#ifdef __cplusplus
}
#endif
#endif
