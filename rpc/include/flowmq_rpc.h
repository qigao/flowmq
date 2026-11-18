#ifndef FLOWMQ_RPC_H
#define FLOWMQ_RPC_H
#include "flowmq_mdp.h"
#include "flowmq_rpc_scheduler.h"
#include <json_parser.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Encoding limits, in addition to FLOWMQ_MDP_PAYLOAD_MAX per document.
 * Depth counts edges from an encoded value's root. */
enum { FLOWMQ_RPC_BATCH_MAX = 32, FLOWMQ_RPC_METHODS_MAX = 1024, FLOWMQ_RPC_DEPTH_MAX = 128 };
enum {
  FLOWMQ_RPC_PARSE_ERROR = -32700,
  FLOWMQ_RPC_INVALID_REQUEST = -32600,
  FLOWMQ_RPC_METHOD_NOT_FOUND = -32601,
  FLOWMQ_RPC_INVALID_PARAMS = -32602,
  FLOWMQ_RPC_INTERNAL_ERROR = -32603,
  FLOWMQ_RPC_OVERLOADED = -32000
};

typedef struct flowmq_rpc_request {
  const char *method;
  const json_value_t *params; /* optional object or array */
  const json_value_t *id;     /* NULL = notification; a JSON null is an explicit ID */
} flowmq_rpc_request;

/* Encode and enqueue one request or a same-service batch (including a batch
 * of one). Inputs are borrowed for this call. Batch IDs must be distinct.
 * Numeric IDs use exact decimal tokens for correlation.
 * EINVAL for invalid descriptors, EMSGSIZE for a batch or document over its
 * limit. The returned MDP handle supports the usual polling, cancel and retry. */
int flowmq_rpc_call(flowmq_mdp_client *client, const char *service,
                    const flowmq_rpc_request *requests, size_t count, bool batch, uint64_t now_ms,
                    uint32_t budget_ms, flowmq_mdp_id *handle);

/* Decode a completed MDP RESULT for this request set. Only the original IDs
 * must remain valid here. Checks every response ID once, in any batch order.
 * On success *out owns the response object/array (json_free), or is NULL
 * for notifications. EPROTO for malformed/mismatched replies. A nonzero MDP
 * completion status is propagated. Inspect REJECTED/UNKNOWN before decoding. */
int flowmq_rpc_decode(const flowmq_mdp_result *result, const flowmq_rpc_request *requests,
                      size_t count, bool batch, json_value_t **out);

typedef struct flowmq_rpc_invocation {
  const json_value_t *params;
  const json_value_t *id;
  flowmq_mdp_id request_uid; /* shared by all members of this MDP call */
} flowmq_rpc_invocation;
typedef struct flowmq_rpc_output {
  json_value_t *value;
  const char *message;
} flowmq_rpc_output;

/* Runs synchronously on the business thread that calls execute. All request
 * fields are borrowed for the callback. Return zero with an owned JSON result
 * (use json_create_null for null), or a JSON-RPC error code with optional
 * owned error data and a message valid until execute returns. Ownership of
 * output.value transfers to the adapter in both cases. A successful
 * notification may omit its result. Keep output within the MDP payload budget. */
typedef int32_t (*flowmq_rpc_handler)(void *context, const flowmq_rpc_invocation *request,
                                      flowmq_rpc_output *output);
typedef struct flowmq_rpc_method {
  const char *name;
  flowmq_rpc_handler handler;
  void *context;
} flowmq_rpc_method;
typedef struct flowmq_rpc_server flowmq_rpc_server;

/* Validate a unique immutable method table. The table, names and contexts
 * must outlive the server. Concurrent callers synchronize mutable handler
 * state. Returns EINVAL for invalid tables and ENOMEM on allocation failure. */
int flowmq_rpc_server_open(const flowmq_rpc_method *methods, size_t count, flowmq_rpc_server **out);
void flowmq_rpc_server_close(flowmq_rpc_server *server);

/* Claim one task from the worker, invoke its methods on this thread, then
 * complete the slot even for malformed input, notifications or an oversized
 * reply. Returns EBUSY when no task is available, otherwise the local MDP
 * claim/completion status. For external effects use an
 * application idempotency key in params and the service's own transaction. */
int flowmq_rpc_execute(const flowmq_rpc_server *server, flowmq_mdp_worker *worker);

typedef struct flowmq_rpc_engine flowmq_rpc_engine;
typedef struct flowmq_rpc_ticket {
  flowmq_mdp_id request;
  uint64_t broker, worker, generation;
  size_t slot, member;
} flowmq_rpc_ticket;
typedef enum flowmq_rpc_event_kind {
  FLOWMQ_RPC_START,
  FLOWMQ_RPC_CHILD,
  FLOWMQ_RPC_CANCEL,
  FLOWMQ_RPC_PROGRESS
} flowmq_rpc_event_kind;
typedef struct flowmq_rpc_event {
  flowmq_rpc_event_kind kind;
  flowmq_rpc_ticket ticket;
  uint64_t deadline_ms;
  const char *method;
  flowmq_rpc_invocation input;
  uint32_t child_tag;
  flowmq_mdp_result child;
  int cancel_status;
} flowmq_rpc_event;
/* A classifier may borrow the input and assign a canonical key backed by
 * input or persistent application storage. Return zero or a JSON-RPC error.
 * The scheduler copies the key before the next callback. */
typedef int32_t (*flowmq_rpc_classify_fn)(void *, const char *, const flowmq_rpc_invocation *,
                                          flowmq_rpc_work_class *);
typedef struct flowmq_rpc_engine_config {
  flowmq_rpc_scheduler_config schedule;
  const char *const *deferred_methods;
  size_t deferred_count;
  size_t child_capacity, children_per_request;
  uint64_t (*now_ms)(void *context);
  flowmq_rpc_classify_fn classify;
  void *context;
} flowmq_rpc_engine_config;
typedef struct flowmq_rpc_branch {
  const char *service;
  flowmq_rpc_request request;
  uint32_t tag;
} flowmq_rpc_branch;
/* Open on one owner that drives ALL engine APIs. Borrow the worker, optional
 * sync method server, optional downstream client, deferred names and contexts
 * until close. Give the engine exclusive task intake for this worker. Drive
 * socket progress independently; serialize all downstream client operations
 * on its I/O owner. Use the worker's monotonic clock. A downstream client
 * requires 1..FLOWMQ_MDP_CAPACITY_MAX child slots and a positive per-call cap.
 * Duplicate / conflicting method names or invalid capacities return EINVAL. */
int flowmq_rpc_engine_open(flowmq_mdp_worker *worker, const flowmq_rpc_server *sync_methods,
                           flowmq_mdp_client *downstream, const flowmq_rpc_engine_config *config,
                           flowmq_rpc_engine **out);
/* Admit at most one MDP task, run one scheduled member, or expose START /
 * CHILD / CANCEL to the owner. PROGRESS reports internal work such as advancing
 * a synchronous batch member. EBUSY means this turn found no runnable work.
 * The ticket and raw child result copy by
 * value. Input DOMs are borrowed until finish/cancel acknowledgement. External
 * threads receive owned data and send a copied ticket back to the owner.
 * CANCEL asks the service to stop local work, settle its context and finish
 * the ticket. The key/slot stay occupied until that acknowledgement. */
int flowmq_rpc_engine_poll(flowmq_rpc_engine *engine, flowmq_rpc_event *event);
/* Earliest absolute deadline of admitted work still awaiting a decision.
 * EBUSY if none, EINVAL for invalid arguments. Combine with socket/heartbeat
 * deadlines when deciding how long to poll. */
int flowmq_rpc_engine_next_deadline(const flowmq_rpc_engine *engine, uint64_t *deadline_ms);
/* Atomically enqueue a group of single downstream calls for a live member.
 * Tags identify sibling branches and must be distinct among outstanding
 * children. Admission is bounded and inherits the parent's remaining budget.
 * On failure newly queued calls are withdrawn before the owner drives I/O. */
int flowmq_rpc_engine_fork(flowmq_rpc_engine *engine, flowmq_rpc_ticket ticket,
                           const flowmq_rpc_branch *branches, size_t count);
/* Complete a member once, or acknowledge CANCEL after external work has
 * quiesced. Always consumes output.value, including on ENOENT (stale ticket).
 * Any remaining child waits are withdrawn locally; remote effects may have
 * committed. Completion after expiry settles the parent with ETIMEDOUT.
 * Returns local settlement status. */
int flowmq_rpc_engine_finish(flowmq_rpc_engine *engine, flowmq_rpc_ticket ticket, int32_t code,
                             flowmq_rpc_output output);
/* Stop starting new documents while admitted methods continue, including
 * their downstream branches. Poll returns shutdown completions for unclaimed
 * MDP work received before broker dispatch quiesces. */
void flowmq_rpc_engine_drain(flowmq_rpc_engine *engine);
/* Also cancel admitted work (e.g. at the deployment shutdown deadline).
 * Poll and finish CANCEL events before close. Close returns EBUSY while any
 * request remains, preserving the engine. Stop broker dispatch and drive MDP
 * reply progress before destroying the worker. */
void flowmq_rpc_engine_stop(flowmq_rpc_engine *engine);
int flowmq_rpc_engine_close(flowmq_rpc_engine *engine);

#ifdef __cplusplus
}
#endif
#endif
