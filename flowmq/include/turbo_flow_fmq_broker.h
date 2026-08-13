#ifndef TURBO_FLOW_FMQ_BROKER_H
#define TURBO_FLOW_FMQ_BROKER_H

#include "turbo_flow_config.h"
#include "turbo_flow_fmq_broker_protocol.h"
#include "turbo_flow_protocol.h"
#include "turbo_str_view.h"

#include <errno.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX 127u
#define TURBO_FLOW_FMQ_BROKER_SERVICE_MAX 127u
#define TURBO_FLOW_FMQ_BROKER_MAX_WORKERS 4096u
#define TURBO_FLOW_FMQ_BROKER_MAX_INFLIGHT 65536u
#define TURBO_FLOW_FMQ_BROKER_API_VERSION 1u
#define TURBO_FLOW_TFCW_PROTOCOL_MAJOR 1u
#define TURBO_FLOW_TFCW_PROTOCOL_MINOR 0u
#define TURBO_FLOW_TFCW_HEADER_SIZE 40u
#define TURBO_FLOW_TFCW_MAX_BODY_SIZE (64u * 1024u * 1024u)
#define TURBO_FLOW_TFCW_MAX_FIELDS 64u
#define TURBO_FLOW_TFCW_FIELD_CRITICAL 0x80u
#define TURBO_FLOW_TFCW_FIELD_ID_MASK 0x7fu
#define TURBO_FLOW_FMQ_CREDIT_WORKER_API_VERSION 1u
#define TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_API_VERSION 1u
#define TURBO_FLOW_FMQ_CREDIT_DURABLE_API_VERSION 1u
#define TURBO_FLOW_FMQ_CREDIT_DURABLE_SCHEMA_MAJOR 1u
#define TURBO_FLOW_FMQ_CREDIT_DURABLE_SCHEMA_MINOR 0u
#define TURBO_FLOW_FMQ_CREDIT_DURABLE_STATE_KEY_MAX 1024u
#define TURBO_FLOW_FMQ_EAGAIN (-(EAGAIN))
#define TURBO_FLOW_FMQ_TFCW_GRAPH_API_VERSION 1u
#define TURBO_FLOW_FMQ_TFCW_MODULE "pattern.fmq.credit"
#define TURBO_FLOW_FMQ_TFCW_PRIMITIVE_TYPE "FmqCreditWorker"
#define TURBO_FLOW_FMQ_TFCW_WORKER_INPUT_OPERATION "fmq.credit.worker_input"
#define TURBO_FLOW_FMQ_TFCW_CONTROL_OPERATION "fmq.credit.control"
#define TURBO_FLOW_FMQ_TFCW_DISPATCH_OPERATION "fmq.credit.dispatch"
#define TURBO_FLOW_FMQ_TFCW_COMPLETE_OPERATION "fmq.credit.complete"

typedef struct turbo_flow_fmq_broker_s turbo_flow_fmq_broker_t;

typedef enum turbo_flow_fmq_broker_scheduler_e {
  TURBO_FLOW_FMQ_BROKER_SCHEDULER_LRU = 1
} turbo_flow_fmq_broker_scheduler_t;

typedef enum turbo_flow_fmq_broker_reliability_e {
  TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE = 0,
  TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE,
  TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE
} turbo_flow_fmq_broker_reliability_t;

typedef struct turbo_flow_fmq_broker_config_s {
  size_t size;
  uint32_t version;
  size_t max_workers;
  size_t max_inflight;
  turbo_flow_fmq_broker_scheduler_t scheduler;
  turbo_flow_fmq_broker_reliability_t reliability;
  uint64_t worker_lease_ms;
} turbo_flow_fmq_broker_config_t;

#define TURBO_FLOW_FMQ_BROKER_CONFIG_INIT                                                          \
  {sizeof(turbo_flow_fmq_broker_config_t), TURBO_FLOW_FMQ_BROKER_API_VERSION,      256u, 4096u,    \
   TURBO_FLOW_FMQ_BROKER_SCHEDULER_LRU,    TURBO_FLOW_FMQ_BROKER_RELIABILITY_NONE, 0u}

typedef struct turbo_flow_fmq_broker_dispatch_result_s {
  size_t size;
  uint64_t request_id;
  char worker_id[TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX + 1u];
  char service[TURBO_FLOW_FMQ_BROKER_SERVICE_MAX + 1u];
  turbo_flow_protocol_route_t worker_route;
} turbo_flow_fmq_broker_dispatch_result_t;

#define TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT                                                 \
  {sizeof(turbo_flow_fmq_broker_dispatch_result_t), 0u, {0}, {0}, TURBO_FLOW_PROTOCOL_ROUTE_INIT}

typedef struct turbo_flow_fmq_broker_completion_result_s {
  size_t size;
  uint64_t request_id;
  char worker_id[TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX + 1u];
  turbo_flow_protocol_route_t client_route;
} turbo_flow_fmq_broker_completion_result_t;

#define TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT                                               \
  {sizeof(turbo_flow_fmq_broker_completion_result_t), 0u, {0}, TURBO_FLOW_PROTOCOL_ROUTE_INIT}

typedef enum turbo_flow_fmq_broker_ack_kind_e {
  TURBO_FLOW_FMQ_BROKER_ACK_ACCEPT = 1,
  TURBO_FLOW_FMQ_BROKER_ACK_WORKER_COMPLETION
} turbo_flow_fmq_broker_ack_kind_t;

typedef struct turbo_flow_fmq_broker_ack_result_s {
  size_t size;
  turbo_flow_fmq_broker_ack_kind_t kind;
  uint64_t request_id;
  turbo_flow_protocol_route_t client_route;
} turbo_flow_fmq_broker_ack_result_t;

#define TURBO_FLOW_FMQ_BROKER_ACK_RESULT_INIT                                                      \
  {sizeof(turbo_flow_fmq_broker_ack_result_t), TURBO_FLOW_FMQ_BROKER_ACK_ACCEPT, 0u,               \
   TURBO_FLOW_PROTOCOL_ROUTE_INIT}

typedef enum turbo_flow_fmq_broker_expiry_disposition_e {
  TURBO_FLOW_FMQ_BROKER_EXPIRED_IDLE = 1,
  TURBO_FLOW_FMQ_BROKER_EXPIRED_DROP,
  TURBO_FLOW_FMQ_BROKER_EXPIRED_REQUEUE
} turbo_flow_fmq_broker_expiry_disposition_t;

typedef struct turbo_flow_fmq_broker_expire_result_s {
  size_t size;
  turbo_flow_fmq_broker_expiry_disposition_t disposition;
  uint64_t request_id;
  char worker_id[TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX + 1u];
  char service[TURBO_FLOW_FMQ_BROKER_SERVICE_MAX + 1u];
  turbo_flow_protocol_route_t client_route;
} turbo_flow_fmq_broker_expire_result_t;

#define TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT                                                   \
  {sizeof(turbo_flow_fmq_broker_expire_result_t),                                                  \
   TURBO_FLOW_FMQ_BROKER_EXPIRED_IDLE,                                                             \
   0u,                                                                                             \
   {0},                                                                                            \
   {0},                                                                                            \
   TURBO_FLOW_PROTOCOL_ROUTE_INIT}

typedef struct turbo_flow_fmq_broker_snapshot_s {
  size_t size;
  size_t workers;
  size_t idle_workers;
  size_t busy_workers;
  size_t accepted_requests;
  size_t inflight;
  uint64_t dispatched;
  uint64_t completed;
  uint64_t canceled;
  uint64_t expired_workers;
  uint64_t expired_drops;
  uint64_t expired_requeues;
  uint64_t accept_acks;
  uint64_t worker_completion_acks;
} turbo_flow_fmq_broker_snapshot_t;

#define TURBO_FLOW_FMQ_BROKER_SNAPSHOT_INIT                                                        \
  {sizeof(turbo_flow_fmq_broker_snapshot_t), 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u}

/**
 * Create a single-threaded Chapter 3/4 request-broker state owner.
 *
 * The broker copies worker/service names and process-local FMQ routes. It owns no
 * network socket or payload queue. Calls on one broker must be serialized by the
 * host. Routes are invalid after their FMQ session reconnects and must never be
 * persisted.
 */
CXX_C_API turbo_flow_fmq_broker_t *
turbo_flow_fmq_broker_create(const turbo_flow_fmq_broker_config_t *config);

/**
 * Create a broker from `channels.<name>` in an immutable resolved YAML snapshot.
 *
 * The channel kind must be `fmq_pattern` and its config pattern must be
 * `load_balancer` or `reliable_request`. Unknown fields and unsupported values
 * fail fast.
 */
CXX_C_API int turbo_flow_fmq_broker_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                                    const char *channel_name,
                                                    turbo_flow_fmq_broker_t **out,
                                                    turbo_flow_config_error_t *error);

CXX_C_API void turbo_flow_fmq_broker_destroy(turbo_flow_fmq_broker_t *broker);

/** Register or idempotently refresh one idle worker after its READY message. */
CXX_C_API int turbo_flow_fmq_broker_worker_ready(turbo_flow_fmq_broker_t *broker,
                                                 const char *worker_id, const char *service,
                                                 const turbo_flow_protocol_route_t *worker_route);

/** Register or refresh a leased worker at an explicit monotonic timestamp. */
CXX_C_API int turbo_flow_fmq_broker_worker_ready_at(turbo_flow_fmq_broker_t *broker,
                                                    const char *worker_id, const char *service,
                                                    const turbo_flow_protocol_route_t *worker_route,
                                                    uint64_t now_ms);

/** Refresh a leased worker without changing its service. Busy workers cannot change route. */
CXX_C_API int
turbo_flow_fmq_broker_worker_heartbeat(turbo_flow_fmq_broker_t *broker, const char *worker_id,
                                       const turbo_flow_protocol_route_t *worker_route,
                                       uint64_t now_ms);

/** Remove an idle worker. A busy worker returns TURBO_EBUSY until its dispatch is canceled. */
CXX_C_API int turbo_flow_fmq_broker_worker_remove(turbo_flow_fmq_broker_t *broker,
                                                  const char *worker_id);

/**
 * Select the least-recently-used idle worker and atomically create one in-flight request.
 * The client route is retained locally for the eventual reply.
 */
CXX_C_API int turbo_flow_fmq_broker_dispatch(turbo_flow_fmq_broker_t *broker, const char *service,
                                             uint64_t request_id,
                                             const turbo_flow_protocol_route_t *client_route,
                                             turbo_flow_fmq_broker_dispatch_result_t *result);

/** Dispatch only to a live leased worker using the caller's monotonic timestamp. */
CXX_C_API int turbo_flow_fmq_broker_dispatch_at(turbo_flow_fmq_broker_t *broker,
                                                const char *service, uint64_t request_id,
                                                const turbo_flow_protocol_route_t *client_route,
                                                uint64_t now_ms,
                                                turbo_flow_fmq_broker_dispatch_result_t *result);

/**
 * Record that an external durable owner has committed a request.
 * Call only after that owner returns success. This stores correlation metadata,
 * not payload bytes, and returns the broker accept ACK route.
 */
CXX_C_API int turbo_flow_fmq_broker_record_accept_commit(
    turbo_flow_fmq_broker_t *broker, const char *service, uint64_t request_id,
    const turbo_flow_protocol_route_t *client_route, turbo_flow_fmq_broker_ack_result_t *ack);

/** Dispatch a previously committed request to one live worker. */
CXX_C_API int
turbo_flow_fmq_broker_dispatch_accepted(turbo_flow_fmq_broker_t *broker, uint64_t request_id,
                                        uint64_t now_ms,
                                        turbo_flow_fmq_broker_dispatch_result_t *result);

/**
 * Remove one expired worker. Busy requests are explicitly returned as DROP or REQUEUE.
 * Repeat until TURBO_ENOENT. This function never queues or retransmits a payload itself.
 */
CXX_C_API int turbo_flow_fmq_broker_expire(turbo_flow_fmq_broker_t *broker, uint64_t now_ms,
                                           turbo_flow_fmq_broker_expire_result_t *result);

/**
 * Cancel one in-flight dispatch after a local send failure.
 * The worker becomes idle and the retained client route is returned to the caller.
 */
CXX_C_API int turbo_flow_fmq_broker_cancel(turbo_flow_fmq_broker_t *broker, uint64_t request_id,
                                           turbo_flow_fmq_broker_completion_result_t *result);

/**
 * Complete one worker reply, return its client route, and make the worker idle.
 * This is routing completion, not a transport or business delivery ACK.
 */
CXX_C_API int turbo_flow_fmq_broker_complete(turbo_flow_fmq_broker_t *broker, const char *worker_id,
                                             uint64_t request_id,
                                             turbo_flow_fmq_broker_completion_result_t *result);

/** Convert a successful completion into the second, distinct ACK kind. */
CXX_C_API int
turbo_flow_fmq_broker_completion_ack(const turbo_flow_fmq_broker_completion_result_t *completion,
                                     turbo_flow_fmq_broker_ack_result_t *ack);

CXX_C_API int turbo_flow_fmq_broker_snapshot(const turbo_flow_fmq_broker_t *broker,
                                             turbo_flow_fmq_broker_snapshot_t *out);

typedef enum turbo_flow_tfcw_kind_e {
  TURBO_FLOW_TFCW_READY = 1,
  TURBO_FLOW_TFCW_CREDIT,
  TURBO_FLOW_TFCW_HEARTBEAT,
  TURBO_FLOW_TFCW_JOB,
  TURBO_FLOW_TFCW_COMPLETE,
  TURBO_FLOW_TFCW_FAIL
} turbo_flow_tfcw_kind_t;

/** Stable canonical-LTV field identifiers used by TFCW/1 bodies. */
typedef enum turbo_flow_tfcw_field_id_e {
  TURBO_FLOW_TFCW_FIELD_WORKER_ID = 1,
  TURBO_FLOW_TFCW_FIELD_SERVICE = 2,
  TURBO_FLOW_TFCW_FIELD_GRANT_MESSAGES = 3,
  TURBO_FLOW_TFCW_FIELD_GRANT_BYTES = 4,
  TURBO_FLOW_TFCW_FIELD_LOGICAL_ADDRESS = 5,
  TURBO_FLOW_TFCW_FIELD_METADATA = 6,
  TURBO_FLOW_TFCW_FIELD_PAYLOAD = 7,
  TURBO_FLOW_TFCW_FIELD_FAILURE_CODE = 8,
  TURBO_FLOW_TFCW_FIELD_RETRYABLE = 9
} turbo_flow_tfcw_field_id_t;

/** TFCW/1 application envelope carried as an ordinary FMQ DATA payload. */
typedef struct turbo_flow_tfcw_envelope_s {
  size_t size;
  turbo_flow_tfcw_kind_t kind;
  uint16_t flags;
  uint64_t request_id;
  uint64_t credit_sequence;
  uint64_t sender_timestamp_ms;
  const uint8_t *body;
  size_t body_size;
} turbo_flow_tfcw_envelope_t;

#define TURBO_FLOW_TFCW_ENVELOPE_INIT                                                              \
  {sizeof(turbo_flow_tfcw_envelope_t), 0, 0u, 0u, 0u, 0u, NULL, 0u}

typedef struct turbo_flow_tfcw_fields_s {
  size_t size;
  uint32_t present;
  tstr_v worker_id;
  tstr_v service;
  uint64_t grant_messages;
  uint64_t grant_bytes;
  tstr_v logical_address;
  tstr_v metadata;
  tstr_v payload;
  uint32_t failure_code;
  int retryable;
} turbo_flow_tfcw_fields_t;

#define TURBO_FLOW_TFCW_FIELDS_INIT                                                                \
  {sizeof(turbo_flow_tfcw_fields_t),                                                               \
   0u,                                                                                             \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   0u,                                                                                             \
   0u,                                                                                             \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   0u,                                                                                             \
   0}

#define TURBO_FLOW_TFCW_FIELD_PRESENT(field_id) (UINT32_C(1) << (field_id))

CXX_C_API int turbo_flow_tfcw_encode(const turbo_flow_tfcw_envelope_t *envelope, uint8_t *out,
                                     size_t out_capacity, size_t *out_size);
CXX_C_API int turbo_flow_tfcw_decode(const uint8_t *data, size_t data_size,
                                     turbo_flow_tfcw_envelope_t *out);
/** Parse validated kind-specific fields as zero-copy views into envelope body. */
CXX_C_API int turbo_flow_tfcw_fields_decode(const turbo_flow_tfcw_envelope_t *envelope,
                                            turbo_flow_tfcw_fields_t *out);

typedef struct turbo_flow_fmq_credit_worker_s turbo_flow_fmq_credit_worker_t;

typedef struct turbo_flow_fmq_credit_worker_config_s {
  size_t size;
  uint32_t version;
  size_t max_workers;
  size_t max_inflight;
  uint64_t worker_lease_ms;
  size_t max_credit_messages_per_worker;
  size_t max_credit_bytes_per_worker;
  size_t max_job_bytes;
  turbo_flow_fmq_broker_reliability_t reliability;
} turbo_flow_fmq_credit_worker_config_t;

#define TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT                                                   \
  {sizeof(turbo_flow_fmq_credit_worker_config_t),                                                  \
   TURBO_FLOW_FMQ_CREDIT_WORKER_API_VERSION,                                                       \
   256u,                                                                                           \
   4096u,                                                                                          \
   15000u,                                                                                         \
   64u,                                                                                            \
   67108864u,                                                                                      \
   8388608u,                                                                                       \
   TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE}

typedef struct turbo_flow_fmq_credit_grant_s {
  size_t size;
  const char *worker_id;
  const char *service;
  turbo_flow_protocol_route_t worker_route;
  uint64_t sequence;
  size_t grant_messages;
  size_t grant_bytes;
  uint64_t now_ms;
} turbo_flow_fmq_credit_grant_t;

#define TURBO_FLOW_FMQ_CREDIT_GRANT_INIT                                                           \
  {sizeof(turbo_flow_fmq_credit_grant_t),                                                          \
   NULL,                                                                                           \
   NULL,                                                                                           \
   TURBO_FLOW_PROTOCOL_ROUTE_INIT,                                                                 \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

typedef struct turbo_flow_fmq_credit_worker_snapshot_s {
  size_t size;
  size_t workers;
  size_t available_workers;
  size_t inflight;
  size_t available_messages;
  size_t available_bytes;
  uint64_t grants;
  uint64_t dispatched;
  uint64_t completed;
  uint64_t canceled;
  uint64_t expired_workers;
  uint64_t expired_requests;
} turbo_flow_fmq_credit_worker_snapshot_t;

#define TURBO_FLOW_FMQ_CREDIT_WORKER_SNAPSHOT_INIT                                                 \
  {sizeof(turbo_flow_fmq_credit_worker_snapshot_t), 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u}

/** Create a host-serialized volatile credit owner. It stores correlation metadata, never payload.
 */
CXX_C_API turbo_flow_fmq_credit_worker_t *
turbo_flow_fmq_credit_worker_create(const turbo_flow_fmq_credit_worker_config_t *config);
/** Resolve a strict `credit_worker` FMQ pattern from an immutable YAML snapshot. */
CXX_C_API int turbo_flow_fmq_credit_worker_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_fmq_credit_worker_t **out, turbo_flow_config_error_t *error);
CXX_C_API void turbo_flow_fmq_credit_worker_destroy(turbo_flow_fmq_credit_worker_t *owner);

/** Apply READY/CREDIT incremental tokens. Sequence 1 starts a route generation. */
CXX_C_API int turbo_flow_fmq_credit_worker_grant(turbo_flow_fmq_credit_worker_t *owner,
                                                 const turbo_flow_fmq_credit_grant_t *grant);
/** Refresh a live worker lease without changing credit. */
CXX_C_API int
turbo_flow_fmq_credit_worker_heartbeat(turbo_flow_fmq_credit_worker_t *owner, const char *worker_id,
                                       const turbo_flow_protocol_route_t *worker_route,
                                       uint64_t now_ms);
/** Nonblocking LRU dispatch; no credit returns TURBO_FLOW_FMQ_EAGAIN and stores no request. */
CXX_C_API int turbo_flow_fmq_credit_worker_dispatch(
    turbo_flow_fmq_credit_worker_t *owner, const char *service, uint64_t request_id,
    size_t encoded_job_bytes, const turbo_flow_protocol_route_t *client_route, uint64_t now_ms,
    turbo_flow_fmq_broker_dispatch_result_t *result);
/** Completion/cancel releases correlation but never manufactures replacement credit. */
CXX_C_API int
turbo_flow_fmq_credit_worker_complete(turbo_flow_fmq_credit_worker_t *owner, const char *worker_id,
                                      const turbo_flow_protocol_route_t *worker_route,
                                      uint64_t request_id,
                                      turbo_flow_fmq_broker_completion_result_t *result);
CXX_C_API int
turbo_flow_fmq_credit_worker_cancel(turbo_flow_fmq_credit_worker_t *owner, uint64_t request_id,
                                    turbo_flow_fmq_broker_completion_result_t *result);
/** Expire one request from a stale worker; the worker is removed after its last correlation. */
CXX_C_API int turbo_flow_fmq_credit_worker_expire(turbo_flow_fmq_credit_worker_t *owner,
                                                  uint64_t now_ms,
                                                  turbo_flow_fmq_broker_expire_result_t *result);
CXX_C_API int turbo_flow_fmq_credit_worker_snapshot(const turbo_flow_fmq_credit_worker_t *owner,
                                                    turbo_flow_fmq_credit_worker_snapshot_t *out);

typedef struct turbo_flow_fmq_tfcw_graph_config_s {
  size_t size;
  uint32_t version;
  const char *service;
  const char *resource_name;
  const char *resource_uid;
  const char *owner_name;
} turbo_flow_fmq_tfcw_graph_config_t;

#define TURBO_FLOW_FMQ_TFCW_GRAPH_CONFIG_INIT                                                     \
  {sizeof(turbo_flow_fmq_tfcw_graph_config_t), TURBO_FLOW_FMQ_TFCW_GRAPH_API_VERSION, NULL, NULL, \
   NULL, NULL}

/**
 * Register one optional volatile TFCW/1 transform resource for graph composition.
 *
 * The adapter owns the created credit worker and serializes every owner
 * mutation. service is the fixed JOB dispatch service. All four operations
 * require the configured resource name and inline execution.
 */
CXX_C_API int turbo_flow_fmq_tfcw_register_graph(
    turbo_flow_t *flow, const char *adapter_name,
    const turbo_flow_fmq_tfcw_graph_config_t *graph_config,
    const turbo_flow_fmq_credit_worker_config_t *credit_config);

/**
 * Resolve a credit_worker YAML channel and register its volatile graph owner.
 *
 * The channel must configure service. at_least_once returns TURBO_ENOTSUP
 * until TurboFlow provides a message-owned queue claim projection; it never
 * falls back to volatile dispatch.
 */
CXX_C_API int turbo_flow_fmq_tfcw_register_resolved_graph(
    turbo_flow_t *flow, const char *adapter_name,
    const turbo_flow_fmq_tfcw_graph_config_t *graph_config,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_config_error_t *error);

typedef struct turbo_flow_fmq_credit_settlement_s turbo_flow_fmq_credit_settlement_t;

typedef struct turbo_flow_fmq_credit_settlement_config_s {
  size_t size;
  uint32_t version;
  size_t capacity;
} turbo_flow_fmq_credit_settlement_config_t;

#define TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_CONFIG_INIT                                               \
  {sizeof(turbo_flow_fmq_credit_settlement_config_t),                                              \
   TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_API_VERSION, 4096u}

typedef enum turbo_flow_fmq_credit_settlement_action_e {
  TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_NONE = 0,
  TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_ACK,
  TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_REQUEUE,
  TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_DROP
} turbo_flow_fmq_credit_settlement_action_t;

typedef struct turbo_flow_fmq_credit_settlement_result_s {
  size_t size;
  turbo_flow_fmq_credit_settlement_action_t action;
  uint64_t request_id;
  uint64_t claim_token;
  /** Valid only for ACK after the storage owner committed claim removal. */
  turbo_flow_fmq_broker_ack_result_t completion_ack;
} turbo_flow_fmq_credit_settlement_result_t;

#define TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT                                               \
  {sizeof(turbo_flow_fmq_credit_settlement_result_t), TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_NONE, 0u,   \
   0u, TURBO_FLOW_FMQ_BROKER_ACK_RESULT_INIT}

typedef struct turbo_flow_fmq_credit_settlement_snapshot_s {
  size_t size;
  size_t tracked;
  size_t dispatched;
  size_t pending_ack;
  size_t pending_requeue;
  size_t pending_drop;
  uint64_t settled_acks;
  uint64_t settled_requeues;
  uint64_t settled_drops;
  uint64_t settlement_failures;
} turbo_flow_fmq_credit_settlement_snapshot_t;

#define TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_SNAPSHOT_INIT                                             \
  {sizeof(turbo_flow_fmq_credit_settlement_snapshot_t), 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u}

/**
 * Create a host-serialized coordinator over a borrowed credit owner and claim settler.
 *
 * The coordinator stores correlation metadata only. The storage owner remains the payload fact
 * source, and failed settlement leaves the claim active for retry or backend recovery.
 */
CXX_C_API turbo_flow_fmq_credit_settlement_t *
turbo_flow_fmq_credit_settlement_create(turbo_flow_fmq_credit_worker_t *credit_owner,
                                        const turbo_flow_claim_settler_t *settler,
                                        const turbo_flow_fmq_credit_settlement_config_t *config);

/** Destroy only after every tracked claim is settled; otherwise returns TURBO_EBUSY. */
CXX_C_API int turbo_flow_fmq_credit_settlement_destroy(turbo_flow_fmq_credit_settlement_t *owner);

/** Atomically reserve coordinator metadata around a nonblocking credit dispatch. */
CXX_C_API int turbo_flow_fmq_credit_settlement_dispatch(
    turbo_flow_fmq_credit_settlement_t *owner, uint64_t claim_token, const char *service,
    uint64_t request_id, size_t encoded_job_bytes, const turbo_flow_protocol_route_t *client_route,
    uint64_t now_ms, turbo_flow_fmq_broker_dispatch_result_t *dispatch);

/**
 * Accept a current-generation worker completion, then settle storage.
 * Completion ACK is returned only after the storage ACK succeeds.
 */
CXX_C_API int turbo_flow_fmq_credit_settlement_complete(
    turbo_flow_fmq_credit_settlement_t *owner, const char *worker_id,
    const turbo_flow_protocol_route_t *worker_route, uint64_t request_id,
    turbo_flow_fmq_credit_settlement_result_t *result);

/** Cancel one dispatched correlation and explicitly requeue or drop its storage claim. */
CXX_C_API int
turbo_flow_fmq_credit_settlement_cancel(turbo_flow_fmq_credit_settlement_t *owner,
                                        uint64_t request_id,
                                        turbo_flow_fmq_credit_settlement_action_t action,
                                        turbo_flow_fmq_credit_settlement_result_t *result);

/** Expire one stale credit correlation and apply its configured drop/requeue disposition. */
CXX_C_API int
turbo_flow_fmq_credit_settlement_expire(turbo_flow_fmq_credit_settlement_t *owner, uint64_t now_ms,
                                        turbo_flow_fmq_broker_expire_result_t *expired,
                                        turbo_flow_fmq_credit_settlement_result_t *result);

/** Retry one storage settlement that previously failed; returns TURBO_ENOENT when none remain. */
CXX_C_API int
turbo_flow_fmq_credit_settlement_retry_one(turbo_flow_fmq_credit_settlement_t *owner,
                                           turbo_flow_fmq_credit_settlement_result_t *result);

CXX_C_API int
turbo_flow_fmq_credit_settlement_snapshot(const turbo_flow_fmq_credit_settlement_t *owner,
                                          turbo_flow_fmq_credit_settlement_snapshot_t *out);

typedef struct turbo_flow_fmq_credit_durable_s turbo_flow_fmq_credit_durable_t;

typedef enum turbo_flow_fmq_credit_shutdown_policy_e {
  /** Cancel live credit correlations and atomically requeue/drop their storage claims. */
  TURBO_FLOW_FMQ_CREDIT_SHUTDOWN_REQUEUE = 1,
  /** Persist current state and leave claims for backend restart replay. */
  TURBO_FLOW_FMQ_CREDIT_SHUTDOWN_PRESERVE
} turbo_flow_fmq_credit_shutdown_policy_t;

typedef struct turbo_flow_fmq_credit_durable_config_s {
  size_t size;
  uint32_t version;
  size_t capacity;
  uint32_t max_attempts;
  uint64_t terminal_ttl_ms;
  const char *state_key;
  turbo_flow_fmq_credit_shutdown_policy_t shutdown_policy;
  size_t shutdown_max_steps;
} turbo_flow_fmq_credit_durable_config_t;

#define TURBO_FLOW_FMQ_CREDIT_DURABLE_CONFIG_INIT                                                  \
  {sizeof(turbo_flow_fmq_credit_durable_config_t),                                                 \
   TURBO_FLOW_FMQ_CREDIT_DURABLE_API_VERSION,                                                      \
   4096u,                                                                                          \
   3u,                                                                                             \
   300000u,                                                                                        \
   NULL,                                                                                           \
   TURBO_FLOW_FMQ_CREDIT_SHUTDOWN_REQUEUE,                                                         \
   4096u}

typedef struct turbo_flow_fmq_credit_durable_snapshot_s {
  size_t size;
  size_t records;
  size_t pending;
  size_t inflight;
  size_t completion_outbox;
  size_t completed;
  size_t poisoned;
  uint64_t retries;
  uint64_t duplicate_dispatches;
  uint64_t settlement_failures;
  int quiesced;
} turbo_flow_fmq_credit_durable_snapshot_t;

#define TURBO_FLOW_FMQ_CREDIT_DURABLE_SNAPSHOT_INIT                                                \
  {sizeof(turbo_flow_fmq_credit_durable_snapshot_t), 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0}

typedef struct turbo_flow_fmq_credit_durable_binding_s {
  size_t size;
  const char *storage_channel;
  const turbo_flow_claim_settler_t *settler;
} turbo_flow_fmq_credit_durable_binding_t;

#define TURBO_FLOW_FMQ_CREDIT_DURABLE_BINDING_INIT                                                 \
  {sizeof(turbo_flow_fmq_credit_durable_binding_t), NULL, NULL}

/**
 * Create an at-least-once coordinator backed by one durable claim fact source.
 *
 * The settler must provide load_state/commit_state. The versioned snapshot persists only logical
 * request addresses; claim tokens and protocol routes remain process-local derived state. Missing,
 * malformed, oversized, or unavailable durable state fails creation without a volatile fallback.
 */
CXX_C_API int turbo_flow_fmq_credit_durable_create(
    turbo_flow_fmq_credit_worker_t *credit_owner, const turbo_flow_claim_settler_t *settler,
    const turbo_flow_fmq_credit_durable_config_t *config, turbo_flow_fmq_credit_durable_t **out);
CXX_C_API int turbo_flow_fmq_credit_durable_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_fmq_credit_durable_binding_t *binding,
    turbo_flow_fmq_credit_worker_t **credit_out, turbo_flow_fmq_credit_durable_t **durable_out,
    turbo_flow_config_error_t *error);
CXX_C_API void turbo_flow_fmq_credit_durable_destroy(turbo_flow_fmq_credit_durable_t *owner);

/**
 * Close dispatch admission and apply the configured bounded shutdown policy.
 * The call is idempotent. A storage failure leaves the owner quiesced and retryable; PRESERVE
 * returns after the durable snapshot is already authoritative.
 */
CXX_C_API int turbo_flow_fmq_credit_durable_shutdown(turbo_flow_fmq_credit_durable_t *owner,
                                                     uint64_t now_ms,
                                                     turbo_flow_fmq_credit_durable_snapshot_t *out);

/**
 * Persist an INFLIGHT transition before exposing the selected worker route to the host.
 * A recovered request already at max_attempts is atomically dropped/poisoned and returns
 * TURBO_EALREADY; the caller must not settle that claim again.
 */
CXX_C_API int turbo_flow_fmq_credit_durable_dispatch(
    turbo_flow_fmq_credit_durable_t *owner, uint64_t claim_token,
    const turbo_flow_fmq_broker_logical_address_t *address, uint64_t runtime_request_id,
    const char *service, size_t encoded_job_bytes, const turbo_flow_protocol_route_t *client_route,
    uint64_t now_ms, turbo_flow_fmq_broker_dispatch_result_t *dispatch);

/** Atomically commit completion-outbox state and ACK the storage claim. */
CXX_C_API int turbo_flow_fmq_credit_durable_complete(
    turbo_flow_fmq_credit_durable_t *owner, const char *worker_id,
    const turbo_flow_protocol_route_t *worker_route, uint64_t runtime_request_id, uint64_t now_ms,
    turbo_flow_fmq_credit_settlement_result_t *result);

/** Expire one stale dispatch and atomically requeue it, or poison it at max_attempts. */
CXX_C_API int
turbo_flow_fmq_credit_durable_expire(turbo_flow_fmq_credit_durable_t *owner, uint64_t now_ms,
                                     turbo_flow_fmq_broker_expire_result_t *expired,
                                     turbo_flow_fmq_credit_settlement_result_t *result);

/** Retry one uncertain atomic commit. Exact retries are required to be idempotent. */
CXX_C_API int
turbo_flow_fmq_credit_durable_retry_one(turbo_flow_fmq_credit_durable_t *owner,
                                        turbo_flow_fmq_credit_settlement_result_t *result);

/** Read/confirm completion delivery by durable logical address, never by a stale route. */
CXX_C_API int
turbo_flow_fmq_credit_durable_outbox_next(const turbo_flow_fmq_credit_durable_t *owner,
                                          turbo_flow_fmq_broker_logical_address_t *address);
CXX_C_API int
turbo_flow_fmq_credit_durable_outbox_confirm(turbo_flow_fmq_credit_durable_t *owner,
                                             const turbo_flow_fmq_broker_logical_address_t *address,
                                             uint64_t now_ms);
/** Remove one COMPLETED/POISONED record after terminal_ttl_ms and persist the deletion. */
CXX_C_API int
turbo_flow_fmq_credit_durable_expire_terminal(turbo_flow_fmq_credit_durable_t *owner,
                                              uint64_t now_ms,
                                              turbo_flow_fmq_broker_logical_address_t *address);
CXX_C_API int turbo_flow_fmq_credit_durable_snapshot(const turbo_flow_fmq_credit_durable_t *owner,
                                                     turbo_flow_fmq_credit_durable_snapshot_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_BROKER_H */
