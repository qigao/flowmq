#ifndef TURBO_FLOW_FMQ_MANAGEMENT_H
#define TURBO_FLOW_FMQ_MANAGEMENT_H

#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_fmq_management_protocol.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_TFMP_AUTHORITY_ID_MAX 255u
#define TURBO_FLOW_TFMP_MANAGEMENT_MAX_PAGE_ITEMS (TURBO_FLOW_TFMP_MAX_FIELDS - 4u)
#define TURBO_FLOW_TFMP_MANAGEMENT_CLIENT_ID_MAX 255u
#define TURBO_FLOW_TFMP_MANAGEMENT_DEDUP_MAX 4096u
#define TURBO_FLOW_TFMP_MANAGEMENT_REFERENCE_MAX 255u
#define TURBO_FLOW_TFMP_MANAGEMENT_DEFAULT_INFLIGHT_PER_TARGET 8u
#define TURBO_FLOW_TFMP_MANAGEMENT_STORE_KEY_MAX 1024u
#define TURBO_FLOW_TFMP_MANAGEMENT_EVENT_MAX 4096u
#define TURBO_FLOW_TFMP_MANAGEMENT_EVENT_TOPIC_MAX 31u

/** Immutable local policy copied into one management owner at creation. */
typedef struct turbo_flow_tfmp_management_config_s {
  size_t size;
  char authority_id[TURBO_FLOW_TFMP_AUTHORITY_ID_MAX + 1u];
  uint32_t max_request_bytes;
  uint32_t max_reply_bytes;
  uint32_t max_page_items;
  size_t dedup_capacity;
  uint64_t dedup_ttl_ms;
  /** Accepted/running operation bound; zero inherits dedup_capacity. */
  size_t operation_capacity;
  /** Bounded event journal size; zero disables events and replay. */
  size_t event_capacity;
} turbo_flow_tfmp_management_config_t;

#define TURBO_FLOW_TFMP_MANAGEMENT_CONFIG_INIT                                                     \
  {sizeof(turbo_flow_tfmp_management_config_t),                                                    \
   "fmq-management",                                                                               \
   TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE,                                                               \
   TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE,                                                               \
   TURBO_FLOW_TFMP_MANAGEMENT_MAX_PAGE_ITEMS,                                                      \
   TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX,                                                        \
   300000u,                                                                                        \
   0u,                                                                                             \
   0u}

/** Strict projection of one `kind: fmq_management` YAML channel. */
typedef struct turbo_flow_tfmp_management_channel_config_s {
  size_t size;
  uint16_t protocol_major;
  uint16_t protocol_minor;
  turbo_flow_tfmp_management_config_t service;
  char rpc_adapter[TURBO_FLOW_TFMP_MANAGEMENT_REFERENCE_MAX + 1u];
  char event_adapter[TURBO_FLOW_TFMP_MANAGEMENT_REFERENCE_MAX + 1u];
  char operation_store[TURBO_FLOW_TFMP_MANAGEMENT_REFERENCE_MAX + 1u];
  char event_replay_store[TURBO_FLOW_TFMP_MANAGEMENT_REFERENCE_MAX + 1u];
  uint32_t max_inflight_per_target;
  uint64_t shutdown_timeout_ms;
} turbo_flow_tfmp_management_channel_config_t;

#define TURBO_FLOW_TFMP_MANAGEMENT_CHANNEL_CONFIG_INIT                                             \
  {sizeof(turbo_flow_tfmp_management_channel_config_t),                                            \
   TURBO_FLOW_TFMP_PROTOCOL_MAJOR,                                                                 \
   TURBO_FLOW_TFMP_PROTOCOL_MINOR,                                                                 \
   TURBO_FLOW_TFMP_MANAGEMENT_CONFIG_INIT,                                                         \
   {0},                                                                                            \
   {0},                                                                                            \
   "memory",                                                                                       \
   {0},                                                                                            \
   TURBO_FLOW_TFMP_MANAGEMENT_DEFAULT_INFLIGHT_PER_TARGET,                                        \
   5000u}

typedef struct turbo_flow_tfmp_management_service_s turbo_flow_tfmp_management_service_t;

typedef struct turbo_flow_tfmp_management_store_binding_s {
  /** Set to sizeof(turbo_flow_tfmp_management_store_binding_t). */
  size_t size;
  /** Borrowed atomic blob store; must outlive the management service. */
  const turbo_flow_blob_store_t *store;
  /** Stable provider key, copied by the service. */
  const char *key;
} turbo_flow_tfmp_management_store_binding_t;

#define TURBO_FLOW_TFMP_MANAGEMENT_STORE_BINDING_INIT                                              \
  {sizeof(turbo_flow_tfmp_management_store_binding_t), NULL, NULL}

typedef enum turbo_flow_tfmp_reconcile_outcome_e {
  /** The desired side effect is already visible; finish without reapplying it. */
  TURBO_FLOW_TFMP_RECONCILE_APPLIED = 1,
  /** The old precondition still holds; the idempotent goal-state command may run once. */
  TURBO_FLOW_TFMP_RECONCILE_NOT_APPLIED = 2,
  /** State advanced to a different value; fail the operation without mutation. */
  TURBO_FLOW_TFMP_RECONCILE_CONFLICT = 3
} turbo_flow_tfmp_reconcile_outcome_t;

/** Immutable typed command view passed to a resource-owner reconciler. */
typedef struct turbo_flow_tfmp_reconcile_request_s {
  size_t size;
  uint16_t command_type;
  /** Borrowed UTF-8 view valid only during inspect(). */
  const char *target_uid;
  size_t target_uid_size;
  int has_expected_generation;
  uint64_t expected_generation;
  const uint8_t *payload;
  size_t payload_size;
} turbo_flow_tfmp_reconcile_request_t;

#define TURBO_FLOW_TFMP_RECONCILE_REQUEST_INIT                                                     \
  {sizeof(turbo_flow_tfmp_reconcile_request_t), 0u, NULL, 0u, 0, 0u, NULL, 0u}

typedef struct turbo_flow_tfmp_reconcile_result_s {
  size_t size;
  turbo_flow_tfmp_reconcile_outcome_t outcome;
  uint64_t generation;
  uint64_t observed_generation;
} turbo_flow_tfmp_reconcile_result_t;

#define TURBO_FLOW_TFMP_RECONCILE_RESULT_INIT                                                      \
  {sizeof(turbo_flow_tfmp_reconcile_result_t), 0, 0u, 0u}

typedef int (*turbo_flow_tfmp_reconcile_supports_fn)(void *ctx, uint16_t command_type);
typedef int (*turbo_flow_tfmp_reconcile_inspect_fn)(
    void *ctx, turbo_flow_t *flow, const turbo_flow_tfmp_reconcile_request_t *request,
    turbo_flow_tfmp_reconcile_result_t *result);

/** Borrowed resource-owner strategy; callbacks must remain valid for service lifetime. */
typedef struct turbo_flow_tfmp_reconcile_binding_s {
  size_t size;
  turbo_flow_tfmp_reconcile_supports_fn supports;
  turbo_flow_tfmp_reconcile_inspect_fn inspect;
  void *ctx;
} turbo_flow_tfmp_reconcile_binding_t;

#define TURBO_FLOW_TFMP_RECONCILE_BINDING_INIT                                                     \
  {sizeof(turbo_flow_tfmp_reconcile_binding_t), NULL, NULL, NULL}

/**
 * Create one single-thread management owner.
 *
 * The config is copied. A fresh UUIDv7 incarnation is generated through
 * TurboUtils; entropy failure is returned without fallback. Configured durable
 * replay may subsequently restore the persisted journal incarnation. The new
 * owner starts in TURBO_FLOW_TFMP_OWNER_STARTING.
 */
CXX_C_API int
turbo_flow_tfmp_management_service_create(const turbo_flow_tfmp_management_config_t *config,
                                          turbo_flow_tfmp_management_service_t **out);

/**
 * Create an owner with an atomic durable operation store.
 *
 * Existing snapshots are strictly decoded before the owner is returned. A
 * missing key starts empty; malformed, oversized, or unavailable storage fails
 * creation without a volatile fallback. This entry point does not persist the
 * event journal; use the resolved-channel entry point for durable event replay.
 */
CXX_C_API int turbo_flow_tfmp_management_service_create_with_store(
    const turbo_flow_tfmp_management_config_t *config,
    const turbo_flow_tfmp_management_store_binding_t *binding,
    turbo_flow_tfmp_management_service_t **out);

/**
 * Create an owner from a resolved management channel projection.
 *
 * `operation_store: memory` requires a NULL binding and exposes volatile ACK
 * only. A referenced `kind: blob_store` channel requires a non-NULL binding
 * resolved and owned by the host; missing or extra bindings fail fast. An
 * external `event_replay_store` must name that same operation store so each
 * durable operation transition and derived event use one atomic blob commit.
 * The binding and its store must outlive the returned service.
 */
CXX_C_API int turbo_flow_tfmp_management_service_create_configured(
    const turbo_flow_tfmp_management_channel_config_t *config,
    const turbo_flow_tfmp_management_store_binding_t *binding,
    turbo_flow_tfmp_management_service_t **out);

/**
 * Bind an owner-specific inspector for durable resource side effects.
 *
 * FLOW_PAUSE/RESUME/DRAIN and POOL_RESIZE have built-in typed inspectors.
 * RESOURCE_QUIESCE/RESUME and ENDPOINT_REPLACE require this binding before
 * their command descriptors advertise durable acceptance. Set once while the
 * service is STARTING and before binding the target.
 */
CXX_C_API int turbo_flow_tfmp_management_service_set_reconciler(
    turbo_flow_tfmp_management_service_t *service,
    const turbo_flow_tfmp_reconcile_binding_t *binding);

/** Resolve and strictly validate one immutable YAML management channel. */
CXX_C_API int turbo_flow_tfmp_management_channel_config_resolve(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_tfmp_management_channel_config_t *out, turbo_flow_config_error_t *error);

CXX_C_API void
turbo_flow_tfmp_management_service_destroy(turbo_flow_tfmp_management_service_t *service);

/**
 * Bind the single borrowed Flow target exposed by this phase-1 owner.
 *
 * Binding is accepted once while the owner is STARTING. `target_uid` is
 * copied; `flow` remains caller-owned and must outlive the service. The host
 * must serialize execute calls with Flow lifecycle, registry reset, and
 * destruction.
 */
CXX_C_API int
turbo_flow_tfmp_management_service_bind_target(turbo_flow_tfmp_management_service_t *service,
                                               const char *target_uid, turbo_flow_t *flow);

/**
 * Apply a legal owner lifecycle transition.
 *
 * STARTING -> READY|FAILED, READY -> DRAINING|FAILED, and
 * DRAINING -> STOPPED|FAILED are accepted. Reapplying the current state is
 * idempotent; terminal states cannot transition.
 */
CXX_C_API int
turbo_flow_tfmp_management_service_set_state(turbo_flow_tfmp_management_service_t *service,
                                             turbo_flow_tfmp_owner_state_t state);

/** Return the current owner state, or zero for an invalid service. */
CXX_C_API turbo_flow_tfmp_owner_state_t
turbo_flow_tfmp_management_service_state(const turbo_flow_tfmp_management_service_t *service);

/**
 * Execute exactly one TFMP request and encode one correlated response.
 *
 * This function is single-thread owner-lane only. It serves capability/health
 * and, when a target is bound, TARGET_LIST/GET and
 * RESOURCE_LIST/GET/DOCUMENT_GET. Unsupported kinds, schema failures, owner
 * errors, and malformed frames become stable protocol error replies; they are
 * not returned as transport failures. If no request kind can be recovered,
 * the response uses TURBO_FLOW_TFMP_PROTOCOL_ERROR with correlation ID zero.
 *
 * `out` is unchanged on TURBO_ENOSPC and `out_len` receives the required size.
 * A TURBO_OK return only means a response was encoded; callers must
 * inspect its TFMP status and disposition.
 */
CXX_C_API int
turbo_flow_tfmp_management_service_execute(turbo_flow_tfmp_management_service_t *service,
                                           const uint8_t *request, size_t request_size,
                                           uint8_t *out, size_t capacity, size_t *out_len);

/**
 * Claim and execute one accepted volatile or durable operation on the owner lane.
 *
 * The acceptance response is emitted before this call can mutate the target. A
 * For a durable operation, RUNNING is committed before target mutation and the
 * terminal state is committed afterward. A terminal commit failure returns the
 * provider error and moves the owner to FAILED; restart will not blindly replay
 * the uncertain mutation. A successful return means one operation reached a
 * terminal state; callers use OPERATION_GET to read that result. TURBO_ENOENT
 * means no operation is ready. Calls must be serialized with execute and Flow
 * lifecycle operations.
 */
CXX_C_API int
turbo_flow_tfmp_management_service_run_one(turbo_flow_tfmp_management_service_t *service);

/**
 * Inspect and settle one durable RUNNING operation recovered after a crash.
 *
 * APPLIED commits success without repeating the side effect. NOT_APPLIED runs
 * the same idempotent goal-state command only while its generation precondition
 * still holds. CONFLICT commits a terminal conflict. Inspection/storage errors
 * leave the recovery record pending for an explicit retry. TURBO_ENOENT means
 * no recovery is pending.
 */
CXX_C_API int
turbo_flow_tfmp_management_service_reconcile_one(turbo_flow_tfmp_management_service_t *service);

/**
 * Thin TurboFlow stage for a dedicated FMQ ROUTER graph.
 *
 * `ctx` is a management service. The stage replaces the request payload with
 * exactly one correlated TFMP response while preserving the FMQ identity route
 * used by the downstream ROUTER adapter. The service and stage must share one owner
 * lane; this function does not add a second lock or state owner.
 */
CXX_C_API int turbo_flow_tfmp_management_stage(turbo_flow_msg_t *msg, void *ctx);

/**
 * Encode the first journaled event whose sequence is greater than `after_sequence`.
 *
 * The topic is `tfmp/1/event/<category>`. TURBO_ENOENT means no newer event;
 * TURBO_ERANGE means the bounded journal no longer contains the requested
 * sequence. `topic_size` and `out_size` receive required sizes on ENOSPC.
 */
CXX_C_API int turbo_flow_tfmp_management_event_next(
    const turbo_flow_tfmp_management_service_t *service, uint64_t after_sequence, char *topic,
    size_t topic_capacity, size_t *topic_size, uint8_t *out, size_t capacity, size_t *out_size);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_MANAGEMENT_H */
