#ifndef TURBO_FLOW_FMQ_DEPLOYMENT_H
#define TURBO_FLOW_FMQ_DEPLOYMENT_H

#include "platform.h"
#include "turbo_uuid.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_FMQ_DEPLOYMENT_AUTHORITY_MAX 255u
#define TURBO_FLOW_FMQ_DEPLOYMENT_MEMBER_ID_MAX 127u
#define TURBO_FLOW_FMQ_DEPLOYMENT_DOMAIN_MAX 127u
#define TURBO_FLOW_FMQ_DEPLOYMENT_ROUTE_MAX 255u
#define TURBO_FLOW_FMQ_DEPLOYMENT_ENDPOINT_MAX 1023u
#define TURBO_FLOW_FMQ_DEPLOYMENT_MAX_MEMBERS 256u

typedef enum turbo_flow_fmq_member_state_e {
  TURBO_FLOW_FMQ_MEMBER_READY = 1,
  TURBO_FLOW_FMQ_MEMBER_DRAINING = 2
} turbo_flow_fmq_member_state_t;

typedef enum turbo_flow_fmq_membership_command_kind_e {
  TURBO_FLOW_FMQ_MEMBERSHIP_JOIN = 1,
  TURBO_FLOW_FMQ_MEMBERSHIP_HEARTBEAT = 2,
  TURBO_FLOW_FMQ_MEMBERSHIP_DRAIN = 3,
  TURBO_FLOW_FMQ_MEMBERSHIP_LEAVE = 4
} turbo_flow_fmq_membership_command_kind_t;

/** Pointer-free member value suitable for a host management transport. */
typedef struct turbo_flow_fmq_deployment_member_s {
  size_t size;
  char member_id[TURBO_FLOW_FMQ_DEPLOYMENT_MEMBER_ID_MAX + 1u];
  turbo_uuid_t incarnation_id;
  char failure_domain[TURBO_FLOW_FMQ_DEPLOYMENT_DOMAIN_MAX + 1u];
  char logical_route[TURBO_FLOW_FMQ_DEPLOYMENT_ROUTE_MAX + 1u];
  char endpoint[TURBO_FLOW_FMQ_DEPLOYMENT_ENDPOINT_MAX + 1u];
  uint32_t priority;
  turbo_flow_fmq_member_state_t state;
  /** Relative lease remaining at snapshot time; never compare across snapshots. */
  uint64_t lease_remaining_ms;
} turbo_flow_fmq_deployment_member_t;

#define TURBO_FLOW_FMQ_DEPLOYMENT_MEMBER_INIT                                                      \
  {sizeof(turbo_flow_fmq_deployment_member_t),                                                     \
   {0},                                                                                            \
   {{0}},                                                                                          \
   {0},                                                                                            \
   {0},                                                                                            \
   {0},                                                                                            \
   0u,                                                                                             \
   TURBO_FLOW_FMQ_MEMBER_READY,                                                                    \
   0u}

/** Stable logical route plus the fencing token required by remote peers. */
typedef struct turbo_flow_fmq_route_snapshot_s {
  size_t size;
  char logical_route[TURBO_FLOW_FMQ_DEPLOYMENT_ROUTE_MAX + 1u];
  uint64_t route_generation;
  int has_primary;
  char primary_member_id[TURBO_FLOW_FMQ_DEPLOYMENT_MEMBER_ID_MAX + 1u];
  turbo_uuid_t primary_incarnation_id;
  char primary_failure_domain[TURBO_FLOW_FMQ_DEPLOYMENT_DOMAIN_MAX + 1u];
  char endpoint[TURBO_FLOW_FMQ_DEPLOYMENT_ENDPOINT_MAX + 1u];
} turbo_flow_fmq_route_snapshot_t;

#define TURBO_FLOW_FMQ_ROUTE_SNAPSHOT_INIT                                                         \
  {sizeof(turbo_flow_fmq_route_snapshot_t), {0}, 0u, 0, {0}, {{0}}, {0}, {0}}

typedef uint64_t (*turbo_flow_fmq_deployment_now_ms_fn)(void *ctx);

typedef struct turbo_flow_fmq_deployment_config_s {
  size_t size;
  char authority_id[TURBO_FLOW_FMQ_DEPLOYMENT_AUTHORITY_MAX + 1u];
  /** Monotonic fencing epoch assigned by the host authority/election service. */
  uint64_t authority_epoch;
  uint64_t lease_timeout_ms;
  size_t member_capacity;
  /** Optional deterministic monotonic clock used by tests and embedded hosts. */
  turbo_flow_fmq_deployment_now_ms_fn now_ms;
  void *now_ms_ctx;
} turbo_flow_fmq_deployment_config_t;

#define TURBO_FLOW_FMQ_DEPLOYMENT_CONFIG_INIT                                                      \
  {sizeof(turbo_flow_fmq_deployment_config_t),                                                     \
   "fmq-deployment",                                                                               \
   1u,                                                                                             \
   15000u,                                                                                         \
   TURBO_FLOW_FMQ_DEPLOYMENT_MAX_MEMBERS,                                                          \
   NULL,                                                                                           \
   NULL}

typedef struct turbo_flow_fmq_membership_command_s {
  size_t size;
  turbo_flow_fmq_membership_command_kind_t kind;
  char authority_id[TURBO_FLOW_FMQ_DEPLOYMENT_AUTHORITY_MAX + 1u];
  uint64_t authority_epoch;
  turbo_uuid_t authority_incarnation_id;
  /** Exact optimistic concurrency token from the latest registry snapshot. */
  uint64_t expected_registry_version;
  turbo_flow_fmq_deployment_member_t member;
} turbo_flow_fmq_membership_command_t;

#define TURBO_FLOW_FMQ_MEMBERSHIP_COMMAND_INIT                                                     \
  {                                                                                                \
      sizeof(turbo_flow_fmq_membership_command_t),                                                 \
      TURBO_FLOW_FMQ_MEMBERSHIP_JOIN,                                                              \
      {0},                                                                                         \
      0u,                                                                                          \
      {{0}},                                                                                       \
      0u,                                                                                          \
      TURBO_FLOW_FMQ_DEPLOYMENT_MEMBER_INIT}

typedef struct turbo_flow_fmq_membership_result_s {
  size_t size;
  uint64_t version_before;
  uint64_t version_after;
  uint64_t route_generation;
  int primary_changed;
} turbo_flow_fmq_membership_result_t;

#define TURBO_FLOW_FMQ_MEMBERSHIP_RESULT_INIT                                                      \
  {sizeof(turbo_flow_fmq_membership_result_t), 0u, 0u, 0u, 0}

/** Caller-owned arrays make the snapshot bounded and serialization-neutral. */
typedef struct turbo_flow_fmq_deployment_snapshot_s {
  size_t size;
  char authority_id[TURBO_FLOW_FMQ_DEPLOYMENT_AUTHORITY_MAX + 1u];
  uint64_t authority_epoch;
  turbo_uuid_t authority_incarnation_id;
  uint64_t registry_version;
  turbo_flow_fmq_deployment_member_t *members;
  size_t member_capacity;
  size_t member_count;
  turbo_flow_fmq_route_snapshot_t *routes;
  size_t route_capacity;
  size_t route_count;
} turbo_flow_fmq_deployment_snapshot_t;

#define TURBO_FLOW_FMQ_DEPLOYMENT_SNAPSHOT_INIT                                                    \
  {sizeof(turbo_flow_fmq_deployment_snapshot_t), {0}, 0u, {{0}}, 0u, NULL, 0u, 0u, NULL, 0u, 0u}

typedef enum turbo_flow_fmq_snapshot_relation_e {
  TURBO_FLOW_FMQ_SNAPSHOT_SAME = 1,
  TURBO_FLOW_FMQ_SNAPSHOT_NEWER = 2,
  TURBO_FLOW_FMQ_SNAPSHOT_STALE = 3,
  TURBO_FLOW_FMQ_SNAPSHOT_SPLIT_BRAIN = 4
} turbo_flow_fmq_snapshot_relation_t;

typedef struct turbo_flow_fmq_deployment_controller_s turbo_flow_fmq_deployment_controller_t;

/**
 * Create a single-lane membership/route owner.
 *
 * A fresh authority incarnation is generated with TurboUtils UUIDv7. The host
 * must externally fence authority_epoch before exposing this owner; commands
 * from another epoch/incarnation fail without changing state.
 */
CXX_C_API int
turbo_flow_fmq_deployment_controller_create(const turbo_flow_fmq_deployment_config_t *config,
                                            turbo_flow_fmq_deployment_controller_t **out);

CXX_C_API void
turbo_flow_fmq_deployment_controller_destroy(turbo_flow_fmq_deployment_controller_t *controller);

/** Apply one join/heartbeat/drain/leave command on the serialized owner lane. */
CXX_C_API int turbo_flow_fmq_deployment_apply(turbo_flow_fmq_deployment_controller_t *controller,
                                              const turbo_flow_fmq_membership_command_t *command,
                                              turbo_flow_fmq_membership_result_t *result);

/** Expire leases and elect replacement primaries. Returns expired member count. */
CXX_C_API int turbo_flow_fmq_deployment_tick(turbo_flow_fmq_deployment_controller_t *controller,
                                             size_t *expired_count);

/** Resolve one route and return its current fencing token. */
CXX_C_API int
turbo_flow_fmq_deployment_resolve(const turbo_flow_fmq_deployment_controller_t *controller,
                                  const char *logical_route,
                                  turbo_flow_fmq_route_snapshot_t *route);

/** Copy a normalized, bounded snapshot into caller-owned storage. */
CXX_C_API int
turbo_flow_fmq_deployment_snapshot(const turbo_flow_fmq_deployment_controller_t *controller,
                                   turbo_flow_fmq_deployment_snapshot_t *snapshot);

/**
 * Compare normalized snapshots.
 *
 * The same authority epoch with different incarnations, or the same registry
 * version with different content, is reported as SPLIT_BRAIN.
 */
CXX_C_API int
turbo_flow_fmq_deployment_snapshot_compare(const turbo_flow_fmq_deployment_snapshot_t *current,
                                           const turbo_flow_fmq_deployment_snapshot_t *candidate,
                                           turbo_flow_fmq_snapshot_relation_t *relation);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_DEPLOYMENT_H */
