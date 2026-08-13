#ifndef TURBO_FLOW_FMQ_RETRY_H
#define TURBO_FLOW_FMQ_RETRY_H

#include "turbo_flow_config.h"
#include "turbo_flow_fmq_broker_protocol.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_FMQ_RETRY_API_VERSION 1u
#define TURBO_FLOW_FMQ_RETRY_MAX_CAPACITY 65536u
#define TURBO_FLOW_FMQ_RETRY_MAX_ATTEMPTS 1000u

typedef struct turbo_flow_fmq_retry_ledger_s turbo_flow_fmq_retry_ledger_t;

typedef struct turbo_flow_fmq_retry_config_s {
  size_t size;
  uint32_t version;
  size_t capacity;
  uint32_t max_attempts;
  uint64_t terminal_ttl_ms;
} turbo_flow_fmq_retry_config_t;

#define TURBO_FLOW_FMQ_RETRY_CONFIG_INIT                                                           \
  {sizeof(turbo_flow_fmq_retry_config_t), TURBO_FLOW_FMQ_RETRY_API_VERSION, 4096u, 3u, 300000u}

typedef enum turbo_flow_fmq_retry_state_e {
  TURBO_FLOW_FMQ_RETRY_PENDING = 1,
  TURBO_FLOW_FMQ_RETRY_INFLIGHT,
  TURBO_FLOW_FMQ_RETRY_COMPLETED,
  TURBO_FLOW_FMQ_RETRY_POISONED
} turbo_flow_fmq_retry_state_t;

typedef enum turbo_flow_fmq_retry_accept_disposition_e {
  TURBO_FLOW_FMQ_RETRY_ACCEPTED_NEW = 1,
  TURBO_FLOW_FMQ_RETRY_DUPLICATE_PENDING,
  TURBO_FLOW_FMQ_RETRY_DUPLICATE_INFLIGHT,
  TURBO_FLOW_FMQ_RETRY_DUPLICATE_COMPLETED,
  TURBO_FLOW_FMQ_RETRY_DUPLICATE_POISONED
} turbo_flow_fmq_retry_accept_disposition_t;

/** Pointer-free transition record; it can also be restored from a durable owner at startup. */
typedef struct turbo_flow_fmq_retry_record_s {
  size_t size;
  uint32_t version;
  turbo_flow_fmq_broker_logical_address_t address;
  turbo_flow_fmq_retry_state_t state;
  uint32_t attempts;
  uint64_t updated_at_ms;
} turbo_flow_fmq_retry_record_t;

#define TURBO_FLOW_FMQ_RETRY_RECORD_INIT                                                           \
  {sizeof(turbo_flow_fmq_retry_record_t),                                                          \
   TURBO_FLOW_FMQ_RETRY_API_VERSION,                                                               \
   TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT,                                                     \
   TURBO_FLOW_FMQ_RETRY_PENDING,                                                                   \
   0u,                                                                                             \
   0u}

typedef struct turbo_flow_fmq_retry_accept_result_s {
  size_t size;
  turbo_flow_fmq_retry_accept_disposition_t disposition;
  turbo_flow_fmq_retry_record_t record;
} turbo_flow_fmq_retry_accept_result_t;

#define TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT                                                    \
  {sizeof(turbo_flow_fmq_retry_accept_result_t), TURBO_FLOW_FMQ_RETRY_ACCEPTED_NEW,                \
   TURBO_FLOW_FMQ_RETRY_RECORD_INIT}

typedef struct turbo_flow_fmq_retry_snapshot_s {
  size_t size;
  size_t records;
  size_t pending;
  size_t inflight;
  size_t completed;
  size_t poisoned;
  uint64_t duplicate_accepts;
  uint64_t retries;
  uint64_t poison_transitions;
  uint64_t terminal_evictions;
} turbo_flow_fmq_retry_snapshot_t;

#define TURBO_FLOW_FMQ_RETRY_SNAPSHOT_INIT                                                         \
  {sizeof(turbo_flow_fmq_retry_snapshot_t), 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u}

/**
 * Create a bounded, host-serialized retry/dedup state owner.
 *
 * The ledger stores no payload and performs no I/O. Calls on one ledger must be serialized by
 * the host. All timestamped calls use one caller-owned monotonic millisecond clock.
 */
CXX_C_API turbo_flow_fmq_retry_ledger_t *
turbo_flow_fmq_retry_ledger_create(const turbo_flow_fmq_retry_config_t *config);

/** Create a retry ledger from a resolved YAML `reliable_request` channel. */
CXX_C_API int turbo_flow_fmq_retry_ledger_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_fmq_retry_ledger_t **out, turbo_flow_config_error_t *error);

CXX_C_API void turbo_flow_fmq_retry_ledger_destroy(turbo_flow_fmq_retry_ledger_t *ledger);

/**
 * Accept a logical request or classify an idempotent duplicate.
 *
 * A duplicate never resets attempts or terminal TTL. TURBO_ENOSPC means the caller must apply
 * backpressure or explicitly expire terminal records; active records are never evicted.
 */
CXX_C_API int
turbo_flow_fmq_retry_ledger_accept(turbo_flow_fmq_retry_ledger_t *ledger,
                                   const turbo_flow_fmq_broker_logical_address_t *address,
                                   uint64_t now_ms, turbo_flow_fmq_retry_accept_result_t *result);

/** Move one PENDING request to INFLIGHT and increment its bounded attempt count. */
CXX_C_API int
turbo_flow_fmq_retry_ledger_begin_attempt(turbo_flow_fmq_retry_ledger_t *ledger,
                                          const turbo_flow_fmq_broker_logical_address_t *address,
                                          uint64_t now_ms, turbo_flow_fmq_retry_record_t *record);

/**
 * Finish one INFLIGHT attempt.
 *
 * Success moves to COMPLETED. Failure returns to PENDING while attempts remain, otherwise it
 * moves to POISONED. In this API the in-memory ledger is the fact owner; the returned record is
 * an observation of the committed memory transition, not proof of an external durable commit.
 */
CXX_C_API int turbo_flow_fmq_retry_ledger_finish_attempt(
    turbo_flow_fmq_retry_ledger_t *ledger, const turbo_flow_fmq_broker_logical_address_t *address,
    int success, uint64_t now_ms, turbo_flow_fmq_retry_record_t *record);

/** Read one record without advancing time or changing state. */
CXX_C_API int
turbo_flow_fmq_retry_ledger_get(const turbo_flow_fmq_retry_ledger_t *ledger,
                                const turbo_flow_fmq_broker_logical_address_t *address,
                                turbo_flow_fmq_retry_record_t *record);

/**
 * Restore one record previously committed by an external fact owner.
 *
 * Exact duplicates are idempotent. A conflicting record for the same logical address returns
 * TURBO_EALREADY. Restored INFLIGHT records remain explicit: the host must reconcile the old
 * attempt and call finish_attempt rather than receiving an implicit retry.
 */
CXX_C_API int turbo_flow_fmq_retry_ledger_restore(turbo_flow_fmq_retry_ledger_t *ledger,
                                                  const turbo_flow_fmq_retry_record_t *record);

/** Remove one COMPLETED/POISONED record whose terminal TTL elapsed. */
CXX_C_API int turbo_flow_fmq_retry_ledger_expire_one(turbo_flow_fmq_retry_ledger_t *ledger,
                                                     uint64_t now_ms,
                                                     turbo_flow_fmq_retry_record_t *expired);

CXX_C_API int turbo_flow_fmq_retry_ledger_snapshot(const turbo_flow_fmq_retry_ledger_t *ledger,
                                                   turbo_flow_fmq_retry_snapshot_t *snapshot);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_RETRY_H */
