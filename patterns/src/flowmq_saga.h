#ifndef FLOWMQ_SAGA_H
#define FLOWMQ_SAGA_H

/**
 * @file flowmq_saga.h
 * SAGA distributed transaction coordinator.
 * 
 * Implements SAGA pattern for distributed transactions with compensation:
 * - Forward execution: T1 → T2 → T3 → ... → Tn (commit if all succeed)
 * - Compensation: Cn-1 ← Cn-2 ← ... ← C1 (rollback on failure)
 * 
 * Each SAGA transaction consists of:
 * - Multiple steps (transactions T1, T2, ...)
 * - Corresponding compensations (C1, C2, ...)
 * - Global SAGA ID and correlation tracking
 * 
 * State machine:
 *   FLOWMQ_SAGA_RUNTIME_PENDING → FLOWMQ_SAGA_RUNTIME_EXECUTING → FLOWMQ_SAGA_RUNTIME_COMMITTED (all steps succeed)
 *                                 → FLOWMQ_SAGA_RUNTIME_COMPENSATING (step failed)
 *                                 → FLOWMQ_SAGA_RUNTIME_ABORTED (compensation complete)
 */

#include "flowmq_protocol_esb.h"
#include "turbo_error.h"
#include "turbo_hash_map.h"
#include "turbo_vec.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Maximum concurrent SAGA transactions per coordinator */
#define FLOWMQ_SAGA_MAX_TRANSACTIONS 256u

/* Maximum steps per SAGA transaction */
#define FLOWMQ_SAGA_MAX_STEPS 32u

/* SAGA transaction state */
typedef enum flowmq_saga_state_e {
  FLOWMQ_SAGA_RUNTIME_PENDING = 0,        /* Created, not started */
  FLOWMQ_SAGA_RUNTIME_EXECUTING = 1,      /* Forward execution in progress */
  FLOWMQ_SAGA_RUNTIME_COMMITTED = 2,      /* All steps succeeded */
  FLOWMQ_SAGA_RUNTIME_COMPENSATING = 3,   /* Compensation in progress */
  FLOWMQ_SAGA_RUNTIME_ABORTED = 4,        /* Compensation complete */
} flowmq_saga_state_t;

/* Single step execution result */
typedef enum flowmq_saga_step_result_e {
  FLOWMQ_SAGA_STEP_PENDING = 0,     /* Not executed */
  FLOWMQ_SAGA_STEP_SUCCESS = 1,     /* Executed successfully */
  FLOWMQ_SAGA_STEP_FAILED = 2,      /* Execution failed */
  FLOWMQ_SAGA_STEP_COMPENSATED = 3, /* Compensation executed */
} flowmq_saga_step_result_t;

/* SAGA step definition */
typedef struct flowmq_saga_step_s {
  uint32_t step_id;                   /* Step identifier (0-based) */
  tstr_t service_name;                /* Target service name (owned) */
  tstr_t transaction_payload;         /* Transaction request (owned) */
  tstr_t compensation_payload;        /* Compensation request (owned) */
  
  flowmq_saga_step_result_t result;   /* Execution result */
  int error_code;                     /* Error code if failed */
  uint64_t executed_ns;               /* Execution timestamp */
  uint64_t compensated_ns;            /* Compensation timestamp */
} flowmq_saga_step_t;

/**
 * SAGA transaction.
 * Manages forward execution and compensation of distributed transaction.
 */
typedef struct flowmq_saga_transaction_s {
  tstr_t saga_id;                     /* Unique SAGA identifier (owned) */
  uint64_t created_ns;                /* Creation timestamp */
  uint64_t deadline_ns;               /* Absolute deadline */
  
  flowmq_saga_state_t state;          /* Transaction state */
  uint32_t total_steps;               /* Total number of steps */
  uint32_t current_step;              /* Current step cursor (0-based):
                                           forward-executes next step while in
                                           FLOWMQ_SAGA_RUNTIME_EXECUTING; compensation
                                           runs reverse from this index while in
                                           FLOWMQ_SAGA_RUNTIME_COMPENSATING */
  
  turbo_vec_t steps;                  /* Vec<flowmq_saga_step_t> */
  
  void *user_context;                 /* User-defined context */
} flowmq_saga_transaction_t;

/**
 * SAGA coordinator.
 * Manages multiple concurrent SAGA transactions.
 */
typedef struct flowmq_saga_coordinator_s {
  turbo_hash_map_t transactions;      /* Map<saga_id -> flowmq_saga_transaction_t*> */
  uint32_t max_transactions;          /* Capacity limit */
  uint64_t next_saga_id;              /* Monotonic ID generator */
} flowmq_saga_coordinator_t;

/* Initialize SAGA coordinator */
int flowmq_saga_coordinator_init(flowmq_saga_coordinator_t *coordinator,
                                 uint32_t max_transactions);

/* Destroy SAGA coordinator */
void flowmq_saga_coordinator_destroy(flowmq_saga_coordinator_t *coordinator);

/* Create new SAGA transaction */
int flowmq_saga_coordinator_create(flowmq_saga_coordinator_t *coordinator,
                                  uint32_t num_steps,
                                  uint64_t deadline_ns,
                                  void *user_context,
                                  tstr_t *out_saga_id);

/* Add step to SAGA transaction (must be in PENDING state) */
int flowmq_saga_coordinator_add_step(flowmq_saga_coordinator_t *coordinator,
                                    const tstr_t *saga_id,
                                    const tstr_t *service_name,
                                    const tstr_t *transaction_payload,
                                    const tstr_t *compensation_payload);

/* Start SAGA execution (move from PENDING → EXECUTING) */
int flowmq_saga_coordinator_start(flowmq_saga_coordinator_t *coordinator,
                                 const tstr_t *saga_id);

/* Record step execution result */
int flowmq_saga_coordinator_record_step_result(flowmq_saga_coordinator_t *coordinator,
                                              const tstr_t *saga_id,
                                              uint32_t step_id,
                                              int success,
                                              int error_code,
                                              uint64_t timestamp_ns);

/* Trigger compensation for failed SAGA */
int flowmq_saga_coordinator_compensate(flowmq_saga_coordinator_t *coordinator,
                                      const tstr_t *saga_id);

/* Record compensation result */
int flowmq_saga_coordinator_record_compensation(flowmq_saga_coordinator_t *coordinator,
                                               const tstr_t *saga_id,
                                               uint32_t step_id,
                                               uint64_t timestamp_ns);

/**
 * Peek current compensation target.
 * 
 * Returns the next step that still needs compensation when the saga is in
 * FLOWMQ_SAGA_RUNTIME_COMPENSATING state. Outputs are owned by caller.
 *
 * - step_id: index of step to compensate
 * - service_name: copied service name
 * - compensation_payload: copied compensation payload
 *
 * The saga state and cursor are not changed by this call.
 */
int flowmq_saga_coordinator_peek_compensation(flowmq_saga_coordinator_t *coordinator,
                                              const tstr_t *saga_id,
                                              uint32_t *out_step_id,
                                              tstr_t *out_service_name,
                                              tstr_t *out_compensation_payload);

/* Get SAGA transaction state */
int flowmq_saga_coordinator_get_state(flowmq_saga_coordinator_t *coordinator,
                                     const tstr_t *saga_id,
                                     flowmq_saga_state_t *out_state);

/* Get current step being executed */
int flowmq_saga_coordinator_get_current_step(flowmq_saga_coordinator_t *coordinator,
                                            const tstr_t *saga_id,
                                            uint32_t *out_step_id);

/* Abort SAGA transaction (cancel or cleanup) */
int flowmq_saga_coordinator_abort(flowmq_saga_coordinator_t *coordinator,
                                 const tstr_t *saga_id);

/* Get statistics */
void flowmq_saga_coordinator_stats(flowmq_saga_coordinator_t *coordinator,
                                  uint32_t *out_active,
                                  uint32_t *out_capacity);

/* Process timeouts (returns number of timed out transactions) */
uint32_t flowmq_saga_coordinator_process_timeouts(flowmq_saga_coordinator_t *coordinator,
                                                 uint64_t now_ns);

/* Cleanup SAGA transaction resources */
void flowmq_saga_transaction_cleanup(flowmq_saga_transaction_t *txn);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_SAGA_H */
