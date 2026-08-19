/**
 * @file flowmq_saga.c
 * SAGA distributed transaction coordinator implementation.
 */

#include "flowmq_saga.h"
#include "tlog.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/* Hash function for saga_id (tstr_t). */
static size_t saga_id_hash(const void *key, size_t key_size, void *ctx) {
  (void)key_size;
  (void)ctx;
  const tstr_t id = *(const tstr_t *)key;
  const size_t len = tstr_len(id);
  size_t hash = 5381;
  for (size_t i = 0; i < len; i++) {
    hash = ((hash << 5) + hash) + (uint8_t)id[i];
  }
  return hash;
}

/* Equality function for saga_id (tstr_t). */
static bool saga_id_eq(const void *left, const void *right, size_t key_size, void *ctx) {
  (void)key_size;
  (void)ctx;
  const tstr_t left_id = *(const tstr_t *)left;
  const tstr_t right_id = *(const tstr_t *)right;
  return tstr_cmp(left_id, right_id) == 0;
}

/* Cleanup single step. */
static void saga_step_cleanup(flowmq_saga_step_t *step) {
  if (!step) return;
  tstr_free(step->service_name);
  tstr_free(step->transaction_payload);
  tstr_free(step->compensation_payload);
}

static flowmq_saga_transaction_t *flowmq_saga_find_transaction(
    flowmq_saga_coordinator_t *coordinator,
    const tstr_t *saga_id,
    int *out_rc) {
  if (!coordinator || !saga_id) {
    if (out_rc) *out_rc = TURBO_EINVAL;
    return NULL;
  }

  flowmq_saga_transaction_t **txn_ptr =
      (flowmq_saga_transaction_t **)turbo_hash_map_get(&coordinator->transactions, saga_id);
  if (!txn_ptr || !*txn_ptr) {
    if (out_rc) *out_rc = TURBO_ENOENT;
    return NULL;
  }

  if (out_rc) *out_rc = TURBO_OK;
  return *txn_ptr;
}

void flowmq_saga_transaction_cleanup(flowmq_saga_transaction_t *txn) {
  if (!txn) return;

  tstr_free(txn->saga_id);

  for (size_t i = 0; i < turbo_vec_size(&txn->steps); i++) {
    flowmq_saga_step_t *step = (flowmq_saga_step_t *)turbo_vec_at(&txn->steps, i);
    saga_step_cleanup(step);
  }
  turbo_vec_destroy(&txn->steps);
}

int flowmq_saga_coordinator_init(flowmq_saga_coordinator_t *coordinator,
                                 uint32_t max_transactions) {
  if (!coordinator) return TURBO_EINVAL;
  if (max_transactions == 0 || max_transactions > FLOWMQ_SAGA_MAX_TRANSACTIONS) {
    return TURBO_EINVAL;
  }

  coordinator->max_transactions = max_transactions;
  coordinator->next_saga_id = 1;

  int rc = turbo_hash_map_init(&coordinator->transactions,
                               sizeof(tstr_t),
                               sizeof(flowmq_saga_transaction_t *),
                               saga_id_hash,
                               saga_id_eq,
                               NULL);
  if (rc != TURBO_OK) {
    return rc;
  }

  return TURBO_OK;
}

void flowmq_saga_coordinator_destroy(flowmq_saga_coordinator_t *coordinator) {
  if (!coordinator) return;

  /* Cleanup all transactions. */
  for (size_t slot = 0; slot < turbo_hash_map_capacity(&coordinator->transactions); ++slot) {
    flowmq_saga_transaction_t **txn_ptr =
        (flowmq_saga_transaction_t **)turbo_hash_map_value_at(&coordinator->transactions, slot);
    if (!txn_ptr || !*txn_ptr) {
      continue;
    }
    flowmq_saga_transaction_cleanup(*txn_ptr);
    free(*txn_ptr);
  }

  turbo_hash_map_destroy(&coordinator->transactions);
}

int flowmq_saga_coordinator_create(flowmq_saga_coordinator_t *coordinator,
                                  uint32_t num_steps,
                                  uint64_t deadline_ns,
                                  void *user_context,
                                  tstr_t *out_saga_id) {
  if (!coordinator || !out_saga_id) return TURBO_EINVAL;
  if (num_steps == 0 || num_steps > FLOWMQ_SAGA_MAX_STEPS) return TURBO_EINVAL;

  /* Check capacity. */
  if (turbo_hash_map_size(&coordinator->transactions) >= coordinator->max_transactions) {
    return TURBO_ENOSPC;
  }

  /* Allocate transaction. */
  flowmq_saga_transaction_t *txn = (flowmq_saga_transaction_t *)calloc(1, sizeof(*txn));
  if (!txn) return TURBO_ENOMEM;

  /* Generate unique SAGA ID. */
  char id_buf[64];
  snprintf(id_buf, sizeof(id_buf), "saga-%" PRIu64, coordinator->next_saga_id++);
  txn->saga_id = tstr_dup(id_buf);
  if (!txn->saga_id) {
    free(txn);
    return TURBO_ENOMEM;
  }

  txn->created_ns = turbo_hrtime();
  txn->deadline_ns = deadline_ns;
  txn->state = FLOWMQ_SAGA_RUNTIME_PENDING;
  txn->total_steps = num_steps;
  txn->current_step = 0;
  txn->user_context = user_context;

  /* Initialize steps vector. */
  int rc = turbo_vec_init(&txn->steps, sizeof(flowmq_saga_step_t));
  if (rc != TURBO_OK) {
    tstr_free(txn->saga_id);
    free(txn);
    return rc;
  }
  rc = turbo_vec_reserve(&txn->steps, num_steps);
  if (rc != TURBO_OK) {
    flowmq_saga_transaction_cleanup(txn);
    free(txn);
    return rc;
  }

  /* Insert into coordinator map. */
  rc = turbo_hash_map_put(&coordinator->transactions, &txn->saga_id, &txn);
  if (rc != TURBO_OK) {
    flowmq_saga_transaction_cleanup(txn);
    free(txn);
    return rc;
  }

  /* Return SAGA ID. */
  *out_saga_id = tstr_dup(txn->saga_id);
  if (!*out_saga_id) {
    flowmq_saga_coordinator_abort(coordinator, &txn->saga_id);
    return TURBO_ENOMEM;
  }

  TLOG_DEBUG("Created SAGA transaction: %.*s, steps=%u, deadline=%" PRIu64,
             (int)tstr_len(txn->saga_id), txn->saga_id, num_steps, deadline_ns);

  return TURBO_OK;
}

int flowmq_saga_coordinator_add_step(flowmq_saga_coordinator_t *coordinator,
                                    const tstr_t *saga_id,
                                    const tstr_t *service_name,
                                    const tstr_t *transaction_payload,
                                    const tstr_t *compensation_payload) {
  if (!coordinator || !saga_id || !service_name ||
      !transaction_payload || !compensation_payload) {
    return TURBO_EINVAL;
  }

  int rc = TURBO_OK;
  flowmq_saga_transaction_t *txn = flowmq_saga_find_transaction(coordinator, saga_id, &rc);
  if (rc != TURBO_OK) return rc;

  /* Must be in PENDING state. */
  if (txn->state != FLOWMQ_SAGA_RUNTIME_PENDING) {
    return TURBO_EINVAL;
  }

  /* Check if all steps already added. */
  if (txn->steps.size >= txn->total_steps) {
    return TURBO_EINVAL;
  }

  /* Create step. */
  flowmq_saga_step_t step = {0};
  step.step_id = (uint32_t)txn->steps.size;
  step.service_name = tstr_dup(*service_name);
  if (!step.service_name) return TURBO_ENOMEM;
  step.transaction_payload = tstr_dup(*transaction_payload);
  if (!step.transaction_payload) {
    tstr_free(step.service_name);
    return TURBO_ENOMEM;
  }
  step.compensation_payload = tstr_dup(*compensation_payload);
  if (!step.compensation_payload) {
    tstr_free(step.service_name);
    tstr_free(step.transaction_payload);
    return TURBO_ENOMEM;
  }
  step.result = FLOWMQ_SAGA_STEP_PENDING;
  step.error_code = TURBO_OK;
  step.executed_ns = 0;
  step.compensated_ns = 0;

  /* Add to vector. */
  rc = turbo_vec_push(&txn->steps, &step);
  if (rc != TURBO_OK) {
    saga_step_cleanup(&step);
    return rc;
  }

  TLOG_DEBUG("Added step %u to SAGA %.*s: service=%.*s",
             step.step_id,
             (int)tstr_len(*saga_id), *saga_id,
             (int)tstr_len(*service_name), *service_name);

  return TURBO_OK;
}

int flowmq_saga_coordinator_start(flowmq_saga_coordinator_t *coordinator,
                                 const tstr_t *saga_id) {
  if (!coordinator || !saga_id) return TURBO_EINVAL;

  int rc = TURBO_OK;
  flowmq_saga_transaction_t *txn = flowmq_saga_find_transaction(coordinator, saga_id, &rc);
  if (rc != TURBO_OK) return rc;

  /* Must be in PENDING state. */
  if (txn->state != FLOWMQ_SAGA_RUNTIME_PENDING) {
    return TURBO_EINVAL;
  }

  /* All steps must be added. */
  if (txn->steps.size != txn->total_steps) {
    return TURBO_EINVAL;
  }

  /* Move to EXECUTING. */
  txn->state = FLOWMQ_SAGA_RUNTIME_EXECUTING;
  txn->current_step = 0;

  TLOG_INFO("Started SAGA transaction: %.*s",
            (int)tstr_len(*saga_id), *saga_id);

  return TURBO_OK;
}

int flowmq_saga_coordinator_record_step_result(flowmq_saga_coordinator_t *coordinator,
                                              const tstr_t *saga_id,
                                              uint32_t step_id,
                                              int success,
                                              int error_code,
                                              uint64_t timestamp_ns) {
  if (!coordinator || !saga_id) return TURBO_EINVAL;

  int rc = TURBO_OK;
  flowmq_saga_transaction_t *txn = flowmq_saga_find_transaction(coordinator, saga_id, &rc);
  if (rc != TURBO_OK) return rc;

  /* Must be in EXECUTING state. */
  if (txn->state != FLOWMQ_SAGA_RUNTIME_EXECUTING) {
    return TURBO_EINVAL;
  }

  /* Validate step_id. */
  if (step_id != txn->current_step || step_id >= txn->total_steps) {
    return TURBO_EINVAL;
  }

  /* Record result. */
  flowmq_saga_step_t *step = (flowmq_saga_step_t *)turbo_vec_at(&txn->steps, step_id);
  step->result = success ? FLOWMQ_SAGA_STEP_SUCCESS : FLOWMQ_SAGA_STEP_FAILED;
  step->error_code = error_code;
  step->executed_ns = timestamp_ns;

  if (success) {
    /* Move to next step. */
    txn->current_step++;

    /* Check if all steps completed. */
    if (txn->current_step >= txn->total_steps) {
      txn->state = FLOWMQ_SAGA_RUNTIME_COMMITTED;
      TLOG_INFO("SAGA committed: %.*s", (int)tstr_len(*saga_id), *saga_id);
    }
  } else {
    /* Failure - move to COMPENSATING. */
    txn->state =
        (step_id == 0 ? FLOWMQ_SAGA_RUNTIME_ABORTED : FLOWMQ_SAGA_RUNTIME_COMPENSATING);
    if (step_id > 0) {
      txn->current_step = step_id - 1u;
    } else {
      txn->current_step = 0u;
    }
    TLOG_WARN("SAGA step %u failed (error=%d), starting compensation: %.*s",
              step_id, error_code, (int)tstr_len(*saga_id), *saga_id);
  }

  return TURBO_OK;
}

int flowmq_saga_coordinator_compensate(flowmq_saga_coordinator_t *coordinator,
                                      const tstr_t *saga_id) {
  if (!coordinator || !saga_id) return TURBO_EINVAL;

  int rc = TURBO_OK;
  flowmq_saga_transaction_t *txn = flowmq_saga_find_transaction(coordinator, saga_id, &rc);
  if (rc != TURBO_OK) return rc;

  /* Must be in COMPENSATING state. */
  if (txn->state != FLOWMQ_SAGA_RUNTIME_COMPENSATING) {
    return TURBO_EINVAL;
  }

  if (txn->current_step >= txn->total_steps || txn->steps.size == 0u) {
    txn->state = FLOWMQ_SAGA_RUNTIME_ABORTED;
    return TURBO_OK;
  }

  /* Find the next step that still needs compensation. */
  for (;;) {
    flowmq_saga_step_t *step = (flowmq_saga_step_t *)turbo_vec_at(&txn->steps, txn->current_step);
    if (!step) {
      return TURBO_EINVAL;
    }

    if (step->result == FLOWMQ_SAGA_STEP_SUCCESS) {
      TLOG_INFO("SAGA compensation in progress: %.*s current_step=%u (service=%.*s)",
                (int)tstr_len(*saga_id), *saga_id, txn->current_step,
                (int)tstr_len(step->service_name), step->service_name);
      return TURBO_OK;
    }
    if (step->result == FLOWMQ_SAGA_STEP_COMPENSATED) {
      if (txn->current_step == 0u) {
        txn->state = FLOWMQ_SAGA_RUNTIME_ABORTED;
        TLOG_INFO("SAGA compensation completed: %.*s",
                  (int)tstr_len(*saga_id), *saga_id);
        return TURBO_OK;
      }
      txn->current_step--;
      continue;
    }
    return TURBO_EPROTO;
  }
}

int flowmq_saga_coordinator_record_compensation(flowmq_saga_coordinator_t *coordinator,
                                               const tstr_t *saga_id,
                                               uint32_t step_id,
                                               uint64_t timestamp_ns) {
  if (!coordinator || !saga_id) return TURBO_EINVAL;

  int rc = TURBO_OK;
  flowmq_saga_transaction_t *txn = flowmq_saga_find_transaction(coordinator, saga_id, &rc);
  if (rc != TURBO_OK) return rc;

  /* Must be in COMPENSATING state. */
  if (txn->state != FLOWMQ_SAGA_RUNTIME_COMPENSATING) {
    return TURBO_EINVAL;
  }

  if (txn->current_step >= txn->total_steps) {
    return TURBO_EINVAL;
  }

  /* Must compensate the current cursor in reverse order to keep rollback sequence strict. */
  if (step_id != txn->current_step) {
    return TURBO_EINVAL;
  }

  /* Record compensation. */
  flowmq_saga_step_t *step = (flowmq_saga_step_t *)turbo_vec_at(&txn->steps, step_id);
  if (step->result != FLOWMQ_SAGA_STEP_SUCCESS) {
    return TURBO_EINVAL;
  }
  step->result = FLOWMQ_SAGA_STEP_COMPENSATED;
  step->compensated_ns = timestamp_ns;

  if (txn->current_step == 0u) {
    txn->state = FLOWMQ_SAGA_RUNTIME_ABORTED;
    return TURBO_OK;
  }

  if (txn->current_step > 0u) {
    txn->current_step--;
  }

  if (flowmq_saga_coordinator_compensate(coordinator, saga_id) == TURBO_EPROTO) {
    return TURBO_EPROTO;
  }

  if (txn->state == FLOWMQ_SAGA_RUNTIME_ABORTED) {
    TLOG_INFO("SAGA aborted (all compensations done): %.*s",
              (int)tstr_len(*saga_id), *saga_id);
    return TURBO_OK;
  }

  if (txn->current_step < txn->total_steps) {
    flowmq_saga_step_t *next =
        (flowmq_saga_step_t *)turbo_vec_at(&txn->steps, txn->current_step);
    if (!next) {
      return TURBO_EINVAL;
    }
    if (next->result != FLOWMQ_SAGA_STEP_SUCCESS) {
      return TURBO_EPROTO;
    }
  }

  TLOG_INFO("SAGA compensation progressed: %.*s, next_step=%u",
            (int)tstr_len(*saga_id), *saga_id, txn->current_step);
  return TURBO_OK;
}

int flowmq_saga_coordinator_peek_compensation(flowmq_saga_coordinator_t *coordinator,
                                             const tstr_t *saga_id,
                                             uint32_t *out_step_id,
                                             tstr_t *out_service_name,
                                             tstr_t *out_compensation_payload) {
  if (!coordinator || !saga_id || !out_step_id ||
      !out_service_name || !out_compensation_payload) {
    return TURBO_EINVAL;
  }

  int rc = TURBO_OK;
  flowmq_saga_transaction_t *txn = flowmq_saga_find_transaction(coordinator, saga_id, &rc);
  if (rc != TURBO_OK) return rc;

  /* Must be in compensating state. */
  if (txn->state != FLOWMQ_SAGA_RUNTIME_COMPENSATING) {
    return TURBO_EINVAL;
  }

  if (txn->steps.size == 0u) {
    return TURBO_EINVAL;
  }

  uint32_t step_idx = txn->current_step;
  for (;;) {
    if (step_idx >= txn->total_steps) {
      return TURBO_EINVAL;
    }

    flowmq_saga_step_t *step = (flowmq_saga_step_t *)turbo_vec_at(&txn->steps, step_idx);
    if (!step) {
      return TURBO_EINVAL;
    }

    if (step->result == FLOWMQ_SAGA_STEP_SUCCESS) {
      *out_step_id = step_idx;
      *out_service_name = tstr_dup(step->service_name);
      if (!*out_service_name) {
        return TURBO_ENOMEM;
      }
      *out_compensation_payload = tstr_dup(step->compensation_payload);
      if (!*out_compensation_payload) {
        tstr_free(*out_service_name);
        return TURBO_ENOMEM;
      }
      return TURBO_OK;
    }

    if (step->result != FLOWMQ_SAGA_STEP_COMPENSATED) {
      return TURBO_EPROTO;
    }

    if (step_idx == 0u) {
      return TURBO_EPROTO;
    }

    step_idx--;
  }
}

int flowmq_saga_coordinator_get_state(flowmq_saga_coordinator_t *coordinator,
                                     const tstr_t *saga_id,
                                     flowmq_saga_state_t *out_state) {
  if (!coordinator || !saga_id || !out_state) return TURBO_EINVAL;

  int rc = TURBO_OK;
  flowmq_saga_transaction_t *txn = flowmq_saga_find_transaction(coordinator, saga_id, &rc);
  if (rc != TURBO_OK) return rc;

  *out_state = txn->state;
  return TURBO_OK;
}

int flowmq_saga_coordinator_get_current_step(flowmq_saga_coordinator_t *coordinator,
                                            const tstr_t *saga_id,
                                            uint32_t *out_step_id) {
  if (!coordinator || !saga_id || !out_step_id) return TURBO_EINVAL;

  int rc = TURBO_OK;
  flowmq_saga_transaction_t *txn = flowmq_saga_find_transaction(coordinator, saga_id, &rc);
  if (rc != TURBO_OK) return rc;

  *out_step_id = txn->current_step;
  return TURBO_OK;
}

int flowmq_saga_coordinator_abort(flowmq_saga_coordinator_t *coordinator,
                                 const tstr_t *saga_id) {
  if (!coordinator || !saga_id) return TURBO_EINVAL;

  int rc = TURBO_OK;
  flowmq_saga_transaction_t *txn = flowmq_saga_find_transaction(coordinator, saga_id, &rc);
  if (rc != TURBO_OK) return rc;

  /* Remove from map. */
  if (turbo_hash_map_remove(&coordinator->transactions, saga_id, NULL) != TURBO_OK) {
    return TURBO_ENOENT;
  }

  /* Cleanup. */
  flowmq_saga_transaction_cleanup(txn);
  free(txn);

  TLOG_INFO("Aborted SAGA transaction: %.*s", (int)tstr_len(*saga_id), *saga_id);

  return TURBO_OK;
}

void flowmq_saga_coordinator_stats(flowmq_saga_coordinator_t *coordinator,
                                  uint32_t *out_active,
                                  uint32_t *out_capacity) {
  if (!coordinator) return;
  if (out_active) *out_active = (uint32_t)turbo_hash_map_size(&coordinator->transactions);
  if (out_capacity) *out_capacity = coordinator->max_transactions;
}

uint32_t flowmq_saga_coordinator_process_timeouts(flowmq_saga_coordinator_t *coordinator,
                                                 uint64_t now_ns) {
  if (!coordinator) return 0;

  uint32_t timed_out = 0;
  for (size_t slot = 0; slot < turbo_hash_map_capacity(&coordinator->transactions); ++slot) {
    flowmq_saga_transaction_t **txn_ptr =
        (flowmq_saga_transaction_t **)turbo_hash_map_value_at(&coordinator->transactions, slot);
    if (!txn_ptr || !*txn_ptr) {
      continue;
    }
    flowmq_saga_transaction_t *txn = *txn_ptr;

    /* Check timeout only for EXECUTING/COMPENSATING. */
    if ((txn->state == FLOWMQ_SAGA_RUNTIME_EXECUTING || txn->state == FLOWMQ_SAGA_RUNTIME_COMPENSATING) &&
        now_ns >= txn->deadline_ns && txn->deadline_ns != 0u) {
      /* Force to ABORTED. */
      txn->state = FLOWMQ_SAGA_RUNTIME_ABORTED;
      timed_out++;

      TLOG_WARN("SAGA timeout: %.*s (deadline=%" PRIu64 ", now=%" PRIu64 ")",
                (int)tstr_len(txn->saga_id), txn->saga_id,
                txn->deadline_ns, now_ns);
    }
  }

  return timed_out;
}

