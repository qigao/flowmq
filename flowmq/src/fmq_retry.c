#include "turbo_flow_fmq_retry.h"

#include "turbo_error.h"
#include "turbo_hash.h"
#include "turbo_vec.h"

#include <stdlib.h>
#include <string.h>

typedef struct flow_fmq_retry_key_s {
  char origin_broker_id[TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX + 1u];
  char client_id[TURBO_FLOW_FMQ_BROKER_LOGICAL_ID_MAX + 1u];
  uint64_t request_id;
} flow_fmq_retry_key_t;

TURBO_VEC_DEFINE(flow_fmq_retry_records, turbo_flow_fmq_retry_record_t)
TURBO_HASH_MAP_DEFINE(flow_fmq_retry_index, flow_fmq_retry_key_t, size_t)

struct turbo_flow_fmq_retry_ledger_s {
  flow_fmq_retry_records records;
  flow_fmq_retry_index index;
  size_t capacity;
  uint32_t max_attempts;
  uint64_t terminal_ttl_ms;
  uint64_t last_now_ms;
  int clock_initialized;
  uint64_t duplicate_accepts;
  uint64_t retries;
  uint64_t poison_transitions;
  uint64_t terminal_evictions;
};

static int flow_fmq_retry_config_valid(const turbo_flow_fmq_retry_config_t *config) {
  return config && config->size >= sizeof(*config) &&
         config->version == TURBO_FLOW_FMQ_RETRY_API_VERSION && config->capacity > 0u &&
         config->capacity <= TURBO_FLOW_FMQ_RETRY_MAX_CAPACITY && config->max_attempts > 0u &&
         config->terminal_ttl_ms > 0u;
}

static flow_fmq_retry_key_t
flow_fmq_retry_key(const turbo_flow_fmq_broker_logical_address_t *address) {
  flow_fmq_retry_key_t key;
  memset(&key, 0, sizeof(key));
  if (address) {
    memcpy(key.origin_broker_id, address->origin_broker_id, strlen(address->origin_broker_id));
    memcpy(key.client_id, address->client_id, strlen(address->client_id));
    key.request_id = address->request_id;
  }
  return key;
}

static turbo_flow_fmq_broker_logical_address_t
flow_fmq_retry_address_copy(const turbo_flow_fmq_broker_logical_address_t *address) {
  turbo_flow_fmq_broker_logical_address_t copy = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
  memcpy(copy.origin_broker_id, address->origin_broker_id, strlen(address->origin_broker_id));
  memcpy(copy.client_id, address->client_id, strlen(address->client_id));
  copy.request_id = address->request_id;
  return copy;
}

static int flow_fmq_retry_find(const turbo_flow_fmq_retry_ledger_t *ledger,
                               const turbo_flow_fmq_broker_logical_address_t *address) {
  flow_fmq_retry_key_t key = flow_fmq_retry_key(address);
  const size_t *index = flow_fmq_retry_index_get_const(&ledger->index, key);
  return index ? (int)*index : -1;
}

static int flow_fmq_retry_clock_valid(const turbo_flow_fmq_retry_ledger_t *ledger,
                                      uint64_t now_ms) {
  if (ledger->clock_initialized && now_ms < ledger->last_now_ms) return TURBO_EALREADY;
  return TURBO_OK;
}

static void flow_fmq_retry_clock_commit(turbo_flow_fmq_retry_ledger_t *ledger, uint64_t now_ms) {
  ledger->clock_initialized = 1;
  ledger->last_now_ms = now_ms;
}

static int flow_fmq_retry_output_valid(const turbo_flow_fmq_retry_record_t *record) {
  return record && record->size >= sizeof(*record);
}

static int flow_fmq_retry_record_valid(const turbo_flow_fmq_retry_ledger_t *ledger,
                                       const turbo_flow_fmq_retry_record_t *record) {
  if (!ledger || !record || record->size < sizeof(*record) ||
      record->version != TURBO_FLOW_FMQ_RETRY_API_VERSION ||
      turbo_flow_fmq_broker_logical_address_validate(&record->address) != TURBO_OK) {
    return 0;
  }
  switch (record->state) {
  case TURBO_FLOW_FMQ_RETRY_PENDING:
    return record->attempts < ledger->max_attempts;
  case TURBO_FLOW_FMQ_RETRY_INFLIGHT:
  case TURBO_FLOW_FMQ_RETRY_COMPLETED:
    return record->attempts > 0u && record->attempts <= ledger->max_attempts;
  case TURBO_FLOW_FMQ_RETRY_POISONED:
    return record->attempts == ledger->max_attempts;
  default:
    return 0;
  }
}

static int flow_fmq_retry_record_equal(const turbo_flow_fmq_retry_record_t *left,
                                       const turbo_flow_fmq_retry_record_t *right) {
  return left->state == right->state && left->attempts == right->attempts &&
         left->updated_at_ms == right->updated_at_ms;
}

static int flow_fmq_retry_insert(turbo_flow_fmq_retry_ledger_t *ledger,
                                 const turbo_flow_fmq_retry_record_t *record) {
  turbo_flow_fmq_retry_record_t stored = *record;
  flow_fmq_retry_key_t key = flow_fmq_retry_key(&record->address);
  size_t index;
  stored.address = flow_fmq_retry_address_copy(&record->address);
  if (flow_fmq_retry_records_size(&ledger->records) >= ledger->capacity) return TURBO_ENOSPC;
  if (flow_fmq_retry_records_push(&ledger->records, stored) != TURBO_OK) return TURBO_ENOMEM;
  index = flow_fmq_retry_records_size(&ledger->records) - 1u;
  if (flow_fmq_retry_index_put(&ledger->index, key, index) != TURBO_OK) {
    (void)turbo_vec_resize(&ledger->records.raw, index);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static int flow_fmq_retry_remove(turbo_flow_fmq_retry_ledger_t *ledger, size_t index) {
  size_t count = flow_fmq_retry_records_size(&ledger->records);
  turbo_flow_fmq_retry_record_t *slot = flow_fmq_retry_records_at(&ledger->records, index);
  flow_fmq_retry_key_t removed_key;
  if (!slot || index >= count) return TURBO_EINVAL;
  removed_key = flow_fmq_retry_key(&slot->address);
  if (index + 1u < count) {
    const turbo_flow_fmq_retry_record_t *last =
        flow_fmq_retry_records_at_const(&ledger->records, count - 1u);
    flow_fmq_retry_key_t last_key;
    if (!last) return TURBO_EPROTO;
    last_key = flow_fmq_retry_key(&last->address);
    if (flow_fmq_retry_index_put(&ledger->index, last_key, index) != TURBO_OK) {
      return TURBO_EPROTO;
    }
    if (!flow_fmq_retry_index_remove(&ledger->index, removed_key, NULL)) {
      (void)flow_fmq_retry_index_put(&ledger->index, last_key, count - 1u);
      return TURBO_EPROTO;
    }
    *slot = *last;
  } else if (!flow_fmq_retry_index_remove(&ledger->index, removed_key, NULL)) {
    return TURBO_EPROTO;
  }
  if (turbo_vec_resize(&ledger->records.raw, count - 1u) != TURBO_OK) return TURBO_EPROTO;
  return TURBO_OK;
}

static turbo_flow_fmq_retry_accept_disposition_t
flow_fmq_retry_duplicate_disposition(turbo_flow_fmq_retry_state_t state) {
  switch (state) {
  case TURBO_FLOW_FMQ_RETRY_PENDING:
    return TURBO_FLOW_FMQ_RETRY_DUPLICATE_PENDING;
  case TURBO_FLOW_FMQ_RETRY_INFLIGHT:
    return TURBO_FLOW_FMQ_RETRY_DUPLICATE_INFLIGHT;
  case TURBO_FLOW_FMQ_RETRY_COMPLETED:
    return TURBO_FLOW_FMQ_RETRY_DUPLICATE_COMPLETED;
  case TURBO_FLOW_FMQ_RETRY_POISONED:
  default:
    return TURBO_FLOW_FMQ_RETRY_DUPLICATE_POISONED;
  }
}

turbo_flow_fmq_retry_ledger_t *
turbo_flow_fmq_retry_ledger_create(const turbo_flow_fmq_retry_config_t *config) {
  turbo_flow_fmq_retry_ledger_t *ledger;
  if (!flow_fmq_retry_config_valid(config)) return NULL;
  ledger = (turbo_flow_fmq_retry_ledger_t *)calloc(1, sizeof(*ledger));
  if (!ledger) return NULL;
  if (flow_fmq_retry_records_init(&ledger->records) != TURBO_OK ||
      flow_fmq_retry_index_init(&ledger->index) != TURBO_OK ||
      flow_fmq_retry_records_reserve(&ledger->records, config->capacity) != TURBO_OK ||
      turbo_hash_map_reserve(&ledger->index.raw, config->capacity) != TURBO_OK) {
    turbo_flow_fmq_retry_ledger_destroy(ledger);
    return NULL;
  }
  ledger->capacity = config->capacity;
  ledger->max_attempts = config->max_attempts;
  ledger->terminal_ttl_ms = config->terminal_ttl_ms;
  return ledger;
}

void turbo_flow_fmq_retry_ledger_destroy(turbo_flow_fmq_retry_ledger_t *ledger) {
  if (!ledger) return;
  flow_fmq_retry_records_destroy(&ledger->records);
  flow_fmq_retry_index_destroy(&ledger->index);
  free(ledger);
}

int turbo_flow_fmq_retry_ledger_accept(turbo_flow_fmq_retry_ledger_t *ledger,
                                       const turbo_flow_fmq_broker_logical_address_t *address,
                                       uint64_t now_ms,
                                       turbo_flow_fmq_retry_accept_result_t *result) {
  turbo_flow_fmq_retry_record_t record = TURBO_FLOW_FMQ_RETRY_RECORD_INIT;
  int found;
  int rc;
  if (!ledger || turbo_flow_fmq_broker_logical_address_validate(address) != TURBO_OK || !result ||
      result->size < sizeof(*result)) {
    return TURBO_EINVAL;
  }
  rc = flow_fmq_retry_clock_valid(ledger, now_ms);
  if (rc != TURBO_OK) return rc;
  found = flow_fmq_retry_find(ledger, address);
  if (found >= 0) {
    const turbo_flow_fmq_retry_record_t *current =
        flow_fmq_retry_records_at_const(&ledger->records, (size_t)found);
    if (!current) return TURBO_EPROTO;
    result->disposition = flow_fmq_retry_duplicate_disposition(current->state);
    result->record = *current;
    ledger->duplicate_accepts += 1u;
    flow_fmq_retry_clock_commit(ledger, now_ms);
    return TURBO_OK;
  }
  record.address = flow_fmq_retry_address_copy(address);
  record.updated_at_ms = now_ms;
  rc = flow_fmq_retry_insert(ledger, &record);
  if (rc != TURBO_OK) return rc;
  result->disposition = TURBO_FLOW_FMQ_RETRY_ACCEPTED_NEW;
  result->record = record;
  flow_fmq_retry_clock_commit(ledger, now_ms);
  return TURBO_OK;
}

int turbo_flow_fmq_retry_ledger_begin_attempt(
    turbo_flow_fmq_retry_ledger_t *ledger, const turbo_flow_fmq_broker_logical_address_t *address,
    uint64_t now_ms, turbo_flow_fmq_retry_record_t *record) {
  turbo_flow_fmq_retry_record_t *current;
  int found;
  int rc;
  if (!ledger || turbo_flow_fmq_broker_logical_address_validate(address) != TURBO_OK ||
      !flow_fmq_retry_output_valid(record)) {
    return TURBO_EINVAL;
  }
  rc = flow_fmq_retry_clock_valid(ledger, now_ms);
  if (rc != TURBO_OK) return rc;
  found = flow_fmq_retry_find(ledger, address);
  if (found < 0) return TURBO_ENOENT;
  current = flow_fmq_retry_records_at(&ledger->records, (size_t)found);
  if (!current) return TURBO_EPROTO;
  if (current->state == TURBO_FLOW_FMQ_RETRY_INFLIGHT) return TURBO_EBUSY;
  if (current->state != TURBO_FLOW_FMQ_RETRY_PENDING) return TURBO_EALREADY;
  if (current->attempts >= ledger->max_attempts) return TURBO_EPROTO;
  current->attempts += 1u;
  current->state = TURBO_FLOW_FMQ_RETRY_INFLIGHT;
  current->updated_at_ms = now_ms;
  *record = *current;
  flow_fmq_retry_clock_commit(ledger, now_ms);
  return TURBO_OK;
}

int turbo_flow_fmq_retry_ledger_finish_attempt(
    turbo_flow_fmq_retry_ledger_t *ledger, const turbo_flow_fmq_broker_logical_address_t *address,
    int success, uint64_t now_ms, turbo_flow_fmq_retry_record_t *record) {
  turbo_flow_fmq_retry_record_t *current;
  int found;
  int rc;
  if (!ledger || turbo_flow_fmq_broker_logical_address_validate(address) != TURBO_OK ||
      (success != 0 && success != 1) || !flow_fmq_retry_output_valid(record)) {
    return TURBO_EINVAL;
  }
  rc = flow_fmq_retry_clock_valid(ledger, now_ms);
  if (rc != TURBO_OK) return rc;
  found = flow_fmq_retry_find(ledger, address);
  if (found < 0) return TURBO_ENOENT;
  current = flow_fmq_retry_records_at(&ledger->records, (size_t)found);
  if (!current) return TURBO_EPROTO;
  if (current->state != TURBO_FLOW_FMQ_RETRY_INFLIGHT) return TURBO_EALREADY;
  if (success) {
    current->state = TURBO_FLOW_FMQ_RETRY_COMPLETED;
  } else if (current->attempts >= ledger->max_attempts) {
    current->state = TURBO_FLOW_FMQ_RETRY_POISONED;
    ledger->poison_transitions += 1u;
  } else {
    current->state = TURBO_FLOW_FMQ_RETRY_PENDING;
    ledger->retries += 1u;
  }
  current->updated_at_ms = now_ms;
  *record = *current;
  flow_fmq_retry_clock_commit(ledger, now_ms);
  return TURBO_OK;
}

int turbo_flow_fmq_retry_ledger_get(const turbo_flow_fmq_retry_ledger_t *ledger,
                                    const turbo_flow_fmq_broker_logical_address_t *address,
                                    turbo_flow_fmq_retry_record_t *record) {
  const turbo_flow_fmq_retry_record_t *current;
  int found;
  if (!ledger || turbo_flow_fmq_broker_logical_address_validate(address) != TURBO_OK ||
      !flow_fmq_retry_output_valid(record)) {
    return TURBO_EINVAL;
  }
  found = flow_fmq_retry_find(ledger, address);
  if (found < 0) return TURBO_ENOENT;
  current = flow_fmq_retry_records_at_const(&ledger->records, (size_t)found);
  if (!current) return TURBO_EPROTO;
  *record = *current;
  return TURBO_OK;
}

int turbo_flow_fmq_retry_ledger_restore(turbo_flow_fmq_retry_ledger_t *ledger,
                                        const turbo_flow_fmq_retry_record_t *record) {
  const turbo_flow_fmq_retry_record_t *current;
  int found;
  int rc;
  if (!flow_fmq_retry_record_valid(ledger, record)) return TURBO_EINVAL;
  found = flow_fmq_retry_find(ledger, &record->address);
  if (found >= 0) {
    current = flow_fmq_retry_records_at_const(&ledger->records, (size_t)found);
    if (!current) return TURBO_EPROTO;
    return flow_fmq_retry_record_equal(current, record) ? TURBO_OK : TURBO_EALREADY;
  }
  rc = flow_fmq_retry_insert(ledger, record);
  if (rc != TURBO_OK) return rc;
  if (!ledger->clock_initialized || record->updated_at_ms > ledger->last_now_ms) {
    ledger->clock_initialized = 1;
    ledger->last_now_ms = record->updated_at_ms;
  }
  return TURBO_OK;
}

int turbo_flow_fmq_retry_ledger_expire_one(turbo_flow_fmq_retry_ledger_t *ledger, uint64_t now_ms,
                                           turbo_flow_fmq_retry_record_t *expired) {
  int rc;
  if (!ledger || !flow_fmq_retry_output_valid(expired)) return TURBO_EINVAL;
  rc = flow_fmq_retry_clock_valid(ledger, now_ms);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < flow_fmq_retry_records_size(&ledger->records); ++i) {
    const turbo_flow_fmq_retry_record_t *record =
        flow_fmq_retry_records_at_const(&ledger->records, i);
    if (record &&
        (record->state == TURBO_FLOW_FMQ_RETRY_COMPLETED ||
         record->state == TURBO_FLOW_FMQ_RETRY_POISONED) &&
        now_ms >= record->updated_at_ms &&
        now_ms - record->updated_at_ms >= ledger->terminal_ttl_ms) {
      turbo_flow_fmq_retry_record_t copy = *record;
      rc = flow_fmq_retry_remove(ledger, i);
      if (rc != TURBO_OK) return rc;
      ledger->terminal_evictions += 1u;
      *expired = copy;
      flow_fmq_retry_clock_commit(ledger, now_ms);
      return TURBO_OK;
    }
  }
  return TURBO_ENOENT;
}

int turbo_flow_fmq_retry_ledger_snapshot(const turbo_flow_fmq_retry_ledger_t *ledger,
                                         turbo_flow_fmq_retry_snapshot_t *snapshot) {
  if (!ledger || !snapshot || snapshot->size < sizeof(*snapshot)) return TURBO_EINVAL;
  *snapshot = (turbo_flow_fmq_retry_snapshot_t)TURBO_FLOW_FMQ_RETRY_SNAPSHOT_INIT;
  snapshot->records = flow_fmq_retry_records_size(&ledger->records);
  for (size_t i = 0u; i < snapshot->records; ++i) {
    const turbo_flow_fmq_retry_record_t *record =
        flow_fmq_retry_records_at_const(&ledger->records, i);
    if (!record) return TURBO_EPROTO;
    switch (record->state) {
    case TURBO_FLOW_FMQ_RETRY_PENDING:
      snapshot->pending += 1u;
      break;
    case TURBO_FLOW_FMQ_RETRY_INFLIGHT:
      snapshot->inflight += 1u;
      break;
    case TURBO_FLOW_FMQ_RETRY_COMPLETED:
      snapshot->completed += 1u;
      break;
    case TURBO_FLOW_FMQ_RETRY_POISONED:
      snapshot->poisoned += 1u;
      break;
    default:
      return TURBO_EPROTO;
    }
  }
  snapshot->duplicate_accepts = ledger->duplicate_accepts;
  snapshot->retries = ledger->retries;
  snapshot->poison_transitions = ledger->poison_transitions;
  snapshot->terminal_evictions = ledger->terminal_evictions;
  return TURBO_OK;
}
