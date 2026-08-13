#include "turbo_flow_fmq_pubsub.h"

#include "turbo_flow_fmq.h"

#include "turbo_containers.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_fmq_pubsub_entry_s {
  tstr_t topic;
  tstr_t payload;
  uint64_t sequence;
  size_t slot;
} flow_fmq_pubsub_entry_t;

typedef struct flow_fmq_pubsub_owned_record_s {
  turbo_flow_fmq_pubsub_operation_t operation;
  uint64_t sequence;
  tstr_t topic;
  tstr_t payload;
} flow_fmq_pubsub_owned_record_t;

struct turbo_flow_fmq_pubsub_state_s {
  turbo_flow_fmq_pubsub_config_t config;
  turbo_vec_t entries;
  turbo_hash_map_t index;
  turbo_deque_t updates;
  size_t state_bytes;
  size_t update_bytes;
  uint64_t latest_sequence;
};

struct turbo_flow_fmq_pubsub_snapshot_cursor_s {
  turbo_vec_t records;
  size_t position;
  uint64_t barrier;
};

struct turbo_flow_fmq_pubsub_update_cursor_s {
  turbo_vec_t records;
  size_t position;
  uint64_t upper_bound;
};

struct turbo_flow_fmq_pubsub_recovery_service_s {
  turbo_flow_fmq_pubsub_state_t *state;
  turbo_flow_fmq_pubsub_recovery_config_t config;
};

static const uint8_t FLOW_FMQ_PUBSUB_RECOVERY_MAGIC[4] = {'T', 'F', 'P', 'S'};

enum {
  FLOW_FMQ_PUBSUB_HEADER_MAJOR = 4,
  FLOW_FMQ_PUBSUB_HEADER_MINOR = 6,
  FLOW_FMQ_PUBSUB_HEADER_KIND = 8,
  FLOW_FMQ_PUBSUB_HEADER_STATUS = 10,
  FLOW_FMQ_PUBSUB_HEADER_FLAGS = 12,
  FLOW_FMQ_PUBSUB_HEADER_CAPABILITIES = 16,
  FLOW_FMQ_PUBSUB_HEADER_NATIVE_STATUS = 20,
  FLOW_FMQ_PUBSUB_HEADER_REQUEST_ID = 24,
  FLOW_FMQ_PUBSUB_HEADER_SEQUENCE = 32,
  FLOW_FMQ_PUBSUB_HEADER_UPPER_BOUND = 40,
  FLOW_FMQ_PUBSUB_HEADER_BODY_SIZE = 48,
  FLOW_FMQ_PUBSUB_HEADER_PREFIX_SIZE = 52,
  FLOW_FMQ_PUBSUB_HEADER_RECORD_COUNT = 54,
  FLOW_FMQ_PUBSUB_RECORD_OPERATION = 0,
  FLOW_FMQ_PUBSUB_RECORD_RESERVED_A = 2,
  FLOW_FMQ_PUBSUB_RECORD_SEQUENCE = 4,
  FLOW_FMQ_PUBSUB_RECORD_TOPIC_SIZE = 12,
  FLOW_FMQ_PUBSUB_RECORD_RESERVED_B = 14,
  FLOW_FMQ_PUBSUB_RECORD_PAYLOAD_SIZE = 16
};

static const uint32_t FLOW_FMQ_PUBSUB_RECOVERY_KNOWN_FLAGS =
    TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_REQUEST |
    TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_RESPONSE |
    TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE;
static const uint32_t FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES =
    TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAP_SNAPSHOT |
    TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAP_UPDATES |
    TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAP_LIVE_SEQUENCE |
    TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAP_PREFIX;

static int flow_fmq_pubsub_view_valid(tstr_v value);

static void flow_fmq_pubsub_write_u16(uint8_t *out, uint16_t value) {
  out[0] = (uint8_t)(value >> 8u);
  out[1] = (uint8_t)value;
}

static void flow_fmq_pubsub_write_u32(uint8_t *out, uint32_t value) {
  out[0] = (uint8_t)(value >> 24u);
  out[1] = (uint8_t)(value >> 16u);
  out[2] = (uint8_t)(value >> 8u);
  out[3] = (uint8_t)value;
}

static void flow_fmq_pubsub_write_u64(uint8_t *out, uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i) out[i] = (uint8_t)(value >> (56u - 8u * i));
}

static uint16_t flow_fmq_pubsub_read_u16(const uint8_t *data) {
  return (uint16_t)(((uint16_t)data[0] << 8u) | data[1]);
}

static uint32_t flow_fmq_pubsub_read_u32(const uint8_t *data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) |
         ((uint32_t)data[2] << 8u) | data[3];
}

static uint64_t flow_fmq_pubsub_read_u64(const uint8_t *data) {
  uint64_t value = 0u;
  for (size_t i = 0u; i < 8u; ++i) value = (value << 8u) | data[i];
  return value;
}

static int flow_fmq_pubsub_size_add(size_t left, size_t right, size_t *out) {
  if (!out || left > SIZE_MAX - right) return TURBO_ERANGE;
  *out = left + right;
  return TURBO_OK;
}

static int flow_fmq_pubsub_record_wire_size(tstr_v topic, tstr_v payload, size_t *out) {
  size_t size;
  int rc;
  if (!out || !flow_fmq_pubsub_view_valid(topic) || !flow_fmq_pubsub_view_valid(payload))
    return TURBO_EINVAL;
  if (topic.len > TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE || topic.len > UINT16_MAX ||
      payload.len > UINT32_MAX)
    return TURBO_EMSGSIZE;
  rc = flow_fmq_pubsub_size_add(TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_HEADER_SIZE, topic.len,
                                &size);
  if (rc == TURBO_OK) rc = flow_fmq_pubsub_size_add(size, payload.len, &size);
  if (rc == TURBO_OK) *out = size;
  return rc;
}

static void flow_fmq_pubsub_header_write(
    uint8_t *out, turbo_flow_fmq_pubsub_recovery_kind_t kind,
    turbo_flow_fmq_pubsub_recovery_status_t status, uint32_t flags, uint32_t capabilities,
    int native_status, uint64_t request_id, uint64_t sequence, uint64_t upper_bound,
    uint32_t body_size, uint16_t prefix_size, uint16_t record_count) {
  memcpy(out, FLOW_FMQ_PUBSUB_RECOVERY_MAGIC, sizeof(FLOW_FMQ_PUBSUB_RECOVERY_MAGIC));
  flow_fmq_pubsub_write_u16(out + FLOW_FMQ_PUBSUB_HEADER_MAJOR,
                            TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_MAJOR);
  flow_fmq_pubsub_write_u16(out + FLOW_FMQ_PUBSUB_HEADER_MINOR,
                            TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_MINOR);
  flow_fmq_pubsub_write_u16(out + FLOW_FMQ_PUBSUB_HEADER_KIND, (uint16_t)kind);
  flow_fmq_pubsub_write_u16(out + FLOW_FMQ_PUBSUB_HEADER_STATUS, (uint16_t)status);
  flow_fmq_pubsub_write_u32(out + FLOW_FMQ_PUBSUB_HEADER_FLAGS, flags);
  flow_fmq_pubsub_write_u32(out + FLOW_FMQ_PUBSUB_HEADER_CAPABILITIES, capabilities);
  flow_fmq_pubsub_write_u32(out + FLOW_FMQ_PUBSUB_HEADER_NATIVE_STATUS,
                            (uint32_t)(int32_t)native_status);
  flow_fmq_pubsub_write_u64(out + FLOW_FMQ_PUBSUB_HEADER_REQUEST_ID, request_id);
  flow_fmq_pubsub_write_u64(out + FLOW_FMQ_PUBSUB_HEADER_SEQUENCE, sequence);
  flow_fmq_pubsub_write_u64(out + FLOW_FMQ_PUBSUB_HEADER_UPPER_BOUND, upper_bound);
  flow_fmq_pubsub_write_u32(out + FLOW_FMQ_PUBSUB_HEADER_BODY_SIZE, body_size);
  flow_fmq_pubsub_write_u16(out + FLOW_FMQ_PUBSUB_HEADER_PREFIX_SIZE, prefix_size);
  flow_fmq_pubsub_write_u16(out + FLOW_FMQ_PUBSUB_HEADER_RECORD_COUNT, record_count);
}

static int flow_fmq_pubsub_record_write(uint8_t *out, size_t capacity,
                                        turbo_flow_fmq_pubsub_operation_t operation,
                                        uint64_t sequence, tstr_v topic, tstr_v payload,
                                        size_t *written) {
  size_t required;
  int rc = flow_fmq_pubsub_record_wire_size(topic, payload, &required);
  if (rc != TURBO_OK) return rc;
  if (!out || capacity < required) return TURBO_ENOSPC;
  flow_fmq_pubsub_write_u16(out + FLOW_FMQ_PUBSUB_RECORD_OPERATION, (uint16_t)operation);
  flow_fmq_pubsub_write_u16(out + FLOW_FMQ_PUBSUB_RECORD_RESERVED_A, 0u);
  flow_fmq_pubsub_write_u64(out + FLOW_FMQ_PUBSUB_RECORD_SEQUENCE, sequence);
  flow_fmq_pubsub_write_u16(out + FLOW_FMQ_PUBSUB_RECORD_TOPIC_SIZE, (uint16_t)topic.len);
  flow_fmq_pubsub_write_u16(out + FLOW_FMQ_PUBSUB_RECORD_RESERVED_B, 0u);
  flow_fmq_pubsub_write_u32(out + FLOW_FMQ_PUBSUB_RECORD_PAYLOAD_SIZE, (uint32_t)payload.len);
  if (topic.len > 0u)
    memcpy(out + TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_HEADER_SIZE, topic.data, topic.len);
  if (payload.len > 0u)
    memcpy(out + TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_HEADER_SIZE + topic.len, payload.data,
           payload.len);
  if (written) *written = required;
  return TURBO_OK;
}

static int flow_fmq_pubsub_view_valid(tstr_v value) {
  return value.len == 0u || value.data != NULL;
}

static int flow_fmq_pubsub_prefix_matches(tstr_v topic, tstr_v prefix) {
  return topic.len >= prefix.len &&
         (prefix.len == 0u || memcmp(topic.data, prefix.data, prefix.len) == 0);
}

static size_t flow_fmq_pubsub_topic_hash(const void *key, size_t key_size, void *ctx) {
  const tstr_v *topic = (const tstr_v *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(topic->data, topic->len, NULL);
}

static bool flow_fmq_pubsub_topic_equal(const void *left, const void *right, size_t key_size,
                                        void *ctx) {
  const tstr_v *a = (const tstr_v *)left;
  const tstr_v *b = (const tstr_v *)right;
  (void)key_size;
  (void)ctx;
  return a->len == b->len && (a->len == 0u || memcmp(a->data, b->data, a->len) == 0);
}

static void flow_fmq_pubsub_owned_record_cleanup(flow_fmq_pubsub_owned_record_t *record) {
  if (!record) return;
  tstr_freep(&record->topic);
  tstr_freep(&record->payload);
}

static void flow_fmq_pubsub_record_vec_clear(turbo_vec_t *records) {
  if (!records) return;
  for (size_t i = 0u; i < turbo_vec_size(records); ++i) {
    flow_fmq_pubsub_owned_record_t *record =
        (flow_fmq_pubsub_owned_record_t *)turbo_vec_at(records, i);
    flow_fmq_pubsub_owned_record_cleanup(record);
  }
  turbo_vec_clear(records);
}

static int flow_fmq_pubsub_record_copy(turbo_vec_t *records,
                                       turbo_flow_fmq_pubsub_operation_t operation,
                                       uint64_t sequence, tstr_v topic, tstr_v payload) {
  flow_fmq_pubsub_owned_record_t record;
  int rc;
  memset(&record, 0, sizeof(record));
  record.operation = operation;
  record.sequence = sequence;
  record.topic = tstr_new_len(topic.data, topic.len);
  record.payload = tstr_new_len(payload.data, payload.len);
  if (!record.topic || !record.payload) {
    flow_fmq_pubsub_owned_record_cleanup(&record);
    return TURBO_ENOMEM;
  }
  rc = turbo_vec_push(records, &record);
  if (rc != TURBO_OK) flow_fmq_pubsub_owned_record_cleanup(&record);
  return rc;
}

static flow_fmq_pubsub_entry_t *flow_fmq_pubsub_find(const turbo_flow_fmq_pubsub_state_t *state,
                                                     tstr_v topic) {
  flow_fmq_pubsub_entry_t *const *found =
      (flow_fmq_pubsub_entry_t *const *)turbo_hash_map_get_const(&state->index, &topic);
  return found ? *found : NULL;
}

static int flow_fmq_pubsub_config_valid(const turbo_flow_fmq_pubsub_config_t *config) {
  return config && config->size >= sizeof(*config) &&
         config->version == TURBO_FLOW_FMQ_PUBSUB_API_VERSION && config->max_topics > 0u &&
         config->max_topics <= TURBO_FLOW_FMQ_PUBSUB_MAX_TOPICS && config->max_state_bytes > 0u &&
         config->update_capacity > 0u &&
         config->update_capacity <= TURBO_FLOW_FMQ_PUBSUB_MAX_UPDATES &&
         config->max_update_bytes > 0u;
}

turbo_flow_fmq_pubsub_state_t *
turbo_flow_fmq_pubsub_state_create(const turbo_flow_fmq_pubsub_config_t *config) {
  turbo_flow_fmq_pubsub_state_t *state;
  int rc;
  if (!flow_fmq_pubsub_config_valid(config)) return NULL;
  state = (turbo_flow_fmq_pubsub_state_t *)calloc(1, sizeof(*state));
  if (!state) return NULL;
  state->config = *config;
  rc = turbo_vec_init(&state->entries, sizeof(flow_fmq_pubsub_entry_t *));
  if (rc != TURBO_OK) goto fail;
  rc = turbo_hash_map_init(&state->index, sizeof(tstr_v), sizeof(flow_fmq_pubsub_entry_t *),
                           flow_fmq_pubsub_topic_hash, flow_fmq_pubsub_topic_equal, NULL);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_deque_init(&state->updates, sizeof(flow_fmq_pubsub_owned_record_t));
  if (rc != TURBO_OK) goto fail;
  if (turbo_vec_reserve(&state->entries, config->max_topics) != TURBO_OK ||
      turbo_hash_map_reserve(&state->index, config->max_topics) != TURBO_OK ||
      turbo_deque_reserve(&state->updates, config->update_capacity) != TURBO_OK) {
    goto fail;
  }
  return state;

fail:
  turbo_flow_fmq_pubsub_state_destroy(state);
  return NULL;
}

void turbo_flow_fmq_pubsub_state_destroy(turbo_flow_fmq_pubsub_state_t *state) {
  if (!state) return;
  for (size_t i = 0u; i < turbo_vec_size(&state->entries); ++i) {
    flow_fmq_pubsub_entry_t **entry = (flow_fmq_pubsub_entry_t **)turbo_vec_at(&state->entries, i);
    if (!entry || !*entry) continue;
    tstr_freep(&(*entry)->topic);
    tstr_freep(&(*entry)->payload);
    free(*entry);
  }
  while (!turbo_deque_empty(&state->updates)) {
    flow_fmq_pubsub_owned_record_t record;
    if (turbo_deque_pop_front(&state->updates, &record) != TURBO_OK) break;
    flow_fmq_pubsub_owned_record_cleanup(&record);
  }
  turbo_deque_destroy(&state->updates);
  turbo_hash_map_destroy(&state->index);
  turbo_vec_destroy(&state->entries);
  free(state);
}

static int flow_fmq_pubsub_journal_append(turbo_flow_fmq_pubsub_state_t *state,
                                          turbo_flow_fmq_pubsub_operation_t operation,
                                          uint64_t sequence, tstr_v topic, tstr_v payload) {
  flow_fmq_pubsub_owned_record_t record;
  size_t record_bytes;
  int rc;
  if (topic.len > SIZE_MAX - payload.len) return TURBO_ERANGE;
  record_bytes = topic.len + payload.len;
  if (record_bytes > state->config.max_update_bytes) return TURBO_ENOSPC;
  memset(&record, 0, sizeof(record));
  record.operation = operation;
  record.sequence = sequence;
  record.topic = tstr_new_len(topic.data, topic.len);
  record.payload = tstr_new_len(payload.data, payload.len);
  if (!record.topic || !record.payload) {
    flow_fmq_pubsub_owned_record_cleanup(&record);
    return TURBO_ENOMEM;
  }
  while (turbo_deque_size(&state->updates) >= state->config.update_capacity ||
         state->update_bytes > state->config.max_update_bytes - record_bytes) {
    flow_fmq_pubsub_owned_record_t evicted;
    size_t evicted_bytes;
    rc = turbo_deque_pop_front(&state->updates, &evicted);
    if (rc != TURBO_OK) {
      flow_fmq_pubsub_owned_record_cleanup(&record);
      return rc;
    }
    evicted_bytes = tstr_len(evicted.topic) + tstr_len(evicted.payload);
    state->update_bytes -= evicted_bytes;
    flow_fmq_pubsub_owned_record_cleanup(&evicted);
  }
  rc = turbo_deque_push_back(&state->updates, &record);
  if (rc != TURBO_OK) {
    flow_fmq_pubsub_owned_record_cleanup(&record);
    return rc;
  }
  state->update_bytes += record_bytes;
  return TURBO_OK;
}

int turbo_flow_fmq_pubsub_put(turbo_flow_fmq_pubsub_state_t *state, tstr_v topic, tstr_v payload,
                              uint64_t *sequence) {
  flow_fmq_pubsub_entry_t *entry;
  flow_fmq_pubsub_entry_t *created = NULL;
  uint64_t next_sequence;
  size_t next_state_bytes;
  tstr_t next_payload;
  int rc;
  if (!state || !sequence || !flow_fmq_pubsub_view_valid(topic) ||
      !flow_fmq_pubsub_view_valid(payload)) {
    return TURBO_EINVAL;
  }
  if (topic.len > TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE) return TURBO_ENAMETOOLONG;
  if (state->latest_sequence == UINT64_MAX || topic.len > SIZE_MAX - payload.len) {
    return TURBO_ERANGE;
  }
  entry = flow_fmq_pubsub_find(state, topic);
  if (!entry && turbo_vec_size(&state->entries) >= state->config.max_topics) return TURBO_ENOSPC;
  next_state_bytes = entry ? state->state_bytes - tstr_len(entry->payload) + payload.len
                           : state->state_bytes + topic.len + payload.len;
  if (next_state_bytes > state->config.max_state_bytes) return TURBO_ENOSPC;
  next_payload = tstr_new_len(payload.data, payload.len);
  if (!next_payload) return TURBO_ENOMEM;
  if (!entry) {
    created = (flow_fmq_pubsub_entry_t *)calloc(1, sizeof(*created));
    if (!created) {
      tstr_free(next_payload);
      return TURBO_ENOMEM;
    }
    created->topic = tstr_new_len(topic.data, topic.len);
    if (!created->topic) {
      tstr_free(next_payload);
      free(created);
      return TURBO_ENOMEM;
    }
    created->payload = next_payload;
    created->slot = turbo_vec_size(&state->entries);
  }
  next_sequence = state->latest_sequence + 1u;
  rc = flow_fmq_pubsub_journal_append(state, TURBO_FLOW_FMQ_PUBSUB_PUT, next_sequence, topic,
                                      payload);
  if (rc != TURBO_OK) {
    if (created) {
      tstr_freep(&created->topic);
      tstr_freep(&created->payload);
      free(created);
    } else {
      tstr_free(next_payload);
    }
    return rc;
  }
  if (entry) {
    tstr_free(entry->payload);
    entry->payload = next_payload;
    entry->sequence = next_sequence;
  } else {
    tstr_v stored_topic;
    created->sequence = next_sequence;
    if (turbo_vec_push(&state->entries, &created) != TURBO_OK) {
      flow_fmq_pubsub_owned_record_t rollback;
      (void)turbo_deque_pop_back(&state->updates, &rollback);
      state->update_bytes -= tstr_len(rollback.topic) + tstr_len(rollback.payload);
      flow_fmq_pubsub_owned_record_cleanup(&rollback);
      tstr_freep(&created->topic);
      tstr_freep(&created->payload);
      free(created);
      return TURBO_ENOMEM;
    }
    stored_topic = tstr_to_v(created->topic);
    rc = turbo_hash_map_put(&state->index, &stored_topic, &created);
    if (rc != TURBO_OK) {
      flow_fmq_pubsub_owned_record_t rollback;
      flow_fmq_pubsub_entry_t *removed = NULL;
      (void)turbo_vec_pop(&state->entries, &removed);
      (void)turbo_deque_pop_back(&state->updates, &rollback);
      state->update_bytes -= tstr_len(rollback.topic) + tstr_len(rollback.payload);
      flow_fmq_pubsub_owned_record_cleanup(&rollback);
      tstr_freep(&created->topic);
      tstr_freep(&created->payload);
      free(created);
      return rc;
    }
  }
  state->state_bytes = next_state_bytes;
  state->latest_sequence = next_sequence;
  *sequence = next_sequence;
  return TURBO_OK;
}

int turbo_flow_fmq_pubsub_delete(turbo_flow_fmq_pubsub_state_t *state, tstr_v topic,
                                 uint64_t *sequence) {
  flow_fmq_pubsub_entry_t *entry;
  uint64_t next_sequence;
  int rc;
  if (!state || !sequence || !flow_fmq_pubsub_view_valid(topic)) return TURBO_EINVAL;
  if (topic.len > TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE) return TURBO_ENAMETOOLONG;
  if (state->latest_sequence == UINT64_MAX) return TURBO_ERANGE;
  next_sequence = state->latest_sequence + 1u;
  rc = flow_fmq_pubsub_journal_append(state, TURBO_FLOW_FMQ_PUBSUB_DELETE, next_sequence, topic,
                                      (tstr_v){0});
  if (rc != TURBO_OK) return rc;
  entry = flow_fmq_pubsub_find(state, topic);
  if (entry) {
    flow_fmq_pubsub_entry_t *removed = NULL;
    flow_fmq_pubsub_entry_t **moved;
    tstr_v stored_topic = tstr_to_v(entry->topic);
    rc = turbo_hash_map_remove(&state->index, &stored_topic, NULL);
    if (rc != TURBO_OK ||
        turbo_vec_swap_remove(&state->entries, entry->slot, &removed) != TURBO_OK) {
      return TURBO_EPROTO;
    }
    moved = (flow_fmq_pubsub_entry_t **)turbo_vec_at(&state->entries, entry->slot);
    if (moved && *moved) (*moved)->slot = entry->slot;
    state->state_bytes -= tstr_len(entry->topic) + tstr_len(entry->payload);
    tstr_freep(&entry->topic);
    tstr_freep(&entry->payload);
    free(entry);
  }
  state->latest_sequence = next_sequence;
  *sequence = next_sequence;
  return TURBO_OK;
}

static void flow_fmq_pubsub_record_export(const flow_fmq_pubsub_owned_record_t *owned,
                                          turbo_flow_fmq_pubsub_record_t *record) {
  *record = (turbo_flow_fmq_pubsub_record_t)TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
  record->operation = owned->operation;
  record->sequence = owned->sequence;
  record->topic = tstr_to_v(owned->topic);
  record->payload = tstr_to_v(owned->payload);
}

int turbo_flow_fmq_pubsub_snapshot_open(const turbo_flow_fmq_pubsub_state_t *state, tstr_v prefix,
                                        turbo_flow_fmq_pubsub_snapshot_cursor_t **out) {
  turbo_flow_fmq_pubsub_snapshot_cursor_t *cursor;
  int rc;
  if (out) *out = NULL;
  if (!state || !out || !flow_fmq_pubsub_view_valid(prefix)) return TURBO_EINVAL;
  if (prefix.len > TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE) return TURBO_ENAMETOOLONG;
  cursor = (turbo_flow_fmq_pubsub_snapshot_cursor_t *)calloc(1, sizeof(*cursor));
  if (!cursor) return TURBO_ENOMEM;
  rc = turbo_vec_init(&cursor->records, sizeof(flow_fmq_pubsub_owned_record_t));
  if (rc != TURBO_OK ||
      turbo_vec_reserve(&cursor->records, turbo_vec_size(&state->entries)) != TURBO_OK) {
    turbo_flow_fmq_pubsub_snapshot_destroy(cursor);
    return TURBO_ENOMEM;
  }
  cursor->barrier = state->latest_sequence;
  for (size_t i = 0u; i < turbo_vec_size(&state->entries); ++i) {
    flow_fmq_pubsub_entry_t *const *entry =
        (flow_fmq_pubsub_entry_t *const *)turbo_vec_at_const(&state->entries, i);
    if (!entry || !*entry || !flow_fmq_pubsub_prefix_matches(tstr_to_v((*entry)->topic), prefix))
      continue;
    rc =
        flow_fmq_pubsub_record_copy(&cursor->records, TURBO_FLOW_FMQ_PUBSUB_PUT, (*entry)->sequence,
                                    tstr_to_v((*entry)->topic), tstr_to_v((*entry)->payload));
    if (rc != TURBO_OK) {
      turbo_flow_fmq_pubsub_snapshot_destroy(cursor);
      return rc;
    }
  }
  *out = cursor;
  return TURBO_OK;
}

uint64_t
turbo_flow_fmq_pubsub_snapshot_barrier(const turbo_flow_fmq_pubsub_snapshot_cursor_t *cursor) {
  return cursor ? cursor->barrier : 0u;
}

int turbo_flow_fmq_pubsub_snapshot_next(turbo_flow_fmq_pubsub_snapshot_cursor_t *cursor,
                                        turbo_flow_fmq_pubsub_record_t *record) {
  const flow_fmq_pubsub_owned_record_t *owned;
  if (!cursor || !record || record->size < sizeof(*record)) return TURBO_EINVAL;
  owned = (const flow_fmq_pubsub_owned_record_t *)turbo_vec_at_const(&cursor->records,
                                                                     cursor->position);
  if (!owned) return TURBO_ENOENT;
  cursor->position += 1u;
  flow_fmq_pubsub_record_export(owned, record);
  return TURBO_OK;
}

void turbo_flow_fmq_pubsub_snapshot_destroy(turbo_flow_fmq_pubsub_snapshot_cursor_t *cursor) {
  if (!cursor) return;
  flow_fmq_pubsub_record_vec_clear(&cursor->records);
  turbo_vec_destroy(&cursor->records);
  free(cursor);
}

int turbo_flow_fmq_pubsub_updates_open(const turbo_flow_fmq_pubsub_state_t *state, tstr_v prefix,
                                       uint64_t after_sequence,
                                       turbo_flow_fmq_pubsub_update_cursor_t **out) {
  turbo_flow_fmq_pubsub_update_cursor_t *cursor;
  const flow_fmq_pubsub_owned_record_t *oldest;
  int rc;
  if (out) *out = NULL;
  if (!state || !out || !flow_fmq_pubsub_view_valid(prefix)) return TURBO_EINVAL;
  if (prefix.len > TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE) return TURBO_ENAMETOOLONG;
  if (after_sequence > state->latest_sequence) return TURBO_ERANGE;
  oldest = (const flow_fmq_pubsub_owned_record_t *)turbo_deque_front_const(&state->updates);
  if (oldest && after_sequence < oldest->sequence - 1u) return TURBO_ERANGE;
  cursor = (turbo_flow_fmq_pubsub_update_cursor_t *)calloc(1, sizeof(*cursor));
  if (!cursor) return TURBO_ENOMEM;
  rc = turbo_vec_init(&cursor->records, sizeof(flow_fmq_pubsub_owned_record_t));
  if (rc != TURBO_OK ||
      turbo_vec_reserve(&cursor->records, turbo_deque_size(&state->updates)) != TURBO_OK) {
    turbo_flow_fmq_pubsub_updates_destroy(cursor);
    return TURBO_ENOMEM;
  }
  cursor->upper_bound = state->latest_sequence;
  for (size_t i = 0u; i < turbo_deque_size(&state->updates); ++i) {
    const flow_fmq_pubsub_owned_record_t *update =
        (const flow_fmq_pubsub_owned_record_t *)turbo_deque_at_const(&state->updates, i);
    if (!update || update->sequence <= after_sequence ||
        !flow_fmq_pubsub_prefix_matches(tstr_to_v(update->topic), prefix)) {
      continue;
    }
    rc = flow_fmq_pubsub_record_copy(&cursor->records, update->operation, update->sequence,
                                     tstr_to_v(update->topic), tstr_to_v(update->payload));
    if (rc != TURBO_OK) {
      turbo_flow_fmq_pubsub_updates_destroy(cursor);
      return rc;
    }
  }
  *out = cursor;
  return TURBO_OK;
}

uint64_t
turbo_flow_fmq_pubsub_updates_upper_bound(const turbo_flow_fmq_pubsub_update_cursor_t *cursor) {
  return cursor ? cursor->upper_bound : 0u;
}

int turbo_flow_fmq_pubsub_updates_next(turbo_flow_fmq_pubsub_update_cursor_t *cursor,
                                       turbo_flow_fmq_pubsub_record_t *record) {
  const flow_fmq_pubsub_owned_record_t *owned;
  if (!cursor || !record || record->size < sizeof(*record)) return TURBO_EINVAL;
  owned = (const flow_fmq_pubsub_owned_record_t *)turbo_vec_at_const(&cursor->records,
                                                                     cursor->position);
  if (!owned) return TURBO_ENOENT;
  cursor->position += 1u;
  flow_fmq_pubsub_record_export(owned, record);
  return TURBO_OK;
}

void turbo_flow_fmq_pubsub_updates_destroy(turbo_flow_fmq_pubsub_update_cursor_t *cursor) {
  if (!cursor) return;
  flow_fmq_pubsub_record_vec_clear(&cursor->records);
  turbo_vec_destroy(&cursor->records);
  free(cursor);
}

int turbo_flow_fmq_pubsub_status(const turbo_flow_fmq_pubsub_state_t *state,
                                 turbo_flow_fmq_pubsub_status_t *status) {
  const flow_fmq_pubsub_owned_record_t *oldest;
  if (!state || !status || status->size < sizeof(*status)) return TURBO_EINVAL;
  oldest = (const flow_fmq_pubsub_owned_record_t *)turbo_deque_front_const(&state->updates);
  *status = (turbo_flow_fmq_pubsub_status_t)TURBO_FLOW_FMQ_PUBSUB_STATUS_INIT;
  status->topics = turbo_vec_size(&state->entries);
  status->state_bytes = state->state_bytes;
  status->retained_updates = turbo_deque_size(&state->updates);
  status->retained_update_bytes = state->update_bytes;
  status->earliest_update_sequence = oldest ? oldest->sequence : 0u;
  status->latest_sequence = state->latest_sequence;
  return TURBO_OK;
}

static int flow_fmq_pubsub_record_decode(const uint8_t *data, size_t data_size,
                                         turbo_flow_fmq_pubsub_record_t *record,
                                         size_t *consumed) {
  size_t required;
  uint16_t operation;
  uint16_t topic_size;
  uint32_t payload_size;
  if (!data || data_size < TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_HEADER_SIZE || !record ||
      record->size < sizeof(*record) || !consumed)
    return TURBO_EPROTO;
  operation = flow_fmq_pubsub_read_u16(data + FLOW_FMQ_PUBSUB_RECORD_OPERATION);
  topic_size = flow_fmq_pubsub_read_u16(data + FLOW_FMQ_PUBSUB_RECORD_TOPIC_SIZE);
  payload_size = flow_fmq_pubsub_read_u32(data + FLOW_FMQ_PUBSUB_RECORD_PAYLOAD_SIZE);
  if ((operation != TURBO_FLOW_FMQ_PUBSUB_PUT && operation != TURBO_FLOW_FMQ_PUBSUB_DELETE) ||
      flow_fmq_pubsub_read_u16(data + FLOW_FMQ_PUBSUB_RECORD_RESERVED_A) != 0u ||
      flow_fmq_pubsub_read_u16(data + FLOW_FMQ_PUBSUB_RECORD_RESERVED_B) != 0u ||
      flow_fmq_pubsub_read_u64(data + FLOW_FMQ_PUBSUB_RECORD_SEQUENCE) == 0u ||
      topic_size > TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE ||
      (operation == TURBO_FLOW_FMQ_PUBSUB_DELETE && payload_size != 0u))
    return TURBO_EPROTO;
  required = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_HEADER_SIZE;
  if (flow_fmq_pubsub_size_add(required, topic_size, &required) != TURBO_OK ||
      flow_fmq_pubsub_size_add(required, payload_size, &required) != TURBO_OK ||
      required > data_size)
    return TURBO_EPROTO;
  *record = (turbo_flow_fmq_pubsub_record_t)TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
  record->operation = (turbo_flow_fmq_pubsub_operation_t)operation;
  record->sequence = flow_fmq_pubsub_read_u64(data + FLOW_FMQ_PUBSUB_RECORD_SEQUENCE);
  record->topic = tstr_v_from_buf(
      (const char *)data + TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_HEADER_SIZE, topic_size);
  record->payload = tstr_v_from_buf(
      (const char *)data + TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_HEADER_SIZE + topic_size,
      payload_size);
  *consumed = required;
  return TURBO_OK;
}

static int flow_fmq_pubsub_message_records_validate(
    const turbo_flow_fmq_pubsub_recovery_message_t *message) {
  turbo_flow_fmq_pubsub_record_t record = TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
  size_t offset = 0u;
  uint64_t previous_sequence = 0u;
  for (uint16_t i = 0u; i < message->record_count; ++i) {
    size_t consumed = 0u;
    int rc = flow_fmq_pubsub_record_decode(
        (const uint8_t *)message->records.data + offset, message->records.len - offset, &record,
        &consumed);
    if (rc != TURBO_OK) return rc;
    if (message->kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_SNAPSHOT) {
      if (record.operation != TURBO_FLOW_FMQ_PUBSUB_PUT || record.sequence > message->sequence)
        return TURBO_EPROTO;
    } else {
      if (record.sequence <= previous_sequence || record.sequence > message->upper_bound)
        return TURBO_EPROTO;
      previous_sequence = record.sequence;
    }
    offset += consumed;
  }
  if (offset != message->records.len) return TURBO_EPROTO;
  if (message->kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_LIVE_UPDATE) {
    if (message->record_count != 1u || previous_sequence != message->sequence ||
        message->upper_bound != message->sequence)
      return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flow_fmq_pubsub_message_semantics(
    const turbo_flow_fmq_pubsub_recovery_message_t *message) {
  int request;
  int response;
  if (!message || (message->flags & ~FLOW_FMQ_PUBSUB_RECOVERY_KNOWN_FLAGS) != 0u ||
      message->status > TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_INTERNAL ||
      message->prefix.len > TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE)
    return TURBO_EPROTO;
  request = (message->flags & TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_REQUEST) != 0u;
  response = (message->flags & TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_RESPONSE) != 0u;
  if (message->kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_LIVE_UPDATE) {
    if (request || response || message->flags != TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE ||
        message->status != TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK ||
        message->capabilities != 0u || message->native_status != TURBO_OK ||
        message->request_id != 0u || message->prefix.len != 0u)
      return TURBO_EPROTO;
    return flow_fmq_pubsub_message_records_validate(message);
  }
  if (request == response || (request && message->request_id == 0u) ||
      (response && message->request_id == 0u &&
       message->kind != TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_ERROR))
    return TURBO_EPROTO;
  if (request) {
    if (message->flags != TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_REQUEST ||
        message->status != TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK ||
        message->capabilities != 0u || message->native_status != TURBO_OK ||
        message->records.len != 0u)
      return TURBO_EPROTO;
    if (message->kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES)
      return message->sequence == 0u && message->upper_bound == 0u &&
                     message->record_count == 0u && message->prefix.len == 0u
                 ? TURBO_OK
                 : TURBO_EPROTO;
    if (message->kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_SNAPSHOT)
      return message->sequence == 0u && message->upper_bound == 0u &&
                     message->record_count == 0u
                 ? TURBO_OK
                 : TURBO_EPROTO;
    if (message->kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES)
      return message->record_count > 0u &&
                     (message->upper_bound == 0u || message->sequence <= message->upper_bound)
                 ? TURBO_OK
                 : TURBO_EPROTO;
    return TURBO_ENOTSUP;
  }
  if ((message->flags & TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE) == 0u &&
      message->kind != TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES)
    return TURBO_EPROTO;
  if (message->status != TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK) {
    return message->capabilities == 0u && message->sequence == 0u &&
                   message->upper_bound == 0u && message->record_count == 0u &&
                   message->prefix.len == 0u && message->records.len == 0u
               ? TURBO_OK
               : TURBO_EPROTO;
  }
  if (message->native_status != TURBO_OK || message->prefix.len != 0u) return TURBO_EPROTO;
  if (message->kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES)
    return message->flags == (TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_RESPONSE |
                              TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE) &&
                   message->capabilities == FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES &&
                   message->upper_bound == 0u && message->record_count == 0u &&
                   message->records.len == 0u
               ? TURBO_OK
               : TURBO_EPROTO;
  if (message->capabilities != 0u) return TURBO_EPROTO;
  if (message->kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_SNAPSHOT) {
    if (message->upper_bound != 0u) return TURBO_EPROTO;
    return flow_fmq_pubsub_message_records_validate(message);
  }
  if (message->kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES) {
    if (message->sequence > message->upper_bound) return TURBO_EPROTO;
    return flow_fmq_pubsub_message_records_validate(message);
  }
  return message->kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_ERROR ? TURBO_OK
                                                                        : TURBO_ENOTSUP;
}

int turbo_flow_fmq_pubsub_recovery_request_encode(
    turbo_flow_fmq_pubsub_recovery_kind_t kind, uint64_t request_id, tstr_v prefix,
    uint64_t after_sequence, uint64_t upper_bound, uint16_t page_limit, uint8_t *out,
    size_t capacity, size_t *out_size) {
  size_t required;
  if (!out_size || !flow_fmq_pubsub_view_valid(prefix) || request_id == 0u) return TURBO_EINVAL;
  if (prefix.len > TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE || prefix.len > UINT16_MAX)
    return TURBO_ENAMETOOLONG;
  if (kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES) {
    if (prefix.len != 0u || after_sequence != 0u || upper_bound != 0u || page_limit != 0u)
      return TURBO_EINVAL;
  } else if (kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_SNAPSHOT) {
    if (after_sequence != 0u || upper_bound != 0u || page_limit != 0u) return TURBO_EINVAL;
  } else if (kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES) {
    if (page_limit == 0u || (upper_bound != 0u && after_sequence > upper_bound))
      return TURBO_EINVAL;
  } else {
    return TURBO_ENOTSUP;
  }
  required = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE + prefix.len;
  *out_size = required;
  if (!out || capacity < required) return TURBO_ENOSPC;
  flow_fmq_pubsub_header_write(
      out, kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK,
      TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_REQUEST, 0u, TURBO_OK, request_id, after_sequence,
      upper_bound, (uint32_t)prefix.len, (uint16_t)prefix.len, page_limit);
  if (prefix.len > 0u)
    memcpy(out + TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE, prefix.data, prefix.len);
  return TURBO_OK;
}

int turbo_flow_fmq_pubsub_recovery_message_decode(
    const uint8_t *data, size_t data_size, turbo_flow_fmq_pubsub_recovery_message_t *message) {
  turbo_flow_fmq_pubsub_recovery_message_t decoded =
      TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
  uint32_t body_size;
  uint16_t prefix_size;
  if (!data || data_size < TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE || !message ||
      message->size < sizeof(*message))
    return TURBO_EINVAL;
  if (memcmp(data, FLOW_FMQ_PUBSUB_RECOVERY_MAGIC, sizeof(FLOW_FMQ_PUBSUB_RECOVERY_MAGIC)) != 0)
    return TURBO_EPROTO;
  decoded.protocol_major = flow_fmq_pubsub_read_u16(data + FLOW_FMQ_PUBSUB_HEADER_MAJOR);
  decoded.protocol_minor = flow_fmq_pubsub_read_u16(data + FLOW_FMQ_PUBSUB_HEADER_MINOR);
  decoded.kind =
      (turbo_flow_fmq_pubsub_recovery_kind_t)flow_fmq_pubsub_read_u16(data +
                                                                     FLOW_FMQ_PUBSUB_HEADER_KIND);
  decoded.status = (turbo_flow_fmq_pubsub_recovery_status_t)flow_fmq_pubsub_read_u16(
      data + FLOW_FMQ_PUBSUB_HEADER_STATUS);
  decoded.flags = flow_fmq_pubsub_read_u32(data + FLOW_FMQ_PUBSUB_HEADER_FLAGS);
  decoded.capabilities = flow_fmq_pubsub_read_u32(data + FLOW_FMQ_PUBSUB_HEADER_CAPABILITIES);
  decoded.native_status =
      (int32_t)flow_fmq_pubsub_read_u32(data + FLOW_FMQ_PUBSUB_HEADER_NATIVE_STATUS);
  decoded.request_id = flow_fmq_pubsub_read_u64(data + FLOW_FMQ_PUBSUB_HEADER_REQUEST_ID);
  decoded.sequence = flow_fmq_pubsub_read_u64(data + FLOW_FMQ_PUBSUB_HEADER_SEQUENCE);
  decoded.upper_bound = flow_fmq_pubsub_read_u64(data + FLOW_FMQ_PUBSUB_HEADER_UPPER_BOUND);
  body_size = flow_fmq_pubsub_read_u32(data + FLOW_FMQ_PUBSUB_HEADER_BODY_SIZE);
  prefix_size = flow_fmq_pubsub_read_u16(data + FLOW_FMQ_PUBSUB_HEADER_PREFIX_SIZE);
  decoded.record_count = flow_fmq_pubsub_read_u16(data + FLOW_FMQ_PUBSUB_HEADER_RECORD_COUNT);
  if (body_size != data_size - TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE ||
      prefix_size > body_size)
    return TURBO_EPROTO;
  decoded.prefix = tstr_v_from_buf(
      (const char *)data + TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE, prefix_size);
  decoded.records = tstr_v_from_buf(
      (const char *)data + TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE + prefix_size,
      body_size - prefix_size);
  *message = decoded;
  if (decoded.protocol_major != TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_MAJOR ||
      decoded.protocol_minor > TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_MINOR)
    return TURBO_ENOTSUP;
  return flow_fmq_pubsub_message_semantics(message);
}

int turbo_flow_fmq_pubsub_recovery_record_iterator_init(
    const turbo_flow_fmq_pubsub_recovery_message_t *message,
    turbo_flow_fmq_pubsub_recovery_record_iterator_t *iterator) {
  if (!message || message->size < sizeof(*message) || !iterator ||
      iterator->size < sizeof(*iterator) ||
      (message->record_count > 0u && !message->records.data))
    return TURBO_EINVAL;
  iterator->records = message->records;
  iterator->offset = 0u;
  iterator->remaining = message->record_count;
  return TURBO_OK;
}

int turbo_flow_fmq_pubsub_recovery_record_next(
    turbo_flow_fmq_pubsub_recovery_record_iterator_t *iterator,
    turbo_flow_fmq_pubsub_record_t *record) {
  size_t consumed = 0u;
  int rc;
  if (!iterator || iterator->size < sizeof(*iterator) || !record ||
      record->size < sizeof(*record))
    return TURBO_EINVAL;
  if (iterator->remaining == 0u)
    return iterator->offset == iterator->records.len ? TURBO_ENOENT : TURBO_EPROTO;
  rc = flow_fmq_pubsub_record_decode((const uint8_t *)iterator->records.data + iterator->offset,
                                     iterator->records.len - iterator->offset, record, &consumed);
  if (rc != TURBO_OK) return rc;
  iterator->offset += consumed;
  iterator->remaining -= 1u;
  return TURBO_OK;
}

int turbo_flow_fmq_pubsub_recovery_service_create(
    turbo_flow_fmq_pubsub_state_t *state, const turbo_flow_fmq_pubsub_recovery_config_t *config,
    turbo_flow_fmq_pubsub_recovery_service_t **out) {
  turbo_flow_fmq_pubsub_recovery_service_t *service;
  if (out) *out = NULL;
  if (!state || !config || config->size < sizeof(*config) || !out ||
      config->version != TURBO_FLOW_FMQ_PUBSUB_API_VERSION ||
      config->max_reply_bytes < TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE ||
      config->max_reply_bytes > UINT32_MAX || config->max_update_records == 0u)
    return TURBO_EINVAL;
  service = (turbo_flow_fmq_pubsub_recovery_service_t *)calloc(1u, sizeof(*service));
  if (!service) return TURBO_ENOMEM;
  service->state = state;
  service->config = *config;
  *out = service;
  return TURBO_OK;
}

void turbo_flow_fmq_pubsub_recovery_service_destroy(
    turbo_flow_fmq_pubsub_recovery_service_t *service) {
  free(service);
}

static int flow_fmq_pubsub_live_stage(turbo_flow_msg_t *msg,
                                      turbo_flow_fmq_pubsub_state_t *state,
                                      turbo_flow_fmq_pubsub_operation_t operation) {
  const turbo_flow_content_descriptor_t *descriptor;
  const char *topic_end;
  tstr_v topic;
  tstr_v payload;
  tstr_t encoded;
  size_t record_size;
  size_t total_size;
  size_t written = 0u;
  uint64_t sequence = 0u;
  int rc;
  if (!msg || !state || (operation != TURBO_FLOW_FMQ_PUBSUB_PUT &&
                         operation != TURBO_FLOW_FMQ_PUBSUB_DELETE))
    return TURBO_EINVAL;
  rc = turbo_flow_fmq_message_topic(msg, &topic);
  if (rc == TURBO_ENOENT) {
    descriptor = turbo_flow_msg_content_descriptor(msg);
    if (!descriptor || turbo_flow_content_descriptor_check(descriptor) != TURBO_OK)
      return TURBO_ENOENT;
    topic_end = (const char *)memchr(descriptor->identity, '\0', sizeof(descriptor->identity));
    if (!topic_end) return TURBO_EPROTO;
    topic = tstr_v_from_buf(descriptor->identity, (size_t)(topic_end - descriptor->identity));
    rc = TURBO_OK;
  }
  if (rc != TURBO_OK) return rc;
  payload = operation == TURBO_FLOW_FMQ_PUBSUB_PUT ? msg->payload : (tstr_v){0};
  rc = flow_fmq_pubsub_record_wire_size(topic, payload, &record_size);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_pubsub_size_add(TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE, record_size,
                                &total_size);
  if (rc != TURBO_OK || total_size > UINT32_MAX) return TURBO_EMSGSIZE;
  encoded = tstr_new_len(NULL, total_size);
  if (!encoded) return TURBO_ENOMEM;
  flow_fmq_pubsub_header_write(
      (uint8_t *)encoded, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_LIVE_UPDATE,
      TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE, 0u,
      TURBO_OK, 0u, 0u, 0u, (uint32_t)record_size, 0u, 1u);
  rc = flow_fmq_pubsub_record_write(
      (uint8_t *)encoded + TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE, record_size, operation, 0u,
      topic, payload, &written);
  if (rc != TURBO_OK || written != record_size) {
    tstr_free(encoded);
    return rc == TURBO_OK ? TURBO_EPROTO : rc;
  }
  rc = operation == TURBO_FLOW_FMQ_PUBSUB_PUT
           ? turbo_flow_fmq_pubsub_put(state, topic, payload, &sequence)
           : turbo_flow_fmq_pubsub_delete(state, topic, &sequence);
  if (rc != TURBO_OK) {
    tstr_free(encoded);
    return rc;
  }
  flow_fmq_pubsub_write_u64((uint8_t *)encoded + FLOW_FMQ_PUBSUB_HEADER_SEQUENCE, sequence);
  flow_fmq_pubsub_write_u64((uint8_t *)encoded + FLOW_FMQ_PUBSUB_HEADER_UPPER_BOUND, sequence);
  flow_fmq_pubsub_write_u64((uint8_t *)encoded + TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE +
                                FLOW_FMQ_PUBSUB_RECORD_SEQUENCE,
                            sequence);
  tstr_freep(&msg->owned_payload);
  msg->owned_payload = encoded;
  msg->payload = tstr_to_v(encoded);
  msg->id = sequence;
  return TURBO_OK;
}

int turbo_flow_fmq_pubsub_put_stage(turbo_flow_msg_t *msg, void *ctx) {
  return flow_fmq_pubsub_live_stage(msg, (turbo_flow_fmq_pubsub_state_t *)ctx,
                                    TURBO_FLOW_FMQ_PUBSUB_PUT);
}

int turbo_flow_fmq_pubsub_delete_stage(turbo_flow_msg_t *msg, void *ctx) {
  return flow_fmq_pubsub_live_stage(msg, (turbo_flow_fmq_pubsub_state_t *)ctx,
                                    TURBO_FLOW_FMQ_PUBSUB_DELETE);
}

static turbo_flow_fmq_pubsub_recovery_status_t flow_fmq_pubsub_status_from_error(int rc) {
  switch (rc) {
  case TURBO_EPROTO:
  case TURBO_EINVAL:
  case TURBO_EMSGSIZE:
  case TURBO_ENAMETOOLONG:
    return TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_BAD_REQUEST;
  case TURBO_ENOTSUP:
    return TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_UNSUPPORTED_KIND;
  case TURBO_ERANGE:
    return TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_STALE_CURSOR;
  case TURBO_ENOSPC:
  case TURBO_ENOMEM:
    return TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_RESOURCE_EXHAUSTED;
  default:
    return TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_INTERNAL;
  }
}

static int flow_fmq_pubsub_response_allocate(
    const turbo_flow_fmq_pubsub_recovery_service_t *service, size_t body_size, tstr_t *out) {
  size_t total_size;
  int rc;
  if (!service || !out) return TURBO_EINVAL;
  rc = flow_fmq_pubsub_size_add(TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE, body_size,
                                &total_size);
  if (rc != TURBO_OK || total_size > service->config.max_reply_bytes || total_size > UINT32_MAX)
    return TURBO_ENOSPC;
  *out = tstr_new_len(NULL, total_size);
  return *out ? TURBO_OK : TURBO_ENOMEM;
}

static int flow_fmq_pubsub_error_response(
    const turbo_flow_fmq_pubsub_recovery_service_t *service,
    turbo_flow_fmq_pubsub_recovery_kind_t kind, uint64_t request_id,
    turbo_flow_fmq_pubsub_recovery_status_t status, int native_status, tstr_t *out) {
  int rc = flow_fmq_pubsub_response_allocate(service, 0u, out);
  if (rc != TURBO_OK) return rc;
  if (kind == 0u || kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_LIVE_UPDATE || request_id == 0u)
    kind = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_ERROR;
  flow_fmq_pubsub_header_write(
      (uint8_t *)*out, kind, status,
      TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_RESPONSE |
          TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE,
      0u, native_status, request_id, 0u, 0u, 0u, 0u, 0u);
  return TURBO_OK;
}

static int flow_fmq_pubsub_capabilities_response(
    const turbo_flow_fmq_pubsub_recovery_service_t *service,
    const turbo_flow_fmq_pubsub_recovery_message_t *request, tstr_t *out) {
  turbo_flow_fmq_pubsub_status_t status = TURBO_FLOW_FMQ_PUBSUB_STATUS_INIT;
  int rc = turbo_flow_fmq_pubsub_status(service->state, &status);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_pubsub_response_allocate(service, 0u, out);
  if (rc != TURBO_OK) return rc;
  flow_fmq_pubsub_header_write(
      (uint8_t *)*out, request->kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK,
      TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_RESPONSE |
          TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE,
      FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES, TURBO_OK, request->request_id,
      status.latest_sequence, 0u, 0u, 0u, 0u);
  return TURBO_OK;
}

static int flow_fmq_pubsub_snapshot_response(
    const turbo_flow_fmq_pubsub_recovery_service_t *service,
    const turbo_flow_fmq_pubsub_recovery_message_t *request, tstr_t *out) {
  turbo_flow_fmq_pubsub_snapshot_cursor_t *cursor = NULL;
  size_t body_size = 0u;
  size_t offset;
  uint16_t record_count;
  int rc = turbo_flow_fmq_pubsub_snapshot_open(service->state, request->prefix, &cursor);
  if (rc != TURBO_OK) return rc;
  if (turbo_vec_size(&cursor->records) > TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MAX_RECORDS) {
    rc = TURBO_ENOSPC;
    goto done;
  }
  record_count = (uint16_t)turbo_vec_size(&cursor->records);
  for (size_t i = 0u; i < turbo_vec_size(&cursor->records); ++i) {
    const flow_fmq_pubsub_owned_record_t *record =
        (const flow_fmq_pubsub_owned_record_t *)turbo_vec_at_const(&cursor->records, i);
    size_t record_size;
    if (!record) {
      rc = TURBO_EPROTO;
      goto done;
    }
    rc = flow_fmq_pubsub_record_wire_size(tstr_to_v(record->topic), tstr_to_v(record->payload),
                                          &record_size);
    if (rc != TURBO_OK || flow_fmq_pubsub_size_add(body_size, record_size, &body_size) != TURBO_OK) {
      if (rc == TURBO_OK) rc = TURBO_ERANGE;
      goto done;
    }
  }
  rc = flow_fmq_pubsub_response_allocate(service, body_size, out);
  if (rc != TURBO_OK) goto done;
  flow_fmq_pubsub_header_write(
      (uint8_t *)*out, request->kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK,
      TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_RESPONSE |
          TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE,
      0u, TURBO_OK, request->request_id, cursor->barrier, 0u, (uint32_t)body_size, 0u,
      record_count);
  offset = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE;
  for (size_t i = 0u; i < turbo_vec_size(&cursor->records); ++i) {
    const flow_fmq_pubsub_owned_record_t *record =
        (const flow_fmq_pubsub_owned_record_t *)turbo_vec_at_const(&cursor->records, i);
    size_t written = 0u;
    rc = flow_fmq_pubsub_record_write(
        (uint8_t *)*out + offset, tstr_len(*out) - offset, record->operation, record->sequence,
        tstr_to_v(record->topic), tstr_to_v(record->payload), &written);
    if (rc != TURBO_OK) {
      tstr_freep(out);
      goto done;
    }
    offset += written;
  }
  if (offset != tstr_len(*out)) {
    tstr_freep(out);
    rc = TURBO_EPROTO;
  }
done:
  turbo_flow_fmq_pubsub_snapshot_destroy(cursor);
  return rc;
}

static int flow_fmq_pubsub_updates_response(
    const turbo_flow_fmq_pubsub_recovery_service_t *service,
    const turbo_flow_fmq_pubsub_recovery_message_t *request, tstr_t *out) {
  turbo_flow_fmq_pubsub_update_cursor_t *cursor = NULL;
  size_t available_records = 0u;
  size_t selected_records;
  size_t body_size = 0u;
  size_t offset;
  uint64_t upper_bound;
  uint64_t resume_sequence;
  uint16_t page_limit = request->record_count;
  int complete;
  int rc = turbo_flow_fmq_pubsub_updates_open(service->state, request->prefix, request->sequence,
                                               &cursor);
  if (rc != TURBO_OK) return rc;
  upper_bound = request->upper_bound ? request->upper_bound : cursor->upper_bound;
  if (upper_bound > cursor->upper_bound || upper_bound < request->sequence) {
    rc = TURBO_ERANGE;
    goto done;
  }
  if (page_limit > service->config.max_update_records)
    page_limit = service->config.max_update_records;
  for (size_t i = 0u; i < turbo_vec_size(&cursor->records); ++i) {
    const flow_fmq_pubsub_owned_record_t *record =
        (const flow_fmq_pubsub_owned_record_t *)turbo_vec_at_const(&cursor->records, i);
    if (!record || record->sequence > upper_bound) break;
    available_records += 1u;
  }
  selected_records = available_records < page_limit ? available_records : page_limit;
  complete = selected_records == available_records;
  resume_sequence = complete ? upper_bound : request->sequence;
  for (size_t i = 0u; i < selected_records; ++i) {
    const flow_fmq_pubsub_owned_record_t *record =
        (const flow_fmq_pubsub_owned_record_t *)turbo_vec_at_const(&cursor->records, i);
    size_t record_size;
    rc = flow_fmq_pubsub_record_wire_size(tstr_to_v(record->topic), tstr_to_v(record->payload),
                                          &record_size);
    if (rc != TURBO_OK || flow_fmq_pubsub_size_add(body_size, record_size, &body_size) != TURBO_OK) {
      if (rc == TURBO_OK) rc = TURBO_ERANGE;
      goto done;
    }
    resume_sequence = record->sequence;
  }
  if (complete) resume_sequence = upper_bound;
  rc = flow_fmq_pubsub_response_allocate(service, body_size, out);
  if (rc != TURBO_OK) goto done;
  flow_fmq_pubsub_header_write(
      (uint8_t *)*out, request->kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK,
      TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_RESPONSE |
          (complete ? TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE : 0u),
      0u, TURBO_OK, request->request_id, resume_sequence, upper_bound, (uint32_t)body_size, 0u,
      (uint16_t)selected_records);
  offset = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE;
  for (size_t i = 0u; i < selected_records; ++i) {
    const flow_fmq_pubsub_owned_record_t *record =
        (const flow_fmq_pubsub_owned_record_t *)turbo_vec_at_const(&cursor->records, i);
    size_t written = 0u;
    rc = flow_fmq_pubsub_record_write(
        (uint8_t *)*out + offset, tstr_len(*out) - offset, record->operation, record->sequence,
        tstr_to_v(record->topic), tstr_to_v(record->payload), &written);
    if (rc != TURBO_OK) {
      tstr_freep(out);
      goto done;
    }
    offset += written;
  }
  if (offset != tstr_len(*out)) {
    tstr_freep(out);
    rc = TURBO_EPROTO;
  }
done:
  turbo_flow_fmq_pubsub_updates_destroy(cursor);
  return rc;
}

static void flow_fmq_pubsub_recover_request_header(
    const uint8_t *data, size_t data_size, turbo_flow_fmq_pubsub_recovery_kind_t *kind,
    uint64_t *request_id, int *unsupported_version) {
  *kind = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_ERROR;
  *request_id = 0u;
  *unsupported_version = 0;
  if (!data || data_size < TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE ||
      memcmp(data, FLOW_FMQ_PUBSUB_RECOVERY_MAGIC, sizeof(FLOW_FMQ_PUBSUB_RECOVERY_MAGIC)) != 0)
    return;
  *kind =
      (turbo_flow_fmq_pubsub_recovery_kind_t)flow_fmq_pubsub_read_u16(data +
                                                                     FLOW_FMQ_PUBSUB_HEADER_KIND);
  *request_id = flow_fmq_pubsub_read_u64(data + FLOW_FMQ_PUBSUB_HEADER_REQUEST_ID);
  *unsupported_version =
      flow_fmq_pubsub_read_u16(data + FLOW_FMQ_PUBSUB_HEADER_MAJOR) !=
          TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_MAJOR ||
      flow_fmq_pubsub_read_u16(data + FLOW_FMQ_PUBSUB_HEADER_MINOR) >
          TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_MINOR;
}

int turbo_flow_fmq_pubsub_recovery_stage(turbo_flow_msg_t *msg, void *ctx) {
  turbo_flow_fmq_pubsub_recovery_service_t *service =
      (turbo_flow_fmq_pubsub_recovery_service_t *)ctx;
  turbo_flow_fmq_pubsub_recovery_message_t request =
      TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
  turbo_flow_fmq_pubsub_recovery_kind_t recovered_kind;
  uint64_t recovered_request_id;
  int unsupported_version;
  tstr_t response = NULL;
  int rc;
  if (!msg || !service || !service->state) return TURBO_EINVAL;
  flow_fmq_pubsub_recover_request_header((const uint8_t *)msg->payload.data, msg->payload.len,
                                         &recovered_kind, &recovered_request_id,
                                         &unsupported_version);
  rc = turbo_flow_fmq_pubsub_recovery_message_decode((const uint8_t *)msg->payload.data,
                                                      msg->payload.len, &request);
  if (rc != TURBO_OK) {
    turbo_flow_fmq_pubsub_recovery_status_t status =
        unsupported_version ? TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_UNSUPPORTED_VERSION
                            : flow_fmq_pubsub_status_from_error(rc);
    rc = flow_fmq_pubsub_error_response(service, recovered_kind, recovered_request_id, status,
                                         rc, &response);
  } else if (request.kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES) {
    rc = flow_fmq_pubsub_capabilities_response(service, &request, &response);
  } else if (request.kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_SNAPSHOT) {
    rc = flow_fmq_pubsub_snapshot_response(service, &request, &response);
  } else if (request.kind == TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES) {
    rc = flow_fmq_pubsub_updates_response(service, &request, &response);
  } else {
    rc = TURBO_ENOTSUP;
  }
  if (rc != TURBO_OK) {
    turbo_flow_fmq_pubsub_recovery_status_t status = flow_fmq_pubsub_status_from_error(rc);
    tstr_freep(&response);
    rc = flow_fmq_pubsub_error_response(service, request.kind ? request.kind : recovered_kind,
                                         request.request_id ? request.request_id
                                                            : recovered_request_id,
                                         status, rc, &response);
  }
  if (rc != TURBO_OK) return rc;
  tstr_freep(&msg->owned_payload);
  msg->owned_payload = response;
  msg->payload = tstr_to_v(response);
  msg->status = TURBO_OK;
  return TURBO_OK;
}
