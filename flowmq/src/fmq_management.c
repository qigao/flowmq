#include "turbo_flow_fmq_management.h"

#include "turbo_error.h"
#include "turbo_parser.h"
#include "turbo_str_view.h"
#include "turbo_uuid.h"

#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define FLOW_TFMP_TARGET_NESTED_MAX 640u
#define FLOW_TFMP_RESOURCE_NESTED_MAX 1024u
#define FLOW_TFMP_DIAGNOSTIC_MAX 127u
#define FLOW_TFMP_EVENT_BODY_MAX 1024u
#define FLOW_TFMP_STORE_RECORD_OVERHEAD 512u
#define FLOW_TFMP_STORE_HEADER_SIZE 8u
#define FLOW_TFMP_STORE_METADATA_SIZE 94u
#define FLOW_TFMP_STORE_EVENT_METADATA_SIZE 24u
#define FLOW_TFMP_STORE_EVENT_RECORD_OVERHEAD 10u
#define FLOW_TFMP_STORE_VERSION_MAJOR 1u
#define FLOW_TFMP_STORE_VERSION_MINOR_OPERATIONS 0u
#define FLOW_TFMP_STORE_VERSION_MINOR_EVENTS 1u

typedef struct flow_tfmp_management_result_s {
  turbo_flow_tfmp_status_t status;
  const char *diagnostic;
  int native_status;
  uint32_t response_flags;
  turbo_flow_tfmp_disposition_t disposition;
} flow_tfmp_management_result_t;

typedef struct flow_tfmp_resource_entry_s {
  size_t source_index;
  turbo_flow_resource_metadata_t metadata;
} flow_tfmp_resource_entry_t;

typedef struct flow_tfmp_command_record_s {
  int occupied;
  char client_id[TURBO_FLOW_TFMP_MANAGEMENT_CLIENT_ID_MAX + 1u];
  char idempotency_key[TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX + 1u];
  uint8_t *request_body;
  size_t request_body_size;
  turbo_flow_tfmp_status_t status;
  int native_status;
  const char *diagnostic;
  uint64_t generation_before;
  uint64_t generation_after;
  uint64_t observed_generation;
  uint64_t terminal_ns;
  uint64_t terminal_unix_ms;
  int durable;
  int has_operation;
  turbo_uuid_t operation_id;
  turbo_flow_tfmp_operation_state_t operation_state;
  uint64_t operation_revision;
  uint64_t submitted_unix_ms;
  uint64_t updated_unix_ms;
  uint64_t accepted_ns;
  uint64_t queue_deadline_ns;
  uint64_t queue_deadline_unix_ms;
  turbo_flow_tfmp_status_t terminal_status;
  turbo_flow_tfmp_disposition_t acceptance_disposition;
  int has_generation_result;
  char cancel_client_id[TURBO_FLOW_TFMP_MANAGEMENT_CLIENT_ID_MAX + 1u];
  char cancel_idempotency_key[TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX + 1u];
  uint8_t *cancel_request_body;
  size_t cancel_request_body_size;
} flow_tfmp_command_record_t;

typedef struct flow_tfmp_event_record_s {
  uint64_t sequence;
  uint16_t category;
  size_t body_size;
  uint8_t body[FLOW_TFMP_EVENT_BODY_MAX];
} flow_tfmp_event_record_t;

struct turbo_flow_tfmp_management_service_s {
  turbo_flow_tfmp_management_config_t config;
  turbo_uuid_t incarnation_id;
  turbo_flow_tfmp_owner_state_t state;
  uint64_t started_ns;
  uint64_t catalog_generation;
  size_t catalog_resource_count;
  turbo_flow_t *target;
  char target_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  turbo_flow_state_t target_state;
  int target_accepting_publishes;
  uint64_t target_generation;
  flow_tfmp_command_record_t *command_records;
  size_t command_record_count;
  uint8_t *reply_body;
  size_t reply_body_capacity;
  uint8_t *stage_reply;
  flow_tfmp_event_record_t *events;
  size_t event_head;
  size_t event_count;
  uint64_t event_sequence;
  int durable_event_replay;
  const turbo_flow_blob_store_t *operation_store;
  char operation_store_key[TURBO_FLOW_TFMP_MANAGEMENT_STORE_KEY_MAX + 1u];
  uint8_t *store_buffer;
  size_t store_buffer_capacity;
  uint8_t *store_record_buffer;
  size_t store_record_buffer_capacity;
  turbo_flow_tfmp_reconcile_binding_t reconciler;
  size_t recovery_required_count;
};

static int flow_tfmp_management_store_commit(turbo_flow_tfmp_management_service_t *service);

static int flow_tfmp_management_operation_terminal(turbo_flow_tfmp_operation_state_t state);

static int
flow_tfmp_management_reconcile_supported(const turbo_flow_tfmp_management_service_t *service,
                                         uint16_t command_type) {
  if (command_type == TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE ||
      command_type == TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME ||
      command_type == TURBO_FLOW_TFMP_COMMAND_FLOW_DRAIN ||
      command_type == TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE)
    return 1;
  return service->reconciler.supports && service->reconciler.inspect &&
         service->reconciler.supports(service->reconciler.ctx, command_type) != 0;
}

static void flow_tfmp_management_result_fail(flow_tfmp_management_result_t *result,
                                             turbo_flow_tfmp_status_t status,
                                             const char *diagnostic, int native_status) {
  result->status = status;
  result->diagnostic = diagnostic;
  result->native_status = native_status;
}

static uint64_t flow_tfmp_management_unix_ms(void) {
  struct timespec value;
  if (timespec_get(&value, TIME_UTC) != TIME_UTC || value.tv_sec < 0) return 0u;
  if ((uint64_t)value.tv_sec > UINT64_MAX / UINT64_C(1000)) return UINT64_MAX;
  return (uint64_t)value.tv_sec * UINT64_C(1000) + (uint64_t)value.tv_nsec / UINT64_C(1000000);
}

static const char *flow_tfmp_management_event_category_name(uint16_t category) {
  switch (category) {
  case TURBO_FLOW_TFMP_EVENT_LIFECYCLE:
    return "lifecycle";
  case TURBO_FLOW_TFMP_EVENT_RESOURCE:
    return "resource";
  case TURBO_FLOW_TFMP_EVENT_OPERATION:
    return "operation";
  case TURBO_FLOW_TFMP_EVENT_FAULT:
    return "fault";
  default:
    return NULL;
  }
}

static int flow_tfmp_management_event_emit(turbo_flow_tfmp_management_service_t *service,
                                           uint16_t category, uint16_t event_type,
                                           const turbo_flow_tfmp_field_t *target_uid,
                                           int has_generation, uint64_t generation,
                                           const turbo_uuid_t *operation_id,
                                           uint64_t operation_revision) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  flow_tfmp_event_record_t *record;
  flow_tfmp_event_record_t previous_record;
  uint8_t body[FLOW_TFMP_EVENT_BODY_MAX];
  size_t previous_head;
  size_t previous_count;
  uint64_t previous_sequence;
  size_t index;
  int rc;
  if (!service || service->config.event_capacity == 0u) return TURBO_OK;
  if (!flow_tfmp_management_event_category_name(category) || service->event_sequence == UINT64_MAX)
    return TURBO_ERANGE;
  rc = turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body));
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, service->config.authority_id,
                                                  strlen(service->config.authority_id));
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 2u, 1, service->incarnation_id.bytes,
                                             sizeof(service->incarnation_id.bytes));
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 3u, 1, category);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 4u, 1, event_type);
  if (rc == TURBO_OK && target_uid)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 5u, 0, (const char *)target_uid->value,
                                                  target_uid->value_size);
  if (rc == TURBO_OK && has_generation)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 6u, 0, generation);
  if (rc == TURBO_OK && operation_id)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 7u, 0, operation_id->bytes,
                                             sizeof(operation_id->bytes));
  if (rc == TURBO_OK && operation_id)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 8u, 0, operation_revision);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 9u, 1, flow_tfmp_management_unix_ms());
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 10u, 1, 0u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 11u, 1, 0u);
  if (rc != TURBO_OK) return rc;
  previous_head = service->event_head;
  previous_count = service->event_count;
  previous_sequence = service->event_sequence;
  if (service->event_count < service->config.event_capacity) {
    index = (service->event_head + service->event_count) % service->config.event_capacity;
    ++service->event_count;
  } else {
    index = service->event_head;
    service->event_head = (service->event_head + 1u) % service->config.event_capacity;
  }
  record = &service->events[index];
  previous_record = *record;
  ++service->event_sequence;
  record->sequence = service->event_sequence;
  record->category = category;
  record->body_size = builder.length;
  memcpy(record->body, body, builder.length);
  if (service->durable_event_replay) {
    rc = flow_tfmp_management_store_commit(service);
    if (rc != TURBO_OK) {
      *record = previous_record;
      service->event_head = previous_head;
      service->event_count = previous_count;
      service->event_sequence = previous_sequence;
      return rc;
    }
  }
  return TURBO_OK;
}

static int flow_tfmp_management_append_identity(const turbo_flow_tfmp_management_service_t *service,
                                                turbo_flow_tfmp_body_builder_t *builder) {
  size_t authority_size = strlen(service->config.authority_id);
  int rc = turbo_flow_tfmp_body_builder_append_utf8(builder, 120u, 1, service->config.authority_id,
                                                    authority_size);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append(builder, 121u, 1, service->incarnation_id.bytes,
                                             sizeof(service->incarnation_id.bytes));
  return rc;
}

static int flow_tfmp_management_find_body_field(const uint8_t *body, size_t body_size,
                                                uint8_t field_id, turbo_flow_tfmp_field_t *out) {
  turbo_flow_tfmp_field_iterator_t iterator = TURBO_FLOW_TFMP_FIELD_ITERATOR_INIT;
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  int rc;
  if ((!body && body_size > 0u) || !out) return TURBO_EINVAL;
  rc = turbo_flow_tfmp_field_iterator_init(&iterator, body, body_size);
  if (rc != TURBO_OK) return rc;
  while ((rc = turbo_flow_tfmp_field_iterator_next(&iterator, &field)) == TURBO_OK) {
    if (field.id == field_id) {
      *out = field;
      return TURBO_OK;
    }
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  }
  return rc;
}

static int flow_tfmp_management_find_field(const turbo_flow_tfmp_envelope_t *envelope,
                                           uint8_t field_id, turbo_flow_tfmp_field_t *out) {
  if (!envelope) return TURBO_EINVAL;
  return flow_tfmp_management_find_body_field(envelope->body, envelope->body_size, field_id, out);
}

static int flow_tfmp_management_field_equals(const turbo_flow_tfmp_field_t *field,
                                             const char *value) {
  size_t value_size = strlen(value);
  return field && field->value_size == value_size &&
         (value_size == 0u || memcmp(field->value, value, value_size) == 0);
}

static int flow_tfmp_management_validate_uid(const turbo_flow_tfmp_field_t *field, int allow_empty,
                                             flow_tfmp_management_result_t *result) {
  if ((!allow_empty && field->value_size == 0u) ||
      field->value_size > TURBO_FLOW_RESOURCE_UID_MAX) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT,
                                     "UID length is outside the supported range", TURBO_EINVAL);
  }
  return TURBO_OK;
}

static int flow_tfmp_management_compare_field(const char *value,
                                              const turbo_flow_tfmp_field_t *field) {
  size_t value_size = strlen(value);
  size_t common_size = value_size < field->value_size ? value_size : field->value_size;
  int compared = common_size > 0u ? memcmp(value, field->value, common_size) : 0;
  if (compared != 0) return compared;
  if (value_size < field->value_size) return -1;
  if (value_size > field->value_size) return 1;
  return 0;
}

static turbo_flow_tfmp_status_t flow_tfmp_management_query_status(int error) {
  switch (error) {
  case TURBO_EINVAL:
  case TURBO_ERANGE:
  case TURBO_EPROTO:
  case TURBO_ECHARSET:
    return TURBO_FLOW_TFMP_STATUS_INTERNAL;
  default:
    return turbo_flow_tfmp_status_from_error(error);
  }
}

static int flow_tfmp_management_refresh_catalog(turbo_flow_tfmp_management_service_t *service) {
  size_t resource_count;
  if (!service->target) return TURBO_ENOTSUP;
  resource_count = turbo_flow_resource_metadata_count(service->target);
  if (resource_count == service->catalog_resource_count) return TURBO_OK;
  if (service->catalog_generation == UINT64_MAX) return TURBO_ERANGE;
  service->catalog_resource_count = resource_count;
  ++service->catalog_generation;
  return TURBO_OK;
}

static int flow_tfmp_management_target_snapshot(turbo_flow_tfmp_management_service_t *service,
                                                turbo_flow_runtime_snapshot_t *snapshot) {
  int rc;
  if (!service->target || !snapshot) return TURBO_ENOTSUP;
  rc = turbo_flow_runtime_snapshot(service->target, snapshot);
  if (rc != TURBO_OK) return rc;
  if (snapshot->state != service->target_state ||
      snapshot->accepting_publishes != service->target_accepting_publishes) {
    if (service->target_generation == UINT64_MAX) return TURBO_ERANGE;
    service->target_state = snapshot->state;
    service->target_accepting_publishes = snapshot->accepting_publishes;
    ++service->target_generation;
  }
  return TURBO_OK;
}

static int flow_tfmp_management_target_state(turbo_flow_state_t state, uint16_t *wire_state) {
  if (!wire_state) return TURBO_EINVAL;
  switch (state) {
  case TURBO_FLOW_STATE_NEW:
    *wire_state = TURBO_FLOW_TFMP_TARGET_STATE_NEW;
    return TURBO_OK;
  case TURBO_FLOW_STATE_PARSED:
    *wire_state = TURBO_FLOW_TFMP_TARGET_STATE_PARSED;
    return TURBO_OK;
  case TURBO_FLOW_STATE_COMPILED:
    *wire_state = TURBO_FLOW_TFMP_TARGET_STATE_COMPILED;
    return TURBO_OK;
  case TURBO_FLOW_STATE_STARTED:
    *wire_state = TURBO_FLOW_TFMP_TARGET_STATE_STARTED;
    return TURBO_OK;
  case TURBO_FLOW_STATE_STOPPED:
    *wire_state = TURBO_FLOW_TFMP_TARGET_STATE_STOPPED;
    return TURBO_OK;
  case TURBO_FLOW_STATE_FAILED:
    *wire_state = TURBO_FLOW_TFMP_TARGET_STATE_FAILED;
    return TURBO_OK;
  default:
    return TURBO_EPROTO;
  }
}

static int flow_tfmp_management_resource_kind(turbo_flow_resource_kind_t kind,
                                              uint16_t *wire_kind) {
  if (!wire_kind) return TURBO_EINVAL;
  switch (kind) {
  case TURBO_FLOW_RESOURCE_CONNECTION:
    *wire_kind = TURBO_FLOW_TFMP_RESOURCE_KIND_CONNECTION;
    return TURBO_OK;
  case TURBO_FLOW_RESOURCE_QUEUE_BUFFER:
    *wire_kind = TURBO_FLOW_TFMP_RESOURCE_KIND_QUEUE_BUFFER;
    return TURBO_OK;
  case TURBO_FLOW_RESOURCE_POOL:
    *wire_kind = TURBO_FLOW_TFMP_RESOURCE_KIND_POOL;
    return TURBO_OK;
  case TURBO_FLOW_RESOURCE_RUNTIME:
    *wire_kind = TURBO_FLOW_TFMP_RESOURCE_KIND_RUNTIME;
    return TURBO_OK;
  case TURBO_FLOW_RESOURCE_SEGMENT:
    *wire_kind = TURBO_FLOW_TFMP_RESOURCE_KIND_SEGMENT;
    return TURBO_OK;
  case TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE:
    *wire_kind = TURBO_FLOW_TFMP_RESOURCE_KIND_PROTOCOL_AGGREGATE;
    return TURBO_OK;
  case TURBO_FLOW_RESOURCE_STORAGE:
    *wire_kind = TURBO_FLOW_TFMP_RESOURCE_KIND_STORAGE;
    return TURBO_OK;
  case TURBO_FLOW_RESOURCE_RULE_SET:
    *wire_kind = TURBO_FLOW_TFMP_RESOURCE_KIND_RULE_SET;
    return TURBO_OK;
  case TURBO_FLOW_RESOURCE_SECURITY_REALM:
    *wire_kind = TURBO_FLOW_TFMP_RESOURCE_KIND_SECURITY_REALM;
    return TURBO_OK;
  default:
    return TURBO_EPROTO;
  }
}

static int flow_tfmp_management_build_target_nested(turbo_flow_tfmp_management_service_t *service,
                                                    uint8_t *out, size_t capacity,
                                                    size_t *out_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  turbo_flow_runtime_snapshot_t snapshot;
  uint16_t wire_state;
  int rc = flow_tfmp_management_target_snapshot(service, &snapshot);
  if (rc != TURBO_OK) return rc;
  rc = flow_tfmp_management_target_state(snapshot.state, &wire_state);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_init(&builder, out, capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, service->target_uid,
                                                  strlen(service->target_uid));
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 2u, 1, TURBO_FLOW_TFMP_TARGET_KIND_FLOW);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 3u, 1, service->target_generation);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 4u, 1, service->target_generation);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 5u, 1, wire_state);
  if (rc == TURBO_OK) *out_size = builder.length;
  return rc;
}

static int
flow_tfmp_management_build_resource_nested(const turbo_flow_resource_metadata_t *metadata,
                                           uint8_t *out, size_t capacity, size_t *out_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  uint16_t wire_kind;
  uint16_t wire_state = metadata->observed_generation == metadata->generation
                            ? TURBO_FLOW_TFMP_RESOURCE_STATE_OBSERVED
                            : TURBO_FLOW_TFMP_RESOURCE_STATE_RECONCILING;
  int rc = flow_tfmp_management_resource_kind(metadata->kind, &wire_kind);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_init(&builder, out, capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, metadata->uid,
                                                  strlen(metadata->uid));
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 2u, 1, wire_kind);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 3u, 1, metadata->generation);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 4u, 1, metadata->observed_generation);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 5u, 1, wire_state);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 6u, 1, metadata->owner_name,
                                                  strlen(metadata->owner_name));
  if (rc == TURBO_OK) *out_size = builder.length;
  return rc;
}

static int flow_tfmp_management_resource_compare(const void *lhs, const void *rhs) {
  const flow_tfmp_resource_entry_t *left = (const flow_tfmp_resource_entry_t *)lhs;
  const flow_tfmp_resource_entry_t *right = (const flow_tfmp_resource_entry_t *)rhs;
  return strcmp(left->metadata.uid, right->metadata.uid);
}

static int flow_tfmp_management_resource_snapshot(turbo_flow_tfmp_management_service_t *service,
                                                  flow_tfmp_resource_entry_t **out,
                                                  size_t *out_count) {
  flow_tfmp_resource_entry_t *entries = NULL;
  size_t count;
  int rc;
  if (!out || !out_count || !service->target) return TURBO_EINVAL;
  *out = NULL;
  *out_count = 0u;
  count = turbo_flow_resource_metadata_count(service->target);
  if (count > SIZE_MAX / sizeof(*entries)) return TURBO_ERANGE;
  if (count > 0u) {
    entries = (flow_tfmp_resource_entry_t *)malloc(count * sizeof(*entries));
    if (!entries) return TURBO_ENOMEM;
  }
  for (size_t i = 0u; i < count; ++i) {
    entries[i].source_index = i;
    entries[i].metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    rc = turbo_flow_resource_metadata_at(service->target, i, &entries[i].metadata);
    if (rc != TURBO_OK) {
      free(entries);
      return rc;
    }
  }
  if (count > 1u) qsort(entries, count, sizeof(*entries), flow_tfmp_management_resource_compare);
  for (size_t i = 1u; i < count; ++i) {
    if (strcmp(entries[i - 1u].metadata.uid, entries[i].metadata.uid) == 0) {
      free(entries);
      return TURBO_EPROTO;
    }
  }
  *out = entries;
  *out_count = count;
  return TURBO_OK;
}

static int flow_tfmp_management_resource_find(turbo_flow_tfmp_management_service_t *service,
                                              const turbo_flow_tfmp_field_t *uid,
                                              size_t *source_index,
                                              turbo_flow_resource_metadata_t *metadata) {
  size_t count = turbo_flow_resource_metadata_count(service->target);
  for (size_t i = 0u; i < count; ++i) {
    turbo_flow_resource_metadata_t candidate = TURBO_FLOW_RESOURCE_METADATA_INIT;
    int rc = turbo_flow_resource_metadata_at(service->target, i, &candidate);
    if (rc != TURBO_OK) return rc;
    if (uid->value_size == strlen(candidate.uid) &&
        memcmp(uid->value, candidate.uid, uid->value_size) == 0) {
      *source_index = i;
      *metadata = candidate;
      return TURBO_OK;
    }
  }
  return TURBO_ENOENT;
}

static uint16_t flow_tfmp_management_command_schema(uint16_t command_type) {
  switch (command_type) {
  case TURBO_FLOW_TFMP_COMMAND_ENDPOINT_REPLACE:
    return TURBO_FLOW_TFMP_COMMAND_SCHEMA_ENDPOINT;
  case TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE:
    return TURBO_FLOW_TFMP_COMMAND_SCHEMA_POOL;
  default:
    return 0u;
  }
}

static int flow_tfmp_management_build_command_descriptor(uint16_t command_type,
                                                         uint16_t durability_mask, uint8_t *out,
                                                         size_t capacity, size_t *out_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  uint16_t payload_schema = flow_tfmp_management_command_schema(command_type);
  int rc = turbo_flow_tfmp_body_builder_init(&builder, out, capacity);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 1u, 1, command_type);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 2u, 1,
                                                 TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 3u, 1, durability_mask);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 4u, 1, payload_schema);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 5u, 1, payload_schema ? 1u : 0u);
  if (rc == TURBO_OK) *out_size = builder.length;
  return rc;
}

static int flow_tfmp_management_build_capabilities(turbo_flow_tfmp_management_service_t *service,
                                                   size_t *body_size) {
  static const uint8_t volatile_capabilities[] = {
      0u, TURBO_FLOW_TFMP_CAPABILITY_TARGET_QUERY,
      0u, TURBO_FLOW_TFMP_CAPABILITY_RESOURCE_QUERY,
      0u, TURBO_FLOW_TFMP_CAPABILITY_CONDITIONAL_COMMAND,
      0u, TURBO_FLOW_TFMP_CAPABILITY_VOLATILE_OPERATION,
      0u, TURBO_FLOW_TFMP_CAPABILITY_OPERATION_CANCEL,
      0u, TURBO_FLOW_TFMP_CAPABILITY_LIVE_EVENT,
      0u, TURBO_FLOW_TFMP_CAPABILITY_EVENT_REPLAY};
  static const uint8_t durable_capabilities[] = {0u, TURBO_FLOW_TFMP_CAPABILITY_TARGET_QUERY,
                                                  0u, TURBO_FLOW_TFMP_CAPABILITY_RESOURCE_QUERY,
                                                  0u, TURBO_FLOW_TFMP_CAPABILITY_CONDITIONAL_COMMAND,
                                                  0u, TURBO_FLOW_TFMP_CAPABILITY_VOLATILE_OPERATION,
                                                 0u, TURBO_FLOW_TFMP_CAPABILITY_DURABLE_OPERATION,
                                                 0u, TURBO_FLOW_TFMP_CAPABILITY_OPERATION_CANCEL,
                                                 0u, TURBO_FLOW_TFMP_CAPABILITY_LIVE_EVENT,
                                                 0u, TURBO_FLOW_TFMP_CAPABILITY_EVENT_REPLAY};
  static const uint16_t command_types[] = {
      TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE,      TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME,
      TURBO_FLOW_TFMP_COMMAND_FLOW_DRAIN,      TURBO_FLOW_TFMP_COMMAND_RESOURCE_QUIESCE,
      TURBO_FLOW_TFMP_COMMAND_RESOURCE_RESUME, TURBO_FLOW_TFMP_COMMAND_ENDPOINT_REPLACE,
      TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE};
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  uint8_t descriptor[64];
  const uint8_t *capabilities =
      service->target ? (service->operation_store ? durable_capabilities : volatile_capabilities)
                      : NULL;
  size_t capability_size = service->target
                               ? (service->operation_store ? sizeof(durable_capabilities)
                                                           : sizeof(volatile_capabilities))
                               : 0u;
  uint16_t durability_mask =
      TURBO_FLOW_TFMP_DURABILITY_MASK_VOLATILE |
      (service->operation_store ? TURBO_FLOW_TFMP_DURABILITY_MASK_DURABLE : 0u);
  if (service->target && service->config.event_capacity == 0u) capability_size -= 4u;
  int rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                             service->reply_body_capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 1u, 1, TURBO_FLOW_TFMP_PROTOCOL_MINOR);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 2u, 1, TURBO_FLOW_TFMP_PROTOCOL_MINOR);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 3u, 1, capabilities, capability_size);
  for (size_t i = 0u;
       rc == TURBO_OK && service->target && i < sizeof(command_types) / sizeof(command_types[0]);
       ++i) {
    size_t descriptor_size = 0u;
    uint16_t command_durability =
        TURBO_FLOW_TFMP_DURABILITY_MASK_VOLATILE |
        (service->operation_store &&
                 flow_tfmp_management_reconcile_supported(service, command_types[i])
             ? TURBO_FLOW_TFMP_DURABILITY_MASK_DURABLE
             : 0u);
    rc = flow_tfmp_management_build_command_descriptor(
        command_types[i], command_durability, descriptor, sizeof(descriptor), &descriptor_size);
    if (rc == TURBO_OK)
      rc = turbo_flow_tfmp_body_builder_append(&builder, 4u, 0, descriptor, descriptor_size);
  }
  if (rc == TURBO_OK)
    rc =
        turbo_flow_tfmp_body_builder_append_u32(&builder, 5u, 1, service->config.max_request_bytes);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u32(&builder, 6u, 1, service->config.max_reply_bytes);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 7u, 1, TURBO_FLOW_TFMP_MAX_FIELDS);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 8u, 1, TURBO_FLOW_TFMP_MAX_NESTING);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u32(&builder, 9u, 1, service->config.max_page_items);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 10u, 1, durability_mask);
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int flow_tfmp_management_build_health(turbo_flow_tfmp_management_service_t *service,
                                             size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  uint64_t now_ns = turbo_hrtime();
  uint64_t uptime_ms =
      now_ns >= service->started_ns ? (now_ns - service->started_ns) / UINT64_C(1000000) : 0u;
  int rc = service->target ? flow_tfmp_management_refresh_catalog(service) : TURBO_OK;
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                           service->reply_body_capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 1u, 1, (uint16_t)service->state);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 2u, 1, uptime_ms);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 3u, 1, service->catalog_generation);
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int flow_tfmp_management_parse_page(turbo_flow_tfmp_management_service_t *service,
                                           const turbo_flow_tfmp_envelope_t *request,
                                           uint8_t limit_field_id, uint8_t after_field_id,
                                           uint8_t generation_field_id, uint32_t *page_limit,
                                           turbo_flow_tfmp_field_t *after,
                                           flow_tfmp_management_result_t *result) {
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  uint64_t requested_generation;
  int rc = flow_tfmp_management_find_field(request, limit_field_id, &field);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_field_read_u32(&field, page_limit);
  if (rc != TURBO_OK) return rc;
  if (*page_limit == 0u) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT,
                                     "page_limit must be positive", TURBO_EINVAL);
    return TURBO_OK;
  }
  if (*page_limit > service->config.max_page_items) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_RESOURCE_EXHAUSTED,
                                     "page_limit exceeds configured management limit",
                                     TURBO_EMSGSIZE);
    return TURBO_OK;
  }
  *after = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  rc = flow_tfmp_management_find_field(request, after_field_id, after);
  if (rc != TURBO_OK && rc != TURBO_ENOENT) return rc;
  if (rc == TURBO_OK) {
    flow_tfmp_management_validate_uid(after, 1, result);
    if (result->status != TURBO_FLOW_TFMP_STATUS_OK) return TURBO_OK;
  }
  field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  rc = flow_tfmp_management_find_field(request, generation_field_id, &field);
  if (rc == TURBO_ENOENT) return TURBO_OK;
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_tfmp_field_read_u64(&field, &requested_generation);
  if (rc != TURBO_OK) return rc;
  if (requested_generation != service->catalog_generation) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_STALE_CURSOR,
                                     "catalog changed while paginating", TURBO_EBUSY);
  }
  return TURBO_OK;
}

static int flow_tfmp_management_build_target_list(turbo_flow_tfmp_management_service_t *service,
                                                  const turbo_flow_tfmp_envelope_t *request,
                                                  flow_tfmp_management_result_t *result,
                                                  size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  turbo_flow_tfmp_field_t after = TURBO_FLOW_TFMP_FIELD_INIT;
  uint8_t target[FLOW_TFMP_TARGET_NESTED_MAX];
  uint32_t page_limit = 0u;
  size_t target_size = 0u;
  int rc = flow_tfmp_management_refresh_catalog(service);
  if (rc == TURBO_OK)
    rc = flow_tfmp_management_parse_page(service, request, 1u, 2u, 3u, &page_limit, &after, result);
  if (rc != TURBO_OK || result->status != TURBO_FLOW_TFMP_STATUS_OK) return rc;
  rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                         service->reply_body_capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 1u, 1, service->catalog_generation);
  if (rc == TURBO_OK &&
      (!after.value || flow_tfmp_management_compare_field(service->target_uid, &after) > 0)) {
    rc = flow_tfmp_management_build_target_nested(service, target, sizeof(target), &target_size);
    if (rc == TURBO_OK)
      rc = turbo_flow_tfmp_body_builder_append(&builder, 2u, 0, target, target_size);
  }
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int flow_tfmp_management_build_target_get(turbo_flow_tfmp_management_service_t *service,
                                                 const turbo_flow_tfmp_envelope_t *request,
                                                 flow_tfmp_management_result_t *result,
                                                 size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  turbo_flow_tfmp_field_t uid = TURBO_FLOW_TFMP_FIELD_INIT;
  uint8_t target[FLOW_TFMP_TARGET_NESTED_MAX];
  size_t target_size = 0u;
  int rc = flow_tfmp_management_find_field(request, 1u, &uid);
  if (rc != TURBO_OK) return rc;
  flow_tfmp_management_validate_uid(&uid, 0, result);
  if (result->status != TURBO_FLOW_TFMP_STATUS_OK) return TURBO_OK;
  if (!flow_tfmp_management_field_equals(&uid, service->target_uid)) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_NOT_FOUND,
                                     "target UID was not found", TURBO_ENOENT);
    return TURBO_OK;
  }
  rc = flow_tfmp_management_build_target_nested(service, target, sizeof(target), &target_size);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                           service->reply_body_capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 1u, 1, target, target_size);
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int flow_tfmp_management_require_target_uid(turbo_flow_tfmp_management_service_t *service,
                                                   const turbo_flow_tfmp_envelope_t *request,
                                                   flow_tfmp_management_result_t *result) {
  turbo_flow_tfmp_field_t target_uid = TURBO_FLOW_TFMP_FIELD_INIT;
  int rc = flow_tfmp_management_find_field(request, 1u, &target_uid);
  if (rc != TURBO_OK) return rc;
  flow_tfmp_management_validate_uid(&target_uid, 0, result);
  if (result->status != TURBO_FLOW_TFMP_STATUS_OK) return TURBO_OK;
  if (!flow_tfmp_management_field_equals(&target_uid, service->target_uid)) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_NOT_FOUND,
                                     "target UID was not found", TURBO_ENOENT);
  }
  return TURBO_OK;
}

static int flow_tfmp_management_build_resource_list(turbo_flow_tfmp_management_service_t *service,
                                                    const turbo_flow_tfmp_envelope_t *request,
                                                    flow_tfmp_management_result_t *result,
                                                    size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  turbo_flow_tfmp_field_t after = TURBO_FLOW_TFMP_FIELD_INIT;
  flow_tfmp_resource_entry_t *entries = NULL;
  uint8_t resource[FLOW_TFMP_RESOURCE_NESTED_MAX];
  uint32_t page_limit = 0u;
  size_t count = 0u;
  size_t begin = 0u;
  size_t end;
  int rc = flow_tfmp_management_require_target_uid(service, request, result);
  if (rc != TURBO_OK || result->status != TURBO_FLOW_TFMP_STATUS_OK) return rc;
  rc = flow_tfmp_management_refresh_catalog(service);
  if (rc == TURBO_OK)
    rc = flow_tfmp_management_parse_page(service, request, 2u, 3u, 4u, &page_limit, &after, result);
  if (rc != TURBO_OK || result->status != TURBO_FLOW_TFMP_STATUS_OK) return rc;
  rc = flow_tfmp_management_resource_snapshot(service, &entries, &count);
  if (rc != TURBO_OK) return rc;
  while (begin < count && after.value &&
         flow_tfmp_management_compare_field(entries[begin].metadata.uid, &after) <= 0) {
    ++begin;
  }
  end = count - begin > page_limit ? begin + page_limit : count;
  rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                         service->reply_body_capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 1u, 1, service->catalog_generation);
  for (size_t i = begin; rc == TURBO_OK && i < end; ++i) {
    size_t resource_size = 0u;
    rc = flow_tfmp_management_build_resource_nested(&entries[i].metadata, resource,
                                                    sizeof(resource), &resource_size);
    if (rc == TURBO_OK)
      rc = turbo_flow_tfmp_body_builder_append(&builder, 2u, 0, resource, resource_size);
  }
  if (rc == TURBO_OK && end < count && end > begin)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 3u, 0, entries[end - 1u].metadata.uid,
                                                  strlen(entries[end - 1u].metadata.uid));
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK) *body_size = builder.length;
  free(entries);
  return rc;
}

static int flow_tfmp_management_resource_request(turbo_flow_tfmp_management_service_t *service,
                                                 const turbo_flow_tfmp_envelope_t *request,
                                                 flow_tfmp_management_result_t *result,
                                                 size_t *source_index,
                                                 turbo_flow_resource_metadata_t *metadata) {
  turbo_flow_tfmp_field_t resource_uid = TURBO_FLOW_TFMP_FIELD_INIT;
  int rc = flow_tfmp_management_require_target_uid(service, request, result);
  if (rc != TURBO_OK || result->status != TURBO_FLOW_TFMP_STATUS_OK) return rc;
  rc = flow_tfmp_management_find_field(request, 2u, &resource_uid);
  if (rc != TURBO_OK) return rc;
  flow_tfmp_management_validate_uid(&resource_uid, 0, result);
  if (result->status != TURBO_FLOW_TFMP_STATUS_OK) return TURBO_OK;
  rc = flow_tfmp_management_resource_find(service, &resource_uid, source_index, metadata);
  if (rc == TURBO_ENOENT) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_NOT_FOUND,
                                     "resource UID was not found", rc);
    return TURBO_OK;
  }
  return rc;
}

static int flow_tfmp_management_build_resource_get(turbo_flow_tfmp_management_service_t *service,
                                                   const turbo_flow_tfmp_envelope_t *request,
                                                   flow_tfmp_management_result_t *result,
                                                   size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  uint8_t resource[FLOW_TFMP_RESOURCE_NESTED_MAX];
  size_t source_index = 0u;
  size_t resource_size = 0u;
  int rc =
      flow_tfmp_management_resource_request(service, request, result, &source_index, &metadata);
  (void)source_index;
  if (rc != TURBO_OK || result->status != TURBO_FLOW_TFMP_STATUS_OK) return rc;
  rc = flow_tfmp_management_build_resource_nested(&metadata, resource, sizeof(resource),
                                                  &resource_size);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                           service->reply_body_capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 1u, 1, resource, resource_size);
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int flow_tfmp_management_build_resource_document(
    turbo_flow_tfmp_management_service_t *service, const turbo_flow_tfmp_envelope_t *request,
    flow_tfmp_management_result_t *result, size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
  uint8_t resource[FLOW_TFMP_RESOURCE_NESTED_MAX];
  size_t source_index = 0u;
  size_t resource_size = 0u;
  int rc =
      flow_tfmp_management_resource_request(service, request, result, &source_index, &metadata);
  if (rc != TURBO_OK || result->status != TURBO_FLOW_TFMP_STATUS_OK) return rc;
  rc = turbo_flow_resource_document_at(service->target, source_index,
                                       TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document);
  if (rc != TURBO_OK) goto cleanup;
  if (!document.schema || document.schema->schema_id > UINT16_MAX ||
      document.schema->schema_version > UINT16_MAX) {
    rc = TURBO_ERANGE;
    goto cleanup;
  }
  metadata.domain = document.domain;
  metadata.kind = document.resource_kind;
  memcpy(metadata.uid, document.uid, sizeof(metadata.uid));
  memcpy(metadata.owner_name, document.owner_name, sizeof(metadata.owner_name));
  metadata.generation = document.generation;
  metadata.observed_generation = document.observed_generation;
  rc = flow_tfmp_management_build_resource_nested(&metadata, resource, sizeof(resource),
                                                  &resource_size);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                           service->reply_body_capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 1u, 1, resource, resource_size);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 2u, 1,
                                                 (uint16_t)document.schema->schema_id);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 3u, 1,
                                                 (uint16_t)document.schema->schema_version);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append(
        &builder, 4u, 1, (const uint8_t *)mem_buffer_const_data(document.payload),
        mem_buffer_used(document.payload));
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK) *body_size = builder.length;

cleanup:
  turbo_flow_resource_document_cleanup(&document);
  return rc;
}

typedef struct flow_tfmp_command_request_s {
  turbo_flow_tfmp_field_t client_id;
  turbo_flow_tfmp_field_t idempotency_key;
  turbo_flow_tfmp_field_t target_uid;
  turbo_flow_tfmp_field_t payload;
  uint16_t command_type;
  uint16_t reply_mode;
  uint16_t required_durability;
  int has_expected_generation;
  uint64_t expected_generation;
  uint64_t queue_timeout_ms;
  uint64_t operation_timeout_ms;
  int has_payload;
} flow_tfmp_command_request_t;

static int flow_tfmp_management_parse_command(const turbo_flow_tfmp_envelope_t *request,
                                              flow_tfmp_command_request_t *command,
                                              flow_tfmp_management_result_t *result) {
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  int rc;
  memset(command, 0, sizeof(*command));
  command->client_id = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  command->idempotency_key = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  command->target_uid = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  command->payload = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  rc = flow_tfmp_management_find_field(request, 1u, &command->client_id);
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 2u, &command->idempotency_key);
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 3u, &command->target_uid);
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 4u, &field);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_field_read_u16(&field, &command->command_type);
  field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 5u, &field);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_field_read_u16(&field, &command->reply_mode);
  field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 6u, &field);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_field_read_u16(&field, &command->required_durability);
  field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  if (rc == TURBO_OK) {
    rc = flow_tfmp_management_find_field(request, 7u, &field);
    if (rc == TURBO_OK) {
      command->has_expected_generation = 1;
      rc = turbo_flow_tfmp_field_read_u64(&field, &command->expected_generation);
    } else if (rc == TURBO_ENOENT) {
      rc = TURBO_OK;
    }
  }
  field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 8u, &field);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_field_read_u64(&field, &command->queue_timeout_ms);
  field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 9u, &field);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_field_read_u64(&field, &command->operation_timeout_ms);
  if (rc == TURBO_OK) {
    rc = flow_tfmp_management_find_field(request, 10u, &command->payload);
    if (rc == TURBO_OK) {
      command->has_payload = 1;
    } else if (rc == TURBO_ENOENT) {
      rc = TURBO_OK;
    }
  }
  if (rc != TURBO_OK) return rc;
  if (command->client_id.value_size == 0u ||
      command->client_id.value_size > TURBO_FLOW_TFMP_MANAGEMENT_CLIENT_ID_MAX ||
      command->idempotency_key.value_size == 0u ||
      command->idempotency_key.value_size > TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT,
                                     "client or idempotency identity is invalid", TURBO_EINVAL);
    return TURBO_OK;
  }
  flow_tfmp_management_validate_uid(&command->target_uid, 0, result);
  return TURBO_OK;
}

static int flow_tfmp_management_record_text_equals(const char *stored,
                                                   const turbo_flow_tfmp_field_t *field) {
  size_t stored_size = strlen(stored);
  return stored_size == field->value_size && memcmp(stored, field->value, field->value_size) == 0;
}

static int flow_tfmp_management_record_expired(const turbo_flow_tfmp_management_service_t *service,
                                               const flow_tfmp_command_record_t *record,
                                               uint64_t now_ns) {
  uint64_t now_unix_ms = flow_tfmp_management_unix_ms();
  (void)now_ns;
  return record->terminal_unix_ms != 0u && now_unix_ms >= record->terminal_unix_ms &&
         now_unix_ms - record->terminal_unix_ms >= service->config.dedup_ttl_ms;
}

static void flow_tfmp_management_record_clear(flow_tfmp_command_record_t *record) {
  free(record->request_body);
  free(record->cancel_request_body);
  memset(record, 0, sizeof(*record));
}

static int flow_tfmp_management_records_purge(turbo_flow_tfmp_management_service_t *service,
                                              uint64_t now_ns) {
  size_t durable_removed = 0u;
  for (size_t i = 0u; i < service->config.dedup_capacity; ++i) {
    flow_tfmp_command_record_t *record = &service->command_records[i];
    if (!record->occupied || !flow_tfmp_management_record_expired(service, record, now_ns))
      continue;
    if (record->durable) {
      record->occupied = 0;
      ++durable_removed;
    } else {
      flow_tfmp_management_record_clear(record);
    }
    --service->command_record_count;
  }
  if (durable_removed > 0u) {
    int rc = flow_tfmp_management_store_commit(service);
    if (rc != TURBO_OK) {
      for (size_t i = 0u; i < service->config.dedup_capacity; ++i) {
        flow_tfmp_command_record_t *record = &service->command_records[i];
        if (!record->occupied && record->durable && record->request_body) {
          record->occupied = 1;
          ++service->command_record_count;
        }
      }
      return rc;
    }
    for (size_t i = 0u; i < service->config.dedup_capacity; ++i) {
      flow_tfmp_command_record_t *record = &service->command_records[i];
      if (!record->occupied && record->durable && record->request_body)
        flow_tfmp_management_record_clear(record);
    }
  }
  return TURBO_OK;
}

static size_t
flow_tfmp_management_active_operation_count(const turbo_flow_tfmp_management_service_t *service) {
  size_t count = 0u;
  for (size_t i = 0u; i < service->config.dedup_capacity; ++i) {
    const flow_tfmp_command_record_t *record = &service->command_records[i];
    if (record->occupied && record->has_operation &&
        !flow_tfmp_management_operation_terminal(record->operation_state))
      ++count;
  }
  return count;
}

static int flow_tfmp_management_record_lookup(turbo_flow_tfmp_management_service_t *service,
                                              const flow_tfmp_command_request_t *command,
                                              const turbo_flow_tfmp_envelope_t *request,
                                              flow_tfmp_command_record_t **out) {
  for (size_t i = 0u; i < service->config.dedup_capacity; ++i) {
    flow_tfmp_command_record_t *record = &service->command_records[i];
    if (!record->occupied ||
        !flow_tfmp_management_record_text_equals(record->client_id, &command->client_id) ||
        !flow_tfmp_management_record_text_equals(record->idempotency_key,
                                                 &command->idempotency_key))
      continue;
    if (record->request_body_size != request->body_size ||
        memcmp(record->request_body, request->body, request->body_size) != 0)
      return TURBO_EPROTO;
    *out = record;
    return TURBO_OK;
  }
  return TURBO_ENOENT;
}

static int flow_tfmp_management_record_reserve(turbo_flow_tfmp_management_service_t *service,
                                               const flow_tfmp_command_request_t *command,
                                               const turbo_flow_tfmp_envelope_t *request,
                                               flow_tfmp_command_record_t **out) {
  flow_tfmp_command_record_t *record = NULL;
  uint8_t *body_copy;
  if (service->command_record_count >= service->config.dedup_capacity) return TURBO_ENOSPC;
  body_copy = (uint8_t *)malloc(request->body_size);
  if (!body_copy) return TURBO_ENOMEM;
  memcpy(body_copy, request->body, request->body_size);
  for (size_t i = 0u; i < service->config.dedup_capacity; ++i) {
    if (!service->command_records[i].occupied) {
      record = &service->command_records[i];
      break;
    }
  }
  if (!record) {
    free(body_copy);
    return TURBO_ENOSPC;
  }
  memcpy(record->client_id, command->client_id.value, command->client_id.value_size);
  record->client_id[command->client_id.value_size] = '\0';
  memcpy(record->idempotency_key, command->idempotency_key.value,
         command->idempotency_key.value_size);
  record->idempotency_key[command->idempotency_key.value_size] = '\0';
  record->request_body = body_copy;
  record->request_body_size = request->body_size;
  record->occupied = 1;
  ++service->command_record_count;
  *out = record;
  return TURBO_OK;
}

static void flow_tfmp_management_record_finish(flow_tfmp_command_record_t *record,
                                               const flow_tfmp_management_result_t *result,
                                               uint64_t generation_before,
                                               uint64_t generation_after,
                                               uint64_t observed_generation) {
  record->status = result->status;
  record->native_status = result->native_status;
  record->diagnostic = result->diagnostic;
  record->generation_before = generation_before;
  record->generation_after = generation_after;
  record->observed_generation = observed_generation;
  record->has_generation_result = 1;
  record->terminal_ns = turbo_hrtime();
  record->terminal_unix_ms = flow_tfmp_management_unix_ms();
}

static int
flow_tfmp_management_build_command_response(turbo_flow_tfmp_management_service_t *service,
                                            const flow_tfmp_command_record_t *record,
                                            size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  int rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                             service->reply_body_capacity);
  if (rc == TURBO_OK && record->has_generation_result)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 1u, 0, record->generation_before);
  if (rc == TURBO_OK && record->has_generation_result)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 2u, 0, record->generation_after);
  if (rc == TURBO_OK && record->has_generation_result)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 3u, 0, record->observed_generation);
  if (rc == TURBO_OK && record->has_operation)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 4u, 0, record->operation_id.bytes,
                                             sizeof(record->operation_id.bytes));
  if (rc == TURBO_OK && record->has_operation)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 5u, 0, 1u);
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static flow_tfmp_command_record_t *
flow_tfmp_management_operation_find(turbo_flow_tfmp_management_service_t *service,
                                    const uint8_t operation_id[16]) {
  for (size_t i = 0u; i < service->config.dedup_capacity; ++i) {
    flow_tfmp_command_record_t *record = &service->command_records[i];
    if (record->occupied && record->has_operation &&
        memcmp(record->operation_id.bytes, operation_id, sizeof(record->operation_id.bytes)) == 0)
      return record;
  }
  return NULL;
}

static int flow_tfmp_management_operation_terminal(turbo_flow_tfmp_operation_state_t state) {
  return state == TURBO_FLOW_TFMP_OPERATION_SUCCEEDED ||
         state == TURBO_FLOW_TFMP_OPERATION_FAILED || state == TURBO_FLOW_TFMP_OPERATION_CANCELED;
}

static int
flow_tfmp_management_build_operation_response(turbo_flow_tfmp_management_service_t *service,
                                              const flow_tfmp_command_record_t *record,
                                              size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  turbo_flow_tfmp_field_t target_uid = TURBO_FLOW_TFMP_FIELD_INIT;
  int rc = flow_tfmp_management_find_body_field(record->request_body, record->request_body_size, 3u,
                                                &target_uid);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                           service->reply_body_capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 1u, 1, record->operation_id.bytes,
                                             sizeof(record->operation_id.bytes));
  if (rc == TURBO_OK)
    rc =
        turbo_flow_tfmp_body_builder_append_u16(&builder, 2u, 1, (uint16_t)record->operation_state);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 3u, 1, record->operation_revision);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 4u, 1, (const char *)target_uid.value,
                                                  target_uid.value_size);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 5u, 1, record->submitted_unix_ms);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 6u, 1, record->updated_unix_ms);
  if (rc == TURBO_OK && record->has_generation_result)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 7u, 0, record->generation_before);
  if (rc == TURBO_OK && record->has_generation_result)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 8u, 0, record->generation_after);
  if (rc == TURBO_OK && record->has_generation_result)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 9u, 0, record->observed_generation);
  if (rc == TURBO_OK && flow_tfmp_management_operation_terminal(record->operation_state))
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 10u, 0,
                                                 (uint16_t)record->terminal_status);
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int flow_tfmp_management_is_flow_command(uint16_t command_type) {
  return command_type == TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE ||
         command_type == TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME ||
         command_type == TURBO_FLOW_TFMP_COMMAND_FLOW_DRAIN;
}

static int flow_tfmp_management_is_resource_command(uint16_t command_type) {
  return command_type == TURBO_FLOW_TFMP_COMMAND_RESOURCE_QUIESCE ||
         command_type == TURBO_FLOW_TFMP_COMMAND_RESOURCE_RESUME ||
         command_type == TURBO_FLOW_TFMP_COMMAND_ENDPOINT_REPLACE ||
         command_type == TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE;
}

static int flow_tfmp_management_copy_field(const turbo_flow_tfmp_field_t *field, char *out,
                                           size_t capacity) {
  if (!field || !out || capacity == 0u || field->value_size >= capacity) return TURBO_EINVAL;
  if (field->value_size > 0u) memcpy(out, field->value, field->value_size);
  out[field->value_size] = '\0';
  return TURBO_OK;
}

static int flow_tfmp_management_pool_kind(uint16_t wire_kind, turbo_flow_pool_kind_t *kind) {
  if (!kind) return TURBO_EINVAL;
  switch (wire_kind) {
  case TURBO_FLOW_TFMP_POOL_KIND_THREAD:
    *kind = TURBO_FLOW_POOL_THREAD;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_POOL_KIND_CORO:
    *kind = TURBO_FLOW_POOL_CORO;
    return TURBO_OK;
  case TURBO_FLOW_TFMP_POOL_KIND_DISRUPTOR:
    *kind = TURBO_FLOW_POOL_DISRUPTOR;
    return TURBO_OK;
  default:
    return TURBO_EINVAL;
  }
}

static uint64_t flow_tfmp_management_deadline(uint64_t timeout_ms) {
  uint64_t now_ns = turbo_hrtime();
  if (timeout_ms == UINT64_MAX || timeout_ms > UINT64_MAX / UINT64_C(1000000)) return UINT64_MAX;
  if (timeout_ms * UINT64_C(1000000) > UINT64_MAX - now_ns) return UINT64_MAX;
  return now_ns + timeout_ms * UINT64_C(1000000);
}

static turbo_flow_tfmp_status_t flow_tfmp_management_command_status(int error);

static int flow_tfmp_management_prepare_resource_command(
    turbo_flow_tfmp_management_service_t *service, const flow_tfmp_command_request_t *request,
    const char *stable_internal_key, turbo_flow_resource_command_t *command) {
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  turbo_uuid_t internal_id;
  char internal_key[TURBO_UUID_STRING_SIZE];
  int rc;
  *command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
  if (!request->has_expected_generation || request->expected_generation == 0u) return TURBO_EINVAL;
  rc = flow_tfmp_management_copy_field(&request->target_uid, command->target_uid,
                                       sizeof(command->target_uid));
  if (rc != TURBO_OK) return rc;
  if (stable_internal_key) {
    if (strlen(stable_internal_key) > TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX) return TURBO_EINVAL;
    memcpy(command->idempotency_key, stable_internal_key, strlen(stable_internal_key) + 1u);
  } else {
    rc = turbo_uuid_v7_generate(&internal_id);
    if (rc == TURBO_OK) rc = turbo_uuid_format(&internal_id, internal_key, sizeof(internal_key));
    if (rc != TURBO_OK) return rc;
    memcpy(command->idempotency_key, internal_key, sizeof(internal_key));
  }
  command->expected_generation = request->expected_generation;
  command->deadline_ns = flow_tfmp_management_deadline(request->operation_timeout_ms);
  command->drain_timeout_ms = request->operation_timeout_ms;

  switch (request->command_type) {
  case TURBO_FLOW_TFMP_COMMAND_RESOURCE_QUIESCE:
    command->kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    return request->has_payload ? TURBO_EPROTO : TURBO_OK;
  case TURBO_FLOW_TFMP_COMMAND_RESOURCE_RESUME:
    command->kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
    return request->has_payload ? TURBO_EPROTO : TURBO_OK;
  case TURBO_FLOW_TFMP_COMMAND_ENDPOINT_REPLACE: {
    uint16_t port = 0u;
    command->kind = TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT;
    if (!request->has_payload) return TURBO_EPROTO;
    rc = flow_tfmp_management_find_body_field(request->payload.value, request->payload.value_size,
                                              1u, &field);
    if (rc == TURBO_OK) {
      rc = flow_tfmp_management_copy_field(&field, command->endpoint_host,
                                           sizeof(command->endpoint_host));
    } else if (rc == TURBO_ENOENT) {
      rc = TURBO_OK;
    }
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    if (rc == TURBO_OK) {
      rc = flow_tfmp_management_find_body_field(request->payload.value, request->payload.value_size,
                                                2u, &field);
      if (rc == TURBO_OK) {
        rc = flow_tfmp_management_copy_field(&field, command->endpoint_path,
                                             sizeof(command->endpoint_path));
      } else if (rc == TURBO_ENOENT) {
        rc = TURBO_OK;
      }
    }
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    if (rc == TURBO_OK) {
      rc = flow_tfmp_management_find_body_field(request->payload.value, request->payload.value_size,
                                                3u, &field);
      if (rc == TURBO_OK) {
        rc = turbo_flow_tfmp_field_read_u16(&field, &port);
      } else if (rc == TURBO_ENOENT) {
        rc = TURBO_OK;
      }
    }
    command->endpoint_port = (int)port;
    if (rc == TURBO_OK && command->endpoint_host[0] == '\0' && command->endpoint_path[0] == '\0')
      rc = TURBO_EINVAL;
    return rc;
  }
  case TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE: {
    turbo_flow_pool_kind_t expected_kind = TURBO_FLOW_POOL_THREAD;
    uint16_t wire_kind = 0u;
    command->kind = TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL;
    if (!request->has_payload) return TURBO_EPROTO;
    rc = flow_tfmp_management_find_body_field(request->payload.value, request->payload.value_size,
                                              1u, &field);
    if (rc == TURBO_OK) rc = turbo_flow_tfmp_field_read_u16(&field, &wire_kind);
    if (rc == TURBO_OK) rc = flow_tfmp_management_pool_kind(wire_kind, &expected_kind);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    if (rc == TURBO_OK)
      rc = flow_tfmp_management_find_body_field(request->payload.value, request->payload.value_size,
                                                2u, &field);
    if (rc == TURBO_OK) rc = turbo_flow_tfmp_field_read_u32(&field, &command->parallelism);
    if (rc != TURBO_OK || command->parallelism == 0u) return rc == TURBO_OK ? TURBO_EINVAL : rc;
    for (size_t i = 0u; i < turbo_flow_pool_count(service->target); ++i) {
      turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
      rc = turbo_flow_pool_resource_status_at(service->target, i, &status);
      if (rc != TURBO_OK) return rc;
      if (strcmp(status.uid, command->target_uid) == 0)
        return status.snapshot.kind == expected_kind ? TURBO_OK : TURBO_EINVAL;
    }
    return TURBO_OK;
  }
  default:
    return TURBO_ENOTSUP;
  }
}

static int flow_tfmp_management_execute_flow_command(turbo_flow_tfmp_management_service_t *service,
                                                     const flow_tfmp_command_request_t *command,
                                                     flow_tfmp_management_result_t *result,
                                                     uint64_t *generation_before,
                                                     uint64_t *generation_after,
                                                     uint64_t *observed_generation) {
  turbo_flow_runtime_snapshot_t snapshot;
  int snapshot_rc = flow_tfmp_management_target_snapshot(service, &snapshot);
  int rc;
  if (snapshot_rc != TURBO_OK) {
    flow_tfmp_management_result_fail(result, flow_tfmp_management_query_status(snapshot_rc),
                                     "failed to read target before command", snapshot_rc);
    return TURBO_OK;
  }
  *generation_before = service->target_generation;
  *generation_after = *generation_before;
  *observed_generation = *generation_before;
  if (command->has_expected_generation && command->expected_generation != *generation_before) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_CONFLICT,
                                     "expected target generation does not match", TURBO_EBUSY);
    return TURBO_OK;
  }
  switch (command->command_type) {
  case TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE:
    rc = turbo_flow_pause(service->target);
    break;
  case TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME:
    rc = turbo_flow_resume(service->target);
    break;
  case TURBO_FLOW_TFMP_COMMAND_FLOW_DRAIN:
    rc = turbo_flow_drain(service->target, command->operation_timeout_ms);
    break;
  default:
    rc = TURBO_ENOTSUP;
    break;
  }
  snapshot_rc = flow_tfmp_management_target_snapshot(service, &snapshot);
  if (snapshot_rc == TURBO_OK) {
    *generation_after = service->target_generation;
    *observed_generation = *generation_after;
  } else if (rc == TURBO_OK) {
    flow_tfmp_management_result_fail(result, flow_tfmp_management_query_status(snapshot_rc),
                                     "failed to read target after command", snapshot_rc);
  }
  if (rc != TURBO_OK)
    flow_tfmp_management_result_fail(result, flow_tfmp_management_command_status(rc),
                                     "target rejected management command", rc);
  return TURBO_OK;
}

static int flow_tfmp_management_execute_resource_command(
    turbo_flow_tfmp_management_service_t *service, const flow_tfmp_command_request_t *request,
    const char *stable_internal_key, flow_tfmp_management_result_t *result,
    uint64_t *generation_before, uint64_t *generation_after, uint64_t *observed_generation) {
  turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
  turbo_flow_resource_command_result_t command_result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
  int rc = flow_tfmp_management_prepare_resource_command(service, request, stable_internal_key,
                                                         &command);
  if (rc != TURBO_OK) {
    flow_tfmp_management_result_fail(result,
                                     rc == TURBO_ENOTSUP
                                         ? TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY
                                         : TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT,
                                     "resource command payload is invalid", rc);
    return TURBO_OK;
  }
  rc = turbo_flow_resource_command(service->target, &command, &command_result);
  *generation_before = command_result.generation_before;
  *generation_after = command_result.generation_after;
  *observed_generation = command_result.observed_generation;
  if (rc == TURBO_EBUSY && command_result.generation_before != request->expected_generation) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_CONFLICT,
                                     "expected resource generation does not match", rc);
  } else if (rc != TURBO_OK) {
    flow_tfmp_management_result_fail(result, flow_tfmp_management_command_status(rc),
                                     "resource owner rejected management command", rc);
  }
  return TURBO_OK;
}

static turbo_flow_tfmp_status_t flow_tfmp_management_command_status(int error) {
  if (error == TURBO_EINVAL || error == TURBO_ESHUTDOWN)
    return TURBO_FLOW_TFMP_STATUS_FAILED_PRECONDITION;
  return turbo_flow_tfmp_status_from_error(error);
}

static int flow_tfmp_management_build_command_submit(turbo_flow_tfmp_management_service_t *service,
                                                      const turbo_flow_tfmp_envelope_t *request,
                                                      flow_tfmp_management_result_t *result,
                                                      size_t *body_size,
                                                      int allow_wait_terminal) {
  flow_tfmp_command_request_t command = {0};
  flow_tfmp_command_record_t *record = NULL;
  uint64_t generation_before = 0u;
  uint64_t generation_after = 0u;
  uint64_t observed_generation = 0u;
  uint64_t now_ns = turbo_hrtime();
  int rc = flow_tfmp_management_parse_command(request, &command, result);
  if (rc != TURBO_OK || result->status != TURBO_FLOW_TFMP_STATUS_OK) return rc;
  if (!allow_wait_terminal &&
      command.reply_mode != TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY,
                                     "network management mutations require ACCEPT_OPERATION",
                                     TURBO_ENOTSUP);
    return TURBO_OK;
  }
  if (!flow_tfmp_management_is_flow_command(command.command_type) &&
      !flow_tfmp_management_is_resource_command(command.command_type)) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY,
                                     "command type is not enabled", TURBO_ENOTSUP);
    return TURBO_OK;
  }
  if (flow_tfmp_management_is_flow_command(command.command_type) &&
      !flow_tfmp_management_field_equals(&command.target_uid, service->target_uid)) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_NOT_FOUND,
                                     "target UID was not found", TURBO_ENOENT);
    return TURBO_OK;
  }
  if ((command.reply_mode != TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL &&
       command.reply_mode != TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION) ||
      (command.required_durability != TURBO_FLOW_TFMP_DURABILITY_VOLATILE &&
       command.required_durability != TURBO_FLOW_TFMP_DURABILITY_DURABLE) ||
      (command.required_durability == TURBO_FLOW_TFMP_DURABILITY_DURABLE &&
       (!service->operation_store ||
        command.reply_mode != TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION ||
        !flow_tfmp_management_reconcile_supported(service, command.command_type)))) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY,
                                     "command mode or durability is not enabled", TURBO_ENOTSUP);
    return TURBO_OK;
  }
  if (service->state != TURBO_FLOW_TFMP_OWNER_READY) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_FAILED_PRECONDITION,
                                     "management owner is not ready for mutation", TURBO_EBUSY);
    return TURBO_OK;
  }
  if (command.reply_mode == TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION &&
      flow_tfmp_management_is_resource_command(command.command_type)) {
    turbo_flow_resource_command_t validation_command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    rc = flow_tfmp_management_prepare_resource_command(service, &command, "tfmp-validate",
                                                       &validation_command);
    if (rc != TURBO_OK) {
      flow_tfmp_management_result_fail(result,
                                       rc == TURBO_ENOTSUP
                                           ? TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY
                                           : TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT,
                                       "resource operation payload is invalid", rc);
      return TURBO_OK;
    }
  }
  rc = flow_tfmp_management_records_purge(service, now_ns);
  if (rc != TURBO_OK) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_UNAVAILABLE,
                                     "durable dedup expiration did not commit", rc);
    return TURBO_OK;
  }
  rc = flow_tfmp_management_record_lookup(service, &command, request, &record);
  if (rc == TURBO_OK) {
    result->status = record->status;
    result->native_status = record->native_status;
    result->diagnostic = record->diagnostic;
    result->disposition = record->has_operation ? record->acceptance_disposition
                                                : TURBO_FLOW_TFMP_DISPOSITION_COMPLETED;
    result->response_flags |= TURBO_FLOW_TFMP_FLAG_REPLAYED;
    return result->status == TURBO_FLOW_TFMP_STATUS_OK
               ? flow_tfmp_management_build_command_response(service, record, body_size)
               : TURBO_OK;
  }
  if (rc == TURBO_EPROTO) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_CONFLICT,
                                     "idempotency key was reused for different bytes", rc);
    return TURBO_OK;
  }
  if (command.reply_mode == TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION &&
      flow_tfmp_management_active_operation_count(service) >= service->config.operation_capacity) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_RESOURCE_EXHAUSTED,
                                     "operation mailbox capacity is exhausted", TURBO_ENOSPC);
    return TURBO_OK;
  }
  rc = flow_tfmp_management_record_reserve(service, &command, request, &record);
  if (rc != TURBO_OK) {
    flow_tfmp_management_result_fail(result, turbo_flow_tfmp_status_from_error(rc),
                                     "command dedup capacity is exhausted", rc);
    return TURBO_OK;
  }
  if (command.reply_mode == TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION) {
    uint64_t now_unix_ms;
    rc = turbo_uuid_v7_generate(&record->operation_id);
    if (rc != TURBO_OK) {
      flow_tfmp_management_record_clear(record);
      --service->command_record_count;
      flow_tfmp_management_result_fail(result, turbo_flow_tfmp_status_from_error(rc),
                                       "failed to allocate operation identity", rc);
      return TURBO_OK;
    }
    now_unix_ms = flow_tfmp_management_unix_ms();
    record->has_operation = 1;
    record->operation_state = TURBO_FLOW_TFMP_OPERATION_ACCEPTED;
    record->operation_revision = 1u;
    record->submitted_unix_ms = now_unix_ms;
    record->updated_unix_ms = now_unix_ms;
    record->accepted_ns = now_ns;
    record->queue_deadline_ns = flow_tfmp_management_deadline(command.queue_timeout_ms);
    record->queue_deadline_unix_ms = command.queue_timeout_ms == UINT64_MAX
                                         ? UINT64_MAX
                                         : (command.queue_timeout_ms > UINT64_MAX - now_unix_ms
                                                ? UINT64_MAX
                                                : now_unix_ms + command.queue_timeout_ms);
    record->status = TURBO_FLOW_TFMP_STATUS_OK;
    record->native_status = TURBO_OK;
    record->durable = command.required_durability == TURBO_FLOW_TFMP_DURABILITY_DURABLE;
    record->acceptance_disposition = record->durable
                                         ? TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_DURABLE
                                         : TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_VOLATILE;
    if (record->durable && !service->durable_event_replay) {
      rc = flow_tfmp_management_store_commit(service);
      if (rc != TURBO_OK) {
        flow_tfmp_management_record_clear(record);
        --service->command_record_count;
        flow_tfmp_management_result_fail(result,
                                         rc == TURBO_ENOSPC || rc == TURBO_EMSGSIZE
                                             ? TURBO_FLOW_TFMP_STATUS_RESOURCE_EXHAUSTED
                                             : TURBO_FLOW_TFMP_STATUS_UNAVAILABLE,
                                         "durable operation acceptance did not commit", rc);
        return TURBO_OK;
      }
    }
    result->disposition = record->acceptance_disposition;
    rc = flow_tfmp_management_event_emit(
        service, TURBO_FLOW_TFMP_EVENT_OPERATION, TURBO_FLOW_TFMP_EVENT_OPERATION_ACCEPTED,
        &command.target_uid, 0, 0u, &record->operation_id, record->operation_revision);
    if (rc != TURBO_OK && service->durable_event_replay) {
      flow_tfmp_management_record_clear(record);
      --service->command_record_count;
      flow_tfmp_management_result_fail(result,
                                       rc == TURBO_ENOSPC || rc == TURBO_EMSGSIZE
                                           ? TURBO_FLOW_TFMP_STATUS_RESOURCE_EXHAUSTED
                                           : TURBO_FLOW_TFMP_STATUS_UNAVAILABLE,
                                       "operation acceptance and event outbox did not commit", rc);
      return TURBO_OK;
    }
    return flow_tfmp_management_build_command_response(service, record, body_size);
  }
  (void)command.queue_timeout_ms;
  if (flow_tfmp_management_is_flow_command(command.command_type)) {
    rc = flow_tfmp_management_execute_flow_command(service, &command, result, &generation_before,
                                                   &generation_after, &observed_generation);
  } else {
    rc = flow_tfmp_management_execute_resource_command(service, &command, NULL, result,
                                                       &generation_before, &generation_after,
                                                       &observed_generation);
  }
  if (rc != TURBO_OK) return rc;
  flow_tfmp_management_record_finish(record, result, generation_before, generation_after,
                                     observed_generation);
  rc = flow_tfmp_management_event_emit(
      service,
      result->status == TURBO_FLOW_TFMP_STATUS_OK
          ? (flow_tfmp_management_is_resource_command(command.command_type)
                 ? TURBO_FLOW_TFMP_EVENT_RESOURCE
                 : TURBO_FLOW_TFMP_EVENT_LIFECYCLE)
          : TURBO_FLOW_TFMP_EVENT_FAULT,
      result->status == TURBO_FLOW_TFMP_STATUS_OK ? TURBO_FLOW_TFMP_EVENT_COMMAND_COMPLETED
                                                  : TURBO_FLOW_TFMP_EVENT_COMMAND_FAILED,
      &command.target_uid, 1, generation_after, NULL, 0u);
  if (rc != TURBO_OK && service->durable_event_replay) {
    service->state = TURBO_FLOW_TFMP_OWNER_FAILED;
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_UNAVAILABLE,
                                     "command completed but durable event outbox did not commit",
                                     rc);
    flow_tfmp_management_record_finish(record, result, generation_before, generation_after,
                                       observed_generation);
    return TURBO_OK;
  }
  if (result->status != TURBO_FLOW_TFMP_STATUS_OK) return TURBO_OK;
  return flow_tfmp_management_build_command_response(service, record, body_size);
}

static int flow_tfmp_management_build_error(turbo_flow_tfmp_management_service_t *service,
                                            const char *diagnostic, int native_status,
                                            size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  if (diagnostic && strlen(diagnostic) > FLOW_TFMP_DIAGNOSTIC_MAX) return TURBO_EINVAL;
  int rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                             service->reply_body_capacity);
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK && diagnostic && diagnostic[0] != '\0')
    rc =
        turbo_flow_tfmp_body_builder_append_utf8(&builder, 122u, 0, diagnostic, strlen(diagnostic));
  if (rc == TURBO_OK && native_status != TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_i32(&builder, 123u, 0, native_status);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int flow_tfmp_management_build_operation_get(turbo_flow_tfmp_management_service_t *service,
                                                    const turbo_flow_tfmp_envelope_t *request,
                                                    flow_tfmp_management_result_t *result,
                                                    size_t *body_size) {
  turbo_flow_tfmp_field_t operation_id = TURBO_FLOW_TFMP_FIELD_INIT;
  flow_tfmp_command_record_t *record;
  int rc = flow_tfmp_management_find_field(request, 1u, &operation_id);
  if (rc != TURBO_OK) return rc;
  record = flow_tfmp_management_operation_find(service, operation_id.value);
  if (!record) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_NOT_FOUND,
                                     "operation ID was not found", TURBO_ENOENT);
    return TURBO_OK;
  }
  return flow_tfmp_management_build_operation_response(service, record, body_size);
}

static int flow_tfmp_management_build_operation_cancel(
    turbo_flow_tfmp_management_service_t *service, const turbo_flow_tfmp_envelope_t *request,
    flow_tfmp_management_result_t *result, size_t *body_size) {
  turbo_flow_tfmp_field_t client_id = TURBO_FLOW_TFMP_FIELD_INIT;
  turbo_flow_tfmp_field_t idempotency_key = TURBO_FLOW_TFMP_FIELD_INIT;
  turbo_flow_tfmp_field_t operation_id = TURBO_FLOW_TFMP_FIELD_INIT;
  turbo_flow_tfmp_field_t revision_field = TURBO_FLOW_TFMP_FIELD_INIT;
  flow_tfmp_command_record_t *record;
  uint64_t expected_revision = 0u;
  int has_expected_revision = 0;
  int rc = flow_tfmp_management_find_field(request, 1u, &client_id);
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 2u, &idempotency_key);
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 3u, &operation_id);
  if (rc == TURBO_OK) {
    rc = flow_tfmp_management_find_field(request, 4u, &revision_field);
    if (rc == TURBO_OK) {
      has_expected_revision = 1;
      rc = turbo_flow_tfmp_field_read_u64(&revision_field, &expected_revision);
    } else if (rc == TURBO_ENOENT) {
      rc = TURBO_OK;
    }
  }
  if (rc != TURBO_OK) return rc;
  if (client_id.value_size == 0u ||
      client_id.value_size > TURBO_FLOW_TFMP_MANAGEMENT_CLIENT_ID_MAX ||
      idempotency_key.value_size == 0u ||
      idempotency_key.value_size > TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT,
                                     "cancel identity is invalid", TURBO_EINVAL);
    return TURBO_OK;
  }
  record = flow_tfmp_management_operation_find(service, operation_id.value);
  if (!record) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_NOT_FOUND,
                                     "operation ID was not found", TURBO_ENOENT);
    return TURBO_OK;
  }
  if (record->cancel_idempotency_key[0] != '\0' &&
      flow_tfmp_management_record_text_equals(record->cancel_client_id, &client_id) &&
      flow_tfmp_management_record_text_equals(record->cancel_idempotency_key, &idempotency_key)) {
    if (record->cancel_request_body_size != request->body_size ||
        memcmp(record->cancel_request_body, request->body, request->body_size) != 0) {
      flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_CONFLICT,
                                       "cancel idempotency key was reused for different bytes",
                                       TURBO_EPROTO);
      return TURBO_OK;
    }
    result->response_flags |= TURBO_FLOW_TFMP_FLAG_REPLAYED;
    return flow_tfmp_management_build_operation_response(service, record, body_size);
  }
  if (has_expected_revision && expected_revision != record->operation_revision) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_CONFLICT,
                                     "expected operation revision does not match", TURBO_EBUSY);
    return TURBO_OK;
  }
  if (record->operation_state == TURBO_FLOW_TFMP_OPERATION_RUNNING ||
      record->operation_state == TURBO_FLOW_TFMP_OPERATION_CANCEL_REQUESTED) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_FAILED_PRECONDITION,
                                     "running operation has no cancellation contract", TURBO_EBUSY);
    return TURBO_OK;
  }
  if (record->operation_state == TURBO_FLOW_TFMP_OPERATION_ACCEPTED) {
    uint8_t *body_copy = (uint8_t *)malloc(request->body_size);
    uint64_t previous_updated_unix_ms = record->updated_unix_ms;
    if (!body_copy) return TURBO_ENOMEM;
    memcpy(body_copy, request->body, request->body_size);
    memcpy(record->cancel_client_id, client_id.value, client_id.value_size);
    record->cancel_client_id[client_id.value_size] = '\0';
    memcpy(record->cancel_idempotency_key, idempotency_key.value, idempotency_key.value_size);
    record->cancel_idempotency_key[idempotency_key.value_size] = '\0';
    record->cancel_request_body = body_copy;
    record->cancel_request_body_size = request->body_size;
    record->operation_state = TURBO_FLOW_TFMP_OPERATION_CANCELED;
    ++record->operation_revision;
    record->updated_unix_ms = flow_tfmp_management_unix_ms();
    record->terminal_status = TURBO_FLOW_TFMP_STATUS_CANCELED;
    record->terminal_ns = turbo_hrtime();
    record->terminal_unix_ms = record->updated_unix_ms;
    if (record->durable && !service->durable_event_replay) {
      rc = flow_tfmp_management_store_commit(service);
      if (rc != TURBO_OK) {
        free(record->cancel_request_body);
        record->cancel_request_body = NULL;
        record->cancel_request_body_size = 0u;
        record->cancel_client_id[0] = '\0';
        record->cancel_idempotency_key[0] = '\0';
        record->operation_state = TURBO_FLOW_TFMP_OPERATION_ACCEPTED;
        --record->operation_revision;
        record->updated_unix_ms = previous_updated_unix_ms;
        record->terminal_status = TURBO_FLOW_TFMP_STATUS_OK;
        record->terminal_ns = 0u;
        record->terminal_unix_ms = 0u;
        flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_UNAVAILABLE,
                                         "durable cancellation did not commit", rc);
        return TURBO_OK;
      }
    }
    rc = flow_tfmp_management_event_emit(service, TURBO_FLOW_TFMP_EVENT_OPERATION,
                                         TURBO_FLOW_TFMP_EVENT_OPERATION_CANCELED, NULL, 0, 0u,
                                         &record->operation_id, record->operation_revision);
    if (rc != TURBO_OK && service->durable_event_replay) {
      free(record->cancel_request_body);
      record->cancel_request_body = NULL;
      record->cancel_request_body_size = 0u;
      record->cancel_client_id[0] = '\0';
      record->cancel_idempotency_key[0] = '\0';
      record->operation_state = TURBO_FLOW_TFMP_OPERATION_ACCEPTED;
      --record->operation_revision;
      record->updated_unix_ms = previous_updated_unix_ms;
      record->terminal_status = TURBO_FLOW_TFMP_STATUS_OK;
      record->terminal_ns = 0u;
      record->terminal_unix_ms = 0u;
      flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_UNAVAILABLE,
                                       "cancellation and event outbox did not commit", rc);
      return TURBO_OK;
    }
  }
  return flow_tfmp_management_build_operation_response(service, record, body_size);
}

static int flow_tfmp_management_build_events_get(turbo_flow_tfmp_management_service_t *service,
                                                 const turbo_flow_tfmp_envelope_t *request,
                                                 flow_tfmp_management_result_t *result,
                                                 size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  turbo_flow_tfmp_field_t incarnation = TURBO_FLOW_TFMP_FIELD_INIT;
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  uint64_t after_sequence = 0u;
  uint64_t last_sequence = 0u;
  uint64_t oldest_sequence = service->event_count > 0u
                                 ? service->events[service->event_head].sequence
                                 : service->event_sequence + 1u;
  uint32_t page_limit = 0u;
  size_t emitted = 0u;
  int rc;
  if (service->config.event_capacity == 0u) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY,
                                     "event replay is not enabled", TURBO_ENOTSUP);
    return TURBO_OK;
  }
  rc = flow_tfmp_management_find_field(request, 1u, &incarnation);
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 2u, &field);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_field_read_u64(&field, &after_sequence);
  field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  if (rc == TURBO_OK) rc = flow_tfmp_management_find_field(request, 3u, &field);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_field_read_u32(&field, &page_limit);
  if (rc != TURBO_OK) return rc;
  if (incarnation.value_size != sizeof(service->incarnation_id.bytes) ||
      memcmp(incarnation.value, service->incarnation_id.bytes,
             sizeof(service->incarnation_id.bytes)) != 0 ||
      after_sequence > service->event_sequence ||
      (service->event_count > 0u && after_sequence < oldest_sequence - 1u)) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_STALE_CURSOR,
                                     "event cursor is outside this incarnation journal",
                                     TURBO_ERANGE);
    return TURBO_OK;
  }
  if (page_limit == 0u || page_limit > service->config.max_page_items) {
    flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT,
                                     "event page limit is outside the configured bound",
                                     TURBO_ERANGE);
    return TURBO_OK;
  }
  last_sequence = after_sequence;
  rc = turbo_flow_tfmp_body_builder_init(&builder, service->reply_body,
                                         service->reply_body_capacity);
  for (size_t i = 0u; rc == TURBO_OK && i < service->event_count && emitted < page_limit; ++i) {
    const flow_tfmp_event_record_t *record =
        &service->events[(service->event_head + i) % service->config.event_capacity];
    if (record->sequence <= after_sequence) continue;
    rc = turbo_flow_tfmp_body_builder_append(&builder, 1u, 0, record->body, record->body_size);
    if (rc == TURBO_OK) {
      last_sequence = record->sequence;
      ++emitted;
    }
  }
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 2u, 1, last_sequence);
  if (rc == TURBO_OK) rc = flow_tfmp_management_append_identity(service, &builder);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int flow_tfmp_management_build_success(turbo_flow_tfmp_management_service_t *service,
                                               const turbo_flow_tfmp_envelope_t *request,
                                               flow_tfmp_management_result_t *result,
                                               size_t *body_size,
                                               int allow_wait_terminal) {
  switch (request->kind) {
  case TURBO_FLOW_TFMP_CAPABILITIES_GET:
    return flow_tfmp_management_build_capabilities(service, body_size);
  case TURBO_FLOW_TFMP_HEALTH_GET:
    return flow_tfmp_management_build_health(service, body_size);
  case TURBO_FLOW_TFMP_TARGET_LIST:
  case TURBO_FLOW_TFMP_TARGET_GET:
  case TURBO_FLOW_TFMP_RESOURCE_LIST:
  case TURBO_FLOW_TFMP_RESOURCE_GET:
  case TURBO_FLOW_TFMP_RESOURCE_DOCUMENT_GET:
  case TURBO_FLOW_TFMP_COMMAND_SUBMIT:
  case TURBO_FLOW_TFMP_OPERATION_GET:
  case TURBO_FLOW_TFMP_OPERATION_CANCEL:
  case TURBO_FLOW_TFMP_EVENTS_GET:
    if (!service->target) {
      flow_tfmp_management_result_fail(result, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY,
                                       "no management target is bound", TURBO_ENOTSUP);
      return TURBO_OK;
    }
    break;
  default:
    return TURBO_ENOTSUP;
  }
  switch (request->kind) {
  case TURBO_FLOW_TFMP_TARGET_LIST:
    return flow_tfmp_management_build_target_list(service, request, result, body_size);
  case TURBO_FLOW_TFMP_TARGET_GET:
    return flow_tfmp_management_build_target_get(service, request, result, body_size);
  case TURBO_FLOW_TFMP_RESOURCE_LIST:
    return flow_tfmp_management_build_resource_list(service, request, result, body_size);
  case TURBO_FLOW_TFMP_RESOURCE_GET:
    return flow_tfmp_management_build_resource_get(service, request, result, body_size);
  case TURBO_FLOW_TFMP_RESOURCE_DOCUMENT_GET:
    return flow_tfmp_management_build_resource_document(service, request, result, body_size);
  case TURBO_FLOW_TFMP_COMMAND_SUBMIT:
    return flow_tfmp_management_build_command_submit(service, request, result, body_size,
                                                     allow_wait_terminal);
  case TURBO_FLOW_TFMP_OPERATION_GET:
    return flow_tfmp_management_build_operation_get(service, request, result, body_size);
  case TURBO_FLOW_TFMP_OPERATION_CANCEL:
    return flow_tfmp_management_build_operation_cancel(service, request, result, body_size);
  case TURBO_FLOW_TFMP_EVENTS_GET:
    return flow_tfmp_management_build_events_get(service, request, result, body_size);
  default:
    return TURBO_ENOTSUP;
  }
}

static int flow_tfmp_management_encode_response(turbo_flow_tfmp_management_service_t *service,
                                                 const turbo_flow_tfmp_envelope_t *request,
                                                 uint16_t kind, uint64_t correlation_id,
                                                 flow_tfmp_management_result_t *result, uint8_t *out,
                                                 size_t capacity, size_t *out_len,
                                                 int allow_wait_terminal) {
  turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  size_t body_size = 0u;
  int rc;

  if (result->status == TURBO_FLOW_TFMP_STATUS_OK) {
    rc = flow_tfmp_management_build_success(service, request, result, &body_size,
                                            allow_wait_terminal);
    if (rc != TURBO_OK) {
      flow_tfmp_management_result_fail(result, flow_tfmp_management_query_status(rc),
                                       "management query failed", rc);
    }
  }
  if (result->status != TURBO_FLOW_TFMP_STATUS_OK) {
    rc = flow_tfmp_management_build_error(service, result->diagnostic, result->native_status,
                                          &body_size);
    if (rc != TURBO_OK) return rc;
  }

  response.minor = TURBO_FLOW_TFMP_PROTOCOL_MINOR;
  response.kind = kind;
  response.flags = TURBO_FLOW_TFMP_FLAG_RESPONSE | result->response_flags;
  response.correlation_id = correlation_id;
  response.status = (uint16_t)result->status;
  response.disposition =
      result->disposition != TURBO_FLOW_TFMP_DISPOSITION_NONE ? result->disposition
      : result->status == TURBO_FLOW_TFMP_STATUS_OK ? TURBO_FLOW_TFMP_DISPOSITION_COMPLETED
                                                    : TURBO_FLOW_TFMP_DISPOSITION_FAILED;
  response.body = service->reply_body;
  response.body_size = body_size;
  rc = turbo_flow_tfmp_envelope_validate_schema(&response);
  if (rc != TURBO_OK) return rc;
  return turbo_flow_tfmp_envelope_encode(&response, out, capacity, out_len);
}

static int flow_tfmp_management_config_error(turbo_flow_config_error_t *error, int status,
                                             const char *channel_name, const char *field,
                                             const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field)
      (void)snprintf(error->path, sizeof(error->path), "$.channels.%s.config.%s", channel_name,
                     field);
    else
      (void)snprintf(error->path, sizeof(error->path), "$.channels.%s",
                     channel_name ? channel_name : "?");
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_tfmp_management_config_u64(const json_value_t *fields, const char *field,
                                           uint64_t minimum, uint64_t maximum, uint64_t *out) {
  json_value_t *value = turbo_json_object_get(fields, field);
  double number;
  uint64_t converted;
  if (!value || turbo_json_type(value) != TURBO_JSON_NUMBER) return TURBO_EINVAL;
  number = turbo_json_number(value);
  if (!isfinite(number) || number < (double)minimum || number > (double)maximum ||
      number > 9007199254740991.0)
    return TURBO_ERANGE;
  converted = (uint64_t)number;
  if ((double)converted != number) return TURBO_EINVAL;
  *out = converted;
  return TURBO_OK;
}

static int flow_tfmp_management_config_copy(const json_value_t *fields, const char *field,
                                            int required, char *out, size_t capacity) {
  json_value_t *value = turbo_json_object_get(fields, field);
  const char *text;
  size_t length;
  if (!value) return required ? TURBO_EINVAL : TURBO_ENOENT;
  if (turbo_json_type(value) != TURBO_JSON_STRING || !(text = turbo_json_string(value)) ||
      text[0] == '\0')
    return TURBO_EINVAL;
  length = strlen(text);
  if (length >= capacity) return TURBO_ENAMETOOLONG;
  memcpy(out, text, length + 1u);
  return TURBO_OK;
}

static int flow_tfmp_management_config_adapter(const json_value_t *document,
                                               const char *channel_name, const char *field,
                                               const char *adapter_name,
                                               const char *required_pattern,
                                               const char *required_topic_policy, int reject_udp,
                                               turbo_flow_config_error_t *error) {
  json_value_t *adapters = turbo_json_object_get(document, "adapters");
  json_value_t *adapter = adapters ? turbo_json_object_get(adapters, adapter_name) : NULL;
  json_value_t *kind = adapter ? turbo_json_object_get(adapter, "kind") : NULL;
  json_value_t *config = adapter ? turbo_json_object_get(adapter, "config") : NULL;
  json_value_t *pattern = config ? turbo_json_object_get(config, "pattern") : NULL;
  json_value_t *topic_policy = config ? turbo_json_object_get(config, "topic_policy") : NULL;
  json_value_t *transport = config ? turbo_json_object_get(config, "transport") : NULL;
  if (!adapter || turbo_json_type(adapter) != TURBO_JSON_OBJECT)
    return flow_tfmp_management_config_error(error, TURBO_ENOENT, channel_name, field,
                                             "adapter reference is not resolved");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "fmq") != 0)
    return flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, field,
                                             "adapter reference must have kind fmq");
  if (!config || turbo_json_type(config) != TURBO_JSON_OBJECT || !pattern ||
      turbo_json_type(pattern) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(pattern), required_pattern) != 0)
    return flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, field,
                                             "adapter has the wrong FMQ pattern");
  if (reject_udp && transport && turbo_json_type(transport) == TURBO_JSON_STRING &&
      strcmp(turbo_json_string(transport), "udp") == 0)
    return flow_tfmp_management_config_error(error, TURBO_ENOTSUP, channel_name, field,
                                             "management RPC does not support raw UDP");
  if (required_topic_policy &&
      (!topic_policy || turbo_json_type(topic_policy) != TURBO_JSON_STRING ||
       strcmp(turbo_json_string(topic_policy), required_topic_policy) != 0))
    return flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, field,
                                             "event adapter must use content-derived FMQ topics");
  return TURBO_OK;
}

static int flow_tfmp_management_config_channel(const json_value_t *document,
                                               const char *channel_name, const char *field,
                                               const char *reference, const char *required_kind,
                                               turbo_flow_config_error_t *error) {
  json_value_t *channels = turbo_json_object_get(document, "channels");
  json_value_t *channel = channels ? turbo_json_object_get(channels, reference) : NULL;
  json_value_t *kind = channel ? turbo_json_object_get(channel, "kind") : NULL;
  json_value_t *config = channel ? turbo_json_object_get(channel, "config") : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT)
    return flow_tfmp_management_config_error(error, TURBO_ENOENT, channel_name, field,
                                             "channel reference is not resolved");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), required_kind) != 0)
    return flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, field,
                                             "channel reference has the wrong kind");
  if (!config || turbo_json_type(config) != TURBO_JSON_OBJECT)
    return flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, field,
                                             "referenced channel config must be a mapping");
  return TURBO_OK;
}

int turbo_flow_tfmp_management_channel_config_resolve(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_tfmp_management_channel_config_t *out, turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {
      "protocol_major",          "protocol_minor",     "authority_id",       "rpc_adapter",
      "event_adapter",           "mailbox_capacity",   "max_request_bytes",  "max_reply_bytes",
      "max_inflight_per_target", "dedup_capacity",     "dedup_ttl_ms",       "operation_store",
      "event_capacity",          "event_replay_store", "shutdown_timeout_ms"};
  turbo_flow_tfmp_management_channel_config_t parsed =
      TURBO_FLOW_TFMP_MANAGEMENT_CHANNEL_CONFIG_INIT;
  turbo_json_doc_t *document = NULL;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  const char *json;
  size_t json_length = 0u;
  uint64_t number;
  int rc = TURBO_OK;
  if (!resolved || !channel_name || !channel_name[0] || !out || out->size < sizeof(*out) ||
      !error || error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_length);
  if (!json || turbo_parse_json((const uint8_t *)json, json_length, &document) != TURBO_OK ||
      !document)
    return flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                             "invalid resolved configuration snapshot");
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  kind = channel ? turbo_json_object_get(channel, "kind") : NULL;
  fields = channel ? turbo_json_object_get(channel, "config") : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_tfmp_management_config_error(error, TURBO_ENOENT, channel_name, NULL,
                                           "channel is not resolved");
    goto done;
  }
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "fmq_management") != 0) {
    rc = flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                           "channel kind must be fmq_management");
    goto done;
  }
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                           "config must be a mapping");
    goto done;
  }
  for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
    const char *candidate = turbo_json_object_key(fields, i);
    int known = 0;
    for (size_t j = 0u; j < sizeof(allowed) / sizeof(allowed[0]); ++j)
      if (candidate && strcmp(candidate, allowed[j]) == 0) known = 1;
    if (!known) {
      rc = flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, candidate,
                                             "unknown FMQ management field");
      goto done;
    }
  }
#define FLOW_TFMP_CONFIG_REQUIRED_U64(name, minimum, maximum, target, message)                     \
  do {                                                                                             \
    rc = flow_tfmp_management_config_u64(fields, name, minimum, maximum, &number);                 \
    if (rc != TURBO_OK) {                                                                          \
      rc = flow_tfmp_management_config_error(error, rc, channel_name, name, message);              \
      goto done;                                                                                   \
    }                                                                                              \
    (target) = number;                                                                             \
  } while (0)
  FLOW_TFMP_CONFIG_REQUIRED_U64("protocol_major", TURBO_FLOW_TFMP_PROTOCOL_MAJOR,
                                TURBO_FLOW_TFMP_PROTOCOL_MAJOR, parsed.protocol_major,
                                "protocol_major must be integer 1");
  FLOW_TFMP_CONFIG_REQUIRED_U64("protocol_minor", TURBO_FLOW_TFMP_PROTOCOL_MINOR,
                                TURBO_FLOW_TFMP_PROTOCOL_MINOR, parsed.protocol_minor,
                                "protocol_minor is not supported");
  FLOW_TFMP_CONFIG_REQUIRED_U64("mailbox_capacity", 1u, TURBO_FLOW_TFMP_MANAGEMENT_DEDUP_MAX,
                                parsed.service.operation_capacity,
                                "mailbox_capacity must be a bounded positive integer");
  FLOW_TFMP_CONFIG_REQUIRED_U64("max_request_bytes", TURBO_FLOW_TFMP_HEADER_SIZE,
                                TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE, parsed.service.max_request_bytes,
                                "max_request_bytes is outside the TFMP envelope limit");
  FLOW_TFMP_CONFIG_REQUIRED_U64("max_reply_bytes", TURBO_FLOW_TFMP_HEADER_SIZE,
                                TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE, parsed.service.max_reply_bytes,
                                "max_reply_bytes is outside the TFMP envelope limit");
  FLOW_TFMP_CONFIG_REQUIRED_U64("max_inflight_per_target", 1u,
                                TURBO_FLOW_TFMP_MANAGEMENT_DEDUP_MAX,
                                parsed.max_inflight_per_target,
                                "max_inflight_per_target must be a bounded positive integer");
  FLOW_TFMP_CONFIG_REQUIRED_U64("dedup_capacity", 1u, TURBO_FLOW_TFMP_MANAGEMENT_DEDUP_MAX,
                                parsed.service.dedup_capacity,
                                "dedup_capacity must be a bounded positive integer");
  FLOW_TFMP_CONFIG_REQUIRED_U64("dedup_ttl_ms", 1u, UINT64_C(9007199254740991),
                                parsed.service.dedup_ttl_ms,
                                "dedup_ttl_ms must be a positive integer");
  FLOW_TFMP_CONFIG_REQUIRED_U64("shutdown_timeout_ms", 1u, UINT64_C(9007199254740991),
                                parsed.shutdown_timeout_ms,
                                "shutdown_timeout_ms must be a positive integer");
#undef FLOW_TFMP_CONFIG_REQUIRED_U64
  if (turbo_json_object_get(fields, "event_capacity")) {
    rc = flow_tfmp_management_config_u64(fields, "event_capacity", 1u,
                                         TURBO_FLOW_TFMP_MANAGEMENT_EVENT_MAX, &number);
    if (rc != TURBO_OK) {
      rc = flow_tfmp_management_config_error(error, rc, channel_name, "event_capacity",
                                             "event_capacity must be a bounded positive integer");
      goto done;
    }
    parsed.service.event_capacity = (size_t)number;
  }
  if (parsed.service.operation_capacity > parsed.service.dedup_capacity) {
    rc = flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, "mailbox_capacity",
                                           "mailbox_capacity cannot exceed dedup_capacity");
    goto done;
  }
  if (parsed.max_inflight_per_target > parsed.service.operation_capacity) {
    rc = flow_tfmp_management_config_error(
        error, TURBO_EINVAL, channel_name, "max_inflight_per_target",
        "max_inflight_per_target cannot exceed mailbox_capacity");
    goto done;
  }
  rc = flow_tfmp_management_config_copy(fields, "authority_id", 1, parsed.service.authority_id,
                                        sizeof(parsed.service.authority_id));
  if (rc != TURBO_OK) {
    rc = flow_tfmp_management_config_error(error, rc, channel_name, "authority_id",
                                           "authority_id is invalid or too long");
    goto done;
  }
  rc = flow_tfmp_management_config_copy(fields, "rpc_adapter", 1, parsed.rpc_adapter,
                                        sizeof(parsed.rpc_adapter));
  if (rc != TURBO_OK) {
    rc = flow_tfmp_management_config_error(error, rc, channel_name, "rpc_adapter",
                                           "rpc_adapter is required and bounded");
    goto done;
  }
  rc = flow_tfmp_management_config_copy(fields, "event_adapter", 0, parsed.event_adapter,
                                        sizeof(parsed.event_adapter));
  if (rc != TURBO_OK && rc != TURBO_ENOENT) {
    rc = flow_tfmp_management_config_error(error, rc, channel_name, "event_adapter",
                                           "event_adapter must be a bounded reference");
    goto done;
  }
  rc = flow_tfmp_management_config_copy(fields, "operation_store", 0, parsed.operation_store,
                                        sizeof(parsed.operation_store));
  if (rc != TURBO_OK && rc != TURBO_ENOENT) {
    rc = flow_tfmp_management_config_error(error, rc, channel_name, "operation_store",
                                           "operation_store must be a bounded reference");
    goto done;
  }
  rc = flow_tfmp_management_config_copy(fields, "event_replay_store", 0, parsed.event_replay_store,
                                        sizeof(parsed.event_replay_store));
  if (rc != TURBO_OK && rc != TURBO_ENOENT) {
    rc = flow_tfmp_management_config_error(error, rc, channel_name, "event_replay_store",
                                           "event_replay_store must be a bounded reference");
    goto done;
  }
  if (strcmp(parsed.operation_store, "memory") != 0) {
    rc = flow_tfmp_management_config_channel(document, channel_name, "operation_store",
                                             parsed.operation_store, "blob_store", error);
    if (rc != TURBO_OK) goto done;
  }
  if ((parsed.event_adapter[0] != '\0' || parsed.event_replay_store[0] != '\0') &&
      parsed.service.event_capacity == 0u) {
    rc = flow_tfmp_management_config_error(error, TURBO_EINVAL, channel_name, "event_capacity",
                                           "event adapters require a positive event_capacity");
    goto done;
  }
  if (parsed.event_replay_store[0] != '\0' && strcmp(parsed.event_replay_store, "memory") != 0) {
    rc = flow_tfmp_management_config_channel(document, channel_name, "event_replay_store",
                                             parsed.event_replay_store, "blob_store", error);
    if (rc != TURBO_OK) goto done;
    if (strcmp(parsed.operation_store, "memory") == 0 ||
        strcmp(parsed.event_replay_store, parsed.operation_store) != 0) {
      rc = flow_tfmp_management_config_error(
          error, TURBO_EINVAL, channel_name, "event_replay_store",
          "durable event replay must share operation_store for atomic outbox commits");
      goto done;
    }
  }
  rc = flow_tfmp_management_config_adapter(document, channel_name, "rpc_adapter",
                                           parsed.rpc_adapter, "router", NULL, 1, error);
  if (rc == TURBO_OK && parsed.event_adapter[0] != '\0')
    rc = flow_tfmp_management_config_adapter(document, channel_name, "event_adapter",
                                             parsed.event_adapter, "pub", "content", 0, error);
  if (rc == TURBO_OK) *out = parsed;

done:
  turbo_free_json(&document);
  return rc;
}

static void flow_tfmp_store_write_u16(uint8_t *out, uint16_t value) {
  out[0] = (uint8_t)(value >> 8u);
  out[1] = (uint8_t)value;
}

static void flow_tfmp_store_write_u32(uint8_t *out, uint32_t value) {
  out[0] = (uint8_t)(value >> 24u);
  out[1] = (uint8_t)(value >> 16u);
  out[2] = (uint8_t)(value >> 8u);
  out[3] = (uint8_t)value;
}

static void flow_tfmp_store_write_u64(uint8_t *out, uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    out[i] = (uint8_t)(value >> (56u - 8u * i));
}

static uint16_t flow_tfmp_store_read_u16(const uint8_t *data) {
  return (uint16_t)(((uint16_t)data[0] << 8u) | data[1]);
}

static uint32_t flow_tfmp_store_read_u32(const uint8_t *data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) | ((uint32_t)data[2] << 8u) |
         data[3];
}

static uint64_t flow_tfmp_store_read_u64(const uint8_t *data) {
  uint64_t value = 0u;
  for (size_t i = 0u; i < 8u; ++i)
    value = (value << 8u) | data[i];
  return value;
}

static int flow_tfmp_store_append(uint8_t *out, size_t capacity, size_t *length, uint8_t type,
                                  const uint8_t *value, size_t value_size) {
  size_t wire_size;
  size_t written;
  if (!out || !length || type == 0u || (!value && value_size > 0u) || *length > capacity)
    return TURBO_EINVAL;
  wire_size = turbo_ltv_wire_size(value_size);
  if (wire_size == 0u || wire_size > capacity - *length) return TURBO_ENOSPC;
  written = turbo_ltv_build(type, value, value_size, out + *length, capacity - *length);
  if (written != wire_size) return TURBO_EPROTO;
  *length += written;
  return TURBO_OK;
}

static int flow_tfmp_store_record_encode(const flow_tfmp_command_record_t *record, uint8_t *out,
                                         size_t capacity, size_t *out_size) {
  uint8_t metadata[FLOW_TFMP_STORE_METADATA_SIZE];
  uint16_t flags =
      (uint16_t)((record->has_generation_result ? 1u : 0u) | (record->has_operation ? 2u : 0u));
  size_t length = 0u;
  int rc;
  if (!record || !record->occupied || !record->durable || !record->request_body || !out ||
      !out_size)
    return TURBO_EINVAL;
  memset(metadata, 0, sizeof(metadata));
  flow_tfmp_store_write_u16(metadata, flags);
  flow_tfmp_store_write_u16(metadata + 2u, (uint16_t)record->status);
  flow_tfmp_store_write_u32(metadata + 4u, (uint32_t)record->native_status);
  flow_tfmp_store_write_u64(metadata + 8u, record->generation_before);
  flow_tfmp_store_write_u64(metadata + 16u, record->generation_after);
  flow_tfmp_store_write_u64(metadata + 24u, record->observed_generation);
  memcpy(metadata + 32u, record->operation_id.bytes, sizeof(record->operation_id.bytes));
  flow_tfmp_store_write_u16(metadata + 48u, (uint16_t)record->operation_state);
  flow_tfmp_store_write_u64(metadata + 50u, record->operation_revision);
  flow_tfmp_store_write_u64(metadata + 58u, record->submitted_unix_ms);
  flow_tfmp_store_write_u64(metadata + 66u, record->updated_unix_ms);
  flow_tfmp_store_write_u64(metadata + 74u, record->queue_deadline_unix_ms);
  flow_tfmp_store_write_u16(metadata + 82u, (uint16_t)record->terminal_status);
  flow_tfmp_store_write_u16(metadata + 84u, (uint16_t)record->acceptance_disposition);
  flow_tfmp_store_write_u64(metadata + 86u, record->terminal_unix_ms);
  rc = flow_tfmp_store_append(out, capacity, &length, 1u, (const uint8_t *)record->client_id,
                              strlen(record->client_id));
  if (rc == TURBO_OK)
    rc =
        flow_tfmp_store_append(out, capacity, &length, 2u, (const uint8_t *)record->idempotency_key,
                               strlen(record->idempotency_key));
  if (rc == TURBO_OK)
    rc = flow_tfmp_store_append(out, capacity, &length, 3u, record->request_body,
                                record->request_body_size);
  if (rc == TURBO_OK)
    rc = flow_tfmp_store_append(out, capacity, &length, 4u, metadata, sizeof(metadata));
  if (rc == TURBO_OK && record->cancel_client_id[0] != '\0')
    rc = flow_tfmp_store_append(out, capacity, &length, 5u,
                                (const uint8_t *)record->cancel_client_id,
                                strlen(record->cancel_client_id));
  if (rc == TURBO_OK && record->cancel_client_id[0] != '\0')
    rc = flow_tfmp_store_append(out, capacity, &length, 6u,
                                (const uint8_t *)record->cancel_idempotency_key,
                                strlen(record->cancel_idempotency_key));
  if (rc == TURBO_OK && record->cancel_client_id[0] != '\0')
    rc = flow_tfmp_store_append(out, capacity, &length, 7u, record->cancel_request_body,
                                record->cancel_request_body_size);
  if (rc == TURBO_OK) *out_size = length;
  return rc;
}

static int flow_tfmp_management_store_commit(turbo_flow_tfmp_management_service_t *service) {
  uint8_t header[FLOW_TFMP_STORE_HEADER_SIZE] = {'T', 'F',
                                                 'M', 'S',
                                                 0u,  FLOW_TFMP_STORE_VERSION_MAJOR,
                                                 0u,  FLOW_TFMP_STORE_VERSION_MINOR_OPERATIONS};
  size_t length = 0u;
  int rc;
  if (!service || !service->operation_store) return TURBO_ENOTSUP;
  if (service->durable_event_replay) header[7] = FLOW_TFMP_STORE_VERSION_MINOR_EVENTS;
  rc = flow_tfmp_store_append(service->store_buffer, service->store_buffer_capacity, &length, 1u,
                              header, sizeof(header));
  if (rc == TURBO_OK)
    rc = flow_tfmp_store_append(service->store_buffer, service->store_buffer_capacity, &length, 2u,
                                (const uint8_t *)service->config.authority_id,
                                strlen(service->config.authority_id));
  for (size_t i = 0u; rc == TURBO_OK && i < service->config.dedup_capacity; ++i) {
    flow_tfmp_command_record_t *record = &service->command_records[i];
    size_t record_size = 0u;
    if (!record->occupied || !record->durable) continue;
    rc = flow_tfmp_store_record_encode(record, service->store_record_buffer,
                                       service->store_record_buffer_capacity, &record_size);
    if (rc == TURBO_OK)
      rc = flow_tfmp_store_append(service->store_buffer, service->store_buffer_capacity, &length,
                                  3u, service->store_record_buffer, record_size);
  }
  if (rc == TURBO_OK && service->durable_event_replay) {
    uint8_t metadata[FLOW_TFMP_STORE_EVENT_METADATA_SIZE];
    memcpy(metadata, service->incarnation_id.bytes, sizeof(service->incarnation_id.bytes));
    flow_tfmp_store_write_u64(metadata + sizeof(service->incarnation_id.bytes),
                              service->event_sequence);
    rc = flow_tfmp_store_append(service->store_buffer, service->store_buffer_capacity, &length, 4u,
                                metadata, sizeof(metadata));
  }
  for (size_t i = 0u; rc == TURBO_OK && service->durable_event_replay && i < service->event_count;
       ++i) {
    const flow_tfmp_event_record_t *event =
        &service->events[(service->event_head + i) % service->config.event_capacity];
    uint8_t encoded[FLOW_TFMP_STORE_EVENT_RECORD_OVERHEAD + FLOW_TFMP_EVENT_BODY_MAX];
    size_t encoded_size = FLOW_TFMP_STORE_EVENT_RECORD_OVERHEAD + event->body_size;
    if (event->body_size > FLOW_TFMP_EVENT_BODY_MAX) return TURBO_EPROTO;
    flow_tfmp_store_write_u64(encoded, event->sequence);
    flow_tfmp_store_write_u16(encoded + 8u, event->category);
    memcpy(encoded + FLOW_TFMP_STORE_EVENT_RECORD_OVERHEAD, event->body, event->body_size);
    rc = flow_tfmp_store_append(service->store_buffer, service->store_buffer_capacity, &length, 5u,
                                encoded, encoded_size);
  }
  if (rc != TURBO_OK) return rc;
  return service->operation_store->commit(
      service->operation_store->ctx, service->operation_store_key, service->store_buffer, length);
}

static int flow_tfmp_store_copy_text(char *out, size_t capacity, const uint8_t *value,
                                     size_t value_size) {
  if (!out || capacity == 0u || !value || value_size == 0u || value_size >= capacity ||
      memchr(value, '\0', value_size) ||
      !tstr_v_utf8_valid(tstr_v_from_buf((const char *)value, value_size)))
    return TURBO_EPROTO;
  memcpy(out, value, value_size);
  out[value_size] = '\0';
  return TURBO_OK;
}

static int flow_tfmp_management_store_decode_record(turbo_flow_tfmp_management_service_t *service,
                                                    const uint8_t *data, size_t data_size,
                                                    int *snapshot_dirty) {
  const uint8_t *values[8] = {0};
  size_t sizes[8] = {0};
  flow_tfmp_command_record_t *record = NULL;
  turbo_flow_tfmp_envelope_t envelope = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  flow_tfmp_command_request_t command = {0};
  flow_tfmp_management_result_t result = {TURBO_FLOW_TFMP_STATUS_OK, NULL, TURBO_OK, 0u,
                                          TURBO_FLOW_TFMP_DISPOSITION_NONE};
  size_t offset = 0u;
  uint8_t previous_type = 0u;
  uint16_t flags;
  uint64_t now_unix_ms = flow_tfmp_management_unix_ms();
  int rc = TURBO_OK;
  if (!service || !data || data_size == 0u || !snapshot_dirty) return TURBO_EINVAL;
  while (offset < data_size) {
    turbo_ltv_message_t *message = NULL;
    uint32_t payload_size = 0u;
    size_t header_size = 0u;
    size_t wire_size;
    uint8_t type;
    rc = turbo_ltv_peek_size(data + offset, data_size - offset, &payload_size, &header_size);
    if (rc != TURBO_OK || payload_size == 0u || header_size > data_size - offset ||
        payload_size > data_size - offset - header_size) {
      return TURBO_EPROTO;
    }
    wire_size = header_size + payload_size;
    if (turbo_parse_ltv(data + offset, wire_size, &message) != TURBO_OK || !message) {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    type = turbo_ltv_type(message);
    if (type == 0u || type > 7u || type <= previous_type) {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    sizes[type] = turbo_ltv_value_len(message);
    if (sizes[type] != (size_t)payload_size - 1u) {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    /* Keep record fields as views into the durable snapshot, not into the
       parser handle whose lifetime ends at the bottom of this loop. */
    values[type] = data + offset + header_size + 1u;
    previous_type = type;
    turbo_free_ltv(&message);
    offset += wire_size;
    (void)header_size;
  }
  if (!values[1] || !values[2] || !values[3] || !values[4] ||
      sizes[4] != FLOW_TFMP_STORE_METADATA_SIZE || (values[5] != NULL) != (values[6] != NULL) ||
      (values[5] != NULL) != (values[7] != NULL) || sizes[3] > service->config.max_request_bytes) {
    return TURBO_EPROTO;
  }
  envelope.kind = TURBO_FLOW_TFMP_COMMAND_SUBMIT;
  envelope.body = values[3];
  envelope.body_size = sizes[3];
  rc = turbo_flow_tfmp_envelope_validate_schema(&envelope);
  if (rc == TURBO_OK) rc = flow_tfmp_management_parse_command(&envelope, &command, &result);
  if (rc != TURBO_OK || result.status != TURBO_FLOW_TFMP_STATUS_OK ||
      command.reply_mode != TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION ||
      command.required_durability != TURBO_FLOW_TFMP_DURABILITY_DURABLE) {
    return TURBO_EPROTO;
  }
  flags = flow_tfmp_store_read_u16(values[4]);
  if ((flags & ~3u) != 0u || (flags & 2u) == 0u ||
      service->command_record_count >= service->config.dedup_capacity) {
    return TURBO_EPROTO;
  }
  for (size_t i = 0u; i < service->config.dedup_capacity; ++i)
    if (!service->command_records[i].occupied) {
      record = &service->command_records[i];
      break;
    }
  if (!record) return TURBO_ENOSPC;
  rc = flow_tfmp_store_copy_text(record->client_id, sizeof(record->client_id), values[1], sizes[1]);
  if (rc == TURBO_OK)
    rc = flow_tfmp_store_copy_text(record->idempotency_key, sizeof(record->idempotency_key),
                                   values[2], sizes[2]);
  if (rc != TURBO_OK) goto fail;
  record->request_body = (uint8_t *)malloc(sizes[3]);
  if (!record->request_body) return TURBO_ENOMEM;
  memcpy(record->request_body, values[3], sizes[3]);
  record->request_body_size = sizes[3];
  record->status = (turbo_flow_tfmp_status_t)flow_tfmp_store_read_u16(values[4] + 2u);
  record->native_status = (int32_t)flow_tfmp_store_read_u32(values[4] + 4u);
  record->generation_before = flow_tfmp_store_read_u64(values[4] + 8u);
  record->generation_after = flow_tfmp_store_read_u64(values[4] + 16u);
  record->observed_generation = flow_tfmp_store_read_u64(values[4] + 24u);
  memcpy(record->operation_id.bytes, values[4] + 32u, sizeof(record->operation_id.bytes));
  record->operation_state =
      (turbo_flow_tfmp_operation_state_t)flow_tfmp_store_read_u16(values[4] + 48u);
  record->operation_revision = flow_tfmp_store_read_u64(values[4] + 50u);
  record->submitted_unix_ms = flow_tfmp_store_read_u64(values[4] + 58u);
  record->updated_unix_ms = flow_tfmp_store_read_u64(values[4] + 66u);
  record->queue_deadline_unix_ms = flow_tfmp_store_read_u64(values[4] + 74u);
  record->terminal_status = (turbo_flow_tfmp_status_t)flow_tfmp_store_read_u16(values[4] + 82u);
  record->acceptance_disposition =
      (turbo_flow_tfmp_disposition_t)flow_tfmp_store_read_u16(values[4] + 84u);
  record->terminal_unix_ms = flow_tfmp_store_read_u64(values[4] + 86u);
  record->has_generation_result = (flags & 1u) != 0u;
  record->has_operation = 1;
  record->durable = 1;
  if (record->status > TURBO_FLOW_TFMP_STATUS_FAILED_PRECONDITION ||
      record->operation_state < TURBO_FLOW_TFMP_OPERATION_ACCEPTED ||
      record->operation_state > TURBO_FLOW_TFMP_OPERATION_CANCELED ||
      record->operation_revision == 0u ||
      record->acceptance_disposition != TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_DURABLE) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  if (values[5]) {
    rc = flow_tfmp_store_copy_text(record->cancel_client_id, sizeof(record->cancel_client_id),
                                   values[5], sizes[5]);
    if (rc == TURBO_OK)
      rc = flow_tfmp_store_copy_text(record->cancel_idempotency_key,
                                     sizeof(record->cancel_idempotency_key), values[6], sizes[6]);
    if (rc != TURBO_OK || sizes[7] == 0u || sizes[7] > service->config.max_request_bytes) {
      rc = TURBO_EPROTO;
      goto fail;
    }
    record->cancel_request_body = (uint8_t *)malloc(sizes[7]);
    if (!record->cancel_request_body) {
      rc = TURBO_ENOMEM;
      goto fail;
    }
    memcpy(record->cancel_request_body, values[7], sizes[7]);
    record->cancel_request_body_size = sizes[7];
  }
  if (record->terminal_unix_ms != 0u && now_unix_ms >= record->terminal_unix_ms &&
      now_unix_ms - record->terminal_unix_ms >= service->config.dedup_ttl_ms) {
    flow_tfmp_management_record_clear(record);
    *snapshot_dirty = 1;
    return TURBO_OK;
  }
  if (record->operation_state == TURBO_FLOW_TFMP_OPERATION_RUNNING ||
      record->operation_state == TURBO_FLOW_TFMP_OPERATION_CANCEL_REQUESTED) {
    ++service->recovery_required_count;
  } else if (flow_tfmp_management_operation_terminal(record->operation_state)) {
    record->terminal_ns = turbo_hrtime();
  }
  if (record->queue_deadline_unix_ms == UINT64_MAX) {
    record->queue_deadline_ns = UINT64_MAX;
  } else if (record->queue_deadline_unix_ms <= now_unix_ms) {
    record->queue_deadline_ns = turbo_hrtime();
  } else {
    uint64_t remaining_ms = record->queue_deadline_unix_ms - now_unix_ms;
    uint64_t now_ns = turbo_hrtime();
    record->queue_deadline_ns = remaining_ms > (UINT64_MAX - now_ns) / UINT64_C(1000000)
                                    ? UINT64_MAX
                                    : now_ns + remaining_ms * UINT64_C(1000000);
  }
  record->accepted_ns = turbo_hrtime();
  record->occupied = 1;
  ++service->command_record_count;
  return TURBO_OK;

fail:
  flow_tfmp_management_record_clear(record);
  return rc;
}

static int
flow_tfmp_management_store_decode_event_metadata(turbo_flow_tfmp_management_service_t *service,
                                                 const uint8_t *value, size_t value_size) {
  if (!service || !value || value_size != FLOW_TFMP_STORE_EVENT_METADATA_SIZE) return TURBO_EPROTO;
  memcpy(service->incarnation_id.bytes, value, sizeof(service->incarnation_id.bytes));
  service->event_sequence = flow_tfmp_store_read_u64(value + sizeof(service->incarnation_id.bytes));
  return TURBO_OK;
}

static int flow_tfmp_management_store_decode_event(turbo_flow_tfmp_management_service_t *service,
                                                   const uint8_t *value, size_t value_size) {
  turbo_flow_tfmp_envelope_t envelope = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  flow_tfmp_event_record_t *event;
  uint64_t sequence;
  uint16_t category;
  size_t body_size;
  int rc;
  if (!service || !value || value_size < FLOW_TFMP_STORE_EVENT_RECORD_OVERHEAD ||
      service->config.event_capacity == 0u ||
      service->event_count >= service->config.event_capacity)
    return TURBO_EPROTO;
  sequence = flow_tfmp_store_read_u64(value);
  category = flow_tfmp_store_read_u16(value + 8u);
  body_size = value_size - FLOW_TFMP_STORE_EVENT_RECORD_OVERHEAD;
  if (sequence == 0u || sequence > service->event_sequence || body_size == 0u ||
      body_size > FLOW_TFMP_EVENT_BODY_MAX || !flow_tfmp_management_event_category_name(category))
    return TURBO_EPROTO;
  if (service->event_count > 0u && sequence <= service->events[service->event_count - 1u].sequence)
    return TURBO_EPROTO;
  envelope.kind = TURBO_FLOW_TFMP_EVENT;
  envelope.flags = TURBO_FLOW_TFMP_FLAG_EVENT;
  envelope.correlation_id = sequence;
  envelope.body = value + FLOW_TFMP_STORE_EVENT_RECORD_OVERHEAD;
  envelope.body_size = body_size;
  rc = turbo_flow_tfmp_envelope_validate_schema(&envelope);
  if (rc == TURBO_OK)
    rc = flow_tfmp_management_find_body_field(envelope.body, body_size, 2u, &field);
  if (rc != TURBO_OK || field.value_size != sizeof(service->incarnation_id.bytes) ||
      memcmp(field.value, service->incarnation_id.bytes, sizeof(service->incarnation_id.bytes)) !=
          0)
    return TURBO_EPROTO;
  field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  rc = flow_tfmp_management_find_body_field(envelope.body, body_size, 3u, &field);
  if (rc != TURBO_OK || turbo_flow_tfmp_field_read_u16(&field, &category) != TURBO_OK ||
      category != flow_tfmp_store_read_u16(value + 8u))
    return TURBO_EPROTO;
  event = &service->events[service->event_count++];
  event->sequence = sequence;
  event->category = category;
  event->body_size = body_size;
  memcpy(event->body, envelope.body, body_size);
  return TURBO_OK;
}

static int flow_tfmp_management_store_load(turbo_flow_tfmp_management_service_t *service) {
  size_t loaded_size = 0u;
  size_t offset = 0u;
  uint8_t previous_type = 0u;
  uint8_t store_minor = FLOW_TFMP_STORE_VERSION_MINOR_OPERATIONS;
  int header_seen = 0;
  int authority_seen = 0;
  int event_metadata_seen = 0;
  int snapshot_dirty = 0;
  int rc;
  if (!service || !service->operation_store) return TURBO_EINVAL;
  rc = service->operation_store->load(service->operation_store->ctx, service->operation_store_key,
                                      service->store_buffer, service->store_buffer_capacity,
                                      &loaded_size);
  if (rc == TURBO_ENOENT) return TURBO_OK;
  if (rc != TURBO_OK) return rc;
  if (loaded_size == 0u) return TURBO_EPROTO;
  while (offset < loaded_size) {
    turbo_ltv_message_t *message = NULL;
    uint32_t payload_size = 0u;
    size_t header_size = 0u;
    size_t wire_size;
    uint8_t type;
    const uint8_t *value;
    size_t value_size;
    rc = turbo_ltv_peek_size(service->store_buffer + offset, loaded_size - offset, &payload_size,
                             &header_size);
    if (rc != TURBO_OK || payload_size == 0u || header_size > loaded_size - offset ||
        payload_size > loaded_size - offset - header_size) {
      return TURBO_EPROTO;
    }
    wire_size = header_size + payload_size;
    if (turbo_parse_ltv(service->store_buffer + offset, wire_size, &message) != TURBO_OK ||
        !message) {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    type = turbo_ltv_type(message);
    value = turbo_ltv_value(message);
    value_size = turbo_ltv_value_len(message);
    if (type == 0u || type > 5u || type < previous_type ||
        (type == previous_type && type != 3u && type != 5u)) {
      turbo_free_ltv(&message);
      return TURBO_EPROTO;
    }
    if (type == 1u) {
      if (header_seen || offset != 0u || value_size != FLOW_TFMP_STORE_HEADER_SIZE ||
          memcmp(value, "TFMS", 4u) != 0 || value[4] != 0u ||
          value[5] != FLOW_TFMP_STORE_VERSION_MAJOR || value[6] != 0u ||
          (value[7] != FLOW_TFMP_STORE_VERSION_MINOR_OPERATIONS &&
           value[7] != FLOW_TFMP_STORE_VERSION_MINOR_EVENTS)) {
        turbo_free_ltv(&message);
        return TURBO_EPROTO;
      }
      store_minor = value[7];
      if (store_minor == FLOW_TFMP_STORE_VERSION_MINOR_EVENTS && !service->durable_event_replay) {
        turbo_free_ltv(&message);
        return TURBO_EPROTO;
      }
      header_seen = 1;
    } else if (type == 2u) {
      size_t authority_size = strlen(service->config.authority_id);
      if (!header_seen || authority_seen || value_size != authority_size ||
          memcmp(value, service->config.authority_id, value_size)) {
        turbo_free_ltv(&message);
        return TURBO_EPROTO;
      }
      authority_seen = 1;
    } else if (type == 3u) {
      if (!authority_seen || event_metadata_seen) {
        turbo_free_ltv(&message);
        return TURBO_EPROTO;
      }
      rc = flow_tfmp_management_store_decode_record(service, value, value_size, &snapshot_dirty);
      if (rc != TURBO_OK) {
        turbo_free_ltv(&message);
        return rc;
      }
    } else if (type == 4u) {
      if (!authority_seen || store_minor != FLOW_TFMP_STORE_VERSION_MINOR_EVENTS ||
          !service->durable_event_replay || event_metadata_seen) {
        turbo_free_ltv(&message);
        return TURBO_EPROTO;
      }
      rc = flow_tfmp_management_store_decode_event_metadata(service, value, value_size);
      if (rc != TURBO_OK) {
        turbo_free_ltv(&message);
        return rc;
      }
      event_metadata_seen = 1;
    } else {
      if (!event_metadata_seen) {
        turbo_free_ltv(&message);
        return TURBO_EPROTO;
      }
      rc = flow_tfmp_management_store_decode_event(service, value, value_size);
      if (rc != TURBO_OK) {
        turbo_free_ltv(&message);
        return rc;
      }
    }
    previous_type = type;
    turbo_free_ltv(&message);
    offset += wire_size;
    (void)header_size;
  }
  if (!header_seen || !authority_seen) return TURBO_EPROTO;
  if (store_minor == FLOW_TFMP_STORE_VERSION_MINOR_EVENTS) {
    if (!event_metadata_seen || (service->event_count == 0u && service->event_sequence != 0u) ||
        (service->event_count > 0u &&
         service->events[service->event_count - 1u].sequence != service->event_sequence))
      return TURBO_EPROTO;
  } else if (service->durable_event_replay) {
    snapshot_dirty = 1;
  }
  return snapshot_dirty ? flow_tfmp_management_store_commit(service) : TURBO_OK;
}

static int flow_tfmp_management_service_create_internal(
    const turbo_flow_tfmp_management_config_t *config,
    const turbo_flow_tfmp_management_store_binding_t *binding, int durable_event_replay,
    turbo_flow_tfmp_management_service_t **out) {
  turbo_flow_tfmp_management_service_t *service = NULL;
  const char *authority_end;
  char diagnostic_probe[FLOW_TFMP_DIAGNOSTIC_MAX + 1u];
  size_t probe_size = 0u;
  int rc;

  if (!config || config->size < sizeof(*config) || !out ||
      (durable_event_replay && (!binding || config->event_capacity == 0u)))
    return TURBO_EINVAL;
  if (binding && (binding->size < sizeof(*binding) || !binding->store ||
                  binding->store->size < sizeof(*binding->store) || !binding->store->ctx ||
                  !binding->store->load || !binding->store->commit ||
                  binding->store->max_value_size == 0u || !binding->key || !binding->key[0] ||
                  strlen(binding->key) > TURBO_FLOW_TFMP_MANAGEMENT_STORE_KEY_MAX))
    return TURBO_EINVAL;
  authority_end = (const char *)memchr(config->authority_id, '\0', sizeof(config->authority_id));
  if (!authority_end || authority_end == config->authority_id ||
      !tstr_v_utf8_valid(
          tstr_v_from_buf(config->authority_id, (size_t)(authority_end - config->authority_id))) ||
      config->max_request_bytes < TURBO_FLOW_TFMP_HEADER_SIZE ||
      config->max_request_bytes > TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE ||
      config->max_reply_bytes < TURBO_FLOW_TFMP_HEADER_SIZE ||
      config->max_reply_bytes > TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE || config->max_page_items == 0u ||
      config->max_page_items > TURBO_FLOW_TFMP_MANAGEMENT_MAX_PAGE_ITEMS ||
      config->dedup_capacity == 0u ||
      config->dedup_capacity > TURBO_FLOW_TFMP_MANAGEMENT_DEDUP_MAX || config->dedup_ttl_ms == 0u) {
    return TURBO_EINVAL;
  }
  if (config->operation_capacity > config->dedup_capacity) return TURBO_EINVAL;
  if (config->event_capacity > TURBO_FLOW_TFMP_MANAGEMENT_EVENT_MAX) return TURBO_EINVAL;

  service = (turbo_flow_tfmp_management_service_t *)calloc(1u, sizeof(*service));
  if (!service) return TURBO_ENOMEM;
  service->config = *config;
  service->durable_event_replay = durable_event_replay != 0;
  if (service->config.operation_capacity == 0u)
    service->config.operation_capacity = service->config.dedup_capacity;
  if (binding) {
    if (config->max_request_bytes > (SIZE_MAX - FLOW_TFMP_STORE_RECORD_OVERHEAD) / 2u) {
      rc = TURBO_ERANGE;
      goto cleanup;
    }
    service->operation_store = binding->store;
    service->store_buffer_capacity = binding->store->max_value_size;
    service->store_record_buffer_capacity =
        (size_t)config->max_request_bytes * 2u + FLOW_TFMP_STORE_RECORD_OVERHEAD;
    memcpy(service->operation_store_key, binding->key, strlen(binding->key) + 1u);
    service->store_buffer = (uint8_t *)malloc(service->store_buffer_capacity);
    service->store_record_buffer = (uint8_t *)malloc(service->store_record_buffer_capacity);
    if (!service->store_buffer || !service->store_record_buffer) {
      rc = TURBO_ENOMEM;
      goto cleanup;
    }
  }
  service->reply_body_capacity = config->max_reply_bytes - TURBO_FLOW_TFMP_HEADER_SIZE;
  service->reply_body = (uint8_t *)malloc(service->reply_body_capacity);
  if (!service->reply_body) {
    rc = TURBO_ENOMEM;
    goto cleanup;
  }
  service->stage_reply = (uint8_t *)malloc(config->max_reply_bytes);
  if (!service->stage_reply) {
    rc = TURBO_ENOMEM;
    goto cleanup;
  }
  service->command_records = (flow_tfmp_command_record_t *)calloc(
      config->dedup_capacity, sizeof(*service->command_records));
  if (!service->command_records) {
    rc = TURBO_ENOMEM;
    goto cleanup;
  }
  if (config->event_capacity > 0u) {
    service->events =
        (flow_tfmp_event_record_t *)calloc(config->event_capacity, sizeof(*service->events));
    if (!service->events) {
      rc = TURBO_ENOMEM;
      goto cleanup;
    }
  }
  service->state = TURBO_FLOW_TFMP_OWNER_STARTING;
  service->started_ns = turbo_hrtime();
  rc = turbo_uuid_v7_generate(&service->incarnation_id);
  if (rc != TURBO_OK) goto cleanup;
  if (service->operation_store) {
    rc = flow_tfmp_management_store_load(service);
    if (rc != TURBO_OK) goto cleanup;
  }

  rc = flow_tfmp_management_build_capabilities(service, &probe_size);
  if (rc != TURBO_OK) goto cleanup;
  memset(diagnostic_probe, 'x', FLOW_TFMP_DIAGNOSTIC_MAX);
  diagnostic_probe[FLOW_TFMP_DIAGNOSTIC_MAX] = '\0';
  rc = flow_tfmp_management_build_error(service, diagnostic_probe, TURBO_EPROTO, &probe_size);
  if (rc != TURBO_OK) goto cleanup;
  *out = service;
  return TURBO_OK;

cleanup:
  if (service->command_records)
    for (size_t i = 0u; i < service->config.dedup_capacity; ++i)
      flow_tfmp_management_record_clear(&service->command_records[i]);
  free(service->store_record_buffer);
  free(service->store_buffer);
  free(service->command_records);
  free(service->events);
  free(service->stage_reply);
  free(service->reply_body);
  free(service);
  return rc;
}

int turbo_flow_tfmp_management_service_create_with_store(
    const turbo_flow_tfmp_management_config_t *config,
    const turbo_flow_tfmp_management_store_binding_t *binding,
    turbo_flow_tfmp_management_service_t **out) {
  return flow_tfmp_management_service_create_internal(config, binding, 0, out);
}

int turbo_flow_tfmp_management_service_create(const turbo_flow_tfmp_management_config_t *config,
                                              turbo_flow_tfmp_management_service_t **out) {
  return turbo_flow_tfmp_management_service_create_with_store(config, NULL, out);
}

int turbo_flow_tfmp_management_service_create_configured(
    const turbo_flow_tfmp_management_channel_config_t *config,
    const turbo_flow_tfmp_management_store_binding_t *binding,
    turbo_flow_tfmp_management_service_t **out) {
  const char *store_end;
  const char *event_store_end;
  int uses_memory;
  int durable_event_replay;
  if (!config || config->size < sizeof(*config) || !out) return TURBO_EINVAL;
  store_end = (const char *)memchr(config->operation_store, '\0', sizeof(config->operation_store));
  if (!store_end || store_end == config->operation_store) return TURBO_EINVAL;
  uses_memory = (size_t)(store_end - config->operation_store) == sizeof("memory") - 1u &&
                memcmp(config->operation_store, "memory", sizeof("memory") - 1u) == 0;
  if (uses_memory != (binding == NULL)) return TURBO_EINVAL;
  event_store_end =
      (const char *)memchr(config->event_replay_store, '\0', sizeof(config->event_replay_store));
  if (!event_store_end) return TURBO_EINVAL;
  durable_event_replay = event_store_end != config->event_replay_store &&
                         strcmp(config->event_replay_store, "memory") != 0;
  if (durable_event_replay &&
      (uses_memory || strcmp(config->event_replay_store, config->operation_store) != 0))
    return TURBO_EINVAL;
  return flow_tfmp_management_service_create_internal(&config->service, binding,
                                                      durable_event_replay, out);
}

int turbo_flow_tfmp_management_service_set_reconciler(
    turbo_flow_tfmp_management_service_t *service,
    const turbo_flow_tfmp_reconcile_binding_t *binding) {
  if (!service || !binding || binding->size < sizeof(*binding) || !binding->supports ||
      !binding->inspect)
    return TURBO_EINVAL;
  if (service->state != TURBO_FLOW_TFMP_OWNER_STARTING || service->target) return TURBO_EBUSY;
  if (service->reconciler.inspect) return TURBO_EALREADY;
  service->reconciler = *binding;
  return TURBO_OK;
}

void turbo_flow_tfmp_management_service_destroy(turbo_flow_tfmp_management_service_t *service) {
  if (!service) return;
  for (size_t i = 0u; i < service->config.dedup_capacity; ++i)
    flow_tfmp_management_record_clear(&service->command_records[i]);
  free(service->store_record_buffer);
  free(service->store_buffer);
  free(service->command_records);
  free(service->events);
  free(service->stage_reply);
  free(service->reply_body);
  free(service);
}

int turbo_flow_tfmp_management_service_bind_target(turbo_flow_tfmp_management_service_t *service,
                                                   const char *target_uid, turbo_flow_t *flow) {
  turbo_flow_runtime_snapshot_t snapshot;
  size_t uid_size;
  int rc;
  if (!service || !target_uid || !flow) return TURBO_EINVAL;
  if (service->state != TURBO_FLOW_TFMP_OWNER_STARTING) return TURBO_EBUSY;
  if (service->target) return TURBO_EALREADY;
  uid_size = strlen(target_uid);
  if (uid_size == 0u || uid_size > TURBO_FLOW_RESOURCE_UID_MAX ||
      !tstr_v_utf8_valid(tstr_v_from_buf(target_uid, uid_size))) {
    return TURBO_EINVAL;
  }
  rc = turbo_flow_runtime_snapshot(flow, &snapshot);
  if (rc != TURBO_OK) return rc;
  memcpy(service->target_uid, target_uid, uid_size + 1u);
  service->target = flow;
  service->target_state = snapshot.state;
  service->target_accepting_publishes = snapshot.accepting_publishes;
  service->target_generation = 1u;
  service->catalog_generation = 1u;
  service->catalog_resource_count = turbo_flow_resource_metadata_count(flow);
  return TURBO_OK;
}

int turbo_flow_tfmp_management_service_set_state(turbo_flow_tfmp_management_service_t *service,
                                                 turbo_flow_tfmp_owner_state_t state) {
  turbo_flow_tfmp_owner_state_t previous_state;
  int accepted = 0;
  int rc;
  if (!service || state < TURBO_FLOW_TFMP_OWNER_STARTING || state > TURBO_FLOW_TFMP_OWNER_FAILED)
    return TURBO_EINVAL;
  if (service->state == state) return TURBO_OK;
  switch (service->state) {
  case TURBO_FLOW_TFMP_OWNER_STARTING:
    accepted = (state == TURBO_FLOW_TFMP_OWNER_READY && service->recovery_required_count == 0u) ||
               state == TURBO_FLOW_TFMP_OWNER_FAILED;
    break;
  case TURBO_FLOW_TFMP_OWNER_READY:
    accepted = state == TURBO_FLOW_TFMP_OWNER_DRAINING || state == TURBO_FLOW_TFMP_OWNER_FAILED;
    break;
  case TURBO_FLOW_TFMP_OWNER_DRAINING:
    accepted = state == TURBO_FLOW_TFMP_OWNER_STOPPED || state == TURBO_FLOW_TFMP_OWNER_FAILED;
    break;
  case TURBO_FLOW_TFMP_OWNER_STOPPED:
  case TURBO_FLOW_TFMP_OWNER_FAILED:
  default:
    break;
  }
  if (!accepted) return TURBO_EBUSY;
  previous_state = service->state;
  service->state = state;
  {
    turbo_flow_tfmp_field_t target_uid = TURBO_FLOW_TFMP_FIELD_INIT;
    const turbo_flow_tfmp_field_t *target = NULL;
    if (service->target) {
      target_uid.value = (const uint8_t *)service->target_uid;
      target_uid.value_size = strlen(service->target_uid);
      target = &target_uid;
    }
    rc = flow_tfmp_management_event_emit(
        service, TURBO_FLOW_TFMP_EVENT_LIFECYCLE, TURBO_FLOW_TFMP_EVENT_OWNER_STATE_CHANGED, target,
        service->target != NULL, service->target_generation, NULL, 0u);
    if (rc != TURBO_OK && service->durable_event_replay) {
      service->state = previous_state;
      return rc;
    }
  }
  return TURBO_OK;
}

turbo_flow_tfmp_owner_state_t
turbo_flow_tfmp_management_service_state(const turbo_flow_tfmp_management_service_t *service) {
  return service ? service->state : (turbo_flow_tfmp_owner_state_t)0;
}

static int flow_tfmp_management_service_execute_impl(
    turbo_flow_tfmp_management_service_t *service, const uint8_t *request, size_t request_size,
    uint8_t *out, size_t capacity, size_t *out_len, int allow_wait_terminal) {
  turbo_flow_tfmp_envelope_t envelope = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  flow_tfmp_management_result_t result = {TURBO_FLOW_TFMP_STATUS_OK, NULL, TURBO_OK, 0u,
                                          TURBO_FLOW_TFMP_DISPOSITION_NONE};
  const turbo_flow_tfmp_envelope_t *decoded_request = &envelope;
  uint16_t reply_kind = TURBO_FLOW_TFMP_PROTOCOL_ERROR;
  uint64_t correlation_id = 0u;
  int rc;

  if (!service || (!request && request_size > 0u) || !out_len) return TURBO_EINVAL;
  *out_len = 0u;
  turbo_flow_tfmp_envelope_peek_request_identity(request, request_size, &reply_kind,
                                                 &correlation_id);

  if (request_size > service->config.max_request_bytes) {
    flow_tfmp_management_result_fail(&result, TURBO_FLOW_TFMP_STATUS_RESOURCE_EXHAUSTED,
                                     "request exceeds configured management limit", TURBO_EMSGSIZE);
    decoded_request = NULL;
    goto reply;
  }
  rc = turbo_flow_tfmp_envelope_decode(request, request_size, &envelope);
  if (rc != TURBO_OK) {
    flow_tfmp_management_result_fail(
        &result,
        rc == TURBO_ENOTSUP ? TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_VERSION
                            : turbo_flow_tfmp_status_from_error(rc),
        rc == TURBO_ENOTSUP ? "unsupported TFMP major version" : "malformed TFMP request", rc);
    decoded_request = NULL;
    goto reply;
  }
  if (envelope.flags != 0u) {
    reply_kind = TURBO_FLOW_TFMP_PROTOCOL_ERROR;
    correlation_id = 0u;
    flow_tfmp_management_result_fail(&result, TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT,
                                     "management owner accepts requests only", TURBO_EPROTO);
    decoded_request = NULL;
    goto reply;
  }
  reply_kind = envelope.kind;
  correlation_id = envelope.correlation_id;
  rc = turbo_flow_tfmp_envelope_validate_schema(&envelope);
  if (rc != TURBO_OK) {
    flow_tfmp_management_result_fail(
        &result, turbo_flow_tfmp_status_from_error(rc),
        rc == TURBO_ENOTSUP ? "unsupported TFMP capability" : "TFMP request schema violation", rc);
    goto reply;
  }
  switch (envelope.kind) {
  case TURBO_FLOW_TFMP_CAPABILITIES_GET:
  case TURBO_FLOW_TFMP_HEALTH_GET:
  case TURBO_FLOW_TFMP_TARGET_LIST:
  case TURBO_FLOW_TFMP_TARGET_GET:
  case TURBO_FLOW_TFMP_RESOURCE_LIST:
  case TURBO_FLOW_TFMP_RESOURCE_GET:
  case TURBO_FLOW_TFMP_RESOURCE_DOCUMENT_GET:
  case TURBO_FLOW_TFMP_COMMAND_SUBMIT:
  case TURBO_FLOW_TFMP_OPERATION_GET:
  case TURBO_FLOW_TFMP_OPERATION_CANCEL:
  case TURBO_FLOW_TFMP_EVENTS_GET:
    break;
  default:
    flow_tfmp_management_result_fail(&result, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY,
                                     "TFMP capability is not enabled by this owner", TURBO_ENOTSUP);
    break;
  }

reply:
  return flow_tfmp_management_encode_response(service, decoded_request, reply_kind, correlation_id,
                                              &result, out, capacity, out_len,
                                              allow_wait_terminal);
}

int turbo_flow_tfmp_management_service_execute(turbo_flow_tfmp_management_service_t *service,
                                               const uint8_t *request, size_t request_size,
                                               uint8_t *out, size_t capacity, size_t *out_len) {
  return flow_tfmp_management_service_execute_impl(service, request, request_size, out, capacity,
                                                   out_len, 1);
}

static int flow_tfmp_management_reconcile_pool(turbo_flow_tfmp_management_service_t *service,
                                               const flow_tfmp_command_request_t *command,
                                               turbo_flow_tfmp_reconcile_result_t *result) {
  turbo_flow_resource_command_t prepared = TURBO_FLOW_RESOURCE_COMMAND_INIT;
  int rc =
      flow_tfmp_management_prepare_resource_command(service, command, "tfmp-reconcile", &prepared);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < turbo_flow_pool_count(service->target); ++i) {
    turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
    rc = turbo_flow_pool_resource_status_at(service->target, i, &status);
    if (rc != TURBO_OK) return rc;
    if (strcmp(status.uid, prepared.target_uid) != 0) continue;
    result->generation = status.generation;
    result->observed_generation = status.observed_generation;
    if (status.snapshot.parallelism == prepared.parallelism)
      result->outcome = TURBO_FLOW_TFMP_RECONCILE_APPLIED;
    else if (status.generation == prepared.expected_generation)
      result->outcome = TURBO_FLOW_TFMP_RECONCILE_NOT_APPLIED;
    else result->outcome = TURBO_FLOW_TFMP_RECONCILE_CONFLICT;
    return TURBO_OK;
  }
  return TURBO_ENOENT;
}

static int flow_tfmp_management_reconcile_inspect(turbo_flow_tfmp_management_service_t *service,
                                                  const flow_tfmp_command_request_t *command,
                                                  turbo_flow_tfmp_reconcile_result_t *result) {
  if (flow_tfmp_management_is_flow_command(command->command_type)) {
    turbo_flow_runtime_snapshot_t snapshot;
    int rc = flow_tfmp_management_target_snapshot(service, &snapshot);
    if (rc != TURBO_OK) return rc;
    result->generation = service->target_generation;
    result->observed_generation = service->target_generation;
    if ((command->command_type == TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE &&
         !snapshot.accepting_publishes) ||
        (command->command_type == TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME &&
         snapshot.accepting_publishes) ||
        (command->command_type == TURBO_FLOW_TFMP_COMMAND_FLOW_DRAIN &&
         !snapshot.accepting_publishes && snapshot.active_publishes == 0u)) {
      result->outcome = TURBO_FLOW_TFMP_RECONCILE_APPLIED;
    } else if (!command->has_expected_generation ||
               command->expected_generation == result->generation) {
      result->outcome = TURBO_FLOW_TFMP_RECONCILE_NOT_APPLIED;
    } else {
      result->outcome = TURBO_FLOW_TFMP_RECONCILE_CONFLICT;
    }
    return TURBO_OK;
  }
  if (command->command_type == TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE)
    return flow_tfmp_management_reconcile_pool(service, command, result);
  if (service->reconciler.inspect && service->reconciler.supports &&
      service->reconciler.supports(service->reconciler.ctx, command->command_type)) {
    turbo_flow_tfmp_reconcile_request_t request = TURBO_FLOW_TFMP_RECONCILE_REQUEST_INIT;
    request.command_type = command->command_type;
    request.target_uid = (const char *)command->target_uid.value;
    request.target_uid_size = command->target_uid.value_size;
    request.has_expected_generation = command->has_expected_generation;
    request.expected_generation = command->expected_generation;
    request.payload = command->has_payload ? command->payload.value : NULL;
    request.payload_size = command->has_payload ? command->payload.value_size : 0u;
    return service->reconciler.inspect(service->reconciler.ctx, service->target, &request, result);
  }
  return TURBO_ENOTSUP;
}

static int flow_tfmp_management_reconcile_finish(
    turbo_flow_tfmp_management_service_t *service, flow_tfmp_command_record_t *record,
    const turbo_flow_tfmp_field_t *target_uid, const flow_tfmp_management_result_t *result,
    uint64_t generation_before, uint64_t generation_after, uint64_t observed_generation) {
  int rc;
  record->generation_before = generation_before;
  record->generation_after = generation_after;
  record->observed_generation = observed_generation;
  record->has_generation_result = 1;
  record->terminal_status = result->status;
  record->operation_state = result->status == TURBO_FLOW_TFMP_STATUS_OK
                                ? TURBO_FLOW_TFMP_OPERATION_SUCCEEDED
                                : TURBO_FLOW_TFMP_OPERATION_FAILED;
  ++record->operation_revision;
  record->updated_unix_ms = flow_tfmp_management_unix_ms();
  record->terminal_ns = turbo_hrtime();
  record->terminal_unix_ms = record->updated_unix_ms;
  if (!service->durable_event_replay) {
    rc = flow_tfmp_management_store_commit(service);
    if (rc != TURBO_OK) {
      service->state = TURBO_FLOW_TFMP_OWNER_FAILED;
      return rc;
    }
  }
  rc = flow_tfmp_management_event_emit(
      service,
      result->status == TURBO_FLOW_TFMP_STATUS_OK ? TURBO_FLOW_TFMP_EVENT_OPERATION
                                                  : TURBO_FLOW_TFMP_EVENT_FAULT,
      result->status == TURBO_FLOW_TFMP_STATUS_OK ? TURBO_FLOW_TFMP_EVENT_OPERATION_TERMINAL
                                                  : TURBO_FLOW_TFMP_EVENT_COMMAND_FAILED,
      target_uid && target_uid->value ? target_uid : NULL, 1, generation_after,
      &record->operation_id, record->operation_revision);
  if (rc != TURBO_OK) {
    if (service->durable_event_replay) service->state = TURBO_FLOW_TFMP_OWNER_FAILED;
    return rc;
  }
  if (service->recovery_required_count == 0u) return TURBO_EPROTO;
  --service->recovery_required_count;
  return TURBO_OK;
}

int turbo_flow_tfmp_management_service_reconcile_one(
    turbo_flow_tfmp_management_service_t *service) {
  flow_tfmp_command_record_t *record = NULL;
  flow_tfmp_command_request_t command = {0};
  flow_tfmp_management_result_t execution = {TURBO_FLOW_TFMP_STATUS_OK, NULL, TURBO_OK, 0u,
                                             TURBO_FLOW_TFMP_DISPOSITION_NONE};
  turbo_flow_tfmp_reconcile_result_t inspected = TURBO_FLOW_TFMP_RECONCILE_RESULT_INIT;
  turbo_flow_tfmp_envelope_t request = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  turbo_flow_tfmp_field_t target_uid = TURBO_FLOW_TFMP_FIELD_INIT;
  uint64_t generation_before = 0u;
  uint64_t generation_after = 0u;
  uint64_t observed_generation = 0u;
  char operation_key[TURBO_UUID_STRING_SIZE];
  int rc;
  if (!service || !service->target || !service->operation_store) return TURBO_EINVAL;
  if (service->state != TURBO_FLOW_TFMP_OWNER_STARTING &&
      service->state != TURBO_FLOW_TFMP_OWNER_READY)
    return TURBO_EBUSY;
  for (size_t i = 0u; i < service->config.dedup_capacity; ++i) {
    flow_tfmp_command_record_t *candidate = &service->command_records[i];
    if (candidate->occupied && candidate->durable && candidate->has_operation &&
        (candidate->operation_state == TURBO_FLOW_TFMP_OPERATION_RUNNING ||
         candidate->operation_state == TURBO_FLOW_TFMP_OPERATION_CANCEL_REQUESTED)) {
      record = candidate;
      break;
    }
  }
  if (!record) return TURBO_ENOENT;
  request.kind = TURBO_FLOW_TFMP_COMMAND_SUBMIT;
  request.body = record->request_body;
  request.body_size = record->request_body_size;
  rc = flow_tfmp_management_parse_command(&request, &command, &execution);
  if (rc != TURBO_OK || execution.status != TURBO_FLOW_TFMP_STATUS_OK) return TURBO_EPROTO;
  rc = flow_tfmp_management_find_body_field(record->request_body, record->request_body_size, 3u,
                                            &target_uid);
  if (rc != TURBO_OK) return TURBO_EPROTO;
  rc = flow_tfmp_management_reconcile_inspect(service, &command, &inspected);
  if (rc != TURBO_OK) return rc;
  if (inspected.outcome < TURBO_FLOW_TFMP_RECONCILE_APPLIED ||
      inspected.outcome > TURBO_FLOW_TFMP_RECONCILE_CONFLICT || inspected.generation == 0u ||
      (inspected.outcome == TURBO_FLOW_TFMP_RECONCILE_NOT_APPLIED &&
       command.has_expected_generation && inspected.generation != command.expected_generation))
    return TURBO_EPROTO;

  generation_before = inspected.generation;
  generation_after = inspected.generation;
  observed_generation = inspected.observed_generation;
  if (inspected.outcome == TURBO_FLOW_TFMP_RECONCILE_CONFLICT) {
    flow_tfmp_management_result_fail(&execution, TURBO_FLOW_TFMP_STATUS_CONFLICT,
                                     "recovered side effect conflicts with current generation",
                                     TURBO_EBUSY);
  } else if (inspected.outcome == TURBO_FLOW_TFMP_RECONCILE_NOT_APPLIED) {
    if (flow_tfmp_management_is_flow_command(command.command_type)) {
      rc = flow_tfmp_management_execute_flow_command(service, &command, &execution,
                                                     &generation_before, &generation_after,
                                                     &observed_generation);
    } else {
      rc = turbo_uuid_format(&record->operation_id, operation_key, sizeof(operation_key));
      if (rc == TURBO_OK)
        rc = flow_tfmp_management_execute_resource_command(service, &command, operation_key,
                                                           &execution, &generation_before,
                                                           &generation_after, &observed_generation);
    }
    if (rc != TURBO_OK) return rc;
  }
  return flow_tfmp_management_reconcile_finish(service, record, &target_uid, &execution,
                                               generation_before, generation_after,
                                               observed_generation);
}

int turbo_flow_tfmp_management_service_run_one(turbo_flow_tfmp_management_service_t *service) {
  flow_tfmp_command_record_t *record = NULL;
  flow_tfmp_command_request_t command = {0};
  flow_tfmp_management_result_t result = {TURBO_FLOW_TFMP_STATUS_OK, NULL, TURBO_OK, 0u,
                                          TURBO_FLOW_TFMP_DISPOSITION_NONE};
  turbo_flow_tfmp_envelope_t request = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  turbo_flow_tfmp_field_t target_uid = TURBO_FLOW_TFMP_FIELD_INIT;
  uint64_t generation_before = 0u;
  uint64_t generation_after = 0u;
  uint64_t observed_generation = 0u;
  uint64_t now_ns;
  uint64_t previous_updated_unix_ms;
  char operation_key[TURBO_UUID_STRING_SIZE];
  int rc;
  if (!service) return TURBO_EINVAL;
  if (service->state != TURBO_FLOW_TFMP_OWNER_READY &&
      service->state != TURBO_FLOW_TFMP_OWNER_DRAINING)
    return TURBO_EBUSY;
  for (size_t i = 0u; i < service->config.dedup_capacity; ++i) {
    flow_tfmp_command_record_t *candidate = &service->command_records[i];
    if (candidate->occupied && candidate->has_operation &&
        candidate->operation_state == TURBO_FLOW_TFMP_OPERATION_ACCEPTED) {
      record = candidate;
      break;
    }
  }
  if (!record) return TURBO_ENOENT;

  now_ns = turbo_hrtime();
  previous_updated_unix_ms = record->updated_unix_ms;
  record->operation_state = TURBO_FLOW_TFMP_OPERATION_RUNNING;
  ++record->operation_revision;
  record->updated_unix_ms = flow_tfmp_management_unix_ms();
  if (record->durable && !service->durable_event_replay) {
    rc = flow_tfmp_management_store_commit(service);
    if (rc != TURBO_OK) {
      record->operation_state = TURBO_FLOW_TFMP_OPERATION_ACCEPTED;
      --record->operation_revision;
      record->updated_unix_ms = previous_updated_unix_ms;
      return rc;
    }
  }
  request.kind = TURBO_FLOW_TFMP_COMMAND_SUBMIT;
  request.body = record->request_body;
  request.body_size = record->request_body_size;
  if (flow_tfmp_management_find_body_field(record->request_body, record->request_body_size, 3u,
                                           &target_uid) != TURBO_OK)
    target_uid = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  rc = flow_tfmp_management_event_emit(service, TURBO_FLOW_TFMP_EVENT_OPERATION,
                                       TURBO_FLOW_TFMP_EVENT_OPERATION_RUNNING,
                                       target_uid.value ? &target_uid : NULL, 0, 0u,
                                       &record->operation_id, record->operation_revision);
  if (rc != TURBO_OK && service->durable_event_replay) {
    record->operation_state = TURBO_FLOW_TFMP_OPERATION_ACCEPTED;
    --record->operation_revision;
    record->updated_unix_ms = previous_updated_unix_ms;
    return rc;
  }
  if (record->queue_deadline_ns != UINT64_MAX && now_ns >= record->queue_deadline_ns) {
    flow_tfmp_management_result_fail(&result, TURBO_FLOW_TFMP_STATUS_DEADLINE_EXCEEDED,
                                     "operation expired before mutation began", TURBO_ETIMEDOUT);
  } else {
    rc = flow_tfmp_management_parse_command(&request, &command, &result);
    if (rc != TURBO_OK) {
      flow_tfmp_management_result_fail(&result, TURBO_FLOW_TFMP_STATUS_INTERNAL,
                                       "stored operation command is invalid", rc);
    } else if (result.status == TURBO_FLOW_TFMP_STATUS_OK &&
               flow_tfmp_management_is_flow_command(command.command_type)) {
      rc = flow_tfmp_management_execute_flow_command(service, &command, &result, &generation_before,
                                                     &generation_after, &observed_generation);
      if (rc != TURBO_OK)
        flow_tfmp_management_result_fail(&result, TURBO_FLOW_TFMP_STATUS_INTERNAL,
                                         "flow operation execution failed", rc);
    } else if (result.status == TURBO_FLOW_TFMP_STATUS_OK &&
               flow_tfmp_management_is_resource_command(command.command_type)) {
      rc = turbo_uuid_format(&record->operation_id, operation_key, sizeof(operation_key));
      if (rc == TURBO_OK)
        rc = flow_tfmp_management_execute_resource_command(service, &command, operation_key,
                                                           &result, &generation_before,
                                                           &generation_after, &observed_generation);
      if (rc != TURBO_OK)
        flow_tfmp_management_result_fail(&result, TURBO_FLOW_TFMP_STATUS_INTERNAL,
                                         "resource operation execution failed", rc);
    } else if (result.status == TURBO_FLOW_TFMP_STATUS_OK) {
      flow_tfmp_management_result_fail(&result, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY,
                                       "stored operation command type is not enabled",
                                       TURBO_ENOTSUP);
    }
  }

  record->generation_before = generation_before;
  record->generation_after = generation_after;
  record->observed_generation = observed_generation;
  record->has_generation_result = 1;
  record->terminal_status = result.status;
  record->operation_state = result.status == TURBO_FLOW_TFMP_STATUS_OK
                                ? TURBO_FLOW_TFMP_OPERATION_SUCCEEDED
                                : TURBO_FLOW_TFMP_OPERATION_FAILED;
  ++record->operation_revision;
  record->updated_unix_ms = flow_tfmp_management_unix_ms();
  record->terminal_ns = turbo_hrtime();
  record->terminal_unix_ms = record->updated_unix_ms;
  if (record->durable && !service->durable_event_replay) {
    rc = flow_tfmp_management_store_commit(service);
    if (rc != TURBO_OK) {
      service->state = TURBO_FLOW_TFMP_OWNER_FAILED;
      return rc;
    }
  }
  rc = flow_tfmp_management_event_emit(
      service,
      result.status == TURBO_FLOW_TFMP_STATUS_OK ? TURBO_FLOW_TFMP_EVENT_OPERATION
                                                 : TURBO_FLOW_TFMP_EVENT_FAULT,
      result.status == TURBO_FLOW_TFMP_STATUS_OK ? TURBO_FLOW_TFMP_EVENT_OPERATION_TERMINAL
                                                 : TURBO_FLOW_TFMP_EVENT_COMMAND_FAILED,
      target_uid.value ? &target_uid : NULL, 1, generation_after, &record->operation_id,
      record->operation_revision);
  if (rc != TURBO_OK && service->durable_event_replay) {
    service->state = TURBO_FLOW_TFMP_OWNER_FAILED;
    return rc;
  }
  return TURBO_OK;
}

int turbo_flow_tfmp_management_stage(turbo_flow_msg_t *msg, void *ctx) {
  turbo_flow_tfmp_management_service_t *service = (turbo_flow_tfmp_management_service_t *)ctx;
  tstr_t payload;
  size_t reply_size = 0u;
  int rc;
  if (!msg || !service || (!msg->payload.data && msg->payload.len > 0u)) return TURBO_EINVAL;
  rc = flow_tfmp_management_service_execute_impl(
      service, (const uint8_t *)msg->payload.data, msg->payload.len, service->stage_reply,
      service->config.max_reply_bytes, &reply_size, 0);
  if (rc != TURBO_OK) return rc;
  payload = tstr_new_len(service->stage_reply, reply_size);
  if (!payload) return TURBO_ENOMEM;
  turbo_flow_msg_clear_content(msg);
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  msg->buffer = NULL;
  msg->owned_payload = payload;
  msg->payload = tstr_to_v(payload);
  return TURBO_OK;
}

int turbo_flow_tfmp_management_event_next(const turbo_flow_tfmp_management_service_t *service,
                                          uint64_t after_sequence, char *topic,
                                          size_t topic_capacity, size_t *topic_size, uint8_t *out,
                                          size_t capacity, size_t *out_size) {
  static const char prefix[] = "tfmp/1/event/";
  turbo_flow_tfmp_envelope_t envelope = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  const flow_tfmp_event_record_t *selected = NULL;
  const char *category;
  size_t required_topic;
  size_t required_wire = 0u;
  uint64_t oldest_sequence;
  int rc;
  if (!service || !topic_size || !out_size || (!topic && topic_capacity > 0u) ||
      (!out && capacity > 0u))
    return TURBO_EINVAL;
  *topic_size = 0u;
  *out_size = 0u;
  if (service->config.event_capacity == 0u) return TURBO_ENOTSUP;
  oldest_sequence = service->event_count > 0u ? service->events[service->event_head].sequence
                                              : service->event_sequence + 1u;
  if (after_sequence > service->event_sequence ||
      (service->event_count > 0u && after_sequence < oldest_sequence - 1u))
    return TURBO_ERANGE;
  for (size_t i = 0u; i < service->event_count; ++i) {
    const flow_tfmp_event_record_t *candidate =
        &service->events[(service->event_head + i) % service->config.event_capacity];
    if (candidate->sequence > after_sequence) {
      selected = candidate;
      break;
    }
  }
  if (!selected) return TURBO_ENOENT;
  category = flow_tfmp_management_event_category_name(selected->category);
  if (!category) return TURBO_EPROTO;
  required_topic = sizeof(prefix) - 1u + strlen(category);
  envelope.kind = TURBO_FLOW_TFMP_EVENT;
  envelope.flags = TURBO_FLOW_TFMP_FLAG_EVENT;
  envelope.correlation_id = selected->sequence;
  envelope.body = selected->body;
  envelope.body_size = selected->body_size;
  rc = turbo_flow_tfmp_envelope_validate_schema(&envelope);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_envelope_encode(&envelope, NULL, 0u, &required_wire);
  if (rc != TURBO_ENOSPC && rc != TURBO_OK) return rc;
  *topic_size = required_topic;
  *out_size = required_wire;
  if (!topic || topic_capacity <= required_topic || !out || capacity < required_wire)
    return TURBO_ENOSPC;
  memcpy(topic, prefix, sizeof(prefix) - 1u);
  memcpy(topic + sizeof(prefix) - 1u, category, strlen(category) + 1u);
  return turbo_flow_tfmp_envelope_encode(&envelope, out, capacity, out_size);
}
