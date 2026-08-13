#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_fmq.h"
#include "turbo_flow_fmq_management.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

static const turbo_flow_coronet_execution_binding_t MANAGEMENT_PRIVATE_EXECUTION = {
    sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};

typedef struct tfmp_management_capture_s {
  atomic_int called;
  uint8_t payload[TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE];
  size_t payload_size;
} tfmp_management_capture_t;

typedef struct tfmp_management_blob_store_s {
  uint8_t data[64u * 1024u];
  size_t data_size;
  int commit_calls;
  int fail_commit;
  int fail_commit_at;
} tfmp_management_blob_store_t;

static int tfmp_management_blob_load(void *ctx, const char *key, uint8_t *out, size_t capacity,
                                     size_t *out_size) {
  tfmp_management_blob_store_t *store = (tfmp_management_blob_store_t *)ctx;
  if (!store || !key || strcmp(key, "fmq:test") != 0 || !out_size || (!out && capacity > 0u))
    return TURBO_EINVAL;
  *out_size = store->data_size;
  if (store->data_size == 0u) return TURBO_ENOENT;
  if (!out || capacity < store->data_size) return TURBO_ENOSPC;
  memcpy(out, store->data, store->data_size);
  return TURBO_OK;
}

static int tfmp_management_blob_commit(void *ctx, const char *key, const uint8_t *data,
                                       size_t data_size) {
  tfmp_management_blob_store_t *store = (tfmp_management_blob_store_t *)ctx;
  if (!store || !key || strcmp(key, "fmq:test") != 0 || (!data && data_size > 0u) ||
      data_size > sizeof(store->data))
    return TURBO_EINVAL;
  ++store->commit_calls;
  if (store->fail_commit || store->commit_calls == store->fail_commit_at) return TURBO_EIO;
  if (data_size > 0u) memcpy(store->data, data, data_size);
  store->data_size = data_size;
  return TURBO_OK;
}

static int tfmp_management_capture(turbo_flow_msg_t *msg, void *ctx) {
  tfmp_management_capture_t *capture = (tfmp_management_capture_t *)ctx;
  if (!msg || !capture || msg->payload.len > sizeof(capture->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0u) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_size = msg->payload.len;
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return TURBO_OK;
}

static int tfmp_management_publish(turbo_flow_t *flow, const uint8_t *data, size_t data_size) {
  turbo_flow_msg_t msg;
  int rc;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_new_len(data, data_size);
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  rc = turbo_flow_publish(flow, "input", &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

typedef struct tfmp_management_resource_fixture_s {
  turbo_flow_resource_metadata_t metadata;
  const char *payload;
  int document_calls;
  int command_calls;
  int command_status;
  turbo_flow_resource_command_t last_command;
} tfmp_management_resource_fixture_t;

static const char TFMP_MANAGEMENT_TEST_SCHEMA_TEXT[] =
    "schema TfmpManagementTest [id(99), version(1)];\n"
    "message Status { bool ready; }\n";

static const turbo_flow_resource_schema_t TFMP_MANAGEMENT_TEST_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_QUEUE_BUFFER,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TfmpManagementTest",
    "Status",
    99u,
    1u,
    TFMP_MANAGEMENT_TEST_SCHEMA_TEXT};

static int tfmp_management_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  tfmp_management_resource_fixture_t *fixture = (tfmp_management_resource_fixture_t *)ctx;
  *out = fixture->metadata;
  return TURBO_OK;
}

static int tfmp_management_resource_document(void *ctx,
                                             turbo_flow_resource_document_kind_t document_kind,
                                             turbo_flow_resource_document_t *out) {
  tfmp_management_resource_fixture_t *fixture = (tfmp_management_resource_fixture_t *)ctx;
  const char *payload = fixture->payload ? fixture->payload : "{\"ready\":true}";
  fixture->document_calls += 1;
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  return turbo_flow_resource_document_set_payload_copy(
      out, &fixture->metadata, &TFMP_MANAGEMENT_TEST_SCHEMA, payload, strlen(payload));
}

static int tfmp_management_resource_command(void *ctx, turbo_flow_t *flow,
                                            const turbo_flow_resource_command_t *command) {
  tfmp_management_resource_fixture_t *fixture = (tfmp_management_resource_fixture_t *)ctx;
  (void)flow;
  fixture->command_calls += 1;
  fixture->last_command = *command;
  if (fixture->command_status != TURBO_OK) return fixture->command_status;
  fixture->metadata.generation += 1u;
  fixture->metadata.observed_generation = fixture->metadata.generation;
  return TURBO_OK;
}

static int tfmp_management_resource_reconcile_supports(void *ctx, uint16_t command_type) {
  (void)ctx;
  return command_type == TURBO_FLOW_TFMP_COMMAND_RESOURCE_QUIESCE;
}

static int
tfmp_management_resource_reconcile_inspect(void *ctx, turbo_flow_t *flow,
                                           const turbo_flow_tfmp_reconcile_request_t *request,
                                           turbo_flow_tfmp_reconcile_result_t *result) {
  tfmp_management_resource_fixture_t *fixture = (tfmp_management_resource_fixture_t *)ctx;
  (void)flow;
  if (!fixture || !request || request->size < sizeof(*request) || !result ||
      result->size < sizeof(*result) ||
      request->command_type != TURBO_FLOW_TFMP_COMMAND_RESOURCE_QUIESCE ||
      request->target_uid_size != strlen(fixture->metadata.uid) ||
      memcmp(request->target_uid, fixture->metadata.uid, request->target_uid_size) != 0 ||
      !request->has_expected_generation)
    return TURBO_EINVAL;
  result->generation = fixture->metadata.generation;
  result->observed_generation = fixture->metadata.observed_generation;
  if (fixture->command_calls > 0 &&
      fixture->last_command.kind == TURBO_FLOW_RESOURCE_COMMAND_QUIESCE &&
      fixture->metadata.generation > request->expected_generation)
    result->outcome = TURBO_FLOW_TFMP_RECONCILE_APPLIED;
  else if (fixture->metadata.generation == request->expected_generation)
    result->outcome = TURBO_FLOW_TFMP_RECONCILE_NOT_APPLIED;
  else result->outcome = TURBO_FLOW_TFMP_RECONCILE_CONFLICT;
  return TURBO_OK;
}

static int tfmp_management_noop_stage(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return TURBO_OK;
}

static turbo_flow_t *tfmp_management_started_flow_from_source(const char *source) {
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || turbo_flow_parse_string(flow, source, strlen(source)) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "sink", tfmp_management_noop_stage, NULL, NULL) !=
          TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK || turbo_flow_start(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *tfmp_management_started_flow(void) {
  return tfmp_management_started_flow_from_source(
      "source input\nstage sink\nstage main {\n  input -> sink\n}\n");
}

static turbo_flow_t *tfmp_management_started_pool_flow(void) {
  return tfmp_management_started_flow_from_source(
      "source input\nstage sink worker 1 capacity 8\nstage main {\n  input -> sink\n}\n");
}

static int tfmp_management_register_resource(turbo_flow_t *flow,
                                             tfmp_management_resource_fixture_t *fixture,
                                             const char *uid, const char *owner,
                                             uint64_t generation, uint64_t observed_generation,
                                             int with_document) {
  turbo_flow_resource_provider_ops_t ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
  fixture->metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  fixture->metadata.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
  fixture->metadata.kind = TURBO_FLOW_RESOURCE_QUEUE_BUFFER;
  memcpy(fixture->metadata.uid, uid, strlen(uid) + 1u);
  memcpy(fixture->metadata.owner_name, owner, strlen(owner) + 1u);
  fixture->metadata.generation = generation;
  fixture->metadata.observed_generation = observed_generation;
  ops.metadata = tfmp_management_resource_metadata;
  ops.document = with_document ? tfmp_management_resource_document : NULL;
  ops.command = tfmp_management_resource_command;
  return turbo_flow_register_resource_provider(flow, owner, &ops, fixture);
}

static int tfmp_management_encode_request(uint16_t kind, uint64_t correlation_id, uint8_t *wire,
                                          size_t capacity, size_t *wire_size) {
  turbo_flow_tfmp_envelope_t request = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  request.kind = kind;
  request.correlation_id = correlation_id;
  return turbo_flow_tfmp_envelope_encode(&request, wire, capacity, wire_size);
}

static int tfmp_management_encode_body_request(uint16_t kind, uint64_t correlation_id,
                                               const uint8_t *body, size_t body_size, uint8_t *wire,
                                               size_t capacity, size_t *wire_size) {
  turbo_flow_tfmp_envelope_t request = TURBO_FLOW_TFMP_ENVELOPE_INIT;
  request.kind = kind;
  request.correlation_id = correlation_id;
  request.body = body;
  request.body_size = body_size;
  return turbo_flow_tfmp_envelope_encode(&request, wire, capacity, wire_size);
}

static int tfmp_management_build_command(uint8_t *body, size_t capacity, size_t *body_size,
                                         const char *client_id, const char *idempotency_key,
                                         const char *target_uid, uint16_t command_type,
                                         uint16_t reply_mode, uint16_t durability,
                                         int has_expected_generation, uint64_t expected_generation,
                                         uint64_t operation_timeout_ms, const uint8_t *payload,
                                         size_t payload_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  int rc = turbo_flow_tfmp_body_builder_init(&builder, body, capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, client_id, strlen(client_id));
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 2u, 1, idempotency_key,
                                                  strlen(idempotency_key));
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 3u, 1, target_uid, strlen(target_uid));
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 4u, 1, command_type);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 5u, 1, reply_mode);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 6u, 1, durability);
  if (rc == TURBO_OK && has_expected_generation)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 7u, 0, expected_generation);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 8u, 1, 0u);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 9u, 1, operation_timeout_ms);
  if (rc == TURBO_OK && payload)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 10u, 0, payload, payload_size);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int tfmp_management_build_flow_command(uint8_t *body, size_t capacity, size_t *body_size,
                                              const char *client_id, const char *idempotency_key,
                                              const char *target_uid, uint16_t command_type,
                                              uint16_t reply_mode, uint16_t durability,
                                              int has_expected_generation,
                                              uint64_t expected_generation,
                                              uint64_t operation_timeout_ms) {
  return tfmp_management_build_command(
      body, capacity, body_size, client_id, idempotency_key, target_uid, command_type, reply_mode,
      durability, has_expected_generation, expected_generation, operation_timeout_ms, NULL, 0u);
}

static int tfmp_management_build_async_flow_command_with_durability(
    uint8_t *body, size_t capacity, size_t *body_size, const char *client_id,
    const char *idempotency_key, const char *target_uid, uint16_t command_type, uint16_t durability,
    uint64_t queue_timeout_ms) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  int rc = turbo_flow_tfmp_body_builder_init(&builder, body, capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, client_id, strlen(client_id));
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 2u, 1, idempotency_key,
                                                  strlen(idempotency_key));
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 3u, 1, target_uid, strlen(target_uid));
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 4u, 1, command_type);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 5u, 1,
                                                 TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 6u, 1, durability);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 8u, 1, queue_timeout_ms);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 9u, 1, UINT64_MAX);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int tfmp_management_build_async_flow_command(uint8_t *body, size_t capacity,
                                                    size_t *body_size, const char *client_id,
                                                    const char *idempotency_key,
                                                    const char *target_uid, uint16_t command_type,
                                                    uint64_t queue_timeout_ms) {
  return tfmp_management_build_async_flow_command_with_durability(
      body, capacity, body_size, client_id, idempotency_key, target_uid, command_type,
      TURBO_FLOW_TFMP_DURABILITY_VOLATILE, queue_timeout_ms);
}

static int tfmp_management_build_durable_flow_command(uint8_t *body, size_t capacity,
                                                      size_t *body_size, const char *client_id,
                                                      const char *idempotency_key,
                                                      const char *target_uid, uint16_t command_type,
                                                      uint64_t queue_timeout_ms) {
  return tfmp_management_build_async_flow_command_with_durability(
      body, capacity, body_size, client_id, idempotency_key, target_uid, command_type,
      TURBO_FLOW_TFMP_DURABILITY_DURABLE, queue_timeout_ms);
}

static int tfmp_management_build_durable_resource_command(
    uint8_t *body, size_t capacity, size_t *body_size, const char *client_id,
    const char *idempotency_key, const char *target_uid, uint16_t command_type,
    uint64_t expected_generation, uint64_t queue_timeout_ms) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  int rc = turbo_flow_tfmp_body_builder_init(&builder, body, capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, client_id, strlen(client_id));
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 2u, 1, idempotency_key,
                                                  strlen(idempotency_key));
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 3u, 1, target_uid, strlen(target_uid));
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 4u, 1, command_type);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 5u, 1,
                                                 TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u16(&builder, 6u, 1,
                                                 TURBO_FLOW_TFMP_DURABILITY_DURABLE);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 7u, 0, expected_generation);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 8u, 1, queue_timeout_ms);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 9u, 1, UINT64_MAX);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int tfmp_management_build_operation_get(const uint8_t operation_id[16], uint8_t *body,
                                               size_t capacity, size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  int rc = turbo_flow_tfmp_body_builder_init(&builder, body, capacity);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append(&builder, 1u, 1, operation_id, 16u);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int tfmp_management_build_operation_cancel(const char *client_id,
                                                  const char *idempotency_key,
                                                  const uint8_t operation_id[16],
                                                  int has_expected_revision,
                                                  uint64_t expected_revision, uint8_t *body,
                                                  size_t capacity, size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  int rc = turbo_flow_tfmp_body_builder_init(&builder, body, capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, client_id, strlen(client_id));
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append_utf8(&builder, 2u, 1, idempotency_key,
                                                  strlen(idempotency_key));
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append(&builder, 3u, 1, operation_id, 16u);
  if (rc == TURBO_OK && has_expected_revision)
    rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 4u, 0, expected_revision);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int tfmp_management_build_events_get(const uint8_t incarnation_id[16],
                                            uint64_t after_sequence, uint32_t page_limit,
                                            uint8_t *body, size_t capacity, size_t *body_size) {
  turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
  int rc = turbo_flow_tfmp_body_builder_init(&builder, body, capacity);
  if (rc == TURBO_OK)
    rc = turbo_flow_tfmp_body_builder_append(&builder, 1u, 1, incarnation_id, 16u);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u64(&builder, 2u, 1, after_sequence);
  if (rc == TURBO_OK) rc = turbo_flow_tfmp_body_builder_append_u32(&builder, 3u, 1, page_limit);
  if (rc == TURBO_OK) *body_size = builder.length;
  return rc;
}

static int tfmp_management_find_body_field(const uint8_t *body, size_t body_size, uint8_t field_id,
                                           size_t ordinal, turbo_flow_tfmp_field_t *out) {
  turbo_flow_tfmp_field_iterator_t iterator = TURBO_FLOW_TFMP_FIELD_ITERATOR_INIT;
  turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
  size_t found = 0u;
  int rc = turbo_flow_tfmp_field_iterator_init(&iterator, body, body_size);
  if (rc != TURBO_OK) return rc;
  while ((rc = turbo_flow_tfmp_field_iterator_next(&iterator, &field)) == TURBO_OK) {
    if (field.id == field_id && found++ == ordinal) {
      *out = field;
      return TURBO_OK;
    }
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
  }
  return rc;
}

static int tfmp_management_find_field(const turbo_flow_tfmp_envelope_t *envelope, uint8_t field_id,
                                      turbo_flow_tfmp_field_t *out) {
  return tfmp_management_find_body_field(envelope->body, envelope->body_size, field_id, 0u, out);
}

static int tfmp_management_create(turbo_flow_tfmp_management_service_t **service) {
  turbo_flow_tfmp_management_config_t config = TURBO_FLOW_TFMP_MANAGEMENT_CONFIG_INIT;
  memcpy(config.authority_id, "authority-test", sizeof("authority-test"));
  return turbo_flow_tfmp_management_service_create(&config, service);
}

static int tfmp_management_create_with_blob_mode(tfmp_management_blob_store_t *blob,
                                                 turbo_flow_blob_store_t *store, int durable_events,
                                                 turbo_flow_tfmp_management_service_t **service) {
  turbo_flow_tfmp_management_channel_config_t config =
      TURBO_FLOW_TFMP_MANAGEMENT_CHANNEL_CONFIG_INIT;
  turbo_flow_tfmp_management_store_binding_t binding =
      TURBO_FLOW_TFMP_MANAGEMENT_STORE_BINDING_INIT;
  if (!blob || !store || !service) return TURBO_EINVAL;
  memcpy(config.service.authority_id, "authority-test", sizeof("authority-test"));
  memcpy(config.operation_store, "blob.test", sizeof("blob.test"));
  if (durable_events) {
    config.service.event_capacity = 16u;
    memcpy(config.event_replay_store, "blob.test", sizeof("blob.test"));
  }
  *store = (turbo_flow_blob_store_t)TURBO_FLOW_BLOB_STORE_INIT;
  store->max_value_size = sizeof(blob->data);
  store->ctx = blob;
  store->load = tfmp_management_blob_load;
  store->commit = tfmp_management_blob_commit;
  binding.store = store;
  binding.key = "fmq:test";
  return turbo_flow_tfmp_management_service_create_configured(&config, &binding, service);
}

static int tfmp_management_create_with_blob(tfmp_management_blob_store_t *blob,
                                            turbo_flow_blob_store_t *store,
                                            turbo_flow_tfmp_management_service_t **service) {
  return tfmp_management_create_with_blob_mode(blob, store, 0, service);
}

static int
tfmp_management_create_with_durable_events(tfmp_management_blob_store_t *blob,
                                           turbo_flow_blob_store_t *store,
                                           turbo_flow_tfmp_management_service_t **service) {
  return tfmp_management_create_with_blob_mode(blob, store, 1, service);
}

spec("fmq_tfmp_management_owner") {
  it("resolves a pipelined ROUTER management channel without backend fallback") {
    static const char valid_yaml[] = "version: 1\n"
                                     "channels:\n"
                                     "  management:\n"
                                     "    kind: fmq_management\n"
                                     "    config:\n"
                                     "      protocol_major: 1\n"
                                     "      protocol_minor: 0\n"
                                     "      authority_id: authority-yaml\n"
                                     "      rpc_adapter: management.router\n"
                                     "      mailbox_capacity: 8\n"
                                     "      max_request_bytes: 4096\n"
                                     "      max_reply_bytes: 8192\n"
                                     "      max_inflight_per_target: 4\n"
                                     "      dedup_capacity: 16\n"
                                     "      dedup_ttl_ms: 30000\n"
                                     "      operation_store: memory\n"
                                     "      event_adapter: management.pub\n"
                                     "      event_capacity: 4\n"
                                     "      event_replay_store: memory\n"
                                     "      shutdown_timeout_ms: 2000\n"
                                     "adapters:\n"
                                     "  management.router:\n"
                                     "    kind: fmq\n"
                                     "    config:\n"
                                     "      pattern: router\n"
                                     "      mode: bind\n"
                                     "      transport: tcp\n"
                                     "      host: 127.0.0.1\n"
                                     "      port: 7799\n"
                                     "  management.pub:\n"
                                     "    kind: fmq\n"
                                     "    config:\n"
                                     "      pattern: pub\n"
                                     "      mode: bind\n"
                                     "      transport: tcp\n"
                                     "      topic_policy: content\n"
                                     "      host: 127.0.0.1\n"
                                     "      port: 7800\n";
    static const char durable_yaml[] = "version: 1\n"
                                       "channels:\n"
                                       "  redis.operations:\n"
                                       "    kind: blob_store\n"
                                       "    config:\n"
                                       "      backend: redis\n"
                                       "      host: 127.0.0.1\n"
                                       "      port: 6379\n"
                                       "      key: fmq:test\n"
                                       "  management:\n"
                                       "    kind: fmq_management\n"
                                       "    config:\n"
                                       "      protocol_major: 1\n"
                                       "      protocol_minor: 0\n"
                                       "      authority_id: authority-yaml\n"
                                       "      rpc_adapter: management.router\n"
                                       "      mailbox_capacity: 8\n"
                                       "      max_request_bytes: 4096\n"
                                       "      max_reply_bytes: 8192\n"
                                       "      max_inflight_per_target: 4\n"
                                       "      dedup_capacity: 16\n"
                                       "      dedup_ttl_ms: 30000\n"
                                       "      operation_store: redis.operations\n"
                                       "      event_capacity: 4\n"
                                       "      event_replay_store: redis.operations\n"
                                       "      shutdown_timeout_ms: 2000\n"
                                       "adapters:\n"
                                       "  management.router:\n"
                                       "    kind: fmq\n"
                                       "    config:\n"
                                       "      pattern: router\n"
                                       "      mode: bind\n"
                                       "      transport: tcp\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_tfmp_management_channel_config_t config =
        TURBO_FLOW_TFMP_MANAGEMENT_CHANNEL_CONFIG_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;

    check_int_eq(
        turbo_flow_config_resolve_yaml(valid_yaml, sizeof(valid_yaml) - 1u, &resolved, &error),
        TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_management_channel_config_resolve(resolved, "management", &config, &error),
        TURBO_OK);
    check_str_eq(config.service.authority_id, "authority-yaml");
    check_str_eq(config.rpc_adapter, "management.router");
    check_uint_eq(config.max_inflight_per_target, 4u);
    check_str_eq(config.operation_store, "memory");
    check_size_eq(config.service.operation_capacity, 8u);
    check_size_eq(config.service.dedup_capacity, 16u);
    check_size_eq(config.service.event_capacity, 4u);
    check_str_eq(config.event_adapter, "management.pub");
    check_int_eq(turbo_flow_tfmp_management_service_create_configured(&config, NULL, &service),
                 TURBO_OK);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;
    turbo_flow_resolved_config_destroy(resolved);
    resolved = NULL;

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    config =
        (turbo_flow_tfmp_management_channel_config_t)TURBO_FLOW_TFMP_MANAGEMENT_CHANNEL_CONFIG_INIT;
    check_int_eq(
        turbo_flow_config_resolve_yaml(durable_yaml, sizeof(durable_yaml) - 1u, &resolved, &error),
        TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_management_channel_config_resolve(resolved, "management", &config, &error),
        TURBO_OK);
    check_str_eq(config.operation_store, "redis.operations");
    check_str_eq(config.event_replay_store, "redis.operations");
    check_size_eq(config.service.event_capacity, 4u);
    check_int_eq(turbo_flow_tfmp_management_service_create_configured(&config, NULL, &service),
                 TURBO_EINVAL);
    check_null(service);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("serves asynchronous TFMP through a CoroNet Pipe DEALER ROUTER graph") {
    static const char server_dsl[] = "source request adapter fmq.router\n"
                                     "stage management\n"
                                     "stage reply adapter fmq.router\n"
                                     "stage main {\n"
                                     "  request -> management -> reply\n"
                                     "}\n";
    static const char client_dsl[] = "source response adapter fmq.dealer\n"
                                     "source input\n"
                                     "stage send adapter fmq.dealer\n"
                                     "stage capture\n"
                                     "stage main {\n"
                                     "  input -> send\n"
                                     "  response -> capture\n"
                                     "}\n";
    turbo_flow_fmq_config_t router = TURBO_FLOW_FMQ_CONFIG_INIT;
    turbo_flow_fmq_config_t dealer = TURBO_FLOW_FMQ_CONFIG_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_t *target = tfmp_management_started_flow();
    turbo_flow_t *server = turbo_flow_create();
    turbo_flow_t *client = turbo_flow_create();
    tfmp_management_capture_t capture;
    char pipe_path[128];
    uint8_t body[512];
    uint8_t request[1024];
    size_t body_size = 0u;
    size_t request_size = 0u;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    check_not_null(target);
    check_not_null(server);
    check_not_null(client);
    check_int_gt(snprintf(pipe_path, sizeof(pipe_path), "pipe://turbo_flow_tfmp_%llu",
                          (unsigned long long)turbo_hrtime()),
                 0);
    router.pattern = TURBO_FLOW_FMQ_ROUTER;
    router.mode = TURBO_FLOW_FMQ_BIND;
    router.transport = TURBO_FLOW_FMQ_PIPE;
    router.path = pipe_path;
    router.timeout_ms = 2000u;
    dealer = router;
    dealer.pattern = TURBO_FLOW_FMQ_DEALER;
    dealer.mode = TURBO_FLOW_FMQ_CONNECT;
    dealer.identity = "tfmp-client-pipe";

    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:pipe", target),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_register_adapter_ex(server, "fmq.router", &router,
                                                    &MANAGEMENT_PRIVATE_EXECUTION),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(server, "management",
                                              turbo_flow_tfmp_management_stage, service, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(server, server_dsl, strlen(server_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(server), TURBO_OK);
    check_int_eq(turbo_flow_fmq_register_adapter_ex(client, "fmq.dealer", &dealer,
                                                    &MANAGEMENT_PRIVATE_EXECUTION),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(client, "capture", tfmp_management_capture, &capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(client, client_dsl, strlen(client_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(client), TURBO_OK);
    check_int_eq(turbo_flow_start(server), TURBO_OK);
    check_int_eq(turbo_flow_start(client), TURBO_OK);

    check_int_eq(tfmp_management_build_flow_command(
                     body, sizeof(body), &body_size, "client-pipe", "pause", "flow:pipe",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE,
                     TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION,
                     TURBO_FLOW_TFMP_DURABILITY_VOLATILE, 0, 0u, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 150u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(tfmp_management_publish(client, request, request_size), TURBO_OK);
    for (int i = 0; i < 400 && atomic_load_explicit(&capture.called, memory_order_acquire) == 0;
         ++i)
      turbo_sleep_ms(5u);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_int_eq(turbo_flow_tfmp_envelope_decode(capture.payload, capture.payload_size, &response),
                 TURBO_OK);
    check_uint_eq(response.correlation_id, 150u);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_VOLATILE);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_OK);

    check_int_eq(tfmp_management_build_flow_command(
                     body, sizeof(body), &body_size, "client-pipe", "sync-resume", "flow:pipe",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME,
                     TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL,
                     TURBO_FLOW_TFMP_DURABILITY_VOLATILE, 0, 0u, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 151u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(tfmp_management_publish(client, request, request_size), TURBO_OK);
    for (int i = 0; i < 400 && atomic_load_explicit(&capture.called, memory_order_acquire) < 2; ++i)
      turbo_sleep_ms(5u);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 2);
    check_int_eq(turbo_flow_tfmp_envelope_decode(capture.payload, capture.payload_size, &response),
                 TURBO_OK);
    check_uint_eq(response.correlation_id, 151u);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_FAILED);

    check_int_eq(turbo_flow_stop(client), TURBO_OK);
    check_int_eq(turbo_flow_stop(server), TURBO_OK);
    turbo_flow_destroy(client);
    turbo_flow_destroy(server);
    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(target), TURBO_OK);
    turbo_flow_destroy(target);
  }

  it("derives bounded live events and detects replay gaps by incarnation sequence") {
    turbo_flow_tfmp_management_config_t config = TURBO_FLOW_TFMP_MANAGEMENT_CONFIG_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t event = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_t *flow = tfmp_management_started_flow();
    char topic[64];
    uint8_t incarnation_id[16];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t wire[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t wire_size = 0u;
    size_t topic_size = 0u;
    uint64_t last_sequence = 0u;

    check_not_null(flow);
    memcpy(config.authority_id, "authority-events", sizeof("authority-events"));
    config.event_capacity = 2u;
    check_int_eq(turbo_flow_tfmp_management_service_create(&config, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:events", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_async_flow_command(
                     body, sizeof(body), &body_size, "client-events", "pause", "flow:events",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 160u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, wire,
                                                            sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_OK);

    check_int_eq(turbo_flow_tfmp_management_event_next(service, 0u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_ERANGE);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 2u, NULL, 0u, &topic_size, NULL, 0u,
                                                       &wire_size),
                 TURBO_ENOSPC);
    check_size_gt(topic_size, 0u);
    check_size_gt(wire_size, TURBO_FLOW_TFMP_HEADER_SIZE);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 2u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    check_str_eq(topic, "tfmp/1/event/operation");
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &event), TURBO_OK);
    check_int_eq(event.kind, TURBO_FLOW_TFMP_EVENT);
    check_true((event.flags & TURBO_FLOW_TFMP_FLAG_EVENT) != 0u);
    check_uint_eq(event.correlation_id, 3u);
    check_int_eq(tfmp_management_find_field(&event, 2u, &field), TURBO_OK);
    memcpy(incarnation_id, field.value, sizeof(incarnation_id));
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 3u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    check_size_gt(wire_size, 0u);
    event = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &event), TURBO_OK);
    check_uint_eq(event.correlation_id, 4u);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 4u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_ENOENT);

    check_int_eq(
        tfmp_management_build_events_get(incarnation_id, 2u, 8u, body, sizeof(body), &body_size),
        TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_EVENTS_GET, 161u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, wire,
                                                            sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(tfmp_management_find_body_field(response.body, response.body_size, 1u, 0u, &field),
                 TURBO_OK);
    check_int_eq(tfmp_management_find_body_field(response.body, response.body_size, 1u, 1u, &field),
                 TURBO_OK);
    check_int_eq(tfmp_management_find_body_field(response.body, response.body_size, 1u, 2u, &field),
                 TURBO_ENOENT);
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&field, &last_sequence), TURBO_OK);
    check_uint_eq(last_sequence, 4u);

    check_int_eq(
        tfmp_management_build_events_get(incarnation_id, 0u, 8u, body, sizeof(body), &body_size),
        TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_EVENTS_GET, 162u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, wire,
                                                            sizeof(wire), &wire_size),
                 TURBO_OK);
    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_STALE_CURSOR);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("persists one atomic operation and event outbox across owner restart") {
    tfmp_management_blob_store_t blob;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t event = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t incarnation_id[16];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t wire[2048];
    char topic[64];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t wire_size = 0u;
    size_t topic_size = 0u;

    memset(&blob, 0, sizeof(blob));
    check_not_null(flow);
    check_int_eq(tfmp_management_create_with_durable_events(&blob, &store, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:outbox", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 0u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &event), TURBO_OK);
    check_uint_eq(event.correlation_id, 1u);
    check_int_eq(tfmp_management_find_field(&event, 2u, &field), TURBO_OK);
    memcpy(incarnation_id, field.value, sizeof(incarnation_id));

    check_int_eq(tfmp_management_build_durable_flow_command(
                     body, sizeof(body), &body_size, "client-outbox", "pause", "flow:outbox",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 165u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, wire,
                                                            sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &response), TURBO_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_DURABLE);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_OK);
    check_int_eq(blob.commit_calls, 4);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 3u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &event), TURBO_OK);
    check_uint_eq(event.correlation_id, 4u);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;

    check_int_eq(tfmp_management_create_with_durable_events(&blob, &store, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 3u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    event = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &event), TURBO_OK);
    check_uint_eq(event.correlation_id, 4u);
    check_int_eq(tfmp_management_find_field(&event, 2u, &field), TURBO_OK);
    check_mem_eq(field.value, incarnation_id, sizeof(incarnation_id));
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:outbox", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 4u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &event), TURBO_OK);
    check_uint_eq(event.correlation_id, 5u);
    check_int_eq(blob.commit_calls, 5);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;

    --blob.data_size;
    check_int_eq(tfmp_management_create_with_durable_events(&blob, &store, &service), TURBO_EPROTO);
    check_null(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("rolls back an owner transition when the durable event outbox commit fails") {
    tfmp_management_blob_store_t blob;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t wire[2048];
    char topic[64];
    size_t wire_size = 0u;
    size_t topic_size = 0u;

    memset(&blob, 0, sizeof(blob));
    blob.fail_commit = 1;
    check_not_null(flow);
    check_int_eq(tfmp_management_create_with_durable_events(&blob, &store, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:outbox-fail", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_EIO);
    check_int_eq(turbo_flow_tfmp_management_service_state(service), TURBO_FLOW_TFMP_OWNER_STARTING);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 0u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_ENOENT);
    check_size_eq(blob.data_size, 0u);
    blob.fail_commit = 0;
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 0u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects split operation and event stores because they cannot form one outbox") {
    tfmp_management_blob_store_t blob;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    turbo_flow_tfmp_management_channel_config_t config =
        TURBO_FLOW_TFMP_MANAGEMENT_CHANNEL_CONFIG_INIT;
    turbo_flow_tfmp_management_store_binding_t binding =
        TURBO_FLOW_TFMP_MANAGEMENT_STORE_BINDING_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;

    memset(&blob, 0, sizeof(blob));
    memcpy(config.service.authority_id, "authority-test", sizeof("authority-test"));
    config.service.event_capacity = 4u;
    memcpy(config.operation_store, "blob.operations", sizeof("blob.operations"));
    memcpy(config.event_replay_store, "blob.events", sizeof("blob.events"));
    store.max_value_size = sizeof(blob.data);
    store.ctx = &blob;
    store.load = tfmp_management_blob_load;
    store.commit = tfmp_management_blob_commit;
    binding.store = &store;
    binding.key = "fmq:test";
    check_int_eq(turbo_flow_tfmp_management_service_create_configured(&config, &binding, &service),
                 TURBO_EINVAL);
    check_null(service);
  }

  it("serves honest read-only capabilities with stable identity") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    uint8_t request[64];
    uint8_t reply[TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE];
    size_t request_size = 0u;
    size_t reply_size = 0u;

    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_not_null(service);
    check_int_eq(tfmp_management_encode_request(TURBO_FLOW_TFMP_CAPABILITIES_GET, 41u, request,
                                                sizeof(request), &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&response), TURBO_OK);
    check_int_eq(response.kind, TURBO_FLOW_TFMP_CAPABILITIES_GET);
    check_int_eq(response.correlation_id, 41u);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_COMPLETED);
    check_int_eq(tfmp_management_find_field(&response, 3u, &field), TURBO_OK);
    check_size_eq(field.value_size, 0u);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 4u, &field), TURBO_ENOENT);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 120u, &field), TURBO_OK);
    check_size_eq(field.value_size, strlen("authority-test"));
    check_mem_eq(field.value, "authority-test", field.value_size);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 121u, &field), TURBO_OK);
    check_size_eq(field.value_size, 16u);
    turbo_flow_tfmp_management_service_destroy(service);
  }

  it("reports explicit owner lifecycle through health") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    uint8_t request[64];
    uint8_t reply[512];
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint16_t owner_state = 0u;

    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_state(service), TURBO_FLOW_TFMP_OWNER_STARTING);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_STARTING),
        TURBO_EBUSY);
    check_int_eq(tfmp_management_encode_request(TURBO_FLOW_TFMP_HEALTH_GET, 7u, request,
                                                sizeof(request), &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&response), TURBO_OK);
    check_int_eq(tfmp_management_find_field(&response, 1u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &owner_state), TURBO_OK);
    check_int_eq(owner_state, TURBO_FLOW_TFMP_OWNER_READY);
    check_int_eq(
        turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_DRAINING),
        TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_STOPPED),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_EBUSY);
    turbo_flow_tfmp_management_service_destroy(service);
  }

  it("returns terminal protocol errors and remains usable") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    uint8_t request[64];
    uint8_t reply[512];
    size_t request_size = 0u;
    size_t reply_size = 0u;

    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, NULL, 0u, reply, sizeof(reply),
                                                            &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&response), TURBO_OK);
    check_int_eq(response.kind, TURBO_FLOW_TFMP_PROTOCOL_ERROR);
    check_int_eq(response.correlation_id, 0u);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_FAILED);

    check_int_eq(tfmp_management_encode_request(TURBO_FLOW_TFMP_HEALTH_GET, 8u, request,
                                                sizeof(request), &request_size),
                 TURBO_OK);
    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(response.correlation_id, 8u);
    turbo_flow_tfmp_management_service_destroy(service);
  }

  it("distinguishes unsupported versions and capabilities") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    uint8_t request[64];
    uint8_t reply[512];
    size_t request_size = 0u;
    size_t reply_size = 0u;

    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(
        tfmp_management_encode_request(0x7777u, 20u, request, sizeof(request), &request_size),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&response), TURBO_OK);
    check_int_eq(response.kind, 0x7777u);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY);

    check_int_eq(tfmp_management_encode_request(TURBO_FLOW_TFMP_HEALTH_GET, 21u, request,
                                                sizeof(request), &request_size),
                 TURBO_OK);
    request[4] = 0u;
    request[5] = 2u;
    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.kind, TURBO_FLOW_TFMP_HEALTH_GET);
    check_int_eq(response.correlation_id, 21u);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_VERSION);
    turbo_flow_tfmp_management_service_destroy(service);
  }

  it("reports required reply capacity without partial output") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    uint8_t request[64];
    uint8_t reply[16];
    uint8_t unchanged[sizeof(reply)];
    size_t request_size = 0u;
    size_t reply_size = 0u;

    memset(reply, 0xa5, sizeof(reply));
    memcpy(unchanged, reply, sizeof(reply));
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(tfmp_management_encode_request(TURBO_FLOW_TFMP_CAPABILITIES_GET, 30u, request,
                                                sizeof(request), &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_ENOSPC);
    check_size_gt(reply_size, sizeof(reply));
    check_mem_eq(reply, unchanged, sizeof(reply));
    turbo_flow_tfmp_management_service_destroy(service);
  }

  it("binds one target and serves stable target query values") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_tfmp_field_t nested = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    uint8_t body[128];
    uint8_t request[256];
    uint8_t reply[1024];
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint16_t state = 0u;

    check_not_null(flow);
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:test", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:other", flow),
                 TURBO_EALREADY);

    check_int_eq(tfmp_management_encode_request(TURBO_FLOW_TFMP_CAPABILITIES_GET, 50u, request,
                                                sizeof(request), &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(tfmp_management_find_field(&response, 3u, &field), TURBO_OK);
    check_size_eq(field.value_size, 10u);
    check_mem_eq(field.value, "\0\1\0\2\0\3\0\5\0\7", 10u);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_body_field(response.body, response.body_size, 4u, 2u, &field),
                 TURBO_OK);
    nested = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 1u, 0u, &nested),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&nested, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_COMMAND_FLOW_DRAIN);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_body_field(response.body, response.body_size, 4u, 6u, &field),
                 TURBO_OK);
    nested = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 1u, 0u, &nested),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&nested, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE);
    nested = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 4u, 0u, &nested),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&nested, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_COMMAND_SCHEMA_POOL);
    nested = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 5u, 0u, &nested),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&nested, &state), TURBO_OK);
    check_int_eq(state, 1u);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_body_field(response.body, response.body_size, 4u, 7u, &field),
                 TURBO_ENOENT);

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u32(&builder, 1u, 1, 1u), TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_TARGET_LIST, 51u, body,
                                                     builder.length, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    nested = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 1u, 0u, &nested),
                 TURBO_OK);
    check_mem_eq(nested.value, "flow:test", nested.value_size);

    builder = (turbo_flow_tfmp_body_builder_t)TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, "flow:test", strlen("flow:test")),
        TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_TARGET_GET, 52u, body,
                                                     builder.length, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 1u, &field), TURBO_OK);
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 1u, 0u, &nested),
                 TURBO_OK);
    check_size_eq(nested.value_size, strlen("flow:test"));
    check_mem_eq(nested.value, "flow:test", nested.value_size);
    nested = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 5u, 0u, &nested),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&nested, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_TARGET_STATE_NEW);

    builder = (turbo_flow_tfmp_body_builder_t)TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, "flow:missing",
                                                          strlen("flow:missing")),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_TARGET_GET, 53u, body,
                                                     builder.length, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_NOT_FOUND);

    turbo_flow_tfmp_management_service_destroy(service);
    turbo_flow_destroy(flow);
  }

  it("paginates resource snapshots by UID and detects stale cursors") {
    turbo_flow_tfmp_management_config_t config = TURBO_FLOW_TFMP_MANAGEMENT_CONFIG_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_tfmp_field_t nested = TURBO_FLOW_TFMP_FIELD_INIT;
    tfmp_management_resource_fixture_t first = {0};
    tfmp_management_resource_fixture_t second = {0};
    tfmp_management_resource_fixture_t inserted = {0};
    turbo_flow_t *flow = turbo_flow_create();
    uint8_t body[256];
    uint8_t request[512];
    uint8_t reply[2048];
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint64_t catalog_generation = 0u;

    check_not_null(flow);
    check_int_eq(
        tfmp_management_register_resource(flow, &first, "z-resource", "z-owner", 1u, 1u, 0),
        TURBO_OK);
    check_int_eq(
        tfmp_management_register_resource(flow, &second, "a-resource", "a-owner", 2u, 2u, 0),
        TURBO_OK);
    config.max_page_items = 1u;
    check_int_eq(turbo_flow_tfmp_management_service_create(&config, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:page", flow),
                 TURBO_OK);

    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, "flow:page", strlen("flow:page")),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u32(&builder, 2u, 1, 1u), TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_RESOURCE_LIST, 60u, body,
                                                     builder.length, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(tfmp_management_find_field(&response, 1u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&field, &catalog_generation), TURBO_OK);
    check_int_eq(catalog_generation, 1u);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 1u, 0u, &nested),
                 TURBO_OK);
    check_size_eq(nested.value_size, strlen("a-resource"));
    check_mem_eq(nested.value, "a-resource", nested.value_size);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 3u, &field), TURBO_OK);
    check_mem_eq(field.value, "a-resource", field.value_size);

    builder = (turbo_flow_tfmp_body_builder_t)TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, "flow:page", strlen("flow:page")),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u32(&builder, 2u, 1, 1u), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_utf8(&builder, 3u, 0, "a-resource",
                                                          strlen("a-resource")),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u64(&builder, 4u, 0, catalog_generation),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_RESOURCE_LIST, 61u, body,
                                                     builder.length, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    nested = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 1u, 0u, &nested),
                 TURBO_OK);
    check_mem_eq(nested.value, "z-resource", nested.value_size);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 3u, &field), TURBO_ENOENT);

    check_int_eq(
        tfmp_management_register_resource(flow, &inserted, "m-resource", "m-owner", 1u, 1u, 0),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_STALE_CURSOR);

    builder = (turbo_flow_tfmp_body_builder_t)TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, "flow:page", strlen("flow:page")),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u32(&builder, 2u, 1, 0u), TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_RESOURCE_LIST, 62u, body,
                                                     builder.length, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT);

    turbo_flow_tfmp_management_service_destroy(service);
    turbo_flow_destroy(flow);
  }

  it("returns resource convergence metadata and an owned status document") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_body_builder_t builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_tfmp_field_t nested = TURBO_FLOW_TFMP_FIELD_INIT;
    tfmp_management_resource_fixture_t fixture = {0};
    turbo_flow_t *flow = turbo_flow_create();
    const char payload[] = "{\"ready\":false}";
    uint8_t body[256];
    uint8_t request[512];
    uint8_t reply[2048];
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint16_t value = 0u;
    uint64_t generation = 0u;

    check_not_null(flow);
    fixture.payload = payload;
    check_int_eq(
        tfmp_management_register_resource(flow, &fixture, "queue:orders", "orders", 7u, 6u, 1),
        TURBO_OK);
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:document", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_init(&builder, body, sizeof(body)), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_utf8(&builder, 1u, 1, "flow:document",
                                                          strlen("flow:document")),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_utf8(&builder, 2u, 1, "queue:orders",
                                                          strlen("queue:orders")),
                 TURBO_OK);

    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_RESOURCE_GET, 70u, body,
                                                     builder.length, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(tfmp_management_find_field(&response, 1u, &field), TURBO_OK);
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 3u, 0u, &nested),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&nested, &generation), TURBO_OK);
    check_int_eq(generation, 7u);
    nested = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_body_field(field.value, field.value_size, 5u, 0u, &nested),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&nested, &value), TURBO_OK);
    check_int_eq(value, TURBO_FLOW_TFMP_RESOURCE_STATE_RECONCILING);

    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_RESOURCE_DOCUMENT_GET, 71u,
                                                     body, builder.length, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &value), TURBO_OK);
    check_int_eq(value, 99u);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 4u, &field), TURBO_OK);
    check_size_eq(field.value_size, sizeof(payload) - 1u);
    check_mem_eq(field.value, payload, field.value_size);
    check_int_eq(fixture.document_calls, 1);

    turbo_flow_tfmp_management_service_destroy(service);
    turbo_flow_destroy(flow);
  }
}

spec("fmq_tfmp_management_resource_commands") {
  it("executes a generation-checked Flow command to a terminal result") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_runtime_snapshot_t snapshot;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint64_t generation = 0u;

    check_not_null(flow);
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:command", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);

    check_int_eq(tfmp_management_build_flow_command(
                     body, sizeof(body), &body_size, "client-a", "stale", "flow:command",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL,
                     TURBO_FLOW_TFMP_DURABILITY_VOLATILE, 1, 99u, 0u),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 80u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_CONFLICT);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_true(snapshot.accepting_publishes);

    check_int_eq(tfmp_management_build_flow_command(
                     body, sizeof(body), &body_size, "client-a", "pause", "flow:command",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL,
                     TURBO_FLOW_TFMP_DURABILITY_VOLATILE, 1, 1u, 0u),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 81u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_validate_schema(&response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_COMPLETED);
    check_int_eq(tfmp_management_find_field(&response, 1u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&field, &generation), TURBO_OK);
    check_int_eq(generation, 1u);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&field, &generation), TURBO_OK);
    check_int_eq(generation, 2u);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_false(snapshot.accepting_publishes);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("replays exact command bytes and rejects divergent idempotency reuse") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_runtime_snapshot_t snapshot;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;

    check_not_null(flow);
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:dedup", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_flow_command(
                     body, sizeof(body), &body_size, "client-a", "same-key", "flow:dedup",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL,
                     TURBO_FLOW_TFMP_DURABILITY_VOLATILE, 0, 0u, 0u),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 90u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_false((response.flags & TURBO_FLOW_TFMP_FLAG_REPLAYED) != 0u);

    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 91u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_true((response.flags & TURBO_FLOW_TFMP_FLAG_REPLAYED) != 0u);

    check_int_eq(tfmp_management_build_flow_command(
                     body, sizeof(body), &body_size, "client-a", "same-key", "flow:dedup",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME, TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL,
                     TURBO_FLOW_TFMP_DURABILITY_VOLATILE, 0, 0u, 0u),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 92u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_CONFLICT);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_false(snapshot.accepting_publishes);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("adapts a generation-checked resource command without duplicating resource state") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    tfmp_management_resource_fixture_t fixture = {0};
    turbo_flow_t *flow = turbo_flow_create();
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint64_t generation = 0u;

    check_not_null(flow);
    check_int_eq(tfmp_management_register_resource(flow, &fixture, "connection:resource",
                                                   "resource", 3u, 3u, 0),
                 TURBO_OK);
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:resources", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_command(
                     body, sizeof(body), &body_size, "client-resource", "quiesce-stale",
                     "connection:resource", TURBO_FLOW_TFMP_COMMAND_RESOURCE_QUIESCE,
                     TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL, TURBO_FLOW_TFMP_DURABILITY_VOLATILE,
                     1, 2u, UINT64_MAX, NULL, 0u),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 94u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_CONFLICT);
    check_int_eq(fixture.command_calls, 0);

    check_int_eq(tfmp_management_build_command(
                     body, sizeof(body), &body_size, "client-resource", "quiesce",
                     "connection:resource", TURBO_FLOW_TFMP_COMMAND_RESOURCE_QUIESCE,
                     TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL, TURBO_FLOW_TFMP_DURABILITY_VOLATILE,
                     1, 3u, UINT64_MAX, NULL, 0u),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 95u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(fixture.command_calls, 1);
    check_int_eq(fixture.last_command.kind, TURBO_FLOW_RESOURCE_COMMAND_QUIESCE);
    check_str_ne(fixture.last_command.idempotency_key, "quiesce");
    check_int_eq(tfmp_management_find_field(&response, 1u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&field, &generation), TURBO_OK);
    check_size_eq(generation, 3u);
    field = (turbo_flow_tfmp_field_t)TURBO_FLOW_TFMP_FIELD_INIT;
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&field, &generation), TURBO_OK);
    check_size_eq(generation, 4u);

    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 96u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_true((response.flags & TURBO_FLOW_TFMP_FLAG_REPLAYED) != 0u);
    check_int_eq(fixture.command_calls, 1);

    turbo_flow_tfmp_management_service_destroy(service);
    turbo_flow_destroy(flow);
  }

  it("decodes endpoint replacement payload into the typed resource owner command") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_body_builder_t payload_builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    tfmp_management_resource_fixture_t fixture = {0};
    turbo_flow_t *flow = turbo_flow_create();
    uint8_t payload[128];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;

    check_not_null(flow);
    check_int_eq(tfmp_management_register_resource(flow, &fixture, "connection:endpoint",
                                                   "endpoint", 5u, 5u, 0),
                 TURBO_OK);
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:endpoint", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_init(&payload_builder, payload, sizeof(payload)),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_utf8(&payload_builder, 1u, 0, "127.0.0.2",
                                                          strlen("127.0.0.2")),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&payload_builder, 3u, 0, 7001u), TURBO_OK);
    check_int_eq(tfmp_management_build_command(
                     body, sizeof(body), &body_size, "client-resource", "replace-endpoint",
                     "connection:endpoint", TURBO_FLOW_TFMP_COMMAND_ENDPOINT_REPLACE,
                     TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL, TURBO_FLOW_TFMP_DURABILITY_VOLATILE,
                     1, 5u, UINT64_MAX, payload, payload_builder.length),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 97u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(fixture.command_calls, 1);
    check_int_eq(fixture.last_command.kind, TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT);
    check_str_eq(fixture.last_command.endpoint_host, "127.0.0.2");
    check_int_eq(fixture.last_command.endpoint_port, 7001);

    turbo_flow_tfmp_management_service_destroy(service);
    turbo_flow_destroy(flow);
  }

  it("validates the stable pool kind and resizes the addressed pool resource") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_body_builder_t payload_builder = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    turbo_flow_pool_resource_status_t pool_status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
    turbo_flow_pool_snapshot_t pool_snapshot;
    turbo_flow_t *flow = tfmp_management_started_pool_flow();
    uint8_t payload[64];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;

    check_not_null(flow);
    check_int_eq(turbo_flow_pool_resource_status_at(flow, 0u, &pool_status), TURBO_OK);
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:pools", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_init(&payload_builder, payload, sizeof(payload)),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&payload_builder, 1u, 1,
                                                         TURBO_FLOW_TFMP_POOL_KIND_THREAD),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u32(&payload_builder, 2u, 1, 2u), TURBO_OK);
    check_int_eq(tfmp_management_build_command(
                     body, sizeof(body), &body_size, "client-resource", "resize-wrong-kind",
                     pool_status.uid, TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE,
                     TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL, TURBO_FLOW_TFMP_DURABILITY_VOLATILE,
                     1, pool_status.generation, 1000u, payload, payload_builder.length),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 98u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT);
    check_int_eq(turbo_flow_pool_snapshot_at(flow, 0u, &pool_snapshot), TURBO_OK);
    check_uint_eq(pool_snapshot.parallelism, 1u);

    payload_builder = (turbo_flow_tfmp_body_builder_t)TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
    check_int_eq(turbo_flow_tfmp_body_builder_init(&payload_builder, payload, sizeof(payload)),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u16(&payload_builder, 1u, 1,
                                                         TURBO_FLOW_TFMP_POOL_KIND_DISRUPTOR),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_body_builder_append_u32(&payload_builder, 2u, 1, 2u), TURBO_OK);
    check_int_eq(tfmp_management_build_command(
                     body, sizeof(body), &body_size, "client-resource", "resize-pool",
                     pool_status.uid, TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE,
                     TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL, TURBO_FLOW_TFMP_DURABILITY_VOLATILE,
                     1, pool_status.generation, 1000u, payload, payload_builder.length),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 99u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(turbo_flow_pool_snapshot_at(flow, 0u, &pool_snapshot), TURBO_OK);
    check_uint_eq(pool_snapshot.parallelism, 2u);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }
}

spec("fmq_tfmp_management_operations") {
  it("accepts a volatile operation before mutation and publishes its terminal snapshot") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_runtime_snapshot_t snapshot;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t operation_id[16];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint64_t revision = 0u;
    uint16_t state = 0u;

    check_not_null(flow);
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:async", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_async_flow_command(
                     body, sizeof(body), &body_size, "client-async", "pause", "flow:async",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 120u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_VOLATILE);
    check_int_eq(tfmp_management_find_field(&response, 4u, &field), TURBO_OK);
    check_size_eq(field.value_size, sizeof(operation_id));
    memcpy(operation_id, field.value, sizeof(operation_id));
    check_int_eq(tfmp_management_find_field(&response, 5u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&field, &revision), TURBO_OK);
    check_uint_eq(revision, 1u);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_true(snapshot.accepting_publishes);

    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_VOLATILE);
    check_true((response.flags & TURBO_FLOW_TFMP_FLAG_REPLAYED) != 0u);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_OK);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_false(snapshot.accepting_publishes);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_ENOENT);

    check_int_eq(tfmp_management_build_operation_get(operation_id, body, sizeof(body), &body_size),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_OPERATION_GET, 121u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_COMPLETED);
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_OPERATION_SUCCEEDED);
    check_int_eq(tfmp_management_find_field(&response, 3u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&field, &revision), TURBO_OK);
    check_uint_eq(revision, 3u);
    check_int_eq(tfmp_management_find_field(&response, 10u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_STATUS_OK);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("commits a durable accept before mutation and restores it across owner restart") {
    tfmp_management_blob_store_t blob;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_runtime_snapshot_t snapshot;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t operation_id[16];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint16_t state = 0u;

    memset(&blob, 0, sizeof(blob));
    check_not_null(flow);
    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:durable", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_durable_flow_command(
                     body, sizeof(body), &body_size, "client-durable", "pause", "flow:durable",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 170u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_DURABLE);
    check_int_eq(tfmp_management_find_field(&response, 4u, &field), TURBO_OK);
    memcpy(operation_id, field.value, sizeof(operation_id));
    check_int_eq(blob.commit_calls, 1);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_true(snapshot.accepting_publishes);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;

    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:durable", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_DURABLE);
    check_true((response.flags & TURBO_FLOW_TFMP_FLAG_REPLAYED) != 0u);
    check_int_eq(blob.commit_calls, 1);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_OK);
    check_int_eq(blob.commit_calls, 3);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_false(snapshot.accepting_publishes);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;

    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:durable", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_operation_get(operation_id, body, sizeof(body), &body_size),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_OPERATION_GET, 171u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_OPERATION_SUCCEEDED);

    blob.fail_commit = 1;
    check_int_eq(tfmp_management_build_durable_flow_command(
                     body, sizeof(body), &body_size, "client-durable", "resume", "flow:durable",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 172u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_UNAVAILABLE);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_FAILED);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_ENOENT);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_false(snapshot.accepting_publishes);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;

    blob.fail_commit = 0;
    blob.data[0] ^= 0xffu;
    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_EPROTO);
    check_null(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("reconciles a durable operation whose post-mutation outcome was not committed") {
    tfmp_management_blob_store_t blob;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_runtime_snapshot_t snapshot;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t operation_id[16];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint64_t revision = 0u;
    uint16_t value = 0u;

    memset(&blob, 0, sizeof(blob));
    check_not_null(flow);
    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:uncertain", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_durable_flow_command(
                     body, sizeof(body), &body_size, "client-uncertain", "pause", "flow:uncertain",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 180u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_DURABLE);
    check_int_eq(tfmp_management_find_field(&response, 4u, &field), TURBO_OK);
    memcpy(operation_id, field.value, sizeof(operation_id));
    check_int_eq(blob.commit_calls, 1);

    blob.fail_commit_at = 3;
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_EIO);
    check_int_eq(blob.commit_calls, 3);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_false(snapshot.accepting_publishes);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;

    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(blob.commit_calls, 3);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:uncertain", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_EBUSY);
    check_int_eq(turbo_flow_tfmp_management_service_reconcile_one(service), TURBO_OK);
    check_int_eq(blob.commit_calls, 4);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_operation_get(operation_id, body, sizeof(body), &body_size),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_OPERATION_GET, 181u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &value), TURBO_OK);
    check_int_eq(value, TURBO_FLOW_TFMP_OPERATION_SUCCEEDED);
    check_int_eq(tfmp_management_find_field(&response, 3u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u64(&field, &revision), TURBO_OK);
    check_uint_eq(revision, 3u);
    check_int_eq(tfmp_management_find_field(&response, 10u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &value), TURBO_OK);
    check_int_eq(value, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(turbo_flow_tfmp_management_service_reconcile_one(service), TURBO_ENOENT);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_ENOENT);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_false(snapshot.accepting_publishes);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("requires a typed resource inspector before durable acceptance and recovery") {
    tfmp_management_blob_store_t blob;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_reconcile_binding_t reconciler = TURBO_FLOW_TFMP_RECONCILE_BINDING_INIT;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    tfmp_management_resource_fixture_t fixture = {0};
    turbo_flow_t *flow = turbo_flow_create();
    uint8_t operation_id[16];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint16_t state = 0u;

    memset(&blob, 0, sizeof(blob));
    check_not_null(flow);
    check_int_eq(tfmp_management_register_resource(flow, &fixture, "connection:durable", "durable",
                                                   3u, 3u, 0),
                 TURBO_OK);
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
    check_int_eq(tfmp_management_build_durable_resource_command(
                     body, sizeof(body), &body_size, "client-resource-durable", "quiesce",
                     "connection:durable", TURBO_FLOW_TFMP_COMMAND_RESOURCE_QUIESCE, 3u,
                     UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 185u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);

    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_management_service_bind_target(service, "flow:resource-durable", flow),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY);
    check_int_eq(blob.commit_calls, 0);
    check_int_eq(fixture.command_calls, 0);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;

    reconciler.supports = tfmp_management_resource_reconcile_supports;
    reconciler.inspect = tfmp_management_resource_reconcile_inspect;
    reconciler.ctx = &fixture;
    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_reconciler(service, &reconciler), TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_management_service_bind_target(service, "flow:resource-durable", flow),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_DURABLE);
    check_int_eq(tfmp_management_find_field(&response, 4u, &field), TURBO_OK);
    memcpy(operation_id, field.value, sizeof(operation_id));
    blob.fail_commit_at = 3;
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_EIO);
    check_int_eq(fixture.command_calls, 1);
    check_uint_eq(fixture.metadata.generation, 4u);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;

    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_reconciler(service, &reconciler), TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_management_service_bind_target(service, "flow:resource-durable", flow),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_EBUSY);
    check_int_eq(turbo_flow_tfmp_management_service_reconcile_one(service), TURBO_OK);
    check_int_eq(fixture.command_calls, 1);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_operation_get(operation_id, body, sizeof(body), &body_size),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_OPERATION_GET, 186u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_OPERATION_SUCCEEDED);

    turbo_flow_tfmp_management_service_destroy(service);
    turbo_flow_destroy(flow);
  }

  it("commits durable cancellation before success and replays it after restart") {
    tfmp_management_blob_store_t blob;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_runtime_snapshot_t snapshot;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t operation_id[16];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint16_t state = 0u;

    memset(&blob, 0, sizeof(blob));
    check_not_null(flow);
    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(
        turbo_flow_tfmp_management_service_bind_target(service, "flow:cancel-durable", flow),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_durable_flow_command(
                     body, sizeof(body), &body_size, "client-cancel-durable", "pause",
                     "flow:cancel-durable", TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 190u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(tfmp_management_find_field(&response, 4u, &field), TURBO_OK);
    memcpy(operation_id, field.value, sizeof(operation_id));
    check_int_eq(blob.commit_calls, 1);

    check_int_eq(tfmp_management_build_operation_cancel("client-cancel-durable", "cancel",
                                                        operation_id, 1, 1u, body, sizeof(body),
                                                        &body_size),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_OPERATION_CANCEL, 191u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    blob.fail_commit_at = 2;
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_UNAVAILABLE);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_FAILED);
    check_int_eq(blob.commit_calls, 2);

    blob.fail_commit_at = 0;
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_OPERATION_CANCELED);
    check_int_eq(blob.commit_calls, 3);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;

    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(blob.commit_calls, 3);
    check_int_eq(
        turbo_flow_tfmp_management_service_bind_target(service, "flow:cancel-durable", flow),
        TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_true((response.flags & TURBO_FLOW_TFMP_FLAG_REPLAYED) != 0u);
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_OPERATION_CANCELED);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_ENOENT);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_true(snapshot.accepting_publishes);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("cancels only an accepted operation with revision and idempotency checks") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_runtime_snapshot_t snapshot;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t operation_id[16];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint16_t state = 0u;

    check_not_null(flow);
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:cancel", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_async_flow_command(
                     body, sizeof(body), &body_size, "client-cancel", "pause", "flow:cancel",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 130u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(tfmp_management_find_field(&response, 4u, &field), TURBO_OK);
    memcpy(operation_id, field.value, sizeof(operation_id));

    check_int_eq(tfmp_management_build_operation_cancel("client-cancel", "cancel", operation_id, 1,
                                                        2u, body, sizeof(body), &body_size),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_OPERATION_CANCEL, 131u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_CONFLICT);

    check_int_eq(tfmp_management_build_operation_cancel("client-cancel", "cancel", operation_id, 1,
                                                        1u, body, sizeof(body), &body_size),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_OPERATION_CANCEL, 132u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &state), TURBO_OK);
    check_int_eq(state, TURBO_FLOW_TFMP_OPERATION_CANCELED);

    response = (turbo_flow_tfmp_envelope_t)TURBO_FLOW_TFMP_ENVELOPE_INIT;
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_true((response.flags & TURBO_FLOW_TFMP_FLAG_REPLAYED) != 0u);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_ENOENT);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_true(snapshot.accepting_publishes);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("expires a queued operation before mutation") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_field_t field = TURBO_FLOW_TFMP_FIELD_INIT;
    turbo_flow_runtime_snapshot_t snapshot;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t operation_id[16];
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;
    uint16_t value = 0u;

    check_not_null(flow);
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:expired", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_async_flow_command(body, sizeof(body), &body_size,
                                                          "client-expired", "pause", "flow:expired",
                                                          TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, 0u),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 140u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(tfmp_management_find_field(&response, 4u, &field), TURBO_OK);
    memcpy(operation_id, field.value, sizeof(operation_id));
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_OK);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_true(snapshot.accepting_publishes);

    check_int_eq(tfmp_management_build_operation_get(operation_id, body, sizeof(body), &body_size),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_OPERATION_GET, 141u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(tfmp_management_find_field(&response, 2u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &value), TURBO_OK);
    check_int_eq(value, TURBO_FLOW_TFMP_OPERATION_FAILED);
    check_int_eq(tfmp_management_find_field(&response, 10u, &field), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_field_read_u16(&field, &value), TURBO_OK);
    check_int_eq(value, TURBO_FLOW_TFMP_STATUS_DEADLINE_EXCEEDED);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects durable command requests without mutating the target") {
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_runtime_snapshot_t snapshot;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;

    check_not_null(flow);
    check_int_eq(tfmp_management_create(&service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:volatile", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_flow_command(
                     body, sizeof(body), &body_size, "client-a", "durable", "flow:volatile",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL,
                     TURBO_FLOW_TFMP_DURABILITY_DURABLE, 0, 0u, 0u),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 100u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_true(snapshot.accepting_publishes);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("fails before mutation when terminal dedup capacity is exhausted") {
    turbo_flow_tfmp_management_config_t config = TURBO_FLOW_TFMP_MANAGEMENT_CONFIG_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_runtime_snapshot_t snapshot;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t reply[2048];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t reply_size = 0u;

    check_not_null(flow);
    memcpy(config.authority_id, "authority-test", sizeof("authority-test"));
    config.dedup_capacity = 1u;
    check_int_eq(turbo_flow_tfmp_management_service_create(&config, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:bounded", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);

    check_int_eq(tfmp_management_build_flow_command(
                     body, sizeof(body), &body_size, "client-a", "pause", "flow:bounded",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL,
                     TURBO_FLOW_TFMP_DURABILITY_VOLATILE, 0, 0u, 0u),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 110u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_OK);

    check_int_eq(tfmp_management_build_flow_command(
                     body, sizeof(body), &body_size, "client-a", "resume", "flow:bounded",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME, TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL,
                     TURBO_FLOW_TFMP_DURABILITY_VOLATILE, 0, 0u, 0u),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 111u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, reply,
                                                            sizeof(reply), &reply_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(reply, reply_size, &response), TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_TFMP_STATUS_RESOURCE_EXHAUSTED);
    check_int_eq(turbo_flow_runtime_snapshot(flow, &snapshot), TURBO_OK);
    check_false(snapshot.accepting_publishes);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }
}

spec("fmq_tfmp_management_store_migration") {
  it("upgrades an operation-only snapshot before enabling durable event replay") {
    tfmp_management_blob_store_t blob;
    turbo_flow_blob_store_t store = TURBO_FLOW_BLOB_STORE_INIT;
    turbo_flow_tfmp_management_service_t *service = NULL;
    turbo_flow_tfmp_envelope_t event = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_tfmp_envelope_t response = TURBO_FLOW_TFMP_ENVELOPE_INIT;
    turbo_flow_t *flow = tfmp_management_started_flow();
    uint8_t body[512];
    uint8_t request[1024];
    uint8_t wire[2048];
    char topic[64];
    size_t body_size = 0u;
    size_t request_size = 0u;
    size_t wire_size = 0u;
    size_t topic_size = 0u;
    int operation_only_commits = 0;

    memset(&blob, 0, sizeof(blob));
    check_not_null(flow);
    check_int_eq(tfmp_management_create_with_blob(&blob, &store, &service), TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:upgrade", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(tfmp_management_build_durable_flow_command(
                     body, sizeof(body), &body_size, "client-upgrade", "pause", "flow:upgrade",
                     TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE, UINT64_MAX),
                 TURBO_OK);
    check_int_eq(tfmp_management_encode_body_request(TURBO_FLOW_TFMP_COMMAND_SUBMIT, 166u, body,
                                                     body_size, request, sizeof(request),
                                                     &request_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_execute(service, request, request_size, wire,
                                                            sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &response), TURBO_OK);
    check_int_eq(response.disposition, TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_DURABLE);
    check_int_eq(turbo_flow_tfmp_management_service_run_one(service), TURBO_OK);
    operation_only_commits = blob.commit_calls;
    check_int_eq(operation_only_commits, 3);
    turbo_flow_tfmp_management_service_destroy(service);
    service = NULL;

    check_int_eq(tfmp_management_create_with_durable_events(&blob, &store, &service), TURBO_OK);
    check_int_eq(blob.commit_calls, operation_only_commits + 1);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 0u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_ENOENT);
    check_int_eq(turbo_flow_tfmp_management_service_bind_target(service, "flow:upgrade", flow),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_service_set_state(service, TURBO_FLOW_TFMP_OWNER_READY),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_management_event_next(service, 0u, topic, sizeof(topic),
                                                       &topic_size, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_tfmp_envelope_decode(wire, wire_size, &event), TURBO_OK);
    check_uint_eq(event.correlation_id, 1u);
    check_int_eq(blob.commit_calls, operation_only_commits + 2);

    turbo_flow_tfmp_management_service_destroy(service);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }
}
