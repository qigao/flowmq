#include "turbo_flow_fmq_broker.h"

#include "fmq_pattern_config.h"
#include "fmt.h"
#include "turbo_error.h"
#include "turbo_flow.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_fmq_tfcw_graph_s {
  turbo_flow_fmq_credit_worker_t *owner;
  tstr_t service;
  tstr_t resource_name;
  tstr_t resource_uid;
  tstr_t owner_name;
  turbo_mutex_t mutex;
  int mutex_initialized;
  int last_status;
  size_t max_inflight;
} flow_fmq_tfcw_graph_t;

static const char FLOW_FMQ_TFCW_STATUS_SCHEMA_TEXT[] = "schema FlowMQCreditResource [id(1), version(1)]; message CreditWorkerStatus { string workers; string available_workers; string inflight; string available_messages; string available_bytes; string grants; string dispatched; string completed; string canceled; string expired_workers; string expired_requests; }";

static const turbo_flow_resource_schema_t FLOW_FMQ_TFCW_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t), TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON, "FlowMQCreditResource", "CreditWorkerStatus", 1u, 1u,
    FLOW_FMQ_TFCW_STATUS_SCHEMA_TEXT};

static int flow_fmq_tfcw_graph_text_valid(const char *value, size_t maximum) {
  size_t length;
  if (!value || !value[0]) return 0;
  length = strlen(value);
  return length <= maximum;
}

static int flow_fmq_tfcw_credit_config_validate(
    const turbo_flow_fmq_credit_worker_config_t *config) {
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_FMQ_CREDIT_WORKER_API_VERSION ||
      config->max_workers == 0u || config->max_workers > TURBO_FLOW_FMQ_BROKER_MAX_WORKERS ||
      config->max_inflight == 0u || config->max_inflight > TURBO_FLOW_FMQ_BROKER_MAX_INFLIGHT ||
      config->worker_lease_ms == 0u || config->max_credit_messages_per_worker == 0u ||
      config->max_credit_bytes_per_worker == 0u || config->max_job_bytes == 0u ||
      config->max_job_bytes > config->max_credit_bytes_per_worker)
    return TURBO_EINVAL;
  if (config->max_credit_messages_per_worker > SIZE_MAX / config->max_workers ||
      config->max_credit_bytes_per_worker > SIZE_MAX / config->max_workers)
    return TURBO_ERANGE;
  if (config->reliability != TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE)
    return config->reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE
               ? TURBO_ENOTSUP
               : TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_fmq_tfcw_view_copy(tstr_v value, char *out, size_t capacity) {
  if (!out || capacity == 0u || !value.data || value.len == 0u || value.len >= capacity)
    return TURBO_EINVAL;
  memcpy(out, value.data, value.len);
  out[value.len] = 0;
  return TURBO_OK;
}

static int flow_fmq_tfcw_view_equals(tstr_v value, const char *text) {
  size_t length = text ? strlen(text) : 0u;
  return value.data && value.len == length && memcmp(value.data, text, length) == 0;
}

static uint64_t flow_fmq_tfcw_now_ms(void) {
  return turbo_hrtime() / UINT64_C(1000000);
}

static void flow_fmq_tfcw_drop(turbo_flow_msg_t *msg) {
  msg->data_decision.dropped = 1;
  msg->data_decision.stage_index = UINT32_MAX;
}

static int flow_fmq_tfcw_route_replace(turbo_flow_msg_t *msg,
                                       const turbo_flow_protocol_route_t *route) {
  turbo_flow_msg_clear_protocol_settlement(msg);
  return turbo_flow_msg_set_protocol_route(msg, route);
}

static int flow_fmq_tfcw_control(flow_fmq_tfcw_graph_t *graph,
                                 const turbo_flow_tfcw_envelope_t *envelope,
                                 const turbo_flow_tfcw_fields_t *fields,
                                 const turbo_flow_protocol_route_t *route) {
  char worker_id[TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX + 1u];
  int rc;
  if (envelope->kind == TURBO_FLOW_TFCW_HEARTBEAT) {
    rc = flow_fmq_tfcw_view_copy(fields->worker_id, worker_id, sizeof(worker_id));
    if (rc != TURBO_OK) return rc;
    return turbo_flow_fmq_credit_worker_heartbeat(graph->owner, worker_id, route,
                                                  flow_fmq_tfcw_now_ms());
  }
  if (envelope->kind != TURBO_FLOW_TFCW_READY &&
      envelope->kind != TURBO_FLOW_TFCW_CREDIT)
    return TURBO_EPROTO;
  if (!flow_fmq_tfcw_view_equals(fields->service, graph->service) ||
      fields->grant_messages > SIZE_MAX || fields->grant_bytes > SIZE_MAX)
    return TURBO_EPROTO;
  rc = flow_fmq_tfcw_view_copy(fields->worker_id, worker_id, sizeof(worker_id));
  if (rc == TURBO_OK) {
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    grant.worker_id = worker_id;
    grant.service = graph->service;
    grant.worker_route = *route;
    grant.sequence = envelope->credit_sequence;
    grant.grant_messages = (size_t)fields->grant_messages;
    grant.grant_bytes = (size_t)fields->grant_bytes;
    grant.now_ms = flow_fmq_tfcw_now_ms();
    rc = turbo_flow_fmq_credit_worker_grant(graph->owner, &grant);
  }
  return rc;
}

static int flow_fmq_tfcw_dispatch(flow_fmq_tfcw_graph_t *graph, turbo_flow_msg_t *msg,
                                  const turbo_flow_tfcw_envelope_t *envelope,
                                  const turbo_flow_protocol_route_t *client_route) {
  turbo_flow_fmq_broker_dispatch_result_t result =
      TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
  int rc = turbo_flow_fmq_credit_worker_dispatch(
      graph->owner, graph->service, envelope->request_id, msg->payload.len, client_route,
      flow_fmq_tfcw_now_ms(), &result);
  if (rc != TURBO_OK) return rc;
  return flow_fmq_tfcw_route_replace(msg, &result.worker_route);
}

static int flow_fmq_tfcw_complete(flow_fmq_tfcw_graph_t *graph, turbo_flow_msg_t *msg,
                                  const turbo_flow_tfcw_envelope_t *envelope,
                                  const turbo_flow_tfcw_fields_t *fields,
                                  const turbo_flow_protocol_route_t *worker_route) {
  turbo_flow_fmq_broker_completion_result_t result =
      TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
  char worker_id[TURBO_FLOW_FMQ_BROKER_WORKER_ID_MAX + 1u];
  int rc = flow_fmq_tfcw_view_copy(fields->worker_id, worker_id, sizeof(worker_id));
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_fmq_credit_worker_complete(graph->owner, worker_id, worker_route,
                                             envelope->request_id, &result);
  if (rc != TURBO_OK) return rc;
  return flow_fmq_tfcw_route_replace(msg, &result.client_route);
}

static int flow_fmq_tfcw_consume(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flow_fmq_tfcw_graph_t *graph = (flow_fmq_tfcw_graph_t *)ctx;
  turbo_flow_tfcw_envelope_t envelope = TURBO_FLOW_TFCW_ENVELOPE_INIT;
  turbo_flow_tfcw_fields_t fields = TURBO_FLOW_TFCW_FIELDS_INIT;
  const turbo_flow_protocol_route_t *route;
  int rc;
  (void)flow;
  if (!graph || !stage || !stage->operation_name || !msg ||
      (msg->payload.len > 0u && !msg->payload.data))
    return TURBO_EINVAL;
  route = turbo_flow_msg_protocol_route(msg);
  if (!route) return TURBO_EINVAL;
  rc = turbo_flow_tfcw_decode((const uint8_t *)msg->payload.data, msg->payload.len, &envelope);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_tfcw_fields_decode(&envelope, &fields);
  if (rc != TURBO_OK) return rc;

  turbo_mutex_lock(&graph->mutex);
  if (strcmp(stage->operation_name, TURBO_FLOW_FMQ_TFCW_CONTROL_OPERATION) == 0) {
    rc = flow_fmq_tfcw_control(graph, &envelope, &fields, route);
    if (rc == TURBO_OK) flow_fmq_tfcw_drop(msg);
  } else if (strcmp(stage->operation_name, TURBO_FLOW_FMQ_TFCW_DISPATCH_OPERATION) == 0) {
    rc = envelope.kind == TURBO_FLOW_TFCW_JOB
             ? flow_fmq_tfcw_dispatch(graph, msg, &envelope, route)
             : TURBO_EPROTO;
  } else if (strcmp(stage->operation_name, TURBO_FLOW_FMQ_TFCW_COMPLETE_OPERATION) == 0) {
    rc = envelope.kind == TURBO_FLOW_TFCW_COMPLETE || envelope.kind == TURBO_FLOW_TFCW_FAIL
             ? flow_fmq_tfcw_complete(graph, msg, &envelope, &fields, route)
             : TURBO_EPROTO;
  } else if (strcmp(stage->operation_name, TURBO_FLOW_FMQ_TFCW_WORKER_INPUT_OPERATION) == 0) {
    switch (envelope.kind) {
    case TURBO_FLOW_TFCW_READY:
    case TURBO_FLOW_TFCW_CREDIT:
    case TURBO_FLOW_TFCW_HEARTBEAT:
      rc = flow_fmq_tfcw_control(graph, &envelope, &fields, route);
      if (rc == TURBO_OK) flow_fmq_tfcw_drop(msg);
      break;
    case TURBO_FLOW_TFCW_COMPLETE:
    case TURBO_FLOW_TFCW_FAIL:
      rc = flow_fmq_tfcw_complete(graph, msg, &envelope, &fields, route);
      break;
    default:
      rc = TURBO_EPROTO;
      break;
    }
  } else {
    rc = TURBO_EPROTO;
  }
  graph->last_status = rc;
  turbo_mutex_unlock(&graph->mutex);
  return rc;
}

static int flow_fmq_tfcw_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_fmq_tfcw_graph_t *graph = (flow_fmq_tfcw_graph_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int written;
  if (!graph || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  metadata.domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
  metadata.kind = TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE;
  metadata.generation = 1u;
  metadata.observed_generation = 1u;
  written = snprintf(metadata.uid, sizeof(metadata.uid), "%s", graph->resource_uid);
  if (written < 0 || (size_t)written >= sizeof(metadata.uid)) return TURBO_ENAMETOOLONG;
  written = snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", graph->owner_name);
  if (written < 0 || (size_t)written >= sizeof(metadata.owner_name)) return TURBO_ENAMETOOLONG;
  *out = metadata;
  return TURBO_OK;
}

static int flow_fmq_tfcw_resource_snapshot(void *ctx, turbo_flow_resource_snapshot_t *out) {
  flow_fmq_tfcw_graph_t *graph = (flow_fmq_tfcw_graph_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_fmq_credit_worker_snapshot_t credit =
      TURBO_FLOW_FMQ_CREDIT_WORKER_SNAPSHOT_INIT;
  int rc;
  if (!graph || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  rc = flow_fmq_tfcw_resource_metadata(graph, &metadata);
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&graph->mutex);
  rc = turbo_flow_fmq_credit_worker_snapshot(graph->owner, &credit);
  if (rc == TURBO_OK) {
    *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    out->domain = metadata.domain;
    out->kind = metadata.kind;
    memcpy(out->uid, metadata.uid, sizeof(out->uid));
    memcpy(out->owner_name, metadata.owner_name, sizeof(out->owner_name));
    out->generation = metadata.generation;
    out->observed_generation = metadata.observed_generation;
    out->load = credit.inflight;
    out->capacity = graph->max_inflight;
    out->saturated = out->load >= out->capacity;
    out->last_status = graph->last_status;
  } else {
    graph->last_status = rc;
  }
  turbo_mutex_unlock(&graph->mutex);
  return rc;
}

static int flow_fmq_tfcw_resource_document(void *ctx,
                                           turbo_flow_resource_document_kind_t document_kind,
                                           turbo_flow_resource_document_t *out) {
  flow_fmq_tfcw_graph_t *graph = (flow_fmq_tfcw_graph_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_fmq_credit_worker_snapshot_t credit =
      TURBO_FLOW_FMQ_CREDIT_WORKER_SNAPSHOT_INIT;
  tstr_t payload = NULL;
  int rc;
  if (!graph || !out || out->size < sizeof(*out) || out->payload) return TURBO_EINVAL;
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  rc = flow_fmq_tfcw_resource_metadata(graph, &metadata);
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&graph->mutex);
  rc = turbo_flow_fmq_credit_worker_snapshot(graph->owner, &credit);
  if (rc != TURBO_OK) graph->last_status = rc;
  turbo_mutex_unlock(&graph->mutex);
  if (rc != TURBO_OK) return rc;
  payload = tstr_format(
      "{\"workers\":\"{}\",\"available_workers\":\"{}\",\"inflight\":\"{}\","
      "\"available_messages\":\"{}\",\"available_bytes\":\"{}\",\"grants\":\"{}\","
      "\"dispatched\":\"{}\",\"completed\":\"{}\",",
      credit.workers, credit.available_workers, credit.inflight, credit.available_messages,
      credit.available_bytes, credit.grants, credit.dispatched, credit.completed);
  if (payload)
    payload = tstr_append_format(
        payload,
        "\"canceled\":\"{}\",\"expired_workers\":\"{}\",\"expired_requests\":\"{}\"}",
        credit.canceled, credit.expired_workers, credit.expired_requests);
  if (!payload) return TURBO_ENOMEM;
  rc = turbo_flow_resource_document_set_payload_copy(
      out, &metadata, &FLOW_FMQ_TFCW_STATUS_SCHEMA, payload, tstr_len(payload));
  tstr_free(payload);
  return rc;
}

static void flow_fmq_tfcw_shutdown(void *ctx) {
  flow_fmq_tfcw_graph_t *graph = (flow_fmq_tfcw_graph_t *)ctx;
  if (!graph) return;
  turbo_flow_fmq_credit_worker_destroy(graph->owner);
  if (graph->mutex_initialized) turbo_mutex_destroy(&graph->mutex);
  tstr_freep(&graph->service);
  tstr_freep(&graph->resource_name);
  tstr_freep(&graph->resource_uid);
  tstr_freep(&graph->owner_name);
  free(graph);
}

static int flow_fmq_tfcw_register_contract(turbo_flow_t *flow) {
  static const char *const primitive_types[] = {TURBO_FLOW_FMQ_TFCW_PRIMITIVE_TYPE};
  static const char *const operation_names[] = {
      TURBO_FLOW_FMQ_TFCW_WORKER_INPUT_OPERATION, TURBO_FLOW_FMQ_TFCW_CONTROL_OPERATION,
      TURBO_FLOW_FMQ_TFCW_DISPATCH_OPERATION, TURBO_FLOW_FMQ_TFCW_COMPLETE_OPERATION};
  turbo_flow_operation_descriptor_t operations[4];
  turbo_flow_module_descriptor_t module;
  memset(operations, 0, sizeof(operations));
  memset(&module, 0, sizeof(module));
  for (size_t i = 0u; i < 4u; ++i) {
    operations[i].size = sizeof(operations[i]);
    operations[i].name = operation_names[i];
    operations[i].version = TURBO_FLOW_FMQ_TFCW_GRAPH_API_VERSION;
    operations[i].domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
    operations[i].input_domain = TURBO_FLOW_DOMAIN_DATA;
    operations[i].input_type = "Message";
    operations[i].output_domain = TURBO_FLOW_DOMAIN_DATA;
    operations[i].output_type = "Message";
    operations[i].resource_domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
    operations[i].resource_type = TURBO_FLOW_FMQ_TFCW_PRIMITIVE_TYPE;
    operations[i].resource_min_version = TURBO_FLOW_FMQ_TFCW_GRAPH_API_VERSION;
    operations[i].resource_max_version = TURBO_FLOW_FMQ_TFCW_GRAPH_API_VERSION;
    operations[i].scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
    operations[i].scope.state = TURBO_FLOW_STATE_SCOPE_RESOURCE_OWNER;
    operations[i].scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    operations[i].scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
    operations[i].scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
    operations[i].flags = TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE;
    operations[i].execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
    operations[i].runtime.handoff = TURBO_FLOW_HANDOFF_DIRECT;
    operations[i].runtime.ordering = TURBO_FLOW_ORDERING_PRESERVE_INPUT;
    operations[i].runtime.backpressure = TURBO_FLOW_BACKPRESSURE_NONE;
    operations[i].runtime.error_mode = TURBO_FLOW_ERROR_PROPAGATE;
  }
  module.size = sizeof(module);
  module.name = TURBO_FLOW_FMQ_TFCW_MODULE;
  module.version = TURBO_FLOW_FMQ_TFCW_GRAPH_API_VERSION;
  module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                            TURBO_FLOW_MODULE_MANAGED_RESOURCES |
                            TURBO_FLOW_MODULE_NATIVE_API;
  module.primitive_types = primitive_types;
  module.primitive_type_count = 1u;
  module.operation_names = operation_names;
  module.operation_count = 4u;
  return turbo_flow_register_module_contract(flow, &module, operations, 4u);
}

int turbo_flow_fmq_tfcw_register_graph(
    turbo_flow_t *flow, const char *adapter_name,
    const turbo_flow_fmq_tfcw_graph_config_t *graph_config,
    const turbo_flow_fmq_credit_worker_config_t *credit_config) {
  static const char *const operation_names[] = {
      TURBO_FLOW_FMQ_TFCW_WORKER_INPUT_OPERATION, TURBO_FLOW_FMQ_TFCW_CONTROL_OPERATION,
      TURBO_FLOW_FMQ_TFCW_DISPATCH_OPERATION, TURBO_FLOW_FMQ_TFCW_COMPLETE_OPERATION};
  flow_fmq_tfcw_graph_t *graph;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_adapter_schema_t schema;
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  turbo_flow_primitive_descriptor_t primitive;
  turbo_flow_module_adapter_registration_t registration =
      TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
  const char *operation_resources[4];
  int rc;
  if (!flow || !adapter_name || !adapter_name[0] || !graph_config ||
      graph_config->size < sizeof(*graph_config) ||
      graph_config->version != TURBO_FLOW_FMQ_TFCW_GRAPH_API_VERSION ||
      !flow_fmq_tfcw_graph_text_valid(graph_config->service,
                                      TURBO_FLOW_FMQ_BROKER_SERVICE_MAX) ||
      !flow_fmq_tfcw_graph_text_valid(graph_config->resource_name,
                                      TURBO_FLOW_RESOURCE_OWNER_MAX) ||
      !flow_fmq_tfcw_graph_text_valid(graph_config->resource_uid,
                                      TURBO_FLOW_RESOURCE_UID_MAX) ||
      !flow_fmq_tfcw_graph_text_valid(graph_config->owner_name,
                                      TURBO_FLOW_RESOURCE_OWNER_MAX))
    return TURBO_EINVAL;
  rc = flow_fmq_tfcw_credit_config_validate(credit_config);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_tfcw_register_contract(flow);
  if (rc != TURBO_OK) return rc;
  graph = (flow_fmq_tfcw_graph_t *)calloc(1, sizeof(*graph));
  if (!graph) return TURBO_ENOMEM;
  graph->service = tstr_dup(graph_config->service);
  graph->resource_name = tstr_dup(graph_config->resource_name);
  graph->resource_uid = tstr_dup(graph_config->resource_uid);
  graph->owner_name = tstr_dup(graph_config->owner_name);
  graph->max_inflight = credit_config->max_inflight;
  graph->last_status = TURBO_OK;
  turbo_mutex_init(&graph->mutex);
  graph->mutex_initialized = 1;
  graph->owner = turbo_flow_fmq_credit_worker_create(credit_config);
  if (!graph->service || !graph->resource_name || !graph->resource_uid || !graph->owner_name ||
      !graph->owner) {
    flow_fmq_tfcw_shutdown(graph);
    return TURBO_ENOMEM;
  }
  memset(&ops, 0, sizeof(ops));
  ops.consume = flow_fmq_tfcw_consume;
  ops.shutdown = flow_fmq_tfcw_shutdown;
  memset(&schema, 0, sizeof(schema));
  schema.kind = TURBO_FLOW_ADAPTER_KIND_FMQ;
  schema.roles = TURBO_FLOW_ADAPTER_TRANSFORM;
  schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  resource.owner_name = graph->owner_name;
  resource.ops.metadata = flow_fmq_tfcw_resource_metadata;
  resource.ops.snapshot = flow_fmq_tfcw_resource_snapshot;
  resource.ops.document = flow_fmq_tfcw_resource_document;
  resource.ctx = graph;
  memset(&primitive, 0, sizeof(primitive));
  primitive.size = sizeof(primitive);
  primitive.name = graph->resource_name;
  primitive.type_name = TURBO_FLOW_FMQ_TFCW_PRIMITIVE_TYPE;
  primitive.version = TURBO_FLOW_FMQ_TFCW_GRAPH_API_VERSION;
  primitive.domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
  primitive.kind = TURBO_FLOW_PRIMITIVE_RESOURCE;
  for (size_t i = 0u; i < 4u; ++i) operation_resources[i] = graph->resource_name;
  registration.module_name = TURBO_FLOW_FMQ_TFCW_MODULE;
  registration.adapter_name = adapter_name;
  registration.ops = &ops;
  registration.ctx = graph;
  registration.schema = &schema;
  registration.operation_names = operation_names;
  registration.operation_count = 4u;
  registration.resources = &resource;
  registration.resource_count = 1u;
  registration.operation_resource_names = operation_resources;
  registration.primitives = &primitive;
  registration.primitive_count = 1u;
  rc = turbo_flow_register_module_adapter(flow, &registration);
  if (rc != TURBO_OK) flow_fmq_tfcw_shutdown(graph);
  return rc;
}

int turbo_flow_fmq_broker_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                          const char *channel_name, turbo_flow_fmq_broker_t **out,
                                          turbo_flow_config_error_t *error) {
  flow_fmq_pattern_config_t config;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  rc = flow_fmq_pattern_config_resolve(resolved, channel_name, &config, error);
  if (rc != TURBO_OK) return rc;
  if (config.pattern == FLOW_FMQ_PATTERN_PUBSUB_STATE ||
      config.pattern == FLOW_FMQ_PATTERN_CREDIT_WORKER) {
    return flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "pattern",
                                         "broker requires a request pattern");
  }
  *out = turbo_flow_fmq_broker_create(&config.broker);
  return *out ? TURBO_OK
              : flow_fmq_pattern_config_error(error, TURBO_ENOMEM, channel_name, NULL,
                                              "FMQ broker creation failed");
}

int turbo_flow_fmq_credit_worker_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                                 const char *channel_name,
                                                 turbo_flow_fmq_credit_worker_t **out,
                                                 turbo_flow_config_error_t *error) {
  flow_fmq_pattern_config_t config;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  rc = flow_fmq_pattern_config_resolve(resolved, channel_name, &config, error);
  if (rc != TURBO_OK) return rc;
  if (config.pattern != FLOW_FMQ_PATTERN_CREDIT_WORKER) {
    return flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "pattern",
                                         "credit owner requires pattern credit_worker");
  }
  if (config.credit.reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE) {
    return flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "reliability",
                                         "use durable_create_resolved with a storage binding");
  }
  *out = turbo_flow_fmq_credit_worker_create(&config.credit);
  return *out ? TURBO_OK
              : flow_fmq_pattern_config_error(error, TURBO_ENOMEM, channel_name, NULL,
                                              "FMQ credit worker creation failed");
}

int turbo_flow_fmq_tfcw_register_resolved_graph(
    turbo_flow_t *flow, const char *adapter_name,
    const turbo_flow_fmq_tfcw_graph_config_t *graph_config,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_config_error_t *error) {
  flow_fmq_pattern_config_t config;
  turbo_flow_fmq_tfcw_graph_config_t binding;
  int rc;
  if (!flow || !adapter_name || !adapter_name[0] || !graph_config ||
      graph_config->size < sizeof(*graph_config) ||
      graph_config->version != TURBO_FLOW_FMQ_TFCW_GRAPH_API_VERSION || !resolved ||
      !channel_name || !channel_name[0] || !error || error->size < sizeof(*error))
    return TURBO_EINVAL;
  rc = flow_fmq_pattern_config_resolve(resolved, channel_name, &config, error);
  if (rc != TURBO_OK) return rc;
  if (config.pattern != FLOW_FMQ_PATTERN_CREDIT_WORKER)
    return flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "pattern",
                                         "TFCW graph requires pattern credit_worker");
  if (config.credit.reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE)
    return flow_fmq_pattern_config_error(
        error, TURBO_ENOTSUP, channel_name, "reliability",
        "durable TFCW graph requires a message-owned queue claim projection");
  if (!config.credit_service[0])
    return flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, "service",
                                         "TFCW graph requires service");
  binding = *graph_config;
  if (!binding.service)
    binding.service = config.credit_service;
  else if (strcmp(binding.service, config.credit_service) != 0)
    return flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, "service",
                                         "graph service does not match configured service");
  rc = turbo_flow_fmq_tfcw_register_graph(flow, adapter_name, &binding, &config.credit);
  return rc == TURBO_OK
             ? TURBO_OK
             : flow_fmq_pattern_config_error(error, rc, channel_name, NULL,
                                             "TFCW graph registration failed");
}

int turbo_flow_fmq_credit_durable_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_fmq_credit_durable_binding_t *binding,
    turbo_flow_fmq_credit_worker_t **credit_out, turbo_flow_fmq_credit_durable_t **durable_out,
    turbo_flow_config_error_t *error) {
  flow_fmq_pattern_config_t config;
  turbo_flow_fmq_credit_worker_t *credit;
  int rc;
  if (credit_out) *credit_out = NULL;
  if (durable_out) *durable_out = NULL;
  if (!credit_out || !durable_out || !binding || binding->size < sizeof(*binding) ||
      !binding->storage_channel || !binding->settler)
    return TURBO_EINVAL;
  rc = flow_fmq_pattern_config_resolve(resolved, channel_name, &config, error);
  if (rc != TURBO_OK) return rc;
  if (config.pattern != FLOW_FMQ_PATTERN_CREDIT_WORKER ||
      config.credit.reliability != TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE)
    return flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "reliability",
                                         "durable owner requires at_least_once credit_worker");
  if (strcmp(config.durable_storage_channel, binding->storage_channel) != 0)
    return flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, "storage_channel",
                                         "storage binding does not match configured channel");
  credit = turbo_flow_fmq_credit_worker_create(&config.credit);
  if (!credit)
    return flow_fmq_pattern_config_error(error, TURBO_ENOMEM, channel_name, NULL,
                                         "FMQ credit worker creation failed");
  rc = turbo_flow_fmq_credit_durable_create(credit, binding->settler, &config.durable, durable_out);
  if (rc != TURBO_OK) {
    turbo_flow_fmq_credit_worker_destroy(credit);
    return flow_fmq_pattern_config_error(error, rc, channel_name, "storage_channel",
                                         "durable storage state could not be opened");
  }
  *credit_out = credit;
  return TURBO_OK;
}
