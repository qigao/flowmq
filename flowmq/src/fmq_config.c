#include "turbo_flow_fmq.h"
#include "flow_fmq_internal.h"

#include "turbo_error.h"
#include "turbo_parser.h"

#include <limits.h>
#include <math.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

typedef enum flow_fmq_json_field_type_e {
  FLOW_FMQ_JSON_STRING = 1,
  FLOW_FMQ_JSON_BOOL,
  FLOW_FMQ_JSON_U32,
  FLOW_FMQ_JSON_INT,
  FLOW_FMQ_JSON_SIZE,
  FLOW_FMQ_JSON_U64,
  FLOW_FMQ_JSON_ENUM
} flow_fmq_json_field_type_t;

typedef struct flow_fmq_json_field_s {
  const char *name;
  flow_fmq_json_field_type_t type;
  size_t offset;
  uint64_t maximum;
  const char *const *values;
  size_t value_count;
  int enum_base;
  const char *sentinel;
  uint64_t sentinel_value;
} flow_fmq_json_field_t;

static const char *const FLOW_FMQ_JSON_PATTERNS[] = {
    "pub", "sub", "push", "pull", "router", "dealer", "pair", "req", "rep", "xpub", "xsub"};
static const char *const FLOW_FMQ_JSON_MODES[] = {"bind", "connect"};
static const char *const FLOW_FMQ_JSON_TRANSPORTS[] = {"tcp",  "tls", "udp", "kcp",
                                                       "pipe", "ws",  "wss"};
static const char *const FLOW_FMQ_JSON_METADATA_POLICIES[] = {"static", "inherit", "content"};
static const char *const FLOW_FMQ_JSON_ADMISSION_POLICIES[] = {"fail", "block", "drop_oldest"};
static const char *const FLOW_FMQ_JSON_SLOW_PEER_POLICIES[] = {"fail", "drop_oldest", "disconnect"};

#define FLOW_FMQ_JSON_FIELD(member, field_type, max_value)                                         \
  {#member, field_type, offsetof(turbo_flow_fmq_config_t, member), max_value, NULL, 0u, 0, NULL, 0u}
#define FLOW_FMQ_JSON_ENUM_FIELD(member, enum_values)                                              \
  {#member,                                                                                        \
   FLOW_FMQ_JSON_ENUM,                                                                             \
   offsetof(turbo_flow_fmq_config_t, member),                                                      \
   0u,                                                                                             \
   enum_values,                                                                                    \
   sizeof(enum_values) / sizeof((enum_values)[0]),                                                 \
   1,                                                                                              \
   NULL,                                                                                           \
   0u}
#define FLOW_FMQ_JSON_ENUM_ZERO_FIELD(member, enum_values)                                         \
  {#member,                                                                                        \
   FLOW_FMQ_JSON_ENUM,                                                                             \
   offsetof(turbo_flow_fmq_config_t, member),                                                      \
   0u,                                                                                             \
   enum_values,                                                                                    \
   sizeof(enum_values) / sizeof((enum_values)[0]),                                                 \
   0,                                                                                              \
   NULL,                                                                                           \
   0u}
#define FLOW_FMQ_JSON_SENTINEL_FIELD(member, name, value)                                          \
  {#member,                                                                                        \
   FLOW_FMQ_JSON_U64,                                                                              \
   offsetof(turbo_flow_fmq_config_t, member),                                                      \
   UINT64_MAX,                                                                                     \
   NULL,                                                                                           \
   0u,                                                                                             \
   0,                                                                                              \
   name,                                                                                           \
   value}

static const flow_fmq_json_field_t FLOW_FMQ_JSON_FIELDS[] = {
    FLOW_FMQ_JSON_ENUM_FIELD(pattern, FLOW_FMQ_JSON_PATTERNS),
    FLOW_FMQ_JSON_ENUM_FIELD(mode, FLOW_FMQ_JSON_MODES),
    FLOW_FMQ_JSON_ENUM_FIELD(transport, FLOW_FMQ_JSON_TRANSPORTS),
    FLOW_FMQ_JSON_FIELD(host, FLOW_FMQ_JSON_STRING, 0u),
    FLOW_FMQ_JSON_FIELD(port, FLOW_FMQ_JSON_INT, 65535u),
    FLOW_FMQ_JSON_FIELD(topic, FLOW_FMQ_JSON_STRING, 0u),
    FLOW_FMQ_JSON_FIELD(content_type, FLOW_FMQ_JSON_STRING, 0u),
    FLOW_FMQ_JSON_FIELD(identity, FLOW_FMQ_JSON_STRING, 0u),
    FLOW_FMQ_JSON_FIELD(max_frame_size, FLOW_FMQ_JSON_SIZE, UINT32_MAX),
    FLOW_FMQ_JSON_FIELD(max_connections, FLOW_FMQ_JSON_U32, TURBO_FLOW_FMQ_MAX_CONNECTIONS_LIMIT),
    FLOW_FMQ_JSON_FIELD(timeout_ms, FLOW_FMQ_JSON_U64, UINT64_MAX),
    FLOW_FMQ_JSON_FIELD(connect_timeout_ms, FLOW_FMQ_JSON_U64, UINT64_MAX),
    FLOW_FMQ_JSON_SENTINEL_FIELD(send_timeout_ms, "disabled", TURBO_FLOW_FMQ_TIMEOUT_DISABLED),
    FLOW_FMQ_JSON_SENTINEL_FIELD(recv_timeout_ms, "disabled", TURBO_FLOW_FMQ_TIMEOUT_DISABLED),
    FLOW_FMQ_JSON_SENTINEL_FIELD(handshake_timeout_ms, "disabled", TURBO_FLOW_FMQ_TIMEOUT_DISABLED),
    FLOW_FMQ_JSON_SENTINEL_FIELD(reconnect_initial_ms, "disabled",
                                 TURBO_FLOW_FMQ_RECONNECT_DISABLED),
    FLOW_FMQ_JSON_SENTINEL_FIELD(reconnect_max_ms, "unbounded",
                                 TURBO_FLOW_FMQ_RECONNECT_MAX_UNBOUNDED),
    FLOW_FMQ_JSON_FIELD(heartbeat_interval_ms, FLOW_FMQ_JSON_U64, UINT64_MAX),
    FLOW_FMQ_JSON_FIELD(heartbeat_timeout_ms, FLOW_FMQ_JSON_U64, UINT64_MAX),
    FLOW_FMQ_JSON_ENUM_FIELD(topic_policy, FLOW_FMQ_JSON_METADATA_POLICIES),
    FLOW_FMQ_JSON_ENUM_FIELD(identity_policy, FLOW_FMQ_JSON_METADATA_POLICIES),
    FLOW_FMQ_JSON_FIELD(path, FLOW_FMQ_JSON_STRING, 0u),
    FLOW_FMQ_JSON_FIELD(kcp_pre_shared_key, FLOW_FMQ_JSON_STRING, 0u),
    FLOW_FMQ_JSON_FIELD(kcp_mtu, FLOW_FMQ_JSON_U32, UINT16_MAX),
    FLOW_FMQ_JSON_FIELD(kcp_send_window, FLOW_FMQ_JSON_U32, UINT16_MAX),
    FLOW_FMQ_JSON_FIELD(kcp_receive_window, FLOW_FMQ_JSON_U32, UINT16_MAX),
    FLOW_FMQ_JSON_FIELD(kcp_interval_ms, FLOW_FMQ_JSON_U32, 100u),
    FLOW_FMQ_JSON_FIELD(kcp_handshake_retry_ms, FLOW_FMQ_JSON_U32, UINT16_MAX),
    FLOW_FMQ_JSON_FIELD(kcp_fast_resend, FLOW_FMQ_JSON_U32, UINT8_MAX),
    FLOW_FMQ_JSON_FIELD(kcp_congestion_control, FLOW_FMQ_JSON_BOOL, 0u),
    FLOW_FMQ_JSON_FIELD(kcp_fec_data_shards, FLOW_FMQ_JSON_U32, UINT32_MAX),
    FLOW_FMQ_JSON_FIELD(kcp_fec_parity_shards, FLOW_FMQ_JSON_U32, UINT32_MAX),
    FLOW_FMQ_JSON_FIELD(kcp_fec_max_payload_size, FLOW_FMQ_JSON_U32, UINT32_MAX),
    FLOW_FMQ_JSON_FIELD(kcp_fec_receive_groups, FLOW_FMQ_JSON_U32, UINT16_MAX),
    FLOW_FMQ_JSON_FIELD(reuse_port, FLOW_FMQ_JSON_BOOL, 0u),
    FLOW_FMQ_JSON_FIELD(tcp_keepalive, FLOW_FMQ_JSON_BOOL, 0u),
    FLOW_FMQ_JSON_FIELD(tcp_keepalive_idle_ms, FLOW_FMQ_JSON_U64, UINT64_MAX),
    FLOW_FMQ_JSON_FIELD(tcp_keepalive_interval_ms, FLOW_FMQ_JSON_U64, UINT64_MAX),
    FLOW_FMQ_JSON_FIELD(tcp_keepalive_count, FLOW_FMQ_JSON_U32, UINT32_MAX),
    FLOW_FMQ_JSON_FIELD(linger, FLOW_FMQ_JSON_BOOL, 0u),
    FLOW_FMQ_JSON_FIELD(linger_ms, FLOW_FMQ_JSON_U64, UINT64_MAX),
    FLOW_FMQ_JSON_FIELD(send_hwm_bytes, FLOW_FMQ_JSON_SIZE, SIZE_MAX),
    FLOW_FMQ_JSON_FIELD(udp_multicast_group, FLOW_FMQ_JSON_STRING, 0u),
    FLOW_FMQ_JSON_FIELD(udp_multicast_interface, FLOW_FMQ_JSON_STRING, 0u),
    FLOW_FMQ_JSON_FIELD(udp_option_flags, FLOW_FMQ_JSON_U32, UINT32_MAX),
    FLOW_FMQ_JSON_FIELD(udp_multicast_loop, FLOW_FMQ_JSON_BOOL, 0u),
    FLOW_FMQ_JSON_FIELD(udp_multicast_ttl, FLOW_FMQ_JSON_U32, 255u),
    FLOW_FMQ_JSON_FIELD(udp_broadcast, FLOW_FMQ_JSON_BOOL, 0u),
    FLOW_FMQ_JSON_FIELD(frame_hwm_messages, FLOW_FMQ_JSON_SIZE, SIZE_MAX),
    FLOW_FMQ_JSON_FIELD(frame_hwm_bytes, FLOW_FMQ_JSON_SIZE, SIZE_MAX),
    FLOW_FMQ_JSON_ENUM_ZERO_FIELD(frame_admission_policy, FLOW_FMQ_JSON_ADMISSION_POLICIES),
    FLOW_FMQ_JSON_SENTINEL_FIELD(frame_admission_timeout_ms, "unbounded", UINT64_MAX),
    FLOW_FMQ_JSON_FIELD(frame_linger_ms, FLOW_FMQ_JSON_U64, UINT64_MAX),
    FLOW_FMQ_JSON_FIELD(stream_recv_buffer_bytes, FLOW_FMQ_JSON_SIZE,
                        TURBO_FLOW_FMQ_MAX_STREAM_RECV_BUFFER_SIZE),
    FLOW_FMQ_JSON_FIELD(socket_recv_buffer_bytes, FLOW_FMQ_JSON_SIZE, INT_MAX),
    FLOW_FMQ_JSON_FIELD(socket_send_buffer_bytes, FLOW_FMQ_JSON_SIZE, INT_MAX)};

#define FLOW_FMQ_FANOUT_JSON_FIELD(member, field_type, max_value)                                  \
  {#member, field_type, offsetof(turbo_flow_fmq_fanout_config_t, member), max_value, NULL, 0u, 0,  \
   NULL,    0u}
#define FLOW_FMQ_FANOUT_JSON_ENUM_FIELD(member, enum_values)                                       \
  {#member,                                                                                        \
   FLOW_FMQ_JSON_ENUM,                                                                             \
   offsetof(turbo_flow_fmq_fanout_config_t, member),                                               \
   0u,                                                                                             \
   enum_values,                                                                                    \
   sizeof(enum_values) / sizeof((enum_values)[0]),                                                 \
   1,                                                                                              \
   NULL,                                                                                           \
   0u}

static const flow_fmq_json_field_t FLOW_FMQ_FANOUT_JSON_FIELDS[] = {
    FLOW_FMQ_FANOUT_JSON_FIELD(peer_hwm_messages, FLOW_FMQ_JSON_SIZE, SIZE_MAX),
    FLOW_FMQ_FANOUT_JSON_FIELD(peer_hwm_bytes, FLOW_FMQ_JSON_SIZE, SIZE_MAX),
    FLOW_FMQ_FANOUT_JSON_ENUM_FIELD(slow_peer_policy, FLOW_FMQ_JSON_SLOW_PEER_POLICIES)};

static int flow_fmq_json_error(turbo_flow_config_error_t *error, int status, const char *name,
                               const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field) {
      (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.config.%s", name, field);
    } else {
      (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s", name ? name : "?");
    }
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static const flow_fmq_json_field_t *flow_fmq_json_field_find(const char *name) {
  for (size_t i = 0; i < sizeof(FLOW_FMQ_JSON_FIELDS) / sizeof(FLOW_FMQ_JSON_FIELDS[0]); ++i) {
    if (strcmp(FLOW_FMQ_JSON_FIELDS[i].name, name) == 0) return &FLOW_FMQ_JSON_FIELDS[i];
  }
  return NULL;
}

static const flow_fmq_json_field_t *flow_fmq_fanout_json_field_find(const char *name) {
  for (size_t i = 0;
       i < sizeof(FLOW_FMQ_FANOUT_JSON_FIELDS) / sizeof(FLOW_FMQ_FANOUT_JSON_FIELDS[0]); ++i) {
    if (strcmp(FLOW_FMQ_FANOUT_JSON_FIELDS[i].name, name) == 0) {
      return &FLOW_FMQ_FANOUT_JSON_FIELDS[i];
    }
  }
  return NULL;
}

static int flow_fmq_json_u64(const json_value_t *value, const flow_fmq_json_field_t *field,
                             uint64_t *out) {
  double number;
  uint64_t converted;
  if (field->sentinel && turbo_json_type(value) == TURBO_JSON_STRING &&
      strcmp(turbo_json_string(value), field->sentinel) == 0) {
    *out = field->sentinel_value;
    return TURBO_OK;
  }
  if (turbo_json_type(value) != TURBO_JSON_NUMBER) return TURBO_EINVAL;
  number = turbo_json_number(value);
  if (!isfinite(number) || number < 0.0 || number > (double)field->maximum ||
      number > 9007199254740991.0) {
    return TURBO_ERANGE;
  }
  converted = (uint64_t)number;
  if ((double)converted != number) return TURBO_EINVAL;
  *out = converted;
  return TURBO_OK;
}

static int flow_fmq_json_assign(void *object, const flow_fmq_json_field_t *field,
                                const json_value_t *value) {
  unsigned char *target;
  uint64_t number;
  if (!object || !field || !value) return TURBO_EINVAL;
  target = (unsigned char *)object + field->offset;
  if (field->type == FLOW_FMQ_JSON_STRING) {
    if (turbo_json_type(value) == TURBO_JSON_NULL) {
      *(const char **)target = NULL;
      return TURBO_OK;
    }
    if (turbo_json_type(value) != TURBO_JSON_STRING) return TURBO_EINVAL;
    *(const char **)target = turbo_json_string(value);
    return TURBO_OK;
  }
  if (field->type == FLOW_FMQ_JSON_BOOL) {
    if (turbo_json_type(value) != TURBO_JSON_BOOL) return TURBO_EINVAL;
    *(int *)target = turbo_json_bool(value) ? 1 : 0;
    return TURBO_OK;
  }
  if (field->type == FLOW_FMQ_JSON_ENUM) {
    const char *text;
    if (turbo_json_type(value) != TURBO_JSON_STRING) return TURBO_EINVAL;
    text = turbo_json_string(value);
    for (size_t i = 0; i < field->value_count; ++i) {
      if (strcmp(text, field->values[i]) == 0) {
        *(int *)target = (int)i + field->enum_base;
        return TURBO_OK;
      }
    }
    return TURBO_EINVAL;
  }
  {
    int rc = flow_fmq_json_u64(value, field, &number);
    if (rc != TURBO_OK) return rc;
  }
  switch (field->type) {
  case FLOW_FMQ_JSON_U32:
    *(uint32_t *)target = (uint32_t)number;
    return TURBO_OK;
  case FLOW_FMQ_JSON_INT:
    if (number > INT_MAX) return TURBO_ERANGE;
    *(int *)target = (int)number;
    return TURBO_OK;
  case FLOW_FMQ_JSON_SIZE:
    *(size_t *)target = (size_t)number;
    return TURBO_OK;
  case FLOW_FMQ_JSON_U64:
    *(uint64_t *)target = number;
    return TURBO_OK;
  default:
    return TURBO_EINVAL;
  }
}

static int flow_fmq_register_resolved_adapter_internal(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security,
    const turbo_flow_fmq_app_options_t *app_options, turbo_flow_fmq_app_t **app_out,
    turbo_flow_config_error_t *error) {
  turbo_json_doc_t *document = NULL;
  json_value_t *adapters;
  json_value_t *adapter;
  json_value_t *kind;
  json_value_t *fields;
  turbo_flow_fmq_config_t config = TURBO_FLOW_FMQ_CONFIG_INIT;
  turbo_flow_fmq_fanout_config_t fanout = TURBO_FLOW_FMQ_FANOUT_CONFIG_INIT;
  const char *json;
  size_t json_len = 0u;
  int fanout_requested = 0;
  int fanout_hwm_seen = 0;
  int fanout_policy_seen = 0;
  const char *security_realm = NULL;
  const char *auth_provider = NULL;
  const char *auth_method = NULL;
  const char *secret_reference = NULL;
  int security_metadata_seen = 0;
  int rc;
  if (app_out) *app_out = NULL;
  if ((!flow && !app_out) || (flow && app_out) || !name || !name[0] || !resolved || !execution ||
      !error ||
      error->size < sizeof(*error)) {
    return TURBO_EINVAL;
  }
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_len);
  if (!json || turbo_parse_json((const uint8_t *)json, json_len, &document) != TURBO_OK ||
      !document) {
    return flow_fmq_json_error(error, TURBO_EINVAL, name, NULL,
                               "invalid resolved configuration snapshot");
  }
  adapters = turbo_json_object_get(document, "adapters");
  adapter = adapters ? turbo_json_object_get(adapters, name) : NULL;
  if (!adapter || turbo_json_type(adapter) != TURBO_JSON_OBJECT) {
    rc = flow_fmq_json_error(error, TURBO_ENOENT, name, NULL, "adapter is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(adapter, "kind");
  fields = turbo_json_object_get(adapter, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "fmq") != 0) {
    rc = flow_fmq_json_error(error, TURBO_EINVAL, name, NULL, "adapter kind must be fmq");
    goto done;
  }
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_fmq_json_error(error, TURBO_EINVAL, name, NULL, "adapter config must be a mapping");
    goto done;
  }
  for (size_t i = 0; i < turbo_json_object_size(fields); ++i) {
    const char *field_name = turbo_json_object_key(fields, i);
    json_value_t *value = turbo_json_object_value(fields, i);
    const flow_fmq_json_field_t *field = field_name ? flow_fmq_json_field_find(field_name) : NULL;
    const flow_fmq_json_field_t *fanout_field =
        field_name ? flow_fmq_fanout_json_field_find(field_name) : NULL;
    if (field_name &&
        (strcmp(field_name, "security_realm") == 0 || strcmp(field_name, "auth_provider") == 0 ||
         strcmp(field_name, "auth_method") == 0 || strcmp(field_name, "secret_reference") == 0)) {
      const char *text;
      if (turbo_json_type(value) != TURBO_JSON_STRING || !(text = turbo_json_string(value)) ||
          !text[0]) {
        rc = flow_fmq_json_error(error, TURBO_EINVAL, name, field_name,
                                 "FMQ security reference must be a non-empty string");
        goto done;
      }
      if (strcmp(field_name, "security_realm") == 0) security_realm = text;
      else if (strcmp(field_name, "auth_provider") == 0) auth_provider = text;
      else if (strcmp(field_name, "auth_method") == 0) auth_method = text;
      else secret_reference = text;
      security_metadata_seen = 1;
      continue;
    }
    if (fanout_field) {
      rc = flow_fmq_json_assign(&fanout, fanout_field, value);
      if (rc != TURBO_OK) {
        rc = flow_fmq_json_error(error, rc, name, field_name, "invalid FMQ fan-out field value");
        goto done;
      }
      fanout_requested = 1;
      if (strcmp(field_name, "slow_peer_policy") == 0) {
        fanout_policy_seen = 1;
      } else {
        fanout_hwm_seen = 1;
      }
      continue;
    }
    if (!field) {
      rc = flow_fmq_json_error(error, TURBO_EINVAL, name, field_name, "unknown FMQ field");
      goto done;
    }
    rc = flow_fmq_json_assign(&config, field, value);
    if (rc != TURBO_OK) {
      rc = flow_fmq_json_error(error, rc, name, field_name, "invalid FMQ field value");
      goto done;
    }
  }
  if (fanout_requested && (!fanout_hwm_seen || !fanout_policy_seen)) {
    rc = flow_fmq_json_error(error, TURBO_EINVAL, name, NULL,
                             "peer HWM and slow_peer_policy must be configured together");
    goto done;
  }
  if (security_metadata_seen && !security) {
    rc = flow_fmq_json_error(error, TURBO_EPERM, name, NULL,
                             "FMQ security metadata requires an explicit composed binding");
    goto done;
  }
  if (security_metadata_seen && config.mode == TURBO_FLOW_FMQ_BIND) {
    if (!security_realm || !auth_provider || !auth_method || secret_reference ||
        !security->realm_channel || strcmp(security->realm_channel, security_realm) != 0 ||
        !security->auth_method || strcmp(security->auth_method, auth_method) != 0) {
      rc = flow_fmq_json_error(error, TURBO_EINVAL, name, NULL,
                               "BIND security metadata does not match the injected binding");
      goto done;
    }
  } else if (security_metadata_seen && config.mode == TURBO_FLOW_FMQ_CONNECT) {
    if (security_realm || auth_provider || !auth_method || !secret_reference ||
        !security->auth_method || strcmp(security->auth_method, auth_method) != 0 ||
        !security->secret_reference || strcmp(security->secret_reference, secret_reference) != 0) {
      rc = flow_fmq_json_error(error, TURBO_EINVAL, name, NULL,
                               "CONNECT security metadata does not match the injected binding");
      goto done;
    }
  }
  if (app_out) {
    rc = flow_fmq_app_create_endpoint_internal(
        name, &config, fanout_requested ? &fanout : NULL, app_options, execution, security,
        app_out);
  } else if (fanout_requested) {
    rc = security
             ? turbo_flow_fmq_register_secure_fanout_adapter_ex(flow, name, &config, &fanout,
                                                                execution, security)
             : turbo_flow_fmq_register_fanout_adapter_ex(flow, name, &config, &fanout, execution);
  } else {
    rc = security
             ? turbo_flow_fmq_register_secure_adapter_ex(flow, name, &config, execution, security)
             : turbo_flow_fmq_register_adapter_ex(flow, name, &config, execution);
  }
  if (rc != TURBO_OK)
    rc = flow_fmq_json_error(error, rc, name, NULL, "FMQ configuration validation failed");

done:
  turbo_free_json(&document);
  return rc;
}

int turbo_flow_fmq_register_resolved_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution, turbo_flow_config_error_t *error) {
  return flow_fmq_register_resolved_adapter_internal(flow, name, resolved, execution, NULL, NULL,
                                                     NULL, error);
}

int turbo_flow_fmq_register_resolved_secure_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_config_error_t *error) {
  if (!security) return TURBO_EINVAL;
  return flow_fmq_register_resolved_adapter_internal(flow, name, resolved, execution, security,
                                                     NULL, NULL, error);
}

int flow_fmq_app_create_resolved_endpoint_internal(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out,
    turbo_flow_config_error_t *error) {
  const turbo_flow_coronet_execution_binding_t execution = {
      sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
  if (!options || !out) return TURBO_EINVAL;
  return flow_fmq_register_resolved_adapter_internal(
      NULL, adapter_name, resolved, &execution, security, options, out, error);
}

int turbo_flow_fmq_register_resolved_adapter(turbo_flow_t *flow, const char *name,
                                             const turbo_flow_resolved_config_t *resolved,
                                             turbo_flow_config_error_t *error) {
  turbo_flow_coronet_execution_binding_t execution;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  return turbo_flow_fmq_register_resolved_adapter_ex(flow, name, resolved, &execution, error);
}

int turbo_flow_fmq_register_resolved_secure_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_config_error_t *error) {
  turbo_flow_coronet_execution_binding_t execution;
  if (!security) return TURBO_EINVAL;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  return turbo_flow_fmq_register_resolved_secure_adapter_ex(flow, name, resolved, &execution,
                                                            security, error);
}
