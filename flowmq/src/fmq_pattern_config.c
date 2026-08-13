#include "fmq_pattern_config.h"

#include "turbo_error.h"
#include "turbo_parser.h"

#include <math.h>
#include <stdio.h>
#include <string.h>

int flow_fmq_pattern_config_error(turbo_flow_config_error_t *error, int status, const char *name,
                                  const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field)
      (void)snprintf(error->path, sizeof(error->path), "$.channels.%s.config.%s", name, field);
    else (void)snprintf(error->path, sizeof(error->path), "$.channels.%s", name ? name : "?");
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_fmq_pattern_field_allowed(const char *field) {
  static const char *const fields[] = {"pattern",
                                       "scheduler",
                                       "max_workers",
                                       "max_inflight",
                                       "reliability",
                                       "worker_lease_ms",
                                       "dedup_capacity",
                                       "dedup_ttl_ms",
                                       "max_attempts",
                                       "max_topics",
                                       "max_state_bytes",
                                       "update_capacity",
                                       "max_update_bytes",
                                       "max_credit_messages_per_worker",
                                       "max_credit_bytes_per_worker",
                                       "max_job_bytes",
                                       "service",
                                       "storage_channel",
                                       "state_key",
                                       "shutdown_policy",
                                       "shutdown_max_steps"};
  for (size_t i = 0u; i < sizeof(fields) / sizeof(fields[0]); ++i) {
    if (field && strcmp(field, fields[i]) == 0) return 1;
  }
  return 0;
}

static int flow_fmq_pattern_u64(const json_value_t *fields, const char *field, uint64_t minimum,
                                uint64_t *out) {
  json_value_t *value = turbo_json_object_get(fields, field);
  double number;
  uint64_t converted;
  if (!value || turbo_json_type(value) != TURBO_JSON_NUMBER) return TURBO_EINVAL;
  number = turbo_json_number(value);
  if (!isfinite(number) || number < (double)minimum || number > 9007199254740991.0)
    return TURBO_ERANGE;
  converted = (uint64_t)number;
  if ((double)converted != number) return TURBO_EINVAL;
  *out = converted;
  return TURBO_OK;
}

static int flow_fmq_pattern_size(const json_value_t *fields, const char *field, size_t maximum,
                                 size_t *out) {
  uint64_t converted;
  int rc = flow_fmq_pattern_u64(fields, field, 1u, &converted);
  if (rc != TURBO_OK) return rc;
  if (converted > maximum) return TURBO_ERANGE;
  *out = (size_t)converted;
  return TURBO_OK;
}

static int flow_fmq_pattern_text(const json_value_t *fields, const char *field, char *out,
                                 size_t capacity) {
  json_value_t *value = turbo_json_object_get(fields, field);
  const char *text;
  size_t length;
  if (!value || turbo_json_type(value) != TURBO_JSON_STRING || !out || capacity == 0u)
    return TURBO_EINVAL;
  text = turbo_json_string(value);
  length = text ? strlen(text) : 0u;
  if (length == 0u || length >= capacity) return TURBO_ERANGE;
  memcpy(out, text, length + 1u);
  return TURBO_OK;
}

static int flow_fmq_pattern_reject_retry_fields(const json_value_t *fields, const char *name,
                                                turbo_flow_config_error_t *error) {
  static const char *const retry_fields[] = {"dedup_capacity", "dedup_ttl_ms", "max_attempts"};
  for (size_t i = 0u; i < sizeof(retry_fields) / sizeof(retry_fields[0]); ++i) {
    if (turbo_json_object_get(fields, retry_fields[i])) {
      return flow_fmq_pattern_config_error(error, TURBO_EINVAL, name, retry_fields[i],
                                           "retry fields require pattern reliable_request");
    }
  }
  return TURBO_OK;
}

int flow_fmq_pattern_config_resolve(const turbo_flow_resolved_config_t *resolved,
                                    const char *channel_name, flow_fmq_pattern_config_t *out,
                                    turbo_flow_config_error_t *error) {
  turbo_json_doc_t *document = NULL;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  json_value_t *pattern;
  json_value_t *scheduler;
  json_value_t *reliability;
  json_value_t *worker_lease;
  json_value_t *dedup_capacity;
  json_value_t *dedup_ttl;
  json_value_t *max_attempts;
  flow_fmq_pattern_config_t parsed;
  const char *json;
  size_t json_len = 0u;
  size_t parsed_attempts;
  int rc;
  if (!resolved || !channel_name || !channel_name[0] || !out || !error ||
      error->size < sizeof(*error)) {
    return TURBO_EINVAL;
  }
  memset(&parsed, 0, sizeof(parsed));
  parsed.broker = (turbo_flow_fmq_broker_config_t)TURBO_FLOW_FMQ_BROKER_CONFIG_INIT;
  parsed.retry = (turbo_flow_fmq_retry_config_t)TURBO_FLOW_FMQ_RETRY_CONFIG_INIT;
  parsed.pubsub = (turbo_flow_fmq_pubsub_config_t)TURBO_FLOW_FMQ_PUBSUB_CONFIG_INIT;
  parsed.credit = (turbo_flow_fmq_credit_worker_config_t)TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
  parsed.durable =
      (turbo_flow_fmq_credit_durable_config_t)TURBO_FLOW_FMQ_CREDIT_DURABLE_CONFIG_INIT;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_len);
  if (!json || turbo_parse_json((const uint8_t *)json, json_len, &document) != TURBO_OK ||
      !document) {
    return flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                         "invalid resolved configuration snapshot");
  }
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_fmq_pattern_config_error(error, TURBO_ENOENT, channel_name, NULL,
                                       "channel is not resolved");
    goto done;
  }
  kind = turbo_json_object_get(channel, "kind");
  fields = turbo_json_object_get(channel, "config");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "fmq_pattern") != 0) {
    rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                       "channel kind must be fmq_pattern");
    goto done;
  }
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                       "config must be a mapping");
    goto done;
  }
  for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
    const char *field = turbo_json_object_key(fields, i);
    if (!flow_fmq_pattern_field_allowed(field)) {
      rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, field,
                                         "unknown FMQ pattern field");
      goto done;
    }
  }
  pattern = turbo_json_object_get(fields, "pattern");
  scheduler = turbo_json_object_get(fields, "scheduler");
  reliability = turbo_json_object_get(fields, "reliability");
  worker_lease = turbo_json_object_get(fields, "worker_lease_ms");
  dedup_capacity = turbo_json_object_get(fields, "dedup_capacity");
  dedup_ttl = turbo_json_object_get(fields, "dedup_ttl_ms");
  max_attempts = turbo_json_object_get(fields, "max_attempts");
  if (!pattern || turbo_json_type(pattern) != TURBO_JSON_STRING ||
      (strcmp(turbo_json_string(pattern), "load_balancer") != 0 &&
       strcmp(turbo_json_string(pattern), "reliable_request") != 0 &&
       strcmp(turbo_json_string(pattern), "pubsub_state") != 0 &&
       strcmp(turbo_json_string(pattern), "credit_worker") != 0)) {
    rc = flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "pattern",
                                       "unsupported FMQ pattern");
    goto done;
  }
  if (strcmp(turbo_json_string(pattern), "pubsub_state") == 0) {
    static const char *const pubsub_fields[] = {"pattern", "max_topics", "max_state_bytes",
                                                "update_capacity", "max_update_bytes"};
    parsed.pattern = FLOW_FMQ_PATTERN_PUBSUB_STATE;
    for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
      const char *field = turbo_json_object_key(fields, i);
      int allowed = 0;
      for (size_t j = 0u; j < sizeof(pubsub_fields) / sizeof(pubsub_fields[0]); ++j) {
        if (field && strcmp(field, pubsub_fields[j]) == 0) {
          allowed = 1;
          break;
        }
      }
      if (!allowed) {
        rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, field,
                                           "field is not valid for pubsub_state");
        goto done;
      }
    }
    if (turbo_json_object_get(fields, "max_topics")) {
      rc = flow_fmq_pattern_size(fields, "max_topics", TURBO_FLOW_FMQ_PUBSUB_MAX_TOPICS,
                                 &parsed.pubsub.max_topics);
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "max_topics",
                                           "max_topics must be a bounded positive integer");
        goto done;
      }
    }
    if (turbo_json_object_get(fields, "max_state_bytes")) {
      rc = flow_fmq_pattern_size(fields, "max_state_bytes", SIZE_MAX,
                                 &parsed.pubsub.max_state_bytes);
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "max_state_bytes",
                                           "max_state_bytes must be a positive integer");
        goto done;
      }
    }
    if (turbo_json_object_get(fields, "update_capacity")) {
      rc = flow_fmq_pattern_size(fields, "update_capacity", TURBO_FLOW_FMQ_PUBSUB_MAX_UPDATES,
                                 &parsed.pubsub.update_capacity);
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "update_capacity",
                                           "update_capacity must be a bounded positive integer");
        goto done;
      }
    }
    if (turbo_json_object_get(fields, "max_update_bytes")) {
      rc = flow_fmq_pattern_size(fields, "max_update_bytes", SIZE_MAX,
                                 &parsed.pubsub.max_update_bytes);
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "max_update_bytes",
                                           "max_update_bytes must be a positive integer");
        goto done;
      }
    }
    *out = parsed;
    rc = TURBO_OK;
    goto done;
  }
  {
    static const char *const pubsub_only[] = {"max_topics", "max_state_bytes", "update_capacity",
                                              "max_update_bytes"};
    for (size_t i = 0u; i < sizeof(pubsub_only) / sizeof(pubsub_only[0]); ++i) {
      if (turbo_json_object_get(fields, pubsub_only[i])) {
        rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, pubsub_only[i],
                                           "field requires pattern pubsub_state");
        goto done;
      }
    }
  }
  if (!scheduler || turbo_json_type(scheduler) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(scheduler), "lru") != 0) {
    rc = flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "scheduler",
                                       "scheduler must be lru");
    goto done;
  }
  rc = flow_fmq_pattern_size(fields, "max_workers", TURBO_FLOW_FMQ_BROKER_MAX_WORKERS,
                             &parsed.broker.max_workers);
  if (rc != TURBO_OK) {
    rc = flow_fmq_pattern_config_error(error, rc, channel_name, "max_workers",
                                       "max_workers must be a bounded positive integer");
    goto done;
  }
  rc = flow_fmq_pattern_size(fields, "max_inflight", TURBO_FLOW_FMQ_BROKER_MAX_INFLIGHT,
                             &parsed.broker.max_inflight);
  if (rc != TURBO_OK) {
    rc = flow_fmq_pattern_config_error(error, rc, channel_name, "max_inflight",
                                       "max_inflight must be a bounded positive integer");
    goto done;
  }
  if (strcmp(turbo_json_string(pattern), "credit_worker") == 0) {
    static const char *const credit_fields[] = {"pattern",
                                                "scheduler",
                                                "max_workers",
                                                "max_inflight",
                                                "reliability",
                                                "worker_lease_ms",
                                                "max_credit_messages_per_worker",
                                                "max_credit_bytes_per_worker",
                                                "max_job_bytes",
                                                "service",
                                                "storage_channel",
                                                "state_key",
                                                "max_attempts",
                                                "dedup_ttl_ms",
                                                "shutdown_policy",
                                                "shutdown_max_steps"};
    parsed.pattern = FLOW_FMQ_PATTERN_CREDIT_WORKER;
    for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
      const char *field = turbo_json_object_key(fields, i);
      int allowed = 0;
      for (size_t j = 0u; j < sizeof(credit_fields) / sizeof(credit_fields[0]); ++j) {
        if (field && strcmp(field, credit_fields[j]) == 0) {
          allowed = 1;
          break;
        }
      }
      if (!allowed) {
        rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, field,
                                           "field is not valid for credit_worker");
        goto done;
      }
    }
    parsed.credit.max_workers = parsed.broker.max_workers;
    parsed.credit.max_inflight = parsed.broker.max_inflight;
    if (turbo_json_object_get(fields, "service")) {
      rc = flow_fmq_pattern_text(fields, "service", parsed.credit_service,
                                 sizeof(parsed.credit_service));
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "service",
                                           "service must be a bounded non-empty name");
        goto done;
      }
    }
    if (!reliability || turbo_json_type(reliability) != TURBO_JSON_STRING) {
      rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, "reliability",
                                         "credit_worker requires reliability");
      goto done;
    }
    if (strcmp(turbo_json_string(reliability), "at_most_once") == 0) {
      static const char *const durable_only[] = {"storage_channel", "state_key",
                                                 "max_attempts",    "dedup_ttl_ms",
                                                 "shutdown_policy", "shutdown_max_steps"};
      parsed.credit.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE;
      for (size_t i = 0u; i < sizeof(durable_only) / sizeof(durable_only[0]); ++i) {
        if (turbo_json_object_get(fields, durable_only[i])) {
          rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, durable_only[i],
                                             "field requires at_least_once credit_worker");
          goto done;
        }
      }
    } else if (strcmp(turbo_json_string(reliability), "at_least_once") == 0) {
      parsed.credit.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
      rc = flow_fmq_pattern_text(fields, "storage_channel", parsed.durable_storage_channel,
                                 sizeof(parsed.durable_storage_channel));
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "storage_channel",
                                           "storage_channel must be a bounded non-empty reference");
        goto done;
      }
      rc = flow_fmq_pattern_text(fields, "state_key", parsed.durable_state_key,
                                 sizeof(parsed.durable_state_key));
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "state_key",
                                           "state_key must be a bounded non-empty key");
        goto done;
      }
      rc = flow_fmq_pattern_u64(fields, "max_attempts", 1u, &parsed_attempts);
      if (rc != TURBO_OK || parsed_attempts > UINT32_MAX) {
        rc = flow_fmq_pattern_config_error(error, rc == TURBO_OK ? TURBO_ERANGE : rc, channel_name,
                                           "max_attempts", "max_attempts must fit uint32");
        goto done;
      }
      parsed.durable.max_attempts = (uint32_t)parsed_attempts;
      rc = flow_fmq_pattern_u64(fields, "dedup_ttl_ms", 1u, &parsed.durable.terminal_ttl_ms);
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "dedup_ttl_ms",
                                           "dedup_ttl_ms must be a positive integer");
        goto done;
      }
      parsed.durable.capacity = parsed.credit.max_inflight;
      parsed.durable.shutdown_max_steps = parsed.credit.max_inflight;
      if (turbo_json_object_get(fields, "shutdown_policy")) {
        json_value_t *shutdown_value = turbo_json_object_get(fields, "shutdown_policy");
        const char *shutdown_policy =
            shutdown_value && turbo_json_type(shutdown_value) == TURBO_JSON_STRING
                ? turbo_json_string(shutdown_value)
                : NULL;
        if (!shutdown_policy || !shutdown_policy[0]) {
          rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, "shutdown_policy",
                                             "shutdown_policy must be requeue or preserve");
          goto done;
        }
        if (strcmp(shutdown_policy, "requeue") == 0)
          parsed.durable.shutdown_policy = TURBO_FLOW_FMQ_CREDIT_SHUTDOWN_REQUEUE;
        else if (strcmp(shutdown_policy, "preserve") == 0)
          parsed.durable.shutdown_policy = TURBO_FLOW_FMQ_CREDIT_SHUTDOWN_PRESERVE;
        else {
          rc = flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "shutdown_policy",
                                             "shutdown_policy must be requeue or preserve");
          goto done;
        }
      }
      if (turbo_json_object_get(fields, "shutdown_max_steps")) {
        rc = flow_fmq_pattern_size(fields, "shutdown_max_steps", parsed.credit.max_inflight,
                                   &parsed.durable.shutdown_max_steps);
        if (rc != TURBO_OK) {
          rc = flow_fmq_pattern_config_error(
              error, rc, channel_name, "shutdown_max_steps",
              "shutdown_max_steps must be positive and within max_inflight");
          goto done;
        }
      }
    } else {
      rc = flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "reliability",
                                         "reliability must be at_most_once or at_least_once");
      goto done;
    }
    rc = flow_fmq_pattern_u64(fields, "worker_lease_ms", 1u, &parsed.credit.worker_lease_ms);
    if (rc != TURBO_OK) {
      rc = flow_fmq_pattern_config_error(error, rc, channel_name, "worker_lease_ms",
                                         "worker_lease_ms must be a positive integer");
      goto done;
    }
    rc = flow_fmq_pattern_size(fields, "max_credit_messages_per_worker", SIZE_MAX,
                               &parsed.credit.max_credit_messages_per_worker);
    if (rc != TURBO_OK ||
        parsed.credit.max_credit_messages_per_worker > SIZE_MAX / parsed.credit.max_workers) {
      rc = flow_fmq_pattern_config_error(
          error, rc == TURBO_OK ? TURBO_ERANGE : rc, channel_name, "max_credit_messages_per_worker",
          "message credit bound is invalid or overflows total capacity");
      goto done;
    }
    rc = flow_fmq_pattern_size(fields, "max_credit_bytes_per_worker", SIZE_MAX,
                               &parsed.credit.max_credit_bytes_per_worker);
    if (rc != TURBO_OK ||
        parsed.credit.max_credit_bytes_per_worker > SIZE_MAX / parsed.credit.max_workers) {
      rc = flow_fmq_pattern_config_error(
          error, rc == TURBO_OK ? TURBO_ERANGE : rc, channel_name, "max_credit_bytes_per_worker",
          "byte credit bound is invalid or overflows total capacity");
      goto done;
    }
    rc = flow_fmq_pattern_size(fields, "max_job_bytes", SIZE_MAX, &parsed.credit.max_job_bytes);
    if (rc != TURBO_OK || parsed.credit.max_job_bytes > parsed.credit.max_credit_bytes_per_worker) {
      rc = flow_fmq_pattern_config_error(error, rc == TURBO_OK ? TURBO_ERANGE : rc, channel_name,
                                         "max_job_bytes",
                                         "max_job_bytes must fit one worker byte-credit window");
      goto done;
    }
    *out = parsed;
    if (out->credit.reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE)
      out->durable.state_key = out->durable_state_key;
    rc = TURBO_OK;
    goto done;
  }
  {
    static const char *const credit_only[] = {"max_credit_messages_per_worker",
                                              "max_credit_bytes_per_worker",
                                              "max_job_bytes",
                                              "service",
                                              "storage_channel",
                                              "state_key",
                                              "shutdown_policy",
                                              "shutdown_max_steps"};
    for (size_t i = 0u; i < sizeof(credit_only) / sizeof(credit_only[0]); ++i) {
      if (turbo_json_object_get(fields, credit_only[i])) {
        rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, credit_only[i],
                                           "field requires pattern credit_worker");
        goto done;
      }
    }
  }
  if (strcmp(turbo_json_string(pattern), "load_balancer") == 0) {
    parsed.pattern = FLOW_FMQ_PATTERN_LOAD_BALANCER;
    if (reliability && (turbo_json_type(reliability) != TURBO_JSON_STRING ||
                        strcmp(turbo_json_string(reliability), "none") != 0)) {
      rc = flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "reliability",
                                         "load_balancer reliability must be none");
      goto done;
    }
    if (worker_lease) {
      rc = flow_fmq_pattern_u64(fields, "worker_lease_ms", 0u, &parsed.broker.worker_lease_ms);
      if (rc != TURBO_OK || parsed.broker.worker_lease_ms != 0u) {
        rc = flow_fmq_pattern_config_error(error, rc == TURBO_OK ? TURBO_EINVAL : rc, channel_name,
                                           "worker_lease_ms",
                                           "load_balancer worker_lease_ms must be zero");
        goto done;
      }
    }
    rc = flow_fmq_pattern_reject_retry_fields(fields, channel_name, error);
    if (rc != TURBO_OK) goto done;
  } else {
    parsed.pattern = FLOW_FMQ_PATTERN_RELIABLE_REQUEST;
    if (!reliability || turbo_json_type(reliability) != TURBO_JSON_STRING) {
      rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, "reliability",
                                         "reliable_request requires reliability");
      goto done;
    }
    if (strcmp(turbo_json_string(reliability), "at_most_once") == 0) {
      parsed.broker.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE;
      parsed.retry.max_attempts = 1u;
    } else if (strcmp(turbo_json_string(reliability), "at_least_once") == 0) {
      parsed.broker.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    } else {
      rc = flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "reliability",
                                         "reliability must be at_most_once or at_least_once");
      goto done;
    }
    rc = flow_fmq_pattern_u64(fields, "worker_lease_ms", 1u, &parsed.broker.worker_lease_ms);
    if (rc != TURBO_OK) {
      rc = flow_fmq_pattern_config_error(error, rc, channel_name, "worker_lease_ms",
                                         "worker_lease_ms must be a positive integer");
      goto done;
    }
    parsed.retry.capacity = parsed.broker.max_inflight;
    if (dedup_capacity) {
      rc = flow_fmq_pattern_size(fields, "dedup_capacity", TURBO_FLOW_FMQ_RETRY_MAX_CAPACITY,
                                 &parsed.retry.capacity);
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "dedup_capacity",
                                           "dedup_capacity must be a bounded positive integer");
        goto done;
      }
    }
    if (dedup_ttl) {
      rc = flow_fmq_pattern_u64(fields, "dedup_ttl_ms", 1u, &parsed.retry.terminal_ttl_ms);
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "dedup_ttl_ms",
                                           "dedup_ttl_ms must be a positive integer");
        goto done;
      }
    }
    if (max_attempts) {
      rc = flow_fmq_pattern_size(fields, "max_attempts", TURBO_FLOW_FMQ_RETRY_MAX_ATTEMPTS,
                                 &parsed_attempts);
      if (rc != TURBO_OK) {
        rc = flow_fmq_pattern_config_error(error, rc, channel_name, "max_attempts",
                                           "max_attempts must be a bounded positive integer");
        goto done;
      }
      parsed.retry.max_attempts = (uint32_t)parsed_attempts;
    }
    if (parsed.broker.reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE &&
        parsed.retry.max_attempts != 1u) {
      rc = flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, "max_attempts",
                                         "at_most_once requires max_attempts 1");
      goto done;
    }
  }
  *out = parsed;
  rc = TURBO_OK;

done:
  turbo_free_json(&document);
  return rc;
}
