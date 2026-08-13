#ifndef FLOW_FMQ_PATTERN_CONFIG_H
#define FLOW_FMQ_PATTERN_CONFIG_H

#include "turbo_flow_fmq_broker.h"
#include "turbo_flow_fmq_pubsub.h"
#include "turbo_flow_fmq_retry.h"

#define FLOW_FMQ_PATTERN_REFERENCE_MAX 255u

typedef enum flow_fmq_pattern_kind_e {
  FLOW_FMQ_PATTERN_LOAD_BALANCER = 1,
  FLOW_FMQ_PATTERN_RELIABLE_REQUEST,
  FLOW_FMQ_PATTERN_PUBSUB_STATE,
  FLOW_FMQ_PATTERN_CREDIT_WORKER
} flow_fmq_pattern_kind_t;

typedef struct flow_fmq_pattern_config_s {
  flow_fmq_pattern_kind_t pattern;
  turbo_flow_fmq_broker_config_t broker;
  turbo_flow_fmq_retry_config_t retry;
  turbo_flow_fmq_pubsub_config_t pubsub;
  turbo_flow_fmq_credit_worker_config_t credit;
  char credit_service[TURBO_FLOW_FMQ_BROKER_SERVICE_MAX + 1u];
  turbo_flow_fmq_credit_durable_config_t durable;
  char durable_storage_channel[FLOW_FMQ_PATTERN_REFERENCE_MAX + 1u];
  char durable_state_key[TURBO_FLOW_FMQ_CREDIT_DURABLE_STATE_KEY_MAX + 1u];
} flow_fmq_pattern_config_t;

int flow_fmq_pattern_config_resolve(const turbo_flow_resolved_config_t *resolved,
                                    const char *channel_name, flow_fmq_pattern_config_t *out,
                                    turbo_flow_config_error_t *error);

int flow_fmq_pattern_config_error(turbo_flow_config_error_t *error, int status, const char *name,
                                  const char *field, const char *message);

#endif /* FLOW_FMQ_PATTERN_CONFIG_H */
