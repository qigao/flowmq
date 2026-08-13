#include "turbo_flow_fmq_retry.h"

#include "fmq_pattern_config.h"
#include "turbo_error.h"

int turbo_flow_fmq_retry_ledger_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                                const char *channel_name,
                                                turbo_flow_fmq_retry_ledger_t **out,
                                                turbo_flow_config_error_t *error) {
  flow_fmq_pattern_config_t config;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  rc = flow_fmq_pattern_config_resolve(resolved, channel_name, &config, error);
  if (rc != TURBO_OK) return rc;
  if (config.pattern != FLOW_FMQ_PATTERN_RELIABLE_REQUEST) {
    return flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "pattern",
                                         "retry ledger requires reliable_request");
  }
  *out = turbo_flow_fmq_retry_ledger_create(&config.retry);
  return *out ? TURBO_OK
              : flow_fmq_pattern_config_error(error, TURBO_ENOMEM, channel_name, NULL,
                                              "FMQ retry ledger creation failed");
}
