#include "turbo_flow_fmq.h"

#include "turbo_error.h"
#include "turbo_fs.h"
#include "turbo_thread.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOWMQ_YAML_ENDPOINT_DEFAULT_DURATION_MS 1000u

static int yaml_endpoint_on_message(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *message,
                                    void *ctx) {
  (void)app;
  (void)ctx;
  if (!message) return TURBO_EINVAL;
  printf("received payload=");
  if (message->payload.len > 0u)
    (void)fwrite(message->payload.data, 1u, message->payload.len, stdout);
  printf("\n");
  return TURBO_OK;
}

static int yaml_endpoint_parse_duration(const char *text, unsigned int *duration_ms) {
  char *end = NULL;
  unsigned long value;
  if (!text || !text[0] || !duration_ms) return TURBO_EINVAL;
  value = strtoul(text, &end, 10);
  if (!end || *end != '\0' || value > 0xfffffffful) return TURBO_EINVAL;
  *duration_ms = (unsigned int)value;
  return TURBO_OK;
}

static int yaml_endpoint_requires_callback(const char *pattern) {
  if (!pattern || !pattern[0]) return TURBO_EINVAL;
  return strcmp(pattern, "pub") != 0 && strcmp(pattern, "push") != 0;
}

int main(int argc, char **argv) {
  turbo_fs_buf_t yaml = {0};
  turbo_flow_resolved_config_t *resolved = NULL;
  turbo_flow_fmq_app_options_t options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
  turbo_flow_fmq_app_t *app = NULL;
  turbo_flow_resolved_adapter_view_t adapter_view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  const char *pattern = NULL;
  unsigned int duration_ms = FLOWMQ_YAML_ENDPOINT_DEFAULT_DURATION_MS;
  const char *payload = NULL;
  int started = 0;
  int rc;

  if (argc < 3 || argc > 5) {
    fprintf(stderr, "usage: %s <yaml> <adapter> [duration_ms] [payload]\n", argv[0]);
    return 2;
  }
  if (argc >= 4) {
    rc = yaml_endpoint_parse_duration(argv[3], &duration_ms);
    if (rc != TURBO_OK) {
      fprintf(stderr, "invalid duration_ms: %s\n", argv[3]);
      return 2;
    }
  }
  if (argc == 5) payload = argv[4];

  rc = turbo_fs_read_file(argv[1], &yaml);
  if (rc == TURBO_OK)
    rc = turbo_flow_config_resolve_yaml(yaml.base, yaml.len, &resolved, &error);
  if (rc == TURBO_OK) {
    rc = turbo_flow_resolved_config_adapter(resolved, argv[2], &adapter_view);
  }
  if (rc == TURBO_OK) rc = turbo_flow_resolved_adapter_get_string(&adapter_view, "pattern", &pattern);
  if (rc == TURBO_OK) {
    if (yaml_endpoint_requires_callback(pattern)) options.on_message = yaml_endpoint_on_message;
    rc = turbo_flow_fmq_app_create_resolved(resolved, argv[2], &options, &app, &error);
  }
  if (rc == TURBO_OK) {
    rc = turbo_flow_fmq_app_start(app);
    started = rc == TURBO_OK;
  }
  if (rc == TURBO_OK && payload) rc = turbo_flow_fmq_app_send(app, payload, strlen(payload));
  if (rc == TURBO_OK) turbo_sleep_ms(duration_ms);
  if (started) {
    int stop_rc = turbo_flow_fmq_app_stop(app);
    if (rc == TURBO_OK) rc = stop_rc;
  }

  if (rc != TURBO_OK) {
    if (error.status != TURBO_OK)
      fprintf(stderr, "yaml endpoint failed: status=%d path=%s message=%s\n", error.status,
              error.path, error.message);
    else
      fprintf(stderr, "yaml endpoint failed: status=%d\n", rc);
  }
  turbo_flow_fmq_app_destroy(app);
  turbo_flow_resolved_config_destroy(resolved);
  turbo_fs_buf_free(&yaml);
  return rc == TURBO_OK ? 0 : 1;
}
