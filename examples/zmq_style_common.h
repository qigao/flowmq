#ifndef FLOWMQ_ZMQ_STYLE_COMMON_H
#define FLOWMQ_ZMQ_STYLE_COMMON_H

#include "turbo_flow_fmq.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#define FMQ_EXAMPLE_RETRY_COUNT 400
#define FMQ_EXAMPLE_RETRY_DELAY_MS 5u

static int fmq_example_parse_port(int argc, char **argv, unsigned short default_port,
                                  unsigned short *port) {
  char *end = NULL;
  long value;
  if (!port || argc < 1 || argc > 2) return TURBO_EINVAL;
  if (argc == 1) {
    *port = default_port;
    return TURBO_OK;
  }
  value = strtol(argv[1], &end, 10);
  if (!end || *end != '\0' || value < 1 || value > 65535) return TURBO_EINVAL;
  *port = (unsigned short)value;
  return TURBO_OK;
}

static int fmq_example_send_when_connected(turbo_flow_fmq_app_t *app, const void *data,
                                           size_t size) {
  int rc = TURBO_ENOTCONN;
  for (int attempt = 0; attempt < FMQ_EXAMPLE_RETRY_COUNT && rc == TURBO_ENOTCONN; ++attempt) {
    rc = turbo_flow_fmq_app_send(app, data, size);
    if (rc == TURBO_ENOTCONN) turbo_sleep_ms(FMQ_EXAMPLE_RETRY_DELAY_MS);
  }
  return rc;
}

static int fmq_example_wait_for(const atomic_int *value, int expected) {
  if (!value) return TURBO_EINVAL;
  for (int attempt = 0; attempt < FMQ_EXAMPLE_RETRY_COUNT; ++attempt) {
    if (atomic_load_explicit(value, memory_order_acquire) >= expected) return TURBO_OK;
    turbo_sleep_ms(FMQ_EXAMPLE_RETRY_DELAY_MS);
  }
  return TURBO_ETIMEDOUT;
}

#endif /* FLOWMQ_ZMQ_STYLE_COMMON_H */
