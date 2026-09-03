#include "flowmq_transport.h"

#include <salts/error_codes.h>

#include <limits.h>

enum {
  FLOWMQ_IO_DEFAULT_COMMAND_CAPACITY = 2u,
  FLOWMQ_IO_DEFAULT_REQUEST_CAPACITY = 8u,
  FLOWMQ_IO_DEFAULT_COMPLETION_BATCH_CAPACITY = 8u,
  FLOWMQ_IO_DEFAULT_EVENT_CAPACITY = 16u,
  FLOWMQ_IO_DEFAULT_RECEIVE_BUFFER_BYTES = 17u * 1024u,
  FLOWMQ_IO_DEFAULT_TLS_BUFFER_BYTES = 17u * 1024u,
  FLOWMQ_IO_DEFAULT_POLL_TIMEOUT_MS = 10u
};

static int flowmq_power_of_two(size_t value) {
  return value != 0u && (value & (value - 1u)) == 0u;
}

int flowmq_transport_validate(flowmq_transport_t transport) {
  return transport == FLOWMQ_TRANSPORT_TCP || transport == FLOWMQ_TRANSPORT_TLS ? SALTS_OK
                                                                                : SALTS_EINVAL;
}

void flowmq_timeouts_resolve(flowmq_timeout_config_t *timeouts, uint64_t fallback_timeout_ms) {
  uint64_t default_timeout_ms;
  if (timeouts == NULL) return;
  timeouts->explicit_flags = timeouts->set_flags;
  default_timeout_ms = (timeouts->set_flags & FLOWMQ_TIMEOUT_SET_DEFAULT)
                           ? timeouts->timeout_ms
                           : (timeouts->timeout_ms != 0u ? timeouts->timeout_ms
                                                        : fallback_timeout_ms);
  timeouts->timeout_ms = default_timeout_ms;
  if (!(timeouts->set_flags & FLOWMQ_TIMEOUT_SET_CONNECT) &&
      timeouts->connect_timeout_ms == 0u)
    timeouts->connect_timeout_ms = default_timeout_ms;
  if (!(timeouts->set_flags & FLOWMQ_TIMEOUT_SET_SEND) && timeouts->send_timeout_ms == 0u)
    timeouts->send_timeout_ms = default_timeout_ms;
  if (!(timeouts->set_flags & FLOWMQ_TIMEOUT_SET_RECV) && timeouts->recv_timeout_ms == 0u)
    timeouts->recv_timeout_ms = default_timeout_ms;
  if (!(timeouts->set_flags & FLOWMQ_TIMEOUT_SET_HANDSHAKE) &&
      timeouts->handshake_timeout_ms == 0u)
    timeouts->handshake_timeout_ms = default_timeout_ms;
  timeouts->set_flags = FLOWMQ_TIMEOUT_SET_ALL;
}

void flowmq_io_config_init(flowmq_io_config_t *config) {
  if (config == NULL) return;
  *config = (flowmq_io_config_t){
      .command_capacity = FLOWMQ_IO_DEFAULT_COMMAND_CAPACITY,
      .request_capacity = FLOWMQ_IO_DEFAULT_REQUEST_CAPACITY,
      .completion_batch_capacity = FLOWMQ_IO_DEFAULT_COMPLETION_BATCH_CAPACITY,
      .event_capacity = FLOWMQ_IO_DEFAULT_EVENT_CAPACITY,
      .receive_buffer_bytes = FLOWMQ_IO_DEFAULT_RECEIVE_BUFFER_BYTES,
      .tls_io_buffer_bytes = FLOWMQ_IO_DEFAULT_TLS_BUFFER_BYTES,
      .poll_timeout_ms = FLOWMQ_IO_DEFAULT_POLL_TIMEOUT_MS};
}

int flowmq_io_config_validate(const flowmq_io_config_t *config) {
  if (config == NULL || !flowmq_power_of_two(config->command_capacity) ||
      config->request_capacity == 0u || config->completion_batch_capacity == 0u ||
      config->completion_batch_capacity > config->request_capacity ||
      !flowmq_power_of_two(config->event_capacity) || config->event_capacity < 2u ||
      config->receive_buffer_bytes == 0u || config->tls_io_buffer_bytes < 17u * 1024u ||
      config->tls_io_buffer_bytes > INT_MAX || config->poll_timeout_ms == 0u)
    return SALTS_EINVAL;
  return SALTS_OK;
}
