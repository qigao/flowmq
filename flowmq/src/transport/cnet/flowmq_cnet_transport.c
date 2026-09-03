#include "flowmq_cnet_transport.h"

#include <turbo/error_codes.h>

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

native_io_backend_kind flowmq_cnet_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#elif defined(__APPLE__) || defined(__FreeBSD__)
  return NATIVE_IO_BACKEND_KQUEUE;
#else
  return (native_io_backend_kind)0;
#endif
}

int flowmq_cnet_endpoint_validate(flowmq_transport_t transport, const char *host, int port) {
  if (flowmq_transport_validate(transport) != TURBO_OK || host == NULL || host[0] == '\0' ||
      port < 1 || port > 65535)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowmq_cnet_timeout(uint64_t value, uint32_t *out) {
  if (out == NULL || value > UINT32_MAX) return TURBO_ERANGE;
  *out = (uint32_t)value;
  return TURBO_OK;
}

int flowmq_cnet_client_config(const flowmq_io_config_t *io,
                              const flowmq_timeout_config_t *timeouts,
                              flowmq_transport_t transport, size_t connection_capacity,
                              size_t max_send_bytes, cnet_client_config *out) {
  uint32_t connect_timeout_ms = 0u;
  uint32_t read_timeout_ms = 0u;
  uint32_t write_timeout_ms = 0u;
  uint32_t handshake_timeout_ms = 0u;
  int status;
  if (out == NULL) return TURBO_EINVAL;
  memset(out, 0, sizeof(*out));
  if (flowmq_io_config_validate(io) != TURBO_OK || timeouts == NULL ||
      flowmq_transport_validate(transport) != TURBO_OK || connection_capacity == 0u ||
      max_send_bytes == 0u || connection_capacity > SIZE_MAX / 2u ||
      io->request_capacity < connection_capacity * 2u ||
      io->event_capacity < connection_capacity * 2u)
    return TURBO_EINVAL;
  status = flowmq_cnet_timeout(timeouts->connect_timeout_ms, &connect_timeout_ms);
  if (status == TURBO_OK)
    status = flowmq_cnet_timeout(timeouts->recv_timeout_ms, &read_timeout_ms);
  if (status == TURBO_OK)
    status = flowmq_cnet_timeout(timeouts->send_timeout_ms, &write_timeout_ms);
  if (status == TURBO_OK)
    status = flowmq_cnet_timeout(timeouts->handshake_timeout_ms, &handshake_timeout_ms);
  if (status != TURBO_OK) return status;
  if (transport == FLOWMQ_TRANSPORT_TLS && handshake_timeout_ms == 0u) return TURBO_EINVAL;
  *out = (cnet_client_config){
      .backend = flowmq_cnet_backend(),
      .connection_capacity = connection_capacity,
      .command_capacity = io->command_capacity,
      .request_capacity = io->request_capacity,
      .completion_batch_capacity = io->completion_batch_capacity,
      .event_capacity = io->event_capacity,
      .max_send_bytes = max_send_bytes,
      .receive_buffer_bytes = io->receive_buffer_bytes,
      .connect_timeout_ms = connect_timeout_ms,
      .read_timeout_ms = read_timeout_ms,
      .write_timeout_ms = write_timeout_ms,
      .tls_io_buffer_bytes = transport == FLOWMQ_TRANSPORT_TLS ? io->tls_io_buffer_bytes : 0u,
      .tls_handshake_timeout_ms =
          transport == FLOWMQ_TRANSPORT_TLS ? handshake_timeout_ms : 0u};
  return native_io_backend_kind_supported(out->backend) ? TURBO_OK : TURBO_ENOTSUP;
}

int flowmq_cnet_uri(flowmq_transport_t transport, const char *host, int port, char **out_uri) {
  const char *scheme;
  int bracket;
  int size;
  char *uri;
  if (out_uri == NULL) return TURBO_EINVAL;
  *out_uri = NULL;
  if (flowmq_cnet_endpoint_validate(transport, host, port) != TURBO_OK) return TURBO_EINVAL;
  scheme = transport == FLOWMQ_TRANSPORT_TLS ? "tls" : "tcp";
  bracket = strchr(host, ':') != NULL && host[0] != '[';
  size = snprintf(NULL, 0, bracket ? "%s://[%s]:%d" : "%s://%s:%d", scheme, host, port);
  if (size < 0) return TURBO_EIO;
  uri = (char *)malloc((size_t)size + 1u);
  if (uri == NULL) return TURBO_ENOMEM;
  if (snprintf(uri, (size_t)size + 1u, bracket ? "%s://[%s]:%d" : "%s://%s:%d", scheme,
               host, port) != size) {
    free(uri);
    return TURBO_EIO;
  }
  *out_uri = uri;
  return TURBO_OK;
}
