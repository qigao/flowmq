#include "flowmq_cnet_transport.h"

#include <core/error_codes.h>

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

static const char tcp_scheme[] = "tcp://";
static const char tls_scheme[] = "tls://";

flowmq_transport_t flowmq_cnet_uri_transport(const char *uri) {
  if (strncmp(uri, tcp_scheme, sizeof(tcp_scheme) - 1u) == 0) return FLOWMQ_TRANSPORT_TCP;
  if (strncmp(uri, tls_scheme, sizeof(tls_scheme) - 1u) == 0) return FLOWMQ_TRANSPORT_TLS;
  return 0;
}

int flowmq_cnet_endpoint_parse(const char *endpoint, bool allow_zero_port,
                               flowmq_cnet_endpoint_t *parts) {
  const char *host_begin;
  const char *host_end;
  const char *port_begin;
  char *port_end = NULL;
  unsigned long port;
  size_t host_size;
  if (endpoint == NULL || parts == NULL) return SALTS_EINVAL;
  memset(parts, 0, sizeof(*parts));
  parts->transport = flowmq_cnet_uri_transport(endpoint);
  if (parts->transport == 0) return SALTS_EINVAL;
  host_begin = endpoint + sizeof(tcp_scheme) - 1u;
  if (*host_begin == '[') {
    host_begin++;
    host_end = strchr(host_begin, ']');
    if (host_end == NULL || host_end[1] != ':') return SALTS_EINVAL;
    port_begin = host_end + 2u;
  } else {
    host_end = strrchr(host_begin, ':');
    if (host_end == NULL) return SALTS_EINVAL;
    port_begin = host_end + 1u;
  }
  host_size = (size_t)(host_end - host_begin);
  if (host_size == 0u || host_size >= sizeof(parts->host) || *port_begin == '\0')
    return SALTS_EINVAL;
  memcpy(parts->host, host_begin, host_size);
  parts->host[host_size] = '\0';
  port = strtoul(port_begin, &port_end, 10);
  if (port_end == port_begin || *port_end != '\0' || port > 65535u ||
      (!allow_zero_port && port == 0u))
    return SALTS_EINVAL;
  parts->port = (uint16_t)port;
  return SALTS_OK;
}

int flowmq_cnet_endpoint_format(const flowmq_cnet_endpoint_t *endpoint,
                                char *uri, size_t capacity) {
  const char *scheme = endpoint->transport == FLOWMQ_TRANSPORT_TLS
                           ? tls_scheme : tcp_scheme;
  const bool bracket = strchr(endpoint->host, ':') != NULL;
  const int written = snprintf(uri, capacity,
                               bracket ? "%s[%s]:%u" : "%s%s:%u",
                               scheme, endpoint->host, (unsigned)endpoint->port);
  if (written < 0 || (size_t)written >= capacity) return SALTS_EMSGSIZE;
  return SALTS_OK;
}

static int flowmq_cnet_timeout(uint64_t value, uint32_t *out) {
  if (out == NULL || value > UINT32_MAX) return SALTS_ERANGE;
  *out = (uint32_t)value;
  return SALTS_OK;
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
  if (out == NULL) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  if (flowmq_io_config_validate(io) != SALTS_OK || timeouts == NULL ||
      flowmq_transport_validate(transport) != SALTS_OK || connection_capacity == 0u ||
      max_send_bytes == 0u || connection_capacity > SIZE_MAX / 2u ||
      io->request_capacity < connection_capacity * 2u ||
      io->event_capacity < connection_capacity * 2u)
    return SALTS_EINVAL;
  status = flowmq_cnet_timeout(timeouts->connect_timeout_ms, &connect_timeout_ms);
  if (status == SALTS_OK)
    status = flowmq_cnet_timeout(timeouts->recv_timeout_ms, &read_timeout_ms);
  if (status == SALTS_OK)
    status = flowmq_cnet_timeout(timeouts->send_timeout_ms, &write_timeout_ms);
  if (status == SALTS_OK)
    status = flowmq_cnet_timeout(timeouts->handshake_timeout_ms, &handshake_timeout_ms);
  if (status != SALTS_OK) return status;
  if (transport == FLOWMQ_TRANSPORT_TLS && handshake_timeout_ms == 0u) return SALTS_EINVAL;
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
  return native_io_backend_kind_supported(out->backend) ? SALTS_OK : SALTS_ENOTSUP;
}
