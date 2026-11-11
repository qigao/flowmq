#include "flowmq_cnet_transport.h"

#include <core/error_codes.h>
#include <core/clock.h>
#include <native_io_transport.h>
#include <threads/threads.h>

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <netinet/in.h>
#include <sys/socket.h>
#endif

#include <inttypes.h>
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

static const char *const schemes[] = {
    "", "tcp://", "tls://", "ipc://", "ws://", "wss://", "kcp://"};

flowmq_transport_t flowmq_cnet_uri_transport(const char *uri) {
  for (unsigned i = FLOWMQ_TRANSPORT_TCP; i <= FLOWMQ_TRANSPORT_KCP; ++i) {
    if (strncmp(uri, schemes[i], strlen(schemes[i])) == 0)
      return (flowmq_transport_t)i;
  }
  return 0;
}

static int flowmq_cnet_parse_number(const char *first, const char *last,
                                   uint32_t limit, uint32_t *out) {
  uint32_t number = 0u;
  if (first == last) return SALTS_EINVAL;
  for (; first != last; ++first) {
    if (*first < '0' || *first > '9') return SALTS_EINVAL;
    const uint32_t digit = (uint32_t)(*first - '0');
    if (number > (limit - digit) / 10u) return SALTS_EINVAL;
    number = number * 10u + digit;
  }
  *out = number;
  return SALTS_OK;
}

int flowmq_cnet_endpoint_parse(const char *endpoint, bool allow_zero_port,
                               flowmq_cnet_endpoint_t *parts) {
  if (endpoint == NULL || parts == NULL) return SALTS_EINVAL;
  memset(parts, 0, sizeof(*parts));
  parts->transport = flowmq_cnet_uri_transport(endpoint);
  if (parts->transport == 0) return SALTS_EINVAL;
  const char *host = endpoint + strlen(schemes[parts->transport]);
  if (parts->transport == FLOWMQ_TRANSPORT_IPC) {
    const size_t size = strlen(host);
    if (size >= sizeof(parts->host)) return SALTS_EMSGSIZE;
    const int status = native_io_transport_ipc_validate_name(host, size);
    if (status != SALTS_OK) return status;
    memcpy(parts->host, host, size + 1u);
    return SALTS_OK;
  }

  const bool websocket = flowmq_cnet_is_websocket(parts->transport);
  const bool kcp = parts->transport == FLOWMQ_TRANSPORT_KCP;
  const char *end = host + strcspn(host, "/?#");
  const char *host_end;
  const char *port;
  const bool bracket = *host == '[';
  if (bracket) {
    ++host;
    host_end = memchr(host, ']', (size_t)(end - host));
    if (host_end == NULL || end - host_end < 2 || host_end[1] != ':')
      return SALTS_EINVAL;
    port = host_end + 2;
  } else {
    host_end = memchr(host, ':', (size_t)(end - host));
    if (host_end == NULL) return SALTS_EINVAL;
    port = host_end + 1;
  }
  const size_t host_size = (size_t)(host_end - host);
  if (host_size == 0u || host_size >= sizeof(parts->host))
    return SALTS_EINVAL;
  for (const char *p = host; p != host_end; ++p) {
    const unsigned char c = (unsigned char)*p;
    if (c <= 32u || c >= 127u || c == '@' || c == '[' || c == ']')
      return SALTS_EINVAL;
  }
  memcpy(parts->host, host, host_size);
  if (bracket && native_io_transport_validate_ipv6_literal(parts->host) != SALTS_OK)
    return SALTS_EINVAL;
  uint32_t port_number;
  int status = flowmq_cnet_parse_number(port, end, UINT16_MAX, &port_number);
  if (status != SALTS_OK || (!allow_zero_port && port_number == 0u))
    return SALTS_EINVAL;
  parts->port = (uint16_t)port_number;
  if (websocket) {
    const char *path = *end == '\0' ? "/" : end;
    const size_t size = strlen(path);
    if (*path != '/' || size >= sizeof(parts->path)) return SALTS_EINVAL;
    for (size_t i = 0u; i < size; ++i) {
      const unsigned char c = (unsigned char)path[i];
      if (c <= 32u || c >= 127u || c == '#') return SALTS_EINVAL;
    }
    memcpy(parts->path, path, size + 1u);
  } else if (kcp) {
    const char prefix[] = "?conv=";
    if (strncmp(end, prefix, sizeof(prefix) - 1u) != 0) return SALTS_EINVAL;
    const char *conv = end + sizeof(prefix) - 1u;
    status = flowmq_cnet_parse_number(conv, conv + strlen(conv), UINT32_MAX,
                                     &parts->conversation);
    if (status != SALTS_OK || parts->conversation == 0u) return SALTS_EINVAL;
  } else if (*end != '\0') {
    return SALTS_EINVAL;
  }
  return SALTS_OK;
}

int flowmq_cnet_endpoint_format(const flowmq_cnet_endpoint_t *endpoint,
                                char *uri, size_t capacity) {
  const char *scheme = schemes[endpoint->transport];
  int written;
  if (endpoint->transport == FLOWMQ_TRANSPORT_IPC) {
    written = snprintf(uri, capacity, "%s%s", scheme, endpoint->host);
  } else {
    const bool bracket = strchr(endpoint->host, ':') != NULL;
    written = snprintf(uri, capacity, bracket ? "%s[%s]:%u%s" : "%s%s:%u%s",
                       scheme, endpoint->host, (unsigned)endpoint->port,
                       flowmq_cnet_is_websocket(endpoint->transport) ? endpoint->path : "");
  }
  if (written < 0 || (size_t)written >= capacity) return SALTS_EMSGSIZE;
  if (endpoint->transport == FLOWMQ_TRANSPORT_KCP) {
    const size_t offset = (size_t)written;
    written = snprintf(uri + offset, capacity - offset, "?conv=%" PRIu32,
                       endpoint->conversation);
    if (written < 0 || (size_t)written >= capacity - offset) return SALTS_EMSGSIZE;
  }
  return SALTS_OK;
}

int flowmq_cnet_datagram_peer(const flowmq_cnet_endpoint_t *endpoint,
                              cnet_datagram_peer *out) {
  struct sockaddr_storage address;
  size_t size;
  int status = native_io_transport_parse_numeric_address(
      endpoint->host, endpoint->port, &address, sizeof(address), &size);
  if (status != SALTS_OK) return status == SALTS_ENOENT ? SALTS_EINVAL : status;
  *out = (cnet_datagram_peer){.port = endpoint->port};
  if (address.ss_family == AF_INET) {
    struct sockaddr_in ipv4;
    memcpy(&ipv4, &address, sizeof(ipv4));
    out->family = CNET_DATAGRAM_ADDRESS_IPV4;
    memcpy(out->address, &ipv4.sin_addr, sizeof(ipv4.sin_addr));
  } else {
    struct sockaddr_in6 ipv6;
    memcpy(&ipv6, &address, sizeof(ipv6));
    out->family = CNET_DATAGRAM_ADDRESS_IPV6;
    out->scope_id = ipv6.sin6_scope_id;
    memcpy(out->address, &ipv6.sin6_addr, sizeof(ipv6.sin6_addr));
  }
  return SALTS_OK;
}

int flowmq_cnet_ipc_listener_close(cnet_ipc_listener *listener, uint32_t timeout_ms) {
  const uint64_t started = monotonic_ms();
  int first_error = SALTS_OK;
  for (;;) {
    bool stopped = false;
    size_t events;
    int status = cnet_ipc_listener_stop(listener, &stopped);
    if (first_error == SALTS_OK && status != SALTS_OK && status != SALTS_EBUSY)
      first_error = status;
    if (stopped) {
      const int destroy_status = cnet_ipc_listener_destroy(listener);
      return first_error == SALTS_OK ? destroy_status : first_error;
    }
    if (monotonic_ms() - started >= timeout_ms) return SALTS_ETIMEDOUT;
    status = cnet_ipc_listener_advance(listener, 1u, &events);
    if (first_error == SALTS_OK && status != SALTS_OK) first_error = status;
    if (events == 0u) sleep_ms(1u);
  }
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
  if (flowmq_cnet_uses_tls(transport) && handshake_timeout_ms == 0u) return SALTS_EINVAL;
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
      .tls_io_buffer_bytes = flowmq_cnet_uses_tls(transport) ? io->tls_io_buffer_bytes : 0u,
      .tls_handshake_timeout_ms =
          flowmq_cnet_uses_tls(transport) ? handshake_timeout_ms : 0u};
  return native_io_backend_kind_supported(out->backend) ? SALTS_OK : SALTS_ENOTSUP;
}
