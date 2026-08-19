#include "flowmq_coronet_transport.h"

#include "CoroNet/turbo_stream.h"
#include "turbo_error.h"

#include <limits.h>
#include <string.h>

_Static_assert(FLOWMQ_KCP_PSK_SIZE == TURBO_KCP_PSK_SIZE,
               "FlowMQ and CoroNet KCP key sizes must match");

typedef enum flowmq_timeout_kind_e {
  FLOWMQ_TIMEOUT_CONNECT,
  FLOWMQ_TIMEOUT_SEND,
  FLOWMQ_TIMEOUT_RECV
} flowmq_timeout_kind_t;

int flowmq_coronet_transport_validate(flowmq_coronet_transport_t transport) {
  return transport >= FLOWMQ_TRANSPORT_TCP && transport <= FLOWMQ_TRANSPORT_WSS ? TURBO_OK
                                                                                : TURBO_EINVAL;
}

int flowmq_coronet_tls_require_tls13(void) {
  return turbo_stream_tls_set_protocol_mode(TURBO_TLS_PROTOCOL_TLS13_ONLY);
}

int flowmq_coronet_tls_is_tls13_only(void) {
  return turbo_stream_tls_get_protocol_mode() == TURBO_TLS_PROTOCOL_TLS13_ONLY;
}

static int flowmq_coronet_transport_is_ws(flowmq_coronet_transport_t transport) {
  return transport == FLOWMQ_TRANSPORT_WS || transport == FLOWMQ_TRANSPORT_WSS;
}

static int flowmq_coronet_transport_is_tcp_backed(flowmq_coronet_transport_t transport) {
  return transport == FLOWMQ_TRANSPORT_TCP || transport == FLOWMQ_TRANSPORT_TLS ||
         flowmq_coronet_transport_is_ws(transport);
}

static int flowmq_coronet_transport_supports_send_hwm(flowmq_coronet_transport_t transport) {
  return flowmq_coronet_transport_is_tcp_backed(transport) || transport == FLOWMQ_TRANSPORT_PIPE;
}

int flowmq_coronet_endpoint_validate(flowmq_coronet_transport_t transport, const char *host,
                                     int port, const char *path) {
  if (flowmq_coronet_transport_validate(transport) != TURBO_OK) return TURBO_EINVAL;
  if (transport == FLOWMQ_TRANSPORT_PIPE) {
    return ((path && path[0] != '\0') || (host && host[0] != '\0')) ? TURBO_OK : TURBO_EINVAL;
  }
  return host && host[0] != '\0' && port >= 1 && port <= 65535 ? TURBO_OK : TURBO_EINVAL;
}

void flowmq_coronet_timeouts_resolve(flowmq_coronet_timeout_config_t *timeouts,
                                     uint64_t fallback_timeout_ms) {
  uint64_t default_timeout_ms;
  if (!timeouts) return;
  timeouts->explicit_flags = timeouts->set_flags;
  default_timeout_ms = (timeouts->set_flags & FLOWMQ_TIMEOUT_SET_DEFAULT)
                           ? timeouts->timeout_ms
                           : (timeouts->timeout_ms ? timeouts->timeout_ms : fallback_timeout_ms);
  timeouts->timeout_ms = default_timeout_ms;
  if (!(timeouts->set_flags & FLOWMQ_TIMEOUT_SET_CONNECT) && !timeouts->connect_timeout_ms)
    timeouts->connect_timeout_ms = default_timeout_ms;
  if (!(timeouts->set_flags & FLOWMQ_TIMEOUT_SET_SEND) && !timeouts->send_timeout_ms)
    timeouts->send_timeout_ms = default_timeout_ms;
  if (!(timeouts->set_flags & FLOWMQ_TIMEOUT_SET_RECV) && !timeouts->recv_timeout_ms)
    timeouts->recv_timeout_ms = default_timeout_ms;
  if (!(timeouts->set_flags & FLOWMQ_TIMEOUT_SET_HANDSHAKE) && !timeouts->handshake_timeout_ms)
    timeouts->handshake_timeout_ms = default_timeout_ms;
  timeouts->set_flags = FLOWMQ_TIMEOUT_SET_ALL;
}

static int flowmq_coronet_connection_timeout_resolve(
    flowmq_coronet_transport_t transport, const flowmq_coronet_timeout_config_t *timeouts,
    uint64_t *timeout_ms) {
  int connect_explicit;
  int handshake_explicit;
  if (flowmq_coronet_transport_validate(transport) != TURBO_OK || !timeouts || !timeout_ms)
    return TURBO_EINVAL;
  *timeout_ms = timeouts->connect_timeout_ms;
  connect_explicit = (timeouts->explicit_flags & FLOWMQ_TIMEOUT_SET_CONNECT) != 0;
  handshake_explicit = (timeouts->explicit_flags & FLOWMQ_TIMEOUT_SET_HANDSHAKE) != 0;
  if (transport != FLOWMQ_TRANSPORT_TLS && !flowmq_coronet_transport_is_ws(transport))
    return handshake_explicit ? TURBO_EINVAL : TURBO_OK;
  if (connect_explicit && handshake_explicit &&
      timeouts->connect_timeout_ms != timeouts->handshake_timeout_ms)
    return TURBO_EINVAL;
  if (handshake_explicit) *timeout_ms = timeouts->handshake_timeout_ms;
  return TURBO_OK;
}

static void flowmq_coronet_apply_timeout(coro_socket_t *socket,
                                         const flowmq_coronet_timeout_config_t *timeouts,
                                         flowmq_timeout_kind_t kind) {
  uint64_t timeout_ms;
  if (!socket || !timeouts) return;
  switch (kind) {
  case FLOWMQ_TIMEOUT_CONNECT:
    timeout_ms = timeouts->connect_timeout_ms;
    break;
  case FLOWMQ_TIMEOUT_SEND:
    timeout_ms = timeouts->send_timeout_ms;
    break;
  case FLOWMQ_TIMEOUT_RECV:
    timeout_ms = timeouts->recv_timeout_ms;
    break;
  default:
    timeout_ms = timeouts->timeout_ms;
    break;
  }
  (void)coro_socket_set_timeout(socket, timeout_ms);
}

static int flowmq_coronet_socket_options_validate(
    flowmq_coronet_transport_t transport, const flowmq_coronet_socket_options_t *options) {
  if (flowmq_coronet_transport_validate(transport) != TURBO_OK || !options) return TURBO_EINVAL;
  if (options->tcp_keepalive) {
    if (!flowmq_coronet_transport_is_tcp_backed(transport)) return TURBO_EINVAL;
    if (options->tcp_keepalive_idle_ms > UINT32_MAX ||
        options->tcp_keepalive_interval_ms > UINT32_MAX)
      return TURBO_ERANGE;
  } else if (options->tcp_keepalive_idle_ms || options->tcp_keepalive_interval_ms ||
             options->tcp_keepalive_count) {
    return TURBO_EINVAL;
  }
  if (options->linger) {
    if (!flowmq_coronet_transport_is_tcp_backed(transport)) return TURBO_EINVAL;
    if (options->linger_ms > UINT32_MAX) return TURBO_ERANGE;
  } else if (options->linger_ms) {
    return TURBO_EINVAL;
  }
  if (options->send_hwm_bytes && !flowmq_coronet_transport_supports_send_hwm(transport))
    return TURBO_EINVAL;
  if ((options->socket_recv_buffer_bytes || options->socket_send_buffer_bytes) &&
      !flowmq_coronet_transport_is_tcp_backed(transport))
    return TURBO_EINVAL;
  if (options->socket_recv_buffer_bytes > (size_t)INT_MAX ||
      options->socket_send_buffer_bytes > (size_t)INT_MAX)
    return TURBO_ERANGE;
  return TURBO_OK;
}

static int flowmq_coronet_udp_options_validate(flowmq_coronet_transport_t transport,
                                               const flowmq_coronet_udp_options_t *options) {
  int has_group;
  int has_interface;
  if (flowmq_coronet_transport_validate(transport) != TURBO_OK || !options) return TURBO_EINVAL;
  has_group = options->multicast_group && options->multicast_group[0] != '\0';
  has_interface = options->multicast_interface && options->multicast_interface[0] != '\0';
  if (options->option_flags & ~FLOWMQ_UDP_OPTION_ALL) return TURBO_EINVAL;
  if ((has_group || has_interface || options->option_flags) && transport != FLOWMQ_TRANSPORT_UDP)
    return TURBO_EINVAL;
  if (has_interface && !has_group) return TURBO_EINVAL;
  if (!(options->option_flags & FLOWMQ_UDP_OPTION_MULTICAST_LOOP) && options->multicast_loop)
    return TURBO_EINVAL;
  if (!(options->option_flags & FLOWMQ_UDP_OPTION_MULTICAST_TTL) && options->multicast_ttl)
    return TURBO_EINVAL;
  if (!(options->option_flags & FLOWMQ_UDP_OPTION_BROADCAST) && options->broadcast)
    return TURBO_EINVAL;
  if ((options->option_flags & FLOWMQ_UDP_OPTION_MULTICAST_LOOP) &&
      options->multicast_loop != 0 && options->multicast_loop != 1)
    return TURBO_EINVAL;
  if ((options->option_flags & FLOWMQ_UDP_OPTION_MULTICAST_TTL) && options->multicast_ttl > 255u)
    return TURBO_ERANGE;
  if ((options->option_flags & FLOWMQ_UDP_OPTION_BROADCAST) && options->broadcast != 0 &&
      options->broadcast != 1)
    return TURBO_EINVAL;
  return TURBO_OK;
}

int flowmq_coronet_connect_config_validate(
    flowmq_coronet_transport_t transport, const flowmq_coronet_timeout_config_t *timeouts,
    const flowmq_coronet_socket_options_t *socket_options,
    const flowmq_coronet_udp_options_t *udp_options,
    const flowmq_coronet_tls_client_config_t *tls) {
  uint64_t connection_timeout_ms;
  int rc;
  if (!timeouts || !socket_options || !udp_options ||
      (timeouts->set_flags & ~FLOWMQ_TIMEOUT_SET_ALL) ||
      (timeouts->explicit_flags & ~FLOWMQ_TIMEOUT_SET_ALL))
    return TURBO_EINVAL;
  if (tls && transport != FLOWMQ_TRANSPORT_TLS && transport != FLOWMQ_TRANSPORT_WSS)
    return TURBO_EINVAL;
  if (tls && tls->verify_peer != 0 && tls->verify_peer != 1) return TURBO_EINVAL;
  rc = flowmq_coronet_connection_timeout_resolve(transport, timeouts, &connection_timeout_ms);
  if (rc != TURBO_OK) return rc;
  rc = flowmq_coronet_socket_options_validate(transport, socket_options);
  if (rc != TURBO_OK) return rc;
  return flowmq_coronet_udp_options_validate(transport, udp_options);
}

int flowmq_coronet_server_config_validate(
    flowmq_coronet_transport_t transport, const flowmq_coronet_timeout_config_t *timeouts,
    const flowmq_coronet_socket_options_t *socket_options,
    const flowmq_coronet_udp_options_t *udp_options,
    const flowmq_coronet_tls_server_config_t *tls) {
  int secure = transport == FLOWMQ_TRANSPORT_TLS || transport == FLOWMQ_TRANSPORT_WSS;
  int rc;
  if (!timeouts || !socket_options || !udp_options ||
      (timeouts->set_flags & ~FLOWMQ_TIMEOUT_SET_ALL) ||
      (timeouts->explicit_flags & ~FLOWMQ_TIMEOUT_SET_ALL))
    return TURBO_EINVAL;
  if (secure) {
    if (!tls || !tls->cert_file || !tls->cert_file[0] || !tls->key_file ||
        !tls->key_file[0])
      return TURBO_EINVAL;
  } else if (tls) {
    return TURBO_EINVAL;
  }
  if (tls && tls->require_client_certificate != 0 &&
      tls->require_client_certificate != 1)
    return TURBO_EINVAL;
  if (tls && tls->require_client_certificate && (!tls->ca_file || !tls->ca_file[0]))
    return TURBO_EINVAL;
  rc = flowmq_coronet_socket_options_validate(transport, socket_options);
  if (rc != TURBO_OK) return rc;
  return flowmq_coronet_udp_options_validate(transport, udp_options);
}

int flowmq_coronet_transport_kcp_resolve(flowmq_coronet_transport_t transport,
                                         const flowmq_coronet_kcp_options_t *options,
                                         turbo_kcp_config_t *config, int *configured) {
  static const uint64_t KCP_MAX_FEC_STATE_BYTES = UINT64_C(64) * 1024u * 1024u;
  uint8_t key_bits = 0u;
  uint64_t fec_state_bytes;
  uint32_t total_shards;
  int has_options;
  size_t i;
  if (!options || !config || !configured) return TURBO_EINVAL;
  turbo_kcp_config_default(config);
  *configured = 0;
  for (i = 0u; i < FLOWMQ_KCP_PSK_SIZE; ++i) key_bits |= options->pre_shared_key[i];
  has_options = key_bits || options->mtu || options->send_window || options->receive_window ||
                options->interval_ms || options->handshake_retry_ms || options->fast_resend ||
                options->no_congestion_window || options->data_shards || options->parity_shards ||
                options->max_payload_size || options->receive_group_count;
  if (transport != FLOWMQ_TRANSPORT_KCP) return has_options ? TURBO_EINVAL : TURBO_OK;
  if (!key_bits || options->mtu < 576u || options->mtu > UINT16_MAX ||
      !options->send_window || options->send_window > UINT16_MAX || !options->receive_window ||
      options->receive_window > UINT16_MAX || !options->interval_ms ||
      options->interval_ms > 100u || !options->handshake_retry_ms ||
      options->handshake_retry_ms > UINT16_MAX || options->fast_resend > UINT8_MAX ||
      (options->no_congestion_window != 0 && options->no_congestion_window != 1) ||
      !options->data_shards || options->data_shards > 255u || !options->parity_shards ||
      options->parity_shards > 255u ||
      options->max_payload_size < options->mtu + TURBO_KCP_SECURE_RECORD_OVERHEAD ||
      options->max_payload_size > UINT16_MAX || !options->receive_group_count ||
      options->receive_group_count > 64u)
    return TURBO_EINVAL;
  total_shards = options->data_shards + options->parity_shards;
  if (total_shards > 255u) return TURBO_EINVAL;
  fec_state_bytes = (uint64_t)total_shards * ((uint64_t)options->max_payload_size + 2u) *
                    options->receive_group_count;
  if (fec_state_bytes > KCP_MAX_FEC_STATE_BYTES) return TURBO_ERANGE;
  memcpy(config->pre_shared_key, options->pre_shared_key, TURBO_KCP_PSK_SIZE);
  config->mtu = (uint16_t)options->mtu;
  config->send_window = (uint16_t)options->send_window;
  config->receive_window = (uint16_t)options->receive_window;
  config->interval_ms = (uint16_t)options->interval_ms;
  config->handshake_retry_ms = (uint16_t)options->handshake_retry_ms;
  config->fast_resend = (uint8_t)options->fast_resend;
  config->no_congestion_window = (uint8_t)options->no_congestion_window;
  config->fec.backend = TURBO_KCP_FEC_BACKEND_REED_SOLOMON;
  config->fec.data_shards = (uint16_t)options->data_shards;
  config->fec.parity_shards = (uint16_t)options->parity_shards;
  config->fec.max_payload_size = (uint16_t)options->max_payload_size;
  config->fec.receive_group_count = (uint16_t)options->receive_group_count;
  *configured = 1;
  return TURBO_OK;
}

coro_socket_t *flowmq_coronet_transport_create(coro_context_t *ctx,
                                               flowmq_coronet_transport_t transport,
                                               int listener) {
  if (!ctx || (listener != 0 && listener != 1) ||
      flowmq_coronet_transport_validate(transport) != TURBO_OK)
    return NULL;
  switch (transport) {
  case FLOWMQ_TRANSPORT_TCP:
  case FLOWMQ_TRANSPORT_WS:
    return coro_socket_create_tcpv4(ctx);
  case FLOWMQ_TRANSPORT_TLS:
  case FLOWMQ_TRANSPORT_WSS:
    return listener && transport == FLOWMQ_TRANSPORT_WSS ? coro_socket_create_tcpv4(ctx)
                                                         : coro_socket_create(ctx, CORO_SOCKET_TLS);
  case FLOWMQ_TRANSPORT_UDP:
    return coro_socket_create_udpv4(ctx);
  case FLOWMQ_TRANSPORT_KCP:
    return coro_socket_create_kcp(ctx);
  case FLOWMQ_TRANSPORT_PIPE:
    return coro_socket_create_pipe(ctx);
  default:
    return NULL;
  }
}

int flowmq_coronet_transport_apply(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                   const turbo_kcp_config_t *kcp_config, int kcp_configured,
                                   const flowmq_coronet_socket_options_t *socket_options) {
  int rc;
  if (!socket || !socket_options || flowmq_coronet_transport_validate(transport) != TURBO_OK)
    return TURBO_EINVAL;
  if (kcp_configured) {
    if (transport != FLOWMQ_TRANSPORT_KCP || !kcp_config) return TURBO_EINVAL;
    rc = coro_socket_set_kcp_config(socket, kcp_config);
    if (rc != TURBO_OK) return rc;
  }
  rc = flowmq_coronet_socket_options_validate(transport, socket_options);
  if (rc != TURBO_OK) return rc;
  if (socket_options->tcp_keepalive) {
    const turbo_tcp_keepalive_config_t keepalive = {
        1, (uint32_t)socket_options->tcp_keepalive_idle_ms,
        (uint32_t)socket_options->tcp_keepalive_interval_ms,
        socket_options->tcp_keepalive_count};
    rc = coro_socket_set_tcp_keepalive(socket, &keepalive);
    if (rc != TURBO_OK) return rc;
  }
  if (socket_options->linger) {
    const turbo_socket_linger_config_t linger = {1, (uint32_t)socket_options->linger_ms};
    rc = coro_socket_set_linger(socket, &linger);
    if (rc != TURBO_OK) return rc;
  }
  if (socket_options->send_hwm_bytes) {
    rc = coro_socket_set_send_hwm(socket, socket_options->send_hwm_bytes);
    if (rc != TURBO_OK) return rc;
  }
  if (socket_options->socket_recv_buffer_bytes) {
    rc = coro_socket_set_recv_buffer_size(socket, socket_options->socket_recv_buffer_bytes);
    if (rc != TURBO_OK) return rc;
  }
  if (socket_options->socket_send_buffer_bytes) {
    rc = coro_socket_set_send_buffer_size(socket, socket_options->socket_send_buffer_bytes);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flowmq_coronet_apply_udp_options(coro_socket_t *socket,
                                            flowmq_coronet_transport_t transport,
                                            const flowmq_coronet_udp_options_t *options) {
  int rc = flowmq_coronet_udp_options_validate(transport, options);
  if (!socket) return TURBO_EINVAL;
  if (rc != TURBO_OK) return rc;
  if (options->option_flags & FLOWMQ_UDP_OPTION_MULTICAST_LOOP) {
    rc = coro_socket_set_multicast_loop(socket, options->multicast_loop);
    if (rc != TURBO_OK) return rc;
  }
  if (options->option_flags & FLOWMQ_UDP_OPTION_MULTICAST_TTL) {
    rc = coro_socket_set_multicast_ttl(socket, (int)options->multicast_ttl);
    if (rc != TURBO_OK) return rc;
  }
  if (options->option_flags & FLOWMQ_UDP_OPTION_BROADCAST)
    return coro_socket_set_broadcast(socket, options->broadcast);
  return TURBO_OK;
}

static const char *flowmq_coronet_endpoint(const char *host, const char *path) {
  return path && path[0] != '\0' ? path : host;
}

static const char *flowmq_coronet_ws_path(const char *path) {
  return path && path[0] != '\0' ? path : "/";
}

int flowmq_coronet_transport_connect(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                     const char *host, int port, const char *path,
                                     const char *server_name,
                                     const flowmq_coronet_timeout_config_t *timeouts,
                                     const flowmq_coronet_udp_options_t *udp_options) {
  uint64_t timeout_ms = 0u;
  int rc;
  if (!socket || !timeouts || !udp_options ||
      flowmq_coronet_endpoint_validate(transport, host, port, path) != TURBO_OK)
    return TURBO_EINVAL;
  rc = flowmq_coronet_connection_timeout_resolve(transport, timeouts, &timeout_ms);
  if (rc != TURBO_OK) return rc;
  (void)coro_socket_set_timeout(socket, timeout_ms);
  switch (transport) {
  case FLOWMQ_TRANSPORT_TCP:
  case FLOWMQ_TRANSPORT_UDP:
  case FLOWMQ_TRANSPORT_KCP:
    rc = coro_socket_connect(socket, host, port);
    break;
  case FLOWMQ_TRANSPORT_TLS:
    rc = coro_socket_connect_host_ex(socket, host, port, server_name ? server_name : host);
    break;
  case FLOWMQ_TRANSPORT_PIPE:
    rc = coro_socket_connect_pipe(socket, flowmq_coronet_endpoint(host, path));
    break;
  case FLOWMQ_TRANSPORT_WS:
  case FLOWMQ_TRANSPORT_WSS:
    rc = coro_socket_connect_ws_host_ex(socket, host, port, server_name ? server_name : host,
                                        flowmq_coronet_ws_path(path),
                                        transport == FLOWMQ_TRANSPORT_WSS, NULL);
    break;
  default:
    rc = TURBO_ENOTSUP;
    break;
  }
  if (rc != TURBO_OK) return rc;
  rc = flowmq_coronet_apply_udp_options(socket, transport, udp_options);
  if (rc != TURBO_OK) return rc;
  flowmq_coronet_apply_timeout(socket, timeouts, FLOWMQ_TIMEOUT_RECV);
  return TURBO_OK;
}

int flowmq_coronet_transport_listen(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                    const char *host, int port, const char *path,
                                    const flowmq_coronet_timeout_config_t *timeouts,
                                    const flowmq_coronet_udp_options_t *udp_options, int reuse_port,
                                    coro_handler_fn handler, void *ctx) {
  return flowmq_coronet_transport_listen_ex(
      socket, transport, host, port, path, timeouts, udp_options, reuse_port, handler, ctx,
      NULL, NULL);
}

int flowmq_coronet_transport_listen_ex(
    coro_socket_t *socket, flowmq_coronet_transport_t transport, const char *host, int port,
    const char *path, const flowmq_coronet_timeout_config_t *timeouts,
    const flowmq_coronet_udp_options_t *udp_options, int reuse_port, coro_handler_fn handler,
    void *ctx, coro_handler_closed_fn handler_closed, void *handler_closed_ctx) {
  int rc;
  if (!socket || !timeouts || !udp_options || !handler ||
      flowmq_coronet_endpoint_validate(transport, host, port, path) != TURBO_OK)
    return TURBO_EINVAL;
  if (reuse_port) coro_socket_set_reuse_port(socket, 1);
  if (transport == FLOWMQ_TRANSPORT_UDP) {
    rc = coro_socket_set_udp_sessionized(socket, 1);
    if (rc != TURBO_OK) return rc;
  }
  flowmq_coronet_apply_timeout(socket, timeouts, FLOWMQ_TIMEOUT_RECV);
  if (flowmq_coronet_transport_is_ws(transport))
    rc = coro_socket_listen_ws_ex(socket, host, port, transport == FLOWMQ_TRANSPORT_WSS,
                                  handler, ctx, handler_closed, handler_closed_ctx);
  else if (transport == FLOWMQ_TRANSPORT_PIPE)
    rc = coro_socket_listen_on_ex(socket, flowmq_coronet_endpoint(host, path), 0, handler, ctx,
                                  handler_closed, handler_closed_ctx);
  else
    rc = coro_socket_listen_on_ex(socket, host, port, handler, ctx, handler_closed,
                                  handler_closed_ctx);
  if (rc != TURBO_OK) return rc;
  rc = flowmq_coronet_apply_udp_options(socket, transport, udp_options);
  if (rc != TURBO_OK || !udp_options->multicast_group || !udp_options->multicast_group[0]) return rc;
  return coro_socket_join_multicast(socket, udp_options->multicast_group,
                                    udp_options->multicast_interface);
}

int flowmq_coronet_transport_send(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                  const flowmq_coronet_timeout_config_t *timeouts,
                                  const char *data, size_t len) {
  if (!socket || !timeouts || !data || !len ||
      flowmq_coronet_transport_validate(transport) != TURBO_OK)
    return TURBO_EINVAL;
  flowmq_coronet_apply_timeout(socket, timeouts, FLOWMQ_TIMEOUT_SEND);
  /* FMQ v3 is binary even when carried by WebSocket. CoroNet's generic WS
   * send uses opcode BINARY; the text API would impose an invalid UTF-8 contract. */
  return coro_socket_send(socket, data, len);
}

int flowmq_coronet_transport_sendv(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                   const flowmq_coronet_timeout_config_t *timeouts,
                                   const turbo_iovec_t *iov, size_t iovcnt) {
  if (!socket || !timeouts || !iov || !iovcnt) return TURBO_EINVAL;
  if (transport != FLOWMQ_TRANSPORT_TCP) return TURBO_ENOTSUP;
  flowmq_coronet_apply_timeout(socket, timeouts, FLOWMQ_TIMEOUT_SEND);
  return coro_socket_sendv(socket, iov, iovcnt);
}

int flowmq_coronet_transport_leave_multicast(coro_socket_t *socket,
                                             flowmq_coronet_transport_t transport,
                                             const flowmq_coronet_udp_options_t *udp_options) {
  if (!socket || !udp_options) return TURBO_EINVAL;
  if (!udp_options->multicast_group || !udp_options->multicast_group[0]) return TURBO_OK;
  if (transport != FLOWMQ_TRANSPORT_UDP) return TURBO_EINVAL;
  return coro_socket_leave_multicast(socket, udp_options->multicast_group,
                                     udp_options->multicast_interface);
}
