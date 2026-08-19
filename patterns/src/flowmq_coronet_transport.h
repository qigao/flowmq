#ifndef FLOWMQ_CORONET_TRANSPORT_H
#define FLOWMQ_CORONET_TRANSPORT_H

#include "CoroNet/turbo_coro_socket.h"
#include "flowmq_coronet.h"

int flowmq_coronet_transport_validate(flowmq_coronet_transport_t transport);
int flowmq_coronet_endpoint_validate(flowmq_coronet_transport_t transport, const char *host,
                                     int port, const char *path);
int flowmq_coronet_connect_config_validate(
    flowmq_coronet_transport_t transport, const flowmq_coronet_timeout_config_t *timeouts,
    const flowmq_coronet_socket_options_t *socket_options,
    const flowmq_coronet_udp_options_t *udp_options,
    const flowmq_coronet_tls_client_config_t *tls);
int flowmq_coronet_server_config_validate(
    flowmq_coronet_transport_t transport, const flowmq_coronet_timeout_config_t *timeouts,
    const flowmq_coronet_socket_options_t *socket_options,
    const flowmq_coronet_udp_options_t *udp_options,
    const flowmq_coronet_tls_server_config_t *tls);
int flowmq_coronet_transport_kcp_resolve(
    flowmq_coronet_transport_t transport,
    const flowmq_coronet_kcp_options_t *options, turbo_kcp_config_t *config,
    int *configured);
coro_socket_t *flowmq_coronet_transport_create(coro_context_t *ctx,
                                               flowmq_coronet_transport_t transport, int listener);
int flowmq_coronet_transport_apply(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                   const turbo_kcp_config_t *kcp_config,
                                   int kcp_configured,
                                   const flowmq_coronet_socket_options_t *socket_options);
int flowmq_coronet_transport_connect(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                     const char *host, int port, const char *path,
                                     const char *server_name,
                                     const flowmq_coronet_timeout_config_t *timeouts,
                                     const flowmq_coronet_udp_options_t *udp_options);
int flowmq_coronet_transport_listen(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                    const char *host, int port, const char *path,
                                    const flowmq_coronet_timeout_config_t *timeouts,
                                    const flowmq_coronet_udp_options_t *udp_options, int reuse_port,
                                    coro_handler_fn handler, void *ctx);
int flowmq_coronet_transport_listen_ex(
    coro_socket_t *socket, flowmq_coronet_transport_t transport, const char *host, int port,
    const char *path, const flowmq_coronet_timeout_config_t *timeouts,
    const flowmq_coronet_udp_options_t *udp_options, int reuse_port, coro_handler_fn handler,
    void *ctx, coro_handler_closed_fn handler_closed, void *handler_closed_ctx);
int flowmq_coronet_transport_send(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                  const flowmq_coronet_timeout_config_t *timeouts,
                                  const char *data, size_t len);
int flowmq_coronet_transport_sendv(coro_socket_t *socket, flowmq_coronet_transport_t transport,
                                   const flowmq_coronet_timeout_config_t *timeouts,
                                   const turbo_iovec_t *iov, size_t iovcnt);
int flowmq_coronet_transport_leave_multicast(coro_socket_t *socket,
                                             flowmq_coronet_transport_t transport,
                                             const flowmq_coronet_udp_options_t *udp_options);

#endif /* FLOWMQ_CORONET_TRANSPORT_H */
