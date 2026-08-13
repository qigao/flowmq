#ifndef FLOWMQ_SECURITY_H
#define FLOWMQ_SECURITY_H

#include "CoroNet/turbo_coro_socket.h"
#include "flowmq_protocol.h"
#include "turbo_flow_fmq.h"

typedef struct flowmq_security_binding_runtime_s {
  int enabled;
  turbo_flow_fmq_endpoint_mode_t mode;
  turbo_flow_fmq_transport_t transport;
  tstr_t realm_channel;
  tstr_t auth_method;
  tstr_t secret_reference;
  turbo_flow_security_auth_provider_t auth_provider;
  turbo_flow_security_key_provider_t key_provider;
  turbo_flow_security_realm_t *realm;
  int (*verify_peer_certificate_identity)(void *ctx,
                                          const char *certificate_sha256,
                                          const char *claimed_identity);
  void *peer_certificate_identity_ctx;
} flowmq_security_binding_runtime_t;

int flowmq_security_binding_init(flowmq_security_binding_runtime_t *runtime,
                                 turbo_flow_fmq_endpoint_mode_t mode,
                                 turbo_flow_fmq_transport_t transport,
                                 const turbo_flow_fmq_security_binding_t *binding);
void flowmq_security_binding_destroy(flowmq_security_binding_runtime_t *runtime);
int flowmq_security_client_hello(flowmq_security_binding_runtime_t *runtime,
                                 coro_socket_t *socket, tstr_v identity, tstr_t *payload);
int flowmq_security_client_accept(flowmq_security_binding_runtime_t *runtime,
                                  coro_socket_t *socket, tstr_v payload);
int flowmq_security_server_authenticate(
    flowmq_security_binding_runtime_t *runtime, coro_socket_t *socket, tstr_v claimed_identity,
    tstr_v payload, const char *connection_resource,
    turbo_flow_security_principal_t *principal_out);
int flowmq_security_server_accept(flowmq_security_binding_runtime_t *runtime,
                                  coro_socket_t *socket, tstr_t *payload);
int flowmq_security_authorize(flowmq_security_binding_runtime_t *runtime,
                              const turbo_flow_security_principal_t *principal, uint32_t action,
                              turbo_flow_security_resource_type_t resource_type,
                              const char *resource, const void *protocol_context);
void flowmq_security_clear(void *data, size_t size);

#endif /* FLOWMQ_SECURITY_H */
