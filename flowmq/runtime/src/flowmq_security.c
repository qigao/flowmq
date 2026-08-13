#include "flowmq_security.h"

#include "turbo_error.h"

#include <string.h>
#include <time.h>

_Static_assert(CORO_TLS_CHANNEL_BINDING_SIZE == FLOWMQ_PROTOCOL_CHANNEL_BINDING_SIZE,
               "FMQ and CoroNet channel binding sizes must match");

static int flowmq_security_text_equals(tstr_v left, const char *right) {
  size_t right_len;
  if (!right) return 0;
  right_len = strlen(right);
  return left.len == right_len && (right_len == 0u || memcmp(left.data, right, right_len) == 0);
}

static int flowmq_security_bytes_equal(tstr_v left, const uint8_t *right, size_t right_size) {
  volatile unsigned char difference = 0u;
  if (!right || left.len != right_size || (left.len != 0u && !left.data)) return 0;
  for (size_t i = 0u; i < right_size; ++i)
    difference |= (unsigned char)left.data[i] ^ right[i];
  return difference == 0u;
}

void flowmq_security_clear(void *data, size_t size) {
  volatile unsigned char *bytes = (volatile unsigned char *)data;
  if (!data) return;
  while (size != 0u) {
    *bytes++ = 0u;
    --size;
  }
}

static int flowmq_security_binding_validate(turbo_flow_fmq_endpoint_mode_t mode,
                                            turbo_flow_fmq_transport_t transport,
                                            const turbo_flow_fmq_security_binding_t *binding) {
  if (!binding || binding->size < sizeof(*binding) || !binding->auth_method ||
      binding->auth_method[0] == '\0' ||
      strlen(binding->auth_method) > FLOWMQ_PROTOCOL_MAX_AUTH_METHOD_SIZE) {
    return TURBO_EINVAL;
  }
  switch (transport) {
  case TURBO_FLOW_FMQ_TCP:
  case TURBO_FLOW_FMQ_TLS:
  case TURBO_FLOW_FMQ_UDP:
  case TURBO_FLOW_FMQ_KCP:
  case TURBO_FLOW_FMQ_PIPE:
  case TURBO_FLOW_FMQ_WS:
  case TURBO_FLOW_FMQ_WSS:
    break;
  default:
    return TURBO_EINVAL;
  }
  if (mode == TURBO_FLOW_FMQ_BIND) {
    if (!binding->realm_channel || binding->realm_channel[0] == '\0' ||
        strlen(binding->realm_channel) > TURBO_FLOW_RESOURCE_UID_MAX || !binding->auth_provider ||
        binding->auth_provider->size < sizeof(*binding->auth_provider) ||
        !binding->auth_provider->authenticate || !binding->realm || binding->key_provider ||
        binding->secret_reference ||
        (!binding->verify_peer_certificate_identity && binding->peer_certificate_identity_ctx)) {
      return TURBO_EINVAL;
    }
    if (binding->verify_peer_certificate_identity &&
        transport != TURBO_FLOW_FMQ_TLS && transport != TURBO_FLOW_FMQ_WSS) {
      return TURBO_ENOTSUP;
    }
    return TURBO_OK;
  }
  if (mode == TURBO_FLOW_FMQ_CONNECT) {
    if (binding->realm_channel || binding->auth_provider || binding->realm ||
        !binding->key_provider || binding->key_provider->size < sizeof(*binding->key_provider) ||
        !binding->key_provider->acquire || !binding->key_provider->release ||
        !binding->secret_reference || binding->secret_reference[0] == '\0' ||
        strlen(binding->secret_reference) > TURBO_FLOW_SECURITY_SECRET_REF_MAX ||
        binding->verify_peer_certificate_identity ||
        binding->peer_certificate_identity_ctx) {
      return TURBO_EINVAL;
    }
    return TURBO_OK;
  }
  return TURBO_EINVAL;
}

int flowmq_security_binding_init(flowmq_security_binding_runtime_t *runtime,
                                 turbo_flow_fmq_endpoint_mode_t mode,
                                 turbo_flow_fmq_transport_t transport,
                                 const turbo_flow_fmq_security_binding_t *binding) {
  int rc;
  if (!runtime) return TURBO_EINVAL;
  memset(runtime, 0, sizeof(*runtime));
  if (!binding) return TURBO_OK;
  rc = flowmq_security_binding_validate(mode, transport, binding);
  if (rc != TURBO_OK) return rc;
  runtime->realm_channel =
      binding->realm_channel ? tstr_dup(binding->realm_channel) : tstr_new_len(NULL, 0u);
  runtime->auth_method = tstr_dup(binding->auth_method);
  runtime->secret_reference =
      binding->secret_reference ? tstr_dup(binding->secret_reference) : tstr_new_len(NULL, 0u);
  if (!runtime->realm_channel || !runtime->auth_method || !runtime->secret_reference) {
    flowmq_security_binding_destroy(runtime);
    return TURBO_ENOMEM;
  }
  runtime->mode = mode;
  runtime->transport = transport;
  runtime->realm = binding->realm;
  runtime->verify_peer_certificate_identity =
      binding->verify_peer_certificate_identity;
  runtime->peer_certificate_identity_ctx =
      binding->peer_certificate_identity_ctx;
  if (binding->auth_provider) runtime->auth_provider = *binding->auth_provider;
  if (binding->key_provider) runtime->key_provider = *binding->key_provider;
  runtime->enabled = 1;
  return TURBO_OK;
}

static int flowmq_security_get_channel_binding(flowmq_security_binding_runtime_t *runtime,
                                               coro_socket_t *socket, uint8_t *binding,
                                               tstr_v *binding_view) {
  int rc;
  if (!runtime || !socket || !binding || !binding_view) return TURBO_EINVAL;
  *binding_view = tstr_v_from_buf(NULL, 0u);
  if (runtime->transport != TURBO_FLOW_FMQ_TLS && runtime->transport != TURBO_FLOW_FMQ_WSS) {
    return TURBO_OK;
  }
  rc = coro_socket_tls_export_channel_binding(socket, binding);
  if (rc != TURBO_OK) return rc;
  *binding_view = tstr_v_from_buf((const char *)binding, CORO_TLS_CHANNEL_BINDING_SIZE);
  return TURBO_OK;
}

void flowmq_security_binding_destroy(flowmq_security_binding_runtime_t *runtime) {
  if (!runtime) return;
  tstr_freep(&runtime->realm_channel);
  tstr_freep(&runtime->auth_method);
  tstr_freep(&runtime->secret_reference);
  memset(runtime, 0, sizeof(*runtime));
}

int flowmq_security_client_hello(flowmq_security_binding_runtime_t *runtime, coro_socket_t *socket,
                                 tstr_v identity, tstr_t *payload) {
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  flowmq_protocol_security_t security;
  uint8_t binding[CORO_TLS_CHANNEL_BINDING_SIZE];
  tstr_v binding_view;
  int rc;
  if (!runtime || !runtime->enabled || runtime->mode != TURBO_FLOW_FMQ_CONNECT || !socket ||
      !payload || *payload || identity.len == 0u || !identity.data) {
    return TURBO_EINVAL;
  }
  rc = flowmq_security_get_channel_binding(runtime, socket, binding, &binding_view);
  if (rc != TURBO_OK) return rc;
  rc =
      turbo_flow_security_secret_acquire(&runtime->key_provider, runtime->secret_reference, &lease);
  if (rc != TURBO_OK) goto done;
  if (!lease.bytes || lease.byte_count == 0u ||
      lease.byte_count > FLOWMQ_PROTOCOL_MAX_AUTH_SECRET_SIZE) {
    rc = lease.byte_count > FLOWMQ_PROTOCOL_MAX_AUTH_SECRET_SIZE ? TURBO_EMSGSIZE : TURBO_EPERM;
    goto done;
  }
  memset(&security, 0, sizeof(security));
  security.mode = FLOWMQ_PROTOCOL_SECURITY_AUTH;
  security.identity = identity;
  security.method = tstr_to_v(runtime->auth_method);
  security.secret = tstr_v_from_buf((const char *)lease.bytes, lease.byte_count);
  security.channel_binding = binding_view;
  rc = flowmq_protocol_security_encode(&security, payload);

done:
  turbo_flow_security_secret_release(&runtime->key_provider, &lease);
  flowmq_security_clear(binding, sizeof(binding));
  return rc;
}

int flowmq_security_client_accept(flowmq_security_binding_runtime_t *runtime, coro_socket_t *socket,
                                  tstr_v payload) {
  flowmq_protocol_security_t security;
  uint8_t binding[CORO_TLS_CHANNEL_BINDING_SIZE];
  tstr_v binding_view;
  int rc;
  if (!runtime || !runtime->enabled || runtime->mode != TURBO_FLOW_FMQ_CONNECT || !socket)
    return TURBO_EINVAL;
  rc = flowmq_protocol_security_decode(payload, &security);
  if (rc != TURBO_OK || security.mode != FLOWMQ_PROTOCOL_SECURITY_ACCEPTED) return TURBO_EPERM;
  rc = flowmq_security_get_channel_binding(runtime, socket, binding, &binding_view);
  if (rc == TURBO_OK &&
      !flowmq_security_bytes_equal(security.channel_binding, binding, binding_view.len)) {
    rc = TURBO_EPERM;
  }
  flowmq_security_clear(binding, sizeof(binding));
  return rc;
}

int flowmq_security_authorize(flowmq_security_binding_runtime_t *runtime,
                              const turbo_flow_security_principal_t *principal, uint32_t action,
                              turbo_flow_security_resource_type_t resource_type,
                              const char *resource, const void *protocol_context) {
  turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
  turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
  time_t now;
  if (!runtime || !runtime->enabled || !principal || !resource || resource[0] == '\0' ||
      !runtime->realm) {
    return TURBO_EINVAL;
  }
  now = time(NULL);
  if (now < 0 || (principal->expires_at != 0u && now == 0)) return TURBO_EIO;
  request.principal = principal;
  request.domain_id = principal->domain_id;
  request.action = action;
  request.resource_type = resource_type;
  request.resource = resource;
  request.protocol_context = protocol_context;
  return turbo_flow_security_realm_authorize(runtime->realm, &request, (uint64_t)now, &decision);
}

int flowmq_security_server_authenticate(flowmq_security_binding_runtime_t *runtime,
                                        coro_socket_t *socket, tstr_v claimed_identity,
                                        tstr_v payload, const char *connection_resource,
                                        turbo_flow_security_principal_t *principal_out) {
  flowmq_protocol_security_t security;
  turbo_flow_security_auth_request_t request = TURBO_FLOW_SECURITY_AUTH_REQUEST_INIT;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  uint8_t binding[CORO_TLS_CHANNEL_BINDING_SIZE];
  char certificate_sha256[CORO_TLS_PEER_CERT_SHA256_CAPACITY] = {0};
  tstr_v binding_view;
  tstr_t identity = NULL;
  int rc;
  if (!runtime || !runtime->enabled || runtime->mode != TURBO_FLOW_FMQ_BIND || !socket ||
      !principal_out || !connection_resource || !connection_resource[0]) {
    return TURBO_EINVAL;
  }
  rc = flowmq_protocol_security_decode(payload, &security);
  if (rc != TURBO_OK || security.mode != FLOWMQ_PROTOCOL_SECURITY_AUTH ||
      !flowmq_security_text_equals(security.method, runtime->auth_method) ||
      security.identity.len != claimed_identity.len || claimed_identity.len == 0u ||
      memcmp(security.identity.data, claimed_identity.data, claimed_identity.len) != 0) {
    return TURBO_EPERM;
  }
  rc = flowmq_security_get_channel_binding(runtime, socket, binding, &binding_view);
  if (rc != TURBO_OK) return rc;
  if (!flowmq_security_bytes_equal(security.channel_binding, binding, binding_view.len)) {
    rc = TURBO_EPERM;
    goto done;
  }
  identity = tstr_from_v(security.identity);
  if (!identity) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  if (runtime->verify_peer_certificate_identity) {
    rc = coro_socket_tls_get_verified_peer_certificate_sha256(
        socket, certificate_sha256);
    if (rc != TURBO_OK) goto done;
    rc = runtime->verify_peer_certificate_identity(
        runtime->peer_certificate_identity_ctx, certificate_sha256, identity);
    if (rc != TURBO_OK) {
      rc = TURBO_EPERM;
      goto done;
    }
  }
  request.identity = identity;
  request.method = runtime->auth_method;
  request.secret = (const uint8_t *)security.secret.data;
  request.secret_size = security.secret.len;
  request.protocol = "fmq3";
  rc = turbo_flow_security_authenticate(&runtime->auth_provider, &request, &principal);
  if (rc != TURBO_OK) goto done;
  if (strcmp(principal.principal_id, identity) != 0) {
    rc = TURBO_EPERM;
    goto done;
  }
  rc = flowmq_security_authorize(runtime, &principal, TURBO_FLOW_SECURITY_ACTION_CONNECT,
                                 TURBO_FLOW_SECURITY_RESOURCE_GENERIC, connection_resource, NULL);
  if (rc == TURBO_OK) *principal_out = principal;

done:
  tstr_free(identity);
  flowmq_security_clear(certificate_sha256, sizeof(certificate_sha256));
  flowmq_security_clear(binding, sizeof(binding));
  return rc;
}

int flowmq_security_server_accept(flowmq_security_binding_runtime_t *runtime, coro_socket_t *socket,
                                  tstr_t *payload) {
  flowmq_protocol_security_t security;
  uint8_t binding[CORO_TLS_CHANNEL_BINDING_SIZE];
  tstr_v binding_view;
  int rc;
  if (!runtime || !runtime->enabled || runtime->mode != TURBO_FLOW_FMQ_BIND || !socket ||
      !payload || *payload) {
    return TURBO_EINVAL;
  }
  rc = flowmq_security_get_channel_binding(runtime, socket, binding, &binding_view);
  if (rc != TURBO_OK) return rc;
  memset(&security, 0, sizeof(security));
  security.mode = FLOWMQ_PROTOCOL_SECURITY_ACCEPTED;
  security.channel_binding = binding_view;
  rc = flowmq_protocol_security_encode(&security, payload);
  flowmq_security_clear(binding, sizeof(binding));
  return rc;
}
