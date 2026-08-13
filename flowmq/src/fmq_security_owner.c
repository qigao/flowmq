#include "turbo_flow_fmq_security.h"

#include "turbo_error.h"
#include "turbo_str.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_fmq_security_owner_s {
  tstr_t realm_channel;
  tstr_t auth_method;
  tstr_t secret_reference;
  turbo_flow_security_key_provider_t key_provider;
  turbo_flow_security_auth_provider_owner_t auth_provider;
  turbo_flow_security_policy_provider_owner_t policy_provider;
  turbo_flow_security_realm_t *realm;
  turbo_flow_fmq_security_binding_t binding;
};

static int flow_fmq_security_owner_error(turbo_flow_config_error_t *error, int status,
                                         const char *adapter_name, const char *field,
                                         const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.config%s%s", adapter_name,
                   field ? "." : "", field ? field : "");
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_fmq_security_owner_optional_string(const turbo_flow_resolved_adapter_view_t *view,
                                                   const char *field, const char **value,
                                                   turbo_flow_config_error_t *error,
                                                   const char *adapter_name) {
  int rc = turbo_flow_resolved_adapter_get_string(view, field, value);
  if (rc == TURBO_ENOENT) return TURBO_OK;
  if (rc != TURBO_OK || !*value || !(*value)[0])
    return flow_fmq_security_owner_error(error, rc == TURBO_OK ? TURBO_EINVAL : rc, adapter_name,
                                         field, "security reference must be a non-empty string");
  return TURBO_OK;
}

int turbo_flow_fmq_security_owner_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_security_owner_config_t *config, turbo_flow_fmq_security_owner_t **out,
    turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  turbo_flow_fmq_security_owner_t *owner = NULL;
  const char *mode = NULL;
  const char *realm_channel = NULL;
  const char *auth_provider_channel = NULL;
  const char *auth_method = NULL;
  const char *secret_reference = NULL;
  const char *policy_source;
  int bind_mode;
  int rc;
  if (out) *out = NULL;
  if (!resolved || !adapter_name || !adapter_name[0] || !config || config->size < sizeof(*config) ||
      config->api_version != TURBO_FLOW_FMQ_SECURITY_OWNER_API_VERSION || !out || !error ||
      error->size < sizeof(*error) || !config->key_provider ||
      config->key_provider->size < sizeof(*config->key_provider) ||
      !config->key_provider->acquire || !config->key_provider->release)
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  rc = turbo_flow_resolved_config_adapter(resolved, adapter_name, &view);
  if (rc != TURBO_OK || strcmp(view.kind, "fmq") != 0)
    return flow_fmq_security_owner_error(error, rc == TURBO_OK ? TURBO_EINVAL : rc, adapter_name,
                                         NULL, "adapter must be a resolved FMQ endpoint");
  rc = turbo_flow_resolved_adapter_get_string(&view, "mode", &mode);
  if (rc != TURBO_OK || (strcmp(mode, "bind") != 0 && strcmp(mode, "connect") != 0))
    return flow_fmq_security_owner_error(error, rc == TURBO_OK ? TURBO_EINVAL : rc, adapter_name,
                                         "mode", "secure FMQ mode must be bind or connect");
  bind_mode = strcmp(mode, "bind") == 0;
  rc = flow_fmq_security_owner_optional_string(&view, "security_realm", &realm_channel, error,
                                               adapter_name);
  if (rc == TURBO_OK)
    rc = flow_fmq_security_owner_optional_string(&view, "auth_provider", &auth_provider_channel,
                                                 error, adapter_name);
  if (rc == TURBO_OK)
    rc = flow_fmq_security_owner_optional_string(&view, "auth_method", &auth_method, error,
                                                 adapter_name);
  if (rc == TURBO_OK)
    rc = flow_fmq_security_owner_optional_string(&view, "secret_reference", &secret_reference,
                                                 error, adapter_name);
  if (rc != TURBO_OK) return rc;
  if (bind_mode) {
    if (!realm_channel || !auth_provider_channel || !auth_method || secret_reference)
      return flow_fmq_security_owner_error(
          error, TURBO_EINVAL, adapter_name, NULL,
          "BIND security requires security_realm, auth_provider, and auth_method only");
    if (!config->auth_provider_factories || config->auth_provider_factory_count == 0u ||
        !config->policy_provider_factories || config->policy_provider_factory_count == 0u)
      return flow_fmq_security_owner_error(error, TURBO_ENOTSUP, adapter_name, NULL,
                                           "BIND security provider registries are empty");
  } else if (realm_channel || auth_provider_channel || !auth_method || !secret_reference) {
    return flow_fmq_security_owner_error(
        error, TURBO_EINVAL, adapter_name, NULL,
        "CONNECT security requires auth_method and secret_reference only");
  }
  owner = (turbo_flow_fmq_security_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) return TURBO_ENOMEM;
  owner->auth_provider =
      (turbo_flow_security_auth_provider_owner_t)TURBO_FLOW_SECURITY_AUTH_PROVIDER_OWNER_INIT;
  owner->policy_provider =
      (turbo_flow_security_policy_provider_owner_t)TURBO_FLOW_SECURITY_POLICY_PROVIDER_OWNER_INIT;
  owner->binding = (turbo_flow_fmq_security_binding_t)TURBO_FLOW_FMQ_SECURITY_BINDING_INIT;
  owner->key_provider = *config->key_provider;
  owner->realm_channel = realm_channel ? tstr_dup(realm_channel) : NULL;
  owner->auth_method = tstr_dup(auth_method);
  owner->secret_reference = secret_reference ? tstr_dup(secret_reference) : NULL;
  if ((realm_channel && !owner->realm_channel) || !owner->auth_method ||
      (secret_reference && !owner->secret_reference)) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  owner->binding.auth_method = owner->auth_method;
  if (!bind_mode) {
    owner->binding.key_provider = &owner->key_provider;
    owner->binding.secret_reference = owner->secret_reference;
    *out = owner;
    return TURBO_OK;
  }
  rc = turbo_flow_security_realm_create_resolved(resolved, realm_channel, NULL, &owner->realm,
                                                 error);
  if (rc != TURBO_OK) goto fail;
  policy_source = turbo_flow_security_realm_policy_source(owner->realm);
  rc = turbo_flow_security_policy_provider_owner_create_registered(
      config->policy_provider_factories, config->policy_provider_factory_count, resolved,
      policy_source, config->key_provider, &owner->policy_provider, error);
  if (rc != TURBO_OK) goto fail;
  if (owner->policy_provider.provider) {
    rc = turbo_flow_security_realm_bind_policy_provider(owner->realm,
                                                        owner->policy_provider.provider);
  } else {
    rc = turbo_flow_security_realm_bind_authorization_provider(
        owner->realm, owner->policy_provider.authorization_provider);
  }
  if (rc != TURBO_OK) {
    flow_fmq_security_owner_error(error, rc, adapter_name, "security_realm",
                                  "failed to bind ACL provider to realm");
    goto fail;
  }
  rc = turbo_flow_security_auth_provider_owner_create_registered(
      config->auth_provider_factories, config->auth_provider_factory_count, resolved,
      auth_provider_channel, config->key_provider, &owner->auth_provider, error);
  if (rc != TURBO_OK) goto fail;
  if (strcmp(owner->auth_provider.method, auth_method) != 0) {
    rc = flow_fmq_security_owner_error(error, TURBO_EINVAL, adapter_name, "auth_method",
                                       "FMQ auth_method does not match auth provider method");
    goto fail;
  }
  owner->binding.realm_channel = owner->realm_channel;
  owner->binding.auth_provider = owner->auth_provider.provider;
  owner->binding.realm = owner->realm;
  *out = owner;
  return TURBO_OK;

fail:
  turbo_flow_fmq_security_owner_destroy(owner);
  return rc;
}

const turbo_flow_fmq_security_binding_t *
turbo_flow_fmq_security_owner_binding(const turbo_flow_fmq_security_owner_t *owner) {
  return owner ? &owner->binding : NULL;
}

void turbo_flow_fmq_security_owner_destroy(turbo_flow_fmq_security_owner_t *owner) {
  if (!owner) return;
  turbo_flow_security_auth_provider_owner_destroy(&owner->auth_provider);
  turbo_flow_security_realm_destroy(owner->realm);
  turbo_flow_security_policy_provider_owner_destroy(&owner->policy_provider);
  tstr_freep(&owner->realm_channel);
  tstr_freep(&owner->auth_method);
  tstr_freep(&owner->secret_reference);
  memset(&owner->key_provider, 0, sizeof(owner->key_provider));
  memset(&owner->binding, 0, sizeof(owner->binding));
  free(owner);
}
