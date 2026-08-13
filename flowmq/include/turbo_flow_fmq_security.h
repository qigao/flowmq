#ifndef TURBO_FLOW_FMQ_SECURITY_H
#define TURBO_FLOW_FMQ_SECURITY_H

#include "turbo_flow_fmq.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_FMQ_SECURITY_OWNER_API_VERSION 1u

typedef struct turbo_flow_fmq_security_owner_s turbo_flow_fmq_security_owner_t;

/**
 * Product composition inputs for one secure FMQ endpoint.
 *
 * BIND endpoints select exact auth and ACL provider factories from resolved channel metadata.
 * CONNECT endpoints use only key_provider and the adapter's secret_reference. All dependencies
 * are borrowed during creation; the key provider context must outlive the resulting owner.
 */
typedef struct turbo_flow_fmq_security_owner_config_s {
  size_t size;
  uint32_t api_version;
  const turbo_flow_security_key_provider_t *key_provider;
  const turbo_flow_security_auth_provider_factory_t *const *auth_provider_factories;
  size_t auth_provider_factory_count;
  const turbo_flow_security_policy_provider_factory_t *const *policy_provider_factories;
  size_t policy_provider_factory_count;
} turbo_flow_fmq_security_owner_config_t;

#define TURBO_FLOW_FMQ_SECURITY_OWNER_CONFIG_INIT                                                  \
  {sizeof(turbo_flow_fmq_security_owner_config_t),                                                 \
   TURBO_FLOW_FMQ_SECURITY_OWNER_API_VERSION,                                                      \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u}

/**
 * Create all host-owned security capabilities referenced by one FMQ adapter.
 *
 * BIND requires security_realm, auth_provider, and auth_method. CONNECT requires auth_method and
 * secret_reference. Partial or mixed configurations fail closed. YAML contains references only;
 * ACL rules, credentials, and provider pointers are never accepted there.
 */
CXX_C_API int turbo_flow_fmq_security_owner_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_security_owner_config_t *config, turbo_flow_fmq_security_owner_t **out,
    turbo_flow_config_error_t *error);

/** Borrowed binding valid until owner destruction. */
CXX_C_API const turbo_flow_fmq_security_binding_t *
turbo_flow_fmq_security_owner_binding(const turbo_flow_fmq_security_owner_t *owner);

/**
 * Destroy after the FMQ adapter/application has stopped and released its borrowed realm/provider.
 */
CXX_C_API void turbo_flow_fmq_security_owner_destroy(turbo_flow_fmq_security_owner_t *owner);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_SECURITY_H */
