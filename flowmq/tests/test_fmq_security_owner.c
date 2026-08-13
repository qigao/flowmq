#include "turbo_flow_fmq_security.h"
#include "turbo_flow_security_sqlite.h"
#ifdef FMQ_TEST_HAVE_HTTP_SECURITY
  #include "turbo_flow_http_acl.h"
  #include "turbo_flow_http_auth.h"
#endif

#include "tinytest.h"
#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct fmq_security_test_auth_s {
  turbo_flow_security_auth_provider_t provider;
} fmq_security_test_auth_t;

static int fmq_security_test_authenticate(void *ctx,
                                          const turbo_flow_security_auth_request_t *request,
                                          turbo_flow_security_principal_t *principal_out) {
  (void)ctx;
  if (!request || !principal_out) return TURBO_EINVAL;
  return TURBO_ENOTSUP;
}

static void fmq_security_test_auth_destroy(void *owner) { free(owner); }

static int fmq_security_test_auth_create(void *ctx, const turbo_flow_resolved_config_t *resolved,
                                         const char *channel_name,
                                         const turbo_flow_security_key_provider_t *key_provider,
                                         turbo_flow_security_auth_provider_owner_t *owner_out,
                                         turbo_flow_config_error_t *error) {
  fmq_security_test_auth_t *owner;
  (void)ctx;
  (void)resolved;
  (void)channel_name;
  (void)key_provider;
  (void)error;
  if (!owner_out || owner_out->size < sizeof(*owner_out)) return TURBO_EINVAL;
  owner = (fmq_security_test_auth_t *)calloc(1u, sizeof(*owner));
  if (!owner) return TURBO_ENOMEM;
  owner->provider = (turbo_flow_security_auth_provider_t)TURBO_FLOW_SECURITY_AUTH_PROVIDER_INIT;
  owner->provider.ctx = owner;
  owner->provider.authenticate = fmq_security_test_authenticate;
  owner_out->backend = "test";
  owner_out->method = "token";
  owner_out->provider = &owner->provider;
  owner_out->owner = owner;
  owner_out->destroy = fmq_security_test_auth_destroy;
  return TURBO_OK;
}

static const turbo_flow_security_auth_provider_factory_t *fmq_security_test_auth_factory(void) {
  static const turbo_flow_security_auth_provider_factory_t factory = {
      sizeof(factory), TURBO_FLOW_SECURITY_ABI_V3, "test", fmq_security_test_auth_create, NULL};
  return &factory;
}

static int fmq_security_test_secret_acquire(void *ctx, const char *reference,
                                            turbo_flow_security_secret_lease_t *lease_out) {
  static const uint8_t secret[] = "test-secret";
  (void)ctx;
  (void)reference;
  if (!lease_out || lease_out->size < sizeof(*lease_out)) return TURBO_EINVAL;
  *lease_out = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  lease_out->bytes = secret;
  lease_out->byte_count = sizeof(secret) - 1u;
  lease_out->provider_lease = (void *)secret;
  return TURBO_OK;
}

static void fmq_security_test_secret_release(void *ctx, turbo_flow_security_secret_lease_t *lease) {
  (void)ctx;
  if (lease) *lease = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
}

static turbo_flow_security_rule_t fmq_security_test_rule(turbo_flow_security_effect_t effect) {
  turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
  rule.effect = effect;
  rule.subject_kind = TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL;
  (void)snprintf(rule.subject, sizeof(rule.subject), "client-a");
  (void)snprintf(rule.domain_id, sizeof(rule.domain_id), "root-a");
  rule.action_mask = TURBO_FLOW_SECURITY_ACTION_CONNECT;
  rule.resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
  rule.match_kind = TURBO_FLOW_SECURITY_MATCH_EXACT;
  (void)snprintf(rule.pattern, sizeof(rule.pattern), "fmq:fmq.secure:connection");
  return rule;
}

static turbo_flow_security_principal_t fmq_security_test_principal(uint64_t policy_version) {
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  (void)snprintf(principal.principal_id, sizeof(principal.principal_id), "client-a");
  (void)snprintf(principal.principal_type, sizeof(principal.principal_type), "service");
  (void)snprintf(principal.domain_id, sizeof(principal.domain_id), "root-a");
  (void)snprintf(principal.auth_method, sizeof(principal.auth_method), "token");
  principal.scope = TURBO_FLOW_SECURITY_SCOPE_SELF;
  principal.group_count = 1u;
  (void)snprintf(principal.groups[0], sizeof(principal.groups[0]), "root-a");
  principal.policy_version = policy_version;
  return principal;
}

static void fmq_security_test_normalize_path(char *path) {
  if (!path) return;
  for (; *path; ++path)
    if (*path == '\\') *path = '/';
}

spec("FlowMQ dynamic security composition") {
  it("composes a SQLite ACL realm and refreshes exact policy generations") {
    char *database_path = tt_make_temp_file("flowmq-acl", ".sqlite3");
    char yaml[4096];
    turbo_flow_security_sqlite_config_t sqlite_config = TURBO_FLOW_SECURITY_SQLITE_CONFIG_INIT;
    turbo_flow_security_sqlite_provider_t *publisher = NULL;
    turbo_flow_security_rule_t allow_rule = fmq_security_test_rule(TURBO_FLOW_SECURITY_ALLOW);
    turbo_flow_security_rule_t deny_rule = fmq_security_test_rule(TURBO_FLOW_SECURITY_DENY);
    turbo_flow_security_policy_bundle_t bundle = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
    turbo_flow_security_key_provider_t key_provider = TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT;
    const turbo_flow_security_auth_provider_factory_t *auth_factories[] = {
        fmq_security_test_auth_factory()};
    const turbo_flow_security_policy_provider_factory_t *policy_factories[] = {
        turbo_flow_security_sqlite_provider_factory()};
    turbo_flow_fmq_security_owner_config_t owner_config = TURBO_FLOW_FMQ_SECURITY_OWNER_CONFIG_INIT;
    turbo_flow_fmq_security_owner_t *owner = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_security_principal_t principal = fmq_security_test_principal(1u);
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
    const turbo_flow_fmq_security_binding_t *binding;
    turbo_flow_t *flow;
    int written;

    check_not_null(database_path);
    fmq_security_test_normalize_path(database_path);
    sqlite_config.database_path = database_path;
    sqlite_config.namespace_name = "flowmq.fmq3";
    check_int_eq(turbo_flow_security_sqlite_provider_create(&sqlite_config, &publisher), TURBO_OK);
    bundle.policy_version = 1u;
    bundle.rules = &allow_rule;
    bundle.rule_count = 1u;
    check_int_eq(turbo_flow_security_sqlite_provider_publish(publisher, &bundle), TURBO_OK);
    written =
        snprintf(yaml, sizeof(yaml),
                 "version: 1\nchannels:\n  acl.fmq:\n    kind: acl_provider\n    config:\n"
                 "      backend: sqlite\n      database_path: %s\n"
                 "      namespace_name: flowmq.fmq3\n  auth.fmq:\n    kind: auth_provider\n"
                 "    config:\n      backend: test\n  security.fmq:\n    kind: security_realm\n"
                 "    config:\n      resource_uid: security:flowmq.fmq3\n"
                 "      owner_name: security.fmq\n      policy_source: acl.fmq\nadapters:\n"
                 "  fmq.secure:\n    kind: fmq\n    config:\n      pattern: pub\n"
                 "      mode: bind\n      transport: tcp\n      host: 127.0.0.1\n"
                 "      port: 17701\n      topic: secure\n      security_realm: security.fmq\n"
                 "      auth_provider: auth.fmq\n      auth_method: token\n",
                 database_path);
    check_int_gt(written, 0);
    check_true((size_t)written < sizeof(yaml));
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, (size_t)written, &resolved, &error),
                 TURBO_OK);

    key_provider.acquire = fmq_security_test_secret_acquire;
    key_provider.release = fmq_security_test_secret_release;
    owner_config.key_provider = &key_provider;
    owner_config.auth_provider_factories = auth_factories;
    owner_config.auth_provider_factory_count = 1u;
    owner_config.policy_provider_factories = policy_factories;
    owner_config.policy_provider_factory_count = 1u;
    check_int_eq(turbo_flow_fmq_security_owner_create_resolved(resolved, "fmq.secure",
                                                               &owner_config, &owner, &error),
                 TURBO_OK);
    binding = turbo_flow_fmq_security_owner_binding(owner);
    check_not_null(binding);
    check_not_null(binding->realm);

    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_fmq_register_resolved_adapter(flow, "fmq.secure", resolved, &error),
                 TURBO_EPERM);
    turbo_flow_destroy(flow);
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_fmq_register_resolved_secure_adapter(flow, "fmq.secure", resolved,
                                                                 binding, &error),
                 TURBO_OK);

    request.principal = &principal;
    request.domain_id = "root-a";
    request.action = TURBO_FLOW_SECURITY_ACTION_CONNECT;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
    request.resource = "fmq:fmq.secure:connection";
    check_int_eq(turbo_flow_security_realm_authorize(binding->realm, &request, 100u, &decision),
                 TURBO_OK);
    check_uint_eq(decision.policy_version, 1u);

    bundle.policy_version = 2u;
    bundle.rules = &deny_rule;
    check_int_eq(turbo_flow_security_sqlite_provider_publish(publisher, &bundle), TURBO_OK);
    principal.policy_version = 2u;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    check_int_eq(turbo_flow_security_realm_authorize(binding->realm, &request, 100u, &decision),
                 TURBO_EPERM);
    check_uint_eq(decision.policy_version, 2u);

    turbo_flow_destroy(flow);
    turbo_flow_fmq_security_owner_destroy(owner);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_security_sqlite_provider_destroy(publisher);
    check_int_eq(tt_remove_file(database_path), 0);
    free(database_path);
  }

  it("composes CONNECT credentials and rejects partial security metadata") {
    static const char yaml[] =
        "version: 1\nchannels: {}\nadapters:\n  fmq.client:\n    kind: fmq\n    config:\n"
        "      pattern: sub\n      mode: connect\n      transport: tcp\n"
        "      host: 127.0.0.1\n      port: 17701\n      identity: client-a\n"
        "      auth_method: token\n      secret_reference: secret/fmq-client\n";
    static const char partial_yaml[] =
        "version: 1\nchannels: {}\nadapters:\n  fmq.client:\n    kind: fmq\n    config:\n"
        "      pattern: sub\n      mode: connect\n      transport: tcp\n"
        "      host: 127.0.0.1\n      port: 17701\n      identity: client-a\n"
        "      auth_method: token\n";
    turbo_flow_security_key_provider_t key_provider = TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT;
    turbo_flow_fmq_security_owner_config_t owner_config = TURBO_FLOW_FMQ_SECURITY_OWNER_CONFIG_INIT;
    turbo_flow_fmq_security_owner_t *owner = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    const turbo_flow_fmq_security_binding_t *binding;
    turbo_flow_t *flow;

    key_provider.acquire = fmq_security_test_secret_acquire;
    key_provider.release = fmq_security_test_secret_release;
    owner_config.key_provider = &key_provider;
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_security_owner_create_resolved(resolved, "fmq.client",
                                                               &owner_config, &owner, &error),
                 TURBO_OK);
    binding = turbo_flow_fmq_security_owner_binding(owner);
    check_not_null(binding);
    check_str_eq(binding->auth_method, "token");
    check_str_eq(binding->secret_reference, "secret/fmq-client");
    check_null(binding->realm);
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_fmq_register_resolved_secure_adapter(flow, "fmq.client", resolved,
                                                                 binding, &error),
                 TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_fmq_security_owner_destroy(owner);
    turbo_flow_resolved_config_destroy(resolved);

    owner = NULL;
    resolved = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(
        turbo_flow_config_resolve_yaml(partial_yaml, sizeof(partial_yaml) - 1u, &resolved, &error),
        TURBO_OK);
    check_int_eq(turbo_flow_fmq_security_owner_create_resolved(resolved, "fmq.client",
                                                               &owner_config, &owner, &error),
                 TURBO_EINVAL);
    check_null(owner);
    turbo_flow_resolved_config_destroy(resolved);
  }

#ifdef FMQ_TEST_HAVE_HTTP_SECURITY
  it("composes exact HTTPS authentication and ACL providers without network fallback") {
    static const char yaml[] =
        "version: 1\nchannels:\n  acl.fmq:\n    kind: acl_provider\n    config:\n"
        "      backend: https\n      url: https://auth.internal/v1/flowmq/acl-bundle\n"
        "      service_id: broker-main\n      service_domain: root-a\n"
        "      service_token_ref: secret/control-plane\n"
        "      timeout_ms: 2500\n      max_response_size: 4194304\n  auth.fmq:\n"
        "    kind: auth_provider\n    config:\n      backend: https\n"
        "      url: https://auth.internal/v2/authenticate\n      method: password\n"
        "      service_id: broker-main\n      service_domain: root-a\n"
        "      service_token_ref: secret/control-plane\n      timeout_ms: 2500\n"
        "  security.fmq:\n"
        "    kind: security_realm\n    config:\n"
        "      resource_uid: security:flowmq.fmq3\n      owner_name: security.fmq\n"
        "      policy_source: acl.fmq\nadapters:\n  fmq.secure:\n    kind: fmq\n"
        "    config:\n      pattern: router\n      mode: bind\n      transport: tls\n"
        "      host: 127.0.0.1\n      port: 17702\n"
        "      security_realm: security.fmq\n      auth_provider: auth.fmq\n"
        "      auth_method: password\n";
    turbo_flow_security_key_provider_t key_provider = TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT;
    const turbo_flow_security_auth_provider_factory_t *auth_factories[] = {
        turbo_flow_http_auth_provider_factory()};
    const turbo_flow_security_policy_provider_factory_t *policy_factories[] = {
        turbo_flow_http_acl_provider_factory()};
    turbo_flow_fmq_security_owner_config_t owner_config = TURBO_FLOW_FMQ_SECURITY_OWNER_CONFIG_INIT;
    turbo_flow_fmq_security_owner_t *owner = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    const turbo_flow_fmq_security_binding_t *binding;

    key_provider.acquire = fmq_security_test_secret_acquire;
    key_provider.release = fmq_security_test_secret_release;
    owner_config.key_provider = &key_provider;
    owner_config.auth_provider_factories = auth_factories;
    owner_config.auth_provider_factory_count = 1u;
    owner_config.policy_provider_factories = policy_factories;
    owner_config.policy_provider_factory_count = 1u;
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_security_owner_create_resolved(resolved, "fmq.secure",
                                                               &owner_config, &owner, &error),
                 TURBO_OK);
    binding = turbo_flow_fmq_security_owner_binding(owner);
    check_not_null(binding);
    check_not_null(binding->realm);
    check_not_null(binding->auth_provider);
    check_str_eq(binding->auth_method, "password");
    turbo_flow_fmq_security_owner_destroy(owner);
    turbo_flow_resolved_config_destroy(resolved);
  }
#endif
}
