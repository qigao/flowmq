#include "flowmq_tls_identity_map.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static const char cert_a[] =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
static const char cert_b[] =
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

spec("FlowMQ TLS identity map") {
  it("accepts only exact certificate and HELLO identity tuples") {
    flowmq_tls_identity_binding_t bindings[2] = {
        {sizeof(bindings[0]), cert_a, "mesh-agent:node-01"},
        {sizeof(bindings[1]), cert_b, "mesh-agent:node-01"}};
    flowmq_tls_identity_map_config_t config =
        FLOWMQ_TLS_IDENTITY_MAP_CONFIG_INIT;
    flowmq_tls_identity_map_t *map = NULL;
    config.bindings = bindings;
    config.binding_count = 2u;
    config.policy_generation = 7u;
    check_int_eq(flowmq_tls_identity_map_create(&config, &map), TURBO_OK);
    check_not_null(map);
    check_uint_eq(flowmq_tls_identity_map_generation(map), 7u);
    check_int_eq(flowmq_tls_identity_map_verify(
                     map, cert_a, tstr_v_from_cstr("mesh-agent:node-01")),
                 TURBO_OK);
    check_int_eq(flowmq_tls_identity_map_verify(
                     map, cert_b, tstr_v_from_cstr("mesh-agent:node-01")),
                 TURBO_OK);
    check_int_eq(flowmq_tls_identity_map_verify(
                     map, cert_a, tstr_v_from_cstr("meshd:node-01")),
                 TURBO_EPERM);
    flowmq_tls_identity_map_destroy(map);
  }

  it("copies configuration strings into immutable owned storage") {
    char fingerprint[sizeof(cert_a)];
    char identity[] = "mesh-agent:node-01";
    flowmq_tls_identity_binding_t binding =
        FLOWMQ_TLS_IDENTITY_BINDING_INIT;
    flowmq_tls_identity_map_config_t config =
        FLOWMQ_TLS_IDENTITY_MAP_CONFIG_INIT;
    flowmq_tls_identity_map_t *map = NULL;
    memcpy(fingerprint, cert_a, sizeof(cert_a));
    binding.certificate_sha256 = fingerprint;
    binding.hello_identity = identity;
    config.bindings = &binding;
    config.binding_count = 1u;
    check_int_eq(flowmq_tls_identity_map_create(&config, &map), TURBO_OK);
    memset(fingerprint, 'x', sizeof(fingerprint) - 1u);
    identity[0] = 'x';
    check_int_eq(flowmq_tls_identity_map_verify(
                     map, cert_a, tstr_v_from_cstr("mesh-agent:node-01")),
                 TURBO_OK);
    flowmq_tls_identity_map_destroy(map);
  }

  it("rejects malformed conflicting and over-budget policy") {
    flowmq_tls_identity_binding_t bindings[2] = {
        {sizeof(bindings[0]), cert_a, "mesh-agent:node-01"},
        {sizeof(bindings[1]), cert_a, "mesh-agent:node-02"}};
    flowmq_tls_identity_map_config_t config =
        FLOWMQ_TLS_IDENTITY_MAP_CONFIG_INIT;
    flowmq_tls_identity_map_t *map = NULL;
    config.bindings = bindings;
    config.binding_count = 2u;
    check_int_eq(flowmq_tls_identity_map_create(&config, &map), TURBO_EINVAL);
    check_null(map);
    bindings[1].certificate_sha256 =
        "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    check_int_eq(flowmq_tls_identity_map_create(&config, &map), TURBO_EINVAL);
    check_null(map);
    bindings[1].certificate_sha256 = cert_b;
    config.max_total_string_bytes = 1u;
    check_int_eq(flowmq_tls_identity_map_create(&config, &map), TURBO_ERANGE);
    check_null(map);
  }
}
