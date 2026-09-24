#include "flowmq_socket_option.h"

#include "salts_error.h"
#include "tinytest.h"

#include <stdint.h>
#include <string.h>

static const int FLOWMQ_PUBLIC_OPTIONS[] = {
    FLOWMQ_IDENTITY,
    FLOWMQ_SUBSCRIBE,
    FLOWMQ_UNSUBSCRIBE,
    FLOWMQ_RCVMORE,
    FLOWMQ_RECONNECT_IVL,
    FLOWMQ_RECONNECT_IVL_MAX,
    FLOWMQ_SNDHWM,
    FLOWMQ_RCVHWM,
    FLOWMQ_HEARTBEAT_IVL,
    FLOWMQ_HEARTBEAT_TIMEOUT,
    FLOWMQ_TLS_CA_FILE,
    FLOWMQ_TLS_CERT_FILE,
    FLOWMQ_TLS_KEY_FILE,
    FLOWMQ_TLS_KEY_PASSWORD,
    FLOWMQ_TLS_SERVER_NAME,
    FLOWMQ_TLS_REQUIRE_CLIENT_CERTIFICATE,
    FLOWMQ_TLS_IDENTITY_POLICY,
    FLOWMQ_TLS_IDENTITY_REJECTIONS,
    FLOWMQ_SNDHWM_BYTES,
    FLOWMQ_RCVHWM_BYTES,
    FLOWMQ_FLOW_UPDATE_QUANTUM,
    FLOWMQ_FLOW_UPDATE_IVL};

spec("flowmq_socket_option metadata") {
  it("covers every public socket option exactly once") {
    check_equal(flowmq_socket_option_descriptor_count(),
                sizeof(FLOWMQ_PUBLIC_OPTIONS) /
                    sizeof(FLOWMQ_PUBLIC_OPTIONS[0]));

    for (size_t i = 0u;
         i < sizeof(FLOWMQ_PUBLIC_OPTIONS) /
                 sizeof(FLOWMQ_PUBLIC_OPTIONS[0]);
         ++i) {
      const flowmq_socket_option_desc_t *desc =
          flowmq_socket_option_descriptor(FLOWMQ_PUBLIC_OPTIONS[i]);
      check_not_null(desc);
      check_equal(desc->option, FLOWMQ_PUBLIC_OPTIONS[i]);
      for (size_t j = i + 1u;
           j < sizeof(FLOWMQ_PUBLIC_OPTIONS) /
                   sizeof(FLOWMQ_PUBLIC_OPTIONS[0]);
           ++j)
        check_true(FLOWMQ_PUBLIC_OPTIONS[i] != FLOWMQ_PUBLIC_OPTIONS[j]);
    }

    check_true(flowmq_socket_option_descriptor(-1) == NULL);
    check_true(flowmq_socket_option_descriptor(9999) == NULL);
  }

  it("describes access phase ABI and pattern restrictions") {
    const flowmq_socket_option_desc_t *reconnect =
        flowmq_socket_option_descriptor(FLOWMQ_RECONNECT_IVL);
    const flowmq_socket_option_desc_t *subscribe =
        flowmq_socket_option_descriptor(FLOWMQ_SUBSCRIBE);
    const flowmq_socket_option_desc_t *rcvmore =
        flowmq_socket_option_descriptor(FLOWMQ_RCVMORE);
    const flowmq_socket_option_desc_t *policy =
        flowmq_socket_option_descriptor(FLOWMQ_TLS_IDENTITY_POLICY);

    check_equal(reconnect->value_kind, FLOWMQ_SOCKET_OPTION_VALUE_INT);
    check_equal(reconnect->access,
                FLOWMQ_SOCKET_OPTION_ACCESS_SET |
                    FLOWMQ_SOCKET_OPTION_ACCESS_GET);
    check_equal(reconnect->set_phase, FLOWMQ_SOCKET_OPTION_SET_STARTUP);
    check_equal(reconnect->abi_size, sizeof(int));
    check_equal(reconnect->min_value, -1);

    check_equal(subscribe->value_kind, FLOWMQ_SOCKET_OPTION_VALUE_BYTES);
    check_equal(subscribe->access, FLOWMQ_SOCKET_OPTION_ACCESS_SET);
    check_equal(subscribe->set_phase, FLOWMQ_SOCKET_OPTION_SET_RUNTIME);
    check_true(subscribe->pattern_mask != 0u);

    check_equal(rcvmore->access, FLOWMQ_SOCKET_OPTION_ACCESS_GET);
    check_equal(rcvmore->abi_size, sizeof(int));

    check_equal(policy->value_kind, FLOWMQ_SOCKET_OPTION_VALUE_STRUCT);
    check_equal(policy->access, FLOWMQ_SOCKET_OPTION_ACCESS_SET);
    check_true(policy->pattern_mask != 0u);
  }

  it("preserves startup and runtime mutation contracts") {
    const flowmq_pattern_desc_t *pair =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_PAIR);
    const flowmq_pattern_desc_t *xsub =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_XSUB);
    int reconnect = -1;

    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_RECONNECT_IVL, 0, pair, &reconnect,
                    sizeof(reconnect), NULL),
                SALTS_OK);
    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_RECONNECT_IVL, 1, pair, &reconnect,
                    sizeof(reconnect), NULL),
                SALTS_EBUSY);

    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_SUBSCRIBE, 0, xsub, NULL, 0u, NULL),
                SALTS_OK);
    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_SUBSCRIBE, 1, xsub, NULL, 0u, NULL),
                SALTS_OK);
    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_SUBSCRIBE, 1, pair, NULL, 0u, NULL),
                SALTS_EINVAL);

    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_RCVMORE, 0, pair, &reconnect, sizeof(reconnect),
                    NULL),
                SALTS_ENOTSUP);
    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_RCVMORE, 1, pair, &reconnect, sizeof(reconnect),
                    NULL),
                SALTS_EBUSY);
    check_equal(flowmq_socket_option_validate_set(
                    9999, 0, pair, &reconnect, sizeof(reconnect), NULL),
                SALTS_ENOTSUP);
    check_equal(flowmq_socket_option_validate_set(
                    9999, 1, pair, &reconnect, sizeof(reconnect), NULL),
                SALTS_EBUSY);
  }

  it("centralizes scalar string bytes and struct validation") {
    const flowmq_pattern_desc_t *pair =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_PAIR);
    const flowmq_pattern_desc_t *router =
        flowmq_pattern_descriptor(FLOWMQ_PROTOCOL_ROUTER);
    int reconnect = -2;
    int hwm = FLOWMQ_SOCKET_OPTION_MESSAGE_HWM_MAX + 1;
    size_t hwm_bytes = FLOWMQ_SOCKET_OPTION_HWM_BYTES_MAX + (size_t)1u;
    const char identity_with_nul[] = {'a', '\0', 'b'};
    flowmq_tls_identity_map_config_t policy =
        FLOWMQ_TLS_IDENTITY_MAP_CONFIG_INIT;

    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_RECONNECT_IVL, 0, pair, &reconnect,
                    sizeof(reconnect), NULL),
                SALTS_EINVAL);
    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_SNDHWM, 0, pair, &hwm, sizeof(hwm), NULL),
                SALTS_EINVAL);
    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_SNDHWM_BYTES, 0, pair, &hwm_bytes,
                    sizeof(hwm_bytes), NULL),
                SALTS_EINVAL);
    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_IDENTITY, 0, pair, identity_with_nul,
                    sizeof(identity_with_nul), NULL),
                SALTS_EINVAL);

    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_TLS_IDENTITY_POLICY, 0, pair, &policy,
                    sizeof(policy), NULL),
                SALTS_EINVAL);
    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_TLS_IDENTITY_POLICY, 0, router, &policy,
                    sizeof(policy), NULL),
                SALTS_OK);
    check_equal(flowmq_socket_option_validate_set(
                    FLOWMQ_TLS_IDENTITY_POLICY, 0, router, &policy,
                    sizeof(policy) - 1u, NULL),
                SALTS_EINVAL);
  }

  it("uses descriptor ABI for getters without expanding get access") {
    int value = 0;
    size_t size = sizeof(value) - 1u;

    check_equal(flowmq_socket_option_prepare_get(
                    FLOWMQ_RECONNECT_IVL, &value, &size, NULL),
                SALTS_EMSGSIZE);
    check_equal(size, sizeof(value));

    size = sizeof(value);
    check_equal(flowmq_socket_option_prepare_get(
                    FLOWMQ_RECONNECT_IVL, NULL, &size, NULL),
                SALTS_EINVAL);
    check_equal(size, sizeof(value));

    size = sizeof(value);
    check_equal(flowmq_socket_option_prepare_get(FLOWMQ_SNDHWM, &value, &size,
                                                 NULL),
                SALTS_ENOTSUP);

    {
      uint64_t counter = 0u;
      size = sizeof(counter);
      check_equal(flowmq_socket_option_prepare_get(
                      FLOWMQ_TLS_IDENTITY_REJECTIONS, &counter, &size, NULL),
                  SALTS_OK);
      check_equal(size, sizeof(counter));
    }
  }
}
