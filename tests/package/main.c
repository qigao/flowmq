#include <flowmq_socket.h>
#include <flowmq_tls_identity_map.h>

#include <stddef.h>
#include <stdint.h>

_Static_assert(FLOWMQ_TLS_IDENTITY_MAP_API_VERSION == 1u,
               "TLS identity-map API version changed");
_Static_assert(FLOWMQ_TLS_IDENTITY_POLICY == 1007,
               "TLS identity-policy sockopt id changed");
_Static_assert(FLOWMQ_TLS_IDENTITY_REJECTIONS == 1008,
               "TLS identity rejection-counter sockopt id changed");
_Static_assert(FLOWMQ_TLS_CERTIFICATE_SHA256_TEXT_SIZE == 71u,
               "canonical SHA-256 text contract changed");
_Static_assert(FLOWMQ_TLS_CERTIFICATE_SHA256_CAPACITY == 72u,
               "canonical SHA-256 capacity changed");

int main(void)
{
    flowmq_tls_identity_map_config_t policy =
        FLOWMQ_TLS_IDENTITY_MAP_CONFIG_INIT;
    int (*setopt_fn)(flowmq_socket_t *, int, const void *, size_t) =
        flowmq_setsockopt;
    int (*getopt_fn)(const flowmq_socket_t *, int, void *, size_t *) =
        flowmq_getsockopt;

    policy.binding_count = 0u;
    return setopt_fn == NULL || getopt_fn == NULL ||
                   policy.struct_size != sizeof(policy)
               ? 1
               : 0;
}
