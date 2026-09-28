#include <flowmq.h>
#include <salts_error.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>

_Static_assert(FLOWMQ_MEDIA_PROVIDER_API_VERSION == 3u,
               "media-provider API version changed");
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
    static const char fingerprint[] =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000";
    static const char identity[] = "package-consumer";
    flowmq_tls_identity_binding_t binding = {
        sizeof(binding), fingerprint, identity};
    flowmq_tls_identity_map_config_t policy =
        FLOWMQ_TLS_IDENTITY_MAP_CONFIG_INIT;
    flowmq_ctx_t *ctx = NULL;
    flowmq_socket_t *router = NULL;
    int reconnect_ms = 25;
    int reconnect_read = 0;
    size_t reconnect_read_size = sizeof(reconnect_read);
    size_t send_hwm_bytes = 4096u;
    flowmq_router_peer_status_t peer_status = FLOWMQ_ROUTER_PEER_STATUS_INIT;
    mem_slice_t invalid_slice = {0};
    mem_slice_t recv_slice = {0};
    int result = 1;

    policy.bindings = &binding;
    policy.binding_count = 1u;

    ctx = flowmq_ctx_new();
    if (ctx == NULL) goto cleanup;
    router = flowmq_socket(ctx, FLOWMQ_ROUTER);
    if (router == NULL) goto cleanup;

    /* int ABI */
    if (flowmq_setsockopt(router, FLOWMQ_RECONNECT_IVL, &reconnect_ms,
                          sizeof(reconnect_ms)) != 0)
        goto cleanup;
    if (flowmq_getsockopt(router, FLOWMQ_RECONNECT_IVL, &reconnect_read,
                          &reconnect_read_size) != 0 ||
        reconnect_read != reconnect_ms)
        goto cleanup;

    /* size_t ABI */
    if (flowmq_setsockopt(router, FLOWMQ_SNDHWM_BYTES, &send_hwm_bytes,
                          sizeof(send_hwm_bytes)) != 0)
        goto cleanup;

    /* variable string ABI */
    if (flowmq_setsockopt(router, FLOWMQ_TLS_SERVER_NAME, "localhost",
                          strlen("localhost")) != 0)
        goto cleanup;

    /* public struct ABI + ROUTER pattern restriction */
    if (flowmq_setsockopt(router, FLOWMQ_TLS_IDENTITY_POLICY, &policy,
                          sizeof(policy)) != 0)
        goto cleanup;

    /* installed public struct + function ABI; no peer exists yet */
    if (flowmq_router_peer_status(router, identity, strlen(identity),
                                  &peer_status) != SALTS_ENOENT)
        goto cleanup;

    /*
     * Pin the retained public symbol and canonical Salts Core slice ABI in the
     * installed shared library. An empty slice is invalid before any transport
     * progress, so this is deterministic and does not require a live peer.
     */
    if (flowmq_send_slice(router, &invalid_slice, FLOWMQ_DONTWAIT) !=
        SALTS_EINVAL)
        goto cleanup;

    /* Pin the owned receive symbol without requiring a live transport. */
    if (flowmq_recv_slice(router, &recv_slice, FLOWMQ_DONTWAIT) != SALTS_EBUSY)
        goto cleanup;
    if (recv_slice.buffer != NULL || recv_slice.data != NULL ||
        recv_slice.length != 0u)
        goto cleanup;

    result = 0;

cleanup:
    if (router != NULL && flowmq_close(router) != 0) result = 1;
    if (ctx != NULL && flowmq_ctx_term(ctx) != 0) result = 1;
    return result;
}
