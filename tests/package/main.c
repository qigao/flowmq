#include <flowmq.h>
#include <cmeta_error.h>

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
_Static_assert(FLOWMQ_REUSE_PORT == 1105,
               "reuse-port sockopt id changed");
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
    flowmq_owner_t *owner = NULL;
    flowmq_socket_t *owner_socket = NULL;
    int reconnect_ms = 25;
    int reuse_port = 0;
    int reconnect_read = 0;
    size_t reconnect_read_size = sizeof(reconnect_read);
    size_t send_hwm_bytes = 4096u;
    flowmq_router_peer_status_t peer_status = FLOWMQ_ROUTER_PEER_STATUS_INIT;
    mem_slice_t invalid_slice = {0};
    mem_slice_t recv_slice = {0};
    mem_slice_t recv_slices[1] = {{0}};
    size_t recv_slice_count = 0u;
    int result = 1;

    policy.bindings = &binding;
    policy.binding_count = 1u;

    ctx = flowmq_ctx_new();
    if (ctx == NULL) goto cleanup;
    router = flowmq_socket(ctx, FLOWMQ_ROUTER);
    if (router == NULL) goto cleanup;

    {
        flowmq_peer_pool_config_t config = FLOWMQ_PEER_POOL_CONFIG_INIT;
        flowmq_peer_pool_snapshot_t snapshot = FLOWMQ_PEER_POOL_SNAPSHOT_INIT;
        config.max_peers = config.max_connecting = 1u;
        if (flowmq_socket_set_peer_pool(router, &config) != SALTS_OK ||
            flowmq_socket_get_peer_pool(router, &snapshot) != SALTS_OK ||
            !snapshot.enabled || snapshot.max_peers != 1u || !snapshot.drained)
            goto cleanup;
    }

    /* int ABI */
    if (flowmq_setsockopt(router, FLOWMQ_REUSE_PORT, &reuse_port,
                          sizeof(reuse_port)) != 0)
        goto cleanup;
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

    /* Installed C11 client-policy ABI: host snapshot, no network fallback. */
    {
        flowmq_destination_endpoint_t endpoint = {
            .endpoint_id = 42u, .authority_id = 7u,
            .uri = "tcp://127.0.0.1:12345",
            .weight = 1u, .eligible = 1};
        flowmq_destination_selection_t selection =
            FLOWMQ_DESTINATION_SELECTION_INIT;
        flowmq_destination_result_t chosen = FLOWMQ_DESTINATION_RESULT_INIT;
        selection.snapshot_generation = 9u;
        selection.explicit_endpoint_id = 42u;
        if (flowmq_destination_choose(&endpoint, 1u, &selection, &chosen) !=
            SALTS_OK || chosen.endpoint_id != 42u ||
            chosen.snapshot_generation != 9u || chosen.index != 0u)
            goto cleanup;
    }

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

    /* Pin the additive owned receive-vector ABI without a live peer. */
    if (flowmq_recv_slicev(router, recv_slices, 1u, &recv_slice_count,
                           FLOWMQ_DONTWAIT) != SALTS_EBUSY)
        goto cleanup;
    if (recv_slice_count != 0u ||
        recv_slices[0].buffer != NULL || recv_slices[0].data != NULL ||
        recv_slices[0].length != 0u)
        goto cleanup;

    /*
     * Pin the installed owner-lane header and shared-library symbols without
     * requiring a live network peer. The owner keeps the context leased,
     * creates one bounded socket, performs one nonblocking owner poll, and
     * releases the socket/backend through the public lifecycle.
     */
    {
        flowmq_owner_config_t owner_config = FLOWMQ_OWNER_CONFIG_INIT;
        flowmq_pollitem_t item;
        size_t ready = 0u;
        owner_config.socket_capacity = 1u;
        owner = flowmq_owner_new(ctx, &owner_config);
        if (owner == NULL) goto cleanup;
        if (flowmq_ctx_term(ctx) != SALTS_EBUSY) goto cleanup;
        owner_socket = flowmq_owner_socket(owner, FLOWMQ_PAIR);
        if (owner_socket == NULL) goto cleanup;
        item = (flowmq_pollitem_t){
            .socket = owner_socket,
            .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT | FLOWMQ_POLLERR};
        if (flowmq_owner_poll(owner, &item, 1u, 0u, &ready) != SALTS_OK)
            goto cleanup;
        if (flowmq_close(owner_socket) != SALTS_EBUSY) goto cleanup;
        if (flowmq_owner_close_socket(owner, owner_socket) != SALTS_OK)
            goto cleanup;
        owner_socket = NULL;
        if (flowmq_owner_term(owner) != SALTS_OK) goto cleanup;
        owner = NULL;
    }

    result = 0;

cleanup:
    if (owner_socket != NULL && owner != NULL &&
        flowmq_owner_close_socket(owner, owner_socket) != SALTS_OK)
        result = 1;
    if (owner != NULL && flowmq_owner_term(owner) != SALTS_OK) result = 1;
    if (router != NULL && flowmq_close(router) != 0) result = 1;
    if (ctx != NULL && flowmq_ctx_term(ctx) != 0) result = 1;
    return result;
}
