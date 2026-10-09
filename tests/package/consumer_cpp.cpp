#include <flowmq.h>
#include <cmeta_error.h>

#include <cstddef>
#include <cstdint>

static_assert(FLOWMQ_DESTINATION_VERSION == 1u,
              "installed FlowMQ selection ABI version changed");
static_assert(FLOWMQ_DESTINATION_MAX_ENDPOINTS == 4u,
              "installed bounded admission limit changed");

int main() {
  flowmq_ctx_t *ctx = flowmq_ctx_new();
  if (ctx == nullptr) return 3;
  flowmq_socket_t *socket = flowmq_socket(ctx, FLOWMQ_PAIR);
  flowmq_peer_pool_config_t config = FLOWMQ_PEER_POOL_CONFIG_INIT;
  flowmq_peer_pool_snapshot_t snapshot = FLOWMQ_PEER_POOL_SNAPSHOT_INIT;
  const bool pool_ok = socket != nullptr &&
      flowmq_socket_set_peer_pool(socket, &config) == SALTS_OK &&
      flowmq_socket_get_peer_pool(socket, &snapshot) == SALTS_OK &&
      snapshot.enabled && snapshot.drained && snapshot.active_leases == 0u;
  const int close_status = socket != nullptr ? flowmq_close(socket) : SALTS_OK;
  const int term_status = flowmq_ctx_term(ctx);
  if (!pool_ok || close_status != SALTS_OK || term_status != SALTS_OK) return 4;
  const flowmq_destination_endpoint_t endpoints[] = {
      {10u, 55u, "tcp://127.0.0.1:10001", 1u, 0u, 1},
      {20u, 55u, "tcp://127.0.0.1:10002", 1u, 0u, 1}};
  flowmq_destination_selection_t request =
      FLOWMQ_DESTINATION_SELECTION_INIT;
  flowmq_destination_result_t result = FLOWMQ_DESTINATION_RESULT_INIT;
  request.kind = FLOWMQ_DESTINATION_EXPLICIT;
  request.snapshot_generation = 7u;
  request.explicit_endpoint_id = 20u;
  if (flowmq_destination_choose(
          endpoints, 2u, &request, &result) != SALTS_OK)
    return 1;
  return result.endpoint_id == UINT64_C(20) &&
                 result.snapshot_generation == UINT64_C(7) &&
                 result.index == std::size_t(1)
             ? 0 : 2;
}
