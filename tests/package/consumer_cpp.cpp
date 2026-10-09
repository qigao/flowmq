#include <flowmq.h>
#include <cmeta_error.h>

#include <cstddef>
#include <cstdint>

static_assert(FLOWMQ_DESTINATION_VERSION == 1u,
              "installed FlowMQ selection ABI version changed");
static_assert(FLOWMQ_DESTINATION_MAX_ENDPOINTS == 4u,
              "installed bounded admission limit changed");

int main() {
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
