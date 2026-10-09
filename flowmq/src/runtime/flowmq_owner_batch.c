#include "flowmq_owner_batch.h"

#include <salts/error_codes.h>

int flowmq_owner_batch_route(
    const native_io_completion *completions, size_t completion_count,
    size_t socket_capacity, flowmq_owner_batch_route_fn route, void *user) {
  int first_error = SALTS_OK;
  if ((completions == NULL && completion_count != 0u) ||
      (route == NULL && completion_count != 0u) ||
      (socket_capacity == 0u && completion_count != 0u))
    return SALTS_EINVAL;

  for (size_t index = 0u; index < completion_count; ++index) {
    bool consumed = false;
    bool route_failed = false;
    for (size_t slot = 0u; slot < socket_capacity; ++slot) {
      bool socket_consumed = false;
      size_t callbacks = 0u;
      int status = route(user, slot, &completions[index],
                         &socket_consumed, &callbacks);
      if (status != SALTS_OK) {
        if (first_error == SALTS_OK) first_error = status;
        /* A callback may already have consumed this event; do not probe
         * another socket after a failed router. Continue with later events. */
        route_failed = true;
        break;
      }
      if (socket_consumed) {
        consumed = true;
        break;
      }
    }
    if (!consumed && !route_failed && first_error == SALTS_OK)
      first_error = SALTS_EPROTO;
  }
  return first_error;
}
