#ifndef FLOWMQ_OWNER_INTERNAL_H
#define FLOWMQ_OWNER_INTERNAL_H

#include "flowmq_owner.h"
#include "flowmq_owner_batch.h"

/* Private qualification seam; never installed. The batch and route context
 * are borrowed only for this synchronous call on the Owner's progress lane.
 * A supplied dispatcher must settle the entire batch through the supplied
 * route before returning, including after an injected error. It must not
 * observe, recurse into Owner progress, retain the batch, or independently
 * mutate socket lifecycle.
 * NULL selects ordinary production routing. No callback is stored in Owner. */
typedef int (*flowmq_owner_internal_batch_fn)(
    void *user, const native_io_completion *completions, size_t count,
    size_t socket_capacity, flowmq_owner_batch_route_fn route, void *route_user);

int flowmq_owner_internal_progress_once(
    flowmq_owner_t *owner, uint32_t max_wait_ms,
    flowmq_owner_internal_batch_fn dispatch, void *user);

#endif
