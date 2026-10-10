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

/* Private mailbox qualification: one progress pass returns even on a pure
 * control wake, and an empty owner waits on its backend rather than sleeping.
 * Only the owner thread may step. A producer must publish before wake; wake is
 * the only cross-thread operation. Join all wake callers before closing any
 * owner storage. These seams do not change the public owner_poll contract. */
int flowmq_owner_internal_step(flowmq_owner_t *owner, uint32_t max_wait_ms);
int flowmq_owner_internal_wake(flowmq_owner_t *owner);

/* Owner-thread diagnostic snapshot; no progress or completion consumption.
 * Counts all operations of the shared backend, not only DATA writes/syscalls. */
int flowmq_owner_internal_native_stats(
    const flowmq_owner_t *owner, native_io_backend_stats *stats);

#endif
