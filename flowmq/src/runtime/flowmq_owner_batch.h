#ifndef FLOWMQ_OWNER_BATCH_H
#define FLOWMQ_OWNER_BATCH_H

#include <salts/native_io.h>

#include <stdbool.h>
#include <stddef.h>

/* Private borrowed-completion batch contract. The caller owns the NativeIO
 * observe and must keep events live throughout this call. A router may consume
 * its completion before returning an error; in that case no other router can
 * claim that same completion, but later events must still be inspected. */
typedef int (*flowmq_owner_batch_route_fn)(
    void *user, size_t socket_slot, const native_io_completion *completion,
    bool *consumed, size_t *events);

int flowmq_owner_batch_route(
    const native_io_completion *completions, size_t completion_count,
    size_t socket_capacity, flowmq_owner_batch_route_fn route, void *user);

#endif /* FLOWMQ_OWNER_BATCH_H */
