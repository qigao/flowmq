#ifndef FLOWMQ_SOCKET_EXTERNAL_INTERNAL_H
#define FLOWMQ_SOCKET_EXTERNAL_INTERNAL_H

#include "flowmq_socket.h"

#include <salts/native_io.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Benchmark-only external-progress seam.
 *
 * This header is private to the build tree and is never installed. It exists
 * only to qualify #64 before any public owner-lane API is considered.
 */
int flowmq_socket_internal_attach_external_backend(
    flowmq_socket_t *socket, native_io_backend *backend);

int flowmq_socket_internal_advance_external(
    flowmq_socket_t *socket, size_t *events);

/* Runs the ordinary post-CNet FlowMQ-local progress stage exactly once. */
int flowmq_socket_internal_progress_local(flowmq_socket_t *socket);

int flowmq_socket_internal_external_timeout(
    flowmq_socket_t *socket, uint32_t max_wait_ms, uint32_t *wait_ms);

int flowmq_socket_internal_route_external_completion(
    flowmq_socket_t *socket, const native_io_completion *completion,
    bool *consumed, size_t *events);

/*
 * Starts/continues external CNet shutdown. SALTS_EBUSY means the embedding
 * owner must continue advance/observe/route and retry. After SALTS_OK,
 * flowmq_close() may destroy and release the socket.
 */
int flowmq_socket_internal_stop_external(flowmq_socket_t *socket);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_SOCKET_EXTERNAL_INTERNAL_H */
