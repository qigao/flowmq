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
 * Private external-progress seam.
 *
 * This header is never installed. The public owner-lane API wraps these
 * operations without exposing NativeIO/CNet request or completion types.
 * The benchmark-only entry points are retained so #67 remains a direct
 * qualification harness for the same implementation.
 */
int flowmq_ctx_internal_owner_acquire(flowmq_ctx_t *ctx);
int flowmq_ctx_internal_owner_release(flowmq_ctx_t *ctx);

int flowmq_socket_internal_attach_external_backend(
    flowmq_socket_t *socket, native_io_backend *backend);

int flowmq_socket_internal_attach_owner_backend(
    flowmq_socket_t *socket, native_io_backend *backend,
    const void *owner_token);

int flowmq_socket_internal_owned_by(
    const flowmq_socket_t *socket, const void *owner_token);

int flowmq_socket_internal_runtime_active(const flowmq_socket_t *socket);

int flowmq_socket_internal_owner_backend_config(
    size_t socket_capacity, native_io_backend_config *config);

int flowmq_socket_internal_advance_external(
    flowmq_socket_t *socket, size_t *events);

/* Runs the ordinary post-CNet FlowMQ-local progress stage exactly once. */
int flowmq_socket_internal_progress_local(flowmq_socket_t *socket);

/*
 * Returns the minimum CNet + FlowMQ-local deadline capped by max_wait_ms.
 * This includes reconnect, heartbeat and receiver FLOW_UPDATE timing.
 */
int flowmq_socket_internal_external_timeout(
    flowmq_socket_t *socket, uint32_t max_wait_ms, uint32_t *wait_ms);

int flowmq_socket_internal_route_external_completion(
    flowmq_socket_t *socket, const native_io_completion *completion,
    bool *consumed, size_t *events);


typedef struct flowmq_owned_receive_diagnostics_s {
  uint64_t switches;
  uint64_t callbacks;
  uint64_t fastpath;
  uint64_t fallback_decoder;
  uint64_t fallback_incomplete;
  uint64_t fallback_coalesced;
  uint64_t fallback_shape;
} flowmq_owned_receive_diagnostics_t;

int flowmq_socket_internal_owned_receive_diagnostics(
    const flowmq_socket_t *socket,
    flowmq_owned_receive_diagnostics_t *out);

/* Computes ordinary POLLIN/POLLOUT/POLLERR state without driving progress. */
int flowmq_socket_internal_poll_revents(
    const flowmq_socket_t *socket, short events, short *revents);

int flowmq_socket_internal_async_error_matches(
    const flowmq_socket_t *socket, int status);

/*
 * Starts/continues external CNet shutdown. SALTS_EBUSY means the embedding
 * owner must continue advance/observe/route and retry. After SALTS_OK,
 * flowmq_socket_internal_owner_close_storage() may release storage.
 */
int flowmq_socket_internal_stop_external(flowmq_socket_t *socket);

int flowmq_socket_internal_owner_close_storage(
    flowmq_socket_t *socket, const void *owner_token);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_SOCKET_EXTERNAL_INTERNAL_H */
