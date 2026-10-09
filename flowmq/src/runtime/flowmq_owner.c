#include "flowmq_owner.h"

#include "flowmq_socket_external_internal.h"
#include "flowmq_owner_batch.h"

#include <salts/clock.h>
#include <salts/error_codes.h>
#include <salts/native_io.h>
#include <salts/thread.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum { FLOWMQ_OWNER_CLOSE_TIMEOUT_MS = 1000u };

struct flowmq_owner_s {
  flowmq_ctx_t *ctx;
  native_io_backend backend;
  native_io_completion *completions;
  flowmq_socket_t **sockets;
  size_t socket_capacity;
  size_t socket_count;
  size_t completion_capacity;
  unsigned backend_initialized : 1;
  unsigned backend_closed : 1;
};

static size_t flowmq_owner_find_socket(const flowmq_owner_t *owner,
                                       const flowmq_socket_t *socket) {
  if (owner == NULL || socket == NULL) return SIZE_MAX;
  for (size_t i = 0u; i < owner->socket_capacity; ++i) {
    if (owner->sockets[i] == socket) return i;
  }
  return SIZE_MAX;
}

static int flowmq_owner_validate_items(const flowmq_owner_t *owner,
                                       flowmq_pollitem_t *items,
                                       size_t item_count) {
  if (owner == NULL || items == NULL || item_count == 0u)
    return SALTS_EINVAL;
  for (size_t i = 0u; i < item_count; ++i) {
    if (items[i].socket == NULL ||
        flowmq_owner_find_socket(owner, items[i].socket) == SIZE_MAX ||
        !flowmq_socket_internal_owned_by(items[i].socket, owner))
      return SALTS_EINVAL;
  }
  return SALTS_OK;
}

static int flowmq_owner_readiness(flowmq_owner_t *owner,
                                  flowmq_pollitem_t *items,
                                  size_t item_count,
                                  size_t *ready) {
  int status;
  (void)owner;
  *ready = 0u;
  for (size_t i = 0u; i < item_count; ++i) {
    status = flowmq_socket_internal_poll_revents(
        items[i].socket, items[i].events, &items[i].revents);
    if (status != SALTS_OK) return status;
    if (items[i].revents != 0) ++*ready;
  }
  return SALTS_OK;
}

static int flowmq_owner_progress_local(flowmq_socket_t *socket) {
  int status = flowmq_socket_internal_progress_local(socket);
  if (status == SALTS_OK) return SALTS_OK;
  return flowmq_socket_internal_async_error_matches(socket, status)
             ? SALTS_OK
             : status;
}

static int flowmq_owner_route_completion_slot(
    void *user, size_t slot, const native_io_completion *completion,
    bool *consumed, size_t *events) {
  flowmq_owner_t *owner = (flowmq_owner_t *)user;
  flowmq_socket_t *socket = owner->sockets[slot];
  if (socket == NULL || !flowmq_socket_internal_runtime_active(socket)) {
    *consumed = false;
    if (events != NULL) *events = 0u;
    return SALTS_OK;
  }
  return flowmq_socket_internal_route_external_completion(
      socket, completion, consumed, events);
}

static int flowmq_owner_progress_once(flowmq_owner_t *owner,
                                      uint32_t max_wait_ms) {
  uint32_t wait_ms = max_wait_ms;
  size_t active_count = 0u;
  size_t completion_count = 0u;
  int first_error = SALTS_OK;
  int status;

  if (owner == NULL || !owner->backend_initialized || owner->backend_closed)
    return SALTS_EINVAL;

  for (size_t i = 0u; i < owner->socket_capacity; ++i) {
    flowmq_socket_t *socket = owner->sockets[i];
    uint32_t socket_wait = max_wait_ms;
    size_t events = 0u;
    if (socket == NULL ||
        !flowmq_socket_internal_runtime_active(socket))
      continue;
    ++active_count;
    status = flowmq_socket_internal_advance_external(socket, &events);
    if (status != SALTS_OK) return status;
    status = flowmq_socket_internal_external_timeout(
        socket, max_wait_ms, &socket_wait);
    if (status != SALTS_OK) return status;
    if (socket_wait < wait_ms) wait_ms = socket_wait;
  }

  if (active_count == 0u) {
    if (max_wait_ms != 0u) cmeta_sleep_ms(max_wait_ms);
    return SALTS_OK;
  }

  status = native_io_backend_observe(
      &owner->backend, owner->completions, owner->completion_capacity,
      wait_ms, &completion_count);
  if (status != SALTS_OK && status != SALTS_ETIMEDOUT) return status;

  if (status == SALTS_OK)
    first_error = flowmq_owner_batch_route(
        owner->completions, completion_count, owner->socket_capacity,
        flowmq_owner_route_completion_slot, owner);

  /*
   * Match the qualified #67 ordering:
   * external CNet advance/observe/route first, then exactly one FlowMQ-local
   * progress pass. Work admitted by this local pass is advanced on the next
   * owner cycle; there is no second post-route CNet advance.
   */
  for (size_t i = 0u; i < owner->socket_capacity; ++i) {
    flowmq_socket_t *socket = owner->sockets[i];
    if (socket == NULL ||
        !flowmq_socket_internal_runtime_active(socket))
      continue;
    status = flowmq_owner_progress_local(socket);
    if (status != SALTS_OK && first_error == SALTS_OK)
      first_error = status;
  }
  return first_error;
}

flowmq_owner_t *flowmq_owner_new(
    flowmq_ctx_t *ctx, const flowmq_owner_config_t *config) {
  native_io_backend_config backend_config;
  flowmq_owner_t *owner;
  size_t socket_capacity = FLOWMQ_OWNER_DEFAULT_SOCKET_CAPACITY;
  int status;

  if (ctx == NULL) return NULL;
  if (config != NULL) {
    if (config->size < sizeof(flowmq_owner_config_t)) return NULL;
    if (config->socket_capacity != 0u)
      socket_capacity = config->socket_capacity;
  }
  if (socket_capacity == 0u ||
      socket_capacity > FLOWMQ_OWNER_MAX_SOCKET_CAPACITY)
    return NULL;

  status = flowmq_socket_internal_owner_backend_config(
      socket_capacity, &backend_config);
  if (status != SALTS_OK) return NULL;

  owner = (flowmq_owner_t *)calloc(1u, sizeof(*owner));
  if (owner == NULL) return NULL;
  owner->sockets = (flowmq_socket_t **)calloc(
      socket_capacity, sizeof(*owner->sockets));
  owner->completion_capacity = backend_config.completion_batch_capacity;
  owner->completions = (native_io_completion *)calloc(
      owner->completion_capacity, sizeof(*owner->completions));
  if (owner->sockets == NULL || owner->completions == NULL) {
    free(owner->completions);
    free(owner->sockets);
    free(owner);
    return NULL;
  }

  status = flowmq_ctx_internal_owner_acquire(ctx);
  if (status != SALTS_OK) {
    free(owner->completions);
    free(owner->sockets);
    free(owner);
    return NULL;
  }

  status = native_io_backend_init(&owner->backend, &backend_config);
  if (status != SALTS_OK) {
    (void)flowmq_ctx_internal_owner_release(ctx);
    free(owner->completions);
    free(owner->sockets);
    free(owner);
    return NULL;
  }

  owner->ctx = ctx;
  owner->socket_capacity = socket_capacity;
  owner->backend_initialized = 1u;
  return owner;
}

flowmq_socket_t *flowmq_owner_socket(flowmq_owner_t *owner, int type) {
  flowmq_socket_t *socket;
  size_t slot = SIZE_MAX;
  int status;

  if (owner == NULL || owner->ctx == NULL || owner->backend_closed ||
      owner->socket_count >= owner->socket_capacity)
    return NULL;

  for (size_t i = 0u; i < owner->socket_capacity; ++i) {
    if (owner->sockets[i] == NULL) {
      slot = i;
      break;
    }
  }
  if (slot == SIZE_MAX) return NULL;

  socket = flowmq_socket(owner->ctx, type);
  if (socket == NULL) return NULL;
  status = flowmq_socket_internal_attach_owner_backend(
      socket, &owner->backend, owner);
  if (status != SALTS_OK) {
    (void)flowmq_close(socket);
    return NULL;
  }

  owner->sockets[slot] = socket;
  ++owner->socket_count;
  return socket;
}

int flowmq_owner_poll(flowmq_owner_t *owner,
                      flowmq_pollitem_t *items,
                      size_t item_count,
                      uint32_t timeout_ms,
                      size_t *ready) {
  const uint64_t started_ms = cmeta_monotonic_ms();
  int first = 1;
  int status;

  if (ready != NULL) *ready = 0u;
  if (ready == NULL ||
      flowmq_owner_validate_items(owner, items, item_count) != SALTS_OK)
    return SALTS_EINVAL;

  for (;;) {
    uint32_t wait_ms = 0u;
    if (!first) {
      const uint64_t elapsed_ms = cmeta_monotonic_ms() - started_ms;
      if (elapsed_ms >= timeout_ms) return SALTS_OK;
      wait_ms = (uint32_t)((uint64_t)timeout_ms - elapsed_ms);
    }
    first = 0;

    status = flowmq_owner_progress_once(owner, wait_ms);
    if (status != SALTS_OK) return status;

    status = flowmq_owner_readiness(owner, items, item_count, ready);
    if (status != SALTS_OK) return status;
    if (*ready != 0u || timeout_ms == 0u) return SALTS_OK;
  }
}

int flowmq_owner_close_socket(flowmq_owner_t *owner,
                              flowmq_socket_t *socket) {
  const size_t slot = flowmq_owner_find_socket(owner, socket);
  int status;

  if (owner == NULL || socket == NULL || slot == SIZE_MAX ||
      !flowmq_socket_internal_owned_by(socket, owner))
    return SALTS_EINVAL;

  if (flowmq_socket_internal_runtime_active(socket)) {
    const uint64_t deadline =
        cmeta_monotonic_ms() + FLOWMQ_OWNER_CLOSE_TIMEOUT_MS;
    for (;;) {
      status = flowmq_socket_internal_stop_external(socket);
      if (status == SALTS_OK) break;
      if (status != SALTS_EBUSY) return status;
      {
        const uint64_t now_ms = cmeta_monotonic_ms();
        uint32_t wait_ms;
        if (now_ms >= deadline) return SALTS_ETIMEDOUT;
        wait_ms = (uint32_t)(deadline - now_ms);
        status = flowmq_owner_progress_once(owner, wait_ms);
        if (status != SALTS_OK) return status;
      }
    }
  }

  status = flowmq_socket_internal_owner_close_storage(socket, owner);
  if (status != SALTS_OK) return status;
  owner->sockets[slot] = NULL;
  --owner->socket_count;
  return SALTS_OK;
}

int flowmq_owner_term(flowmq_owner_t *owner) {
  int status;
  if (owner == NULL || owner->ctx == NULL) return SALTS_EINVAL;
  if (owner->socket_count != 0u) return SALTS_EBUSY;

  if (owner->backend_initialized && !owner->backend_closed) {
    status = native_io_backend_close(&owner->backend);
    if (status != SALTS_OK && status != SALTS_EALREADY) return status;
    owner->backend_closed = 1u;
  }
  if (owner->backend_initialized) {
    status = native_io_backend_destroy(&owner->backend);
    if (status != SALTS_OK) return status;
    owner->backend_initialized = 0u;
  }

  status = flowmq_ctx_internal_owner_release(owner->ctx);
  if (status != SALTS_OK) return status;
  owner->ctx = NULL;
  free(owner->completions);
  free(owner->sockets);
  free(owner);
  return SALTS_OK;
}
