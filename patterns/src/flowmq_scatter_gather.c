/**
 * @file flowmq_scatter_gather.c
 * Scatter/Gather session state management implementation.
 */

#include "flowmq_scatter_gather.h"
#include "flowmq_stl_error_internal.h"

#include <rocida/stl.h>

#include <string.h>

/* Hash map for correlation_id -> session* */

/* Internal helpers */
static int flowmq_scatter_session_init(flowmq_scatter_session_t *session,
                                       uint64_t correlation_id,
                                       uint32_t expected_responses,
                                       flowmq_protocol_gather_policy_t policy,
                                       uint64_t deadline_ns,
                                       void *client_context,
                                       uint64_t created_ns) {
  if (!session || expected_responses == 0 ||
      expected_responses > FLOWMQ_PROTOCOL_ESB_MAX_FANOUT_COUNT) {
    return TURBO_EINVAL;
  }

  memset(session, 0, sizeof(*session));
  session->correlation_id = correlation_id;
  session->created_ns = created_ns;
  session->deadline_ns = deadline_ns;
  session->state = FLOWMQ_SCATTER_PENDING;
  session->policy = policy;
  session->expected_responses = expected_responses;
  session->client_context = client_context;

  /* Pre-allocate partial results vector */
  if (vec_init_bytes(&session->partial_results,
                           sizeof(flowmq_scatter_partial_response_t),
                           _Alignof(flowmq_scatter_partial_response_t),
                           expected_responses) != STL_OK) {
    return TURBO_ENOMEM;
  }
  if (vec_reserve(&session->partial_results, expected_responses) != 0) {
    vec_destroy(&session->partial_results);
    return TURBO_ENOMEM;
  }

  return TURBO_OK;
}

static int flowmq_scatter_gather_calculate_deadline(uint64_t now_ns,
                                                   uint64_t timeout_ms,
                                                   uint64_t *deadline_ns) {
  if (!deadline_ns) return TURBO_EINVAL;

  if (timeout_ms == 0u) {
    *deadline_ns = 0u;
    return TURBO_OK;
  }

  if (timeout_ms > UINT64_MAX / FLOWMQ_SCATTER_GATHER_NS_PER_MS) {
    return TURBO_ERANGE;
  }

  uint64_t timeout_ns = timeout_ms * FLOWMQ_SCATTER_GATHER_NS_PER_MS;
  if (now_ns > UINT64_MAX - timeout_ns) {
    return TURBO_ERANGE;
  }

  *deadline_ns = now_ns + timeout_ns;
  return TURBO_OK;
}

static int flowmq_scatter_gather_create_session_impl(flowmq_scatter_gather_manager_t *manager,
                                                     uint32_t expected_responses,
                                                     flowmq_protocol_gather_policy_t policy,
                                                     uint64_t deadline_ns,
                                                     void *client_context,
                                                     uint64_t *correlation_id) {
  if (!manager || !correlation_id) {
    return TURBO_EINVAL;
  }

  /* Check capacity */
  if (manager->active_sessions >= manager->max_sessions) {
    return TURBO_ENOSPC;
  }

  /* Validate policy */
  if (policy > FLOWMQ_GATHER_QUORUM) {
    return TURBO_EINVAL;
  }

  /* Allocate session */
  flowmq_scatter_session_t *session =
      (flowmq_scatter_session_t *)malloc(sizeof(flowmq_scatter_session_t));
  if (!session) {
    return TURBO_ENOMEM;
  }

  /* Assign correlation ID */
  uint64_t id = manager->next_correlation_id++;
  if (manager->next_correlation_id == 0) {
    /* Wrapped around, skip 0 */
    manager->next_correlation_id = 1;
  }

  uint64_t now_ns = turbo_hrtime();
  int rc = flowmq_scatter_session_init(session, id, expected_responses, policy,
                                       deadline_ns, client_context, now_ns);
  if (rc != TURBO_OK) {
    free(session);
    return rc;
  }

  /* Insert into map */
  if (hash_map_put(&manager->sessions, &id, &session) != TURBO_OK) {
    flowmq_scatter_session_cleanup(session);
    free(session);
    return TURBO_ENOMEM;
  }

  manager->active_sessions++;
  *correlation_id = id;
  return TURBO_OK;
}

void flowmq_scatter_session_cleanup(flowmq_scatter_session_t *session) {
  if (!session) {
    return;
  }

  /* Free all partial response payloads */
  for (size_t i = 0; i < vec_size(&session->partial_results); i++) {
    flowmq_scatter_partial_response_t *resp =
        (flowmq_scatter_partial_response_t *)vec_at(&session->partial_results, i);
    if (resp) {
      tstr_free(resp->payload);
    }
  }

  vec_destroy(&session->partial_results);
  memset(session, 0, sizeof(*session));
}

/* Check if session is complete based on policy */
static int flowmq_scatter_check_completion(flowmq_scatter_session_t *session) {
  if (!session || session->state != FLOWMQ_SCATTER_PENDING) {
    return 0;
  }

  switch (session->policy) {
    case FLOWMQ_GATHER_ALL:
      /* Need all responses */
      return session->received_responses >= session->expected_responses;

    case FLOWMQ_GATHER_FIRST_N:
      /* Already complete if we have expected count */
      return session->received_responses >= session->expected_responses;

    case FLOWMQ_GATHER_QUORUM:
      /* Need majority (N/2 + 1) */
      {
        uint32_t quorum = (session->expected_responses / 2) + 1;
        return session->received_responses >= quorum;
      }

    default:
      return 0;
  }
}

int flowmq_scatter_gather_manager_init(flowmq_scatter_gather_manager_t *manager,
                                       uint32_t max_sessions,
                                       uint64_t default_timeout_ms) {
  if (!manager || max_sessions == 0 ||
      max_sessions > FLOWMQ_SCATTER_GATHER_MAX_SESSIONS) {
    return TURBO_EINVAL;
  }

  memset(manager, 0, sizeof(*manager));
  manager->max_sessions = max_sessions;
  manager->default_timeout_ms = default_timeout_ms;
  manager->next_correlation_id = 1;  /* Start from 1, 0 reserved for invalid */

  /* Initialize session map */
  if (hash_map_init_bytes(&manager->sessions,
                                sizeof(uint64_t), _Alignof(uint64_t),
                                sizeof(flowmq_scatter_session_t *),
                                _Alignof(flowmq_scatter_session_t *), max_sessions,
                                hash_bytes, hash_key_equal, NULL) != STL_OK) {
    return TURBO_ENOMEM;
  }
  if (hash_map_reserve(&manager->sessions, max_sessions) != 0) {
    hash_map_destroy(&manager->sessions);
    return TURBO_ENOMEM;
  }

  return TURBO_OK;
}

void flowmq_scatter_gather_manager_destroy(flowmq_scatter_gather_manager_t *manager) {
  if (!manager) {
    return;
  }

  /* Cleanup all active sessions */
  for (size_t slot = 0; slot < hash_map_capacity(&manager->sessions); ++slot) {
    flowmq_scatter_session_t **session_ptr =
        (flowmq_scatter_session_t **)hash_map_value_at(&manager->sessions, slot);
    if (!session_ptr || !*session_ptr) {
      continue;
    }
    flowmq_scatter_session_cleanup(*session_ptr);
    free(*session_ptr);
  }

  hash_map_destroy(&manager->sessions);
  memset(manager, 0, sizeof(*manager));
}

int flowmq_scatter_gather_create_session(flowmq_scatter_gather_manager_t *manager,
                                         uint32_t expected_responses,
                                         flowmq_protocol_gather_policy_t policy,
                                         uint64_t timeout_ns,
                                         void *client_context,
                                         uint64_t *correlation_id) {
  if (!manager || !correlation_id) {
    return TURBO_EINVAL;
  }

  /* Calculate deadline (0 means use manager default timeout). */
  uint64_t deadline = timeout_ns;
  uint64_t now_ns = turbo_hrtime();
  if (deadline == 0) {
    int rc = flowmq_scatter_gather_calculate_deadline(
      now_ns, manager->default_timeout_ms, &deadline);
    if (rc != TURBO_OK) {
      return rc;
    }
  }

  return flowmq_scatter_gather_create_session_impl(
      manager, expected_responses, policy, deadline, client_context, correlation_id);
}

int flowmq_scatter_gather_create_session_ms(flowmq_scatter_gather_manager_t *manager,
                                           uint32_t expected_responses,
                                           flowmq_protocol_gather_policy_t policy,
                                           uint64_t timeout_ms,
                                           void *client_context,
                                           uint64_t *correlation_id) {
  if (!manager) {
    return TURBO_EINVAL;
  }

  uint64_t local_correlation_id = 0u;
  uint64_t deadline_ns = 0;
  int rc = flowmq_scatter_gather_calculate_deadline(
      turbo_hrtime(), timeout_ms == 0u ? manager->default_timeout_ms : timeout_ms, &deadline_ns);
  if (rc != TURBO_OK) {
    return rc;
  }

  return flowmq_scatter_gather_create_session_impl(
      manager, expected_responses, policy, deadline_ns, client_context,
      correlation_id ? correlation_id : &local_correlation_id);
}

int flowmq_scatter_gather_record_response(flowmq_scatter_gather_manager_t *manager,
                                          uint64_t correlation_id,
                                          uint32_t index,
                                          tstr *payload,
                                          int status,
                                          uint64_t received_ns) {
  if (!manager || !payload) {
    return TURBO_EINVAL;
  }

  /* Find session */
  flowmq_scatter_session_t **session_ptr =
      (flowmq_scatter_session_t **)hash_map_get(&manager->sessions, &correlation_id);
  if (!session_ptr || !*session_ptr) {
    return TURBO_ENOENT;
  }

  flowmq_scatter_session_t *session = *session_ptr;

  /* Validate state */
  if (session->state != FLOWMQ_SCATTER_PENDING) {
    return TURBO_EINVAL;
  }

  /* Validate index */
  if (index >= session->expected_responses) {
    return TURBO_ERANGE;
  }

  /* Check for duplicate response */
  for (size_t i = 0; i < vec_size(&session->partial_results); i++) {
    flowmq_scatter_partial_response_t *existing =
        (flowmq_scatter_partial_response_t *)vec_at(&session->partial_results, i);
    if (existing && existing->index == index) {
      /* Duplicate - ignore or replace? For now, ignore */
      return TURBO_EALREADY;
    }
  }

  /* Create partial response */
  flowmq_scatter_partial_response_t resp = {0};
  resp.index = index;
  resp.received_ns = received_ns;
  resp.payload = *payload;  /* Move ownership */
  resp.status = status;
  *payload = NULL;  /* Clear source */

  /* Add to results */
  if (vec_push(&session->partial_results, &resp) != TURBO_OK) {
    tstr_free(resp.payload);
    return TURBO_ENOMEM;
  }

  /* Update counters */
  session->received_responses++;
  if (status == TURBO_OK) {
    session->success_responses++;
  } else {
    session->error_responses++;
  }

  /* Check completion */
  if (flowmq_scatter_check_completion(session)) {
    session->state = FLOWMQ_SCATTER_COMPLETED;
  }

  return TURBO_OK;
}

int flowmq_scatter_gather_check_session(flowmq_scatter_gather_manager_t *manager,
                                        uint64_t correlation_id,
                                        flowmq_scatter_session_state_t *out_state) {
  if (!manager || !out_state) {
    return TURBO_EINVAL;
  }

  flowmq_scatter_session_t **session_ptr =
      (flowmq_scatter_session_t **)hash_map_get(&manager->sessions, &correlation_id);
  if (!session_ptr || !*session_ptr) {
    return TURBO_ENOENT;
  }

  *out_state = (*session_ptr)->state;
  return TURBO_OK;
}

int flowmq_scatter_gather_finalize_session(flowmq_scatter_gather_manager_t *manager,
                                           uint64_t correlation_id,
                                           flowmq_scatter_session_t *out_session) {
  if (!manager || !out_session) {
    return TURBO_EINVAL;
  }

  /* Find and remove session */
  flowmq_scatter_session_t **session_ptr =
      (flowmq_scatter_session_t **)hash_map_get(&manager->sessions, &correlation_id);
  if (!session_ptr || !*session_ptr) {
    return TURBO_ENOENT;
  }

  flowmq_scatter_session_t *session = *session_ptr;

  /* Check if complete */
  if (session->state == FLOWMQ_SCATTER_PENDING) {
    return TURBO_EINVAL;
  }

  /* Transfer ownership */
  *out_session = *session;

  /* Remove from map */
  (void)hash_map_remove(&manager->sessions, &correlation_id, NULL);
  free(session);  /* Free the pointer, not the session data */

  manager->active_sessions--;
  return TURBO_OK;
}

int flowmq_scatter_gather_cancel_session(flowmq_scatter_gather_manager_t *manager,
                                         uint64_t correlation_id) {
  if (!manager) {
    return TURBO_EINVAL;
  }

  flowmq_scatter_session_t **session_ptr =
      (flowmq_scatter_session_t **)hash_map_get(&manager->sessions, &correlation_id);
  if (!session_ptr || !*session_ptr) {
    return TURBO_ENOENT;
  }

  flowmq_scatter_session_t *session = *session_ptr;
  session->state = FLOWMQ_SCATTER_CANCELLED;

  /* Cleanup and remove */
  flowmq_scatter_session_cleanup(session);
  (void)hash_map_remove(&manager->sessions, &correlation_id, NULL);
  free(session);

  manager->active_sessions--;
  return TURBO_OK;
}

uint32_t flowmq_scatter_gather_process_timeouts(flowmq_scatter_gather_manager_t *manager,
                                                uint64_t now_ns,
                                                flowmq_scatter_completion_fn completion_fn,
                                                void *context) {
  if (!manager) {
    return 0;
  }

  uint32_t timed_out = 0;
  /* Collect timed out sessions (can't modify map during iteration) */
  uint64_t timed_out_ids[FLOWMQ_SCATTER_GATHER_MAX_SESSIONS];
  uint32_t count = 0;

  for (size_t slot = 0; slot < hash_map_capacity(&manager->sessions); ++slot) {
    flowmq_scatter_session_t **session_ptr =
        (flowmq_scatter_session_t **)hash_map_value_at(&manager->sessions, slot);
    if (!session_ptr || !*session_ptr) {
      continue;
    }
    flowmq_scatter_session_t *session = *session_ptr;
    if (session && session->state == FLOWMQ_SCATTER_PENDING &&
        session->deadline_ns > 0 && now_ns >= session->deadline_ns) {
      if (count < FLOWMQ_SCATTER_GATHER_MAX_SESSIONS) {
        timed_out_ids[count++] = session->correlation_id;
      }
    }
  }

  /* Mark timed out sessions */
  for (uint32_t i = 0; i < count; i++) {
    flowmq_scatter_session_t **session_ptr =
        (flowmq_scatter_session_t **)hash_map_get(&manager->sessions, &timed_out_ids[i]);
    if (session_ptr && *session_ptr) {
      flowmq_scatter_session_t *session = *session_ptr;
      session->state = FLOWMQ_SCATTER_TIMEOUT;
      timed_out++;

      /* Call completion callback */
      if (completion_fn) {
        completion_fn(session, context);
      }
    }
  }

  return timed_out;
}

void flowmq_scatter_gather_stats(const flowmq_scatter_gather_manager_t *manager,
                                 uint32_t *active,
                                 uint32_t *capacity) {
  if (!manager) {
    return;
  }

  if (active) {
    *active = manager->active_sessions;
  }
  if (capacity) {
    *capacity = manager->max_sessions;
  }
}
