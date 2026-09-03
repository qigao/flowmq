#ifndef FLOWMQ_SCATTER_GATHER_H
#define FLOWMQ_SCATTER_GATHER_H

/**
 * @file flowmq_scatter_gather.h
 * Scatter/Gather session state management.
 *
 * Manages scatter request fanout and response aggregation with configurable
 * policies (ALL, FIRST_N, QUORUM). Each session tracks expected responses,
 * collects partial results, and triggers completion based on policy.
 */

#include "flowmq_esb.h"
#include "turbo_error.h"
#include <rocida/stl.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Maximum number of concurrent scatter sessions per broker */
#define FLOWMQ_SCATTER_GATHER_MAX_SESSIONS 512u

/* 1 millisecond in nanoseconds (for timeout conversion). */
#define FLOWMQ_SCATTER_GATHER_NS_PER_MS 1000000ULL
/* Maximum safe timeout in milliseconds for conversion to nanoseconds. */
#define FLOWMQ_SCATTER_GATHER_MAX_TIMEOUT_MS (UINT64_MAX / FLOWMQ_SCATTER_GATHER_NS_PER_MS)

/* Session state */
typedef enum flowmq_scatter_session_state_e {
  FLOWMQ_SCATTER_PENDING = 0,   /* Waiting for responses */
  FLOWMQ_SCATTER_COMPLETED = 1, /* Policy satisfied */
  FLOWMQ_SCATTER_TIMEOUT = 2,   /* Deadline expired */
  FLOWMQ_SCATTER_CANCELLED = 3  /* Explicitly cancelled */
} flowmq_scatter_session_state_t;

/* Single partial response */
typedef struct flowmq_scatter_partial_response_s {
  uint32_t index;       /* Response index (0-based) */
  uint64_t received_ns; /* Monotonic timestamp when received */
  tstr payload;         /* Owned response payload */
  int status;           /* Response status (TURBO_OK or error) */
} flowmq_scatter_partial_response_t;

/**
 * Scatter/Gather session.
 * Tracks one scatter request and aggregates responses.
 */
typedef struct flowmq_scatter_session_s {
  uint64_t correlation_id; /* Unique session identifier */
  uint64_t created_ns;     /* Monotonic creation timestamp */
  uint64_t deadline_ns;    /* Absolute deadline */

  flowmq_scatter_session_state_t state;
  flowmq_esb_gather_policy_t policy;

  uint32_t expected_responses; /* Total expected */
  uint32_t received_responses; /* Actually received */
  uint32_t success_responses;  /* Successful responses */
  uint32_t error_responses;    /* Failed responses */

  vec_t partial_results; /* Vec<flowmq_scatter_partial_response_t> */

  /* Client context (borrowed, for callback) */
  void *client_context;
} flowmq_scatter_session_t;

/**
 * Scatter/Gather session manager.
 * Manages multiple concurrent scatter sessions.
 */
typedef struct flowmq_scatter_gather_manager_s {
  uint32_t max_sessions;        /* Capacity limit */
  uint32_t active_sessions;     /* Current count */
  uint64_t next_correlation_id; /* Monotonic ID generator */

  hash_map_t sessions; /* Map<correlation_id, session*> */

  /* Timeout tracking (optional) */
  uint64_t default_timeout_ms;
} flowmq_scatter_gather_manager_t;

/**
 * Completion callback when session reaches terminal state.
 *
 * @param session Completed session (read-only).
 * @param context User-provided context.
 */
typedef void (*flowmq_scatter_completion_fn)(const flowmq_scatter_session_t *session,
                                             void *context);

/**
 * Initialize scatter/gather manager.
 *
 * @param manager Manager instance to initialize.
 * @param max_sessions Maximum concurrent sessions (≤512).
 * @param default_timeout_ms Default session timeout in milliseconds.
 * @return TURBO_OK on success, error code otherwise.
 */
int flowmq_scatter_gather_manager_init(flowmq_scatter_gather_manager_t *manager,
                                       uint32_t max_sessions, uint64_t default_timeout_ms);

/**
 * Destroy scatter/gather manager.
 * Cancels all active sessions and releases resources.
 *
 * @param manager Manager instance.
 */
void flowmq_scatter_gather_manager_destroy(flowmq_scatter_gather_manager_t *manager);

/**
 * Create new scatter session.
 *
 * @param manager Manager instance.
 * @param expected_responses Number of services to scatter to.
 * @param policy Aggregation policy.
 * @param timeout_ns Absolute deadline in nanoseconds, or 0 to use
 *        manager default timeout (relative to current monotonic time).
 * @param client_context User context (borrowed).
 * @param correlation_id Output: assigned correlation ID.
 * @return TURBO_OK on success, TURBO_ENOSPC if capacity reached, error otherwise.
 */
int flowmq_scatter_gather_create_session(flowmq_scatter_gather_manager_t *manager,
                                         uint32_t expected_responses,
                                         flowmq_esb_gather_policy_t policy, uint64_t timeout_ns,
                                         void *client_context, uint64_t *correlation_id);

/**
 * Create new scatter session with timeout in relative milliseconds.
 *
 * @param manager Manager instance.
 * @param expected_responses Number of services to scatter to.
 * @param policy Aggregation policy.
 * @param timeout_ms Relative timeout in milliseconds, or 0 to use
 *        manager default timeout.
 * @param client_context User context (borrowed).
 * @param correlation_id Output: assigned correlation ID.
 * @return TURBO_OK on success, TURBO_ENOSPC if capacity reached, error otherwise.
 */
int flowmq_scatter_gather_create_session_ms(flowmq_scatter_gather_manager_t *manager,
                                            uint32_t expected_responses,
                                            flowmq_esb_gather_policy_t policy, uint64_t timeout_ms,
                                            void *client_context, uint64_t *correlation_id);

/**
 * Record partial response for session.
 *
 * @param manager Manager instance.
 * @param correlation_id Session identifier.
 * @param index Response index (must be < expected_responses).
 * @param payload Response payload (moved, manager takes ownership).
 * @param status Response status (TURBO_OK or error).
 * @param received_ns Monotonic timestamp when received.
 * @return TURBO_OK on success, TURBO_ENOENT if session not found, error otherwise.
 */
int flowmq_scatter_gather_record_response(flowmq_scatter_gather_manager_t *manager,
                                          uint64_t correlation_id, uint32_t index, tstr *payload,
                                          int status, uint64_t received_ns);

/**
 * Check if session is complete based on policy.
 *
 * @param manager Manager instance.
 * @param correlation_id Session identifier.
 * @param out_state Output: current session state.
 * @return TURBO_OK if session exists, TURBO_ENOENT otherwise.
 */
int flowmq_scatter_gather_check_session(flowmq_scatter_gather_manager_t *manager,
                                        uint64_t correlation_id,
                                        flowmq_scatter_session_state_t *out_state);

/**
 * Finalize and retrieve completed session.
 * Removes session from manager and transfers ownership to caller.
 *
 * @param manager Manager instance.
 * @param correlation_id Session identifier.
 * @param out_session Output: session (caller owns, must cleanup).
 * @return TURBO_OK on success, TURBO_ENOENT if not found, TURBO_EINVAL if not complete.
 */
int flowmq_scatter_gather_finalize_session(flowmq_scatter_gather_manager_t *manager,
                                           uint64_t correlation_id,
                                           flowmq_scatter_session_t *out_session);

/**
 * Cancel active session.
 *
 * @param manager Manager instance.
 * @param correlation_id Session identifier.
 * @return TURBO_OK on success, TURBO_ENOENT if not found.
 */
int flowmq_scatter_gather_cancel_session(flowmq_scatter_gather_manager_t *manager,
                                         uint64_t correlation_id);

/**
 * Process timeouts for all active sessions.
 * Marks expired sessions as TIMEOUT state.
 *
 * @param manager Manager instance.
 * @param now_ns Current monotonic timestamp.
 * @param completion_fn Callback for timed-out sessions (optional).
 * @param context User context for callback.
 * @return Number of sessions timed out.
 */
uint32_t flowmq_scatter_gather_process_timeouts(flowmq_scatter_gather_manager_t *manager,
                                                uint64_t now_ns,
                                                flowmq_scatter_completion_fn completion_fn,
                                                void *context);

/**
 * Cleanup session resources.
 * Releases partial response payloads and resets structure.
 *
 * @param session Session to cleanup (accepts NULL).
 */
void flowmq_scatter_session_cleanup(flowmq_scatter_session_t *session);

/**
 * Get manager statistics.
 *
 * @param manager Manager instance.
 * @param active Output: active session count.
 * @param capacity Output: maximum session capacity.
 */
void flowmq_scatter_gather_stats(const flowmq_scatter_gather_manager_t *manager, uint32_t *active,
                                 uint32_t *capacity);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_SCATTER_GATHER_H */
