#ifndef FLOWMQ_DESTINATION_H
#define FLOWMQ_DESTINATION_H

#include "flowmq_socket.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_DESTINATION_VERSION 1u
#define FLOWMQ_DESTINATION_MAX_ENDPOINTS 4u

/* FlowMQ-owned policy names; CNet's policy ABI is private to this adapter. */
typedef enum flowmq_destination_kind_e {
  FLOWMQ_DESTINATION_EXPLICIT = 1,
  FLOWMQ_DESTINATION_ROUND_ROBIN = 2,
  FLOWMQ_DESTINATION_WEIGHTED_RR = 3,
  FLOWMQ_DESTINATION_LEAST_INFLIGHT = 4,
  FLOWMQ_DESTINATION_STRICT_KEY = 5
} flowmq_destination_kind_t;

/* Borrowed, immutable input snapshot. endpoint_id is a stable remote
 * identity; authority_id asserts the same application/security authority for
 * the whole set. Actual TLS certificate/hostname checks are still performed
 * by CNet for the chosen URI. The host must prefilter authorization and health
 * before publishing eligible=1, and cannot mix tcp:// and tls:// in one set. */
typedef struct flowmq_destination_endpoint_s {
  uint64_t endpoint_id;
  uint64_t authority_id;
  const char *uri;
  uint32_t weight;
  uint64_t inflight; /* Advisory only; not a reserved protocol slot. */
  int eligible;      /* Exactly 0 or 1. */
} flowmq_destination_endpoint_t;

typedef struct flowmq_destination_selection_s {
  size_t size;
  uint32_t version;
  flowmq_destination_kind_t kind;
  uint64_t snapshot_generation;
  uint64_t expires_at_ms; /* Monotonic, UINT64_MAX = immutable/no expiry. */
  uint64_t sequence; /* Caller-owned admission ticket, not per DATA packet. */
  uint64_t explicit_endpoint_id;
  uint64_t key_hash;
  int key_known;
} flowmq_destination_selection_t;

#define FLOWMQ_DESTINATION_SELECTION_INIT \
  { sizeof(flowmq_destination_selection_t), FLOWMQ_DESTINATION_VERSION, \
    FLOWMQ_DESTINATION_EXPLICIT, 0u, UINT64_MAX, 0u, 0u, 0u, 0 }

typedef struct flowmq_destination_result_s {
  size_t size;
  uint64_t snapshot_generation;
  uint64_t endpoint_id;
  size_t index; /* SIZE_MAX on error. */
} flowmq_destination_result_t;

#define FLOWMQ_DESTINATION_RESULT_INIT \
  { sizeof(flowmq_destination_result_t), 0u, 0u, SIZE_MAX }

/* Caller-driven, allocation-free remote *connection-admission* decision.
 * Validates stable sorted IDs, URIs, one authority and scheme, then delegates
 * policy to Salts CNet 2.3. Not an Owner placement, pool acquisition, network
 * operation, retry or application DATA routing action.
 *
 * Strict-key intentionally fails ENOBUFS when its HRW winner is ineligible:
 * no silent failover, TLS downgrade, changed authority or per-message rehash.
 * An expired generation fails ETIMEDOUT. The caller owns snapshots for the
 * duration of this synchronous call. */
FLOWMQ_C_API int flowmq_destination_choose(
    const flowmq_destination_endpoint_t *endpoints, size_t endpoint_count,
    const flowmq_destination_selection_t *selection,
    flowmq_destination_result_t *result);

/* Choose once and invoke existing flowmq_connect() exactly once with the
 * chosen URI. Subsequent CNet reconnects remain pinned to that URI and retain
 * FlowMQ protocol/HWM/REQ-REP semantics. Success means dial *admission*,
 * never authenticated FMQ/6 READY or remote DATA receipt. */
FLOWMQ_C_API int flowmq_connect_selected(
    flowmq_socket_t *socket,
    const flowmq_destination_endpoint_t *endpoints, size_t endpoint_count,
    const flowmq_destination_selection_t *selection,
    flowmq_destination_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_DESTINATION_H */
