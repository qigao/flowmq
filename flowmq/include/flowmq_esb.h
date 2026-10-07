#ifndef FLOWMQ_ESB_H
#define FLOWMQ_ESB_H

#include "flowmq_export.h"
#include "flowmq_protocol_catalog.h"

#include "str.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_ESB_HEADER_SIZE 16u
#define FLOWMQ_ESB_TLV_HEADER_SIZE 5u
#define FLOWMQ_ESB_MAX_CONSUMER_GROUP_SIZE 255u
#define FLOWMQ_ESB_MAX_FANOUT_COUNT 1024u
#define FLOWMQ_ESB_MAX_SAGA_STEPS 256u
#define FLOWMQ_ESB_MAX_STREAM_PARTITIONS 256u
#define FLOWMQ_ESB_MAX_STREAM_CONSUMER_GROUPS 64u
#define FLOWMQ_ESB_MAX_STREAM_GROUP_MEMBERS 32u

/** FES/1 application message kinds carried inside an ordinary FMQ DATA payload. */
typedef enum flowmq_esb_message_kind_e {
  FLOWMQ_ESB_SCATTER_REQUEST = 1,
  FLOWMQ_ESB_GATHER_RESPONSE,
  FLOWMQ_ESB_PARTIAL_RESPONSE,
  FLOWMQ_ESB_SAGA_EXECUTE,
  FLOWMQ_ESB_SAGA_COMMIT,
  FLOWMQ_ESB_SAGA_COMPENSATE,
  FLOWMQ_ESB_SAGA_ABORT,
  FLOWMQ_ESB_STREAM_PUBLISH,
  FLOWMQ_ESB_STREAM_SUBSCRIBE,
  FLOWMQ_ESB_STREAM_COMMIT,
  FLOWMQ_ESB_STREAM_REBALANCE,
  FLOWMQ_ESB_PRIORITY_PUBLISH,
  FLOWMQ_ESB_CIRCUIT_STATUS
} flowmq_esb_message_kind_t;

/** Stable FES/1 field identifiers. Unknown fields are protocol errors. */
typedef enum flowmq_esb_field_e {
  FLOWMQ_ESB_FIELD_EXPECTED_RESPONSES = 1,
  FLOWMQ_ESB_FIELD_AGGREGATION_POLICY,
  FLOWMQ_ESB_FIELD_PARTIAL_INDEX,
  FLOWMQ_ESB_FIELD_SAGA_ID,
  FLOWMQ_ESB_FIELD_SAGA_STEP,
  FLOWMQ_ESB_FIELD_SAGA_STATE,
  FLOWMQ_ESB_FIELD_PARTITION_ID,
  FLOWMQ_ESB_FIELD_OFFSET,
  FLOWMQ_ESB_FIELD_CONSUMER_GROUP,
  FLOWMQ_ESB_FIELD_PRIORITY,
  FLOWMQ_ESB_FIELD_CIRCUIT_STATE,
  FLOWMQ_ESB_FIELD_FAILURE_COUNT
} flowmq_esb_field_t;

typedef enum flowmq_esb_gather_policy_e {
  FLOWMQ_ESB_GATHER_ALL = 0,
  FLOWMQ_ESB_GATHER_FIRST_N,
  FLOWMQ_ESB_GATHER_QUORUM
} flowmq_esb_gather_policy_t;

typedef enum flowmq_esb_saga_state_e {
  FLOWMQ_ESB_SAGA_PENDING = 0,
  FLOWMQ_ESB_SAGA_EXECUTING,
  FLOWMQ_ESB_SAGA_COMPENSATING,
  FLOWMQ_ESB_SAGA_COMMITTED,
  FLOWMQ_ESB_SAGA_COMPENSATED,
  FLOWMQ_ESB_SAGA_PARTIALLY_COMPENSATED
} flowmq_esb_saga_state_t;

typedef enum flowmq_esb_circuit_state_e {
  FLOWMQ_ESB_CIRCUIT_CLOSED = 0,
  FLOWMQ_ESB_CIRCUIT_OPEN,
  FLOWMQ_ESB_CIRCUIT_HALF_OPEN
} flowmq_esb_circuit_state_t;

/**
 * Transport-neutral FES/1 message.
 *
 * `consumer_group` and `payload` are borrowed. A decoded value borrows from
 * the encoded input; an encoded value borrows until flowmq_esb_encode returns.
 * Only fields selected by `kind` may be non-zero/non-empty.
 */
typedef struct flowmq_esb_message_s {
  flowmq_esb_message_kind_t kind;
  uint32_t expected_responses;
  flowmq_esb_gather_policy_t aggregation_policy;
  uint32_t partial_index;
  uint64_t saga_id;
  uint32_t saga_step;
  flowmq_esb_saga_state_t saga_state;
  uint32_t partition_id;
  uint64_t offset;
  vstr consumer_group;
  uint8_t priority;
  flowmq_esb_circuit_state_t circuit_state;
  uint32_t failure_count;
  vstr payload;
} flowmq_esb_message_t;

/**
 * Validate a message and return its exact FES/1 byte count.
 *
 * @param message Borrowed application metadata and payload.
 * @param max_message_size Non-zero caller limit for the complete envelope.
 * @param encoded_size Exact byte count on success; reset to zero on entry.
 * @return SALTS_OK, SALTS_EINVAL, SALTS_EPROTO, SALTS_EMSGSIZE, or SALTS_ERANGE.
 */
FLOWMQ_C_API int flowmq_esb_encoded_size(const flowmq_esb_message_t *message,
                                         size_t max_message_size, size_t *encoded_size);
/**
 * Encode one FES/1 message.
 *
 * @param message Borrowed application metadata and payload.
 * @param max_message_size Non-zero caller limit for the complete envelope.
 * @param encoded Output initialized to NULL; caller releases success output with tstr_free().
 * @return SALTS_OK plus the validation errors above, or SALTS_ENOMEM.
 */
FLOWMQ_C_API int flowmq_esb_encode(const flowmq_esb_message_t *message, size_t max_message_size,
                                   tstr *encoded);
/**
 * Strictly decode one complete FES/1 message into views borrowed from `encoded`.
 *
 * @param encoded Complete envelope bytes kept alive while output views are used.
 * @param max_message_size Non-zero caller limit for the complete envelope.
 * @param message Output reset to zero before decoding and on malformed input.
 * @return SALTS_OK, SALTS_EINVAL, SALTS_EPROTO, SALTS_EMSGSIZE, or SALTS_ERANGE.
 */
FLOWMQ_C_API int flowmq_esb_decode(vstr encoded, size_t max_message_size,
                                   flowmq_esb_message_t *message);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_ESB_H */
