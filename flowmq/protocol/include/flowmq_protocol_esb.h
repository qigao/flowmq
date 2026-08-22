#ifndef FLOWMQ_PROTOCOL_ESB_H
#define FLOWMQ_PROTOCOL_ESB_H

#include "flowmq_export.h"

/**
 * @file flowmq_protocol_esb.h
 * FlowMQ ESB (Enterprise Service Bus) pattern extensions.
 * 
 * This file extends the base FlowMQ v3 protocol with ESB-specific patterns:
 * - SCATTER/GATHER: Request fanout with response aggregation
 * - STREAM: Kafka-style partitioned streams with consumer groups
 * - PRIORITY_QUEUE: Priority-ordered message delivery
 * - SAGA: Distributed transaction coordination with compensation
 * - CIRCUIT_BREAKER: Fault isolation and fast failure
 * 
 * ESB patterns use the reserved pattern range 12-31 and frame kinds 7-31.
 * All ESB extensions maintain wire compatibility with FMQ v3.
 */

#include "flowmq_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ESB Pattern value range: 12-31 (reserved in FMQ v3) */
#define FLOWMQ_PROTOCOL_ESB_PATTERN_MIN 12u
#define FLOWMQ_PROTOCOL_ESB_PATTERN_MAX 31u

/* ESB Frame kind range: 7-31 (reserved in FMQ v3) */
#define FLOWMQ_PROTOCOL_ESB_FRAME_MIN 7u
#define FLOWMQ_PROTOCOL_ESB_FRAME_MAX 31u

/* ESB-specific size limits */
#define FLOWMQ_PROTOCOL_ESB_MAX_CONSUMER_GROUP_SIZE 255u
#define FLOWMQ_PROTOCOL_ESB_MAX_FANOUT_COUNT 1024u
#define FLOWMQ_PROTOCOL_ESB_MAX_SAGA_STEPS 256u
#define FLOWMQ_PROTOCOL_ESB_MAX_STREAM_PARTITIONS 256u
#define FLOWMQ_PROTOCOL_ESB_MAX_STREAM_CONSUMER_GROUPS 64u
#define FLOWMQ_PROTOCOL_ESB_MAX_STREAM_GROUP_MEMBERS 32u

/**
 * ESB-specific message patterns.
 * These patterns extend the base FlowMQ patterns (1-11) with ESB semantics.
 */
typedef enum flowmq_protocol_esb_pattern_e {
  /* SCATTER/GATHER: Request fanout with aggregation */
  FLOWMQ_PROTOCOL_SCATTER = 12,    /* Scatter client (initiates fanout) */
  FLOWMQ_PROTOCOL_GATHER = 13,     /* Gather broker (aggregates responses) */
  
  /* SAGA: Distributed transaction coordination */
  FLOWMQ_PROTOCOL_SAGA_COORDINATOR = 14,  /* Saga coordinator */
  FLOWMQ_PROTOCOL_SAGA_PARTICIPANT = 15,  /* Saga participant service */
  
  /* STREAM: Partitioned log with consumer groups */
  FLOWMQ_PROTOCOL_STREAM_PRODUCER = 16,   /* Stream producer */
  FLOWMQ_PROTOCOL_STREAM_CONSUMER = 17,   /* Stream consumer with group */
  
  /* PRIORITY_QUEUE: Priority-ordered delivery */
  FLOWMQ_PROTOCOL_PRIORITY_PRODUCER = 18, /* Priority queue producer */
  FLOWMQ_PROTOCOL_PRIORITY_CONSUMER = 19, /* Priority queue consumer */
  
  /* CIRCUIT_BREAKER: Fault isolation */
  FLOWMQ_PROTOCOL_CB_CLIENT = 20,         /* Circuit breaker client */
  FLOWMQ_PROTOCOL_CB_SERVICE = 21         /* Protected service */
} flowmq_protocol_esb_pattern_t;

/**
 * ESB-specific frame kinds.
 * These extend base frame kinds (1-6) with ESB protocol messages.
 */
typedef enum flowmq_protocol_esb_frame_kind_e {
  /* SCATTER/GATHER frames */
  FLOWMQ_PROTOCOL_FRAME_SCATTER_REQUEST = 7,   /* Scatter request to broker */
  FLOWMQ_PROTOCOL_FRAME_GATHER_RESPONSE = 8,   /* Aggregated response */
  FLOWMQ_PROTOCOL_FRAME_PARTIAL_RESPONSE = 9,  /* Single service response */
  
  /* SAGA frames */
  FLOWMQ_PROTOCOL_FRAME_SAGA_EXECUTE = 10,     /* Execute saga step */
  FLOWMQ_PROTOCOL_FRAME_SAGA_COMMIT = 11,      /* Commit saga */
  FLOWMQ_PROTOCOL_FRAME_SAGA_COMPENSATE = 12,  /* Compensate step */
  FLOWMQ_PROTOCOL_FRAME_SAGA_ABORT = 13,       /* Abort saga */
  
  /* STREAM frames */
  FLOWMQ_PROTOCOL_FRAME_STREAM_PUBLISH = 14,   /* Publish to partition */
  FLOWMQ_PROTOCOL_FRAME_STREAM_SUBSCRIBE = 15, /* Subscribe with consumer group */
  FLOWMQ_PROTOCOL_FRAME_STREAM_COMMIT = 16,    /* Commit offset */
  FLOWMQ_PROTOCOL_FRAME_STREAM_REBALANCE = 17, /* Consumer group rebalance */
  
  /* PRIORITY_QUEUE frames */
  FLOWMQ_PROTOCOL_FRAME_PRIORITY_PUBLISH = 18, /* Publish with priority */
  
  /* CIRCUIT_BREAKER frames */
  FLOWMQ_PROTOCOL_FRAME_CB_STATUS = 19         /* Circuit breaker status */
} flowmq_protocol_esb_frame_kind_t;

/**
 * Scatter/Gather aggregation policy.
 * Defines when the broker considers a scatter request completed.
 */
typedef enum flowmq_protocol_gather_policy_e {
  FLOWMQ_GATHER_ALL = 0,        /* Wait for all expected responses */
  FLOWMQ_GATHER_FIRST_N = 1,    /* Complete after first N responses */
  FLOWMQ_GATHER_QUORUM = 2      /* Complete after quorum (N/2 + 1) */
} flowmq_protocol_gather_policy_t;

/**
 * Saga transaction state.
 */
typedef enum flowmq_protocol_saga_state_e {
  FLOWMQ_PROTOCOL_SAGA_PENDING = 0,          /* Initial state */
  FLOWMQ_PROTOCOL_SAGA_EXECUTING = 1,        /* Executing steps */
  FLOWMQ_PROTOCOL_SAGA_COMPENSATING = 2,     /* Compensating failed steps */
  FLOWMQ_PROTOCOL_SAGA_COMMITTED = 3,        /* Successfully committed */
  FLOWMQ_PROTOCOL_SAGA_COMPENSATED = 4,      /* Successfully compensated */
  FLOWMQ_PROTOCOL_SAGA_PARTIALLY_COMPENSATED = 5  /* Compensation failed */
} flowmq_protocol_saga_state_t;

/**
 * Circuit breaker state.
 */
typedef enum flowmq_protocol_cb_state_e {
  FLOWMQ_CB_CLOSED = 0,      /* Normal operation */
  FLOWMQ_CB_OPEN = 1,        /* Circuit broken, rejecting requests */
  FLOWMQ_CB_HALF_OPEN = 2    /* Testing recovery */
} flowmq_protocol_cb_state_t;

typedef enum flowmq_protocol_esb_decode_mode_e {
  FLOWMQ_PROTOCOL_ESB_DECODE_RELAXED = 0, /* Skip unknown TLV (compat mode). */
  FLOWMQ_PROTOCOL_ESB_DECODE_STRICT = 1,  /* Unknown TLV is treated as protocol error. */
} flowmq_protocol_esb_decode_mode_t;

/**
 * Extended frame structure for ESB patterns.
 * This structure extends flowmq_protocol_frame_t with ESB-specific fields.
 * 
 * Field usage by pattern:
 * - SCATTER/GATHER: expected_responses, aggregation_policy, partial_index
 * - SAGA: saga_id, saga_step, saga_state
 * - STREAM: partition_id, offset, consumer_group
 * - PRIORITY_QUEUE: priority
 * - CIRCUIT_BREAKER: cb_state, failure_count
 * 
 * Unused fields must be zero-initialized.
 */
typedef struct flowmq_protocol_esb_frame_s {
  /* Base FMQ v3 frame (must be first member for safe casting) */
  flowmq_protocol_frame_t base;
  
  /* SCATTER/GATHER fields */
  uint32_t expected_responses;        /* Number of services to scatter to */
  uint8_t aggregation_policy;         /* flowmq_protocol_gather_policy_t */
  uint32_t partial_index;             /* Index in aggregated response array */
  
  /* SAGA fields */
  uint64_t saga_id;                   /* Unique saga identifier */
  uint32_t saga_step;                 /* Current step number (0-based) */
  uint8_t saga_state;                 /* flowmq_protocol_saga_state_t */
  
  /* STREAM fields */
  uint32_t partition_id;              /* Target partition (0-based) */
  uint64_t offset;                    /* Message offset in partition */
  vstr consumer_group;              /* Borrowed view: lifetime tied to base frame payload. */
  
  /* PRIORITY_QUEUE fields */
  uint8_t priority;                   /* Priority level (0=lowest, 255=highest) */
  
  /* CIRCUIT_BREAKER fields */
  uint8_t cb_state;                   /* flowmq_protocol_cb_state_t */
  uint32_t failure_count;             /* Consecutive failure count */
  
  /* Reserved for future extensions (must be zero) */
  uint32_t reserved1;
  uint64_t reserved2;
} flowmq_protocol_esb_frame_t;

/**
 * Initialize an ESB frame with default values.
 * All extended fields are zero-initialized.
 */
#define FLOWMQ_PROTOCOL_ESB_FRAME_INIT \
  {0}

/**
 * Validate ESB pattern value.
 * @param pattern Pattern value to validate.
 * @return TURBO_OK if valid ESB pattern, TURBO_EINVAL otherwise.
 */
FLOWMQ_C_API int flowmq_protocol_esb_pattern_validate(flowmq_protocol_pattern_t pattern);

/**
 * Check if two ESB patterns are compatible for peer connection.
 * @param local Local endpoint pattern.
 * @param remote Remote endpoint pattern.
 * @return Non-zero if compatible, zero otherwise.
 */
FLOWMQ_C_API int flowmq_protocol_esb_patterns_compatible(
    flowmq_protocol_pattern_t local,
    flowmq_protocol_pattern_t remote);

/**
 * Encode ESB frame to wire format.
 * ESB-specific fields are encoded in the frame payload as TLV (Type-Length-Value).
 * 
 * @param frame ESB frame to encode.
 * @param max_frame_size Maximum allowed frame size.
 * @param out Output buffer (caller owns, release with tstr_free()).
 * @return TURBO_OK on success, error code otherwise.
 */
FLOWMQ_C_API int flowmq_protocol_esb_encode_frame(
    const flowmq_protocol_esb_frame_t *frame,
    size_t max_frame_size,
    tstr *out);

/**
 * Decode ESB frame from wire format.
 * ESB-specific fields are parsed from the payload TLV section.
 * Unknown TLV fields are skipped (compat mode).
 * 
 * @param data Input buffer.
 * @param data_len Input buffer length.
 * @param max_frame_size Maximum allowed frame size.
 * @param out Decoded ESB frame.
 * @param consumed Bytes consumed from input.
 * @return TURBO_OK on success, FLOWMQ_PROTOCOL_INCOMPLETE if more data needed, error otherwise.
 */
FLOWMQ_C_API int flowmq_protocol_esb_decode_frame(
    const char *data,
    size_t data_len,
    size_t max_frame_size,
    flowmq_protocol_esb_frame_t *out,
    size_t *consumed);

/**
 * Decode ESB-specific fields from an already-decoded base frame.
 *
 * This is intended for runtime paths that already decode base frames for transport
 * handling but still need ESB-specific TLV fields for application logic.
 *
 * @param base Decoded base frame (ownership remains external).
 * @param out Decoded ESB frame (borrowing policy_group and tstr views from base.payload).
 * @return TURBO_OK on success, error otherwise.
 */
FLOWMQ_C_API int flowmq_protocol_esb_decode_frame_from_base(
    const flowmq_protocol_frame_t *base,
    flowmq_protocol_esb_frame_t *out);

/**
 * Decode ESB-specific fields from an already-decoded base frame with explicit mode.
 *
 * @param base Decoded base frame (ownership remains external).
 * @param mode Decode strategy for unknown TLV fields.
 * @param out Decoded ESB frame (borrowing policy_group and tstr views from base.payload).
 * @return TURBO_OK on success, error otherwise.
 */
FLOWMQ_C_API int flowmq_protocol_esb_decode_frame_from_base_ex(
    const flowmq_protocol_frame_t *base,
    flowmq_protocol_esb_decode_mode_t mode,
    flowmq_protocol_esb_frame_t *out);

/**
 * Decode ESB frame from wire format with explicit TLV strategy.
 *
 * @param data Input buffer.
 * @param data_len Input buffer length.
 * @param max_frame_size Maximum allowed frame size.
 * @param mode Decode strategy: relaxed (default wrapper) or strict.
 * @param out Decoded ESB frame.
 * @param consumed Bytes consumed from input.
 * @return TURBO_OK on success, FLOWMQ_PROTOCOL_INCOMPLETE if more data needed, error otherwise.
 */
FLOWMQ_C_API int flowmq_protocol_esb_decode_frame_ex(
    const char *data,
    size_t data_len,
    size_t max_frame_size,
    flowmq_protocol_esb_decode_mode_t mode,
    flowmq_protocol_esb_frame_t *out,
    size_t *consumed);

/**
 * Copy consumer_group from decoded frame into owned storage.
 *
 * Decoded `frame.consumer_group` is a borrowed view over the decoded payload.
 * Call this helper to persist its content after input payload is no longer valid.
 *
 * @param frame Decoded ESB frame.
 * @param out Output owned copy.
 * @return TURBO_OK on success, TURBO_ENOMEM on allocation failure, TURBO_EINVAL on invalid args.
 */
FLOWMQ_C_API int flowmq_protocol_esb_frame_consumer_group_copy(
    const flowmq_protocol_esb_frame_t *frame,
    tstr *out);

/**
 * Clean up ESB frame resources.
 * Releases owned payload and ESB-specific owned fields.
 * 
 * @param frame ESB frame to clean up (accepts NULL).
 */
FLOWMQ_C_API void flowmq_protocol_esb_frame_cleanup(flowmq_protocol_esb_frame_t *frame);

/**
 * Calculate encoded size of ESB frame.
 * @param frame ESB frame to measure.
 * @param max_frame_size Maximum allowed frame size.
 * @param size Output: calculated size.
 * @return TURBO_OK on success, error code otherwise.
 */
FLOWMQ_C_API int flowmq_protocol_esb_encoded_size(
    const flowmq_protocol_esb_frame_t *frame,
    size_t max_frame_size,
    size_t *size);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_PROTOCOL_ESB_H */
