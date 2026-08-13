#ifndef TURBO_FLOW_FMQ_MANAGEMENT_PROTOCOL_H
#define TURBO_FLOW_FMQ_MANAGEMENT_PROTOCOL_H

#include "platform.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_TFMP_PROTOCOL_MAJOR 1u
#define TURBO_FLOW_TFMP_PROTOCOL_MINOR 0u
#define TURBO_FLOW_TFMP_HEADER_SIZE 40u
#define TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE (64u * 1024u)
#define TURBO_FLOW_TFMP_MAX_BODY_SIZE                                                              \
  (TURBO_FLOW_TFMP_MAX_MESSAGE_SIZE - TURBO_FLOW_TFMP_HEADER_SIZE)
#define TURBO_FLOW_TFMP_MAX_FIELDS 64u
#define TURBO_FLOW_TFMP_MAX_NESTING 4u
#define TURBO_FLOW_TFMP_MAX_PAGE_ITEMS (TURBO_FLOW_TFMP_MAX_FIELDS - 4u)
#define TURBO_FLOW_TFMP_FIELD_CRITICAL 0x80u
#define TURBO_FLOW_TFMP_FIELD_ID_MASK 0x7fu

#define TURBO_FLOW_TFMP_FLAG_RESPONSE 0x00000001u
#define TURBO_FLOW_TFMP_FLAG_REPLAYED 0x00000002u
#define TURBO_FLOW_TFMP_FLAG_EVENT 0x00000004u

typedef enum turbo_flow_tfmp_message_kind_e {
  /** Response-only fallback when no request kind can be recovered safely. */
  TURBO_FLOW_TFMP_PROTOCOL_ERROR = 0x7fff,
  TURBO_FLOW_TFMP_CAPABILITIES_GET = 0x0001,
  TURBO_FLOW_TFMP_HEALTH_GET = 0x0002,
  TURBO_FLOW_TFMP_TARGET_LIST = 0x0010,
  TURBO_FLOW_TFMP_TARGET_GET = 0x0011,
  TURBO_FLOW_TFMP_RESOURCE_LIST = 0x0020,
  TURBO_FLOW_TFMP_RESOURCE_GET = 0x0021,
  TURBO_FLOW_TFMP_RESOURCE_DOCUMENT_GET = 0x0022,
  TURBO_FLOW_TFMP_COMMAND_SUBMIT = 0x0030,
  TURBO_FLOW_TFMP_OPERATION_GET = 0x0040,
  TURBO_FLOW_TFMP_OPERATION_CANCEL = 0x0041,
  TURBO_FLOW_TFMP_EVENTS_GET = 0x0050,
  TURBO_FLOW_TFMP_EVENT = 0x8001
} turbo_flow_tfmp_message_kind_t;

typedef enum turbo_flow_tfmp_command_type_e {
  TURBO_FLOW_TFMP_COMMAND_FLOW_PAUSE = 0x0001,
  TURBO_FLOW_TFMP_COMMAND_FLOW_RESUME = 0x0002,
  TURBO_FLOW_TFMP_COMMAND_FLOW_DRAIN = 0x0003,
  TURBO_FLOW_TFMP_COMMAND_RESOURCE_QUIESCE = 0x0100,
  TURBO_FLOW_TFMP_COMMAND_RESOURCE_RESUME = 0x0101,
  TURBO_FLOW_TFMP_COMMAND_ENDPOINT_REPLACE = 0x0102,
  TURBO_FLOW_TFMP_COMMAND_POOL_RESIZE = 0x0103
} turbo_flow_tfmp_command_type_t;

/** Stable command payload schema registry. Zero means that a command has no payload. */
typedef enum turbo_flow_tfmp_command_schema_e {
  TURBO_FLOW_TFMP_COMMAND_SCHEMA_ENDPOINT = 1,
  TURBO_FLOW_TFMP_COMMAND_SCHEMA_POOL = 2
} turbo_flow_tfmp_command_schema_t;

/** Stable pool kind values; these do not alias turbo_flow_pool_kind_t. */
typedef enum turbo_flow_tfmp_pool_kind_e {
  TURBO_FLOW_TFMP_POOL_KIND_THREAD = 1,
  TURBO_FLOW_TFMP_POOL_KIND_CORO = 2,
  TURBO_FLOW_TFMP_POOL_KIND_DISRUPTOR = 3
} turbo_flow_tfmp_pool_kind_t;

typedef enum turbo_flow_tfmp_status_e {
  TURBO_FLOW_TFMP_STATUS_OK = 0,
  TURBO_FLOW_TFMP_STATUS_INVALID_ARGUMENT = 1,
  TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_VERSION = 2,
  TURBO_FLOW_TFMP_STATUS_UNSUPPORTED_CAPABILITY = 3,
  TURBO_FLOW_TFMP_STATUS_NOT_FOUND = 4,
  TURBO_FLOW_TFMP_STATUS_CONFLICT = 5,
  TURBO_FLOW_TFMP_STATUS_BUSY = 6,
  TURBO_FLOW_TFMP_STATUS_DEADLINE_EXCEEDED = 7,
  TURBO_FLOW_TFMP_STATUS_RESOURCE_EXHAUSTED = 8,
  TURBO_FLOW_TFMP_STATUS_UNAVAILABLE = 9,
  TURBO_FLOW_TFMP_STATUS_INTERNAL = 10,
  TURBO_FLOW_TFMP_STATUS_STALE_CURSOR = 11,
  TURBO_FLOW_TFMP_STATUS_CANCELED = 12,
  TURBO_FLOW_TFMP_STATUS_FAILED_PRECONDITION = 13
} turbo_flow_tfmp_status_t;

typedef enum turbo_flow_tfmp_disposition_e {
  TURBO_FLOW_TFMP_DISPOSITION_NONE = 0,
  TURBO_FLOW_TFMP_DISPOSITION_COMPLETED = 1,
  TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_VOLATILE = 2,
  TURBO_FLOW_TFMP_DISPOSITION_ACCEPTED_DURABLE = 3,
  TURBO_FLOW_TFMP_DISPOSITION_FAILED = 4
} turbo_flow_tfmp_disposition_t;

typedef enum turbo_flow_tfmp_owner_state_e {
  TURBO_FLOW_TFMP_OWNER_STARTING = 1,
  TURBO_FLOW_TFMP_OWNER_READY = 2,
  TURBO_FLOW_TFMP_OWNER_DRAINING = 3,
  TURBO_FLOW_TFMP_OWNER_STOPPED = 4,
  TURBO_FLOW_TFMP_OWNER_FAILED = 5
} turbo_flow_tfmp_owner_state_t;

typedef enum turbo_flow_tfmp_capability_e {
  TURBO_FLOW_TFMP_CAPABILITY_TARGET_QUERY = 1,
  TURBO_FLOW_TFMP_CAPABILITY_RESOURCE_QUERY = 2,
  TURBO_FLOW_TFMP_CAPABILITY_CONDITIONAL_COMMAND = 3,
  TURBO_FLOW_TFMP_CAPABILITY_IMMEDIATE_COMMAND = 4,
  TURBO_FLOW_TFMP_CAPABILITY_VOLATILE_OPERATION = 5,
  TURBO_FLOW_TFMP_CAPABILITY_DURABLE_OPERATION = 6,
  TURBO_FLOW_TFMP_CAPABILITY_OPERATION_CANCEL = 7,
  TURBO_FLOW_TFMP_CAPABILITY_LIVE_EVENT = 8,
  TURBO_FLOW_TFMP_CAPABILITY_EVENT_REPLAY = 9
} turbo_flow_tfmp_capability_t;

/** Stable operation states. Terminal states are SUCCEEDED, FAILED, and CANCELED. */
typedef enum turbo_flow_tfmp_operation_state_e {
  TURBO_FLOW_TFMP_OPERATION_ACCEPTED = 1,
  TURBO_FLOW_TFMP_OPERATION_RUNNING = 2,
  TURBO_FLOW_TFMP_OPERATION_SUCCEEDED = 3,
  TURBO_FLOW_TFMP_OPERATION_FAILED = 4,
  TURBO_FLOW_TFMP_OPERATION_CANCEL_REQUESTED = 5,
  TURBO_FLOW_TFMP_OPERATION_CANCELED = 6
} turbo_flow_tfmp_operation_state_t;

typedef enum turbo_flow_tfmp_event_category_e {
  TURBO_FLOW_TFMP_EVENT_LIFECYCLE = 1,
  TURBO_FLOW_TFMP_EVENT_RESOURCE = 2,
  TURBO_FLOW_TFMP_EVENT_OPERATION = 3,
  TURBO_FLOW_TFMP_EVENT_FAULT = 4
} turbo_flow_tfmp_event_category_t;

typedef enum turbo_flow_tfmp_event_type_e {
  TURBO_FLOW_TFMP_EVENT_OWNER_STATE_CHANGED = 1,
  TURBO_FLOW_TFMP_EVENT_COMMAND_COMPLETED = 2,
  TURBO_FLOW_TFMP_EVENT_OPERATION_ACCEPTED = 3,
  TURBO_FLOW_TFMP_EVENT_OPERATION_RUNNING = 4,
  TURBO_FLOW_TFMP_EVENT_OPERATION_TERMINAL = 5,
  TURBO_FLOW_TFMP_EVENT_OPERATION_CANCELED = 6,
  TURBO_FLOW_TFMP_EVENT_COMMAND_FAILED = 7
} turbo_flow_tfmp_event_type_t;

/** Stable wire kind for a management target. */
typedef enum turbo_flow_tfmp_target_kind_e {
  TURBO_FLOW_TFMP_TARGET_KIND_FLOW = 1
} turbo_flow_tfmp_target_kind_t;

/** Stable wire state for a Flow target; values do not alias turbo_flow_state_t. */
typedef enum turbo_flow_tfmp_target_state_e {
  TURBO_FLOW_TFMP_TARGET_STATE_NEW = 1,
  TURBO_FLOW_TFMP_TARGET_STATE_PARSED = 2,
  TURBO_FLOW_TFMP_TARGET_STATE_COMPILED = 3,
  TURBO_FLOW_TFMP_TARGET_STATE_STARTED = 4,
  TURBO_FLOW_TFMP_TARGET_STATE_STOPPED = 5,
  TURBO_FLOW_TFMP_TARGET_STATE_FAILED = 6
} turbo_flow_tfmp_target_state_t;

/** Stable wire kinds for TurboFlow resource metadata. */
typedef enum turbo_flow_tfmp_resource_kind_e {
  TURBO_FLOW_TFMP_RESOURCE_KIND_CONNECTION = 1,
  TURBO_FLOW_TFMP_RESOURCE_KIND_QUEUE_BUFFER = 2,
  TURBO_FLOW_TFMP_RESOURCE_KIND_POOL = 3,
  TURBO_FLOW_TFMP_RESOURCE_KIND_RUNTIME = 4,
  TURBO_FLOW_TFMP_RESOURCE_KIND_SEGMENT = 5,
  TURBO_FLOW_TFMP_RESOURCE_KIND_PROTOCOL_AGGREGATE = 6,
  TURBO_FLOW_TFMP_RESOURCE_KIND_STORAGE = 7,
  TURBO_FLOW_TFMP_RESOURCE_KIND_RULE_SET = 8,
  TURBO_FLOW_TFMP_RESOURCE_KIND_SECURITY_REALM = 9
} turbo_flow_tfmp_resource_kind_t;

/** Resource convergence state derived from generation and observed_generation. */
typedef enum turbo_flow_tfmp_resource_state_e {
  TURBO_FLOW_TFMP_RESOURCE_STATE_OBSERVED = 1,
  TURBO_FLOW_TFMP_RESOURCE_STATE_RECONCILING = 2
} turbo_flow_tfmp_resource_state_t;

typedef enum turbo_flow_tfmp_durability_e {
  TURBO_FLOW_TFMP_DURABILITY_VOLATILE = 1,
  TURBO_FLOW_TFMP_DURABILITY_DURABLE = 2
} turbo_flow_tfmp_durability_t;

#define TURBO_FLOW_TFMP_REPLY_MODE_WAIT_TERMINAL 0x0001u
#define TURBO_FLOW_TFMP_REPLY_MODE_ACCEPT_OPERATION 0x0002u
#define TURBO_FLOW_TFMP_DURABILITY_MASK_VOLATILE 0x0001u
#define TURBO_FLOW_TFMP_DURABILITY_MASK_DURABLE 0x0002u

/** Zero-copy view of one decoded TFMP envelope. */
typedef struct turbo_flow_tfmp_envelope_s {
  size_t size;
  uint16_t major;
  uint16_t minor;
  uint16_t kind;
  uint32_t flags;
  uint64_t correlation_id;
  uint16_t status;
  uint16_t disposition;
  const uint8_t *body;
  size_t body_size;
} turbo_flow_tfmp_envelope_t;

#define TURBO_FLOW_TFMP_ENVELOPE_INIT                                                              \
  {sizeof(turbo_flow_tfmp_envelope_t),                                                             \
   TURBO_FLOW_TFMP_PROTOCOL_MAJOR,                                                                 \
   TURBO_FLOW_TFMP_PROTOCOL_MINOR,                                                                 \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u}

/** Borrowed view of one canonical LTV body field. */
typedef struct turbo_flow_tfmp_field_s {
  size_t size;
  uint8_t id;
  int critical;
  const uint8_t *value;
  size_t value_size;
} turbo_flow_tfmp_field_t;

#define TURBO_FLOW_TFMP_FIELD_INIT {sizeof(turbo_flow_tfmp_field_t), 0u, 0, NULL, 0u}

/** Stateful zero-copy iterator. Initialize it before the first next call. */
typedef struct turbo_flow_tfmp_field_iterator_s {
  size_t size;
  const uint8_t *body;
  size_t body_size;
  size_t offset;
  size_t field_count;
  uint8_t previous_id;
  uint8_t previous_type;
} turbo_flow_tfmp_field_iterator_t;

#define TURBO_FLOW_TFMP_FIELD_ITERATOR_INIT                                                        \
  {sizeof(turbo_flow_tfmp_field_iterator_t), NULL, 0u, 0u, 0u, 0u, 0u}

/** Caller-buffer builder for an ordered canonical LTV body. */
typedef struct turbo_flow_tfmp_body_builder_s {
  size_t size;
  uint8_t *data;
  size_t capacity;
  size_t length;
  size_t field_count;
  uint8_t previous_id;
  uint8_t previous_type;
} turbo_flow_tfmp_body_builder_t;

#define TURBO_FLOW_TFMP_BODY_BUILDER_INIT                                                          \
  {sizeof(turbo_flow_tfmp_body_builder_t), NULL, 0u, 0u, 0u, 0u, 0u}

/**
 * Validate and encode one complete TFMP envelope without partial output.
 *
 * `envelope->body` is borrowed for the call. `out` may be NULL to query the
 * exact size through `out_len`.
 *
 * @return TURBO_OK, TURBO_EINVAL for invalid envelope semantics,
 *         TURBO_EMSGSIZE for a body above the protocol limit,
 *         TURBO_EPROTO for invalid canonical LTV, or TURBO_ENOSPC when `out`
 *         is NULL or too small. `out` is unchanged on failure.
 */
CXX_C_API int turbo_flow_tfmp_envelope_encode(const turbo_flow_tfmp_envelope_t *envelope,
                                              uint8_t *out, size_t capacity, size_t *out_len);

/**
 * Decode exactly one envelope. Trailing bytes are rejected and `out` is
 * unchanged on failure. The returned body borrows `data` and remains valid
 * only while the input bytes remain alive and unchanged.
 *
 * @return TURBO_OK, TURBO_EINVAL for invalid arguments/output ABI,
 *         TURBO_ENOTSUP for another major version, TURBO_EMSGSIZE for an
 *         oversized envelope, or TURBO_EPROTO for malformed bytes.
 */
CXX_C_API int turbo_flow_tfmp_envelope_decode(const uint8_t *data, size_t data_len,
                                              turbo_flow_tfmp_envelope_t *out);

/**
 * Recover the request identity from a possibly malformed frame.
 *
 * A valid magic and enough fixed-header bytes are required for each value.
 * When the kind cannot be trusted, `kind` is set to
 * TURBO_FLOW_TFMP_PROTOCOL_ERROR. When the correlation ID cannot be trusted,
 * it is set to zero. This helper performs no full envelope validation.
 */
CXX_C_API void turbo_flow_tfmp_envelope_peek_request_identity(const uint8_t *data, size_t data_len,
                                                              uint16_t *kind,
                                                              uint64_t *correlation_id);

/** Map one process-local Turbo error to the stable TFMP protocol status. */
CXX_C_API turbo_flow_tfmp_status_t turbo_flow_tfmp_status_from_error(int error);

/**
 * Validate a complete canonical LTV body and optionally return its field count.
 * Empty bodies use `body == NULL && body_size == 0`.
 *
 * @return TURBO_OK, TURBO_EINVAL for an invalid view, TURBO_EMSGSIZE for body
 *         or field-count limits, or TURBO_EPROTO for noncanonical LTV/order.
 */
CXX_C_API int turbo_flow_tfmp_body_validate(const uint8_t *body, size_t body_size,
                                            size_t *field_count);

/** Initialize an iterator over caller-owned immutable body bytes. */
CXX_C_API int turbo_flow_tfmp_field_iterator_init(turbo_flow_tfmp_field_iterator_t *iterator,
                                                  const uint8_t *body, size_t body_size);

/**
 * Advance an iterator. `out->value` borrows the iterator body and no allocation
 * occurs. Repeated adjacent IDs are accepted only when their critical bit is
 * identical.
 *
 * @return TURBO_OK for a field, TURBO_ENOENT at end, TURBO_EINVAL for an
 *         invalid ABI/argument, TURBO_EMSGSIZE at a limit, or TURBO_EPROTO for
 *         malformed/noncanonical input. `out` is unchanged on failure.
 */
CXX_C_API int turbo_flow_tfmp_field_iterator_next(turbo_flow_tfmp_field_iterator_t *iterator,
                                                  turbo_flow_tfmp_field_t *out);

/** Initialize a builder over a caller-owned output buffer; no ownership transfers. */
CXX_C_API int turbo_flow_tfmp_body_builder_init(turbo_flow_tfmp_body_builder_t *builder,
                                                uint8_t *data, size_t capacity);

/**
 * Append a field in nondecreasing field-ID order. Repeated adjacent IDs are
 * allowed with a stable critical bit. Empty values may use `value == NULL`.
 *
 * @return TURBO_OK, TURBO_EINVAL for invalid state/order, TURBO_EMSGSIZE at a
 *         protocol limit, TURBO_ENOSPC for caller-buffer exhaustion, or
 *         TURBO_EPROTO if the shared TurboUtils builder violates its contract.
 *         Builder length/order state is unchanged on failure.
 *
 * @code
 * uint8_t body[64];
 * turbo_flow_tfmp_body_builder_t b = TURBO_FLOW_TFMP_BODY_BUILDER_INIT;
 * turbo_flow_tfmp_body_builder_init(&b, body, sizeof(body));
 * turbo_flow_tfmp_body_builder_append(&b, 1, 1,
 *                                     (const uint8_t *)"node-a", 6);
 * @endcode
 */
CXX_C_API int turbo_flow_tfmp_body_builder_append(turbo_flow_tfmp_body_builder_t *builder,
                                                  uint8_t field_id, int critical,
                                                  const uint8_t *value, size_t value_size);

/** Typed network-order field builders. They preserve builder state on failure. */
CXX_C_API int turbo_flow_tfmp_body_builder_append_u16(turbo_flow_tfmp_body_builder_t *builder,
                                                      uint8_t field_id, int critical,
                                                      uint16_t value);
CXX_C_API int turbo_flow_tfmp_body_builder_append_u32(turbo_flow_tfmp_body_builder_t *builder,
                                                      uint8_t field_id, int critical,
                                                      uint32_t value);
CXX_C_API int turbo_flow_tfmp_body_builder_append_u64(turbo_flow_tfmp_body_builder_t *builder,
                                                      uint8_t field_id, int critical,
                                                      uint64_t value);
CXX_C_API int turbo_flow_tfmp_body_builder_append_i32(turbo_flow_tfmp_body_builder_t *builder,
                                                      uint8_t field_id, int critical,
                                                      int32_t value);
CXX_C_API int turbo_flow_tfmp_body_builder_append_bool(turbo_flow_tfmp_body_builder_t *builder,
                                                       uint8_t field_id, int critical, int value);
CXX_C_API int turbo_flow_tfmp_body_builder_append_utf8(turbo_flow_tfmp_body_builder_t *builder,
                                                       uint8_t field_id, int critical,
                                                       const char *value, size_t value_size);

/** Typed readers reject incorrect widths; BOOL accepts only wire values 0 and 1. */
CXX_C_API int turbo_flow_tfmp_field_read_u16(const turbo_flow_tfmp_field_t *field, uint16_t *out);
CXX_C_API int turbo_flow_tfmp_field_read_u32(const turbo_flow_tfmp_field_t *field, uint32_t *out);
CXX_C_API int turbo_flow_tfmp_field_read_u64(const turbo_flow_tfmp_field_t *field, uint64_t *out);
CXX_C_API int turbo_flow_tfmp_field_read_i32(const turbo_flow_tfmp_field_t *field, int32_t *out);
CXX_C_API int turbo_flow_tfmp_field_read_bool(const turbo_flow_tfmp_field_t *field, int *out);

/** Validate that a field is strict UTF-8 and contains no embedded NUL. */
CXX_C_API int turbo_flow_tfmp_field_validate_utf8(const turbo_flow_tfmp_field_t *field);

/**
 * Validate a decoded envelope against the built-in TFMP/1 field registry.
 *
 * This enforces field type/width, required and repeated markers, response
 * common fields, command payload schemas, and the four-level nesting limit.
 * Unknown optional fields are skipped; unknown critical fields and unsupported
 * request/success-response message or command kinds return TURBO_ENOTSUP.
 * Error responses with an unknown echoed kind still validate their common
 * identity fields. The envelope and all body views remain caller-owned and
 * unchanged.
 *
 * Call this after turbo_flow_tfmp_envelope_decode(). A non-OK RPC response does
 * not require success-only fields, but still requires authority_id and the
 * 16-byte incarnation_id.
 *
 * @return TURBO_OK, TURBO_EINVAL for an invalid ABI/view, TURBO_ENOTSUP for an
 *         unsupported kind/critical field/command, TURBO_EMSGSIZE at a nesting
 *         or body limit, or TURBO_EPROTO for a schema violation.
 */
CXX_C_API int turbo_flow_tfmp_envelope_validate_schema(const turbo_flow_tfmp_envelope_t *envelope);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_MANAGEMENT_PROTOCOL_H */
