#ifndef TURBO_FLOW_FMQ_PUBSUB_H
#define TURBO_FLOW_FMQ_PUBSUB_H

#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_str_view.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_FMQ_PUBSUB_API_VERSION 1u
#define TURBO_FLOW_FMQ_PUBSUB_MAX_TOPICS 65536u
#define TURBO_FLOW_FMQ_PUBSUB_MAX_UPDATES 262144u
#define TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE 1024u
#define TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE 56u
#define TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_HEADER_SIZE 20u
#define TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_MAJOR 1u
#define TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_MINOR 0u
#define TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MAX_RECORDS 65535u
#define TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MEDIA_TYPE                                              \
  "application/vnd.turboflow.fmq-pubsub-v1"

typedef struct turbo_flow_fmq_pubsub_state_s turbo_flow_fmq_pubsub_state_t;
typedef struct turbo_flow_fmq_pubsub_snapshot_cursor_s turbo_flow_fmq_pubsub_snapshot_cursor_t;
typedef struct turbo_flow_fmq_pubsub_update_cursor_s turbo_flow_fmq_pubsub_update_cursor_t;
typedef struct turbo_flow_fmq_pubsub_recovery_service_s
    turbo_flow_fmq_pubsub_recovery_service_t;

typedef struct turbo_flow_fmq_pubsub_config_s {
  size_t size;
  uint32_t version;
  size_t max_topics;
  size_t max_state_bytes;
  size_t update_capacity;
  size_t max_update_bytes;
} turbo_flow_fmq_pubsub_config_t;

#define TURBO_FLOW_FMQ_PUBSUB_CONFIG_INIT                                                          \
  {sizeof(turbo_flow_fmq_pubsub_config_t),                                                         \
   TURBO_FLOW_FMQ_PUBSUB_API_VERSION,                                                              \
   4096u,                                                                                          \
   16u * 1024u * 1024u,                                                                            \
   8192u,                                                                                          \
   32u * 1024u * 1024u}

typedef enum turbo_flow_fmq_pubsub_operation_e {
  TURBO_FLOW_FMQ_PUBSUB_PUT = 1,
  TURBO_FLOW_FMQ_PUBSUB_DELETE
} turbo_flow_fmq_pubsub_operation_t;

/**
 * Immutable borrowed record returned by a snapshot or update cursor.
 * The views remain valid until that cursor is destroyed.
 */
typedef struct turbo_flow_fmq_pubsub_record_s {
  size_t size;
  turbo_flow_fmq_pubsub_operation_t operation;
  uint64_t sequence;
  tstr_v topic;
  tstr_v payload;
} turbo_flow_fmq_pubsub_record_t;

#define TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT                                                          \
  {sizeof(turbo_flow_fmq_pubsub_record_t), TURBO_FLOW_FMQ_PUBSUB_PUT, 0u, {0}, {0}}

typedef struct turbo_flow_fmq_pubsub_status_s {
  size_t size;
  size_t topics;
  size_t state_bytes;
  size_t retained_updates;
  size_t retained_update_bytes;
  uint64_t earliest_update_sequence;
  uint64_t latest_sequence;
} turbo_flow_fmq_pubsub_status_t;

#define TURBO_FLOW_FMQ_PUBSUB_STATUS_INIT                                                          \
  {sizeof(turbo_flow_fmq_pubsub_status_t), 0u, 0u, 0u, 0u, 0u, 0u}

/** TFPS/1 message kind carried inside an ordinary FMQ DATA payload. */
typedef enum turbo_flow_fmq_pubsub_recovery_kind_e {
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES = 1,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_SNAPSHOT = 2,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES = 3,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_LIVE_UPDATE = 4,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_ERROR = 0x7fff
} turbo_flow_fmq_pubsub_recovery_kind_t;

/** Stable TFPS/1 status; native_status is diagnostic and must not drive client control flow. */
typedef enum turbo_flow_fmq_pubsub_recovery_status_e {
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK = 0,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_BAD_REQUEST = 1,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_UNSUPPORTED_VERSION = 2,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_UNSUPPORTED_KIND = 3,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_STALE_CURSOR = 4,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_RESOURCE_EXHAUSTED = 5,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_INTERNAL = 6
} turbo_flow_fmq_pubsub_recovery_status_t;

typedef enum turbo_flow_fmq_pubsub_recovery_flags_e {
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_REQUEST = 1u << 0,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_RESPONSE = 1u << 1,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE = 1u << 2
} turbo_flow_fmq_pubsub_recovery_flags_t;

typedef enum turbo_flow_fmq_pubsub_recovery_capability_e {
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAP_SNAPSHOT = 1u << 0,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAP_UPDATES = 1u << 1,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAP_LIVE_SEQUENCE = 1u << 2,
  TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAP_PREFIX = 1u << 3
} turbo_flow_fmq_pubsub_recovery_capability_t;

/** Zero-copy decoded TFPS/1 header and body views. */
typedef struct turbo_flow_fmq_pubsub_recovery_message_s {
  size_t size;
  uint16_t protocol_major;
  uint16_t protocol_minor;
  turbo_flow_fmq_pubsub_recovery_kind_t kind;
  turbo_flow_fmq_pubsub_recovery_status_t status;
  uint32_t flags;
  uint32_t capabilities;
  int native_status;
  uint64_t request_id;
  uint64_t sequence;
  uint64_t upper_bound;
  uint16_t record_count;
  tstr_v prefix;
  tstr_v records;
} turbo_flow_fmq_pubsub_recovery_message_t;

#define TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT                                                \
  {sizeof(turbo_flow_fmq_pubsub_recovery_message_t),                                               \
   TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_MAJOR,                                                  \
   TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_MINOR,                                                  \
   TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES,                                                    \
   TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK,                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0,                                                                                              \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   {0},                                                                                            \
   {0}}

typedef struct turbo_flow_fmq_pubsub_recovery_record_iterator_s {
  size_t size;
  tstr_v records;
  size_t offset;
  uint16_t remaining;
} turbo_flow_fmq_pubsub_recovery_record_iterator_t;

#define TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_ITERATOR_INIT                                        \
  {sizeof(turbo_flow_fmq_pubsub_recovery_record_iterator_t), {0}, 0u, 0u}

typedef struct turbo_flow_fmq_pubsub_recovery_config_s {
  size_t size;
  uint32_t version;
  size_t max_reply_bytes;
  uint16_t max_update_records;
} turbo_flow_fmq_pubsub_recovery_config_t;

#define TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CONFIG_INIT                                                 \
  {sizeof(turbo_flow_fmq_pubsub_recovery_config_t), TURBO_FLOW_FMQ_PUBSUB_API_VERSION,             \
   8u * 1024u * 1024u, 1024u}

/**
 * Create a bounded Chapter 5 last-value and ordered-update state owner.
 *
 * Calls that mutate or inspect one state must be serialized by the host. The owner copies all
 * topics and payloads and performs no network or storage I/O. A failed mutation leaves the
 * latest-value state and sequence unchanged.
 *
 * @param config Versioned limits; all four capacity fields must be positive.
 * @return New owner, or NULL for invalid config or allocation failure.
 */
CXX_C_API turbo_flow_fmq_pubsub_state_t *
turbo_flow_fmq_pubsub_state_create(const turbo_flow_fmq_pubsub_config_t *config);

/**
 * Create from a resolved YAML `fmq_pattern` channel whose pattern is `pubsub_state`.
 * Unknown, broker-only, or zero-valued fields fail fast.
 *
 * Example channel:
 * `config: {pattern: pubsub_state, max_topics: 4096, update_capacity: 8192}`.
 *
 * @return TURBO_OK, TURBO_EINVAL, TURBO_ENOTSUP, TURBO_ERANGE, or TURBO_ENOMEM.
 */
CXX_C_API int turbo_flow_fmq_pubsub_state_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_fmq_pubsub_state_t **out, turbo_flow_config_error_t *error);

CXX_C_API void turbo_flow_fmq_pubsub_state_destroy(turbo_flow_fmq_pubsub_state_t *state);

/**
 * Store or replace one exact topic and append the same transition to the ordered journal.
 * Prefix semantics are applied only by snapshot/update readers. Empty topics are valid and
 * represent the root topic. Topics longer than TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE return
 * TURBO_ENAMETOOLONG. TURBO_ENOSPC means a state or journal byte/count limit rejected the
 * operation; no sequence is consumed.
 */
CXX_C_API int turbo_flow_fmq_pubsub_put(turbo_flow_fmq_pubsub_state_t *state, tstr_v topic,
                                        tstr_v payload, uint64_t *sequence);

/**
 * Delete one topic and append a tombstone even when the topic is not currently present.
 * This lets recovering subscribers remove stale local state deterministically.
 * Topics longer than TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE return TURBO_ENAMETOOLONG.
 */
CXX_C_API int turbo_flow_fmq_pubsub_delete(turbo_flow_fmq_pubsub_state_t *state, tstr_v topic,
                                           uint64_t *sequence);

/**
 * Open an immutable, deep-copied last-value snapshot for one byte-prefix subtree.
 * The cursor's barrier is the latest committed sequence observed atomically with the snapshot.
 * Records are state values and are not promised to be sequence-sorted.
 * Prefixes longer than TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE return TURBO_ENAMETOOLONG.
 */
CXX_C_API int turbo_flow_fmq_pubsub_snapshot_open(const turbo_flow_fmq_pubsub_state_t *state,
                                                  tstr_v prefix,
                                                  turbo_flow_fmq_pubsub_snapshot_cursor_t **out);

CXX_C_API uint64_t
turbo_flow_fmq_pubsub_snapshot_barrier(const turbo_flow_fmq_pubsub_snapshot_cursor_t *cursor);

/** Return TURBO_OK for one record or TURBO_ENOENT at the end. */
CXX_C_API int turbo_flow_fmq_pubsub_snapshot_next(turbo_flow_fmq_pubsub_snapshot_cursor_t *cursor,
                                                  turbo_flow_fmq_pubsub_record_t *record);

CXX_C_API void
turbo_flow_fmq_pubsub_snapshot_destroy(turbo_flow_fmq_pubsub_snapshot_cursor_t *cursor);

/**
 * Open an immutable cursor over retained transitions `(after_sequence, upper_bound]` matching a
 * byte-prefix subtree. Records are returned in strictly increasing sequence order.
 *
 * TURBO_ERANGE means `after_sequence` is ahead of the owner or is older than the retained journal;
 * the caller must acquire a new snapshot instead of accepting a gap.
 */
CXX_C_API int turbo_flow_fmq_pubsub_updates_open(const turbo_flow_fmq_pubsub_state_t *state,
                                                 tstr_v prefix, uint64_t after_sequence,
                                                 turbo_flow_fmq_pubsub_update_cursor_t **out);

CXX_C_API uint64_t
turbo_flow_fmq_pubsub_updates_upper_bound(const turbo_flow_fmq_pubsub_update_cursor_t *cursor);

/** Return TURBO_OK for one record or TURBO_ENOENT at the end. */
CXX_C_API int turbo_flow_fmq_pubsub_updates_next(turbo_flow_fmq_pubsub_update_cursor_t *cursor,
                                                 turbo_flow_fmq_pubsub_record_t *record);

CXX_C_API void turbo_flow_fmq_pubsub_updates_destroy(turbo_flow_fmq_pubsub_update_cursor_t *cursor);

CXX_C_API int turbo_flow_fmq_pubsub_status(const turbo_flow_fmq_pubsub_state_t *state,
                                           turbo_flow_fmq_pubsub_status_t *status);

/**
 * Encode one TFPS/1 recovery request into caller storage.
 *
 * CAPABILITIES requires empty prefix/sequence/upper/page fields. SNAPSHOT accepts a prefix and
 * requires the numeric fields to be zero. UPDATES requires a non-zero page_limit; upper_bound is
 * zero for "freeze at the current owner sequence" or a previously returned fixed upper bound.
 * `out` is unchanged on TURBO_ENOSPC and `out_size` receives the required size.
 */
CXX_C_API int turbo_flow_fmq_pubsub_recovery_request_encode(
    turbo_flow_fmq_pubsub_recovery_kind_t kind, uint64_t request_id, tstr_v prefix,
    uint64_t after_sequence, uint64_t upper_bound, uint16_t page_limit, uint8_t *out,
    size_t capacity, size_t *out_size);

/** Decode and strictly validate one complete TFPS/1 message without copying its body. */
CXX_C_API int turbo_flow_fmq_pubsub_recovery_message_decode(
    const uint8_t *data, size_t data_size, turbo_flow_fmq_pubsub_recovery_message_t *message);

/** Initialize a zero-copy iterator after a successful message decode. */
CXX_C_API int turbo_flow_fmq_pubsub_recovery_record_iterator_init(
    const turbo_flow_fmq_pubsub_recovery_message_t *message,
    turbo_flow_fmq_pubsub_recovery_record_iterator_t *iterator);

/** Return TURBO_OK for one record or TURBO_ENOENT after exactly record_count records. */
CXX_C_API int turbo_flow_fmq_pubsub_recovery_record_next(
    turbo_flow_fmq_pubsub_recovery_record_iterator_t *iterator,
    turbo_flow_fmq_pubsub_record_t *record);

/**
 * Create a thin TFPS/1 recovery adapter borrowing an existing state owner.
 *
 * The service never owns or copies state. State mutation stages and recovery stage must execute on
 * the same serialized owner lane. max_reply_bytes is a hard allocation/wire bound; an oversized
 * snapshot becomes a terminal RESOURCE_EXHAUSTED response rather than a partial snapshot.
 */
CXX_C_API int turbo_flow_fmq_pubsub_recovery_service_create(
    turbo_flow_fmq_pubsub_state_t *state, const turbo_flow_fmq_pubsub_recovery_config_t *config,
    turbo_flow_fmq_pubsub_recovery_service_t **out);

CXX_C_API void turbo_flow_fmq_pubsub_recovery_service_destroy(
    turbo_flow_fmq_pubsub_recovery_service_t *service);

/**
 * Store a raw publication as PUT, then replace its payload with one sequenced TFPS LIVE_UPDATE.
 * Topic comes from FMQ input metadata, or from a valid content descriptor when called locally.
 */
CXX_C_API int turbo_flow_fmq_pubsub_put_stage(turbo_flow_msg_t *msg, void *ctx);

/**
 * Store a tombstone, then replace its payload with one sequenced TFPS LIVE_UPDATE.
 * Topic resolution is identical to turbo_flow_fmq_pubsub_put_stage().
 */
CXX_C_API int turbo_flow_fmq_pubsub_delete_stage(turbo_flow_msg_t *msg, void *ctx);

/** Replace one TFPS request with exactly one terminal response for an ordinary FMQ REP graph. */
CXX_C_API int turbo_flow_fmq_pubsub_recovery_stage(turbo_flow_msg_t *msg, void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_PUBSUB_H */
