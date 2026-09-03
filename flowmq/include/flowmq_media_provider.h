#ifndef FLOWMQ_MEDIA_PROVIDER_H
#define FLOWMQ_MEDIA_PROVIDER_H

#include "flowmq_export.h"
#include "flowmq_protocol_catalog.h"

#include "platform.h"
#include "flowmq_media_provider_v1.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_MEDIA_PROVIDER_API_VERSION 3u
#define FLOWMQ_MEDIA_PROVIDER_SCHEMA_VERSION FLOWMQ_PROTOCOL_FMP_VERSION

typedef struct flowmq_media_provider_limits_s {
  size_t maximum_id_bytes;
  size_t maximum_type_bytes;
  size_t maximum_timestamp_bytes;
  size_t maximum_payload_bytes;
  size_t maximum_error_bytes;
  size_t maximum_cursor_bytes;
} flowmq_media_provider_limits_t;

#define FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT {256u, 128u, 64u, 1024u * 1024u, 4096u, 1024u}

/**
 * Strictly validate one unsigned 64-bit decimal string without coercion.
 * @param text NUL-terminated canonical decimal input.
 * @param value Receives the parsed value on success.
 * @return SALTS_OK, SALTS_EINVAL, SALTS_EPROTO, or SALTS_ERANGE.
 */
FLOWMQ_C_API int flowmq_media_provider_parse_u64(const char *text, uint64_t *value);
/**
 * Read the common fixed prefix before selecting a message-specific decoder.
 * @param encoded Borrowed FMP/1 wire buffer.
 * @param encoded_size Available bytes in @p encoded.
 * @param kind Receives the validated message kind.
 * @return SALTS_OK, SALTS_EINVAL, SALTS_ENOTSUP for another schema version,
 * or SALTS_EPROTO for a truncated prefix or unknown message kind.
 */
FLOWMQ_C_API int flowmq_media_provider_peek_kind(const void *encoded, size_t encoded_size,
                                                 ProviderMessageKind_t *kind);

/**
 * Validate FMP/1 message views without copying or taking ownership. Each view
 * and its backing wire buffer must remain alive and unchanged for the duration
 * of the call. The generated `Provider*V1_view_bind()` functions construct the
 * required views; generated builders create compatible wire data.
 *
 * Every validator returns SALTS_OK on success, SALTS_EINVAL for invalid API
 * arguments or limits, SALTS_ENOTSUP for another schema version,
 * SALTS_EMSGSIZE for a configured field limit, and SALTS_EPROTO/SALTS_ERANGE
 * for malformed field data.
 */
FLOWMQ_C_API int
flowmq_media_provider_validate_command(const ProviderCommandV1_view_t *message,
                                       const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_receipt(const ProviderReceiptV1_view_t *message,
                                       const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_completion(const ProviderCompletionV1_view_t *message,
                                          const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_completion_ack(const ProviderCompletionAckV1_view_t *message,
                                              const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int flowmq_media_provider_validate_event(const ProviderEventV1_view_t *message,
                                                      const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_event_ack(const ProviderEventAckV1_view_t *message,
                                         const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int flowmq_media_provider_validate_query(const ProviderQueryV1_view_t *message,
                                                      const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_observation(const ProviderObservationV1_view_t *message,
                                           const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_call_offer(const ProviderCallOfferV1_view_t *message,
                                          const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_session_bound(const ProviderSessionBoundV1_view_t *message,
                                             const flowmq_media_provider_limits_t *limits);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_MEDIA_PROVIDER_H */
