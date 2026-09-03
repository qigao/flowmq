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

/** Strictly validate one unsigned 64-bit decimal string without coercion. */
FLOWMQ_C_API int flowmq_media_provider_parse_u64(const char *text, uint64_t *value);
/**
 * Read the common fixed prefix before selecting a message-specific decoder.
 * Returns TURBO_ENOTSUP for another schema version and TURBO_EPROTO for an
 * unknown message kind.
 */
FLOWMQ_C_API int flowmq_media_provider_peek_kind(const void *encoded, size_t encoded_size,
                                                 ProviderMessageKind_t *kind);
FLOWMQ_C_API int
flowmq_media_provider_validate_command(const ProviderCommandV1_t *message,
                                       const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_receipt(const ProviderReceiptV1_t *message,
                                       const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_completion(const ProviderCompletionV1_t *message,
                                          const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_completion_ack(const ProviderCompletionAckV1_t *message,
                                              const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int flowmq_media_provider_validate_event(const ProviderEventV1_t *message,
                                                      const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_event_ack(const ProviderEventAckV1_t *message,
                                         const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int flowmq_media_provider_validate_query(const ProviderQueryV1_t *message,
                                                      const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_observation(const ProviderObservationV1_t *message,
                                           const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_call_offer(const ProviderCallOfferV1_t *message,
                                          const flowmq_media_provider_limits_t *limits);
FLOWMQ_C_API int
flowmq_media_provider_validate_session_bound(const ProviderSessionBoundV1_t *message,
                                             const flowmq_media_provider_limits_t *limits);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_MEDIA_PROVIDER_H */
