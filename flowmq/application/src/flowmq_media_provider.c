#include "flowmq_media_provider.h"

#include "turbo_error.h"
#include "turbo_str.h"

#include <stdint.h>
#include <string.h>

typedef struct flowmq_media_provider_envelope_view_s {
  uint32_t schema_version;
  ProviderMessageKind_t message_kind;
  const char *message_id;
  const char *correlation_id;
  const char *causation_id;
  const char *tenant_id;
  const char *provider_id;
  const char *session_id;
  const char *partition_key;
  const char *producer_id;
  const char *created_at;
  const char *deadline_at;
} flowmq_media_provider_envelope_view_t;

static int flowmq_media_provider_text(const char *value, size_t maximum,
                                      int required) {
  size_t size;
  if (!value) return TURBO_EINVAL;
  size = tstr_len((tstr_t)value);
  if (required && size == 0u) return TURBO_EPROTO;
  return size <= maximum ? TURBO_OK : TURBO_EMSGSIZE;
}

static int flowmq_media_provider_limits_validate(
    const flowmq_media_provider_limits_t *limits) {
  return limits && limits->maximum_id_bytes != 0u &&
                 limits->maximum_type_bytes != 0u &&
                 limits->maximum_timestamp_bytes != 0u &&
                 limits->maximum_payload_bytes != 0u &&
                 limits->maximum_error_bytes != 0u &&
                 limits->maximum_cursor_bytes != 0u
             ? TURBO_OK
             : TURBO_EINVAL;
}

static int flowmq_media_provider_envelope_validate(
    const flowmq_media_provider_envelope_view_t *envelope,
    const flowmq_media_provider_limits_t *limits, int require_session,
    ProviderMessageKind_t expected_kind) {
  int rc;
  if (!envelope || flowmq_media_provider_limits_validate(limits) != TURBO_OK)
    return TURBO_EINVAL;
  if (envelope->schema_version != 1u) return TURBO_ENOTSUP;
  if (!ProviderMessageKind_is_valid(envelope->message_kind) ||
      envelope->message_kind != expected_kind)
    return TURBO_EPROTO;
#define FLOWMQ_VALIDATE_ENVELOPE_TEXT(member, maximum, required)                \
  do {                                                                         \
    rc = flowmq_media_provider_text(envelope->member, maximum, required);       \
    if (rc != TURBO_OK) return rc;                                              \
  } while (0)
  FLOWMQ_VALIDATE_ENVELOPE_TEXT(message_id, limits->maximum_id_bytes, 1);
  FLOWMQ_VALIDATE_ENVELOPE_TEXT(correlation_id, limits->maximum_id_bytes, 0);
  FLOWMQ_VALIDATE_ENVELOPE_TEXT(causation_id, limits->maximum_id_bytes, 0);
  FLOWMQ_VALIDATE_ENVELOPE_TEXT(tenant_id, limits->maximum_id_bytes, 1);
  FLOWMQ_VALIDATE_ENVELOPE_TEXT(provider_id, limits->maximum_id_bytes, 1);
  FLOWMQ_VALIDATE_ENVELOPE_TEXT(session_id, limits->maximum_id_bytes,
                                require_session);
  FLOWMQ_VALIDATE_ENVELOPE_TEXT(partition_key, limits->maximum_id_bytes, 1);
  FLOWMQ_VALIDATE_ENVELOPE_TEXT(producer_id, limits->maximum_id_bytes, 1);
  FLOWMQ_VALIDATE_ENVELOPE_TEXT(created_at, limits->maximum_timestamp_bytes, 1);
  FLOWMQ_VALIDATE_ENVELOPE_TEXT(deadline_at, limits->maximum_timestamp_bytes, 0);
#undef FLOWMQ_VALIDATE_ENVELOPE_TEXT
  return TURBO_OK;
}

#define FLOWMQ_ENVELOPE(message)                                                \
  (flowmq_media_provider_envelope_view_t) {                                     \
    (message)->schema_version, (message)->message_kind, (message)->message_id,  \
        (message)->correlation_id, (message)->causation_id,                     \
        (message)->tenant_id, (message)->provider_id, (message)->session_id,    \
        (message)->partition_key, (message)->producer_id,                       \
        (message)->created_at, (message)->deadline_at                           \
  }

int flowmq_media_provider_parse_u64(const char *text, uint64_t *value) {
  uint64_t parsed = 0u;
  size_t index;
  size_t size;
  if (!text || !value) return TURBO_EINVAL;
  size = strlen(text);
  if (size == 0u || (size > 1u && text[0] == '0')) return TURBO_EPROTO;
  for (index = 0u; index < size; ++index) {
    uint64_t digit;
    if (text[index] < '0' || text[index] > '9') return TURBO_EPROTO;
    digit = (uint64_t)(text[index] - '0');
    if (parsed > (UINT64_MAX - digit) / 10u) return TURBO_ERANGE;
    parsed = parsed * 10u + digit;
  }
  *value = parsed;
  return TURBO_OK;
}

int flowmq_media_provider_peek_kind(const void *encoded, size_t encoded_size,
                                    ProviderMessageKind_t *kind) {
  ProviderCommandV1_view_t view;
  ProviderMessageKind_t parsed;
  if (!encoded || !kind) return TURBO_EINVAL;
  if (!ProviderCommandV1_view_bind(&view, encoded, encoded_size))
    return TURBO_EPROTO;
  if (ProviderCommandV1_schema_version_get(&view) != 1u) return TURBO_ENOTSUP;
  parsed = ProviderCommandV1_message_kind_get(&view);
  if (!ProviderMessageKind_is_valid(parsed)) return TURBO_EPROTO;
  *kind = parsed;
  return TURBO_OK;
}

int flowmq_media_provider_validate_command(
    const ProviderCommandV1_t *message,
    const flowmq_media_provider_limits_t *limits) {
  flowmq_media_provider_envelope_view_t envelope;
  uint64_t epoch;
  int rc;
  if (!message) return TURBO_EINVAL;
  envelope = FLOWMQ_ENVELOPE(message);
  rc = flowmq_media_provider_envelope_validate(
      &envelope, limits, 1, ProviderMessageKind_Command);
  if (rc != TURBO_OK) return rc;
  if ((rc = flowmq_media_provider_text(message->command_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->command_type, limits->maximum_type_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->worker_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->semantic_fingerprint,
                                       limits->maximum_id_bytes, 1)) != TURBO_OK ||
      (rc = flowmq_media_provider_text(message->payload_json,
                                       limits->maximum_payload_bytes, 1)) != TURBO_OK)
    return rc;
  rc = flowmq_media_provider_parse_u64(message->dispatch_epoch, &epoch);
  return rc == TURBO_OK && epoch == 0u ? TURBO_EPROTO : rc;
}

int flowmq_media_provider_validate_receipt(
    const ProviderReceiptV1_t *message,
    const flowmq_media_provider_limits_t *limits) {
  flowmq_media_provider_envelope_view_t envelope;
  uint64_t epoch;
  int rc;
  if (!message) return TURBO_EINVAL;
  envelope = FLOWMQ_ENVELOPE(message);
  rc = flowmq_media_provider_envelope_validate(
      &envelope, limits, 1, ProviderMessageKind_Receipt);
  if (rc != TURBO_OK) return rc;
  if (!ProviderReceiptDisposition_is_valid(message->disposition)) return TURBO_EPROTO;
  if ((rc = flowmq_media_provider_text(message->command_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->worker_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_code, limits->maximum_type_bytes, 0)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_message, limits->maximum_error_bytes, 0)) !=
          TURBO_OK)
    return rc;
  rc = flowmq_media_provider_parse_u64(message->dispatch_epoch, &epoch);
  return rc == TURBO_OK && epoch == 0u ? TURBO_EPROTO : rc;
}

int flowmq_media_provider_validate_completion(
    const ProviderCompletionV1_t *message,
    const flowmq_media_provider_limits_t *limits) {
  flowmq_media_provider_envelope_view_t envelope;
  uint64_t epoch;
  uint64_t completed_at_unix_ms;
  int rc;
  if (!message) return TURBO_EINVAL;
  envelope = FLOWMQ_ENVELOPE(message);
  rc = flowmq_media_provider_envelope_validate(
      &envelope, limits, 1, ProviderMessageKind_Completion);
  if (rc != TURBO_OK) return rc;
  if (!ProviderTerminalStatus_is_valid(message->terminal_status)) return TURBO_EPROTO;
  if ((rc = flowmq_media_provider_text(message->command_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->worker_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->event_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->event_type, limits->maximum_type_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->completed_at,
                                       limits->maximum_timestamp_bytes, 1)) != TURBO_OK ||
      (rc = flowmq_media_provider_text(message->completed_at_unix_ms,
                                       limits->maximum_timestamp_bytes, 1)) != TURBO_OK ||
      (rc = flowmq_media_provider_text(message->result_json,
                                       limits->maximum_payload_bytes, 1)) != TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_code, limits->maximum_type_bytes, 0)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_message, limits->maximum_error_bytes, 0)) !=
          TURBO_OK)
    return rc;
  rc = flowmq_media_provider_parse_u64(message->dispatch_epoch, &epoch);
  if (rc != TURBO_OK || epoch == 0u) return rc == TURBO_OK ? TURBO_EPROTO : rc;
  rc = flowmq_media_provider_parse_u64(message->completed_at_unix_ms,
                                       &completed_at_unix_ms);
  return rc == TURBO_OK && completed_at_unix_ms == 0u ? TURBO_EPROTO : rc;
}

int flowmq_media_provider_validate_completion_ack(
    const ProviderCompletionAckV1_t *message,
    const flowmq_media_provider_limits_t *limits) {
  flowmq_media_provider_envelope_view_t envelope;
  uint64_t epoch;
  uint64_t sequence;
  int rc;
  if (!message) return TURBO_EINVAL;
  envelope = FLOWMQ_ENVELOPE(message);
  rc = flowmq_media_provider_envelope_validate(
      &envelope, limits, 1, ProviderMessageKind_CompletionAck);
  if (rc != TURBO_OK) return rc;
  if (!ProviderCompletionAckDisposition_is_valid(message->disposition))
    return TURBO_EPROTO;
  if ((rc = flowmq_media_provider_text(message->command_id,
                                       limits->maximum_id_bytes, 1)) != TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_code,
                                       limits->maximum_type_bytes, 0)) != TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_message,
                                       limits->maximum_error_bytes, 0)) != TURBO_OK)
    return rc;
  rc = flowmq_media_provider_parse_u64(message->dispatch_epoch, &epoch);
  if (rc != TURBO_OK || epoch == 0u) return rc == TURBO_OK ? TURBO_EPROTO : rc;
  return flowmq_media_provider_parse_u64(message->committed_sequence, &sequence);
}

int flowmq_media_provider_validate_event(
    const ProviderEventV1_t *message,
    const flowmq_media_provider_limits_t *limits) {
  flowmq_media_provider_envelope_view_t envelope;
  uint64_t sequence;
  int rc;
  if (!message) return TURBO_EINVAL;
  envelope = FLOWMQ_ENVELOPE(message);
  rc = flowmq_media_provider_envelope_validate(
      &envelope, limits, 1, ProviderMessageKind_Event);
  if (rc != TURBO_OK) return rc;
  if ((rc = flowmq_media_provider_text(message->event_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->event_type, limits->maximum_type_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->aggregate_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->occurred_at,
                                       limits->maximum_timestamp_bytes, 1)) != TURBO_OK ||
      (rc = flowmq_media_provider_text(message->payload_json,
                                       limits->maximum_payload_bytes, 1)) != TURBO_OK)
    return rc;
  return flowmq_media_provider_parse_u64(message->sequence, &sequence);
}

int flowmq_media_provider_validate_event_ack(
    const ProviderEventAckV1_t *message,
    const flowmq_media_provider_limits_t *limits) {
  flowmq_media_provider_envelope_view_t envelope;
  uint64_t sequence;
  int rc;
  if (!message) return TURBO_EINVAL;
  envelope = FLOWMQ_ENVELOPE(message);
  rc = flowmq_media_provider_envelope_validate(
      &envelope, limits, 1, ProviderMessageKind_EventAck);
  if (rc != TURBO_OK) return rc;
  if (!ProviderEventAckDisposition_is_valid(message->disposition)) return TURBO_EPROTO;
  if ((rc = flowmq_media_provider_text(message->event_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_code, limits->maximum_type_bytes, 0)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_message, limits->maximum_error_bytes, 0)) !=
          TURBO_OK)
    return rc;
  return flowmq_media_provider_parse_u64(message->committed_sequence, &sequence);
}

int flowmq_media_provider_validate_query(
    const ProviderQueryV1_t *message,
    const flowmq_media_provider_limits_t *limits) {
  flowmq_media_provider_envelope_view_t envelope;
  uint64_t revision;
  int rc;
  if (!message) return TURBO_EINVAL;
  envelope = FLOWMQ_ENVELOPE(message);
  rc = flowmq_media_provider_envelope_validate(
      &envelope, limits, 0, ProviderMessageKind_Query);
  if (rc != TURBO_OK) return rc;
  if (message->limit == 0u) return TURBO_EPROTO;
  if ((rc = flowmq_media_provider_text(message->query_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->query_type, limits->maximum_type_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->cursor, limits->maximum_cursor_bytes, 0)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->payload_json,
                                       limits->maximum_payload_bytes, 1)) != TURBO_OK)
    return rc;
  return flowmq_media_provider_parse_u64(message->expected_revision, &revision);
}

int flowmq_media_provider_validate_observation(
    const ProviderObservationV1_t *message,
    const flowmq_media_provider_limits_t *limits) {
  flowmq_media_provider_envelope_view_t envelope;
  uint64_t revision;
  int rc;
  if (!message) return TURBO_EINVAL;
  envelope = FLOWMQ_ENVELOPE(message);
  rc = flowmq_media_provider_envelope_validate(
      &envelope, limits, 0, ProviderMessageKind_Observation);
  if (rc != TURBO_OK) return rc;
  if (!ProviderQueryStatus_is_valid(message->status)) return TURBO_EPROTO;
  if ((rc = flowmq_media_provider_text(message->query_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->observation_type, limits->maximum_type_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->cursor, limits->maximum_cursor_bytes, 0)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->next_cursor, limits->maximum_cursor_bytes, 0)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->payload_json,
                                       limits->maximum_payload_bytes, 1)) != TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_code, limits->maximum_type_bytes, 0)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_message, limits->maximum_error_bytes, 0)) !=
          TURBO_OK)
    return rc;
  return flowmq_media_provider_parse_u64(message->revision, &revision);
}

int flowmq_media_provider_validate_call_offer(
    const ProviderCallOfferV1_t *message,
    const flowmq_media_provider_limits_t *limits) {
  flowmq_media_provider_envelope_view_t envelope;
  uint64_t generation;
  int rc;
  if (!message) return TURBO_EINVAL;
  envelope = FLOWMQ_ENVELOPE(message);
  rc = flowmq_media_provider_envelope_validate(
      &envelope, limits, 0, ProviderMessageKind_CallOffer);
  if (rc != TURBO_OK) return rc;
  if ((rc = flowmq_media_provider_text(message->ingress_event_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->call_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->transport, limits->maximum_type_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->source, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->destination, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->payload_json,
                                       limits->maximum_payload_bytes, 1)) != TURBO_OK)
    return rc;
  return flowmq_media_provider_parse_u64(message->call_generation, &generation);
}

int flowmq_media_provider_validate_session_bound(
    const ProviderSessionBoundV1_t *message,
    const flowmq_media_provider_limits_t *limits) {
  flowmq_media_provider_envelope_view_t envelope;
  uint64_t generation;
  int rc;
  if (!message) return TURBO_EINVAL;
  envelope = FLOWMQ_ENVELOPE(message);
  rc = flowmq_media_provider_envelope_validate(
      &envelope, limits, message->accepted != 0,
      ProviderMessageKind_SessionBound);
  if (rc != TURBO_OK) return rc;
  if ((rc = flowmq_media_provider_text(message->ingress_event_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->call_id, limits->maximum_id_bytes, 1)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->bound_session_id,
                                       limits->maximum_id_bytes,
                                       message->accepted != 0)) != TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_code, limits->maximum_type_bytes, 0)) !=
          TURBO_OK ||
      (rc = flowmq_media_provider_text(message->error_message, limits->maximum_error_bytes, 0)) !=
          TURBO_OK)
    return rc;
  return flowmq_media_provider_parse_u64(message->call_generation,
                                         &generation);
}
