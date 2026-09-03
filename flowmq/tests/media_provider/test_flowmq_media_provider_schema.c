#include "flowmq_media_provider.h"

#include "salts_error.h"
#include "tinytest.h"

#include <stdint.h>
#include <string.h>

enum { FLOWMQ_MEDIA_PROVIDER_TEST_BUFFER_SIZE = 2048u };

#define FLOWMQ_TEST_SET_TEXT(record, builder, member, value)                                       \
  do {                                                                                             \
    if (!record##_##member##_set(&(builder), (value), strlen(value))) return 0;                    \
  } while (0)

#define FLOWMQ_TEST_SET_ENVELOPE(record, builder, kind)                                            \
  do {                                                                                             \
    if (!record##_schema_version_set(&(builder), FLOWMQ_MEDIA_PROVIDER_SCHEMA_VERSION) ||          \
        !record##_message_kind_set(&(builder), (kind)))                                            \
      return 0;                                                                                    \
    FLOWMQ_TEST_SET_TEXT(record, builder, message_id, "msg-1");                                    \
    FLOWMQ_TEST_SET_TEXT(record, builder, correlation_id, "corr-1");                               \
    FLOWMQ_TEST_SET_TEXT(record, builder, causation_id, "cause-1");                                \
    FLOWMQ_TEST_SET_TEXT(record, builder, tenant_id, "tenant-1");                                  \
    FLOWMQ_TEST_SET_TEXT(record, builder, provider_id, "turbomedia");                              \
    FLOWMQ_TEST_SET_TEXT(record, builder, session_id, "session-1");                                \
    FLOWMQ_TEST_SET_TEXT(record, builder, partition_key, "session-1");                             \
    FLOWMQ_TEST_SET_TEXT(record, builder, producer_id, "iris-a");                                  \
    FLOWMQ_TEST_SET_TEXT(record, builder, created_at, "2026-08-14T00:00:00Z");                     \
    FLOWMQ_TEST_SET_TEXT(record, builder, deadline_at, "2026-08-14T00:00:30Z");                    \
  } while (0)

static int flowmq_test_build_command(uint8_t *buffer, size_t size, ProviderCommandV1_view_t *view) {
  ProviderCommandV1_builder_t builder;
  memset(buffer, 0, size);
  if (!ProviderCommandV1_builder_bind(&builder, buffer, size)) return 0;
  FLOWMQ_TEST_SET_ENVELOPE(ProviderCommandV1, builder, ProviderMessageKind_Command);
  FLOWMQ_TEST_SET_TEXT(ProviderCommandV1, builder, command_id, "command-1");
  FLOWMQ_TEST_SET_TEXT(ProviderCommandV1, builder, command_type, "media.play");
  FLOWMQ_TEST_SET_TEXT(ProviderCommandV1, builder, worker_id, "worker-a");
  FLOWMQ_TEST_SET_TEXT(ProviderCommandV1, builder, dispatch_epoch, "18446744073709551615");
  FLOWMQ_TEST_SET_TEXT(ProviderCommandV1, builder, semantic_fingerprint, "sha256:abc");
  FLOWMQ_TEST_SET_TEXT(ProviderCommandV1, builder, payload_json, "{\"text\":\"hello\"}");
  return ProviderCommandV1_view_bind(view, buffer, size) ? 1 : 0;
}

static int flowmq_test_build_receipt(uint8_t *buffer, size_t size,
                                     ProviderReceiptDisposition_t disposition,
                                     ProviderReceiptV1_view_t *view) {
  ProviderReceiptV1_builder_t builder;
  memset(buffer, 0, size);
  if (!ProviderReceiptV1_builder_bind(&builder, buffer, size)) return 0;
  FLOWMQ_TEST_SET_ENVELOPE(ProviderReceiptV1, builder, ProviderMessageKind_Receipt);
  if (!ProviderReceiptV1_disposition_set(&builder, disposition) ||
      !ProviderReceiptV1_status_code_set(&builder, 0) ||
      !ProviderReceiptV1_retry_after_ms_set(&builder, 0u))
    return 0;
  FLOWMQ_TEST_SET_TEXT(ProviderReceiptV1, builder, command_id, "command-1");
  FLOWMQ_TEST_SET_TEXT(ProviderReceiptV1, builder, worker_id, "worker-a");
  FLOWMQ_TEST_SET_TEXT(ProviderReceiptV1, builder, dispatch_epoch, "9");
  FLOWMQ_TEST_SET_TEXT(ProviderReceiptV1, builder, error_code, "");
  FLOWMQ_TEST_SET_TEXT(ProviderReceiptV1, builder, error_message, "");
  return ProviderReceiptV1_view_bind(view, buffer, size) ? 1 : 0;
}

static int flowmq_test_build_completion(uint8_t *buffer, size_t size, const char *event_id,
                                        ProviderCompletionV1_view_t *view) {
  ProviderCompletionV1_builder_t builder;
  memset(buffer, 0, size);
  if (!ProviderCompletionV1_builder_bind(&builder, buffer, size)) return 0;
  FLOWMQ_TEST_SET_ENVELOPE(ProviderCompletionV1, builder, ProviderMessageKind_Completion);
  if (!ProviderCompletionV1_terminal_status_set(&builder, ProviderTerminalStatus_Succeeded))
    return 0;
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionV1, builder, command_id, "command-1");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionV1, builder, worker_id, "worker-a");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionV1, builder, dispatch_epoch, "9");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionV1, builder, event_id, event_id);
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionV1, builder, event_type, "play.finished");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionV1, builder, completed_at, "2026-08-14T00:00:02Z");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionV1, builder, completed_at_unix_ms, "1786665602000");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionV1, builder, result_json, "{}");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionV1, builder, error_code, "");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionV1, builder, error_message, "");
  return ProviderCompletionV1_view_bind(view, buffer, size) ? 1 : 0;
}

static int flowmq_test_build_event_ack(uint8_t *buffer, size_t size,
                                       ProviderEventAckV1_view_t *view) {
  ProviderEventAckV1_builder_t builder;
  memset(buffer, 0, size);
  if (!ProviderEventAckV1_builder_bind(&builder, buffer, size)) return 0;
  FLOWMQ_TEST_SET_ENVELOPE(ProviderEventAckV1, builder, ProviderMessageKind_EventAck);
  if (!ProviderEventAckV1_disposition_set(&builder, ProviderEventAckDisposition_Committed))
    return 0;
  FLOWMQ_TEST_SET_TEXT(ProviderEventAckV1, builder, event_id, "event-1");
  FLOWMQ_TEST_SET_TEXT(ProviderEventAckV1, builder, committed_sequence, "18446744073709551615");
  FLOWMQ_TEST_SET_TEXT(ProviderEventAckV1, builder, error_code, "");
  FLOWMQ_TEST_SET_TEXT(ProviderEventAckV1, builder, error_message, "");
  return ProviderEventAckV1_view_bind(view, buffer, size) ? 1 : 0;
}

static int flowmq_test_build_completion_ack(uint8_t *buffer, size_t size,
                                            ProviderCompletionAckV1_view_t *view) {
  ProviderCompletionAckV1_builder_t builder;
  memset(buffer, 0, size);
  if (!ProviderCompletionAckV1_builder_bind(&builder, buffer, size)) return 0;
  FLOWMQ_TEST_SET_ENVELOPE(ProviderCompletionAckV1, builder, ProviderMessageKind_CompletionAck);
  if (!ProviderCompletionAckV1_disposition_set(
          &builder, ProviderCompletionAckDisposition_CompletionCommitted))
    return 0;
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionAckV1, builder, command_id, "command-1");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionAckV1, builder, dispatch_epoch, "9");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionAckV1, builder, committed_sequence, "42");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionAckV1, builder, error_code, "");
  FLOWMQ_TEST_SET_TEXT(ProviderCompletionAckV1, builder, error_message, "");
  return ProviderCompletionAckV1_view_bind(view, buffer, size) ? 1 : 0;
}

static int flowmq_test_build_event(uint8_t *buffer, size_t size, ProviderEventV1_view_t *view) {
  ProviderEventV1_builder_t builder;
  memset(buffer, 0, size);
  if (!ProviderEventV1_builder_bind(&builder, buffer, size)) return 0;
  FLOWMQ_TEST_SET_ENVELOPE(ProviderEventV1, builder, ProviderMessageKind_Event);
  FLOWMQ_TEST_SET_TEXT(ProviderEventV1, builder, event_id, "event-1");
  FLOWMQ_TEST_SET_TEXT(ProviderEventV1, builder, event_type, "play.finished");
  FLOWMQ_TEST_SET_TEXT(ProviderEventV1, builder, aggregate_id, "aggregate-1");
  FLOWMQ_TEST_SET_TEXT(ProviderEventV1, builder, sequence, "42");
  FLOWMQ_TEST_SET_TEXT(ProviderEventV1, builder, occurred_at, "2026-08-14T00:00:02Z");
  FLOWMQ_TEST_SET_TEXT(ProviderEventV1, builder, payload_json, "{}");
  return ProviderEventV1_view_bind(view, buffer, size) ? 1 : 0;
}

static int flowmq_test_build_query(uint8_t *buffer, size_t size, ProviderQueryV1_view_t *view) {
  ProviderQueryV1_builder_t builder;
  memset(buffer, 0, size);
  if (!ProviderQueryV1_builder_bind(&builder, buffer, size)) return 0;
  FLOWMQ_TEST_SET_ENVELOPE(ProviderQueryV1, builder, ProviderMessageKind_Query);
  if (!ProviderQueryV1_limit_set(&builder, 100u)) return 0;
  FLOWMQ_TEST_SET_TEXT(ProviderQueryV1, builder, query_id, "query-1");
  FLOWMQ_TEST_SET_TEXT(ProviderQueryV1, builder, query_type, "media.status");
  FLOWMQ_TEST_SET_TEXT(ProviderQueryV1, builder, expected_revision, "0");
  FLOWMQ_TEST_SET_TEXT(ProviderQueryV1, builder, cursor, "");
  FLOWMQ_TEST_SET_TEXT(ProviderQueryV1, builder, payload_json, "{}");
  return ProviderQueryV1_view_bind(view, buffer, size) ? 1 : 0;
}

static int flowmq_test_build_observation(uint8_t *buffer, size_t size, uint8_t has_more,
                                         ProviderObservationV1_view_t *view) {
  ProviderObservationV1_builder_t builder;
  memset(buffer, 0, size);
  if (!ProviderObservationV1_builder_bind(&builder, buffer, size)) return 0;
  FLOWMQ_TEST_SET_ENVELOPE(ProviderObservationV1, builder, ProviderMessageKind_Observation);
  if (!ProviderObservationV1_status_set(&builder, ProviderQueryStatus_QueryOk) ||
      !ProviderObservationV1_has_more_set(&builder, has_more))
    return 0;
  FLOWMQ_TEST_SET_TEXT(ProviderObservationV1, builder, query_id, "query-1");
  FLOWMQ_TEST_SET_TEXT(ProviderObservationV1, builder, observation_type, "media.status");
  FLOWMQ_TEST_SET_TEXT(ProviderObservationV1, builder, revision, "42");
  FLOWMQ_TEST_SET_TEXT(ProviderObservationV1, builder, cursor, "cursor-1");
  FLOWMQ_TEST_SET_TEXT(ProviderObservationV1, builder, next_cursor, "cursor-2");
  FLOWMQ_TEST_SET_TEXT(ProviderObservationV1, builder, payload_json, "{}");
  FLOWMQ_TEST_SET_TEXT(ProviderObservationV1, builder, error_code, "");
  FLOWMQ_TEST_SET_TEXT(ProviderObservationV1, builder, error_message, "");
  return ProviderObservationV1_view_bind(view, buffer, size) ? 1 : 0;
}

static int flowmq_test_build_call_offer(uint8_t *buffer, size_t size,
                                        ProviderCallOfferV1_view_t *view) {
  ProviderCallOfferV1_builder_t builder;
  memset(buffer, 0, size);
  if (!ProviderCallOfferV1_builder_bind(&builder, buffer, size)) return 0;
  FLOWMQ_TEST_SET_ENVELOPE(ProviderCallOfferV1, builder, ProviderMessageKind_CallOffer);
  FLOWMQ_TEST_SET_TEXT(ProviderCallOfferV1, builder, ingress_event_id, "ingress-1");
  FLOWMQ_TEST_SET_TEXT(ProviderCallOfferV1, builder, call_id, "call-1");
  FLOWMQ_TEST_SET_TEXT(ProviderCallOfferV1, builder, call_generation, "7");
  FLOWMQ_TEST_SET_TEXT(ProviderCallOfferV1, builder, transport, "sip");
  FLOWMQ_TEST_SET_TEXT(ProviderCallOfferV1, builder, source, "alice");
  FLOWMQ_TEST_SET_TEXT(ProviderCallOfferV1, builder, destination, "bob");
  FLOWMQ_TEST_SET_TEXT(ProviderCallOfferV1, builder, payload_json, "{}");
  return ProviderCallOfferV1_view_bind(view, buffer, size) ? 1 : 0;
}

static int flowmq_test_build_session_bound(uint8_t *buffer, size_t size, uint8_t accepted,
                                           const char *bound_session_id,
                                           ProviderSessionBoundV1_view_t *view) {
  ProviderSessionBoundV1_builder_t builder;
  memset(buffer, 0, size);
  if (!ProviderSessionBoundV1_builder_bind(&builder, buffer, size)) return 0;
  FLOWMQ_TEST_SET_ENVELOPE(ProviderSessionBoundV1, builder, ProviderMessageKind_SessionBound);
  if (!ProviderSessionBoundV1_accepted_set(&builder, accepted)) return 0;
  FLOWMQ_TEST_SET_TEXT(ProviderSessionBoundV1, builder, ingress_event_id, "ingress-1");
  FLOWMQ_TEST_SET_TEXT(ProviderSessionBoundV1, builder, call_id, "call-1");
  FLOWMQ_TEST_SET_TEXT(ProviderSessionBoundV1, builder, call_generation, "7");
  FLOWMQ_TEST_SET_TEXT(ProviderSessionBoundV1, builder, bound_session_id, bound_session_id);
  FLOWMQ_TEST_SET_TEXT(ProviderSessionBoundV1, builder, error_code, "");
  FLOWMQ_TEST_SET_TEXT(ProviderSessionBoundV1, builder, error_message, "");
  return ProviderSessionBoundV1_view_bind(view, buffer, size) ? 1 : 0;
}

spec("FlowMQ media-provider wire contract") {
  it("uses the global FMP/1 schema version") {
    check_equal(FLOWMQ_MEDIA_PROVIDER_SCHEMA_VERSION, FLOWMQ_PROTOCOL_FMP_VERSION);
  }

  it("validates a command without allocating or converting decimal fences") {
    uint8_t encoded[FLOWMQ_MEDIA_PROVIDER_TEST_BUFFER_SIZE];
    ProviderCommandV1_view_t command;
    tbe_var_data_t epoch;
    flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;
    check_true(flowmq_test_build_command(encoded, sizeof(encoded), &command));
    check_equal(flowmq_media_provider_validate_command(&command, &limits), SALTS_OK);
    check_true(ProviderCommandV1_dispatch_epoch(&command, &epoch));
    check_equal(epoch.size, strlen("18446744073709551615"));
    check_true(memcmp(epoch.data, "18446744073709551615", epoch.size) == 0);
  }

  it("uses an explicit durable receipt instead of transport admission") {
    uint8_t encoded[FLOWMQ_MEDIA_PROVIDER_TEST_BUFFER_SIZE];
    ProviderReceiptV1_view_t receipt;
    tbe_var_data_t command_id;
    flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;
    check_true(flowmq_test_build_receipt(encoded, sizeof(encoded),
                                         ProviderReceiptDisposition_DurableAccepted, &receipt));
    check_equal(flowmq_media_provider_validate_receipt(&receipt, &limits), SALTS_OK);
    check_equal(ProviderReceiptV1_disposition_get(&receipt),
                ProviderReceiptDisposition_DurableAccepted);
    check_true(ProviderReceiptV1_command_id(&receipt, &command_id));
    check_equal(command_id.size, strlen("command-1"));
    check_true(memcmp(command_id.data, "command-1", command_id.size) == 0);
  }

  it("keeps completion result event identity separate from transport identity") {
    uint8_t encoded[FLOWMQ_MEDIA_PROVIDER_TEST_BUFFER_SIZE];
    ProviderCompletionV1_view_t completion;
    flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;
    check_true(
        flowmq_test_build_completion(encoded, sizeof(encoded), "event-result-1", &completion));
    check_equal(flowmq_media_provider_validate_completion(&completion, &limits), SALTS_OK);
    check_true(flowmq_test_build_completion(encoded, sizeof(encoded), "", &completion));
    check_equal(flowmq_media_provider_validate_completion(&completion, &limits), SALTS_EPROTO);
  }

  it("keeps event acknowledgement distinct from command completion") {
    uint8_t encoded[FLOWMQ_MEDIA_PROVIDER_TEST_BUFFER_SIZE];
    ProviderEventAckV1_view_t ack;
    flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;
    check_true(flowmq_test_build_event_ack(encoded, sizeof(encoded), &ack));
    check_equal(flowmq_media_provider_validate_event_ack(&ack, &limits), SALTS_OK);
    check_equal(ProviderEventAckV1_disposition_get(&ack), ProviderEventAckDisposition_Committed);
  }

  it("acknowledges completion only after a durable Iris commit") {
    uint8_t encoded[FLOWMQ_MEDIA_PROVIDER_TEST_BUFFER_SIZE];
    ProviderCompletionAckV1_view_t ack;
    flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;
    check_true(flowmq_test_build_completion_ack(encoded, sizeof(encoded), &ack));
    check_equal(flowmq_media_provider_validate_completion_ack(&ack, &limits), SALTS_OK);
    check_equal(ProviderCompletionAckV1_disposition_get(&ack),
                ProviderCompletionAckDisposition_CompletionCommitted);
  }

  it("rejects an unknown receipt disposition") {
    uint8_t encoded[FLOWMQ_MEDIA_PROVIDER_TEST_BUFFER_SIZE];
    ProviderReceiptV1_view_t receipt;
    flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;
    check_true(flowmq_test_build_receipt(encoded, sizeof(encoded),
                                         (ProviderReceiptDisposition_t)0xffu, &receipt));
    check_equal(flowmq_media_provider_validate_receipt(&receipt, &limits), SALTS_EPROTO);
  }

  it("validates every remaining FMP wire view and strict logical booleans") {
    uint8_t encoded[FLOWMQ_MEDIA_PROVIDER_TEST_BUFFER_SIZE];
    ProviderEventV1_view_t event;
    ProviderQueryV1_view_t query;
    ProviderObservationV1_view_t observation;
    ProviderCallOfferV1_view_t call_offer;
    ProviderSessionBoundV1_view_t session_bound;
    flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;

    check_true(flowmq_test_build_event(encoded, sizeof(encoded), &event));
    check_equal(flowmq_media_provider_validate_event(&event, &limits), SALTS_OK);
    check_true(flowmq_test_build_query(encoded, sizeof(encoded), &query));
    check_equal(flowmq_media_provider_validate_query(&query, &limits), SALTS_OK);
    check_true(flowmq_test_build_observation(encoded, sizeof(encoded), 1u, &observation));
    check_equal(flowmq_media_provider_validate_observation(&observation, &limits), SALTS_OK);
    check_true(flowmq_test_build_call_offer(encoded, sizeof(encoded), &call_offer));
    check_equal(flowmq_media_provider_validate_call_offer(&call_offer, &limits), SALTS_OK);
    check_true(
        flowmq_test_build_session_bound(encoded, sizeof(encoded), 1u, "session-1", &session_bound));
    check_equal(flowmq_media_provider_validate_session_bound(&session_bound, &limits), SALTS_OK);

    check_true(flowmq_test_build_observation(encoded, sizeof(encoded), 2u, &observation));
    check_equal(flowmq_media_provider_validate_observation(&observation, &limits), SALTS_EPROTO);
    check_true(
        flowmq_test_build_session_bound(encoded, sizeof(encoded), 2u, "session-1", &session_bound));
    check_equal(flowmq_media_provider_validate_session_bound(&session_bound, &limits),
                SALTS_EPROTO);
  }

  it("rejects non-canonical and overflowing uint64 decimal strings") {
    uint64_t value = 0u;
    check_equal(flowmq_media_provider_parse_u64("0", &value), SALTS_OK);
    check_equal(value, 0u);
    check_equal(flowmq_media_provider_parse_u64("18446744073709551615", &value), SALTS_OK);
    check_equal(flowmq_media_provider_parse_u64("01", &value), SALTS_EPROTO);
    check_equal(flowmq_media_provider_parse_u64("-1", &value), SALTS_EPROTO);
    check_equal(flowmq_media_provider_parse_u64("18446744073709551616", &value), SALTS_ERANGE);
  }

  it("rejects truncated wire data") {
    uint8_t encoded[FLOWMQ_MEDIA_PROVIDER_TEST_BUFFER_SIZE];
    ProviderReceiptV1_view_t receipt;
    flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;
    check_true(flowmq_test_build_receipt(encoded, sizeof(encoded),
                                         ProviderReceiptDisposition_DurableAccepted, &receipt));
    check_true(ProviderReceiptV1_view_bind(&receipt, encoded, ProviderReceiptV1_BLOCK_LENGTH));
    check_equal(flowmq_media_provider_validate_receipt(&receipt, &limits), SALTS_EPROTO);
  }

  it("routes binary payloads by the common fixed message kind prefix") {
    uint8_t encoded[FLOWMQ_MEDIA_PROVIDER_TEST_BUFFER_SIZE];
    ProviderReceiptV1_view_t receipt;
    ProviderMessageKind_t kind = ProviderMessageKind_Command;
    check_true(flowmq_test_build_receipt(encoded, sizeof(encoded),
                                         ProviderReceiptDisposition_DurableAccepted, &receipt));
    check_equal(flowmq_media_provider_peek_kind(encoded, sizeof(encoded), &kind), SALTS_OK);
    check_equal(kind, ProviderMessageKind_Receipt);
    encoded[ProviderReceiptV1_message_kind_OFFSET] = 0xffu;
    check_equal(flowmq_media_provider_peek_kind(encoded, sizeof(encoded), &kind), SALTS_EPROTO);
    encoded[ProviderReceiptV1_message_kind_OFFSET] = (uint8_t)ProviderMessageKind_Receipt;
    tbe_wire_write_u32(encoded, FlowMqMediaProviderV1_WIRE_BIG_ENDIAN,
                       FLOWMQ_MEDIA_PROVIDER_SCHEMA_VERSION + 1u);
    check_equal(flowmq_media_provider_peek_kind(encoded, sizeof(encoded), &kind), SALTS_ENOTSUP);
  }
}

#undef FLOWMQ_TEST_SET_ENVELOPE
#undef FLOWMQ_TEST_SET_TEXT
