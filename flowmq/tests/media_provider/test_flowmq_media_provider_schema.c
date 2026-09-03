#include "flowmq_media_provider.h"
#include "flowmq_media_provider_v1.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdio.h>
#include <string.h>

static const char *provider_envelope_fields =
    "\"schema_version\":1,\"message_id\":\"msg-1\","
    "\"correlation_id\":\"corr-1\",\"causation_id\":\"cause-1\","
    "\"tenant_id\":\"tenant-1\",\"provider_id\":\"turbomedia\","
    "\"session_id\":\"session-1\",\"partition_key\":\"session-1\","
    "\"producer_id\":\"iris-a\",\"created_at\":\"2026-08-14T00:00:00Z\","
    "\"deadline_at\":\"2026-08-14T00:00:30Z\"";

spec("FlowMQ media-provider typed contract") {
  static DataBind *codec;

  before_all() {
    DataBindError error = DATA_BIND_ERROR_INIT;
    check_equal(FlowMqMediaProviderV1_codec_create(&codec, &error), DATA_BIND_OK);
    check_not_null(codec);
  }

  after_all() {
    data_bind_free(codec);
    codec = NULL;
  }

  it("uses the global FMP/1 schema version") {
    check_equal(FLOWMQ_MEDIA_PROVIDER_SCHEMA_VERSION, FLOWMQ_PROTOCOL_FMP_VERSION);
  }

  it("round-trips a command without converting decimal fences to JSON numbers") {
    DataBindError error = DATA_BIND_ERROR_INIT;
    ProviderCommandV1_t command;
    char input[2048];
    char *output = NULL;
    size_t output_size = 0u;
    DataBindStatus parse_status;
    int written = snprintf(input, sizeof(input),
                           "{%s,\"message_kind\":\"Command\",\"command_id\":\"command-1\","
                           "\"command_type\":\"media.play\",\"worker_id\":\"worker-a\","
                           "\"dispatch_epoch\":\"18446744073709551615\","
                           "\"semantic_fingerprint\":\"sha256:abc\","
                           "\"payload_json\":\"{\\\"text\\\":\\\"hello\\\"}\"}",
                           provider_envelope_fields);
    check_greater(written, 0);
    check_less(written, (int)sizeof(input));
    ProviderCommandV1_init(&command);
    parse_status = ProviderCommandV1_from_json(codec, &command, input, (size_t)written, &error);
    info("parse error code=%d path=%s message=%s", error.code, error.path, error.message);
    check_equal(parse_status, DATA_BIND_OK);
    check_equal(command.dispatch_epoch, "18446744073709551615");
    check_equal(ProviderCommandV1_to_json(codec, &command, &output, &output_size, &error),
                DATA_BIND_OK);
    check_not_null(output);
    check_contains(output, "\"dispatch_epoch\":\"18446744073709551615\"");
    check_contains(output, "\"command_id\":\"command-1\"");
    {
      flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;
      check_equal(flowmq_media_provider_validate_command(&command, &limits), TURBO_OK);
    }
    tbe_typed_serialized_free(output);
    ProviderCommandV1_clear(&command);
  }

  it("uses an explicit durable receipt instead of transport admission") {
    DataBindError error = DATA_BIND_ERROR_INIT;
    ProviderReceiptV1_t receipt;
    char input[2048];
    DataBindStatus parse_status;
    int written = snprintf(input, sizeof(input),
                           "{%s,\"message_kind\":\"Receipt\",\"command_id\":\"command-1\","
                           "\"worker_id\":\"worker-a\",\"dispatch_epoch\":\"9\","
                           "\"disposition\":\"DurableAccepted\",\"status_code\":0,"
                           "\"retry_after_ms\":0,\"error_code\":\"\",\"error_message\":\"\"}",
                           provider_envelope_fields);
    check_greater(written, 0);
    check_less(written, (int)sizeof(input));
    ProviderReceiptV1_init(&receipt);
    parse_status = ProviderReceiptV1_from_json(codec, &receipt, input, (size_t)written, &error);
    info("parse error code=%d path=%s message=%s", error.code, error.path, error.message);
    check_equal(parse_status, DATA_BIND_OK);
    check_equal(receipt.disposition, ProviderReceiptDisposition_DurableAccepted);
    check_equal(receipt.command_id, "command-1");
    ProviderReceiptV1_clear(&receipt);
  }

  it("keeps completion result event identity separate from transport identity") {
    DataBindError error = DATA_BIND_ERROR_INIT;
    ProviderCompletionV1_t completion;
    flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;
    char input[2048];
    int written = snprintf(input, sizeof(input),
                           "{%s,\"message_kind\":\"Completion\","
                           "\"terminal_status\":\"Succeeded\",\"command_id\":\"command-1\","
                           "\"worker_id\":\"worker-a\",\"dispatch_epoch\":\"9\","
                           "\"event_id\":\"event-result-1\",\"event_type\":\"play.finished\","
                           "\"completed_at\":\"2026-08-14T00:00:02Z\","
                           "\"completed_at_unix_ms\":\"1786665602000\","
                           "\"result_json\":\"{}\",\"error_code\":\"\","
                           "\"error_message\":\"\"}",
                           provider_envelope_fields);
    check_greater(written, 0);
    check_less(written, (int)sizeof(input));
    ProviderCompletionV1_init(&completion);
    check_equal(ProviderCompletionV1_from_json(codec, &completion, input, (size_t)written, &error),
                DATA_BIND_OK);
    check_equal(completion.message_id, "msg-1");
    check_equal(completion.event_id, "event-result-1");
    check_equal(flowmq_media_provider_validate_completion(&completion, &limits), TURBO_OK);
    tstr_freep(&completion.event_id);
    completion.event_id = tstr_dup("");
    check_not_null(completion.event_id);
    check_not_equal(flowmq_media_provider_validate_completion(&completion, &limits), TURBO_OK);
    ProviderCompletionV1_clear(&completion);
  }

  it("keeps event acknowledgement distinct from command completion") {
    DataBindError error = DATA_BIND_ERROR_INIT;
    ProviderEventAckV1_t ack;
    char input[2048];
    DataBindStatus parse_status;
    int written = snprintf(input, sizeof(input),
                           "{%s,\"message_kind\":\"EventAck\",\"event_id\":\"event-1\","
                           "\"disposition\":\"Committed\",\"committed_sequence\":"
                           "\"18446744073709551615\",\"error_code\":\"\","
                           "\"error_message\":\"\"}",
                           provider_envelope_fields);
    check_greater(written, 0);
    check_less(written, (int)sizeof(input));
    ProviderEventAckV1_init(&ack);
    parse_status = ProviderEventAckV1_from_json(codec, &ack, input, (size_t)written, &error);
    info("parse error code=%d path=%s message=%s", error.code, error.path, error.message);
    check_equal(parse_status, DATA_BIND_OK);
    check_equal(ack.disposition, ProviderEventAckDisposition_Committed);
    check_equal(ack.committed_sequence, "18446744073709551615");
    ProviderEventAckV1_clear(&ack);
  }

  it("acknowledges completion only after a durable Iris commit") {
    DataBindError error = DATA_BIND_ERROR_INIT;
    ProviderCompletionAckV1_t ack;
    char input[2048];
    int written = snprintf(input, sizeof(input),
                           "{%s,\"message_kind\":\"CompletionAck\","
                           "\"disposition\":\"CompletionCommitted\","
                           "\"command_id\":\"command-1\",\"dispatch_epoch\":\"9\","
                           "\"committed_sequence\":\"42\",\"error_code\":\"\","
                           "\"error_message\":\"\"}",
                           provider_envelope_fields);
    check_greater(written, 0);
    check_less(written, (int)sizeof(input));
    ProviderCompletionAckV1_init(&ack);
    check_equal(ProviderCompletionAckV1_from_json(codec, &ack, input, (size_t)written, &error),
                DATA_BIND_OK);
    {
      flowmq_media_provider_limits_t limits = FLOWMQ_MEDIA_PROVIDER_LIMITS_INIT;
      check_equal(flowmq_media_provider_validate_completion_ack(&ack, &limits), TURBO_OK);
    }
    check_equal(ack.disposition, ProviderCompletionAckDisposition_CompletionCommitted);
    ProviderCompletionAckV1_clear(&ack);
  }

  it("rejects an unknown receipt disposition") {
    DataBindError error = DATA_BIND_ERROR_INIT;
    ProviderReceiptV1_t receipt;
    char input[2048];
    int written = snprintf(input, sizeof(input),
                           "{%s,\"message_kind\":\"Receipt\",\"command_id\":\"command-1\","
                           "\"worker_id\":\"worker-a\",\"dispatch_epoch\":\"9\","
                           "\"disposition\":\"HttpAccepted\",\"status_code\":0,"
                           "\"retry_after_ms\":0,\"error_code\":\"\",\"error_message\":\"\"}",
                           provider_envelope_fields);
    check_greater(written, 0);
    check_less(written, (int)sizeof(input));
    ProviderReceiptV1_init(&receipt);
    check_not_equal(ProviderReceiptV1_from_json(codec, &receipt, input, (size_t)written, &error),
                    DATA_BIND_OK);
    ProviderReceiptV1_clear(&receipt);
  }

  it("rejects non-canonical and overflowing uint64 decimal strings") {
    uint64_t value = 0u;
    check_equal(flowmq_media_provider_parse_u64("0", &value), TURBO_OK);
    check_equal(value, 0u);
    check_equal(flowmq_media_provider_parse_u64("18446744073709551615", &value), TURBO_OK);
    check_equal(flowmq_media_provider_parse_u64("01", &value), TURBO_EPROTO);
    check_equal(flowmq_media_provider_parse_u64("-1", &value), TURBO_EPROTO);
    check_equal(flowmq_media_provider_parse_u64("18446744073709551616", &value), TURBO_ERANGE);
  }

  it("routes binary payloads by the common fixed message kind prefix") {
    DataBindError error = DATA_BIND_ERROR_INIT;
    ProviderReceiptV1_t receipt;
    ProviderMessageKind_t kind = ProviderMessageKind_Command;
    char input[2048];
    uint8_t *encoded = NULL;
    size_t encoded_size = 0u;
    int written = snprintf(input, sizeof(input),
                           "{%s,\"message_kind\":\"Receipt\",\"command_id\":\"command-1\","
                           "\"worker_id\":\"worker-a\",\"dispatch_epoch\":\"9\","
                           "\"disposition\":\"DurableAccepted\",\"status_code\":0,"
                           "\"retry_after_ms\":0,\"error_code\":\"\",\"error_message\":\"\"}",
                           provider_envelope_fields);
    check_greater(written, 0);
    check_less(written, (int)sizeof(input));
    ProviderReceiptV1_init(&receipt);
    check_equal(ProviderReceiptV1_from_json(codec, &receipt, input, (size_t)written, &error),
                DATA_BIND_OK);
    {
      DataBindStatus status = ProviderReceiptV1_to_bin(&receipt, &encoded, &encoded_size, &error);
      info("binary error code=%d path=%s message=%s", error.code, error.path, error.message);
      check_equal(status, DATA_BIND_OK);
    }
    check_equal(flowmq_media_provider_peek_kind(encoded, encoded_size, &kind), TURBO_OK);
    check_equal(kind, ProviderMessageKind_Receipt);
    encoded[4] = 0xffu;
    check_equal(flowmq_media_provider_peek_kind(encoded, encoded_size, &kind), TURBO_EPROTO);
    tbe_typed_serialized_free(encoded);
    ProviderReceiptV1_clear(&receipt);
  }
}
