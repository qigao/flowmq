#include "tinytest.h"
#include "turbo_flow_config.h"
#include "turbo_flow_fmq_broker.h"
#include "turbo_flow_fmq_pubsub.h"
#include "turbo_str_view.h"

#include <string.h>

static tstr_v fmq_pubsub_view(const char *text) { return tstr_v_from_cstr(text); }

suite("FMQ Chapter 5 pub-sub state") {
  it("keeps latest values and bridges a snapshot barrier to ordered updates") {
    turbo_flow_fmq_pubsub_config_t config = TURBO_FLOW_FMQ_PUBSUB_CONFIG_INIT;
    turbo_flow_fmq_pubsub_state_t *state;
    turbo_flow_fmq_pubsub_snapshot_cursor_t *snapshot = NULL;
    turbo_flow_fmq_pubsub_update_cursor_t *updates = NULL;
    turbo_flow_fmq_pubsub_record_t record = TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
    uint64_t sequence = 0u;
    uint64_t barrier;

    config.max_topics = 8u;
    config.max_state_bytes = 1024u;
    config.update_capacity = 8u;
    config.max_update_bytes = 1024u;
    state = turbo_flow_fmq_pubsub_state_create(&config);
    check_not_null(state);
    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("orders.a"),
                                           fmq_pubsub_view("old"), &sequence),
                 TURBO_OK);
    check_uint_eq(sequence, 1u);
    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("metrics.a"),
                                           fmq_pubsub_view("metric"), &sequence),
                 TURBO_OK);
    check_uint_eq(sequence, 2u);
    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("orders.a"),
                                           fmq_pubsub_view("current"), &sequence),
                 TURBO_OK);
    check_uint_eq(sequence, 3u);

    check_int_eq(turbo_flow_fmq_pubsub_snapshot_open(state, fmq_pubsub_view("orders."), &snapshot),
                 TURBO_OK);
    check_not_null(snapshot);
    barrier = turbo_flow_fmq_pubsub_snapshot_barrier(snapshot);
    check_uint_eq(barrier, 3u);
    check_int_eq(turbo_flow_fmq_pubsub_snapshot_next(snapshot, &record), TURBO_OK);
    check_int_eq(record.operation, TURBO_FLOW_FMQ_PUBSUB_PUT);
    check_uint_eq(record.sequence, 3u);
    check_size_eq(record.topic.len, 8u);
    check_mem_eq(record.topic.data, "orders.a", 8u);
    check_size_eq(record.payload.len, 7u);
    check_mem_eq(record.payload.data, "current", 7u);
    check_int_eq(turbo_flow_fmq_pubsub_snapshot_next(snapshot, &record), TURBO_ENOENT);

    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("orders.b"),
                                           fmq_pubsub_view("created"), &sequence),
                 TURBO_OK);
    check_uint_eq(sequence, 4u);
    check_int_eq(turbo_flow_fmq_pubsub_delete(state, fmq_pubsub_view("orders.a"), &sequence),
                 TURBO_OK);
    check_uint_eq(sequence, 5u);

    check_int_eq(
        turbo_flow_fmq_pubsub_updates_open(state, fmq_pubsub_view("orders."), barrier, &updates),
        TURBO_OK);
    check_not_null(updates);
    check_uint_eq(turbo_flow_fmq_pubsub_updates_upper_bound(updates), 5u);
    record = (turbo_flow_fmq_pubsub_record_t)TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
    check_int_eq(turbo_flow_fmq_pubsub_updates_next(updates, &record), TURBO_OK);
    check_int_eq(record.operation, TURBO_FLOW_FMQ_PUBSUB_PUT);
    check_uint_eq(record.sequence, 4u);
    check_mem_eq(record.topic.data, "orders.b", 8u);
    check_mem_eq(record.payload.data, "created", 7u);
    check_int_eq(turbo_flow_fmq_pubsub_updates_next(updates, &record), TURBO_OK);
    check_int_eq(record.operation, TURBO_FLOW_FMQ_PUBSUB_DELETE);
    check_uint_eq(record.sequence, 5u);
    check_mem_eq(record.topic.data, "orders.a", 8u);
    check_size_eq(record.payload.len, 0u);
    check_int_eq(turbo_flow_fmq_pubsub_updates_next(updates, &record), TURBO_ENOENT);

    turbo_flow_fmq_pubsub_updates_destroy(updates);
    turbo_flow_fmq_pubsub_snapshot_destroy(snapshot);
    turbo_flow_fmq_pubsub_state_destroy(state);
  }

  it("rejects an update cursor when the bounded journal has a gap") {
    turbo_flow_fmq_pubsub_config_t config = TURBO_FLOW_FMQ_PUBSUB_CONFIG_INIT;
    turbo_flow_fmq_pubsub_state_t *state;
    turbo_flow_fmq_pubsub_update_cursor_t *updates = NULL;
    turbo_flow_fmq_pubsub_record_t record = TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
    turbo_flow_fmq_pubsub_status_t status = TURBO_FLOW_FMQ_PUBSUB_STATUS_INIT;
    uint64_t sequence = 0u;

    config.max_topics = 4u;
    config.max_state_bytes = 128u;
    config.update_capacity = 2u;
    config.max_update_bytes = 128u;
    state = turbo_flow_fmq_pubsub_state_create(&config);
    check_not_null(state);
    check_int_eq(
        turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("a"), fmq_pubsub_view("1"), &sequence),
        TURBO_OK);
    check_int_eq(
        turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("b"), fmq_pubsub_view("2"), &sequence),
        TURBO_OK);
    check_int_eq(
        turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("c"), fmq_pubsub_view("3"), &sequence),
        TURBO_OK);
    check_uint_eq(sequence, 3u);
    check_int_eq(turbo_flow_fmq_pubsub_status(state, &status), TURBO_OK);
    check_size_eq(status.retained_updates, 2u);
    check_uint_eq(status.earliest_update_sequence, 2u);
    check_int_eq(turbo_flow_fmq_pubsub_updates_open(state, (tstr_v){0}, 0u, &updates),
                 TURBO_ERANGE);
    check_null(updates);
    check_int_eq(turbo_flow_fmq_pubsub_updates_open(state, (tstr_v){0}, 1u, &updates), TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_updates_next(updates, &record), TURBO_OK);
    check_uint_eq(record.sequence, 2u);
    check_int_eq(turbo_flow_fmq_pubsub_updates_next(updates, &record), TURBO_OK);
    check_uint_eq(record.sequence, 3u);
    check_int_eq(turbo_flow_fmq_pubsub_updates_next(updates, &record), TURBO_ENOENT);
    turbo_flow_fmq_pubsub_updates_destroy(updates);
    turbo_flow_fmq_pubsub_state_destroy(state);
  }

  it("preserves state and sequence when a state or journal limit rejects a put") {
    turbo_flow_fmq_pubsub_config_t config = TURBO_FLOW_FMQ_PUBSUB_CONFIG_INIT;
    turbo_flow_fmq_pubsub_state_t *state;
    turbo_flow_fmq_pubsub_status_t status = TURBO_FLOW_FMQ_PUBSUB_STATUS_INIT;
    uint64_t sequence = 0u;

    config.max_topics = 1u;
    config.max_state_bytes = 4u;
    config.update_capacity = 4u;
    config.max_update_bytes = 16u;
    state = turbo_flow_fmq_pubsub_state_create(&config);
    check_not_null(state);
    check_int_eq(
        turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("a"), fmq_pubsub_view("12"), &sequence),
        TURBO_OK);
    check_int_eq(
        turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("b"), fmq_pubsub_view("x"), &sequence),
        TURBO_ENOSPC);
    check_int_eq(
        turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("a"), fmq_pubsub_view("1234"), &sequence),
        TURBO_ENOSPC);
    check_int_eq(turbo_flow_fmq_pubsub_status(state, &status), TURBO_OK);
    check_size_eq(status.topics, 1u);
    check_size_eq(status.state_bytes, 3u);
    check_uint_eq(status.latest_sequence, 1u);
    turbo_flow_fmq_pubsub_state_destroy(state);

    config.max_topics = 2u;
    config.max_state_bytes = 64u;
    config.max_update_bytes = 2u;
    state = turbo_flow_fmq_pubsub_state_create(&config);
    check_not_null(state);
    check_int_eq(
        turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("a"), fmq_pubsub_view("12"), &sequence),
        TURBO_ENOSPC);
    status = (turbo_flow_fmq_pubsub_status_t)TURBO_FLOW_FMQ_PUBSUB_STATUS_INIT;
    check_int_eq(turbo_flow_fmq_pubsub_status(state, &status), TURBO_OK);
    check_size_eq(status.topics, 0u);
    check_uint_eq(status.latest_sequence, 0u);
    turbo_flow_fmq_pubsub_state_destroy(state);
  }

  it("rejects topics and subtree prefixes that cannot fit the FMQ v3 topic field") {
    turbo_flow_fmq_pubsub_config_t config = TURBO_FLOW_FMQ_PUBSUB_CONFIG_INIT;
    turbo_flow_fmq_pubsub_state_t *state = turbo_flow_fmq_pubsub_state_create(&config);
    turbo_flow_fmq_pubsub_snapshot_cursor_t *snapshot = NULL;
    turbo_flow_fmq_pubsub_update_cursor_t *updates = NULL;
    char oversized[TURBO_FLOW_FMQ_PUBSUB_MAX_TOPIC_SIZE + 1u];
    tstr_v topic = {oversized, sizeof(oversized)};
    uint64_t sequence = 0u;

    check_not_null(state);
    memset(oversized, 'x', sizeof(oversized));
    check_int_eq(turbo_flow_fmq_pubsub_put(state, topic, fmq_pubsub_view("value"), &sequence),
                 TURBO_ENAMETOOLONG);
    check_int_eq(turbo_flow_fmq_pubsub_delete(state, topic, &sequence), TURBO_ENAMETOOLONG);
    check_int_eq(turbo_flow_fmq_pubsub_snapshot_open(state, topic, &snapshot), TURBO_ENAMETOOLONG);
    check_null(snapshot);
    check_int_eq(turbo_flow_fmq_pubsub_updates_open(state, topic, 0u, &updates),
                 TURBO_ENAMETOOLONG);
    check_null(updates);
    turbo_flow_fmq_pubsub_state_destroy(state);
  }

  it("creates pub-sub state from YAML and rejects broker-only fields") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  state:\n"
                               "    kind: fmq_pattern\n"
                               "    config:\n"
                               "      pattern: pubsub_state\n"
                               "      max_topics: 8\n"
                               "      max_state_bytes: 1024\n"
                               "      update_capacity: 16\n"
                               "      max_update_bytes: 2048\n"
                               "adapters: {}\n";
    static const char bad_yaml[] = "version: 1\n"
                                   "channels:\n"
                                   "  state:\n"
                                   "    kind: fmq_pattern\n"
                                   "    config:\n"
                                   "      pattern: pubsub_state\n"
                                   "      scheduler: lru\n"
                                   "adapters: {}\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_fmq_pubsub_state_t *state = NULL;
    turbo_flow_fmq_broker_t *broker = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_state_create_resolved(resolved, "state", &state, &error),
                 TURBO_OK);
    check_not_null(state);
    check_int_eq(turbo_flow_fmq_broker_create_resolved(resolved, "state", &broker, &error),
                 TURBO_ENOTSUP);
    check_null(broker);
    turbo_flow_fmq_pubsub_state_destroy(state);
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    state = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(bad_yaml, sizeof(bad_yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_state_create_resolved(resolved, "state", &state, &error),
                 TURBO_EINVAL);
    check_null(state);
    check_str_contains(error.path, "channels.state.config.scheduler");
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("round trips strict TFPS recovery requests without partial output") {
    turbo_flow_fmq_pubsub_recovery_message_t message = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    uint8_t wire[128];
    uint8_t untouched[8];
    size_t wire_size = 0u;

    memset(untouched, 0xa5, sizeof(untouched));
    check_int_eq(turbo_flow_fmq_pubsub_recovery_request_encode(
                     TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES, 41u, fmq_pubsub_view("orders."), 7u,
                     19u, 5u, untouched, sizeof(untouched), &wire_size),
                 TURBO_ENOSPC);
    check_size_eq(wire_size, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_HEADER_SIZE + 7u);
    for (size_t i = 0u; i < sizeof(untouched); ++i)
      check_uint_eq(untouched[i], 0xa5u);

    check_int_eq(turbo_flow_fmq_pubsub_recovery_request_encode(
                     TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES, 41u, fmq_pubsub_view("orders."), 7u,
                     19u, 5u, wire, sizeof(wire), &wire_size),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode(wire, wire_size, &message),
                 TURBO_OK);
    check_int_eq(message.kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES);
    check_uint_eq(message.request_id, 41u);
    check_uint_eq(message.sequence, 7u);
    check_uint_eq(message.upper_bound, 19u);
    check_uint_eq(message.record_count, 5u);
    check_size_eq(message.prefix.len, 7u);
    check_mem_eq(message.prefix.data, "orders.", 7u);

    wire[0] = 'X';
    message = (turbo_flow_fmq_pubsub_recovery_message_t)TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode(wire, wire_size, &message),
                 TURBO_EPROTO);
  }

  it("turns PUT and DELETE stages into sequenced live updates from one state owner") {
    turbo_flow_fmq_pubsub_config_t config = TURBO_FLOW_FMQ_PUBSUB_CONFIG_INIT;
    turbo_flow_fmq_pubsub_state_t *state;
    turbo_flow_fmq_pubsub_recovery_message_t message = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    turbo_flow_fmq_pubsub_recovery_record_iterator_t iterator =
        TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_ITERATOR_INIT;
    turbo_flow_fmq_pubsub_record_t record = TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
    turbo_flow_fmq_pubsub_status_t status = TURBO_FLOW_FMQ_PUBSUB_STATUS_INIT;
    turbo_flow_content_descriptor_t descriptor = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
    turbo_flow_msg_t msg;

    config.max_topics = 4u;
    config.max_state_bytes = 1024u;
    config.update_capacity = 8u;
    config.max_update_bytes = 2048u;
    state = turbo_flow_fmq_pubsub_state_create(&config);
    check_not_null(state);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_new_len("created", 7u);
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_content_descriptor_init(&descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                                                    TURBO_FLOW_CONTENT_PROFILE_FMQ_DATA,
                                                    TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                                    "application/octet-stream", "orders.a"),
                 TURBO_OK);
    check_int_eq(turbo_flow_msg_copy_content_descriptor(&msg, &descriptor), TURBO_OK);

    check_int_eq(turbo_flow_fmq_pubsub_put_stage(&msg, state), TURBO_OK);
    check_uint_eq(msg.id, 1u);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode((const uint8_t *)msg.payload.data,
                                                               msg.payload.len, &message),
                 TURBO_OK);
    check_int_eq(message.kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_LIVE_UPDATE);
    check_uint_eq(message.sequence, 1u);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_iterator_init(&message, &iterator),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_next(&iterator, &record), TURBO_OK);
    check_int_eq(record.operation, TURBO_FLOW_FMQ_PUBSUB_PUT);
    check_uint_eq(record.sequence, 1u);
    check_mem_eq(record.topic.data, "orders.a", 8u);
    check_mem_eq(record.payload.data, "created", 7u);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_next(&iterator, &record), TURBO_ENOENT);

    check_int_eq(turbo_flow_fmq_pubsub_delete_stage(&msg, state), TURBO_OK);
    check_uint_eq(msg.id, 2u);
    message = (turbo_flow_fmq_pubsub_recovery_message_t)TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    iterator = (turbo_flow_fmq_pubsub_recovery_record_iterator_t)
        TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_ITERATOR_INIT;
    record = (turbo_flow_fmq_pubsub_record_t)TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode((const uint8_t *)msg.payload.data,
                                                               msg.payload.len, &message),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_iterator_init(&message, &iterator),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_next(&iterator, &record), TURBO_OK);
    check_int_eq(record.operation, TURBO_FLOW_FMQ_PUBSUB_DELETE);
    check_uint_eq(record.sequence, 2u);
    check_size_eq(record.payload.len, 0u);
    check_int_eq(turbo_flow_fmq_pubsub_status(state, &status), TURBO_OK);
    check_size_eq(status.topics, 0u);
    check_uint_eq(status.latest_sequence, 2u);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_fmq_pubsub_state_destroy(state);
  }

  it("serves snapshot barriers and fixed-upper-bound update pages with terminal errors") {
    turbo_flow_fmq_pubsub_config_t config = TURBO_FLOW_FMQ_PUBSUB_CONFIG_INIT;
    turbo_flow_fmq_pubsub_recovery_config_t recovery_config =
        TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CONFIG_INIT;
    turbo_flow_fmq_pubsub_state_t *state;
    turbo_flow_fmq_pubsub_recovery_service_t *service = NULL;
    turbo_flow_fmq_pubsub_recovery_message_t response = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    turbo_flow_fmq_pubsub_recovery_record_iterator_t iterator =
        TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_ITERATOR_INIT;
    turbo_flow_fmq_pubsub_record_t record = TURBO_FLOW_FMQ_PUBSUB_RECORD_INIT;
    turbo_flow_msg_t msg;
    uint8_t request[128];
    size_t request_size = 0u;
    uint64_t sequence = 0u;
    uint64_t barrier;
    uint64_t upper_bound;

    config.max_topics = 8u;
    config.max_state_bytes = 2048u;
    config.update_capacity = 8u;
    config.max_update_bytes = 4096u;
    recovery_config.max_reply_bytes = 4096u;
    recovery_config.max_update_records = 1u;
    state = turbo_flow_fmq_pubsub_state_create(&config);
    check_not_null(state);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_service_create(state, &recovery_config, &service),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("orders.a"), fmq_pubsub_view("a"),
                                           &sequence),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("metrics.a"),
                                           fmq_pubsub_view("m"), &sequence),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("orders.b"), fmq_pubsub_view("b"),
                                           &sequence),
                 TURBO_OK);

    turbo_flow_msg_init(&msg);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_request_encode(
                     TURBO_FLOW_FMQ_PUBSUB_RECOVERY_SNAPSHOT, 51u, fmq_pubsub_view("orders."), 0u,
                     0u, 0u, request, sizeof(request), &request_size),
                 TURBO_OK);
    msg.owned_payload = tstr_new_len(request, request_size);
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_stage(&msg, service), TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode((const uint8_t *)msg.payload.data,
                                                               msg.payload.len, &response),
                 TURBO_OK);
    check_int_eq(response.status, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK);
    check_uint_eq(response.request_id, 51u);
    check_uint_eq(response.sequence, 3u);
    check_uint_eq(response.record_count, 2u);
    barrier = response.sequence;

    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("orders.c"), fmq_pubsub_view("c"),
                                           &sequence),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_delete(state, fmq_pubsub_view("orders.a"), &sequence),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("metrics.b"),
                                           fmq_pubsub_view("n"), &sequence),
                 TURBO_OK);

    check_int_eq(turbo_flow_fmq_pubsub_recovery_request_encode(
                     TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES, 52u, fmq_pubsub_view("orders."),
                     barrier, 0u, 8u, request, sizeof(request), &request_size),
                 TURBO_OK);
    tstr_freep(&msg.owned_payload);
    msg.owned_payload = tstr_new_len(request, request_size);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_stage(&msg, service), TURBO_OK);
    response =
        (turbo_flow_fmq_pubsub_recovery_message_t)TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode((const uint8_t *)msg.payload.data,
                                                               msg.payload.len, &response),
                 TURBO_OK);
    check_false((response.flags & TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE) != 0u);
    check_uint_eq(response.sequence, 4u);
    check_uint_eq(response.upper_bound, 6u);
    upper_bound = response.upper_bound;
    iterator = (turbo_flow_fmq_pubsub_recovery_record_iterator_t)
        TURBO_FLOW_FMQ_PUBSUB_RECOVERY_RECORD_ITERATOR_INIT;
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_iterator_init(&response, &iterator),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_record_next(&iterator, &record), TURBO_OK);
    check_uint_eq(record.sequence, 4u);

    check_int_eq(turbo_flow_fmq_pubsub_recovery_request_encode(
                     TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES, 53u, fmq_pubsub_view("orders."),
                     response.sequence, upper_bound, 8u, request, sizeof(request), &request_size),
                 TURBO_OK);
    tstr_freep(&msg.owned_payload);
    msg.owned_payload = tstr_new_len(request, request_size);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_stage(&msg, service), TURBO_OK);
    response =
        (turbo_flow_fmq_pubsub_recovery_message_t)TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode((const uint8_t *)msg.payload.data,
                                                               msg.payload.len, &response),
                 TURBO_OK);
    check_true((response.flags & TURBO_FLOW_FMQ_PUBSUB_RECOVERY_FLAG_COMPLETE) != 0u);
    check_uint_eq(response.sequence, upper_bound);
    check_uint_eq(response.upper_bound, upper_bound);
    check_uint_eq(response.record_count, 1u);

    tstr_freep(&msg.owned_payload);
    msg.owned_payload = tstr_new_len("x", 1u);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_stage(&msg, service), TURBO_OK);
    response =
        (turbo_flow_fmq_pubsub_recovery_message_t)TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode((const uint8_t *)msg.payload.data,
                                                               msg.payload.len, &response),
                 TURBO_OK);
    check_int_eq(response.kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_PROTOCOL_ERROR);
    check_int_eq(response.status, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_BAD_REQUEST);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_fmq_pubsub_recovery_service_destroy(service);
    turbo_flow_fmq_pubsub_state_destroy(state);
  }

  it("returns a stale-cursor contract and keeps the recovery REP service usable") {
    turbo_flow_fmq_pubsub_config_t config = TURBO_FLOW_FMQ_PUBSUB_CONFIG_INIT;
    turbo_flow_fmq_pubsub_recovery_config_t recovery_config =
        TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CONFIG_INIT;
    turbo_flow_fmq_pubsub_state_t *state;
    turbo_flow_fmq_pubsub_recovery_service_t *service = NULL;
    turbo_flow_fmq_pubsub_recovery_message_t response = TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    turbo_flow_msg_t msg;
    uint8_t request[128];
    size_t request_size = 0u;
    uint64_t sequence = 0u;

    config.max_topics = 4u;
    config.max_state_bytes = 1024u;
    config.update_capacity = 2u;
    config.max_update_bytes = 1024u;
    recovery_config.max_reply_bytes = 1024u;
    recovery_config.max_update_records = 2u;
    state = turbo_flow_fmq_pubsub_state_create(&config);
    check_not_null(state);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_service_create(state, &recovery_config, &service),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("orders.a"), fmq_pubsub_view("a"),
                                           &sequence),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("orders.b"), fmq_pubsub_view("b"),
                                           &sequence),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_put(state, fmq_pubsub_view("orders.c"), fmq_pubsub_view("c"),
                                           &sequence),
                 TURBO_OK);

    check_int_eq(turbo_flow_fmq_pubsub_recovery_request_encode(
                     TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES, 61u, fmq_pubsub_view("orders."), 0u,
                     0u, 2u, request, sizeof(request), &request_size),
                 TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_new_len(request, request_size);
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_stage(&msg, service), TURBO_OK);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode((const uint8_t *)msg.payload.data,
                                                               msg.payload.len, &response),
                 TURBO_OK);
    check_int_eq(response.kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_UPDATES);
    check_int_eq(response.status, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_STALE_CURSOR);
    check_uint_eq(response.request_id, 61u);
    check_int_eq(response.native_status, TURBO_ERANGE);
    check_uint_eq(response.record_count, 0u);

    check_int_eq(turbo_flow_fmq_pubsub_recovery_request_encode(
                     TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES, 62u, (tstr_v){0}, 0u, 0u, 0u,
                     request, sizeof(request), &request_size),
                 TURBO_OK);
    tstr_freep(&msg.owned_payload);
    msg.owned_payload = tstr_new_len(request, request_size);
    check_not_null(msg.owned_payload);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_fmq_pubsub_recovery_stage(&msg, service), TURBO_OK);
    response =
        (turbo_flow_fmq_pubsub_recovery_message_t)TURBO_FLOW_FMQ_PUBSUB_RECOVERY_MESSAGE_INIT;
    check_int_eq(turbo_flow_fmq_pubsub_recovery_message_decode((const uint8_t *)msg.payload.data,
                                                               msg.payload.len, &response),
                 TURBO_OK);
    check_int_eq(response.kind, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_CAPABILITIES);
    check_int_eq(response.status, TURBO_FLOW_FMQ_PUBSUB_RECOVERY_STATUS_OK);
    check_uint_eq(response.request_id, 62u);
    check_uint_eq(response.sequence, 3u);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_fmq_pubsub_recovery_service_destroy(service);
    turbo_flow_fmq_pubsub_state_destroy(state);
  }
}
