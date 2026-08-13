#include "platform.h"
#include "fmq_bench_stats.h"
#include "fmt.h"
#include "tinytest.h"
#include "turbo_flow_config.h"
#include "turbo_flow_fmq_broker.h"
#include "turbo_flow_fmq_broker_protocol.h"
#include "turbo_flow_fmq_retry.h"
#include "turbo_parser.h"
#include "turbo_str.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FMQ_CREDIT_PRESSURE_INFLIGHT 4096u
#define FMQ_CREDIT_PRESSURE_PERMUTATION_STEP 4051u
#define FMQ_CREDIT_PRESSURE_WORKERS 256u
#define FMQ_CREDIT_PRESSURE_PER_WORKER (FMQ_CREDIT_PRESSURE_INFLIGHT / FMQ_CREDIT_PRESSURE_WORKERS)
#define FMQ_CREDIT_LATENCY_WARMUP 128u
#define FMQ_CREDIT_LATENCY_SAMPLES FMQ_CREDIT_PRESSURE_INFLIGHT
#define FMQ_CREDIT_LATENCY_TOTAL (FMQ_CREDIT_LATENCY_WARMUP + FMQ_CREDIT_LATENCY_SAMPLES)
#define FMQ_CREDIT_LATENCY_JOB_BYTES 64u
#define FMQ_CREDIT_LATENCY_TOTAL_BYTES (FMQ_CREDIT_LATENCY_TOTAL * FMQ_CREDIT_LATENCY_JOB_BYTES)
#define FMQ_CREDIT_LATENCY_LEASE_MS 60000u

static turbo_flow_protocol_route_t broker_route(uint64_t owner, uint64_t session,
                                                uint64_t generation) {
  turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  route.protocol = TURBO_FLOW_PROTOCOL_FMQ;
  route.owner_instance_id = owner;
  route.session_id = session;
  route.session_generation = generation;
  return route;
}

static turbo_flow_fmq_broker_logical_address_t
broker_logical_address(const char *broker_id, const char *client_id, uint64_t request_id) {
  turbo_flow_fmq_broker_logical_address_t address = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
  size_t broker_len = strlen(broker_id);
  size_t client_len = strlen(client_id);
  memcpy(address.origin_broker_id, broker_id, broker_len);
  memcpy(address.client_id, client_id, client_len);
  address.request_id = request_id;
  return address;
}


static void broker_write_u64(uint8_t out[8], uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    out[i] = (uint8_t)(value >> (56u - 8u * i));
}

static size_t broker_tfcw_append(uint8_t *body, size_t capacity, size_t offset, uint8_t field_id,
                                 const void *value, size_t value_size) {
  size_t written =
      turbo_ltv_build(field_id | TURBO_FLOW_TFCW_FIELD_CRITICAL, (const uint8_t *)value, value_size,
                      body + offset, capacity - offset);
  return written > 0u ? offset + written : 0u;
}

static size_t broker_tfcw_ready_body(uint8_t *body, size_t capacity, const char *worker_id,
                                     const char *service, uint64_t messages, uint64_t bytes) {
  uint8_t encoded_messages[8];
  uint8_t encoded_bytes[8];
  size_t offset = 0u;
  broker_write_u64(encoded_messages, messages);
  broker_write_u64(encoded_bytes, bytes);
  offset = broker_tfcw_append(body, capacity, offset, TURBO_FLOW_TFCW_FIELD_WORKER_ID, worker_id,
                              strlen(worker_id));
  offset = broker_tfcw_append(body, capacity, offset, TURBO_FLOW_TFCW_FIELD_SERVICE, service,
                              strlen(service));
  offset = broker_tfcw_append(body, capacity, offset, TURBO_FLOW_TFCW_FIELD_GRANT_MESSAGES,
                              encoded_messages, sizeof(encoded_messages));
  return broker_tfcw_append(body, capacity, offset, TURBO_FLOW_TFCW_FIELD_GRANT_BYTES,
                            encoded_bytes, sizeof(encoded_bytes));
}

static size_t broker_tfcw_job_body(uint8_t *body, size_t capacity, const char *address,
                                   const char *payload) {
  size_t offset = broker_tfcw_append(body, capacity, 0u,
                                     TURBO_FLOW_TFCW_FIELD_LOGICAL_ADDRESS, address,
                                     strlen(address));
  return broker_tfcw_append(body, capacity, offset, TURBO_FLOW_TFCW_FIELD_PAYLOAD, payload,
                            strlen(payload));
}

static size_t broker_tfcw_complete_body(uint8_t *body, size_t capacity, const char *worker_id) {
  return broker_tfcw_append(body, capacity, 0u, TURBO_FLOW_TFCW_FIELD_WORKER_ID, worker_id,
                            strlen(worker_id));
}

static int broker_tfcw_encode_message(turbo_flow_msg_t *msg, turbo_flow_tfcw_kind_t kind,
                                      uint64_t request_id, uint64_t sequence, const uint8_t *body,
                                      size_t body_size,
                                      const turbo_flow_protocol_route_t *route) {
  turbo_flow_tfcw_envelope_t envelope = TURBO_FLOW_TFCW_ENVELOPE_INIT;
  size_t encoded_size = 0u;
  int rc;
  turbo_flow_msg_init(msg);
  envelope.kind = kind;
  envelope.request_id = request_id;
  envelope.credit_sequence = sequence;
  envelope.body = body;
  envelope.body_size = body_size;
  rc = turbo_flow_tfcw_encode(&envelope, NULL, 0u, &encoded_size);
  if (rc != TURBO_ENOSPC) return rc;
  msg->owned_payload = tstr_new_len(NULL, encoded_size);
  if (!msg->owned_payload) return TURBO_ENOMEM;
  rc = turbo_flow_tfcw_encode(&envelope, (uint8_t *)msg->owned_payload, encoded_size,
                              &encoded_size);
  if (rc != TURBO_OK) {
    turbo_flow_msg_cleanup(msg);
    return rc;
  }
  msg->payload = tstr_to_v(msg->owned_payload);
  rc = turbo_flow_msg_set_protocol_route(msg, route);
  if (rc != TURBO_OK) turbo_flow_msg_cleanup(msg);
  return rc;
}

typedef struct broker_tfcw_route_probe_s {
  size_t calls;
  turbo_flow_protocol_route_t route;
} broker_tfcw_route_probe_t;

static int broker_tfcw_capture_route(turbo_flow_msg_t *msg, void *ctx) {
  broker_tfcw_route_probe_t *probe = (broker_tfcw_route_probe_t *)ctx;
  const turbo_flow_protocol_route_t *route = turbo_flow_msg_protocol_route(msg);
  if (!route) return TURBO_EINVAL;
  probe->calls += 1u;
  probe->route = *route;
  return TURBO_OK;
}

static int broker_tfcw_route_equal(const turbo_flow_protocol_route_t *left,
                                   const turbo_flow_protocol_route_t *right) {
  return left && right && left->protocol == right->protocol &&
         left->owner_instance_id == right->owner_instance_id &&
         left->session_id == right->session_id &&
         left->session_generation == right->session_generation;
}

typedef struct broker_claim_settler_probe_s {
  int ack_failures;
  int requeue_failures;
  uint64_t ack_token;
  uint64_t requeue_token;
  uint64_t drop_token;
  size_t ack_calls;
  size_t requeue_calls;
  size_t drop_calls;
} broker_claim_settler_probe_t;

static int broker_claim_settler_ack(void *ctx, uint64_t token) {
  broker_claim_settler_probe_t *probe = (broker_claim_settler_probe_t *)ctx;
  probe->ack_calls += 1u;
  probe->ack_token = token;
  if (probe->ack_failures > 0) {
    probe->ack_failures -= 1;
    return TURBO_EIO;
  }
  return TURBO_OK;
}

static int broker_claim_settler_requeue(void *ctx, uint64_t token) {
  broker_claim_settler_probe_t *probe = (broker_claim_settler_probe_t *)ctx;
  probe->requeue_calls += 1u;
  probe->requeue_token = token;
  if (probe->requeue_failures > 0) {
    probe->requeue_failures -= 1;
    return TURBO_EIO;
  }
  return TURBO_OK;
}

static int broker_claim_settler_drop(void *ctx, uint64_t token) {
  broker_claim_settler_probe_t *probe = (broker_claim_settler_probe_t *)ctx;
  probe->drop_calls += 1u;
  probe->drop_token = token;
  return TURBO_OK;
}

typedef struct broker_durable_settler_probe_s {
  uint8_t state[65536];
  size_t state_size;
  uint64_t active_token;
  int claim_active;
  int fail_after_commit;
  size_t commits;
} broker_durable_settler_probe_t;

static int broker_durable_state_load(void *ctx, const char *key, uint8_t *out, size_t capacity,
                                     size_t *out_size) {
  broker_durable_settler_probe_t *probe = (broker_durable_settler_probe_t *)ctx;
  if (!probe || !key || !key[0] || !out || !out_size) return TURBO_EINVAL;
  if (probe->state_size == 0u) return TURBO_ENOENT;
  if (probe->state_size > capacity) return TURBO_ENOSPC;
  memcpy(out, probe->state, probe->state_size);
  *out_size = probe->state_size;
  return TURBO_OK;
}

static int broker_durable_state_commit(void *ctx, uint64_t token,
                                       turbo_flow_claim_commit_action_t action, const char *key,
                                       const uint8_t *state, size_t state_size) {
  broker_durable_settler_probe_t *probe = (broker_durable_settler_probe_t *)ctx;
  if (!probe || !key || !key[0] || !state || state_size == 0u || state_size > sizeof(probe->state))
    return TURBO_EINVAL;
  if (action != TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY) {
    if (!probe->claim_active) {
      return probe->state_size == state_size && memcmp(probe->state, state, state_size) == 0
                 ? TURBO_OK
                 : TURBO_EALREADY;
    }
    if (token == 0u || token != probe->active_token) return TURBO_EBUSY;
  } else if (token != 0u) {
    return TURBO_EINVAL;
  }
  memcpy(probe->state, state, state_size);
  probe->state_size = state_size;
  probe->commits += 1u;
  if (action == TURBO_FLOW_CLAIM_COMMIT_ACK || action == TURBO_FLOW_CLAIM_COMMIT_DROP)
    probe->claim_active = 0;
  if (probe->fail_after_commit) {
    probe->fail_after_commit = 0;
    return TURBO_EIO;
  }
  return TURBO_OK;
}

spec("fmq broker") {
  it("load balances idle workers and retains the client return route") {
    turbo_flow_fmq_broker_config_t config = TURBO_FLOW_FMQ_BROKER_CONFIG_INIT;
    turbo_flow_fmq_broker_t *broker;
    turbo_flow_protocol_route_t worker_a = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t worker_b = broker_route(10u, 2u, 1u);
    turbo_flow_protocol_route_t client_a = broker_route(20u, 11u, 3u);
    turbo_flow_protocol_route_t client_b = broker_route(20u, 12u, 3u);
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_completion_result_t completion =
        TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    turbo_flow_fmq_broker_snapshot_t snapshot = TURBO_FLOW_FMQ_BROKER_SNAPSHOT_INIT;

    config.max_workers = 2u;
    config.max_inflight = 2u;
    broker = turbo_flow_fmq_broker_create(&config);
    check_not_null(broker);
    check_int_eq(turbo_flow_fmq_broker_worker_ready(broker, "worker-a", "echo", &worker_a),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_worker_ready(broker, "worker-b", "echo", &worker_b),
                 TURBO_OK);

    check_int_eq(turbo_flow_fmq_broker_dispatch(broker, "echo", 101u, &client_a, &dispatch),
                 TURBO_OK);
    check_str_eq(dispatch.worker_id, "worker-a");
    check_uint_eq(dispatch.worker_route.session_id, 1u);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_broker_dispatch(broker, "echo", 102u, &client_b, &dispatch),
                 TURBO_OK);
    check_str_eq(dispatch.worker_id, "worker-b");
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_broker_dispatch(broker, "echo", 103u, &client_a, &dispatch),
                 TURBO_ENOSPC);

    check_int_eq(turbo_flow_fmq_broker_complete(broker, "worker-b", 101u, &completion),
                 TURBO_EPROTO);
    completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_broker_complete(broker, "worker-a", 101u, &completion), TURBO_OK);
    check_uint_eq(completion.client_route.owner_instance_id, 20u);
    check_uint_eq(completion.client_route.session_id, 11u);

    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_broker_dispatch(broker, "echo", 103u, &client_a, &dispatch),
                 TURBO_OK);
    check_str_eq(dispatch.worker_id, "worker-a");
    check_int_eq(turbo_flow_fmq_broker_worker_remove(broker, "worker-a"), TURBO_EBUSY);

    completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_broker_cancel(broker, 103u, &completion), TURBO_OK);
    check_str_eq(completion.worker_id, "worker-a");
    check_int_eq(turbo_flow_fmq_broker_worker_remove(broker, "worker-a"), TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_snapshot(broker, &snapshot), TURBO_OK);
    check_size_eq(snapshot.workers, 1u);
    check_size_eq(snapshot.busy_workers, 1u);
    check_size_eq(snapshot.inflight, 1u);
    check_uint_eq(snapshot.dispatched, 3u);
    check_uint_eq(snapshot.completed, 1u);
    check_uint_eq(snapshot.canceled, 1u);

    completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_broker_cancel(broker, 102u, &completion), TURBO_OK);
    turbo_flow_fmq_broker_destroy(broker);
  }

  it("separates services and rejects invalid route ownership") {
    turbo_flow_fmq_broker_config_t config = TURBO_FLOW_FMQ_BROKER_CONFIG_INIT;
    turbo_flow_fmq_broker_t *broker;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 2u, 1u);
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;

    config.max_workers = 1u;
    config.max_inflight = 1u;
    broker = turbo_flow_fmq_broker_create(&config);
    check_not_null(broker);
    worker.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    check_int_eq(turbo_flow_fmq_broker_worker_ready(broker, "worker", "echo", &worker),
                 TURBO_EINVAL);
    worker.protocol = TURBO_FLOW_PROTOCOL_FMQ;
    check_int_eq(turbo_flow_fmq_broker_worker_ready(broker, "worker", "echo", &worker), TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_dispatch(broker, "image", 1u, &client, &dispatch),
                 TURBO_ENOTCONN);
    check_int_eq(turbo_flow_fmq_broker_dispatch(broker, "echo", 1u, &client, &dispatch), TURBO_OK);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_broker_dispatch(broker, "echo", 1u, &client, &dispatch),
                 TURBO_EALREADY);
    turbo_flow_fmq_broker_destroy(broker);
  }

  it("expires a busy at-least-once worker as an explicit requeue") {
    turbo_flow_fmq_broker_config_t config = TURBO_FLOW_FMQ_BROKER_CONFIG_INIT;
    turbo_flow_fmq_broker_t *broker;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t replacement = broker_route(10u, 2u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 11u, 3u);
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_expire_result_t expired = TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;
    turbo_flow_fmq_broker_completion_result_t completion =
        TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    turbo_flow_fmq_broker_snapshot_t snapshot = TURBO_FLOW_FMQ_BROKER_SNAPSHOT_INIT;

    config.max_workers = 1u;
    config.max_inflight = 1u;
    config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    config.worker_lease_ms = 100u;
    broker = turbo_flow_fmq_broker_create(&config);
    check_not_null(broker);
    check_int_eq(turbo_flow_fmq_broker_worker_ready(broker, "worker", "echo", &worker),
                 TURBO_ENOTSUP);
    check_int_eq(turbo_flow_fmq_broker_worker_ready_at(broker, "worker", "echo", &worker, 100u),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_dispatch_at(broker, "echo", 101u, &client, 150u, &dispatch),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_worker_heartbeat(broker, "worker", &replacement, 170u),
                 TURBO_EBUSY);
    check_int_eq(turbo_flow_fmq_broker_worker_heartbeat(broker, "worker", &worker, 180u), TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_worker_heartbeat(broker, "worker", &worker, 179u),
                 TURBO_EALREADY);
    check_int_eq(turbo_flow_fmq_broker_expire(broker, 279u, &expired), TURBO_ENOENT);
    check_int_eq(turbo_flow_fmq_broker_expire(broker, 280u, &expired), TURBO_OK);
    check_int_eq(expired.disposition, TURBO_FLOW_FMQ_BROKER_EXPIRED_REQUEUE);
    check_uint_eq(expired.request_id, 101u);
    check_str_eq(expired.worker_id, "worker");
    check_str_eq(expired.service, "echo");
    check_uint_eq(expired.client_route.session_id, 11u);
    check_int_eq(turbo_flow_fmq_broker_complete(broker, "worker", 101u, &completion), TURBO_ENOENT);
    check_int_eq(turbo_flow_fmq_broker_snapshot(broker, &snapshot), TURBO_OK);
    check_size_eq(snapshot.workers, 0u);
    check_size_eq(snapshot.inflight, 0u);
    check_uint_eq(snapshot.expired_workers, 1u);
    check_uint_eq(snapshot.expired_drops, 0u);
    check_uint_eq(snapshot.expired_requeues, 1u);
    turbo_flow_fmq_broker_destroy(broker);
  }

  it("separates owner accept ACK from worker completion ACK") {
    turbo_flow_fmq_broker_config_t config = TURBO_FLOW_FMQ_BROKER_CONFIG_INIT;
    turbo_flow_fmq_broker_t *broker;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 11u, 3u);
    turbo_flow_protocol_route_t conflicting_client = broker_route(20u, 12u, 3u);
    turbo_flow_fmq_broker_ack_result_t accept_ack = TURBO_FLOW_FMQ_BROKER_ACK_RESULT_INIT;
    turbo_flow_fmq_broker_ack_result_t completion_ack = TURBO_FLOW_FMQ_BROKER_ACK_RESULT_INIT;
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_completion_result_t completion =
        TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    turbo_flow_fmq_broker_snapshot_t snapshot = TURBO_FLOW_FMQ_BROKER_SNAPSHOT_INIT;

    config.max_workers = 1u;
    config.max_inflight = 2u;
    config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    config.worker_lease_ms = 100u;
    broker = turbo_flow_fmq_broker_create(&config);
    check_not_null(broker);
    check_int_eq(turbo_flow_fmq_broker_worker_ready_at(broker, "worker", "echo", &worker, 10u),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_fmq_broker_record_accept_commit(broker, "echo", 7u, &client, &accept_ack),
        TURBO_OK);
    check_int_eq(accept_ack.kind, TURBO_FLOW_FMQ_BROKER_ACK_ACCEPT);
    check_uint_eq(accept_ack.request_id, 7u);
    check_uint_eq(accept_ack.client_route.session_id, 11u);
    accept_ack = (turbo_flow_fmq_broker_ack_result_t)TURBO_FLOW_FMQ_BROKER_ACK_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_broker_record_accept_commit(broker, "echo", 7u, &client, &accept_ack),
        TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_record_accept_commit(broker, "echo", 7u, &conflicting_client,
                                                            &accept_ack),
                 TURBO_EPROTO);
    check_int_eq(turbo_flow_fmq_broker_snapshot(broker, &snapshot), TURBO_OK);
    check_size_eq(snapshot.accepted_requests, 1u);
    check_uint_eq(snapshot.accept_acks, 1u);
    check_uint_eq(snapshot.worker_completion_acks, 0u);

    check_int_eq(turbo_flow_fmq_broker_dispatch_accepted(broker, 7u, 20u, &dispatch), TURBO_OK);
    accept_ack = (turbo_flow_fmq_broker_ack_result_t)TURBO_FLOW_FMQ_BROKER_ACK_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_broker_record_accept_commit(broker, "echo", 7u, &client, &accept_ack),
        TURBO_OK);
    check_int_eq(accept_ack.kind, TURBO_FLOW_FMQ_BROKER_ACK_ACCEPT);
    snapshot = (turbo_flow_fmq_broker_snapshot_t)TURBO_FLOW_FMQ_BROKER_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_fmq_broker_snapshot(broker, &snapshot), TURBO_OK);
    check_size_eq(snapshot.accepted_requests, 0u);
    check_size_eq(snapshot.inflight, 1u);
    check_int_eq(turbo_flow_fmq_broker_complete(broker, "worker", 7u, &completion), TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_completion_ack(&completion, &completion_ack), TURBO_OK);
    check_int_eq(completion_ack.kind, TURBO_FLOW_FMQ_BROKER_ACK_WORKER_COMPLETION);
    check_uint_eq(completion_ack.request_id, 7u);
    check_uint_eq(completion_ack.client_route.session_id, 11u);
    snapshot = (turbo_flow_fmq_broker_snapshot_t)TURBO_FLOW_FMQ_BROKER_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_fmq_broker_snapshot(broker, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.accept_acks, 1u);
    check_uint_eq(snapshot.worker_completion_acks, 1u);
    turbo_flow_fmq_broker_destroy(broker);
  }


  it("drops at-most-once inflight work and excludes stale idle workers") {
    turbo_flow_fmq_broker_config_t config = TURBO_FLOW_FMQ_BROKER_CONFIG_INIT;
    turbo_flow_fmq_broker_t *broker;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 11u, 3u);
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_expire_result_t expired = TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;

    config.max_workers = 1u;
    config.max_inflight = 1u;
    config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE;
    config.worker_lease_ms = 100u;
    broker = turbo_flow_fmq_broker_create(&config);
    check_not_null(broker);
    check_int_eq(turbo_flow_fmq_broker_worker_ready_at(broker, "worker", "echo", &worker, 10u),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_dispatch_at(broker, "echo", 1u, &client, 10u, &dispatch),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_expire(broker, 110u, &expired), TURBO_OK);
    check_int_eq(expired.disposition, TURBO_FLOW_FMQ_BROKER_EXPIRED_DROP);
    check_uint_eq(expired.request_id, 1u);

    expired = (turbo_flow_fmq_broker_expire_result_t)TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_broker_worker_ready_at(broker, "stale", "echo", &worker, 200u),
                 TURBO_OK);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_broker_dispatch_at(broker, "echo", 2u, &client, 300u, &dispatch),
                 TURBO_ENOTCONN);
    check_int_eq(turbo_flow_fmq_broker_expire(broker, 300u, &expired), TURBO_OK);
    check_int_eq(expired.disposition, TURBO_FLOW_FMQ_BROKER_EXPIRED_IDLE);
    check_uint_eq(expired.request_id, 0u);
    turbo_flow_fmq_broker_destroy(broker);
  }

  it("creates a bounded load balancer from resolved YAML") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  requests:\n"
                               "    kind: fmq_pattern\n"
                               "    config:\n"
                               "      pattern: load_balancer\n"
                               "      scheduler: lru\n"
                               "      max_workers: 4\n"
                               "      max_inflight: 16\n"
                               "adapters: {}\n";
    static const char bad_yaml[] = "version: 1\n"
                                   "channels:\n"
                                   "  requests:\n"
                                   "    kind: fmq_pattern\n"
                                   "    config:\n"
                                   "      pattern: load_balancer\n"
                                   "      scheduler: lru\n"
                                   "      max_workers: 4\n"
                                   "      max_inflight: 16\n"
                                   "      fallback: true\n"
                                   "adapters: {}\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_fmq_broker_t *broker = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_create_resolved(resolved, "requests", &broker, &error),
                 TURBO_OK);
    check_not_null(broker);
    turbo_flow_fmq_broker_destroy(broker);
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    broker = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(bad_yaml, sizeof(bad_yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_create_resolved(resolved, "requests", &broker, &error),
                 TURBO_EINVAL);
    check_null(broker);
    check_str_contains(error.path, "channels.requests.config.fallback");
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("creates reliable request policy from YAML and rejects a zero lease") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  requests:\n"
                               "    kind: fmq_pattern\n"
                               "    config:\n"
                               "      pattern: reliable_request\n"
                               "      scheduler: lru\n"
                               "      max_workers: 4\n"
                               "      max_inflight: 16\n"
                               "      reliability: at_least_once\n"
                               "      worker_lease_ms: 3000\n"
                               "      dedup_capacity: 1\n"
                               "      dedup_ttl_ms: 10\n"
                               "      max_attempts: 2\n"
                               "adapters: {}\n";
    static const char bad_yaml[] = "version: 1\n"
                                   "channels:\n"
                                   "  requests:\n"
                                   "    kind: fmq_pattern\n"
                                   "    config:\n"
                                   "      pattern: reliable_request\n"
                                   "      scheduler: lru\n"
                                   "      max_workers: 4\n"
                                   "      max_inflight: 16\n"
                                   "      reliability: at_most_once\n"
                                   "      worker_lease_ms: 0\n"
                                   "adapters: {}\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_fmq_broker_t *broker = NULL;
    turbo_flow_fmq_retry_ledger_t *ledger = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_fmq_broker_logical_address_t logical =
        broker_logical_address("broker-a", "client-a", 1u);
    turbo_flow_fmq_broker_logical_address_t second =
        broker_logical_address("broker-a", "client-b", 2u);
    turbo_flow_fmq_retry_accept_result_t accepted = TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;
    turbo_flow_fmq_retry_record_t record = TURBO_FLOW_FMQ_RETRY_RECORD_INIT;

    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_create_resolved(resolved, "requests", &broker, &error),
                 TURBO_OK);
    check_not_null(broker);
    check_int_eq(turbo_flow_fmq_retry_ledger_create_resolved(resolved, "requests", &ledger, &error),
                 TURBO_OK);
    check_not_null(ledger);
    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &logical, 0u, &accepted), TURBO_OK);
    check_int_eq(turbo_flow_fmq_retry_ledger_begin_attempt(ledger, &logical, 1u, &record),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_retry_ledger_finish_attempt(ledger, &logical, 0, 2u, &record),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_retry_ledger_begin_attempt(ledger, &logical, 3u, &record),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_retry_ledger_finish_attempt(ledger, &logical, 0, 4u, &record),
                 TURBO_OK);
    check_int_eq(record.state, TURBO_FLOW_FMQ_RETRY_POISONED);
    accepted = (turbo_flow_fmq_retry_accept_result_t)TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &second, 5u, &accepted), TURBO_ENOSPC);
    check_int_eq(turbo_flow_fmq_retry_ledger_expire_one(ledger, 14u, &record), TURBO_OK);
    turbo_flow_fmq_retry_ledger_destroy(ledger);
    turbo_flow_fmq_broker_destroy(broker);
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    broker = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(bad_yaml, sizeof(bad_yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_create_resolved(resolved, "requests", &broker, &error),
                 TURBO_ERANGE);
    check_null(broker);
    check_str_contains(error.path, "channels.requests.config.worker_lease_ms");
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("rejects retry controls on incompatible YAML patterns") {
    static const char load_balancer_yaml[] = "version: 1\n"
                                             "channels:\n"
                                             "  requests:\n"
                                             "    kind: fmq_pattern\n"
                                             "    config:\n"
                                             "      pattern: load_balancer\n"
                                             "      scheduler: lru\n"
                                             "      max_workers: 4\n"
                                             "      max_inflight: 16\n"
                                             "      max_attempts: 2\n"
                                             "adapters: {}\n";
    static const char at_most_once_yaml[] = "version: 1\n"
                                            "channels:\n"
                                            "  requests:\n"
                                            "    kind: fmq_pattern\n"
                                            "    config:\n"
                                            "      pattern: reliable_request\n"
                                            "      scheduler: lru\n"
                                            "      max_workers: 4\n"
                                            "      max_inflight: 16\n"
                                            "      reliability: at_most_once\n"
                                            "      worker_lease_ms: 1000\n"
                                            "      max_attempts: 2\n"
                                            "adapters: {}\n";
    static const char plain_load_balancer_yaml[] = "version: 1\n"
                                                   "channels:\n"
                                                   "  requests:\n"
                                                   "    kind: fmq_pattern\n"
                                                   "    config:\n"
                                                   "      pattern: load_balancer\n"
                                                   "      scheduler: lru\n"
                                                   "      max_workers: 4\n"
                                                   "      max_inflight: 16\n"
                                                   "adapters: {}\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_fmq_broker_t *broker = NULL;
    turbo_flow_fmq_retry_ledger_t *ledger = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_int_eq(turbo_flow_config_resolve_yaml(load_balancer_yaml, sizeof(load_balancer_yaml) - 1u,
                                                &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_create_resolved(resolved, "requests", &broker, &error),
                 TURBO_EINVAL);
    check_str_contains(error.path, "channels.requests.config.max_attempts");
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(at_most_once_yaml, sizeof(at_most_once_yaml) - 1u,
                                                &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_create_resolved(resolved, "requests", &broker, &error),
                 TURBO_EINVAL);
    check_str_contains(error.path, "channels.requests.config.max_attempts");
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(plain_load_balancer_yaml,
                                                sizeof(plain_load_balancer_yaml) - 1u, &resolved,
                                                &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_retry_ledger_create_resolved(resolved, "requests", &ledger, &error),
                 TURBO_ENOTSUP);
    check_null(ledger);
    check_str_contains(error.path, "channels.requests.config.pattern");
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("round trips a versioned inter-broker logical address") {
    turbo_flow_fmq_broker_logical_address_t address = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
    turbo_flow_fmq_broker_logical_address_t decoded = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
    uint8_t encoded[TURBO_FLOW_FMQ_BROKER_ENVELOPE_MAX_SIZE];
    static const uint8_t expected_request_id[] = {1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u};
    size_t encoded_len = 0u;

    memcpy(address.origin_broker_id, "broker-east", sizeof("broker-east"));
    memcpy(address.client_id, "client-42", sizeof("client-42"));
    address.request_id = UINT64_C(0x0102030405060708);

    check_int_eq(turbo_flow_fmq_broker_logical_address_validate(&address), TURBO_OK);
    check_int_eq(turbo_flow_fmq_broker_logical_address_encode(&address, encoded, sizeof(encoded),
                                                              &encoded_len),
                 TURBO_OK);
    check_size_eq(encoded_len, 44u);
    check_mem_eq(encoded, "TFBR", 4u);
    check_uint_eq(encoded[4], 0u);
    check_uint_eq(encoded[5], TURBO_FLOW_FMQ_BROKER_PROTOCOL_VERSION);
    check_mem_eq(encoded + 16u, expected_request_id, sizeof(expected_request_id));

    check_int_eq(turbo_flow_fmq_broker_logical_address_decode(encoded, encoded_len, &decoded),
                 TURBO_OK);
    check_str_eq(decoded.origin_broker_id, "broker-east");
    check_str_eq(decoded.client_id, "client-42");
    check_hex64_eq(decoded.request_id, UINT64_C(0x0102030405060708));
  }

  it("reports logical address capacity without partially writing") {
    turbo_flow_fmq_broker_logical_address_t address = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
    uint8_t encoded[TURBO_FLOW_FMQ_BROKER_ENVELOPE_MAX_SIZE];
    uint8_t unchanged[TURBO_FLOW_FMQ_BROKER_ENVELOPE_MAX_SIZE];
    size_t required = 0u;

    memcpy(address.origin_broker_id, "broker", sizeof("broker"));
    memcpy(address.client_id, "client", sizeof("client"));
    address.request_id = 9u;
    memset(encoded, 0xa5, sizeof(encoded));
    memcpy(unchanged, encoded, sizeof(encoded));

    check_int_eq(turbo_flow_fmq_broker_logical_address_encode(
                     &address, encoded, TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE, &required),
                 TURBO_ENOSPC);
    check_size_eq(required, TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE + 12u);
    check_mem_eq(encoded, unchanged, sizeof(encoded));
  }

  it("rejects malformed logical address envelopes without changing output") {
    turbo_flow_fmq_broker_logical_address_t address = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
    turbo_flow_fmq_broker_logical_address_t decoded = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
    uint8_t encoded[TURBO_FLOW_FMQ_BROKER_ENVELOPE_MAX_SIZE];
    uint8_t malformed[TURBO_FLOW_FMQ_BROKER_ENVELOPE_MAX_SIZE + 1u];
    size_t encoded_len = 0u;

    memcpy(address.origin_broker_id, "broker", sizeof("broker"));
    memcpy(address.client_id, "client", sizeof("client"));
    address.request_id = 9u;
    check_int_eq(turbo_flow_fmq_broker_logical_address_encode(&address, encoded, sizeof(encoded),
                                                              &encoded_len),
                 TURBO_OK);
    memcpy(decoded.origin_broker_id, "unchanged", sizeof("unchanged"));

    memcpy(malformed, encoded, encoded_len);
    malformed[0] = 'X';
    check_int_eq(turbo_flow_fmq_broker_logical_address_decode(malformed, encoded_len, &decoded),
                 TURBO_EPROTO);
    check_str_eq(decoded.origin_broker_id, "unchanged");

    memcpy(malformed, encoded, encoded_len);
    malformed[5] = 2u;
    check_int_eq(turbo_flow_fmq_broker_logical_address_decode(malformed, encoded_len, &decoded),
                 TURBO_EPROTO);

    memcpy(malformed, encoded, encoded_len);
    malformed[15] = 1u;
    check_int_eq(turbo_flow_fmq_broker_logical_address_decode(malformed, encoded_len, &decoded),
                 TURBO_EPROTO);

    memcpy(malformed, encoded, encoded_len);
    memset(malformed + 16u, 0, 8u);
    check_int_eq(turbo_flow_fmq_broker_logical_address_decode(malformed, encoded_len, &decoded),
                 TURBO_EPROTO);

    memcpy(malformed, encoded, encoded_len);
    malformed[TURBO_FLOW_FMQ_BROKER_ENVELOPE_HEADER_SIZE] = 0u;
    check_int_eq(turbo_flow_fmq_broker_logical_address_decode(malformed, encoded_len, &decoded),
                 TURBO_EPROTO);

    check_int_eq(turbo_flow_fmq_broker_logical_address_decode(encoded, encoded_len - 1u, &decoded),
                 TURBO_EPROTO);
    memcpy(malformed, encoded, encoded_len);
    malformed[encoded_len] = 0u;
    check_int_eq(
        turbo_flow_fmq_broker_logical_address_decode(malformed, encoded_len + 1u, &decoded),
        TURBO_EPROTO);

    address.request_id = 0u;
    check_int_eq(turbo_flow_fmq_broker_logical_address_validate(&address), TURBO_EINVAL);
    address.request_id = 9u;
    memset(address.client_id, 'x', sizeof(address.client_id));
    check_int_eq(turbo_flow_fmq_broker_logical_address_validate(&address), TURBO_EINVAL);
  }

  it("deduplicates requests and poisons a bounded failed retry sequence") {
    turbo_flow_fmq_retry_config_t config = TURBO_FLOW_FMQ_RETRY_CONFIG_INIT;
    turbo_flow_fmq_retry_ledger_t *ledger;
    turbo_flow_fmq_broker_logical_address_t address =
        broker_logical_address("broker-a", "client-7", 42u);
    turbo_flow_fmq_retry_accept_result_t accepted = TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;
    turbo_flow_fmq_retry_record_t record = TURBO_FLOW_FMQ_RETRY_RECORD_INIT;
    turbo_flow_fmq_retry_snapshot_t snapshot = TURBO_FLOW_FMQ_RETRY_SNAPSHOT_INIT;

    config.capacity = 2u;
    config.max_attempts = 2u;
    config.terminal_ttl_ms = 100u;
    ledger = turbo_flow_fmq_retry_ledger_create(&config);
    check_not_null(ledger);

    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &address, 100u, &accepted), TURBO_OK);
    check_int_eq(accepted.disposition, TURBO_FLOW_FMQ_RETRY_ACCEPTED_NEW);
    check_int_eq(accepted.record.state, TURBO_FLOW_FMQ_RETRY_PENDING);
    accepted = (turbo_flow_fmq_retry_accept_result_t)TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &address, 101u, &accepted), TURBO_OK);
    check_int_eq(accepted.disposition, TURBO_FLOW_FMQ_RETRY_DUPLICATE_PENDING);

    check_int_eq(turbo_flow_fmq_retry_ledger_begin_attempt(ledger, &address, 110u, &record),
                 TURBO_OK);
    check_int_eq(record.state, TURBO_FLOW_FMQ_RETRY_INFLIGHT);
    check_uint_eq(record.attempts, 1u);
    accepted = (turbo_flow_fmq_retry_accept_result_t)TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &address, 111u, &accepted), TURBO_OK);
    check_int_eq(accepted.disposition, TURBO_FLOW_FMQ_RETRY_DUPLICATE_INFLIGHT);
    check_int_eq(turbo_flow_fmq_retry_ledger_finish_attempt(ledger, &address, 0, 120u, &record),
                 TURBO_OK);
    check_int_eq(record.state, TURBO_FLOW_FMQ_RETRY_PENDING);

    check_int_eq(turbo_flow_fmq_retry_ledger_begin_attempt(ledger, &address, 130u, &record),
                 TURBO_OK);
    check_uint_eq(record.attempts, 2u);
    check_int_eq(turbo_flow_fmq_retry_ledger_finish_attempt(ledger, &address, 0, 140u, &record),
                 TURBO_OK);
    check_int_eq(record.state, TURBO_FLOW_FMQ_RETRY_POISONED);
    accepted = (turbo_flow_fmq_retry_accept_result_t)TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &address, 150u, &accepted), TURBO_OK);
    check_int_eq(accepted.disposition, TURBO_FLOW_FMQ_RETRY_DUPLICATE_POISONED);

    check_int_eq(turbo_flow_fmq_retry_ledger_snapshot(ledger, &snapshot), TURBO_OK);
    check_size_eq(snapshot.records, 1u);
    check_size_eq(snapshot.poisoned, 1u);
    check_uint_eq(snapshot.duplicate_accepts, 3u);
    check_uint_eq(snapshot.retries, 1u);
    check_uint_eq(snapshot.poison_transitions, 1u);
    turbo_flow_fmq_retry_ledger_destroy(ledger);
  }

  it("expires only terminal dedup records and preserves active requests") {
    turbo_flow_fmq_retry_config_t config = TURBO_FLOW_FMQ_RETRY_CONFIG_INIT;
    turbo_flow_fmq_retry_ledger_t *ledger;
    turbo_flow_fmq_broker_logical_address_t completed =
        broker_logical_address("broker-a", "client-a", 1u);
    turbo_flow_fmq_broker_logical_address_t active =
        broker_logical_address("broker-a", "client-b", 2u);
    turbo_flow_fmq_broker_logical_address_t replacement =
        broker_logical_address("broker-a", "client-c", 3u);
    turbo_flow_fmq_retry_accept_result_t accepted = TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;
    turbo_flow_fmq_retry_record_t record = TURBO_FLOW_FMQ_RETRY_RECORD_INIT;

    config.capacity = 2u;
    config.terminal_ttl_ms = 10u;
    ledger = turbo_flow_fmq_retry_ledger_create(&config);
    check_not_null(ledger);
    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &completed, 0u, &accepted), TURBO_OK);
    check_int_eq(turbo_flow_fmq_retry_ledger_begin_attempt(ledger, &completed, 1u, &record),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_retry_ledger_finish_attempt(ledger, &completed, 1, 2u, &record),
                 TURBO_OK);
    accepted = (turbo_flow_fmq_retry_accept_result_t)TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &active, 3u, &accepted), TURBO_OK);
    accepted = (turbo_flow_fmq_retry_accept_result_t)TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &replacement, 4u, &accepted),
                 TURBO_ENOSPC);
    check_int_eq(turbo_flow_fmq_retry_ledger_expire_one(ledger, 11u, &record), TURBO_ENOENT);
    check_int_eq(turbo_flow_fmq_retry_ledger_expire_one(ledger, 12u, &record), TURBO_OK);
    check_int_eq(record.state, TURBO_FLOW_FMQ_RETRY_COMPLETED);
    check_int_eq(turbo_flow_fmq_retry_ledger_get(ledger, &active, &record), TURBO_OK);
    check_int_eq(record.state, TURBO_FLOW_FMQ_RETRY_PENDING);
    accepted = (turbo_flow_fmq_retry_accept_result_t)TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &replacement, 13u, &accepted),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_retry_ledger_begin_attempt(ledger, &active, 12u, &record),
                 TURBO_EALREADY);
    turbo_flow_fmq_retry_ledger_destroy(ledger);
  }

  it("restores durable retry records without inventing crash recovery") {
    turbo_flow_fmq_retry_config_t config = TURBO_FLOW_FMQ_RETRY_CONFIG_INIT;
    turbo_flow_fmq_retry_ledger_t *ledger;
    turbo_flow_fmq_retry_record_t pending = TURBO_FLOW_FMQ_RETRY_RECORD_INIT;
    turbo_flow_fmq_retry_record_t inflight = TURBO_FLOW_FMQ_RETRY_RECORD_INIT;
    turbo_flow_fmq_retry_record_t conflict;
    turbo_flow_fmq_retry_record_t result = TURBO_FLOW_FMQ_RETRY_RECORD_INIT;
    turbo_flow_fmq_retry_accept_result_t accepted = TURBO_FLOW_FMQ_RETRY_ACCEPT_RESULT_INIT;

    config.capacity = 3u;
    ledger = turbo_flow_fmq_retry_ledger_create(&config);
    check_not_null(ledger);
    pending.address = broker_logical_address("broker-a", "client-a", 1u);
    pending.attempts = 1u;
    pending.updated_at_ms = 50u;
    check_int_eq(turbo_flow_fmq_retry_ledger_restore(ledger, &pending), TURBO_OK);
    check_int_eq(turbo_flow_fmq_retry_ledger_restore(ledger, &pending), TURBO_OK);
    check_int_eq(turbo_flow_fmq_retry_ledger_accept(ledger, &pending.address, 50u, &accepted),
                 TURBO_OK);
    check_int_eq(accepted.disposition, TURBO_FLOW_FMQ_RETRY_DUPLICATE_PENDING);
    conflict = pending;
    conflict.updated_at_ms = 51u;
    check_int_eq(turbo_flow_fmq_retry_ledger_restore(ledger, &conflict), TURBO_EALREADY);

    inflight.address = broker_logical_address("broker-b", "client-b", 2u);
    inflight.state = TURBO_FLOW_FMQ_RETRY_INFLIGHT;
    inflight.attempts = 1u;
    inflight.updated_at_ms = 60u;
    check_int_eq(turbo_flow_fmq_retry_ledger_restore(ledger, &inflight), TURBO_OK);
    check_int_eq(
        turbo_flow_fmq_retry_ledger_finish_attempt(ledger, &inflight.address, 0, 61u, &result),
        TURBO_OK);
    check_int_eq(result.state, TURBO_FLOW_FMQ_RETRY_PENDING);
    check_uint_eq(result.attempts, 1u);
    turbo_flow_fmq_retry_ledger_destroy(ledger);
  }

  it("creates bounded volatile and storage-bound durable credit workers from strict YAML") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  bulk-workers:\n"
                               "    kind: fmq_pattern\n"
                               "    config:\n"
                               "      pattern: credit_worker\n"
                               "      scheduler: lru\n"
                               "      reliability: at_most_once\n"
                               "      max_workers: 4\n"
                               "      max_inflight: 16\n"
                               "      worker_lease_ms: 3000\n"
                               "      max_credit_messages_per_worker: 8\n"
                               "      max_credit_bytes_per_worker: 4096\n"
                               "      max_job_bytes: 1024\n"
                               "adapters: {}\n";
    static const char durable_yaml[] = "version: 1\n"
                                       "channels:\n"
                                       "  bulk-workers:\n"
                                       "    kind: fmq_pattern\n"
                                       "    config:\n"
                                       "      pattern: credit_worker\n"
                                       "      scheduler: lru\n"
                                       "      reliability: at_least_once\n"
                                       "      max_workers: 4\n"
                                       "      max_inflight: 16\n"
                                       "      worker_lease_ms: 3000\n"
                                       "      max_credit_messages_per_worker: 8\n"
                                       "      max_credit_bytes_per_worker: 4096\n"
                                       "      max_job_bytes: 1024\n"
                                       "      storage_channel: redis.claims\n"
                                       "      state_key: fmq:bulk-workers:state\n"
                                       "      max_attempts: 3\n"
                                       "      dedup_ttl_ms: 300000\n"
                                       "      shutdown_policy: preserve\n"
                                       "      shutdown_max_steps: 16\n"
                                       "adapters: {}\n";
    static const char oversized_yaml[] = "version: 1\n"
                                         "channels:\n"
                                         "  bulk-workers:\n"
                                         "    kind: fmq_pattern\n"
                                         "    config:\n"
                                         "      pattern: credit_worker\n"
                                         "      scheduler: lru\n"
                                         "      reliability: at_most_once\n"
                                         "      max_workers: 4\n"
                                         "      max_inflight: 16\n"
                                         "      worker_lease_ms: 3000\n"
                                         "      max_credit_messages_per_worker: 8\n"
                                         "      max_credit_bytes_per_worker: 4096\n"
                                         "      max_job_bytes: 4097\n"
                                         "adapters: {}\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_fmq_credit_worker_t *owner = NULL;
    turbo_flow_fmq_credit_durable_t *durable = NULL;
    turbo_flow_fmq_broker_t *broker = NULL;
    broker_durable_settler_probe_t probe;
    turbo_flow_claim_settler_t settler = TURBO_FLOW_CLAIM_SETTLER_INIT;
    turbo_flow_fmq_credit_durable_binding_t binding = TURBO_FLOW_FMQ_CREDIT_DURABLE_BINDING_INIT;
    turbo_flow_fmq_credit_durable_snapshot_t durable_snapshot =
        TURBO_FLOW_FMQ_CREDIT_DURABLE_SNAPSHOT_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    memset(&probe, 0, sizeof(probe));
    settler.ctx = &probe;
    settler.max_state_size = sizeof(probe.state);
    settler.load_state = broker_durable_state_load;
    settler.commit_state = broker_durable_state_commit;
    binding.storage_channel = "redis.claims";
    binding.settler = &settler;

    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_fmq_credit_worker_create_resolved(resolved, "bulk-workers", &owner, &error),
        TURBO_OK);
    check_not_null(owner);
    check_int_eq(turbo_flow_fmq_broker_create_resolved(resolved, "bulk-workers", &broker, &error),
                 TURBO_ENOTSUP);
    check_null(broker);
    turbo_flow_fmq_credit_worker_destroy(owner);
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(
        turbo_flow_config_resolve_yaml(durable_yaml, sizeof(durable_yaml) - 1u, &resolved, &error),
        TURBO_OK);
    check_int_eq(
        turbo_flow_fmq_credit_worker_create_resolved(resolved, "bulk-workers", &owner, &error),
        TURBO_ENOTSUP);
    check_str_contains(error.path, "channels.bulk-workers.config.reliability");
    binding.storage_channel = "redis.wrong";
    check_int_eq(turbo_flow_fmq_credit_durable_create_resolved(resolved, "bulk-workers", &binding,
                                                               &owner, &durable, &error),
                 TURBO_EINVAL);
    check_null(owner);
    check_null(durable);
    check_str_contains(error.path, "channels.bulk-workers.config.storage_channel");
    binding.storage_channel = "redis.claims";
    check_int_eq(turbo_flow_fmq_credit_durable_create_resolved(resolved, "bulk-workers", &binding,
                                                               &owner, &durable, &error),
                 TURBO_OK);
    check_not_null(owner);
    check_not_null(durable);
    check_int_eq(turbo_flow_fmq_credit_durable_shutdown(durable, 0u, &durable_snapshot), TURBO_OK);
    check_true(durable_snapshot.quiesced);
    turbo_flow_fmq_credit_durable_destroy(durable);
    turbo_flow_fmq_credit_worker_destroy(owner);
    owner = NULL;
    durable = NULL;
    turbo_flow_resolved_config_destroy(resolved);

    resolved = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_int_eq(turbo_flow_config_resolve_yaml(oversized_yaml, sizeof(oversized_yaml) - 1u,
                                                &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_fmq_credit_worker_create_resolved(resolved, "bulk-workers", &owner, &error),
        TURBO_ERANGE);
    check_str_contains(error.path, "channels.bulk-workers.config.max_job_bytes");
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("encodes and validates the canonical TFCW application envelope") {
    uint8_t body[128];
    uint8_t encoded[256];
    uint8_t unchanged[256];
    uint8_t malformed[256];
    size_t body_size = broker_tfcw_ready_body(body, sizeof(body), "worker-a", "jobs", 2u, 4096u);
    size_t encoded_size = 0u;
    turbo_flow_tfcw_envelope_t envelope = TURBO_FLOW_TFCW_ENVELOPE_INIT;
    turbo_flow_tfcw_envelope_t decoded = TURBO_FLOW_TFCW_ENVELOPE_INIT;
    turbo_flow_tfcw_fields_t fields = TURBO_FLOW_TFCW_FIELDS_INIT;

    check_true(body_size > 0u);
    envelope.kind = TURBO_FLOW_TFCW_READY;
    envelope.credit_sequence = 1u;
    envelope.sender_timestamp_ms = UINT64_C(0x0102030405060708);
    envelope.body = body;
    envelope.body_size = body_size;
    memset(encoded, 0xa5, sizeof(encoded));
    memcpy(unchanged, encoded, sizeof(encoded));
    check_int_eq(turbo_flow_tfcw_encode(&envelope, encoded, 39u, &encoded_size), TURBO_ENOSPC);
    check_size_eq(encoded_size, TURBO_FLOW_TFCW_HEADER_SIZE + body_size);
    check_mem_eq(encoded, unchanged, sizeof(encoded));

    check_int_eq(turbo_flow_tfcw_encode(&envelope, encoded, sizeof(encoded), &encoded_size),
                 TURBO_OK);
    check_mem_eq(encoded, "TFCW", 4u);
    check_uint_eq(encoded[4], TURBO_FLOW_TFCW_PROTOCOL_MAJOR);
    check_uint_eq(encoded[5], TURBO_FLOW_TFCW_PROTOCOL_MINOR);
    check_uint_eq(encoded[7], TURBO_FLOW_TFCW_READY);
    check_uint_eq(encoded[31], 1u);
    check_int_eq(turbo_flow_tfcw_decode(encoded, encoded_size, &decoded), TURBO_OK);
    check_int_eq(decoded.kind, TURBO_FLOW_TFCW_READY);
    check_uint_eq(decoded.credit_sequence, 1u);
    check_hex64_eq(decoded.sender_timestamp_ms, UINT64_C(0x0102030405060708));
    check_size_eq(decoded.body_size, body_size);
    check_mem_eq(decoded.body, body, body_size);
    check_int_eq(turbo_flow_tfcw_fields_decode(&decoded, &fields), TURBO_OK);
    check_mem_eq(fields.worker_id.data, "worker-a", 8u);
    check_mem_eq(fields.service.data, "jobs", 4u);
    check_uint_eq(fields.grant_messages, 2u);
    check_uint_eq(fields.grant_bytes, 4096u);

    memcpy(malformed, encoded, encoded_size);
    malformed[10] = 1u;
    decoded.kind = TURBO_FLOW_TFCW_FAIL;
    check_int_eq(turbo_flow_tfcw_decode(malformed, encoded_size, &decoded), TURBO_EPROTO);
    check_int_eq(decoded.kind, TURBO_FLOW_TFCW_FAIL);
    memcpy(malformed, encoded, encoded_size);
    malformed[TURBO_FLOW_TFCW_HEADER_SIZE] =
        TURBO_FLOW_TFCW_FIELD_SERVICE | TURBO_FLOW_TFCW_FIELD_CRITICAL;
    check_int_eq(turbo_flow_tfcw_decode(malformed, encoded_size, &decoded), TURBO_EPROTO);
    memcpy(malformed, encoded, encoded_size);
    malformed[5] = 1u;
    check_int_eq(turbo_flow_tfcw_decode(malformed, encoded_size, &decoded), TURBO_ENOTSUP);
    check_int_eq(turbo_flow_tfcw_decode(encoded, encoded_size - 1u, &decoded), TURBO_EPROTO);
  }

  it("dispatches multiple jobs with independent message and byte credit") {
    turbo_flow_fmq_credit_worker_config_t config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_worker_t *owner;
    turbo_flow_protocol_route_t route_a = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t route_b = broker_route(10u, 2u, 1u);
    turbo_flow_protocol_route_t next_route_a = broker_route(10u, 1u, 2u);
    turbo_flow_protocol_route_t client = broker_route(20u, 1u, 1u);
    turbo_flow_fmq_credit_grant_t grant_a = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_fmq_credit_grant_t grant_b = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_completion_result_t completion =
        TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    turbo_flow_fmq_credit_worker_snapshot_t snapshot = TURBO_FLOW_FMQ_CREDIT_WORKER_SNAPSHOT_INIT;

    config.max_workers = 2u;
    config.max_inflight = 4u;
    config.worker_lease_ms = 100u;
    config.max_credit_messages_per_worker = 2u;
    config.max_credit_bytes_per_worker = 100u;
    config.max_job_bytes = 60u;
    config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    owner = turbo_flow_fmq_credit_worker_create(&config);
    check_not_null(owner);
    grant_a.worker_id = "worker-a";
    grant_a.service = "jobs";
    grant_a.worker_route = route_a;
    grant_a.sequence = 1u;
    grant_a.grant_messages = 2u;
    grant_a.grant_bytes = 100u;
    grant_a.now_ms = 10u;
    grant_b.worker_id = "worker-b";
    grant_b.service = "jobs";
    grant_b.worker_route = route_b;
    grant_b.sequence = 1u;
    grant_b.grant_messages = 1u;
    grant_b.grant_bytes = 50u;
    grant_b.now_ms = 11u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant_a), TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant_b), TURBO_OK);

    check_int_eq(
        turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", 1u, 40u, &client, 12u, &dispatch),
        TURBO_OK);
    check_str_eq(dispatch.worker_id, "worker-a");
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", 2u, 40u, &client, 12u, &dispatch),
        TURBO_OK);
    check_str_eq(dispatch.worker_id, "worker-b");
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", 3u, 60u, &client, 12u, &dispatch),
        TURBO_OK);
    check_str_eq(dispatch.worker_id, "worker-a");
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", 4u, 1u, &client, 12u, &dispatch),
        TURBO_FLOW_FMQ_EAGAIN);

    check_int_eq(
        turbo_flow_fmq_credit_worker_complete(owner, "worker-a", &next_route_a, 1u, &completion),
        TURBO_EPROTO);
    check_int_eq(
        turbo_flow_fmq_credit_worker_complete(owner, "worker-a", &route_a, 1u, &completion),
        TURBO_OK);
    grant_a.worker_route = next_route_a;
    grant_a.sequence = 1u;
    grant_a.grant_messages = 1u;
    grant_a.grant_bytes = 20u;
    grant_a.now_ms = 20u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant_a), TURBO_EBUSY);
    completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_credit_worker_complete(owner, "worker-a", &route_a, 3u, &completion),
        TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant_a), TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant_a), TURBO_OK);
    grant_a.grant_bytes = 21u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant_a), TURBO_EPROTO);
    grant_a.worker_route = next_route_a;
    grant_a.sequence = 3u;
    grant_a.grant_bytes = 1u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant_a), TURBO_EPROTO);

    check_int_eq(turbo_flow_fmq_credit_worker_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.workers, 2u);
    check_size_eq(snapshot.inflight, 1u);
    check_size_eq(snapshot.available_messages, 1u);
    check_size_eq(snapshot.available_bytes, 30u);
    check_uint_eq(snapshot.grants, 3u);
    check_uint_eq(snapshot.dispatched, 3u);
    check_uint_eq(snapshot.completed, 2u);
    completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_worker_cancel(owner, 2u, &completion), TURBO_OK);
    turbo_flow_fmq_credit_worker_destroy(owner);
  }

  it("keeps 4096 credit correlations consistent through permuted completion") {
    turbo_flow_fmq_credit_worker_config_t config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_worker_t *owner;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 1u, 1u);
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_completion_result_t completion =
        TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    turbo_flow_fmq_credit_worker_snapshot_t snapshot = TURBO_FLOW_FMQ_CREDIT_WORKER_SNAPSHOT_INIT;

    config.max_workers = 1u;
    config.max_inflight = FMQ_CREDIT_PRESSURE_INFLIGHT;
    config.worker_lease_ms = 1000u;
    config.max_credit_messages_per_worker = FMQ_CREDIT_PRESSURE_INFLIGHT;
    config.max_credit_bytes_per_worker = FMQ_CREDIT_PRESSURE_INFLIGHT;
    config.max_job_bytes = 1u;
    config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE;
    owner = turbo_flow_fmq_credit_worker_create(&config);
    check_not_null(owner);

    grant.worker_id = "worker";
    grant.service = "jobs";
    grant.worker_route = worker;
    grant.sequence = 1u;
    grant.grant_messages = FMQ_CREDIT_PRESSURE_INFLIGHT;
    grant.grant_bytes = FMQ_CREDIT_PRESSURE_INFLIGHT;
    grant.now_ms = 1u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant), TURBO_OK);

    for (uint64_t request_id = 1u; request_id <= FMQ_CREDIT_PRESSURE_INFLIGHT; ++request_id) {
      dispatch =
          (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
      check_int_eq(turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", request_id, 1u, &client, 1u,
                                                         &dispatch),
                   TURBO_OK);
      check_uint_eq(dispatch.request_id, request_id);
    }
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_worker_dispatch(
                     owner, "jobs", FMQ_CREDIT_PRESSURE_INFLIGHT + 1u, 1u, &client, 1u, &dispatch),
                 TURBO_ENOSPC);

    completion =
        (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_worker_complete(owner, "worker", &worker, 1u, &completion),
                 TURBO_OK);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_worker_dispatch(
                     owner, "jobs", FMQ_CREDIT_PRESSURE_INFLIGHT + 1u, 1u, &client, 1u, &dispatch),
                 TURBO_FLOW_FMQ_EAGAIN);

    grant.sequence = 2u;
    grant.grant_messages = 1u;
    grant.grant_bytes = 1u;
    grant.now_ms = 2u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant), TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_worker_dispatch(
                     owner, "jobs", FMQ_CREDIT_PRESSURE_INFLIGHT + 1u, 1u, &client, 2u, &dispatch),
                 TURBO_OK);

    for (uint64_t i = 0u; i < FMQ_CREDIT_PRESSURE_INFLIGHT; ++i) {
      uint64_t request_id =
          (i * FMQ_CREDIT_PRESSURE_PERMUTATION_STEP) % FMQ_CREDIT_PRESSURE_INFLIGHT + 2u;
      completion =
          (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
      check_int_eq(
          turbo_flow_fmq_credit_worker_complete(owner, "worker", &worker, request_id, &completion),
          TURBO_OK);
      check_uint_eq(completion.request_id, request_id);
    }

    check_int_eq(turbo_flow_fmq_credit_worker_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.inflight, 0u);
    check_size_eq(snapshot.available_messages, 0u);
    check_size_eq(snapshot.available_bytes, 0u);
    check_uint_eq(snapshot.grants, 2u);
    check_uint_eq(snapshot.dispatched, FMQ_CREDIT_PRESSURE_INFLIGHT + 1u);
    check_uint_eq(snapshot.completed, FMQ_CREDIT_PRESSURE_INFLIGHT + 1u);
    turbo_flow_fmq_credit_worker_destroy(owner);
  }

  bench("reports 4096 in-flight credit owner latency percentiles") {
    turbo_flow_fmq_credit_worker_config_t config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_worker_t *owner = NULL;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 1u, 1u);
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_completion_result_t completion =
        TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
    turbo_flow_fmq_credit_worker_snapshot_t snapshot = TURBO_FLOW_FMQ_CREDIT_WORKER_SNAPSHOT_INIT;
    uint64_t *dispatch_latencies = NULL;
    uint64_t *complete_latencies = NULL;
    uint64_t total_started_ns;
    uint64_t total_elapsed_ns;
    size_t dispatched = 0u;
    size_t completed = 0u;
    int status = TURBO_OK;
    double throughput;

    dispatch_latencies =
        (uint64_t *)calloc(FMQ_CREDIT_LATENCY_SAMPLES, sizeof(*dispatch_latencies));
    complete_latencies =
        (uint64_t *)calloc(FMQ_CREDIT_LATENCY_SAMPLES, sizeof(*complete_latencies));
    check_not_null(dispatch_latencies);
    check_not_null(complete_latencies);
    if (!dispatch_latencies || !complete_latencies) goto cleanup;

    config.max_workers = 1u;
    config.max_inflight = FMQ_CREDIT_LATENCY_SAMPLES;
    config.worker_lease_ms = FMQ_CREDIT_LATENCY_LEASE_MS;
    config.max_credit_messages_per_worker = FMQ_CREDIT_LATENCY_TOTAL;
    config.max_credit_bytes_per_worker = FMQ_CREDIT_LATENCY_TOTAL_BYTES;
    config.max_job_bytes = FMQ_CREDIT_LATENCY_JOB_BYTES;
    config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_MOST_ONCE;
    owner = turbo_flow_fmq_credit_worker_create(&config);
    check_not_null(owner);
    if (!owner) goto cleanup;

    grant.worker_id = "worker";
    grant.service = "jobs";
    grant.worker_route = worker;
    grant.sequence = 1u;
    grant.grant_messages = FMQ_CREDIT_LATENCY_TOTAL;
    grant.grant_bytes = FMQ_CREDIT_LATENCY_TOTAL_BYTES;
    grant.now_ms = 1u;
    status = turbo_flow_fmq_credit_worker_grant(owner, &grant);
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;

    for (uint64_t request_id = 1u; request_id <= FMQ_CREDIT_LATENCY_WARMUP; ++request_id) {
      dispatch =
          (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
      status = turbo_flow_fmq_credit_worker_dispatch(
          owner, "jobs", request_id, FMQ_CREDIT_LATENCY_JOB_BYTES, &client, 1u, &dispatch);
      if (status != TURBO_OK) break;
      completion =
          (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
      status =
          turbo_flow_fmq_credit_worker_complete(owner, "worker", &worker, request_id, &completion);
      if (status != TURBO_OK) break;
    }
    check_int_eq(status, TURBO_OK);
    if (status != TURBO_OK) goto cleanup;

    total_started_ns = turbo_hrtime();
    for (size_t i = 0u; i < FMQ_CREDIT_LATENCY_SAMPLES; ++i) {
      uint64_t request_id = FMQ_CREDIT_LATENCY_WARMUP + (uint64_t)i + 1u;
      uint64_t started_ns = turbo_hrtime();
      dispatch =
          (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
      status = turbo_flow_fmq_credit_worker_dispatch(
          owner, "jobs", request_id, FMQ_CREDIT_LATENCY_JOB_BYTES, &client, 1u, &dispatch);
      dispatch_latencies[i] = turbo_hrtime() - started_ns;
      if (status != TURBO_OK) break;
      dispatched += 1u;
    }
    check_int_eq(status, TURBO_OK);
    check_size_eq(dispatched, FMQ_CREDIT_LATENCY_SAMPLES);
    if (status != TURBO_OK || dispatched != FMQ_CREDIT_LATENCY_SAMPLES) goto cleanup;

    for (size_t i = 0u; i < dispatched; ++i) {
      uint64_t request_id = FMQ_CREDIT_LATENCY_WARMUP + (uint64_t)i + 1u;
      uint64_t started_ns = turbo_hrtime();
      completion =
          (turbo_flow_fmq_broker_completion_result_t)TURBO_FLOW_FMQ_BROKER_COMPLETION_RESULT_INIT;
      status =
          turbo_flow_fmq_credit_worker_complete(owner, "worker", &worker, request_id, &completion);
      complete_latencies[i] = turbo_hrtime() - started_ns;
      if (status != TURBO_OK) break;
      completed += 1u;
    }
    total_elapsed_ns = turbo_hrtime() - total_started_ns;
    check_int_eq(status, TURBO_OK);
    check_size_eq(completed, FMQ_CREDIT_LATENCY_SAMPLES);
    if (status != TURBO_OK || completed != FMQ_CREDIT_LATENCY_SAMPLES) goto cleanup;

    check_int_eq(turbo_flow_fmq_credit_worker_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.inflight, 0u);
    check_size_eq(snapshot.available_messages, 0u);
    check_size_eq(snapshot.available_bytes, 0u);
    check_uint_eq(snapshot.dispatched, FMQ_CREDIT_LATENCY_TOTAL);
    check_uint_eq(snapshot.completed, FMQ_CREDIT_LATENCY_TOTAL);

    /* Reporting sorts two bounded 4096-element arrays: O(n log n) time and O(n) samples. */
    qsort(dispatch_latencies, dispatched, sizeof(*dispatch_latencies), fmq_bench_u64_compare);
    qsort(complete_latencies, completed, sizeof(*complete_latencies), fmq_bench_u64_compare);
    throughput =
        total_elapsed_ns > 0u ? ((double)completed * 1000000000.0) / (double)total_elapsed_ns : 0.0;
    printf("FMQ_BENCH_RESULT component=credit_owner mode=peak-inflight"
           " payload_bytes=%u warmup=%u samples=%u throughput_roundtrip_s=%.2f"
           " dispatch_p50_ns=%" PRIu64 " dispatch_p95_ns=%" PRIu64 " dispatch_p99_ns=%" PRIu64
           " complete_p50_ns=%" PRIu64 " complete_p95_ns=%" PRIu64 " complete_p99_ns=%" PRIu64 "\n",
           FMQ_CREDIT_LATENCY_JOB_BYTES, FMQ_CREDIT_LATENCY_WARMUP, FMQ_CREDIT_LATENCY_SAMPLES,
           throughput, fmq_bench_percentile(dispatch_latencies, dispatched, 50u),
           fmq_bench_percentile(dispatch_latencies, dispatched, 95u),
           fmq_bench_percentile(dispatch_latencies, dispatched, 99u),
           fmq_bench_percentile(complete_latencies, completed, 50u),
           fmq_bench_percentile(complete_latencies, completed, 95u),
           fmq_bench_percentile(complete_latencies, completed, 99u));

  cleanup:
    turbo_flow_fmq_credit_worker_destroy(owner);
    free(complete_latencies);
    free(dispatch_latencies);
  }

  it("expires 4096 slow-worker correlations across every worker generation") {
    turbo_flow_fmq_credit_worker_config_t config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_worker_t *owner;
    turbo_flow_protocol_route_t client = broker_route(20u, 1u, 1u);
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_expire_result_t expired = TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;
    turbo_flow_fmq_credit_worker_snapshot_t snapshot = TURBO_FLOW_FMQ_CREDIT_WORKER_SNAPSHOT_INIT;

    config.max_workers = FMQ_CREDIT_PRESSURE_WORKERS;
    config.max_inflight = FMQ_CREDIT_PRESSURE_INFLIGHT;
    config.worker_lease_ms = 10u;
    config.max_credit_messages_per_worker = FMQ_CREDIT_PRESSURE_PER_WORKER;
    config.max_credit_bytes_per_worker = FMQ_CREDIT_PRESSURE_PER_WORKER;
    config.max_job_bytes = 1u;
    config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    owner = turbo_flow_fmq_credit_worker_create(&config);
    check_not_null(owner);

    for (size_t worker_index = 0u; worker_index < FMQ_CREDIT_PRESSURE_WORKERS; ++worker_index) {
      char worker_id[32];
      fmt(worker_id, sizeof(worker_id), "worker-{}", worker_index);
      grant = (turbo_flow_fmq_credit_grant_t)TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
      grant.worker_id = worker_id;
      grant.service = "jobs";
      grant.worker_route = broker_route(10u, worker_index + 1u, 1u);
      grant.sequence = 1u;
      grant.grant_messages = FMQ_CREDIT_PRESSURE_PER_WORKER;
      grant.grant_bytes = FMQ_CREDIT_PRESSURE_PER_WORKER;
      grant.now_ms = 1u;
      check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant), TURBO_OK);
    }

    for (uint64_t request_id = 1u; request_id <= FMQ_CREDIT_PRESSURE_INFLIGHT; ++request_id) {
      dispatch =
          (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
      check_int_eq(turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", request_id, 1u, &client, 1u,
                                                         &dispatch),
                   TURBO_OK);
    }

    for (size_t i = 0u; i < FMQ_CREDIT_PRESSURE_INFLIGHT; ++i) {
      expired = (turbo_flow_fmq_broker_expire_result_t)TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;
      check_int_eq(turbo_flow_fmq_credit_worker_expire(owner, 11u, &expired), TURBO_OK);
      check_int_eq(expired.disposition, TURBO_FLOW_FMQ_BROKER_EXPIRED_REQUEUE);
      check_true(expired.request_id > 0u && expired.request_id <= FMQ_CREDIT_PRESSURE_INFLIGHT);
    }
    expired = (turbo_flow_fmq_broker_expire_result_t)TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_worker_expire(owner, 11u, &expired), TURBO_ENOENT);

    check_int_eq(turbo_flow_fmq_credit_worker_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.workers, 0u);
    check_size_eq(snapshot.inflight, 0u);
    check_uint_eq(snapshot.grants, FMQ_CREDIT_PRESSURE_WORKERS);
    check_uint_eq(snapshot.dispatched, FMQ_CREDIT_PRESSURE_INFLIGHT);
    check_uint_eq(snapshot.expired_workers, FMQ_CREDIT_PRESSURE_WORKERS);
    check_uint_eq(snapshot.expired_requests, FMQ_CREDIT_PRESSURE_INFLIGHT);
    turbo_flow_fmq_credit_worker_destroy(owner);
  }

  it("expires every correlation before removing an at-least-once credit worker") {
    turbo_flow_fmq_credit_worker_config_t config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_worker_t *owner;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 1u, 1u);
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_broker_expire_result_t expired = TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;
    turbo_flow_fmq_credit_worker_snapshot_t snapshot = TURBO_FLOW_FMQ_CREDIT_WORKER_SNAPSHOT_INIT;

    config.max_workers = 1u;
    config.max_inflight = 3u;
    config.worker_lease_ms = 10u;
    config.max_credit_messages_per_worker = 3u;
    config.max_credit_bytes_per_worker = 300u;
    config.max_job_bytes = 100u;
    config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    owner = turbo_flow_fmq_credit_worker_create(&config);
    check_not_null(owner);
    grant.worker_id = "worker";
    grant.service = "jobs";
    grant.worker_route = worker;
    grant.sequence = 1u;
    grant.grant_messages = 3u;
    grant.grant_bytes = 300u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(owner, &grant), TURBO_OK);
    check_int_eq(
        turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", 1u, 100u, &client, 1u, &dispatch),
        TURBO_OK);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(
        turbo_flow_fmq_credit_worker_dispatch(owner, "jobs", 2u, 100u, &client, 1u, &dispatch),
        TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_worker_expire(owner, 9u, &expired), TURBO_ENOENT);
    check_int_eq(turbo_flow_fmq_credit_worker_expire(owner, 10u, &expired), TURBO_OK);
    check_int_eq(expired.disposition, TURBO_FLOW_FMQ_BROKER_EXPIRED_REQUEUE);
    check_uint_eq(expired.request_id, 1u);
    expired = (turbo_flow_fmq_broker_expire_result_t)TURBO_FLOW_FMQ_BROKER_EXPIRE_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_worker_expire(owner, 10u, &expired), TURBO_OK);
    check_int_eq(expired.disposition, TURBO_FLOW_FMQ_BROKER_EXPIRED_REQUEUE);
    check_uint_eq(expired.request_id, 2u);
    check_int_eq(turbo_flow_fmq_credit_worker_expire(owner, 10u, &expired), TURBO_ENOENT);
    check_int_eq(turbo_flow_fmq_credit_worker_snapshot(owner, &snapshot), TURBO_OK);
    check_size_eq(snapshot.workers, 0u);
    check_size_eq(snapshot.inflight, 0u);
    check_uint_eq(snapshot.expired_workers, 1u);
    check_uint_eq(snapshot.expired_requests, 2u);
    turbo_flow_fmq_credit_worker_destroy(owner);
  }


  it("retains a failed storage ACK until an explicit settlement retry") {
    turbo_flow_fmq_credit_worker_config_t credit_config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_settlement_config_t settlement_config =
        TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_CONFIG_INIT;
    broker_claim_settler_probe_t probe;
    turbo_flow_claim_settler_t settler = TURBO_FLOW_CLAIM_SETTLER_INIT;
    turbo_flow_fmq_credit_worker_t *credit;
    turbo_flow_fmq_credit_settlement_t *coordinator;
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 1u, 1u);
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_credit_settlement_result_t settled =
        TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
    turbo_flow_fmq_credit_settlement_snapshot_t snapshot =
        TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_SNAPSHOT_INIT;

    memset(&probe, 0, sizeof(probe));
    probe.ack_failures = 1;
    settler.ctx = &probe;
    settler.ack = broker_claim_settler_ack;
    settler.requeue = broker_claim_settler_requeue;
    settler.drop = broker_claim_settler_drop;
    credit_config.max_workers = 1u;
    credit_config.max_inflight = 1u;
    credit_config.max_credit_messages_per_worker = 1u;
    credit_config.max_credit_bytes_per_worker = 32u;
    credit_config.max_job_bytes = 32u;
    credit_config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    credit = turbo_flow_fmq_credit_worker_create(&credit_config);
    check_not_null(credit);
    settlement_config.capacity = 1u;
    coordinator = turbo_flow_fmq_credit_settlement_create(credit, &settler, &settlement_config);
    check_not_null(coordinator);
    grant.worker_id = "worker";
    grant.service = "jobs";
    grant.worker_route = worker;
    grant.sequence = 1u;
    grant.grant_messages = 1u;
    grant.grant_bytes = 32u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(credit, &grant), TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_settlement_dispatch(coordinator, 77u, "jobs", 7u, 16u,
                                                           &client, 1u, &dispatch),
                 TURBO_OK);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_settlement_dispatch(coordinator, 77u, "jobs", 8u, 16u,
                                                           &client, 1u, &dispatch),
                 TURBO_EALREADY);
    check_int_eq(turbo_flow_fmq_credit_settlement_dispatch(coordinator, 88u, "jobs", 8u, 16u,
                                                           &client, 1u, &dispatch),
                 TURBO_ENOSPC);
    check_int_eq(
        turbo_flow_fmq_credit_settlement_complete(coordinator, "worker", &worker, 7u, &settled),
        TURBO_EIO);
    check_size_eq(probe.ack_calls, 1u);
    check_uint_eq(probe.ack_token, 77u);
    check_int_eq(turbo_flow_fmq_credit_settlement_snapshot(coordinator, &snapshot), TURBO_OK);
    check_size_eq(snapshot.tracked, 1u);
    check_size_eq(snapshot.pending_ack, 1u);
    check_uint_eq(snapshot.settlement_failures, 1u);
    check_int_eq(turbo_flow_fmq_credit_settlement_destroy(coordinator), TURBO_EBUSY);

    check_int_eq(turbo_flow_fmq_credit_settlement_retry_one(coordinator, &settled), TURBO_OK);
    check_int_eq(settled.action, TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_ACK);
    check_uint_eq(settled.request_id, 7u);
    check_uint_eq(settled.claim_token, 77u);
    check_size_eq(probe.ack_calls, 2u);
    settled =
        (turbo_flow_fmq_credit_settlement_result_t)TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_settlement_retry_one(coordinator, &settled), TURBO_ENOENT);
    grant.sequence = 2u;
    grant.grant_messages = 1u;
    grant.grant_bytes = 16u;
    grant.now_ms = 2u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(credit, &grant), TURBO_OK);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_settlement_dispatch(coordinator, 88u, "jobs", 8u, 16u,
                                                           &client, 2u, &dispatch),
                 TURBO_OK);
    settled =
        (turbo_flow_fmq_credit_settlement_result_t)TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_settlement_cancel(
                     coordinator, 8u, TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_DROP, &settled),
                 TURBO_OK);
    check_int_eq(settled.action, TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_DROP);
    check_uint_eq(settled.claim_token, 88u);
    check_size_eq(probe.drop_calls, 1u);
    check_uint_eq(probe.drop_token, 88u);
    snapshot =
        (turbo_flow_fmq_credit_settlement_snapshot_t)TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_fmq_credit_settlement_snapshot(coordinator, &snapshot), TURBO_OK);
    check_size_eq(snapshot.tracked, 0u);
    check_uint_eq(snapshot.settled_acks, 1u);
    check_uint_eq(snapshot.settled_drops, 1u);
    check_int_eq(turbo_flow_fmq_credit_settlement_destroy(coordinator), TURBO_OK);
    turbo_flow_fmq_credit_worker_destroy(credit);
  }

  it("recovers durable credit state and replays completion by logical address") {
    turbo_flow_fmq_credit_worker_config_t credit_config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_durable_config_t durable_config =
        TURBO_FLOW_FMQ_CREDIT_DURABLE_CONFIG_INIT;
    broker_durable_settler_probe_t probe;
    turbo_flow_claim_settler_t settler = TURBO_FLOW_CLAIM_SETTLER_INIT;
    turbo_flow_fmq_credit_worker_t *credit = NULL;
    turbo_flow_fmq_credit_durable_t *durable = NULL;
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 1u, 1u);
    turbo_flow_fmq_broker_logical_address_t address =
        broker_logical_address("broker-a", "client-a", 42u);
    turbo_flow_fmq_broker_logical_address_t outbox = TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_credit_settlement_result_t settled =
        TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_RESULT_INIT;
    turbo_flow_fmq_credit_durable_snapshot_t snapshot = TURBO_FLOW_FMQ_CREDIT_DURABLE_SNAPSHOT_INIT;

    memset(&probe, 0, sizeof(probe));
    settler.ctx = &probe;
    settler.max_state_size = sizeof(probe.state);
    settler.load_state = broker_durable_state_load;
    settler.commit_state = broker_durable_state_commit;
    credit_config.max_workers = 1u;
    credit_config.max_inflight = 1u;
    credit_config.max_credit_messages_per_worker = 1u;
    credit_config.max_credit_bytes_per_worker = 32u;
    credit_config.max_job_bytes = 32u;
    credit_config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    durable_config.capacity = 2u;
    durable_config.terminal_ttl_ms = 10u;
    durable_config.state_key = "fmq:credit:state";
    grant.worker_id = "worker";
    grant.service = "jobs";
    grant.worker_route = worker;
    grant.sequence = 1u;
    grant.grant_messages = 1u;
    grant.grant_bytes = 32u;

    credit = turbo_flow_fmq_credit_worker_create(&credit_config);
    check_not_null(credit);
    check_int_eq(turbo_flow_fmq_credit_durable_create(credit, &settler, &durable_config, &durable),
                 TURBO_OK);
    check_not_null(durable);
    check_int_eq(turbo_flow_fmq_credit_worker_grant(credit, &grant), TURBO_OK);
    probe.active_token = 101u;
    probe.claim_active = 1;
    probe.fail_after_commit = 1;
    check_int_eq(turbo_flow_fmq_credit_durable_dispatch(durable, 101u, &address, 1001u, "jobs", 16u,
                                                        &client, 1u, &dispatch),
                 TURBO_EIO);
    check_int_eq(turbo_flow_fmq_credit_durable_snapshot(durable, &snapshot), TURBO_OK);
    check_size_eq(snapshot.pending, 1u);
    grant.sequence = 2u;
    grant.grant_bytes = 16u;
    grant.now_ms = 2u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(credit, &grant), TURBO_OK);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_dispatch(durable, 101u, &address, 1001u, "jobs", 16u,
                                                        &client, 2u, &dispatch),
                 TURBO_OK);
    snapshot =
        (turbo_flow_fmq_credit_durable_snapshot_t)TURBO_FLOW_FMQ_CREDIT_DURABLE_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_snapshot(durable, &snapshot), TURBO_OK);
    check_size_eq(snapshot.inflight, 1u);

    /* Simulated process loss: the persisted INFLIGHT state is normalized to PENDING. */
    turbo_flow_fmq_credit_durable_destroy(durable);
    turbo_flow_fmq_credit_worker_destroy(credit);
    durable = NULL;
    credit = turbo_flow_fmq_credit_worker_create(&credit_config);
    check_not_null(credit);
    check_int_eq(turbo_flow_fmq_credit_durable_create(credit, &settler, &durable_config, &durable),
                 TURBO_OK);
    snapshot =
        (turbo_flow_fmq_credit_durable_snapshot_t)TURBO_FLOW_FMQ_CREDIT_DURABLE_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_snapshot(durable, &snapshot), TURBO_OK);
    check_size_eq(snapshot.pending, 1u);
    grant.sequence = 1u;
    check_int_eq(turbo_flow_fmq_credit_worker_grant(credit, &grant), TURBO_OK);
    probe.active_token = 102u;
    probe.claim_active = 1;
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_dispatch(durable, 102u, &address, 1002u, "jobs", 16u,
                                                        &client, 2u, &dispatch),
                 TURBO_OK);

    /* Redis applied the transaction, but the reply was lost. Exact retry reconciles success. */
    probe.fail_after_commit = 1;
    check_int_eq(
        turbo_flow_fmq_credit_durable_complete(durable, "worker", &worker, 1002u, 3u, &settled),
        TURBO_EIO);
    check_false(probe.claim_active);
    check_int_eq(turbo_flow_fmq_credit_durable_retry_one(durable, &settled), TURBO_OK);
    check_int_eq(settled.action, TURBO_FLOW_FMQ_CREDIT_SETTLEMENT_ACK);
    check_uint_eq(settled.claim_token, 102u);
    check_int_eq(turbo_flow_fmq_credit_durable_outbox_next(durable, &outbox), TURBO_OK);
    check_str_eq(outbox.origin_broker_id, "broker-a");
    check_str_eq(outbox.client_id, "client-a");
    check_uint_eq(outbox.request_id, 42u);

    turbo_flow_fmq_credit_durable_destroy(durable);
    turbo_flow_fmq_credit_worker_destroy(credit);
    durable = NULL;
    credit = turbo_flow_fmq_credit_worker_create(&credit_config);
    check_not_null(credit);
    check_int_eq(turbo_flow_fmq_credit_durable_create(credit, &settler, &durable_config, &durable),
                 TURBO_OK);
    outbox = (turbo_flow_fmq_broker_logical_address_t)TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_outbox_next(durable, &outbox), TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_durable_outbox_confirm(durable, &outbox, 4u), TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_durable_outbox_confirm(durable, &outbox, 4u), TURBO_OK);
    outbox = (turbo_flow_fmq_broker_logical_address_t)TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_outbox_next(durable, &outbox), TURBO_ENOENT);
    snapshot =
        (turbo_flow_fmq_credit_durable_snapshot_t)TURBO_FLOW_FMQ_CREDIT_DURABLE_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_snapshot(durable, &snapshot), TURBO_OK);
    check_size_eq(snapshot.completed, 1u);
    check_uint_eq(snapshot.retries, 0u);
    check_uint_eq(snapshot.settlement_failures, 0u);
    outbox = (turbo_flow_fmq_broker_logical_address_t)TURBO_FLOW_FMQ_BROKER_LOGICAL_ADDRESS_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_expire_terminal(durable, 13u, &outbox),
                 TURBO_ENOENT);
    check_int_eq(turbo_flow_fmq_credit_durable_expire_terminal(durable, 14u, &outbox), TURBO_OK);
    check_uint_eq(outbox.request_id, 42u);
    snapshot =
        (turbo_flow_fmq_credit_durable_snapshot_t)TURBO_FLOW_FMQ_CREDIT_DURABLE_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_snapshot(durable, &snapshot), TURBO_OK);
    check_size_eq(snapshot.records, 0u);
    turbo_flow_fmq_credit_durable_destroy(durable);
    turbo_flow_fmq_credit_worker_destroy(credit);
  }

  it("quiesces durable dispatch and retries uncertain shutdown settlement") {
    turbo_flow_fmq_credit_worker_config_t credit_config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_durable_config_t durable_config =
        TURBO_FLOW_FMQ_CREDIT_DURABLE_CONFIG_INIT;
    broker_durable_settler_probe_t probe;
    turbo_flow_claim_settler_t settler = TURBO_FLOW_CLAIM_SETTLER_INIT;
    turbo_flow_fmq_credit_worker_t *credit;
    turbo_flow_fmq_credit_durable_t *durable = NULL;
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 1u, 1u);
    turbo_flow_fmq_broker_logical_address_t address =
        broker_logical_address("broker-a", "client-shutdown", 88u);
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_credit_durable_snapshot_t snapshot = TURBO_FLOW_FMQ_CREDIT_DURABLE_SNAPSHOT_INIT;

    memset(&probe, 0, sizeof(probe));
    settler.ctx = &probe;
    settler.max_state_size = sizeof(probe.state);
    settler.load_state = broker_durable_state_load;
    settler.commit_state = broker_durable_state_commit;
    credit_config.max_workers = 1u;
    credit_config.max_inflight = 1u;
    credit_config.max_credit_messages_per_worker = 1u;
    credit_config.max_credit_bytes_per_worker = 32u;
    credit_config.max_job_bytes = 32u;
    credit_config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    durable_config.capacity = 1u;
    durable_config.state_key = "fmq:credit:shutdown";
    durable_config.shutdown_policy = TURBO_FLOW_FMQ_CREDIT_SHUTDOWN_REQUEUE;
    durable_config.shutdown_max_steps = 1u;
    grant.worker_id = "worker";
    grant.service = "jobs";
    grant.worker_route = worker;
    grant.sequence = 1u;
    grant.grant_messages = 1u;
    grant.grant_bytes = 32u;

    credit = turbo_flow_fmq_credit_worker_create(&credit_config);
    check_not_null(credit);
    check_int_eq(turbo_flow_fmq_credit_durable_create(credit, &settler, &durable_config, &durable),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_worker_grant(credit, &grant), TURBO_OK);
    probe.active_token = 501u;
    probe.claim_active = 1;
    check_int_eq(turbo_flow_fmq_credit_durable_dispatch(durable, 501u, &address, 5001u, "jobs", 16u,
                                                        &client, 1u, &dispatch),
                 TURBO_OK);

    probe.fail_after_commit = 1;
    check_int_eq(turbo_flow_fmq_credit_durable_shutdown(durable, 2u, &snapshot), TURBO_EIO);
    check_true(snapshot.quiesced);
    check_size_eq(snapshot.pending, 1u);
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_dispatch(durable, 502u, &address, 5002u, "jobs", 16u,
                                                        &client, 2u, &dispatch),
                 TURBO_ESHUTDOWN);

    snapshot =
        (turbo_flow_fmq_credit_durable_snapshot_t)TURBO_FLOW_FMQ_CREDIT_DURABLE_SNAPSHOT_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_shutdown(durable, 3u, &snapshot), TURBO_OK);
    check_true(snapshot.quiesced);
    check_size_eq(snapshot.pending, 1u);
    check_size_eq(snapshot.inflight, 0u);

    turbo_flow_fmq_credit_durable_destroy(durable);
    turbo_flow_fmq_credit_worker_destroy(credit);
  }


  it("poisons a recovered max-attempt claim without redispatching it") {
    turbo_flow_fmq_credit_worker_config_t credit_config = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
    turbo_flow_fmq_credit_durable_config_t durable_config =
        TURBO_FLOW_FMQ_CREDIT_DURABLE_CONFIG_INIT;
    broker_durable_settler_probe_t probe;
    turbo_flow_claim_settler_t settler = TURBO_FLOW_CLAIM_SETTLER_INIT;
    turbo_flow_fmq_credit_worker_t *credit;
    turbo_flow_fmq_credit_durable_t *durable = NULL;
    turbo_flow_fmq_credit_grant_t grant = TURBO_FLOW_FMQ_CREDIT_GRANT_INIT;
    turbo_flow_protocol_route_t worker = broker_route(10u, 1u, 1u);
    turbo_flow_protocol_route_t client = broker_route(20u, 1u, 1u);
    turbo_flow_fmq_broker_logical_address_t address =
        broker_logical_address("broker-a", "client-max", 99u);
    turbo_flow_fmq_broker_dispatch_result_t dispatch = TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    turbo_flow_fmq_credit_durable_snapshot_t snapshot = TURBO_FLOW_FMQ_CREDIT_DURABLE_SNAPSHOT_INIT;

    memset(&probe, 0, sizeof(probe));
    settler.ctx = &probe;
    settler.max_state_size = sizeof(probe.state);
    settler.load_state = broker_durable_state_load;
    settler.commit_state = broker_durable_state_commit;
    credit_config.max_workers = 1u;
    credit_config.max_inflight = 1u;
    credit_config.max_credit_messages_per_worker = 1u;
    credit_config.max_credit_bytes_per_worker = 32u;
    credit_config.max_job_bytes = 32u;
    credit_config.reliability = TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE;
    durable_config.capacity = 1u;
    durable_config.max_attempts = 1u;
    durable_config.terminal_ttl_ms = 10u;
    durable_config.state_key = "fmq:credit:max-attempt";
    grant.worker_id = "worker";
    grant.service = "jobs";
    grant.worker_route = worker;
    grant.sequence = 1u;
    grant.grant_messages = 1u;
    grant.grant_bytes = 32u;

    credit = turbo_flow_fmq_credit_worker_create(&credit_config);
    check_not_null(credit);
    check_int_eq(turbo_flow_fmq_credit_durable_create(credit, &settler, &durable_config, &durable),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_credit_worker_grant(credit, &grant), TURBO_OK);
    probe.active_token = 1u;
    probe.claim_active = 1;
    check_int_eq(turbo_flow_fmq_credit_durable_dispatch(durable, 1u, &address, 1u, "jobs", 16u,
                                                        &client, 1u, &dispatch),
                 TURBO_OK);
    turbo_flow_fmq_credit_durable_destroy(durable);
    turbo_flow_fmq_credit_worker_destroy(credit);

    credit = turbo_flow_fmq_credit_worker_create(&credit_config);
    check_not_null(credit);
    durable = NULL;
    check_int_eq(turbo_flow_fmq_credit_durable_create(credit, &settler, &durable_config, &durable),
                 TURBO_OK);
    probe.active_token = 2u;
    probe.claim_active = 1;
    dispatch = (turbo_flow_fmq_broker_dispatch_result_t)TURBO_FLOW_FMQ_BROKER_DISPATCH_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_credit_durable_dispatch(durable, 2u, &address, 2u, "jobs", 16u,
                                                        &client, 2u, &dispatch),
                 TURBO_EALREADY);
    check_false(probe.claim_active);
    check_int_eq(turbo_flow_fmq_credit_durable_snapshot(durable, &snapshot), TURBO_OK);
    check_size_eq(snapshot.poisoned, 1u);
    check_size_eq(snapshot.inflight, 0u);
    turbo_flow_fmq_credit_durable_destroy(durable);
    turbo_flow_fmq_credit_worker_destroy(credit);
  }

  group("TFCW graph pattern") {
    it("registers typed operations") {
      turbo_flow_fmq_credit_worker_config_t credit = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
      turbo_flow_fmq_tfcw_graph_config_t graph = TURBO_FLOW_FMQ_TFCW_GRAPH_CONFIG_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      graph.service = "jobs";
      graph.resource_name = "credit";
      graph.resource_uid = "fmq:credit:test";
      graph.owner_name = "credit-test";
      credit.max_workers = 1u;
      credit.max_inflight = 2u;
      check_not_null(flow);
      check_int_eq(turbo_flow_fmq_tfcw_register_graph(flow, "tfcw", &graph, &credit), TURBO_OK);
      check_not_null(turbo_flow_find_module(flow, TURBO_FLOW_FMQ_TFCW_MODULE));
      check_not_null(turbo_flow_find_primitive(flow, "credit"));
      check_not_null(turbo_flow_find_operation(flow, TURBO_FLOW_FMQ_TFCW_DISPATCH_OPERATION));
      {
        turbo_flow_resource_snapshot_t snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
        turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
        check_size_eq(turbo_flow_resource_count(flow), 1u);
        check_int_eq(turbo_flow_resource_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
        check_str_eq(snapshot.uid, "fmq:credit:test");
        check_size_eq(snapshot.load, 0u);
        check_size_eq(snapshot.capacity, 2u);
        check_int_eq(turbo_flow_resource_document_at(
                         flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
                     TURBO_OK);
        check_str_eq(document.schema->type_name, "CreditWorkerStatus");
        turbo_flow_resource_document_cleanup(&document);
      }
      turbo_flow_destroy(flow);
    }

    it("rejects non-inline execution") {
      static const char dsl[] = "source input\nstage dispatch adapter tfcw operation fmq.credit.dispatch resource credit exec thread workers 1\nstage main {\n  input -> dispatch\n}\n";
      turbo_flow_fmq_credit_worker_config_t credit = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
      turbo_flow_fmq_tfcw_graph_config_t graph = TURBO_FLOW_FMQ_TFCW_GRAPH_CONFIG_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      graph.service = "jobs";
      graph.resource_name = "credit";
      graph.resource_uid = "fmq:credit:inline";
      graph.owner_name = "credit-inline";
      credit.max_workers = 1u;
      credit.max_inflight = 1u;
      check_not_null(flow);
      check_int_eq(turbo_flow_fmq_tfcw_register_graph(flow, "tfcw", &graph, &credit), TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_EINVAL);
      turbo_flow_destroy(flow);
    }

    it("does not downgrade durable resolved configuration") {
      static const char yaml[] = "version: 1\nchannels:\n  bulk:\n    kind: fmq_pattern\n    config:\n      pattern: credit_worker\n      scheduler: lru\n      reliability: at_least_once\n      service: jobs\n      max_workers: 1\n      max_inflight: 1\n      worker_lease_ms: 1000\n      max_credit_messages_per_worker: 1\n      max_credit_bytes_per_worker: 1024\n      max_job_bytes: 1024\n      storage_channel: redis.claims\n      state_key: fmq:test\n      max_attempts: 2\n      dedup_ttl_ms: 1000\n      shutdown_policy: requeue\n      shutdown_max_steps: 1\nadapters: {}\n";
      turbo_flow_fmq_tfcw_graph_config_t graph = TURBO_FLOW_FMQ_TFCW_GRAPH_CONFIG_INIT;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_resolved_config_t *resolved = NULL;
      turbo_flow_t *flow = turbo_flow_create();
      graph.resource_name = "credit";
      graph.resource_uid = "fmq:credit:durable";
      graph.owner_name = "credit-durable";
      check_not_null(flow);
      check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                   TURBO_OK);
      check_int_eq(turbo_flow_fmq_tfcw_register_resolved_graph(
                       flow, "tfcw", &graph, resolved, "bulk", &error),
                   TURBO_ENOTSUP);
      check_null(turbo_flow_find_primitive(flow, "credit"));
      check_str_contains(error.path, "channels.bulk.config.reliability");
      turbo_flow_resolved_config_destroy(resolved);
      turbo_flow_destroy(flow);
    }

    it("drops READY and replaces JOB and COMPLETE routes") {
      static const char dsl[] = "source control_in\nsource job_in\nsource reply_in\nstage control adapter tfcw operation fmq.credit.control resource credit\nstage dispatch adapter tfcw operation fmq.credit.dispatch resource credit\nstage complete adapter tfcw operation fmq.credit.complete resource credit\nstage tail\nstage main {\n  control_in -> control -> tail\n  job_in -> dispatch -> tail\n  reply_in -> complete -> tail\n}\n";
      turbo_flow_fmq_credit_worker_config_t credit = TURBO_FLOW_FMQ_CREDIT_WORKER_CONFIG_INIT;
      turbo_flow_fmq_tfcw_graph_config_t graph = TURBO_FLOW_FMQ_TFCW_GRAPH_CONFIG_INIT;
      turbo_flow_protocol_route_t worker_route = broker_route(1u, 10u, 1u);
      turbo_flow_protocol_route_t client_route = broker_route(2u, 20u, 1u);
      turbo_flow_msg_t message;
      broker_tfcw_route_probe_t probe;
      uint8_t body[128];
      size_t body_size;
      turbo_flow_t *flow = turbo_flow_create();

      graph.service = "jobs";
      graph.resource_name = "credit";
      graph.resource_uid = "fmq:credit:graph";
      graph.owner_name = "credit-graph";
      credit.max_workers = 1u;
      credit.max_inflight = 2u;
      credit.max_credit_messages_per_worker = 2u;
      credit.max_credit_bytes_per_worker = 4096u;
      credit.max_job_bytes = 1024u;
      credit.worker_lease_ms = 60000u;
      memset(&probe, 0, sizeof(probe));
      check_not_null(flow);
      check_int_eq(turbo_flow_fmq_tfcw_register_graph(flow, "tfcw", &graph, &credit), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "tail", broker_tfcw_capture_route, &probe,
                                                NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
      check_int_eq(turbo_flow_start(flow), TURBO_OK);
      body_size = broker_tfcw_ready_body(body, sizeof(body), "worker-a", "jobs", 2u, 4096u);
      check_int_eq(broker_tfcw_encode_message(&message, TURBO_FLOW_TFCW_READY, 0u, 1u, body,
                                              body_size, &worker_route),
                   TURBO_OK);
      check_int_eq(turbo_flow_publish(flow, "control_in", &message), TURBO_OK);
      check_size_eq(probe.calls, 0u);
      turbo_flow_msg_cleanup(&message);
      body_size = broker_tfcw_job_body(body, sizeof(body), "client-a/41", "payload");
      check_int_eq(broker_tfcw_encode_message(&message, TURBO_FLOW_TFCW_JOB, 41u, 0u, body,
                                              body_size, &client_route),
                   TURBO_OK);
      check_int_eq(turbo_flow_publish(flow, "job_in", &message), TURBO_OK);
      check_size_eq(probe.calls, 1u);
      check_true(broker_tfcw_route_equal(&probe.route, &worker_route));
      turbo_flow_msg_cleanup(&message);
      body_size = broker_tfcw_job_body(body, sizeof(body), "client-a/42", "payload");
      check_int_eq(broker_tfcw_encode_message(&message, TURBO_FLOW_TFCW_JOB, 42u, 0u, body,
                                              body_size, &client_route),
                   TURBO_OK);
      check_int_eq(turbo_flow_publish(flow, "job_in", &message), TURBO_OK);
      check_size_eq(probe.calls, 2u);
      turbo_flow_msg_cleanup(&message);
      body_size = broker_tfcw_complete_body(body, sizeof(body), "worker-a");
      check_int_eq(broker_tfcw_encode_message(&message, TURBO_FLOW_TFCW_COMPLETE, 41u, 0u, body,
                                              body_size, &worker_route),
                   TURBO_OK);
      check_int_eq(turbo_flow_publish(flow, "reply_in", &message), TURBO_OK);
      check_size_eq(probe.calls, 3u);
      check_true(broker_tfcw_route_equal(&probe.route, &client_route));
      turbo_flow_msg_cleanup(&message);
      body_size = broker_tfcw_job_body(body, sizeof(body), "client-a/43", "payload");
      check_int_eq(broker_tfcw_encode_message(&message, TURBO_FLOW_TFCW_JOB, 43u, 0u, body,
                                              body_size, &client_route),
                   TURBO_OK);
      check_int_eq(turbo_flow_publish(flow, "job_in", &message), TURBO_FLOW_FMQ_EAGAIN);
      check_size_eq(probe.calls, 3u);
      turbo_flow_msg_cleanup(&message);
      check_int_eq(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }
  }
}
