#include "flowmq_destination.h"
#include "flowmq_tls_test_material.h"

#include "cmeta_error.h"
#include "tinytest.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum { FLOWMQ_DESTINATION_TEST_LIMIT = 10000u };

spec("FlowMQ CNet 2.3 remote admission policy") {
  it("uses canonical CNet selection without copying remote policy algorithms") {
    flowmq_destination_endpoint_t endpoints[2] = {
        {.endpoint_id = 10u, .authority_id = 7u,
         .uri = "tcp://127.0.0.1:10001", .weight = 1u,
         .inflight = 9u, .eligible = 1},
        {.endpoint_id = 20u, .authority_id = 7u,
         .uri = "tcp://127.0.0.1:10002", .weight = 3u,
         .inflight = 1u, .eligible = 1}};
    flowmq_destination_selection_t policy = FLOWMQ_DESTINATION_SELECTION_INIT;
    flowmq_destination_result_t result = FLOWMQ_DESTINATION_RESULT_INIT;
    policy.snapshot_generation = 31u;
    policy.explicit_endpoint_id = 20u;

    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_OK);
    check_equal(result.endpoint_id, UINT64_C(20));
    check_equal(result.index, (size_t)1u);
    check_equal(result.snapshot_generation, UINT64_C(31));

    policy.kind = FLOWMQ_DESTINATION_ROUND_ROBIN;
    policy.sequence = 0u;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_OK);
    check_equal(result.endpoint_id, UINT64_C(10));
    policy.sequence = 1u;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_OK);
    check_equal(result.endpoint_id, UINT64_C(20));

    policy.kind = FLOWMQ_DESTINATION_WEIGHTED_RR;
    policy.sequence = 2u;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_OK);
    check_equal(result.endpoint_id, UINT64_C(20));

    policy.kind = FLOWMQ_DESTINATION_LEAST_INFLIGHT;
    policy.sequence = 0u;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_OK);
    check_equal(result.endpoint_id, UINT64_C(20));

    policy.kind = FLOWMQ_DESTINATION_STRICT_KEY;
    policy.key_known = 1;
    policy.key_hash = UINT64_C(67890);
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_OK);
    /* Strict-key fixes the winner before checking health; a FULL winner
     * cannot silently redirect to the other eligible endpoint. */
    endpoints[result.index].eligible = 0;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_ENOBUFS);
    check_equal(result.index, SIZE_MAX);
  }

  it("fails closed on invalid snapshots, mixed security and expired generations") {
    flowmq_destination_endpoint_t endpoints[2] = {
        {.endpoint_id = 10u, .authority_id = 7u,
         .uri = "tcp://127.0.0.1:10001", .weight = 1u, .eligible = 1},
        {.endpoint_id = 20u, .authority_id = 7u,
         .uri = "tcp://127.0.0.1:10002", .weight = 1u, .eligible = 1}};
    flowmq_destination_selection_t policy = FLOWMQ_DESTINATION_SELECTION_INIT;
    flowmq_destination_result_t result = FLOWMQ_DESTINATION_RESULT_INIT;
    policy.snapshot_generation = 9u;
    policy.explicit_endpoint_id = 20u;

    check_equal(flowmq_destination_choose(NULL, 2u, &policy, &result),
                SALTS_EINVAL);
    check_equal(flowmq_destination_choose(endpoints, 0u, &policy, &result),
                SALTS_EINVAL);
    check_equal(flowmq_destination_choose(endpoints, 5u, &policy, &result),
                SALTS_EINVAL);
    policy.snapshot_generation = 0u;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_EINVAL);
    policy.snapshot_generation = 9u;
    policy.expires_at_ms = 1u;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_ETIMEDOUT);
    policy.expires_at_ms = UINT64_MAX;

    endpoints[1].endpoint_id = 10u;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_EINVAL);
    endpoints[1].endpoint_id = 20u;
    endpoints[1].authority_id = 8u;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_EINVAL);
    endpoints[1].authority_id = 7u;
    endpoints[1].uri = "tls://127.0.0.1:10002";
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_EINVAL);
    endpoints[1].uri = "tcp://127.0.0.1:10002";
    endpoints[1].weight = 0u;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_EINVAL);
    endpoints[1].weight = 1u;
    endpoints[1].eligible = 2;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_EINVAL);
    endpoints[1].eligible = 1;
    policy.explicit_endpoint_id = 12345u;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_ENOENT);
    policy.explicit_endpoint_id = 20u;
    endpoints[1].eligible = 0;
    check_equal(flowmq_destination_choose(endpoints, 2u, &policy, &result),
                SALTS_ENOBUFS);
    check_equal(result.endpoint_id, UINT64_C(0));
    check_equal(result.index, SIZE_MAX);
  }

  it("uses one selected physical TCP URI for a real owner-driven PAIR session") {
    static const char payload[] = "selected-remote-only";
    char first_uri[128] = {0};
    char second_uri[128] = {0};
    char last_uri[128] = {0};
    char received[64] = {0};
    size_t uri_size = 0u;
    size_t received_size = 0u;
    size_t ready = 0u;
    int sent = 0;
    int received_message = 0;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *second = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *client = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_destination_selection_t policy = FLOWMQ_DESTINATION_SELECTION_INIT;
    flowmq_destination_result_t result = FLOWMQ_DESTINATION_RESULT_INIT;
    flowmq_destination_endpoint_t endpoints[2] = {{0}};

    check_not_null(ctx);
    check_not_null(first);
    check_not_null(second);
    check_not_null(client);
    check_equal(flowmq_bind(first, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_bind(second, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(first, first_uri,
                                     sizeof(first_uri), &uri_size), SALTS_OK);
    check_equal(flowmq_last_endpoint(second, second_uri,
                                     sizeof(second_uri), &uri_size), SALTS_OK);
    endpoints[0] = (flowmq_destination_endpoint_t){
        .endpoint_id = 10u, .authority_id = 55u, .uri = first_uri,
        .weight = 1u, .eligible = 1};
    endpoints[1] = (flowmq_destination_endpoint_t){
        .endpoint_id = 20u, .authority_id = 55u, .uri = second_uri,
        .weight = 1u, .eligible = 1};
    policy.kind = FLOWMQ_DESTINATION_EXPLICIT;
    policy.snapshot_generation = 12u;
    policy.explicit_endpoint_id = 20u;
    check_equal(flowmq_connect_selected(
                    client, endpoints, 2u, &policy, &result), SALTS_OK);
    check_equal(result.endpoint_id, UINT64_C(20));
    check_equal(result.snapshot_generation, UINT64_C(12));
    check_equal(result.index, (size_t)1u);
    check_equal(flowmq_last_endpoint(client, last_uri, sizeof(last_uri),
                                     &uri_size), SALTS_OK);
    check_equal(strcmp(last_uri, second_uri), 0);

    /* PAIR is single-peer. A second admission cannot silently migrate or
     * duplicate an already selected connection. */
    check_equal(flowmq_connect_selected(
                    client, endpoints, 2u, &policy, &result), SALTS_EBUSY);
    check_equal(result.index, SIZE_MAX);

    for (size_t i = 0u;
         i < FLOWMQ_DESTINATION_TEST_LIMIT && !received_message; ++i) {
      flowmq_pollitem_t items[3] = {
          {.socket = first}, {.socket = second}, {.socket = client}};
      check_equal(flowmq_poll(items, 3u, 1u, &ready), SALTS_OK);
      if (!sent && flowmq_send(client, payload, sizeof(payload) - 1u,
                                FLOWMQ_DONTWAIT) == SALTS_OK)
        sent = 1;
      if (sent && flowmq_recv(second, received, sizeof(received),
                              &received_size, FLOWMQ_DONTWAIT) == SALTS_OK)
        received_message = 1;
    }
    check_true(received_message);
    check_equal(received_size, sizeof(payload) - 1u);
    check_equal(memcmp(received, payload, received_size), 0);
    check_equal(flowmq_recv(first, received, sizeof(received),
                            &received_size, FLOWMQ_DONTWAIT), SALTS_EBUSY);

    check_equal(flowmq_close(client), SALTS_OK);
    check_equal(flowmq_close(second), SALTS_OK);
    check_equal(flowmq_close(first), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("selects one actually verified TLS peer without contacting the other") {
    static const char payload[] = "authenticated-selected-destination";
    char first_uri[128] = {0};
    char second_uri[128] = {0};
    char last_uri[128] = {0};
    char received[64] = {0};
    size_t uri_size = 0u;
    size_t received_size = 0u;
    size_t ready = 0u;
    int sent = 0;
    int received_message = 0;
    char *ca_path = tt_make_temp_file("flowmq-destination-ca", ".pem");
    char *cert_path = tt_make_temp_file("flowmq-destination-cert", ".pem");
    char *key_path = tt_make_temp_file("flowmq-destination-key", ".pem");
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *second = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *client = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_destination_selection_t policy = FLOWMQ_DESTINATION_SELECTION_INIT;
    flowmq_destination_result_t result = FLOWMQ_DESTINATION_RESULT_INIT;
    flowmq_destination_endpoint_t endpoints[2] = {{0}};

    check_not_null(ctx);
    check_not_null(first);
    check_not_null(second);
    check_not_null(client);
    check_not_null(ca_path);
    check_not_null(cert_path);
    check_not_null(key_path);
    check_equal(tt_write_file(ca_path, FLOWMQ_TLS_TEST_ROOT_CA,
                              sizeof(FLOWMQ_TLS_TEST_ROOT_CA) - 1u), 0);
    check_equal(tt_write_file(cert_path, FLOWMQ_TLS_TEST_CERTIFICATE,
                              sizeof(FLOWMQ_TLS_TEST_CERTIFICATE) - 1u), 0);
    check_equal(tt_write_file(key_path, FLOWMQ_TLS_TEST_KEY,
                              sizeof(FLOWMQ_TLS_TEST_KEY) - 1u), 0);
    check_equal(flowmq_setsockopt(first, FLOWMQ_TLS_CERT_FILE, cert_path,
                                  strlen(cert_path)), SALTS_OK);
    check_equal(flowmq_setsockopt(first, FLOWMQ_TLS_KEY_FILE, key_path,
                                  strlen(key_path)), SALTS_OK);
    check_equal(flowmq_setsockopt(second, FLOWMQ_TLS_CERT_FILE, cert_path,
                                  strlen(cert_path)), SALTS_OK);
    check_equal(flowmq_setsockopt(second, FLOWMQ_TLS_KEY_FILE, key_path,
                                  strlen(key_path)), SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_CA_FILE, ca_path,
                                  strlen(ca_path)), SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_SERVER_NAME,
                                  "localhost", strlen("localhost")), SALTS_OK);
    check_equal(flowmq_bind(first, "tls://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_bind(second, "tls://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(first, first_uri,
                                     sizeof(first_uri), &uri_size), SALTS_OK);
    check_equal(flowmq_last_endpoint(second, second_uri,
                                     sizeof(second_uri), &uri_size), SALTS_OK);
    endpoints[0] = (flowmq_destination_endpoint_t){
        .endpoint_id = 10u, .authority_id = 55u, .uri = first_uri,
        .weight = 1u, .eligible = 1};
    endpoints[1] = (flowmq_destination_endpoint_t){
        .endpoint_id = 20u, .authority_id = 55u, .uri = second_uri,
        .weight = 1u, .eligible = 1};
    policy.kind = FLOWMQ_DESTINATION_EXPLICIT;
    policy.snapshot_generation = 73u;
    policy.explicit_endpoint_id = 20u;
    check_equal(flowmq_connect_selected(
                    client, endpoints, 2u, &policy, &result), SALTS_OK);
    check_equal(result.endpoint_id, UINT64_C(20));
    check_equal(result.snapshot_generation, UINT64_C(73));
    check_equal(flowmq_last_endpoint(client, last_uri,
                                     sizeof(last_uri), &uri_size), SALTS_OK);
    check_equal(strcmp(last_uri, second_uri), 0);

    /* TLS admission succeeds only after CNet authenticates the certificate
     * and hostname. A nonchosen listener has no DATA or alternate dial. */
    for (size_t i = 0u;
         i < FLOWMQ_DESTINATION_TEST_LIMIT && !received_message; ++i) {
      flowmq_pollitem_t items[3] = {
          {.socket = first}, {.socket = second}, {.socket = client}};
      check_equal(flowmq_poll(items, 3u, 1u, &ready), SALTS_OK);
      if (!sent && flowmq_send(client, payload, sizeof(payload) - 1u,
                               FLOWMQ_DONTWAIT) == SALTS_OK)
        sent = 1;
      if (sent && flowmq_recv(second, received, sizeof(received),
                              &received_size, FLOWMQ_DONTWAIT) == SALTS_OK)
        received_message = 1;
    }
    check_true(received_message);
    check_equal(received_size, sizeof(payload) - 1u);
    check_equal(memcmp(received, payload, received_size), 0);
    check_equal(flowmq_recv(first, received, sizeof(received),
                            &received_size, FLOWMQ_DONTWAIT), SALTS_EBUSY);

    check_equal(flowmq_close(client), SALTS_OK);
    check_equal(flowmq_close(second), SALTS_OK);
    check_equal(flowmq_close(first), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
    check_equal(tt_remove_file(ca_path), 0);
    check_equal(tt_remove_file(cert_path), 0);
    check_equal(tt_remove_file(key_path), 0);
    free(ca_path);
    free(cert_path);
    free(key_path);
  }
}
