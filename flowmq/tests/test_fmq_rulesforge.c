#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_fmq.h"
#include "turbo_flow_rulesforge.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

enum {
  FMQ_RULESFORGE_TEST_TIMEOUT_MS = 2000,
  FMQ_RULESFORGE_TEST_WAIT_ATTEMPTS = 400,
  FMQ_RULESFORGE_TEST_WAIT_STEP_MS = 5,
  FMQ_RULESFORGE_MATCHED_FLAG = 1u << 8
};

typedef struct fmq_rulesforge_capture_s {
  atomic_int called;
  char payload[64];
  size_t payload_size;
  uint32_t match_count;
} fmq_rulesforge_capture_t;

static const turbo_flow_coronet_execution_binding_t FMQ_RULESFORGE_PRIVATE_EXECUTION = {
    sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};

static unsigned short fmq_rulesforge_test_port(void) {
  struct sockaddr_in address;
#ifdef _WIN32
  SOCKET socket_handle = INVALID_SOCKET;
  int address_size = (int)sizeof(address);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0u;
  socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle == INVALID_SOCKET) {
    WSACleanup();
    return 0u;
  }
#else
  int socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  socklen_t address_size = (socklen_t)sizeof(address);
  if (socket_handle < 0) return 0u;
#endif
  unsigned short port = 0u;

  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(socket_handle, (struct sockaddr *)&address, sizeof(address)) == 0 &&
      getsockname(socket_handle, (struct sockaddr *)&address, &address_size) == 0) {
    port = ntohs(address.sin_port);
  }
#ifdef _WIN32
  closesocket(socket_handle);
  WSACleanup();
#else
  close(socket_handle);
#endif
  return port;
}

static void fmq_rulesforge_config(turbo_flow_fmq_config_t *config,
                                  turbo_flow_fmq_pattern_t pattern,
                                  turbo_flow_fmq_endpoint_mode_t mode,
                                  unsigned short port, const char *topic) {
  *config = (turbo_flow_fmq_config_t)TURBO_FLOW_FMQ_CONFIG_INIT;
  config->pattern = pattern;
  config->mode = mode;
  config->transport = TURBO_FLOW_FMQ_TCP;
  config->host = "127.0.0.1";
  config->port = (int)port;
  config->topic = topic;
  config->content_type = "application/json";
  config->timeout_ms = FMQ_RULESFORGE_TEST_TIMEOUT_MS;
}

static int fmq_rulesforge_capture(turbo_flow_msg_t *message, void *ctx) {
  fmq_rulesforge_capture_t *capture = (fmq_rulesforge_capture_t *)ctx;
  if (!message || !capture || message->payload.len >= sizeof(capture->payload))
    return TURBO_EINVAL;
  if (message->payload.len != 0u)
    memcpy(capture->payload, message->payload.data, message->payload.len);
  capture->payload[message->payload.len] = '\0';
  capture->payload_size = message->payload.len;
  capture->match_count = message->data_decision.match_count;
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return TURBO_OK;
}

static int fmq_rulesforge_wait_capture(fmq_rulesforge_capture_t *capture, int expected) {
  for (int attempt = 0; attempt < FMQ_RULESFORGE_TEST_WAIT_ATTEMPTS; ++attempt) {
    if (atomic_load_explicit(&capture->called, memory_order_acquire) >= expected)
      return TURBO_OK;
    turbo_sleep_ms(FMQ_RULESFORGE_TEST_WAIT_STEP_MS);
  }
  return TURBO_ETIMEDOUT;
}

static int fmq_rulesforge_send_when_ready(turbo_flow_fmq_app_t *publisher,
                                          const char *payload) {
  int rc = TURBO_ENOTCONN;
  size_t payload_size;
  if (!publisher || !payload) return TURBO_EINVAL;
  payload_size = strlen(payload);
  for (int attempt = 0;
       attempt < FMQ_RULESFORGE_TEST_WAIT_ATTEMPTS && rc == TURBO_ENOTCONN;
       ++attempt) {
    rc = turbo_flow_fmq_app_send(publisher, payload, payload_size);
    if (rc == TURBO_ENOTCONN) turbo_sleep_ms(FMQ_RULESFORGE_TEST_WAIT_STEP_MS);
  }
  return rc;
}

spec("FlowMQ RulesForge content routing") {
  it("routes JSON messages through fixed Graph branches") {
    static const char schema_text[] =
        "schema FlowMQRouting [id(41), version(1), byte_order(little)]; "
        "message Order { int64 amount; }";
    static const char graph[] =
        "source inbound adapter fmq.orders\n"
        "stage classify operation rulesforge.apply resource rules.orders\n"
        "stage high_value\n"
        "stage normal\n"
        "stage main {\n"
        "  inbound -> classify\n"
        "  route classify -> high_value when msg.rule_match_count > 0\n"
        "  route classify -> normal when msg.rule_match_count == 0\n"
        "}\n";
    static const char high_value_json[] = "{\"amount\":15000}";
    static const char normal_json[] = "{\"amount\":250}";
    char rule_text[2048];
    char *schema_path = tt_make_temp_file("flowmq_rulesforge", ".schema");
    turbo_flow_fmq_config_t publisher_config;
    turbo_flow_fmq_config_t subscriber_config;
    turbo_flow_fmq_app_options_t publisher_options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
    turbo_flow_rulesforge_json_provider_t provider =
        TURBO_FLOW_RULEFORGE_JSON_PROVIDER_INIT;
    turbo_flow_fmq_app_t *publisher = NULL;
    turbo_flow_t *subscriber = turbo_flow_create();
    ruleforge_knowledge_base_t knowledge_base = NULL;
    fmq_rulesforge_capture_t high_value;
    fmq_rulesforge_capture_t normal;
    unsigned short port = fmq_rulesforge_test_port();
    int rule_size;

    memset(&high_value, 0, sizeof(high_value));
    memset(&normal, 0, sizeof(normal));
    atomic_init(&high_value.called, 0);
    atomic_init(&normal.called, 0);
    check_not_null(schema_path);
    check_not_null(subscriber);
    check_uint_ne(port, 0u);
    for (size_t i = 0u; schema_path[i] != '\0'; ++i) {
      if (schema_path[i] == '\\') schema_path[i] = '/';
    }
    check_int_eq(tt_write_file(schema_path, schema_text, sizeof(schema_text) - 1u), 0);
    rule_size = snprintf(rule_text, sizeof(rule_text),
                         "import \"%s\";\n"
                         "rule \"High value order\"\n"
                         "when\n"
                         "  Order(amount >= 10000)\n"
                         "then\n"
                         "end\n",
                         schema_path);
    check_int_gt(rule_size, 0);
    check_int_lt(rule_size, (int)sizeof(rule_text));
    check_int_eq(ruleforge_init(), RULES_FORGE_OK);
    check_int_eq(ruleforge_kb_create(&knowledge_base), RULES_FORGE_OK);
    check_int_eq(ruleforge_kb_load_drl(knowledge_base, rule_text), RULES_FORGE_OK);

    fmq_rulesforge_config(&publisher_config, TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_BIND,
                          port, "orders.created");
    fmq_rulesforge_config(&subscriber_config, TURBO_FLOW_FMQ_SUB,
                          TURBO_FLOW_FMQ_CONNECT, port, "orders.");
    provider.knowledge_base = knowledge_base;
    provider.fact_type = "Order";
    provider.matched_flag = FMQ_RULESFORGE_MATCHED_FLAG;
    provider.max_rules = 16;

    check_int_eq(turbo_flow_fmq_app_create(&publisher_config, &publisher_options, &publisher),
                 TURBO_OK);
    check_int_eq(turbo_flow_fmq_register_adapter_ex(
                     subscriber, "fmq.orders", &subscriber_config,
                     &FMQ_RULESFORGE_PRIVATE_EXECUTION),
                 TURBO_OK);
    check_int_eq(turbo_flow_rulesforge_register_json_provider(
                     subscriber, "rules.orders", &provider),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(
                     subscriber, "high_value", fmq_rulesforge_capture, &high_value, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(
                     subscriber, "normal", fmq_rulesforge_capture, &normal, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(subscriber, graph, sizeof(graph) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_start(publisher), TURBO_OK);
    check_int_eq(turbo_flow_start(subscriber), TURBO_OK);

    check_int_eq(fmq_rulesforge_send_when_ready(publisher, high_value_json), TURBO_OK);
    check_int_eq(fmq_rulesforge_wait_capture(&high_value, 1), TURBO_OK);
    check_int_eq(atomic_load_explicit(&normal.called, memory_order_acquire), 0);
    check_str_eq(high_value.payload, high_value_json);
    check_uint_eq(high_value.match_count, 1u);

    check_int_eq(fmq_rulesforge_send_when_ready(publisher, normal_json), TURBO_OK);
    check_int_eq(fmq_rulesforge_wait_capture(&normal, 1), TURBO_OK);
    check_int_eq(atomic_load_explicit(&high_value.called, memory_order_acquire), 1);
    check_str_eq(normal.payload, normal_json);
    check_uint_eq(normal.match_count, 0u);

    check_int_eq(turbo_flow_stop(subscriber), TURBO_OK);
    check_int_eq(turbo_flow_fmq_app_stop(publisher), TURBO_OK);
    turbo_flow_destroy(subscriber);
    turbo_flow_fmq_app_destroy(publisher);
    check_int_eq(ruleforge_kb_destroy(knowledge_base), RULES_FORGE_OK);
    check_int_eq(ruleforge_cleanup(), RULES_FORGE_OK);
    check_int_eq(tt_remove_file(schema_path), 0);
    free(schema_path);
  }
}
