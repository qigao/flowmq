#include "flowmq_rpc.h"
#include <atomic/atomic.h>
#include <core/clock.h>
#include <core/error.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <threads/thread_pool.h>
#include <threads/threads.h>
#include <tinytest.h>

static flowmq_ctx_t *ctx;
static flowmq_socket_t *sockets[4];
static flowmq_mdp_client *client;
static flowmq_mdp_worker *worker;
static flowmq_mdp_worker *dependency;
static flowmq_socket_t *dependency_socket;
static flowmq_rpc_engine *dependency_engine;
static flowmq_mdp_broker *broker;
static flowmq_rpc_server *server;
static unsigned calls;
static uint64_t now;

static int allow(void *arg, int worker_side, const void *identity, size_t n, const char *service) {
  (void)arg;
  if (!strcmp(service, "dependency"))
    return n == 1 && *(const char *)identity == (worker_side ? 'd' : 'c');
  return n == 1 && *(const char *)identity == (worker_side ? 'w' : 'c') && !strcmp(service, "calc");
}

static int32_t echo(void *counter, const flowmq_rpc_invocation *request, flowmq_rpc_output *out) {
  ++*(unsigned *)counter;
  if (!request->params) return FLOWMQ_RPC_INVALID_PARAMS;
  out->value = json_clone(request->params);
  return out->value ? 0 : FLOWMQ_RPC_INTERNAL_ERROR;
}
static int32_t closed(void *arg, const flowmq_rpc_invocation *request, flowmq_rpc_output *out) {
  (void)arg;
  (void)request;
  out->message = "Account closed";
  out->value = json_create_int64(7);
  return -32010;
}
static int32_t oversized(void *arg, const flowmq_rpc_invocation *request, flowmq_rpc_output *out) {
  (void)arg;
  (void)request;
  char large[FLOWMQ_MDP_PAYLOAD_MAX];
  memset(large, 's', sizeof(large));
  out->value = json_create_string_n(large, sizeof(large));
  return 0;
}
static const flowmq_rpc_method methods[] = {
    {"echo", echo, &calls}, {"closed", closed, NULL}, {"large", oversized, NULL}};

static void start_slots(const char *worker_endpoint, size_t slots) {
  calls = 0;
  now = 1000;
  ctx = flowmq_ctx_new();
  for (size_t i = 0; i < 4; ++i) {
    sockets[i] = flowmq_socket(ctx, i < 2 ? FLOWMQ_ROUTER : FLOWMQ_DEALER);
    check(sockets[i] != NULL);
  }
  for (size_t i = 0; i < 2; ++i) {
    char endpoint[128];
    size_t len = 0;
    check_equal(flowmq_bind(sockets[i], i ? worker_endpoint : "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(sockets[i], endpoint, sizeof(endpoint), &len), SALTS_OK);
    check_equal(flowmq_setsockopt(sockets[i + 2], FLOWMQ_IDENTITY, i ? "w" : "c", 1), SALTS_OK);
    check_equal(flowmq_connect(sockets[i + 2], endpoint), SALTS_OK);
  }
  const flowmq_mdp_broker_config cfg = {101, 8, 2, 8, 100, allow, NULL};
  check_equal(flowmq_mdp_broker_open(sockets[0], sockets[1], &cfg, &broker), SALTS_OK);
  check_equal(flowmq_mdp_client_open(sockets[2], 201, 8, &client), SALTS_OK);
  check_equal(flowmq_mdp_worker_open(sockets[3], "calc", 301, slots, 10, &worker), SALTS_OK);
  check_equal(flowmq_rpc_server_open(methods, 3, &server), SALTS_OK);
}
static void start(const char *worker_endpoint) { start_slots(worker_endpoint, 1); }
static void stop(void) {
  if (dependency) {
    check_equal(flowmq_rpc_engine_close(dependency_engine), SALTS_OK);
    flowmq_mdp_worker_retire(dependency);
    check_equal(flowmq_mdp_worker_close(dependency), SALTS_OK);
    check_equal(flowmq_close(dependency_socket), SALTS_OK);
    dependency = NULL;
    dependency_engine = NULL;
    dependency_socket = NULL;
  }
  flowmq_rpc_server_close(server);
  flowmq_mdp_worker_retire(worker);
  check_equal(flowmq_mdp_worker_close(worker), SALTS_OK);
  flowmq_mdp_client_close(client);
  flowmq_mdp_broker_close(broker);
  for (size_t i = 0; i < 4; ++i)
    check_equal(flowmq_close(sockets[i]), SALTS_OK);
  check_equal(flowmq_ctx_term(ctx), SALTS_OK);
}
static void step(void) {
  flowmq_pollitem_t items[5];
  size_t ready = 0;
  for (size_t i = 0; i < 4; ++i)
    items[i] = (flowmq_pollitem_t){sockets[i], FLOWMQ_POLLIN, 0};
  if (dependency) items[4] = (flowmq_pollitem_t){dependency_socket, FLOWMQ_POLLIN, 0};
  check_equal(flowmq_poll(items, dependency ? 5 : 4, 1, &ready), SALTS_OK);
  check_equal(flowmq_mdp_client_step(client, now), SALTS_OK);
  check_equal(flowmq_mdp_worker_step(worker, now), SALTS_OK);
  if (dependency) check_equal(flowmq_mdp_worker_step(dependency, now), SALTS_OK);
  check_equal(flowmq_mdp_broker_step(broker, now), SALTS_OK);
}
static flowmq_mdp_result receive(flowmq_mdp_id handle, bool execute) {
  flowmq_mdp_result result = {0};
  for (size_t i = 0; i < 2000; ++i) {
    step();
    if (execute) {
      int rc = flowmq_rpc_execute(server, worker);
      check(rc == SALTS_OK || rc == SALTS_EBUSY);
    }
    int rc = flowmq_mdp_take_result(client, handle, &result);
    if (rc == SALTS_OK) return result;
    check_equal(rc, SALTS_EBUSY);
  }
  check(false);
  return result;
}
static flowmq_mdp_result submit_raw(const char *document) {
  flowmq_mdp_body body = {0};
  flowmq_mdp_id handle;
  check_equal(flowmq_mdp_add_part(&body, document, strlen(document)), SALTS_OK);
  check_equal(flowmq_mdp_call(client, "calc", &body, now, 500, &handle), SALTS_OK);
  return receive(handle, true);
}
static json_value_t *raw_response(const char *document) {
  flowmq_mdp_result r = submit_raw(document);
  check_equal(r.outcome, FLOWMQ_MDP_RESULT);
  check_equal(r.status, SALTS_OK);
  check_equal(r.body.count, 1u);
  json_value_t *v = json_parse((const char *)r.body.bytes, r.body.size);
  check(v != NULL);
  return v;
}
static int error_code(const json_value_t *response) {
  return json_get_int(json_object_get(response, "error"), "code", 0);
}
static int on_business_thread(void *unused) {
  (void)unused;
  for (int i = 0; i < 2000; ++i) {
    int rc = flowmq_rpc_execute(server, worker);
    if (rc != SALTS_EBUSY) return rc;
    sleep_ms(1);
  }
  return SALTS_ETIMEDOUT;
}

spec("JSON-RPC over single-node MDP") {
  before_each() { start("tcp://127.0.0.1:0"); }
  after_each() { stop(); }
  it("runs the registered method on a business thread and preserves exact IDs and escaped data") {
    json_value_t *id = json_create_uint64(UINT64_MAX);
    const char params[] = "{\"name\":\"a\\u0000b\"}";
    json_value_t *input = json_parse(params, sizeof(params) - 1);
    flowmq_rpc_request req = {"echo", input, id};
    flowmq_mdp_id handle;
    check_equal(flowmq_rpc_call(client, "calc", &req, 1, false, now, 500, &handle), SALTS_OK);
    json_free(input);
    req.params = NULL;
    thrd_t thread;
    check_equal(thrd_create(&thread, on_business_thread, NULL), thrd_success);
    flowmq_mdp_result r = receive(handle, false);
    int rc;
    check_equal(thrd_join(thread, &rc), thrd_success);
    check_equal(rc, SALTS_OK);
    json_value_t *response = NULL;
    check_equal(flowmq_rpc_decode(&r, &req, 1, false, &response), SALTS_OK);
    check_equal(calls, 1u);
    const json_value_t *name = json_object_get(json_object_get(response, "result"), "name");
    check_equal(json_string_len(name), 3u);
    check_equal(memcmp(json_string(name), "a\0b", 3), 0);
    check_equal(
        strcmp(json_number_text(json_object_get(response, "id"), NULL), "18446744073709551615"), 0);
    json_free(response);
    json_free(id);
  }
  it("returns only batch replies with IDs and releases a single slot after notifications") {
    json_value_t *params = json_parse("[2,3]", 5), *id = json_create_string_n("a\0b", 3),
                 *nil = json_create_null();
    flowmq_rpc_request req[] = {
        {"echo", params, NULL}, {"echo", params, id}, {"closed", NULL, nil}};
    flowmq_mdp_id h;
    for (size_t n = 1; n <= 2; ++n) {
      flowmq_rpc_request notifications[] = {req[0], req[0]};
      check_equal(flowmq_rpc_call(client, "calc", notifications, n, n == 2, now, 500, &h),
                  SALTS_OK);
      flowmq_mdp_result r = receive(h, true);
      json_value_t *response = NULL;
      check_equal(flowmq_rpc_decode(&r, notifications, n, n == 2, &response), SALTS_OK);
      check(response == NULL);
    }
    check_equal(flowmq_rpc_call(client, "calc", req, 3, true, now, 500, &h), SALTS_OK);
    flowmq_mdp_result r = receive(h, true);
    json_value_t *response = NULL;
    check_equal(flowmq_rpc_decode(&r, req, 3, true, &response), SALTS_OK);
    check_equal(json_array_size(response), 2u);
    check_equal(json_array_size(json_object_get(json_array_get(response, 0), "result")), 2u);
    const json_value_t *err = json_object_get(json_array_get(response, 1), "error");
    check_equal(json_get_int(err, "code", 0), -32010);
    check_equal(strcmp(json_get_string(err, "message"), "Account closed"), 0);
    check_equal(json_get_int(err, "data", 0), 7);
    check_equal(calls, 5u);
    json_free(response);
    json_free(id);
    json_free(nil);
    json_free(params);
  }
  it("completes malformed requests and handles each invalid member in a batch") {
    const char *bad[] = {"{", "[]", "42", "{\"method\":\"echo\"}"};
    for (size_t i = 0; i < 4; ++i) {
      json_value_t *r = raw_response(bad[i]);
      check_equal(error_code(r), i ? FLOWMQ_RPC_INVALID_REQUEST : FLOWMQ_RPC_PARSE_ERROR);
      check(json_object_get(r, "id") != NULL && json_is_null(json_object_get(r, "id")));
      json_free(r);
    }
    json_value_t *r = raw_response("[1,{\"jsonrpc\":\"2.0\",\"method\":\"unknown\",\"id\":\"x\"},"
                                   "{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"id\":2}]");
    check_equal(json_array_size(r), 3u);
    check_equal(error_code(json_array_get(r, 0)), FLOWMQ_RPC_INVALID_REQUEST);
    check_equal(error_code(json_array_get(r, 1)), FLOWMQ_RPC_METHOD_NOT_FOUND);
    check_equal(error_code(json_array_get(r, 2)), FLOWMQ_RPC_INVALID_PARAMS);
    json_free(r);
  }
  it("reports oversized output and leaves the worker available") {
    flowmq_mdp_result r = submit_raw("{\"jsonrpc\":\"2.0\",\"method\":\"large\",\"id\":1}");
    check_equal(r.outcome, FLOWMQ_MDP_RESULT);
    check_equal(r.status, SALTS_EMSGSIZE);
    check_equal(r.body.count, 0u);
    json_value_t *v =
        raw_response("{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":[],\"id\":2}");
    check(json_object_get(v, "result") != NULL);
    json_free(v);
  }
  it("validates the method table and rejects invalid calls before dispatch") {
    flowmq_rpc_method duplicate[] = {methods[0], methods[0]};
    flowmq_rpc_server *unused = NULL;
    check_equal(flowmq_rpc_server_open(duplicate, 2, &unused), SALTS_EINVAL);
    json_value_t *id = json_create_int64(3);
    flowmq_rpc_request req[] = {{"echo", NULL, id}, {"closed", NULL, id}};
    flowmq_mdp_id h;
    check_equal(flowmq_rpc_call(client, "calc", req, 2, true, now, 500, &h), SALTS_EINVAL);
    check_equal(flowmq_rpc_call(client, "calc", req, FLOWMQ_RPC_BATCH_MAX + 1, true, now, 500, &h),
                SALTS_EMSGSIZE);
    json_value_t *invalid = json_create_array();
    check(json_array_add_checked(invalid, json_create_string_n("\xff", 1)));
    req[0].params = invalid;
    check_equal(flowmq_rpc_call(client, "calc", req, 1, false, now, 500, &h), SALTS_EINVAL);
    json_free(invalid);
    char bytes[FLOWMQ_MDP_PAYLOAD_MAX];
    memset(bytes, 'm', sizeof(bytes));
    json_value_t *large = json_create_array(), *s = json_create_string_n(bytes, sizeof(bytes));
    check(json_array_add_checked(large, s));
    req[0].params = large;
    check_equal(flowmq_rpc_call(client, "calc", req, 1, false, now, 500, &h), SALTS_EMSGSIZE);
    json_free(large);
    req[0].params = NULL;
    check_equal(flowmq_rpc_call(client, "forbidden", req, 1, false, now, 500, &h), SALTS_OK);
    flowmq_mdp_result r = receive(h, true);
    check_equal(r.outcome, FLOWMQ_MDP_REJECTED);
    check_equal(calls, 0u);
    json_free(id);
  }
  it("rejects an over-limit incoming batch before invoking any member") {
    char document[2048] = "[";
    for (unsigned i = 0; i <= FLOWMQ_RPC_BATCH_MAX; ++i) {
      if (i) strcat(document, ",");
      strcat(document, "{\"jsonrpc\":\"2.0\",\"method\":\"echo\"}");
    }
    strcat(document, "]");
    flowmq_mdp_result r = submit_raw(document);
    check_equal(r.outcome, FLOWMQ_MDP_RESULT);
    check_equal(r.status, SALTS_EMSGSIZE);
    check_equal(r.body.count, 0u);
    check_equal(calls, 0u);
  }
  it("keeps an executed call unknown if its reply misses the client deadline") {
    json_value_t *id = json_create_int64(1);
    flowmq_rpc_request req = {"echo", NULL, id};
    flowmq_mdp_id h;
    check_equal(flowmq_rpc_call(client, "calc", &req, 1, false, now, 100, &h), SALTS_OK);
    int rc = SALTS_EBUSY;
    for (int i = 0; i < 2000 && rc == SALTS_EBUSY; ++i) {
      step();
      rc = flowmq_rpc_execute(server, worker);
    }
    check_equal(rc, SALTS_OK);
    now += 101;
    check_equal(flowmq_mdp_client_step(client, now), SALTS_OK);
    flowmq_mdp_result r;
    check_equal(flowmq_mdp_take_result(client, h, &r), SALTS_OK);
    check_equal(r.outcome, FLOWMQ_MDP_UNKNOWN);
    check_equal(calls, 1u);
    json_free(id);
  }
}

static flowmq_rpc_engine *engine;
static uint64_t clock_now(void *context) {
  (void)context;
  return now;
}
static int32_t classify_order(void *context, const char *name, const flowmq_rpc_invocation *input,
                              flowmq_rpc_work_class *work) {
  (void)context;
  if (strcmp(name, "order")) return 0;
  const json_value_t *key = json_object_get(input->params, "order");
  if (!key || json_type(key) != JSON_STRING) return FLOWMQ_RPC_INVALID_PARAMS;
  work->key = json_string(key);
  work->key_size = json_string_len(key);
  work->ordered = true;
  return 0;
}
static void start_engine(void) {
  start_slots("tcp://127.0.0.1:0", 5);
  static const char *const deferred[] = {"order", "child"};
  const flowmq_rpc_engine_config cfg = {.schedule = {.capacity = 5, .per_key_limit = 2},
                                        .deferred_methods = deferred,
                                        .deferred_count = 2,
                                        .child_capacity = 2,
                                        .children_per_request = 2,
                                        .now_ms = clock_now,
                                        .classify = classify_order};
  check_equal(flowmq_rpc_engine_open(worker, server, client, &cfg, &engine), SALTS_OK);
}
static flowmq_mdp_id send_document(const char *doc, uint32_t budget) {
  flowmq_mdp_body body = {0};
  flowmq_mdp_id id;
  check_equal(flowmq_mdp_add_part(&body, doc, strlen(doc)), SALTS_OK);
  check_equal(flowmq_mdp_call(client, "calc", &body, now, budget, &id), SALTS_OK);
  return id;
}
static void start_dependency(void) {
  char endpoint[128];
  size_t len = 0;
  check_equal(flowmq_last_endpoint(sockets[1], endpoint, sizeof(endpoint), &len), SALTS_OK);
  dependency_socket = flowmq_socket(ctx, FLOWMQ_DEALER);
  check_not_null(dependency_socket);
  check_equal(flowmq_setsockopt(dependency_socket, FLOWMQ_IDENTITY, "d", 1), SALTS_OK);
  check_equal(flowmq_connect(dependency_socket, endpoint), SALTS_OK);
  check_equal(flowmq_mdp_worker_open(dependency_socket, "dependency", 302, 2, 10, &dependency),
              SALTS_OK);
  static const char *const deferred[] = {"child"};
  const flowmq_rpc_engine_config cfg = {.schedule = {.capacity = 2},
                                        .deferred_methods = deferred,
                                        .deferred_count = 1,
                                        .now_ms = clock_now};
  check_equal(flowmq_rpc_engine_open(dependency, NULL, NULL, &cfg, &dependency_engine), SALTS_OK);
}
static flowmq_rpc_event next_event_from(flowmq_rpc_engine *from) {
  flowmq_rpc_event event = {0};
  for (int i = 0; i < 1000; ++i) {
    step();
    int rc = flowmq_rpc_engine_poll(from, &event);
    if (rc == SALTS_OK && event.kind == FLOWMQ_RPC_PROGRESS) continue;
    if (rc == SALTS_OK) return event;
    check_equal(rc, SALTS_EBUSY);
  }
  check(false);
  return event;
}
static flowmq_rpc_event next_event(void) { return next_event_from(engine); }
static flowmq_mdp_result engine_result(flowmq_mdp_id id) {
  flowmq_mdp_result r = {0};
  for (int i = 0; i < 1000; ++i) {
    step();
    flowmq_rpc_event event;
    int progress = flowmq_rpc_engine_poll(engine, &event);
    check(progress == SALTS_EBUSY || (progress == SALTS_OK && event.kind == FLOWMQ_RPC_PROGRESS));
    int rc = flowmq_mdp_take_result(client, id, &r);
    if (rc == SALTS_OK) return r;
    check_equal(rc, SALTS_EBUSY);
  }
  check(false);
  return r;
}
static void finish_number(flowmq_rpc_ticket ticket, int number) {
  check_equal(flowmq_rpc_engine_finish(engine, ticket, 0,
                                       (flowmq_rpc_output){.value = json_create_int64(number)}),
              SALTS_OK);
}

typedef struct pool_work {
  flowmq_rpc_ticket ticket;
  int input, result;
  atomic_int gate, entered, cancelled, done;
} pool_work;

static void compute_on_pool(void *context) {
  pool_work *work = context;
  atomic_store(&work->entered, 1);
  uint64_t start_ms = monotonic_ms();
  while (!atomic_load(&work->gate) && monotonic_ms() - start_ms < 5000)
    sleep_ms(1);
  if (!atomic_load(&work->cancelled)) work->result = work->input * 2;
  atomic_store(&work->done, 1);
}

static void await_flag(atomic_int *flag) {
  for (unsigned i = 0; i < 1000 && !atomic_load(flag); ++i)
    sleep_ms(1);
  check_equal(atomic_load(flag), 1);
}

spec("deferred single-owner JSON-RPC orchestration") {
  before_each() { start_engine(); }
  after_each() {
    check_equal(flowmq_rpc_engine_close(engine), SALTS_OK);
    engine = NULL;
    stop();
  }
  it("keeps batch members sequential while another order and two child calls progress") {
    start_dependency();
    flowmq_mdp_id a = send_document(
        "[{\"jsonrpc\":\"2.0\",\"method\":\"order\",\"params\":{\"order\":\"A\"},\"id\":1},"
        "{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":[]},"
        "{\"jsonrpc\":\"2.0\",\"method\":\"closed\",\"id\":2}]",
        500);
    flowmq_rpc_event parent = next_event();
    uint64_t deadline = 0;
    check_equal(flowmq_rpc_engine_next_deadline(engine, &deadline), SALTS_OK);
    check_equal(deadline, parent.deadline_ms);
    check_equal(parent.kind, FLOWMQ_RPC_START);
    check_equal(strcmp(parent.method, "order"), 0);
    check_equal(calls, 0u);
    json_value_t *one = json_create_int64(11), *two = json_create_int64(12);
    flowmq_rpc_branch children[] = {{"dependency", {"child", NULL, one}, 10},
                                    {"dependency", {NULL, NULL, two}, 20}};
    check_equal(flowmq_rpc_engine_fork(engine, parent.ticket, children, 2), SALTS_EINVAL);
    children[1].request.method = "child";
    check_equal(flowmq_rpc_engine_fork(engine, parent.ticket, children, 2), SALTS_OK);
    check_equal(flowmq_rpc_engine_fork(engine, parent.ticket, children, 1), SALTS_ENOBUFS);
    flowmq_rpc_event x = next_event_from(dependency_engine), y = next_event_from(dependency_engine);
    check_equal(x.kind, FLOWMQ_RPC_START);
    check_equal(y.kind, FLOWMQ_RPC_START);
    check_equal(strcmp(x.method, "child"), 0);
    check_equal(strcmp(y.method, "child"), 0);
    if (json_number(x.input.id) == 12) {
      flowmq_rpc_event tmp = x;
      x = y;
      y = tmp;
    }
    check_equal(json_number(x.input.id), 11.);
    check_equal(json_number(y.input.id), 12.);

    flowmq_mdp_id b = send_document(
        "{\"jsonrpc\":\"2.0\",\"method\":\"order\",\"params\":{\"order\":\"B\"},\"id\":3}", 500);
    flowmq_rpc_event other = next_event();
    check_equal(strcmp(json_get_string(other.input.params, "order"), "B"), 0);
    finish_number(other.ticket, 6);
    check_equal(engine_result(b).outcome, FLOWMQ_MDP_RESULT);
    check_equal(flowmq_rpc_engine_finish(engine, other.ticket, 0, (flowmq_rpc_output){0}),
                SALTS_ENOENT);
    check_equal(flowmq_rpc_engine_finish(dependency_engine, y.ticket, 0,
                                         (flowmq_rpc_output){.value = json_create_int64(8)}),
                SALTS_OK);
    flowmq_rpc_event second = next_event();
    check_equal(second.kind, FLOWMQ_RPC_CHILD);
    check_equal(second.child_tag, 20u);
    json_value_t *reply = NULL;
    check_equal(flowmq_rpc_decode(&second.child, &children[1].request, 1, false, &reply), SALTS_OK);
    check_equal(json_get_int(reply, "result", -1), 8);
    json_free(reply);
    check_equal(calls, 0u);
    check_equal(flowmq_rpc_engine_finish(dependency_engine, x.ticket, 0,
                                         (flowmq_rpc_output){.value = json_create_int64(3)}),
                SALTS_OK);
    flowmq_rpc_event first = next_event();
    check_equal(first.kind, FLOWMQ_RPC_CHILD);
    check_equal(first.child_tag, 10u);
    check_equal(first.ticket.generation, parent.ticket.generation);
    check_equal(flowmq_rpc_decode(&first.child, &children[0].request, 1, false, &reply), SALTS_OK);
    check_equal(json_get_int(reply, "result", -1), 3);
    json_free(reply);
    finish_number(first.ticket, 11);
    flowmq_mdp_result result = engine_result(a);
    check_equal(result.outcome, FLOWMQ_MDP_RESULT);
    check_equal(result.status, SALTS_OK);
    json_value_t *doc = json_parse((const char *)result.body.bytes, result.body.size);
    check_equal(json_array_size(doc), 2u);
    check_equal(json_get_int(json_array_get(doc, 0), "result", -1), 11);
    check_equal(error_code(json_array_get(doc, 1)), -32010);
    check_equal(calls, 1u);
    json_free(doc);
    json_free(one);
    json_free(two);
  }
  it("holds the business key until cancellation is acknowledged and rejects stale completions") {
    const char *doc =
        "{\"jsonrpc\":\"2.0\",\"method\":\"order\",\"params\":{\"order\":\"same\"},\"id\":5}";
    flowmq_mdp_id a = send_document(doc, 40);
    flowmq_rpc_event first = next_event();
    flowmq_mdp_id b = send_document(doc, 500);
    for (int i = 0; i < 20; ++i) {
      step();
      flowmq_rpc_event ignored;
      int progress = flowmq_rpc_engine_poll(engine, &ignored);
      check(progress == SALTS_EBUSY ||
            (progress == SALTS_OK && ignored.kind == FLOWMQ_RPC_PROGRESS));
    }
    now = first.deadline_ms;
    flowmq_rpc_event cancel = next_event();
    check_equal(cancel.kind, FLOWMQ_RPC_CANCEL);
    check_equal(cancel.cancel_status, SALTS_ETIMEDOUT);
    check_equal(cancel.ticket.generation, first.ticket.generation);
    flowmq_rpc_event pending;
    check_equal(flowmq_rpc_engine_poll(engine, &pending), SALTS_EBUSY);
    check_equal(flowmq_rpc_engine_close(engine), SALTS_EBUSY);
    finish_number(cancel.ticket, 123);
    flowmq_rpc_event next = next_event();
    check_equal(next.kind, FLOWMQ_RPC_START);
    check(next.ticket.generation != cancel.ticket.generation);
    check_equal(flowmq_rpc_engine_finish(engine, first.ticket, 0, (flowmq_rpc_output){0}),
                SALTS_ENOENT);
    finish_number(next.ticket, 13);
    check_equal(engine_result(a).outcome, FLOWMQ_MDP_UNKNOWN);
    check_equal(engine_result(b).outcome, FLOWMQ_MDP_RESULT);
  }
  it("propagates a rejected branch and discards a late child reply after early parent settlement") {
    start_dependency();
    flowmq_mdp_id a = send_document("{\"jsonrpc\":\"2.0\",\"method\":\"child\",\"id\":1}", 500);
    flowmq_rpc_event parent = next_event();
    json_value_t *id = json_create_int64(4);
    flowmq_rpc_branch branches[] = {{"dependency", {"child", NULL, id}, 1},
                                    {"forbidden", {"child", NULL, id}, 2}};
    check_equal(flowmq_rpc_engine_fork(engine, parent.ticket, branches, 2), SALTS_OK);
    flowmq_rpc_event running = next_event_from(dependency_engine);
    flowmq_rpc_event rejected = next_event();
    check_equal(rejected.kind, FLOWMQ_RPC_CHILD);
    check_equal(rejected.child_tag, 2u);
    check_equal(rejected.child.outcome, FLOWMQ_MDP_REJECTED);
    check_equal(rejected.child.status, SALTS_EPERM);
    check_equal(flowmq_rpc_engine_finish(engine, parent.ticket, -32014,
                                         (flowmq_rpc_output){.message = "Dependency denied"}),
                SALTS_OK);
    flowmq_mdp_result result = engine_result(a);
    json_value_t *response = json_parse((const char *)result.body.bytes, result.body.size);
    check_equal(error_code(response), -32014);
    json_free(response);

    flowmq_mdp_id b = send_document("{\"jsonrpc\":\"2.0\",\"method\":\"child\",\"id\":2}", 500);
    flowmq_rpc_event next = next_event();
    check_equal(parent.ticket.slot, next.ticket.slot);
    check(next.ticket.generation != parent.ticket.generation);
    check_equal(flowmq_rpc_engine_finish(dependency_engine, running.ticket, 0,
                                         (flowmq_rpc_output){.value = json_create_int64(8)}),
                SALTS_OK);
    for (int i = 0; i < 25; ++i) {
      step();
      flowmq_rpc_event none;
      check_equal(flowmq_rpc_engine_poll(engine, &none), SALTS_EBUSY);
    }
    finish_number(next.ticket, 9);
    check_equal(engine_result(b).outcome, FLOWMQ_MDP_RESULT);
    json_free(id);
  }
  it("counts worker queue time against the deadline and drains notifications during shutdown") {
    flowmq_mdp_id a = send_document("{\"jsonrpc\":\"2.0\",\"method\":\"child\",\"id\":1}", 20);
    for (int i = 0; i < 40; ++i)
      step();
    now += 21;
    check_equal(engine_result(a).outcome, FLOWMQ_MDP_UNKNOWN);
    flowmq_mdp_id b = send_document("{\"jsonrpc\":\"2.0\",\"method\":\"child\"}", 500);
    flowmq_rpc_event start = next_event();
    check_equal(start.kind, FLOWMQ_RPC_START);
    check(start.input.id == NULL);
    flowmq_rpc_engine_stop(engine);
    flowmq_rpc_event abort = next_event();
    check_equal(abort.kind, FLOWMQ_RPC_CANCEL);
    check_equal(abort.cancel_status, SALTS_ESHUTDOWN);
    check_equal(flowmq_rpc_engine_finish(engine, abort.ticket, 0, (flowmq_rpc_output){0}),
                SALTS_OK);
    flowmq_mdp_result result = engine_result(b);
    check_equal(result.status, SALTS_ESHUTDOWN);
    check_equal(result.body.count, 0u);
  }
  it("observes worker retirement with all records held and preserves them until acknowledgement") {
    flowmq_rpc_ticket tickets[5];
    for (size_t i = 0; i < 5; ++i) {
      (void)send_document("{\"jsonrpc\":\"2.0\",\"method\":\"child\",\"id\":1}", 200);
      tickets[i] = next_event().ticket;
    }
    flowmq_mdp_worker_retire(worker);
    flowmq_rpc_branch branch = {"dependency", {"child", NULL, NULL}, 1};
    check_equal(flowmq_rpc_engine_fork(engine, tickets[0], &branch, 1), SALTS_ESHUTDOWN);
    for (size_t i = 0; i < 5; ++i) {
      flowmq_rpc_event cancel;
      check_equal(flowmq_rpc_engine_poll(engine, &cancel), SALTS_OK);
      check_equal(cancel.kind, FLOWMQ_RPC_CANCEL);
      check_equal(cancel.cancel_status, SALTS_ESHUTDOWN);
      check_equal(flowmq_rpc_engine_close(engine), SALTS_EBUSY);
      check_equal(flowmq_rpc_engine_finish(engine, cancel.ticket, 0, (flowmq_rpc_output){0}),
                  SALTS_OK);
    }
  }
  it("retains tickets across pool saturation while the owner handles cancellation and I/O") {
    const threadpool_config_t limits = {.num_threads = 1, .queue_capacity = 1};
    threadpool_t *pool = threadpool_create_with_config(&limits);
    check_not_null(pool);
    const char *doc = "{\"jsonrpc\":\"2.0\",\"method\":\"child\",\"params\":[21],\"id\":1}";
    flowmq_mdp_id ids[3];
    pool_work jobs[3] = {0};
    for (size_t i = 0; i < 3; ++i) {
      ids[i] = send_document(doc, i == 1 ? 40 : 500);
      flowmq_rpc_event event = next_event();
      jobs[i].ticket = event.ticket;
      jobs[i].input = (int)json_number(json_array_get(event.input.params, 0));
      if (i) atomic_store(&jobs[i].gate, 1);
      int rc = threadpool_try_submit(pool, compute_on_pool, &jobs[i]);
      check_equal(rc, i == 2 ? SALTS_ENOBUFS : SALTS_OK);
      if (!i) await_flag(&jobs[i].entered);
    }
    /* A is running, B is queued and C is still owned by the application. */
    check_equal(atomic_load(&jobs[2].entered), 0);
    now += 41;
    flowmq_rpc_event cancelled = next_event();
    check_equal(cancelled.kind, FLOWMQ_RPC_CANCEL);
    check_equal(cancelled.ticket.generation, jobs[1].ticket.generation);
    atomic_store(&jobs[1].cancelled, 1);
    atomic_store(&jobs[0].gate, 1);
    await_flag(&jobs[0].done);
    await_flag(&jobs[1].done);
    finish_number(jobs[0].ticket, jobs[0].result);
    check_equal(flowmq_rpc_engine_finish(engine, cancelled.ticket, 0, (flowmq_rpc_output){0}),
                SALTS_OK);
    check_equal(jobs[1].result, 0);
    check_equal(threadpool_try_submit(pool, compute_on_pool, &jobs[2]), SALTS_OK);
    await_flag(&jobs[2].done);
    finish_number(jobs[2].ticket, jobs[2].result);
    check_equal(threadpool_shutdown_with_policy(pool, SALTS_THREADPOOL_SHUTDOWN_DRAIN), SALTS_OK);
    check_equal(threadpool_wait_status(pool), SALTS_OK);
    threadpool_destroy(pool);
    check_equal(engine_result(ids[0]).outcome, FLOWMQ_MDP_RESULT);
    check_equal(engine_result(ids[1]).outcome, FLOWMQ_MDP_UNKNOWN);
    flowmq_mdp_result last = engine_result(ids[2]);
    json_value_t *reply = json_parse((const char *)last.body.bytes, last.body.size);
    check_equal(json_get_int(reply, "result", 0), 42);
    json_free(reply);
    flowmq_mdp_id next = send_document(doc, 500);
    flowmq_rpc_event final = next_event();
    flowmq_rpc_engine_drain(engine);
    finish_number(final.ticket, 7);
    check_equal(engine_result(next).status, SALTS_OK);
    flowmq_mdp_id after_drain = send_document(doc, 500);
    check_equal(engine_result(after_drain).status, SALTS_ESHUTDOWN);
  }
}

spec("local JSON-RPC worker") {
  it("routes a TCP client's method call to a worker over native IPC") {
    char *directory = tt_make_temp_dir("fmqrpc");
    check_not_null(directory);
    char endpoint[256];
#ifdef _WIN32
    const char *name = directory;
    for (const char *p = directory; *p; ++p)
      if (*p == '/' || *p == '\\') name = p + 1;
    int n = snprintf(endpoint, sizeof(endpoint), "ipc://%s-worker", name);
#else
    int n = snprintf(endpoint, sizeof(endpoint), "ipc://%s/worker", directory);
#endif
    check(n > 0 && (size_t)n < sizeof(endpoint));
    start(endpoint);
    json_value_t *r =
        raw_response("{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":[42],\"id\":1}");
    const json_value_t *value = json_array_get(json_object_get(r, "result"), 0);
    check(value && json_number(value) == 42);
    check_equal(calls, 1u);
    json_free(r);
    stop();
    check_equal(tt_remove_tree(directory), 0);
    free(directory);
  }
}

spec("client JSON-RPC correlation") {
  it("accepts reordered results and rejects duplicates, missing IDs and malformed errors") {
    json_value_t *a = json_create_int64(1), *b = json_create_int64(2);
    flowmq_rpc_request req[] = {{"a", NULL, a}, {"b", NULL, b}};
    const char *docs[] = {"[{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":null},{\"jsonrpc\":\"2.0\","
                          "\"id\":1,\"result\":true}]",
                          "[{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":null},{\"jsonrpc\":\"2.0\","
                          "\"id\":2,\"result\":true}]",
                          "[{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":true}]",
                          "[{\"jsonrpc\":\"2.0\",\"id\":2,\"error\":{}},{\"jsonrpc\":\"2.0\","
                          "\"id\":1,\"result\":true}]"};
    for (size_t i = 0; i < 4; ++i) {
      flowmq_mdp_result r = {.outcome = FLOWMQ_MDP_RESULT};
      check_equal(flowmq_mdp_add_part(&r.body, docs[i], strlen(docs[i])), SALTS_OK);
      json_value_t *v = NULL;
      check_equal(flowmq_rpc_decode(&r, req, 2, true, &v), i ? SALTS_EPROTO : SALTS_OK);
      json_free(v);
    }
    json_free(a);
    json_free(b);
  }
}
