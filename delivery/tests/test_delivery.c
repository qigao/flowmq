#include "flowmq_delivery.h"
#include "flowmq_socket.h"
#include <core/error.h>
#include <tinytest.h>
#include <stdlib.h>
#include <string.h>

static orm_runtime_t *runtime;
static flowmq_delivery_store *sender, *receiver;
static orm_error_t error;
static flowmq_delivery_config outbox, inbox;
static char *sender_path, *receiver_path;
static flowmq_delivery_item sent, received;
static flowmq_ctx_t *ctx;
static flowmq_socket_t *left, *right;

static void ok(orm_status_t rc) {
  if (rc != ORM_STATUS_OK) fprintf(stderr, "ORM %u: %s\n", (unsigned)rc, error.message);
  check_equal(rc, ORM_STATUS_OK);
}

static void reopen(void) {
  flowmq_delivery_close(sender);
  flowmq_delivery_close(receiver);
  sender = receiver = NULL;
  ok(flowmq_delivery_open(runtime, &outbox, 0, &sender, &error));
  ok(flowmq_delivery_open(runtime, &inbox, 0, &receiver, &error));
}

static size_t transfer(flowmq_socket_t *from, flowmq_socket_t *to,
    const void *data, size_t size, void *output, size_t capacity) {
  int submitted = 0;
  for (int i = 0; i < 3000; ++i) {
    flowmq_pollitem_t poll[] = {{from, FLOWMQ_POLLOUT, 0}, {to, FLOWMQ_POLLIN, 0}};
    size_t ready = 0;
    check_equal(flowmq_poll(poll, 2, 1, &ready), SALTS_OK);
    if (!submitted) {
      int rc = flowmq_send(from, data, size, FLOWMQ_DONTWAIT);
      if (rc == SALTS_OK) submitted = 1;
      else check_true(rc == SALTS_EBUSY || rc == SALTS_ENOBUFS);
    }
    size_t n = 0;
    int rc = flowmq_recv(to, output, capacity, &n, FLOWMQ_DONTWAIT);
    if (rc == SALTS_OK) return n;
    check_equal(rc, SALTS_EBUSY);
  }
  check_true(0);
  return 0;
}

spec("durable socket delivery") {
  before_each() {
    orm_runtime_config_t config;
    orm_runtime_config_init(&config);
    orm_error_init(&error);
    ok(orm_runtime_create(&config, &runtime, &error));
    orm_driver_load_config_t load = {0};
    load.struct_size = sizeof(load);
    load.abi_version = ORM_RUNTIME_ABI_VERSION;
    load.module_path = orm_view(getenv("ORM_SQLITE_PLUGIN"));
    load.expected_driver_id = orm_view("sqlite");
    ok(orm_runtime_load_driver(runtime, &load, &error));
    sender_path = tt_make_temp_file("flowmq-out", ".db");
    receiver_path = tt_make_temp_file("flowmq-in", ".db");
    check_not_null(sender_path);
    check_not_null(receiver_path);
    outbox = (flowmq_delivery_config){sender_path, 77, FLOWMQ_DELIVERY_OUTBOX, 2, 120};
    inbox = (flowmq_delivery_config){receiver_path, 77, FLOWMQ_DELIVERY_INBOX, 1, 120};
    ok(flowmq_delivery_open(runtime, &outbox, 1, &sender, &error));
    ok(flowmq_delivery_open(runtime, &inbox, 1, &receiver, &error));
  }

  after_each() {
    flowmq_delivery_item_release(&sent);
    flowmq_delivery_item_release(&received);
    flowmq_delivery_close(sender);
    flowmq_delivery_close(receiver);
    sender = receiver = NULL;
    if (left) flowmq_close(left);
    if (right) flowmq_close(right);
    if (ctx) flowmq_ctx_term(ctx);
    left = right = NULL;
    ctx = NULL;
    if (runtime) {
      ok(orm_runtime_close(runtime, &error));
      orm_runtime_release(runtime);
      runtime = NULL;
    }
    if (sender_path) { tt_remove_file(sender_path); free(sender_path); sender_path = NULL; }
    if (receiver_path) { tt_remove_file(receiver_path); free(receiver_path); receiver_path = NULL; }
  }

  it("recovers an unacknowledged message over TCP after consumption and preserves parts") {
    ctx = flowmq_ctx_new();
    left = flowmq_socket(ctx, FLOWMQ_PAIR);
    right = flowmq_socket(ctx, FLOWMQ_PAIR);
    check_equal(flowmq_bind(right, "tcp://127.0.0.1:0"), SALTS_OK);
    char endpoint[128];
    size_t endpoint_size;
    check_equal(flowmq_last_endpoint(right, endpoint, sizeof(endpoint), &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(left, endpoint), SALTS_OK);

    int64_t seq = 0;
    flowmq_delivery_part parts[] = {{"a\0b", 3}, {NULL, 0}, {"end", 3}};
    ok(flowmq_delivery_enqueue(sender, parts, 3, &seq, &error));
    check_equal(seq, INT64_C(1));
    reopen();
    unsigned char wire[120], ack[FLOWMQ_DELIVERY_HEADER];
    ok(flowmq_delivery_peek(sender, &sent, &error));
    size_t n = transfer(left, right, sent.frame, sent.size, wire, sizeof(wire));
    ok(flowmq_delivery_accept(receiver, wire, n, ack, &error));
    reopen(); /* drop the first ACK */
    ok(flowmq_delivery_peek(receiver, &received, &error));
    check_equal(received.part_count, 3u);
    check_equal(received.parts[0].size, 3u);
    check_equal(received.parts[0].data, parts[0].data, 3);
    check_equal(received.parts[1].size, 0u);
    ok(flowmq_delivery_consume(receiver, seq, &error));
    flowmq_delivery_item_release(&received);
    n = transfer(left, right, sent.frame, sent.size, wire, sizeof(wire));
    ok(flowmq_delivery_accept(receiver, wire, n, ack, &error));
    n = transfer(right, left, ack, sizeof(ack), wire, sizeof(wire));
    ok(flowmq_delivery_settle(sender, wire, n, &error));
    ok(flowmq_delivery_settle(sender, wire, n, &error));
    flowmq_delivery_item_release(&sent);
    reopen();
    ok(flowmq_delivery_peek(receiver, &received, &error));
    ok(flowmq_delivery_peek(sender, &sent, &error));
    check_null(received.frame);
    check_null(sent.frame);
  }

  it("rejects gaps and full queues while allowing duplicates at capacity") {
    int64_t seq = 0;
    flowmq_delivery_part part = {"x", 1};
    unsigned char ack[FLOWMQ_DELIVERY_HEADER];
    ok(flowmq_delivery_enqueue(sender, &part, 1, &seq, &error));
    ok(flowmq_delivery_enqueue(sender, &part, 1, &seq, &error));
    check_equal(flowmq_delivery_enqueue(sender, &part, 1, &seq, &error), ORM_STATUS_LIMIT_EXCEEDED);
    ok(flowmq_delivery_peek(sender, &sent, &error));
    sent.frame[19] = 2;
    check_equal(flowmq_delivery_accept(receiver, sent.frame, sent.size, ack, &error), ORM_STATUS_INVALID_STATE);
    sent.frame[19] = 1;
    ok(flowmq_delivery_accept(receiver, sent.frame, sent.size, ack, &error));
    ok(flowmq_delivery_accept(receiver, sent.frame, sent.size, ack, &error));
    ack[11] ^= 1;
    check_equal(flowmq_delivery_settle(sender, ack, sizeof(ack), &error), ORM_STATUS_VALIDATION_ERROR);
    ack[11] ^= 1;
    ok(flowmq_delivery_settle(sender, ack, sizeof(ack), &error));
    flowmq_delivery_item_release(&sent);
    ok(flowmq_delivery_peek(sender, &sent, &error));
    check_equal(sent.sequence, INT64_C(2));
    check_equal(flowmq_delivery_accept(receiver, sent.frame, sent.size, ack, &error), ORM_STATUS_LIMIT_EXCEEDED);
    ok(flowmq_delivery_consume(receiver, 1, &error));
    ok(flowmq_delivery_accept(receiver, sent.frame, sent.size, ack, &error));
  }
}
