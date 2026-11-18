#include "flowmq_mdp.h"
#include <core/clock.h>
#include <core/error.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <threads/threads.h>
#include <tinytest.h>

static flowmq_ctx_t *ctx;
static flowmq_socket_t *sockets[5];
static flowmq_mdp_client *client;
static flowmq_mdp_worker *worker;
static flowmq_mdp_worker *second_worker;
static flowmq_mdp_broker *broker;
static uint64_t now;
static flowmq_mdp_body body;

static int allow(void *arg, int is_worker, const void *id, size_t n, const char *service) {
  (void)arg;
  if (n != 1) return 0;
  char peer = *(const char *)id;
  return (is_worker ? (peer == 'w' || peer == 'v') : peer == 'c') &&
         (!strcmp(service, "echo") || (!is_worker && !strcmp(service, "offline")));
}

static void attach(flowmq_socket_t *router, flowmq_socket_t *dealer, const char *id) {
  char endpoint[128];
  size_t size = 0;
  check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint), &size), SALTS_OK);
  check_equal(flowmq_setsockopt(dealer, FLOWMQ_IDENTITY, id, 1), SALTS_OK);
  check_equal(flowmq_connect(dealer, endpoint), SALTS_OK);
}

static void step(int run_worker) {
  size_t count = sockets[4] ? 5 : 4;
  flowmq_pollitem_t items[5];
  for (size_t i = 0; i < count; ++i)
    items[i] = (flowmq_pollitem_t){sockets[i], FLOWMQ_POLLIN, 0};
  size_t ready = 0;
  check_equal(flowmq_poll(items, count, 1, &ready), SALTS_OK);
  check_equal(flowmq_mdp_client_step(client, now), SALTS_OK);
  if (run_worker) check_equal(flowmq_mdp_worker_step(worker, now), SALTS_OK);
  if (second_worker) check_equal(flowmq_mdp_worker_step(second_worker, now), SALTS_OK);
  check_equal(flowmq_mdp_broker_step(broker, now), SALTS_OK);
}
static flowmq_mdp_task task(void) {
  flowmq_mdp_task t = {0};
  for (size_t i = 0; i < 2000; ++i) {
    step(1);
    int rc = flowmq_mdp_take_task(worker, &t);
    if (rc == SALTS_OK) return t;
    check_equal(rc, SALTS_EBUSY);
  }
  check_true(0);
  return t;
}
static flowmq_mdp_result result(flowmq_mdp_id id, int run_worker) {
  flowmq_mdp_result r = {0};
  for (size_t i = 0; i < 2000; ++i) {
    step(run_worker);
    int rc = flowmq_mdp_take_result(client, id, &r);
    if (rc == SALTS_OK) return r;
    check_equal(rc, SALTS_EBUSY);
  }
  check_true(0);
  return r;
}
static flowmq_mdp_id call(const char *service, uint32_t budget) {
  flowmq_mdp_id id = {0};
  check_equal(flowmq_mdp_call(client, service, &body, now, budget, &id), SALTS_OK);
  return id;
}

static int execute_echo(void *context) {
  flowmq_mdp_worker *target = context;
  flowmq_mdp_task item;
  for (int i = 0; i < 300; ++i) {
    int rc = flowmq_mdp_take_task(target, &item);
    if (rc == SALTS_OK) return flowmq_mdp_complete(target, &item, 17, &item.body);
    if (rc != SALTS_EBUSY) return rc;
    sleep_ms(1);
  }
  return SALTS_ETIMEDOUT;
}

static void open_nodes(const char *front, const char *back) {
  now = 1000;
  ctx = flowmq_ctx_new();
  for (size_t i = 0; i < 4; ++i)
    sockets[i] = flowmq_socket(ctx, i < 2 ? FLOWMQ_ROUTER : FLOWMQ_DEALER);
  check_equal(flowmq_bind(sockets[0], front), SALTS_OK);
  check_equal(flowmq_bind(sockets[1], back), SALTS_OK);
  attach(sockets[0], sockets[2], "c");
  attach(sockets[1], sockets[3], "w");
  flowmq_mdp_broker_config config = {101, 5, 2, 3, 100, allow, NULL};
  check_equal(flowmq_mdp_broker_open(sockets[0], sockets[1], &config, &broker), SALTS_OK);
  check_equal(flowmq_mdp_client_open(sockets[2], 201, 5, &client), SALTS_OK);
  check_equal(flowmq_mdp_worker_open(sockets[3], "echo", 301, 2, 10, &worker), SALTS_OK);
  body = (flowmq_mdp_body){0};
  check_equal(flowmq_mdp_add_part(&body, "a\0b", 3), SALTS_OK);
  check_equal(flowmq_mdp_add_part(&body, NULL, 0), SALTS_OK);
}
static void close_nodes(void) {
  if (second_worker) {
    flowmq_mdp_worker_retire(second_worker);
    check_equal(flowmq_mdp_worker_close(second_worker), SALTS_OK);
    second_worker = NULL;
  }
  flowmq_mdp_worker_retire(worker);
  if (worker) check_equal(flowmq_mdp_worker_close(worker), SALTS_OK);
  worker = NULL;
  flowmq_mdp_client_close(client);
  client = NULL;
  flowmq_mdp_broker_close(broker);
  broker = NULL;
  for (size_t i = 0; i < 5; ++i)
    if (sockets[i]) {
      flowmq_close(sockets[i]);
      sockets[i] = NULL;
    }
  if (ctx) flowmq_ctx_term(ctx);
  ctx = NULL;
}

static void configure_policy(const flowmq_mdp_service_policy *policy) {
  /* This fixture has not driven either socket yet. */
  flowmq_mdp_broker_close(broker);
  const flowmq_mdp_broker_config config = {101, 8, 2, 5, 100, allow, NULL};
  check_equal(flowmq_mdp_broker_open(sockets[0], sockets[1], &config, &broker), SALTS_OK);
  check_equal(flowmq_mdp_broker_set_policies(broker, policy, 1), SALTS_OK);
}

static void register_second(size_t slots) {
  sockets[4] = flowmq_socket(ctx, FLOWMQ_DEALER);
  attach(sockets[1], sockets[4], "v");
  check_equal(flowmq_mdp_worker_open(sockets[4], "echo", 302, slots, 10, &second_worker), SALTS_OK);
}

static flowmq_mdp_worker *assigned_worker(flowmq_mdp_task *job) {
  for (size_t i = 0; i < 1000; ++i) {
    step(1);
    int rc = flowmq_mdp_take_task(worker, job);
    if (rc == SALTS_OK) return worker;
    check_equal(rc, SALTS_EBUSY);
    rc = flowmq_mdp_take_task(second_worker, job);
    if (rc == SALTS_OK) return second_worker;
    check_equal(rc, SALTS_EBUSY);
  }
  check(false);
  return NULL;
}

static int cache_key(void *arg, const flowmq_mdp_body *input, uint64_t *point) {
  (void)arg;
  if (!input->size) return SALTS_EINVAL;
  *point = (uint64_t)input->bytes[0] << 56;
  return SALTS_OK;
}

spec("single-node MDP over TCP") {
  before_each() { open_nodes("tcp://127.0.0.1:0", "tcp://127.0.0.1:0"); }
  after_each() { close_nodes(); }
  it("selects by occupied fraction, preserving credit until a matching completion") {
    const flowmq_mdp_service_policy placement = {"echo", FLOWMQ_MDP_LEAST_LOAD};
    configure_policy(&placement);
    register_second(8);
    for (size_t i = 0; i < 30; ++i)
      step(1);
    check_equal(flowmq_mdp_broker_set_policies(broker, &placement, 1), SALTS_EBUSY);
    flowmq_mdp_task jobs[4];
    flowmq_mdp_worker *owners[4];
    flowmq_mdp_id ids[4];
    size_t small = 0;
    for (size_t i = 0; i < 4; ++i) {
      ids[i] = call("echo", 500);
      owners[i] = assigned_worker(&jobs[i]);
      if (owners[i] == worker) ++small;
      if (i == 2) check_equal(small, 1u);
    }
    check(owners[3] == second_worker);
    for (size_t i = 0; i < 4; ++i) {
      check_equal(flowmq_mdp_complete(owners[i], &jobs[i], 0, &body), SALTS_OK);
      check_equal(result(ids[i], 1).outcome, FLOWMQ_MDP_RESULT);
    }
  }
  it("pins equal cache keys until their preferred peer reaches the policy bound") {
    const flowmq_mdp_service_policy placement = {"echo", FLOWMQ_MDP_CACHE_RING, 50, cache_key};
    configure_policy(&placement);
    register_second(2);
    for (size_t i = 0; i < 30; ++i)
      step(1);
    flowmq_mdp_task first, next;
    flowmq_mdp_id id = call("echo", 500);
    flowmq_mdp_worker *preferred = assigned_worker(&first);
    check_equal(flowmq_mdp_complete(preferred, &first, 0, &body), SALTS_OK);
    check_equal(result(id, 1).outcome, FLOWMQ_MDP_RESULT);
    id = call("echo", 500);
    check(assigned_worker(&first) == preferred);
    flowmq_mdp_id other_id = call("echo", 500);
    flowmq_mdp_worker *other = assigned_worker(&next);
    check(other != preferred);
    check_equal(flowmq_mdp_complete(preferred, &first, 0, &body), SALTS_OK);
    check_equal(flowmq_mdp_complete(other, &next, 0, &body), SALTS_OK);
    check_equal(result(id, 1).outcome, FLOWMQ_MDP_RESULT);
    check_equal(result(other_id, 1).outcome, FLOWMQ_MDP_RESULT);
    body = (flowmq_mdp_body){0};
    flowmq_mdp_result rejected = result(call("echo", 500), 1);
    check_equal(rejected.outcome, FLOWMQ_MDP_REJECTED);
    check_equal(rejected.status, SALTS_EINVAL);
  }
  it("denies a second exclusive worker and pauses admission after owner loss") {
    const flowmq_mdp_service_policy placement = {"echo", FLOWMQ_MDP_EXCLUSIVE};
    configure_policy(&placement);
    for (int i = 0; i < 30; ++i)
      step(1);
    register_second(2);
    int registration = SALTS_OK;
    flowmq_mdp_worker *contender = second_worker;
    second_worker = NULL;
    for (int i = 0; i < 1000 && registration == SALTS_OK; ++i) {
      step(1);
      registration = flowmq_mdp_worker_step(contender, now);
    }
    check_equal(registration, SALTS_ENOBUFS);
    check_equal(flowmq_mdp_worker_close(contender), SALTS_OK);
    now += 101;
    for (int i = 0; i < 5; ++i)
      step(0);
    flowmq_mdp_result r = result(call("echo", 500), 0);
    check_equal(r.outcome, FLOWMQ_MDP_REJECTED);
    check_equal(r.status, SALTS_ENOTCONN);
    flowmq_mdp_worker_retire(worker);
    check_equal(flowmq_mdp_worker_close(worker), SALTS_OK);
    worker = NULL;
    check_equal(flowmq_mdp_broker_resume_service(broker, "echo"), SALTS_OK);
    check_equal(flowmq_mdp_broker_resume_service(broker, "echo"), SALTS_EBUSY);
    check_equal(flowmq_mdp_worker_open(sockets[4], "echo", 303, 1, 10, &worker), SALTS_OK);
    flowmq_mdp_id fresh = call("echo", 500);
    flowmq_mdp_task job = task();
    check_equal(job.worker, UINT64_C(303));
    check_equal(flowmq_mdp_complete(worker, &job, 0, &body), SALTS_OK);
    check_equal(result(fresh, 1).outcome, FLOWMQ_MDP_RESULT);
  }
  it("takes older ready work before a newly dispatched request in a reused slot") {
    flowmq_mdp_id a = call("echo", 1000);
    flowmq_mdp_task first = task();
    flowmq_mdp_id b = call("echo", 1000);
    for (int i = 0; i < 30; ++i)
      step(1);
    check_equal(flowmq_mdp_complete(worker, &first, 0, &body), SALTS_OK);
    check_equal(result(a, 1).outcome, FLOWMQ_MDP_RESULT);
    flowmq_mdp_id c = call("echo", 1000);
    for (int i = 0; i < 30; ++i)
      step(1);
    flowmq_mdp_task older = task(), newer = task();
    check_equal(older.id.sequence, b.sequence);
    check_equal(newer.id.sequence, c.sequence);
    check_equal(newer.slot, first.slot);
    check_equal(flowmq_mdp_complete(worker, &newer, 0, &body), SALTS_OK);
    check_equal(flowmq_mdp_complete(worker, &older, 0, &body), SALTS_OK);
    check_equal(result(b, 1).outcome, FLOWMQ_MDP_RESULT);
    check_equal(result(c, 1).outcome, FLOWMQ_MDP_RESULT);
  }
  it("correlates out-of-order completions, empty parts and long tasks") {
    flowmq_mdp_id a = call("echo", 1000), b = call("echo", 1000);
    flowmq_mdp_task x = task(), y = task();
    check_not_equal(x.slot, y.slot);
    check_equal(x.body.count, 2u);
    check_equal(x.body.lengths[1], 0u);
    for (size_t i = 0; i < 30; ++i) {
      now += 5;
      for (int j = 0; j < 4; ++j)
        step(1);
    }
    check_equal(flowmq_mdp_complete(worker, &y, 12, &body), SALTS_OK);
    check_equal(flowmq_mdp_complete(worker, &x, 11, &body), SALTS_OK);
    check_equal(flowmq_mdp_complete(worker, &x, 11, &body), SALTS_ENOENT);
    flowmq_mdp_result first = result(a, 1), second = result(b, 1);
    check_equal(first.outcome, FLOWMQ_MDP_RESULT);
    check_equal(second.outcome, FLOWMQ_MDP_RESULT);
    check_equal(first.status, x.id.sequence == a.sequence ? 11 : 12);
    check_equal(second.body.bytes, body.bytes, 3);
  }
  it("rotates a service across two registered workers") {
    sockets[4] = flowmq_socket(ctx, FLOWMQ_DEALER);
    attach(sockets[1], sockets[4], "v");
    check_equal(flowmq_mdp_worker_open(sockets[4], "echo", 302, 2, 10, &second_worker), SALTS_OK);
    for (int i = 0; i < 40; ++i)
      step(1);
    flowmq_mdp_id a = call("echo", 500), b = call("echo", 500);
    flowmq_mdp_task first = task(), second;
    int rc = SALTS_EBUSY;
    for (int i = 0; i < 2000 && rc == SALTS_EBUSY; ++i) {
      step(1);
      rc = flowmq_mdp_take_task(second_worker, &second);
    }
    check_equal(rc, SALTS_OK);
    check_not_equal(first.id.sequence, second.id.sequence);
    check_equal(flowmq_mdp_complete(second_worker, &second, 0, &body), SALTS_OK);
    check_equal(flowmq_mdp_complete(worker, &first, 0, &body), SALTS_OK);
    check_equal(result(a, 1).outcome, FLOWMQ_MDP_RESULT);
    check_equal(result(b, 1).outcome, FLOWMQ_MDP_RESULT);
  }
  it("hands an owned task to a business thread and returns its result on the I/O owner") {
    flowmq_mdp_id id = call("echo", 500);
    thrd_t business;
    check_equal(thrd_create(&business, execute_echo, worker), thrd_success);
    flowmq_mdp_result r;
    int ready = SALTS_EBUSY;
    for (int i = 0; i < 2000 && ready == SALTS_EBUSY; ++i) {
      step(1);
      ready = flowmq_mdp_take_result(client, id, &r);
    }
    int status;
    check_equal(thrd_join(business, &status), thrd_success);
    check_equal(status, SALTS_OK);
    check_equal(ready, SALTS_OK);
    check_equal(r.outcome, FLOWMQ_MDP_RESULT);
    check_equal(r.status, 17);
    check_equal(r.body.bytes, body.bytes, 3);
  }
  it("reports worker registration denied by service ACL") {
    check_equal(flowmq_mdp_worker_close(worker), SALTS_OK);
    check_equal(flowmq_mdp_worker_open(sockets[3], "restricted", 302, 2, 10, &worker), SALTS_OK);
    int rc = SALTS_OK;
    for (int i = 0; i < 2000 && rc == SALTS_OK; ++i) {
      step(0);
      rc = flowmq_mdp_worker_step(worker, now);
    }
    check_equal(rc, SALTS_EPERM);
    flowmq_mdp_task unused;
    check_equal(flowmq_mdp_take_task(worker, &unused), SALTS_ESHUTDOWN);
  }
  it("rejects unauthorized services and enforces per-client admission") {
    flowmq_mdp_id denied = call("secret", 500);
    flowmq_mdp_result r = result(denied, 1);
    check_equal(r.outcome, FLOWMQ_MDP_REJECTED);
    check_equal(r.status, SALTS_EPERM);
    for (int i = 0; i < 3; ++i)
      (void)call("offline", 1000);
    for (int i = 0; i < 16; ++i)
      step(1);
    flowmq_mdp_id overflow = call("offline", 1000);
    r = result(overflow, 1);
    check_equal(r.outcome, FLOWMQ_MDP_REJECTED);
    check_equal(r.status, SALTS_ENOBUFS);
  }
  it("distinguishes local expiration from lost dispatched work") {
    flowmq_mdp_id never_sent = call("echo", 5);
    now += 5;
    flowmq_mdp_result r = result(never_sent, 1);
    check_equal(r.outcome, FLOWMQ_MDP_REJECTED);
    flowmq_mdp_id active = call("echo", 500);
    flowmq_mdp_task t = task();
    now += 101;
    r = result(active, 0);
    check_equal(r.outcome, FLOWMQ_MDP_UNKNOWN);
    flowmq_mdp_worker_retire(worker);
    check_equal(flowmq_mdp_worker_close(worker), SALTS_EBUSY);
    check_equal(flowmq_mdp_complete(worker, &t, 0, &body), SALTS_OK);
  }
  it("does not reclaim a running slot when the client cancels") {
    flowmq_mdp_id id = call("echo", 500);
    flowmq_mdp_task t = task();
    check_equal(flowmq_mdp_cancel(client, id), SALTS_OK);
    flowmq_mdp_result unused;
    check_equal(flowmq_mdp_take_result(client, id, &unused), SALTS_ENOENT);
    check_equal(flowmq_mdp_complete(worker, &t, 0, &body), SALTS_OK);
    for (int i = 0; i < 10; ++i)
      step(1);
  }

  it("withdraws queued calls and retries a definite rejection within its original deadline") {
    flowmq_mdp_id waiting[3];
    for (size_t i = 0; i < 3; ++i)
      waiting[i] = call("offline", 500);
    for (int i = 0; i < 15; ++i)
      step(1);
    flowmq_mdp_id id = call("echo", 500), retry = {0};
    for (int i = 0; i < 15; ++i)
      step(1);
    check_equal(flowmq_mdp_cancel(client, waiting[0]), SALTS_OK);
    for (int i = 0; i < 10; ++i)
      step(1);
    now += 30;
    flowmq_mdp_result rejected;
    check_equal(flowmq_mdp_peek_result(client, id, &rejected), SALTS_OK);
    check_equal(rejected.outcome, FLOWMQ_MDP_REJECTED);
    check_equal(rejected.status, SALTS_ENOBUFS);
    check_equal(flowmq_mdp_retry(client, id, now, &retry), SALTS_OK);
    check_equal(retry.sequence, id.sequence);
    check_equal(retry.attempt, id.attempt + 1);
    flowmq_mdp_task t = task();
    check_equal(t.id.attempt, retry.attempt);
    check_true(t.remaining_ms <= 470);
    check_equal(flowmq_mdp_complete(worker, &t, 0, &body), SALTS_OK);
    check_equal(result(retry, 1).outcome, FLOWMQ_MDP_RESULT);
  }

  it("drains active work and rejects queued calls on broker stop") {
    flowmq_mdp_id active = call("echo", 500);
    flowmq_mdp_task t = task();
    flowmq_mdp_id waiting = call("offline", 500);
    for (int i = 0; i < 10; ++i)
      step(1);
    flowmq_mdp_broker_stop(broker);
    check_false(flowmq_mdp_broker_drained(broker));
    flowmq_mdp_result rejected = result(waiting, 1);
    check_equal(rejected.outcome, FLOWMQ_MDP_REJECTED);
    check_equal(rejected.status, SALTS_ESHUTDOWN);
    check_equal(flowmq_mdp_complete(worker, &t, 0, &body), SALTS_OK);
    check_equal(result(active, 1).outcome, FLOWMQ_MDP_RESULT);
    check_true(flowmq_mdp_broker_drained(broker));
  }

  it("keeps an expired running task charged until business completion") {
    flowmq_mdp_id expiring = call("echo", 5);
    flowmq_mdp_task expired = task();
    flowmq_mdp_id live = call("echo", 1000);
    flowmq_mdp_task other = task();
    now += 6;
    flowmq_mdp_result late = result(expiring, 1);
    check_equal(late.outcome, FLOWMQ_MDP_UNKNOWN);
    check_equal(late.status, SALTS_ETIMEDOUT);
    flowmq_mdp_id queued = call("echo", 1000);
    for (int i = 0; i < 16; ++i)
      step(1);
    flowmq_mdp_task next;
    check_equal(flowmq_mdp_take_task(worker, &next), SALTS_EBUSY);
    check_equal(flowmq_mdp_complete(worker, &expired, 0, &body), SALTS_OK);
    next = task();
    check_equal(next.slot, expired.slot);
    check_equal(next.id.sequence, queued.sequence);
    check_equal(flowmq_mdp_complete(worker, &next, 0, &body), SALTS_OK);
    check_equal(flowmq_mdp_complete(worker, &other, 0, &body), SALTS_OK);
    check_equal(result(live, 1).outcome, FLOWMQ_MDP_RESULT);
    check_equal(result(queued, 1).outcome, FLOWMQ_MDP_RESULT);
  }

  it("rejects extra wire parts and accepts the next complete request") {
    flowmq_mdp_id initial = call("echo", 1000);
    flowmq_mdp_task initial_task = task();
    check_equal(flowmq_mdp_complete(worker, &initial_task, 0, &body), SALTS_OK);
    check_equal(result(initial, 1).outcome, FLOWMQ_MDP_RESULT);
    check_equal(flowmq_send(sockets[2], "bad", 3, FLOWMQ_SNDMORE | FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_send(sockets[2], "tail", 4, FLOWMQ_DONTWAIT), SALTS_OK);
    flowmq_pollitem_t items[] = {{sockets[0], FLOWMQ_POLLIN, 0}, {sockets[2], FLOWMQ_POLLIN, 0}};
    int status = SALTS_OK;
    for (size_t i = 0; i < 2000 && status == SALTS_OK; ++i) {
      size_t ready = 0;
      check_equal(flowmq_poll(items, 2, 1, &ready), SALTS_OK);
      status = flowmq_mdp_broker_step(broker, now);
    }
    check_equal(status, SALTS_EPROTO);
    flowmq_mdp_id id = call("echo", 1000);
    flowmq_mdp_task accepted = task();
    check_equal(flowmq_mdp_complete(worker, &accepted, 0, &body), SALTS_OK);
    check_equal(result(id, 1).outcome, FLOWMQ_MDP_RESULT);
  }

  it("releases the router route after final admission fails") {
    flowmq_mdp_id first = call("echo", 1000);
    flowmq_mdp_task accepted = task();
    check_equal(flowmq_router_try_send(sockets[0], "c", 1, "x", SIZE_MAX), SALTS_EMSGSIZE);
    check_equal(flowmq_mdp_complete(worker, &accepted, 0, &body), SALTS_OK);
    check_equal(result(first, 1).outcome, FLOWMQ_MDP_RESULT);
  }

  it("isolates a replacement worker reusing the same routing identity") {
    flowmq_mdp_id active = call("echo", 500);
    flowmq_mdp_task old = task();
    flowmq_mdp_worker_retire(worker);
    check_equal(flowmq_mdp_complete(worker, &old, 0, &body), SALTS_OK);
    check_equal(flowmq_mdp_worker_close(worker), SALTS_OK);
    worker = NULL;
    check_equal(flowmq_close(sockets[3]), SALTS_OK);
    sockets[3] = NULL;
    now += 110;
    check_equal(flowmq_mdp_broker_step(broker, now), SALTS_OK);
    char endpoint[128];
    size_t size = 0;
    check_equal(flowmq_last_endpoint(sockets[1], endpoint, sizeof(endpoint), &size), SALTS_OK);
    sockets[3] = flowmq_socket(ctx, FLOWMQ_DEALER);
    check_equal(flowmq_setsockopt(sockets[3], FLOWMQ_IDENTITY, "w", 1), SALTS_OK);
    check_equal(flowmq_connect(sockets[3], endpoint), SALTS_OK);
    check_equal(flowmq_mdp_worker_open(sockets[3], "echo", 302, 2, 10, &worker), SALTS_OK);
    check_equal(result(active, 1).outcome, FLOWMQ_MDP_UNKNOWN);
    flowmq_mdp_id next = call("echo", 500);
    flowmq_mdp_task replacement = task();
    check_equal(flowmq_mdp_complete(worker, &old, 0, &body), SALTS_ENOENT);
    check_equal(flowmq_mdp_complete(worker, &replacement, 0, &body), SALTS_OK);
    check_equal(result(next, 1).outcome, FLOWMQ_MDP_RESULT);
  }
}

static void make_ipc_endpoint(const char *directory, const char *role, char *endpoint,
                              size_t size) {
#ifdef _WIN32
  const char *name = directory;
  for (const char *p = directory; *p; ++p)
    if (*p == '/' || *p == '\\') name = p + 1;
  int written = snprintf(endpoint, size, "ipc://%s-%s", name, role);
#else
  int written = snprintf(endpoint, size, "ipc://%s/%s", directory, role);
#endif
  check(written > 0 && (size_t)written < size);
}

static void check_echo(const char *front, const char *back) {
  open_nodes(front, back);
  flowmq_mdp_id id = call("echo", 1000);
  flowmq_mdp_task job = task();
  check_equal(flowmq_mdp_complete(worker, &job, 0, &job.body), SALTS_OK);
  flowmq_mdp_result reply = result(id, 1);
  check_equal(reply.outcome, FLOWMQ_MDP_RESULT);
  check_equal(reply.status, 0);
  check_equal(reply.body.count, body.count);
  check_equal(reply.body.size, body.size);
  check_equal(reply.body.lengths[0], 3u);
  check_equal(reply.body.lengths[1], 0u);
  check_equal(reply.body.bytes, body.bytes, body.size);
}

spec("MDP with native IPC") {
  static char *directory;
  static char front[256], back[256];
  const char *tcp = "tcp://127.0.0.1:0";
  const char *ws = "ws://127.0.0.1:0/fmq";
  const char *kcp = "kcp://127.0.0.1:0?conv=42";
  before_each() {
    directory = tt_make_temp_dir("fmqmdp");
    check_not_null(directory);
    make_ipc_endpoint(directory, "front", front, sizeof(front));
    make_ipc_endpoint(directory, "back", back, sizeof(back));
  }
  after_each() {
    close_nodes();
    if (directory) {
      check_equal(tt_remove_tree(directory), 0);
      free(directory);
      directory = NULL;
    }
  }
  it("IPC client and IPC worker") { check_echo(front, back); }
  it("IPC client and TCP worker") { check_echo(front, tcp); }
  it("TCP client and IPC worker") { check_echo(tcp, back); }
  it("IPC client and WS worker") { check_echo(front, ws); }
  it("WS client and IPC worker") { check_echo(ws, back); }
  it("IPC client and KCP worker") { check_echo(front, kcp); }
  it("KCP client and IPC worker") { check_echo(kcp, back); }
}

static int authorize_worker(void *context, int is_worker, const void *id, size_t size,
                            const char *service) {
  int *registrations = context;
  if (!is_worker || size == 0 || *(const char *)id != 'w' || strcmp(service, "echo")) return 0;
  ++*registrations;
  return 1;
}

spec("one broker serving TCP and IPC workers concurrently") {
  enum { workers_per_listener = 33, total = 2 * workers_per_listener, socket_count = 4 + total };
  static flowmq_socket_t *nodes[socket_count];
  static flowmq_mdp_worker *workers[total];
  static char *directory;
  after_each() {
    for (size_t i = 0; i < total; ++i) {
      if (workers[i]) {
        flowmq_mdp_worker_retire(workers[i]);
        check_equal(flowmq_mdp_worker_close(workers[i]), SALTS_OK);
        workers[i] = NULL;
      }
    }
    flowmq_mdp_client_close(client);
    client = NULL;
    flowmq_mdp_broker_close(broker);
    broker = NULL;
    for (size_t i = socket_count; i > 0; --i) {
      if (nodes[i - 1]) {
        check_equal(flowmq_close(nodes[i - 1]), SALTS_OK);
        nodes[i - 1] = NULL;
      }
    }
    if (ctx) check_equal(flowmq_ctx_term(ctx), SALTS_OK);
    ctx = NULL;
    if (directory) {
      check_equal(tt_remove_tree(directory), 0);
      free(directory);
      directory = NULL;
    }
  }
  it("dispatches to every slot, including equal identities on different listeners") {
    ctx = flowmq_ctx_new();
    check_not_null(ctx);
    flowmq_pollitem_t items[socket_count];
    for (size_t i = 0; i < socket_count; ++i) {
      nodes[i] = flowmq_socket(ctx, i < 3 ? FLOWMQ_ROUTER : FLOWMQ_DEALER);
      check_not_null(nodes[i]);
      items[i] = (flowmq_pollitem_t){nodes[i], FLOWMQ_POLLIN, 0};
    }
    directory = tt_make_temp_dir("fmqmdp");
    check_not_null(directory);
    char ipc[256];
    make_ipc_endpoint(directory, "workers", ipc, sizeof(ipc));
    check_equal(flowmq_socket_set_peer_capacity(nodes[1], 0), SALTS_EINVAL);
    check_equal(flowmq_socket_set_peer_capacity(nodes[1], SIZE_MAX), SALTS_ERANGE);
    check_equal(flowmq_socket_set_peer_capacity(nodes[1], workers_per_listener), SALTS_OK);
    check_equal(flowmq_socket_set_peer_capacity(nodes[2], workers_per_listener), SALTS_OK);
    check_equal(flowmq_bind(nodes[0], "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_bind(nodes[1], "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_socket_set_peer_capacity(nodes[1], workers_per_listener + 1), SALTS_EBUSY);
    check_equal(flowmq_bind(nodes[2], ipc), SALTS_OK);
    attach(nodes[0], nodes[3], "c");
    int registrations[2] = {0};
    flowmq_mdp_worker_listener listeners[] = {{nodes[1], authorize_worker, &registrations[0]},
                                              {nodes[2], authorize_worker, &registrations[1]}};
    flowmq_mdp_broker_config config = {1, total, total, total, 30000, allow, NULL, total};
    check_equal(flowmq_mdp_broker_open_listeners(nodes[0], listeners, 2, &config, &broker),
                SALTS_OK);
    check_equal(flowmq_mdp_client_open(nodes[3], 2, total, &client), SALTS_OK);
    for (size_t i = 0; i < total; ++i) {
      char identity[32], address[256];
      size_t size;
      int len = snprintf(identity, sizeof(identity), "w%u", (unsigned)(i % workers_per_listener));
      flowmq_socket_t *dealer = nodes[4 + i];
      check_equal(flowmq_setsockopt(dealer, FLOWMQ_IDENTITY, identity, (size_t)len), SALTS_OK);
      check_equal(flowmq_last_endpoint(nodes[i < workers_per_listener ? 1 : 2], address,
                                       sizeof(address), &size),
                  SALTS_OK);
      check_equal(flowmq_connect(dealer, address), SALTS_OK);
      check_equal(flowmq_mdp_worker_open(dealer, "echo", 100 + i, 1, 5000, &workers[i]), SALTS_OK);
    }
    flowmq_mdp_id ids[total];
    flowmq_mdp_body empty = {0};
    now = monotonic_ms();
    for (size_t i = 0; i < total; ++i)
      check_equal(flowmq_mdp_call(client, "echo", &empty, now, 30000, &ids[i]), SALTS_OK);
    size_t assigned = 0, received = 0;
    int expected[total] = {0};
    flowmq_mdp_task jobs[total] = {0};
    uint64_t deadline = now + 30000;
    while (received < total && (now = monotonic_ms()) < deadline) {
      size_t ready;
      check_equal(flowmq_poll(items, socket_count, 1, &ready), SALTS_OK);
      check_equal(flowmq_mdp_broker_step(broker, now), SALTS_OK);
      check_equal(flowmq_mdp_client_step(client, now), SALTS_OK);
      for (size_t i = 0; i < total; ++i) {
        check_equal(flowmq_mdp_worker_step(workers[i], now), SALTS_OK);
        if (jobs[i].worker) continue;
        int rc = flowmq_mdp_take_task(workers[i], &jobs[i]);
        if (rc == SALTS_EBUSY) continue;
        check_equal(rc, SALTS_OK);
        check_equal(jobs[i].worker, 100u + i);
        check(jobs[i].id.sequence > 0 && jobs[i].id.sequence <= total);
        expected[jobs[i].id.sequence - 1] = (int)i + 1;
        if (++assigned == total)
          for (size_t j = 0; j < total; ++j)
            check_equal(flowmq_mdp_complete(workers[j], &jobs[j], (int)j + 1, &empty), SALTS_OK);
      }
      while (received < total) {
        flowmq_mdp_result r;
        int rc = flowmq_mdp_take_result(client, ids[received], &r);
        if (rc == SALTS_EBUSY) break;
        check_equal(rc, SALTS_OK);
        check_equal(r.outcome, FLOWMQ_MDP_RESULT);
        check_equal(r.status, expected[received]);
        ++received;
      }
    }
    check_equal(assigned, (size_t)total);
    check_equal(received, (size_t)total);
    check(registrations[0] >= workers_per_listener && registrations[1] >= workers_per_listener);
  }
}
