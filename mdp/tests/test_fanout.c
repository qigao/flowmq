#include "flowmq_mdp_fanout.h"
#include <core/error.h>
#include <string.h>
#include <tinytest.h>

static const flowmq_mdp_member members[] = {{10, 410}, {20, 420}, {30, 430}};
static flowmq_ctx_t *ctx;
static flowmq_socket_t *sockets[5];
static flowmq_mdp_client *client;
static flowmq_mdp_worker *workers[2];
static flowmq_mdp_broker *broker;
static flowmq_mdp_fanout *group;
static flowmq_mdp_body input;
static unsigned executions[2];
static uint64_t now;

static int authorize(void *ctx_arg, int worker, const void *identity, size_t size,
                     const char *service) {
  (void)ctx_arg;
  if (size != 1) return 0;
  for (size_t i = 0; i < 3; ++i) {
    char name[FLOWMQ_MDP_SERVICE_MAX + 1];
    if (flowmq_mdp_member_service(members[i], name) == SALTS_OK && !strcmp(service, name))
      return *(const char *)identity == (worker ? 'a' + (int)i : 'c');
  }
  return 0;
}

static void attach(flowmq_socket_t *router, flowmq_socket_t *dealer, char id) {
  char endpoint[128];
  size_t length = 0;
  check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint), &length), SALTS_OK);
  check_equal(flowmq_setsockopt(dealer, FLOWMQ_IDENTITY, &id, 1), SALTS_OK);
  check_equal(flowmq_connect(dealer, endpoint), SALTS_OK);
}

static void open_fixture(uint64_t first_session) {
  now = 1000;
  executions[0] = executions[1] = 0;
  ctx = flowmq_ctx_new();
  check(ctx);
  for (size_t i = 0; i < 5; ++i) {
    sockets[i] = flowmq_socket(ctx, i < 2 ? FLOWMQ_ROUTER : FLOWMQ_DEALER);
    check(sockets[i]);
  }
  check_equal(flowmq_bind(sockets[0], "tcp://127.0.0.1:0"), SALTS_OK);
  check_equal(flowmq_bind(sockets[1], "tcp://127.0.0.1:0"), SALTS_OK);
  attach(sockets[0], sockets[2], 'c');
  flowmq_mdp_broker_config cfg = {5, 8, 2, 8, 500, authorize, NULL};
  check_equal(flowmq_mdp_broker_open(sockets[0], sockets[1], &cfg, &broker), SALTS_OK);
  check_equal(flowmq_mdp_client_open(sockets[2], 100, 3, &client), SALTS_OK);
  for (size_t i = 0; i < 2; ++i) {
    attach(sockets[1], sockets[3 + i], (char)('a' + i));
    char name[FLOWMQ_MDP_SERVICE_MAX + 1];
    check_equal(flowmq_mdp_member_service(members[i], name), SALTS_OK);
    check_equal(flowmq_mdp_worker_open(sockets[3 + i], name, i ? members[i].session : first_session,
                                       2, 10, &workers[i]),
                SALTS_OK);
  }
  input = (flowmq_mdp_body){0};
  check_equal(flowmq_mdp_add_part(&input, NULL, 0), SALTS_OK);
  check_equal(flowmq_mdp_add_part(&input, "x\0y", 3), SALTS_OK);
}

static void close_fixture(void) {
  check_equal(flowmq_mdp_fanout_close(group), SALTS_OK);
  group = NULL;
  for (size_t i = 0; i < 2; ++i) {
    if (workers[i]) {
      flowmq_mdp_worker_retire(workers[i]);
      check_equal(flowmq_mdp_worker_close(workers[i]), SALTS_OK);
      workers[i] = NULL;
    }
  }
  flowmq_mdp_broker_close(broker);
  broker = NULL;
  flowmq_mdp_client_close(client);
  client = NULL;
  for (size_t i = 0; i < 5; ++i) {
    if (sockets[i]) flowmq_close(sockets[i]);
    sockets[i] = NULL;
  }
  if (ctx) flowmq_ctx_term(ctx);
  ctx = NULL;
}

static void step(void) {
  flowmq_pollitem_t items[5];
  size_t ready = 0;
  for (size_t i = 0; i < 5; ++i)
    items[i] = (flowmq_pollitem_t){sockets[i], FLOWMQ_POLLIN, 0};
  check_equal(flowmq_poll(items, 5, 1, &ready), SALTS_OK);
  check_equal(flowmq_mdp_client_step(client, now), SALTS_OK);
  for (size_t i = 0; i < 2; ++i)
    check_equal(flowmq_mdp_worker_step(workers[i], now), SALTS_OK);
  check_equal(flowmq_mdp_broker_step(broker, now), SALTS_OK);
}

static int32_t handle_member(void *context, uint64_t operation_id, flowmq_mdp_member self,
                             const flowmq_mdp_body *command, flowmq_mdp_body *reply) {
  unsigned *calls = context;
  ++*calls;
  if (operation_id != 777 || command->count != 2 || command->size != 3 || command->lengths[0] ||
      command->lengths[1] != 3 || memcmp(command->bytes, "x\0y", 3))
    return SALTS_EPROTO;
  int rc = flowmq_mdp_add_part(reply, &self.id, sizeof(self.id));
  return rc == SALTS_OK && self.id == members[1].id ? SALTS_EPERM : rc;
}

spec("all-member operations over a single MDP broker") {
  after_each() { close_fixture(); }
  it("correlates out-of-order application outcomes and separately reports the missing member") {
    open_fixture(members[0].session);
    flowmq_mdp_member snapshot[3];
    memcpy(snapshot, members, sizeof(snapshot));
    check_equal(flowmq_mdp_fanout_start(client, 777, snapshot, 3, &input, now, 100, &group),
                SALTS_OK);
    /* The directory may change as soon as start returns. */
    snapshot[2] = (flowmq_mdp_member){999, 999};
    flowmq_mdp_fanout_summary summary;
    for (size_t phase = 0; phase < 2; ++phase) {
      size_t i = 1 - phase;
      for (int turn = 0; turn < 2000; ++turn) {
        step();
        int rc = flowmq_mdp_fanout_execute(workers[i], members[i], handle_member, &executions[i]);
        check(rc == SALTS_OK || rc == SALTS_EBUSY);
        check_equal(flowmq_mdp_fanout_poll(group, &summary), SALTS_EBUSY);
        if (summary.pending == 2 - phase) break;
      }
      check_equal(summary.pending, 2 - phase);
      check_equal(executions[i], 1u);
    }
    flowmq_mdp_result result;
    flowmq_mdp_member member;
    check_equal(flowmq_mdp_fanout_result_at(group, 2, &member, &result), SALTS_EBUSY);
    now += 100;
    step();
    for (size_t i = 0; i < 2; ++i) {
      check_equal(flowmq_mdp_fanout_poll(group, &summary), SALTS_OK);
      check_equal(summary.succeeded, 1u);
      check_equal(summary.failed, 1u);
      check_equal(summary.missing, 1u);
    }
    for (size_t i = 0; i < 3; ++i) {
      check_equal(flowmq_mdp_fanout_result_at(group, i, &member, &result), SALTS_OK);
      check_equal(member.id, members[i].id);
      check_equal(member.session, members[i].session);
      if (i < 2) {
        check_equal(result.outcome, FLOWMQ_MDP_RESULT);
        check_equal(result.status, i ? SALTS_EPERM : SALTS_OK);
        check_equal(result.body.size, sizeof(member.id));
        check_equal(memcmp(result.body.bytes, &member.id, sizeof(member.id)), 0);
      } else {
        check_equal(result.outcome, FLOWMQ_MDP_UNKNOWN);
        check_equal(result.status, SALTS_ETIMEDOUT);
      }
    }
  }
  it("rolls back every unsent reservation when one client slot is unavailable") {
    open_fixture(members[0].session);
    char service[FLOWMQ_MDP_SERVICE_MAX + 1];
    check_equal(flowmq_mdp_member_service(members[0], service), SALTS_OK);
    flowmq_mdp_id reserved;
    check_equal(flowmq_mdp_call(client, service, &input, now, 100, &reserved), SALTS_OK);
    check_equal(flowmq_mdp_fanout_start(client, 777, members, 3, &input, now, 100, &group),
                SALTS_ENOBUFS);
    check(group == NULL);
    check_equal(flowmq_mdp_cancel(client, reserved), SALTS_OK);
    check_equal(flowmq_mdp_fanout_start(client, 777, members, 3, &input, now, 100, &group),
                SALTS_OK);
    check_equal(flowmq_mdp_fanout_close(group), SALTS_OK);
    group = NULL;
  }
  it("rejects duplicate stable member IDs and request bodies that exceed the metadata budget") {
    open_fixture(members[0].session);
    flowmq_mdp_member duplicates[2] = {members[0], {members[0].id, members[0].session + 1}};
    check_equal(flowmq_mdp_fanout_start(client, 777, duplicates, 2, &input, now, 100, &group),
                SALTS_EINVAL);
    flowmq_mdp_body oversized = {0};
    for (size_t i = 0; i < FLOWMQ_MDP_PARTS_MAX; ++i)
      check_equal(flowmq_mdp_add_part(&oversized, NULL, 0), SALTS_OK);
    check_equal(flowmq_mdp_fanout_start(client, 777, members, 2, &oversized, now, 100, &group),
                SALTS_EMSGSIZE);
    check(group == NULL);
  }
  it("discards even a successful reply from a replacement using a stale service name") {
    open_fixture(members[0].session + 1);
    check_equal(flowmq_mdp_fanout_start(client, 777, members, 1, &input, now, 100, &group),
                SALTS_OK);
    flowmq_mdp_task task;
    int rc = SALTS_EBUSY;
    for (int i = 0; i < 2000 && rc == SALTS_EBUSY; ++i) {
      step();
      rc = flowmq_mdp_take_task(workers[0], &task);
    }
    check_equal(rc, SALTS_OK);
    check_equal(flowmq_mdp_complete(workers[0], &task, SALTS_OK, &input), SALTS_OK);
    check_equal(flowmq_mdp_complete(workers[0], &task, SALTS_OK, &input), SALTS_ENOENT);
    flowmq_mdp_fanout_summary summary;
    rc = SALTS_EBUSY;
    for (int i = 0; i < 2000 && rc == SALTS_EBUSY; ++i) {
      step();
      rc = flowmq_mdp_fanout_poll(group, &summary);
    }
    check_equal(rc, SALTS_OK);
    check_equal(summary.missing, 1u);
    check_equal(summary.succeeded, 0u);
    flowmq_mdp_member target;
    flowmq_mdp_result result;
    check_equal(flowmq_mdp_fanout_result_at(group, 0, &target, &result), SALTS_OK);
    check_equal(result.outcome, FLOWMQ_MDP_UNKNOWN);
    check_equal(result.status, SALTS_EPROTO);
    check_equal(result.body.count, 0u);
  }
  it("leaves an unsubmitted operation definitively rejected when its deadline passes") {
    open_fixture(members[0].session);
    check_equal(flowmq_mdp_fanout_start(client, 777, members, 1, &input, now, 100, &group),
                SALTS_OK);
    now += 100;
    step();
    flowmq_mdp_fanout_summary summary;
    check_equal(flowmq_mdp_fanout_poll(group, &summary), SALTS_OK);
    check_equal(summary.missing, 1u);
    flowmq_mdp_member member;
    flowmq_mdp_result result;
    check_equal(flowmq_mdp_fanout_result_at(group, 0, &member, &result), SALTS_OK);
    check_equal(result.outcome, FLOWMQ_MDP_REJECTED);
    check_equal(result.status, SALTS_ETIMEDOUT);
    check_equal(executions[0], 0u);
  }
  it("settles misaddressed work without invoking the replacement's handler") {
    flowmq_mdp_member replacement = {members[0].id, members[0].session + 1};
    open_fixture(replacement.session);
    check_equal(flowmq_mdp_fanout_start(client, 777, members, 1, &input, now, 100, &group),
                SALTS_OK);
    flowmq_mdp_fanout_summary summary;
    int rc = SALTS_EBUSY;
    for (int turn = 0; turn < 2000 && rc == SALTS_EBUSY; ++turn) {
      step();
      int exec = flowmq_mdp_fanout_execute(workers[0], replacement, handle_member, &executions[0]);
      check(exec == SALTS_EBUSY || exec == SALTS_OK);
      rc = flowmq_mdp_fanout_poll(group, &summary);
    }
    check_equal(rc, SALTS_OK);
    check_equal(executions[0], 0u);
    check_equal(summary.missing, 1u);
  }
}
