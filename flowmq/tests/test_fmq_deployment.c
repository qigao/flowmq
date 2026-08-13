#include "tinytest.h"
#include "turbo_flow_fmq_deployment.h"

#include <string.h>

typedef struct deployment_clock_s {
  uint64_t now_ms;
} deployment_clock_t;

static uint64_t deployment_now_ms(void *ctx) { return ((deployment_clock_t *)ctx)->now_ms; }

static turbo_flow_fmq_deployment_member_t
deployment_member(const char *member_id, const char *failure_domain, const char *logical_route,
                  const char *endpoint, uint32_t priority) {
  turbo_flow_fmq_deployment_member_t member = TURBO_FLOW_FMQ_DEPLOYMENT_MEMBER_INIT;
  (void)turbo_uuid_v7_generate(&member.incarnation_id);
  memcpy(member.member_id, member_id, strlen(member_id) + 1u);
  memcpy(member.failure_domain, failure_domain, strlen(failure_domain) + 1u);
  memcpy(member.logical_route, logical_route, strlen(logical_route) + 1u);
  memcpy(member.endpoint, endpoint, strlen(endpoint) + 1u);
  member.priority = priority;
  member.state = TURBO_FLOW_FMQ_MEMBER_READY;
  return member;
}

static int deployment_snapshot(turbo_flow_fmq_deployment_controller_t *controller,
                               turbo_flow_fmq_deployment_snapshot_t *snapshot,
                               turbo_flow_fmq_deployment_member_t *members,
                               turbo_flow_fmq_route_snapshot_t *routes) {
  *snapshot = (turbo_flow_fmq_deployment_snapshot_t)TURBO_FLOW_FMQ_DEPLOYMENT_SNAPSHOT_INIT;
  snapshot->members = members;
  snapshot->member_capacity = 8u;
  snapshot->routes = routes;
  snapshot->route_capacity = 8u;
  return turbo_flow_fmq_deployment_snapshot(controller, snapshot);
}

static turbo_flow_fmq_membership_command_t
deployment_command(turbo_flow_fmq_membership_command_kind_t kind,
                   const turbo_flow_fmq_deployment_snapshot_t *snapshot,
                   const turbo_flow_fmq_deployment_member_t *member) {
  turbo_flow_fmq_membership_command_t command = TURBO_FLOW_FMQ_MEMBERSHIP_COMMAND_INIT;
  command.kind = kind;
  memcpy(command.authority_id, snapshot->authority_id, strlen(snapshot->authority_id) + 1u);
  command.authority_epoch = snapshot->authority_epoch;
  command.authority_incarnation_id = snapshot->authority_incarnation_id;
  command.expected_registry_version = snapshot->registry_version;
  command.member = *member;
  return command;
}

spec("fmq_deployment") {
  it("owns membership and fences deterministic cross-domain failover") {
    deployment_clock_t clock = {1000u};
    turbo_flow_fmq_deployment_config_t config = TURBO_FLOW_FMQ_DEPLOYMENT_CONFIG_INIT;
    turbo_flow_fmq_deployment_controller_t *controller = NULL;
    turbo_flow_fmq_deployment_snapshot_t snapshot = TURBO_FLOW_FMQ_DEPLOYMENT_SNAPSHOT_INIT;
    turbo_flow_fmq_deployment_member_t members[8];
    turbo_flow_fmq_route_snapshot_t routes[8];
    turbo_flow_fmq_route_snapshot_t route = TURBO_FLOW_FMQ_ROUTE_SNAPSHOT_INIT;
    turbo_flow_fmq_deployment_member_t first =
        deployment_member("broker-a", "az-a", "orders", "tcp://a:7001", 10u);
    turbo_flow_fmq_deployment_member_t second =
        deployment_member("broker-b", "az-b", "orders", "tcp://b:7001", 20u);
    turbo_flow_fmq_membership_command_t command;
    turbo_flow_fmq_membership_result_t result = TURBO_FLOW_FMQ_MEMBERSHIP_RESULT_INIT;

    config.now_ms = deployment_now_ms;
    config.now_ms_ctx = &clock;
    config.lease_timeout_ms = 100u;
    config.member_capacity = 8u;
    check_int_eq(turbo_flow_fmq_deployment_controller_create(&config, &controller), TURBO_OK);
    check_not_null(controller);
    check_int_eq(deployment_snapshot(controller, &snapshot, members, routes), TURBO_OK);
    check_uint_eq(snapshot.registry_version, 1u);

    command = deployment_command(TURBO_FLOW_FMQ_MEMBERSHIP_JOIN, &snapshot, &first);
    check_int_eq(turbo_flow_fmq_deployment_apply(controller, &command, &result), TURBO_OK);
    check_true(result.primary_changed);
    check_uint_eq(result.route_generation, 1u);
    check_int_eq(turbo_flow_fmq_deployment_resolve(controller, "orders", &route), TURBO_OK);
    check_str_eq(route.primary_member_id, "broker-a");

    check_int_eq(deployment_snapshot(controller, &snapshot, members, routes), TURBO_OK);
    command = deployment_command(TURBO_FLOW_FMQ_MEMBERSHIP_JOIN, &snapshot, &second);
    result = (turbo_flow_fmq_membership_result_t)TURBO_FLOW_FMQ_MEMBERSHIP_RESULT_INIT;
    check_int_eq(turbo_flow_fmq_deployment_apply(controller, &command, &result), TURBO_OK);
    check_false(result.primary_changed);
    check_int_eq(turbo_flow_fmq_deployment_resolve(controller, "orders", &route), TURBO_OK);
    check_str_eq(route.primary_member_id, "broker-a");

    check_int_eq(deployment_snapshot(controller, &snapshot, members, routes), TURBO_OK);
    command = deployment_command(TURBO_FLOW_FMQ_MEMBERSHIP_DRAIN, &snapshot, &first);
    check_int_eq(turbo_flow_fmq_deployment_apply(controller, &command, &result), TURBO_OK);
    check_true(result.primary_changed);
    check_uint_eq(result.route_generation, 2u);
    check_int_eq(turbo_flow_fmq_deployment_resolve(controller, "orders", &route), TURBO_OK);
    check_str_eq(route.primary_member_id, "broker-b");
    check_str_eq(route.primary_failure_domain, "az-b");
    check_str_eq(route.endpoint, "tcp://b:7001");

    turbo_flow_fmq_deployment_controller_destroy(controller);
  }

  it("expires leases, rejects stale incarnations, and preserves one route owner") {
    deployment_clock_t clock = {10u};
    turbo_flow_fmq_deployment_config_t config = TURBO_FLOW_FMQ_DEPLOYMENT_CONFIG_INIT;
    turbo_flow_fmq_deployment_controller_t *controller = NULL;
    turbo_flow_fmq_deployment_snapshot_t snapshot = TURBO_FLOW_FMQ_DEPLOYMENT_SNAPSHOT_INIT;
    turbo_flow_fmq_deployment_member_t members[8];
    turbo_flow_fmq_route_snapshot_t routes[8];
    turbo_flow_fmq_route_snapshot_t route = TURBO_FLOW_FMQ_ROUTE_SNAPSHOT_INIT;
    turbo_flow_fmq_deployment_member_t first =
        deployment_member("broker-a", "az-a", "billing", "tcp://a:7002", 10u);
    turbo_flow_fmq_deployment_member_t replacement =
        deployment_member("broker-a", "az-b", "billing", "tcp://b:7002", 10u);
    turbo_flow_fmq_membership_command_t command;
    turbo_flow_fmq_membership_result_t result = TURBO_FLOW_FMQ_MEMBERSHIP_RESULT_INIT;
    size_t expired = 0u;

    config.now_ms = deployment_now_ms;
    config.now_ms_ctx = &clock;
    config.lease_timeout_ms = 50u;
    config.member_capacity = 8u;
    check_int_eq(turbo_flow_fmq_deployment_controller_create(&config, &controller), TURBO_OK);
    check_int_eq(deployment_snapshot(controller, &snapshot, members, routes), TURBO_OK);
    command = deployment_command(TURBO_FLOW_FMQ_MEMBERSHIP_JOIN, &snapshot, &first);
    check_int_eq(turbo_flow_fmq_deployment_apply(controller, &command, &result), TURBO_OK);

    check_int_eq(deployment_snapshot(controller, &snapshot, members, routes), TURBO_OK);
    command = deployment_command(TURBO_FLOW_FMQ_MEMBERSHIP_HEARTBEAT, &snapshot, &replacement);
    check_int_eq(turbo_flow_fmq_deployment_apply(controller, &command, &result), TURBO_EPERM);
    check_int_eq(turbo_flow_fmq_deployment_resolve(controller, "billing", &route), TURBO_OK);
    check_str_eq(route.primary_member_id, "broker-a");
    check_str_eq(route.primary_failure_domain, "az-a");

    clock.now_ms = 61u;
    check_int_eq(turbo_flow_fmq_deployment_tick(controller, &expired), TURBO_OK);
    check_size_eq(expired, 1u);
    check_int_eq(turbo_flow_fmq_deployment_resolve(controller, "billing", &route), TURBO_ENOTCONN);
    check_uint_eq(route.route_generation, 2u);

    check_int_eq(deployment_snapshot(controller, &snapshot, members, routes), TURBO_OK);
    command = deployment_command(TURBO_FLOW_FMQ_MEMBERSHIP_JOIN, &snapshot, &replacement);
    check_int_eq(turbo_flow_fmq_deployment_apply(controller, &command, &result), TURBO_OK);
    check_int_eq(turbo_flow_fmq_deployment_resolve(controller, "billing", &route), TURBO_OK);
    check_uint_eq(route.route_generation, 3u);
    check_str_eq(route.primary_failure_domain, "az-b");

    command.expected_registry_version -= 1u;
    check_int_eq(turbo_flow_fmq_deployment_apply(controller, &command, &result), TURBO_EALREADY);
    command.expected_registry_version += 1u;
    command.authority_epoch += 1u;
    check_int_eq(turbo_flow_fmq_deployment_apply(controller, &command, &result), TURBO_EPERM);

    turbo_flow_fmq_deployment_controller_destroy(controller);
  }

  it("classifies normalized snapshots and detects split brain") {
    deployment_clock_t clock = {100u};
    turbo_flow_fmq_deployment_config_t config = TURBO_FLOW_FMQ_DEPLOYMENT_CONFIG_INIT;
    turbo_flow_fmq_deployment_controller_t *controller = NULL;
    turbo_flow_fmq_deployment_member_t left_members[8];
    turbo_flow_fmq_deployment_member_t right_members[8];
    turbo_flow_fmq_route_snapshot_t left_routes[8];
    turbo_flow_fmq_route_snapshot_t right_routes[8];
    turbo_flow_fmq_deployment_snapshot_t left = TURBO_FLOW_FMQ_DEPLOYMENT_SNAPSHOT_INIT;
    turbo_flow_fmq_deployment_snapshot_t right = TURBO_FLOW_FMQ_DEPLOYMENT_SNAPSHOT_INIT;
    turbo_flow_fmq_deployment_member_t member =
        deployment_member("broker-a", "az-a", "events", "tcp://a:7003", 1u);
    turbo_flow_fmq_membership_command_t command;
    turbo_flow_fmq_membership_result_t result = TURBO_FLOW_FMQ_MEMBERSHIP_RESULT_INIT;
    turbo_flow_fmq_snapshot_relation_t relation = 0;

    config.now_ms = deployment_now_ms;
    config.now_ms_ctx = &clock;
    config.member_capacity = 8u;
    check_int_eq(turbo_flow_fmq_deployment_controller_create(&config, &controller), TURBO_OK);
    check_int_eq(deployment_snapshot(controller, &left, left_members, left_routes), TURBO_OK);
    command = deployment_command(TURBO_FLOW_FMQ_MEMBERSHIP_JOIN, &left, &member);
    check_int_eq(turbo_flow_fmq_deployment_apply(controller, &command, &result), TURBO_OK);
    check_int_eq(deployment_snapshot(controller, &left, left_members, left_routes), TURBO_OK);
    clock.now_ms += 10u;
    check_int_eq(deployment_snapshot(controller, &right, right_members, right_routes), TURBO_OK);
    check_int_eq(turbo_flow_fmq_deployment_snapshot_compare(&left, &right, &relation), TURBO_OK);
    check_int_eq(relation, TURBO_FLOW_FMQ_SNAPSHOT_SAME);

    memcpy(right.routes[0].endpoint, "tcp://evil:1", sizeof("tcp://evil:1"));
    check_int_eq(turbo_flow_fmq_deployment_snapshot_compare(&left, &right, &relation), TURBO_OK);
    check_int_eq(relation, TURBO_FLOW_FMQ_SNAPSHOT_SPLIT_BRAIN);
    right.routes[0] = left.routes[0];
    (void)turbo_uuid_v7_generate(&right.authority_incarnation_id);
    check_int_eq(turbo_flow_fmq_deployment_snapshot_compare(&left, &right, &relation), TURBO_OK);
    check_int_eq(relation, TURBO_FLOW_FMQ_SNAPSHOT_SPLIT_BRAIN);
    right.authority_epoch += 1u;
    check_int_eq(turbo_flow_fmq_deployment_snapshot_compare(&left, &right, &relation), TURBO_OK);
    check_int_eq(relation, TURBO_FLOW_FMQ_SNAPSHOT_NEWER);

    turbo_flow_fmq_deployment_controller_destroy(controller);
  }
}
