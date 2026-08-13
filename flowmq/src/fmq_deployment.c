#include "turbo_flow_fmq_deployment.h"

#include "turbo_error.h"
#include "turbo_str_view.h"
#include "turbo_vec.h"

#include <stdlib.h>
#include <string.h>

typedef struct flow_fmq_deployment_member_record_s {
  turbo_flow_fmq_deployment_member_t value;
  uint64_t lease_deadline_ms;
} flow_fmq_deployment_member_record_t;

TURBO_VEC_DEFINE(flow_fmq_deployment_members, flow_fmq_deployment_member_record_t)
TURBO_VEC_DEFINE(flow_fmq_deployment_routes, turbo_flow_fmq_route_snapshot_t)

struct turbo_flow_fmq_deployment_controller_s {
  turbo_flow_fmq_deployment_config_t config;
  turbo_uuid_t incarnation_id;
  uint64_t registry_version;
  flow_fmq_deployment_members members;
  flow_fmq_deployment_routes routes;
};

static uint64_t flow_fmq_deployment_default_now_ms(void *ctx) {
  (void)ctx;
  return turbo_hrtime() / UINT64_C(1000000);
}

static uint64_t
flow_fmq_deployment_now_ms(const turbo_flow_fmq_deployment_controller_t *controller) {
  return controller->config.now_ms ? controller->config.now_ms(controller->config.now_ms_ctx)
                                   : flow_fmq_deployment_default_now_ms(NULL);
}

static int flow_fmq_deployment_text_valid(const char *value, size_t capacity) {
  const char *end;
  if (!value || capacity == 0u) return 0;
  end = (const char *)memchr(value, '\0', capacity);
  return end && end != value && tstr_v_utf8_valid(tstr_v_from_buf(value, (size_t)(end - value)));
}

static int flow_fmq_deployment_uuid_is_zero(const turbo_uuid_t *value) {
  static const uint8_t zero[sizeof(value->bytes)] = {0};
  return !value || memcmp(value->bytes, zero, sizeof(zero)) == 0;
}

static int flow_fmq_deployment_member_valid(const turbo_flow_fmq_deployment_member_t *member) {
  return member && member->size >= sizeof(*member) &&
         flow_fmq_deployment_text_valid(member->member_id, sizeof(member->member_id)) &&
         !flow_fmq_deployment_uuid_is_zero(&member->incarnation_id) &&
         flow_fmq_deployment_text_valid(member->failure_domain, sizeof(member->failure_domain)) &&
         flow_fmq_deployment_text_valid(member->logical_route, sizeof(member->logical_route)) &&
         flow_fmq_deployment_text_valid(member->endpoint, sizeof(member->endpoint)) &&
         (member->state == TURBO_FLOW_FMQ_MEMBER_READY ||
          member->state == TURBO_FLOW_FMQ_MEMBER_DRAINING);
}

static int
flow_fmq_deployment_member_identity_equal(const flow_fmq_deployment_member_record_t *record,
                                          const turbo_flow_fmq_deployment_member_t *member) {
  return record && member && strcmp(record->value.member_id, member->member_id) == 0 &&
         memcmp(record->value.incarnation_id.bytes, member->incarnation_id.bytes,
                sizeof(member->incarnation_id.bytes)) == 0;
}

static ptrdiff_t
flow_fmq_deployment_member_find(const turbo_flow_fmq_deployment_controller_t *controller,
                                const char *member_id) {
  for (size_t i = 0u; i < flow_fmq_deployment_members_size(&controller->members); ++i) {
    const flow_fmq_deployment_member_record_t *record =
        flow_fmq_deployment_members_at_const(&controller->members, i);
    if (record && strcmp(record->value.member_id, member_id) == 0) return (ptrdiff_t)i;
  }
  return -1;
}

static ptrdiff_t
flow_fmq_deployment_route_find(const turbo_flow_fmq_deployment_controller_t *controller,
                               const char *logical_route) {
  for (size_t i = 0u; i < flow_fmq_deployment_routes_size(&controller->routes); ++i) {
    const turbo_flow_fmq_route_snapshot_t *route =
        flow_fmq_deployment_routes_at_const(&controller->routes, i);
    if (route && strcmp(route->logical_route, logical_route) == 0) return (ptrdiff_t)i;
  }
  return -1;
}

static int flow_fmq_deployment_member_eligible(const flow_fmq_deployment_member_record_t *record,
                                               const char *logical_route, uint64_t now_ms) {
  return record && record->value.state == TURBO_FLOW_FMQ_MEMBER_READY &&
         record->lease_deadline_ms > now_ms &&
         strcmp(record->value.logical_route, logical_route) == 0;
}

static int
flow_fmq_deployment_candidate_better(const flow_fmq_deployment_member_record_t *candidate,
                                     const flow_fmq_deployment_member_record_t *selected) {
  if (!selected) return 1;
  if (candidate->value.priority != selected->value.priority)
    return candidate->value.priority > selected->value.priority;
  return strcmp(candidate->value.member_id, selected->value.member_id) < 0;
}

static void flow_fmq_deployment_route_clear_primary(turbo_flow_fmq_route_snapshot_t *route) {
  route->has_primary = 0;
  route->primary_member_id[0] = '\0';
  memset(&route->primary_incarnation_id, 0, sizeof(route->primary_incarnation_id));
  route->primary_failure_domain[0] = '\0';
  route->endpoint[0] = '\0';
}

static void
flow_fmq_deployment_route_set_primary(turbo_flow_fmq_route_snapshot_t *route,
                                      const flow_fmq_deployment_member_record_t *record) {
  route->has_primary = 1;
  memcpy(route->primary_member_id, record->value.member_id, strlen(record->value.member_id) + 1u);
  route->primary_incarnation_id = record->value.incarnation_id;
  memcpy(route->primary_failure_domain, record->value.failure_domain,
         strlen(record->value.failure_domain) + 1u);
  memcpy(route->endpoint, record->value.endpoint, strlen(record->value.endpoint) + 1u);
}

/* O(m) election, with m <= configured member_capacity (maximum 256). */
static int flow_fmq_deployment_elect_route(turbo_flow_fmq_deployment_controller_t *controller,
                                           const char *logical_route, uint64_t now_ms,
                                           uint64_t *route_generation, int *changed) {
  turbo_flow_fmq_route_snapshot_t *route;
  const flow_fmq_deployment_member_record_t *selected = NULL;
  const flow_fmq_deployment_member_record_t *incumbent = NULL;
  ptrdiff_t route_index = flow_fmq_deployment_route_find(controller, logical_route);
  char previous_domain[TURBO_FLOW_FMQ_DEPLOYMENT_DOMAIN_MAX + 1u] = {0};
  int prefer_other_domain = 0;
  if (route_index < 0) {
    turbo_flow_fmq_route_snapshot_t created = TURBO_FLOW_FMQ_ROUTE_SNAPSHOT_INIT;
    if (flow_fmq_deployment_routes_size(&controller->routes) >= controller->config.member_capacity)
      return TURBO_ENOSPC;
    memcpy(created.logical_route, logical_route, strlen(logical_route) + 1u);
    if (flow_fmq_deployment_routes_push(&controller->routes, created) != TURBO_OK)
      return TURBO_ENOMEM;
    route_index = (ptrdiff_t)flow_fmq_deployment_routes_size(&controller->routes) - 1;
  }
  route = flow_fmq_deployment_routes_at(&controller->routes, (size_t)route_index);
  if (!route) return TURBO_EPROTO;

  if (route->has_primary) {
    memcpy(previous_domain, route->primary_failure_domain,
           strlen(route->primary_failure_domain) + 1u);
    for (size_t i = 0u; i < flow_fmq_deployment_members_size(&controller->members); ++i) {
      const flow_fmq_deployment_member_record_t *candidate =
          flow_fmq_deployment_members_at_const(&controller->members, i);
      if (candidate && strcmp(candidate->value.member_id, route->primary_member_id) == 0 &&
          memcmp(candidate->value.incarnation_id.bytes, route->primary_incarnation_id.bytes,
                 sizeof(candidate->value.incarnation_id.bytes)) == 0 &&
          flow_fmq_deployment_member_eligible(candidate, logical_route, now_ms)) {
        incumbent = candidate;
        break;
      }
    }
  }
  if (incumbent) {
    selected = incumbent;
  } else {
    if (route->has_primary) {
      for (size_t i = 0u; i < flow_fmq_deployment_members_size(&controller->members); ++i) {
        const flow_fmq_deployment_member_record_t *candidate =
            flow_fmq_deployment_members_at_const(&controller->members, i);
        if (flow_fmq_deployment_member_eligible(candidate, logical_route, now_ms) &&
            strcmp(candidate->value.failure_domain, previous_domain) != 0) {
          prefer_other_domain = 1;
          break;
        }
      }
    }
    for (size_t i = 0u; i < flow_fmq_deployment_members_size(&controller->members); ++i) {
      const flow_fmq_deployment_member_record_t *candidate =
          flow_fmq_deployment_members_at_const(&controller->members, i);
      if (!flow_fmq_deployment_member_eligible(candidate, logical_route, now_ms) ||
          (prefer_other_domain && strcmp(candidate->value.failure_domain, previous_domain) == 0))
        continue;
      if (flow_fmq_deployment_candidate_better(candidate, selected)) selected = candidate;
    }
  }

  *changed = 0;
  if (!selected) {
    if (route->has_primary) {
      if (route->route_generation == UINT64_MAX) return TURBO_ERANGE;
      ++route->route_generation;
      flow_fmq_deployment_route_clear_primary(route);
      *changed = 1;
    }
  } else if (!route->has_primary ||
             strcmp(route->primary_member_id, selected->value.member_id) != 0 ||
             memcmp(route->primary_incarnation_id.bytes, selected->value.incarnation_id.bytes,
                    sizeof(selected->value.incarnation_id.bytes)) != 0) {
    if (route->route_generation == UINT64_MAX) return TURBO_ERANGE;
    ++route->route_generation;
    flow_fmq_deployment_route_set_primary(route, selected);
    *changed = 1;
  } else if (strcmp(route->endpoint, selected->value.endpoint) != 0 ||
             strcmp(route->primary_failure_domain, selected->value.failure_domain) != 0) {
    if (route->route_generation == UINT64_MAX) return TURBO_ERANGE;
    ++route->route_generation;
    flow_fmq_deployment_route_set_primary(route, selected);
    *changed = 1;
  }
  *route_generation = route->route_generation;
  return TURBO_OK;
}

static int
flow_fmq_deployment_command_authorized(const turbo_flow_fmq_deployment_controller_t *controller,
                                       const turbo_flow_fmq_membership_command_t *command) {
  return strcmp(controller->config.authority_id, command->authority_id) == 0 &&
         controller->config.authority_epoch == command->authority_epoch &&
         memcmp(controller->incarnation_id.bytes, command->authority_incarnation_id.bytes,
                sizeof(controller->incarnation_id.bytes)) == 0;
}

int turbo_flow_fmq_deployment_controller_create(const turbo_flow_fmq_deployment_config_t *config,
                                                turbo_flow_fmq_deployment_controller_t **out) {
  turbo_flow_fmq_deployment_controller_t *controller;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out ||
      !flow_fmq_deployment_text_valid(config->authority_id, sizeof(config->authority_id)) ||
      config->authority_epoch == 0u || config->lease_timeout_ms == 0u ||
      config->member_capacity == 0u ||
      config->member_capacity > TURBO_FLOW_FMQ_DEPLOYMENT_MAX_MEMBERS)
    return TURBO_EINVAL;
  controller = (turbo_flow_fmq_deployment_controller_t *)calloc(1u, sizeof(*controller));
  if (!controller) return TURBO_ENOMEM;
  controller->config = *config;
  if (flow_fmq_deployment_members_init(&controller->members) != TURBO_OK ||
      flow_fmq_deployment_routes_init(&controller->routes) != TURBO_OK ||
      flow_fmq_deployment_members_reserve(&controller->members, config->member_capacity) !=
          TURBO_OK ||
      flow_fmq_deployment_routes_reserve(&controller->routes, config->member_capacity) !=
          TURBO_OK) {
    turbo_flow_fmq_deployment_controller_destroy(controller);
    return TURBO_ENOMEM;
  }
  rc = turbo_uuid_v7_generate(&controller->incarnation_id);
  if (rc != TURBO_OK) {
    turbo_flow_fmq_deployment_controller_destroy(controller);
    return rc;
  }
  controller->registry_version = 1u;
  *out = controller;
  return TURBO_OK;
}

void turbo_flow_fmq_deployment_controller_destroy(
    turbo_flow_fmq_deployment_controller_t *controller) {
  if (!controller) return;
  flow_fmq_deployment_routes_destroy(&controller->routes);
  flow_fmq_deployment_members_destroy(&controller->members);
  free(controller);
}

int turbo_flow_fmq_deployment_apply(turbo_flow_fmq_deployment_controller_t *controller,
                                    const turbo_flow_fmq_membership_command_t *command,
                                    turbo_flow_fmq_membership_result_t *result) {
  flow_fmq_deployment_member_record_t *record = NULL;
  uint64_t now_ms;
  uint64_t route_generation = 0u;
  ptrdiff_t index;
  ptrdiff_t route_index;
  int primary_changed = 0;
  int rc;
  if (!controller || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) || !flow_fmq_deployment_member_valid(&command->member) ||
      command->kind < TURBO_FLOW_FMQ_MEMBERSHIP_JOIN ||
      command->kind > TURBO_FLOW_FMQ_MEMBERSHIP_LEAVE)
    return TURBO_EINVAL;
  *result = (turbo_flow_fmq_membership_result_t)TURBO_FLOW_FMQ_MEMBERSHIP_RESULT_INIT;
  result->version_before = controller->registry_version;
  result->version_after = controller->registry_version;
  if (!flow_fmq_deployment_command_authorized(controller, command)) return TURBO_EPERM;
  if (command->expected_registry_version < controller->registry_version) return TURBO_EALREADY;
  if (command->expected_registry_version > controller->registry_version) return TURBO_EPROTO;
  if (controller->registry_version == UINT64_MAX) return TURBO_ERANGE;
  now_ms = flow_fmq_deployment_now_ms(controller);
  index = flow_fmq_deployment_member_find(controller, command->member.member_id);
  route_index = flow_fmq_deployment_route_find(controller, command->member.logical_route);
  if (route_index < 0 &&
      flow_fmq_deployment_routes_size(&controller->routes) >= controller->config.member_capacity)
    return TURBO_ENOSPC;
  if (route_index >= 0 && command->kind != TURBO_FLOW_FMQ_MEMBERSHIP_HEARTBEAT) {
    const turbo_flow_fmq_route_snapshot_t *route =
        flow_fmq_deployment_routes_at_const(&controller->routes, (size_t)route_index);
    if (route && route->route_generation == UINT64_MAX) return TURBO_ERANGE;
  }
  if (index >= 0) record = flow_fmq_deployment_members_at(&controller->members, (size_t)index);

  if (command->kind == TURBO_FLOW_FMQ_MEMBERSHIP_JOIN) {
    flow_fmq_deployment_member_record_t added;
    if (record && !flow_fmq_deployment_member_identity_equal(record, &command->member) &&
        record->lease_deadline_ms > now_ms)
      return TURBO_EBUSY;
    if (!record) {
      if (flow_fmq_deployment_members_size(&controller->members) >=
          controller->config.member_capacity)
        return TURBO_ENOSPC;
      memset(&added, 0, sizeof(added));
      added.value = command->member;
      added.value.lease_remaining_ms = 0u;
      added.lease_deadline_ms = now_ms > UINT64_MAX - controller->config.lease_timeout_ms
                                    ? UINT64_MAX
                                    : now_ms + controller->config.lease_timeout_ms;
      if (flow_fmq_deployment_members_push(&controller->members, added) != TURBO_OK)
        return TURBO_ENOMEM;
      record = flow_fmq_deployment_members_at(
          &controller->members, flow_fmq_deployment_members_size(&controller->members) - 1u);
    } else {
      record->value = command->member;
      record->value.lease_remaining_ms = 0u;
      record->lease_deadline_ms = now_ms > UINT64_MAX - controller->config.lease_timeout_ms
                                      ? UINT64_MAX
                                      : now_ms + controller->config.lease_timeout_ms;
    }
  } else {
    if (!record) return TURBO_ENOENT;
    if (!flow_fmq_deployment_member_identity_equal(record, &command->member)) return TURBO_EPERM;
    if (strcmp(record->value.logical_route, command->member.logical_route) != 0 ||
        strcmp(record->value.failure_domain, command->member.failure_domain) != 0)
      return TURBO_EPROTO;
    if (command->kind == TURBO_FLOW_FMQ_MEMBERSHIP_HEARTBEAT) {
      if (strcmp(record->value.endpoint, command->member.endpoint) != 0 ||
          record->value.priority != command->member.priority ||
          record->value.state != command->member.state)
        return TURBO_EPROTO;
      record->lease_deadline_ms = now_ms > UINT64_MAX - controller->config.lease_timeout_ms
                                      ? UINT64_MAX
                                      : now_ms + controller->config.lease_timeout_ms;
    } else if (command->kind == TURBO_FLOW_FMQ_MEMBERSHIP_DRAIN) {
      record->value.state = TURBO_FLOW_FMQ_MEMBER_DRAINING;
    } else {
      rc = turbo_vec_swap_remove(&controller->members.raw, (size_t)index, NULL);
      if (rc != TURBO_OK) return rc;
      record = NULL;
    }
  }
  rc = flow_fmq_deployment_elect_route(controller, command->member.logical_route, now_ms,
                                       &route_generation, &primary_changed);
  if (rc != TURBO_OK) return rc;
  ++controller->registry_version;
  result->version_after = controller->registry_version;
  result->route_generation = route_generation;
  result->primary_changed = primary_changed;
  return TURBO_OK;
}

int turbo_flow_fmq_deployment_tick(turbo_flow_fmq_deployment_controller_t *controller,
                                   size_t *expired_count) {
  char affected[TURBO_FLOW_FMQ_DEPLOYMENT_MAX_MEMBERS][TURBO_FLOW_FMQ_DEPLOYMENT_ROUTE_MAX + 1u];
  size_t affected_count = 0u;
  uint64_t now_ms;
  int rc;
  if (!controller || !expired_count) return TURBO_EINVAL;
  *expired_count = 0u;
  now_ms = flow_fmq_deployment_now_ms(controller);
  for (size_t i = 0u; i < flow_fmq_deployment_members_size(&controller->members); ++i) {
    const flow_fmq_deployment_member_record_t *record =
        flow_fmq_deployment_members_at_const(&controller->members, i);
    ptrdiff_t route_index;
    const turbo_flow_fmq_route_snapshot_t *route;
    if (!record || record->lease_deadline_ms > now_ms) continue;
    if (controller->registry_version == UINT64_MAX) return TURBO_ERANGE;
    route_index = flow_fmq_deployment_route_find(controller, record->value.logical_route);
    if (route_index < 0) return TURBO_EPROTO;
    route = flow_fmq_deployment_routes_at_const(&controller->routes, (size_t)route_index);
    if (route && route->has_primary && route->route_generation == UINT64_MAX &&
        strcmp(route->primary_member_id, record->value.member_id) == 0 &&
        memcmp(route->primary_incarnation_id.bytes, record->value.incarnation_id.bytes,
               sizeof(record->value.incarnation_id.bytes)) == 0)
      return TURBO_ERANGE;
  }
  for (size_t i = flow_fmq_deployment_members_size(&controller->members); i > 0u; --i) {
    flow_fmq_deployment_member_record_t *record =
        flow_fmq_deployment_members_at(&controller->members, i - 1u);
    int seen = 0;
    if (!record || record->lease_deadline_ms > now_ms) continue;
    for (size_t j = 0u; j < affected_count; ++j)
      if (strcmp(affected[j], record->value.logical_route) == 0) seen = 1;
    if (!seen) {
      memcpy(affected[affected_count], record->value.logical_route,
             strlen(record->value.logical_route) + 1u);
      ++affected_count;
    }
    rc = turbo_vec_swap_remove(&controller->members.raw, i - 1u, NULL);
    if (rc != TURBO_OK) return rc;
    ++*expired_count;
  }
  if (*expired_count == 0u) return TURBO_OK;
  for (size_t i = 0u; i < affected_count; ++i) {
    uint64_t generation = 0u;
    int changed = 0;
    rc = flow_fmq_deployment_elect_route(controller, affected[i], now_ms, &generation, &changed);
    if (rc != TURBO_OK) return rc;
  }
  ++controller->registry_version;
  return TURBO_OK;
}

int turbo_flow_fmq_deployment_resolve(const turbo_flow_fmq_deployment_controller_t *controller,
                                      const char *logical_route,
                                      turbo_flow_fmq_route_snapshot_t *route) {
  ptrdiff_t index;
  if (!controller || !logical_route || !route || route->size < sizeof(*route) ||
      !flow_fmq_deployment_text_valid(logical_route, strlen(logical_route) + 1u))
    return TURBO_EINVAL;
  index = flow_fmq_deployment_route_find(controller, logical_route);
  if (index < 0) return TURBO_ENOENT;
  *route = *flow_fmq_deployment_routes_at_const(&controller->routes, (size_t)index);
  return route->has_primary ? TURBO_OK : TURBO_ENOTCONN;
}

static int flow_fmq_deployment_member_compare(const void *left, const void *right) {
  const turbo_flow_fmq_deployment_member_t *a = (const turbo_flow_fmq_deployment_member_t *)left;
  const turbo_flow_fmq_deployment_member_t *b = (const turbo_flow_fmq_deployment_member_t *)right;
  int route_order = strcmp(a->logical_route, b->logical_route);
  return route_order != 0 ? route_order : strcmp(a->member_id, b->member_id);
}

static int flow_fmq_deployment_route_compare(const void *left, const void *right) {
  return strcmp(((const turbo_flow_fmq_route_snapshot_t *)left)->logical_route,
                ((const turbo_flow_fmq_route_snapshot_t *)right)->logical_route);
}

int turbo_flow_fmq_deployment_snapshot(const turbo_flow_fmq_deployment_controller_t *controller,
                                       turbo_flow_fmq_deployment_snapshot_t *snapshot) {
  uint64_t now_ms;
  size_t member_count;
  size_t route_count;
  if (!controller || !snapshot || snapshot->size < sizeof(*snapshot) ||
      (!snapshot->members && snapshot->member_capacity > 0u) ||
      (!snapshot->routes && snapshot->route_capacity > 0u))
    return TURBO_EINVAL;
  member_count = flow_fmq_deployment_members_size(&controller->members);
  route_count = flow_fmq_deployment_routes_size(&controller->routes);
  snapshot->member_count = member_count;
  snapshot->route_count = route_count;
  if (snapshot->member_capacity < member_count || snapshot->route_capacity < route_count)
    return TURBO_ENOSPC;
  now_ms = flow_fmq_deployment_now_ms(controller);
  memcpy(snapshot->authority_id, controller->config.authority_id,
         strlen(controller->config.authority_id) + 1u);
  snapshot->authority_epoch = controller->config.authority_epoch;
  snapshot->authority_incarnation_id = controller->incarnation_id;
  snapshot->registry_version = controller->registry_version;
  for (size_t i = 0u; i < member_count; ++i) {
    const flow_fmq_deployment_member_record_t *record =
        flow_fmq_deployment_members_at_const(&controller->members, i);
    snapshot->members[i] = record->value;
    snapshot->members[i].lease_remaining_ms =
        record->lease_deadline_ms > now_ms ? record->lease_deadline_ms - now_ms : 0u;
  }
  for (size_t i = 0u; i < route_count; ++i)
    snapshot->routes[i] = *flow_fmq_deployment_routes_at_const(&controller->routes, i);
  qsort(snapshot->members, member_count, sizeof(*snapshot->members),
        flow_fmq_deployment_member_compare);
  qsort(snapshot->routes, route_count, sizeof(*snapshot->routes),
        flow_fmq_deployment_route_compare);
  return TURBO_OK;
}

static int
flow_fmq_deployment_member_snapshot_equal(const turbo_flow_fmq_deployment_member_t *left,
                                          const turbo_flow_fmq_deployment_member_t *right) {
  return strcmp(left->member_id, right->member_id) == 0 &&
         memcmp(left->incarnation_id.bytes, right->incarnation_id.bytes,
                sizeof(left->incarnation_id.bytes)) == 0 &&
         strcmp(left->failure_domain, right->failure_domain) == 0 &&
         strcmp(left->logical_route, right->logical_route) == 0 &&
         strcmp(left->endpoint, right->endpoint) == 0 && left->priority == right->priority &&
         left->state == right->state;
}

static int flow_fmq_deployment_route_snapshot_equal(const turbo_flow_fmq_route_snapshot_t *left,
                                                    const turbo_flow_fmq_route_snapshot_t *right) {
  return strcmp(left->logical_route, right->logical_route) == 0 &&
         left->route_generation == right->route_generation &&
         left->has_primary == right->has_primary &&
         strcmp(left->primary_member_id, right->primary_member_id) == 0 &&
         memcmp(left->primary_incarnation_id.bytes, right->primary_incarnation_id.bytes,
                sizeof(left->primary_incarnation_id.bytes)) == 0 &&
         strcmp(left->primary_failure_domain, right->primary_failure_domain) == 0 &&
         strcmp(left->endpoint, right->endpoint) == 0;
}

int turbo_flow_fmq_deployment_snapshot_compare(
    const turbo_flow_fmq_deployment_snapshot_t *current,
    const turbo_flow_fmq_deployment_snapshot_t *candidate,
    turbo_flow_fmq_snapshot_relation_t *relation) {
  int content_equal = 1;
  if (!current || current->size < sizeof(*current) || !candidate ||
      candidate->size < sizeof(*candidate) || !relation ||
      !flow_fmq_deployment_text_valid(current->authority_id, sizeof(current->authority_id)) ||
      !flow_fmq_deployment_text_valid(candidate->authority_id, sizeof(candidate->authority_id)) ||
      (current->member_count > 0u && !current->members) ||
      (candidate->member_count > 0u && !candidate->members) ||
      (current->route_count > 0u && !current->routes) ||
      (candidate->route_count > 0u && !candidate->routes))
    return TURBO_EINVAL;
  if (strcmp(current->authority_id, candidate->authority_id) != 0) {
    *relation = TURBO_FLOW_FMQ_SNAPSHOT_SPLIT_BRAIN;
    return TURBO_OK;
  }
  if (candidate->authority_epoch < current->authority_epoch) {
    *relation = TURBO_FLOW_FMQ_SNAPSHOT_STALE;
    return TURBO_OK;
  }
  if (candidate->authority_epoch > current->authority_epoch) {
    *relation = TURBO_FLOW_FMQ_SNAPSHOT_NEWER;
    return TURBO_OK;
  }
  if (memcmp(current->authority_incarnation_id.bytes, candidate->authority_incarnation_id.bytes,
             sizeof(current->authority_incarnation_id.bytes)) != 0) {
    *relation = TURBO_FLOW_FMQ_SNAPSHOT_SPLIT_BRAIN;
    return TURBO_OK;
  }
  if (candidate->registry_version < current->registry_version) {
    *relation = TURBO_FLOW_FMQ_SNAPSHOT_STALE;
    return TURBO_OK;
  }
  if (candidate->registry_version > current->registry_version) {
    *relation = TURBO_FLOW_FMQ_SNAPSHOT_NEWER;
    return TURBO_OK;
  }
  if (current->member_count != candidate->member_count ||
      current->route_count != candidate->route_count) {
    content_equal = 0;
  }
  for (size_t i = 0u; content_equal && i < current->member_count; ++i)
    content_equal =
        flow_fmq_deployment_member_snapshot_equal(&current->members[i], &candidate->members[i]);
  for (size_t i = 0u; content_equal && i < current->route_count; ++i)
    content_equal =
        flow_fmq_deployment_route_snapshot_equal(&current->routes[i], &candidate->routes[i]);
  *relation = content_equal ? TURBO_FLOW_FMQ_SNAPSHOT_SAME : TURBO_FLOW_FMQ_SNAPSHOT_SPLIT_BRAIN;
  return TURBO_OK;
}
