#include "flowmq_destination.h"

#include <cnet/destination_policy.h>
#include <salts/clock.h>
#include <salts/error_codes.h>

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

enum { FLOWMQ_DESTINATION_URI_CAPACITY = 320u };

static int flowmq_destination_uri_scheme(const char *uri) {
  size_t count = 0u;
  if (uri == NULL) return 0;
  while (count < FLOWMQ_DESTINATION_URI_CAPACITY && uri[count] != '\0')
    ++count;
  if (count <= 6u || count >= FLOWMQ_DESTINATION_URI_CAPACITY)
    return 0;
  if (strncmp(uri, "tcp://", 6u) == 0) return 1;
  if (strncmp(uri, "tls://", 6u) == 0) return 2;
  return 0;
}

static int flowmq_destination_policy_kind(
    flowmq_destination_kind_t kind, cnet_destination_policy_kind *out) {
  if (out == NULL) return SALTS_EINVAL;
  switch (kind) {
    case FLOWMQ_DESTINATION_EXPLICIT:
      *out = CNET_DESTINATION_EXPLICIT;
      break;
    case FLOWMQ_DESTINATION_ROUND_ROBIN:
      *out = CNET_DESTINATION_ROUND_ROBIN;
      break;
    case FLOWMQ_DESTINATION_WEIGHTED_RR:
      *out = CNET_DESTINATION_WEIGHTED_RR;
      break;
    case FLOWMQ_DESTINATION_LEAST_INFLIGHT:
      *out = CNET_DESTINATION_LEAST_INFLIGHT;
      break;
    case FLOWMQ_DESTINATION_STRICT_KEY:
      *out = CNET_DESTINATION_STRICT_KEY;
      break;
    default:
      return SALTS_EINVAL;
  }
  return SALTS_OK;
}

int flowmq_destination_choose(
    const flowmq_destination_endpoint_t *endpoints, size_t endpoint_count,
    const flowmq_destination_selection_t *selection,
    flowmq_destination_result_t *result) {
  cnet_destination_hint hints[FLOWMQ_DESTINATION_MAX_ENDPOINTS] = {{0}};
  cnet_destination_selection policy = {0};
  cnet_destination_result selected = {0};
  cnet_destination_policy_kind kind;
  uint64_t authority_id = 0u;
  int scheme = 0;
  int status;

  if (result == NULL || result->size < sizeof(*result)) return SALTS_EINVAL;
  result->snapshot_generation = 0u;
  result->endpoint_id = 0u;
  result->index = SIZE_MAX;
  if (endpoints == NULL || endpoint_count == 0u ||
      endpoint_count > FLOWMQ_DESTINATION_MAX_ENDPOINTS ||
      selection == NULL || selection->size < sizeof(*selection) ||
      selection->version != FLOWMQ_DESTINATION_VERSION ||
      selection->snapshot_generation == 0u ||
      (selection->key_known != 0 && selection->key_known != 1))
    return SALTS_EINVAL;

  status = flowmq_destination_policy_kind(selection->kind, &kind);
  if (status != SALTS_OK) return status;

  for (size_t i = 0u; i < endpoint_count; ++i) {
    const flowmq_destination_endpoint_t *endpoint = &endpoints[i];
    const int current_scheme = flowmq_destination_uri_scheme(endpoint->uri);
    if (endpoint->authority_id == 0u || current_scheme == 0 ||
        (i != 0u && (authority_id != endpoint->authority_id ||
                     scheme != current_scheme)) ||
        (endpoint->eligible != 0 && endpoint->eligible != 1))
      return SALTS_EINVAL;
    authority_id = endpoint->authority_id;
    scheme = current_scheme;
    hints[i] = (cnet_destination_hint){
        .endpoint_id = endpoint->endpoint_id,
        .weight = endpoint->weight,
        .inflight = endpoint->inflight,
        .eligible = endpoint->eligible != 0};
  }

  policy = (cnet_destination_selection){
      .size = sizeof(policy),
      .version = CNET_DESTINATION_POLICY_VERSION,
      .kind = kind,
      .endpoints = hints,
      .endpoint_count = endpoint_count,
      .snapshot_generation = selection->snapshot_generation,
      .expires_at_ms = selection->expires_at_ms,
      .now_ms = cmeta_monotonic_ms(),
      .sequence = selection->sequence,
      .explicit_endpoint_id = selection->explicit_endpoint_id,
      .key_hash = selection->key_hash,
      .key_known = selection->key_known != 0};
  status = cnet_destination_choose(&policy, &selected);
  if (status != SALTS_OK) return status;
  if (selected.index >= endpoint_count ||
      selected.endpoint_id != endpoints[selected.index].endpoint_id)
    return SALTS_EPROTO;

  result->snapshot_generation = selected.snapshot_generation;
  result->endpoint_id = selected.endpoint_id;
  result->index = selected.index;
  return SALTS_OK;
}

int flowmq_connect_selected(
    flowmq_socket_t *socket,
    const flowmq_destination_endpoint_t *endpoints, size_t endpoint_count,
    const flowmq_destination_selection_t *selection,
    flowmq_destination_result_t *result) {
  int status;
  if (socket == NULL) return SALTS_EINVAL;
  status = flowmq_destination_choose(
      endpoints, endpoint_count, selection, result);
  if (status != SALTS_OK) return status;
  /* No second policy evaluation, endpoint fallback or implicit DATA replay. */
  status = flowmq_connect(socket, endpoints[result->index].uri);
  if (status != SALTS_OK) {
    result->snapshot_generation = 0u;
    result->endpoint_id = 0u;
    result->index = SIZE_MAX;
  }
  return status;
}
