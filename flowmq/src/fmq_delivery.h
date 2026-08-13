#ifndef TURBO_FLOW_FMQ_DELIVERY_H
#define TURBO_FLOW_FMQ_DELIVERY_H

#include "turbo_error.h"
#include "turbo_flow_protocol.h"

#include <stdint.h>

typedef struct flow_fmq_route_token_s {
  uint64_t session_id;
  uint64_t generation;
} flow_fmq_route_token_t;

static inline int flow_fmq_route_matches(uint64_t current_generation,
                                         flow_fmq_route_token_t requested,
                                         flow_fmq_route_token_t peer) {
  turbo_flow_pattern_route_t requested_route = TURBO_FLOW_PATTERN_ROUTE_INIT;
  turbo_flow_pattern_route_t peer_route = TURBO_FLOW_PATTERN_ROUTE_INIT;
  requested_route.route_id = requested.session_id;
  requested_route.generation = requested.generation;
  peer_route.route_id = peer.session_id;
  peer_route.generation = peer.generation;
  return current_generation != 0u && requested.generation == current_generation &&
         turbo_flow_pattern_routes_match(&requested_route, &peer_route);
}

typedef enum flow_fmq_delivery_stage_e {
  FLOW_FMQ_DELIVERY_NOT_SUBMITTED = 0,
  FLOW_FMQ_DELIVERY_WRITE_STARTED,
  FLOW_FMQ_DELIVERY_PARTIAL,
  FLOW_FMQ_DELIVERY_DELIVERED
} flow_fmq_delivery_stage_t;

static inline int flow_fmq_delivery_status_retryable(int status) {
  switch (status) {
  case TURBO_ETIMEDOUT:
  case TURBO_ENOTCONN:
  case TURBO_ECONNABORTED:
  case TURBO_ECONNREFUSED:
  case TURBO_ECONNRESET:
  case TURBO_EHOSTUNREACH:
  case TURBO_ENETDOWN:
  case TURBO_ENETUNREACH:
  case TURBO_EPIPE:
  case TURBO_EIO:
    return 1;
  default:
    return 0;
  }
}

static inline int flow_fmq_delivery_retryable(flow_fmq_delivery_stage_t stage, int status) {
  return stage == FLOW_FMQ_DELIVERY_NOT_SUBMITTED &&
         flow_fmq_delivery_status_retryable(status);
}

static inline int flow_fmq_delivery_requires_session_reset(flow_fmq_delivery_stage_t stage) {
  return stage != FLOW_FMQ_DELIVERY_NOT_SUBMITTED;
}

#endif /* TURBO_FLOW_FMQ_DELIVERY_H */
