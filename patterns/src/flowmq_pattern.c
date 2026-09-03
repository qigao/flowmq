#include "flowmq_pattern.h"
#include "flowmq_protocol_esb.h"

#include "turbo_error.h"

int flowmq_pattern_validate(flowmq_protocol_pattern_t pattern) {
  /* Base FlowMQ patterns: 1-11 */
  if (pattern >= FLOWMQ_PROTOCOL_PUB && pattern <= FLOWMQ_PROTOCOL_XSUB) {
    return TURBO_OK;
  }
  
  /* ESB patterns: 12-31 */
  if (pattern >= FLOWMQ_PROTOCOL_ESB_PATTERN_MIN && 
      pattern <= FLOWMQ_PROTOCOL_ESB_PATTERN_MAX) {
    return flowmq_protocol_esb_pattern_validate(pattern);
  }
  
  return TURBO_EINVAL;
}

int flowmq_patterns_compatible(flowmq_protocol_pattern_t local, flowmq_protocol_pattern_t remote) {
  if (flowmq_pattern_validate(local) != TURBO_OK || flowmq_pattern_validate(remote) != TURBO_OK)
    return 0;
  
  /* Check if both are ESB patterns */
  int local_is_esb = (local >= FLOWMQ_PROTOCOL_ESB_PATTERN_MIN && 
                      local <= FLOWMQ_PROTOCOL_ESB_PATTERN_MAX);
  int remote_is_esb = (remote >= FLOWMQ_PROTOCOL_ESB_PATTERN_MIN && 
                       remote <= FLOWMQ_PROTOCOL_ESB_PATTERN_MAX);
  
  /* ESB patterns can only connect to other ESB patterns */
  if (local_is_esb != remote_is_esb) {
    return 0;
  }
  
  /* Both are ESB patterns */
  if (local_is_esb && remote_is_esb) {
    return flowmq_protocol_esb_patterns_compatible(local, remote);
  }
  
  /* Both are base FlowMQ patterns - original logic */
  switch (local) {
  case FLOWMQ_PROTOCOL_PUB:
    return remote == FLOWMQ_PROTOCOL_SUB || remote == FLOWMQ_PROTOCOL_XSUB;
  case FLOWMQ_PROTOCOL_SUB:
    return remote == FLOWMQ_PROTOCOL_PUB || remote == FLOWMQ_PROTOCOL_XPUB;
  case FLOWMQ_PROTOCOL_PUSH:
    return remote == FLOWMQ_PROTOCOL_PULL;
  case FLOWMQ_PROTOCOL_PULL:
    return remote == FLOWMQ_PROTOCOL_PUSH;
  case FLOWMQ_PROTOCOL_ROUTER:
    return remote == FLOWMQ_PROTOCOL_DEALER || remote == FLOWMQ_PROTOCOL_REQ ||
           remote == FLOWMQ_PROTOCOL_ROUTER;
  case FLOWMQ_PROTOCOL_DEALER:
    return remote == FLOWMQ_PROTOCOL_ROUTER || remote == FLOWMQ_PROTOCOL_REP ||
           remote == FLOWMQ_PROTOCOL_DEALER;
  case FLOWMQ_PROTOCOL_PAIR:
    return remote == FLOWMQ_PROTOCOL_PAIR;
  case FLOWMQ_PROTOCOL_REQ:
    return remote == FLOWMQ_PROTOCOL_REP || remote == FLOWMQ_PROTOCOL_ROUTER;
  case FLOWMQ_PROTOCOL_REP:
    return remote == FLOWMQ_PROTOCOL_REQ || remote == FLOWMQ_PROTOCOL_DEALER;
  case FLOWMQ_PROTOCOL_XPUB:
    return remote == FLOWMQ_PROTOCOL_SUB || remote == FLOWMQ_PROTOCOL_XSUB;
  case FLOWMQ_PROTOCOL_XSUB:
    return remote == FLOWMQ_PROTOCOL_PUB || remote == FLOWMQ_PROTOCOL_XPUB;
  default:
    return 0;
  }
}

int flowmq_pattern_hello_validate(flowmq_protocol_pattern_t local,
                                  const flowmq_protocol_frame_t *hello) {
  flowmq_protocol_security_t security;
  if (flowmq_pattern_validate(local) != TURBO_OK || !hello) return TURBO_EINVAL;
  if (hello->kind != FLOWMQ_PROTOCOL_FRAME_HELLO ||
      flowmq_protocol_security_decode(hello->payload, &security) != TURBO_OK ||
      !flowmq_patterns_compatible(local, hello->pattern) ||
      (hello->pattern == FLOWMQ_PROTOCOL_DEALER && hello->identity.len == 0u))
    return TURBO_EPROTO;
  return TURBO_OK;
}

int flowmq_pattern_socket_hello_validate(flowmq_protocol_pattern_t local,
                                         const flowmq_protocol_frame_t *hello) {
  int status = flowmq_pattern_hello_validate(local, hello);
  if (status != TURBO_OK) return status;
  /* FMS/3 is only a wire envelope until a credential verifier and TLS
   * principal binding are configured by the socket runtime. */
  return hello->payload.len == 0u ? TURBO_OK : TURBO_EPROTO;
}

int flowmq_pattern_data_direction_validate(flowmq_protocol_pattern_t local,
                                           const flowmq_protocol_frame_t *frame) {
  if (flowmq_pattern_validate(local) != TURBO_OK || !frame) return TURBO_EINVAL;
  if (!flowmq_patterns_compatible(local, frame->pattern)) return TURBO_EPROTO;
  if (frame->kind == FLOWMQ_PROTOCOL_FRAME_PING ||
      frame->kind == FLOWMQ_PROTOCOL_FRAME_PONG ||
      frame->kind == FLOWMQ_PROTOCOL_FRAME_SETTINGS ||
      frame->kind == FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE) {
    return TURBO_OK;
  }
  if (frame->pattern >= FLOWMQ_PROTOCOL_ESB_PATTERN_MIN &&
      frame->pattern <= FLOWMQ_PROTOCOL_ESB_PATTERN_MAX) {
    if (frame->kind < FLOWMQ_PROTOCOL_ESB_FRAME_MIN ||
        frame->kind > FLOWMQ_PROTOCOL_ESB_FRAME_MAX) {
      return TURBO_EPROTO;
    }
    return TURBO_OK;
  }
  if (frame->kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE ||
      frame->kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE) {
    return (local == FLOWMQ_PROTOCOL_PUB || local == FLOWMQ_PROTOCOL_XPUB) &&
                   (frame->pattern == FLOWMQ_PROTOCOL_SUB ||
                    frame->pattern == FLOWMQ_PROTOCOL_XSUB)
               ? TURBO_OK
               : TURBO_EPROTO;
  }
  if (frame->kind != FLOWMQ_PROTOCOL_FRAME_DATA) return TURBO_EPROTO;
  if (local == FLOWMQ_PROTOCOL_PUB || local == FLOWMQ_PROTOCOL_PUSH ||
      local == FLOWMQ_PROTOCOL_XPUB || frame->pattern == FLOWMQ_PROTOCOL_SUB ||
      frame->pattern == FLOWMQ_PROTOCOL_PULL || frame->pattern == FLOWMQ_PROTOCOL_XSUB)
    return TURBO_EPROTO;
  return TURBO_OK;
}

int flowmq_pattern_encode_hello(flowmq_protocol_pattern_t pattern, vstr identity, vstr topic,
                                size_t max_frame_size, tstr *encoded) {
  return flowmq_pattern_encode_hello_ex(pattern, identity, topic, (vstr){0}, max_frame_size,
                                        encoded);
}

int flowmq_pattern_encode_hello_ex(flowmq_protocol_pattern_t pattern, vstr identity, vstr topic,
                                   vstr security_payload, size_t max_frame_size,
                                   tstr *encoded) {
  flowmq_protocol_frame_t frame;
  if (flowmq_pattern_validate(pattern) != TURBO_OK || !encoded) return TURBO_EINVAL;
  frame = (flowmq_protocol_frame_t){0};
  frame.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
  frame.pattern = pattern;
  frame.identity = identity;
  frame.topic = topic;
  frame.payload = security_payload;
  return flowmq_protocol_encode_frame(&frame, max_frame_size, encoded);
}

int flowmq_pattern_encode_heartbeat(flowmq_protocol_pattern_t pattern,
                                    flowmq_protocol_frame_kind_t kind, size_t max_frame_size,
                                    tstr *encoded) {
  flowmq_protocol_frame_t frame;
  if (flowmq_pattern_validate(pattern) != TURBO_OK || !encoded ||
      (kind != FLOWMQ_PROTOCOL_FRAME_PING && kind != FLOWMQ_PROTOCOL_FRAME_PONG))
    return TURBO_EINVAL;
  frame = (flowmq_protocol_frame_t){0};
  frame.kind = kind;
  frame.pattern = pattern;
  return flowmq_protocol_encode_frame(&frame, max_frame_size, encoded);
}

int flowmq_pattern_encode_subscription(flowmq_protocol_pattern_t pattern,
                                       flowmq_protocol_frame_kind_t kind, vstr topic,
                                       size_t max_frame_size, tstr *encoded) {
  flowmq_protocol_frame_t frame;
  if ((pattern != FLOWMQ_PROTOCOL_SUB && pattern != FLOWMQ_PROTOCOL_XSUB) || !encoded ||
      (kind != FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE && kind != FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE))
    return TURBO_EINVAL;
  frame = (flowmq_protocol_frame_t){0};
  frame.kind = kind;
  frame.pattern = pattern;
  frame.topic = topic;
  return flowmq_protocol_encode_frame(&frame, max_frame_size, encoded);
}
