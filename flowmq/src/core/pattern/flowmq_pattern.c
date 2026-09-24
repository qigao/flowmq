#include "flowmq_pattern.h"
#include "flowmq_pattern_schema.h"
#include "flowmq_security.h"

#include "salts_error.h"

#define FLOWMQ_PATTERN_DESC_ROW(pattern_value, capabilities_value, compatible_value,              \
                                route_value, subscription_value, mute_value, fsm_value)            \
  [pattern_value] = {pattern_value, capabilities_value, compatible_value, route_value,             \
                     subscription_value, mute_value, fsm_value},

static const flowmq_pattern_desc_t
    FLOWMQ_PATTERN_DESCRIPTORS[FLOWMQ_PROTOCOL_XSUB + 1u] = {
        Replay(FLOWMQ_PATTERN_SCHEMA, FLOWMQ_PATTERN_DESC_ROW)};

#undef FLOWMQ_PATTERN_DESC_ROW

#define FLOWMQ_PATTERN_COUNT_ROW(pattern_value, capabilities_value, compatible_value, route_value, \
                                 subscription_value, mute_value, fsm_value)                         \
  +1
#define FLOWMQ_PATTERN_MASK_ROW(pattern_value, capabilities_value, compatible_value, route_value,  \
                                subscription_value, mute_value, fsm_value)                          \
  | FLOWMQ_PATTERN_BIT(pattern_value)

enum {
  FLOWMQ_PATTERN_SCHEMA_COUNT =
      0 Replay(FLOWMQ_PATTERN_SCHEMA, FLOWMQ_PATTERN_COUNT_ROW),
  FLOWMQ_PATTERN_SCHEMA_MASK =
      0 Replay(FLOWMQ_PATTERN_SCHEMA, FLOWMQ_PATTERN_MASK_ROW)
};

#undef FLOWMQ_PATTERN_COUNT_ROW
#undef FLOWMQ_PATTERN_MASK_ROW

#define FLOWMQ_PATTERN_EXPECTED_COUNT                                                   \
  (FLOWMQ_PROTOCOL_XSUB - FLOWMQ_PROTOCOL_PUB + 1u)
#define FLOWMQ_PATTERN_EXPECTED_MASK                                                    \
  ((UINT16_C(1) << (FLOWMQ_PROTOCOL_XSUB + 1u)) -                                       \
   (UINT16_C(1) << FLOWMQ_PROTOCOL_PUB))

_Static_assert(FLOWMQ_PATTERN_SCHEMA_COUNT == FLOWMQ_PATTERN_EXPECTED_COUNT,
               "pattern schema must contain every FMQ/6 pattern exactly once");
_Static_assert(FLOWMQ_PATTERN_SCHEMA_MASK == FLOWMQ_PATTERN_EXPECTED_MASK,
               "pattern schema must cover the complete public FMQ/6 pattern range");

const flowmq_pattern_desc_t *
flowmq_pattern_descriptor(flowmq_protocol_pattern_t pattern) {
  const flowmq_pattern_desc_t *desc;
  if (pattern < FLOWMQ_PROTOCOL_PUB || pattern > FLOWMQ_PROTOCOL_XSUB)
    return NULL;
  desc = &FLOWMQ_PATTERN_DESCRIPTORS[pattern];
  return desc->pattern == pattern ? desc : NULL;
}

int flowmq_pattern_validate(flowmq_protocol_pattern_t pattern) {
  return flowmq_pattern_descriptor(pattern) != NULL ? SALTS_OK : SALTS_EINVAL;
}

int flowmq_pattern_has_capability(flowmq_protocol_pattern_t pattern,
                                  uint16_t capability) {
  const flowmq_pattern_desc_t *desc = flowmq_pattern_descriptor(pattern);
  return desc != NULL && (desc->capabilities & capability) == capability;
}

int flowmq_patterns_compatible(flowmq_protocol_pattern_t local,
                               flowmq_protocol_pattern_t remote) {
  const flowmq_pattern_desc_t *local_desc = flowmq_pattern_descriptor(local);
  const flowmq_pattern_desc_t *remote_desc = flowmq_pattern_descriptor(remote);
  return local_desc != NULL && remote_desc != NULL &&
         (local_desc->compatible_mask & FLOWMQ_PATTERN_BIT(remote)) != 0u;
}

int flowmq_pattern_hello_validate(flowmq_protocol_pattern_t local,
                                  const flowmq_protocol_frame_t *hello) {
  flowmq_security_t security;
  if (flowmq_pattern_validate(local) != SALTS_OK || !hello) return SALTS_EINVAL;
  if (hello->kind != FLOWMQ_PROTOCOL_FRAME_HELLO ||
      flowmq_security_decode(hello->payload, &security) != SALTS_OK ||
      !flowmq_patterns_compatible(local, hello->pattern) ||
      (hello->pattern == FLOWMQ_PROTOCOL_DEALER && hello->identity.len == 0u))
    return SALTS_EPROTO;
  return SALTS_OK;
}

int flowmq_pattern_socket_hello_validate(flowmq_protocol_pattern_t local,
                                         const flowmq_protocol_frame_t *hello) {
  int status = flowmq_pattern_hello_validate(local, hello);
  if (status != SALTS_OK) return status;
  /* FMS/3 is only a wire envelope until a credential verifier and TLS
   * principal binding are configured by the socket runtime. */
  return hello->payload.len == 0u ? SALTS_OK : SALTS_EPROTO;
}

int flowmq_pattern_data_direction_validate(flowmq_protocol_pattern_t local,
                                           const flowmq_protocol_frame_t *frame) {
  const flowmq_pattern_desc_t *local_desc;
  const flowmq_pattern_desc_t *remote_desc;
  if (frame == NULL) return SALTS_EINVAL;
  local_desc = flowmq_pattern_descriptor(local);
  remote_desc = flowmq_pattern_descriptor(frame->pattern);
  if (local_desc == NULL) return SALTS_EINVAL;
  if (remote_desc == NULL ||
      (local_desc->compatible_mask & FLOWMQ_PATTERN_BIT(frame->pattern)) == 0u)
    return SALTS_EPROTO;
  if (frame->kind == FLOWMQ_PROTOCOL_FRAME_PING ||
      frame->kind == FLOWMQ_PROTOCOL_FRAME_PONG ||
      frame->kind == FLOWMQ_PROTOCOL_FRAME_SETTINGS ||
      frame->kind == FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE) {
    return SALTS_OK;
  }
  if (frame->kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE ||
      frame->kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE) {
    return local_desc->subscription_class == FLOWMQ_PATTERN_SUB_PUBLISHER &&
                   remote_desc->subscription_class == FLOWMQ_PATTERN_SUB_SUBSCRIBER
               ? SALTS_OK
               : SALTS_EPROTO;
  }
  if (frame->kind != FLOWMQ_PROTOCOL_FRAME_DATA) return SALTS_EPROTO;
  if ((local_desc->capabilities & FLOWMQ_PATTERN_CAP_DATA_RECV) == 0u ||
      (remote_desc->capabilities & FLOWMQ_PATTERN_CAP_DATA_SEND) == 0u)
    return SALTS_EPROTO;
  return SALTS_OK;
}

int flowmq_pattern_encode_hello(flowmq_protocol_pattern_t pattern, vstr identity,
                                vstr topic, size_t max_frame_size,
                                tstr *encoded) {
  return flowmq_pattern_encode_hello_ex(pattern, identity, topic, (vstr){0},
                                        max_frame_size, encoded);
}

int flowmq_pattern_encode_hello_ex(flowmq_protocol_pattern_t pattern,
                                   vstr identity, vstr topic,
                                   vstr security_payload,
                                   size_t max_frame_size, tstr *encoded) {
  flowmq_protocol_frame_t frame;
  if (flowmq_pattern_validate(pattern) != SALTS_OK || !encoded)
    return SALTS_EINVAL;
  frame = (flowmq_protocol_frame_t){0};
  frame.kind = FLOWMQ_PROTOCOL_FRAME_HELLO;
  frame.pattern = pattern;
  frame.identity = identity;
  frame.topic = topic;
  frame.payload = security_payload;
  return flowmq_protocol_encode_frame(&frame, max_frame_size, encoded);
}

int flowmq_pattern_encode_heartbeat(flowmq_protocol_pattern_t pattern,
                                    flowmq_protocol_frame_kind_t kind,
                                    size_t max_frame_size, tstr *encoded) {
  flowmq_protocol_frame_t frame;
  if (flowmq_pattern_validate(pattern) != SALTS_OK || !encoded ||
      (kind != FLOWMQ_PROTOCOL_FRAME_PING &&
       kind != FLOWMQ_PROTOCOL_FRAME_PONG))
    return SALTS_EINVAL;
  frame = (flowmq_protocol_frame_t){0};
  frame.kind = kind;
  frame.pattern = pattern;
  return flowmq_protocol_encode_frame(&frame, max_frame_size, encoded);
}

int flowmq_pattern_encode_subscription(flowmq_protocol_pattern_t pattern,
                                       flowmq_protocol_frame_kind_t kind,
                                       vstr topic, size_t max_frame_size,
                                       tstr *encoded) {
  const flowmq_pattern_desc_t *desc = flowmq_pattern_descriptor(pattern);
  flowmq_protocol_frame_t frame;
  if (desc == NULL ||
      desc->subscription_class != FLOWMQ_PATTERN_SUB_SUBSCRIBER || !encoded ||
      (kind != FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE &&
       kind != FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE))
    return SALTS_EINVAL;
  frame = (flowmq_protocol_frame_t){0};
  frame.kind = kind;
  frame.pattern = pattern;
  frame.topic = topic;
  return flowmq_protocol_encode_frame(&frame, max_frame_size, encoded);
}
