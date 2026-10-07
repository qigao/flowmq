#ifndef FLOWMQ_PATTERN_H
#define FLOWMQ_PATTERN_H

#include "flowmq_protocol.h"

#include <stdint.h>

typedef enum flowmq_pattern_capability_e {
  FLOWMQ_PATTERN_CAP_API_SEND = 1u << 0,
  FLOWMQ_PATTERN_CAP_API_RECV = 1u << 1,
  FLOWMQ_PATTERN_CAP_DATA_SEND = 1u << 2,
  FLOWMQ_PATTERN_CAP_DATA_RECV = 1u << 3,
  FLOWMQ_PATTERN_CAP_SUB_EVENTS = 1u << 4
} flowmq_pattern_capability_t;

typedef enum flowmq_pattern_route_class_e {
  FLOWMQ_PATTERN_ROUTE_NONE = 0,
  FLOWMQ_PATTERN_ROUTE_FANOUT,
  FLOWMQ_PATTERN_ROUTE_ROUND_ROBIN,
  FLOWMQ_PATTERN_ROUTE_IDENTITY,
  FLOWMQ_PATTERN_ROUTE_REPLY_PEER,
  FLOWMQ_PATTERN_ROUTE_SINGLE
} flowmq_pattern_route_class_t;

typedef enum flowmq_pattern_subscription_class_e {
  FLOWMQ_PATTERN_SUB_NONE = 0,
  FLOWMQ_PATTERN_SUB_PUBLISHER,
  FLOWMQ_PATTERN_SUB_SUBSCRIBER
} flowmq_pattern_subscription_class_t;

typedef enum flowmq_pattern_mute_class_e {
  FLOWMQ_PATTERN_MUTE_BLOCK = 0,
  FLOWMQ_PATTERN_MUTE_DROP
} flowmq_pattern_mute_class_t;

typedef enum flowmq_pattern_fsm_class_e {
  FLOWMQ_PATTERN_FSM_NONE = 0,
  FLOWMQ_PATTERN_FSM_REQ,
  FLOWMQ_PATTERN_FSM_REP
} flowmq_pattern_fsm_class_t;

typedef struct flowmq_pattern_desc_s {
  flowmq_protocol_pattern_t pattern;
  uint16_t capabilities;
  uint16_t compatible_mask;
  uint8_t routing_class;
  uint8_t subscription_class;
  uint8_t mute_class;
  uint8_t fsm_class;
} flowmq_pattern_desc_t;

const flowmq_pattern_desc_t *
flowmq_pattern_descriptor(flowmq_protocol_pattern_t pattern);
int flowmq_pattern_validate(flowmq_protocol_pattern_t pattern);
int flowmq_pattern_has_capability(flowmq_protocol_pattern_t pattern,
                                  uint16_t capability);
int flowmq_patterns_compatible(flowmq_protocol_pattern_t local,
                               flowmq_protocol_pattern_t remote);
int flowmq_pattern_hello_validate(flowmq_protocol_pattern_t local,
                                  const flowmq_protocol_frame_t *hello);
int flowmq_pattern_socket_hello_validate(flowmq_protocol_pattern_t local,
                                         const flowmq_protocol_frame_t *hello);
int flowmq_pattern_data_direction_validate(flowmq_protocol_pattern_t local,
                                           const flowmq_protocol_frame_t *frame);
int flowmq_pattern_encode_hello(flowmq_protocol_pattern_t pattern, vstr identity,
                                vstr topic, size_t max_frame_size,
                                tstr *encoded);
int flowmq_pattern_encode_hello_ex(flowmq_protocol_pattern_t pattern,
                                   vstr identity, vstr topic,
                                   vstr security_payload,
                                   size_t max_frame_size, tstr *encoded);
int flowmq_pattern_encode_heartbeat(flowmq_protocol_pattern_t pattern,
                                    flowmq_protocol_frame_kind_t kind,
                                    size_t max_frame_size, tstr *encoded);
int flowmq_pattern_encode_subscription(flowmq_protocol_pattern_t pattern,
                                       flowmq_protocol_frame_kind_t kind,
                                       vstr topic, size_t max_frame_size,
                                       tstr *encoded);

#endif /* FLOWMQ_PATTERN_H */
