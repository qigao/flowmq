#ifndef FLOWMQ_PATTERN_H
#define FLOWMQ_PATTERN_H

#include "flowmq_protocol.h"

int flowmq_pattern_validate(flowmq_protocol_pattern_t pattern);
int flowmq_patterns_compatible(flowmq_protocol_pattern_t local, flowmq_protocol_pattern_t remote);
int flowmq_pattern_hello_validate(flowmq_protocol_pattern_t local,
                                  const flowmq_protocol_frame_t *hello);
int flowmq_pattern_data_direction_validate(flowmq_protocol_pattern_t local,
                                           const flowmq_protocol_frame_t *frame);
int flowmq_pattern_encode_hello(flowmq_protocol_pattern_t pattern, tstr_v identity, tstr_v topic,
                                size_t max_frame_size, tstr_t *encoded);
int flowmq_pattern_encode_hello_ex(flowmq_protocol_pattern_t pattern, tstr_v identity, tstr_v topic,
                                   tstr_v security_payload, size_t max_frame_size,
                                   tstr_t *encoded);
int flowmq_pattern_encode_heartbeat(flowmq_protocol_pattern_t pattern,
                                    flowmq_protocol_frame_kind_t kind, size_t max_frame_size,
                                    tstr_t *encoded);
int flowmq_pattern_encode_subscription(flowmq_protocol_pattern_t pattern,
                                       flowmq_protocol_frame_kind_t kind, tstr_v topic,
                                       size_t max_frame_size, tstr_t *encoded);

#endif /* FLOWMQ_PATTERN_H */
