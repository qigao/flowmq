#include "flowmq_core.h"

#include "flowmq_pattern.h"

int flowmq_core_pattern_validate(flowmq_protocol_pattern_t pattern) {
  return flowmq_pattern_validate(pattern);
}

int flowmq_core_patterns_compatible(flowmq_protocol_pattern_t local,
                                    flowmq_protocol_pattern_t remote) {
  return flowmq_patterns_compatible(local, remote);
}
