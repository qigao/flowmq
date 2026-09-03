#ifndef FLOWMQ_CORE_H
#define FLOWMQ_CORE_H

#include "flowmq_export.h"

#include "flowmq_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Validate one built-in FlowMQ transport pattern.
 * @param pattern Numeric FMQ pattern value.
 * @return SALTS_OK when supported, otherwise SALTS_EINVAL.
 */
FLOWMQ_C_API int flowmq_core_pattern_validate(flowmq_protocol_pattern_t pattern);

/**
 * Return non-zero when two built-in FlowMQ transport patterns may form a peer
 * connection. This is a symmetric protocol contract and performs no I/O.
 * @param local Local endpoint pattern.
 * @param remote Remote endpoint pattern.
 * @return Non-zero when compatible; zero for invalid or incompatible values.
 */
FLOWMQ_C_API int flowmq_core_patterns_compatible(flowmq_protocol_pattern_t local,
                                              flowmq_protocol_pattern_t remote);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_CORE_H */
