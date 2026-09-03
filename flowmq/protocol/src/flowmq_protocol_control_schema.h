#ifndef FLOWMQ_PROTOCOL_CONTROL_SCHEMA_H
#define FLOWMQ_PROTOCOL_CONTROL_SCHEMA_H

#include <cmeta/pp.h>

/* One schema drives both directions of each fixed-width FMQ/6 control codec. */
#define FLOWMQ_PROTOCOL_SETTINGS_SCHEMA(M)                                                         \
  Schema(M, (capabilities, U32, 0u), (max_frame_size, U32, 4u), (session_generation, U64, 8u),     \
         (initial_max_data, U64, 16u), (flow_update_quantum, U32, 24u),                            \
         (flow_update_interval_ms, U32, 28u))

#define FLOWMQ_PROTOCOL_FLOW_UPDATE_SCHEMA(M)                                                      \
  Schema(M, (session_generation, U64, 0u), (consumed_data, U64, 8u), (max_data, U64, 16u))

#endif /* FLOWMQ_PROTOCOL_CONTROL_SCHEMA_H */
