#ifndef TURBO_FLOW_FMQ_PROTOCOL_H
#define TURBO_FLOW_FMQ_PROTOCOL_H

#include "flowmq_protocol.h"
#include "CoroNet/turbo_kcp.h"
#include "turbo_flow_fmq.h"

#define FLOW_FMQ_PATTERN_WIRE_ASSERT(name, value)                                                  \
  _Static_assert(TURBO_FLOW_FMQ_##name == value, "TurboFlow FMQ " #name " wire value changed");  \
  _Static_assert(FLOWMQ_PROTOCOL_##name == value, "FlowMQ Protocol " #name " wire value changed")

FLOW_FMQ_PATTERN_WIRE_ASSERT(PUB, 1);
FLOW_FMQ_PATTERN_WIRE_ASSERT(SUB, 2);
FLOW_FMQ_PATTERN_WIRE_ASSERT(PUSH, 3);
FLOW_FMQ_PATTERN_WIRE_ASSERT(PULL, 4);
FLOW_FMQ_PATTERN_WIRE_ASSERT(ROUTER, 5);
FLOW_FMQ_PATTERN_WIRE_ASSERT(DEALER, 6);
FLOW_FMQ_PATTERN_WIRE_ASSERT(PAIR, 7);
FLOW_FMQ_PATTERN_WIRE_ASSERT(REQ, 8);
FLOW_FMQ_PATTERN_WIRE_ASSERT(REP, 9);
FLOW_FMQ_PATTERN_WIRE_ASSERT(XPUB, 10);
FLOW_FMQ_PATTERN_WIRE_ASSERT(XSUB, 11);

#undef FLOW_FMQ_PATTERN_WIRE_ASSERT

#define FLOW_FMQ_PROTOCOL_VERSION FLOWMQ_PROTOCOL_WIRE_VERSION
#define FLOW_FMQ_HEADER_SIZE FLOWMQ_PROTOCOL_HEADER_SIZE
#define FLOW_FMQ_PACKET_PAYLOAD_SIZE FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE
#define FLOW_FMQ_PACKET_FIRST FLOWMQ_PROTOCOL_PACKET_FIRST
#define FLOW_FMQ_PACKET_LAST FLOWMQ_PROTOCOL_PACKET_LAST
#define FLOW_FMQ_INCOMPLETE FLOWMQ_PROTOCOL_INCOMPLETE
#define FLOW_FMQ_FRAME_HELLO FLOWMQ_PROTOCOL_FRAME_HELLO
#define FLOW_FMQ_FRAME_DATA FLOWMQ_PROTOCOL_FRAME_DATA
#define FLOW_FMQ_FRAME_PING FLOWMQ_PROTOCOL_FRAME_PING
#define FLOW_FMQ_FRAME_PONG FLOWMQ_PROTOCOL_FRAME_PONG
#define FLOW_FMQ_FRAME_SUBSCRIBE FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE
#define FLOW_FMQ_FRAME_UNSUBSCRIBE FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE
#define FLOW_FMQ_HEARTBEAT_WAIT FLOWMQ_PROTOCOL_HEARTBEAT_WAIT
#define FLOW_FMQ_HEARTBEAT_SEND_PING FLOWMQ_PROTOCOL_HEARTBEAT_SEND_PING
#define FLOW_FMQ_HEARTBEAT_EXPIRED FLOWMQ_PROTOCOL_HEARTBEAT_EXPIRED
#define FLOW_FMQ_HEARTBEAT_RECV_EXPIRED FLOWMQ_PROTOCOL_HEARTBEAT_RECV_EXPIRED

typedef flowmq_protocol_frame_kind_t flow_fmq_frame_kind_t;
typedef flowmq_protocol_frame_t flow_fmq_frame_t;
typedef flowmq_protocol_segmented_frame_t flow_fmq_segmented_frame_t;
typedef flowmq_protocol_heartbeat_action_t flow_fmq_heartbeat_action_t;
typedef flowmq_protocol_heartbeat_deadlines_t flow_fmq_heartbeat_deadlines_t;

#define flow_fmq_heartbeat_deadlines_init flowmq_protocol_heartbeat_deadlines_init
#define flow_fmq_heartbeat_deadlines_on_receive flowmq_protocol_heartbeat_deadlines_on_receive
#define flow_fmq_heartbeat_deadlines_on_ping flowmq_protocol_heartbeat_deadlines_on_ping
#define flow_fmq_heartbeat_deadlines_next flowmq_protocol_heartbeat_deadlines_next
#define flow_fmq_encode_frame flowmq_protocol_encode_frame
#define flow_fmq_encode_frame_segmented flowmq_protocol_encode_frame_segmented
#define flow_fmq_segmented_frame_cleanup flowmq_protocol_segmented_frame_cleanup
#define flow_fmq_decode_frame flowmq_protocol_decode_frame
#define flow_fmq_encoded_topic flowmq_protocol_encoded_topic
#define flow_fmq_frame_cleanup flowmq_protocol_frame_cleanup
#define flow_fmq_encoded_size_limit flowmq_protocol_encoded_size_limit
#define flow_fmq_encoded_size flowmq_protocol_encoded_size

/*
 * Private cross-target encoder used by the FMQ adapter's batch arena. The
 * installed protocol API remains allocation-owning.
 */
int flowmq_protocol_encode_frame_into_internal(
    const flowmq_protocol_frame_t *frame, size_t max_frame_size, void *storage,
    size_t storage_size, size_t *encoded_size);

int flow_fmq_config_validate(const turbo_flow_fmq_config_t *config);
int flow_fmq_kcp_config_resolve(const turbo_flow_fmq_config_t *config,
                                turbo_kcp_config_t *out,
                                int *configured);

#endif /* TURBO_FLOW_FMQ_PROTOCOL_H */
