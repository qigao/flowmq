#ifndef FLOWMQ_PATTERN_SCHEMA_H
#define FLOWMQ_PATTERN_SCHEMA_H

#include "flowmq_pattern.h"

#include <cmeta/pp.h>

#define FLOWMQ_PATTERN_BIT(pattern_value) \
  ((uint16_t)(UINT16_C(1) << (pattern_value)))

/*
 * One compile-time schema is the semantic source for immutable FMQ/6 pattern
 * policy. Runtime code consumes the generated descriptor table directly; it
 * does not perform CMeta reflection or schema traversal per message.
 *
 * Row fields:
 *   pattern, capabilities, compatible_mask, routing_class,
 *   subscription_class, mute_class, fsm_class
 */
#define FLOWMQ_PATTERN_SCHEMA(M)                                                     \
  Schema(M,                                                                         \
         (FLOWMQ_PROTOCOL_PUB,                                                       \
          FLOWMQ_PATTERN_CAP_API_SEND | FLOWMQ_PATTERN_CAP_DATA_SEND,                \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_SUB) |                                  \
              FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_XSUB),                              \
          FLOWMQ_PATTERN_ROUTE_FANOUT, FLOWMQ_PATTERN_SUB_PUBLISHER,                 \
          FLOWMQ_PATTERN_MUTE_DROP, FLOWMQ_PATTERN_FSM_NONE),                        \
         (FLOWMQ_PROTOCOL_SUB,                                                       \
          FLOWMQ_PATTERN_CAP_API_RECV | FLOWMQ_PATTERN_CAP_DATA_RECV,                \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_PUB) |                                  \
              FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_XPUB),                              \
          FLOWMQ_PATTERN_ROUTE_NONE, FLOWMQ_PATTERN_SUB_SUBSCRIBER,                  \
          FLOWMQ_PATTERN_MUTE_BLOCK, FLOWMQ_PATTERN_FSM_NONE),                       \
         (FLOWMQ_PROTOCOL_PUSH,                                                      \
          FLOWMQ_PATTERN_CAP_API_SEND | FLOWMQ_PATTERN_CAP_DATA_SEND,                \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_PULL),                                  \
          FLOWMQ_PATTERN_ROUTE_ROUND_ROBIN, FLOWMQ_PATTERN_SUB_NONE,                 \
          FLOWMQ_PATTERN_MUTE_BLOCK, FLOWMQ_PATTERN_FSM_NONE),                       \
         (FLOWMQ_PROTOCOL_PULL,                                                      \
          FLOWMQ_PATTERN_CAP_API_RECV | FLOWMQ_PATTERN_CAP_DATA_RECV,                \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_PUSH),                                  \
          FLOWMQ_PATTERN_ROUTE_NONE, FLOWMQ_PATTERN_SUB_NONE,                        \
          FLOWMQ_PATTERN_MUTE_BLOCK, FLOWMQ_PATTERN_FSM_NONE),                       \
         (FLOWMQ_PROTOCOL_ROUTER,                                                    \
          FLOWMQ_PATTERN_CAP_API_SEND | FLOWMQ_PATTERN_CAP_API_RECV |                \
              FLOWMQ_PATTERN_CAP_DATA_SEND | FLOWMQ_PATTERN_CAP_DATA_RECV,           \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_DEALER) |                               \
              FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_REQ) |                              \
              FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_ROUTER),                            \
          FLOWMQ_PATTERN_ROUTE_IDENTITY, FLOWMQ_PATTERN_SUB_NONE,                    \
          FLOWMQ_PATTERN_MUTE_BLOCK, FLOWMQ_PATTERN_FSM_NONE),                       \
         (FLOWMQ_PROTOCOL_DEALER,                                                    \
          FLOWMQ_PATTERN_CAP_API_SEND | FLOWMQ_PATTERN_CAP_API_RECV |                \
              FLOWMQ_PATTERN_CAP_DATA_SEND | FLOWMQ_PATTERN_CAP_DATA_RECV,           \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_ROUTER) |                               \
              FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_REP) |                              \
              FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_DEALER),                            \
          FLOWMQ_PATTERN_ROUTE_ROUND_ROBIN, FLOWMQ_PATTERN_SUB_NONE,                 \
          FLOWMQ_PATTERN_MUTE_BLOCK, FLOWMQ_PATTERN_FSM_NONE),                       \
         (FLOWMQ_PROTOCOL_PAIR,                                                      \
          FLOWMQ_PATTERN_CAP_API_SEND | FLOWMQ_PATTERN_CAP_API_RECV |                \
              FLOWMQ_PATTERN_CAP_DATA_SEND | FLOWMQ_PATTERN_CAP_DATA_RECV,           \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_PAIR),                                  \
          FLOWMQ_PATTERN_ROUTE_SINGLE, FLOWMQ_PATTERN_SUB_NONE,                      \
          FLOWMQ_PATTERN_MUTE_BLOCK, FLOWMQ_PATTERN_FSM_NONE),                       \
         (FLOWMQ_PROTOCOL_REQ,                                                       \
          FLOWMQ_PATTERN_CAP_API_SEND | FLOWMQ_PATTERN_CAP_API_RECV |                \
              FLOWMQ_PATTERN_CAP_DATA_SEND | FLOWMQ_PATTERN_CAP_DATA_RECV,           \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_REP) |                                  \
              FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_ROUTER),                            \
          FLOWMQ_PATTERN_ROUTE_ROUND_ROBIN, FLOWMQ_PATTERN_SUB_NONE,                 \
          FLOWMQ_PATTERN_MUTE_BLOCK, FLOWMQ_PATTERN_FSM_REQ),                        \
         (FLOWMQ_PROTOCOL_REP,                                                       \
          FLOWMQ_PATTERN_CAP_API_SEND | FLOWMQ_PATTERN_CAP_API_RECV |                \
              FLOWMQ_PATTERN_CAP_DATA_SEND | FLOWMQ_PATTERN_CAP_DATA_RECV,           \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_REQ) |                                  \
              FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_DEALER),                            \
          FLOWMQ_PATTERN_ROUTE_REPLY_PEER, FLOWMQ_PATTERN_SUB_NONE,                  \
          FLOWMQ_PATTERN_MUTE_BLOCK, FLOWMQ_PATTERN_FSM_REP),                        \
         (FLOWMQ_PROTOCOL_XPUB,                                                      \
          FLOWMQ_PATTERN_CAP_API_SEND | FLOWMQ_PATTERN_CAP_API_RECV |                \
              FLOWMQ_PATTERN_CAP_DATA_SEND | FLOWMQ_PATTERN_CAP_SUB_EVENTS,          \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_SUB) |                                  \
              FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_XSUB),                              \
          FLOWMQ_PATTERN_ROUTE_FANOUT, FLOWMQ_PATTERN_SUB_PUBLISHER,                 \
          FLOWMQ_PATTERN_MUTE_DROP, FLOWMQ_PATTERN_FSM_NONE),                        \
         (FLOWMQ_PROTOCOL_XSUB,                                                      \
          FLOWMQ_PATTERN_CAP_API_SEND | FLOWMQ_PATTERN_CAP_API_RECV |                \
              FLOWMQ_PATTERN_CAP_DATA_RECV,                                          \
          FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_PUB) |                                  \
              FLOWMQ_PATTERN_BIT(FLOWMQ_PROTOCOL_XPUB),                              \
          FLOWMQ_PATTERN_ROUTE_NONE, FLOWMQ_PATTERN_SUB_SUBSCRIBER,                  \
          FLOWMQ_PATTERN_MUTE_BLOCK, FLOWMQ_PATTERN_FSM_NONE))

#endif /* FLOWMQ_PATTERN_SCHEMA_H */
