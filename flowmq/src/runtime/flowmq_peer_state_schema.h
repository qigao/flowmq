#ifndef FLOWMQ_PEER_STATE_SCHEMA_H
#define FLOWMQ_PEER_STATE_SCHEMA_H

#include "flowmq_peer_state.h"

#include <cmeta/pp.h>

#define FLOWMQ_PEER_LIFECYCLE_BIT(state_value)   ((uint8_t)(UINT8_C(1) << (state_value)))

/*
 * Lifecycle owns slot/session existence only. Handshake progress, write lane,
 * multipart, heartbeat, queues and flow credit remain independent dimensions.
 */
#define FLOWMQ_PEER_LIFECYCLE_SCHEMA(M)                                            Schema(M,                                                                               (FLOWMQ_PEER_LIFECYCLE_FREE,                                                      FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_ALLOCATED)),                    (FLOWMQ_PEER_LIFECYCLE_ALLOCATED,                                                 FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_FREE) |                              FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_CONNECTED) |                     FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_RETIRED)),                  (FLOWMQ_PEER_LIFECYCLE_CONNECTED,                                                 FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_CLOSE_RETRY) |                       FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_CLOSING) |                       FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_RETIRED)),                  (FLOWMQ_PEER_LIFECYCLE_CLOSE_RETRY,                                               FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_CLOSING) |                           FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_RETIRED)),                  (FLOWMQ_PEER_LIFECYCLE_CLOSING,                                                   FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_RETIRED)),                      (FLOWMQ_PEER_LIFECYCLE_RETIRED,                                                   FLOWMQ_PEER_LIFECYCLE_BIT(FLOWMQ_PEER_LIFECYCLE_FREE)))

/*
 * TX and RX are independent monotone chains:
 *   HELLO_TX -> SETTINGS_TX
 *   HELLO_RX -> SETTINGS_RX
 * They may interleave arbitrarily across directions.
 */
#define FLOWMQ_PEER_HANDSHAKE_SCHEMA(M)                                            Schema(M,                                                                               (FLOWMQ_PEER_HANDSHAKE_HELLO_TX, 0u),                                            (FLOWMQ_PEER_HANDSHAKE_HELLO_RX, 0u),                                            (FLOWMQ_PEER_HANDSHAKE_SETTINGS_TX,                                               FLOWMQ_PEER_HANDSHAKE_HELLO_TX),                                                (FLOWMQ_PEER_HANDSHAKE_SETTINGS_RX,                                               FLOWMQ_PEER_HANDSHAKE_HELLO_RX))

/* HELLO/SETTINGS completion advances the corresponding handshake TX fact. */
#define FLOWMQ_PEER_WRITE_SCHEMA(M)                                                Schema(M,                                                                               (FLOWMQ_PEER_WRITE_IDLE, 0u),                                                    (FLOWMQ_PEER_WRITE_HELLO, FLOWMQ_PEER_HANDSHAKE_HELLO_TX),                       (FLOWMQ_PEER_WRITE_SETTINGS, FLOWMQ_PEER_HANDSHAKE_SETTINGS_TX),                  (FLOWMQ_PEER_WRITE_CONTROL, 0u),                                                 (FLOWMQ_PEER_WRITE_DATA, 0u))

#endif /* FLOWMQ_PEER_STATE_SCHEMA_H */
