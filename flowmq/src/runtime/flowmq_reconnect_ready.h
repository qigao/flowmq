#ifndef FLOWMQ_RECONNECT_READY_H
#define FLOWMQ_RECONNECT_READY_H

#include "flowmq_peer_state.h"
#include "flowmq_reconnect.h"

#include "cmeta_error.h"

/* Private session gate. TCP/TLS CONNECTED or any partial HELLO/SETTINGS
 * handshake is not a recovered FMQ/6 session. The caller must also validate
 * the endpoint identity and its active generation before invoking this gate.
 * A peer-local latch makes the reset once-only for that peer generation. */
static inline int flowmq_reconnect_reset_on_protocol_ready(
    flowmq_reconnect_t *reconnect, const flowmq_peer_state_t *state,
    unsigned *ready_recorded) {
  if (reconnect == NULL || state == NULL || ready_recorded == NULL)
    return SALTS_EINVAL;
  if (*ready_recorded != 0u) return SALTS_EALREADY;
  if (!flowmq_peer_state_ready(state)) return SALTS_EBUSY;
  flowmq_reconnect_reset(reconnect);
  *ready_recorded = 1u;
  return SALTS_OK;
}

#endif
