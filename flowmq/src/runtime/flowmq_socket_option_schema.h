#ifndef FLOWMQ_SOCKET_OPTION_SCHEMA_H
#define FLOWMQ_SOCKET_OPTION_SCHEMA_H

#include "flowmq_socket_option.h"
#include "flowmq_tls_identity_map.h"

#include <cmeta/pp.h>

#include <limits.h>

#define FLOWMQ_SOCKET_OPTION_PATTERN_BIT(pattern_value) \
  ((uint16_t)(UINT16_C(1) << (pattern_value)))

/*
 * CMeta's strict-C11 Schema kernel admits up to 16 rows per Schema(...).
 * Keep two implementation fragments, but expose exactly one canonical replay
 * source: FLOWMQ_SOCKET_OPTION_SCHEMA(M).
 *
 * Row fields:
 *   option, value_kind, access, set_phase, validation,
 *   ABI size, minimum, maximum/size limit, allowed-pattern mask.
 *
 * A zero pattern mask means all valid socket patterns are admitted.
 */
#define FLOWMQ_SOCKET_OPTION_SCHEMA_CLASSIC(M)                                      \
  Schema(M,                                                                         \
         (FLOWMQ_IDENTITY, FLOWMQ_SOCKET_OPTION_VALUE_STRING,                       \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_STRING, 0u, 1,                              \
          FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE, 0u),                                   \
         (FLOWMQ_SUBSCRIBE, FLOWMQ_SOCKET_OPTION_VALUE_BYTES,                       \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_RUNTIME,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_BYTES, 0u, 0,                               \
          FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE,                                           \
          FLOWMQ_SOCKET_OPTION_PATTERN_BIT(FLOWMQ_PROTOCOL_SUB) |                   \
              FLOWMQ_SOCKET_OPTION_PATTERN_BIT(FLOWMQ_PROTOCOL_XSUB)),              \
         (FLOWMQ_UNSUBSCRIBE, FLOWMQ_SOCKET_OPTION_VALUE_BYTES,                     \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_RUNTIME,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_BYTES, 0u, 0,                               \
          FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE,                                           \
          FLOWMQ_SOCKET_OPTION_PATTERN_BIT(FLOWMQ_PROTOCOL_SUB) |                   \
              FLOWMQ_SOCKET_OPTION_PATTERN_BIT(FLOWMQ_PROTOCOL_XSUB)),              \
         (FLOWMQ_RCVMORE, FLOWMQ_SOCKET_OPTION_VALUE_INT,                           \
          FLOWMQ_SOCKET_OPTION_ACCESS_GET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_EXACT, sizeof(int), 0, 0u, 0u),             \
         (FLOWMQ_RECONNECT_IVL, FLOWMQ_SOCKET_OPTION_VALUE_INT,                     \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET | FLOWMQ_SOCKET_OPTION_ACCESS_GET,         \
          FLOWMQ_SOCKET_OPTION_SET_STARTUP,                                         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_SIGNED_RANGE, sizeof(int), -1, INT_MAX,     \
          0u),                                                                      \
         (FLOWMQ_RECONNECT_IVL_MAX, FLOWMQ_SOCKET_OPTION_VALUE_INT,                 \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET | FLOWMQ_SOCKET_OPTION_ACCESS_GET,         \
          FLOWMQ_SOCKET_OPTION_SET_STARTUP,                                         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_SIGNED_RANGE, sizeof(int), 0, INT_MAX,      \
          0u),                                                                      \
         (FLOWMQ_SNDHWM, FLOWMQ_SOCKET_OPTION_VALUE_INT,                            \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_SIGNED_RANGE, sizeof(int), 1,               \
          FLOWMQ_SOCKET_OPTION_MESSAGE_HWM_MAX, 0u),                                \
         (FLOWMQ_RCVHWM, FLOWMQ_SOCKET_OPTION_VALUE_INT,                            \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_SIGNED_RANGE, sizeof(int), 1,               \
          FLOWMQ_SOCKET_OPTION_MESSAGE_HWM_MAX, 0u),                                \
         (FLOWMQ_HEARTBEAT_IVL, FLOWMQ_SOCKET_OPTION_VALUE_INT,                     \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_SIGNED_RANGE, sizeof(int), 0, INT_MAX,      \
          0u),                                                                      \
         (FLOWMQ_HEARTBEAT_TIMEOUT, FLOWMQ_SOCKET_OPTION_VALUE_INT,                 \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_SIGNED_RANGE, sizeof(int), 0, INT_MAX,      \
          0u),                                                                      \
         (FLOWMQ_TLS_CA_FILE, FLOWMQ_SOCKET_OPTION_VALUE_STRING,                    \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_STRING, 0u, 1,                              \
          FLOWMQ_SOCKET_OPTION_TLS_PATH_CAPACITY - 1u, 0u))

#define FLOWMQ_SOCKET_OPTION_SCHEMA_EXTENDED(M)                                     \
  Schema(M,                                                                         \
         (FLOWMQ_TLS_CERT_FILE, FLOWMQ_SOCKET_OPTION_VALUE_STRING,                  \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_STRING, 0u, 1,                              \
          FLOWMQ_SOCKET_OPTION_TLS_PATH_CAPACITY - 1u, 0u),                         \
         (FLOWMQ_TLS_KEY_FILE, FLOWMQ_SOCKET_OPTION_VALUE_STRING,                   \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_STRING, 0u, 1,                              \
          FLOWMQ_SOCKET_OPTION_TLS_PATH_CAPACITY - 1u, 0u),                         \
         (FLOWMQ_TLS_KEY_PASSWORD, FLOWMQ_SOCKET_OPTION_VALUE_STRING,               \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_STRING, 0u, 1,                              \
          FLOWMQ_SOCKET_OPTION_TLS_PASSWORD_CAPACITY - 1u, 0u),                     \
         (FLOWMQ_TLS_SERVER_NAME, FLOWMQ_SOCKET_OPTION_VALUE_STRING,                \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_STRING, 0u, 1,                              \
          FLOWMQ_SOCKET_OPTION_TLS_SERVER_NAME_CAPACITY - 1u, 0u),                  \
         (FLOWMQ_TLS_REQUIRE_CLIENT_CERTIFICATE, FLOWMQ_SOCKET_OPTION_VALUE_INT,     \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_EXACT, sizeof(int), 0, 0u, 0u),             \
         (FLOWMQ_TLS_IDENTITY_POLICY, FLOWMQ_SOCKET_OPTION_VALUE_STRUCT,             \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_EXACT,                                      \
          sizeof(flowmq_tls_identity_map_config_t), 0, 0u,                          \
          FLOWMQ_SOCKET_OPTION_PATTERN_BIT(FLOWMQ_PROTOCOL_ROUTER)),                 \
         (FLOWMQ_TLS_IDENTITY_REJECTIONS, FLOWMQ_SOCKET_OPTION_VALUE_U64,            \
          FLOWMQ_SOCKET_OPTION_ACCESS_GET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_EXACT, sizeof(uint64_t), 0, 0u, 0u),        \
         (FLOWMQ_SNDHWM_BYTES, FLOWMQ_SOCKET_OPTION_VALUE_SIZE,                     \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_UNSIGNED_RANGE, sizeof(size_t), 1,          \
          FLOWMQ_SOCKET_OPTION_HWM_BYTES_MAX, 0u),                                  \
         (FLOWMQ_RCVHWM_BYTES, FLOWMQ_SOCKET_OPTION_VALUE_SIZE,                     \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_UNSIGNED_RANGE, sizeof(size_t), 1,          \
          FLOWMQ_SOCKET_OPTION_HWM_BYTES_MAX, 0u),                                  \
         (FLOWMQ_FLOW_UPDATE_QUANTUM, FLOWMQ_SOCKET_OPTION_VALUE_SIZE,              \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_UNSIGNED_RANGE, sizeof(size_t), 1,          \
          UINT32_MAX, 0u),                                                          \
         (FLOWMQ_FLOW_UPDATE_IVL, FLOWMQ_SOCKET_OPTION_VALUE_INT,                   \
          FLOWMQ_SOCKET_OPTION_ACCESS_SET, FLOWMQ_SOCKET_OPTION_SET_STARTUP,         \
          FLOWMQ_SOCKET_OPTION_VALIDATE_SIGNED_RANGE, sizeof(int), 1, INT_MAX,      \
          0u))

#define FLOWMQ_SOCKET_OPTION_SCHEMA(M)       \
  FLOWMQ_SOCKET_OPTION_SCHEMA_CLASSIC(M)     \
  FLOWMQ_SOCKET_OPTION_SCHEMA_EXTENDED(M)

#endif /* FLOWMQ_SOCKET_OPTION_SCHEMA_H */
