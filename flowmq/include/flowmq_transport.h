#ifndef FLOWMQ_TRANSPORT_H
#define FLOWMQ_TRANSPORT_H

#include "flowmq_export.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_TRANSPORT_API_VERSION 1u

typedef enum flowmq_transport_e {
  FLOWMQ_TRANSPORT_TCP = 1,
  FLOWMQ_TRANSPORT_TLS = 2
} flowmq_transport_t;

typedef enum flowmq_timeout_flag_e {
  FLOWMQ_TIMEOUT_SET_DEFAULT = 1u << 0,
  FLOWMQ_TIMEOUT_SET_CONNECT = 1u << 1,
  FLOWMQ_TIMEOUT_SET_SEND = 1u << 2,
  FLOWMQ_TIMEOUT_SET_RECV = 1u << 3,
  FLOWMQ_TIMEOUT_SET_HANDSHAKE = 1u << 4
} flowmq_timeout_flag_t;

#define FLOWMQ_TIMEOUT_SET_ALL ((1u << 5) - 1u)

typedef struct flowmq_timeout_config_s {
  uint64_t timeout_ms;
  uint64_t connect_timeout_ms;
  uint64_t send_timeout_ms;
  uint64_t recv_timeout_ms;
  uint64_t handshake_timeout_ms;
  uint32_t set_flags;
  uint32_t explicit_flags;
} flowmq_timeout_config_t;

/** TLS client policy copied by socket creation. Peer and hostname checks are mandatory. */
typedef struct flowmq_tls_client_config_s {
  const char *ca_file;
  const char *ca_path;
  const char *cert_file;
  const char *key_file;
  const char *key_password;
  const char *server_name;
} flowmq_tls_client_config_t;

/** TLS server policy copied by socket creation. */
typedef struct flowmq_tls_server_config_s {
  const char *ca_file;
  const char *ca_path;
  const char *cert_file;
  const char *key_file;
  const char *key_password;
  int require_client_certificate;
} flowmq_tls_server_config_t;

/**
 * Hard bounds for one socket-owned CNet progress loop. Command and event
 * capacities must be powers of two. Values are copied during socket creation.
 */
typedef struct flowmq_io_config_s {
  size_t command_capacity;
  size_t request_capacity;
  size_t completion_batch_capacity;
  size_t event_capacity;
  size_t receive_buffer_bytes;
  size_t tls_io_buffer_bytes;
  uint32_t poll_timeout_ms;
} flowmq_io_config_t;

FLOWMQ_C_API int flowmq_transport_validate(flowmq_transport_t transport);
FLOWMQ_C_API void flowmq_timeouts_resolve(flowmq_timeout_config_t *timeouts,
                                          uint64_t fallback_timeout_ms);
FLOWMQ_C_API void flowmq_io_config_init(flowmq_io_config_t *config);
FLOWMQ_C_API int flowmq_io_config_validate(const flowmq_io_config_t *config);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_TRANSPORT_H */
