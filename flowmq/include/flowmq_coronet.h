#ifndef FLOWMQ_CORONET_H
#define FLOWMQ_CORONET_H

#include "flowmq_export.h"

#include "platform.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_CORONET_API_VERSION 2u
#define FLOWMQ_KCP_PSK_SIZE 32u

typedef struct coro_context_s coro_context_t;

typedef enum flowmq_coronet_transport_e {
  FLOWMQ_TRANSPORT_TCP = 1,
  FLOWMQ_TRANSPORT_TLS,
  FLOWMQ_TRANSPORT_UDP,
  FLOWMQ_TRANSPORT_KCP,
  FLOWMQ_TRANSPORT_PIPE,
  FLOWMQ_TRANSPORT_WS,
  FLOWMQ_TRANSPORT_WSS
} flowmq_coronet_transport_t;

typedef enum flowmq_coronet_timeout_flag_e {
  FLOWMQ_TIMEOUT_SET_DEFAULT = 1u << 0,
  FLOWMQ_TIMEOUT_SET_CONNECT = 1u << 1,
  FLOWMQ_TIMEOUT_SET_SEND = 1u << 2,
  FLOWMQ_TIMEOUT_SET_RECV = 1u << 3,
  FLOWMQ_TIMEOUT_SET_HANDSHAKE = 1u << 4
} flowmq_coronet_timeout_flag_t;

#define FLOWMQ_TIMEOUT_SET_ALL ((1u << 5) - 1u)

typedef struct flowmq_coronet_timeout_config_s {
  uint64_t timeout_ms;
  uint64_t connect_timeout_ms;
  uint64_t send_timeout_ms;
  uint64_t recv_timeout_ms;
  uint64_t handshake_timeout_ms;
  uint32_t set_flags;
  uint32_t explicit_flags;
} flowmq_coronet_timeout_config_t;

typedef struct flowmq_coronet_socket_options_s {
  int tcp_keepalive;
  uint64_t tcp_keepalive_idle_ms;
  uint64_t tcp_keepalive_interval_ms;
  uint32_t tcp_keepalive_count;
  int linger;
  uint64_t linger_ms;
  size_t send_hwm_bytes;
  size_t socket_recv_buffer_bytes;
  size_t socket_send_buffer_bytes;
} flowmq_coronet_socket_options_t;

typedef enum flowmq_coronet_udp_option_flag_e {
  FLOWMQ_UDP_OPTION_MULTICAST_LOOP = 1u << 0,
  FLOWMQ_UDP_OPTION_MULTICAST_TTL = 1u << 1,
  FLOWMQ_UDP_OPTION_BROADCAST = 1u << 2
} flowmq_coronet_udp_option_flag_t;

#define FLOWMQ_UDP_OPTION_ALL ((1u << 3) - 1u)

typedef struct flowmq_coronet_udp_options_s {
  const char *multicast_group;
  const char *multicast_interface;
  uint32_t option_flags;
  int multicast_loop;
  uint32_t multicast_ttl;
  int broadcast;
} flowmq_coronet_udp_options_t;

typedef struct flowmq_coronet_kcp_options_s {
  uint8_t pre_shared_key[FLOWMQ_KCP_PSK_SIZE];
  uint32_t mtu;
  uint32_t send_window;
  uint32_t receive_window;
  uint32_t interval_ms;
  uint32_t handshake_retry_ms;
  uint32_t fast_resend;
  int no_congestion_window;
  uint32_t data_shards;
  uint32_t parity_shards;
  uint32_t max_payload_size;
  uint32_t receive_group_count;
} flowmq_coronet_kcp_options_t;

typedef struct flowmq_coronet_tls_client_config_s {
  const char *ca_file;
  const char *cert_file;
  const char *key_file;
  const char *key_password;
  const char *server_name;
  int verify_peer;
} flowmq_coronet_tls_client_config_t;

/**
 * TLS listener configuration. The endpoint copies every referenced string.
 * cert_file and key_file are required for TLS/WSS. ca_file is required when
 * require_client_certificate is non-zero.
 */
typedef struct flowmq_coronet_tls_server_config_s {
  const char *ca_file;
  const char *cert_file;
  const char *key_file;
  const char *key_password;
  int require_client_certificate;
} flowmq_coronet_tls_server_config_t;

/**
 * Validate one transport value without creating a socket.
 * @param transport Transport to validate.
 * @return TURBO_OK or TURBO_EINVAL.
 */
FLOWMQ_C_API int flowmq_coronet_transport_validate(flowmq_coronet_transport_t transport);

/**
 * Establish the process-wide CoroNet TLS 1.3-only policy. Call during process
 * startup before any TLS endpoint is created; do not toggle it at runtime.
 */
FLOWMQ_C_API int flowmq_coronet_tls_require_tls13(void);

/** Return non-zero only while the process-wide CoroNet policy is TLS 1.3-only. */
FLOWMQ_C_API int flowmq_coronet_tls_is_tls13_only(void);

/**
 * Resolve unset operation timeouts from one default value.
 * Explicit zero values remain disabled when their corresponding set flag is present.
 * @param timeouts Mutable timeout value; NULL is ignored.
 * @param fallback_timeout_ms Used only when no default timeout was supplied.
 */
FLOWMQ_C_API void flowmq_coronet_timeouts_resolve(flowmq_coronet_timeout_config_t *timeouts,
                                               uint64_t fallback_timeout_ms);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_CORONET_H */
