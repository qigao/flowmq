#include "fmq_protocol.h"
#include "flow_fmq_internal.h"

#include "CoroNet/turbo_kcp.h"
#include "flowmq_coronet_transport.h"
#include "flow_coronet_runtime.h"
#include "turbo_error.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_FMQ_DEFAULT_TIMEOUT_MS 1000u

static void flow_fmq_timeout_config_resolve(tf_coronet_socket_timeout_config_t *timeouts,
                                            const turbo_flow_fmq_config_t *config) {
  if (!timeouts || !config) return;
  timeouts->timeout_ms = config->timeout_ms;
  timeouts->connect_timeout_ms = config->connect_timeout_ms;
  timeouts->send_timeout_ms = config->send_timeout_ms;
  timeouts->recv_timeout_ms = config->recv_timeout_ms;
  timeouts->handshake_timeout_ms = config->handshake_timeout_ms;
  if (timeouts->timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_DEFAULT;
  if (timeouts->connect_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_CONNECT;
  if (timeouts->send_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_SEND;
  if (timeouts->recv_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_RECV;
  if (timeouts->handshake_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_HANDSHAKE;
  if (timeouts->timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->timeout_ms = 0;
  if (timeouts->connect_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED)
    timeouts->connect_timeout_ms = 0;
  if (timeouts->send_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->send_timeout_ms = 0;
  if (timeouts->recv_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->recv_timeout_ms = 0;
  if (timeouts->handshake_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED)
    timeouts->handshake_timeout_ms = 0;
  tf_coronet_socket_timeouts_resolve(timeouts, FLOW_FMQ_DEFAULT_TIMEOUT_MS);
}

static int flow_fmq_coronet_options_validate(const turbo_flow_fmq_config_t *config) {
  tf_coronet_transport_t transport;
  turbo_kcp_config_t kcp_config;
  tf_coronet_socket_options_t socket_options;
  tf_coronet_udp_options_t udp_options;
  int kcp_configured;
  int rc;
  if (!config) return TURBO_EINVAL;
  transport = flowmq_coronet_transport_coronet((flowmq_coronet_transport_t)config->transport);

  rc = tf_coronet_endpoint_config_validate(transport, config->host, config->port, config->path);
  if (rc != TURBO_OK) return rc;
  rc = tf_coronet_reuse_port_validate(transport, config->reuse_port,
                                      config->mode == TURBO_FLOW_FMQ_BIND);
  if (rc != TURBO_OK) return rc;

  rc = flow_fmq_kcp_config_resolve(config, &kcp_config, &kcp_configured);
  if (rc != TURBO_OK) return rc;

  memset(&socket_options, 0, sizeof(socket_options));
  socket_options.tcp_keepalive = config->tcp_keepalive;
  socket_options.tcp_keepalive_idle_ms = config->tcp_keepalive_idle_ms;
  socket_options.tcp_keepalive_interval_ms = config->tcp_keepalive_interval_ms;
  socket_options.tcp_keepalive_count = config->tcp_keepalive_count;
  socket_options.linger = config->linger;
  socket_options.linger_ms = config->linger_ms;
  socket_options.send_hwm_bytes = config->send_hwm_bytes;
  socket_options.socket_recv_buffer_bytes = config->socket_recv_buffer_bytes;
  socket_options.socket_send_buffer_bytes = config->socket_send_buffer_bytes;
  rc = tf_coronet_socket_options_validate(transport, &socket_options);
  if (rc != TURBO_OK) return rc;

  memset(&udp_options, 0, sizeof(udp_options));
  udp_options.multicast_group = config->udp_multicast_group;
  udp_options.multicast_interface = config->udp_multicast_interface;
  udp_options.option_flags = config->udp_option_flags;
  udp_options.multicast_loop = config->udp_multicast_loop;
  udp_options.multicast_ttl = config->udp_multicast_ttl;
  udp_options.broadcast = config->udp_broadcast;
  return tf_coronet_udp_options_validate(transport, &udp_options,
                                         config->mode == TURBO_FLOW_FMQ_BIND);
}

int flow_fmq_kcp_config_resolve(const turbo_flow_fmq_config_t *config,
                                turbo_kcp_config_t *out,
                                int *configured) {
  tf_coronet_kcp_options_t options;
  turbo_kcp_config_t defaults;
  if (!config || !out || !configured) return TURBO_EINVAL;
  turbo_kcp_config_default(&defaults);
  memset(&options, 0, sizeof(options));
  if (config->transport != TURBO_FLOW_FMQ_KCP) {
    if (config->kcp_pre_shared_key || config->kcp_mtu ||
        config->kcp_send_window || config->kcp_receive_window ||
        config->kcp_interval_ms || config->kcp_handshake_retry_ms ||
        config->kcp_fast_resend || config->kcp_congestion_control ||
        config->kcp_fec_data_shards || config->kcp_fec_parity_shards ||
        config->kcp_fec_max_payload_size ||
        config->kcp_fec_receive_groups)
      return TURBO_EINVAL;
    return flowmq_coronet_transport_kcp_resolve(
        (flowmq_coronet_transport_t)config->transport, &options, out,
        configured);
  }
  if (tf_coronet_kcp_pre_shared_key_parse(config->kcp_pre_shared_key,
                                          options.pre_shared_key) != TURBO_OK)
    return TURBO_EINVAL;
  options.mtu = config->kcp_mtu ? config->kcp_mtu : defaults.mtu;
  options.send_window = config->kcp_send_window
                            ? config->kcp_send_window
                            : defaults.send_window;
  options.receive_window = config->kcp_receive_window
                               ? config->kcp_receive_window
                               : defaults.receive_window;
  options.interval_ms = config->kcp_interval_ms
                            ? config->kcp_interval_ms
                            : defaults.interval_ms;
  options.handshake_retry_ms =
      config->kcp_handshake_retry_ms ? config->kcp_handshake_retry_ms
                                     : defaults.handshake_retry_ms;
  options.fast_resend = config->kcp_fast_resend
                            ? config->kcp_fast_resend
                            : defaults.fast_resend;
  options.no_congestion_window = config->kcp_congestion_control ? 0 : 1;
  options.data_shards = config->kcp_fec_data_shards
                            ? config->kcp_fec_data_shards
                            : defaults.fec.data_shards;
  options.parity_shards = config->kcp_fec_parity_shards
                              ? config->kcp_fec_parity_shards
                              : defaults.fec.parity_shards;
  options.max_payload_size =
      config->kcp_fec_max_payload_size ? config->kcp_fec_max_payload_size
                                       : defaults.fec.max_payload_size;
  options.receive_group_count =
      config->kcp_fec_receive_groups ? config->kcp_fec_receive_groups
                                     : defaults.fec.receive_group_count;
  return flowmq_coronet_transport_kcp_resolve(
      (flowmq_coronet_transport_t)config->transport, &options, out,
      configured);
}

static int flow_fmq_frame_hwm_validate(const turbo_flow_fmq_config_t *config) {
  if (!config) return TURBO_EINVAL;
  if (config->frame_hwm_bytes != 0 && config->frame_hwm_bytes < FLOW_FMQ_HEADER_SIZE) {
    return TURBO_ERANGE;
  }
  if (config->frame_admission_policy < TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL ||
      config->frame_admission_policy > TURBO_FLOW_FMQ_FRAME_ADMISSION_DROP_OLDEST) {
    return TURBO_EINVAL;
  }
  if (config->frame_admission_policy != TURBO_FLOW_FMQ_FRAME_ADMISSION_BLOCK &&
      config->frame_admission_timeout_ms != 0) {
    return TURBO_EINVAL;
  }
  if (config->frame_admission_policy != TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL &&
      config->frame_hwm_messages == 0 && config->frame_hwm_bytes == 0) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int flow_fmq_heartbeat_config_validate(const turbo_flow_fmq_config_t *config) {
  if (!config) return TURBO_EINVAL;
  if (config->heartbeat_interval_ms == 0 && config->heartbeat_timeout_ms == 0) return TURBO_OK;
  if (config->heartbeat_interval_ms == 0 || config->heartbeat_timeout_ms == 0) {
    return TURBO_EINVAL;
  }
  return config->heartbeat_timeout_ms < config->heartbeat_interval_ms ? TURBO_ERANGE : TURBO_OK;
}

static int flow_fmq_tls_config_validate(const turbo_flow_fmq_config_t *config) {
  const turbo_flow_fmq_tls_config_t *tls;
  if (!config || !config->tls) return TURBO_OK;
  tls = config->tls;
  if (tls->size != sizeof(*tls) ||
      (tls->verify_peer != 0 && tls->verify_peer != 1) ||
      (tls->require_client_certificate != 0 && tls->require_client_certificate != 1) ||
      tls->rotation_generation == 0u) {
    return TURBO_EINVAL;
  }
  if (config->transport != TURBO_FLOW_FMQ_TLS && config->transport != TURBO_FLOW_FMQ_WSS) {
    return TURBO_ENOTSUP;
  }
  if ((tls->cert_file && !tls->key_file) || (!tls->cert_file && tls->key_file) ||
      (tls->key_password && !tls->key_file)) {
    return TURBO_EINVAL;
  }
  if (config->mode == TURBO_FLOW_FMQ_CONNECT) {
    if (!tls->verify_peer || !tls->server_name || !tls->server_name[0] ||
        tls->require_client_certificate) {
      return TURBO_EINVAL;
    }
  } else if (!tls->cert_file || !tls->cert_file[0] || !tls->key_file ||
             !tls->key_file[0] || tls->server_name ||
             (tls->require_client_certificate && (!tls->ca_file || !tls->ca_file[0]))) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int flow_fmq_scheme_lookup(const char *scheme, size_t len,
                                  turbo_flow_fmq_transport_t *out) {
  static const struct {
    const char *name;
    turbo_flow_fmq_transport_t transport;
  } kSchemes[] = {
      {"tcp", TURBO_FLOW_FMQ_TCP},
      {"tls", TURBO_FLOW_FMQ_TLS},
      {"udp", TURBO_FLOW_FMQ_UDP},
      {"kcp", TURBO_FLOW_FMQ_KCP},
      {"ws", TURBO_FLOW_FMQ_WS},
      {"wss", TURBO_FLOW_FMQ_WSS},
  };
  size_t i;
  for (i = 0u; i < sizeof(kSchemes) / sizeof(kSchemes[0]); ++i) {
    size_t name_len = strlen(kSchemes[i].name);
    if (name_len == len && memcmp(scheme, kSchemes[i].name, len) == 0) {
      *out = kSchemes[i].transport;
      return 1;
    }
  }
  return 0;
}

static int flow_fmq_port_parse(const char *text, int *out) {
  long value;
  char *end = NULL;
  if (!text || text[0] == '\0') return 0;
  errno = 0;
  value = strtol(text, &end, 10);
  if (errno != 0 || !end || *end != '\0' || value < 1 || value > 65535) return 0;
  *out = (int)value;
  return 1;
}

int flow_fmq_endpoint_effective(const turbo_flow_fmq_config_t *config,
                                char *host_buf, size_t host_cap,
                                flow_fmq_endpoint_effective_t *out) {
  const char *host;
  const char *rest;
  const char *colon;
  const char *scheme_end;
  turbo_flow_fmq_transport_t scheme_transport;
  size_t scheme_len;
  size_t host_len;
  size_t i;
  int embedded_port = 0;
  int has_embedded_port = 0;
  if (!config || !host_buf || host_cap == 0u || !out) return TURBO_EINVAL;
  out->host = NULL;
  out->port = config->port;
  out->scheme_used = 0;
  host = config->host;
  if (!host || host[0] == '\0') {
    out->transport = config->transport;
    out->host = host ? host : "";
    return TURBO_OK;
  }
  scheme_end = strstr(host, "://");
  if (!scheme_end) {
    out->transport = config->transport;
    out->host = host;
    return TURBO_OK;
  }
  scheme_len = (size_t)(scheme_end - host);
  if (scheme_len == 0u || !flow_fmq_scheme_lookup(host, scheme_len, &scheme_transport)) {
    return TURBO_EINVAL; /* unknown/empty scheme: fail fast, never guess */
  }
  rest = scheme_end + 3u;
  if (rest[0] == '\0') return TURBO_EINVAL;
  if (rest[0] == '[') {
    const char *close = strchr(rest + 1, ']');
    if (!close || close == rest + 1) return TURBO_EINVAL;
    host = rest + 1;
    host_len = (size_t)(close - host);
    if (close[1] != '\0') {
      if (close[1] != ':' || !flow_fmq_port_parse(close + 2, &embedded_port)) {
        return TURBO_EINVAL;
      }
      has_embedded_port = 1;
    }
  } else {
    size_t colon_count = 0u;
    for (i = 0u; rest[i] != '\0'; ++i) {
      if (rest[i] == ':') colon_count++;
    }
    if (colon_count == 0u) {
      host = rest;
      host_len = strlen(rest);
    } else if (colon_count == 1u) {
      colon = strchr(rest, ':');
      host = rest;
      host_len = (size_t)(colon - rest);
      if (host_len == 0u || !flow_fmq_port_parse(colon + 1, &embedded_port)) {
        return TURBO_EINVAL;
      }
      has_embedded_port = 1;
    } else {
      /* unbracketed IPv6 literal: the whole remainder is the host */
      host = rest;
      host_len = strlen(rest);
    }
  }
  if (host_len == 0u) return TURBO_EINVAL;
  for (i = 0u; i < host_len; ++i) {
    char c = host[i];
    if (c == '/' || c == ' ' || c == '\t' || c == '\r' || c == '\n') {
      return TURBO_EINVAL;
    }
  }
  if (host_len >= host_cap) return TURBO_ENAMETOOLONG;
  memcpy(host_buf, host, host_len);
  host_buf[host_len] = '\0';
  if (has_embedded_port) {
    if (config->port != 0 && config->port != embedded_port) return TURBO_EINVAL;
    out->port = embedded_port;
  } else {
    out->port = config->port;
  }
  if (config->transport != 0 && config->transport != scheme_transport) {
    return TURBO_EINVAL; /* explicit transport conflicts with the scheme prefix */
  }
  out->transport = scheme_transport;
  out->host = host_buf;
  out->scheme_used = 1;
  return TURBO_OK;
}

static int flow_fmq_config_validate_impl(const turbo_flow_fmq_config_t *config);

int flow_fmq_config_validate(const turbo_flow_fmq_config_t *config) {
  turbo_flow_fmq_config_t normalized;
  flow_fmq_endpoint_effective_t effective;
  char host_buf[FLOW_FMQ_ENDPOINT_HOST_MAX + 1u];
  int rc;

  if (!config || config->size != sizeof(*config)) return TURBO_EINVAL;
  /* Normalize an optional "<scheme>://" prefix on host so every downstream
     check observes the effective transport/host/port. */
  rc = flow_fmq_endpoint_effective(config, host_buf, sizeof(host_buf), &effective);
  if (rc != TURBO_OK) return rc;
  normalized = *config;
  normalized.transport = effective.transport;
  normalized.host = effective.host;
  normalized.port = effective.port;
  return flow_fmq_config_validate_impl(&normalized);
}

static int flow_fmq_config_validate_impl(const turbo_flow_fmq_config_t *config) {
  tf_coronet_socket_timeout_config_t timeouts;
  uint64_t connection_timeout_ms;
  size_t max_frame_size;
  uint32_t max_connections;
  int rc;

  if (!config || config->size != sizeof(*config) ||
      turbo_flow_fmq_pattern_validate(config->pattern) != TURBO_OK ||
      config->mode < TURBO_FLOW_FMQ_BIND || config->mode > TURBO_FLOW_FMQ_CONNECT ||
      config->transport < TURBO_FLOW_FMQ_TCP || config->transport > TURBO_FLOW_FMQ_WSS) {
    return TURBO_EINVAL;
  }
  if (config->timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED ||
      config->connect_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) {
    return TURBO_EINVAL;
  }
  if ((config->stream_recv_buffer_bytes != 0u &&
       config->stream_recv_buffer_bytes < TURBO_FLOW_FMQ_MIN_STREAM_RECV_BUFFER_SIZE) ||
      config->stream_recv_buffer_bytes > TURBO_FLOW_FMQ_MAX_STREAM_RECV_BUFFER_SIZE) {
    return TURBO_ERANGE;
  }
  memset(&timeouts, 0, sizeof(timeouts));
  flow_fmq_timeout_config_resolve(&timeouts, config);
  if (tf_coronet_connection_timeout_resolve(
          flowmq_coronet_transport_coronet((flowmq_coronet_transport_t)config->transport),
          &timeouts, &connection_timeout_ms) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  if (config->mode == TURBO_FLOW_FMQ_BIND &&
      (config->transport == TURBO_FLOW_FMQ_TLS || config->transport == TURBO_FLOW_FMQ_WS ||
       config->transport == TURBO_FLOW_FMQ_WSS) &&
      (timeouts.explicit_flags & TF_CORONET_TIMEOUT_SET_HANDSHAKE) != 0) {
    return TURBO_ENOTSUP;
  }
  rc = flow_fmq_coronet_options_validate(config);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_frame_hwm_validate(config);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_heartbeat_config_validate(config);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_tls_config_validate(config);
  if (rc != TURBO_OK) return rc;
  if ((config->pattern == TURBO_FLOW_FMQ_PUB || config->pattern == TURBO_FLOW_FMQ_PUSH ||
       config->pattern == TURBO_FLOW_FMQ_ROUTER || config->pattern == TURBO_FLOW_FMQ_REP ||
       config->pattern == TURBO_FLOW_FMQ_XPUB) &&
      config->mode != TURBO_FLOW_FMQ_BIND) {
    return TURBO_EINVAL;
  }
  if ((config->pattern == TURBO_FLOW_FMQ_SUB || config->pattern == TURBO_FLOW_FMQ_PULL ||
       config->pattern == TURBO_FLOW_FMQ_DEALER || config->pattern == TURBO_FLOW_FMQ_REQ ||
       config->pattern == TURBO_FLOW_FMQ_XSUB) &&
      config->mode != TURBO_FLOW_FMQ_CONNECT) {
    return TURBO_EINVAL;
  }
  if (config->pattern == TURBO_FLOW_FMQ_DEALER &&
      (!config->identity || config->identity[0] == '\0')) {
    return TURBO_EINVAL;
  }
  if (config->identity && strlen(config->identity) > TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE) {
    return TURBO_EMSGSIZE;
  }
  if (config->topic && strlen(config->topic) > TURBO_FLOW_FMQ_MAX_TOPIC_SIZE) {
    return TURBO_EMSGSIZE;
  }
  if (config->topic_policy != 0 && (config->topic_policy < TURBO_FLOW_FMQ_METADATA_STATIC ||
                                    config->topic_policy > TURBO_FLOW_FMQ_METADATA_CONTENT)) {
    return TURBO_EINVAL;
  }
  if (config->identity_policy != 0 && (config->identity_policy < TURBO_FLOW_FMQ_METADATA_STATIC ||
                                       config->identity_policy > TURBO_FLOW_FMQ_METADATA_CONTENT)) {
    return TURBO_EINVAL;
  }
  max_frame_size =
      config->max_frame_size ? config->max_frame_size : TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE;
  max_connections =
      config->max_connections ? config->max_connections : TURBO_FLOW_FMQ_DEFAULT_MAX_CONNECTIONS;
  if (max_frame_size == 0 || max_frame_size > UINT32_MAX || max_connections == 0 ||
      max_connections > TURBO_FLOW_FMQ_MAX_CONNECTIONS_LIMIT) {
    return TURBO_ERANGE;
  }
  if (config->mode == TURBO_FLOW_FMQ_CONNECT && config->max_connections != 0) return TURBO_EINVAL;
  if (config->reconnect_initial_ms > 0 && config->reconnect_initial_ms > config->reconnect_max_ms &&
      config->reconnect_max_ms != 0) {
    return TURBO_ERANGE;
  }
  return TURBO_OK;
}
