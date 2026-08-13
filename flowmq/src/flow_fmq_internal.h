#ifndef FLOW_FMQ_INTERNAL_H
#define FLOW_FMQ_INTERNAL_H

#include "turbo_flow_fmq.h"

/* Longest accepted endpoint host (hostname, IPv4 or bracketed IPv6). The
   scheme-prefixed form is bounded to keep parsing deterministic. */
#define FLOW_FMQ_ENDPOINT_HOST_MAX 1024u

/* Resolved endpoint after an optional "<scheme>://" prefix on
   turbo_flow_fmq_config_t::host has been parsed. */
typedef struct {
  turbo_flow_fmq_transport_t transport;
  /* Borrowed: points into config->host when no scheme was present, or into the
     caller-provided host_buf when a scheme prefix was stripped. */
  const char *host;
  int port;
  int scheme_used;
} flow_fmq_endpoint_effective_t;

/* Resolve the effective transport/host/port from config. When config->host
   carries a known scheme prefix ("tcp://", "tls://", "udp://", "kcp://",
   "ws://", "wss://"), the prefix selects the transport and the host[:port]
   remainder is written into host_buf (host_cap bytes). Returns:
   - TURBO_OK: *out filled. host_buf holds the stripped host when a scheme was
     present; otherwise *out->host borrows config->host and host_buf is unused.
   - TURBO_EINVAL: unknown/empty scheme, malformed host:port, an embedded port
     that conflicts with config->port, or an explicit transport that conflicts
     with the scheme.
   - TURBO_ENAMETOOLONG: the stripped host does not fit host_buf.
   A NULL/empty config->host resolves to config->transport with an empty host. */
int flow_fmq_endpoint_effective(const turbo_flow_fmq_config_t *config,
                                char *host_buf, size_t host_cap,
                                flow_fmq_endpoint_effective_t *out);

int flow_fmq_app_create_endpoint_internal(
    const char *name, const turbo_flow_fmq_config_t *endpoint,
    const turbo_flow_fmq_fanout_config_t *fanout,
    const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out);

int flow_fmq_app_create_resolved_endpoint_internal(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out,
    turbo_flow_config_error_t *error);

#endif
