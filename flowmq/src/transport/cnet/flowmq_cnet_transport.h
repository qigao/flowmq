#ifndef FLOWMQ_CNET_TRANSPORT_H
#define FLOWMQ_CNET_TRANSPORT_H

#include "flowmq_transport.h"

#include <cnet/cnet.h>

int flowmq_cnet_endpoint_validate(flowmq_transport_t transport, const char *host, int port);
int flowmq_cnet_client_config(const flowmq_io_config_t *io,
                              const flowmq_timeout_config_t *timeouts,
                              flowmq_transport_t transport, size_t connection_capacity,
                              size_t max_send_bytes, cnet_client_config *out);
int flowmq_cnet_uri(flowmq_transport_t transport, const char *host, int port, char **out_uri);
native_io_backend_kind flowmq_cnet_backend(void);

#endif /* FLOWMQ_CNET_TRANSPORT_H */
