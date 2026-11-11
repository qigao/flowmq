#ifndef FLOWMQ_CNET_TRANSPORT_H
#define FLOWMQ_CNET_TRANSPORT_H

#include "flowmq_transport.h"

#include <cnet/cnet.h>
#include <cnet/ipc.h>

enum {
  FLOWMQ_CNET_HOST_CAPACITY = 256u,
  FLOWMQ_CNET_URI_CAPACITY = 320u
};

typedef struct flowmq_cnet_endpoint_s {
  flowmq_transport_t transport;
  char host[FLOWMQ_CNET_HOST_CAPACITY];
  uint16_t port;
} flowmq_cnet_endpoint_t;

typedef enum flowmq_cnet_capability_e {
  FLOWMQ_CNET_RETAINED_SEND = 1u << 0,
  FLOWMQ_CNET_OWNED_RECEIVE = 1u << 1,
  FLOWMQ_CNET_EXTERNAL_OWNER = 1u << 2,
  FLOWMQ_CNET_EXTERNAL_LISTENER = 1u << 3
} flowmq_cnet_capability_t;

static inline bool flowmq_cnet_supports(flowmq_transport_t transport,
                                        flowmq_cnet_capability_t capability) {
  switch (transport) {
    case FLOWMQ_TRANSPORT_TCP:
      return (capability & (FLOWMQ_CNET_RETAINED_SEND | FLOWMQ_CNET_OWNED_RECEIVE |
                            FLOWMQ_CNET_EXTERNAL_OWNER | FLOWMQ_CNET_EXTERNAL_LISTENER)) != 0;
    case FLOWMQ_TRANSPORT_TLS:
      return capability == FLOWMQ_CNET_RETAINED_SEND;
    case FLOWMQ_TRANSPORT_IPC:
      return capability == FLOWMQ_CNET_EXTERNAL_OWNER;
    default:
      return false;
  }
}

flowmq_transport_t flowmq_cnet_uri_transport(const char *uri);
int flowmq_cnet_endpoint_parse(const char *uri, bool allow_zero_port,
                               flowmq_cnet_endpoint_t *endpoint);
int flowmq_cnet_endpoint_format(const flowmq_cnet_endpoint_t *endpoint,
                                char *uri, size_t capacity);
int flowmq_cnet_ipc_listener_close(cnet_ipc_listener *listener, uint32_t timeout_ms);
int flowmq_cnet_client_config(const flowmq_io_config_t *io,
                              const flowmq_timeout_config_t *timeouts,
                              flowmq_transport_t transport, size_t connection_capacity,
                              size_t max_send_bytes, cnet_client_config *out);
native_io_backend_kind flowmq_cnet_backend(void);

#endif /* FLOWMQ_CNET_TRANSPORT_H */
