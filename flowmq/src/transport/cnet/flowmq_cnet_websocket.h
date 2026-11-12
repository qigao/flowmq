#ifndef FLOWMQ_CNET_WEBSOCKET_H
#define FLOWMQ_CNET_WEBSOCKET_H

#include "flowmq_cnet_transport.h"

enum {
  FLOWMQ_WS_HEADER_LIMIT = 8192u,
  FLOWMQ_WS_ACCEPT_CAPACITY = 29u
};

int flowmq_ws_client_request(const flowmq_cnet_endpoint_t *endpoint,
                             char *request, size_t capacity, size_t *size,
                             char accept[FLOWMQ_WS_ACCEPT_CAPACITY]);
int flowmq_ws_upgrade_validate(bool server, const char *data, size_t size,
                               const char *path,
                               char accept[FLOWMQ_WS_ACCEPT_CAPACITY]);
int flowmq_ws_server_response(const char *accept, char *response,
                              size_t capacity, size_t *size);
const char *flowmq_ws_server_rejection(int status);

#endif
