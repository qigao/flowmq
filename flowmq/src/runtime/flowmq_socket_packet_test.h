#ifndef FLOWMQ_SOCKET_PACKET_TEST_H
#define FLOWMQ_SOCKET_PACKET_TEST_H

#include "flowmq_socket.h"
#include <cnet/cnet.h>

typedef void (*flowmq_packet_test_observer)(
    void *user, size_t peer_index, cnet_packet_session session, size_t size,
    int status, uint64_t tag);

/* The test archive exposes the actual terminal callback for stale-event
 * replay. The observer runs only when the callback resolves a live peer. */
cnet_packet_send_terminal_fn flowmq_socket_test_observe_packet_sends(
    flowmq_socket_t *socket, flowmq_packet_test_observer observer, void *user);

#endif
