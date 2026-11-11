#if defined(_WIN32)
  #include <winsock2.h>
typedef SOCKET relay_socket;
typedef int relay_address_size;
typedef int relay_send_size;
#else
  #include <netinet/in.h>
  #include <sys/socket.h>
typedef int relay_socket;
typedef socklen_t relay_address_size;
typedef size_t relay_send_size;
#endif

#include "flowmq_protocol.h"
#include "flowmq_socket.h"
#include "tinytest.h"

#include <cnet/cnet.h>
#include <core/clock.h>
#include <core/error.h>
#include <native_io_socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  KCP_PUSH_COMMAND = 81,
  KCP_COMMAND_OFFSET = 4,
  KCP_SEQUENCE_OFFSET = 12,
  KCP_WARMUP_DATAGRAMS = 12,
  RELAY_BATCH_SIZE = 64,
  PROGRESS_TIMEOUT_MS = 5000,
  DUPLICATE_OBSERVE_MS = 100
};

typedef enum relay_phase {
  RELAY_PASS,
  RELAY_WARMUP,
  RELAY_HOLD,
  RELAY_REORDER,
  RELAY_DROP,
  RELAY_RECOVER,
  RELAY_COMPLETE
} relay_phase;

typedef struct udp_relay {
  uintptr_t handle;
  struct sockaddr_in server;
  struct sockaddr_in client;
  relay_phase phase;
  size_t warmup_datagrams;
  uint32_t held_sequence;
  uint32_t later_sequence;
  uint32_t dropped_sequence;
  unsigned char held[CNET_KCP_DEFAULT_MTU];
} udp_relay;

static int loopback_address(uint16_t port, struct sockaddr_in *address) {
  const native_io_stream_endpoint endpoint = {
      .family = NATIVE_IO_DATAGRAM_ADDRESS_IPV4, .port = port, .address = {127, 0, 0, 1}};
  size_t size;
  return native_io_socket_encode_datagram_endpoint(&endpoint, address, sizeof(*address), &size);
}

static int relay_send(const udp_relay *relay, const struct sockaddr_in *address,
                      const unsigned char *packet, size_t size) {
  const int sent = (int)sendto((relay_socket)relay->handle, (const char *)packet,
                             (relay_send_size)size, 0, (const struct sockaddr *)address,
                             (relay_address_size)sizeof(*address));
  return sent == (int)size ? SALTS_OK : SALTS_EIO;
}

static int relay_to_server(udp_relay *relay, const unsigned char *packet, size_t size) {
  /* A full-MTU plain PUSH has one KCP header with a little-endian sequence.
   * Leave small handshake, control and tail packets on their normal path. */
  if (size != sizeof(relay->held) || packet[KCP_COMMAND_OFFSET] != KCP_PUSH_COMMAND)
    return relay_send(relay, &relay->server, packet, size);
  const unsigned char *sn = packet + KCP_SEQUENCE_OFFSET;
  const uint32_t sequence = (uint32_t)sn[0] | (uint32_t)sn[1] << 8u |
                            (uint32_t)sn[2] << 16u | (uint32_t)sn[3] << 24u;
  switch (relay->phase) {
    case RELAY_WARMUP:
      /* Grow the congestion window before holding two consecutive segments. */
      if (++relay->warmup_datagrams == KCP_WARMUP_DATAGRAMS) relay->phase = RELAY_HOLD;
      break;
    case RELAY_HOLD:
      memcpy(relay->held, packet, size);
      relay->held_sequence = sequence;
      relay->phase = RELAY_REORDER;
      return SALTS_OK;
    case RELAY_REORDER:
      if (sequence == relay->held_sequence) return SALTS_OK;
      if (sequence > relay->held_sequence) {
        int status = relay_send(relay, &relay->server, packet, size);
        if (status != SALTS_OK) return status;
        status = relay_send(relay, &relay->server, packet, size);
        if (status != SALTS_OK) return status;
        relay->later_sequence = sequence;
        relay->phase = RELAY_DROP;
        return relay_send(relay, &relay->server, relay->held, sizeof(relay->held));
      }
      break;
    case RELAY_DROP:
      if (sequence > relay->later_sequence) {
        relay->dropped_sequence = sequence;
        relay->phase = RELAY_RECOVER;
        return SALTS_OK;
      }
      break;
    case RELAY_RECOVER:
      if (sequence == relay->dropped_sequence) relay->phase = RELAY_COMPLETE;
      break;
    case RELAY_PASS:
    case RELAY_COMPLETE:
      break;
  }
  return relay_send(relay, &relay->server, packet, size);
}

static int relay_poll(udp_relay *relay) {
  for (size_t i = 0u; i < RELAY_BATCH_SIZE; ++i) {
    native_io_socket_pollfd fd = {.handle = relay->handle, .events = NATIVE_IO_SOCKET_POLL_READ};
    int ready;
    const native_io_socket_result result = native_io_socket_poll(&fd, 1u, &ready);
    if (result.status != SALTS_OK || ready == 0) return result.status;
    if (fd.revents != NATIVE_IO_SOCKET_POLL_READ) return SALTS_EIO;

    unsigned char packet[CNET_KCP_DEFAULT_MTU];
    struct sockaddr_in source;
    relay_address_size address_size = (relay_address_size)sizeof(source);
    const int size = (int)recvfrom((relay_socket)relay->handle, (char *)packet,
                                  (relay_send_size)sizeof(packet), 0,
                                  (struct sockaddr *)&source, &address_size);
    if (size <= 0) return SALTS_EIO;
    int status;
    if (source.sin_port == relay->server.sin_port) {
      status = relay_send(relay, &relay->client, packet, (size_t)size);
    } else {
      relay->client = source;
      status = relay_to_server(relay, packet, (size_t)size);
    }
    if (status != SALTS_OK) return status;
  }
  return SALTS_OK;
}

static int wait_for(udp_relay *relay, flowmq_socket_t *server, flowmq_socket_t *client,
                    int events, uint32_t timeout_ms) {
  flowmq_pollitem_t items[] = {{.socket = server, .events = events}, {.socket = client}};
  const bool handshake = events == FLOWMQ_POLLOUT;
  if (handshake) items[1].events = events;
  const uint64_t start = monotonic_ms();
  do {
    int status = relay_poll(relay);
    if (status != SALTS_OK) return status;
    size_t ready;
    status = flowmq_poll(items, 2u, 1u, &ready);
    if (status != SALTS_OK) return status;
    if ((items[0].revents | items[1].revents) & FLOWMQ_POLLERR) return SALTS_EIO;
    if (ready == (handshake ? 2u : 1u)) return SALTS_OK;
  } while (monotonic_ms() - start < timeout_ms);
  return SALTS_ETIMEDOUT;
}

static void receive_part(udp_relay *relay, flowmq_socket_t *server, flowmq_socket_t *client,
                         const void *expected, size_t size, int more) {
  unsigned char data[FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE];
  size_t actual;
  int actual_more;
  size_t option_size = sizeof(actual_more);
  check_equal(wait_for(relay, server, client, FLOWMQ_POLLIN, PROGRESS_TIMEOUT_MS), SALTS_OK);
  check_equal(flowmq_recv(server, data, sizeof(data), &actual, FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(actual, size);
  check_equal(memcmp(data, expected, size), 0);
  check_equal(flowmq_getsockopt(server, FLOWMQ_RCVMORE, &actual_more, &option_size), SALTS_OK);
  check_equal(actual_more, more);
}

spec("FlowMQ KCP over impaired UDP") {
  static flowmq_ctx_t *ctx;
  static flowmq_socket_t *server, *client;
  static udp_relay relay;
  static bool network_started;

  after_each() {
    if (client != NULL) check_equal(flowmq_close(client), SALTS_OK);
    if (server != NULL) check_equal(flowmq_close(server), SALTS_OK);
    if (ctx != NULL) check_equal(flowmq_ctx_term(ctx), SALTS_OK);
    if (relay.handle != UINTPTR_MAX)
      check_equal(native_io_socket_close(relay.handle).status, SALTS_OK);
    if (network_started) native_io_network_shutdown();
  }

  it("retransmits a dropped segment and delivers ordered multipart once") {
    relay.handle = UINTPTR_MAX;
    check_equal(native_io_network_init(), SALTS_OK);
    network_started = true;
    ctx = flowmq_ctx_new();
    check_not_null(ctx);
    server = flowmq_socket(ctx, FLOWMQ_PAIR);
    client = flowmq_socket(ctx, FLOWMQ_PAIR);
    check_not_null(server);
    check_not_null(client);

    char uri[128];
    size_t endpoint_size;
    check_equal(flowmq_bind(server, "kcp://127.0.0.1:0?conv=4281"), SALTS_OK);
    check_equal(flowmq_last_endpoint(server, uri, sizeof(uri), &endpoint_size), SALTS_OK);
    const char *port_text = strrchr(uri, ':');
    check_not_null(port_text);
    const unsigned long server_port = strtoul(port_text + 1, NULL, 10);
    check(server_port > 0u && server_port <= UINT16_MAX);
    check_equal(loopback_address((uint16_t)server_port, &relay.server), SALTS_OK);

    struct sockaddr_in local;
    struct sockaddr_storage bound;
    native_io_stream_endpoint relay_endpoint;
    size_t local_size;
    check_equal(loopback_address(0u, &local), SALTS_OK);
    check_equal(native_io_socket_create_unattached(AF_INET, true, &relay.handle).status, SALTS_OK);
    check_equal(native_io_socket_bind(relay.handle, &local, sizeof(local)).status, SALTS_OK);
    check_equal(native_io_socket_nonblocking(relay.handle).status, SALTS_OK);
    check_equal(native_io_socket_query(relay.handle, false, &bound, sizeof(bound),
                                      &local_size).status, SALTS_OK);
    check_equal(native_io_socket_decode_endpoint(&bound, local_size, &relay_endpoint, true),
                SALTS_OK);
    const int written = snprintf(uri, sizeof(uri), "kcp://127.0.0.1:%u?conv=4281",
                                 (unsigned int)relay_endpoint.port);
    check(written > 0 && (size_t)written < sizeof(uri));
    check_equal(flowmq_connect(client, uri), SALTS_OK);
    check_equal(wait_for(&relay, server, client, FLOWMQ_POLLOUT, PROGRESS_TIMEOUT_MS), SALTS_OK);

    unsigned char payload[FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE];
    for (size_t i = 0u; i < sizeof(payload); ++i)
      payload[i] = (unsigned char)(i ^ (i >> 7u) ^ (i >> 14u));
    relay.phase = RELAY_WARMUP;
    check_equal(flowmq_send(client, payload, sizeof(payload), FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE),
                SALTS_OK);
    check_equal(flowmq_send(client, "tail", 4u, FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_send(client, "next", 4u, FLOWMQ_DONTWAIT), SALTS_OK);
    receive_part(&relay, server, client, payload, sizeof(payload), 1);
    receive_part(&relay, server, client, "tail", 4u, 0);
    receive_part(&relay, server, client, "next", 4u, 0);
    check_equal(relay.phase, RELAY_COMPLETE);
    check(relay.held_sequence < relay.later_sequence && relay.later_sequence < relay.dropped_sequence);
    check_equal(wait_for(&relay, server, client, FLOWMQ_POLLIN, DUPLICATE_OBSERVE_MS),
                SALTS_ETIMEDOUT);
  }
}
