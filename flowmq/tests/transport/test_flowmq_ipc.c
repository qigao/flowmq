#if !defined(_WIN32)
#define _POSIX_C_SOURCE 200809L
#endif

#include "flowmq_peer_pool.h"
#include "flowmq_socket.h"
#include "tinytest.h"

#include <core/clock.h>
#include <core/error.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if !defined(_WIN32)
#include <sys/stat.h>
#endif

static int ipc_poll(flowmq_socket_t *first, flowmq_socket_t *second,
                    int events) {
  flowmq_pollitem_t items[] = {
      {.socket = first, .events = events}, {.socket = second}};
  const size_t count = second == NULL ? 1u : 2u;
  const uint64_t started = monotonic_ms();
  do {
    size_t ready;
    const int status = flowmq_poll(items, count, 1u, &ready);
    if (status != SALTS_OK) return status;
    if (items[0].revents & events) return SALTS_OK;
    if ((items[0].revents | items[1].revents) & FLOWMQ_POLLERR) return SALTS_EIO;
  } while (monotonic_ms() - started < 5000u);
  return SALTS_ETIMEDOUT;
}

static void ipc_bind(flowmq_socket_t *listener, const char *endpoint, bool *bound) {
  const int status = flowmq_bind(listener, endpoint);
  *bound = status == SALTS_OK;
  check_equal(status, SALTS_OK);
}

static void ipc_close_listener(flowmq_socket_t **listener, const char *endpoint,
                               bool *bound) {
  if (*listener != NULL) {
    check_equal(flowmq_close(*listener), SALTS_OK);
    *listener = NULL;
  }
#if defined(_WIN32)
  (void)endpoint;
#else
  if (*bound) check_equal(remove(endpoint + sizeof("ipc://") - 1u), 0);
#endif
  *bound = false;
}

static void ipc_receive(flowmq_socket_t *receiver, flowmq_socket_t *sender,
                        const void *expected, size_t expected_size, int more) {
  unsigned char data[64];
  size_t length;
  int received_more;
  size_t option_size = sizeof(received_more);
  check_equal(ipc_poll(receiver, sender, FLOWMQ_POLLIN), SALTS_OK);
  check_equal(flowmq_recv(receiver, data, sizeof(data), &length,
                           FLOWMQ_DONTWAIT), SALTS_OK);
  check_equal(length, expected_size);
  check_equal(memcmp(data, expected, expected_size), 0);
  check_equal(flowmq_getsockopt(receiver, FLOWMQ_RCVMORE, &received_more,
                               &option_size), SALTS_OK);
  check_equal(received_more, more);
}

static int ipc_wait_retired(flowmq_socket_t *router, const char *identity,
                            size_t identity_size) {
  flowmq_router_peer_status_t peer = FLOWMQ_ROUTER_PEER_STATUS_INIT;
  flowmq_pollitem_t item = {.socket = router};
  const uint64_t started = monotonic_ms();
  do {
    size_t ready;
    int status = flowmq_poll(&item, 1u, 1u, &ready);
    if (status != SALTS_OK) return status;
    if (item.revents & FLOWMQ_POLLERR) return SALTS_EIO;
    status = flowmq_router_peer_status(router, identity, identity_size, &peer);
    if (status == SALTS_ENOENT) return SALTS_OK;
    if (status != SALTS_OK) return status;
  } while (monotonic_ms() - started < 5000u);
  return SALTS_ETIMEDOUT;
}

spec("FlowMQ local IPC transport") {
  group("native IPC") {
    static flowmq_ctx_t *ctx;
    static flowmq_socket_t *server, *client, *neighbor;
    static mem_slice_t held;
    static char *directory;
    static char endpoint[256];
    static bool bound;

    before_each() {
      const char *name;
      int written;
      bound = false;
      client = NULL;
      server = NULL;
      neighbor = NULL;
      directory = NULL;
      held = (mem_slice_t){0};
      ctx = flowmq_ctx_new();
      check_not_null(ctx);
      directory = tt_make_temp_dir("fmqipc");
      check_not_null(directory);
      name = directory;
#if defined(_WIN32)
      for (const char *cursor = directory; *cursor != '\0'; ++cursor)
        if (*cursor == '/' || *cursor == '\\') name = cursor + 1;
      written = snprintf(endpoint, sizeof(endpoint), "ipc://%s", name);
#else
      written = snprintf(endpoint, sizeof(endpoint), "ipc://%s/socket", name);
#endif
      check(written > 0 && (size_t)written < sizeof(endpoint));
    }

    after_each() {
      mem_slice_release(&held);
      if (neighbor != NULL) check_equal(flowmq_close(neighbor), SALTS_OK);
      if (client != NULL) check_equal(flowmq_close(client), SALTS_OK);
      ipc_close_listener(&server, endpoint, &bound);
      if (directory != NULL) {
        check_equal(tt_remove_tree(directory), 0);
        free(directory);
      }
      if (ctx != NULL) check_equal(flowmq_ctx_term(ctx), SALTS_OK);
    }

    it("copies direct, queued multipart and reply traffic through IPC") {
      const char first[] = "first";
      const char last[] = "last";
      char last_endpoint[sizeof(endpoint)];
      unsigned char payload[32768], received[sizeof(payload)];
      size_t length;
      int more;
      client = flowmq_socket(ctx, FLOWMQ_PAIR);
      server = flowmq_socket(ctx, FLOWMQ_PAIR);
      check_not_null(client);
      check_not_null(server);
      ipc_bind(server, endpoint, &bound);
      check_equal(flowmq_last_endpoint(server, last_endpoint, sizeof(last_endpoint),
                                       &length), SALTS_OK);
      check_equal(strcmp(last_endpoint, endpoint), 0);
      check_equal(flowmq_connect(client, endpoint), SALTS_OK);
      check_equal(ipc_poll(client, server, FLOWMQ_POLLOUT), SALTS_OK);

      mem_buffer_t *buffer = mem_get_buffer(mem_global(), sizeof(first));
      check_not_null(buffer);
      memcpy(mem_buffer_data(buffer), first, sizeof(first));
      mem_set_used(buffer, sizeof(first));
      mem_slice_t retained = mem_slice(buffer, 0u, sizeof(first));
      const int retained_status = flowmq_send_slice(client, &retained, FLOWMQ_DONTWAIT);
      mem_slice_release(&retained);
      mem_buffer_release(buffer);
      check_equal(retained_status, SALTS_ENOTSUP);

      check_equal(flowmq_send(client, first, sizeof(first), FLOWMQ_DONTWAIT), SALTS_OK);
      memset(payload, 0xa5, sizeof(payload));
      check_equal(flowmq_send(client, payload, sizeof(payload),
                               FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
      memset(payload, 0, sizeof(payload));
      check_equal(flowmq_send(client, NULL, 0u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
      check_equal(flowmq_send(client, last, sizeof(last), FLOWMQ_DONTWAIT), SALTS_OK);

      const size_t sizes[] = {sizeof(first), sizeof(payload), 0u, sizeof(last)};
      for (size_t i = 0u; i < sizeof(sizes) / sizeof(sizes[0]); ++i) {
        size_t option_size = sizeof(more);
        check_equal(ipc_poll(server, client, FLOWMQ_POLLIN), SALTS_OK);
        check_equal(flowmq_recv(server, received, sizeof(received), &length,
                                 FLOWMQ_DONTWAIT), SALTS_OK);
        check_equal(length, sizes[i]);
        check_equal(flowmq_getsockopt(server, FLOWMQ_RCVMORE, &more, &option_size), SALTS_OK);
        check_equal(more, i == 1u || i == 2u ? 1 : 0);
        if (i == 0u) check_equal(memcmp(received, first, sizeof(first)), 0);
        if (i == 1u) {
          memset(payload, 0xa5, sizeof(payload));
          check_equal(memcmp(received, payload, sizeof(payload)), 0);
        }
        if (i == 3u) check_equal(memcmp(received, last, sizeof(last)), 0);
      }
      check_equal(flowmq_send(server, last, sizeof(last), FLOWMQ_DONTWAIT), SALTS_OK);
      check_equal(ipc_poll(client, server, FLOWMQ_POLLIN), SALTS_OK);
      check_equal(flowmq_recv(client, received, sizeof(received), &length,
                               FLOWMQ_DONTWAIT), SALTS_OK);
      check_equal(length, sizeof(last));
      check_equal(memcmp(received, last, sizeof(last)), 0);
    }

    it("preserves REQ/REP multipart state and held receive bytes over IPC") {
      static const char header[] = "request-header";
      static const char body[] = "request-body";
      static const char reply[] = "reply";
      const size_t max_request_bytes = sizeof(header) + sizeof(body);
      char oversized[sizeof(header) + sizeof(body) + 1u] = {0};
      int more;
      size_t option_size = sizeof(more);
      client = flowmq_socket(ctx, FLOWMQ_REQ);
      server = flowmq_socket(ctx, FLOWMQ_REP);
      check_not_null(client);
      check_not_null(server);
      check_equal(flowmq_setsockopt(client, FLOWMQ_SNDHWM_BYTES, &max_request_bytes,
                                   sizeof(max_request_bytes)), SALTS_OK);
      ipc_bind(server, endpoint, &bound);
      check_equal(flowmq_connect(client, endpoint), SALTS_OK);
      check_equal(ipc_poll(client, server, FLOWMQ_POLLOUT), SALTS_OK);
      check_equal(flowmq_send(client, oversized, sizeof(oversized), FLOWMQ_DONTWAIT),
                  SALTS_EMSGSIZE);
      check_equal(flowmq_send(client, header, sizeof(header),
                               FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
      check_equal(flowmq_send(client, body, sizeof(body), FLOWMQ_DONTWAIT), SALTS_OK);
      check_equal(flowmq_send(client, body, sizeof(body), FLOWMQ_DONTWAIT), SALTS_EPROTO);
      check_equal(ipc_poll(server, client, FLOWMQ_POLLIN), SALTS_OK);
      check_equal(flowmq_recv_slice(server, &held, FLOWMQ_DONTWAIT), SALTS_OK);
      check_equal(held.length, sizeof(header));
      check_equal(flowmq_getsockopt(server, FLOWMQ_RCVMORE, &more, &option_size), SALTS_OK);
      check_equal(more, 1);
      check_equal(flowmq_send(server, reply, sizeof(reply), FLOWMQ_DONTWAIT), SALTS_EPROTO);
      ipc_receive(server, client, body, sizeof(body), 0);
      check_equal(flowmq_send(server, reply, sizeof(reply), FLOWMQ_DONTWAIT), SALTS_OK);
      ipc_receive(client, server, reply, sizeof(reply), 0);
      check_equal(memcmp(held.data, header, sizeof(header)), 0);
    }

    it("routes DEALER identities and isolates an IPC peer with exhausted credit") {
      static const char slow[] = {'s', 'l', 'o', 'w'};
      static const char fast[] = {'f', 'a', 's', 't'};
      static const char request[] = "req";
      static const char response[] = "credit-window";
      const size_t receive_window = sizeof(response);
      flowmq_router_peer_status_t slow_status = FLOWMQ_ROUTER_PEER_STATUS_INIT;
      server = flowmq_socket(ctx, FLOWMQ_ROUTER);
      client = flowmq_socket(ctx, FLOWMQ_DEALER);
      neighbor = flowmq_socket(ctx, FLOWMQ_DEALER);
      check_not_null(server);
      check_not_null(client);
      check_not_null(neighbor);
      check_equal(flowmq_setsockopt(client, FLOWMQ_IDENTITY, slow, sizeof(slow)), SALTS_OK);
      check_equal(flowmq_setsockopt(neighbor, FLOWMQ_IDENTITY, fast, sizeof(fast)), SALTS_OK);
      check_equal(flowmq_setsockopt(client, FLOWMQ_RCVHWM_BYTES, &receive_window,
                                   sizeof(receive_window)), SALTS_OK);
      ipc_bind(server, endpoint, &bound);

      check_equal(flowmq_connect(client, endpoint), SALTS_OK);
      check_equal(ipc_poll(client, server, FLOWMQ_POLLOUT), SALTS_OK);
      check_equal(flowmq_send(client, request, sizeof(request), FLOWMQ_DONTWAIT), SALTS_OK);
      ipc_receive(server, client, slow, sizeof(slow), 1);
      ipc_receive(server, client, request, sizeof(request), 0);

      check_equal(flowmq_connect(neighbor, endpoint), SALTS_OK);
      check_equal(ipc_poll(neighbor, server, FLOWMQ_POLLOUT), SALTS_OK);
      check_equal(flowmq_send(neighbor, request, sizeof(request), FLOWMQ_DONTWAIT), SALTS_OK);
      ipc_receive(server, neighbor, fast, sizeof(fast), 1);
      ipc_receive(server, neighbor, request, sizeof(request), 0);

      check_equal(flowmq_send(server, slow, sizeof(slow), FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE),
                  SALTS_OK);
      check_equal(flowmq_send(server, response, sizeof(response), FLOWMQ_DONTWAIT), SALTS_OK);
      check_equal(ipc_poll(client, server, FLOWMQ_POLLIN), SALTS_OK);
      check_equal(flowmq_router_peer_status(server, slow, sizeof(slow), &slow_status), SALTS_OK);
      check_equal(slow_status.send_credit_bytes, 0u);
      check_equal(flowmq_send(server, slow, sizeof(slow), FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE),
                  SALTS_OK);
      check_equal(flowmq_send(server, response, sizeof(response), FLOWMQ_DONTWAIT), SALTS_ENOBUFS);
      check_equal(flowmq_send(server, fast, sizeof(fast), FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE),
                  SALTS_OK);
      check_equal(flowmq_send(server, response, sizeof(response), FLOWMQ_DONTWAIT), SALTS_OK);
      ipc_receive(neighbor, server, response, sizeof(response), 0);
      ipc_receive(client, server, response, sizeof(response), 0);
    }

    it("restores a SUB subscription after rebinding the IPC listener") {
      static const char prefix[] = "ipc.";
      static const unsigned char subscription[] = {1u, 'i', 'p', 'c', '.'};
      static const char unrelated[] = "other.data";
      static const char before[] = "ipc.before";
      static const char after[] = "ipc.after";
      const int retry_ms = 5;
      client = flowmq_socket(ctx, FLOWMQ_SUB);
      server = flowmq_socket(ctx, FLOWMQ_XPUB);
      check_not_null(client);
      check_not_null(server);
      check_equal(flowmq_setsockopt(client, FLOWMQ_SUBSCRIBE, prefix, sizeof(prefix) - 1u), SALTS_OK);
      check_equal(flowmq_setsockopt(client, FLOWMQ_RECONNECT_IVL, &retry_ms,
                                   sizeof(retry_ms)), SALTS_OK);
      ipc_bind(server, endpoint, &bound);
      check_equal(flowmq_connect(client, endpoint), SALTS_OK);
      ipc_receive(server, client, subscription, sizeof(subscription), 0);
      check_equal(flowmq_send(server, unrelated, sizeof(unrelated), FLOWMQ_DONTWAIT), SALTS_OK);
      check_equal(flowmq_send(server, before, sizeof(before), FLOWMQ_DONTWAIT), SALTS_OK);
      ipc_receive(client, server, before, sizeof(before), 0);

      ipc_close_listener(&server, endpoint, &bound);
      server = flowmq_socket(ctx, FLOWMQ_XPUB);
      check_not_null(server);
      ipc_bind(server, endpoint, &bound);
      ipc_receive(server, client, subscription, sizeof(subscription), 0);
      check_equal(flowmq_send(server, unrelated, sizeof(unrelated), FLOWMQ_DONTWAIT), SALTS_OK);
      check_equal(flowmq_send(server, after, sizeof(after), FLOWMQ_DONTWAIT), SALTS_OK);
      ipc_receive(client, server, after, sizeof(after), 0);
    }

    it("cancels an unfinished IPC multipart send before reconnecting") {
      static const char old[] = "old-part";
      static const char fresh[] = "fresh-message";
      const int retry_ms = 5;
      unsigned char data[32];
      size_t length;
      client = flowmq_socket(ctx, FLOWMQ_PAIR);
      server = flowmq_socket(ctx, FLOWMQ_PAIR);
      check_not_null(client);
      check_not_null(server);
      check_equal(flowmq_setsockopt(client, FLOWMQ_RECONNECT_IVL, &retry_ms,
                                   sizeof(retry_ms)), SALTS_OK);
      ipc_bind(server, endpoint, &bound);
      check_equal(flowmq_connect(client, endpoint), SALTS_OK);
      check_equal(ipc_poll(client, server, FLOWMQ_POLLOUT), SALTS_OK);
      check_equal(flowmq_send(client, old, sizeof(old), FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
      ipc_close_listener(&server, endpoint, &bound);
      check_equal(ipc_poll(client, NULL, FLOWMQ_POLLERR), SALTS_OK);
      check_equal(flowmq_send(client, old, sizeof(old), FLOWMQ_DONTWAIT), SALTS_ENOTCONN);

      server = flowmq_socket(ctx, FLOWMQ_PAIR);
      check_not_null(server);
      ipc_bind(server, endpoint, &bound);
      check_equal(ipc_poll(client, server, FLOWMQ_POLLOUT), SALTS_OK);
      check_equal(flowmq_send(client, fresh, sizeof(fresh), FLOWMQ_DONTWAIT), SALTS_OK);
      ipc_receive(server, client, fresh, sizeof(fresh), 0);
      check_equal(flowmq_recv(server, data, sizeof(data), &length, FLOWMQ_DONTWAIT), SALTS_EBUSY);
    }

    it("reuses IPC peer slots while retaining old inbound data and isolating close completions") {
      static const char replace[] = {'r', 'e', 'p', 'l', 'a', 'c', 'e'};
      static const char healthy[] = {'h', 'e', 'a', 'l', 't', 'h', 'y'};
      static const unsigned char pending[128u * 1024u] = {0xa5};
      const int hwm = 2;
      enum { GENERATIONS = 8 };
      size_t ready;

      server = flowmq_socket(ctx, FLOWMQ_ROUTER);
      client = flowmq_socket(ctx, FLOWMQ_DEALER);
      neighbor = flowmq_socket(ctx, FLOWMQ_DEALER);
      check_not_null(server);
      check_not_null(client);
      check_not_null(neighbor);
      check_equal(flowmq_setsockopt(server, FLOWMQ_SNDHWM, &hwm, sizeof(hwm)), SALTS_OK);
      check_equal(flowmq_setsockopt(client, FLOWMQ_IDENTITY, replace, sizeof(replace)), SALTS_OK);
      check_equal(flowmq_setsockopt(neighbor, FLOWMQ_IDENTITY, healthy, sizeof(healthy)), SALTS_OK);
      ipc_bind(server, endpoint, &bound);
      check_equal(flowmq_connect(neighbor, endpoint), SALTS_OK);
      check_equal(ipc_poll(neighbor, server, FLOWMQ_POLLOUT), SALTS_OK);
      check_equal(flowmq_connect(client, endpoint), SALTS_OK);
      check_equal(ipc_poll(client, server, FLOWMQ_POLLOUT), SALTS_OK);

      /* Keep the ROUTER and the healthy peer alive across more generations
       * than the listener has peer slots. */
      for (unsigned char generation = 0u; generation < GENERATIONS; ++generation) {
        const unsigned char probe = (unsigned char)(generation + 32u);
        flowmq_router_peer_status_t peer = FLOWMQ_ROUTER_PEER_STATUS_INIT;
        flowmq_pollitem_t listener = {.socket = server};
        check_equal(flowmq_send(client, &generation, sizeof(generation), FLOWMQ_DONTWAIT), SALTS_OK);
        check_equal(ipc_poll(server, client, FLOWMQ_POLLIN), SALTS_OK);

        for (int i = 0; i <= hwm; ++i) {
          check_equal(flowmq_send(server, replace, sizeof(replace),
                                   FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
          check_equal(flowmq_send(server, pending, sizeof(pending), FLOWMQ_DONTWAIT),
                      i < hwm ? SALTS_OK : SALTS_ENOBUFS);
        }
        /* Give CNet a write to settle while leaving the second send queued. */
        check_equal(flowmq_poll(&listener, 1u, 0u, &ready), SALTS_OK);
        check_equal(flowmq_router_peer_status(server, replace, sizeof(replace), &peer), SALTS_OK);
        check_equal(peer.outstanding_messages, (size_t)hwm);
        check_equal(flowmq_close(client), SALTS_OK);
        client = NULL;
        check_equal(ipc_wait_retired(server, replace, sizeof(replace)), SALTS_OK);

        client = flowmq_socket(ctx, FLOWMQ_DEALER);
        check_not_null(client);
        check_equal(flowmq_setsockopt(client, FLOWMQ_IDENTITY, replace, sizeof(replace)), SALTS_OK);
        check_equal(flowmq_connect(client, endpoint), SALTS_OK);
        check_equal(ipc_poll(client, server, FLOWMQ_POLLOUT), SALTS_OK);

        /* The old queued message remains readable after the same identity
         * reconnects. Its retirement must not change the new peer's counters. */
        ipc_receive(server, client, replace, sizeof(replace), 1);
        ipc_receive(server, client, &generation, sizeof(generation), 0);
        check_equal(flowmq_send(neighbor, &probe, sizeof(probe), FLOWMQ_DONTWAIT), SALTS_OK);
        ipc_receive(server, neighbor, healthy, sizeof(healthy), 1);
        ipc_receive(server, neighbor, &probe, sizeof(probe), 0);
        check_equal(flowmq_router_peer_status(server, replace, sizeof(replace), &peer), SALTS_OK);
        check_equal(peer.ready, 1);
        check_equal(peer.admitted_messages, 0u);
        check_equal(peer.completed_messages, 0u);
        check_equal(peer.rejected_messages, 0u);
        check_equal(peer.outstanding_messages, 0u);
        check_equal(flowmq_poll(&listener, 1u, 0u, &ready), SALTS_OK);
      }

      check_equal(flowmq_send(server, replace, sizeof(replace),
                               FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
      check_equal(flowmq_send(server, "fresh", sizeof("fresh"), FLOWMQ_DONTWAIT), SALTS_OK);
      ipc_receive(client, server, "fresh", sizeof("fresh"), 0);
    }

    it("closes and rebinds an IPC listener while dials are unadopted") {
      static const char payload[] = "after-listener-drain";
      const int retry_ms = 5;

      for (size_t i = 0u; i < 4u; ++i) {
        size_t ready;
        server = flowmq_socket(ctx, FLOWMQ_PAIR);
        client = flowmq_socket(ctx, FLOWMQ_PAIR);
        check_not_null(server);
        check_not_null(client);
        check_equal(flowmq_setsockopt(client, FLOWMQ_RECONNECT_IVL, &retry_ms,
                                     sizeof(retry_ms)), SALTS_OK);
        ipc_bind(server, endpoint, &bound);
        check_equal(flowmq_connect(client, endpoint), SALTS_OK);
        flowmq_pollitem_t dial = {.socket = client, .events = FLOWMQ_POLLOUT};
        /* The client advances alone while the listener has not adopted a peer. */
        check_equal(flowmq_poll(&dial, 1u, 1u, &ready), SALTS_OK);
        check_equal(dial.revents, 0);
        ipc_close_listener(&server, endpoint, &bound);
        server = flowmq_socket(ctx, FLOWMQ_PAIR);
        check_not_null(server);
        ipc_bind(server, endpoint, &bound);
        check_equal(ipc_poll(client, server, FLOWMQ_POLLOUT), SALTS_OK);
        check_equal(flowmq_send(client, payload, sizeof(payload), FLOWMQ_DONTWAIT), SALTS_OK);
        ipc_receive(server, client, payload, sizeof(payload), 0);
        check_equal(flowmq_close(client), SALTS_OK);
        client = NULL;
        ipc_close_listener(&server, endpoint, &bound);
      }
    }

#if !defined(_WIN32)
    it("preserves the POSIX pathname and the application's private directory") {
      const char *path = endpoint + sizeof("ipc://") - 1u;
      struct stat original, current, directory_status;
      server = flowmq_socket(ctx, FLOWMQ_PAIR);
      client = flowmq_socket(ctx, FLOWMQ_PAIR);
      neighbor = flowmq_socket(ctx, FLOWMQ_PAIR);
      check_not_null(server);
      check_not_null(client);
      check_not_null(neighbor);
      check_equal(chmod(directory, S_IRWXU), 0);
      ipc_bind(server, endpoint, &bound);
      check_equal(stat(path, &original), 0);
      check(S_ISSOCK(original.st_mode));
      check_not_equal(flowmq_bind(neighbor, endpoint), SALTS_OK);
      check_equal(flowmq_connect(client, endpoint), SALTS_OK);
      check_equal(ipc_poll(client, server, FLOWMQ_POLLOUT), SALTS_OK);
      check_equal(flowmq_close(server), SALTS_OK);
      server = NULL;

      check_equal(stat(path, &current), 0);
      check(S_ISSOCK(current.st_mode));
      check(current.st_dev == original.st_dev && current.st_ino == original.st_ino);
      check_equal(stat(directory, &directory_status), 0);
      check((directory_status.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) == S_IRWXU);
    }
#endif
  }

  it("rejects incompatible IPC policies before initializing the socket") {
#if defined(_WIN32)
    const char *endpoint = "ipc://flowmq-incompatible-options";
#else
    const char *endpoint = "ipc:///flowmq-incompatible-options";
#endif
    flowmq_peer_pool_config_t pool = FLOWMQ_PEER_POOL_CONFIG_INIT;
    const int enabled = 1;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *socket = flowmq_socket(ctx, FLOWMQ_PAIR);
    check_not_null(socket);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_REUSE_PORT, &enabled, sizeof(enabled)), SALTS_OK);
    check_equal(flowmq_bind(socket, endpoint), SALTS_ENOTSUP);
    check_equal(flowmq_socket_set_peer_pool(socket, &pool), SALTS_OK);
    check_equal(flowmq_connect(socket, endpoint), SALTS_ENOTSUP);
    check_equal(flowmq_close(socket), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }
}
