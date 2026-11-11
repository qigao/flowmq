#include "flowmq_peer_pool.h"
#include "flowmq_socket.h"
#include "tinytest.h"

#include <core/clock.h>
#include <core/error.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int ipc_poll(flowmq_socket_t *first, flowmq_socket_t *second,
                    int events) {
  flowmq_pollitem_t items[] = {
      {.socket = first, .events = events}, {.socket = second}};
  const uint64_t started = monotonic_ms();
  do {
    size_t ready;
    const int status = flowmq_poll(items, 2u, 1u, &ready);
    if (status != SALTS_OK) return status;
    if ((items[0].revents | items[1].revents) & FLOWMQ_POLLERR) return SALTS_EIO;
    if (items[0].revents & events) return SALTS_OK;
  } while (monotonic_ms() - started < 5000u);
  return SALTS_ETIMEDOUT;
}

spec("FlowMQ local IPC transport") {
  group("native IPC") {
    static flowmq_ctx_t *ctx;
    static flowmq_socket_t *server, *client;
    static char *directory;
    static char endpoint[256];
    static bool bound;

    before_each() {
      const char *name;
      int written;
      bound = false;
      ctx = flowmq_ctx_new();
      check_not_null(ctx);
      client = flowmq_socket(ctx, FLOWMQ_PAIR);
      server = flowmq_socket(ctx, FLOWMQ_PAIR);
      check_not_null(client);
      check_not_null(server);
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
      if (client != NULL) check_equal(flowmq_close(client), SALTS_OK);
      if (server != NULL) check_equal(flowmq_close(server), SALTS_OK);
#if !defined(_WIN32)
      if (bound) check_equal(remove(endpoint + sizeof("ipc://") - 1u), 0);
#endif
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
      const int status = flowmq_bind(server, endpoint);
      bound = status == SALTS_OK;
      check_equal(status, SALTS_OK);
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
