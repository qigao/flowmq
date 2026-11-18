#include "flowmq_socket.h"
#include "flowmq_owner.h"
#include "flowmq_peer_pool.h"
#include "flowmq_socket_external_internal.h"
#include "flowmq_cnet_transport.h"
#include "flowmq_protocol.h"
#include "tinytest.h"

#include <core/clock.h>
#include <core/error.h>
#include <string.h>

enum { PROGRESS_SOCKETS = 6u, PROGRESS_RAW_PEERS = 4u, PROGRESS_DEADLINE_MS = 3000u,
       HANDSHAKE_OBSERVE_MS = 7000u, READY_OBSERVE_MS = 5200u };

typedef struct progress_fixture_s {
  flowmq_ctx_t *ctx;
  flowmq_owner_t *owner;
  flowmq_socket_t *sockets[PROGRESS_SOCKETS];
  cnet_client raw;
  size_t raw_connected;
  size_t raw_closed;
  cnet_connection raw_hello_peers[PROGRESS_RAW_PEERS];
  size_t raw_hello_count;
} progress_fixture;

static flowmq_socket_t *progress_socket_create(progress_fixture *f, size_t slot,
                                                int pattern, bool pooled) {
  const flowmq_reconnect_config_t reconnect = FLOWMQ_RECONNECT_DISABLED;
  flowmq_socket_t *socket = slot == 0u && f->owner != NULL
      ? flowmq_owner_socket(f->owner, pattern) : flowmq_socket(f->ctx, pattern);
  f->sockets[slot] = socket;
  check_not_null(socket);
  check_equal(flowmq_setsockopt(socket, FLOWMQ_RECONNECT, &reconnect,
                               sizeof(reconnect)), SALTS_OK);
#if defined(FLOWMQ_TEST_PEER_POOL)
  const flowmq_peer_pool_config_t pool = FLOWMQ_PEER_POOL_CONFIG_INIT;
  if (pooled) check_equal(flowmq_socket_set_peer_pool(socket, &pool), SALTS_OK);
#else
  (void)pooled;
#endif
  return socket;
}

static flowmq_socket_t *progress_socket(progress_fixture *f, size_t slot,
                                         int pattern) {
  return progress_socket_create(f, slot, pattern, true);
}

static void progress_once(progress_fixture *f) {
  flowmq_pollitem_t items[PROGRESS_SOCKETS] = {0};
  size_t count = 0u, ready = 0u;
  if (f->owner != NULL) {
    items[0].socket = f->sockets[0];
    check_equal(flowmq_owner_poll(f->owner, items, 1u, 0u, &ready), SALTS_OK);
  }
  for (size_t i = f->owner != NULL ? 1u : 0u; i < PROGRESS_SOCKETS; ++i)
    if (f->sockets[i] != NULL) items[count++].socket = f->sockets[i];
  if (count != 0u)
    check_equal(flowmq_poll(items, count, 1u, &ready), SALTS_OK);
}

static void progress_bind(progress_fixture *f, const char *uri, char *endpoint,
                           size_t capacity) {
  size_t size = 0u;
  check_equal(f->owner == NULL ? flowmq_bind(f->sockets[0], uri)
      : flowmq_socket_internal_bind_external(f->sockets[0], uri), SALTS_OK);
  check_equal(flowmq_last_endpoint(f->sockets[0], endpoint, capacity, &size), SALTS_OK);
}

static void progress_receive(progress_fixture *f, flowmq_socket_t *socket,
                              const char *expected, int more) {
  const uint64_t deadline = monotonic_ms() + PROGRESS_DEADLINE_MS;
  char received[64];
  size_t size = 0u, option_size = sizeof(more);
  int status = SALTS_EBUSY, actual_more = -1;
  while (status == SALTS_EBUSY && monotonic_ms() < deadline) {
    progress_once(f);
    status = flowmq_recv(socket, received, sizeof(received), &size, FLOWMQ_DONTWAIT);
  }
  check_equal(status, SALTS_OK);
  check_equal(size, strlen(expected));
  check_equal(memcmp(received, expected, strlen(expected)), 0);
  check_equal(flowmq_getsockopt(socket, FLOWMQ_RCVMORE, &actual_more,
                               &option_size), SALTS_OK);
  check_equal(actual_more, more);
}

static void progress_exchange(progress_fixture *f, flowmq_socket_t *sender,
                               flowmq_socket_t *receiver, const char *payload) {
  const uint64_t deadline = monotonic_ms() + PROGRESS_DEADLINE_MS;
  int status = SALTS_EBUSY;
  while (status == SALTS_EBUSY && monotonic_ms() < deadline) {
    progress_once(f);
    status = flowmq_send(sender, payload, strlen(payload), FLOWMQ_DONTWAIT);
  }
  check_equal(status, SALTS_OK);
  progress_receive(f, receiver, payload, 0);
}

static void progress_raw_state(void *user, cnet_connection connection,
                                cnet_connection_state state,
                                const cnet_error *error) {
  progress_fixture *f = user;
  (void)error;
  if (state == CNET_CONNECTION_CONNECTED) {
    ++f->raw_connected;
    check_equal(cnet_receive(&f->raw, connection, 1u), SALTS_OK);
  } else if (state == CNET_CONNECTION_CLOSED || state == CNET_CONNECTION_FAILED) {
    ++f->raw_closed;
  }
}

static void progress_raw_receive(void *user, cnet_connection connection,
                                  const cnet_receive_view *view) {
  progress_fixture *f = user;
  (void)view; /* Drain server HELLO without answering it. */
  size_t i;
  for (i = 0u; i < f->raw_hello_count; ++i)
    if (f->raw_hello_peers[i].slot == connection.slot &&
        f->raw_hello_peers[i].generation == connection.generation) break;
  if (i == f->raw_hello_count) {
    check_true(f->raw_hello_count < PROGRESS_RAW_PEERS);
    f->raw_hello_peers[f->raw_hello_count++] = connection;
  }
  check_equal(cnet_receive(&f->raw, connection, 1u), SALTS_OK);
}

static void progress_raw_init(progress_fixture *f, size_t capacity) {
  flowmq_io_config_t io;
  flowmq_timeout_config_t timeouts = {.set_flags = FLOWMQ_TIMEOUT_SET_RECV};
  cnet_client_config config;
  flowmq_io_config_init(&io);
  io.command_capacity = 16u;
  flowmq_timeouts_resolve(&timeouts, 1000u);
  check_equal(flowmq_cnet_client_config(&io, &timeouts, FLOWMQ_TRANSPORT_TCP,
                                       capacity, 16384u, &config), SALTS_OK);
  check_equal(cnet_client_init(&f->raw, &config), SALTS_OK);
  f->raw_connected = f->raw_closed = f->raw_hello_count = 0u;
}

spec("flowmq runtime admission and session progress") {
  static progress_fixture f;

  before_each() {
    memset(&f, 0, sizeof(f));
    f.ctx = flowmq_ctx_new();
    check_not_null(f.ctx);
  }

  after_each() {
    if (f.raw.impl != NULL) {
      check_equal(cnet_client_stop(&f.raw, 1000u), SALTS_OK);
      check_equal(cnet_client_destroy(&f.raw), SALTS_OK);
    }
    for (size_t i = PROGRESS_SOCKETS; i > 0u; --i) {
      flowmq_socket_t *socket = f.sockets[i - 1u];
      if (socket != NULL)
        check_equal(i == 1u && f.owner != NULL
            ? flowmq_owner_close_socket(f.owner, socket) : flowmq_close(socket), SALTS_OK);
    }
    if (f.owner != NULL) check_equal(flowmq_owner_term(f.owner), SALTS_OK);
    if (f.ctx != NULL) check_equal(flowmq_ctx_term(f.ctx), SALTS_OK);
  }

  it("keeps ordinary peers progressing while a fifth connection waits in backlog") {
    char endpoint[128];
    progress_socket(&f, 0u, FLOWMQ_PULL);
    progress_bind(&f, "tcp://127.0.0.1:0", endpoint, sizeof(endpoint));
    for (size_t i = 1u; i <= 4u; ++i) {
      progress_socket(&f, i, FLOWMQ_PUSH);
      check_equal(flowmq_connect(f.sockets[i], endpoint), SALTS_OK);
      progress_exchange(&f, f.sockets[i], f.sockets[0], "warm");
    }
    progress_socket(&f, 5u, FLOWMQ_PUSH);
    check_equal(flowmq_connect(f.sockets[5], endpoint), SALTS_OK);
    for (size_t i = 0u; i < 64u; ++i) progress_once(&f);
    check_equal(flowmq_send(f.sockets[5], "wait", 4u, FLOWMQ_DONTWAIT), SALTS_EBUSY);
    progress_exchange(&f, f.sockets[4], f.sockets[0], "while-full");
    check_equal(flowmq_close(f.sockets[1]), SALTS_OK);
    f.sockets[1] = NULL;
    progress_exchange(&f, f.sockets[5], f.sockets[0], "new-peer");
    progress_exchange(&f, f.sockets[4], f.sockets[0], "still-live");
  }

  it("rejects XSUB DATA sends instead of waiting for an unsupported route") {
    flowmq_socket_t *xsub = progress_socket(&f, 0u, FLOWMQ_XSUB);
    const char command[] = {1, 't'};
    check_equal(flowmq_send(xsub, command, sizeof(command), FLOWMQ_DONTWAIT), SALTS_ENOTSUP);
    check_equal(flowmq_send(xsub, command, sizeof(command), 0), SALTS_ENOTSUP);
    check_equal(flowmq_send(xsub, "data", 4u, FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_ENOTSUP);
    check_equal(flowmq_setsockopt(xsub, FLOWMQ_SUBSCRIBE, "t", 1u), SALTS_OK);
  }

  it("disarms the handshake budget after READY without imposing an idle timeout") {
    char endpoint[128];
    progress_socket(&f, 0u, FLOWMQ_PAIR);
    progress_socket(&f, 1u, FLOWMQ_PAIR);
    progress_bind(&f, "tcp://127.0.0.1:0", endpoint, sizeof(endpoint));
    check_equal(flowmq_connect(f.sockets[1], endpoint), SALTS_OK);
    progress_exchange(&f, f.sockets[1], f.sockets[0], "ready");
    const uint64_t deadline = monotonic_ms() + READY_OBSERVE_MS;
    while (monotonic_ms() < deadline) progress_once(&f);
    progress_exchange(&f, f.sockets[1], f.sockets[0], "still-ready");
  }

  it("preserves a blocking ROUTER multipart transaction until HWM admission succeeds") {
    char endpoint[128];
    const int hwm = 1;
    progress_socket(&f, 0u, FLOWMQ_ROUTER);
    progress_socket(&f, 1u, FLOWMQ_DEALER);
    check_equal(flowmq_setsockopt(f.sockets[0], FLOWMQ_SNDHWM, &hwm, sizeof(hwm)), SALTS_OK);
    check_equal(flowmq_setsockopt(f.sockets[1], FLOWMQ_IDENTITY, "route", 5u), SALTS_OK);
    progress_bind(&f, "tcp://127.0.0.1:0", endpoint, sizeof(endpoint));
    check_equal(flowmq_connect(f.sockets[1], endpoint), SALTS_OK);
    const uint64_t deadline = monotonic_ms() + PROGRESS_DEADLINE_MS;
    int status = SALTS_EBUSY;
    while (status == SALTS_EBUSY && monotonic_ms() < deadline) {
      progress_once(&f);
      status = flowmq_send(f.sockets[1], "warm", 4u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    progress_receive(&f, f.sockets[0], "route", 1);
    progress_receive(&f, f.sockets[0], "warm", 0);
    check_equal(flowmq_send(f.sockets[0], "route", 5u, FLOWMQ_SNDMORE | FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_send(f.sockets[0], "queued", 6u, FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_send(f.sockets[0], "route", 5u, FLOWMQ_SNDMORE | FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_send(f.sockets[0], "first", 5u, FLOWMQ_SNDMORE | FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_send(f.sockets[0], "last", 4u, 0), SALTS_OK);
    progress_receive(&f, f.sockets[1], "queued", 0);
    progress_receive(&f, f.sockets[1], "first", 1);
    progress_receive(&f, f.sockets[1], "last", 0);
  }

  it("resumes XPUB events after its receive HWM on owned and copied transport paths") {
    const char *uris[] = {"tcp://127.0.0.1:0", "ws://127.0.0.1:0/events"};
    for (size_t transport = 0u; transport < 2u; ++transport) {
      char endpoint[128];
      unsigned char event[16];
      const int hwm = 1;
      /* Peer pools are a TCP/TLS capability; the copied WS path is exercised
       * without a pool in both builds. */
      progress_socket_create(&f, 0u, FLOWMQ_XPUB, transport == 0u);
      progress_socket_create(&f, 1u, FLOWMQ_XSUB, transport == 0u);
      check_equal(flowmq_setsockopt(f.sockets[0], FLOWMQ_RCVHWM, &hwm, sizeof(hwm)), SALTS_OK);
      check_equal(flowmq_setsockopt(f.sockets[1], FLOWMQ_SUBSCRIBE, "one", 3u), SALTS_OK);
      check_equal(flowmq_setsockopt(f.sockets[1], FLOWMQ_SUBSCRIBE, "two", 3u), SALTS_OK);
      progress_bind(&f, uris[transport], endpoint, sizeof(endpoint));
      check_equal(flowmq_connect(f.sockets[1], endpoint), SALTS_OK);
      size_t matches = 0u;
      const uint64_t deadline = monotonic_ms() + PROGRESS_DEADLINE_MS;
      while (matches == 0u && monotonic_ms() < deadline) {
        progress_once(&f);
        check_equal(flowmq_socket_internal_fanout_match_count(f.sockets[0], "two", 3u,
                                                            &matches), SALTS_OK);
      }
      check_equal(matches, 1u); /* Second event reached the full inbound queue. */
      for (size_t i = 0u; i < 2u; ++i) {
        size_t size = 0u;
        int status = SALTS_EBUSY;
        const uint64_t receive_deadline = monotonic_ms() + PROGRESS_DEADLINE_MS;
        while (status == SALTS_EBUSY && monotonic_ms() < receive_deadline) {
          progress_once(&f);
          status = flowmq_recv(f.sockets[0], event, sizeof(event), &size, FLOWMQ_DONTWAIT);
        }
        check_equal(status, SALTS_OK);
        check_equal(size, 4u);
        check_equal(event[0], 1u);
        check_equal(memcmp(event + 1u, i == 0u ? "one" : "two", 3u), 0);
      }
      check_equal(flowmq_setsockopt(f.sockets[1], FLOWMQ_UNSUBSCRIBE, "one", 3u), SALTS_OK);
      /* A subsequent command proves receive was rearmed; no duplicate replay. */
      const char unsubscribe[] = {0, 'o', 'n', 'e'};
      size_t size = 0u;
      int status = SALTS_EBUSY;
      const uint64_t receive_deadline = monotonic_ms() + PROGRESS_DEADLINE_MS;
      while (status == SALTS_EBUSY && monotonic_ms() < receive_deadline) {
        progress_once(&f);
        status = flowmq_recv(f.sockets[0], event, sizeof(event), &size, FLOWMQ_DONTWAIT);
      }
      check_equal(status, SALTS_OK);
      check_equal(size, sizeof(unsubscribe));
      check_equal(memcmp(event, unsubscribe, sizeof(unsubscribe)), 0);
      check_equal(flowmq_recv(f.sockets[0], event, sizeof(event), &size, FLOWMQ_DONTWAIT), SALTS_EBUSY);
      for (size_t i = 0u; i < 2u; ++i) {
        check_equal(flowmq_close(f.sockets[i]), SALTS_OK);
        f.sockets[i] = NULL;
      }
    }
  }

  it("consumes coalesced HELLO SETTINGS and multipart DATA in ordinary and owner domains") {
    for (size_t domain = 0u; domain < 2u; ++domain) {
      char endpoint[128];
      cnet_connection connection;
      tstr encoded[4] = {0};
      flowmq_protocol_frame_t frame = {.kind = FLOWMQ_PROTOCOL_FRAME_HELLO,
                                       .pattern = FLOWMQ_PROTOCOL_PUSH};
      const flowmq_protocol_settings_t settings = {
          .capabilities = FLOWMQ_PROTOCOL_CAP_FLOW_CREDIT,
          .max_frame_size = 1024u * 1024u, .session_generation = 1u,
          .initial_max_data = 1024u * 1024u, .flow_update_quantum = 1024u,
          .flow_update_interval_ms = 10u};
      unsigned char settings_payload[FLOWMQ_PROTOCOL_SETTINGS_PAYLOAD_SIZE];
      size_t total = 0u, offset = 0u, events = 0u;
      if (domain != 0u) {
        f.owner = flowmq_owner_new(f.ctx, NULL);
        check_not_null(f.owner);
      }
      progress_socket(&f, 0u, FLOWMQ_PULL);
      progress_bind(&f, "tcp://127.0.0.1:0", endpoint, sizeof(endpoint));
      progress_raw_init(&f, 1u);
      const cnet_connect_options options = {
          .uri = endpoint, .observer = {.on_state = progress_raw_state,
              .on_receive = progress_raw_receive, .user = &f}};
      check_equal(cnet_connect(&f.raw, &options, &connection), SALTS_OK);
      const uint64_t deadline = monotonic_ms() + PROGRESS_DEADLINE_MS;
      while (f.raw_hello_count == 0u && monotonic_ms() < deadline) {
        check_equal(cnet_client_poll(&f.raw, 1u, &events), SALTS_OK);
        progress_once(&f);
      }
      check_equal(f.raw_hello_count, 1u);
      check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded[0]), SALTS_OK);
      check_equal(flowmq_protocol_settings_encode(&settings, settings_payload), SALTS_OK);
      frame.kind = FLOWMQ_PROTOCOL_FRAME_SETTINGS;
      frame.payload = vstr_from_buf((const char *)settings_payload, sizeof(settings_payload));
      check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded[1]), SALTS_OK);
      frame.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
      frame.message_id = 1u;
      frame.more = 1;
      frame.payload = (vstr){0};
      check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded[2]), SALTS_OK);
      frame.message_id = 2u;
      frame.more = 0;
      frame.payload = vstr_from_cstr("coalesced");
      check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded[3]), SALTS_OK);
      for (size_t i = 0u; i < 4u; ++i) total += tstr_len(encoded[i]);
      mem_buffer_t *buffer = mem_get_buffer(mem_global(), total);
      check_not_null(buffer);
      for (size_t i = 0u; i < 4u; ++i) {
        memcpy(mem_buffer_data(buffer) + offset, encoded[i], tstr_len(encoded[i]));
        offset += tstr_len(encoded[i]);
        tstr_free(encoded[i]);
      }
      mem_set_used(buffer, total);
      check_equal(cnet_send_buffer(&f.raw, connection, buffer), SALTS_OK);
      mem_buffer_release(buffer);
      /* Finish the raw writer while the FlowMQ receiver remains idle. */
      for (size_t i = 0u; i < 16u; ++i)
        check_equal(cnet_client_poll(&f.raw, 1u, &events), SALTS_OK);
      progress_receive(&f, f.sockets[0], "", 1);
      progress_receive(&f, f.sockets[0], "coalesced", 0);
      check_equal(cnet_client_stop(&f.raw, 1000u), SALTS_OK);
      check_equal(cnet_client_destroy(&f.raw), SALTS_OK);
      check_equal(f.owner != NULL ? flowmq_owner_close_socket(f.owner, f.sockets[0])
                                 : flowmq_close(f.sockets[0]), SALTS_OK);
      f.sockets[0] = NULL;
      if (f.owner != NULL) {
        check_equal(flowmq_owner_term(f.owner), SALTS_OK);
        f.owner = NULL;
      }
    }
  }

  it("expires silent FMQ handshakes and restores admission in ordinary and owner domains") {
    for (size_t domain = 0u; domain < 2u; ++domain) {
      char endpoint[128];
      if (domain != 0u) {
        f.owner = flowmq_owner_new(f.ctx, NULL);
        check_not_null(f.owner);
      }
      progress_socket(&f, 0u, FLOWMQ_PULL);
      progress_bind(&f, "tcp://127.0.0.1:0", endpoint, sizeof(endpoint));
      progress_raw_init(&f, PROGRESS_RAW_PEERS);
      for (size_t i = 0u; i < PROGRESS_RAW_PEERS; ++i) {
        cnet_connection connection;
        const cnet_connect_options options = {
            .uri = endpoint, .observer = {.on_state = progress_raw_state,
                .on_receive = progress_raw_receive, .user = &f}};
        check_equal(cnet_connect(&f.raw, &options, &connection), SALTS_OK);
      }
      size_t events = 0u;
      bool wait_projected = domain == 0u;
      const uint64_t deadline = monotonic_ms() + HANDSHAKE_OBSERVE_MS;
      while (f.raw_closed != PROGRESS_RAW_PEERS && monotonic_ms() < deadline) {
        check_equal(cnet_client_poll(&f.raw, 1u, &events), SALTS_OK);
        progress_once(&f);
        if (!wait_projected && f.raw_hello_count == PROGRESS_RAW_PEERS) {
          uint32_t wait_ms = 0u;
          check_equal(flowmq_socket_internal_external_timeout(f.sockets[0],
                                                               6000u, &wait_ms), SALTS_OK);
          /* Wait until pending HELLO writes settle; then an idle CNet lane
           * must still wake for the FMQ handshake deadline. */
          if (wait_ms != 0u) {
            check_true(wait_ms <= 5000u);
            wait_projected = true;
          }
        }
      }
      check_equal(f.raw_connected, PROGRESS_RAW_PEERS);
      check_equal(f.raw_hello_count, PROGRESS_RAW_PEERS);
      check_equal(f.raw_closed, PROGRESS_RAW_PEERS);
      check_true(wait_projected);
      check_equal(cnet_client_stop(&f.raw, 1000u), SALTS_OK);
      check_equal(cnet_client_destroy(&f.raw), SALTS_OK);
      progress_socket(&f, 1u, FLOWMQ_PUSH);
      check_equal(flowmq_connect(f.sockets[1], endpoint), SALTS_OK);
      progress_exchange(&f, f.sockets[1], f.sockets[0], "after-timeout");
      check_equal(flowmq_close(f.sockets[1]), SALTS_OK);
      f.sockets[1] = NULL;
      check_equal(f.owner != NULL ? flowmq_owner_close_socket(f.owner, f.sockets[0])
                                 : flowmq_close(f.sockets[0]), SALTS_OK);
      f.sockets[0] = NULL;
      if (f.owner != NULL) {
        check_equal(flowmq_owner_term(f.owner), SALTS_OK);
        f.owner = NULL;
      }
    }
  }
}
