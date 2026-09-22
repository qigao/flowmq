#include "flowmq_socket.h"
#include "flowmq_tls_test_material.h"
#include "tinytest.h"
#include "salts_error.h"
#include <salts/clock.h>

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

enum {
  FLOWMQ_TEST_PROGRESS_LIMIT = 10000u,
  FLOWMQ_TEST_SEGMENTED_PAYLOAD_SIZE = 64u * 1024u + 37u,
  FLOWMQ_TEST_QUEUED_MESSAGES = 8u,
  FLOWMQ_TEST_HEARTBEAT_IVL_MS = 20,
  FLOWMQ_TEST_HEARTBEAT_TIMEOUT_MS = 200,
  FLOWMQ_TEST_HEARTBEAT_ALIVE_OBSERVE_MS = 300,
  FLOWMQ_TEST_HEARTBEAT_DEAD_OBSERVE_MS = 100
};

static int progress_pair(flowmq_socket_t *first, flowmq_socket_t *second) {
  flowmq_pollitem_t items[] = {{.socket = first}, {.socket = second}};
  size_t ready = 0u;
  return flowmq_poll(items, 2u, 1u, &ready);
}

static int progress_three(flowmq_socket_t *first, flowmq_socket_t *second,
                          flowmq_socket_t *third) {
  flowmq_pollitem_t items[] = {
      {.socket = first}, {.socket = second}, {.socket = third}};
  size_t ready = 0u;
  return flowmq_poll(items, 3u, 1u, &ready);
}

spec("flowmq_socket lifecycle and pattern surface") {
  it("creates every classic ZeroMQ socket type and keeps context ownership explicit") {
    static const int types[] = {
        FLOWMQ_PAIR, FLOWMQ_PUB,    FLOWMQ_SUB,    FLOWMQ_REQ,
        FLOWMQ_REP,  FLOWMQ_DEALER, FLOWMQ_ROUTER, FLOWMQ_PULL,
        FLOWMQ_PUSH, FLOWMQ_XPUB,   FLOWMQ_XSUB};
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *sockets[sizeof(types) / sizeof(types[0])] = {0};

    check_not_null(ctx);
    for (size_t i = 0u; i < sizeof(types) / sizeof(types[0]); ++i) {
      sockets[i] = flowmq_socket(ctx, types[i]);
      check_not_null(sockets[i]);
    }
    check_equal(flowmq_ctx_term(ctx), SALTS_EBUSY);
    for (size_t i = 0u; i < sizeof(types) / sizeof(types[0]); ++i)
      check_equal(flowmq_close(sockets[i]), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("rejects unsupported directions before considering peer readiness") {
    char byte = 0;
    size_t received = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *pub = flowmq_socket(ctx, FLOWMQ_PUB);
    flowmq_socket_t *sub = flowmq_socket(ctx, FLOWMQ_SUB);
    flowmq_socket_t *push = flowmq_socket(ctx, FLOWMQ_PUSH);
    flowmq_socket_t *pull = flowmq_socket(ctx, FLOWMQ_PULL);

    check_equal(flowmq_recv(pub, &byte, sizeof(byte), &received, FLOWMQ_DONTWAIT),
                SALTS_ENOTSUP);
    check_equal(flowmq_send(sub, &byte, sizeof(byte), FLOWMQ_DONTWAIT), SALTS_ENOTSUP);
    check_equal(flowmq_recv(push, &byte, sizeof(byte), &received, FLOWMQ_DONTWAIT),
                SALTS_ENOTSUP);
    check_equal(flowmq_send(pull, &byte, sizeof(byte), FLOWMQ_DONTWAIT), SALTS_ENOTSUP);

    check_equal(flowmq_close(pub), SALTS_OK);
    check_equal(flowmq_close(sub), SALTS_OK);
    check_equal(flowmq_close(push), SALTS_OK);
    check_equal(flowmq_close(pull), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("reports poll readiness only when the pattern FSM can perform the operation") {
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *sub = flowmq_socket(ctx, FLOWMQ_SUB);
    flowmq_socket_t *pull = flowmq_socket(ctx, FLOWMQ_PULL);
    flowmq_socket_t *rep = flowmq_socket(ctx, FLOWMQ_REP);
    flowmq_pollitem_t items[] = {
        {.socket = sub, .events = FLOWMQ_POLLOUT},
        {.socket = pull, .events = FLOWMQ_POLLOUT},
        {.socket = rep, .events = FLOWMQ_POLLOUT}};
    size_t ready = 0u;

    check_equal(flowmq_poll(items, 3u, 0u, &ready), SALTS_OK);
    check_equal(ready, 0u);
    check_equal(items[0].revents, 0);
    check_equal(items[1].revents, 0);
    check_equal(items[2].revents, 0);

    check_equal(flowmq_close(rep), SALTS_OK);
    check_equal(flowmq_close(pull), SALTS_OK);
    check_equal(flowmq_close(sub), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("does not commit a request when no peer accepted it") {
    const char payload[] = "request";
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *req = flowmq_socket(ctx, FLOWMQ_REQ);

    check_equal(flowmq_send(req, payload, sizeof(payload) - 1u, FLOWMQ_DONTWAIT),
                SALTS_EBUSY);
    check_equal(flowmq_send(req, payload, sizeof(payload) - 1u, FLOWMQ_DONTWAIT),
                SALTS_EBUSY);

    check_equal(flowmq_close(req), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("allows a PAIR socket to own only one peer") {
    char first_endpoint[128] = {0};
    char second_endpoint[128] = {0};
    size_t endpoint_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *second = flowmq_socket(ctx, FLOWMQ_PAIR);

    check_equal(flowmq_bind(first, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(first, first_endpoint,
                                     sizeof(first_endpoint), &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_bind(second, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(second, second_endpoint,
                                     sizeof(second_endpoint), &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(sender, first_endpoint), SALTS_OK);
    check_equal(flowmq_connect(sender, second_endpoint), SALTS_EBUSY);

    check_equal(flowmq_close(second), SALTS_OK);
    check_equal(flowmq_close(first), SALTS_OK);
    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("preserves a multi-packet payload after send returns") {
    static unsigned char payload[FLOWMQ_TEST_SEGMENTED_PAYLOAD_SIZE];
    static unsigned char expected[FLOWMQ_TEST_SEGMENTED_PAYLOAD_SIZE];
    static unsigned char received[FLOWMQ_TEST_SEGMENTED_PAYLOAD_SIZE];
    char endpoint[128] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;

    for (size_t i = 0u; i < sizeof(payload); ++i)
      payload[i] = (unsigned char)((i * 31u + 7u) & 0xffu);
    memcpy(expected, payload, sizeof(payload));
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, payload, sizeof(payload), FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    memset(payload, 0xa5, sizeof(payload));

    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_recv(receiver, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(expected));
    check_equal(memcmp(received, expected, sizeof(expected)), 0);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("preserves queued frame boundaries and order") {
    char endpoint[128] = {0};
    unsigned char payloads[FLOWMQ_TEST_QUEUED_MESSAGES][16] = {0};
    unsigned char received[16] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;

    for (size_t message = 0u; message < FLOWMQ_TEST_QUEUED_MESSAGES;
         ++message) {
      memset(payloads[message], (int)(message + 1u),
             sizeof(payloads[message]));
    }
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, payloads[0], sizeof(payloads[0]),
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    for (size_t message = 1u; message < FLOWMQ_TEST_QUEUED_MESSAGES;
         ++message) {
      check_equal(flowmq_send(sender, payloads[message],
                              sizeof(payloads[message]), FLOWMQ_DONTWAIT),
                  SALTS_OK);
    }

    for (size_t message = 0u; message < FLOWMQ_TEST_QUEUED_MESSAGES;
         ++message) {
      status = SALTS_EBUSY;
      for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                          status == SALTS_EBUSY;
           ++i) {
        check_equal(progress_pair(sender, receiver), SALTS_OK);
        status = flowmq_recv(receiver, received, sizeof(received),
                             &received_size, FLOWMQ_DONTWAIT);
      }
      check_equal(status, SALTS_OK);
      check_equal(received_size, sizeof(payloads[message]));
      check_equal(memcmp(received, payloads[message], received_size), 0);
    }

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("isolates an incompatible peer without poisoning a ROUTER socket") {
    static const char identity[] = "healthy";
    static const char payload[] = "still-works";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *router = flowmq_socket(ctx, FLOWMQ_ROUTER);
    flowmq_socket_t *dealer = flowmq_socket(ctx, FLOWMQ_DEALER);
    flowmq_socket_t *incompatible = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(dealer, FLOWMQ_IDENTITY, identity,
                                  sizeof(identity) - 1u), SALTS_OK);
    check_equal(flowmq_bind(router, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(dealer, endpoint), SALTS_OK);
    for (size_t i = 0u; i < 100u; ++i)
      check_equal(progress_pair(dealer, router), SALTS_OK);
    check_equal(flowmq_connect(incompatible, endpoint), SALTS_OK);
    for (size_t i = 0u; i < 100u; ++i)
      check_equal(progress_three(incompatible, dealer, router), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_three(incompatible, dealer, router), SALTS_OK);
      status = flowmq_send(dealer, payload, sizeof(payload) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_three(incompatible, dealer, router), SALTS_OK);
      status = flowmq_recv(router, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(identity) - 1u);
    check_equal(flowmq_recv(router, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(received_size, sizeof(payload) - 1u);
    check_equal(memcmp(received, payload, received_size), 0);

    check_equal(flowmq_close(incompatible), SALTS_OK);
    check_equal(flowmq_close(dealer), SALTS_OK);
    check_equal(flowmq_close(router), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("does not route a REP reply to a replacement peer in a reused slot") {
    static const char request[] = "request";
    static const char replacement_request[] = "replacement";
    static const char reply[] = "reply";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *server = flowmq_socket(ctx, FLOWMQ_REP);
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_REQ);
    flowmq_socket_t *replacement = flowmq_socket(ctx, FLOWMQ_DEALER);
    int status = SALTS_EBUSY;

    check_equal(flowmq_bind(server, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(server, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(first, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(first, server), SALTS_OK);
      status = flowmq_send(first, request, sizeof(request) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(first, server), SALTS_OK);
      status = flowmq_recv(server, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_close(first), SALTS_OK);
    for (size_t i = 0u; i < 200u; ++i) {
      flowmq_pollitem_t item = {.socket = server};
      size_t ready = 0u;
      check_equal(flowmq_poll(&item, 1u, 0u, &ready), SALTS_OK);
    }
    {
      flowmq_pollitem_t cancelled = {
          .socket = server, .events = FLOWMQ_POLLOUT};
      size_t ready = 0u;
      check_equal(flowmq_poll(&cancelled, 1u, 0u, &ready), SALTS_OK);
      check_equal(ready, 1u);
      check_equal(cancelled.revents, FLOWMQ_POLLERR);
    }

    check_equal(flowmq_connect(replacement, endpoint), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(replacement, server), SALTS_OK);
      status = flowmq_send(replacement, replacement_request,
                           sizeof(replacement_request) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(server, reply, sizeof(reply) - 1u, 0),
                SALTS_ENOTCONN);
    {
      flowmq_pollitem_t consumed = {.socket = server};
      size_t ready = 0u;
      check_equal(flowmq_poll(&consumed, 1u, 0u, &ready), SALTS_OK);
      check_equal(ready, 0u);
      check_equal(consumed.revents, 0);
    }
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(replacement, server), SALTS_OK);
      status = flowmq_recv(server, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(replacement_request) - 1u);
    check_equal(flowmq_send(server, reply, sizeof(reply) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(replacement, server), SALTS_OK);
      status = flowmq_recv(replacement, received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(reply) - 1u);

    check_equal(flowmq_close(replacement), SALTS_OK);
    check_equal(flowmq_close(server), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("cancels a multipart REP route before a peer slot is reused") {
    static const char request[] = "request";
    static const char old_first[] = "old-first";
    static const char old_final[] = "old-final";
    static const char replacement_request[] = "replacement";
    static const char replacement_reply[] = "new-reply";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *server = flowmq_socket(ctx, FLOWMQ_REP);
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_REQ);
    flowmq_socket_t *replacement = flowmq_socket(ctx, FLOWMQ_DEALER);
    int status = SALTS_EBUSY;

    check_equal(flowmq_bind(server, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(server, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(first, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(first, server), SALTS_OK);
      status = flowmq_send(first, request, sizeof(request) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(first, server), SALTS_OK);
      status = flowmq_recv(server, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(server, old_first, sizeof(old_first) - 1u,
                            FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
    check_equal(flowmq_close(first), SALTS_OK);
    for (size_t i = 0u; i < 200u; ++i) {
      flowmq_pollitem_t item = {.socket = server};
      size_t ready = 0u;
      check_equal(flowmq_poll(&item, 1u, 0u, &ready), SALTS_OK);
    }

    check_equal(flowmq_connect(replacement, endpoint), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(replacement, server), SALTS_OK);
      status = flowmq_send(replacement, replacement_request,
                           sizeof(replacement_request) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(server, old_final, sizeof(old_final) - 1u, 0),
                SALTS_ENOTCONN);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(replacement, server), SALTS_OK);
      status = flowmq_recv(server, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(replacement_request) - 1u);
    check_equal(flowmq_send(server, replacement_reply,
                            sizeof(replacement_reply) - 1u, FLOWMQ_DONTWAIT),
                SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(replacement, server), SALTS_OK);
      status = flowmq_recv(replacement, received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(replacement_reply) - 1u);

    check_equal(flowmq_close(replacement), SALTS_OK);
    check_equal(flowmq_close(server), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("resets a REQ transaction after its bound peer disconnects") {
    static const char first_request[] = "first";
    static const char next_request[] = "next";
    static const char reply[] = "reply";
    char first_endpoint[128] = {0};
    char next_endpoint[128] = {0};
    char received[16] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *client = flowmq_socket(ctx, FLOWMQ_REQ);
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_REP);
    flowmq_socket_t *next = flowmq_socket(ctx, FLOWMQ_REP);
    int status = SALTS_EBUSY;

    check_equal(flowmq_bind(first, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(first, first_endpoint,
                                     sizeof(first_endpoint), &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(client, first_endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(client, first), SALTS_OK);
      status = flowmq_send(client, first_request, sizeof(first_request) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_close(first), SALTS_OK);
    for (size_t i = 0u; i < 200u; ++i) {
      flowmq_pollitem_t item = {.socket = client};
      size_t ready = 0u;
      check_equal(flowmq_poll(&item, 1u, 0u, &ready), SALTS_OK);
    }
    {
      flowmq_pollitem_t cancelled = {
          .socket = client, .events = FLOWMQ_POLLIN};
      size_t ready = 0u;
      check_equal(flowmq_poll(&cancelled, 1u, 0u, &ready), SALTS_OK);
      check_equal(ready, 1u);
      check_equal(cancelled.revents, FLOWMQ_POLLERR);
    }
    check_equal(flowmq_recv(client, received, sizeof(received), &received_size,
                            0), SALTS_ENOTCONN);

    check_equal(flowmq_bind(next, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(next, next_endpoint, sizeof(next_endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(client, next_endpoint), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(client, next), SALTS_OK);
      status = flowmq_send(client, next_request, sizeof(next_request) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(client, next), SALTS_OK);
      status = flowmq_recv(next, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(next_request) - 1u);
    check_equal(flowmq_send(next, reply, sizeof(reply) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(client, next), SALTS_OK);
      status = flowmq_recv(client, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(reply) - 1u);

    check_equal(flowmq_close(next), SALTS_OK);
    check_equal(flowmq_close(client), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("waits for the requested timeout when multiple sockets stay idle") {
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *second = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_pollitem_t items[] = {{.socket = first, .events = FLOWMQ_POLLIN},
                                 {.socket = second, .events = FLOWMQ_POLLIN}};
    size_t ready = 0u;
    uint64_t started_ms = salts_monotonic_ms();

    check_equal(flowmq_poll(items, 2u, 20u, &ready), SALTS_OK);
    check_equal(ready, 0u);
    check_greater_equal(salts_monotonic_ms() - started_ms, UINT64_C(15));

    check_equal(flowmq_close(first), SALTS_OK);
    check_equal(flowmq_close(second), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("keeps an idle connection open without a receive deadline") {
    static const char payload[] = "still-open";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t ready = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_pollitem_t idle_items[] = {
        {.socket = sender}, {.socket = receiver}};
    int status = SALTS_EBUSY;

    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, payload, sizeof(payload) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_recv(receiver, received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);

    check_equal(flowmq_poll(idle_items, 2u, 1100u, &ready), SALTS_OK);
    check_equal(ready, 0u);
    check_equal(flowmq_send(sender, payload, sizeof(payload) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("validates caller-driven control options before runtime startup") {
    int interval_ms = FLOWMQ_TEST_HEARTBEAT_IVL_MS;
    int timeout_ms = FLOWMQ_TEST_HEARTBEAT_TIMEOUT_MS;
    int negative_ms = -1;
    int zero_ms = 0;
    size_t flow_quantum = 64u * 1024u;
    size_t zero_quantum = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *socket = flowmq_socket(ctx, FLOWMQ_PAIR);

    check_equal(flowmq_setsockopt(socket, FLOWMQ_HEARTBEAT_IVL,
                                 &interval_ms, sizeof(interval_ms)),
                SALTS_OK);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_HEARTBEAT_TIMEOUT,
                                 &timeout_ms, sizeof(timeout_ms)),
                SALTS_OK);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_HEARTBEAT_IVL,
                                 &negative_ms, sizeof(negative_ms)),
                SALTS_EINVAL);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_HEARTBEAT_TIMEOUT,
                                 &negative_ms, sizeof(negative_ms)),
                SALTS_EINVAL);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_HEARTBEAT_IVL,
                                 NULL, sizeof(interval_ms)),
                SALTS_EINVAL);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_FLOW_UPDATE_IVL,
                                 &interval_ms, sizeof(interval_ms)), SALTS_OK);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_FLOW_UPDATE_IVL,
                                 &zero_ms, sizeof(zero_ms)), SALTS_EINVAL);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_FLOW_UPDATE_QUANTUM,
                                 &flow_quantum, sizeof(flow_quantum)), SALTS_OK);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_FLOW_UPDATE_QUANTUM,
                                 &zero_quantum, sizeof(zero_quantum)),
                SALTS_EINVAL);
    check_equal(flowmq_bind(socket, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_HEARTBEAT_IVL,
                                 &interval_ms, sizeof(interval_ms)),
                 SALTS_EBUSY);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_FLOW_UPDATE_IVL,
                                 &interval_ms, sizeof(interval_ms)), SALTS_EBUSY);

    check_equal(flowmq_close(socket), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("exposes ZeroMQ-compatible reconnect policy defaults and validation") {
    int value = 0;
    int disabled = -1;
    int invalid = -2;
    int interval_ms = 25;
    int maximum_ms = 100;
    size_t value_size = sizeof(value);
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *socket = flowmq_socket(ctx, FLOWMQ_PAIR);

    check_equal(flowmq_getsockopt(socket, FLOWMQ_RECONNECT_IVL, &value,
                                 &value_size), SALTS_OK);
    check_equal(value, 100);
    value_size = sizeof(value);
    check_equal(flowmq_getsockopt(socket, FLOWMQ_RECONNECT_IVL_MAX, &value,
                                 &value_size), SALTS_OK);
    check_equal(value, 0);

    check_equal(flowmq_setsockopt(socket, FLOWMQ_RECONNECT_IVL, &interval_ms,
                                 sizeof(interval_ms)), SALTS_OK);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_RECONNECT_IVL_MAX, &maximum_ms,
                                 sizeof(maximum_ms)), SALTS_OK);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_RECONNECT_IVL, &disabled,
                                 sizeof(disabled)), SALTS_OK);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_RECONNECT_IVL, &invalid,
                                 sizeof(invalid)), SALTS_EINVAL);
    check_equal(flowmq_setsockopt(socket, FLOWMQ_RECONNECT_IVL_MAX, &disabled,
                                 sizeof(disabled)), SALTS_EINVAL);

    value_size = sizeof(value);
    check_equal(flowmq_getsockopt(socket, FLOWMQ_RECONNECT_IVL, &value,
                                 &value_size), SALTS_OK);
    check_equal(value, -1);

    check_equal(flowmq_close(socket), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("reconnects a caller-driven TCP endpoint after the listener restarts") {
    static const char before[] = "before-restart";
    static const char after[] = "after-restart";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    int reconnect_ms = 1;
    int sent_before = 0;
    int received_before = 0;
    int received_after = 0;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *server = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *client = flowmq_socket(ctx, FLOWMQ_PAIR);

    check_equal(flowmq_setsockopt(client, FLOWMQ_RECONNECT_IVL,
                                 &reconnect_ms, sizeof(reconnect_ms)),
                SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_RECONNECT_IVL_MAX,
                                 &reconnect_ms, sizeof(reconnect_ms)),
                SALTS_OK);
    check_equal(flowmq_bind(server, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(server, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(client, endpoint), SALTS_OK);

    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && !received_before;
         ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      if (!sent_before &&
          flowmq_send(client, before, sizeof(before) - 1u,
                      FLOWMQ_DONTWAIT) == SALTS_OK)
        sent_before = 1;
      if (sent_before &&
          flowmq_recv(server, received, sizeof(received), &received_size,
                      FLOWMQ_DONTWAIT) == SALTS_OK)
        received_before = 1;
    }
    check_true(received_before);
    check_equal(received_size, sizeof(before) - 1u);
    check_equal(memcmp(received, before, sizeof(before) - 1u), 0);
    check_equal(flowmq_close(server), SALTS_OK);

    server = flowmq_socket(ctx, FLOWMQ_PAIR);
    check_not_null(server);
    check_equal(flowmq_bind(server, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && !received_after;
         ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      (void)flowmq_send(client, after, sizeof(after) - 1u, FLOWMQ_DONTWAIT);
      if (flowmq_recv(server, received, sizeof(received), &received_size,
                      FLOWMQ_DONTWAIT) == SALTS_OK)
        received_after = 1;
    }
    check_true(received_after);
    check_equal(received_size, sizeof(after) - 1u);
    check_equal(memcmp(received, after, sizeof(after) - 1u), 0);

    check_equal(flowmq_close(client), SALTS_OK);
    check_equal(flowmq_close(server), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("does not reconnect an endpoint when its interval is disabled") {
    static const char payload[] = "disabled-reconnect";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t ready = 0u;
    int disabled = -1;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *server = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *client = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_pollitem_t items[] = {{.socket = client}, {.socket = server}};

    check_equal(flowmq_setsockopt(client, FLOWMQ_RECONNECT_IVL, &disabled,
                                 sizeof(disabled)), SALTS_OK);
    check_equal(flowmq_bind(server, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(server, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(client, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT; ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      if (flowmq_send(client, payload, sizeof(payload) - 1u,
                      FLOWMQ_DONTWAIT) == SALTS_OK)
        break;
    }
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT; ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      if (flowmq_recv(server, received, sizeof(received), &received_size,
                      FLOWMQ_DONTWAIT) == SALTS_OK)
        break;
    }
    check_equal(received_size, sizeof(payload) - 1u);
    check_equal(flowmq_close(server), SALTS_OK);

    server = flowmq_socket(ctx, FLOWMQ_PAIR);
    check_not_null(server);
    check_equal(flowmq_bind(server, endpoint), SALTS_OK);
    items[1].socket = server;
    check_equal(flowmq_poll(items, 2u, 50u, &ready), SALTS_OK);
    check_equal(flowmq_send(client, payload, sizeof(payload) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_EBUSY);
    check_equal(flowmq_poll(items, 2u, 20u, &ready), SALTS_OK);
    check_equal(flowmq_recv(server, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_EBUSY);

    check_equal(flowmq_close(client), SALTS_OK);
    check_equal(flowmq_close(server), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("keeps a heartbeat-enabled connection open while both peers progress") {
    static const char payload[] = "heartbeat-alive";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t ready = 0u;
    int interval_ms = FLOWMQ_TEST_HEARTBEAT_IVL_MS;
    int timeout_ms = FLOWMQ_TEST_HEARTBEAT_TIMEOUT_MS;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_pollitem_t connected_items[] = {
        {.socket = sender, .events = FLOWMQ_POLLOUT}, {.socket = receiver}};
    flowmq_pollitem_t idle_items[] = {{.socket = sender}, {.socket = receiver}};
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(sender, FLOWMQ_HEARTBEAT_IVL,
                                 &interval_ms, sizeof(interval_ms)),
                SALTS_OK);
    check_equal(flowmq_setsockopt(sender, FLOWMQ_HEARTBEAT_TIMEOUT,
                                 &timeout_ms, sizeof(timeout_ms)),
                SALTS_OK);
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    check_equal(flowmq_poll(connected_items, 2u, 100u, &ready), SALTS_OK);
    check_equal(ready, 1u);
    check_equal(flowmq_poll(idle_items, 2u,
                            FLOWMQ_TEST_HEARTBEAT_ALIVE_OBSERVE_MS, &ready),
                SALTS_OK);
    check_equal(ready, 0u);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, payload, sizeof(payload) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_recv(receiver, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(payload) - 1u);
    check_equal(memcmp(received, payload, received_size), 0);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("uses the heartbeat interval as the default timeout") {
    static const char payload[] = "heartbeat-timeout";
    char endpoint[128] = {0};
    size_t endpoint_size = 0u;
    size_t ready = 0u;
    int interval_ms = FLOWMQ_TEST_HEARTBEAT_IVL_MS;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_pollitem_t connected_items[] = {
        {.socket = sender, .events = FLOWMQ_POLLOUT}, {.socket = receiver}};
    flowmq_pollitem_t sender_item = {.socket = sender};
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(sender, FLOWMQ_HEARTBEAT_IVL,
                                 &interval_ms, sizeof(interval_ms)),
                SALTS_OK);
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    check_equal(flowmq_poll(connected_items, 2u, 100u, &ready), SALTS_OK);
    check_equal(ready, 1u);

    check_equal(flowmq_poll(&sender_item, 1u,
                            FLOWMQ_TEST_HEARTBEAT_DEAD_OBSERVE_MS, &ready),
                SALTS_OK);
    check_equal(ready, 0u);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(flowmq_poll(&sender_item, 1u, 0u, &ready), SALTS_OK);
      status = flowmq_send(sender, payload, sizeof(payload) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_EBUSY);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("keeps progressing multiple sockets until one requested event is ready") {
    char endpoint[128] = {0};
    size_t endpoint_size = 0u;
    size_t ready = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_pollitem_t items[] = {
        {.socket = sender, .events = FLOWMQ_POLLOUT}, {.socket = receiver}};

    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    check_equal(flowmq_poll(items, 2u, 100u, &ready), SALTS_OK);
    check_equal(ready, 1u);
    check_equal(items[0].revents, FLOWMQ_POLLOUT);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("exchanges owned messages over caller-driven loopback TCP") {
    static const char request[] = "direct-cnet-request";
    static const char reply[] = "direct-cnet-reply";
    char endpoint[128] = {0};
    char received[64] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *server = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *client = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;

    check_equal(flowmq_bind(server, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(server, endpoint, sizeof(endpoint), &endpoint_size),
                SALTS_OK);
    check_true(endpoint_size != 0u);
    check_equal(flowmq_connect(client, endpoint), SALTS_OK);

    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      status = flowmq_send(client, request, sizeof(request) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);

    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      status = flowmq_recv(server, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(request) - 1u);
    check_equal(memcmp(received, request, received_size), 0);

    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      status = flowmq_send(server, reply, sizeof(reply) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);

    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      status = flowmq_recv(client, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(reply) - 1u);
    check_equal(memcmp(received, reply, received_size), 0);

    check_equal(flowmq_close(client), SALTS_OK);
    check_equal(flowmq_close(server), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("uses verified TLS for initial and reconnected sessions") {
    static const char payload[] = "verified-tls";
    static const char reconnected_payload[] = "verified-tls-reconnected";
    char endpoint[128] = {0};
    char received[64] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    char *cert_path = tt_make_temp_file("flowmq-socket-cert", ".pem");
    char *key_path = tt_make_temp_file("flowmq-socket-key", ".pem");
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *server = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *client = flowmq_socket(ctx, FLOWMQ_PAIR);
    int reconnect_ms = 1;
    int status = SALTS_EBUSY;

    check_not_null(cert_path);
    check_not_null(key_path);
    check_equal(tt_write_file(cert_path, FLOWMQ_TLS_TEST_CERTIFICATE,
                              sizeof(FLOWMQ_TLS_TEST_CERTIFICATE) - 1u), 0);
    check_equal(tt_write_file(key_path, FLOWMQ_TLS_TEST_KEY,
                              sizeof(FLOWMQ_TLS_TEST_KEY) - 1u), 0);
    check_equal(flowmq_setsockopt(server, FLOWMQ_TLS_CERT_FILE, cert_path,
                                 strlen(cert_path)), SALTS_OK);
    check_equal(flowmq_setsockopt(server, FLOWMQ_TLS_KEY_FILE, key_path,
                                 strlen(key_path)), SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_CA_FILE, cert_path,
                                 strlen(cert_path)), SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_TLS_SERVER_NAME, "localhost",
                                 strlen("localhost")), SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_RECONNECT_IVL,
                                 &reconnect_ms, sizeof(reconnect_ms)),
                SALTS_OK);
    check_equal(flowmq_setsockopt(client, FLOWMQ_RECONNECT_IVL_MAX,
                                 &reconnect_ms, sizeof(reconnect_ms)),
                SALTS_OK);
    check_equal(flowmq_bind(server, "tls://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(server, endpoint, sizeof(endpoint), &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(client, endpoint), SALTS_OK);

    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      status = flowmq_send(client, payload, sizeof(payload) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      status = flowmq_recv(server, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(payload) - 1u);
    check_equal(memcmp(received, payload, received_size), 0);

    check_equal(flowmq_close(server), SALTS_OK);
    server = flowmq_socket(ctx, FLOWMQ_PAIR);
    check_not_null(server);
    check_equal(flowmq_setsockopt(server, FLOWMQ_TLS_CERT_FILE, cert_path,
                                 strlen(cert_path)), SALTS_OK);
    check_equal(flowmq_setsockopt(server, FLOWMQ_TLS_KEY_FILE, key_path,
                                 strlen(key_path)), SALTS_OK);
    check_equal(flowmq_bind(server, endpoint), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status != SALTS_OK;
         ++i) {
      check_equal(progress_pair(client, server), SALTS_OK);
      (void)flowmq_send(client, reconnected_payload,
                        sizeof(reconnected_payload) - 1u, FLOWMQ_DONTWAIT);
      status = flowmq_recv(server, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(reconnected_payload) - 1u);
    check_equal(memcmp(received, reconnected_payload, received_size), 0);

    check_equal(flowmq_close(client), SALTS_OK);
    check_equal(flowmq_close(server), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
    check_equal(tt_remove_file(cert_path), 0);
    check_equal(tt_remove_file(key_path), 0);
    free(cert_path);
    free(key_path);
  }

  it("leaves TLS options mutable after bind rejects missing server credentials") {
    static const char path[] = "missing.pem";
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *server = flowmq_socket(ctx, FLOWMQ_PAIR);

    check_equal(flowmq_bind(server, "tls://127.0.0.1:0"), SALTS_EINVAL);
    check_equal(flowmq_setsockopt(server, FLOWMQ_TLS_CERT_FILE, path,
                                 sizeof(path) - 1u), SALTS_OK);
    check_equal(flowmq_setsockopt(server, FLOWMQ_TLS_KEY_FILE, path,
                                 sizeof(path) - 1u), SALTS_OK);

    check_equal(flowmq_close(server), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("uses a routing-id envelope for ROUTER and DEALER") {
    static const char identity[] = "dealer-a";
    static const char request[] = "routed-request";
    static const char reply[] = "routed-reply";
    char endpoint[128] = {0};
    char received[64] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *router = flowmq_socket(ctx, FLOWMQ_ROUTER);
    flowmq_socket_t *dealer = flowmq_socket(ctx, FLOWMQ_DEALER);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(dealer, FLOWMQ_IDENTITY, identity,
                                 sizeof(identity) - 1u), SALTS_OK);
    check_equal(flowmq_bind(router, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint), &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(dealer, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(dealer, router), SALTS_OK);
      status = flowmq_send(dealer, request, sizeof(request) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);

    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(dealer, router), SALTS_OK);
      status = flowmq_recv(router, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(identity) - 1u);
    check_equal(memcmp(received, identity, received_size), 0);
    check_equal(flowmq_recv(router, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(received_size, sizeof(request) - 1u);
    check_equal(memcmp(received, request, received_size), 0);

    check_equal(flowmq_send(router, identity, sizeof(identity) - 1u,
                            FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(dealer, router), SALTS_OK);
      status = flowmq_send(router, reply, sizeof(reply) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(dealer, router), SALTS_OK);
      status = flowmq_recv(dealer, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(reply) - 1u);
    check_equal(memcmp(received, reply, received_size), 0);

    check_equal(flowmq_close(dealer), SALTS_OK);
    check_equal(flowmq_close(router), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("rejects a duplicate ROUTER peer identity without replacing the first peer") {
    static const char identity[] = "duplicate";
    static const char first_payload[] = "first-peer";
    static const char duplicate_payload[] = "duplicate-peer";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *router = flowmq_socket(ctx, FLOWMQ_ROUTER);
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_DEALER);
    flowmq_socket_t *duplicate = flowmq_socket(ctx, FLOWMQ_DEALER);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(first, FLOWMQ_IDENTITY, identity,
                                  sizeof(identity) - 1u), SALTS_OK);
    check_equal(flowmq_setsockopt(duplicate, FLOWMQ_IDENTITY, identity,
                                  sizeof(identity) - 1u), SALTS_OK);
    check_equal(flowmq_bind(router, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(first, endpoint), SALTS_OK);
    for (size_t i = 0u; i < 100u; ++i)
      check_equal(progress_pair(first, router), SALTS_OK);
    check_equal(flowmq_connect(duplicate, endpoint), SALTS_OK);
    for (size_t i = 0u; i < 200u; ++i)
      check_equal(progress_three(first, duplicate, router), SALTS_OK);
    {
      int duplicate_status =
          flowmq_send(duplicate, duplicate_payload,
                      sizeof(duplicate_payload) - 1u, FLOWMQ_DONTWAIT);
      check_equal(duplicate_status == SALTS_OK ||
                      duplicate_status == SALTS_EBUSY,
                  1);
    }
    for (size_t i = 0u; i < 50u; ++i)
      check_equal(progress_three(first, duplicate, router), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_three(first, duplicate, router), SALTS_OK);
      status = flowmq_send(first, first_payload, sizeof(first_payload) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_three(first, duplicate, router), SALTS_OK);
      status = flowmq_recv(router, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(identity) - 1u);
    check_equal(flowmq_recv(router, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(received_size, sizeof(first_payload) - 1u);
    check_equal(memcmp(received, first_payload, received_size), 0);

    check_equal(flowmq_close(duplicate), SALTS_OK);
    check_equal(flowmq_close(first), SALTS_OK);
    check_equal(flowmq_close(router), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("cancels a ROUTER multipart route when its peer session disconnects") {
    static const char identity[] = "route";
    static const char first_request[] = "first";
    static const char next_request[] = "next";
    static const char old_reply[] = "old";
    static const char next_reply[] = "reply";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *router = flowmq_socket(ctx, FLOWMQ_ROUTER);
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_DEALER);
    flowmq_socket_t *replacement = flowmq_socket(ctx, FLOWMQ_DEALER);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(first, FLOWMQ_IDENTITY, identity,
                                  sizeof(identity) - 1u), SALTS_OK);
    check_equal(flowmq_setsockopt(replacement, FLOWMQ_IDENTITY, identity,
                                  sizeof(identity) - 1u), SALTS_OK);
    check_equal(flowmq_bind(router, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(first, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(first, router), SALTS_OK);
      status = flowmq_send(first, first_request, sizeof(first_request) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(first, router), SALTS_OK);
      status = flowmq_recv(router, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_recv(router, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_send(router, identity, sizeof(identity) - 1u,
                            FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
    check_equal(flowmq_close(first), SALTS_OK);
    for (size_t i = 0u; i < 200u; ++i) {
      flowmq_pollitem_t item = {.socket = router};
      size_t ready = 0u;
      check_equal(flowmq_poll(&item, 1u, 0u, &ready), SALTS_OK);
    }

    check_equal(flowmq_connect(replacement, endpoint), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(replacement, router), SALTS_OK);
      status = flowmq_send(replacement, next_request,
                           sizeof(next_request) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(router, old_reply, sizeof(old_reply) - 1u, 0),
                SALTS_ENOTCONN);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(replacement, router), SALTS_OK);
      status = flowmq_recv(router, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_recv(router, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_send(router, identity, sizeof(identity) - 1u,
                            FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
    check_equal(flowmq_send(router, next_reply, sizeof(next_reply) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(replacement, router), SALTS_OK);
      status = flowmq_recv(replacement, received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(next_reply) - 1u);

    check_equal(flowmq_close(replacement), SALTS_OK);
    check_equal(flowmq_close(router), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("filters PUB data by SUB prefix") {
    static const char subscription[] = "orders.";
    static const char rejected[] = "payments.created";
    static const char accepted[] = "orders.created";
    char endpoint[128] = {0};
    char received[64] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *pub = flowmq_socket(ctx, FLOWMQ_PUB);
    flowmq_socket_t *sub = flowmq_socket(ctx, FLOWMQ_SUB);

    check_equal(flowmq_setsockopt(sub, FLOWMQ_SUBSCRIBE, subscription,
                                 sizeof(subscription) - 1u), SALTS_OK);
    check_equal(flowmq_bind(pub, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(pub, endpoint, sizeof(endpoint), &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(sub, endpoint), SALTS_OK);
    for (size_t i = 0u; i < 100u; ++i)
      check_equal(progress_pair(sub, pub), SALTS_OK);

    check_equal(flowmq_send(pub, rejected, sizeof(rejected) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    for (size_t i = 0u; i < 100u; ++i)
      check_equal(progress_pair(sub, pub), SALTS_OK);
    check_equal(flowmq_recv(sub, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_EBUSY);

    check_equal(flowmq_send(pub, accepted, sizeof(accepted) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT; ++i) {
      check_equal(progress_pair(sub, pub), SALTS_OK);
      if (flowmq_recv(sub, received, sizeof(received), &received_size,
                      FLOWMQ_DONTWAIT) == SALTS_OK)
        break;
    }
    check_equal(received_size, sizeof(accepted) - 1u);
    check_equal(memcmp(received, accepted, received_size), 0);

    check_equal(flowmq_close(sub), SALTS_OK);
    check_equal(flowmq_close(pub), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("fans one PUB message out to every matching SUB peer") {
    static const char payload[] = "broadcast";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *pub = flowmq_socket(ctx, FLOWMQ_PUB);
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_SUB);
    flowmq_socket_t *second = flowmq_socket(ctx, FLOWMQ_SUB);

    check_equal(flowmq_setsockopt(first, FLOWMQ_SUBSCRIBE, NULL, 0u), SALTS_OK);
    check_equal(flowmq_setsockopt(second, FLOWMQ_SUBSCRIBE, NULL, 0u), SALTS_OK);
    check_equal(flowmq_bind(pub, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(pub, endpoint, sizeof(endpoint), &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(first, endpoint), SALTS_OK);
    check_equal(flowmq_connect(second, endpoint), SALTS_OK);
    for (size_t i = 0u; i < 200u; ++i)
      check_equal(progress_three(first, second, pub), SALTS_OK);

    check_equal(flowmq_send(pub, payload, sizeof(payload) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT; ++i) {
      check_equal(progress_three(first, second, pub), SALTS_OK);
      if (flowmq_recv(first, received, sizeof(received), &received_size,
                      FLOWMQ_DONTWAIT) == SALTS_OK)
        break;
    }
    check_equal(received_size, sizeof(payload) - 1u);
    received_size = 0u;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT; ++i) {
      check_equal(progress_three(first, second, pub), SALTS_OK);
      if (flowmq_recv(second, received, sizeof(received), &received_size,
                      FLOWMQ_DONTWAIT) == SALTS_OK)
        break;
    }
    check_equal(received_size, sizeof(payload) - 1u);

    check_equal(flowmq_close(second), SALTS_OK);
    check_equal(flowmq_close(first), SALTS_OK);
    check_equal(flowmq_close(pub), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("does not inherit a multipart PUB snapshot across peer sessions") {
    static const char first_part[] = "old.topic";
    static const char final_part[] = "old-payload";
    static const char next_message[] = "new.topic";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *pub = flowmq_socket(ctx, FLOWMQ_PUB);
    flowmq_socket_t *first = flowmq_socket(ctx, FLOWMQ_SUB);
    flowmq_socket_t *replacement = flowmq_socket(ctx, FLOWMQ_SUB);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(first, FLOWMQ_SUBSCRIBE, NULL, 0u),
                SALTS_OK);
    check_equal(flowmq_setsockopt(replacement, FLOWMQ_SUBSCRIBE, NULL, 0u),
                SALTS_OK);
    check_equal(flowmq_bind(pub, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(pub, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(first, endpoint), SALTS_OK);
    for (size_t i = 0u; i < 200u; ++i)
      check_equal(progress_pair(first, pub), SALTS_OK);
    check_equal(flowmq_send(pub, first_part, sizeof(first_part) - 1u,
                            FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE), SALTS_OK);
    check_equal(flowmq_close(first), SALTS_OK);
    for (size_t i = 0u; i < 200u; ++i) {
      flowmq_pollitem_t item = {.socket = pub};
      size_t ready = 0u;
      check_equal(flowmq_poll(&item, 1u, 0u, &ready), SALTS_OK);
    }

    check_equal(flowmq_connect(replacement, endpoint), SALTS_OK);
    for (size_t i = 0u; i < 200u; ++i)
      check_equal(progress_pair(replacement, pub), SALTS_OK);
    check_equal(flowmq_send(pub, final_part, sizeof(final_part) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    for (size_t i = 0u; i < 100u; ++i)
      check_equal(progress_pair(replacement, pub), SALTS_OK);
    check_equal(flowmq_recv(replacement, received, sizeof(received),
                            &received_size, FLOWMQ_DONTWAIT), SALTS_EBUSY);

    check_equal(flowmq_send(pub, next_message, sizeof(next_message) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(replacement, pub), SALTS_OK);
      status = flowmq_recv(replacement, received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(next_message) - 1u);

    check_equal(flowmq_close(replacement), SALTS_OK);
    check_equal(flowmq_close(pub), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("does not expose a multipart message before its final part arrives") {
    static const char first_part[] = "first";
    static const char final_part[] = "final";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t option_size = sizeof(int);
    int more = 0;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;

    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, first_part, sizeof(first_part) - 1u,
                           FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE);
    }
    check_equal(status, SALTS_OK);
    for (size_t i = 0u; i < 100u; ++i)
      check_equal(progress_pair(sender, receiver), SALTS_OK);
    check_equal(flowmq_recv(receiver, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_EBUSY);

    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, final_part, sizeof(final_part) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_recv(receiver, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(first_part) - 1u);
    check_equal(memcmp(received, first_part, received_size), 0);
    check_equal(flowmq_getsockopt(receiver, FLOWMQ_RCVMORE, &more,
                                  &option_size), SALTS_OK);
    check_equal(more, 1);
    check_equal(flowmq_recv(receiver, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(received_size, sizeof(final_part) - 1u);
    check_equal(memcmp(received, final_part, received_size), 0);
    option_size = sizeof(int);
    check_equal(flowmq_getsockopt(receiver, FLOWMQ_RCVMORE, &more,
                                  &option_size), SALTS_OK);
    check_equal(more, 0);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("returns multipart credit only after the final part is consumed") {
    static const char first_part[] = "1234";
    static const char final_part[] = "5678";
    static const char next[] = "x";
    char endpoint[128] = {0};
    char received[8] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t receive_hwm_bytes = 8u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(receiver, FLOWMQ_RCVHWM_BYTES,
                                  &receive_hwm_bytes,
                                  sizeof(receive_hwm_bytes)), SALTS_OK);
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, first_part, sizeof(first_part) - 1u,
                           FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(sender, final_part, sizeof(final_part) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT; ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_recv(receiver, received, sizeof(received),
                           &received_size, FLOWMQ_DONTWAIT);
      if (status != SALTS_EBUSY) break;
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(first_part) - 1u);
    for (size_t i = 0u; i < 32u; ++i)
      check_equal(progress_pair(sender, receiver), SALTS_OK);
    check_equal(flowmq_send(sender, next, sizeof(next) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_ENOBUFS);
    check_equal(flowmq_recv(receiver, received, sizeof(received),
                            &received_size, FLOWMQ_DONTWAIT), SALTS_OK);
    status = SALTS_ENOBUFS;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_ENOBUFS;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, next, sizeof(next) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("applies the byte HWM to the complete multipart message") {
    static const char first_part[] = "abc";
    static const char oversized_final[] = "de";
    static const char fitting_final[] = "d";
    char endpoint[128] = {0};
    char received[16] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t send_hwm_bytes = 4u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;
    int oversized_status;
    int fitting_status = SALTS_EBUSY;
    int premature_receive_status = SALTS_OK;
    int first_receive_status = SALTS_EBUSY;
    int final_receive_status = SALTS_EBUSY;
    size_t first_received_size = 0u;
    size_t final_received_size = 0u;
    char first_received[16] = {0};
    char final_received[16] = {0};

    check_equal(
        flowmq_setsockopt(sender, FLOWMQ_SNDHWM_BYTES, &send_hwm_bytes, sizeof(send_hwm_bytes)),
        SALTS_OK);
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint), &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, first_part, sizeof(first_part) - 1u,
                           FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE);
    }
    check_equal(status, SALTS_OK);
    for (size_t i = 0u; i < 100u; ++i)
      check_equal(progress_pair(sender, receiver), SALTS_OK);

    oversized_status =
        flowmq_send(sender, oversized_final, sizeof(oversized_final) - 1u, FLOWMQ_DONTWAIT);
    if (oversized_status == SALTS_EMSGSIZE) {
      premature_receive_status =
          flowmq_recv(receiver, received, sizeof(received), &received_size, FLOWMQ_DONTWAIT);
      fitting_status =
          flowmq_send(sender, fitting_final, sizeof(fitting_final) - 1u, FLOWMQ_DONTWAIT);
      for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && first_receive_status == SALTS_EBUSY;
           ++i) {
        check_equal(progress_pair(sender, receiver), SALTS_OK);
        first_receive_status = flowmq_recv(receiver, first_received, sizeof(first_received),
                                           &first_received_size, FLOWMQ_DONTWAIT);
      }
      if (first_receive_status == SALTS_OK)
        final_receive_status = flowmq_recv(receiver, final_received, sizeof(final_received),
                                           &final_received_size, FLOWMQ_DONTWAIT);
    }

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
    check_equal(oversized_status, SALTS_EMSGSIZE);
    check_equal(premature_receive_status, SALTS_EBUSY);
    check_equal(fitting_status, SALTS_OK);
    check_equal(first_receive_status, SALTS_OK);
    check_equal(first_received_size, sizeof(first_part) - 1u);
    check_equal(memcmp(first_received, first_part, first_received_size), 0);
    check_equal(final_receive_status, SALTS_OK);
    check_equal(final_received_size, sizeof(fitting_final) - 1u);
    check_equal(memcmp(final_received, fitting_final, final_received_size), 0);
  }

  it("bounds queued sends by message count and recovers after completion") {
    static const char first[] = "one";
    static const char second[] = "two";
    static const char third[] = "three";
    char endpoint[128] = {0};
    char received[16] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    int send_hwm = 2;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(sender, FLOWMQ_SNDHWM, &send_hwm,
                                 sizeof(send_hwm)), SALTS_OK);
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, first, sizeof(first) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(sender, second, sizeof(second) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(flowmq_send(sender, third, sizeof(third) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_ENOBUFS);

    status = SALTS_ENOBUFS;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_ENOBUFS;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, third, sizeof(third) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    for (size_t message = 0u; message < 3u; ++message) {
      status = SALTS_EBUSY;
      for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                          status == SALTS_EBUSY;
           ++i) {
        check_equal(progress_pair(sender, receiver), SALTS_OK);
        status = flowmq_recv(receiver, received, sizeof(received),
                             &received_size, FLOWMQ_DONTWAIT);
      }
      check_equal(status, SALTS_OK);
    }

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("blocking send drives its socket until HWM admission becomes available") {
    static const char first[] = "one";
    static const char second[] = "two";
    char endpoint[128] = {0};
    size_t endpoint_size = 0u;
    int send_hwm = 1;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(sender, FLOWMQ_SNDHWM, &send_hwm,
                                 sizeof(send_hwm)), SALTS_OK);
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, first, sizeof(first) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(sender, second, sizeof(second) - 1u, 0), SALTS_OK);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("blocking receive drives its socket until an admitted message arrives") {
    static const char payload[] = "blocking-receive";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t ready = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_pollitem_t sender_item = {.socket = sender};
    int status = SALTS_EBUSY;

    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, payload, sizeof(payload) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_poll(&sender_item, 1u, 100u, &ready), SALTS_OK);
    check_equal(flowmq_recv(receiver, received, sizeof(received), &received_size,
                            0), SALTS_OK);
    check_equal(received_size, sizeof(payload) - 1u);
    check_equal(memcmp(received, payload, received_size), 0);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("coordinates the byte HWM before excess data reaches the receiver") {
    static const char first[] = "1234";
    static const char second[] = "5678";
    char endpoint[128] = {0};
    char received[8] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t receive_hwm_bytes = 4u;
    int receive_hwm = 2;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(receiver, FLOWMQ_RCVHWM, &receive_hwm,
                                 sizeof(receive_hwm)), SALTS_OK);
    check_equal(flowmq_setsockopt(receiver, FLOWMQ_RCVHWM_BYTES,
                                 &receive_hwm_bytes,
                                 sizeof(receive_hwm_bytes)), SALTS_OK);
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, first, sizeof(first) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    for (size_t i = 0u; i < 32u; ++i)
      check_equal(progress_pair(sender, receiver), SALTS_OK);
    check_equal(flowmq_send(sender, second, sizeof(second) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_ENOBUFS);

    check_equal(flowmq_recv(receiver, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(received_size, sizeof(first) - 1u);
    check_equal(memcmp(received, first, received_size), 0);
    check_equal(flowmq_recv(receiver, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_EBUSY);

    status = SALTS_ENOBUFS;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_ENOBUFS;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, second, sizeof(second) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_recv(receiver, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(second) - 1u);
    check_equal(memcmp(received, second, received_size), 0);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("stops at negotiated receive credit and resumes after application consumption") {
    static const char full_window[] = "1234";
    static const char next[] = "x";
    char endpoint[128] = {0};
    char received[8] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t receive_hwm_bytes = sizeof(full_window) - 1u;
    size_t ready = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_pollitem_t sender_item = {
        .socket = sender, .events = FLOWMQ_POLLOUT};
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(receiver, FLOWMQ_RCVHWM_BYTES,
                                  &receive_hwm_bytes,
                                  sizeof(receive_hwm_bytes)), SALTS_OK);
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, full_window, sizeof(full_window) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    for (size_t i = 0u; i < 32u; ++i)
      check_equal(progress_pair(sender, receiver), SALTS_OK);

    check_equal(flowmq_send(sender, next, sizeof(next) - 1u, FLOWMQ_DONTWAIT),
                SALTS_ENOBUFS);
    check_equal(flowmq_poll(&sender_item, 1u, 0u, &ready), SALTS_OK);
    check_equal(ready, 0u);
    check_equal(flowmq_recv(receiver, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(received_size, sizeof(full_window) - 1u);

    status = SALTS_ENOBUFS;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_ENOBUFS;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, next, sizeof(next) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("publishes sub-quantum credit on the configured caller-driven deadline") {
    static const char first[] = "1234";
    static const char second[] = "56789";
    char endpoint[128] = {0};
    char received[8] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t receive_hwm_bytes = 8u;
    size_t flow_quantum = 8u;
    size_t ready = 0u;
    int flow_interval_ms = 1;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_pollitem_t items[] = {{.socket = sender}, {.socket = receiver}};
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(receiver, FLOWMQ_RCVHWM_BYTES,
                                  &receive_hwm_bytes,
                                  sizeof(receive_hwm_bytes)), SALTS_OK);
    check_equal(flowmq_setsockopt(receiver, FLOWMQ_FLOW_UPDATE_QUANTUM,
                                  &flow_quantum, sizeof(flow_quantum)), SALTS_OK);
    check_equal(flowmq_setsockopt(receiver, FLOWMQ_FLOW_UPDATE_IVL,
                                  &flow_interval_ms,
                                  sizeof(flow_interval_ms)), SALTS_OK);
    check_equal(flowmq_bind(receiver, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(sender, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, first, sizeof(first) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_recv(receiver, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(sender, second, sizeof(second) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_ENOBUFS);

    check_equal(flowmq_poll(items, 2u, 5u, &ready), SALTS_OK);
    status = SALTS_ENOBUFS;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        (status == SALTS_ENOBUFS || status == SALTS_EBUSY);
         ++i) {
      check_equal(progress_pair(sender, receiver), SALTS_OK);
      status = flowmq_send(sender, second, sizeof(second) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_close(receiver), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("does not charge ROUTER synthetic identity against wire credit") {
    static const char identity[] = "d";
    static const char payload[] = "12345678";
    static const char next[] = "xy";
    char endpoint[128] = {0};
    char received[16] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    size_t receive_hwm_bytes = 9u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *router = flowmq_socket(ctx, FLOWMQ_ROUTER);
    flowmq_socket_t *dealer = flowmq_socket(ctx, FLOWMQ_DEALER);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(dealer, FLOWMQ_IDENTITY, identity,
                                  sizeof(identity) - 1u), SALTS_OK);
    check_equal(flowmq_setsockopt(router, FLOWMQ_RCVHWM_BYTES,
                                  &receive_hwm_bytes,
                                  sizeof(receive_hwm_bytes)), SALTS_OK);
    check_equal(flowmq_bind(router, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(router, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(dealer, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(dealer, router), SALTS_OK);
      status = flowmq_send(dealer, payload, sizeof(payload) - 1u,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT; ++i) {
      check_equal(progress_pair(dealer, router), SALTS_OK);
      status = flowmq_recv(router, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
      if (status != SALTS_EBUSY) break;
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(identity) - 1u);
    check_equal(flowmq_send(dealer, next, sizeof(next) - 1u, FLOWMQ_DONTWAIT),
                SALTS_ENOBUFS);
    check_equal(flowmq_recv(router, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    check_equal(received_size, sizeof(payload) - 1u);

    status = SALTS_ENOBUFS;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_ENOBUFS;
         ++i) {
      check_equal(progress_pair(dealer, router), SALTS_OK);
      status = flowmq_send(dealer, next, sizeof(next) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);

    check_equal(flowmq_close(dealer), SALTS_OK);
    check_equal(flowmq_close(router), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("rejects a send larger than its byte HWM") {
    static const char payload[] = "12345";
    size_t send_hwm_bytes = 4u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *sender = flowmq_socket(ctx, FLOWMQ_PAIR);

    check_equal(flowmq_setsockopt(sender, FLOWMQ_SNDHWM_BYTES,
                                 &send_hwm_bytes, sizeof(send_hwm_bytes)),
                SALTS_OK);
    check_equal(flowmq_send(sender, payload, sizeof(payload) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_EMSGSIZE);

    check_equal(flowmq_close(sender), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("enforces REQ REP alternation over TCP") {
    static const char request[] = "request";
    static const char reply[] = "reply";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *req = flowmq_socket(ctx, FLOWMQ_REQ);
    flowmq_socket_t *rep = flowmq_socket(ctx, FLOWMQ_REP);
    int status = SALTS_EBUSY;

    check_equal(flowmq_bind(rep, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(rep, endpoint, sizeof(endpoint), &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(req, endpoint), SALTS_OK);
    check_equal(flowmq_send(rep, reply, sizeof(reply) - 1u, 0), SALTS_EPROTO);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(req, rep), SALTS_OK);
      status = flowmq_send(req, request, sizeof(request) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(req, request, sizeof(request) - 1u, 0),
                SALTS_EPROTO);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(req, rep), SALTS_OK);
      status = flowmq_recv(rep, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(request) - 1u);
    check_equal(flowmq_recv(rep, received, sizeof(received), &received_size, 0),
                SALTS_EPROTO);

    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(req, rep), SALTS_OK);
      status = flowmq_send(rep, reply, sizeof(reply) - 1u, FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(req, rep), SALTS_OK);
      status = flowmq_recv(req, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(received_size, sizeof(reply) - 1u);

    check_equal(flowmq_close(req), SALTS_OK);
    check_equal(flowmq_close(rep), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("rejects blocking direction changes inside multipart messages") {
    static const char first_part[] = "first";
    static const char final_part[] = "final";
    static const char interleaved[] = "interleaved";
    char endpoint[128] = {0};
    char received[32] = {0};
    size_t endpoint_size = 0u;
    size_t received_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *left = flowmq_socket(ctx, FLOWMQ_PAIR);
    flowmq_socket_t *right = flowmq_socket(ctx, FLOWMQ_PAIR);
    int status = SALTS_EBUSY;

    check_equal(flowmq_bind(left, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(left, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(right, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(left, right), SALTS_OK);
      status = flowmq_send(left, first_part, sizeof(first_part) - 1u,
                           FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_recv(left, received, sizeof(received), &received_size, 0),
                SALTS_EPROTO);
    check_equal(flowmq_send(left, final_part, sizeof(final_part) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);

    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(left, right), SALTS_OK);
      status = flowmq_recv(right, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_recv(right, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);

    status = flowmq_send(right, first_part, sizeof(first_part) - 1u,
                         FLOWMQ_DONTWAIT | FLOWMQ_SNDMORE);
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(right, final_part, sizeof(final_part) - 1u,
                            FLOWMQ_DONTWAIT), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT &&
                        status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(left, right), SALTS_OK);
      status = flowmq_recv(left, received, sizeof(received), &received_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_send(left, interleaved, sizeof(interleaved) - 1u, 0),
                SALTS_EPROTO);
    check_equal(flowmq_recv(left, received, sizeof(received), &received_size,
                            FLOWMQ_DONTWAIT), SALTS_OK);

    check_equal(flowmq_close(right), SALTS_OK);
    check_equal(flowmq_close(left), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("exposes XSUB subscription commands through XPUB receive") {
    static const char topic[] = "events.";
    char endpoint[128] = {0};
    unsigned char event[32] = {0};
    size_t endpoint_size = 0u;
    size_t event_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *xpub = flowmq_socket(ctx, FLOWMQ_XPUB);
    flowmq_socket_t *xsub = flowmq_socket(ctx, FLOWMQ_XSUB);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(xsub, FLOWMQ_SUBSCRIBE, topic,
                                 sizeof(topic) - 1u), SALTS_OK);
    check_equal(flowmq_bind(xpub, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(xpub, endpoint, sizeof(endpoint), &endpoint_size),
                SALTS_OK);
    check_equal(flowmq_connect(xsub, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY; ++i) {
      check_equal(progress_pair(xsub, xpub), SALTS_OK);
      status = flowmq_recv(xpub, event, sizeof(event), &event_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(event_size, sizeof(topic));
    check_equal(event[0], 1u);
    check_equal(memcmp(event + 1u, topic, sizeof(topic) - 1u), 0);

    check_equal(flowmq_close(xsub), SALTS_OK);
    check_equal(flowmq_close(xpub), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("replays desired XSUB subscriptions into a reconnected session") {
    static const char topic[] = "events.";
    char endpoint[128] = {0};
    unsigned char event[32] = {0};
    size_t endpoint_size = 0u;
    size_t event_size = 0u;
    int reconnect_ms = 1;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *xpub = flowmq_socket(ctx, FLOWMQ_XPUB);
    flowmq_socket_t *xsub = flowmq_socket(ctx, FLOWMQ_XSUB);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(xsub, FLOWMQ_RECONNECT_IVL,
                                 &reconnect_ms, sizeof(reconnect_ms)),
                SALTS_OK);
    check_equal(flowmq_setsockopt(xsub, FLOWMQ_RECONNECT_IVL_MAX,
                                 &reconnect_ms, sizeof(reconnect_ms)),
                SALTS_OK);
    check_equal(flowmq_setsockopt(xsub, FLOWMQ_SUBSCRIBE, topic,
                                 sizeof(topic) - 1u), SALTS_OK);
    check_equal(flowmq_bind(xpub, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(xpub, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(xsub, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(xsub, xpub), SALTS_OK);
      status = flowmq_recv(xpub, event, sizeof(event), &event_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(flowmq_close(xpub), SALTS_OK);

    xpub = flowmq_socket(ctx, FLOWMQ_XPUB);
    check_not_null(xpub);
    check_equal(flowmq_bind(xpub, endpoint), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(xsub, xpub), SALTS_OK);
      status = flowmq_recv(xpub, event, sizeof(event), &event_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(event_size, sizeof(topic));
    check_equal(event[0], 1u);
    check_equal(memcmp(event + 1u, topic, sizeof(topic) - 1u), 0);

    check_equal(flowmq_close(xsub), SALTS_OK);
    check_equal(flowmq_close(xpub), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }

  it("propagates a dynamic XSUB unsubscribe command to XPUB") {
    static const char topic[] = "events.";
    char endpoint[128] = {0};
    unsigned char event[32] = {0};
    size_t endpoint_size = 0u;
    size_t event_size = 0u;
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *xpub = flowmq_socket(ctx, FLOWMQ_XPUB);
    flowmq_socket_t *xsub = flowmq_socket(ctx, FLOWMQ_XSUB);
    int status = SALTS_EBUSY;

    check_equal(flowmq_setsockopt(xsub, FLOWMQ_SUBSCRIBE, topic,
                                 sizeof(topic) - 1u), SALTS_OK);
    check_equal(flowmq_bind(xpub, "tcp://127.0.0.1:0"), SALTS_OK);
    check_equal(flowmq_last_endpoint(xpub, endpoint, sizeof(endpoint),
                                     &endpoint_size), SALTS_OK);
    check_equal(flowmq_connect(xsub, endpoint), SALTS_OK);
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(xsub, xpub), SALTS_OK);
      status = flowmq_recv(xpub, event, sizeof(event), &event_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(event[0], 1u);

    check_equal(flowmq_setsockopt(xsub, FLOWMQ_UNSUBSCRIBE, topic,
                                 sizeof(topic) - 1u), SALTS_OK);
    status = SALTS_EBUSY;
    for (size_t i = 0u; i < FLOWMQ_TEST_PROGRESS_LIMIT && status == SALTS_EBUSY;
         ++i) {
      check_equal(progress_pair(xsub, xpub), SALTS_OK);
      status = flowmq_recv(xpub, event, sizeof(event), &event_size,
                           FLOWMQ_DONTWAIT);
    }
    check_equal(status, SALTS_OK);
    check_equal(event_size, sizeof(topic));
    check_equal(event[0], 0u);
    check_equal(memcmp(event + 1u, topic, sizeof(topic) - 1u), 0);

    check_equal(flowmq_close(xsub), SALTS_OK);
    check_equal(flowmq_close(xpub), SALTS_OK);
    check_equal(flowmq_ctx_term(ctx), SALTS_OK);
  }
}
