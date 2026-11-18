#ifndef FLOWMQ_MDP_INTERNAL_H
#define FLOWMQ_MDP_INTERNAL_H
#include "flowmq_mdp.h"
#include "flowmq_protocol.h"
#include <core/error.h>
#include <stdlib.h>
#include <string.h>

enum mdp_command {
  CALL = 1,
  REGISTER,
  REGISTERED,
  HEARTBEAT,
  DISPATCH,
  COMPLETE,
  RESULT,
  REJECT,
  UNKNOWN,
  LEAVE,
  CANCEL
};
typedef struct mdp_packet {
  enum mdp_command command;
  flowmq_mdp_id id;
  uint64_t worker, broker;
  uint32_t slot, budget;
  int32_t status;
  char service[FLOWMQ_MDP_SERVICE_MAX + 1];
  flowmq_mdp_body body;
} mdp_packet;
typedef struct mdp_route {
  unsigned char bytes[FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE];
  size_t size;
} mdp_route;
typedef struct mdp_reader {
  mdp_route route;
  int discard;
} mdp_reader;

int mdp_valid_service(const char *service);
int mdp_valid_body(const flowmq_mdp_body *body);
int mdp_take_member_result(flowmq_mdp_client *client, flowmq_mdp_id id,
                           flowmq_mdp_result *out, uint64_t *worker_session);
int mdp_send(flowmq_socket_t *s, const mdp_route *route, const mdp_packet *packet);
int mdp_receive(flowmq_socket_t *s, mdp_reader *reader, int routed, mdp_route *route,
                mdp_packet *out);
static inline int mdp_same_id(flowmq_mdp_id a, flowmq_mdp_id b) {
  return a.client == b.client && a.sequence == b.sequence && a.attempt == b.attempt;
}
static inline int mdp_same_route(const mdp_route *a, const mdp_route *b) {
  return a->size == b->size && !memcmp(a->bytes, b->bytes, a->size);
}
static inline int mdp_busy(int rc) { return rc == SALTS_EBUSY || rc == SALTS_ENOBUFS; }
#endif
