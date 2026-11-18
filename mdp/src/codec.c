#include "mdp_internal.h"

/* RPC integers use network byte order, independent of the host ABI. */
enum {
  HEADER = 55,
  WIRE_LIMIT = HEADER + FLOWMQ_MDP_SERVICE_MAX + 4 * FLOWMQ_MDP_PARTS_MAX + FLOWMQ_MDP_PAYLOAD_MAX
};

int mdp_valid_service(const char *s) {
  if (!s || !*s) return 0;
  for (size_t n = 0; n <= FLOWMQ_MDP_SERVICE_MAX; ++n)
    if (!s[n]) return 1;
  return 0;
}

int mdp_valid_body(const flowmq_mdp_body *b) {
  if (!b || b->count > FLOWMQ_MDP_PARTS_MAX || b->size > FLOWMQ_MDP_PAYLOAD_MAX) return 0;
  uint32_t total = 0;
  for (size_t i = 0; i < b->count; ++i) {
    if (b->lengths[i] > b->size - total) return 0;
    total += b->lengths[i];
  }
  return total == b->size;
}

int flowmq_mdp_add_part(flowmq_mdp_body *b, const void *data, size_t size) {
  if (!mdp_valid_body(b) || (!data && size)) return SALTS_EINVAL;
  if (b->count == FLOWMQ_MDP_PARTS_MAX || size > FLOWMQ_MDP_PAYLOAD_MAX - b->size)
    return SALTS_EMSGSIZE;
  if (size) memcpy(b->bytes + b->size, data, size);
  b->lengths[b->count++] = (uint32_t)size;
  b->size += (uint32_t)size;
  return SALTS_OK;
}

static void put(unsigned char *p, uint64_t value, size_t n) {
  while (n) {
    p[--n] = (unsigned char)value;
    value >>= 8;
  }
}
static uint64_t get(const unsigned char *p, size_t n) {
  uint64_t v = 0;
  for (size_t i = 0; i < n; ++i)
    v = (v << 8) | p[i];
  return v;
}

static int valid(const mdp_packet *p) {
  int request = p->id.client && p->id.sequence && p->id.attempt;
  switch (p->command) {
  case CALL:
    return request && p->budget && !p->broker && !p->worker;
  case CANCEL:
    return request && !p->broker && !p->worker && !p->body.count;
  case REGISTER:
    return p->worker && !p->broker && p->slot && p->slot <= FLOWMQ_MDP_CAPACITY_MAX &&
           !p->body.count;
  case REGISTERED:
  case HEARTBEAT:
  case LEAVE:
    return p->worker && p->broker && !p->body.count;
  case DISPATCH:
    return request && p->worker && p->broker && p->budget;
  case COMPLETE:
    return request && p->worker && p->broker;
  case RESULT:
    return request && p->broker;
  case REJECT:
  case UNKNOWN:
    return request && p->broker && !p->body.count;
  default:
    return 0;
  }
}

int mdp_send(flowmq_socket_t *s, const mdp_route *route, const mdp_packet *p) {
  unsigned char frame[WIRE_LIMIT];
  if (!s || !mdp_valid_service(p->service) || !mdp_valid_body(&p->body) || !valid(p))
    return SALTS_EINVAL;
  size_t name = strlen(p->service);
  memcpy(frame, "FMR1", 4);
  frame[4] = (unsigned char)p->command;
  put(frame + 5, p->id.client, 8);
  put(frame + 13, p->id.sequence, 8);
  put(frame + 21, p->id.attempt, 4);
  put(frame + 25, p->worker, 8);
  put(frame + 33, p->broker, 8);
  put(frame + 41, p->slot, 4);
  put(frame + 45, p->budget, 4);
  put(frame + 49, (uint32_t)p->status, 4);
  frame[53] = (unsigned char)name;
  frame[54] = (unsigned char)p->body.count;
  memcpy(frame + HEADER, p->service, name);
  size_t offset = HEADER + name;
  for (size_t i = 0; i < p->body.count; ++i) {
    put(frame + offset, p->body.lengths[i], 4);
    offset += 4;
  }
  memcpy(frame + offset, p->body.bytes, p->body.size);
  if (route)
    return flowmq_router_try_send(s, route->bytes, route->size, frame, offset + p->body.size);
  return flowmq_send(s, frame, offset + p->body.size, FLOWMQ_DONTWAIT);
}

static int unpack(const unsigned char *buf, size_t size, mdp_packet *p) {
  if (size < HEADER || size > WIRE_LIMIT || memcmp(buf, "FMR1", 4)) return SALTS_EPROTO;
  size_t name = buf[53], parts = buf[54];
  if (!name || name > FLOWMQ_MDP_SERVICE_MAX || parts > FLOWMQ_MDP_PARTS_MAX ||
      size < HEADER + name + 4 * parts || memchr(buf + HEADER, 0, name))
    return SALTS_EPROTO;
  memset(p, 0, sizeof(*p));
  p->command = (enum mdp_command)buf[4];
  p->id = (flowmq_mdp_id){get(buf + 5, 8), get(buf + 13, 8), (uint32_t)get(buf + 21, 4)};
  p->worker = get(buf + 25, 8);
  p->broker = get(buf + 33, 8);
  p->slot = (uint32_t)get(buf + 41, 4);
  p->budget = (uint32_t)get(buf + 45, 4);
  p->status = (int32_t)(uint32_t)get(buf + 49, 4);
  memcpy(p->service, buf + HEADER, name);
  size_t offset = HEADER + name;
  p->body.count = (uint32_t)parts;
  for (size_t i = 0; i < parts; ++i) {
    p->body.lengths[i] = (uint32_t)get(buf + offset, 4);
    offset += 4;
  }
  p->body.size = (uint32_t)(size - offset);
  if (!mdp_valid_body(&p->body) || !valid(p)) return SALTS_EPROTO;
  memcpy(p->body.bytes, buf + offset, p->body.size);
  return SALTS_OK;
}

/* At most one identity and one body per turn. A malformed multipart tail is
 * discarded one part per later turn so it cannot monopolize the owner. */
int mdp_receive(flowmq_socket_t *s, mdp_reader *r, int routed, mdp_route *route, mdp_packet *out) {
  for (int i = 0; i < 2; ++i) {
    mem_slice_t part = {0};
    int rc = flowmq_recv_slice(s, &part, FLOWMQ_DONTWAIT);
    if (rc != SALTS_OK) {
      if (rc == SALTS_ENOTCONN) memset(r, 0, sizeof(*r));
      return rc;
    }
    int more = 0;
    size_t n = sizeof(more);
    rc = flowmq_getsockopt(s, FLOWMQ_RCVMORE, &more, &n);
    if (rc == SALTS_OK) {
      if (r->discard) {
        r->discard = more;
        rc = SALTS_EBUSY;
      } else if (routed && !r->route.size && more && part.length &&
                 part.length <= sizeof(r->route.bytes)) {
        r->route.size = part.length;
        memcpy(r->route.bytes, part.data, part.length);
        mem_slice_release(&part);
        continue;
      } else if (more || (routed && !r->route.size)) {
        r->discard = more;
        r->route.size = 0;
        rc = SALTS_EPROTO;
      } else {
        if (route) *route = r->route;
        r->route.size = 0;
        rc = unpack((const unsigned char *)part.data, part.length, out);
      }
    }
    mem_slice_release(&part);
    return rc;
  }
  return SALTS_EBUSY;
}
