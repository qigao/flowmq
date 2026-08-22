#include "flowmq_protocol.h"
#include "flowmq_protocol_esb.h"

#include "turbo_buffer.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <limits.h>
#include <string.h>

static const unsigned char FLOW_FMQ_MAGIC[4] = {'T', 'F', 'M', 'Q'};
static const unsigned char FLOW_FMQ_SECURITY_MAGIC[4] = {'F', 'M', 'S', '3'};

#define FLOW_FMQ_SECURITY_HEADER_SIZE FLOWMQ_PROTOCOL_SECURITY_HEADER_SIZE

#define FLOW_FMQ_PROTOCOL_VERSION FLOWMQ_PROTOCOL_WIRE_VERSION
#define FLOW_FMQ_HEADER_SIZE FLOWMQ_PROTOCOL_HEADER_SIZE
#define FLOW_FMQ_PACKET_PAYLOAD_SIZE FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE
#define FLOW_FMQ_PACKET_FIRST FLOWMQ_PROTOCOL_PACKET_FIRST
#define FLOW_FMQ_PACKET_LAST FLOWMQ_PROTOCOL_PACKET_LAST
#define FLOW_FMQ_INCOMPLETE FLOWMQ_PROTOCOL_INCOMPLETE
#define FLOW_FMQ_FRAME_HELLO FLOWMQ_PROTOCOL_FRAME_HELLO
#define FLOW_FMQ_FRAME_DATA FLOWMQ_PROTOCOL_FRAME_DATA
#define FLOW_FMQ_FRAME_PING FLOWMQ_PROTOCOL_FRAME_PING
#define FLOW_FMQ_FRAME_PONG FLOWMQ_PROTOCOL_FRAME_PONG
#define FLOW_FMQ_FRAME_SUBSCRIBE FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE
#define FLOW_FMQ_FRAME_UNSUBSCRIBE FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE
#define FLOW_FMQ_HEARTBEAT_WAIT FLOWMQ_PROTOCOL_HEARTBEAT_WAIT
#define FLOW_FMQ_HEARTBEAT_SEND_PING FLOWMQ_PROTOCOL_HEARTBEAT_SEND_PING
#define FLOW_FMQ_HEARTBEAT_EXPIRED FLOWMQ_PROTOCOL_HEARTBEAT_EXPIRED
#define FLOW_FMQ_HEARTBEAT_RECV_EXPIRED FLOWMQ_PROTOCOL_HEARTBEAT_RECV_EXPIRED
#define TURBO_FLOW_FMQ_PUB FLOWMQ_PROTOCOL_PUB
#define TURBO_FLOW_FMQ_SUB FLOWMQ_PROTOCOL_SUB
#define TURBO_FLOW_FMQ_PUSH FLOWMQ_PROTOCOL_PUSH
#define TURBO_FLOW_FMQ_XPUB FLOWMQ_PROTOCOL_XPUB
#define TURBO_FLOW_FMQ_XSUB FLOWMQ_PROTOCOL_XSUB
#define TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE
#define TURBO_FLOW_FMQ_MAX_TOPIC_SIZE FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE

typedef flowmq_protocol_pattern_t turbo_flow_fmq_pattern_t;
typedef flowmq_protocol_frame_kind_t flow_fmq_frame_kind_t;
typedef flowmq_protocol_frame_t flow_fmq_frame_t;
typedef flowmq_protocol_heartbeat_action_t flow_fmq_heartbeat_action_t;
typedef flowmq_protocol_heartbeat_deadlines_t flow_fmq_heartbeat_deadlines_t;

#define flow_fmq_heartbeat_deadlines_init flowmq_protocol_heartbeat_deadlines_init
#define flow_fmq_heartbeat_deadlines_on_receive flowmq_protocol_heartbeat_deadlines_on_receive
#define flow_fmq_heartbeat_deadlines_on_ping flowmq_protocol_heartbeat_deadlines_on_ping
#define flow_fmq_heartbeat_deadlines_next flowmq_protocol_heartbeat_deadlines_next
#define flow_fmq_encode_frame flowmq_protocol_encode_frame
#define flow_fmq_decode_frame flowmq_protocol_decode_frame
#define flow_fmq_encoded_topic flowmq_protocol_encoded_topic
#define flow_fmq_frame_cleanup flowmq_protocol_frame_cleanup
#define flow_fmq_encoded_size_limit flowmq_protocol_encoded_size_limit
#define flow_fmq_encoded_size flowmq_protocol_encoded_size

static int flow_fmq_is_esb_pattern(flowmq_protocol_pattern_t pattern) {
  return pattern >= FLOWMQ_PROTOCOL_ESB_PATTERN_MIN &&
         pattern <= FLOWMQ_PROTOCOL_ESB_PATTERN_MAX;
}

static int flow_fmq_is_esb_kind(flowmq_protocol_frame_kind_t kind) {
  return kind >= FLOWMQ_PROTOCOL_ESB_FRAME_MIN && kind <= FLOWMQ_PROTOCOL_ESB_FRAME_MAX;
}

static void flow_fmq_write_u16(unsigned char *out, uint16_t value) {
  out[0] = (unsigned char)(value >> 8);
  out[1] = (unsigned char)value;
}

static void flow_fmq_write_u32(unsigned char *out, uint32_t value) {
  out[0] = (unsigned char)(value >> 24);
  out[1] = (unsigned char)(value >> 16);
  out[2] = (unsigned char)(value >> 8);
  out[3] = (unsigned char)value;
}

static uint16_t flow_fmq_read_u16(const unsigned char *data) {
  return (uint16_t)(((uint16_t)data[0] << 8) | data[1]);
}

static uint32_t flow_fmq_read_u32(const unsigned char *data) {
  return ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) | ((uint32_t)data[2] << 8) | data[3];
}

static uint32_t flow_fmq_read_u32_le(const unsigned char *data) {
  return (uint32_t)data[0] | ((uint32_t)data[1] << 8u) | ((uint32_t)data[2] << 16u) |
         ((uint32_t)data[3] << 24u);
}

static int flow_fmq_decode_esb_trailing_payload(const char *data, size_t data_len, size_t *used) {
  const unsigned char *cursor = (const unsigned char *)data;
  size_t offset = 0u;
  if (!used) return TURBO_EINVAL;
  *used = 0u;
  if (!data || data_len == 0u) return TURBO_OK;
  while (offset < data_len) {
    size_t tlv_len = 0u;
    if (data_len - offset < 5u) return FLOWMQ_PROTOCOL_INCOMPLETE;
    tlv_len = (size_t)flow_fmq_read_u32_le(cursor + offset + 1u);
    if (offset + 5u + tlv_len > data_len) return FLOWMQ_PROTOCOL_INCOMPLETE;
    offset += 5u + tlv_len;
  }
  *used = offset;
  return TURBO_OK;
}

static int flow_fmq_security_view_validate(const flowmq_protocol_security_t *security) {
  if (!security) return TURBO_EINVAL;
  if ((security->identity.len > 0u && !security->identity.data) ||
      (security->method.len > 0u && !security->method.data) ||
      (security->secret.len > 0u && !security->secret.data) ||
      (security->channel_binding.len > 0u && !security->channel_binding.data)) {
    return TURBO_EINVAL;
  }
  if (security->identity.len > FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE ||
      security->method.len > FLOWMQ_PROTOCOL_MAX_AUTH_METHOD_SIZE ||
      security->secret.len > FLOWMQ_PROTOCOL_MAX_AUTH_SECRET_SIZE) {
    return TURBO_EMSGSIZE;
  }
  if ((security->identity.len != 0u &&
       memchr(security->identity.data, '\0', security->identity.len) != NULL) ||
      (security->method.len != 0u &&
       memchr(security->method.data, '\0', security->method.len) != NULL)) {
    return TURBO_EPROTO;
  }
  switch (security->mode) {
  case FLOWMQ_PROTOCOL_SECURITY_NONE:
    return security->identity.len == 0u && security->method.len == 0u &&
                   security->secret.len == 0u && security->channel_binding.len == 0u
               ? TURBO_OK
               : TURBO_EPROTO;
  case FLOWMQ_PROTOCOL_SECURITY_AUTH:
    return security->identity.len != 0u && security->method.len != 0u &&
                   security->secret.len != 0u &&
                   (security->channel_binding.len == 0u ||
                    security->channel_binding.len == FLOWMQ_PROTOCOL_CHANNEL_BINDING_SIZE)
               ? TURBO_OK
               : TURBO_EPROTO;
  case FLOWMQ_PROTOCOL_SECURITY_ACCEPTED:
    return security->identity.len == 0u && security->method.len == 0u &&
                   security->secret.len == 0u &&
                   (security->channel_binding.len == 0u ||
                    security->channel_binding.len == FLOWMQ_PROTOCOL_CHANNEL_BINDING_SIZE)
               ? TURBO_OK
               : TURBO_EPROTO;
  default:
    return TURBO_EPROTO;
  }
}

int flowmq_protocol_security_encode(const flowmq_protocol_security_t *security, tstr *payload) {
  size_t total;
  unsigned char *header;
  size_t offset;
  int rc;
  if (!payload || *payload) return TURBO_EINVAL;
  rc = flow_fmq_security_view_validate(security);
  if (rc != TURBO_OK) return rc;
  if (security->mode == FLOWMQ_PROTOCOL_SECURITY_NONE) {
    *payload = tstr_new_len(NULL, 0u);
    return *payload ? TURBO_OK : TURBO_ENOMEM;
  }
  total = FLOW_FMQ_SECURITY_HEADER_SIZE + security->identity.len + security->method.len +
          security->channel_binding.len + security->secret.len;
  *payload = tstr_new_len(NULL, total);
  if (!*payload) return TURBO_ENOMEM;
  header = (unsigned char *)*payload;
  memcpy(header, FLOW_FMQ_SECURITY_MAGIC, sizeof(FLOW_FMQ_SECURITY_MAGIC));
  header[4] = (unsigned char)security->mode;
  header[5] = (unsigned char)security->identity.len;
  header[6] = (unsigned char)security->method.len;
  header[7] = (unsigned char)security->channel_binding.len;
  flow_fmq_write_u32(header + 8u, (uint32_t)security->secret.len);
  offset = FLOW_FMQ_SECURITY_HEADER_SIZE;
  if (security->identity.len != 0u) {
    memcpy(*payload + offset, security->identity.data, security->identity.len);
    offset += security->identity.len;
  }
  if (security->method.len != 0u) {
    memcpy(*payload + offset, security->method.data, security->method.len);
    offset += security->method.len;
  }
  memcpy(*payload + offset, security->channel_binding.data, security->channel_binding.len);
  offset += security->channel_binding.len;
  if (security->secret.len != 0u) memcpy(*payload + offset, security->secret.data, security->secret.len);
  return TURBO_OK;
}

int flowmq_protocol_security_decode(vstr payload, flowmq_protocol_security_t *security) {
  const unsigned char *header = (const unsigned char *)payload.data;
  size_t identity_len;
  size_t method_len;
  size_t binding_len;
  size_t secret_len;
  size_t total;
  size_t offset;
  int rc;
  if (!security || (payload.len > 0u && !payload.data)) return TURBO_EINVAL;
  memset(security, 0, sizeof(*security));
  if (payload.len == 0u) return TURBO_OK;
  if (payload.len < FLOW_FMQ_SECURITY_HEADER_SIZE ||
      memcmp(header, FLOW_FMQ_SECURITY_MAGIC, sizeof(FLOW_FMQ_SECURITY_MAGIC)) != 0) {
    return TURBO_EPROTO;
  }
  security->mode = (flowmq_protocol_security_mode_t)header[4];
  if (security->mode == FLOWMQ_PROTOCOL_SECURITY_NONE) return TURBO_EPROTO;
  identity_len = header[5];
  method_len = header[6];
  binding_len = header[7];
  secret_len = flow_fmq_read_u32(header + 8u);
  if (identity_len > SIZE_MAX - method_len || identity_len + method_len > SIZE_MAX - binding_len ||
      identity_len + method_len + binding_len > SIZE_MAX - secret_len ||
      FLOW_FMQ_SECURITY_HEADER_SIZE >
          SIZE_MAX - (identity_len + method_len + binding_len + secret_len)) {
    return TURBO_ERANGE;
  }
  total = FLOW_FMQ_SECURITY_HEADER_SIZE + identity_len + method_len + binding_len + secret_len;
  if (total != payload.len) return TURBO_EPROTO;
  offset = FLOW_FMQ_SECURITY_HEADER_SIZE;
  security->identity = vstr_from_buf(payload.data + offset, identity_len);
  offset += identity_len;
  security->method = vstr_from_buf(payload.data + offset, method_len);
  offset += method_len;
  security->channel_binding = vstr_from_buf(payload.data + offset, binding_len);
  offset += binding_len;
  security->secret = vstr_from_buf(payload.data + offset, secret_len);
  rc = flow_fmq_security_view_validate(security);
  if (rc != TURBO_OK) memset(security, 0, sizeof(*security));
  return rc;
}

static uint64_t flow_fmq_deadline_duration_ns(uint64_t duration_ms) {
  return duration_ms > UINT64_MAX / UINT64_C(1000000) ? UINT64_MAX
                                                      : duration_ms * UINT64_C(1000000);
}

static uint64_t flow_fmq_deadline_add(uint64_t now_ns, uint64_t duration_ns) {
  return duration_ns == UINT64_MAX || now_ns > UINT64_MAX - duration_ns ? UINT64_MAX
                                                                        : now_ns + duration_ns;
}

void flow_fmq_heartbeat_deadlines_init(flow_fmq_heartbeat_deadlines_t *state, uint64_t now_ns,
                                       uint64_t interval_ms, uint64_t timeout_ms,
                                       uint64_t recv_timeout_ms) {
  if (!state) return;
  memset(state, 0, sizeof(*state));
  state->interval_ns = flow_fmq_deadline_duration_ns(interval_ms);
  state->timeout_ns = flow_fmq_deadline_duration_ns(timeout_ms);
  state->recv_timeout_ns =
      recv_timeout_ms == 0u ? UINT64_MAX : flow_fmq_deadline_duration_ns(recv_timeout_ms);
  flow_fmq_heartbeat_deadlines_on_receive(state, now_ns);
}

void flow_fmq_heartbeat_deadlines_on_receive(flow_fmq_heartbeat_deadlines_t *state,
                                             uint64_t now_ns) {
  if (!state) return;
  state->next_ping_ns = flow_fmq_deadline_add(now_ns, state->interval_ns);
  state->heartbeat_deadline_ns = flow_fmq_deadline_add(now_ns, state->timeout_ns);
  state->recv_deadline_ns = flow_fmq_deadline_add(now_ns, state->recv_timeout_ns);
}

void flow_fmq_heartbeat_deadlines_on_ping(flow_fmq_heartbeat_deadlines_t *state, uint64_t now_ns) {
  if (!state) return;
  state->next_ping_ns = flow_fmq_deadline_add(now_ns, state->interval_ns);
}

flow_fmq_heartbeat_action_t
flow_fmq_heartbeat_deadlines_next(const flow_fmq_heartbeat_deadlines_t *state, uint64_t now_ns,
                                  uint64_t *wait_deadline_ns) {
  uint64_t deadline;
  if (!state || !wait_deadline_ns || state->interval_ns == 0u || state->timeout_ns == 0u) {
    return FLOW_FMQ_HEARTBEAT_RECV_EXPIRED;
  }
  *wait_deadline_ns = 0u;
  if (state->heartbeat_deadline_ns <= now_ns) return FLOW_FMQ_HEARTBEAT_EXPIRED;
  if (state->recv_deadline_ns <= now_ns) return FLOW_FMQ_HEARTBEAT_RECV_EXPIRED;
  if (state->next_ping_ns <= now_ns) return FLOW_FMQ_HEARTBEAT_SEND_PING;
  deadline = state->next_ping_ns;
  if (state->heartbeat_deadline_ns < deadline) deadline = state->heartbeat_deadline_ns;
  if (state->recv_deadline_ns < deadline) deadline = state->recv_deadline_ns;
  *wait_deadline_ns = deadline;
  return FLOW_FMQ_HEARTBEAT_WAIT;
}

static int flow_fmq_frame_lengths(const flow_fmq_frame_t *frame, size_t max_frame_size,
                                  size_t *total) {
  size_t body;
  size_t packet_count;
  if (!frame || !total || frame->kind < FLOW_FMQ_FRAME_HELLO ||
      (frame->kind > FLOW_FMQ_FRAME_UNSUBSCRIBE && !flow_fmq_is_esb_kind(frame->kind)) ||
      frame->pattern < TURBO_FLOW_FMQ_PUB ||
      (frame->pattern > TURBO_FLOW_FMQ_XSUB && !flow_fmq_is_esb_pattern(frame->pattern))) {
    return TURBO_EINVAL;
  }
  if ((frame->identity.len > 0 && !frame->identity.data) ||
      (frame->topic.len > 0 && !frame->topic.data) ||
      (frame->payload.len > 0 && !frame->payload.data)) {
    return TURBO_EINVAL;
  }
  if (frame->identity.len > TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE ||
      frame->topic.len > TURBO_FLOW_FMQ_MAX_TOPIC_SIZE || frame->payload.len > UINT32_MAX) {
    return TURBO_EMSGSIZE;
  }
  if (frame->identity.len > SIZE_MAX - frame->topic.len ||
      frame->identity.len + frame->topic.len > SIZE_MAX - frame->payload.len) {
    return TURBO_ERANGE;
  }
  if ((frame->kind == FLOW_FMQ_FRAME_PING || frame->kind == FLOW_FMQ_FRAME_PONG) &&
      (frame->identity.len != 0 || frame->topic.len != 0 || frame->payload.len != 0)) {
    return TURBO_EPROTO;
  }
  if ((frame->kind == FLOW_FMQ_FRAME_SUBSCRIBE || frame->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) &&
      (frame->identity.len != 0 || frame->payload.len != 0)) {
    return TURBO_EPROTO;
  }
  if ((frame->kind == FLOW_FMQ_FRAME_SUBSCRIBE || frame->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) &&
      frame->pattern != TURBO_FLOW_FMQ_SUB && frame->pattern != TURBO_FLOW_FMQ_XSUB) {
    return TURBO_EPROTO;
  }
  if (frame->kind == FLOW_FMQ_FRAME_HELLO) {
    flowmq_protocol_security_t security;
    int security_rc = flowmq_protocol_security_decode(frame->payload, &security);
    if (security_rc != TURBO_OK) return security_rc;
  }
  if ((frame->kind == FLOW_FMQ_FRAME_DATA || flow_fmq_is_esb_kind(frame->kind)) &&
      frame->message_id == 0u) {
    return TURBO_EPROTO;
  }
  if (frame->kind != FLOW_FMQ_FRAME_DATA && !flow_fmq_is_esb_kind(frame->kind) &&
      frame->message_id != 0u) {
    return TURBO_EPROTO;
  }
  body = frame->identity.len + frame->topic.len + frame->payload.len;
  if (body > max_frame_size) return TURBO_EMSGSIZE;
  packet_count = frame->kind == FLOW_FMQ_FRAME_DATA && frame->payload.len > 0
                     ? (frame->payload.len - 1u) / FLOW_FMQ_PACKET_PAYLOAD_SIZE + 1u
                     : 1u;
  if (packet_count > (SIZE_MAX - body) / FLOW_FMQ_HEADER_SIZE) return TURBO_ERANGE;
  *total = body + packet_count * FLOW_FMQ_HEADER_SIZE;
  return TURBO_OK;
}

int flow_fmq_encoded_size(const flow_fmq_frame_t *frame, size_t max_frame_size, size_t *size) {
  return flow_fmq_frame_lengths(frame, max_frame_size, size);
}

static void flow_fmq_write_u64(unsigned char *dst, uint64_t value) {
  for (unsigned int i = 0; i < 8u; ++i)
    dst[i] = (unsigned char)(value >> (56u - i * 8u));
}

static uint64_t flow_fmq_read_u64(const unsigned char *src) {
  uint64_t value = 0u;
  for (unsigned int i = 0; i < 8u; ++i)
    value = (value << 8u) | src[i];
  return value;
}

static void flow_fmq_write_packet_header(unsigned char *header,
                                         const flow_fmq_frame_t *frame, uint8_t flags,
                                         uint16_t identity_len, uint16_t topic_len,
                                         size_t chunk_len, size_t payload_offset) {
  memcpy(header, FLOW_FMQ_MAGIC, sizeof(FLOW_FMQ_MAGIC));
  header[4] = FLOW_FMQ_PROTOCOL_VERSION;
  header[5] = (unsigned char)frame->kind;
  header[6] = (unsigned char)frame->pattern;
  header[7] = flags;
  flow_fmq_write_u16(header + 8, identity_len);
  flow_fmq_write_u16(header + 10, topic_len);
  flow_fmq_write_u32(header + 12, (uint32_t)chunk_len);
  flow_fmq_write_u64(header + 16, frame->message_id);
  flow_fmq_write_u32(header + 24, (uint32_t)frame->payload.len);
  flow_fmq_write_u32(header + 28, (uint32_t)payload_offset);
}

int flow_fmq_encoded_size_limit(size_t max_frame_size, size_t *limit) {
  size_t packet_count;
  if (!limit || max_frame_size == 0u) return TURBO_EINVAL;
  packet_count = (max_frame_size - 1u) / FLOW_FMQ_PACKET_PAYLOAD_SIZE + 1u;
  if (packet_count > (SIZE_MAX - max_frame_size) / FLOW_FMQ_HEADER_SIZE) return TURBO_ERANGE;
  *limit = max_frame_size + packet_count * FLOW_FMQ_HEADER_SIZE;
  return TURBO_OK;
}

static void flow_fmq_encode_frame_bytes(const flow_fmq_frame_t *frame, unsigned char *out) {
  size_t encoded_offset = 0u;
  size_t payload_offset = 0u;
  do {
    unsigned char *header = out + encoded_offset;
    size_t chunk_len = frame->payload.len - payload_offset;
    uint8_t flags = payload_offset == 0u ? FLOW_FMQ_PACKET_FIRST : 0u;
    uint16_t identity_len = payload_offset == 0u ? (uint16_t)frame->identity.len : 0u;
    uint16_t topic_len = payload_offset == 0u ? (uint16_t)frame->topic.len : 0u;
    if (chunk_len > FLOW_FMQ_PACKET_PAYLOAD_SIZE) chunk_len = FLOW_FMQ_PACKET_PAYLOAD_SIZE;
    if (payload_offset + chunk_len == frame->payload.len) flags |= FLOW_FMQ_PACKET_LAST;
    flow_fmq_write_packet_header(header, frame, flags, identity_len, topic_len, chunk_len,
                                 payload_offset);
    encoded_offset += FLOW_FMQ_HEADER_SIZE;
    if (identity_len > 0u) {
      memcpy(out + encoded_offset, frame->identity.data, identity_len);
      encoded_offset += identity_len;
    }
    if (topic_len > 0u) {
      memcpy(out + encoded_offset, frame->topic.data, topic_len);
      encoded_offset += topic_len;
    }
    if (chunk_len > 0u) {
      memcpy(out + encoded_offset, frame->payload.data + payload_offset, chunk_len);
      encoded_offset += chunk_len;
      payload_offset += chunk_len;
    }
  } while (payload_offset < frame->payload.len);
}

int flow_fmq_encode_frame(const flow_fmq_frame_t *frame, size_t max_frame_size, tstr *out) {
  size_t total;
  int rc;
  if (!out || *out) return TURBO_EINVAL;
  rc = flow_fmq_frame_lengths(frame, max_frame_size, &total);
  if (rc != TURBO_OK) return rc;
  *out = tstr_new_len(NULL, total);
  if (!*out) return TURBO_ENOMEM;
  flow_fmq_encode_frame_bytes(frame, (unsigned char *)*out);
  return TURBO_OK;
}

int flowmq_protocol_encode_frame_into_internal(
    const flowmq_protocol_frame_t *frame, size_t max_frame_size, void *storage,
    size_t storage_size, size_t *encoded_size) {
  size_t total;
  int rc;
  if (!storage || !encoded_size) return TURBO_EINVAL;
  rc = flow_fmq_frame_lengths(frame, max_frame_size, &total);
  if (rc != TURBO_OK) return rc;
  *encoded_size = total;
  if (storage_size < total) return TURBO_ENOSPC;
  flow_fmq_encode_frame_bytes(frame, (unsigned char *)storage);
  return TURBO_OK;
}

int flowmq_protocol_encode_frame_segmented(const flowmq_protocol_frame_t *frame,
                                           size_t max_frame_size,
                                           flowmq_protocol_segmented_frame_t *out) {
  flowmq_protocol_segment_t *segments;
  unsigned char *framing;
  size_t encoded_size;
  size_t packet_count;
  size_t segment_count;
  size_t segment_bytes;
  size_t framing_size;
  size_t allocation_size;
  size_t framing_offset = 0u;
  size_t payload_offset = 0u;
  size_t segment_index = 0u;
  void *storage;
  int rc;

  if (!out || out->size != sizeof(*out) || out->segments || out->segment_count != 0u ||
      out->encoded_size != 0u || out->storage) {
    return TURBO_EINVAL;
  }
  rc = flow_fmq_frame_lengths(frame, max_frame_size, &encoded_size);
  if (rc != TURBO_OK) return rc;
  packet_count = frame->kind == FLOW_FMQ_FRAME_DATA && frame->payload.len > 0u
                     ? (frame->payload.len - 1u) / FLOW_FMQ_PACKET_PAYLOAD_SIZE + 1u
                     : 1u;
  if (frame->payload.len > 0u && packet_count > SIZE_MAX / 2u) return TURBO_ERANGE;
  segment_count = frame->payload.len > 0u ? packet_count * 2u : 1u;
  if (segment_count > SIZE_MAX / sizeof(*segments)) return TURBO_ERANGE;
  segment_bytes = segment_count * sizeof(*segments);
  if (packet_count > (SIZE_MAX - frame->identity.len - frame->topic.len) /
                         FLOW_FMQ_HEADER_SIZE) {
    return TURBO_ERANGE;
  }
  framing_size = packet_count * FLOW_FMQ_HEADER_SIZE + frame->identity.len + frame->topic.len;
  if (segment_bytes > SIZE_MAX - framing_size) return TURBO_ERANGE;
  allocation_size = segment_bytes + framing_size;
  storage = mem_alloc(mem_global(), allocation_size);
  if (!storage) return TURBO_ENOMEM;
  segments = (flowmq_protocol_segment_t *)storage;
  framing = (unsigned char *)storage + segment_bytes;

  do {
    unsigned char *header = framing + framing_offset;
    size_t chunk_len = frame->payload.len - payload_offset;
    const uint16_t identity_len = payload_offset == 0u ? (uint16_t)frame->identity.len : 0u;
    const uint16_t topic_len = payload_offset == 0u ? (uint16_t)frame->topic.len : 0u;
    uint8_t flags = payload_offset == 0u ? FLOW_FMQ_PACKET_FIRST : 0u;
    size_t header_segment_size = FLOW_FMQ_HEADER_SIZE;
    if (chunk_len > FLOW_FMQ_PACKET_PAYLOAD_SIZE) chunk_len = FLOW_FMQ_PACKET_PAYLOAD_SIZE;
    if (payload_offset + chunk_len == frame->payload.len) flags |= FLOW_FMQ_PACKET_LAST;
    flow_fmq_write_packet_header(header, frame, flags, identity_len, topic_len, chunk_len,
                                 payload_offset);
    framing_offset += FLOW_FMQ_HEADER_SIZE;
    if (identity_len > 0u) {
      memcpy(framing + framing_offset, frame->identity.data, identity_len);
      framing_offset += identity_len;
      header_segment_size += identity_len;
    }
    if (topic_len > 0u) {
      memcpy(framing + framing_offset, frame->topic.data, topic_len);
      framing_offset += topic_len;
      header_segment_size += topic_len;
    }
    segments[segment_index].data = header;
    segments[segment_index].size = header_segment_size;
    segment_index += 1u;
    if (chunk_len > 0u) {
      segments[segment_index].data = frame->payload.data + payload_offset;
      segments[segment_index].size = chunk_len;
      segment_index += 1u;
      payload_offset += chunk_len;
    }
  } while (payload_offset < frame->payload.len);

  out->segments = segments;
  out->segment_count = segment_index;
  out->encoded_size = encoded_size;
  out->storage = storage;
  return TURBO_OK;
}

void flowmq_protocol_segmented_frame_cleanup(flowmq_protocol_segmented_frame_t *frame) {
  if (!frame) return;
  mem_free(mem_global(), frame->storage);
  *frame = (flowmq_protocol_segmented_frame_t)FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
}

typedef struct flow_fmq_packet_s {
  flow_fmq_frame_kind_t kind;
  turbo_flow_fmq_pattern_t pattern;
  uint8_t flags;
  uint16_t identity_len;
  uint16_t topic_len;
  uint32_t chunk_len;
  uint64_t message_id;
  uint32_t payload_len;
  uint32_t payload_offset;
  size_t record_len;
} flow_fmq_packet_t;

static int flow_fmq_decode_packet(const char *data, size_t data_len, flow_fmq_packet_t *packet) {
  const unsigned char *header = (const unsigned char *)data;
  size_t body_len;
  if (!packet || (data_len > 0u && !data)) return TURBO_EINVAL;
  if (data_len >= sizeof(FLOW_FMQ_MAGIC) &&
      memcmp(header, FLOW_FMQ_MAGIC, sizeof(FLOW_FMQ_MAGIC)) != 0)
    return TURBO_EPROTO;
  if (data_len >= 5u && header[4] != FLOW_FMQ_PROTOCOL_VERSION) return TURBO_EPROTO;
  if (data_len < FLOW_FMQ_HEADER_SIZE) return FLOW_FMQ_INCOMPLETE;
  if ((header[7] & ~(FLOW_FMQ_PACKET_FIRST | FLOW_FMQ_PACKET_LAST)) != 0u) return TURBO_EPROTO;
  if (header[5] < FLOW_FMQ_FRAME_HELLO ||
      (header[5] > FLOW_FMQ_FRAME_UNSUBSCRIBE &&
       !flow_fmq_is_esb_kind((flowmq_protocol_frame_kind_t)header[5])) ||
      header[6] < TURBO_FLOW_FMQ_PUB ||
      (header[6] > TURBO_FLOW_FMQ_XSUB && !flow_fmq_is_esb_pattern(header[6])))
    return TURBO_EPROTO;
  memset(packet, 0, sizeof(*packet));
  packet->kind = (flow_fmq_frame_kind_t)header[5];
  packet->pattern = (turbo_flow_fmq_pattern_t)header[6];
  packet->flags = header[7];
  packet->identity_len = flow_fmq_read_u16(header + 8);
  packet->topic_len = flow_fmq_read_u16(header + 10);
  packet->chunk_len = flow_fmq_read_u32(header + 12);
  packet->message_id = flow_fmq_read_u64(header + 16);
  packet->payload_len = flow_fmq_read_u32(header + 24);
  packet->payload_offset = flow_fmq_read_u32(header + 28);
  if (packet->identity_len > TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE ||
      packet->topic_len > TURBO_FLOW_FMQ_MAX_TOPIC_SIZE ||
      packet->chunk_len > FLOW_FMQ_PACKET_PAYLOAD_SIZE)
    return TURBO_EMSGSIZE;
  body_len = (size_t)packet->identity_len + packet->topic_len + packet->chunk_len;
  if (body_len > SIZE_MAX - FLOW_FMQ_HEADER_SIZE) return TURBO_ERANGE;
  packet->record_len = FLOW_FMQ_HEADER_SIZE + body_len;
  return data_len < packet->record_len ? FLOW_FMQ_INCOMPLETE : TURBO_OK;
}

int flow_fmq_encoded_topic(const char *data, size_t data_len, size_t max_frame_size,
                           vstr *topic) {
  flow_fmq_packet_t packet;
  int rc;
  if (!topic) return TURBO_EINVAL;
  *topic = (vstr){0};
  rc = flow_fmq_decode_packet(data, data_len, &packet);
  if (rc != TURBO_OK) return rc;
  if ((packet.flags & FLOW_FMQ_PACKET_FIRST) == 0u || packet.payload_offset != 0u ||
      (size_t)packet.identity_len + packet.topic_len + packet.payload_len > max_frame_size) {
    return TURBO_EPROTO;
  }
  if (packet.kind == FLOW_FMQ_FRAME_DATA || flow_fmq_is_esb_kind(packet.kind)) {
    if (packet.message_id == 0u) return TURBO_EPROTO;
  } else if (packet.message_id != 0u) {
    return TURBO_EPROTO;
  }
  *topic = vstr_from_buf(data + FLOW_FMQ_HEADER_SIZE + packet.identity_len, packet.topic_len);
  return TURBO_OK;
}

int flow_fmq_decode_frame(const char *data, size_t data_len, size_t max_frame_size,
                          flow_fmq_frame_t *out, size_t *consumed) {
  flow_fmq_packet_t first;
  size_t cursor = 0u;
  size_t payload_copied = 0u;
  size_t packet_count = 0u;
  int rc;
  if (!out || !consumed || (data_len > 0 && !data)) return TURBO_EINVAL;
  *consumed = 0;
  memset(out, 0, sizeof(*out));
  rc = flow_fmq_decode_packet(data, data_len, &first);
  if (rc != TURBO_OK) return rc;
  if ((first.flags & FLOW_FMQ_PACKET_FIRST) == 0u || first.payload_offset != 0u)
    return TURBO_EPROTO;
  if ((size_t)first.identity_len + first.topic_len + first.payload_len > max_frame_size)
    return TURBO_EMSGSIZE;
  if (first.kind == FLOW_FMQ_FRAME_DATA || flow_fmq_is_esb_kind(first.kind)) {
    if (first.message_id == 0u) return TURBO_EPROTO;
  } else if (first.message_id != 0u || first.payload_len != first.chunk_len ||
             (first.flags & FLOW_FMQ_PACKET_LAST) == 0u) {
    return TURBO_EPROTO;
  }
  for (;;) {
    flow_fmq_packet_t packet;
    rc = flow_fmq_decode_packet(data + cursor, data_len - cursor, &packet);
    if (rc != TURBO_OK) return rc;
    if (packet.kind != first.kind || packet.pattern != first.pattern ||
        packet.message_id != first.message_id || packet.payload_len != first.payload_len ||
        packet.payload_offset != payload_copied)
      return TURBO_EPROTO;
    if (packet_count == 0u) {
      if ((packet.flags & FLOW_FMQ_PACKET_FIRST) == 0u) return TURBO_EPROTO;
    } else if ((packet.flags & FLOW_FMQ_PACKET_FIRST) != 0u || packet.identity_len != 0u ||
               packet.topic_len != 0u)
      return TURBO_EPROTO;
    if ((size_t)packet.chunk_len > first.payload_len - payload_copied) return TURBO_EPROTO;
    payload_copied += packet.chunk_len;
    cursor += packet.record_len;
    packet_count += 1u;
    if ((packet.flags & FLOW_FMQ_PACKET_LAST) != 0u) break;
    if (packet.chunk_len == 0u || payload_copied == first.payload_len) return TURBO_EPROTO;
  }
  if (payload_copied != first.payload_len) return TURBO_EPROTO;

  size_t payload_cursor = cursor;
  size_t extension_len = 0u;
  size_t base_payload_len = first.payload_len;
  if (flow_fmq_is_esb_kind(first.kind)) {
    rc = flow_fmq_decode_esb_trailing_payload(data + cursor, data_len - cursor, &extension_len);
    if (rc != TURBO_OK) return rc;
    if ((size_t)first.identity_len + first.topic_len + base_payload_len + extension_len > max_frame_size)
      return TURBO_EMSGSIZE;
  }
  if (base_payload_len > SIZE_MAX - extension_len) return TURBO_ERANGE;
  size_t total_payload_len = base_payload_len + extension_len;
  cursor += extension_len;

  out->kind = first.kind;
  out->pattern = first.pattern;
  out->message_id = first.message_id;
  out->identity = vstr_from_buf(data + FLOW_FMQ_HEADER_SIZE, first.identity_len);
  out->topic = vstr_from_buf(data + FLOW_FMQ_HEADER_SIZE + first.identity_len, first.topic_len);
  if (packet_count == 1u && extension_len == 0u) {
    out->payload = vstr_from_buf(
        data + FLOW_FMQ_HEADER_SIZE + first.identity_len + first.topic_len, base_payload_len);
  } else {
    size_t read_cursor = 0u;
    size_t write_offset = 0u;
    out->owned_payload = tstr_new_len(NULL, total_payload_len);
    if (!out->owned_payload) return TURBO_ENOMEM;
    while (read_cursor < payload_cursor) {
      flow_fmq_packet_t packet;
      rc = flow_fmq_decode_packet(data + read_cursor, payload_cursor - read_cursor, &packet);
      if (rc != TURBO_OK) {
        flow_fmq_frame_cleanup(out);
        return rc;
      }
      if (packet.chunk_len > 0u) {
        memcpy(out->owned_payload + write_offset,
               data + read_cursor + FLOW_FMQ_HEADER_SIZE + packet.identity_len + packet.topic_len,
               packet.chunk_len);
        write_offset += packet.chunk_len;
      }
      read_cursor += packet.record_len;
    }
    if (extension_len > 0u) {
      memcpy(out->owned_payload + write_offset, data + payload_cursor, extension_len);
    }
    out->payload = vstr_from_buf((const char *)out->owned_payload, base_payload_len);
  }
  if ((out->kind == FLOW_FMQ_FRAME_PING || out->kind == FLOW_FMQ_FRAME_PONG) &&
      (out->identity.len != 0 || out->topic.len != 0 || out->payload.len != 0)) {
    flow_fmq_frame_cleanup(out);
    return TURBO_EPROTO;
  }
  if ((out->kind == FLOW_FMQ_FRAME_SUBSCRIBE || out->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) &&
      (out->identity.len != 0 || out->payload.len != 0)) {
    flow_fmq_frame_cleanup(out);
    return TURBO_EPROTO;
  }
  if (out->kind == FLOW_FMQ_FRAME_HELLO) {
    flowmq_protocol_security_t security;
    rc = flowmq_protocol_security_decode(out->payload, &security);
    if (rc != TURBO_OK) {
      flow_fmq_frame_cleanup(out);
      return rc;
    }
  }
  if ((out->kind == FLOW_FMQ_FRAME_SUBSCRIBE || out->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) &&
      out->pattern != TURBO_FLOW_FMQ_SUB && out->pattern != TURBO_FLOW_FMQ_XSUB) {
    flow_fmq_frame_cleanup(out);
    return TURBO_EPROTO;
  }
  *consumed = cursor;
  return TURBO_OK;
}

void flow_fmq_frame_cleanup(flow_fmq_frame_t *frame) {
  if (!frame) return;
  tstr_freep(&frame->owned_payload);
  memset(frame, 0, sizeof(*frame));
}
