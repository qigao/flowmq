#include "flowmq_protocol.h"
#include "flowmq_protocol_control_schema.h"
#include "flowmq_protocol_internal.h"
#include "flowmq_security.h"

#include "salts_buffer.h"
#include "salts_error.h"
#include "salts_str.h"

#include <limits.h>
#include <string.h>

static const unsigned char FLOWMQ_PROTOCOL_MAGIC[4] = {'T', 'F', 'M', 'Q'};

static int flowmq_protocol_is_core_kind(flowmq_protocol_frame_kind_t kind) {
  return (kind >= FLOWMQ_PROTOCOL_FRAME_HELLO && kind <= FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE) ||
         kind == FLOWMQ_PROTOCOL_FRAME_SETTINGS || kind == FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE;
}

static void flowmq_protocol_write_u16(unsigned char *out, uint16_t value) {
  out[0] = (unsigned char)(value >> 8);
  out[1] = (unsigned char)value;
}

static void flowmq_protocol_write_u32(unsigned char *out, uint32_t value) {
  out[0] = (unsigned char)(value >> 24);
  out[1] = (unsigned char)(value >> 16);
  out[2] = (unsigned char)(value >> 8);
  out[3] = (unsigned char)value;
}

static uint16_t flowmq_protocol_read_u16(const unsigned char *data) {
  return (uint16_t)(((uint16_t)data[0] << 8) | data[1]);
}

static uint32_t flowmq_protocol_read_u32(const unsigned char *data) {
  return ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) | ((uint32_t)data[2] << 8) | data[3];
}

static uint64_t flowmq_protocol_deadline_duration_ns(uint64_t duration_ms) {
  return duration_ms > UINT64_MAX / UINT64_C(1000000) ? UINT64_MAX
                                                      : duration_ms * UINT64_C(1000000);
}

static uint64_t flowmq_protocol_deadline_add(uint64_t now_ns, uint64_t duration_ns) {
  return duration_ns == UINT64_MAX || now_ns > UINT64_MAX - duration_ns ? UINT64_MAX
                                                                        : now_ns + duration_ns;
}

void flowmq_protocol_heartbeat_deadlines_init(flowmq_protocol_heartbeat_deadlines_t *state,
                                              uint64_t now_ns, uint64_t interval_ms,
                                              uint64_t timeout_ms, uint64_t recv_timeout_ms) {
  if (!state) return;
  memset(state, 0, sizeof(*state));
  state->interval_ns = flowmq_protocol_deadline_duration_ns(interval_ms);
  state->timeout_ns = flowmq_protocol_deadline_duration_ns(timeout_ms);
  state->recv_timeout_ns =
      recv_timeout_ms == 0u ? UINT64_MAX : flowmq_protocol_deadline_duration_ns(recv_timeout_ms);
  flowmq_protocol_heartbeat_deadlines_on_receive(state, now_ns);
}

void flowmq_protocol_heartbeat_deadlines_on_receive(flowmq_protocol_heartbeat_deadlines_t *state,
                                                    uint64_t now_ns) {
  if (!state) return;
  state->next_ping_ns = flowmq_protocol_deadline_add(now_ns, state->interval_ns);
  state->heartbeat_deadline_ns = UINT64_MAX;
  state->recv_deadline_ns = flowmq_protocol_deadline_add(now_ns, state->recv_timeout_ns);
}

void flowmq_protocol_heartbeat_deadlines_on_ping(flowmq_protocol_heartbeat_deadlines_t *state,
                                                 uint64_t now_ns) {
  if (!state) return;
  state->next_ping_ns = flowmq_protocol_deadline_add(now_ns, state->interval_ns);
  if (state->heartbeat_deadline_ns == UINT64_MAX && state->timeout_ns != 0u)
    state->heartbeat_deadline_ns = flowmq_protocol_deadline_add(now_ns, state->timeout_ns);
}

flowmq_protocol_heartbeat_action_t
flowmq_protocol_heartbeat_deadlines_next(const flowmq_protocol_heartbeat_deadlines_t *state,
                                         uint64_t now_ns, uint64_t *wait_deadline_ns) {
  uint64_t deadline;
  if (!state || !wait_deadline_ns || state->interval_ns == 0u) {
    return FLOWMQ_PROTOCOL_HEARTBEAT_RECV_EXPIRED;
  }
  *wait_deadline_ns = 0u;
  if (state->heartbeat_deadline_ns <= now_ns) return FLOWMQ_PROTOCOL_HEARTBEAT_EXPIRED;
  if (state->recv_deadline_ns <= now_ns) return FLOWMQ_PROTOCOL_HEARTBEAT_RECV_EXPIRED;
  if (state->next_ping_ns <= now_ns) return FLOWMQ_PROTOCOL_HEARTBEAT_SEND_PING;
  deadline = state->next_ping_ns;
  if (state->heartbeat_deadline_ns < deadline) deadline = state->heartbeat_deadline_ns;
  if (state->recv_deadline_ns < deadline) deadline = state->recv_deadline_ns;
  *wait_deadline_ns = deadline;
  return FLOWMQ_PROTOCOL_HEARTBEAT_WAIT;
}

static int flowmq_protocol_flow_control_frame_validate(const flowmq_protocol_frame_t *frame);

static int flowmq_protocol_frame_lengths(const flowmq_protocol_frame_t *frame,
                                         size_t max_frame_size, size_t *total) {
  size_t body;
  size_t packet_count;
  if (!frame || !total || !flowmq_protocol_is_core_kind(frame->kind) ||
      frame->pattern < FLOWMQ_PROTOCOL_PUB || frame->pattern > FLOWMQ_PROTOCOL_XSUB) {
    return SALTS_EINVAL;
  }
  if ((frame->identity.len > 0 && !frame->identity.data) ||
      (frame->topic.len > 0 && !frame->topic.data) ||
      (frame->payload.len > 0 && !frame->payload.data)) {
    return SALTS_EINVAL;
  }
  if (frame->identity.len > FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE ||
      frame->topic.len > FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE || frame->payload.len > UINT32_MAX) {
    return SALTS_EMSGSIZE;
  }
  if (frame->identity.len > SIZE_MAX - frame->topic.len ||
      frame->identity.len + frame->topic.len > SIZE_MAX - frame->payload.len) {
    return SALTS_ERANGE;
  }
  if ((frame->kind == FLOWMQ_PROTOCOL_FRAME_PING || frame->kind == FLOWMQ_PROTOCOL_FRAME_PONG) &&
      (frame->identity.len != 0 || frame->topic.len != 0 || frame->payload.len != 0)) {
    return SALTS_EPROTO;
  }
  if ((frame->kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE ||
       frame->kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE) &&
      (frame->identity.len != 0 || frame->payload.len != 0)) {
    return SALTS_EPROTO;
  }
  if ((frame->kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE ||
       frame->kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE) &&
      frame->pattern != FLOWMQ_PROTOCOL_SUB && frame->pattern != FLOWMQ_PROTOCOL_XSUB) {
    return SALTS_EPROTO;
  }
  if (frame->more && frame->kind != FLOWMQ_PROTOCOL_FRAME_DATA) return SALTS_EPROTO;
  if (frame->kind == FLOWMQ_PROTOCOL_FRAME_HELLO) {
    flowmq_security_t security;
    int security_rc = flowmq_security_decode(frame->payload, &security);
    if (security_rc != SALTS_OK) return security_rc;
  }
  {
    int flow_control_rc = flowmq_protocol_flow_control_frame_validate(frame);
    if (flow_control_rc != SALTS_OK) return flow_control_rc;
  }
  if (frame->kind == FLOWMQ_PROTOCOL_FRAME_DATA && frame->message_id == 0u) {
    return SALTS_EPROTO;
  }
  if (frame->kind != FLOWMQ_PROTOCOL_FRAME_DATA && frame->message_id != 0u) {
    return SALTS_EPROTO;
  }
  body = frame->identity.len + frame->topic.len + frame->payload.len;
  if (body > max_frame_size) return SALTS_EMSGSIZE;
  packet_count = frame->kind == FLOWMQ_PROTOCOL_FRAME_DATA && frame->payload.len > 0
                     ? (frame->payload.len - 1u) / FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 1u
                     : 1u;
  if (packet_count > (SIZE_MAX - body) / FLOWMQ_PROTOCOL_HEADER_SIZE) return SALTS_ERANGE;
  *total = body + packet_count * FLOWMQ_PROTOCOL_HEADER_SIZE;
  return SALTS_OK;
}

int flowmq_protocol_encoded_size(const flowmq_protocol_frame_t *frame, size_t max_frame_size,
                                 size_t *size) {
  return flowmq_protocol_frame_lengths(frame, max_frame_size, size);
}

static void flowmq_protocol_write_u64(unsigned char *dst, uint64_t value) {
  for (unsigned int i = 0; i < 8u; ++i)
    dst[i] = (unsigned char)(value >> (56u - i * 8u));
}

static uint64_t flowmq_protocol_read_u64(const unsigned char *src) {
  uint64_t value = 0u;
  for (unsigned int i = 0; i < 8u; ++i)
    value = (value << 8u) | src[i];
  return value;
}

static int flowmq_protocol_settings_validate(const flowmq_protocol_settings_t *settings) {
  if (!settings) return SALTS_EINVAL;
  if (settings->capabilities != FLOWMQ_PROTOCOL_CAP_FLOW_CREDIT || settings->max_frame_size == 0u ||
      settings->session_generation == 0u || settings->initial_max_data == 0u ||
      settings->flow_update_quantum == 0u ||
      settings->flow_update_quantum > settings->initial_max_data ||
      settings->flow_update_interval_ms == 0u) {
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int flowmq_protocol_flow_update_validate(const flowmq_protocol_flow_update_t *update) {
  if (!update) return SALTS_EINVAL;
  if (update->session_generation == 0u || update->max_data < update->consumed_data)
    return SALTS_EPROTO;
  return SALTS_OK;
}

#define FLOWMQ_PROTOCOL_CONTROL_WRITE_U32(member, offset)                                          \
  flowmq_protocol_write_u32(payload + (offset), value->member);
#define FLOWMQ_PROTOCOL_CONTROL_WRITE_U64(member, offset)                                          \
  flowmq_protocol_write_u64(payload + (offset), value->member);
#define FLOWMQ_PROTOCOL_CONTROL_WRITE_ROW(member, width, offset)                                   \
  CMETA_PP_CAT(FLOWMQ_PROTOCOL_CONTROL_WRITE_, width)(member, offset)

#define FLOWMQ_PROTOCOL_CONTROL_READ_U32(member, offset)                                           \
  value->member = flowmq_protocol_read_u32(data + (offset));
#define FLOWMQ_PROTOCOL_CONTROL_READ_U64(member, offset)                                           \
  value->member = flowmq_protocol_read_u64(data + (offset));
#define FLOWMQ_PROTOCOL_CONTROL_READ_ROW(member, width, offset)                                    \
  CMETA_PP_CAT(FLOWMQ_PROTOCOL_CONTROL_READ_, width)(member, offset)

int flowmq_protocol_settings_encode(const flowmq_protocol_settings_t *settings,
                                    unsigned char payload[FLOWMQ_PROTOCOL_SETTINGS_PAYLOAD_SIZE]) {
  const flowmq_protocol_settings_t *value = settings;
  int status;
  if (!payload) return SALTS_EINVAL;
  status = flowmq_protocol_settings_validate(settings);
  if (status != SALTS_OK) return status;
  Replay(FLOWMQ_PROTOCOL_SETTINGS_SCHEMA, FLOWMQ_PROTOCOL_CONTROL_WRITE_ROW) return SALTS_OK;
}

int flowmq_protocol_settings_decode(vstr payload, flowmq_protocol_settings_t *settings) {
  const unsigned char *data = (const unsigned char *)payload.data;
  flowmq_protocol_settings_t *value = settings;
  if (!settings || !payload.data) return SALTS_EINVAL;
  memset(settings, 0, sizeof(*settings));
  if (payload.len != FLOWMQ_PROTOCOL_SETTINGS_PAYLOAD_SIZE) return SALTS_EPROTO;
  Replay(FLOWMQ_PROTOCOL_SETTINGS_SCHEMA,
         FLOWMQ_PROTOCOL_CONTROL_READ_ROW) return flowmq_protocol_settings_validate(settings);
}

int flowmq_protocol_flow_update_encode(
    const flowmq_protocol_flow_update_t *update,
    unsigned char payload[FLOWMQ_PROTOCOL_FLOW_UPDATE_PAYLOAD_SIZE]) {
  const flowmq_protocol_flow_update_t *value = update;
  int status;
  if (!payload) return SALTS_EINVAL;
  status = flowmq_protocol_flow_update_validate(update);
  if (status != SALTS_OK) return status;
  Replay(FLOWMQ_PROTOCOL_FLOW_UPDATE_SCHEMA, FLOWMQ_PROTOCOL_CONTROL_WRITE_ROW) return SALTS_OK;
}

int flowmq_protocol_flow_update_decode(vstr payload, flowmq_protocol_flow_update_t *update) {
  const unsigned char *data = (const unsigned char *)payload.data;
  flowmq_protocol_flow_update_t *value = update;
  if (!update || !payload.data) return SALTS_EINVAL;
  memset(update, 0, sizeof(*update));
  if (payload.len != FLOWMQ_PROTOCOL_FLOW_UPDATE_PAYLOAD_SIZE) return SALTS_EPROTO;
  Replay(FLOWMQ_PROTOCOL_FLOW_UPDATE_SCHEMA,
         FLOWMQ_PROTOCOL_CONTROL_READ_ROW) return flowmq_protocol_flow_update_validate(update);
}

#undef FLOWMQ_PROTOCOL_CONTROL_READ_ROW
#undef FLOWMQ_PROTOCOL_CONTROL_READ_U64
#undef FLOWMQ_PROTOCOL_CONTROL_READ_U32
#undef FLOWMQ_PROTOCOL_CONTROL_WRITE_ROW
#undef FLOWMQ_PROTOCOL_CONTROL_WRITE_U64
#undef FLOWMQ_PROTOCOL_CONTROL_WRITE_U32

static int flowmq_protocol_flow_control_frame_validate(const flowmq_protocol_frame_t *frame) {
  if (frame->kind != FLOWMQ_PROTOCOL_FRAME_SETTINGS &&
      frame->kind != FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE) {
    return SALTS_OK;
  }
  if (frame->identity.len != 0u || frame->topic.len != 0u || frame->message_id != 0u ||
      frame->more) {
    return SALTS_EPROTO;
  }
  if (frame->kind == FLOWMQ_PROTOCOL_FRAME_SETTINGS) {
    flowmq_protocol_settings_t settings;
    return flowmq_protocol_settings_decode(frame->payload, &settings);
  }
  {
    flowmq_protocol_flow_update_t update;
    return flowmq_protocol_flow_update_decode(frame->payload, &update);
  }
}

static void flowmq_protocol_write_packet_header(unsigned char *header,
                                                const flowmq_protocol_frame_t *frame, uint8_t flags,
                                                uint16_t identity_len, uint16_t topic_len,
                                                size_t chunk_len, size_t payload_offset) {
  memcpy(header, FLOWMQ_PROTOCOL_MAGIC, sizeof(FLOWMQ_PROTOCOL_MAGIC));
  header[4] = FLOWMQ_PROTOCOL_WIRE_VERSION;
  header[5] = (unsigned char)frame->kind;
  header[6] = (unsigned char)frame->pattern;
  header[7] = flags | (frame->more ? FLOWMQ_PROTOCOL_MESSAGE_MORE : 0u);
  flowmq_protocol_write_u16(header + 8, identity_len);
  flowmq_protocol_write_u16(header + 10, topic_len);
  flowmq_protocol_write_u32(header + 12, (uint32_t)chunk_len);
  flowmq_protocol_write_u64(header + 16, frame->message_id);
  flowmq_protocol_write_u32(header + 24, (uint32_t)frame->payload.len);
  flowmq_protocol_write_u32(header + 28, (uint32_t)payload_offset);
}

int flowmq_protocol_encoded_size_limit(size_t max_frame_size, size_t *limit) {
  size_t packet_count;
  if (!limit || max_frame_size == 0u) return SALTS_EINVAL;
  packet_count = (max_frame_size - 1u) / FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 1u;
  if (packet_count > (SIZE_MAX - max_frame_size) / FLOWMQ_PROTOCOL_HEADER_SIZE) return SALTS_ERANGE;
  *limit = max_frame_size + packet_count * FLOWMQ_PROTOCOL_HEADER_SIZE;
  return SALTS_OK;
}

static void flowmq_protocol_encode_frame_bytes(const flowmq_protocol_frame_t *frame,
                                               unsigned char *out) {
  size_t encoded_offset = 0u;
  size_t payload_offset = 0u;
  do {
    unsigned char *header = out + encoded_offset;
    size_t chunk_len = frame->payload.len - payload_offset;
    uint8_t flags = payload_offset == 0u ? FLOWMQ_PROTOCOL_PACKET_FIRST : 0u;
    uint16_t identity_len = payload_offset == 0u ? (uint16_t)frame->identity.len : 0u;
    uint16_t topic_len = payload_offset == 0u ? (uint16_t)frame->topic.len : 0u;
    if (chunk_len > FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE)
      chunk_len = FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE;
    if (payload_offset + chunk_len == frame->payload.len) flags |= FLOWMQ_PROTOCOL_PACKET_LAST;
    flowmq_protocol_write_packet_header(header, frame, flags, identity_len, topic_len, chunk_len,
                                        payload_offset);
    encoded_offset += FLOWMQ_PROTOCOL_HEADER_SIZE;
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

int flowmq_protocol_encode_frame(const flowmq_protocol_frame_t *frame, size_t max_frame_size,
                                 tstr *out) {
  size_t total;
  int rc;
  if (!out || *out) return SALTS_EINVAL;
  rc = flowmq_protocol_frame_lengths(frame, max_frame_size, &total);
  if (rc != SALTS_OK) return rc;
  *out = tstr_new_len(NULL, total);
  if (!*out) return SALTS_ENOMEM;
  flowmq_protocol_encode_frame_bytes(frame, (unsigned char *)*out);
  return SALTS_OK;
}

int flowmq_protocol_encode_frame_into_internal(const flowmq_protocol_frame_t *frame,
                                               size_t max_frame_size, void *storage,
                                               size_t storage_size, size_t *encoded_size) {
  size_t total;
  int rc;
  if (!storage || !encoded_size) return SALTS_EINVAL;
  rc = flowmq_protocol_frame_lengths(frame, max_frame_size, &total);
  if (rc != SALTS_OK) return rc;
  *encoded_size = total;
  if (storage_size < total) return SALTS_ENOSPC;
  flowmq_protocol_encode_frame_bytes(frame, (unsigned char *)storage);
  return SALTS_OK;
}

typedef struct flowmq_protocol_segmented_layout_s {
  size_t encoded_size;
  size_t packet_count;
  size_t segment_count;
  size_t framing_size;
} flowmq_protocol_segmented_layout_t;

static int flowmq_protocol_segmented_layout(
    const flowmq_protocol_frame_t *frame, size_t max_frame_size,
    flowmq_protocol_segmented_layout_t *layout) {
  int rc;
  if (!layout) return SALTS_EINVAL;
  memset(layout, 0, sizeof(*layout));
  rc = flowmq_protocol_frame_lengths(frame, max_frame_size,
                                     &layout->encoded_size);
  if (rc != SALTS_OK) return rc;
  layout->packet_count =
      frame->kind == FLOWMQ_PROTOCOL_FRAME_DATA && frame->payload.len > 0u
          ? (frame->payload.len - 1u) /
                    FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE +
                1u
          : 1u;
  if (frame->payload.len > 0u && layout->packet_count > SIZE_MAX / 2u)
    return SALTS_ERANGE;
  layout->segment_count =
      frame->payload.len > 0u ? layout->packet_count * 2u : 1u;
  if (layout->packet_count >
      (SIZE_MAX - frame->identity.len - frame->topic.len) /
          FLOWMQ_PROTOCOL_HEADER_SIZE)
    return SALTS_ERANGE;
  layout->framing_size =
      layout->packet_count * FLOWMQ_PROTOCOL_HEADER_SIZE +
      frame->identity.len + frame->topic.len;
  return SALTS_OK;
}

static void flowmq_protocol_fill_segments(
    const flowmq_protocol_frame_t *frame,
    flowmq_protocol_segment_t *segments, unsigned char *framing) {
  size_t framing_offset = 0u;
  size_t payload_offset = 0u;
  size_t segment_index = 0u;

  do {
    unsigned char *header = framing + framing_offset;
    size_t chunk_len = frame->payload.len - payload_offset;
    const uint16_t identity_len = payload_offset == 0u ? (uint16_t)frame->identity.len : 0u;
    const uint16_t topic_len = payload_offset == 0u ? (uint16_t)frame->topic.len : 0u;
    uint8_t flags = payload_offset == 0u ? FLOWMQ_PROTOCOL_PACKET_FIRST : 0u;
    size_t header_segment_size = FLOWMQ_PROTOCOL_HEADER_SIZE;
    if (chunk_len > FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE)
      chunk_len = FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE;
    if (payload_offset + chunk_len == frame->payload.len) flags |= FLOWMQ_PROTOCOL_PACKET_LAST;
    flowmq_protocol_write_packet_header(header, frame, flags, identity_len, topic_len, chunk_len,
                                        payload_offset);
    framing_offset += FLOWMQ_PROTOCOL_HEADER_SIZE;
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
}

int flowmq_protocol_encode_frame_segmented_into_internal(
    const flowmq_protocol_frame_t *frame, size_t max_frame_size,
    flowmq_protocol_segment_t *segments, size_t segment_capacity,
    void *framing, size_t framing_capacity, size_t *segment_count,
    size_t *encoded_size) {
  flowmq_protocol_segmented_layout_t layout;
  int rc;
  if (!segment_count || !encoded_size) return SALTS_EINVAL;
  *segment_count = 0u;
  *encoded_size = 0u;
  if (!segments || !framing || segment_capacity == 0u ||
      framing_capacity == 0u)
    return SALTS_EINVAL;
  rc = flowmq_protocol_segmented_layout(frame, max_frame_size, &layout);
  if (rc != SALTS_OK) return rc;
  if (segment_capacity < layout.segment_count ||
      framing_capacity < layout.framing_size)
    return SALTS_ENOSPC;
  flowmq_protocol_fill_segments(frame, segments, (unsigned char *)framing);
  *segment_count = layout.segment_count;
  *encoded_size = layout.encoded_size;
  return SALTS_OK;
}

int flowmq_protocol_encode_frame_segmented(const flowmq_protocol_frame_t *frame,
                                           size_t max_frame_size,
                                           flowmq_protocol_segmented_frame_t *out) {
  flowmq_protocol_segmented_layout_t layout;
  flowmq_protocol_segment_t *segments;
  unsigned char *framing;
  size_t segment_bytes;
  size_t allocation_size;
  void *storage;
  int rc;

  if (!out || out->size != sizeof(*out) || out->segments ||
      out->segment_count != 0u || out->encoded_size != 0u || out->storage)
    return SALTS_EINVAL;
  rc = flowmq_protocol_segmented_layout(frame, max_frame_size, &layout);
  if (rc != SALTS_OK) return rc;
  if (layout.segment_count > SIZE_MAX / sizeof(*segments)) return SALTS_ERANGE;
  segment_bytes = layout.segment_count * sizeof(*segments);
  if (segment_bytes > SIZE_MAX - layout.framing_size) return SALTS_ERANGE;
  allocation_size = segment_bytes + layout.framing_size;
  storage = mem_alloc(mem_global(), allocation_size);
  if (!storage) return SALTS_ENOMEM;
  segments = (flowmq_protocol_segment_t *)storage;
  framing = (unsigned char *)storage + segment_bytes;
  flowmq_protocol_fill_segments(frame, segments, framing);

  out->segments = segments;
  out->segment_count = layout.segment_count;
  out->encoded_size = layout.encoded_size;
  out->storage = storage;
  return SALTS_OK;
}

void flowmq_protocol_segmented_frame_cleanup(flowmq_protocol_segmented_frame_t *frame) {
  if (!frame) return;
  mem_free(mem_global(), frame->storage);
  *frame = (flowmq_protocol_segmented_frame_t)FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
}

typedef struct flowmq_protocol_packet_s {
  flowmq_protocol_frame_kind_t kind;
  flowmq_protocol_pattern_t pattern;
  uint8_t flags;
  uint16_t identity_len;
  uint16_t topic_len;
  uint32_t chunk_len;
  uint64_t message_id;
  uint32_t payload_len;
  uint32_t payload_offset;
  size_t record_len;
} flowmq_protocol_packet_t;

static int flowmq_protocol_decode_packet(const char *data, size_t data_len,
                                         flowmq_protocol_packet_t *packet) {
  const unsigned char *header = (const unsigned char *)data;
  size_t body_len;
  if (!packet || (data_len > 0u && !data)) return SALTS_EINVAL;
  if (data_len >= sizeof(FLOWMQ_PROTOCOL_MAGIC) &&
      memcmp(header, FLOWMQ_PROTOCOL_MAGIC, sizeof(FLOWMQ_PROTOCOL_MAGIC)) != 0)
    return SALTS_EPROTO;
  if (data_len >= 5u && header[4] != FLOWMQ_PROTOCOL_WIRE_VERSION) return SALTS_EPROTO;
  if (data_len < FLOWMQ_PROTOCOL_HEADER_SIZE) return FLOWMQ_PROTOCOL_INCOMPLETE;
  if ((header[7] & ~(FLOWMQ_PROTOCOL_PACKET_FIRST | FLOWMQ_PROTOCOL_PACKET_LAST |
                     FLOWMQ_PROTOCOL_MESSAGE_MORE)) != 0u)
    return SALTS_EPROTO;
  if (!flowmq_protocol_is_core_kind((flowmq_protocol_frame_kind_t)header[5]) ||
      header[6] < FLOWMQ_PROTOCOL_PUB || header[6] > FLOWMQ_PROTOCOL_XSUB)
    return SALTS_EPROTO;
  memset(packet, 0, sizeof(*packet));
  packet->kind = (flowmq_protocol_frame_kind_t)header[5];
  packet->pattern = (flowmq_protocol_pattern_t)header[6];
  packet->flags = header[7];
  packet->identity_len = flowmq_protocol_read_u16(header + 8);
  packet->topic_len = flowmq_protocol_read_u16(header + 10);
  packet->chunk_len = flowmq_protocol_read_u32(header + 12);
  packet->message_id = flowmq_protocol_read_u64(header + 16);
  packet->payload_len = flowmq_protocol_read_u32(header + 24);
  packet->payload_offset = flowmq_protocol_read_u32(header + 28);
  if (packet->identity_len > FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE ||
      packet->topic_len > FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE ||
      packet->chunk_len > FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE)
    return SALTS_EMSGSIZE;
  body_len = (size_t)packet->identity_len + packet->topic_len + packet->chunk_len;
  if (body_len > SIZE_MAX - FLOWMQ_PROTOCOL_HEADER_SIZE) return SALTS_ERANGE;
  packet->record_len = FLOWMQ_PROTOCOL_HEADER_SIZE + body_len;
  return data_len < packet->record_len ? FLOWMQ_PROTOCOL_INCOMPLETE : SALTS_OK;
}

int flowmq_protocol_encoded_topic(const char *data, size_t data_len, size_t max_frame_size,
                                  vstr *topic) {
  flowmq_protocol_packet_t packet;
  int rc;
  if (!topic) return SALTS_EINVAL;
  *topic = (vstr){0};
  rc = flowmq_protocol_decode_packet(data, data_len, &packet);
  if (rc != SALTS_OK) return rc;
  if ((packet.flags & FLOWMQ_PROTOCOL_PACKET_FIRST) == 0u || packet.payload_offset != 0u ||
      (size_t)packet.identity_len + packet.topic_len + packet.payload_len > max_frame_size) {
    return SALTS_EPROTO;
  }
  if (packet.kind == FLOWMQ_PROTOCOL_FRAME_DATA) {
    if (packet.message_id == 0u) return SALTS_EPROTO;
  } else if (packet.message_id != 0u) {
    return SALTS_EPROTO;
  }
  *topic =
      vstr_from_buf(data + FLOWMQ_PROTOCOL_HEADER_SIZE + packet.identity_len, packet.topic_len);
  return SALTS_OK;
}

int flowmq_protocol_decode_frame(const char *data, size_t data_len, size_t max_frame_size,
                                 flowmq_protocol_frame_t *out, size_t *consumed) {
  flowmq_protocol_packet_t first;
  size_t cursor = 0u;
  size_t payload_copied = 0u;
  size_t packet_count = 0u;
  int rc;
  if (!out || !consumed || (data_len > 0 && !data)) return SALTS_EINVAL;
  *consumed = 0;
  memset(out, 0, sizeof(*out));
  rc = flowmq_protocol_decode_packet(data, data_len, &first);
  if (rc != SALTS_OK) return rc;
  if ((first.flags & FLOWMQ_PROTOCOL_PACKET_FIRST) == 0u || first.payload_offset != 0u)
    return SALTS_EPROTO;
  if ((size_t)first.identity_len + first.topic_len + first.payload_len > max_frame_size)
    return SALTS_EMSGSIZE;
  if (first.kind == FLOWMQ_PROTOCOL_FRAME_DATA) {
    if (first.message_id == 0u) return SALTS_EPROTO;
  } else if (first.message_id != 0u || first.payload_len != first.chunk_len ||
             (first.flags & FLOWMQ_PROTOCOL_PACKET_LAST) == 0u) {
    return SALTS_EPROTO;
  }
  if ((first.flags & FLOWMQ_PROTOCOL_MESSAGE_MORE) != 0u &&
      first.kind != FLOWMQ_PROTOCOL_FRAME_DATA)
    return SALTS_EPROTO;
  for (;;) {
    flowmq_protocol_packet_t packet;
    rc = flowmq_protocol_decode_packet(data + cursor, data_len - cursor, &packet);
    if (rc != SALTS_OK) return rc;
    if (packet.kind != first.kind || packet.pattern != first.pattern ||
        packet.message_id != first.message_id || packet.payload_len != first.payload_len ||
        (packet.flags & FLOWMQ_PROTOCOL_MESSAGE_MORE) !=
            (first.flags & FLOWMQ_PROTOCOL_MESSAGE_MORE) ||
        packet.payload_offset != payload_copied)
      return SALTS_EPROTO;
    if (packet_count == 0u) {
      if ((packet.flags & FLOWMQ_PROTOCOL_PACKET_FIRST) == 0u) return SALTS_EPROTO;
    } else if ((packet.flags & FLOWMQ_PROTOCOL_PACKET_FIRST) != 0u || packet.identity_len != 0u ||
               packet.topic_len != 0u)
      return SALTS_EPROTO;
    if ((size_t)packet.chunk_len > first.payload_len - payload_copied) return SALTS_EPROTO;
    payload_copied += packet.chunk_len;
    cursor += packet.record_len;
    packet_count += 1u;
    if ((packet.flags & FLOWMQ_PROTOCOL_PACKET_LAST) != 0u) break;
    if (packet.chunk_len == 0u || payload_copied == first.payload_len) return SALTS_EPROTO;
  }
  if (payload_copied != first.payload_len) return SALTS_EPROTO;

  out->kind = first.kind;
  out->pattern = first.pattern;
  out->message_id = first.message_id;
  out->more = (first.flags & FLOWMQ_PROTOCOL_MESSAGE_MORE) != 0u;
  out->identity = vstr_from_buf(data + FLOWMQ_PROTOCOL_HEADER_SIZE, first.identity_len);
  out->topic =
      vstr_from_buf(data + FLOWMQ_PROTOCOL_HEADER_SIZE + first.identity_len, first.topic_len);
  if (packet_count == 1u) {
    out->payload =
        vstr_from_buf(data + FLOWMQ_PROTOCOL_HEADER_SIZE + first.identity_len + first.topic_len,
                      first.payload_len);
  } else {
    size_t read_cursor = 0u;
    size_t write_offset = 0u;
    out->owned_payload = tstr_new_len(NULL, first.payload_len);
    if (!out->owned_payload) return SALTS_ENOMEM;
    while (read_cursor < cursor) {
      flowmq_protocol_packet_t packet;
      rc = flowmq_protocol_decode_packet(data + read_cursor, cursor - read_cursor, &packet);
      if (rc != SALTS_OK) {
        flowmq_protocol_frame_cleanup(out);
        return rc;
      }
      if (packet.chunk_len > 0u) {
        memcpy(out->owned_payload + write_offset,
               data + read_cursor + FLOWMQ_PROTOCOL_HEADER_SIZE + packet.identity_len +
                   packet.topic_len,
               packet.chunk_len);
        write_offset += packet.chunk_len;
      }
      read_cursor += packet.record_len;
    }
    out->payload = vstr_from_buf((const char *)out->owned_payload, first.payload_len);
  }
  if ((out->kind == FLOWMQ_PROTOCOL_FRAME_PING || out->kind == FLOWMQ_PROTOCOL_FRAME_PONG) &&
      (out->identity.len != 0 || out->topic.len != 0 || out->payload.len != 0)) {
    flowmq_protocol_frame_cleanup(out);
    return SALTS_EPROTO;
  }
  if ((out->kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE ||
       out->kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE) &&
      (out->identity.len != 0 || out->payload.len != 0)) {
    flowmq_protocol_frame_cleanup(out);
    return SALTS_EPROTO;
  }
  if (out->kind == FLOWMQ_PROTOCOL_FRAME_HELLO) {
    flowmq_security_t security;
    rc = flowmq_security_decode(out->payload, &security);
    if (rc != SALTS_OK) {
      flowmq_protocol_frame_cleanup(out);
      return rc;
    }
  }
  if ((out->kind == FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE ||
       out->kind == FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE) &&
      out->pattern != FLOWMQ_PROTOCOL_SUB && out->pattern != FLOWMQ_PROTOCOL_XSUB) {
    flowmq_protocol_frame_cleanup(out);
    return SALTS_EPROTO;
  }
  rc = flowmq_protocol_flow_control_frame_validate(out);
  if (rc != SALTS_OK) {
    flowmq_protocol_frame_cleanup(out);
    return rc;
  }
  *consumed = cursor;
  return SALTS_OK;
}

void flowmq_protocol_frame_cleanup(flowmq_protocol_frame_t *frame) {
  if (!frame) return;
  tstr_freep(&frame->owned_payload);
  memset(frame, 0, sizeof(*frame));
}
