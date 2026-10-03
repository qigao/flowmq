#ifndef FLOWMQ_PROTOCOL_INTERNAL_H
#define FLOWMQ_PROTOCOL_INTERNAL_H

#include "flowmq_protocol.h"

#include <string.h>

typedef struct flowmq_protocol_packet_header_internal_s {
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
} flowmq_protocol_packet_header_internal_t;

static inline uint16_t flowmq_protocol_internal_read_u16(
    const unsigned char *data) {
  return (uint16_t)(((uint16_t)data[0] << 8) | data[1]);
}

static inline uint32_t flowmq_protocol_internal_read_u32(
    const unsigned char *data) {
  return ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) |
         ((uint32_t)data[2] << 8) | data[3];
}

static inline uint64_t flowmq_protocol_internal_read_u64(
    const unsigned char *data) {
  return ((uint64_t)flowmq_protocol_internal_read_u32(data) << 32) |
         flowmq_protocol_internal_read_u32(data + 4u);
}

/*
 * Decode and validate only the fixed FMQ packet header. The packet body may be
 * absent; callers use record_len to decide when identity/topic/payload bytes
 * are complete. This stays header-only/private so segmented stream cursors do
 * not create a new installed ABI.
 */
static inline int flowmq_protocol_decode_packet_header_internal(
    const void *data, size_t data_len,
    flowmq_protocol_packet_header_internal_t *packet) {
  static const unsigned char magic[4] = {'T', 'F', 'M', 'Q'};
  const unsigned char *header = (const unsigned char *)data;
  size_t body_len;
  if (packet == NULL || (data_len > 0u && data == NULL)) return SALTS_EINVAL;
  if (data_len >= sizeof(magic) &&
      memcmp(header, magic, sizeof(magic)) != 0)
    return SALTS_EPROTO;
  if (data_len >= 5u && header[4] != FLOWMQ_PROTOCOL_WIRE_VERSION)
    return SALTS_EPROTO;
  if (data_len < FLOWMQ_PROTOCOL_HEADER_SIZE)
    return FLOWMQ_PROTOCOL_INCOMPLETE;
  if ((header[7] &
       ~(FLOWMQ_PROTOCOL_PACKET_FIRST | FLOWMQ_PROTOCOL_PACKET_LAST |
         FLOWMQ_PROTOCOL_MESSAGE_MORE)) != 0u)
    return SALTS_EPROTO;
  if (!(((flowmq_protocol_frame_kind_t)header[5] >=
             FLOWMQ_PROTOCOL_FRAME_HELLO &&
         (flowmq_protocol_frame_kind_t)header[5] <=
             FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE) ||
        (flowmq_protocol_frame_kind_t)header[5] ==
            FLOWMQ_PROTOCOL_FRAME_SETTINGS ||
        (flowmq_protocol_frame_kind_t)header[5] ==
            FLOWMQ_PROTOCOL_FRAME_FLOW_UPDATE) ||
      header[6] < FLOWMQ_PROTOCOL_PUB || header[6] > FLOWMQ_PROTOCOL_XSUB)
    return SALTS_EPROTO;

  memset(packet, 0, sizeof(*packet));
  packet->kind = (flowmq_protocol_frame_kind_t)header[5];
  packet->pattern = (flowmq_protocol_pattern_t)header[6];
  packet->flags = header[7];
  packet->identity_len = flowmq_protocol_internal_read_u16(header + 8u);
  packet->topic_len = flowmq_protocol_internal_read_u16(header + 10u);
  packet->chunk_len = flowmq_protocol_internal_read_u32(header + 12u);
  packet->message_id = flowmq_protocol_internal_read_u64(header + 16u);
  packet->payload_len = flowmq_protocol_internal_read_u32(header + 24u);
  packet->payload_offset = flowmq_protocol_internal_read_u32(header + 28u);
  if (packet->identity_len > FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE ||
      packet->topic_len > FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE ||
      packet->chunk_len > FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE)
    return SALTS_EMSGSIZE;
  body_len = (size_t)packet->identity_len + packet->topic_len +
             packet->chunk_len;
  if (body_len > SIZE_MAX - FLOWMQ_PROTOCOL_HEADER_SIZE) return SALTS_ERANGE;
  packet->record_len = FLOWMQ_PROTOCOL_HEADER_SIZE + body_len;
  return SALTS_OK;
}

int flowmq_protocol_encode_frame_into_internal(
    const flowmq_protocol_frame_t *frame, size_t max_frame_size, void *storage,
    size_t storage_size, size_t *encoded_size);

int flowmq_protocol_encode_frame_segmented_into_internal(
    const flowmq_protocol_frame_t *frame, size_t max_frame_size,
    flowmq_protocol_segment_t *segments, size_t segment_capacity,
    void *framing, size_t framing_capacity, size_t *segment_count,
    size_t *encoded_size);

#endif /* FLOWMQ_PROTOCOL_INTERNAL_H */
