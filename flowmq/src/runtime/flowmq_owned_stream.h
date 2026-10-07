#ifndef FLOWMQ_OWNED_STREAM_H
#define FLOWMQ_OWNED_STREAM_H

#include "flowmq_protocol_internal.h"
#include "flowmq_stream_decoder.h"

#include "cmeta_buffer.h"
#include "cmeta_error.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * One 1 MiB FMQ DATA frame contains at most 16 protocol packets. Keep enough
 * raw receive ownership for the common case where each packet is split into
 * at most a header-side and payload-side CNet receive value. More fragmented
 * TCP shapes explicitly replay into the existing contiguous decoder.
 */
enum { FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY = 32u };

typedef struct flowmq_owned_stream_s {
  mem_slice_t segments[FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY];
  size_t segment_count;
  size_t front;
  size_t front_offset;
  size_t bytes;
} flowmq_owned_stream_t;

#define FLOWMQ_OWNED_STREAM_INIT { {0}, 0u, 0u, 0u, 0u }

static inline size_t flowmq_owned_stream_size(
    const flowmq_owned_stream_t *stream) {
  return stream != NULL ? stream->bytes : 0u;
}

static inline size_t flowmq_owned_stream_segment_count(
    const flowmq_owned_stream_t *stream) {
  return stream != NULL && stream->segment_count >= stream->front
             ? stream->segment_count - stream->front
             : 0u;
}

static inline void flowmq_owned_stream_reset(flowmq_owned_stream_t *stream) {
  if (stream == NULL) return;
  for (size_t i = 0u; i < stream->segment_count; ++i)
    mem_slice_release(&stream->segments[i]);
  memset(stream, 0, sizeof(*stream));
}

static inline void flowmq_owned_stream_compact(flowmq_owned_stream_t *stream) {
  size_t active;
  if (stream == NULL || stream->front == 0u) return;
  active = flowmq_owned_stream_segment_count(stream);
  if (active != 0u)
    memmove(stream->segments, stream->segments + stream->front,
            active * sizeof(stream->segments[0]));
  memset(stream->segments + active, 0,
         (FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY - active) *
             sizeof(stream->segments[0]));
  stream->segment_count = active;
  stream->front = 0u;
}

static inline int flowmq_owned_stream_slice_valid(const mem_slice_t *slice) {
  const char *base;
  uintptr_t base_address;
  uintptr_t data_address;
  uintptr_t delta;
  size_t used;
  if (slice == NULL || slice->buffer == NULL || slice->data == NULL ||
      slice->length == 0u)
    return 0;
  base = mem_buffer_const_data(slice->buffer);
  used = mem_buffer_used(slice->buffer);
  if (base == NULL || used == 0u) return 0;
  base_address = (uintptr_t)(const void *)base;
  data_address = (uintptr_t)slice->data;
  if (data_address < base_address) return 0;
  delta = data_address - base_address;
  return delta <= (uintptr_t)SIZE_MAX && (size_t)delta < used &&
         slice->length <= used - (size_t)delta;
}

/* Move exactly one canonical owned receive descriptor into bounded storage. */
static inline int flowmq_owned_stream_append_move(
    flowmq_owned_stream_t *stream, mem_slice_t *slice) {
  if (stream == NULL || !flowmq_owned_stream_slice_valid(slice))
    return SALTS_EINVAL;
  if (slice->length > SIZE_MAX - stream->bytes) return SALTS_ERANGE;
  if (stream->segment_count == FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY &&
      stream->front != 0u)
    flowmq_owned_stream_compact(stream);
  if (stream->segment_count == FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY)
    return SALTS_ENOBUFS;
  stream->segments[stream->segment_count++] = *slice;
  stream->bytes += slice->length;
  memset(slice, 0, sizeof(*slice));
  return SALTS_OK;
}

/* Copy a small logical range without changing ownership/cursor state. */
static inline int flowmq_owned_stream_peek(
    const flowmq_owned_stream_t *stream, size_t offset,
    void *destination, size_t size) {
  unsigned char *out = (unsigned char *)destination;
  size_t logical = 0u;
  size_t copied = 0u;
  if (stream == NULL || (destination == NULL && size != 0u))
    return SALTS_EINVAL;
  if (offset > stream->bytes || size > stream->bytes - offset)
    return SALTS_ENOSPC;
  if (size == 0u) return SALTS_OK;

  for (size_t i = stream->front;
       i < stream->segment_count && copied < size; ++i) {
    const mem_slice_t *slice = &stream->segments[i];
    size_t begin = i == stream->front ? stream->front_offset : 0u;
    size_t available = slice->length - begin;
    if (offset >= logical + available) {
      logical += available;
      continue;
    }
    {
      size_t within = offset > logical ? offset - logical : 0u;
      size_t take = available - within;
      if (take > size - copied) take = size - copied;
      memcpy(out + copied,
             (const unsigned char *)slice->data + begin + within, take);
      copied += take;
      offset += take;
      logical += available;
    }
  }
  return copied == size ? SALTS_OK : SALTS_EPROTO;
}

/* Advance and release fully consumed owned receive descriptors. */
static inline int flowmq_owned_stream_consume(
    flowmq_owned_stream_t *stream, size_t size) {
  if (stream == NULL) return SALTS_EINVAL;
  if (size > stream->bytes) return SALTS_ERANGE;
  while (size != 0u) {
    mem_slice_t *slice;
    size_t available;
    if (stream->front >= stream->segment_count) return SALTS_EPROTO;
    slice = &stream->segments[stream->front];
    available = slice->length - stream->front_offset;
    if (size < available) {
      stream->front_offset += size;
      stream->bytes -= size;
      size = 0u;
      break;
    }
    size -= available;
    stream->bytes -= available;
    mem_slice_release(slice);
    ++stream->front;
    stream->front_offset = 0u;
  }
  if (stream->bytes == 0u) {
    stream->segment_count = 0u;
    stream->front = 0u;
    stream->front_offset = 0u;
  }
  return SALTS_OK;
}

/*
 * Probe exactly the first FMQ frame boundary without flattening its body.
 * Header bytes may cross owned receive descriptors. Full packet bodies must be
 * present before the cursor advances to the next header.
 */
static inline int flowmq_owned_stream_first_frame_size(
    const flowmq_owned_stream_t *stream, size_t max_frame_size,
    size_t *frame_size) {
  unsigned char header_bytes[FLOWMQ_PROTOCOL_HEADER_SIZE];
  flowmq_protocol_packet_header_internal_t first = {0};
  size_t cursor = 0u;
  size_t payload_seen = 0u;
  size_t packet_count = 0u;
  int status;
  if (stream == NULL || frame_size == NULL || max_frame_size == 0u)
    return SALTS_EINVAL;
  *frame_size = 0u;

  for (;;) {
    flowmq_protocol_packet_header_internal_t packet = {0};
    if (stream->bytes - cursor < FLOWMQ_PROTOCOL_HEADER_SIZE)
      return FLOWMQ_PROTOCOL_INCOMPLETE;
    status = flowmq_owned_stream_peek(
        stream, cursor, header_bytes, sizeof(header_bytes));
    if (status != SALTS_OK) return status;
    status = flowmq_protocol_decode_packet_header_internal(
        header_bytes, sizeof(header_bytes), &packet);
    if (status != SALTS_OK) return status;
    if (packet.record_len > stream->bytes - cursor)
      return FLOWMQ_PROTOCOL_INCOMPLETE;

    if (packet_count == 0u) {
      first = packet;
      if ((first.flags & FLOWMQ_PROTOCOL_PACKET_FIRST) == 0u ||
          first.payload_offset != 0u)
        return SALTS_EPROTO;
      if ((size_t)first.identity_len + first.topic_len + first.payload_len >
          max_frame_size)
        return SALTS_EMSGSIZE;
      if (first.kind == FLOWMQ_PROTOCOL_FRAME_DATA) {
        if (first.message_id == 0u) return SALTS_EPROTO;
      } else if (first.message_id != 0u ||
                 first.payload_len != first.chunk_len ||
                 (first.flags & FLOWMQ_PROTOCOL_PACKET_LAST) == 0u) {
        return SALTS_EPROTO;
      }
      if ((first.flags & FLOWMQ_PROTOCOL_MESSAGE_MORE) != 0u &&
          first.kind != FLOWMQ_PROTOCOL_FRAME_DATA)
        return SALTS_EPROTO;
    } else {
      if (packet.kind != first.kind || packet.pattern != first.pattern ||
          packet.message_id != first.message_id ||
          packet.payload_len != first.payload_len ||
          (packet.flags & FLOWMQ_PROTOCOL_MESSAGE_MORE) !=
              (first.flags & FLOWMQ_PROTOCOL_MESSAGE_MORE) ||
          packet.payload_offset != payload_seen ||
          (packet.flags & FLOWMQ_PROTOCOL_PACKET_FIRST) != 0u ||
          packet.identity_len != 0u || packet.topic_len != 0u)
        return SALTS_EPROTO;
    }

    if ((size_t)packet.chunk_len > first.payload_len - payload_seen)
      return SALTS_EPROTO;
    payload_seen += packet.chunk_len;
    cursor += packet.record_len;
    ++packet_count;

    if ((packet.flags & FLOWMQ_PROTOCOL_PACKET_LAST) != 0u) {
      if (payload_seen != first.payload_len) return SALTS_EPROTO;
      *frame_size = cursor;
      return SALTS_OK;
    }
    if (packet.chunk_len == 0u || payload_seen == first.payload_len)
      return SALTS_EPROTO;
  }
}


typedef struct flowmq_owned_data_projection_s {
  flowmq_protocol_pattern_t pattern;
  uint64_t message_id;
  size_t payload_size;
  size_t frame_size;
  mem_slice_t segments[FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY];
  size_t segment_count;
  int more;
} flowmq_owned_data_projection_t;

#define FLOWMQ_OWNED_DATA_PROJECTION_INIT \
  { 0u, 0u, 0u, 0u, {0}, 0u, 0 }

static inline void flowmq_owned_data_projection_reset(
    flowmq_owned_data_projection_t *projection) {
  if (projection == NULL) return;
  for (size_t i = 0u; i < projection->segment_count; ++i)
    mem_slice_release(&projection->segments[i]);
  memset(projection, 0, sizeof(*projection));
}

/*
 * Retain one logical unread stream range as canonical backing sub-slices.
 * Output capacity is fixed/bounded; failure releases every new retain.
 */
static inline int flowmq_owned_stream_retain_range(
    const flowmq_owned_stream_t *stream, size_t offset, size_t size,
    mem_slice_t *segments, size_t capacity, size_t *count) {
  size_t logical = 0u;
  size_t produced = 0u;
  size_t retained_bytes = 0u;
  if (stream == NULL || segments == NULL || count == NULL)
    return SALTS_EINVAL;
  *count = 0u;
  if (offset > stream->bytes || size > stream->bytes - offset)
    return SALTS_ENOSPC;
  if (size == 0u) return SALTS_OK;

  for (size_t i = stream->front;
       i < stream->segment_count && retained_bytes < size; ++i) {
    const mem_slice_t *source = &stream->segments[i];
    const char *base = mem_buffer_const_data(source->buffer);
    size_t begin = i == stream->front ? stream->front_offset : 0u;
    size_t available = source->length - begin;
    size_t within;
    size_t take;
    uintptr_t base_address;
    uintptr_t data_address;
    uintptr_t delta;

    if (offset >= logical + available) {
      logical += available;
      continue;
    }
    within = offset > logical ? offset - logical : 0u;
    take = available - within;
    if (take > size - retained_bytes) take = size - retained_bytes;
    if (take == 0u) {
      logical += available;
      continue;
    }
    if (produced == capacity || base == NULL) {
      for (size_t j = 0u; j < produced; ++j)
        mem_slice_release(&segments[j]);
      return produced == capacity ? SALTS_ENOSPC : SALTS_EPROTO;
    }

    base_address = (uintptr_t)(const void *)base;
    data_address =
        (uintptr_t)(const void *)((const unsigned char *)source->data +
                                  begin + within);
    if (data_address < base_address ||
        data_address - base_address > (uintptr_t)SIZE_MAX) {
      for (size_t j = 0u; j < produced; ++j)
        mem_slice_release(&segments[j]);
      return SALTS_EPROTO;
    }
    delta = data_address - base_address;
    if ((size_t)delta > mem_buffer_used(source->buffer) ||
        take > mem_buffer_used(source->buffer) - (size_t)delta) {
      for (size_t j = 0u; j < produced; ++j)
        mem_slice_release(&segments[j]);
      return SALTS_EPROTO;
    }
    segments[produced] =
        mem_slice(source->buffer, (size_t)delta, take);
    if (segments[produced].buffer == NULL ||
        segments[produced].length != take) {
      mem_slice_release(&segments[produced]);
      for (size_t j = 0u; j < produced; ++j)
        mem_slice_release(&segments[j]);
      return SALTS_EPROTO;
    }
    ++produced;
    retained_bytes += take;
    offset += take;
    logical += available;
  }

  if (retained_bytes != size) {
    for (size_t j = 0u; j < produced; ++j)
      mem_slice_release(&segments[j]);
    return SALTS_EPROTO;
  }
  *count = produced;
  return SALTS_OK;
}

/*
 * Validate and project exactly the first complete DATA frame into retained
 * payload subranges. Packet headers/identity/topic remain in the raw owned
 * stream and are not retained by the projection.
 *
 * SALTS_ENOTSUP means the complete first frame is valid but not a DATA frame;
 * callers should use the existing copied decoder path.
 * SALTS_ENOSPC means the bounded payload-vector shape does not fit and should
 * likewise fall back without claiming zero-copy.
 */
static inline int flowmq_owned_stream_project_first_data(
    const flowmq_owned_stream_t *stream, size_t max_frame_size,
    flowmq_owned_data_projection_t *projection) {
  unsigned char header_bytes[FLOWMQ_PROTOCOL_HEADER_SIZE];
  flowmq_protocol_packet_header_internal_t first = {0};
  size_t frame_size = 0u;
  size_t cursor = 0u;
  size_t payload_seen = 0u;
  size_t packet_count = 0u;
  int status;

  if (stream == NULL || projection == NULL || max_frame_size == 0u)
    return SALTS_EINVAL;
  if (projection->segment_count != 0u)
    return SALTS_EBUSY;
  memset(projection, 0, sizeof(*projection));

  status = flowmq_owned_stream_first_frame_size(
      stream, max_frame_size, &frame_size);
  if (status != SALTS_OK) return status;

  status = flowmq_owned_stream_peek(
      stream, 0u, header_bytes, sizeof(header_bytes));
  if (status != SALTS_OK) return status;
  status = flowmq_protocol_decode_packet_header_internal(
      header_bytes, sizeof(header_bytes), &first);
  if (status != SALTS_OK) return status;
  if (first.kind != FLOWMQ_PROTOCOL_FRAME_DATA || first.payload_len == 0u)
    return SALTS_ENOTSUP;

  projection->pattern = first.pattern;
  projection->message_id = first.message_id;
  projection->payload_size = first.payload_len;
  projection->frame_size = frame_size;
  projection->more =
      (first.flags & FLOWMQ_PROTOCOL_MESSAGE_MORE) != 0u;

  while (cursor < frame_size) {
    flowmq_protocol_packet_header_internal_t packet = {0};
    size_t payload_offset;
    size_t added = 0u;
    if (frame_size - cursor < FLOWMQ_PROTOCOL_HEADER_SIZE) {
      status = SALTS_EPROTO;
      goto fail;
    }
    status = flowmq_owned_stream_peek(
        stream, cursor, header_bytes, sizeof(header_bytes));
    if (status != SALTS_OK) goto fail;
    status = flowmq_protocol_decode_packet_header_internal(
        header_bytes, sizeof(header_bytes), &packet);
    if (status != SALTS_OK) goto fail;
    if (packet.record_len > frame_size - cursor) {
      status = SALTS_EPROTO;
      goto fail;
    }

    payload_offset = cursor + FLOWMQ_PROTOCOL_HEADER_SIZE +
                     packet.identity_len + packet.topic_len;
    if (packet.chunk_len != 0u) {
      status = flowmq_owned_stream_retain_range(
          stream, payload_offset, packet.chunk_len,
          projection->segments + projection->segment_count,
          FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY - projection->segment_count,
          &added);
      if (status != SALTS_OK) goto fail;
      projection->segment_count += added;
    }
    payload_seen += packet.chunk_len;
    cursor += packet.record_len;
    ++packet_count;
  }

  if (cursor != frame_size || payload_seen != projection->payload_size ||
      packet_count == 0u || projection->segment_count == 0u) {
    status = SALTS_EPROTO;
    goto fail;
  }
  return SALTS_OK;

fail:
  flowmq_owned_data_projection_reset(projection);
  return status;
}

/*
 * Replay one unread prefix into the existing copied decoder in order, then
 * consume/release exactly that prefix from owned storage. The capacity
 * preflight prevents a normal bounded fallback from partially mutating the
 * decoder.
 */
static inline int flowmq_owned_stream_replay_prefix(
    flowmq_owned_stream_t *stream, flowmq_stream_decoder_t *decoder,
    size_t size) {
  size_t remaining = size;
  int status = SALTS_OK;
  if (stream == NULL || decoder == NULL) return SALTS_EINVAL;
  if (size > stream->bytes) return SALTS_ERANGE;
  if (size > flowmq_stream_decoder_available(decoder)) return SALTS_ENOSPC;

  for (size_t i = stream->front;
       i < stream->segment_count && remaining != 0u; ++i) {
    const mem_slice_t *slice = &stream->segments[i];
    size_t begin = i == stream->front ? stream->front_offset : 0u;
    size_t take = slice->length - begin;
    if (take > remaining) take = remaining;
    if (take == 0u) continue;
    status = flowmq_stream_decoder_append(
        decoder, (const unsigned char *)slice->data + begin, take);
    if (status != SALTS_OK) return status;
    remaining -= take;
  }
  if (remaining != 0u) return SALTS_EPROTO;
  return flowmq_owned_stream_consume(stream, size);
}

/* Replay and release all unread ownership. */
static inline int flowmq_owned_stream_replay(
    flowmq_owned_stream_t *stream, flowmq_stream_decoder_t *decoder) {
  if (stream == NULL) return SALTS_EINVAL;
  return flowmq_owned_stream_replay_prefix(stream, decoder, stream->bytes);
}

#endif /* FLOWMQ_OWNED_STREAM_H */
