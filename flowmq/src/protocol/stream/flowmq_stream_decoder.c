#include "flowmq_stream_decoder.h"

#include "salts_error.h"

#include <string.h>

int flowmq_stream_decoder_prepare(flowmq_stream_decoder_t *stream, size_t max_frame_size) {
  size_t encoded_limit;
  int rc;
  if (!stream || max_frame_size == 0u) return SALTS_EINVAL;
  if (stream->initialized)
    return stream->max_frame_size == max_frame_size ? SALTS_OK : SALTS_EALREADY;
  rc = flowmq_protocol_encoded_size_limit(max_frame_size, &encoded_limit);
  if (rc != SALTS_OK) return rc;
  rc = salts_bytes_init(&stream->buffer, encoded_limit);
  if (rc != SALTS_OK) return rc;
  stream->max_frame_size = max_frame_size;
  stream->initialized = 1;
  return SALTS_OK;
}

void flowmq_stream_decoder_destroy(flowmq_stream_decoder_t *stream) {
  if (!stream) return;
  if (stream->initialized) salts_bytes_destroy(&stream->buffer);
  memset(stream, 0, sizeof(*stream));
}

void flowmq_stream_decoder_destroy_sensitive(flowmq_stream_decoder_t *stream) {
  salts_bytes_view_t view;
  if (!stream) return;
  if (stream->initialized && salts_bytes_view(&stream->buffer, &view) == SALTS_OK) {
    volatile uint8_t *bytes = (volatile uint8_t *)(uintptr_t)view.data;
    for (size_t i = 0u; i < view.size; ++i) bytes[i] = 0u;
  }
  flowmq_stream_decoder_destroy(stream);
}

size_t flowmq_stream_decoder_available(const flowmq_stream_decoder_t *stream) {
  return stream && stream->initialized ? salts_bytes_available(&stream->buffer) : 0u;
}

int flowmq_stream_decoder_append(flowmq_stream_decoder_t *stream, const void *data, size_t size) {
  if (!stream || !stream->initialized) return SALTS_EINVAL;
  return salts_bytes_append(&stream->buffer, data, size);
}

int flowmq_stream_decoder_next(flowmq_stream_decoder_t *stream, flowmq_protocol_frame_t *frame,
                               size_t *consumed) {
  salts_bytes_view_t view;
  int rc;
  if (!stream || !stream->initialized || !frame || !consumed) return SALTS_EINVAL;
  rc = salts_bytes_view(&stream->buffer, &view);
  if (rc != SALTS_OK) return rc;
  return flowmq_protocol_decode_frame((const char *)view.data, view.size, stream->max_frame_size,
                                      frame, consumed);
}

int flowmq_stream_decoder_consume(flowmq_stream_decoder_t *stream, size_t count) {
  if (!stream || !stream->initialized) return SALTS_EINVAL;
  return salts_bytes_consume(&stream->buffer, count);
}

int flowmq_stream_decoder_consume_sensitive(flowmq_stream_decoder_t *stream, size_t count) {
  salts_bytes_view_t view;
  volatile uint8_t *bytes;
  int rc;
  if (!stream || !stream->initialized) return SALTS_EINVAL;
  rc = salts_bytes_view(&stream->buffer, &view);
  if (rc != SALTS_OK) return rc;
  if (count > view.size) return SALTS_ERANGE;
  bytes = (volatile uint8_t *)(uintptr_t)view.data;
  for (size_t i = 0u; i < count; ++i) bytes[i] = 0u;
  return salts_bytes_consume(&stream->buffer, count);
}
