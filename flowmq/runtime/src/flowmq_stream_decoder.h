#ifndef FLOWMQ_STREAM_DECODER_H
#define FLOWMQ_STREAM_DECODER_H

#include "flowmq_protocol.h"
#include "turbo_byte_buffer.h"

#include <stddef.h>

typedef struct flowmq_stream_decoder_s {
  turbo_byte_buffer_t buffer;
  size_t max_frame_size;
  int initialized;
} flowmq_stream_decoder_t;

int flowmq_stream_decoder_prepare(flowmq_stream_decoder_t *stream, size_t max_frame_size);
void flowmq_stream_decoder_destroy(flowmq_stream_decoder_t *stream);
void flowmq_stream_decoder_destroy_sensitive(flowmq_stream_decoder_t *stream);
size_t flowmq_stream_decoder_available(const flowmq_stream_decoder_t *stream);
int flowmq_stream_decoder_append(flowmq_stream_decoder_t *stream, const void *data, size_t size);
int flowmq_stream_decoder_next(flowmq_stream_decoder_t *stream, flowmq_protocol_frame_t *frame,
                               size_t *consumed);
int flowmq_stream_decoder_consume(flowmq_stream_decoder_t *stream, size_t count);
/** Clear sensitive prefix bytes before consuming them from the owned buffer. */
int flowmq_stream_decoder_consume_sensitive(flowmq_stream_decoder_t *stream, size_t count);

#endif /* FLOWMQ_STREAM_DECODER_H */
