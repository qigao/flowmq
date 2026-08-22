#include "flowmq_protocol.h"

#include "turbo_error.h"
#include "turbo_str.h"

#include <stdio.h>
#include <string.h>

enum { EXAMPLE_MAX_FRAME_SIZE = 4096 };

int main(void) {
  static const char payload[] = "payload bytes are borrowed, not copied";
  flowmq_protocol_frame_t frame;
  flowmq_protocol_segmented_frame_t encoded = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;
  size_t payload_segment = 0u;
  int rc;

  memset(&frame, 0, sizeof(frame));
  frame.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
  frame.pattern = FLOWMQ_PROTOCOL_PUSH;
  frame.message_id = 7u;
  frame.topic = vstr_from_cstr("examples.segmented");
  frame.payload = vstr_from_buf(payload, sizeof(payload) - 1u);

  rc = flowmq_protocol_encode_frame_segmented(&frame, EXAMPLE_MAX_FRAME_SIZE,
                                               &encoded);
  if (rc != TURBO_OK) {
    fprintf(stderr, "FlowMQ segmented encode failed: %d\n", rc);
    return 1;
  }
  for (size_t index = 0u; index < encoded.segment_count; ++index) {
    if (encoded.segments[index].data == payload) payload_segment = index + 1u;
  }
  if (payload_segment == 0u) {
    fprintf(stderr, "FlowMQ did not preserve the borrowed payload segment\n");
    flowmq_protocol_segmented_frame_cleanup(&encoded);
    return 1;
  }

  printf("segments=%zu encoded=%zu payload_segment=%zu\n",
         encoded.segment_count, encoded.encoded_size, payload_segment - 1u);
  flowmq_protocol_segmented_frame_cleanup(&encoded);
  return 0;
}
