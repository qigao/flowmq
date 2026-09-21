#include "flowmq_protocol.h"

#include "salts_error.h"
#include "str.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

enum { EXAMPLE_MAX_FRAME_SIZE = 4096 };

int main(void) {
  static const char payload[] = "hello from standalone FlowMQ";
  flowmq_protocol_frame_t input;
  flowmq_protocol_frame_t output;
  tstr encoded = NULL;
  size_t consumed = 0u;
  int rc;

  memset(&input, 0, sizeof(input));
  memset(&output, 0, sizeof(output));
  input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
  input.pattern = FLOWMQ_PROTOCOL_PUB;
  input.message_id = UINT64_C(42);
  input.identity = vstr_from_cstr("publisher-a");
  input.topic = vstr_from_cstr("examples.standalone");
  input.payload = vstr_from_buf(payload, sizeof(payload) - 1u);

  rc = flowmq_protocol_encode_frame(&input, EXAMPLE_MAX_FRAME_SIZE, &encoded);
  if (rc == SALTS_OK)
    rc = flowmq_protocol_decode_frame(encoded, tstr_len(encoded),
                                      EXAMPLE_MAX_FRAME_SIZE, &output,
                                      &consumed);
  if (rc != SALTS_OK) {
    fprintf(stderr, "FlowMQ frame round-trip failed: %d\n", rc);
    tstr_free(encoded);
    return 1;
  }

  printf("message=%" PRIu64 " topic=%.*s payload=%.*s encoded=%zu bytes\n",
         output.message_id, (int)output.topic.len, output.topic.data,
         (int)output.payload.len, output.payload.data, consumed);
  flowmq_protocol_frame_cleanup(&output);
  tstr_free(encoded);
  return 0;
}
