#include "flowmq_protocol.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <string.h>

enum {
  BENCH_SMALL_PAYLOAD_BYTES = 64,
  BENCH_LARGE_PAYLOAD_BYTES = 64 * 1024,
  BENCH_FRAME_METADATA_BUDGET = 4096,
  BENCH_MAX_FRAME_BYTES = BENCH_LARGE_PAYLOAD_BYTES + BENCH_FRAME_METADATA_BUDGET,
  BENCH_SMALL_SAMPLES = 10000,
  BENCH_LARGE_SAMPLES = 1000
};

static void bench_protocol_roundtrip(size_t payload_size, size_t samples,
                                     const char *encode_title,
                                     const char *decode_title) {
  static char payload[BENCH_LARGE_PAYLOAD_BYTES];
  flowmq_protocol_frame_t input;
  flowmq_protocol_frame_t output;
  tstr_t encoded = NULL;
  size_t consumed = 0u;
  int rc = TURBO_OK;

  memset(payload, 0x5a, payload_size);
  memset(&input, 0, sizeof(input));
  input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
  input.pattern = FLOWMQ_PROTOCOL_PUB;
  input.message_id = UINT64_C(1);
  input.identity = tstr_v_from_cstr("bench-publisher");
  input.topic = tstr_v_from_cstr("bench.protocol");
  input.payload = tstr_v_from_buf(payload, payload_size);

  check_int_eq(flowmq_protocol_encode_frame(&input, BENCH_MAX_FRAME_BYTES, &encoded),
               TURBO_OK);
  memset(&output, 0, sizeof(output));
  check_int_eq(flowmq_protocol_decode_frame(encoded, tstr_len(encoded),
                                            BENCH_MAX_FRAME_BYTES, &output, &consumed),
               TURBO_OK);
  check_size_eq(output.payload.len, payload_size);
  check_mem_eq(output.payload.data, payload, payload_size);
  flowmq_protocol_frame_cleanup(&output);

  benchmark_bytes(encode_title, samples, payload_size) {
    tstr_t sample = NULL;
    rc = flowmq_protocol_encode_frame(&input, BENCH_MAX_FRAME_BYTES, &sample);
    tstr_free(sample);
  }
  check_int_eq(rc, TURBO_OK);

  benchmark_bytes(decode_title, samples, payload_size) {
    memset(&output, 0, sizeof(output));
    rc = flowmq_protocol_decode_frame(encoded, tstr_len(encoded), BENCH_MAX_FRAME_BYTES,
                                      &output, &consumed);
    flowmq_protocol_frame_cleanup(&output);
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(consumed, tstr_len(encoded));
  tstr_free(encoded);
}

spec("standalone FlowMQ protocol benchmark") {
  bench("contiguous FMQ v3 framing") {
    bench_protocol_roundtrip(BENCH_SMALL_PAYLOAD_BYTES, BENCH_SMALL_SAMPLES,
                             "encode 64-byte payload",
                             "decode 64-byte payload");
    bench_protocol_roundtrip(BENCH_LARGE_PAYLOAD_BYTES, BENCH_LARGE_SAMPLES,
                             "encode 64-KiB payload",
                             "decode 64-KiB payload");
  }
}
