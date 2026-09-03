#include "flowmq_protocol.h"
#include "flowmq_protocol_internal.h"

#include "tinytest.h"
#include "salts_error.h"
#include "salts_str.h"

#include <string.h>

enum {
  BENCH_SMALL_PAYLOAD_BYTES = 64,
  BENCH_LARGE_PAYLOAD_BYTES = 64 * 1024,
  BENCH_FRAME_METADATA_BUDGET = 4096,
  BENCH_MAX_FRAME_BYTES = BENCH_LARGE_PAYLOAD_BYTES + BENCH_FRAME_METADATA_BUDGET,
  BENCH_CONTIGUOUS_CAPACITY = BENCH_MAX_FRAME_BYTES + FLOWMQ_PROTOCOL_HEADER_SIZE,
  BENCH_SEGMENT_CAPACITY = 2,
  BENCH_SMALL_SAMPLES = 10000,
  BENCH_LARGE_SAMPLES = 1000
};

static void bench_protocol_roundtrip(size_t payload_size, size_t samples, const char *encode_title,
                                     const char *encode_into_title,
                                     const char *segmented_title, const char *decode_title) {
  static char payload[BENCH_LARGE_PAYLOAD_BYTES];
  static unsigned char contiguous_storage[BENCH_CONTIGUOUS_CAPACITY];
  static unsigned char framing[BENCH_FRAME_METADATA_BUDGET];
  static flowmq_protocol_segment_t segments[BENCH_SEGMENT_CAPACITY];
  flowmq_protocol_frame_t input;
  flowmq_protocol_frame_t output;
  tstr encoded = NULL;
  size_t consumed = 0u;
  size_t encoded_size = 0u;
  size_t segment_count = 0u;
  int rc = SALTS_OK;

  memset(payload, 0x5a, payload_size);
  memset(&input, 0, sizeof(input));
  input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
  input.pattern = FLOWMQ_PROTOCOL_PUB;
  input.message_id = UINT64_C(1);
  input.identity = vstr_from_cstr("bench-publisher");
  input.topic = vstr_from_cstr("bench.protocol");
  input.payload = vstr_from_buf(payload, payload_size);

  check_equal(flowmq_protocol_encode_frame(&input, BENCH_MAX_FRAME_BYTES, &encoded), SALTS_OK);
  memset(&output, 0, sizeof(output));
  check_equal(flowmq_protocol_decode_frame(encoded, tstr_len(encoded), BENCH_MAX_FRAME_BYTES,
                                           &output, &consumed),
              SALTS_OK);
  check_equal(output.payload.len, payload_size);
  check_equal(output.payload.data, payload, payload_size);
  flowmq_protocol_frame_cleanup(&output);
  check_equal(flowmq_protocol_encode_frame_into_internal(
                  &input, BENCH_MAX_FRAME_BYTES, contiguous_storage,
                  sizeof(contiguous_storage), &encoded_size),
              SALTS_OK);
  check_equal(encoded_size, tstr_len(encoded));
  check_equal(flowmq_protocol_encode_frame_segmented_into_internal(
                  &input, BENCH_MAX_FRAME_BYTES, segments,
                  BENCH_SEGMENT_CAPACITY, framing, sizeof(framing),
                  &segment_count, &encoded_size),
              SALTS_OK);
  check_equal(segment_count, (size_t)BENCH_SEGMENT_CAPACITY);
  check_equal(encoded_size, tstr_len(encoded));
  check_true(segments[1].data == payload);

  benchmark_bytes(encode_title, samples, payload_size) {
    tstr sample = NULL;
    rc = flowmq_protocol_encode_frame(&input, BENCH_MAX_FRAME_BYTES, &sample);
    tstr_free(sample);
  }
  check_equal(rc, SALTS_OK);

  benchmark_bytes(encode_into_title, samples, payload_size) {
    rc = flowmq_protocol_encode_frame_into_internal(
        &input, BENCH_MAX_FRAME_BYTES, contiguous_storage,
        sizeof(contiguous_storage), &encoded_size);
  }
  check_equal(rc, SALTS_OK);

  benchmark_bytes(segmented_title, samples, payload_size) {
    rc = flowmq_protocol_encode_frame_segmented_into_internal(
        &input, BENCH_MAX_FRAME_BYTES, segments, BENCH_SEGMENT_CAPACITY,
        framing, sizeof(framing), &segment_count, &encoded_size);
  }
  check_equal(rc, SALTS_OK);

  benchmark_bytes(decode_title, samples, payload_size) {
    memset(&output, 0, sizeof(output));
    rc = flowmq_protocol_decode_frame(encoded, tstr_len(encoded), BENCH_MAX_FRAME_BYTES, &output,
                                      &consumed);
    flowmq_protocol_frame_cleanup(&output);
  }
  check_equal(rc, SALTS_OK);
  check_equal(consumed, tstr_len(encoded));
  tstr_free(encoded);
}

spec("standalone FlowMQ protocol benchmark") {
  bench("contiguous FMQ/6 framing") {
    bench_protocol_roundtrip(BENCH_SMALL_PAYLOAD_BYTES, BENCH_SMALL_SAMPLES,
                             "allocating encode 64-byte payload",
                             "contiguous encode 64-byte payload",
                             "segmented framing 64-byte logical payload",
                             "decode 64-byte payload");
    bench_protocol_roundtrip(BENCH_LARGE_PAYLOAD_BYTES, BENCH_LARGE_SAMPLES,
                             "allocating encode 64-KiB payload",
                             "contiguous encode 64-KiB payload",
                             "segmented framing 64-KiB logical payload",
                             "decode 64-KiB payload");
  }
}
