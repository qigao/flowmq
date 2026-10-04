#include "flowmq_owned_stream.h"

#include "flowmq_protocol.h"
#include "salts_error.h"
#include "tinytest.h"

#include <stdlib.h>
#include <string.h>

typedef struct owned_stream_release_probe_s {
  size_t calls;
} owned_stream_release_probe_t;

static void owned_stream_release(void *data, void *user_data) {
  owned_stream_release_probe_t *probe =
      (owned_stream_release_probe_t *)user_data;
  if (probe != NULL) ++probe->calls;
  free(data);
}

static mem_slice_t owned_stream_copy_slice(
    const void *data, size_t size, owned_stream_release_probe_t *probe) {
  void *copy = malloc(size);
  mem_buffer_t *buffer;
  mem_slice_t slice = {0};
  if (copy == NULL) return slice;
  memcpy(copy, data, size);
  buffer = mem_wrap_external(copy, size, owned_stream_release, probe);
  if (buffer == NULL) {
    free(copy);
    return slice;
  }
  slice = mem_slice(buffer, 0u, size);
  mem_buffer_release(buffer);
  return slice;
}

spec("flowmq bounded owned receive stream") {
  it("moves ownership and peeks across receive boundaries") {
    static const unsigned char first[] = {'a', 'b', 'c'};
    static const unsigned char second[] = {'d', 'e', 'f', 'g', 'h'};
    unsigned char observed[4] = {0};
    owned_stream_release_probe_t first_release = {0};
    owned_stream_release_probe_t second_release = {0};
    flowmq_owned_stream_t stream = FLOWMQ_OWNED_STREAM_INIT;
    mem_slice_t first_slice =
        owned_stream_copy_slice(first, sizeof(first), &first_release);
    mem_slice_t second_slice =
        owned_stream_copy_slice(second, sizeof(second), &second_release);

    check_not_null(first_slice.buffer);
    check_not_null(second_slice.buffer);
    check_equal(flowmq_owned_stream_append_move(&stream, &first_slice),
                SALTS_OK);
    check_equal(flowmq_owned_stream_append_move(&stream, &second_slice),
                SALTS_OK);
    check_null(first_slice.buffer);
    check_null(second_slice.buffer);
    check_equal(flowmq_owned_stream_size(&stream), (size_t)8u);
    check_equal(flowmq_owned_stream_segment_count(&stream), (size_t)2u);

    check_equal(flowmq_owned_stream_peek(&stream, 2u, observed,
                                         sizeof(observed)),
                SALTS_OK);
    check_equal(memcmp(observed, "cdef", sizeof(observed)), 0);

    check_equal(flowmq_owned_stream_consume(&stream, 4u), SALTS_OK);
    check_equal(first_release.calls, (size_t)1u);
    check_equal(second_release.calls, (size_t)0u);
    check_equal(flowmq_owned_stream_size(&stream), (size_t)4u);
    memset(observed, 0, sizeof(observed));
    check_equal(flowmq_owned_stream_peek(&stream, 0u, observed,
                                         sizeof(observed)),
                SALTS_OK);
    check_equal(memcmp(observed, "efgh", sizeof(observed)), 0);

    flowmq_owned_stream_reset(&stream);
    check_equal(second_release.calls, (size_t)1u);
    check_equal(flowmq_owned_stream_size(&stream), (size_t)0u);
  }

  it("probes a multi-packet frame across a fragmented header and replays it") {
    enum { PAYLOAD_BYTES = FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 37u };
    static unsigned char payload[PAYLOAD_BYTES];
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_frame_t output = {0};
    flowmq_stream_decoder_t decoder = {0};
    flowmq_owned_stream_t stream = FLOWMQ_OWNED_STREAM_INIT;
    owned_stream_release_probe_t releases[3] = {{0}, {0}, {0}};
    mem_slice_t slices[3] = {{0}, {0}, {0}};
    tstr encoded = NULL;
    size_t cuts[4];
    size_t frame_size = 0u;
    size_t consumed = 0u;

    for (size_t i = 0u; i < sizeof(payload); ++i)
      payload[i] = (unsigned char)((i * 19u + 7u) & 0xffu);

    input.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    input.pattern = FLOWMQ_PROTOCOL_PUSH;
    input.message_id = UINT64_C(99);
    input.payload = vstr_from_buf((const char *)payload, sizeof(payload));
    check_equal(flowmq_protocol_encode_frame(&input, sizeof(payload), &encoded),
                SALTS_OK);
    check_not_null(encoded);

    cuts[0] = 0u;
    cuts[1] = 11u;
    cuts[2] = FLOWMQ_PROTOCOL_HEADER_SIZE + 4096u;
    cuts[3] = tstr_len(encoded);
    for (size_t i = 0u; i < 3u; ++i) {
      slices[i] = owned_stream_copy_slice(
          encoded + cuts[i], cuts[i + 1u] - cuts[i], &releases[i]);
      check_not_null(slices[i].buffer);
      check_equal(flowmq_owned_stream_append_move(&stream, &slices[i]),
                  SALTS_OK);
      if (i < 2u)
        check_equal(flowmq_owned_stream_first_frame_size(
                        &stream, sizeof(payload), &frame_size),
                    FLOWMQ_PROTOCOL_INCOMPLETE);
    }

    check_equal(flowmq_owned_stream_first_frame_size(
                    &stream, sizeof(payload), &frame_size),
                SALTS_OK);
    check_equal(frame_size, tstr_len(encoded));
    check_equal(flowmq_stream_decoder_prepare(&decoder, sizeof(payload)),
                SALTS_OK);
    check_equal(flowmq_owned_stream_replay(&stream, &decoder), SALTS_OK);
    check_equal(flowmq_owned_stream_size(&stream), (size_t)0u);
    for (size_t i = 0u; i < 3u; ++i)
      check_equal(releases[i].calls, (size_t)1u);

    check_equal(flowmq_stream_decoder_next(&decoder, &output, &consumed),
                SALTS_OK);
    check_equal(consumed, tstr_len(encoded));
    check_equal(output.kind, FLOWMQ_PROTOCOL_FRAME_DATA);
    check_equal(output.message_id, UINT64_C(99));
    check_equal(output.payload.len, sizeof(payload));
    check_equal(memcmp(output.payload.data, payload, sizeof(payload)), 0);
    flowmq_protocol_frame_cleanup(&output);
    check_equal(flowmq_stream_decoder_consume(&decoder, consumed), SALTS_OK);
    flowmq_stream_decoder_destroy(&decoder);
    tstr_free(encoded);
  }

  it("fails bounded admission without stealing the overflowing slice") {
    unsigned char bytes[FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY + 1u] = {0};
    owned_stream_release_probe_t release = {0};
    flowmq_owned_stream_t stream = FLOWMQ_OWNED_STREAM_INIT;
    mem_slice_t overflow = {0};

    for (size_t i = 0u; i < FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY; ++i) {
      mem_slice_t slice =
          owned_stream_copy_slice(&bytes[i], 1u, &release);
      check_not_null(slice.buffer);
      check_equal(flowmq_owned_stream_append_move(&stream, &slice), SALTS_OK);
      check_null(slice.buffer);
    }
    check_equal(flowmq_owned_stream_segment_count(&stream),
                (size_t)FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY);

    overflow = owned_stream_copy_slice(
        &bytes[FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY], 1u, &release);
    check_not_null(overflow.buffer);
    check_equal(flowmq_owned_stream_append_move(&stream, &overflow),
                SALTS_ENOBUFS);
    check_not_null(overflow.buffer);
    check_equal(release.calls, (size_t)0u);

    flowmq_owned_stream_reset(&stream);
    check_equal(release.calls,
                (size_t)FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY);
    mem_slice_release(&overflow);
    check_equal(release.calls,
                (size_t)FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY + 1u);
  }
}
