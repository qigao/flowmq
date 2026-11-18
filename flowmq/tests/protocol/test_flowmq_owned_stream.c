#include "flowmq_owned_stream.h"

#include "flowmq_protocol.h"
#include <core/error.h>
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

spec("flowmq protocol bounded owned receive stream") {
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

  it("decodes a multi-packet frame across fragmented headers") {
    enum { PAYLOAD_BYTES = FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 37u };
    static unsigned char payload[PAYLOAD_BYTES];
    flowmq_protocol_frame_t input = {0};
    flowmq_protocol_frame_t output = {0};
    tstr assembled = NULL;
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
    assembled = tstr_new_len(NULL, frame_size);
    check_not_null(assembled);
    check_equal(flowmq_owned_stream_peek(&stream, 0u, assembled, frame_size), SALTS_OK);
    check_equal(flowmq_protocol_decode_frame(assembled, frame_size, sizeof(payload),
                                             &output, &consumed), SALTS_OK);
    check_equal(consumed, frame_size);
    check_equal(flowmq_owned_stream_consume(&stream, frame_size), SALTS_OK);
    for (size_t i = 0u; i < 3u; ++i)
      check_equal(releases[i].calls, (size_t)1u);
    check_equal(output.kind, FLOWMQ_PROTOCOL_FRAME_DATA);
    check_equal(output.message_id, UINT64_C(99));
    check_equal(output.payload.len, sizeof(payload));
    check_equal(memcmp(output.payload.data, payload, sizeof(payload)), 0);
    flowmq_protocol_frame_cleanup(&output);
    tstr_free(assembled);
    tstr_free(encoded);
  }

  it("projects a multi-packet DATA frame into retained payload ranges") {
    enum { PAYLOAD_BYTES = FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE * 2u + 113u };
    static unsigned char payload[PAYLOAD_BYTES];
    static unsigned char observed[PAYLOAD_BYTES];
    flowmq_protocol_frame_t frame = {0};
    flowmq_owned_stream_t stream = FLOWMQ_OWNED_STREAM_INIT;
    flowmq_owned_data_projection_t projection =
        FLOWMQ_OWNED_DATA_PROJECTION_INIT;
    owned_stream_release_probe_t releases[4] = {{0}, {0}, {0}, {0}};
    mem_slice_t slices[4] = {{0}, {0}, {0}, {0}};
    tstr encoded = NULL;
    size_t cuts[5];
    size_t observed_size = 0u;

    for (size_t i = 0u; i < sizeof(payload); ++i)
      payload[i] = (unsigned char)((i * 23u + 5u) & 0xffu);
    frame.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    frame.pattern = FLOWMQ_PROTOCOL_PUSH;
    frame.message_id = UINT64_C(701);
    frame.more = 1;
    frame.payload =
        vstr_from_buf((const char *)payload, sizeof(payload));
    check_equal(flowmq_protocol_encode_frame(
                    &frame, sizeof(payload), &encoded),
                SALTS_OK);
    check_not_null(encoded);

    cuts[0] = 0u;
    cuts[1] = 17u;
    cuts[2] = FLOWMQ_PROTOCOL_HEADER_SIZE + 8192u;
    cuts[3] = FLOWMQ_PROTOCOL_HEADER_SIZE +
              FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE + 9u;
    cuts[4] = tstr_len(encoded);
    for (size_t i = 0u; i < 4u; ++i) {
      slices[i] = owned_stream_copy_slice(
          encoded + cuts[i], cuts[i + 1u] - cuts[i], &releases[i]);
      check_not_null(slices[i].buffer);
      check_equal(flowmq_owned_stream_append_move(&stream, &slices[i]),
                  SALTS_OK);
    }

    check_equal(flowmq_owned_stream_project_first_data(
                    &stream, sizeof(payload), &projection),
                SALTS_OK);
    check_equal(projection.pattern, FLOWMQ_PROTOCOL_PUSH);
    check_equal(projection.message_id, UINT64_C(701));
    check_equal(projection.payload_size, sizeof(payload));
    check_equal(projection.frame_size, tstr_len(encoded));
    check_equal(projection.more, 1);
    check_true(projection.segment_count >= (size_t)3u);
    check_true(projection.segment_count <=
               (size_t)FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY);

    for (size_t i = 0u; i < projection.segment_count; ++i) {
      check_not_null(projection.segments[i].buffer);
      check_true(observed_size <= sizeof(observed));
      check_true(projection.segments[i].length <=
                 sizeof(observed) - observed_size);
      memcpy(observed + observed_size, projection.segments[i].data,
             projection.segments[i].length);
      observed_size += projection.segments[i].length;
    }
    check_equal(observed_size, sizeof(payload));
    check_equal(memcmp(observed, payload, sizeof(payload)), 0);

    check_equal(flowmq_owned_stream_consume(
                    &stream, projection.frame_size),
                SALTS_OK);
    check_equal(flowmq_owned_stream_size(&stream), (size_t)0u);
    /*
     * The first raw backing contains only the first 17 bytes of the FMQ
     * header, so the payload projection retains no reference to it. Consuming
     * the raw frame releases that header-only owner immediately. The remaining
     * backings contribute payload ranges and stay alive through the projection.
     */
    check_equal(releases[0].calls, (size_t)1u);
    for (size_t i = 1u; i < 4u; ++i)
      check_equal(releases[i].calls, (size_t)0u);

    flowmq_owned_data_projection_reset(&projection);
    for (size_t i = 0u; i < 4u; ++i)
      check_equal(releases[i].calls, (size_t)1u);
    tstr_free(encoded);
  }

  it("distinguishes control metadata from retained DATA") {
    flowmq_protocol_frame_t frame = {0};
    flowmq_owned_stream_t stream = FLOWMQ_OWNED_STREAM_INIT;
    flowmq_owned_data_projection_t projection =
        FLOWMQ_OWNED_DATA_PROJECTION_INIT;
    owned_stream_release_probe_t release = {0};
    mem_slice_t slice = {0};
    tstr encoded = NULL;

    frame.kind = FLOWMQ_PROTOCOL_FRAME_PING;
    frame.pattern = FLOWMQ_PROTOCOL_PAIR;
    check_equal(flowmq_protocol_encode_frame(&frame, 1024u, &encoded),
                SALTS_OK);
    check_not_null(encoded);
    slice = owned_stream_copy_slice(
        encoded, tstr_len(encoded), &release);
    check_not_null(slice.buffer);
    check_equal(flowmq_owned_stream_append_move(&stream, &slice), SALTS_OK);

    check_equal(flowmq_owned_stream_project_first_data(
                    &stream, 1024u, &projection),
                SALTS_ENOTSUP);
    check_equal(projection.segment_count, (size_t)0u);
    check_equal(flowmq_owned_stream_size(&stream), tstr_len(encoded));
    check_equal(release.calls, (size_t)0u);

    flowmq_owned_stream_reset(&stream);
    check_equal(release.calls, (size_t)1u);
    tstr_free(encoded);
  }

  it("keeps a coalesced tail owned until its projection is released") {
    static const char first_payload[] = "first-owned-frame";
    static const char second_payload[] = "second-owned-frame";
    flowmq_protocol_frame_t first = {0};
    flowmq_protocol_frame_t second = {0};
    flowmq_owned_data_projection_t projection = FLOWMQ_OWNED_DATA_PROJECTION_INIT;
    flowmq_owned_stream_t stream = FLOWMQ_OWNED_STREAM_INIT;
    owned_stream_release_probe_t release = {0};
    tstr first_encoded = NULL;
    tstr second_encoded = NULL;
    unsigned char *combined;
    mem_slice_t combined_slice;
    size_t first_size = 0u;
    size_t second_size = 0u;

    first.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    first.pattern = FLOWMQ_PROTOCOL_PUSH;
    first.message_id = UINT64_C(101);
    first.payload =
        vstr_from_buf(first_payload, sizeof(first_payload) - 1u);
    second.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    second.pattern = FLOWMQ_PROTOCOL_PUSH;
    second.message_id = UINT64_C(102);
    second.payload =
        vstr_from_buf(second_payload, sizeof(second_payload) - 1u);

    check_equal(flowmq_protocol_encode_frame(&first, 1024u, &first_encoded),
                SALTS_OK);
    check_equal(flowmq_protocol_encode_frame(&second, 1024u, &second_encoded),
                SALTS_OK);
    check_not_null(first_encoded);
    check_not_null(second_encoded);

    combined =
        (unsigned char *)malloc(tstr_len(first_encoded) +
                                tstr_len(second_encoded));
    check_not_null(combined);
    memcpy(combined, first_encoded, tstr_len(first_encoded));
    memcpy(combined + tstr_len(first_encoded), second_encoded,
           tstr_len(second_encoded));
    combined_slice = owned_stream_copy_slice(
        combined, tstr_len(first_encoded) + tstr_len(second_encoded),
        &release);
    free(combined);
    check_not_null(combined_slice.buffer);
    check_equal(flowmq_owned_stream_append_move(&stream, &combined_slice),
                SALTS_OK);

    check_equal(flowmq_owned_stream_first_frame_size(
                    &stream, 1024u, &first_size),
                SALTS_OK);
    check_equal(first_size, tstr_len(first_encoded));
    check_equal(flowmq_owned_stream_project_first_data(&stream, 1024u, &projection),
                SALTS_OK);
    check_equal(projection.message_id, UINT64_C(101));
    check_equal(projection.segment_count, 1u);
    check_equal(memcmp(projection.segments[0].data, first_payload,
                       sizeof(first_payload) - 1u), 0);
    check_equal(flowmq_owned_stream_consume(&stream, first_size), SALTS_OK);
    flowmq_owned_data_projection_reset(&projection);
    check_equal(release.calls, 0u);
    check_equal(flowmq_owned_stream_size(&stream), tstr_len(second_encoded));

    check_equal(flowmq_owned_stream_first_frame_size(&stream, 1024u, &second_size),
                SALTS_OK);
    check_equal(second_size, tstr_len(second_encoded));
    check_equal(flowmq_owned_stream_project_first_data(&stream, 1024u, &projection),
                SALTS_OK);
    check_equal(projection.message_id, UINT64_C(102));
    check_equal(projection.segment_count, 1u);
    check_equal(memcmp(projection.segments[0].data, second_payload,
                       sizeof(second_payload) - 1u), 0);
    check_equal(flowmq_owned_stream_consume(&stream, second_size), SALTS_OK);
    check_equal(release.calls, 0u);
    flowmq_owned_data_projection_reset(&projection);
    check_equal(release.calls, 1u);
    check_equal(flowmq_owned_stream_size(&stream), 0u);
    tstr_free(first_encoded);
    tstr_free(second_encoded);
  }

  it("assembles a 1 MiB frame from thousands of TCP fragments with bounded blocks") {
    enum { PAYLOAD_BYTES = 1024u * 1024u };
    static unsigned char payload[PAYLOAD_BYTES];
    flowmq_protocol_frame_t frame = {0};
    flowmq_owned_stream_t stream = FLOWMQ_OWNED_STREAM_INIT;
    flowmq_owned_data_projection_t projection = FLOWMQ_OWNED_DATA_PROJECTION_INIT;
    owned_stream_release_probe_t release = {0};
    mem_pool_t pool = {0};
    tstr encoded = NULL;
    size_t callbacks = 0u;
    size_t offset = 0u;
    size_t total = 0u;

    for (size_t i = 0u; i < sizeof(payload); ++i)
      payload[i] = (unsigned char)((i * 17u + 3u) & 0xffu);
    frame.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
    frame.pattern = FLOWMQ_PROTOCOL_PUSH;
    frame.message_id = UINT64_C(805);
    frame.payload = vstr_from_buf((const char *)payload, sizeof(payload));
    check_equal(mem_init(&pool, 0u), 0);
    check_equal(flowmq_protocol_encode_frame(&frame, sizeof(payload), &encoded), SALTS_OK);
    check_not_null(encoded);
    while (offset < tstr_len(encoded)) {
      size_t take = offset < 97u ? 1u : 137u;
      mem_slice_t slice;
      if (take > tstr_len(encoded) - offset) take = tstr_len(encoded) - offset;
      slice = owned_stream_copy_slice(encoded + offset, take, &release);
      check_not_null(slice.buffer);
      check_equal(flowmq_owned_stream_assemble(
                      &stream, &pool, &slice, tstr_len(encoded)), SALTS_OK);
      check_null(slice.buffer);
      offset += take;
      ++callbacks;
    }
    check_true(callbacks > 1000u);
    check_equal(release.calls, callbacks);
    check_equal(flowmq_owned_stream_segment_count(&stream), 9u);
    check_equal(flowmq_owned_stream_project_first_data(
                    &stream, sizeof(payload), &projection), SALTS_OK);
    check_true(projection.segment_count <= FLOWMQ_OWNED_STREAM_SEGMENT_CAPACITY);
    check_equal(flowmq_owned_stream_consume(&stream, projection.frame_size), SALTS_OK);
    /* Published payload remains immutable when assembly starts a new frame. */
    {
      mem_slice_t next = owned_stream_copy_slice("next", 4u, &release);
      check_equal(flowmq_owned_stream_assemble(&stream, &pool, &next, 32u), SALTS_OK);
      flowmq_owned_stream_reset(&stream);
    }
    for (size_t i = 0u; i < projection.segment_count; ++i) {
      const mem_slice_t *part = &projection.segments[i];
      check_true(part->length <= sizeof(payload) - total);
      check_equal(memcmp(part->data, payload + total, part->length), 0);
      total += part->length;
    }
    check_equal(total, sizeof(payload));
    flowmq_owned_data_projection_reset(&projection);
    tstr_free(encoded);
    mem_destroy(&pool);
  }

  it("seals a published block while assembling a maximum frame behind its prefix") {
    static char payload[1024u * 1024u];
    flowmq_protocol_frame_t frame = {.kind = FLOWMQ_PROTOCOL_FRAME_DATA,
        .pattern = FLOWMQ_PROTOCOL_PUSH, .message_id = 806u,
        .payload = {.data = "stable", .len = 6u}};
    flowmq_owned_stream_t stream = FLOWMQ_OWNED_STREAM_INIT;
    flowmq_owned_data_projection_t first = FLOWMQ_OWNED_DATA_PROJECTION_INIT;
    flowmq_owned_data_projection_t next = FLOWMQ_OWNED_DATA_PROJECTION_INIT;
    owned_stream_release_probe_t release = {0};
    mem_pool_t pool = {0};
    tstr small = NULL, large = NULL;
    unsigned char prefix[FLOWMQ_PROTOCOL_HEADER_SIZE + 6u + 11u];
    mem_slice_t slice;
    size_t used;
    check_equal(mem_init(&pool, 0u), 0);
    check_equal(flowmq_protocol_encode_frame(&frame, sizeof(payload), &small), SALTS_OK);
    frame.message_id = 807u;
    frame.payload = vstr_from_buf(payload, sizeof(payload));
    check_equal(flowmq_protocol_encode_frame(&frame, sizeof(payload), &large), SALTS_OK);
    memcpy(prefix, small, tstr_len(small));
    memcpy(prefix + tstr_len(small), large, 11u);
    slice = owned_stream_copy_slice(prefix, sizeof(prefix), &release);
    check_equal(flowmq_owned_stream_assemble(&stream, &pool, &slice,
                                            tstr_len(large) * 2u), SALTS_OK);
    check_equal(flowmq_owned_stream_project_first_data(&stream, sizeof(payload), &first), SALTS_OK);
    check_equal(first.segment_count, 1u);
    used = mem_buffer_used(first.segments[0].buffer);
    check_equal(flowmq_owned_stream_consume(&stream, first.frame_size), SALTS_OK);
    slice = owned_stream_copy_slice(large + 11u, tstr_len(large) - 11u, &release);
    check_equal(flowmq_owned_stream_assemble(&stream, &pool, &slice,
                                            tstr_len(large) * 2u), SALTS_OK);
    check_equal(mem_buffer_used(first.segments[0].buffer), used);
    check_equal(memcmp(first.segments[0].data, "stable", 6u), 0);
    check_equal(flowmq_owned_stream_project_first_data(&stream, sizeof(payload), &next), SALTS_OK);
    check_true(next.segment_count >= 16u);
    check_true(next.segment_count <= 25u);
    check_equal(flowmq_owned_stream_consume(&stream, next.frame_size), SALTS_OK);
    flowmq_owned_data_projection_reset(&next);
    check_equal(memcmp(first.segments[0].data, "stable", 6u), 0);
    flowmq_owned_data_projection_reset(&first);
    tstr_free(small);
    tstr_free(large);
    mem_destroy(&pool);
  }

  it("rejects assembly byte overflow before consuming caller ownership") {
    flowmq_owned_stream_t stream = FLOWMQ_OWNED_STREAM_INIT;
    owned_stream_release_probe_t release = {0};
    mem_slice_t first = owned_stream_copy_slice("abc", 3u, &release);
    mem_slice_t next = owned_stream_copy_slice("de", 2u, &release);
    check_equal(flowmq_owned_stream_assemble(&stream, mem_global(), &first, 4u), SALTS_OK);
    check_equal(flowmq_owned_stream_assemble(&stream, mem_global(), &next, 4u), SALTS_ENOBUFS);
    check_equal(flowmq_owned_stream_size(&stream), 3u);
    check_not_null(next.buffer);
    check_equal(release.calls, 1u);
    flowmq_owned_stream_reset(&stream);
    mem_slice_release(&next);
    check_equal(release.calls, 2u);
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
