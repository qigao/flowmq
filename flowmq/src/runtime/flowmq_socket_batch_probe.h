#ifndef FLOWMQ_SOCKET_BATCH_PROBE_H
#define FLOWMQ_SOCKET_BATCH_PROBE_H

#include "flowmq_socket.h"
#include <cnet/cnet.h>

/* Benchmark-only copy-lane counters, compiled solely into the probe target.
 * One progress thread owns the socket and reads snapshots between operations.
 * These count successful DATA admission into CNet, not native submissions or
 * peer acknowledgements. Qualification covers single-part PAIR copy sends;
 * retained and multipart admission are intentionally outside this probe.
 * Fixed storage, no payload references, callbacks, atomics or allocations.
 * The benchmark bounds the measured workload well below uint64_t overflow.
 */
enum { FLOWMQ_BATCH_PROBE_MAX_FRAMES = 128u };

typedef struct flowmq_socket_batch_probe_s {
  uint64_t direct_writes;
  uint64_t queued_writes;
  uint64_t messages;
  uint64_t payload_bytes;
  uint64_t queued_ranges;
  uint64_t queued_range_histogram[FLOWMQ_BATCH_PROBE_MAX_FRAMES + 1u];
  /* Nested wall-clock intervals, not independently additive CPU samples.
   * receive_callback_ns is included in client_poll_ns. */
  uint64_t drive_calls;
  uint64_t listener_ns;
  uint64_t client_poll_ns;
  uint64_t local_progress_ns;
  uint64_t receive_callback_ns;
  uint64_t receive_callbacks;
  uint64_t receive_bytes;
  uint64_t receive_fast_slices;
  uint64_t receive_stream_slices;
  uint64_t receive_decoder_slices;
  uint64_t coalesced_writes;
  uint64_t coalesced_ranges;
  uint64_t coalesced_bytes_peak;
  uint64_t submitted_ranges;
} flowmq_socket_batch_probe_t;

#if defined(FLOWMQ_BATCH_PROBE)
int flowmq_socket_batch_probe_read(const flowmq_socket_t *socket,
                                 flowmq_socket_batch_probe_t *out);
/* Private interventions, startup-only: 0 = historical SG/32; 1 = coalesce/32;
 * 2 = coalesce/128; 3 = production bounded-copy policy. Modes 1/2 keep the
 * historical interventions without the production small-frame/byte limits.
 * Retained application payloads are unchanged in every mode. */
int flowmq_socket_batch_probe_coalesce(flowmq_socket_t *socket, int mode);
#endif

#endif
