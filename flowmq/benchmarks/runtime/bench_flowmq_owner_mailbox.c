#include "flowmq_owner_internal.h"
#include "flowmq_socket_external_internal.h"
#include "flowmq_bench_metrics.h"
#include "tinytest.h"
#include <salts/clock.h>
#include <salts/disruptor.h>
#include <salts/thread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
#include <zmq.h>
#include <errno.h>
#endif

enum { MAIL_LANES = 8, MAIL_PRODUCERS = 2, MAIL_CAPACITY = 128,
       MAIL_BUFFERS = 64, MAIL_BATCH = 16, MAIL_MAX_BYTES = 65536,
       MAIL_WAIT_MS = 1000, MAIL_DEADLINE_MS = 30000, MAIL_REPEATS = 4,
       MAIL_ROUNDS = 32, MAIL_SATURATED_ROUNDS = 2048, MAIL_RX_SEGMENTS = 64,
       MAIL_ZMQ_COPY = 4, MAIL_ZMQ_OWNED = 5,
       MAIL_ZMQ_COPY_EVENTS = 6, MAIL_ZMQ_OWNED_EVENTS = 7 };

typedef struct mail_header_s {
  uint64_t lane, producer, sequence, prepared_ns;
} mail_header;
typedef struct mail_entry_s { mem_slice_t slice; } mail_entry;
typedef struct mail_local_batch_s {
  mail_entry entries[MAIL_BATCH];
  size_t count, next, producer;
} mail_local_batch;
typedef struct mail_release_counter_s { atomic_size_t count; } mail_release_counter;
typedef struct mail_run_s mail_run;
typedef struct mail_lane_s mail_lane;
typedef struct mail_producer_s {
  mail_lane *lane;
  size_t id, next_buffer, published, full, buffer_waits, buffer_probes, wakes;
  mem_buffer_t *buffers[MAIL_BUFFERS];
  unsigned char *storage;
  mail_release_counter released;
} mail_producer;
struct mail_lane_s {
  mail_run *run;
  size_t id;
  flowmq_ctx_t *ctx;
  flowmq_owner_t *owner;
  flowmq_socket_t *sender, *receiver;
  disruptor_t *queue;
  mail_producer producers[MAIL_PRODUCERS];
  uint64_t *latencies;
  size_t received, admitted, polls, send_busy, segments, app_full, peak_inflight, event_checks;
  flowmq_retained_queue_stats_t sg_stats;
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  void *zmq_ctx, *zmq_sender, *zmq_receiver;
#endif
  uint64_t finished_ns;
};
struct mail_run_s {
  cmeta_mutex_t mutex;
  cmeta_cond_t changed;
  atomic_int error;
  size_t ready, writers_ready, done, rounds, bytes, cancel_after, admission_limit;
  int limit_bytes;
  int started, cleanup, notify, retained, direct;
  uint32_t pause_ms;
};

static void mail_error(mail_run *run, int status) {
  int expected = SALTS_OK;
  if (status != SALTS_OK) atomic_compare_exchange_strong(&run->error, &expected, status);
}
static int mail_status(mail_run *run, uint64_t deadline) {
  const int status = atomic_load(&run->error);
  return status != SALTS_OK ? status : cmeta_monotonic_ms() >= deadline ? SALTS_ETIMEDOUT : SALTS_OK;
}
static void mail_released(void *data, void *user) {
  (void)data;
  mail_release_counter *counter = user;
  atomic_fetch_add(&counter->count, 1u);
}
static unsigned char mail_byte(const mail_header *h, size_t offset) {
  return (unsigned char)(h->sequence + h->producer * 31u + h->lane * 17u + offset);
}
static void mail_fill(unsigned char *data, size_t bytes, const mail_header *h) {
  memcpy(data, h, sizeof(*h));
  for (size_t i = sizeof(*h); i < bytes; ++i) data[i] = mail_byte(h, i);
}
static void mail_start_wait(mail_run *run) {
  cmeta_mutex_lock(&run->mutex);
  while (!run->started) cmeta_cond_wait(&run->changed, &run->mutex);
  cmeta_mutex_unlock(&run->mutex);
}

/* Exactly one producer owns each base reference. All other users must already
 * hold a slice reference; no one may acquire from a saved bare buffer pointer.
 * Thus ref_count == 1 establishes exclusive reuse, not approximate queue size. */
static int mail_try_prepare(mail_producer *p, uint64_t sequence, mem_slice_t *out) {
  mail_run *run = p->lane->run;
  for (size_t i = 0u; i < MAIL_BUFFERS; ++i) {
    const size_t index = (p->next_buffer + i) % MAIL_BUFFERS;
    mem_buffer_t *buffer = p->buffers[index];
    if (mem_buffer_ref_count(buffer) != 1u) continue;
    p->buffer_probes += i + 1u;
    const mail_header h = {p->lane->id, p->id, sequence, cmeta_hrtime()};
    mail_fill((unsigned char *)mem_buffer_data(buffer), run->bytes, &h);
    *out = mem_slice(buffer, 0u, run->bytes);
    if (out->buffer == NULL) return SALTS_ENOMEM;
    p->next_buffer = (index + 1u) % MAIL_BUFFERS;
    return SALTS_OK;
  }
  p->buffer_probes += MAIL_BUFFERS;
  ++p->buffer_waits;
  return SALTS_EBUSY;
}

static int mail_prepare(mail_producer *p, uint64_t sequence, mem_slice_t *out,
                        uint64_t deadline) {
  for (;;) {
    int status = mail_status(p->lane->run, deadline);
    if (status != SALTS_OK) return status;
    status = mail_try_prepare(p, sequence, out);
    if (status != SALTS_EBUSY) return status;
    cmeta_thread_yield();
  }
}

/* One bounded staging batch, not another mailbox. Never wait here: the same
 * owner must receive and advance I/O to make retained sources reusable. */
static int mail_direct_entry(mail_lane *lane, mail_local_batch *batch, mail_entry **out) {
  if (batch->next == MAIL_BATCH) batch->count = batch->next = 0u;
  if (batch->count != MAIL_BATCH) {
    mail_producer *p = &lane->producers[batch->producer];
    if (p->published == lane->run->rounds * MAIL_BATCH) return SALTS_EBUSY;
    while (batch->count < MAIL_BATCH) {
      const int status = mail_try_prepare(p, p->published + batch->count,
                                          &batch->entries[batch->count].slice);
      if (status != SALTS_OK) return status;
      ++batch->count;
    }
    p->published += MAIL_BATCH;
    batch->producer = (batch->producer + 1u) % MAIL_PRODUCERS;
  }
  *out = &batch->entries[batch->next];
  return SALTS_OK;
}

static void mail_produce(void *arg) {
  mail_producer *p = arg;
  mail_run *run = p->lane->run;
  cmeta_mutex_lock(&run->mutex);
  ++run->writers_ready;
  cmeta_cond_broadcast(&run->changed);
  cmeta_mutex_unlock(&run->mutex);
  mail_start_wait(run);
  const uint64_t deadline = cmeta_monotonic_ms() + MAIL_DEADLINE_MS;
  int status = SALTS_OK;
  for (size_t round = 0u; status == SALTS_OK && round < run->rounds; ++round) {
    mem_slice_t prepared[MAIL_BATCH] = {0};
    disruptor_sequence_range_t range = {0};
    if (run->pause_ms != 0u) cmeta_sleep_ms(run->pause_ms);
    for (size_t i = 0u; status == SALTS_OK && i < MAIL_BATCH; ++i)
      status = mail_prepare(p, round * MAIL_BATCH + i, &prepared[i], deadline);
    while (status == SALTS_OK &&
           !disruptor_publisher_try_claim_n(p->lane->queue, MAIL_BATCH, &range)) {
      ++p->full;
      status = mail_status(run, deadline);
      cmeta_thread_yield();
    }
    if (status == SALTS_OK) {
      for (size_t i = 0u; i < MAIL_BATCH; ++i) {
        const disruptor_cursor_t cursor = {range.first_sequence + i};
        mail_entry *entry = disruptor_acquire_entry(p->lane->queue, &cursor);
        entry->slice = prepared[i];
        memset(&prepared[i], 0, sizeof(prepared[i]));
      }
      /* Publish accepts the range even behind another producer's sequence
       * gap. Never retry publication or mutate the moved slice descriptors. */
      if (!disruptor_publisher_publish_range(p->lane->queue, &range)) status = SALTS_EPROTO;
      else {
        p->published += MAIL_BATCH;
        if (run->notify) {
          ++p->wakes;
          status = flowmq_owner_internal_wake(p->lane->owner);
        }
      }
    }
    for (size_t i = 0u; i < MAIL_BATCH; ++i) mem_slice_release(&prepared[i]);
  }
  mail_error(run, status);
  /* Also wake on terminal producer failure. Publication is already committed
   * if its wake failed; the controller stops the run, never resubmits it. */
  if (status != SALTS_OK) (void)flowmq_owner_internal_wake(p->lane->owner);
}

#if defined(FLOWMQ_BENCH_WITH_ZMQ)
static int mail_zmq_open(mail_lane *lane) {
  char endpoint[128];
  size_t endpoint_size = sizeof(endpoint);
  const int hwm = MAIL_CAPACITY, linger = 0, immediate = 1;
  lane->zmq_ctx = zmq_ctx_new();
  if (lane->zmq_ctx == NULL) return SALTS_ENOMEM;
  if (zmq_ctx_set(lane->zmq_ctx, ZMQ_IO_THREADS, 1) != 0 ||
      zmq_ctx_get(lane->zmq_ctx, ZMQ_IO_THREADS) != 1) return SALTS_EIO;
  lane->zmq_sender = zmq_socket(lane->zmq_ctx, ZMQ_PAIR);
  lane->zmq_receiver = zmq_socket(lane->zmq_ctx, ZMQ_PAIR);
  if (lane->zmq_sender == NULL || lane->zmq_receiver == NULL) return SALTS_ENOMEM;
  void *sockets[] = {lane->zmq_sender, lane->zmq_receiver};
  for (size_t i = 0u; i < 2u; ++i)
    if (zmq_setsockopt(sockets[i], ZMQ_LINGER, &linger, sizeof(linger)) != 0 ||
        zmq_setsockopt(sockets[i], ZMQ_IMMEDIATE, &immediate, sizeof(immediate)) != 0 ||
        zmq_setsockopt(sockets[i], ZMQ_SNDHWM, &hwm, sizeof(hwm)) != 0 ||
        zmq_setsockopt(sockets[i], ZMQ_RCVHWM, &hwm, sizeof(hwm)) != 0) return SALTS_EIO;
  if (zmq_bind(lane->zmq_receiver, "tcp://127.0.0.1:*") != 0 ||
      zmq_getsockopt(lane->zmq_receiver, ZMQ_LAST_ENDPOINT, endpoint, &endpoint_size) != 0 ||
      zmq_connect(lane->zmq_sender, endpoint) != 0) return SALTS_EIO;
  return SALTS_OK;
}

static void mail_zmq_release(void *data, void *hint) {
  (void)data;
  /* libzmq may call on an arbitrary thread, even before send returns. The
   * permanent producer reference keeps buffer/callback state alive. */
  mem_buffer_release((mem_buffer_t *)hint);
}

static int mail_zmq_send(mail_lane *lane, const mem_slice_t *slice, int owned) {
  int sent, status;
  if (!owned) {
    sent = zmq_send(lane->zmq_sender, slice->data, slice->length, ZMQ_DONTWAIT);
    if (sent < 0) return zmq_errno() == EAGAIN ? SALTS_EBUSY : SALTS_EIO;
    return (size_t)sent == slice->length ? SALTS_OK : SALTS_EPROTO;
  }
  zmq_msg_t message;
  mem_buffer_t *buffer = mem_buffer_retain(slice->buffer);
  if (buffer == NULL) return SALTS_EPROTO;
  if (zmq_msg_init_data(&message, (void *)slice->data, slice->length,
                        mail_zmq_release, buffer) != 0) {
    mem_buffer_release(buffer);
    return SALTS_ENOMEM;
  }
  sent = zmq_msg_send(&message, lane->zmq_sender, ZMQ_DONTWAIT);
  status = sent < 0 ? (zmq_errno() == EAGAIN ? SALTS_EBUSY : SALTS_EIO)
                    : (size_t)sent == slice->length ? SALTS_OK : SALTS_EPROTO;
  /* Failure preserves message ownership; success leaves an empty message.
   * Closing either releases exactly the ownership still held locally. */
  if (zmq_msg_close(&message) != 0) status = SALTS_EIO;
  return status;
}

static void mail_zmq_close(mail_lane *lane) {
  if (lane->zmq_sender != NULL && zmq_close(lane->zmq_sender) != 0) mail_error(lane->run, SALTS_EIO);
  if (lane->zmq_receiver != NULL && zmq_close(lane->zmq_receiver) != 0) mail_error(lane->run, SALTS_EIO);
  if (lane->zmq_ctx != NULL && zmq_ctx_term(lane->zmq_ctx) != 0) mail_error(lane->run, SALTS_EIO);
}
#endif

static int mail_send(mail_lane *lane, const mem_slice_t *slice, int mode) {
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  if (mode >= MAIL_ZMQ_COPY)
    return mail_zmq_send(lane, slice, mode == MAIL_ZMQ_OWNED || mode == MAIL_ZMQ_OWNED_EVENTS);
#endif
  return mode >= 2 ? flowmq_socket_internal_send_slice_queued(lane->sender, slice)
       : mode != 0 ? flowmq_send_slice(lane->sender, slice, FLOWMQ_DONTWAIT)
       : flowmq_send(lane->sender, slice->data, slice->length, FLOWMQ_DONTWAIT);
}

static int mail_progress(mail_lane *lane, uint32_t wait) {
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  if (lane->run->retained >= MAIL_ZMQ_COPY_EVENTS) {
    int events = 0;
    size_t size = sizeof(events);
    /* Public readiness queries also consume sender-side credit commands.
     * This isolates DONTWAIT command throttling without changing HWM or
     * blocking a consumer that must service both sockets on this thread. */
    ++lane->event_checks;
    if (zmq_getsockopt(lane->zmq_sender, ZMQ_EVENTS, &events, &size) != 0)
      return SALTS_EIO;
  }
#endif
  /* ZMQ background workers own network progress. Its empty FlowMQ owner is
   * only the common persistent mailbox wake primitive, never an I/O proxy. */
  if (lane->run->retained >= MAIL_ZMQ_COPY && wait == 0u) return SALTS_OK;
  return flowmq_owner_internal_step(lane->owner, wait);
}

static int mail_open(mail_lane *lane) {
  flowmq_owner_config_t config = FLOWMQ_OWNER_CONFIG_INIT;
  char endpoint[128];
  size_t size = 0u, ready = 0u;
  const int hwm = MAIL_CAPACITY;
  const int reconnect = -1;
  /* Boundary fixtures leave twice the tested burst in the other byte budget,
   * so message-HWM and byte-HWM rejection are independently exercised. */
  const size_t hwm_bytes = (lane->run->admission_limit != 0u
      ? 2u * lane->run->admission_limit : MAIL_CAPACITY) * lane->run->bytes;
  int status;
  lane->ctx = flowmq_ctx_new();
  if (lane->ctx == NULL) return SALTS_ENOMEM;
  config.socket_capacity = 2u;
  lane->owner = flowmq_owner_new(lane->ctx, &config);
  if (lane->owner == NULL) return SALTS_ENOMEM;
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  if (lane->run->retained >= MAIL_ZMQ_COPY) return mail_zmq_open(lane);
#endif
  lane->sender = flowmq_owner_socket(lane->owner, FLOWMQ_PAIR);
  lane->receiver = flowmq_owner_socket(lane->owner, FLOWMQ_PAIR);
  if (lane->sender == NULL || lane->receiver == NULL) return SALTS_ENOMEM;
  if (lane->run->retained >= 2) {
    status = flowmq_socket_internal_retained_queue(
        lane->sender, lane->run->retained == 2 ? 1u : MAIL_BATCH);
    if (status != SALTS_OK) return status;
  }
  flowmq_socket_t *sockets[] = {lane->sender, lane->receiver};
  for (size_t i = 0; i < 2u; ++i) {
    status = flowmq_setsockopt(sockets[i], FLOWMQ_RECONNECT_IVL, &reconnect, sizeof(reconnect));
    if (status == SALTS_OK) status = flowmq_setsockopt(sockets[i], FLOWMQ_SNDHWM, &hwm, sizeof(hwm));
    if (status == SALTS_OK) status = flowmq_setsockopt(sockets[i], FLOWMQ_RCVHWM, &hwm, sizeof(hwm));
    if (status == SALTS_OK) status = flowmq_setsockopt(sockets[i], FLOWMQ_SNDHWM_BYTES, &hwm_bytes, sizeof(hwm_bytes));
    if (status == SALTS_OK) status = flowmq_setsockopt(sockets[i], FLOWMQ_RCVHWM_BYTES, &hwm_bytes, sizeof(hwm_bytes));
    if (status != SALTS_OK) return status;
  }
  if (lane->run->admission_limit != 0u) {
    const int limit = (int)lane->run->admission_limit;
    const size_t limit_bytes = lane->run->admission_limit * lane->run->bytes;
    status = lane->run->limit_bytes
        ? flowmq_setsockopt(lane->sender, FLOWMQ_SNDHWM_BYTES, &limit_bytes, sizeof(limit_bytes))
        : flowmq_setsockopt(lane->sender, FLOWMQ_SNDHWM, &limit, sizeof(limit));
    if (status != SALTS_OK) return status;
  }
  status = flowmq_socket_internal_bind_external(lane->receiver, "tcp://127.0.0.1:0");
  if (status == SALTS_OK) status = flowmq_last_endpoint(lane->receiver, endpoint, sizeof(endpoint), &size);
  if (status == SALTS_OK) status = flowmq_connect(lane->sender, endpoint);
  const uint64_t deadline = cmeta_monotonic_ms() + MAIL_DEADLINE_MS;
  flowmq_pollitem_t items[] = {{lane->sender, FLOWMQ_POLLOUT, 0}, {lane->receiver, FLOWMQ_POLLOUT, 0}};
  while (status == SALTS_OK && ready != 2u) {
    status = mail_status(lane->run, deadline);
    if (status == SALTS_OK) status = flowmq_owner_poll(lane->owner, items, 2u, 0u, &ready);
  }
  return status;
}

static int mail_validate_bytes(const unsigned char *data, size_t size,
                                mail_header *header, size_t *offset) {
  int status = SALTS_OK;
  for (size_t j = 0u; j < size; ++j, ++*offset) {
    const unsigned char value = data[j];
    if (*offset < sizeof(*header)) ((unsigned char *)header)[*offset] = value;
    else if (value != mail_byte(header, *offset)) status = SALTS_EPROTO;
  }
  return status;
}

/* Validate the receive vector directly, including a header split across
 * ranges. No coalesce/copy receive is used by either send policy. */
static int mail_receive(mail_lane *lane, uint64_t expected[MAIL_PRODUCERS]) {
  mem_slice_t parts[MAIL_RX_SEGMENTS];
  size_t count = 0u, offset = 0u;
  mail_header header = {0};
  int status = SALTS_OK;
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  zmq_msg_t message;
  if (lane->run->retained >= MAIL_ZMQ_COPY) {
    if (zmq_msg_init(&message) != 0) return SALTS_EIO;
    if (zmq_msg_recv(&message, lane->zmq_receiver, ZMQ_DONTWAIT) < 0) {
      status = zmq_errno() == EAGAIN ? SALTS_EBUSY : SALTS_EIO;
      if (zmq_msg_close(&message) != 0) return SALTS_EIO;
      return status;
    }
    count = 1u;
    status = mail_validate_bytes(zmq_msg_data(&message), zmq_msg_size(&message), &header, &offset);
    if (zmq_msg_more(&message)) status = SALTS_EPROTO;
  } else
#endif
  {
    memset(parts, 0, sizeof(parts));
    status = flowmq_recv_slicev(lane->receiver, parts, MAIL_RX_SEGMENTS, &count, FLOWMQ_DONTWAIT);
    if (status != SALTS_OK) return status;
    for (size_t i = 0u; i < count; ++i) {
      const int checked = mail_validate_bytes((const unsigned char *)parts[i].data,
                                               parts[i].length, &header, &offset);
      if (status == SALTS_OK) status = checked;
    }
  }
  const uint64_t completed = cmeta_hrtime();
  if (offset != lane->run->bytes || header.lane != lane->id ||
      header.producer >= MAIL_PRODUCERS || header.prepared_ns > completed)
    status = SALTS_EPROTO;
  else if (header.sequence != expected[header.producer]) status = SALTS_EPROTO;
  if (status == SALTS_OK) {
    ++expected[header.producer];
    lane->latencies[lane->received++] = completed - header.prepared_ns;
    lane->segments += count;
  }
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  if (lane->run->retained >= MAIL_ZMQ_COPY) {
    if (zmq_msg_close(&message) != 0) status = SALTS_EIO;
  } else
#endif
    for (size_t i = 0u; i < count; ++i) mem_slice_release(&parts[i]);
  return status;
}

static int mail_warmup(mail_lane *lane) {
  const mail_header header = {lane->id, 0u, 0u, cmeta_hrtime()};
  uint64_t expected[MAIL_PRODUCERS] = {0};
  mem_buffer_t *buffer = lane->producers[0].buffers[0];
  mem_slice_t slice;
  int status = SALTS_OK, sent = 0;
  const uint64_t deadline = cmeta_monotonic_ms() + MAIL_DEADLINE_MS;
  mail_fill((unsigned char *)mem_buffer_data(buffer), lane->run->bytes, &header);
  slice = mem_slice(buffer, 0u, lane->run->bytes);
  if (slice.buffer == NULL) return SALTS_ENOMEM;
  while (status == SALTS_OK && lane->received == 0u) {
    status = mail_status(lane->run, deadline);
    if (status == SALTS_OK && !sent) {
      status = mail_send(lane, &slice, lane->run->retained >= MAIL_ZMQ_COPY ? MAIL_ZMQ_COPY : 0);
      if (status == SALTS_OK) sent = 1;
      else if (status == SALTS_EBUSY || status == SALTS_ENOBUFS) status = SALTS_OK;
    }
    if (status == SALTS_OK) status = mail_progress(lane, 0u);
    if (status == SALTS_OK) status = mail_receive(lane, expected);
    if (status == SALTS_EBUSY) status = SALTS_OK;
  }
  mem_slice_release(&slice);
  lane->received = 0u;
  lane->segments = 0u;
  lane->event_checks = 0u;
  return status;
}

static void mail_consume(void *arg) {
  mail_lane *lane = arg;
  mail_run *run = lane->run;
  const size_t total = run->rounds * MAIL_BATCH * MAIL_PRODUCERS;
  uint64_t expected[MAIL_PRODUCERS] = {0};
  disruptor_cursor_t pending = {0};
  mail_local_batch local = {0};
  int status = mail_open(lane);
  if (status == SALTS_OK) status = mail_warmup(lane);
  mail_error(run, status);
  cmeta_mutex_lock(&run->mutex);
  ++run->ready;
  cmeta_cond_broadcast(&run->changed);
  cmeta_mutex_unlock(&run->mutex);
  mail_start_wait(run);
  const uint64_t deadline = cmeta_monotonic_ms() + MAIL_DEADLINE_MS;
  while (status == SALTS_OK && lane->received != total) {
    int observed_empty = 0;
    status = mail_status(run, deadline);
    if (status != SALTS_OK) break;
    for (size_t batch = 0u; status == SALTS_OK && batch < MAIL_BATCH; ++batch) {
      /* Equal numeric transport HWM does not imply equal application windows.
       * Cap accepted-but-not-received messages identically for both engines. */
      if (lane->admitted - lane->received >= MAIL_CAPACITY) {
        ++lane->app_full;
        break;
      }
      mail_entry *entry = NULL;
      if (run->direct) {
        status = mail_direct_entry(lane, &local, &entry);
        if (status == SALTS_EBUSY) { status = SALTS_OK; break; }
        if (status != SALTS_OK) break;
      } else {
        if (pending.sequence == 0u && !disruptor_worker_try_claim(lane->queue, &pending)) {
          observed_empty = 1;
          break;
        }
        entry = disruptor_acquire_entry(lane->queue, &pending);
      }
      status = mail_send(lane, &entry->slice, run->retained);
      if (status == SALTS_EBUSY || status == SALTS_ENOBUFS) {
        ++lane->send_busy;
        status = SALTS_OK;
        break;
      }
      if (status != SALTS_OK) break;
      /* The PAIR retained path must hold the source after admission; the copy
       * control must not. This catches accidental flattening/fallback or an
       * early lifetime release before the mailbox drops its own reference. */
      const uint32_t refs = mem_buffer_ref_count(entry->slice.buffer);
      if ((run->retained > 0 && run->retained < MAIL_ZMQ_COPY && refs < 3u) ||
          ((run->retained == 0 || run->retained == MAIL_ZMQ_COPY ||
            run->retained == MAIL_ZMQ_COPY_EVENTS) && refs != 2u)) {
        status = SALTS_EPROTO;
        break;
      }
      mem_slice_release(&entry->slice);
      if (run->direct) ++local.next;
      else {
        disruptor_worker_release_entry(lane->queue, &pending);
        pending.sequence = 0u;
      }
      ++lane->admitted;
      if (lane->peak_inflight < lane->admitted - lane->received)
        lane->peak_inflight = lane->admitted - lane->received;
    }
    for (size_t batch = 0u; status == SALTS_OK && batch < MAIL_BATCH && lane->received < total; ++batch) {
      status = mail_receive(lane, expected);
      if (status == SALTS_EBUSY) { status = SALTS_OK; break; }
    }
    if (status == SALTS_OK && lane->received > lane->admitted) status = SALTS_EPROTO;
    if (status == SALTS_OK && run->cancel_after != 0u && lane->received >= run->cancel_after)
      status = SALTS_ECANCELED;
    if (status != SALTS_OK || lane->received == total) break;
    ++lane->polls;
    /* A nonempty/budget-exhausted mailbox or undelivered message must keep
     * making progress. Only true application-idle lanes may park. A publish
     * racing this check carries a persistent NativeIO wake into step(). */
    const uint32_t wait = !run->direct && run->notify && observed_empty && pending.sequence == 0u &&
        lane->admitted == lane->received ? MAIL_WAIT_MS : 0u;
    status = mail_progress(lane, wait);
  }
  lane->finished_ns = cmeta_hrtime();
  if (status == SALTS_OK)
    for (size_t i = 0u; i < MAIL_PRODUCERS; ++i)
      if (expected[i] != run->rounds * MAIL_BATCH) status = SALTS_EPROTO;
  if (lane->sender != NULL) {
    const int read = flowmq_socket_internal_retained_queue_stats(lane->sender, &lane->sg_stats);
    if (status == SALTS_OK) status = read;
  }
  if (status == SALTS_OK && (run->retained == 2 || run->retained == 3) &&
      (lane->sg_stats.messages != total || lane->sg_stats.max_ranges > 32u ||
       lane->sg_stats.max_messages > (run->retained == 2 ? 1u : MAIL_BATCH)))
    status = SALTS_EPROTO;
  mail_error(run, status);
  cmeta_mutex_lock(&run->mutex);
  ++run->done;
  cmeta_cond_broadcast(&run->changed);
  while (!run->cleanup) cmeta_cond_wait(&run->changed, &run->mutex);
  cmeta_mutex_unlock(&run->mutex);
  /* All producers are joined before cleanup. Drain references even on error. */
  for (size_t i = local.next; i < local.count; ++i)
    mem_slice_release(&local.entries[i].slice);
  while (lane->queue != NULL) {
    if (pending.sequence == 0u && !disruptor_worker_try_claim(lane->queue, &pending)) break;
    mail_entry *entry = disruptor_acquire_entry(lane->queue, &pending);
    mem_slice_release(&entry->slice);
    disruptor_worker_release_entry(lane->queue, &pending);
    pending.sequence = 0u;
  }
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  mail_zmq_close(lane);
#endif
  if (lane->sender != NULL) mail_error(run, flowmq_owner_close_socket(lane->owner, lane->sender));
  if (lane->receiver != NULL) mail_error(run, flowmq_owner_close_socket(lane->owner, lane->receiver));
  if (lane->owner != NULL) mail_error(run, flowmq_owner_term(lane->owner));
  if (lane->ctx != NULL) mail_error(run, flowmq_ctx_term(lane->ctx));
}

static int mail_compare(const void *a, const void *b) {
  const uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
  return (x > y) - (x < y);
}
static void mail_join(cmeta_thread_t *thread) {
  if (cmeta_thread_join(thread) != SALTS_OK) abort();
}

static int mail_run_path(size_t lanes, size_t bytes, size_t rounds, uint32_t pause_ms,
                         int retained, int notify, size_t repeat, int measured, size_t cancel_after,
                         int direct) {
  mail_run run = {0};
  mail_lane workers[MAIL_LANES] = {0};
  cmeta_thread_t consumers[MAIL_LANES] = {0}, producers[MAIL_LANES * MAIL_PRODUCERS] = {0};
  flowmq_bench_metrics_t before = {0}, after = {0};
  const size_t total = lanes * MAIL_PRODUCERS * rounds * MAIL_BATCH;
  uint64_t *latencies = NULL, start = 0u, finish = 0u;
  size_t created = 0u, writers = 0u, admitted = 0u, received = 0u;
  size_t polls = 0u, busy = 0u, wakes = 0u, full = 0u, buffer_waits = 0u, segments = 0u;
  size_t app_full = 0u, peak_inflight = 0u, event_checks = 0u;
  size_t buffer_probes = 0u;
  flowmq_retained_queue_stats_t sg_stats = {0};
  int status = SALTS_OK;
  if (lanes == 0u || lanes > MAIL_LANES || bytes < sizeof(mail_header) ||
      bytes > MAIL_MAX_BYTES || rounds == 0u || rounds > MAIL_SATURATED_ROUNDS) return SALTS_EINVAL;
  if (retained < 0 || retained > MAIL_ZMQ_OWNED_EVENTS) return SALTS_EINVAL;
  if (direct && pause_ms != 0u) return SALTS_EINVAL;
#if !defined(FLOWMQ_BENCH_WITH_ZMQ)
  if (retained >= MAIL_ZMQ_COPY) return SALTS_ENOTSUP;
#endif
  latencies = calloc(total, sizeof(*latencies));
  if (latencies == NULL) return SALTS_ENOMEM;
  atomic_init(&run.error, SALTS_OK);
  run.bytes = bytes; run.rounds = rounds; run.pause_ms = pause_ms;
  run.retained = retained; run.notify = notify;
  run.direct = direct;
  run.cancel_after = cancel_after;
  cmeta_mutex_init(&run.mutex); cmeta_cond_init(&run.changed);
  for (size_t i = 0u; status == SALTS_OK && i < lanes; ++i) {
    mail_lane *lane = &workers[i];
    lane->id = i; lane->run = &run; lane->latencies = latencies + i * (total / lanes);
    if (!direct) {
      const disruptor_config_t config = {sizeof(mail_entry), MAIL_CAPACITY, 1u, DISRUPTOR_MODE_WORKER_POOL};
      lane->queue = disruptor_create(&config);
      if (lane->queue == NULL) { status = SALTS_ENOMEM; break; }
    }
    for (size_t j = 0u; status == SALTS_OK && j < MAIL_PRODUCERS; ++j) {
      mail_producer *p = &lane->producers[j];
      p->id = j; p->lane = lane; atomic_init(&p->released.count, 0u);
      p->storage = malloc(MAIL_BUFFERS * bytes);
      if (p->storage == NULL) { status = SALTS_ENOMEM; break; }
      for (size_t k = 0u; k < MAIL_BUFFERS; ++k) {
        p->buffers[k] = mem_wrap_external(p->storage + k * bytes, bytes, mail_released, &p->released);
        if (p->buffers[k] == NULL) { status = SALTS_ENOMEM; break; }
      }
    }
  }
  for (size_t i = 0u; status == SALTS_OK && i < lanes; ++i) {
    status = cmeta_thread_create(&consumers[i], mail_consume, &workers[i]);
    if (status == SALTS_OK) ++created;
  }
  mail_error(&run, status);
  cmeta_mutex_lock(&run.mutex);
  while (run.ready != created) cmeta_cond_wait(&run.changed, &run.mutex);
  cmeta_mutex_unlock(&run.mutex);
  status = atomic_load(&run.error);
  for (size_t i = 0u; !direct && status == SALTS_OK && i < created; ++i)
    for (size_t j = 0u; status == SALTS_OK && j < MAIL_PRODUCERS; ++j) {
      status = cmeta_thread_create(&producers[writers], mail_produce, &workers[i].producers[j]);
      if (status == SALTS_OK) ++writers;
    }
  mail_error(&run, status);
  cmeta_mutex_lock(&run.mutex);
  while (run.writers_ready != writers) cmeta_cond_wait(&run.changed, &run.mutex);
  cmeta_mutex_unlock(&run.mutex);
  mail_error(&run, flowmq_bench_metrics_read(&before));
  benchmark_io("mailbox to TCP receive vector", 1u, total, total * bytes) {
    cmeta_mutex_lock(&run.mutex);
    start = cmeta_hrtime(); run.started = 1;
    cmeta_cond_broadcast(&run.changed);
    while (run.done != created) cmeta_cond_wait(&run.changed, &run.mutex);
    cmeta_mutex_unlock(&run.mutex);
    if (!measured || atomic_load(&run.error) != SALTS_OK) goto measured_done;
  }
measured_done:
  mail_error(&run, flowmq_bench_metrics_read(&after));
  for (size_t i = 0u; i < writers; ++i) mail_join(&producers[i]);
  cmeta_mutex_lock(&run.mutex); run.cleanup = 1;
  cmeta_cond_broadcast(&run.changed); cmeta_mutex_unlock(&run.mutex);
  for (size_t i = 0u; i < created; ++i) mail_join(&consumers[i]);
  for (size_t i = 0u; i < lanes; ++i) {
    mail_lane *lane = &workers[i];
    admitted += lane->admitted; received += lane->received;
    polls += lane->polls; busy += lane->send_busy; segments += lane->segments;
    app_full += lane->app_full;
    event_checks += lane->event_checks;
    if (peak_inflight < lane->peak_inflight) peak_inflight = lane->peak_inflight;
    sg_stats.writes += lane->sg_stats.writes;
    sg_stats.messages += lane->sg_stats.messages;
    sg_stats.ranges += lane->sg_stats.ranges;
    if (sg_stats.max_messages < lane->sg_stats.max_messages) sg_stats.max_messages = lane->sg_stats.max_messages;
    if (sg_stats.max_ranges < lane->sg_stats.max_ranges) sg_stats.max_ranges = lane->sg_stats.max_ranges;
    if (lane->finished_ns > finish) finish = lane->finished_ns;
    for (size_t j = 0u; j < MAIL_PRODUCERS; ++j) {
      mail_producer *p = &lane->producers[j];
      size_t allocated = 0u;
      wakes += p->wakes; full += p->full; buffer_waits += p->buffer_waits;
      buffer_probes += p->buffer_probes;
      if (atomic_load(&run.error) == SALTS_OK && p->published != rounds * MAIL_BATCH)
        mail_error(&run, SALTS_EPROTO);
      for (size_t k = 0u; k < MAIL_BUFFERS; ++k) if (p->buffers[k] != NULL) {
        ++allocated;
        /* A leaked in-flight reference must not be hidden by freeing backing. */
        if (mem_buffer_ref_count(p->buffers[k]) != 1u) abort();
        mem_buffer_release(p->buffers[k]);
      }
      if (allocated != 0u && atomic_load(&p->released.count) != allocated) abort();
      free(p->storage);
    }
    if (lane->queue != NULL) disruptor_destroy(lane->queue);
  }
  status = atomic_load(&run.error);
  if (status == SALTS_OK && (received != total || admitted != total || finish <= start ||
                            peak_inflight > MAIL_CAPACITY)) status = SALTS_EPROTO;
  if (status == SALTS_OK && measured) {
    qsort(latencies, total, sizeof(*latencies), mail_compare);
    const char *send_names[] = {"copy", "sg", "queued_sg", "batch_sg", "zmq_copy", "zmq_owned",
                                "zmq_copy_events", "zmq_owned_events"};
    printf("MAILBOX_RESULT,%zu,%zu,%zu,%u,%s,%s,%zu,%llu,%.3f,%.3f,%d,%.3f,%llu,%zu,%zu,%zu,%zu,%zu,%zu,%llu,%llu,%llu,%zu,%zu,%zu,%zu,%zu,%s,%zu,%zu\n",
           bytes, lanes, repeat, pause_ms, send_names[retained], direct ? "local" : notify ? "wake" : "spin",
           total, (unsigned long long)(finish - start), (double)total * 1e9 / (finish - start),
           (double)(after.cpu_ns - before.cpu_ns) / total, after.cpu_cycles_available,
           (double)(after.cpu_cycles - before.cpu_cycles) / total,
           (unsigned long long)latencies[(total * 99u + 99u) / 100u - 1u],
           polls, busy, wakes, full, buffer_waits, segments,
           (unsigned long long)sg_stats.writes, (unsigned long long)sg_stats.messages,
           (unsigned long long)sg_stats.ranges, sg_stats.max_messages, sg_stats.max_ranges,
           app_full, peak_inflight, event_checks, direct ? "direct" : "mailbox",
           created + writers, buffer_probes);
  }
  if (status != SALTS_OK && !(cancel_after != 0u && status == SALTS_ECANCELED))
    fprintf(stderr, "MAILBOX_ERROR,status=%d,lanes=%zu,bytes=%zu,sg=%d,wake=%d,received=%zu/%zu\n",
                                 status, lanes, bytes, retained, notify, received, total);
  cmeta_cond_destroy(&run.changed); cmeta_mutex_destroy(&run.mutex); free(latencies);
  return status;
}

static int mail_run_case(size_t lanes, size_t bytes, size_t rounds, uint32_t pause_ms,
                         int retained, int notify, size_t repeat, int measured, size_t cancel_after) {
  return mail_run_path(lanes, bytes, rounds, pause_ms, retained, notify, repeat, measured, cancel_after, 0);
}

static int mail_queue_case(void) {
  const disruptor_config_t config = {sizeof(mail_entry), 2u, 1u, DISRUPTOR_MODE_WORKER_POOL};
  disruptor_t *queue = disruptor_create(&config);
  unsigned char data[64] = {0};
  mail_release_counter released; atomic_init(&released.count, 0u);
  mem_buffer_t *buffer = mem_wrap_external(data, sizeof(data), mail_released, &released);
  disruptor_cursor_t first = {0}, second = {0}, extra = {0}, read = {0};
  mail_entry *entries[2] = {0};
  mem_slice_t rejected = {0};
  int status = SALTS_OK;
  if (queue == NULL || buffer == NULL) { status = SALTS_ENOMEM; goto done; }
  if (!disruptor_publisher_try_claim(queue, &first) ||
      !disruptor_publisher_try_claim(queue, &second)) { status = SALTS_EPROTO; goto done; }
  entries[0] = disruptor_acquire_entry(queue, &first);
  entries[1] = disruptor_acquire_entry(queue, &second);
  entries[0]->slice = mem_slice(buffer, 0u, sizeof(data));
  entries[1]->slice = mem_slice(buffer, 0u, sizeof(data));
  rejected = mem_slice(buffer, 0u, sizeof(data));
  if (entries[0]->slice.buffer == NULL || entries[1]->slice.buffer == NULL || rejected.buffer == NULL) {
    status = SALTS_ENOMEM; goto done;
  }
  if (disruptor_publisher_try_claim(queue, &extra) || mem_buffer_ref_count(buffer) != 4u)
    status = SALTS_EPROTO;
  mem_slice_release(&rejected);
  /* Later producer publication is accepted but cannot bypass the earlier gap. */
  if (!disruptor_publisher_publish(queue, &second) || disruptor_worker_try_claim(queue, &read))
    status = SALTS_EPROTO;
  if (!disruptor_publisher_publish(queue, &first)) status = SALTS_EPROTO;
  for (size_t i = 0u; status == SALTS_OK && i < 2u; ++i) {
    if (!disruptor_worker_try_claim(queue, &read) || read.sequence != first.sequence + i) {
      status = SALTS_EPROTO; break;
    }
    mail_entry *entry = disruptor_acquire_entry(queue, &read);
    if (entry->slice.data != (char *)data) status = SALTS_EPROTO;
    mem_slice_release(&entry->slice);
    disruptor_worker_release_entry(queue, &read);
  }
  if (mem_buffer_ref_count(buffer) != 1u) status = SALTS_EPROTO;
done:
  mem_slice_release(&rejected);
  for (size_t i = 0u; i < 2u; ++i) if (entries[i] != NULL) mem_slice_release(&entries[i]->slice);
  if (buffer != NULL) {
    mem_buffer_release(buffer);
    if (atomic_load(&released.count) != 1u) status = SALTS_EPROTO;
  }
  if (queue != NULL) disruptor_destroy(queue);
  return status;
}

typedef struct wake_case_s { flowmq_owner_t *owner; atomic_int entered; int status; } wake_case;
static void mail_delayed_wake(void *arg) {
  wake_case *test = arg;
  while (!atomic_load(&test->entered)) cmeta_thread_yield();
  cmeta_sleep_ms(10u);
  test->status = flowmq_owner_internal_wake(test->owner);
}
static int mail_wake_case(int delayed) {
  flowmq_ctx_t *ctx = flowmq_ctx_new();
  if (ctx == NULL) return SALTS_ENOMEM;
  flowmq_owner_t *owner = flowmq_owner_new(ctx, NULL);
  int status = owner != NULL ? SALTS_OK : SALTS_ENOMEM;
  wake_case test = {0}; cmeta_thread_t thread = NULL;
  test.owner = owner; atomic_init(&test.entered, 0);
  if (status == SALTS_OK && delayed) status = cmeta_thread_create(&thread, mail_delayed_wake, &test);
  if (status == SALTS_OK && !delayed)
    for (size_t i = 0u; status == SALTS_OK && i < 16u; ++i) status = flowmq_owner_internal_wake(owner);
  if (status == SALTS_OK) {
    atomic_store(&test.entered, 1);
    const uint64_t start = cmeta_monotonic_ms();
    status = flowmq_owner_internal_step(owner, 2000u);
    if (status == SALTS_OK && cmeta_monotonic_ms() - start >= 1000u) status = SALTS_ETIMEDOUT;
  }
  if (thread != NULL) { mail_join(&thread); if (status == SALTS_OK) status = test.status; }
  if (owner != NULL) { const int closed = flowmq_owner_term(owner); if (status == SALTS_OK) status = closed; }
  { const int closed = flowmq_ctx_term(ctx); if (status == SALTS_OK) status = closed; }
  return status;
}

/* Queue a deterministic burst without progress: prove the first message is
 * submitted immediately, full rejection owns nothing, and actual CNet writes
 * join distinct wire messages. Large frames exercise logical/native splitting. */
static int mail_batch_case(size_t bytes, int mixed, int byte_hwm, int close_pending) {
  enum { COUNT = 32 };
  mail_run run = {0};
  mail_lane lane = {0};
  mem_buffer_t *buffers[COUNT] = {0};
  uint64_t latencies[COUNT] = {0}, expected[MAIL_PRODUCERS] = {0};
  mail_release_counter released;
  unsigned char *storage = malloc(COUNT * bytes);
  int status, final_status;
  if (storage == NULL) return SALTS_ENOMEM;
  atomic_init(&run.error, SALTS_OK);
  atomic_init(&released.count, 0u);
  run.bytes = bytes; run.retained = 3;
  run.admission_limit = COUNT; run.limit_bytes = byte_hwm;
  lane.run = &run; lane.latencies = latencies;
  status = mail_open(&lane);
  if (status == SALTS_OK && flowmq_socket_internal_retained_queue(lane.sender, 1u) != SALTS_EBUSY)
    status = SALTS_EPROTO;
  for (size_t i = 0u; status == SALTS_OK && i < COUNT; ++i) {
    mem_slice_t slice;
    const mail_header header = {0u, 0u, i, cmeta_hrtime()};
    mail_fill(storage + i * bytes, bytes, &header);
    buffers[i] = mem_wrap_external(storage + i * bytes, bytes, mail_released, &released);
    if (buffers[i] == NULL) { status = SALTS_ENOMEM; break; }
    slice = mem_slice(buffers[i], 0u, bytes);
    if (i == 1u && (flowmq_send_slice(lane.sender, &slice, FLOWMQ_DONTWAIT) != SALTS_EBUSY ||
                    mem_buffer_ref_count(buffers[i]) != 2u)) status = SALTS_EPROTO;
    if (status == SALTS_OK)
      status = mixed && i == 12u
          ? flowmq_send(lane.sender, slice.data, slice.length, FLOWMQ_DONTWAIT)
          : flowmq_socket_internal_send_slice_queued(lane.sender, &slice);
    mem_slice_release(&slice);
  }
  if (status == SALTS_OK) {
    mem_slice_t rejected = mem_slice(buffers[0], 0u, bytes);
    const uint32_t refs = mem_buffer_ref_count(buffers[0]);
    if (flowmq_socket_internal_send_slice_queued(lane.sender, &rejected) != SALTS_ENOBUFS ||
        mem_buffer_ref_count(buffers[0]) != refs) status = SALTS_EPROTO;
    mem_slice_release(&rejected);
  }
  if (status == SALTS_OK) {
    status = flowmq_socket_internal_retained_queue_stats(lane.sender, &lane.sg_stats);
    if (status == SALTS_OK && (lane.sg_stats.messages != 1u || lane.sg_stats.writes != 1u))
      status = SALTS_EPROTO;
  }
  const uint64_t deadline = cmeta_monotonic_ms() + MAIL_DEADLINE_MS;
  while (status == SALTS_OK && !close_pending && lane.received < COUNT) {
    status = mail_status(&run, deadline);
    if (status == SALTS_OK) status = flowmq_owner_internal_step(lane.owner, 0u);
    if (status == SALTS_OK) status = mail_receive(&lane, expected);
    if (status == SALTS_EBUSY) status = SALTS_OK;
  }
  if (status == SALTS_OK && !close_pending) {
    status = flowmq_socket_internal_retained_queue_stats(lane.sender, &lane.sg_stats);
    const size_t ranges_per_message = 2u * ((bytes + 65535u) / 65536u);
    const size_t expected_max = 32u / ranges_per_message;
    if (status == SALTS_OK && (expected[0] != COUNT ||
        lane.sg_stats.messages != COUNT - (mixed != 0) ||
        lane.sg_stats.max_messages != expected_max ||
        lane.sg_stats.max_ranges != expected_max * ranges_per_message)) status = SALTS_EPROTO;
  }
  final_status = status;
  if (lane.sender != NULL) {
    status = flowmq_owner_close_socket(lane.owner, lane.sender);
    if (final_status == SALTS_OK) final_status = status;
  }
  if (lane.receiver != NULL) {
    status = flowmq_owner_close_socket(lane.owner, lane.receiver);
    if (final_status == SALTS_OK) final_status = status;
  }
  if (lane.owner != NULL) {
    status = flowmq_owner_term(lane.owner);
    if (final_status == SALTS_OK) final_status = status;
  }
  if (lane.ctx != NULL) {
    status = flowmq_ctx_term(lane.ctx);
    if (final_status == SALTS_OK) final_status = status;
  }
  size_t allocated = 0u;
  for (size_t i = 0u; i < COUNT; ++i) if (buffers[i] != NULL) {
    ++allocated;
    if (mem_buffer_ref_count(buffers[i]) != 1u) abort();
    mem_buffer_release(buffers[i]);
  }
  if (atomic_load(&released.count) != allocated) final_status = SALTS_EPROTO;
  free(storage);
  return final_status;
}

static void mail_report_header(void) {
  printf("MAILBOX_CONFIG,producers_per_lane=%d,capacity=%d,buffers_per_producer=%d,batch=%d,rounds=%d,small_saturated_rounds=%d,repeats=%d,affinity=unbound,warmup=one_full_size_copy,receive=borrowed_or_slicev,application_window=capacity\n",
         MAIL_PRODUCERS, MAIL_CAPACITY, MAIL_BUFFERS, MAIL_BATCH, MAIL_ROUNDS, MAIL_SATURATED_ROUNDS, MAIL_REPEATS);
  printf("MAILBOX_HEADER,payload_bytes,lanes,repeat,pause_ms,send,wait,messages,wall_ns,messages_per_second,cpu_ns_per_message,cpu_cycles_available,cpu_cycles_per_message,prepare_receive_p99_ns,poll_calls,send_busy,wake_calls,queue_full,buffer_waits,receive_segments,sg_writes,sg_messages,sg_ranges,sg_max_messages,sg_max_ranges,app_full,peak_inflight_per_lane,send_event_checks,path,application_workers,buffer_probes\n");
}

static int mail_direct_prepare_case(void) {
  mail_run run = {0};
  mail_lane lane = {0};
  mail_local_batch batch = {0};
  mem_slice_t held[MAIL_BUFFERS] = {0};
  unsigned char storage[MAIL_BUFFERS][64] = {0};
  mail_entry *entry = NULL;
  mail_producer *p = &lane.producers[0];
  int status = SALTS_OK;
  size_t allocated = 0u;
  run.bytes = sizeof(storage[0]); run.rounds = 2u;
  lane.run = &run; p->lane = &lane;
  atomic_init(&p->released.count, 0u);
  for (size_t i = 0u; i < MAIL_BUFFERS; ++i) {
    p->buffers[i] = mem_wrap_external(storage[i], sizeof(storage[i]), mail_released, &p->released);
    if (p->buffers[i] == NULL) { status = SALTS_ENOMEM; break; }
    ++allocated;
    /* Leave only three sources available: preparation must return with an
     * owned partial batch instead of waiting for the owner's own progress. */
    if (i >= 3u) {
      held[i] = mem_slice(p->buffers[i], 0u, sizeof(storage[i]));
      if (held[i].buffer == NULL) { status = SALTS_ENOMEM; break; }
    }
  }
  if (status == SALTS_OK &&
      (mail_direct_entry(&lane, &batch, &entry) != SALTS_EBUSY || batch.count != 3u ||
       batch.next != 0u || p->published != 0u || entry != NULL)) status = SALTS_EPROTO;
  for (size_t i = 0u; i < MAIL_BUFFERS; ++i) mem_slice_release(&held[i]);
  if (status == SALTS_OK) status = mail_direct_entry(&lane, &batch, &entry);
  if (status == SALTS_OK && (batch.count != MAIL_BATCH || p->published != MAIL_BATCH ||
                            entry != &batch.entries[0])) status = SALTS_EPROTO;
  for (size_t i = 0u; status == SALTS_OK && i < batch.count; ++i) {
    mail_header header = {0};
    size_t offset = 0u;
    status = mail_validate_bytes((const unsigned char *)batch.entries[i].slice.data,
                                 batch.entries[i].slice.length, &header, &offset);
    if (status == SALTS_OK && (offset != run.bytes || header.sequence != i ||
                              header.producer != 0u || header.lane != 0u)) status = SALTS_EPROTO;
  }
  for (size_t i = 0u; i < batch.count; ++i) mem_slice_release(&batch.entries[i].slice);
  for (size_t i = 0u; i < allocated; ++i) {
    if (mem_buffer_ref_count(p->buffers[i]) != 1u) abort();
    mem_buffer_release(p->buffers[i]);
  }
  if (atomic_load(&p->released.count) != allocated) status = SALTS_EPROTO;
  return status;
}

#if defined(FLOWMQ_BENCH_WITH_ZMQ)
static int mail_zmq_rejection_case(void) {
  mail_run run = {0};
  mail_lane lane = {0};
  unsigned char data[64] = {0};
  mail_release_counter released;
  mem_buffer_t *buffer;
  mem_slice_t slice = {0};
  atomic_init(&run.error, SALTS_OK);
  atomic_init(&released.count, 0u);
  lane.run = &run;
  lane.zmq_ctx = zmq_ctx_new();
  if (lane.zmq_ctx == NULL) return SALTS_ENOMEM;
  lane.zmq_sender = zmq_socket(lane.zmq_ctx, ZMQ_PAIR);
  buffer = mem_wrap_external(data, sizeof(data), mail_released, &released);
  int status = lane.zmq_sender != NULL && buffer != NULL ? SALTS_OK : SALTS_ENOMEM;
  if (status == SALTS_OK) {
    const int linger = 0, immediate = 1;
    if (zmq_setsockopt(lane.zmq_sender, ZMQ_LINGER, &linger, sizeof(linger)) != 0 ||
        zmq_setsockopt(lane.zmq_sender, ZMQ_IMMEDIATE, &immediate, sizeof(immediate)) != 0)
      status = SALTS_EIO;
  }
  if (status == SALTS_OK) {
    slice = mem_slice(buffer, 0u, sizeof(data));
    if (slice.buffer == NULL) status = SALTS_ENOMEM;
    for (size_t i = 0u; status == SALTS_OK && i < MAIL_BATCH; ++i)
      if (mail_zmq_send(&lane, &slice, 1) != SALTS_EBUSY ||
          mem_buffer_ref_count(buffer) != 2u) status = SALTS_EPROTO;
  }
  mail_zmq_close(&lane);
  if (status == SALTS_OK) status = atomic_load(&run.error);
  mem_slice_release(&slice);
  if (buffer != NULL) {
    if (mem_buffer_ref_count(buffer) != 1u) abort();
    mem_buffer_release(buffer);
    if (atomic_load(&released.count) != 1u) status = SALTS_EPROTO;
  }
  return status;
}
#endif

spec("FlowMQ internal SG mailbox") {
  it("lane direct correctness: partial preparation yields and resumes without rewriting owned sources") {
    check_equal(mail_direct_prepare_case(), SALTS_OK);
  }
  it("lane direct correctness: local sources preserve FIFO and lifetime through wrap and cancellation") {
    const size_t sizes[] = {64u, MAIL_MAX_BYTES}, counts[] = {1u, MAIL_LANES};
    const int modes[] = {0, 1, 2, 3,
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
                         MAIL_ZMQ_COPY_EVENTS, MAIL_ZMQ_OWNED_EVENTS,
#endif
    };
    for (size_t size = 0u; size < 2u; ++size)
      for (size_t lane = 0u; lane < 2u; ++lane)
        for (size_t mode = 0u; mode < sizeof(modes) / sizeof(modes[0]); ++mode) {
          check_equal(mail_run_path(counts[lane], sizes[size], 8u, 0u,
                                   modes[mode], 0, 0u, 0, 0u, 1), SALTS_OK);
          check_equal(mail_run_path(counts[lane], sizes[size], 8u, 0u,
                                   modes[mode], 0, 0u, 0, MAIL_BATCH, 1), SALTS_ECANCELED);
        }
  }
  bench("lane handoff comparison: direct owner versus producer mailbox") {
    const size_t sizes[] = {64u, MAIL_MAX_BYTES}, counts[] = {1u, MAIL_LANES};
    printf("MAILBOX_TOPOLOGY,logical_sources_per_lane=2,direct_workers_per_lane=1,mailbox_workers_per_lane=3,direct_staging=16,load=saturated\n");
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
    int major, minor, patch;
    zmq_version(&major, &minor, &patch);
    printf("MAILBOX_ZMQ,version=%d.%d.%d,context=per_lane,io_threads_per_context=1,events=sender_once_per_progress\n", major, minor, patch);
#endif
    mail_report_header();
    for (size_t repeat = 0u; repeat < MAIL_REPEATS; ++repeat)
      for (size_t size = 0u; size < 2u; ++size)
        for (size_t lane = 0u; lane < 2u; ++lane) {
          const size_t bytes = sizes[(size + repeat) % 2u];
          const int modes[] = {0, bytes == 64u ? 3 : 1,
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
                               MAIL_ZMQ_COPY_EVENTS, MAIL_ZMQ_OWNED_EVENTS,
#endif
          };
          const size_t mode_count = sizeof(modes) / sizeof(modes[0]);
          for (size_t order = 0u; order < mode_count; ++order)
            for (size_t path = 0u; path < 2u; ++path) {
              const int direct = (int)((path + repeat) % 2u);
              check_equal(mail_run_path(counts[(lane + repeat) % 2u], bytes,
                  bytes == 64u ? MAIL_SATURATED_ROUNDS : MAIL_ROUNDS, 0u,
                  modes[(order + repeat) % mode_count], !direct, repeat + 1u, 1, 0u, direct), SALTS_OK);
            }
        }
  }
#if defined(FLOWMQ_BENCH_WITH_ZMQ)
  it("mailbox ZMQ correctness: rejection releases only the temporary external message reference") {
    check_equal(mail_zmq_rejection_case(), SALTS_OK);
  }
  it("mailbox ZMQ correctness: copy and owned MPSC wrap preserve FIFO and source lifetime") {
    const size_t sizes[] = {64u, MAIL_MAX_BYTES};
    const size_t counts[] = {1u, MAIL_LANES};
    for (size_t size = 0u; size < 2u; ++size)
      for (size_t lane = 0u; lane < 2u; ++lane)
        for (int mode = MAIL_ZMQ_COPY; mode <= MAIL_ZMQ_OWNED_EVENTS; ++mode)
          for (int notify = 0; notify <= 1; ++notify)
            check_equal(mail_run_case(counts[lane], sizes[size], 8u, 0u, mode, notify, 0u, 0, 0u), SALTS_OK);
  }
  it("mailbox ZMQ correctness: paced wake and cancellation release background I/O references") {
    for (int mode = MAIL_ZMQ_COPY; mode <= MAIL_ZMQ_OWNED_EVENTS; ++mode) {
      check_equal(mail_run_case(MAIL_LANES, MAIL_MAX_BYTES, 8u, 5u, mode, 1, 0u, 0, 0u), SALTS_OK);
      check_equal(mail_run_case(MAIL_LANES, MAIL_MAX_BYTES, 32u, 0u, mode, 1, 0u, 0, MAIL_BATCH), SALTS_ECANCELED);
    }
  }
  bench("mailbox ZMQ comparison: common producers and copy or retained payload") {
    const size_t sizes[] = {64u, MAIL_MAX_BYTES}, counts[] = {1u, MAIL_LANES};
    int major, minor, patch;
    zmq_version(&major, &minor, &patch);
    printf("MAILBOX_ZMQ,version=%d.%d.%d,context=per_lane,io_threads_per_context=1,send=dontwait,receive=msg,control_wait=empty_owner\n", major, minor, patch);
    mail_report_header();
    for (size_t repeat = 0u; repeat < MAIL_REPEATS; ++repeat)
      for (size_t size = 0u; size < 2u; ++size)
        for (size_t lane = 0u; lane < 2u; ++lane)
          for (size_t load = 0u; load < 2u; ++load)
            for (size_t wait = 0u; wait < 2u; ++wait)
              for (size_t order = 0u; order < 4u; ++order) {
                const size_t bytes = sizes[(size + repeat) % 2u];
                const int modes[] = {0, bytes == 64u ? 3 : 1, MAIL_ZMQ_COPY, MAIL_ZMQ_OWNED};
                const size_t rounds = load == 0u && bytes == 64u ? MAIL_SATURATED_ROUNDS : MAIL_ROUNDS;
                check_equal(mail_run_case(counts[(lane + repeat) % 2u], bytes, rounds,
                    load != 0u ? 5u : 0u, modes[(order + repeat) % 4u],
                    (int)((wait + repeat) % 2u), repeat + 1u, 1, 0u), SALTS_OK);
              }
  }
  bench("mailbox ZMQ progress: sender events isolate nonblocking credit delay") {
    const size_t sizes[] = {64u, MAIL_MAX_BYTES}, counts[] = {1u, MAIL_LANES};
    int major, minor, patch;
    zmq_version(&major, &minor, &patch);
    printf("MAILBOX_ZMQ,version=%d.%d.%d,context=per_lane,io_threads_per_context=1,send=dontwait,receive=msg,control_wait=empty_owner,events=sender_once_per_progress\n", major, minor, patch);
    mail_report_header();
    for (size_t repeat = 0u; repeat < MAIL_REPEATS; ++repeat)
      for (size_t size = 0u; size < 2u; ++size)
        for (size_t lane = 0u; lane < 2u; ++lane)
          for (size_t order = 0u; order < 6u; ++order) {
            const size_t bytes = sizes[(size + repeat) % 2u];
            const int modes[] = {0, bytes == 64u ? 3 : 1, MAIL_ZMQ_COPY,
                                 MAIL_ZMQ_OWNED, MAIL_ZMQ_COPY_EVENTS, MAIL_ZMQ_OWNED_EVENTS};
            check_equal(mail_run_case(counts[(lane + repeat) % 2u], bytes,
                bytes == 64u ? MAIL_SATURATED_ROUNDS : MAIL_ROUNDS, 0u,
                modes[(order + repeat) % 6u], 1, repeat + 1u, 1, 0u), SALTS_OK);
          }
  }
#endif
  it("mailbox correctness: queue configuration rejects invalid limits and unsupported owners") {
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_owner_t *owner = ctx != NULL ? flowmq_owner_new(ctx, NULL) : NULL;
    flowmq_socket_t *pair = owner != NULL ? flowmq_owner_socket(owner, FLOWMQ_PAIR) : NULL;
    flowmq_socket_t *push = owner != NULL ? flowmq_owner_socket(owner, FLOWMQ_PUSH) : NULL;
    flowmq_socket_t *ordinary = ctx != NULL ? flowmq_socket(ctx, FLOWMQ_PAIR) : NULL;
    int status = pair != NULL && push != NULL && ordinary != NULL ? SALTS_OK : SALTS_ENOMEM;
    if (status == SALTS_OK &&
        (flowmq_socket_internal_retained_queue(NULL, 1u) != SALTS_EINVAL ||
         flowmq_socket_internal_retained_queue(pair, 0u) != SALTS_EINVAL ||
         flowmq_socket_internal_retained_queue(pair, MAIL_BATCH + 1u) != SALTS_EINVAL ||
         flowmq_socket_internal_retained_queue(push, 1u) != SALTS_ENOTSUP ||
         flowmq_socket_internal_retained_queue(ordinary, 1u) != SALTS_ENOTSUP ||
         flowmq_socket_internal_retained_queue(pair, 1u) != SALTS_OK ||
         flowmq_socket_internal_retained_queue(pair, MAIL_BATCH) != SALTS_OK)) status = SALTS_EPROTO;
    if (ordinary != NULL) { const int closed = flowmq_close(ordinary); if (status == SALTS_OK) status = closed; }
    if (push != NULL) { const int closed = flowmq_owner_close_socket(owner, push); if (status == SALTS_OK) status = closed; }
    if (pair != NULL) { const int closed = flowmq_owner_close_socket(owner, pair); if (status == SALTS_OK) status = closed; }
    if (owner != NULL) { const int closed = flowmq_owner_term(owner); if (status == SALTS_OK) status = closed; }
    if (ctx != NULL) { const int closed = flowmq_ctx_term(ctx); if (status == SALTS_OK) status = closed; }
    check_equal(status, SALTS_OK);
  }
  it("mailbox correctness: batches distinct frames within range bounds and preserves mixed FIFO") {
    check_equal(mail_batch_case(64u, 0, 0, 0), SALTS_OK);
    check_equal(mail_batch_case(64u, 1, 1, 0), SALTS_OK);
    check_equal(mail_batch_case(65537u, 0, 1, 0), SALTS_OK);
    check_equal(mail_batch_case(1024u * 1024u, 0, 0, 0), SALTS_OK);
  }
  it("mailbox correctness: closing drops queued frames and drains the native retained write") {
    check_equal(mail_batch_case(65537u, 1, 1, 1), SALTS_OK);
  }
  it("mailbox correctness: full admission preserves ownership and publication gaps preserve order") {
    check_equal(mail_queue_case(), SALTS_OK);
  }
  it("mailbox correctness: wakes before and during an empty owner wait") {
    check_equal(mail_wake_case(0), SALTS_OK);
    check_equal(mail_wake_case(1), SALTS_OK);
  }
  it("mailbox correctness: retains payload through MPSC wrap and network completion") {
    const size_t sizes[] = {64u, MAIL_MAX_BYTES};
    const size_t counts[] = {1u, MAIL_LANES};
    for (size_t size = 0u; size < 2u; ++size)
      for (size_t lane = 0u; lane < 2u; ++lane)
        for (int retained = 0; retained < 4; ++retained)
          for (int notify = 0; notify <= 1; ++notify)
            check_equal(mail_run_case(counts[lane], sizes[size], 8u, 0u, retained, notify, 0u, 0, 0u), SALTS_OK);
  }
  it("mailbox correctness: cancellation joins producers and releases pending SG references") {
    for (int retained = 1; retained < 4; ++retained) {
      check_equal(mail_run_case(1u, MAIL_MAX_BYTES, 32u, 0u, retained, 1, 0u, 0, MAIL_BATCH), SALTS_ECANCELED);
      check_equal(mail_run_case(MAIL_LANES, MAIL_MAX_BYTES, 32u, 0u, retained, 1, 0u, 0, MAIL_BATCH), SALTS_ECANCELED);
    }
  }
  bench("mailbox comparison: retained SG and copy with spin and wake") {
    const size_t sizes[] = {64u, MAIL_MAX_BYTES};
    const size_t counts[] = {1u, MAIL_LANES};
    mail_report_header();
    for (size_t repeat = 0u; repeat < MAIL_REPEATS; ++repeat)
      for (size_t size = 0u; size < 2u; ++size)
        for (size_t lane = 0u; lane < 2u; ++lane)
          for (size_t load = 0u; load < 2u; ++load)
          for (size_t wait = 0u; wait < 2u; ++wait)
            for (size_t mode = 0u; mode < 4u; ++mode) {
              const size_t policy = (mode + repeat) % 4u;
              const size_t bytes = sizes[(size + repeat) % 2u];
              const size_t rounds = load == 0u && bytes == 64u ? MAIL_SATURATED_ROUNDS : MAIL_ROUNDS;
              check_equal(mail_run_case(counts[(lane + repeat) % 2u], bytes,
                                        rounds, load != 0u ? 5u : 0u,
                                        (int)policy, (int)((wait + repeat) % 2u), repeat + 1u, 1, 0u), SALTS_OK);
            }
  }
}
