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

enum { MAIL_LANES = 8, MAIL_PRODUCERS = 2, MAIL_CAPACITY = 128,
       MAIL_BUFFERS = 64, MAIL_BATCH = 16, MAIL_MAX_BYTES = 65536,
       MAIL_WAIT_MS = 1000, MAIL_DEADLINE_MS = 30000, MAIL_REPEATS = 4,
       MAIL_ROUNDS = 32, MAIL_SATURATED_ROUNDS = 2048, MAIL_RX_SEGMENTS = 64 };

typedef struct mail_header_s {
  uint64_t lane, producer, sequence, prepared_ns;
} mail_header;
typedef struct mail_entry_s { mem_slice_t slice; } mail_entry;
typedef struct mail_release_counter_s { atomic_size_t count; } mail_release_counter;
typedef struct mail_run_s mail_run;
typedef struct mail_lane_s mail_lane;
typedef struct mail_producer_s {
  mail_lane *lane;
  size_t id, next_buffer, published, full, buffer_waits, wakes;
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
  size_t received, admitted, polls, send_busy, segments;
  uint64_t finished_ns;
};
struct mail_run_s {
  cmeta_mutex_t mutex;
  cmeta_cond_t changed;
  atomic_int error;
  size_t ready, writers_ready, done, rounds, bytes, cancel_after;
  int started, cleanup, notify, retained;
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
static int mail_prepare(mail_producer *p, uint64_t sequence, mem_slice_t *out,
                        uint64_t deadline) {
  mail_run *run = p->lane->run;
  for (;;) {
    int status = mail_status(run, deadline);
    if (status != SALTS_OK) return status;
    for (size_t i = 0u; i < MAIL_BUFFERS; ++i) {
      const size_t index = (p->next_buffer + i) % MAIL_BUFFERS;
      mem_buffer_t *buffer = p->buffers[index];
      if (mem_buffer_ref_count(buffer) != 1u) continue;
      const mail_header h = {p->lane->id, p->id, sequence, cmeta_hrtime()};
      mail_fill((unsigned char *)mem_buffer_data(buffer), run->bytes, &h);
      *out = mem_slice(buffer, 0u, run->bytes);
      if (out->buffer == NULL) return SALTS_ENOMEM;
      p->next_buffer = (index + 1u) % MAIL_BUFFERS;
      return SALTS_OK;
    }
    ++p->buffer_waits;
    cmeta_thread_yield();
  }
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

static int mail_open(mail_lane *lane) {
  flowmq_owner_config_t config = FLOWMQ_OWNER_CONFIG_INIT;
  char endpoint[128];
  size_t size = 0u, ready = 0u;
  const int hwm = MAIL_CAPACITY;
  const int reconnect = -1;
  const size_t hwm_bytes = (size_t)MAIL_CAPACITY * lane->run->bytes;
  int status;
  lane->ctx = flowmq_ctx_new();
  if (lane->ctx == NULL) return SALTS_ENOMEM;
  config.socket_capacity = 2u;
  lane->owner = flowmq_owner_new(lane->ctx, &config);
  if (lane->owner == NULL) return SALTS_ENOMEM;
  lane->sender = flowmq_owner_socket(lane->owner, FLOWMQ_PAIR);
  lane->receiver = flowmq_owner_socket(lane->owner, FLOWMQ_PAIR);
  if (lane->sender == NULL || lane->receiver == NULL) return SALTS_ENOMEM;
  flowmq_socket_t *sockets[] = {lane->sender, lane->receiver};
  for (size_t i = 0; i < 2u; ++i) {
    status = flowmq_setsockopt(sockets[i], FLOWMQ_RECONNECT_IVL, &reconnect, sizeof(reconnect));
    if (status == SALTS_OK) status = flowmq_setsockopt(sockets[i], FLOWMQ_SNDHWM, &hwm, sizeof(hwm));
    if (status == SALTS_OK) status = flowmq_setsockopt(sockets[i], FLOWMQ_RCVHWM, &hwm, sizeof(hwm));
    if (status == SALTS_OK) status = flowmq_setsockopt(sockets[i], FLOWMQ_SNDHWM_BYTES, &hwm_bytes, sizeof(hwm_bytes));
    if (status == SALTS_OK) status = flowmq_setsockopt(sockets[i], FLOWMQ_RCVHWM_BYTES, &hwm_bytes, sizeof(hwm_bytes));
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

/* Validate the receive vector directly, including a header split across
 * ranges. No coalesce/copy receive is used by either send policy. */
static int mail_receive(mail_lane *lane, uint64_t expected[MAIL_PRODUCERS]) {
  mem_slice_t parts[MAIL_RX_SEGMENTS] = {0};
  size_t count = 0u, offset = 0u;
  mail_header header = {0};
  int status = flowmq_recv_slicev(lane->receiver, parts, MAIL_RX_SEGMENTS, &count, FLOWMQ_DONTWAIT);
  if (status != SALTS_OK) return status;
  for (size_t i = 0u; i < count; ++i) {
    for (size_t j = 0u; j < parts[i].length; ++j, ++offset) {
      const unsigned char value = (unsigned char)parts[i].data[j];
      if (offset < sizeof(header)) ((unsigned char *)&header)[offset] = value;
      else if (value != mail_byte(&header, offset)) status = SALTS_EPROTO;
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
  for (size_t i = 0u; i < count; ++i) mem_slice_release(&parts[i]);
  return status;
}

static void mail_consume(void *arg) {
  mail_lane *lane = arg;
  mail_run *run = lane->run;
  const size_t total = run->rounds * MAIL_BATCH * MAIL_PRODUCERS;
  uint64_t expected[MAIL_PRODUCERS] = {0};
  disruptor_cursor_t pending = {0};
  int status = mail_open(lane);
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
      if (pending.sequence == 0u && !disruptor_worker_try_claim(lane->queue, &pending)) {
        observed_empty = 1;
        break;
      }
      mail_entry *entry = disruptor_acquire_entry(lane->queue, &pending);
      status = run->retained
          ? flowmq_send_slice(lane->sender, &entry->slice, FLOWMQ_DONTWAIT)
          : flowmq_send(lane->sender, entry->slice.data, entry->slice.length, FLOWMQ_DONTWAIT);
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
      if ((run->retained && refs < 3u) || (!run->retained && refs != 2u)) {
        status = SALTS_EPROTO;
        break;
      }
      mem_slice_release(&entry->slice);
      disruptor_worker_release_entry(lane->queue, &pending);
      pending.sequence = 0u;
      ++lane->admitted;
    }
    for (size_t batch = 0u; status == SALTS_OK && batch < MAIL_BATCH && lane->received < total; ++batch) {
      status = mail_receive(lane, expected);
      if (status == SALTS_EBUSY) { status = SALTS_OK; break; }
    }
    if (status == SALTS_OK && run->cancel_after != 0u && lane->received >= run->cancel_after)
      status = SALTS_ECANCELED;
    if (status != SALTS_OK || lane->received == total) break;
    ++lane->polls;
    /* A nonempty/budget-exhausted mailbox or undelivered message must keep
     * making progress. Only true application-idle lanes may park. A publish
     * racing this check carries a persistent NativeIO wake into step(). */
    const uint32_t wait = run->notify && observed_empty && pending.sequence == 0u &&
        lane->admitted == lane->received ? MAIL_WAIT_MS : 0u;
    status = flowmq_owner_internal_step(lane->owner, wait);
  }
  lane->finished_ns = cmeta_hrtime();
  if (status == SALTS_OK)
    for (size_t i = 0u; i < MAIL_PRODUCERS; ++i)
      if (expected[i] != run->rounds * MAIL_BATCH) status = SALTS_EPROTO;
  mail_error(run, status);
  cmeta_mutex_lock(&run->mutex);
  ++run->done;
  cmeta_cond_broadcast(&run->changed);
  while (!run->cleanup) cmeta_cond_wait(&run->changed, &run->mutex);
  cmeta_mutex_unlock(&run->mutex);
  /* All producers are joined before cleanup. Drain references even on error. */
  do {
    if (pending.sequence == 0u && !disruptor_worker_try_claim(lane->queue, &pending)) break;
    mail_entry *entry = disruptor_acquire_entry(lane->queue, &pending);
    mem_slice_release(&entry->slice);
    disruptor_worker_release_entry(lane->queue, &pending);
    pending.sequence = 0u;
  } while (1);
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

static int mail_run_case(size_t lanes, size_t bytes, size_t rounds, uint32_t pause_ms,
                         int retained, int notify, size_t repeat, int measured, size_t cancel_after) {
  mail_run run = {0};
  mail_lane workers[MAIL_LANES] = {0};
  cmeta_thread_t consumers[MAIL_LANES] = {0}, producers[MAIL_LANES * MAIL_PRODUCERS] = {0};
  flowmq_bench_metrics_t before = {0}, after = {0};
  const size_t total = lanes * MAIL_PRODUCERS * rounds * MAIL_BATCH;
  uint64_t *latencies = NULL, start = 0u, finish = 0u;
  size_t created = 0u, writers = 0u, admitted = 0u, received = 0u;
  size_t polls = 0u, busy = 0u, wakes = 0u, full = 0u, buffer_waits = 0u, segments = 0u;
  int status = SALTS_OK;
  if (lanes == 0u || lanes > MAIL_LANES || bytes < sizeof(mail_header) ||
      bytes > MAIL_MAX_BYTES || rounds == 0u || rounds > MAIL_SATURATED_ROUNDS) return SALTS_EINVAL;
  latencies = calloc(total, sizeof(*latencies));
  if (latencies == NULL) return SALTS_ENOMEM;
  atomic_init(&run.error, SALTS_OK);
  run.bytes = bytes; run.rounds = rounds; run.pause_ms = pause_ms;
  run.retained = retained; run.notify = notify;
  run.cancel_after = cancel_after;
  cmeta_mutex_init(&run.mutex); cmeta_cond_init(&run.changed);
  for (size_t i = 0u; status == SALTS_OK && i < lanes; ++i) {
    mail_lane *lane = &workers[i];
    lane->id = i; lane->run = &run; lane->latencies = latencies + i * (total / lanes);
    const disruptor_config_t config = {sizeof(mail_entry), MAIL_CAPACITY, 1u, DISRUPTOR_MODE_WORKER_POOL};
    lane->queue = disruptor_create(&config);
    if (lane->queue == NULL) { status = SALTS_ENOMEM; break; }
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
  for (size_t i = 0u; status == SALTS_OK && i < created; ++i)
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
    if (lane->finished_ns > finish) finish = lane->finished_ns;
    for (size_t j = 0u; j < MAIL_PRODUCERS; ++j) {
      mail_producer *p = &lane->producers[j];
      size_t allocated = 0u;
      wakes += p->wakes; full += p->full; buffer_waits += p->buffer_waits;
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
  if (status == SALTS_OK && (received != total || admitted != total || finish <= start)) status = SALTS_EPROTO;
  if (status == SALTS_OK && measured) {
    qsort(latencies, total, sizeof(*latencies), mail_compare);
    printf("MAILBOX_RESULT,%zu,%zu,%zu,%u,%s,%s,%zu,%llu,%.3f,%.3f,%d,%.3f,%llu,%zu,%zu,%zu,%zu,%zu,%zu\n",
           bytes, lanes, repeat, pause_ms, retained ? "sg" : "copy", notify ? "wake" : "spin",
           total, (unsigned long long)(finish - start), (double)total * 1e9 / (finish - start),
           (double)(after.cpu_ns - before.cpu_ns) / total, after.cpu_cycles_available,
           (double)(after.cpu_cycles - before.cpu_cycles) / total,
           (unsigned long long)latencies[(total * 99u + 99u) / 100u - 1u],
           polls, busy, wakes, full, buffer_waits, segments);
  }
  if (status != SALTS_OK && !(cancel_after != 0u && status == SALTS_ECANCELED))
    fprintf(stderr, "MAILBOX_ERROR,status=%d,lanes=%zu,bytes=%zu,sg=%d,wake=%d,received=%zu/%zu\n",
                                 status, lanes, bytes, retained, notify, received, total);
  cmeta_cond_destroy(&run.changed); cmeta_mutex_destroy(&run.mutex); free(latencies);
  return status;
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

spec("FlowMQ internal SG mailbox") {
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
        for (int retained = 0; retained <= 1; ++retained)
          for (int notify = 0; notify <= 1; ++notify)
            check_equal(mail_run_case(counts[lane], sizes[size], 8u, 0u, retained, notify, 0u, 0, 0u), SALTS_OK);
  }
  it("mailbox correctness: cancellation joins producers and releases pending SG references") {
    check_equal(mail_run_case(1u, MAIL_MAX_BYTES, 32u, 0u, 1, 1, 0u, 0, MAIL_BATCH), SALTS_ECANCELED);
    check_equal(mail_run_case(MAIL_LANES, MAIL_MAX_BYTES, 32u, 0u, 1, 1, 0u, 0, MAIL_BATCH), SALTS_ECANCELED);
  }
  bench("mailbox comparison: retained SG and copy with spin and wake") {
    const size_t sizes[] = {64u, MAIL_MAX_BYTES};
    const size_t counts[] = {1u, MAIL_LANES};
    printf("MAILBOX_CONFIG,producers_per_lane=%d,capacity=%d,buffers_per_producer=%d,batch=%d,rounds=%d,small_saturated_rounds=%d,repeats=%d,affinity=unbound,receive=slicev\n",
           MAIL_PRODUCERS, MAIL_CAPACITY, MAIL_BUFFERS, MAIL_BATCH, MAIL_ROUNDS, MAIL_SATURATED_ROUNDS, MAIL_REPEATS);
    printf("MAILBOX_HEADER,payload_bytes,lanes,repeat,pause_ms,send,wait,messages,wall_ns,messages_per_second,cpu_ns_per_message,cpu_cycles_available,cpu_cycles_per_message,prepare_receive_p99_ns,poll_calls,send_busy,wake_calls,queue_full,buffer_waits,receive_segments\n");
    for (size_t repeat = 0u; repeat < MAIL_REPEATS; ++repeat)
      for (size_t size = 0u; size < 2u; ++size)
        for (size_t lane = 0u; lane < 2u; ++lane)
          for (size_t load = 0u; load < 2u; ++load)
            for (size_t mode = 0u; mode < 4u; ++mode) {
              const size_t policy = (mode + repeat) % 4u;
              const size_t bytes = sizes[(size + repeat) % 2u];
              const size_t rounds = load == 0u && bytes == 64u ? MAIL_SATURATED_ROUNDS : MAIL_ROUNDS;
              check_equal(mail_run_case(counts[(lane + repeat) % 2u], bytes,
                                        rounds, load != 0u ? 5u : 0u,
                                        (int)(policy / 2u), (int)(policy % 2u), repeat + 1u, 1, 0u), SALTS_OK);
            }
  }
}
