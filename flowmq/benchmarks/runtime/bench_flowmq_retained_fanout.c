#include "flowmq_socket.h"
#include "cmeta_error.h"
#include "cmeta_buffer.h"
#include <salts/clock.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  FANOUT_MAX_PEERS = 4u,
  FANOUT_PART_COUNT = 2u,
  FANOUT_VECTOR_CAPACITY = 64u,
  FANOUT_REPEATS = 7u,
  FANOUT_WARMUPS = 8u,
  FANOUT_PROGRESS_LIMIT = 100000u,
  FANOUT_TIMEOUT_MS = 10000u,
  FANOUT_PEER_COUNT_COUNT = 2u,
  FANOUT_PAYLOAD_COUNT = 2u,
  FANOUT_MODE_COUNT = 2u
};

static const size_t FANOUT_PEER_COUNTS[] = {2u, 4u};
static const size_t FANOUT_PART_SIZES[] = {65536u, 262144u};
static const char FANOUT_TOPIC[] = "fanout.";

typedef enum fanout_mode_e {
  FANOUT_COPY = 0,
  FANOUT_RETAINED = 1
} fanout_mode_t;

typedef struct fanout_fixture_s {
  flowmq_ctx_t *ctx;
  flowmq_socket_t *publisher;
  flowmq_socket_t *subscribers[FANOUT_MAX_PEERS];
  flowmq_socket_t *sockets[FANOUT_MAX_PEERS + 1u];
  size_t socket_count;
  size_t peer_count;
  size_t part_size;
  mem_buffer_t *payload_buffer;
  mem_slice_t parts[FANOUT_PART_COUNT];
} fanout_fixture_t;

typedef struct fanout_result_s {
  const char *mode;
  size_t peer_count;
  size_t part_size;
  size_t samples;
  uint64_t wall_ns;
  uint64_t p50_ns;
  uint64_t p95_ns;
  uint64_t p99_ns;
  double publications_per_second;
  double source_mib_per_second;
  double delivered_mib_per_second;
} fanout_result_t;

static size_t fanout_samples(void) {
  const char *ci = getenv("FLOWMQ_BENCH_CI");
  return ci != NULL && strcmp(ci, "0") != 0 ? 31u : 101u;
}

static const char *fanout_mode_name(fanout_mode_t mode) {
  return mode == FANOUT_RETAINED ? "retained" : "copy";
}

static int fanout_progress(fanout_fixture_t *fixture) {
  flowmq_pollitem_t items[FANOUT_MAX_PEERS + 1u] = {0};
  size_t ready = 0u;
  if (fixture == NULL || fixture->socket_count == 0u)
    return SALTS_EINVAL;
  for (size_t i = 0u; i < fixture->socket_count; ++i)
    items[i].socket = fixture->sockets[i];
  return flowmq_poll(items, fixture->socket_count, 0u, &ready);
}

static int fanout_progress_until_subscription_count(
    fanout_fixture_t *fixture, size_t expected) {
  const uint64_t started_ms = cmeta_monotonic_ms();
  size_t observed = 0u;
  unsigned char event[64] = {0};
  size_t event_size = 0u;
  while (observed < expected &&
         cmeta_monotonic_ms() - started_ms < FANOUT_TIMEOUT_MS) {
    int status = fanout_progress(fixture);
    if (status != SALTS_OK) return status;
    status = flowmq_recv(fixture->publisher, event, sizeof(event),
                         &event_size, FLOWMQ_DONTWAIT);
    if (status == SALTS_EBUSY) continue;
    if (status != SALTS_OK) return status;
    if (event_size != sizeof(FANOUT_TOPIC) ||
        event[0] != 1u ||
        memcmp(event + 1u, FANOUT_TOPIC, sizeof(FANOUT_TOPIC) - 1u) != 0)
      return SALTS_EPROTO;
    ++observed;
  }
  return observed == expected ? SALTS_OK : SALTS_ETIMEDOUT;
}

static void fanout_close(fanout_fixture_t *fixture) {
  if (fixture == NULL) return;
  for (size_t i = 0u; i < FANOUT_PART_COUNT; ++i)
    mem_slice_release(&fixture->parts[i]);
  for (size_t i = 0u; i < fixture->peer_count; ++i) {
    if (fixture->subscribers[i] != NULL)
      (void)flowmq_close(fixture->subscribers[i]);
  }
  if (fixture->publisher != NULL)
    (void)flowmq_close(fixture->publisher);
  if (fixture->ctx != NULL)
    (void)flowmq_ctx_term(fixture->ctx);
  mem_buffer_release(fixture->payload_buffer);
  memset(fixture, 0, sizeof(*fixture));
}

static int fanout_open(fanout_fixture_t *fixture, size_t peer_count,
                       size_t part_size) {
  char endpoint[128] = {0};
  size_t endpoint_size = 0u;
  unsigned char *payload;
  size_t total_size;
  int status;
  if (fixture == NULL || peer_count == 0u ||
      peer_count > FANOUT_MAX_PEERS || part_size == 0u ||
      part_size > SIZE_MAX / FANOUT_PART_COUNT)
    return SALTS_EINVAL;
  memset(fixture, 0, sizeof(*fixture));
  fixture->peer_count = peer_count;
  fixture->part_size = part_size;
  total_size = part_size * FANOUT_PART_COUNT;

  fixture->ctx = flowmq_ctx_new();
  if (fixture->ctx == NULL) return SALTS_ENOMEM;
  fixture->publisher = flowmq_socket(fixture->ctx, FLOWMQ_XPUB);
  if (fixture->publisher == NULL) return SALTS_ENOMEM;
  fixture->sockets[fixture->socket_count++] = fixture->publisher;

  status = flowmq_bind(fixture->publisher, "tcp://127.0.0.1:0");
  if (status != SALTS_OK) return status;
  status = flowmq_last_endpoint(fixture->publisher, endpoint,
                                sizeof(endpoint), &endpoint_size);
  if (status != SALTS_OK) return status;

  for (size_t i = 0u; i < peer_count; ++i) {
    fixture->subscribers[i] = flowmq_socket(fixture->ctx, FLOWMQ_XSUB);
    if (fixture->subscribers[i] == NULL) return SALTS_ENOMEM;
    fixture->sockets[fixture->socket_count++] = fixture->subscribers[i];
    status = flowmq_setsockopt(fixture->subscribers[i], FLOWMQ_SUBSCRIBE,
                               FANOUT_TOPIC, sizeof(FANOUT_TOPIC) - 1u);
    if (status != SALTS_OK) return status;
    status = flowmq_connect(fixture->subscribers[i], endpoint);
    if (status != SALTS_OK) return status;
  }

  status = fanout_progress_until_subscription_count(fixture, peer_count);
  if (status != SALTS_OK) return status;

  fixture->payload_buffer = mem_get_buffer(mem_global(), total_size);
  if (fixture->payload_buffer == NULL) return SALTS_ENOMEM;
  payload = (unsigned char *)mem_buffer_data(fixture->payload_buffer);
  if (payload == NULL) return SALTS_EPROTO;
  for (size_t i = 0u; i < total_size; ++i)
    payload[i] = (unsigned char)((i * 131u + 17u) & 0xffu);
  memcpy(payload, FANOUT_TOPIC, sizeof(FANOUT_TOPIC) - 1u);
  mem_set_used(fixture->payload_buffer, total_size);

  for (size_t i = 0u; i < FANOUT_PART_COUNT; ++i) {
    fixture->parts[i] =
        mem_slice(fixture->payload_buffer, i * part_size, part_size);
    if (fixture->parts[i].buffer == NULL ||
        fixture->parts[i].length != part_size)
      return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int fanout_send_retry(fanout_fixture_t *fixture, fanout_mode_t mode,
                             size_t part, int flags) {
  const uint64_t started_ms = cmeta_monotonic_ms();
  int status = SALTS_EBUSY;
  for (size_t i = 0u; i < FANOUT_PROGRESS_LIMIT &&
                      (status == SALTS_EBUSY || status == SALTS_ENOBUFS);
       ++i) {
    status = fanout_progress(fixture);
    if (status != SALTS_OK) return status;
    status = mode == FANOUT_RETAINED
                 ? flowmq_send_slice(fixture->publisher,
                                     &fixture->parts[part],
                                     flags | FLOWMQ_DONTWAIT)
                 : flowmq_send(fixture->publisher,
                               fixture->parts[part].data,
                               fixture->parts[part].length,
                               flags | FLOWMQ_DONTWAIT);
    if ((status == SALTS_EBUSY || status == SALTS_ENOBUFS) &&
        cmeta_monotonic_ms() - started_ms >= FANOUT_TIMEOUT_MS)
      return SALTS_ETIMEDOUT;
  }
  return status;
}

static int fanout_recv_part(fanout_fixture_t *fixture,
                            flowmq_socket_t *subscriber,
                            size_t part,
                            int expected_more,
                            int validate_payload) {
  mem_slice_t segments[FANOUT_VECTOR_CAPACITY] = {0};
  const unsigned char *expected =
      (const unsigned char *)fixture->parts[part].data;
  const uint64_t started_ms = cmeta_monotonic_ms();
  size_t count = 0u;
  size_t total = 0u;
  size_t offset = 0u;
  size_t option_size = sizeof(int);
  int more = -1;
  int status = SALTS_EBUSY;

  for (size_t i = 0u; i < FANOUT_PROGRESS_LIMIT && status == SALTS_EBUSY;
       ++i) {
    status = fanout_progress(fixture);
    if (status != SALTS_OK) return status;
    count = 0u;
    status = flowmq_recv_slicev(subscriber, segments,
                                FANOUT_VECTOR_CAPACITY, &count,
                                FLOWMQ_DONTWAIT);
    if (status == SALTS_EBUSY &&
        cmeta_monotonic_ms() - started_ms >= FANOUT_TIMEOUT_MS)
      return SALTS_ETIMEDOUT;
  }
  if (status != SALTS_OK) return status;
  if (count == 0u || count > FANOUT_VECTOR_CAPACITY) {
    status = SALTS_EPROTO;
    goto cleanup;
  }

  for (size_t i = 0u; i < count; ++i) {
    if (segments[i].buffer == NULL || segments[i].data == NULL ||
        segments[i].length > fixture->part_size - total) {
      status = SALTS_EPROTO;
      goto cleanup;
    }
    if (validate_payload &&
        memcmp(segments[i].data, expected + offset,
               segments[i].length) != 0) {
      status = SALTS_EPROTO;
      goto cleanup;
    }
    total += segments[i].length;
    offset += segments[i].length;
  }
  if (total != fixture->part_size) {
    status = SALTS_EPROTO;
    goto cleanup;
  }

  status = flowmq_getsockopt(subscriber, FLOWMQ_RCVMORE,
                             &more, &option_size);
  if (status == SALTS_OK && more != expected_more)
    status = SALTS_EPROTO;

cleanup:
  for (size_t i = 0u; i < count && i < FANOUT_VECTOR_CAPACITY; ++i)
    mem_slice_release(&segments[i]);
  return status;
}

static int fanout_cycle(fanout_fixture_t *fixture, fanout_mode_t mode,
                        int validate_payload) {
  int status = fanout_send_retry(fixture, mode, 0u, FLOWMQ_SNDMORE);
  if (status == SALTS_OK)
    status = fanout_send_retry(fixture, mode, 1u, 0);
  if (status != SALTS_OK) return status;

  for (size_t peer = 0u; peer < fixture->peer_count; ++peer) {
    status = fanout_recv_part(fixture, fixture->subscribers[peer],
                              0u, 1, validate_payload);
    if (status != SALTS_OK) return status;
    status = fanout_recv_part(fixture, fixture->subscribers[peer],
                              1u, 0, validate_payload);
    if (status != SALTS_OK) return status;
  }

  /* Settle sender-side CNet completions before the next publication. */
  for (size_t i = 0u; i < 2u; ++i) {
    status = fanout_progress(fixture);
    if (status != SALTS_OK) return status;
  }
  return SALTS_OK;
}

static int fanout_u64_compare(const void *left, const void *right) {
  const uint64_t a = *(const uint64_t *)left;
  const uint64_t b = *(const uint64_t *)right;
  return a < b ? -1 : a > b ? 1 : 0;
}

static uint64_t fanout_percentile(const uint64_t *values, size_t count,
                                  size_t percentile) {
  uint64_t *copy;
  size_t index;
  uint64_t result;
  if (values == NULL || count == 0u) return 0u;
  copy = (uint64_t *)malloc(count * sizeof(*copy));
  if (copy == NULL) return 0u;
  memcpy(copy, values, count * sizeof(*copy));
  qsort(copy, count, sizeof(*copy), fanout_u64_compare);
  index = ((count - 1u) * percentile) / 100u;
  result = copy[index];
  free(copy);
  return result;
}

static int fanout_double_compare(const void *left, const void *right) {
  const double a = *(const double *)left;
  const double b = *(const double *)right;
  return a < b ? -1 : a > b ? 1 : 0;
}

static double fanout_double_median(double *values, size_t count) {
  qsort(values, count, sizeof(*values), fanout_double_compare);
  return values[count / 2u];
}

static int fanout_run(fanout_mode_t mode, size_t peer_count,
                      size_t part_size, fanout_result_t *out) {
  fanout_fixture_t fixture;
  const size_t samples = fanout_samples();
  uint64_t *latencies = NULL;
  uint64_t wall_started = 0u;
  uint64_t wall_ns = 0u;
  int status = SALTS_OK;

  if (out == NULL) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  memset(&fixture, 0, sizeof(fixture));
  latencies = (uint64_t *)calloc(samples, sizeof(*latencies));
  if (latencies == NULL) return SALTS_ENOMEM;

  status = fanout_open(&fixture, peer_count, part_size);
  if (status != SALTS_OK) goto cleanup;

  for (size_t i = 0u; i < FANOUT_WARMUPS; ++i) {
    status = fanout_cycle(&fixture, mode, 1);
    if (status != SALTS_OK) goto cleanup;
  }

  wall_started = cmeta_hrtime();
  for (size_t sample = 0u; sample < samples; ++sample) {
    const uint64_t started = cmeta_hrtime();
    uint64_t finished;
    status = fanout_cycle(&fixture, mode, 0);
    if (status != SALTS_OK) goto cleanup;
    finished = cmeta_hrtime();
    if (finished <= started) {
      status = SALTS_EIO;
      goto cleanup;
    }
    latencies[sample] = finished - started;
  }
  wall_ns = cmeta_hrtime() - wall_started;
  if (wall_ns == 0u) {
    status = SALTS_EIO;
    goto cleanup;
  }

  out->mode = fanout_mode_name(mode);
  out->peer_count = peer_count;
  out->part_size = part_size;
  out->samples = samples;
  out->wall_ns = wall_ns;
  out->p50_ns = fanout_percentile(latencies, samples, 50u);
  out->p95_ns = fanout_percentile(latencies, samples, 95u);
  out->p99_ns = fanout_percentile(latencies, samples, 99u);
  out->publications_per_second =
      (double)samples * 1.0e9 / (double)wall_ns;
  out->source_mib_per_second =
      ((double)samples * (double)part_size * FANOUT_PART_COUNT /
       (1024.0 * 1024.0)) * 1.0e9 / (double)wall_ns;
  out->delivered_mib_per_second =
      out->source_mib_per_second * (double)peer_count;

cleanup:
  fanout_close(&fixture);
  free(latencies);
  return status;
}

static FILE *fanout_open_csv(void) {
  const char *prefix = getenv("FLOWMQ_FANOUT_OUTPUT");
  char path[1024];
  if (prefix == NULL || *prefix == '\0') return NULL;
  if (snprintf(path, sizeof(path), "%s.csv", prefix) < 0) return NULL;
  return fopen(path, "w");
}

static int fanout_write_csv(FILE *csv, size_t repeat,
                            const fanout_result_t *result) {
  if (csv == NULL || result == NULL) return SALTS_OK;
  return fprintf(
             csv,
             "%s,%zu,%zu,%zu,%zu,%" PRIu64 ",%" PRIu64 ",%" PRIu64
             ",%" PRIu64 ",%.6f,%.6f,%.6f\n",
             result->mode, result->peer_count, result->part_size,
             repeat, result->samples, result->wall_ns,
             result->p50_ns, result->p95_ns, result->p99_ns,
             result->publications_per_second,
             result->source_mib_per_second,
             result->delivered_mib_per_second) < 0
             ? SALTS_EIO
             : SALTS_OK;
}

int main(void) {
  fanout_result_t
      results[FANOUT_PEER_COUNT_COUNT][FANOUT_PAYLOAD_COUNT]
             [FANOUT_MODE_COUNT][FANOUT_REPEATS];
  FILE *csv = fanout_open_csv();
  const char *output = getenv("FLOWMQ_FANOUT_OUTPUT");
  int status = SALTS_OK;

  memset(results, 0, sizeof(results));
  if (output != NULL && *output != '\0' && csv == NULL) {
    fprintf(stderr, "failed to create fanout benchmark CSV\n");
    return 2;
  }
  if (csv != NULL)
    fprintf(csv,
            "mode,peer_count,part_size,repeat,samples,wall_ns,"
            "p50_ns,p95_ns,p99_ns,publications_per_second,"
            "source_mib_per_second,delivered_mib_per_second\n");

  for (size_t peers = 0u; peers < FANOUT_PEER_COUNT_COUNT; ++peers) {
    for (size_t payload = 0u; payload < FANOUT_PAYLOAD_COUNT; ++payload) {
      for (size_t repeat = 0u; repeat < FANOUT_REPEATS; ++repeat) {
        for (size_t offset = 0u; offset < FANOUT_MODE_COUNT; ++offset) {
          const fanout_mode_t mode =
              (fanout_mode_t)((peers + payload + repeat + offset) %
                              FANOUT_MODE_COUNT);
          fanout_result_t *result =
              &results[peers][payload][mode][repeat];
          status = fanout_run(mode, FANOUT_PEER_COUNTS[peers],
                              FANOUT_PART_SIZES[payload], result);
          if (status != SALTS_OK) {
            fprintf(stderr,
                    "fanout benchmark failed: mode=%s peers=%zu "
                    "part=%zu repeat=%zu status=%d\n",
                    fanout_mode_name(mode), FANOUT_PEER_COUNTS[peers],
                    FANOUT_PART_SIZES[payload], repeat + 1u, status);
            goto cleanup;
          }
          status = fanout_write_csv(csv, repeat + 1u, result);
          if (status != SALTS_OK) goto cleanup;
        }
      }
    }
  }

  printf("# FlowMQ retained fanout evidence\n\n");
  printf(
      "XPUB fanout uses two-part application messages with an identical "
      "subscription snapshot and equal receiver work. Copy uses flowmq_send; "
      "retained uses flowmq_send_slice over the same immutable backing. "
      "Completion latency ends after all selected peers receive both parts. "
      "No relative performance threshold is enforced.\n\n");
  printf("| peers | part bytes | mode | publications/s median | "
         "source MiB/s median | delivered MiB/s median | "
         "p50 us median | p95 us median | p99 us median |\n");
  printf("| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");

  for (size_t peers = 0u; peers < FANOUT_PEER_COUNT_COUNT; ++peers) {
    for (size_t payload = 0u; payload < FANOUT_PAYLOAD_COUNT; ++payload) {
      double rate[FANOUT_MODE_COUNT][FANOUT_REPEATS];
      double source_mib[FANOUT_MODE_COUNT][FANOUT_REPEATS];
      double delivered_mib[FANOUT_MODE_COUNT][FANOUT_REPEATS];
      double p50[FANOUT_MODE_COUNT][FANOUT_REPEATS];
      double p95[FANOUT_MODE_COUNT][FANOUT_REPEATS];
      double p99[FANOUT_MODE_COUNT][FANOUT_REPEATS];
      double speedup[FANOUT_REPEATS];

      for (size_t mode = 0u; mode < FANOUT_MODE_COUNT; ++mode) {
        for (size_t repeat = 0u; repeat < FANOUT_REPEATS; ++repeat) {
          const fanout_result_t *r =
              &results[peers][payload][mode][repeat];
          rate[mode][repeat] = r->publications_per_second;
          source_mib[mode][repeat] = r->source_mib_per_second;
          delivered_mib[mode][repeat] = r->delivered_mib_per_second;
          p50[mode][repeat] = (double)r->p50_ns / 1000.0;
          p95[mode][repeat] = (double)r->p95_ns / 1000.0;
          p99[mode][repeat] = (double)r->p99_ns / 1000.0;
        }
        printf("| %zu | %zu | %s | %.2f | %.2f | %.2f | %.3f | %.3f | %.3f |\n",
               FANOUT_PEER_COUNTS[peers], FANOUT_PART_SIZES[payload],
               fanout_mode_name((fanout_mode_t)mode),
               fanout_double_median(rate[mode], FANOUT_REPEATS),
               fanout_double_median(source_mib[mode], FANOUT_REPEATS),
               fanout_double_median(delivered_mib[mode], FANOUT_REPEATS),
               fanout_double_median(p50[mode], FANOUT_REPEATS),
               fanout_double_median(p95[mode], FANOUT_REPEATS),
               fanout_double_median(p99[mode], FANOUT_REPEATS));
      }

      for (size_t repeat = 0u; repeat < FANOUT_REPEATS; ++repeat)
        speedup[repeat] =
            results[peers][payload][FANOUT_RETAINED][repeat]
                    .publications_per_second /
            results[peers][payload][FANOUT_COPY][repeat]
                    .publications_per_second;
      printf("\n%zu peers, %zu-byte parts retained/copy paired median "
             "publication throughput: %.3fx\n\n",
             FANOUT_PEER_COUNTS[peers], FANOUT_PART_SIZES[payload],
             fanout_double_median(speedup, FANOUT_REPEATS));
    }
  }

cleanup:
  if (csv != NULL) fclose(csv);
  return status == SALTS_OK ? 0 : 1;
}
