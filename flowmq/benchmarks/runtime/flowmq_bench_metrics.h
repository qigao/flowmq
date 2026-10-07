#ifndef FLOWMQ_BENCH_METRICS_H
#define FLOWMQ_BENCH_METRICS_H

#include <salts/error_codes.h>
#include <stdint.h>

/* Test-only OS adapter: Salts exposes a wall clock, but no process CPU/RSS
 * query. Peak RSS is process-lifetime high water, never a per-case delta. */
typedef struct flowmq_bench_metrics_s {
  uint64_t cpu_ns;
  uint64_t peak_rss_bytes;
} flowmq_bench_metrics_t;

#if defined(_WIN32)
#include <windows.h>
/* PSAPI declarations require the Windows base types above. */
#include <psapi.h>

static int flowmq_bench_metrics_read(flowmq_bench_metrics_t *out) {
  FILETIME created, exited, kernel, user;
  ULARGE_INTEGER kernel_ticks, user_ticks;
  PROCESS_MEMORY_COUNTERS memory = {0};
  const uint64_t ns_per_tick = 100u;
  if (!GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user) ||
      !GetProcessMemoryInfo(GetCurrentProcess(), &memory, sizeof(memory)))
    return SALTS_EIO;
  kernel_ticks.LowPart = kernel.dwLowDateTime;
  kernel_ticks.HighPart = kernel.dwHighDateTime;
  user_ticks.LowPart = user.dwLowDateTime;
  user_ticks.HighPart = user.dwHighDateTime;
  out->cpu_ns = (kernel_ticks.QuadPart + user_ticks.QuadPart) * ns_per_tick;
  out->peak_rss_bytes = (uint64_t)memory.PeakWorkingSetSize;
  return SALTS_OK;
}
#else
  #include <sys/resource.h>

static int flowmq_bench_metrics_read(flowmq_bench_metrics_t *out) {
  struct rusage usage;
  const uint64_t ns_per_second = 1000000000u;
  const uint64_t ns_per_microsecond = 1000u;
  if (getrusage(RUSAGE_SELF, &usage) != 0) return SALTS_EIO;
  out->cpu_ns =
      ((uint64_t)usage.ru_utime.tv_sec + (uint64_t)usage.ru_stime.tv_sec) * ns_per_second +
      ((uint64_t)usage.ru_utime.tv_usec + (uint64_t)usage.ru_stime.tv_usec) * ns_per_microsecond;
  #if defined(__APPLE__)
  out->peak_rss_bytes = (uint64_t)usage.ru_maxrss;
  #else
  out->peak_rss_bytes = (uint64_t)usage.ru_maxrss * 1024u;
  #endif
  return SALTS_OK;
}
#endif

#endif
