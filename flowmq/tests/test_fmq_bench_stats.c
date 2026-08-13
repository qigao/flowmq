#include "fmq_bench_stats.h"
#include "tinytest.h"

#include <stdlib.h>

spec("flow_fmq_bench_stats") {
  it("computes standard nearest-rank percentiles") {
    uint64_t one[] = {7u};
    uint64_t two[] = {10u, 20u};
    uint64_t four[] = {10u, 20u, 30u, 40u};
    uint64_t values[256];

    for (size_t i = 0u; i < 256u; ++i) values[i] = (uint64_t)i + 1u;
    check_uint_eq(fmq_bench_percentile(one, 1u, 50u), 7u);
    check_uint_eq(fmq_bench_percentile(two, 2u, 50u), 10u);
    check_uint_eq(fmq_bench_percentile(four, 4u, 50u), 20u);
    check_uint_eq(fmq_bench_percentile(four, 4u, 75u), 30u);
    check_uint_eq(fmq_bench_percentile(values, 256u, 50u), 128u);
    check_uint_eq(fmq_bench_percentile(values, 256u, 95u), 244u);
    check_uint_eq(fmq_bench_percentile(values, 256u, 99u), 254u);
    check_uint_eq(fmq_bench_percentile(values, 256u, 100u), 256u);
  }

  it("rejects invalid percentile requests") {
    uint64_t values[] = {1u, 2u};
    check_uint_eq(fmq_bench_percentile(NULL, 2u, 50u), 0u);
    check_uint_eq(fmq_bench_percentile(values, 0u, 50u), 0u);
    check_uint_eq(fmq_bench_percentile(values, 2u, 0u), 0u);
    check_uint_eq(fmq_bench_percentile(values, 2u, 101u), 0u);
  }

  it("sorts unsigned latency samples without subtraction overflow") {
    uint64_t values[] = {UINT64_MAX, 0u, 9u, UINT64_C(1) << 63u};
    qsort(values, sizeof(values) / sizeof(values[0]), sizeof(values[0]), fmq_bench_u64_compare);
    check_uint_eq(values[0], 0u);
    check_uint_eq(values[1], 9u);
    check_uint_eq(values[2], UINT64_C(1) << 63u);
    check_uint_eq(values[3], UINT64_MAX);
  }
}
