#include "flowmq_reconnect.h"
#include "tinytest.h"
#include "turbo_error.h"

spec("flowmq_reconnect") {
  it("grows bounded backoff and resets only after success") {
    flowmq_reconnect_t reconnect;
    uint64_t delay_ms = 0u;

    check_int_eq(flowmq_reconnect_init(&reconnect, 20u, 80u, 17u), TURBO_OK);
    check_int_eq(flowmq_reconnect_next(&reconnect, &delay_ms), TURBO_OK);
    check_true(delay_ms >= 10u);
    check_true(delay_ms < 20u);
    check_int_eq(flowmq_reconnect_next(&reconnect, &delay_ms), TURBO_OK);
    check_true(delay_ms >= 20u);
    check_true(delay_ms < 40u);
    check_int_eq(flowmq_reconnect_next(&reconnect, &delay_ms), TURBO_OK);
    check_true(delay_ms >= 40u);
    check_true(delay_ms < 80u);
    check_int_eq(flowmq_reconnect_next(&reconnect, &delay_ms), TURBO_OK);
    check_true(delay_ms >= 40u);
    check_true(delay_ms < 80u);

    flowmq_reconnect_reset(&reconnect);
    check_int_eq(flowmq_reconnect_next(&reconnect, &delay_ms), TURBO_OK);
    check_true(delay_ms >= 10u);
    check_true(delay_ms < 20u);
  }

  it("reports disabled reconnect explicitly") {
    flowmq_reconnect_t reconnect;
    uint64_t delay_ms = 0u;

    check_int_eq(flowmq_reconnect_init(&reconnect, 0u, 0u, 0u), TURBO_OK);
    check_int_eq(flowmq_reconnect_next(&reconnect, &delay_ms), TURBO_ENOENT);
    check_int_eq(flowmq_reconnect_init(&reconnect, 100u, 50u, 0u), TURBO_EINVAL);
  }
}
