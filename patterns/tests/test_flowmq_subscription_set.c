#include "flowmq_subscription_set.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("flowmq_subscription_set") {
  it("keeps one reference-counted topic fact source") {
    flowmq_subscription_set_t subscriptions;
    const flowmq_subscription_t *entry;
    int changed = 0;
    memset(&subscriptions, 0, sizeof(subscriptions));

    check_int_eq(flowmq_subscription_set_init(&subscriptions), TURBO_OK);
    check_int_eq(
        flowmq_subscription_set_update(&subscriptions, 1, tstr_v_from_cstr("orders."), &changed),
        TURBO_OK);
    check_true(changed);
    check_int_eq(
        flowmq_subscription_set_update(&subscriptions, 1, tstr_v_from_cstr("orders."), &changed),
        TURBO_OK);
    check_size_eq(flowmq_subscription_set_count(&subscriptions), 1u);
    entry = flowmq_subscription_set_at(&subscriptions, 0u);
    check_not_null(entry);
    check_size_eq(entry->refs, 2u);
    check_true(flowmq_subscription_set_match(&subscriptions, tstr_v_from_cstr("orders.created")));
    check_false(
        flowmq_subscription_set_match(&subscriptions, tstr_v_from_cstr("payments.created")));

    check_int_eq(
        flowmq_subscription_set_update(&subscriptions, 0, tstr_v_from_cstr("orders."), &changed),
        TURBO_OK);
    check_size_eq(flowmq_subscription_set_at(&subscriptions, 0u)->refs, 1u);
    check_int_eq(
        flowmq_subscription_set_update(&subscriptions, 0, tstr_v_from_cstr("orders."), &changed),
        TURBO_OK);
    check_size_eq(flowmq_subscription_set_count(&subscriptions), 0u);

    flowmq_subscription_set_destroy(&subscriptions);
  }

  it("treats an empty subscription as match-all") {
    flowmq_subscription_set_t subscriptions;
    int changed = 0;
    memset(&subscriptions, 0, sizeof(subscriptions));

    check_int_eq(flowmq_subscription_set_init(&subscriptions), TURBO_OK);
    check_int_eq(flowmq_subscription_set_update(&subscriptions, 1, (tstr_v){0}, &changed),
                 TURBO_OK);
    check_true(flowmq_subscription_set_match(&subscriptions, tstr_v_from_cstr("any.topic")));
    flowmq_subscription_set_clear(&subscriptions);
    check_false(flowmq_subscription_set_match(&subscriptions, tstr_v_from_cstr("any.topic")));

    flowmq_subscription_set_destroy(&subscriptions);
  }
}
