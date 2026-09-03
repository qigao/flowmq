#ifndef FLOWMQ_SUBSCRIPTION_SET_H
#define FLOWMQ_SUBSCRIPTION_SET_H

#include "turbo_str.h"
#include <rocida/stl.h>

#include <stddef.h>

typedef struct flowmq_subscription_s {
  tstr topic;
  size_t refs;
} flowmq_subscription_t;

typedef struct flowmq_subscription_set_s {
  vec_t entries;
  int initialized;
} flowmq_subscription_set_t;

int flowmq_subscription_set_init(flowmq_subscription_set_t *subscriptions);
void flowmq_subscription_set_clear(flowmq_subscription_set_t *subscriptions);
void flowmq_subscription_set_destroy(flowmq_subscription_set_t *subscriptions);
int flowmq_subscription_set_update(flowmq_subscription_set_t *subscriptions, int subscribe,
                                   vstr topic, int *changed);
size_t flowmq_subscription_set_count(const flowmq_subscription_set_t *subscriptions);
const flowmq_subscription_t *
flowmq_subscription_set_at(const flowmq_subscription_set_t *subscriptions, size_t index);
int flowmq_subscription_set_match(const flowmq_subscription_set_t *subscriptions, vstr topic);

#endif /* FLOWMQ_SUBSCRIPTION_SET_H */
