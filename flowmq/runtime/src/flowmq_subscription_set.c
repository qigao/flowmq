#include "flowmq_subscription_set.h"

#include "turbo_error.h"

#include <stdint.h>
#include <string.h>

int flowmq_subscription_set_init(flowmq_subscription_set_t *subscriptions) {
  int rc;
  if (!subscriptions || subscriptions->initialized) return TURBO_EINVAL;
  rc = turbo_vec_init(&subscriptions->entries, sizeof(flowmq_subscription_t));
  if (rc != TURBO_OK) return rc;
  subscriptions->initialized = 1;
  return TURBO_OK;
}

void flowmq_subscription_set_clear(flowmq_subscription_set_t *subscriptions) {
  if (!subscriptions || !subscriptions->initialized) return;
  for (size_t i = 0u; i < turbo_vec_size(&subscriptions->entries); ++i) {
    flowmq_subscription_t *subscription =
        (flowmq_subscription_t *)turbo_vec_at(&subscriptions->entries, i);
    if (subscription) tstr_freep(&subscription->topic);
  }
  turbo_vec_clear(&subscriptions->entries);
}

void flowmq_subscription_set_destroy(flowmq_subscription_set_t *subscriptions) {
  if (!subscriptions) return;
  if (subscriptions->initialized) {
    flowmq_subscription_set_clear(subscriptions);
    turbo_vec_destroy(&subscriptions->entries);
  }
  memset(subscriptions, 0, sizeof(*subscriptions));
}

int flowmq_subscription_set_update(flowmq_subscription_set_t *subscriptions, int subscribe,
                                   tstr_v topic, int *changed) {
  if (!subscriptions || !subscriptions->initialized || !changed || (topic.len > 0u && !topic.data))
    return TURBO_EINVAL;
  *changed = 0;
  for (size_t i = 0u; i < turbo_vec_size(&subscriptions->entries); ++i) {
    flowmq_subscription_t *existing =
        (flowmq_subscription_t *)turbo_vec_at(&subscriptions->entries, i);
    size_t existing_len = existing && existing->topic ? tstr_len(existing->topic) : 0u;
    if (existing_len != topic.len ||
        (topic.len > 0u && memcmp(existing->topic, topic.data, topic.len) != 0))
      continue;
    if (subscribe) {
      if (existing->refs == SIZE_MAX) return TURBO_ERANGE;
      existing->refs += 1u;
    } else if (existing->refs > 1u) {
      existing->refs -= 1u;
    } else {
      flowmq_subscription_t removed;
      int rc = turbo_vec_swap_remove(&subscriptions->entries, i, &removed);
      if (rc != TURBO_OK) return rc;
      tstr_freep(&removed.topic);
    }
    *changed = 1;
    return TURBO_OK;
  }
  if (!subscribe) return TURBO_OK;
  {
    flowmq_subscription_t value = {0};
    int rc;
    value.topic = tstr_new_len(topic.data, topic.len);
    value.refs = 1u;
    if (!value.topic) return TURBO_ENOMEM;
    rc = turbo_vec_push(&subscriptions->entries, &value);
    if (rc != TURBO_OK) tstr_free(value.topic);
    if (rc != TURBO_OK) return rc;
  }
  *changed = 1;
  return TURBO_OK;
}

size_t flowmq_subscription_set_count(const flowmq_subscription_set_t *subscriptions) {
  return subscriptions && subscriptions->initialized ? turbo_vec_size(&subscriptions->entries) : 0u;
}

const flowmq_subscription_t *
flowmq_subscription_set_at(const flowmq_subscription_set_t *subscriptions, size_t index) {
  if (!subscriptions || !subscriptions->initialized) return NULL;
  return (const flowmq_subscription_t *)turbo_vec_at_const(&subscriptions->entries, index);
}

int flowmq_subscription_set_match(const flowmq_subscription_set_t *subscriptions, tstr_v topic) {
  if (!subscriptions || !subscriptions->initialized || (topic.len > 0u && !topic.data)) return 0;
  for (size_t i = 0u; i < turbo_vec_size(&subscriptions->entries); ++i) {
    const flowmq_subscription_t *subscription = flowmq_subscription_set_at(subscriptions, i);
    size_t prefix_len = subscription && subscription->topic ? tstr_len(subscription->topic) : 0u;
    if (prefix_len == 0u ||
        (topic.len >= prefix_len && memcmp(topic.data, subscription->topic, prefix_len) == 0))
      return 1;
  }
  return 0;
}
