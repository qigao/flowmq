/**
 * @file flowmq_stream_partition.c
 * Stream partition and consumer group implementation.
 */

#include "flowmq_stream_partition.h"
#include "flowmq_stl_error_internal.h"

#include <rocida/stl.h>

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>

static size_t flowmq_stream_vstr_hash(const void *key, size_t key_size, void *ctx) {
  (void)key_size;
  (void)ctx;
  const vstr *view = (const vstr *)key;
  if (!view || !view->data) {
    return 0;
  }
  size_t hash = 1469598103934665603ULL;
  for (size_t i = 0; i < view->len; ++i) {
    hash ^= (uint8_t)view->data[i];
    hash *= 1099511628211ULL;
  }
  return hash;
}

static bool flowmq_stream_vstr_eq(const void *left,
                                    const void *right,
                                    size_t key_size,
                                    void *ctx) {
  (void)key_size;
  (void)ctx;
  const vstr *lhs = (const vstr *)left;
  const vstr *rhs = (const vstr *)right;
  if (!lhs || !rhs) {
    return lhs == rhs;
  }
  if (lhs->len != rhs->len) {
    return false;
  }
  if (lhs->len == 0) {
    return true;
  }
  if (!lhs->data || !rhs->data) {
    return false;
  }
  return memcmp(lhs->data, rhs->data, lhs->len) == 0;
}

/* Partition management */
static int flowmq_stream_partition_init(flowmq_stream_partition_t *partition,
                                       uint32_t partition_id,
                                       uint32_t max_messages,
                                       uint64_t max_bytes,
                                       uint64_t retention_ms) {
  if (!partition) {
    return TURBO_EINVAL;
  }
  
  memset(partition, 0, sizeof(*partition));
  partition->partition_id = partition_id;
  partition->high_watermark = 0;
  partition->low_watermark = 0;
  partition->max_messages = max_messages;
  partition->max_bytes = max_bytes;
  partition->retention_ms = retention_ms;
  
  if (deque_init_bytes(&partition->messages, sizeof(flowmq_stream_message_t),
                             _Alignof(flowmq_stream_message_t),
                             max_messages == 0u ? SIZE_MAX : max_messages) != STL_OK) {
    return TURBO_ENOMEM;
  }
  if (deque_reserve(&partition->messages, max_messages > 0 ? max_messages : 1000) != 0) {
    return TURBO_ENOMEM;
  }
  
  return TURBO_OK;
}

static void flowmq_stream_partition_destroy(flowmq_stream_partition_t *partition) {
  if (!partition) {
    return;
  }
  
  /* Free all message payloads */
  for (size_t i = 0; i < partition->messages.size; i++) {
    flowmq_stream_message_t *msg =
        (flowmq_stream_message_t *)deque_at(&partition->messages, i);
    if (msg) {
      tstr_free(msg->payload);
      tstr_free(msg->topic);
    }
  }
  
  deque_destroy(&partition->messages);
  memset(partition, 0, sizeof(*partition));
}

uint32_t flowmq_stream_partition_apply_retention(flowmq_stream_partition_t *partition,
                                                 uint64_t now_ns) {
  if (!partition) {
    return 0;
  }
  
  uint32_t removed = 0;
  
  /* Time-based retention */
  if (partition->retention_ms > 0) {
    uint64_t retention_ns = partition->retention_ms * 1000000ULL;
    
    while (partition->messages.size > 0) {
      flowmq_stream_message_t *oldest =
          (flowmq_stream_message_t *)deque_front(&partition->messages);
      if (!oldest) {
        break;
      }
      
      if (now_ns < oldest->timestamp_ns + retention_ns) {
        /* Not expired */
        break;
      }
      
      /* Remove expired message */
      partition->current_bytes -= tstr_len(oldest->payload);
      partition->low_watermark = oldest->offset + 1;
      tstr_free(oldest->payload);
      tstr_free(oldest->topic);
      (void)deque_pop_front(&partition->messages, NULL);
      removed++;
    }
  }
  
  /* Capacity-based retention (remove oldest if over limit) */
  while (partition->max_messages > 0 && partition->messages.size > partition->max_messages) {
    flowmq_stream_message_t *oldest =
        (flowmq_stream_message_t *)deque_front(&partition->messages);
    if (oldest) {
      partition->current_bytes -= tstr_len(oldest->payload);
      partition->low_watermark = oldest->offset + 1;
      tstr_free(oldest->payload);
      tstr_free(oldest->topic);
    }
    (void)deque_pop_front(&partition->messages, NULL);
    removed++;
  }
  
  while (partition->max_bytes > 0 && partition->current_bytes > partition->max_bytes) {
    flowmq_stream_message_t *oldest =
        (flowmq_stream_message_t *)deque_front(&partition->messages);
    if (oldest) {
      partition->current_bytes -= tstr_len(oldest->payload);
      partition->low_watermark = oldest->offset + 1;
      tstr_free(oldest->payload);
      tstr_free(oldest->topic);
      (void)deque_pop_front(&partition->messages, NULL);
      removed++;
    } else {
      break;
    }
  }
  
  partition->last_cleanup_ns = now_ns;
  return removed;
}

void flowmq_stream_partition_stats(const flowmq_stream_partition_t *partition,
                                   uint32_t *message_count,
                                   uint64_t *bytes,
                                   uint64_t *low_watermark,
                                   uint64_t *high_watermark) {
  if (!partition) {
    return;
  }
  
  if (message_count) {
    *message_count = (uint32_t)partition->messages.size;
  }
  if (bytes) {
    *bytes = partition->current_bytes;
  }
  if (low_watermark) {
    *low_watermark = partition->low_watermark;
  }
  if (high_watermark) {
    *high_watermark = partition->high_watermark;
  }
}

/* Consumer group management */
static int flowmq_stream_consumer_init(flowmq_stream_consumer_t *consumer,
                                      const char *member_id,
                                      uint64_t now_ns) {
  if (!consumer || !member_id) {
    return TURBO_EINVAL;
  }
  
  memset(consumer, 0, sizeof(*consumer));
  
  consumer->member_id = tstr_dup(member_id);
  if (!consumer->member_id) {
    return TURBO_ENOMEM;
  }
  
  consumer->joined_ns = now_ns;
  consumer->last_heartbeat_ns = now_ns;
  
  if (hash_set_init_bytes(&consumer->assigned_partitions, sizeof(uint32_t),
                                _Alignof(uint32_t), FLOWMQ_STREAM_MAX_PARTITIONS,
                                hash_bytes, hash_key_equal, NULL) != STL_OK) {
    tstr_free(consumer->member_id);
    return TURBO_ENOMEM;
  }
  
  return TURBO_OK;
}

static void flowmq_stream_consumer_destroy(flowmq_stream_consumer_t *consumer) {
  if (!consumer) {
    return;
  }
  
  tstr_free(consumer->member_id);
  hash_set_destroy(&consumer->assigned_partitions);
  memset(consumer, 0, sizeof(*consumer));
}

static int flowmq_stream_consumer_group_init(flowmq_stream_consumer_group_t *group,
                                             const char *group_id) {
  if (!group || !group_id) {
    return TURBO_EINVAL;
  }
  
  memset(group, 0, sizeof(*group));
  
  group->group_id = tstr_dup(group_id);
  if (!group->group_id) {
    return TURBO_ENOMEM;
  }
  
  group->generation = 0;
  group->rebalancing = 0;
  
  if (hash_map_init_bytes(&group->members,
                                sizeof(vstr), _Alignof(vstr),
                                sizeof(flowmq_stream_consumer_t *),
                                _Alignof(flowmq_stream_consumer_t *),
                                FLOWMQ_STREAM_MAX_GROUP_MEMBERS,
                                flowmq_stream_vstr_hash, flowmq_stream_vstr_eq,
                                NULL) != STL_OK) {
    tstr_free(group->group_id);
    return TURBO_ENOMEM;
  }
  if (hash_map_init_bytes(&group->offsets,
                                sizeof(uint32_t), _Alignof(uint32_t),
                                sizeof(uint64_t), _Alignof(uint64_t),
                                FLOWMQ_STREAM_MAX_PARTITIONS,
                                hash_bytes, hash_key_equal, NULL) != STL_OK) {
    hash_map_destroy(&group->members);
    tstr_free(group->group_id);
    return TURBO_ENOMEM;
  }
  
  return TURBO_OK;
}

static void flowmq_stream_consumer_group_destroy(flowmq_stream_consumer_group_t *group) {
  if (!group) {
    return;
  }
  
  for (size_t slot = 0; slot < hash_map_capacity(&group->members); ++slot) {
    flowmq_stream_consumer_t **consumer = (flowmq_stream_consumer_t **)hash_map_value_at(
        &group->members, slot);
    if (!consumer || !*consumer) {
      continue;
    }
    flowmq_stream_consumer_destroy(*consumer);
    free(*consumer);
  }
  
  tstr_free(group->group_id);
  hash_map_destroy(&group->members);
  hash_map_destroy(&group->offsets);
  memset(group, 0, sizeof(*group));
}

int flowmq_stream_consumer_group_join(flowmq_stream_consumer_group_t *group,
                                      const char *member_id,
                                      uint64_t now_ns) {
  if (!group || !member_id) {
    return TURBO_EINVAL;
  }
  
  /* Check if already member */
  vstr member_key = vstr_from_cstr(member_id);
  if (hash_map_contains(&group->members, &member_key)) {
    /* Already joined - update heartbeat */
    flowmq_stream_consumer_t **consumer_ptr =
        (flowmq_stream_consumer_t **)hash_map_get(&group->members, &member_key);
    if (consumer_ptr && *consumer_ptr) {
      (*consumer_ptr)->last_heartbeat_ns = now_ns;
    }
    return TURBO_OK;
  }
  
  /* Check capacity */
  if (group->members.size >= FLOWMQ_STREAM_MAX_GROUP_MEMBERS) {
    return TURBO_ENOSPC;
  }
  
  /* Create new consumer */
  flowmq_stream_consumer_t *consumer =
      (flowmq_stream_consumer_t *)malloc(sizeof(flowmq_stream_consumer_t));
  if (!consumer) {
    return TURBO_ENOMEM;
  }
  
  int rc = flowmq_stream_consumer_init(consumer, member_id, now_ns);
  if (rc != TURBO_OK) {
    free(consumer);
    return rc;
  }
  
  /* Add to group */
  vstr key = vstr_from_cstr(consumer->member_id);
  if (hash_map_put(&group->members, &key, &consumer) != TURBO_OK) {
    flowmq_stream_consumer_destroy(consumer);
    free(consumer);
    return TURBO_ENOMEM;
  }
  
  /* Trigger rebalance */
  group->rebalancing = 1;
  
  return TURBO_OK;
}

int flowmq_stream_consumer_group_leave(flowmq_stream_consumer_group_t *group,
                                       const char *member_id) {
  if (!group || !member_id) {
    return TURBO_EINVAL;
  }
  
  vstr member_key = vstr_from_cstr(member_id);
  flowmq_stream_consumer_t **consumer_ptr =
      (flowmq_stream_consumer_t **)hash_map_get(&group->members, &member_key);
  if (!consumer_ptr || !*consumer_ptr) {
    return TURBO_ENOENT;
  }
  
  flowmq_stream_consumer_t *consumer = *consumer_ptr;
  if (hash_map_remove(&group->members, &member_key, NULL) != STL_OK) {
    return TURBO_EPROTO;
  }
  flowmq_stream_consumer_destroy(consumer);
  free(consumer);

  /* Trigger rebalance */
  group->rebalancing = 1;
  
  return TURBO_OK;
}

int flowmq_stream_consumer_group_commit(flowmq_stream_consumer_group_t *group,
                                        uint32_t partition_id,
                                        uint64_t offset) {
  if (!group) {
    return TURBO_EINVAL;
  }
  
  /* Insert or update offset */
  uint64_t *existing = (uint64_t *)hash_map_get(&group->offsets, &partition_id);
  if (existing) {
    *existing = offset;
  } else {
    if (hash_map_put(&group->offsets, &partition_id, &offset) != TURBO_OK) {
      return TURBO_ENOMEM;
    }
  }
  
  return TURBO_OK;
}

int flowmq_stream_consumer_group_get_offset(const flowmq_stream_consumer_group_t *group,
                                            uint32_t partition_id,
                                            uint64_t *out_offset) {
  if (!group || !out_offset) {
    return TURBO_EINVAL;
  }
  
  const uint64_t *offset =
      (const uint64_t *)hash_map_get_const(&group->offsets, &partition_id);
  *out_offset = offset ? *offset : 0;
  return TURBO_OK;
}

int flowmq_stream_consumer_group_rebalance(flowmq_stream_consumer_group_t *group,
                                           uint32_t partition_count) {
  if (!group) {
    return TURBO_EINVAL;
  }
  
  if (group->members.size == 0) {
    group->rebalancing = 0;
    return TURBO_OK;
  }
  
  /* Clear all existing assignments */
  for (size_t slot = 0; slot < hash_map_capacity(&group->members); ++slot) {
    flowmq_stream_consumer_t **consumer = (flowmq_stream_consumer_t **)hash_map_value_at(
        &group->members, slot);
    if (!consumer || !*consumer) {
      continue;
    }
    hash_set_clear(&(*consumer)->assigned_partitions);
  }
  
  /* Round-robin assignment */
  uint32_t member_idx = 0;
  flowmq_stream_consumer_t **members =
      (flowmq_stream_consumer_t **)malloc(group->members.size * sizeof(flowmq_stream_consumer_t *));
  if (!members) {
    return TURBO_ENOMEM;
  }
  
  uint32_t i = 0;
  for (size_t slot = 0; slot < hash_map_capacity(&group->members); ++slot) {
    flowmq_stream_consumer_t **consumer = (flowmq_stream_consumer_t **)hash_map_value_at(
        &group->members, slot);
    if (!consumer || !*consumer) {
      continue;
    }
    members[i++] = *consumer;
  }
  
  for (uint32_t partition_id = 0; partition_id < partition_count; partition_id++) {
    flowmq_stream_consumer_t *consumer = members[member_idx];
    if (consumer &&
        hash_set_add(&consumer->assigned_partitions, &partition_id) != STL_OK) {
      free(members);
      return TURBO_ENOMEM;
    }
    member_idx = (member_idx + 1) % group->members.size;
  }
  
  free(members);
  
  group->generation++;
  group->rebalancing = 0;
  
  return TURBO_OK;
}

/* Topic management */
int flowmq_stream_topic_init(flowmq_stream_topic_t *topic,
                             const char *topic_name,
                             uint32_t partition_count,
                             uint32_t max_messages_per_partition,
                             uint64_t max_bytes_per_partition,
                             uint64_t retention_ms) {
  if (!topic || !topic_name || partition_count == 0 ||
      partition_count > FLOWMQ_STREAM_MAX_PARTITIONS) {
    return TURBO_EINVAL;
  }
  
  memset(topic, 0, sizeof(*topic));
  
  topic->topic_name = tstr_dup(topic_name);
  if (!topic->topic_name) {
    return TURBO_ENOMEM;
  }
  
  topic->partition_count = partition_count;
  topic->default_max_messages = max_messages_per_partition;
  topic->default_max_bytes = max_bytes_per_partition;
  topic->default_retention_ms = retention_ms;
  
  /* Allocate partitions */
  topic->partitions = (flowmq_stream_partition_t *)calloc(partition_count,
                                                          sizeof(flowmq_stream_partition_t));
  if (!topic->partitions) {
    tstr_free(topic->topic_name);
    return TURBO_ENOMEM;
  }
  
  /* Initialize each partition */
  for (uint32_t i = 0; i < partition_count; i++) {
    int rc = flowmq_stream_partition_init(&topic->partitions[i], i, max_messages_per_partition,
                                         max_bytes_per_partition, retention_ms);
    if (rc != TURBO_OK) {
      /* Cleanup already initialized partitions */
      for (uint32_t j = 0; j < i; j++) {
        flowmq_stream_partition_destroy(&topic->partitions[j]);
      }
      free(topic->partitions);
      tstr_free(topic->topic_name);
      return rc;
    }
  }
  
  /* Initialize consumer groups map */
  if (hash_map_init_bytes(&topic->consumer_groups,
                                sizeof(vstr), _Alignof(vstr),
                                sizeof(flowmq_stream_consumer_group_t *),
                                _Alignof(flowmq_stream_consumer_group_t *),
                                FLOWMQ_STREAM_MAX_CONSUMER_GROUPS,
                                flowmq_stream_vstr_hash, flowmq_stream_vstr_eq,
                                NULL) != STL_OK) {
    for (uint32_t j = 0; j < topic->partition_count; j++) {
      flowmq_stream_partition_destroy(&topic->partitions[j]);
    }
    free(topic->partitions);
    tstr_free(topic->topic_name);
    return TURBO_ENOMEM;
  }
  
  return TURBO_OK;
}

void flowmq_stream_topic_destroy(flowmq_stream_topic_t *topic) {
  if (!topic) {
    return;
  }
  
  /* Destroy partitions */
  if (topic->partitions) {
    for (uint32_t i = 0; i < topic->partition_count; i++) {
      flowmq_stream_partition_destroy(&topic->partitions[i]);
    }
    free(topic->partitions);
  }
  
  /* Destroy consumer groups */
  for (size_t slot = 0; slot < hash_map_capacity(&topic->consumer_groups); ++slot) {
    flowmq_stream_consumer_group_t **group =
        (flowmq_stream_consumer_group_t **)hash_map_value_at(&topic->consumer_groups,
                                                                  slot);
    if (!group || !*group) {
      continue;
    }
    flowmq_stream_consumer_group_t *group_ptr = *group;
    flowmq_stream_consumer_group_destroy(group_ptr);
    free(group_ptr);
  }
  
  hash_map_destroy(&topic->consumer_groups);
  tstr_free(topic->topic_name);
  memset(topic, 0, sizeof(*topic));
}

int flowmq_stream_topic_publish(flowmq_stream_topic_t *topic,
                                uint32_t partition_id,
                                tstr *payload,
                                tstr *topic_str,
                                uint64_t timestamp_ns,
                                uint64_t *out_offset) {
  if (!topic || !payload || !topic_str || !out_offset) {
    return TURBO_EINVAL;
  }
  
  if (partition_id >= topic->partition_count) {
    return TURBO_ERANGE;
  }
  
  flowmq_stream_partition_t *partition = &topic->partitions[partition_id];
  
  /* Check capacity */
  if (partition->max_messages > 0 && partition->messages.size >= partition->max_messages) {
    return TURBO_ENOSPC;
  }
  if (partition->max_bytes > 0 &&
      partition->current_bytes + tstr_len(*payload) > partition->max_bytes) {
    return TURBO_ENOSPC;
  }
  
  /* Create message */
  flowmq_stream_message_t msg = {0};
  msg.offset = partition->high_watermark++;
  msg.timestamp_ns = timestamp_ns;
  msg.payload = *payload;  /* Move ownership */
  msg.topic = *topic_str;  /* Move ownership */
  *payload = NULL;
  *topic_str = NULL;
  
  /* Append to partition */
  if (deque_push_back(&partition->messages, &msg) != 0) {
    tstr_free(msg.payload);
    tstr_free(msg.topic);
    return TURBO_ENOMEM;
  }
  
  partition->current_bytes += tstr_len(msg.payload);
  *out_offset = msg.offset;
  
  return TURBO_OK;
}

int flowmq_stream_topic_fetch(flowmq_stream_topic_t *topic,
                              uint32_t partition_id,
                              uint64_t start_offset,
                              uint32_t max_messages,
                              const flowmq_stream_message_t **out_messages,
                              uint32_t *out_count) {
  if (!topic || !out_messages || !out_count) {
    return TURBO_EINVAL;
  }
  
  if (partition_id >= topic->partition_count) {
    return TURBO_ERANGE;
  }
  
  flowmq_stream_partition_t *partition = &topic->partitions[partition_id];
  
  /* Check offset range */
  if (start_offset < partition->low_watermark || start_offset >= partition->high_watermark) {
    *out_count = 0;
    return start_offset >= partition->high_watermark ? TURBO_OK : TURBO_ERANGE;
  }
  
  /* Find starting position */
  uint32_t found = 0;
  for (size_t i = 0; i < partition->messages.size && found < max_messages; i++) {
    const flowmq_stream_message_t *msg =
        (const flowmq_stream_message_t *)deque_at(&partition->messages, i);
    if (msg && msg->offset >= start_offset) {
      /* Return pointer to first message - caller must not modify */
      *out_messages = msg;
      
      /* Count consecutive messages up to max */
      for (size_t j = i; j < partition->messages.size && found < max_messages; j++) {
        const flowmq_stream_message_t *m =
            (const flowmq_stream_message_t *)deque_at(&partition->messages, j);
        if (m) {
          found++;
        }
      }
      break;
    }
  }
  
  *out_count = found;
  return TURBO_OK;
}

int flowmq_stream_topic_get_consumer_group(flowmq_stream_topic_t *topic,
                                           const char *group_id,
                                           flowmq_stream_consumer_group_t **out_group) {
  if (!topic || !group_id || !out_group) {
    return TURBO_EINVAL;
  }
  
  vstr group_key = vstr_from_cstr(group_id);
  
  /* Check if group exists */
  flowmq_stream_consumer_group_t **existing =
      (flowmq_stream_consumer_group_t **)hash_map_get(&topic->consumer_groups, &group_key);
  if (existing) {
    *out_group = *existing;
    return TURBO_OK;
  }
  
  /* Check capacity */
  if (topic->consumer_groups.size >= FLOWMQ_STREAM_MAX_CONSUMER_GROUPS) {
    return TURBO_ENOSPC;
  }
  
  /* Create new group */
  flowmq_stream_consumer_group_t *group =
      (flowmq_stream_consumer_group_t *)malloc(sizeof(flowmq_stream_consumer_group_t));
  if (!group) {
    return TURBO_ENOMEM;
  }
  
  int rc = flowmq_stream_consumer_group_init(group, group_id);
  if (rc != TURBO_OK) {
    free(group);
    return rc;
  }
  
  /* Add to topic */
  vstr inserted_key = vstr_from_cstr(group->group_id);
  if (hash_map_put(&topic->consumer_groups, &inserted_key, &group) != TURBO_OK) {
    flowmq_stream_consumer_group_destroy(group);
    free(group);
    return TURBO_ENOMEM;
  }
  
  *out_group = group;
  return TURBO_OK;
}
