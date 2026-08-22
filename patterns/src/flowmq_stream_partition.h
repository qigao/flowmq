#ifndef FLOWMQ_STREAM_PARTITION_H
#define FLOWMQ_STREAM_PARTITION_H

/**
 * @file flowmq_stream_partition.h
 * Stream partition and consumer group management.
 * 
 * Implements Kafka-style partitioned message streams with:
 * - Per-partition ordered message log
 * - Consumer group with offset tracking
 * - At-least-once delivery semantics
 * - Automatic rebalancing on member changes
 */

#include "flowmq_protocol_esb.h"
#include <turbostl/deque.h>
#include "turbo_error.h"
#include <turbostl/hash_map.h>
#include <turbostl/hash_set.h>

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Configuration limits */
#define FLOWMQ_STREAM_MAX_PARTITIONS FLOWMQ_PROTOCOL_ESB_MAX_STREAM_PARTITIONS
#define FLOWMQ_STREAM_MAX_CONSUMER_GROUPS FLOWMQ_PROTOCOL_ESB_MAX_STREAM_CONSUMER_GROUPS
#define FLOWMQ_STREAM_MAX_GROUP_MEMBERS FLOWMQ_PROTOCOL_ESB_MAX_STREAM_GROUP_MEMBERS
#define FLOWMQ_STREAM_MAX_GROUP_MEMBERS FLOWMQ_PROTOCOL_ESB_MAX_STREAM_GROUP_MEMBERS
#define FLOWMQ_STREAM_DEFAULT_RETENTION_MS (7 * 24 * 60 * 60 * 1000ULL)  /* 7 days */

/* Stream runtime limits are protocol-driven; avoid drift by deriving runtime constants
   directly from protocol constants. */
typedef char flowmq_stream_partition_runtime_limits_check[
    (FLOWMQ_STREAM_MAX_PARTITIONS == FLOWMQ_PROTOCOL_ESB_MAX_STREAM_PARTITIONS &&
     FLOWMQ_STREAM_MAX_CONSUMER_GROUPS == FLOWMQ_PROTOCOL_ESB_MAX_STREAM_CONSUMER_GROUPS &&
     FLOWMQ_STREAM_MAX_GROUP_MEMBERS == FLOWMQ_PROTOCOL_ESB_MAX_STREAM_GROUP_MEMBERS)
        ? 1
        : -1];

/* Single message in partition log */
typedef struct flowmq_stream_message_s {
  uint64_t offset;              /* Monotonic offset in partition */
  uint64_t timestamp_ns;        /* Publish timestamp */
  tstr payload;               /* Owned payload */
  tstr topic;                 /* Owned topic */
} flowmq_stream_message_t;

/**
 * Stream partition.
 * Maintains ordered log of messages with high-water mark.
 */
typedef struct flowmq_stream_partition_s {
  uint32_t partition_id;        /* Partition identifier (0-based) */
  uint64_t high_watermark;      /* Next offset to assign */
  uint64_t low_watermark;       /* Oldest retained offset */
  
  turbo_deque_t messages;       /* Deque<flowmq_stream_message_t> */
  
  /* Capacity limits */
  uint32_t max_messages;        /* Message count limit */
  uint64_t max_bytes;           /* Total payload size limit */
  uint64_t current_bytes;       /* Current payload size */
  
  /* Retention */
  uint64_t retention_ms;        /* Time-based retention */
  uint64_t last_cleanup_ns;     /* Last retention cleanup */
} flowmq_stream_partition_t;

/**
 * Consumer group member.
 */
typedef struct flowmq_stream_consumer_s {
  tstr member_id;             /* Unique member identifier (owned) */
  uint64_t joined_ns;           /* Join timestamp */
  uint64_t last_heartbeat_ns;   /* Last heartbeat */
  
  /* Assigned partitions */
  turbo_hash_set_t assigned_partitions;  /* HashSet<uint32_t> */
} flowmq_stream_consumer_t;

/**
 * Consumer group.
 * Manages group membership and partition assignment.
 */
typedef struct flowmq_stream_consumer_group_s {
  tstr group_id;              /* Group identifier (owned) */
  uint64_t generation;          /* Rebalance generation */
  
  turbo_hash_map_t members;     /* Map<member_id, consumer*> */
  turbo_hash_map_t offsets;     /* Map<partition_id, committed_offset> */
  
  /* Rebalance state */
  int rebalancing;              /* Rebalance in progress */
  uint64_t rebalance_deadline_ns;
} flowmq_stream_consumer_group_t;

/**
 * Stream topic manager.
 * Manages multiple partitions and consumer groups for one topic.
 */
typedef struct flowmq_stream_topic_s {
  tstr topic_name;            /* Topic name (owned) */
  
  uint32_t partition_count;     /* Number of partitions */
  flowmq_stream_partition_t *partitions;  /* Array of partitions */
  
  turbo_hash_map_t consumer_groups;  /* Map<group_id, group*> */
  
  /* Default configuration */
  uint32_t default_max_messages;
  uint64_t default_max_bytes;
  uint64_t default_retention_ms;
} flowmq_stream_topic_t;

/**
 * Initialize stream topic.
 * 
 * @param topic Topic instance to initialize.
 * @param topic_name Topic name (copied).
 * @param partition_count Number of partitions (1-256).
 * @param max_messages_per_partition Maximum messages per partition.
 * @param max_bytes_per_partition Maximum bytes per partition.
 * @param retention_ms Retention time in milliseconds (0 = infinite).
 * @return TURBO_OK on success, error code otherwise.
 */
int flowmq_stream_topic_init(flowmq_stream_topic_t *topic,
                             const char *topic_name,
                             uint32_t partition_count,
                             uint32_t max_messages_per_partition,
                             uint64_t max_bytes_per_partition,
                             uint64_t retention_ms);

/**
 * Destroy stream topic.
 * Releases all messages, consumer groups, and resources.
 * 
 * @param topic Topic instance.
 */
void flowmq_stream_topic_destroy(flowmq_stream_topic_t *topic);

/**
 * Publish message to partition.
 * 
 * @param topic Topic instance.
 * @param partition_id Target partition (must be < partition_count).
 * @param payload Message payload (moved, topic takes ownership).
 * @param topic_str Topic string (moved, topic takes ownership).
 * @param timestamp_ns Publish timestamp.
 * @param out_offset Output: assigned offset.
 * @return TURBO_OK on success, TURBO_ENOSPC if capacity reached, error otherwise.
 */
int flowmq_stream_topic_publish(flowmq_stream_topic_t *topic,
                                uint32_t partition_id,
                                tstr *payload,
                                tstr *topic_str,
                                uint64_t timestamp_ns,
                                uint64_t *out_offset);

/**
 * Fetch messages from partition starting at offset.
 * 
 * @param topic Topic instance.
 * @param partition_id Source partition.
 * @param start_offset Starting offset (inclusive).
 * @param max_messages Maximum messages to fetch.
 * @param out_messages Output: borrowed message array (valid until next operation).
 * @param out_count Output: number of messages fetched.
 * @return TURBO_OK on success, TURBO_ERANGE if offset out of range, error otherwise.
 */
int flowmq_stream_topic_fetch(flowmq_stream_topic_t *topic,
                              uint32_t partition_id,
                              uint64_t start_offset,
                              uint32_t max_messages,
                              const flowmq_stream_message_t **out_messages,
                              uint32_t *out_count);

/**
 * Create or get consumer group.
 * 
 * @param topic Topic instance.
 * @param group_id Group identifier.
 * @param out_group Output: group pointer (borrowed).
 * @return TURBO_OK on success, TURBO_ENOSPC if too many groups, error otherwise.
 */
int flowmq_stream_topic_get_consumer_group(flowmq_stream_topic_t *topic,
                                           const char *group_id,
                                           flowmq_stream_consumer_group_t **out_group);

/**
 * Join consumer to group.
 * Triggers rebalance if group membership changes.
 * 
 * @param group Consumer group.
 * @param member_id Member identifier (copied).
 * @param now_ns Current timestamp.
 * @return TURBO_OK on success, TURBO_ENOSPC if too many members, error otherwise.
 */
int flowmq_stream_consumer_group_join(flowmq_stream_consumer_group_t *group,
                                      const char *member_id,
                                      uint64_t now_ns);

/**
 * Leave consumer from group.
 * Triggers rebalance.
 * 
 * @param group Consumer group.
 * @param member_id Member identifier.
 * @return TURBO_OK on success, TURBO_ENOENT if member not found.
 */
int flowmq_stream_consumer_group_leave(flowmq_stream_consumer_group_t *group,
                                       const char *member_id);

/**
 * Commit offset for partition in consumer group.
 * 
 * @param group Consumer group.
 * @param partition_id Partition identifier.
 * @param offset Committed offset.
 * @return TURBO_OK on success.
 */
int flowmq_stream_consumer_group_commit(flowmq_stream_consumer_group_t *group,
                                        uint32_t partition_id,
                                        uint64_t offset);

/**
 * Get committed offset for partition.
 * 
 * @param group Consumer group.
 * @param partition_id Partition identifier.
 * @param out_offset Output: committed offset (0 if none).
 * @return TURBO_OK on success.
 */
int flowmq_stream_consumer_group_get_offset(const flowmq_stream_consumer_group_t *group,
                                            uint32_t partition_id,
                                            uint64_t *out_offset);

/**
 * Trigger rebalance for consumer group.
 * Redistributes partitions among active members using round-robin.
 * 
 * @param group Consumer group.
 * @param partition_count Total number of partitions in topic.
 * @return TURBO_OK on success.
 */
int flowmq_stream_consumer_group_rebalance(flowmq_stream_consumer_group_t *group,
                                           uint32_t partition_count);

/**
 * Process retention for partition.
 * Removes messages older than retention_ms or exceeding capacity.
 * 
 * @param partition Partition instance.
 * @param now_ns Current timestamp.
 * @return Number of messages removed.
 */
uint32_t flowmq_stream_partition_apply_retention(flowmq_stream_partition_t *partition,
                                                 uint64_t now_ns);

/**
 * Get partition statistics.
 * 
 * @param partition Partition instance.
 * @param message_count Output: current message count.
 * @param bytes Output: current payload bytes.
 * @param low_watermark Output: oldest retained offset.
 * @param high_watermark Output: next offset to assign.
 */
void flowmq_stream_partition_stats(const flowmq_stream_partition_t *partition,
                                   uint32_t *message_count,
                                   uint64_t *bytes,
                                   uint64_t *low_watermark,
                                   uint64_t *high_watermark);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_STREAM_PARTITION_H */
