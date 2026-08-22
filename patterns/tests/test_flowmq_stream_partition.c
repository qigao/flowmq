/**
 * @file test_flowmq_stream_partition.c
 * Unit tests for Stream Partition management.
 */

#include "flowmq_stream_partition.h"
#include "tinytest.h"

#include <string.h>

suite("flowmq_stream_partition") {
  it("initializes and destroys stream topic") {
    flowmq_stream_topic_t topic;
    check_equal(flowmq_stream_topic_init(&topic, "test-topic", 4, 1000, 1024 * 1024, 60000),
                 TURBO_OK);
    check_equal(topic.topic_name, "test-topic");
    check_equal(topic.partition_count, 4);
    
    flowmq_stream_topic_destroy(&topic);
  }
  
  it("publishes messages to partition") {
    flowmq_stream_topic_t topic;
    check_equal(flowmq_stream_topic_init(&topic, "events", 2, 100, 10000, 0), TURBO_OK);
    
    tstr payload = NULL;
    tstr topic_str = NULL;
    payload = tstr_dup("message data");
    topic_str = tstr_dup("events");
    
    uint64_t offset = 0;
    check_equal(flowmq_stream_topic_publish(&topic, 0, &payload, &topic_str, 1000000000ULL,
                                            &offset),
                 TURBO_OK);
    check_equal(offset, 0);  /* First offset */
    
    /* Publish second message */
    payload = tstr_dup("second message");
    topic_str = tstr_dup("events");
    check_equal(flowmq_stream_topic_publish(&topic, 0, &payload, &topic_str, 1000000001ULL,
                                            &offset),
                 TURBO_OK);
    check_equal(offset, 1);
    
    /* Check partition stats */
    uint32_t count = 0;
    uint64_t bytes = 0, low = 0, high = 0;
    flowmq_stream_partition_stats(&topic.partitions[0], &count, &bytes, &low, &high);
    check_equal(count, 2);
    check_equal(low, 0);
    check_equal(high, 2);
    
    flowmq_stream_topic_destroy(&topic);
  }
  
  it("fetches messages from partition") {
    flowmq_stream_topic_t topic;
    check_equal(flowmq_stream_topic_init(&topic, "logs", 1, 100, 10000, 0), TURBO_OK);
    
    /* Publish 5 messages */
    for (uint32_t i = 0; i < 5; i++) {
      tstr payload = NULL;
      tstr topic_str = NULL;
      char msg[32];
      snprintf(msg, sizeof(msg), "message %u", i);
      payload = tstr_dup(msg);
      topic_str = tstr_dup("logs");
      
      uint64_t offset = 0;
      check_equal(flowmq_stream_topic_publish(&topic, 0, &payload, &topic_str,
                                              1000000000ULL + i, &offset),
                   TURBO_OK);
    }
    
    /* Fetch from offset 2, max 3 messages */
    const flowmq_stream_message_t *messages = NULL;
    uint32_t count = 0;
    check_equal(flowmq_stream_topic_fetch(&topic, 0, 2, 3, &messages, &count), TURBO_OK);
    check_equal(count, 3);
    check(messages != NULL);
    
    /* Verify messages */
    check_equal(messages[0].offset, 2);
    check_equal(messages[0].payload, "message 2");
    check_equal(messages[1].offset, 3);
    check_equal(messages[2].offset, 4);
    
    flowmq_stream_topic_destroy(&topic);
  }
  
  it("enforces partition capacity limits") {
    flowmq_stream_topic_t topic;
    /* Max 3 messages per partition */
    check_equal(flowmq_stream_topic_init(&topic, "bounded", 1, 3, 10000, 0), TURBO_OK);
    
    /* Publish 3 messages - should succeed */
    for (uint32_t i = 0; i < 3; i++) {
      tstr payload = NULL;
      tstr topic_str = NULL;
      payload = tstr_dup("data");
      topic_str = tstr_dup("bounded");
      
      uint64_t offset = 0;
      check_equal(flowmq_stream_topic_publish(&topic, 0, &payload, &topic_str, 1000000000ULL,
                                              &offset),
                   TURBO_OK);
    }
    
    /* Fourth message should fail */
    tstr payload = NULL;
    tstr topic_str = NULL;
    payload = tstr_dup("overflow");
    topic_str = tstr_dup("bounded");
    uint64_t offset = 0;
    check_equal(flowmq_stream_topic_publish(&topic, 0, &payload, &topic_str, 1000000000ULL,
                                            &offset),
                 TURBO_ENOSPC);
    tstr_free(payload);
    tstr_free(topic_str);
    
    flowmq_stream_topic_destroy(&topic);
  }
  
  it("applies time-based retention") {
    flowmq_stream_topic_t topic;
    /* 1 second retention */
    check_equal(flowmq_stream_topic_init(&topic, "ephemeral", 1, 100, 10000, 1000), TURBO_OK);
    
    uint64_t base_time = 1000000000ULL;
    
    /* Publish 3 messages at different times */
    for (uint32_t i = 0; i < 3; i++) {
      tstr payload = NULL;
      tstr topic_str = NULL;
      payload = tstr_dup("data");
      topic_str = tstr_dup("ephemeral");
      
      uint64_t offset = 0;
      check_equal(flowmq_stream_topic_publish(&topic, 0, &payload, &topic_str,
                                              base_time + (i * 500000000ULL), &offset),
                   TURBO_OK);
    }
    
    /* Apply retention 2 seconds after base_time */
    uint64_t now_ns = base_time + 2000000000ULL;
    uint32_t removed = flowmq_stream_partition_apply_retention(&topic.partitions[0], now_ns);
    
    /* First message should be removed (older than 1 second) */
    check(removed > 0);
    
    uint32_t count = 0;
    uint64_t low = 0, high = 0;
    flowmq_stream_partition_stats(&topic.partitions[0], &count, NULL, &low, &high);
    check(count < 3);
    check(low > 0);  /* Low watermark advanced */
    
    flowmq_stream_topic_destroy(&topic);
  }
  
  it("creates and manages consumer group") {
    flowmq_stream_topic_t topic;
    check_equal(flowmq_stream_topic_init(&topic, "stream", 4, 100, 10000, 0), TURBO_OK);
    
    flowmq_stream_consumer_group_t *group = NULL;
    check_equal(flowmq_stream_topic_get_consumer_group(&topic, "group-a", &group), TURBO_OK);
    check(group != NULL);
    check_equal(group->group_id, "group-a");
    
    /* Get same group again - should return existing */
    flowmq_stream_consumer_group_t *group2 = NULL;
    check_equal(flowmq_stream_topic_get_consumer_group(&topic, "group-a", &group2), TURBO_OK);
    check(group == group2);
    
    flowmq_stream_topic_destroy(&topic);
  }
  
  it("joins consumers to group and triggers rebalance") {
    flowmq_stream_topic_t topic;
    check_equal(flowmq_stream_topic_init(&topic, "stream", 4, 100, 10000, 0), TURBO_OK);
    
    flowmq_stream_consumer_group_t *group = NULL;
    check_equal(flowmq_stream_topic_get_consumer_group(&topic, "cg1", &group), TURBO_OK);
    
    uint64_t now_ns = 1000000000ULL;
    
    /* Join first consumer */
    check_equal(flowmq_stream_consumer_group_join(group, "consumer-1", now_ns), TURBO_OK);
    check_equal(group->rebalancing, 1);
    check_equal(group->members.size, 1);
    
    /* Trigger rebalance */
    check_equal(flowmq_stream_consumer_group_rebalance(group, topic.partition_count), TURBO_OK);
    check_equal(group->rebalancing, 0);
    check_equal(group->generation, 1);
    
    /* Join second consumer */
    check_equal(flowmq_stream_consumer_group_join(group, "consumer-2", now_ns), TURBO_OK);
    check_equal(group->members.size, 2);
    
    /* Rebalance again */
    check_equal(flowmq_stream_consumer_group_rebalance(group, topic.partition_count), TURBO_OK);
    check_equal(group->generation, 2);
    
    flowmq_stream_topic_destroy(&topic);
  }
  
  it("commits and retrieves offsets") {
    flowmq_stream_topic_t topic;
    check_equal(flowmq_stream_topic_init(&topic, "stream", 2, 100, 10000, 0), TURBO_OK);
    
    flowmq_stream_consumer_group_t *group = NULL;
    check_equal(flowmq_stream_topic_get_consumer_group(&topic, "cg1", &group), TURBO_OK);
    
    /* Commit offset for partition 0 */
    check_equal(flowmq_stream_consumer_group_commit(group, 0, 42), TURBO_OK);
    
    /* Retrieve offset */
    uint64_t offset = 0;
    check_equal(flowmq_stream_consumer_group_get_offset(group, 0, &offset), TURBO_OK);
    check_equal(offset, 42);
    
    /* Get offset for uncommitted partition */
    check_equal(flowmq_stream_consumer_group_get_offset(group, 1, &offset), TURBO_OK);
    check_equal(offset, 0);  /* Default to 0 */
    
    /* Update offset */
    check_equal(flowmq_stream_consumer_group_commit(group, 0, 100), TURBO_OK);
    check_equal(flowmq_stream_consumer_group_get_offset(group, 0, &offset), TURBO_OK);
    check_equal(offset, 100);
    
    flowmq_stream_topic_destroy(&topic);
  }
  
  it("removes consumer from group") {
    flowmq_stream_topic_t topic;
    check_equal(flowmq_stream_topic_init(&topic, "stream", 4, 100, 10000, 0), TURBO_OK);
    
    flowmq_stream_consumer_group_t *group = NULL;
    check_equal(flowmq_stream_topic_get_consumer_group(&topic, "cg1", &group), TURBO_OK);
    
    uint64_t now_ns = 1000000000ULL;
    check_equal(flowmq_stream_consumer_group_join(group, "consumer-1", now_ns), TURBO_OK);
    check_equal(flowmq_stream_consumer_group_join(group, "consumer-2", now_ns), TURBO_OK);
    check_equal(group->members.size, 2);
    
    /* Remove first consumer */
    check_equal(flowmq_stream_consumer_group_leave(group, "consumer-1"), TURBO_OK);
    check_equal(group->members.size, 1);
    check_equal(group->rebalancing, 1);
    
    /* Try to remove non-existent consumer */
    check_equal(flowmq_stream_consumer_group_leave(group, "consumer-999"), TURBO_ENOENT);
    
    flowmq_stream_topic_destroy(&topic);
  }
  
  it("enforces partition ID validation") {
    flowmq_stream_topic_t topic;
    check_equal(flowmq_stream_topic_init(&topic, "stream", 2, 100, 10000, 0), TURBO_OK);
    
    tstr payload = NULL;
    tstr topic_str = NULL;
    payload = tstr_dup("data");
    topic_str = tstr_dup("stream");
    uint64_t offset = 0;
    
    /* Invalid partition ID */
    check_equal(flowmq_stream_topic_publish(&topic, 5, &payload, &topic_str, 1000000000ULL,
                                            &offset),
                 TURBO_ERANGE);
    
    tstr_free(payload);
    tstr_free(topic_str);
    flowmq_stream_topic_destroy(&topic);
  }
  
  it("handles at-least-once semantics with offset replay") {
    flowmq_stream_topic_t topic;
    check_equal(flowmq_stream_topic_init(&topic, "replay", 1, 100, 10000, 0), TURBO_OK);
    
    /* Publish 5 messages */
    for (uint32_t i = 0; i < 5; i++) {
      tstr payload = NULL;
      tstr topic_str = NULL;
      char msg[32];
      snprintf(msg, sizeof(msg), "msg%u", i);
      payload = tstr_dup(msg);
      topic_str = tstr_dup("replay");
      uint64_t offset = 0;
      check_equal(flowmq_stream_topic_publish(&topic, 0, &payload, &topic_str, 1000000000ULL,
                                              &offset),
                   TURBO_OK);
    }
    
    flowmq_stream_consumer_group_t *group = NULL;
    check_equal(flowmq_stream_topic_get_consumer_group(&topic, "cg1", &group), TURBO_OK);
    
    /* Simulate consumer crash after processing offset 2 but before committing */
    /* Consumer restarts and fetches from last committed (0) */
    const flowmq_stream_message_t *messages = NULL;
    uint32_t count = 0;
    uint64_t last_committed = 0;
    check_equal(flowmq_stream_consumer_group_get_offset(group, 0, &last_committed), TURBO_OK);
    
    check_equal(flowmq_stream_topic_fetch(&topic, 0, last_committed, 10, &messages, &count),
                 TURBO_OK);
    check_equal(count, 5);  /* Gets all messages (at-least-once) */
    
    /* After processing, commit offset 5 */
    check_equal(flowmq_stream_consumer_group_commit(group, 0, 5), TURBO_OK);
    
    flowmq_stream_topic_destroy(&topic);
  }
}

