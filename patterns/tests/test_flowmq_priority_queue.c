/**
 * @file test_flowmq_priority_queue.c
 * Unit tests for priority queue.
 */

#include "flowmq_priority_queue.h"
#include "tinytest.h"

#include <string.h>

TEST_SUITE(flowmq_priority_queue) {
  it("initializes and destroys priority queue") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 1000, 10000), TURBO_OK);
    
    uint64_t total_messages = 0, total_bytes = 0;
    uint8_t max_priority = 0;
    flowmq_priority_queue_stats(&pq, &total_messages, &total_bytes, &max_priority);
    check_ull_eq(total_messages, 0);
    check_ull_eq(total_bytes, 0);
    check_int_eq(flowmq_priority_queue_is_empty(&pq), 1);
    
    flowmq_priority_queue_destroy(&pq);
  }
  
  it("enqueues and dequeues single message") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 100, 10000), TURBO_OK);
    
    tstr_t payload = NULL;
    tstr_t topic = NULL;
    payload = tstr_dup("hello");
    topic = tstr_dup("test");
    
    check_int_eq(flowmq_priority_queue_enqueue(&pq, 128, &payload, &topic, 1000000000ULL),
                 TURBO_OK);
    
    check_int_eq(flowmq_priority_queue_is_empty(&pq), 0);
    
    flowmq_priority_message_t msg = {0};
    check_int_eq(flowmq_priority_queue_dequeue(&pq, &msg), TURBO_OK);
    check_int_eq(msg.priority, 128);
    check_str_eq(msg.payload, "hello");
    check_str_eq(msg.topic, "test");
    
    flowmq_priority_message_cleanup(&msg);
    check_int_eq(flowmq_priority_queue_is_empty(&pq), 1);
    
    flowmq_priority_queue_destroy(&pq);
  }
  
  it("dequeues messages in priority order") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 100, 10000), TURBO_OK);
    
    /* Enqueue with priorities: 100, 200, 50, 200, 150 */
    uint8_t priorities[] = {100, 200, 50, 200, 150};
    for (int i = 0; i < 5; i++) {
      tstr_t payload = NULL;
      tstr_t topic = NULL;
      char buf[16];
      snprintf(buf, sizeof(buf), "msg%d", i);
      payload = tstr_dup(buf);
      topic = tstr_dup("test");
      
      check_int_eq(flowmq_priority_queue_enqueue(&pq, priorities[i], &payload, &topic,
                                                 1000000000ULL),
                   TURBO_OK);
    }
    
    /* Dequeue should return: msg1 (200), msg3 (200), msg4 (150), msg0 (100), msg2 (50) */
    /* Within same priority, FIFO order: msg1 before msg3 */
    
    flowmq_priority_message_t msg = {0};
    
    /* First: priority 200, msg1 */
    check_int_eq(flowmq_priority_queue_dequeue(&pq, &msg), TURBO_OK);
    check_int_eq(msg.priority, 200);
    check_str_eq(msg.payload, "msg1");
    flowmq_priority_message_cleanup(&msg);
    
    /* Second: priority 200, msg3 */
    check_int_eq(flowmq_priority_queue_dequeue(&pq, &msg), TURBO_OK);
    check_int_eq(msg.priority, 200);
    check_str_eq(msg.payload, "msg3");
    flowmq_priority_message_cleanup(&msg);
    
    /* Third: priority 150, msg4 */
    check_int_eq(flowmq_priority_queue_dequeue(&pq, &msg), TURBO_OK);
    check_int_eq(msg.priority, 150);
    check_str_eq(msg.payload, "msg4");
    flowmq_priority_message_cleanup(&msg);
    
    /* Fourth: priority 100, msg0 */
    check_int_eq(flowmq_priority_queue_dequeue(&pq, &msg), TURBO_OK);
    check_int_eq(msg.priority, 100);
    check_str_eq(msg.payload, "msg0");
    flowmq_priority_message_cleanup(&msg);
    
    /* Fifth: priority 50, msg2 */
    check_int_eq(flowmq_priority_queue_dequeue(&pq, &msg), TURBO_OK);
    check_int_eq(msg.priority, 50);
    check_str_eq(msg.payload, "msg2");
    flowmq_priority_message_cleanup(&msg);
    
    check_int_eq(flowmq_priority_queue_is_empty(&pq), 1);
    
    flowmq_priority_queue_destroy(&pq);
  }
  
  it("maintains FIFO order within same priority") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 100, 10000), TURBO_OK);
    
    /* Enqueue 5 messages at priority 128 */
    for (int i = 0; i < 5; i++) {
      tstr_t payload = NULL;
      tstr_t topic = NULL;
      char buf[16];
      snprintf(buf, sizeof(buf), "msg%d", i);
      payload = tstr_dup(buf);
      topic = tstr_dup("test");
      
      check_int_eq(flowmq_priority_queue_enqueue(&pq, 128, &payload, &topic, 1000000000ULL),
                   TURBO_OK);
    }
    
    /* Dequeue should return in FIFO order */
    for (int i = 0; i < 5; i++) {
      flowmq_priority_message_t msg = {0};
      check_int_eq(flowmq_priority_queue_dequeue(&pq, &msg), TURBO_OK);
      
      char expected[16];
      snprintf(expected, sizeof(expected), "msg%d", i);
      check_str_eq(msg.payload, expected);
      flowmq_priority_message_cleanup(&msg);
    }
    
    flowmq_priority_queue_destroy(&pq);
  }
  
  it("enforces message count capacity limit") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 3, 100000), TURBO_OK);  /* Max 3 messages */
    
    /* Enqueue 3 messages */
    for (int i = 0; i < 3; i++) {
      tstr_t payload = NULL;
      tstr_t topic = NULL;
      payload = tstr_dup("data");
      topic = tstr_dup("test");
      check_int_eq(flowmq_priority_queue_enqueue(&pq, 128, &payload, &topic, 1000000000ULL),
                   TURBO_OK);
    }
    
    /* Fourth should fail */
    tstr_t payload = NULL;
    tstr_t topic = NULL;
    payload = tstr_dup("overflow");
    topic = tstr_dup("test");
    check_int_eq(flowmq_priority_queue_enqueue(&pq, 128, &payload, &topic, 1000000000ULL),
                 TURBO_ENOSPC);
    tstr_free(payload);
    tstr_free(topic);
    
    flowmq_priority_queue_destroy(&pq);
  }
  
  it("enforces byte capacity limit") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 1000, 20), TURBO_OK);  /* Max 20 bytes */
    
    /* Enqueue 2 messages (10 bytes each: 5 payload + 5 topic) */
    for (int i = 0; i < 2; i++) {
      tstr_t payload = NULL;
      tstr_t topic = NULL;
      payload = tstr_dup("12345");
      topic = tstr_dup("test1");
      check_int_eq(flowmq_priority_queue_enqueue(&pq, 128, &payload, &topic, 1000000000ULL),
                   TURBO_OK);
    }
    
    /* Third message (10 bytes) would exceed 20 byte limit */
    tstr_t payload = NULL;
    tstr_t topic = NULL;
    payload = tstr_dup("12345");
    topic = tstr_dup("test1");
    check_int_eq(flowmq_priority_queue_enqueue(&pq, 128, &payload, &topic, 1000000000ULL),
                 TURBO_ENOSPC);
    tstr_free(payload);
    tstr_free(topic);
    
    flowmq_priority_queue_destroy(&pq);
  }
  
  it("peeks without removing message") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 100, 10000), TURBO_OK);
    
    tstr_t payload = NULL;
    tstr_t topic = NULL;
    payload = tstr_dup("peek-test");
    topic = tstr_dup("test");
    check_int_eq(flowmq_priority_queue_enqueue(&pq, 200, &payload, &topic, 1000000000ULL),
                 TURBO_OK);
    
    /* Peek multiple times */
    const flowmq_priority_message_t *msg = NULL;
    check_int_eq(flowmq_priority_queue_peek(&pq, &msg), TURBO_OK);
    check_int_eq(msg->priority, 200);
    check_str_eq(msg->payload, "peek-test");
    
    check_int_eq(flowmq_priority_queue_peek(&pq, &msg), TURBO_OK);
    check_int_eq(msg->priority, 200);  /* Still there */
    
    /* Queue still not empty */
    check_int_eq(flowmq_priority_queue_is_empty(&pq), 0);
    
    flowmq_priority_queue_destroy(&pq);
  }
  
  it("returns error when dequeuing from empty queue") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 100, 10000), TURBO_OK);
    
    flowmq_priority_message_t msg = {0};
    check_int_eq(flowmq_priority_queue_dequeue(&pq, &msg), TURBO_ENOENT);
    
    const flowmq_priority_message_t *peek_msg = NULL;
    check_int_eq(flowmq_priority_queue_peek(&pq, &peek_msg), TURBO_ENOENT);
    
    flowmq_priority_queue_destroy(&pq);
  }
  
  it("updates max_priority correctly") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 100, 10000), TURBO_OK);
    
    uint8_t max_priority = 0;
    
    /* Enqueue at priority 100 */
    tstr_t p1 = NULL, t1 = NULL;
    p1 = tstr_dup("msg1");
    t1 = tstr_dup("test");
    check_int_eq(flowmq_priority_queue_enqueue(&pq, 100, &p1, &t1, 1000000000ULL), TURBO_OK);
    flowmq_priority_queue_stats(&pq, NULL, NULL, &max_priority);
    check_int_eq(max_priority, 100);
    
    /* Enqueue at priority 200 */
    tstr_t p2 = NULL, t2 = NULL;
    p2 = tstr_dup("msg2");
    t2 = tstr_dup("test");
    check_int_eq(flowmq_priority_queue_enqueue(&pq, 200, &p2, &t2, 1000000000ULL), TURBO_OK);
    flowmq_priority_queue_stats(&pq, NULL, NULL, &max_priority);
    check_int_eq(max_priority, 200);
    
    /* Dequeue priority 200 */
    flowmq_priority_message_t msg = {0};
    check_int_eq(flowmq_priority_queue_dequeue(&pq, &msg), TURBO_OK);
    flowmq_priority_message_cleanup(&msg);
    
    /* Max priority should drop to 100 */
    flowmq_priority_queue_stats(&pq, NULL, NULL, &max_priority);
    check_int_eq(max_priority, 100);
    
    flowmq_priority_queue_destroy(&pq);
  }
  
  it("clears all messages") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 100, 10000), TURBO_OK);
    
    /* Enqueue 5 messages */
    for (int i = 0; i < 5; i++) {
      tstr_t payload = NULL;
      tstr_t topic = NULL;
      payload = tstr_dup("data");
      topic = tstr_dup("test");
      check_int_eq(flowmq_priority_queue_enqueue(&pq, 128, &payload, &topic, 1000000000ULL),
                   TURBO_OK);
    }
    
    uint64_t total_messages = 0;
    flowmq_priority_queue_stats(&pq, &total_messages, NULL, NULL);
    check_ull_eq(total_messages, 5);
    
    /* Clear */
    flowmq_priority_queue_clear(&pq);
    
    flowmq_priority_queue_stats(&pq, &total_messages, NULL, NULL);
    check_ull_eq(total_messages, 0);
    check_int_eq(flowmq_priority_queue_is_empty(&pq), 1);
    
    flowmq_priority_queue_destroy(&pq);
  }
  
  it("counts messages at specific priority") {
    flowmq_priority_queue_t pq;
    check_int_eq(flowmq_priority_queue_init(&pq, 100, 10000), TURBO_OK);
    
    /* Enqueue 3 at priority 100, 2 at priority 200 */
    for (int i = 0; i < 3; i++) {
      tstr_t payload = NULL;
      tstr_t topic = NULL;
      payload = tstr_dup("data");
      topic = tstr_dup("test");
      check_int_eq(flowmq_priority_queue_enqueue(&pq, 100, &payload, &topic, 1000000000ULL),
                   TURBO_OK);
    }
    
    for (int i = 0; i < 2; i++) {
      tstr_t payload = NULL;
      tstr_t topic = NULL;
      payload = tstr_dup("data");
      topic = tstr_dup("test");
      check_int_eq(flowmq_priority_queue_enqueue(&pq, 200, &payload, &topic, 1000000000ULL),
                   TURBO_OK);
    }
    
    check_ull_eq(flowmq_priority_queue_count_at_priority(&pq, 100), 3);
    check_ull_eq(flowmq_priority_queue_count_at_priority(&pq, 200), 2);
    check_ull_eq(flowmq_priority_queue_count_at_priority(&pq, 50), 0);
    
    flowmq_priority_queue_destroy(&pq);
  }
}

