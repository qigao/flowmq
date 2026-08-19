#ifndef FLOWMQ_PRIORITY_QUEUE_H
#define FLOWMQ_PRIORITY_QUEUE_H

/**
 * @file flowmq_priority_queue.h
 * Priority queue for message brokering.
 * 
 * Messages are dequeued in priority order (higher priority first).
 * For equal priority, FIFO order is maintained.
 * 
 * Priority levels: 0-255 (255 = highest, 0 = lowest)
 * 
 * Implementation uses bucket-based approach:
 * - 256 buckets (one per priority level)
 * - Each bucket is a FIFO queue
 * - Maintains max_priority tracker for O(1) peek
 */

#include "turbo_deque.h"
#include "turbo_error.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Priority range: 0 (lowest) to 255 (highest) */
#define FLOWMQ_PRIORITY_MIN 0
#define FLOWMQ_PRIORITY_MAX 255
#define FLOWMQ_PRIORITY_LEVELS 256

/* Default priority if not specified */
#define FLOWMQ_PRIORITY_DEFAULT 128

/* Priority queue message */
typedef struct flowmq_priority_message_s {
  uint8_t priority;           /* Message priority (0-255) */
  uint64_t enqueued_ns;       /* Enqueue timestamp */
  tstr_t payload;             /* Owned payload */
  tstr_t topic;               /* Owned topic */
} flowmq_priority_message_t;

/**
 * Priority queue.
 * Multi-level FIFO queues indexed by priority.
 */
typedef struct flowmq_priority_queue_s {
  turbo_deque_t buckets[FLOWMQ_PRIORITY_LEVELS];  /* Per-priority queues */
  uint8_t max_priority;                           /* Highest non-empty priority */
  uint64_t total_messages;                        /* Total message count */
  uint64_t max_capacity;                          /* Capacity limit (0=unlimited) */
  uint64_t max_bytes;                             /* Byte limit (0=unlimited) */
  uint64_t total_bytes;                           /* Current byte usage */
} flowmq_priority_queue_t;

/* Initialize priority queue */
int flowmq_priority_queue_init(flowmq_priority_queue_t *pq,
                               uint64_t max_capacity,
                               uint64_t max_bytes);

/* Destroy priority queue */
void flowmq_priority_queue_destroy(flowmq_priority_queue_t *pq);

/* Enqueue message with priority (takes ownership of payload/topic) */
int flowmq_priority_queue_enqueue(flowmq_priority_queue_t *pq,
                                 uint8_t priority,
                                 tstr_t *payload,
                                 tstr_t *topic,
                                 uint64_t timestamp_ns);

/* Dequeue highest priority message (caller must free message) */
int flowmq_priority_queue_dequeue(flowmq_priority_queue_t *pq,
                                 flowmq_priority_message_t *out_message);

/* Peek highest priority message (does not remove) */
int flowmq_priority_queue_peek(flowmq_priority_queue_t *pq,
                              const flowmq_priority_message_t **out_message);

/* Get queue statistics */
void flowmq_priority_queue_stats(flowmq_priority_queue_t *pq,
                                uint64_t *out_total_messages,
                                uint64_t *out_total_bytes,
                                uint8_t *out_max_priority);

/* Check if queue is empty */
int flowmq_priority_queue_is_empty(flowmq_priority_queue_t *pq);

/* Get message count for specific priority level */
uint64_t flowmq_priority_queue_count_at_priority(flowmq_priority_queue_t *pq,
                                                 uint8_t priority);

/* Clear all messages */
void flowmq_priority_queue_clear(flowmq_priority_queue_t *pq);

/* Cleanup single message (call after dequeue) */
void flowmq_priority_message_cleanup(flowmq_priority_message_t *msg);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_PRIORITY_QUEUE_H */
