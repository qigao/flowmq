/**
 * @file flowmq_priority_queue.c
 * Priority queue implementation using bucket-based approach.
 */

#include "flowmq_priority_queue.h"
#include "flowmq_stl_error_internal.h"
#include "tlog.h"

#include <string.h>

void flowmq_priority_message_cleanup(flowmq_priority_message_t *msg) {
  if (!msg) return;
  tstr_free(msg->payload);
  tstr_free(msg->topic);
}

int flowmq_priority_queue_init(flowmq_priority_queue_t *pq,
                               uint64_t max_capacity,
                               uint64_t max_bytes) {
  if (!pq) return TURBO_EINVAL;
  if (max_capacity > SIZE_MAX) return TURBO_ERANGE;
  
  pq->max_priority = 0;
  pq->total_messages = 0;
  pq->max_capacity = max_capacity;
  pq->max_bytes = max_bytes;
  pq->total_bytes = 0;
  
  /* Initialize all priority buckets */
  for (int i = 0; i < FLOWMQ_PRIORITY_LEVELS; i++) {
    const size_t bucket_limit = max_capacity == 0u ? SIZE_MAX : (size_t)max_capacity;
    int rc = deque_init_bytes(&pq->buckets[i], sizeof(flowmq_priority_message_t),
                                    _Alignof(flowmq_priority_message_t), bucket_limit);
    if (rc != STL_OK) {
      /* Cleanup already initialized buckets */
      for (int j = 0; j < i; j++) {
        deque_destroy(&pq->buckets[j]);
      }
      return flowmq_stl_error((stl_status)rc);
    }
  }
  
  return TURBO_OK;
}

void flowmq_priority_queue_destroy(flowmq_priority_queue_t *pq) {
  if (!pq) return;
  
  /* Cleanup all messages in all buckets */
  for (int i = 0; i < FLOWMQ_PRIORITY_LEVELS; i++) {
    deque_t *bucket = &pq->buckets[i];
    while (bucket->size > 0) {
      flowmq_priority_message_t *msg = (flowmq_priority_message_t *)deque_front(bucket);
      flowmq_priority_message_cleanup(msg);
      deque_pop_front(bucket, NULL);
    }
    deque_destroy(bucket);
  }
}

int flowmq_priority_queue_enqueue(flowmq_priority_queue_t *pq,
                                 uint8_t priority,
                                 tstr *payload,
                                 tstr *topic,
                                 uint64_t timestamp_ns) {
  if (!pq || !payload || !topic) return TURBO_EINVAL;
  
  /* Check capacity limits */
  if (pq->max_capacity > 0 && pq->total_messages >= pq->max_capacity) {
    return TURBO_ENOSPC;
  }
  
  size_t msg_bytes = tstr_len(*payload) + tstr_len(*topic);
  if (pq->max_bytes > 0 && pq->total_bytes + msg_bytes > pq->max_bytes) {
    return TURBO_ENOSPC;
  }
  
  /* Create message */
  flowmq_priority_message_t msg = {0};
  msg.priority = priority;
  msg.enqueued_ns = timestamp_ns;
  msg.payload = *payload;  /* Transfer ownership */
  msg.topic = *topic;      /* Transfer ownership */
  
  /* Enqueue to appropriate bucket */
  int rc = deque_push_back(&pq->buckets[priority], &msg);
  if (rc != STL_OK) {
    /* Restore ownership on failure */
    *payload = msg.payload;
    *topic = msg.topic;
    return flowmq_stl_error((stl_status)rc);
  }
  
  /* Update statistics */
  pq->total_messages++;
  pq->total_bytes += msg_bytes;
  
  /* Update max_priority */
  if (priority > pq->max_priority) {
    pq->max_priority = priority;
  }
  
  return TURBO_OK;
}

int flowmq_priority_queue_dequeue(flowmq_priority_queue_t *pq,
                                 flowmq_priority_message_t *out_message) {
  if (!pq || !out_message) return TURBO_EINVAL;
  
  if (pq->total_messages == 0) {
    return TURBO_ENOENT;
  }
  
  /* Find highest non-empty bucket */
  for (int pri = FLOWMQ_PRIORITY_MAX; pri >= FLOWMQ_PRIORITY_MIN; pri--) {
    deque_t *bucket = &pq->buckets[pri];
    if (bucket->size > 0) {
      /* Dequeue from this bucket */
      flowmq_priority_message_t *msg = (flowmq_priority_message_t *)deque_front(bucket);
      *out_message = *msg;  /* Transfer ownership */
      
      /* Update statistics */
      pq->total_messages--;
      pq->total_bytes -= (tstr_len(msg->payload) + tstr_len(msg->topic));
      
      deque_pop_front(bucket, NULL);
      
      /* Update max_priority if this was the last message at this priority */
      if (bucket->size == 0 && (uint8_t)pri == pq->max_priority) {
        /* Find new max_priority */
        pq->max_priority = 0;
        for (int p = pri - 1; p >= FLOWMQ_PRIORITY_MIN; p--) {
          if (pq->buckets[p].size > 0) {
            pq->max_priority = (uint8_t)p;
            break;
          }
        }
      }
      
      return TURBO_OK;
    }
  }
  
  /* Should not reach here if total_messages > 0 */
  return TURBO_ENOENT;
}

int flowmq_priority_queue_peek(flowmq_priority_queue_t *pq,
                              const flowmq_priority_message_t **out_message) {
  if (!pq || !out_message) return TURBO_EINVAL;
  
  if (pq->total_messages == 0) {
    return TURBO_ENOENT;
  }
  
  /* Find highest non-empty bucket */
  for (int pri = FLOWMQ_PRIORITY_MAX; pri >= FLOWMQ_PRIORITY_MIN; pri--) {
    deque_t *bucket = &pq->buckets[pri];
    if (bucket->size > 0) {
      *out_message = (const flowmq_priority_message_t *)deque_front(bucket);
      return TURBO_OK;
    }
  }
  
  return TURBO_ENOENT;
}

void flowmq_priority_queue_stats(flowmq_priority_queue_t *pq,
                                uint64_t *out_total_messages,
                                uint64_t *out_total_bytes,
                                uint8_t *out_max_priority) {
  if (!pq) return;
  if (out_total_messages) *out_total_messages = pq->total_messages;
  if (out_total_bytes) *out_total_bytes = pq->total_bytes;
  if (out_max_priority) *out_max_priority = pq->max_priority;
}

int flowmq_priority_queue_is_empty(flowmq_priority_queue_t *pq) {
  if (!pq) return 1;
  return pq->total_messages == 0;
}

uint64_t flowmq_priority_queue_count_at_priority(flowmq_priority_queue_t *pq,
                                                 uint8_t priority) {
  if (!pq) return 0;
  return (uint64_t)pq->buckets[priority].size;
}

void flowmq_priority_queue_clear(flowmq_priority_queue_t *pq) {
  if (!pq) return;
  
  /* Clear all buckets */
  for (int i = 0; i < FLOWMQ_PRIORITY_LEVELS; i++) {
    deque_t *bucket = &pq->buckets[i];
    while (bucket->size > 0) {
      flowmq_priority_message_t *msg = (flowmq_priority_message_t *)deque_front(bucket);
      flowmq_priority_message_cleanup(msg);
      deque_pop_front(bucket, NULL);
    }
  }
  
  pq->total_messages = 0;
  pq->total_bytes = 0;
  pq->max_priority = 0;
}
