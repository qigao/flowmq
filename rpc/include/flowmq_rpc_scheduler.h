#ifndef FLOWMQ_RPC_SCHEDULER_H
#define FLOWMQ_RPC_SCHEDULER_H
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum { FLOWMQ_RPC_KEY_MAX = 128, FLOWMQ_RPC_PRIORITY_MAX = 16, FLOWMQ_RPC_VNODES_MAX = 128 };

typedef enum flowmq_rpc_partitioning {
  FLOWMQ_RPC_HASH_MODULO,
  FLOWMQ_RPC_HASH_RING
} flowmq_rpc_partitioning;

typedef enum flowmq_rpc_queue_policy {
  FLOWMQ_RPC_QUEUE_ROUND_ROBIN,
  FLOWMQ_RPC_QUEUE_FIFO,
  FLOWMQ_RPC_QUEUE_PRIORITY
} flowmq_rpc_queue_policy;

typedef struct flowmq_rpc_scheduler_config {
  size_t capacity;        /* Caller slot range [0, capacity), maximum FLOWMQ_MDP_CAPACITY_MAX. */
  size_t per_key_limit;   /* Includes running and waiting; zero means capacity. */
  size_t partition_count; /* Zero means a single partition. */
  const uint32_t *partition_ids; /* Optional stable member IDs, default 0..count-1. */
  flowmq_rpc_partitioning partitioning;
  flowmq_rpc_queue_policy queue_policy;
  size_t priority_levels;           /* Priority policy only. Level zero is served first. */
  const unsigned *priority_weights; /* Positive per-round service quotas by level. */
  unsigned virtual_nodes;           /* Ring only, 1..FLOWMQ_RPC_VNODES_MAX per member. */
} flowmq_rpc_scheduler_config;

typedef struct flowmq_rpc_work_class {
  const void *key; /* Canonical service key bytes; copied on successful admission. */
  size_t key_size;
  unsigned priority;
  bool ordered; /* Hold the key across suspension and ready turns until finish. */
} flowmq_rpc_work_class;

typedef struct flowmq_rpc_scheduler flowmq_rpc_scheduler;

/* All calls are made by one scheduler owner. For a single partition with
 * round-robin key turns, use { .capacity = 8 }. Configuration is copied and all
 * queue storage reserved at open. EINVAL for unknown policy, invalid limits,
 * or duplicate member IDs; ENOMEM for allocation failure. */
int flowmq_rpc_scheduler_open(const flowmq_rpc_scheduler_config *config,
                              flowmq_rpc_scheduler **out);
void flowmq_rpc_scheduler_close(flowmq_rpc_scheduler *scheduler);

/* Return the stable logical partition ID for canonical key bytes. Empty keys
 * use the lowest member ID. EINVAL for invalid pointers, EMSGSIZE over the key limit. */
int flowmq_rpc_scheduler_partition(const flowmq_rpc_scheduler *scheduler, const void *key,
                                   size_t key_size, uint32_t *partition);

/* Admit one member into an unused caller slot. A null work descriptor is an
 * independent unkeyed request. Nonempty keys can also group independent work
 * for fair rotation. Ordered work conflicts only with other ordered work using
 * the same key. EINVAL for missing ordered keys, invalid slots or priorities,
 * EMSGSIZE for oversized keys, EBUSY for a live slot, ENOBUFS at the key limit.
 * On error no request is admitted. */
int flowmq_rpc_scheduler_admit(flowmq_rpc_scheduler *scheduler, size_t slot,
                               const flowmq_rpc_work_class *work);
/* Give one eligible step to the caller; EBUSY when none is ready. The step
 * stays in flight until ready() or finish(). The owner enforces deadlines and
 * checks its generation-stamped application handle before calling either. */
int flowmq_rpc_scheduler_next(flowmq_rpc_scheduler *scheduler, size_t *slot);
/* Return a previously selected step to the ready set at its tail, keeping its
 * key. EBUSY if already ready or blocked on an earlier writer; ENOENT if absent. */
int flowmq_rpc_scheduler_ready(flowmq_rpc_scheduler *scheduler, size_t slot);
/* End an admitted slot (including cancellation before dispatch). Wakes the
 * next ordered member for this key if applicable. ENOENT if absent. */
int flowmq_rpc_scheduler_finish(flowmq_rpc_scheduler *scheduler, size_t slot);

#ifdef __cplusplus
}
#endif
#endif
