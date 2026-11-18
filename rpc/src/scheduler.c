#include "flowmq_mdp.h"
#include "flowmq_rpc_scheduler.h"
#include <core/error.h>
#include <stdlib.h>
#include <string.h>

/* Intrusive bounded FIFOs. A node's index names its slot (or key group);
 * the list head is a sentinel and never selected. */
typedef struct queue_link {
  struct queue_link *prev, *next;
  size_t index;
} queue_link;

static void queue_init(queue_link *head) {
  head->next = head;
  head->prev = head;
}

static void queue_append(queue_link *head, queue_link *node) {
  node->prev = head->prev;
  node->next = head;
  head->prev->next = node;
  head->prev = node;
}

static void queue_remove(queue_link *node) {
  node->prev->next = node->next;
  node->next->prev = node->prev;
  node->next = node->prev = NULL;
}

typedef enum slot_state { FREE, WAITING_FOR_KEY, READY, SELECTED } slot_state;
typedef struct scheduler_slot {
  slot_state state;
  size_t group;
  unsigned priority;
  queue_link order_node, ready_node;
} scheduler_slot;

typedef struct key_group {
  size_t members, partition, key_size;
  unsigned char key[FLOWMQ_RPC_KEY_MAX];
  queue_link ordered_members, ready_members, turn_node;
} key_group;

typedef struct partition_state {
  uint32_t id;
  size_t ready, level;
  unsigned spent;
  queue_link levels[FLOWMQ_RPC_PRIORITY_MAX], groups;
} partition_state;

typedef struct ring_point {
  uint64_t position;
  size_t partition;
} ring_point;

struct flowmq_rpc_scheduler {
  size_t capacity, per_key_limit, partitions, cursor, levels, points;
  flowmq_rpc_queue_policy policy;
  unsigned weights[FLOWMQ_RPC_PRIORITY_MAX];
  scheduler_slot *slots;
  key_group *groups;
  partition_state *members;
  ring_point *ring;
};

/* Stable FNV-1a with a 64-bit avalanche. The ring hashes two big-endian
 * uint32 values (logical partition ID, virtual node) regardless of host ABI. */
static uint64_t key_hash(const void *bytes, size_t size) {
  uint64_t h = UINT64_C(14695981039346656037);
  const unsigned char *p = bytes;
  for (size_t i = 0; i < size; ++i) {
    h ^= p[i];
    h *= UINT64_C(1099511628211);
  }
  h = (h ^ (h >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
  h = (h ^ (h >> 27)) * UINT64_C(0x94d049bb133111eb);
  return h ^ (h >> 31);
}

static int compare_members(const void *left, const void *right) {
  const partition_state *a = left, *b = right;
  return (a->id > b->id) - (a->id < b->id);
}

static int compare_points(const void *left, const void *right) {
  const ring_point *a = left, *b = right;
  if (a->position != b->position) return a->position < b->position ? -1 : 1;
  return (a->partition > b->partition) - (a->partition < b->partition);
}

static int setup_members(flowmq_rpc_scheduler *s, const flowmq_rpc_scheduler_config *cfg) {
  for (size_t i = 0; i < s->partitions; ++i)
    s->members[i].id = cfg->partition_ids ? cfg->partition_ids[i] : (uint32_t)i;
  qsort(s->members, s->partitions, sizeof(*s->members), compare_members);
  for (size_t i = 0; i < s->partitions; ++i) {
    partition_state *m = &s->members[i];
    if (i && m->id == s->members[i - 1].id) return SALTS_EINVAL;
    queue_init(&m->groups);
    for (size_t level = 0; level < s->levels; ++level)
      queue_init(&m->levels[level]);
  }
  if (cfg->partitioning != FLOWMQ_RPC_HASH_RING) return SALTS_OK;
  /* Configured counts are bounded to 1024 * 128. */
  s->points = s->partitions * cfg->virtual_nodes;
  s->ring = calloc(s->points, sizeof(*s->ring));
  if (!s->ring) return SALTS_ENOMEM;
  for (size_t i = 0; i < s->partitions; ++i) {
    uint32_t id = s->members[i].id;
    for (uint32_t v = 0; v < cfg->virtual_nodes; ++v) {
      unsigned char bytes[8];
      for (size_t j = 0; j < 4; ++j) {
        unsigned shift = 24u - (unsigned)j * 8u;
        bytes[j] = (unsigned char)(id >> shift);
        bytes[4 + j] = (unsigned char)(v >> shift);
      }
      s->ring[i * cfg->virtual_nodes + v] = (ring_point){key_hash(bytes, sizeof(bytes)), i};
    }
  }
  qsort(s->ring, s->points, sizeof(*s->ring), compare_points);
  return SALTS_OK;
}

static bool valid_config(const flowmq_rpc_scheduler_config *cfg) {
  if (!cfg || !cfg->capacity || cfg->capacity > FLOWMQ_MDP_CAPACITY_MAX ||
      cfg->per_key_limit > cfg->capacity || cfg->partition_count > FLOWMQ_MDP_CAPACITY_MAX ||
      (cfg->partition_ids && !cfg->partition_count))
    return false;
  if (cfg->partitioning == FLOWMQ_RPC_HASH_RING) {
    if (!cfg->virtual_nodes || cfg->virtual_nodes > FLOWMQ_RPC_VNODES_MAX) return false;
  } else if (cfg->partitioning != FLOWMQ_RPC_HASH_MODULO || cfg->virtual_nodes) return false;
  if (cfg->queue_policy == FLOWMQ_RPC_QUEUE_PRIORITY) {
    if (!cfg->priority_levels || cfg->priority_levels > FLOWMQ_RPC_PRIORITY_MAX ||
        !cfg->priority_weights)
      return false;
    for (size_t i = 0; i < cfg->priority_levels; ++i)
      if (!cfg->priority_weights[i]) return false;
    return true;
  }
  return (cfg->queue_policy == FLOWMQ_RPC_QUEUE_FIFO ||
          cfg->queue_policy == FLOWMQ_RPC_QUEUE_ROUND_ROBIN) &&
         !cfg->priority_levels && !cfg->priority_weights;
}

int flowmq_rpc_scheduler_open(const flowmq_rpc_scheduler_config *cfg, flowmq_rpc_scheduler **out) {
  if (!out || !valid_config(cfg)) return SALTS_EINVAL;
  flowmq_rpc_scheduler *s = calloc(1, sizeof(*s));
  if (!s) return SALTS_ENOMEM;
  s->capacity = cfg->capacity;
  s->per_key_limit = cfg->per_key_limit ? cfg->per_key_limit : cfg->capacity;
  s->partitions = cfg->partition_count ? cfg->partition_count : 1;
  s->policy = cfg->queue_policy;
  s->levels = cfg->priority_levels ? cfg->priority_levels : 1;
  if (cfg->priority_weights)
    memcpy(s->weights, cfg->priority_weights, s->levels * sizeof(*s->weights));
  s->slots = calloc(s->capacity, sizeof(*s->slots));
  s->groups = calloc(s->capacity, sizeof(*s->groups));
  s->members = calloc(s->partitions, sizeof(*s->members));
  int rc = SALTS_ENOMEM;
  if (s->slots && s->groups && s->members) rc = setup_members(s, cfg);
  if (rc != SALTS_OK) {
    flowmq_rpc_scheduler_close(s);
    return rc;
  }
  *out = s;
  return SALTS_OK;
}

void flowmq_rpc_scheduler_close(flowmq_rpc_scheduler *s) {
  if (s) {
    free(s->ring);
    free(s->members);
    free(s->groups);
    free(s->slots);
    free(s);
  }
}

static size_t partition_index(const flowmq_rpc_scheduler *s, const void *key, size_t size) {
  if (!size || s->partitions == 1) return 0;
  uint64_t h = key_hash(key, size);
  if (!s->ring) return (size_t)(h % s->partitions);
  size_t lo = 0, hi = s->points;
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (s->ring[mid].position < h) lo = mid + 1;
    else hi = mid;
  }
  return s->ring[lo == s->points ? 0 : lo].partition;
}

int flowmq_rpc_scheduler_partition(const flowmq_rpc_scheduler *s, const void *key, size_t size,
                                   uint32_t *partition) {
  if (!s || !partition || (!key && size)) return SALTS_EINVAL;
  if (size > FLOWMQ_RPC_KEY_MAX) return SALTS_EMSGSIZE;
  *partition = s->members[partition_index(s, key, size)].id;
  return SALTS_OK;
}

static size_t find_group(flowmq_rpc_scheduler *s, const flowmq_rpc_work_class *work) {
  size_t unused = s->capacity;
  for (size_t i = 0; i < s->capacity; ++i) {
    key_group *g = &s->groups[i];
    if (!g->members) {
      if (unused == s->capacity) unused = i;
    } else if (g->key_size == work->key_size &&
               (!g->key_size || !memcmp(g->key, work->key, g->key_size)))
      return i;
  }
  /* Each live group owns at least one slot; admission has an unused slot. */
  return unused;
}

static void enqueue(flowmq_rpc_scheduler *s, scheduler_slot *slot) {
  key_group *g = &s->groups[slot->group];
  partition_state *p = &s->members[g->partition];
  if (s->policy == FLOWMQ_RPC_QUEUE_ROUND_ROBIN) {
    if (g->ready_members.next == &g->ready_members) queue_append(&p->groups, &g->turn_node);
    queue_append(&g->ready_members, &slot->ready_node);
  } else queue_append(&p->levels[slot->priority], &slot->ready_node);
  ++p->ready;
  slot->state = READY;
}

static void dequeue(flowmq_rpc_scheduler *s, scheduler_slot *slot, bool selected) {
  key_group *g = &s->groups[slot->group];
  partition_state *p = &s->members[g->partition];
  queue_remove(&slot->ready_node);
  --p->ready;
  if (s->policy == FLOWMQ_RPC_QUEUE_ROUND_ROBIN &&
      (selected || g->ready_members.next == &g->ready_members)) {
    queue_remove(&g->turn_node);
    if (g->ready_members.next != &g->ready_members) queue_append(&p->groups, &g->turn_node);
  }
}

int flowmq_rpc_scheduler_admit(flowmq_rpc_scheduler *s, size_t index,
                               const flowmq_rpc_work_class *work) {
  const flowmq_rpc_work_class unkeyed = {0};
  if (!work) work = &unkeyed;
  if (!s || index >= s->capacity || (work->ordered && !work->key_size) ||
      (work->key_size && !work->key) || work->priority >= s->levels)
    return SALTS_EINVAL;
  if (work->key_size > FLOWMQ_RPC_KEY_MAX) return SALTS_EMSGSIZE;
  scheduler_slot *slot = &s->slots[index];
  if (slot->state != FREE) return SALTS_EBUSY;
  size_t group = find_group(s, work);
  key_group *g = &s->groups[group];
  if (work->key_size && g->members >= s->per_key_limit) return SALTS_ENOBUFS;
  if (!g->members) {
    g->key_size = work->key_size;
    if (work->key_size) memcpy(g->key, work->key, work->key_size);
    g->partition = partition_index(s, work->key, work->key_size);
    g->turn_node.index = group;
    queue_init(&g->ordered_members);
    queue_init(&g->ready_members);
  }
  ++g->members;
  slot->group = group;
  slot->priority = work->priority;
  slot->order_node.index = slot->ready_node.index = index;
  if (work->ordered) {
    queue_append(&g->ordered_members, &slot->order_node);
    if (g->ordered_members.next != &slot->order_node) {
      slot->state = WAITING_FOR_KEY;
      return SALTS_OK;
    }
  }
  enqueue(s, slot);
  return SALTS_OK;
}

static size_t take_from_partition(flowmq_rpc_scheduler *s, partition_state *p) {
  if (s->policy == FLOWMQ_RPC_QUEUE_ROUND_ROBIN)
    return s->groups[p->groups.next->index].ready_members.next->index;
  if (s->policy == FLOWMQ_RPC_QUEUE_FIFO) return p->levels[0].next->index;
  /* p->ready > 0. A quota is charged only for work actually selected. */
  while (p->levels[p->level].next == &p->levels[p->level]) {
    p->level = (p->level + 1) % s->levels;
    p->spent = 0;
  }
  size_t index = p->levels[p->level].next->index;
  if (++p->spent == s->weights[p->level]) {
    p->level = (p->level + 1) % s->levels;
    p->spent = 0;
  }
  return index;
}

int flowmq_rpc_scheduler_next(flowmq_rpc_scheduler *s, size_t *out) {
  if (!s || !out) return SALTS_EINVAL;
  for (size_t i = 0; i < s->partitions; ++i) {
    size_t part = (s->cursor + i) % s->partitions;
    if (!s->members[part].ready) continue;
    size_t index = take_from_partition(s, &s->members[part]);
    dequeue(s, &s->slots[index], true);
    s->slots[index].state = SELECTED;
    s->cursor = (part + 1) % s->partitions;
    *out = index;
    return SALTS_OK;
  }
  return SALTS_EBUSY;
}

int flowmq_rpc_scheduler_ready(flowmq_rpc_scheduler *s, size_t index) {
  if (!s || index >= s->capacity) return SALTS_EINVAL;
  scheduler_slot *slot = &s->slots[index];
  if (slot->state == FREE) return SALTS_ENOENT;
  if (slot->state != SELECTED) return SALTS_EBUSY;
  enqueue(s, slot);
  return SALTS_OK;
}

int flowmq_rpc_scheduler_finish(flowmq_rpc_scheduler *s, size_t index) {
  if (!s || index >= s->capacity) return SALTS_EINVAL;
  scheduler_slot *slot = &s->slots[index];
  if (slot->state == FREE) return SALTS_ENOENT;
  key_group *g = &s->groups[slot->group];
  if (slot->state == READY) dequeue(s, slot, false);
  if (slot->order_node.next) {
    bool held = g->ordered_members.next == &slot->order_node;
    queue_remove(&slot->order_node);
    if (held && g->ordered_members.next != &g->ordered_members)
      enqueue(s, &s->slots[g->ordered_members.next->index]);
  }
  --g->members;
  slot->state = FREE;
  return SALTS_OK;
}
