#include "flowmq_rpc_scheduler.h"
#include <core/error.h>
#include <string.h>
#include <tinytest.h>

static flowmq_rpc_scheduler *scheduler;

static flowmq_rpc_work_class classify(const char *key, unsigned priority, bool ordered) {
  return (flowmq_rpc_work_class){key, strlen(key), priority, ordered};
}

static void take(size_t expected) {
  size_t actual = SIZE_MAX;
  check_equal(flowmq_rpc_scheduler_next(scheduler, &actual), SALTS_OK);
  check_equal(actual, expected);
}

spec("bounded RPC ready scheduling") {
  after_each() {
    flowmq_rpc_scheduler_close(scheduler);
    scheduler = NULL;
  }

  it("admits only each writer's head and preserves its key while suspended") {
    flowmq_rpc_scheduler_config cfg = {
        .capacity = 5, .per_key_limit = 3, .queue_policy = FLOWMQ_RPC_QUEUE_FIFO};
    check_equal(flowmq_rpc_scheduler_open(&cfg, &scheduler), SALTS_OK);
    char temporary[] = "order-42";
    flowmq_rpc_work_class first = classify(temporary, 0, true);
    flowmq_rpc_work_class a = classify("order-42", 0, true);
    flowmq_rpc_work_class b = classify("order-73", 0, true);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 4, &first), SALTS_OK);
    temporary[0] = 'X';
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 0, &a), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 1, &a), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 2, &a), SALTS_ENOBUFS);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 2, &b), SALTS_OK);
    take(4);
    take(2);
    size_t unused;
    check_equal(flowmq_rpc_scheduler_next(scheduler, &unused), SALTS_EBUSY);
    check_equal(flowmq_rpc_scheduler_ready(scheduler, 0), SALTS_EBUSY);
    check_equal(flowmq_rpc_scheduler_ready(scheduler, 4), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_ready(scheduler, 4), SALTS_EBUSY);
    take(4);
    /* Timeout of the middle waiter neither releases the owner nor skips the tail. */
    check_equal(flowmq_rpc_scheduler_finish(scheduler, 0), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_next(scheduler, &unused), SALTS_EBUSY);
    check_equal(flowmq_rpc_scheduler_finish(scheduler, 4), SALTS_OK);
    take(1);
    check_equal(flowmq_rpc_scheduler_finish(scheduler, 1), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_finish(scheduler, 1), SALTS_ENOENT);
    check_equal(flowmq_rpc_scheduler_finish(scheduler, 2), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 0, &b), SALTS_OK);
    take(0);
  }

  it("orders FIFO by readiness, including promotion after a key is released") {
    flowmq_rpc_scheduler_config cfg = {.capacity = 4, .queue_policy = FLOWMQ_RPC_QUEUE_FIFO};
    check_equal(flowmq_rpc_scheduler_open(&cfg, &scheduler), SALTS_OK);
    flowmq_rpc_work_class a = classify("a", 0, true);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 2, &a), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 0, &a), SALTS_OK);
    take(2);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 1, NULL), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_finish(scheduler, 2), SALTS_OK);
    take(1);
    check_equal(flowmq_rpc_scheduler_ready(scheduler, 1), SALTS_OK);
    take(0);
    take(1);
  }

  it("rotates keys even when several independent members of one key are ready") {
    flowmq_rpc_scheduler_config cfg = {.capacity = 5};
    check_equal(flowmq_rpc_scheduler_open(&cfg, &scheduler), SALTS_OK);
    flowmq_rpc_work_class a = classify("tenant-a", 0, false), b = classify("tenant-b", 0, false);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 0, &a), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 1, &a), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 2, &a), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 3, &b), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 4, &b), SALTS_OK);
    take(0);
    take(3);
    take(1);
    check_equal(flowmq_rpc_scheduler_finish(scheduler, 4), SALTS_OK);
    take(2);
    check_equal(flowmq_rpc_scheduler_ready(scheduler, 3), SALTS_OK);
    take(3);
  }

  it("shares service by configured quotas while a high priority writer waits for its key") {
    unsigned weights[] = {2, 1};
    flowmq_rpc_scheduler_config cfg = {.capacity = 6,
                                       .queue_policy = FLOWMQ_RPC_QUEUE_PRIORITY,
                                       .priority_levels = 2,
                                       .priority_weights = weights};
    check_equal(flowmq_rpc_scheduler_open(&cfg, &scheduler), SALTS_OK);
    weights[0] = 100; /* Scheduler owns a copy. */
    flowmq_rpc_work_class high = classify("h", 0, false), low = classify("l", 1, false);
    flowmq_rpc_work_class slow = classify("order", 1, true), rush = classify("order", 0, true);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 0, &slow), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 1, &rush), SALTS_OK);
    for (size_t i = 2; i < 5; ++i)
      check_equal(flowmq_rpc_scheduler_admit(scheduler, i, &high), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 5, &low), SALTS_OK);
    take(2);
    take(3);
    take(0);
    check_equal(flowmq_rpc_scheduler_finish(scheduler, 0), SALTS_OK);
    take(4);
    take(1);
    take(5);
  }

  it("rejects invalid inputs and an unfinished slot reuse without disturbing ready order") {
    flowmq_rpc_scheduler_config cfg = {.capacity = 2};
    flowmq_rpc_work_class invalid = classify("", 0, true);
    check_equal(flowmq_rpc_scheduler_open(&cfg, &scheduler), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 0, &invalid), SALTS_EINVAL);
    invalid.key_size = FLOWMQ_RPC_KEY_MAX + 1;
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 0, &invalid), SALTS_EMSGSIZE);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 0, NULL), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 0, NULL), SALTS_EBUSY);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 2, NULL), SALTS_EINVAL);
    take(0);
    check_equal(flowmq_rpc_scheduler_finish(scheduler, 0), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_ready(scheduler, 0), SALTS_ENOENT);
  }
}

spec("RPC partition placement") {
  it("keeps keys stable across ring reconstruction and moves only acquired intervals on "
     "expansion") {
    uint32_t initial[] = {91, 7, 56}, permuted[] = {56, 91, 7}, expanded[] = {7, 56, 13, 91};
    flowmq_rpc_scheduler_config cfg = {.capacity = 4,
                                       .partition_count = 3,
                                       .partition_ids = initial,
                                       .partitioning = FLOWMQ_RPC_HASH_RING,
                                       .virtual_nodes = 16};
    flowmq_rpc_scheduler *old = NULL, *reordered = NULL, *larger = NULL;
    check_equal(flowmq_rpc_scheduler_open(&cfg, &old), SALTS_OK);
    cfg.partition_ids = permuted;
    check_equal(flowmq_rpc_scheduler_open(&cfg, &reordered), SALTS_OK);
    cfg.partition_ids = expanded;
    cfg.partition_count = 4;
    check_equal(flowmq_rpc_scheduler_open(&cfg, &larger), SALTS_OK);
    unsigned moved = 0, stayed = 0;
    for (unsigned i = 0; i < 2048; ++i) {
      unsigned char key[] = {(unsigned char)(i >> 8), (unsigned char)i};
      uint32_t a, b, c;
      check_equal(flowmq_rpc_scheduler_partition(old, key, sizeof(key), &a), SALTS_OK);
      check_equal(flowmq_rpc_scheduler_partition(reordered, key, sizeof(key), &b), SALTS_OK);
      check_equal(flowmq_rpc_scheduler_partition(larger, key, sizeof(key), &c), SALTS_OK);
      check_equal(a, b);
      if (c == a) ++stayed;
      else {
        check_equal(c, 13u);
        ++moved;
      }
    }
    check(moved > 0 && stayed > 0);
    flowmq_rpc_scheduler_close(larger);
    flowmq_rpc_scheduler_close(reordered);
    flowmq_rpc_scheduler_close(old);
  }

  it("round robins nonempty partitions before taking a second FIFO step from one partition") {
    flowmq_rpc_scheduler_config cfg = {
        .capacity = 3, .partition_count = 2, .queue_policy = FLOWMQ_RPC_QUEUE_FIFO};
    check_equal(flowmq_rpc_scheduler_open(&cfg, &scheduler), SALTS_OK);
    char first_key, second_key;
    bool have_first = false, have_second = false;
    for (unsigned i = 0; i < 256 && !(have_first && have_second); ++i) {
      char key = (char)i;
      uint32_t id;
      check_equal(flowmq_rpc_scheduler_partition(scheduler, &key, 1, &id), SALTS_OK);
      if (id == 0) {
        first_key = key;
        have_first = true;
      } else {
        second_key = key;
        have_second = true;
      }
    }
    check(have_first && have_second);
    flowmq_rpc_work_class a = {&first_key, 1, 0, false}, b = {&second_key, 1, 0, false};
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 0, &a), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 1, &a), SALTS_OK);
    check_equal(flowmq_rpc_scheduler_admit(scheduler, 2, &b), SALTS_OK);
    take(0);
    take(2);
    take(1);
    flowmq_rpc_scheduler_close(scheduler);
    scheduler = NULL;
  }

  it("rejects repeated logical IDs, zero weights and invalid ring or slot budgets") {
    flowmq_rpc_scheduler *out = NULL;
    uint32_t duplicate[] = {7, 7};
    flowmq_rpc_scheduler_config cfg = {
        .capacity = 2, .partition_ids = duplicate, .partition_count = 2};
    check_equal(flowmq_rpc_scheduler_open(&cfg, &out), SALTS_EINVAL);
    cfg.partition_ids = NULL;
    cfg.partitioning = FLOWMQ_RPC_HASH_RING;
    check_equal(flowmq_rpc_scheduler_open(&cfg, &out), SALTS_EINVAL);
    cfg.virtual_nodes = FLOWMQ_RPC_VNODES_MAX + 1;
    check_equal(flowmq_rpc_scheduler_open(&cfg, &out), SALTS_EINVAL);
    cfg = (flowmq_rpc_scheduler_config){.capacity = 2, .queue_policy = FLOWMQ_RPC_QUEUE_PRIORITY};
    unsigned weights[] = {1, 0};
    cfg.priority_levels = 2;
    cfg.priority_weights = weights;
    check_equal(flowmq_rpc_scheduler_open(&cfg, &out), SALTS_EINVAL);
    cfg = (flowmq_rpc_scheduler_config){.capacity = SIZE_MAX};
    check_equal(flowmq_rpc_scheduler_open(&cfg, &out), SALTS_EINVAL);
    check(out == NULL);
  }
}
