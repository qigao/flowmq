#ifndef FLOWMQ_DELIVERY_H
#define FLOWMQ_DELIVERY_H
#include <orm_runtime.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* One provisioned producer/inbox stream per database. A stream ID is a
 * deployment-assigned positive int64, permanently bound to the same producer
 * and inbox. Serialize operations on a store on its storage owner thread. */
typedef struct flowmq_delivery_store flowmq_delivery_store;
typedef enum flowmq_delivery_role {
  FLOWMQ_DELIVERY_OUTBOX = 1, FLOWMQ_DELIVERY_INBOX = 2
} flowmq_delivery_role;
typedef struct flowmq_delivery_config {
  const char *filename;
  int64_t stream_id;
  flowmq_delivery_role role;
  int64_t max_records;
  int64_t max_bytes; /* pending encoded DATA bytes */
} flowmq_delivery_config;

typedef struct flowmq_delivery_part {
  const void *data;
  size_t size;
} flowmq_delivery_part;

enum { FLOWMQ_DELIVERY_HEADER = 20, FLOWMQ_DELIVERY_PARTS_MAX = 64,
       FLOWMQ_DELIVERY_FRAME_MAX = 1024 * 1024 };
typedef struct flowmq_delivery_item {
  unsigned char *frame;
  size_t size;
  int64_t sequence;
  size_t part_count;
  flowmq_delivery_part parts[FLOWMQ_DELIVERY_PARTS_MAX];
} flowmq_delivery_item;

/* runtime has the SQLite driver loaded. create=1 provisions an unused DB;
 * create=0 requires the existing store, identity, role and quotas to match.
 * The runtime must outlive the store. Close/reopen after COMMIT_UNKNOWN. */
orm_status_t flowmq_delivery_open(orm_runtime_t *runtime,
    const flowmq_delivery_config *config, int create,
    flowmq_delivery_store **out, orm_error_t *error);
void flowmq_delivery_close(flowmq_delivery_store *store);

/* Copies all parts before returning. On OK the output sequence is durable. */
orm_status_t flowmq_delivery_enqueue(flowmq_delivery_store *outbox,
    const flowmq_delivery_part *parts, size_t count, int64_t *sequence,
    orm_error_t *error);

/* Read the oldest unacknowledged/undelivered record. An empty store returns
 * OK with item.frame == NULL. Pass a zero-initialized item. On success send
 * its entire frame as one socket part (outbox), or process its parts (inbox).
 * Item owns its memory independently of the store until item_release. */
orm_status_t flowmq_delivery_peek(flowmq_delivery_store *store,
    flowmq_delivery_item *item, orm_error_t *error);
void flowmq_delivery_item_release(flowmq_delivery_item *item);

/* Caller has verified the remote transport principal is authorized for this
 * stream. Input is exactly one complete DATA or ACK envelope. Only a successful
 * accept produces an ACK. Its loss is recovered by repeating the original DATA.
 * Duplicate settlement never advances past the acknowledged sequence. */
orm_status_t flowmq_delivery_accept(flowmq_delivery_store *inbox,
    const void *data, size_t size, unsigned char ack[FLOWMQ_DELIVERY_HEADER],
    orm_error_t *error);
orm_status_t flowmq_delivery_settle(flowmq_delivery_store *outbox,
    const void *ack, size_t size, orm_error_t *error);

/* Called after successful business processing. Only the head may be consumed.
 * External side effects must be idempotent across a crash before this commit. */
orm_status_t flowmq_delivery_consume(flowmq_delivery_store *inbox,
    int64_t sequence, orm_error_t *error);

/* For application state in the inbox database. apply executes under the
 * same transaction as head removal; use the supplied transaction for every
 * mutation. The borrowed handles expire on return. Do not close or commit
 * them, reenter the store, or perform external side effects. Duplicate
 * consumption skips apply. Failure rolls back both changes. */
typedef orm_status_t (*flowmq_delivery_apply_fn)(void *context,
    orm_connection_t *db, orm_transaction_t *tx, orm_error_t *error);
orm_status_t flowmq_delivery_consume_apply(flowmq_delivery_store *inbox,
    int64_t sequence, flowmq_delivery_apply_fn apply, void *context,
    orm_error_t *error);

#ifdef __cplusplus
}
#endif
#endif
