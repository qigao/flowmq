#include "flowmq_delivery.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Process fixture. Exit 42 marks only a specifically reached crash boundary. */
enum { CRASH_BOUNDARY = 42 };
static orm_error_t error;

static void require_ok(orm_status_t rc) {
  if (rc != ORM_STATUS_OK) {
    fprintf(stderr, "ORM %u: %s\n", (unsigned)rc, error.message);
    exit(EXIT_FAILURE);
  }
}

static orm_status_t record_effect(void *crash, orm_connection_t *db,
    orm_transaction_t *tx, orm_error_t *err) {
  orm_query_t *query = NULL;
  orm_result_t *result = NULL;
  /* CREATE without IF NOT EXISTS also verifies rollback of the aborted turn. */
  orm_status_t rc = orm_raw(db, orm_view("create table business_effect(id integer)"), &query, err);
  if (rc == ORM_STATUS_OK) rc = orm_query_execute_in_transaction(query, tx, &result, err);
  orm_result_destroy(result);
  orm_query_destroy(query);
  if (crash && rc == ORM_STATUS_OK) _Exit(CRASH_BOUNDARY);
  return rc;
}

int main(int argc, char **argv) {
  if (argc != 4) return EXIT_FAILURE;
  const char *phase = argv[1];
  int initialize = strcmp(phase, "enqueue") == 0;
  orm_runtime_config_t config;
  orm_runtime_config_init(&config);
  orm_runtime_t *runtime = NULL;
  orm_error_init(&error);
  require_ok(orm_runtime_create(&config, &runtime, &error));
  orm_driver_load_config_t load = {0};
  load.struct_size = sizeof(load);
  load.abi_version = ORM_RUNTIME_ABI_VERSION;
  load.module_path = orm_view(getenv("ORM_SQLITE_PLUGIN"));
  load.expected_driver_id = orm_view("sqlite");
  require_ok(orm_runtime_load_driver(runtime, &load, &error));
  flowmq_delivery_store *sender = NULL, *receiver = NULL;
  flowmq_delivery_config out = {argv[2], 11, FLOWMQ_DELIVERY_OUTBOX, 2, 128};
  flowmq_delivery_config in = {argv[3], 11, FLOWMQ_DELIVERY_INBOX, 2, 128};
  require_ok(flowmq_delivery_open(runtime, &out, initialize, &sender, &error));
  require_ok(flowmq_delivery_open(runtime, &in, initialize, &receiver, &error));
  if (initialize) {
    flowmq_delivery_part part = {"recovery", 8};
    int64_t seq = 0;
    require_ok(flowmq_delivery_enqueue(sender, &part, 1, &seq, &error));
    if (seq != 1) return EXIT_FAILURE;
    _Exit(CRASH_BOUNDARY);
  }
  flowmq_delivery_item pending = {0}, business = {0};
  require_ok(flowmq_delivery_peek(sender, &pending, &error));
  require_ok(flowmq_delivery_peek(receiver, &business, &error));
  if (strcmp(phase, "verify") == 0) {
    if (pending.frame || business.frame) return EXIT_FAILURE;
    /* If the record was already consumed, this non-idempotent action must
     * never run again. The table exists and a second create would fail. */
    require_ok(flowmq_delivery_consume_apply(receiver, 1, record_effect, NULL, &error));
  } else {
    if (pending.sequence != 1 || pending.part_count != 1 ||
        pending.parts[0].size != 8 || memcmp(pending.parts[0].data, "recovery", 8))
      return EXIT_FAILURE;
    unsigned char ack[FLOWMQ_DELIVERY_HEADER];
    if (strcmp(phase, "receive") == 0) {
      require_ok(flowmq_delivery_accept(receiver, pending.frame, pending.size, ack, &error));
      _Exit(CRASH_BOUNDARY);
    } else if (strcmp(phase, "abort-business") == 0 || strcmp(phase, "commit-business") == 0) {
      if (business.sequence != 1) return EXIT_FAILURE;
      require_ok(flowmq_delivery_consume_apply(receiver, 1, record_effect,
          strcmp(phase, "abort-business") == 0 ? runtime : NULL, &error));
      _Exit(CRASH_BOUNDARY);
    } else if (strcmp(phase, "settle") == 0) {
      if (business.frame) return EXIT_FAILURE;
      require_ok(flowmq_delivery_accept(receiver, pending.frame, pending.size, ack, &error));
      require_ok(flowmq_delivery_settle(sender, ack, sizeof(ack), &error));
      _Exit(CRASH_BOUNDARY);
    } else return EXIT_FAILURE;
  }
  flowmq_delivery_item_release(&pending);
  flowmq_delivery_item_release(&business);
  flowmq_delivery_close(sender);
  flowmq_delivery_close(receiver);
  require_ok(orm_runtime_close(runtime, &error));
  orm_runtime_release(runtime);
  return EXIT_SUCCESS;
}
