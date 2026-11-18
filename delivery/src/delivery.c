#include "flowmq_delivery.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct flowmq_delivery_store {
  orm_connection_t *db;
  int64_t stream;
  flowmq_delivery_role role;
  int64_t max_records;
  int64_t max_bytes;
};

static orm_status_t fail(orm_error_t *err, orm_status_t status, const char *reason) {
  if (err) {
    orm_error_init(err);
    err->status = status;
    snprintf(err->message, sizeof(err->message), "%s", reason);
  }
  return status;
}

static orm_status_t query(flowmq_delivery_store *s, orm_transaction_t *tx,
    const char *sql, const orm_value_t *args, size_t n,
    orm_result_t **output, orm_error_t *err) {
  orm_query_t *q = NULL;
  orm_result_t *result = NULL;
  orm_status_t rc = orm_raw(s->db, orm_view(sql), &q, err);
  for (size_t i = 0; rc == ORM_STATUS_OK && i < n; ++i)
    rc = orm_query_bind(q, args[i], err);
  if (rc == ORM_STATUS_OK) {
    rc = tx ? orm_query_execute_in_transaction(q, tx, &result, err)
            : orm_query_execute(q, &result, err);
  }
  orm_query_destroy(q);
  if (rc == ORM_STATUS_OK && output) *output = result;
  else orm_result_destroy(result);
  return rc;
}

static orm_status_t finish(orm_transaction_t *tx, orm_status_t rc, orm_error_t *err) {
  if (tx && rc == ORM_STATUS_OK) rc = orm_transaction_commit(tx, err);
  /* ORM destroys an active transaction by rolling back. An uncertain commit
   * is quarantined by the provider and must be resolved through reopen. */
  orm_transaction_destroy(tx);
  return rc;
}

static orm_status_t integers(flowmq_delivery_store *s, orm_transaction_t *tx,
    const char *sql, int64_t *values, size_t count, orm_error_t *err) {
  orm_result_t *r = NULL;
  orm_status_t rc = query(s, tx, sql, NULL, 0, &r, err);
  for (size_t i = 0; rc == ORM_STATUS_OK && i < count; ++i)
    rc = orm_result_get_int64(r, 0, i, &values[i], err);
  orm_result_destroy(r);
  return rc;
}

static orm_status_t configure_sqlite(flowmq_delivery_store *s, orm_error_t *err) {
  orm_result_t *r = NULL;
  orm_string_view_t mode = {0};
  orm_status_t rc = query(s, NULL, "pragma journal_mode=WAL", NULL, 0, &r, err);
  if (rc == ORM_STATUS_OK) rc = orm_result_get_text(r, 0, 0, &mode, err);
  if (rc == ORM_STATUS_OK && (mode.len != 3 || memcmp(mode.data, "wal", 3)))
    rc = fail(err, ORM_STATUS_UNSUPPORTED, "SQLite file does not support WAL");
  orm_result_destroy(r);
  if (rc == ORM_STATUS_OK)
    rc = query(s, NULL, "pragma synchronous=FULL", NULL, 0, NULL, err);
  int64_t sync = 0;
  if (rc == ORM_STATUS_OK)
    rc = integers(s, NULL, "pragma synchronous", &sync, 1, err);
  if (rc == ORM_STATUS_OK && sync != 2)
    rc = fail(err, ORM_STATUS_UNSUPPORTED, "SQLite FULL synchronization required");
  return rc;
}

orm_status_t flowmq_delivery_open(orm_runtime_t *runtime,
    const flowmq_delivery_config *cfg, int create, flowmq_delivery_store **out,
    orm_error_t *err) {
  if (!runtime || !cfg || !cfg->filename || !cfg->filename[0] || !out ||
      cfg->stream_id <= 0 || cfg->max_records <= 0 || cfg->max_bytes <= 0 ||
      (create != 0 && create != 1) ||
      (cfg->role != FLOWMQ_DELIVERY_OUTBOX && cfg->role != FLOWMQ_DELIVERY_INBOX))
    return fail(err, ORM_STATUS_INVALID_ARGUMENT, "invalid delivery store configuration");
  *out = NULL;
  flowmq_delivery_store *s = calloc(1, sizeof(*s));
  if (!s) return fail(err, ORM_STATUS_OUT_OF_MEMORY, "allocate delivery store");
  s->role = cfg->role;
  s->stream = cfg->stream_id;
  s->max_bytes = cfg->max_bytes;
  s->max_records = cfg->max_records;
  orm_option_t options[] = {
    {orm_view("filename"), orm_view(cfg->filename)},
    {orm_view("open_mode"), orm_view(create ? "read_write_create" : "read_write")}
  };
  orm_config_t config;
  orm_config(&config);
  config.driver = orm_view("sqlite");
  config.options = options;
  config.option_count = 2;
  config.max_parameter_bytes = FLOWMQ_DELIVERY_FRAME_MAX + 128;
  config.max_result_bytes = FLOWMQ_DELIVERY_FRAME_MAX + 128;
  orm_status_t rc = orm_runtime_connect(runtime, &config, &s->db, err);
  if (rc == ORM_STATUS_OK) rc = configure_sqlite(s, err);
  orm_transaction_t *tx = NULL;
  if (rc == ORM_STATUS_OK)
    rc = orm_transaction_begin(s->db, ORM_ISOLATION_SERIALIZABLE, &tx, err);
  if (rc == ORM_STATUS_OK && create) {
    rc = query(s, tx, "create table flowmq_stream(id integer primary key check(id=1),"
        "stream integer not null,role integer not null,records_max integer not null,"
        "bytes_max integer not null,high integer not null,done integer not null,"
        "used integer not null check(used>=0),check(high>=done and done>=0))",
        NULL, 0, NULL, err);
    if (rc == ORM_STATUS_OK)
      rc = query(s, tx, "create table flowmq_message(seq integer primary key,"
          "frame blob not null)", NULL, 0, NULL, err);
    orm_value_t args[] = {orm_i64(s->stream), orm_i64(s->role),
                          orm_i64(s->max_records), orm_i64(s->max_bytes)};
    if (rc == ORM_STATUS_OK)
      rc = query(s, tx, "insert into flowmq_stream values(1,?1,?2,?3,?4,0,0,0)",
                 args, 4, NULL, err);
  }
  int64_t meta[4];
  if (rc == ORM_STATUS_OK)
    rc = integers(s, tx, "select stream,role,records_max,bytes_max from flowmq_stream where id=1",
                  meta, 4, err);
  if (rc == ORM_STATUS_OK && (meta[0] != s->stream || meta[1] != s->role ||
      meta[2] != s->max_records || meta[3] != s->max_bytes))
    rc = fail(err, ORM_STATUS_INVALID_STATE, "delivery store configuration mismatch");
  rc = finish(tx, rc, err);
  if (rc == ORM_STATUS_OK) *out = s;
  else flowmq_delivery_close(s);
  return rc;
}

void flowmq_delivery_close(flowmq_delivery_store *s) {
  if (s) {
    if (s->db) orm_disconnect(s->db);
    free(s);
  }
}

static uint64_t read_be(const unsigned char *p, size_t bytes) {
  uint64_t value = 0;
  for (size_t i = 0; i < bytes; ++i) value = (value << 8) | p[i];
  return value;
}

static void write_be(unsigned char *p, size_t bytes, uint64_t value) {
  while (bytes) {
    p[--bytes] = (unsigned char)value;
    value >>= 8;
  }
}

static void header(unsigned char *p, const char tag[4], int64_t stream, int64_t seq) {
  memcpy(p, tag, 4);
  write_be(p + 4, 8, (uint64_t)stream);
  write_be(p + 12, 8, (uint64_t)seq);
}

static orm_status_t decode(const void *data, size_t size, int64_t stream,
    flowmq_delivery_item *item, orm_error_t *err) {
  const unsigned char *p = data;
  if (!p || size <= FLOWMQ_DELIVERY_HEADER || size > FLOWMQ_DELIVERY_FRAME_MAX ||
      memcmp(p, "FMD1", 4) || read_be(p+4, 8) != (uint64_t)stream)
    return fail(err, ORM_STATUS_VALIDATION_ERROR, "invalid delivery DATA envelope");
  uint64_t seq = read_be(p+12, 8);
  if (seq == 0 || seq > INT64_MAX)
    return fail(err, ORM_STATUS_VALIDATION_ERROR, "invalid sequence");
  item->sequence = (int64_t)seq;
  size_t offset = FLOWMQ_DELIVERY_HEADER;
  while (offset < size) {
    if (size-offset < 4 || item->part_count == FLOWMQ_DELIVERY_PARTS_MAX)
      return fail(err, ORM_STATUS_VALIDATION_ERROR, "invalid part list");
    size_t length = (size_t)read_be(p+offset, 4);
    offset += 4;
    if (length > size-offset)
      return fail(err, ORM_STATUS_VALIDATION_ERROR, "truncated delivery part");
    item->parts[item->part_count++] = (flowmq_delivery_part){p+offset,length};
    offset += length;
  }
  return ORM_STATUS_OK;
}

/* This transaction owns sequence allocation or duplicate detection, the
 * pending frame, and its quota charge. No ACK is observable before commit. */
static orm_status_t append(flowmq_delivery_store *s, unsigned char *encoded,
    const unsigned char *received, size_t size, int64_t *sequence, orm_error_t *err) {
  orm_transaction_t *tx = NULL;
  int64_t state[3];
  orm_status_t rc = orm_transaction_begin(s->db, ORM_ISOLATION_SERIALIZABLE, &tx, err);
  if (rc == ORM_STATUS_OK)
    rc = integers(s, tx, "select high,done,used from flowmq_stream where id=1", state, 3, err);
  if (rc != ORM_STATUS_OK) return finish(tx, rc, err);
  int64_t high = state[0];
  if (received && *sequence <= high) return finish(tx, ORM_STATUS_OK, err);
  if (high == INT64_MAX)
    rc = fail(err, ORM_STATUS_OUT_OF_RANGE, "delivery sequence exhausted");
  else if (received && *sequence != high+1)
    rc = fail(err, ORM_STATUS_INVALID_STATE, "delivery sequence gap");
  else if (high-state[1] >= s->max_records || (int64_t)size > s->max_bytes-state[2])
    rc = fail(err, ORM_STATUS_LIMIT_EXCEEDED, "delivery queue full");
  if (rc == ORM_STATUS_OK) {
    if (encoded) header(encoded, "FMD1", s->stream, high+1);
    orm_value_t args[] = {orm_i64(high+1), orm_blob(received ? received : encoded, size)};
    rc = query(s, tx, "insert into flowmq_message(seq,frame) values(?1,?2)", args, 2, NULL, err);
    args[1] = orm_i64((int64_t)size);
    if (rc == ORM_STATUS_OK)
      rc = query(s, tx, "update flowmq_stream set high=?1,used=used+?2 where id=1", args, 2, NULL, err);
  }
  rc = finish(tx, rc, err);
  if (rc == ORM_STATUS_OK) *sequence = high+1;
  return rc;
}

orm_status_t flowmq_delivery_enqueue(flowmq_delivery_store *s,
    const flowmq_delivery_part *parts, size_t count, int64_t *seq, orm_error_t *err) {
  if (!s || s->role != FLOWMQ_DELIVERY_OUTBOX || !parts || !count ||
      count > FLOWMQ_DELIVERY_PARTS_MAX || !seq)
    return fail(err, ORM_STATUS_INVALID_ARGUMENT, "invalid enqueue");
  size_t size = FLOWMQ_DELIVERY_HEADER;
  for (size_t i = 0; i < count; ++i) {
    if (!parts[i].data && parts[i].size)
      return fail(err, ORM_STATUS_INVALID_ARGUMENT, "missing part data");
    if (size > FLOWMQ_DELIVERY_FRAME_MAX-4 || parts[i].size > FLOWMQ_DELIVERY_FRAME_MAX-size-4)
      return fail(err, ORM_STATUS_LIMIT_EXCEEDED, "encoded frame too large");
    size += 4+parts[i].size;
  }
  unsigned char *frame = malloc(size);
  if (!frame) return fail(err, ORM_STATUS_OUT_OF_MEMORY, "allocate delivery frame");
  size_t offset = FLOWMQ_DELIVERY_HEADER;
  for (size_t i = 0; i < count; ++i) {
    write_be(frame+offset, 4, parts[i].size);
    offset += 4;
    if (parts[i].size) memcpy(frame+offset, parts[i].data, parts[i].size);
    offset += parts[i].size;
  }
  orm_status_t rc = append(s, frame, NULL, size, seq, err);
  free(frame);
  return rc;
}

orm_status_t flowmq_delivery_accept(flowmq_delivery_store *s,
    const void *data, size_t size, unsigned char ack[FLOWMQ_DELIVERY_HEADER], orm_error_t *err) {
  if (!s || s->role != FLOWMQ_DELIVERY_INBOX || !ack)
    return fail(err, ORM_STATUS_INVALID_ARGUMENT, "invalid inbox");
  flowmq_delivery_item item = {0};
  orm_status_t rc = decode(data, size, s->stream, &item, err);
  if (rc == ORM_STATUS_OK) rc = append(s, NULL, data, size, &item.sequence, err);
  if (rc == ORM_STATUS_OK) header(ack, "FMA1", s->stream, item.sequence);
  return rc;
}

void flowmq_delivery_item_release(flowmq_delivery_item *item) {
  if (item) {
    free(item->frame);
    memset(item, 0, sizeof(*item));
  }
}

orm_status_t flowmq_delivery_peek(flowmq_delivery_store *s,
    flowmq_delivery_item *item, orm_error_t *err) {
  if (!s || !item || item->frame)
    return fail(err, ORM_STATUS_INVALID_ARGUMENT, "invalid delivery output");
  orm_result_t *r = NULL;
  orm_status_t rc = query(s, NULL, "select seq,frame from flowmq_message order by seq limit 1",
                          NULL, 0, &r, err);
  uint64_t rows = 0;
  if (rc == ORM_STATUS_OK) rc = orm_result_row_count(r, &rows, err);
  flowmq_delivery_item next = {0};
  if (rc == ORM_STATUS_OK && rows) {
    int64_t seq = 0;
    orm_blob_t blob = {0};
    rc = orm_result_get_int64(r, 0, 0, &seq, err);
    if (rc == ORM_STATUS_OK) rc = orm_result_get_blob(r, 0, 1, &blob, err);
    if (rc == ORM_STATUS_OK && (!blob.size || blob.size > FLOWMQ_DELIVERY_FRAME_MAX))
      rc = fail(err, ORM_STATUS_INVALID_STATE, "invalid stored frame size");
    if (rc == ORM_STATUS_OK) {
      next.size = blob.size;
      next.frame = malloc(next.size);
      if (!next.frame) rc = fail(err, ORM_STATUS_OUT_OF_MEMORY, "read stored frame");
    }
    if (rc == ORM_STATUS_OK) {
      memcpy(next.frame, blob.data, next.size);
      rc = decode(next.frame, next.size, s->stream, &next, err);
      if (rc == ORM_STATUS_OK && next.sequence != seq)
        rc = fail(err, ORM_STATUS_INVALID_STATE, "stored sequence mismatch");
    }
  }
  orm_result_destroy(r);
  if (rc == ORM_STATUS_OK) *item = next;
  else flowmq_delivery_item_release(&next);
  return rc;
}

static orm_status_t remove_head(flowmq_delivery_store *s, int64_t seq,
    flowmq_delivery_apply_fn apply, void *context, orm_error_t *err) {
  orm_transaction_t *tx = NULL;
  int64_t state[2];
  orm_status_t rc = orm_transaction_begin(s->db, ORM_ISOLATION_SERIALIZABLE, &tx, err);
  if (rc == ORM_STATUS_OK)
    rc = integers(s, tx, "select done,high from flowmq_stream where id=1", state, 2, err);
  if (rc != ORM_STATUS_OK) return finish(tx, rc, err);
  if (seq <= state[0]) return finish(tx, ORM_STATUS_OK, err);
  if (seq > state[1] || seq != state[0]+1)
    return finish(tx, fail(err, ORM_STATUS_INVALID_STATE, "record is not queue head"), err);
  if (apply) {
    rc = apply(context, s->db, tx, err);
    if (rc != ORM_STATUS_OK) return finish(tx, rc, err);
  }
  orm_value_t id = orm_i64(seq);
  rc = query(s, tx, "update flowmq_stream set done=?1,"
      "used=used-(select length(frame) from flowmq_message where seq=?1) where id=1",
      &id, 1, NULL, err);
  if (rc == ORM_STATUS_OK)
    rc = query(s, tx, "delete from flowmq_message where seq=?1", &id, 1, NULL, err);
  return finish(tx, rc, err);
}

orm_status_t flowmq_delivery_consume(flowmq_delivery_store *s,
    int64_t seq, orm_error_t *err) {
  if (!s || s->role != FLOWMQ_DELIVERY_INBOX || seq <= 0)
    return fail(err, ORM_STATUS_INVALID_ARGUMENT, "invalid inbox consumption");
  return remove_head(s, seq, NULL, NULL, err);
}

orm_status_t flowmq_delivery_consume_apply(flowmq_delivery_store *s,
    int64_t seq, flowmq_delivery_apply_fn apply, void *context, orm_error_t *err) {
  if (!s || s->role != FLOWMQ_DELIVERY_INBOX || seq <= 0 || !apply)
    return fail(err, ORM_STATUS_INVALID_ARGUMENT, "invalid transactional consumer");
  return remove_head(s, seq, apply, context, err);
}

orm_status_t flowmq_delivery_settle(flowmq_delivery_store *s,
    const void *data, size_t size, orm_error_t *err) {
  const unsigned char *p = data;
  if (!s || s->role != FLOWMQ_DELIVERY_OUTBOX || !p || size != FLOWMQ_DELIVERY_HEADER ||
      memcmp(p, "FMA1", 4) || read_be(p+4, 8) != (uint64_t)s->stream)
    return fail(err, ORM_STATUS_VALIDATION_ERROR, "invalid receipt");
  uint64_t seq = read_be(p+12, 8);
  if (!seq || seq > INT64_MAX)
    return fail(err, ORM_STATUS_VALIDATION_ERROR, "invalid receipt sequence");
  return remove_head(s, (int64_t)seq, NULL, NULL, err);
}
