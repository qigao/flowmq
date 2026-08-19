#ifndef FLOWMQ_POSTED_SEND_H
#define FLOWMQ_POSTED_SEND_H

#include "CoroNet/turbo_coro_context.h"
#include "flowmq_send_admission.h"
#include "turbo_thread.h"

#include <stddef.h>
#include <stdint.h>

typedef int (*flowmq_posted_send_execute_fn)(void *owner, const void *route,
                                             size_t route_size,
                                             const char *encoded,
                                             size_t encoded_size);

typedef struct flowmq_posted_send_s {
  coro_context_t *context;
  void *owner;
  flowmq_posted_send_execute_fn execute;
  flowmq_send_admission_config_t config;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  int initialized;
  int accepting;
  size_t posting;
  flowmq_send_admission_stats_t stats;
} flowmq_posted_send_t;

int flowmq_posted_send_init(flowmq_posted_send_t *send,
                            coro_context_t *context, void *owner,
                            flowmq_posted_send_execute_fn execute,
                            const flowmq_send_admission_config_t *config);
void flowmq_posted_send_start(flowmq_posted_send_t *send);
void flowmq_posted_send_stop(flowmq_posted_send_t *send);
void flowmq_posted_send_destroy(flowmq_posted_send_t *send);
int flowmq_posted_send_copy(flowmq_posted_send_t *send, const void *route,
                            size_t route_size, uint64_t completion_id,
                            const char *encoded, size_t encoded_size);
void flowmq_posted_send_get_stats(flowmq_posted_send_t *send,
                                  flowmq_send_admission_stats_t *stats);

#endif /* FLOWMQ_POSTED_SEND_H */
