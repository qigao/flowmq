#ifndef FLOWMQ_SEND_ADMISSION_H
#define FLOWMQ_SEND_ADMISSION_H

#include "platform.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_SEND_ADMISSION_API_VERSION 1u
#define FLOWMQ_SEND_ADMISSION_DEFAULT_CAPACITY 1024u
#define FLOWMQ_SEND_ADMISSION_DEFAULT_CAPACITY_BYTES (8u * 1024u * 1024u)

/**
 * Reports the local socket-send result for one successfully admitted copied
 * frame. This is not a remote receipt or durable acknowledgement. The callback
 * runs on the endpoint context and must not call endpoint lifecycle functions.
 */
typedef void (*flowmq_send_completion_fn)(void *ctx, uint64_t completion_id,
                                          int status);

typedef struct flowmq_send_admission_config_s {
  size_t capacity;
  size_t capacity_bytes;
  flowmq_send_completion_fn on_complete;
  void *completion_ctx;
} flowmq_send_admission_config_t;

#define FLOWMQ_SEND_ADMISSION_CONFIG_INIT                                      \
  {                                                                            \
    FLOWMQ_SEND_ADMISSION_DEFAULT_CAPACITY,                                    \
        FLOWMQ_SEND_ADMISSION_DEFAULT_CAPACITY_BYTES, NULL, NULL               \
  }

typedef struct flowmq_send_admission_stats_s {
  size_t pending;
  size_t pending_bytes;
  size_t high_water;
  size_t high_water_bytes;
  uint64_t rejected_full;
  uint64_t completed;
  uint64_t failed;
} flowmq_send_admission_stats_t;

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_SEND_ADMISSION_H */
