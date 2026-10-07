#ifndef FLOWMQ_SOCKET_OPTION_H
#define FLOWMQ_SOCKET_OPTION_H

#include "flowmq_pattern.h"
#include "flowmq_socket.h"

#include <stddef.h>
#include <stdint.h>

enum {
  FLOWMQ_SOCKET_OPTION_MESSAGE_HWM_MAX = 1024u,
  FLOWMQ_SOCKET_OPTION_HWM_BYTES_MAX = 64u * 1024u * 1024u,
  FLOWMQ_SOCKET_OPTION_TLS_PATH_CAPACITY = 1024u,
  FLOWMQ_SOCKET_OPTION_TLS_PASSWORD_CAPACITY = 512u,
  FLOWMQ_SOCKET_OPTION_TLS_SERVER_NAME_CAPACITY = 256u
};

typedef enum flowmq_socket_option_value_kind_e {
  FLOWMQ_SOCKET_OPTION_VALUE_INT = 0,
  FLOWMQ_SOCKET_OPTION_VALUE_SIZE,
  FLOWMQ_SOCKET_OPTION_VALUE_U64,
  FLOWMQ_SOCKET_OPTION_VALUE_STRING,
  FLOWMQ_SOCKET_OPTION_VALUE_BYTES,
  FLOWMQ_SOCKET_OPTION_VALUE_STRUCT
} flowmq_socket_option_value_kind_t;

typedef enum flowmq_socket_option_access_e {
  FLOWMQ_SOCKET_OPTION_ACCESS_SET = 1u << 0,
  FLOWMQ_SOCKET_OPTION_ACCESS_GET = 1u << 1
} flowmq_socket_option_access_t;

typedef enum flowmq_socket_option_set_phase_e {
  FLOWMQ_SOCKET_OPTION_SET_STARTUP = 0,
  FLOWMQ_SOCKET_OPTION_SET_RUNTIME
} flowmq_socket_option_set_phase_t;

typedef enum flowmq_socket_option_validation_e {
  FLOWMQ_SOCKET_OPTION_VALIDATE_EXACT = 0,
  FLOWMQ_SOCKET_OPTION_VALIDATE_SIGNED_RANGE,
  FLOWMQ_SOCKET_OPTION_VALIDATE_UNSIGNED_RANGE,
  FLOWMQ_SOCKET_OPTION_VALIDATE_STRING,
  FLOWMQ_SOCKET_OPTION_VALIDATE_BYTES
} flowmq_socket_option_validation_t;

typedef struct flowmq_socket_option_desc_s {
  int option;
  uint8_t value_kind;
  uint8_t access;
  uint8_t set_phase;
  uint8_t validation;
  size_t abi_size;
  int64_t min_value;
  uint64_t max_value;
  uint16_t pattern_mask;
} flowmq_socket_option_desc_t;

size_t flowmq_socket_option_descriptor_count(void);
const flowmq_socket_option_desc_t *
flowmq_socket_option_descriptor(int option);

int flowmq_socket_option_validate_set(
    int option, int runtime_initialized,
    const flowmq_pattern_desc_t *pattern_desc, const void *value, size_t size,
    const flowmq_socket_option_desc_t **out_desc);

int flowmq_socket_option_prepare_get(
    int option, void *value, size_t *size,
    const flowmq_socket_option_desc_t **out_desc);

#endif /* FLOWMQ_SOCKET_OPTION_H */
