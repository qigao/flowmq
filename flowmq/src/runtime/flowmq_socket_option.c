#include "flowmq_socket_option.h"
#include "flowmq_socket_option_schema.h"

#include "salts_error.h"

#include <string.h>

#define FLOWMQ_SOCKET_OPTION_DESC_ROW(option_value, kind_value, access_value,       \
                                      phase_value, validation_value, abi_value,      \
                                      min_value_, max_value_, pattern_mask_value)   \
  {option_value, kind_value, access_value, phase_value, validation_value, abi_value,\
   (int64_t)(min_value_), (uint64_t)(max_value_),                                  \
   (uint16_t)(pattern_mask_value)},

static const flowmq_socket_option_desc_t FLOWMQ_SOCKET_OPTION_DESCRIPTORS[] = {
    Replay(FLOWMQ_SOCKET_OPTION_SCHEMA, FLOWMQ_SOCKET_OPTION_DESC_ROW)};

#undef FLOWMQ_SOCKET_OPTION_DESC_ROW

#define FLOWMQ_SOCKET_OPTION_COUNT_ROW(option_value, kind_value, access_value,       \
                                       phase_value, validation_value, abi_value,      \
                                       min_value_, max_value_, pattern_mask_value)   \
  +1
enum {
  FLOWMQ_SOCKET_OPTION_SCHEMA_COUNT =
      0 Replay(FLOWMQ_SOCKET_OPTION_SCHEMA, FLOWMQ_SOCKET_OPTION_COUNT_ROW)
};
#undef FLOWMQ_SOCKET_OPTION_COUNT_ROW

_Static_assert(FLOWMQ_SOCKET_OPTION_SCHEMA_COUNT == 22,
               "socket option schema must cover every public option");

size_t flowmq_socket_option_descriptor_count(void) {
  return sizeof(FLOWMQ_SOCKET_OPTION_DESCRIPTORS) /
         sizeof(FLOWMQ_SOCKET_OPTION_DESCRIPTORS[0]);
}

const flowmq_socket_option_desc_t *
flowmq_socket_option_descriptor(int option) {
  for (size_t i = 0u; i < flowmq_socket_option_descriptor_count(); ++i) {
    if (FLOWMQ_SOCKET_OPTION_DESCRIPTORS[i].option == option)
      return &FLOWMQ_SOCKET_OPTION_DESCRIPTORS[i];
  }
  return NULL;
}

static int flowmq_socket_option_pattern_allowed(
    const flowmq_socket_option_desc_t *desc,
    const flowmq_pattern_desc_t *pattern_desc) {
  uint16_t bit;
  if (desc->pattern_mask == 0u) return 1;
  if (pattern_desc == NULL || pattern_desc->pattern >= 16u) return 0;
  bit = (uint16_t)(UINT16_C(1) << pattern_desc->pattern);
  return (desc->pattern_mask & bit) != 0u;
}

static int flowmq_socket_option_validate_value(
    const flowmq_socket_option_desc_t *desc, const void *value, size_t size) {
  switch ((flowmq_socket_option_validation_t)desc->validation) {
  case FLOWMQ_SOCKET_OPTION_VALIDATE_EXACT:
    return value != NULL && size == desc->abi_size ? SALTS_OK : SALTS_EINVAL;

  case FLOWMQ_SOCKET_OPTION_VALIDATE_SIGNED_RANGE: {
    int value_int;
    if (value == NULL || size != sizeof(value_int) ||
        desc->abi_size != sizeof(value_int))
      return SALTS_EINVAL;
    memcpy(&value_int, value, sizeof(value_int));
    return (int64_t)value_int >= desc->min_value &&
                   (int64_t)value_int <= (int64_t)desc->max_value
               ? SALTS_OK
               : SALTS_EINVAL;
  }

  case FLOWMQ_SOCKET_OPTION_VALIDATE_UNSIGNED_RANGE: {
    size_t value_size;
    if (value == NULL || size != sizeof(value_size) ||
        desc->abi_size != sizeof(value_size))
      return SALTS_EINVAL;
    memcpy(&value_size, value, sizeof(value_size));
    return (uint64_t)value_size >= (uint64_t)desc->min_value &&
                   (uint64_t)value_size <= desc->max_value
               ? SALTS_OK
               : SALTS_EINVAL;
  }

  case FLOWMQ_SOCKET_OPTION_VALIDATE_STRING:
    if (value == NULL || size < (size_t)desc->min_value ||
        (uint64_t)size > desc->max_value || memchr(value, '\0', size) != NULL)
      return SALTS_EINVAL;
    return SALTS_OK;

  case FLOWMQ_SOCKET_OPTION_VALIDATE_BYTES:
    if ((value == NULL && size != 0u) ||
        size < (size_t)desc->min_value || (uint64_t)size > desc->max_value)
      return SALTS_EINVAL;
    return SALTS_OK;

  default: return SALTS_EPROTO;
  }
}

int flowmq_socket_option_validate_set(
    int option, int runtime_initialized,
    const flowmq_pattern_desc_t *pattern_desc, const void *value, size_t size,
    const flowmq_socket_option_desc_t **out_desc) {
  const flowmq_socket_option_desc_t *desc =
      flowmq_socket_option_descriptor(option);
  if (out_desc != NULL) *out_desc = desc;

  /*
   * Preserve the established setsockopt ordering: once runtime initialization
   * has happened, every non-runtime-mutable option (including unknown or
   * get-only values) reports EBUSY before switch/access dispatch.
   */
  if (runtime_initialized &&
      (desc == NULL ||
       desc->set_phase == FLOWMQ_SOCKET_OPTION_SET_STARTUP))
    return SALTS_EBUSY;

  if (desc == NULL ||
      (desc->access & FLOWMQ_SOCKET_OPTION_ACCESS_SET) == 0u)
    return SALTS_ENOTSUP;
  if (!flowmq_socket_option_pattern_allowed(desc, pattern_desc))
    return SALTS_EINVAL;
  return flowmq_socket_option_validate_value(desc, value, size);
}

int flowmq_socket_option_prepare_get(
    int option, void *value, size_t *size,
    const flowmq_socket_option_desc_t **out_desc) {
  const flowmq_socket_option_desc_t *desc;
  size_t required;
  if (size == NULL) return SALTS_EINVAL;
  desc = flowmq_socket_option_descriptor(option);
  if (out_desc != NULL) *out_desc = desc;
  if (desc == NULL ||
      (desc->access & FLOWMQ_SOCKET_OPTION_ACCESS_GET) == 0u)
    return SALTS_ENOTSUP;
  required = desc->abi_size;
  if (required == 0u) return SALTS_EPROTO;
  if (value == NULL || *size < required) {
    *size = required;
    return value == NULL ? SALTS_EINVAL : SALTS_EMSGSIZE;
  }
  *size = required;
  return SALTS_OK;
}
