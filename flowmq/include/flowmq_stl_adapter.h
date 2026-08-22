#ifndef FLOWMQ_STL_ADAPTER_H
#define FLOWMQ_STL_ADAPTER_H

#include "turbo_error.h"

#include <turbostl/status.h>

static inline int flowmq_stl_status_to_error(turbo_stl_status status) {
  switch (status) {
  case TURBO_STL_OK:
    return TURBO_OK;
  case TURBO_STL_INVALID_ARGUMENT:
    return TURBO_EINVAL;
  case TURBO_STL_OUT_OF_MEMORY:
    return TURBO_ENOMEM;
  case TURBO_STL_CAPACITY_EXCEEDED:
    return TURBO_ENOSPC;
  case TURBO_STL_EMPTY:
  case TURBO_STL_NOT_FOUND:
    return TURBO_ENOENT;
  case TURBO_STL_TYPE_MISMATCH:
  case TURBO_STL_TRAIT_MISSING:
  default:
    return TURBO_EPROTO;
  }
}

#endif /* FLOWMQ_STL_ADAPTER_H */
