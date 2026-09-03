#ifndef FLOWMQ_STL_ERROR_INTERNAL_H
#define FLOWMQ_STL_ERROR_INTERNAL_H

#include "turbo_error.h"

#include <rocida/stl.h>

static inline int flowmq_stl_error(stl_status status) {
  switch (status) {
  case STL_OK:
    return TURBO_OK;
  case STL_INVALID_ARGUMENT:
    return TURBO_EINVAL;
  case STL_OUT_OF_MEMORY:
    return TURBO_ENOMEM;
  case STL_CAPACITY_EXCEEDED:
    return TURBO_ENOSPC;
  case STL_EMPTY:
  case STL_NOT_FOUND:
    return TURBO_ENOENT;
  case STL_TYPE_MISMATCH:
  case STL_TRAIT_MISSING:
  default:
    return TURBO_EPROTO;
  }
}

#endif /* FLOWMQ_STL_ERROR_INTERNAL_H */
