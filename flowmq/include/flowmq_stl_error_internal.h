#ifndef FLOWMQ_STL_ERROR_INTERNAL_H
#define FLOWMQ_STL_ERROR_INTERNAL_H

#include "salts_error.h"

#include <cstl.h>

static inline int flowmq_stl_error(stl_status status) {
  switch (status) {
  case STL_OK:
    return SALTS_OK;
  case STL_INVALID_ARGUMENT:
    return SALTS_EINVAL;
  case STL_OUT_OF_MEMORY:
    return SALTS_ENOMEM;
  case STL_CAPACITY_EXCEEDED:
    return SALTS_ENOSPC;
  case STL_EMPTY:
  case STL_NOT_FOUND:
    return SALTS_ENOENT;
  case STL_TYPE_MISMATCH:
  case STL_TRAIT_MISSING:
  default:
    return SALTS_EPROTO;
  }
}

#endif /* FLOWMQ_STL_ERROR_INTERNAL_H */
