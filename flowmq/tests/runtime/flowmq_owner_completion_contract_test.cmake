# Source-level regression guard only; the real NativeIO multi-socket runtime
# test remains test_flowmq_owner. Fault-injected terminal proof is #119's
# separate acceptance gate, not established by this contract check.
if(NOT DEFINED FLOWMQ_OWNER_SOURCE OR NOT EXISTS "${FLOWMQ_OWNER_SOURCE}")
  message(FATAL_ERROR "FlowMQ owner source is required")
endif()
file(READ "${FLOWMQ_OWNER_SOURCE}" source)

string(FIND "${source}" "  if (status == SALTS_OK) {\n    /* Once observe returns a batch" observe_start)
string(FIND "${source}" "  /*\n   * Match the qualified #67 ordering:" progress_start)
if(observe_start LESS 0 OR progress_start LESS_EQUAL observe_start)
  message(FATAL_ERROR "Owner observe/progress boundary changed; requalify routing")
endif()
math(EXPR batch_size "${progress_start} - ${observe_start}")
string(SUBSTRING "${source}" ${observe_start} ${batch_size} batch)

foreach(marker IN ITEMS
  "bool route_failed = false;"
  "if (first_error == SALTS_OK) first_error = status;"
  "route_failed = true;"
  "if (!consumed && !route_failed && first_error == SALTS_OK)"
  "first_error = SALTS_EPROTO;")
  string(FIND "${batch}" "${marker}" marker_offset)
  if(marker_offset LESS 0)
    message(FATAL_ERROR "Observed batch fails required settlement marker: ${marker}")
  endif()
endforeach()

string(FIND "${batch}" "return status;" early_status)
string(FIND "${batch}" "return SALTS_EPROTO;" early_unclaimed)
if(NOT early_status LESS 0 OR NOT early_unclaimed LESS 0)
  message(FATAL_ERROR "Observed completion loop returned before routing whole batch")
endif()

string(FIND "${source}" "return first_error;" deferred_error)
if(deferred_error LESS progress_start)
  message(FATAL_ERROR "Owner does not return deferred first error after local progress")
endif()
