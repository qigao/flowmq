if(NOT DEFINED FLOWMQ_SALTS_IDLC_EXECUTABLE OR
   NOT DEFINED FLOWMQ_DATABIND_SCHEMA OR
   NOT DEFINED FLOWMQ_DATABIND_OUTPUT OR
   NOT DEFINED FLOWMQ_SALTS_HOST_ROOT OR
   NOT DEFINED FLOWMQ_SALTS_UTILS_HOST_ROOT)
  message(FATAL_ERROR "FlowMQ DataBind codegen wrapper is missing required inputs")
endif()

if(WIN32)
  set(ENV{PATH}
      "${FLOWMQ_SALTS_UTILS_HOST_ROOT}/bin;${FLOWMQ_SALTS_HOST_ROOT}/bin;$ENV{PATH}")
elseif(APPLE)
  set(ENV{PATH}
      "${FLOWMQ_SALTS_UTILS_HOST_ROOT}/bin:${FLOWMQ_SALTS_HOST_ROOT}/bin:$ENV{PATH}")
  set(ENV{DYLD_LIBRARY_PATH}
      "${FLOWMQ_SALTS_UTILS_HOST_ROOT}/lib:${FLOWMQ_SALTS_HOST_ROOT}/lib:$ENV{DYLD_LIBRARY_PATH}")
else()
  set(ENV{PATH}
      "${FLOWMQ_SALTS_UTILS_HOST_ROOT}/bin:${FLOWMQ_SALTS_HOST_ROOT}/bin:$ENV{PATH}")
  set(ENV{LD_LIBRARY_PATH}
      "${FLOWMQ_SALTS_UTILS_HOST_ROOT}/lib:${FLOWMQ_SALTS_HOST_ROOT}/lib:$ENV{LD_LIBRARY_PATH}")
endif()

execute_process(
  COMMAND "${FLOWMQ_SALTS_IDLC_EXECUTABLE}"
          "${FLOWMQ_DATABIND_SCHEMA}"
          --lang c
          --binary-codec
          --output "${FLOWMQ_DATABIND_OUTPUT}"
  RESULT_VARIABLE _flowmq_databind_status
  OUTPUT_VARIABLE _flowmq_databind_stdout
  ERROR_VARIABLE _flowmq_databind_stderr)
if(NOT _flowmq_databind_status EQUAL 0)
  message(FATAL_ERROR
          "FlowMQ binary FMP/1 salts-idlc failed with status ${_flowmq_databind_status}\n"
          "stdout:\n${_flowmq_databind_stdout}\n"
          "stderr:\n${_flowmq_databind_stderr}")
endif()
