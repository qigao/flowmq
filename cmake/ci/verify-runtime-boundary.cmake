cmake_minimum_required(VERSION 3.20)

get_filename_component(FLOWMQ_SOURCE_ROOT
                       "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)
set(FLOWMQ_RUNTIME_ROOT "${FLOWMQ_SOURCE_ROOT}/flowmq/src/runtime")

if(NOT IS_DIRECTORY "${FLOWMQ_RUNTIME_ROOT}")
  message(FATAL_ERROR
          "FlowMQ runtime source directory is missing: ${FLOWMQ_RUNTIME_ROOT}")
endif()

file(GLOB FLOWMQ_RUNTIME_BOUNDARY_FILES
     LIST_DIRECTORIES FALSE
     "${FLOWMQ_RUNTIME_ROOT}/*.c"
     "${FLOWMQ_RUNTIME_ROOT}/*.h")
list(SORT FLOWMQ_RUNTIME_BOUNDARY_FILES)

if(NOT FLOWMQ_RUNTIME_BOUNDARY_FILES)
  message(FATAL_ERROR "FlowMQ runtime boundary gate found no runtime sources")
endif()

set(FLOWMQ_RUNTIME_FORBIDDEN_TOKENS
    "cflow/"
    "cflow_"
    "FunctionMeta"
    "FunctionAbi"
    "cmeta_function_desc"
    "cmeta_function_abi_desc"
    "cmeta_callable")

foreach(_flowmq_runtime_file IN LISTS FLOWMQ_RUNTIME_BOUNDARY_FILES)
  file(READ "${_flowmq_runtime_file}" _flowmq_runtime_text)
  foreach(_flowmq_forbidden IN LISTS FLOWMQ_RUNTIME_FORBIDDEN_TOKENS)
    string(FIND "${_flowmq_runtime_text}"
                "${_flowmq_forbidden}"
                _flowmq_forbidden_index)
    if(NOT _flowmq_forbidden_index EQUAL -1)
      file(RELATIVE_PATH _flowmq_runtime_rel
           "${FLOWMQ_SOURCE_ROOT}" "${_flowmq_runtime_file}")
      message(FATAL_ERROR
        "FlowMQ runtime boundary violation in ${_flowmq_runtime_rel}: "
        "forbidden execution/reflection token '${_flowmq_forbidden}'. "
        "CMeta Schema/Replay descriptors remain allowed, but CFlow execution "
        "and reflected FunctionMeta/FunctionAbi/callable machinery are "
        "control/test-plane only.")
    endif()
  endforeach()
endforeach()

message(STATUS
        "FlowMQ runtime boundary verified: no CFlow execution or reflected-function machinery")
