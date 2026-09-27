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


set(FLOWMQ_SOCKET_SOURCE
    "${FLOWMQ_RUNTIME_ROOT}/flowmq_socket.c")
file(READ "${FLOWMQ_SOCKET_SOURCE}" _flowmq_socket_text)

foreach(_flowmq_sg_required IN ITEMS
        "use_retained_sg = socket->transport == FLOWMQ_TRANSPORT_TCP;"
        "status = cnet_send_slicev(&socket->client, peer->connection, slices,"
        "status = cnet_sendv(&socket->client, peer->connection,")
  string(FIND "${_flowmq_socket_text}"
              "${_flowmq_sg_required}"
              _flowmq_sg_required_index)
  if(_flowmq_sg_required_index EQUAL -1)
    message(FATAL_ERROR
      "FlowMQ CNet SG contract is missing required runtime fragment: "
      "${_flowmq_sg_required}")
  endif()
endforeach()

string(FIND "${_flowmq_socket_text}"
            "status == SALTS_ENOTSUP"
            _flowmq_sg_fallback_index)
if(NOT _flowmq_sg_fallback_index EQUAL -1)
  message(FATAL_ERROR
    "FlowMQ runtime must not retry a retained-SG SALTS_ENOTSUP through a copy fallback")
endif()

message(STATUS
        "FlowMQ CNet SG contract verified: TCP retained SG, explicit TLS copy path, no ENOTSUP fallback")
