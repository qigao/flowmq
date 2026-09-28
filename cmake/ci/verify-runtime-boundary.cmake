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
set(FLOWMQ_SOCKET_HEADER
    "${FLOWMQ_SOURCE_ROOT}/flowmq/include/flowmq_socket.h")
file(READ "${FLOWMQ_SOCKET_SOURCE}" _flowmq_socket_text)
file(READ "${FLOWMQ_SOCKET_HEADER}" _flowmq_socket_header_text)

foreach(_flowmq_sg_required IN ITEMS
        "use_retained_sg = socket->transport == FLOWMQ_TRANSPORT_TCP;"
        "mem_slice_t slices[CNET_RETAINED_VECTOR_MAX] = {0};"
        "batch_count == CNET_RETAINED_VECTOR_MAX"
        "status = cnet_send_slicev(&socket->client, peer->connection, slices,"
        "status = cnet_sendv(&socket->client, peer->connection,"
        "socket_options.nodelay = 1;"
        "cnet_client_set_stream_socket_options(&socket->client, &socket_options)"
        "flowmq_socket_prepare_retained_frame("
        "flowmq_socket_peer_admit_retained("
        "flowmq_socket_peer_admit_retained_message("
        "flowmq_socket_stage_retained_frame("
        "flowmq_socket_release_retained_staged("
        "mem_slice_t send_retained_staged[CNET_RETAINED_VECTOR_MAX];"
        "CNET_RETAINED_VECTOR_MAX - socket->send_retained_count"
        "if (socket->send_retained_count != 0u) return SALTS_ENOTSUP;"
        "if (socket->send_staged_count != 0u) return SALTS_ENOTSUP;"
        "status = cnet_send_slicev(&socket->client, peer->connection,"
        "socket->transport != FLOWMQ_TRANSPORT_TCP")
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
            "FLOWMQ_PATTERN_ROUTE_FANOUT &&"
            _flowmq_retained_fanout_guard_index)
if(_flowmq_retained_fanout_guard_index EQUAL -1)
  message(FATAL_ERROR
    "FlowMQ retained multipart must keep fanout fail-closed until it has an atomic multi-peer ownership design")
endif()

string(FIND "${_flowmq_socket_text}"
            "NATIVE_IO_VECTOR_MAX"
            _flowmq_native_vector_index)
if(NOT _flowmq_native_vector_index EQUAL -1)
  message(FATAL_ERROR
    "FlowMQ runtime must depend on CNet logical retained-vector bounds, not NativeIO physical vector windows")
endif()

string(FIND "${_flowmq_socket_text}"
            "status == SALTS_ENOTSUP"
            _flowmq_sg_fallback_index)
if(NOT _flowmq_sg_fallback_index EQUAL -1)
  message(FATAL_ERROR
    "FlowMQ runtime must not retry a retained-SG SALTS_ENOTSUP through a copy fallback")
endif()

string(FIND "${_flowmq_socket_header_text}"
            "FLOWMQ_C_API int flowmq_send_slice("
            _flowmq_retained_api_index)
if(_flowmq_retained_api_index EQUAL -1)
  message(FATAL_ERROR
    "FlowMQ public API must expose explicit retained flowmq_send_slice()")
endif()

string(FIND "${_flowmq_socket_header_text}"
            "FLOWMQ_C_API int flowmq_recv_slice("
            _flowmq_owned_recv_api_index)
if(_flowmq_owned_recv_api_index EQUAL -1)
  message(FATAL_ERROR
    "FlowMQ public API must expose explicit owned flowmq_recv_slice()")
endif()

foreach(_flowmq_owned_recv_required IN ITEMS
        "static int flowmq_socket_try_recv_slice("
        "out->buffer != NULL || out->data != NULL || out->length != 0u"
        "out->buffer = message->buffer;"
        "message->buffer = NULL;"
        "status = flowmq_socket_try_recv_slice(socket, out, flags);")
  string(FIND "${_flowmq_socket_text}"
              "${_flowmq_owned_recv_required}"
              _flowmq_owned_recv_required_index)
  if(_flowmq_owned_recv_required_index EQUAL -1)
    message(FATAL_ERROR
      "FlowMQ owned receive contract is missing required ownership-transfer fragment: "
      "${_flowmq_owned_recv_required}")
  endif()
endforeach()

message(STATUS
        "FlowMQ CNet SG / owned receive contract verified: bounded retained multipart, no copy mixing, TCP_NODELAY policy, explicit TLS copy path, direct inbound owner transfer, no fallback")
