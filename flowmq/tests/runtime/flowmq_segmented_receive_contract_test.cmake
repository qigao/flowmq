if(NOT DEFINED FLOWMQ_SOCKET_SOURCE OR
   NOT EXISTS "${FLOWMQ_SOCKET_SOURCE}")
  message(FATAL_ERROR "FLOWMQ_SOCKET_SOURCE must name flowmq_socket.c")
endif()

file(READ "${FLOWMQ_SOCKET_SOURCE}" source)

function(extract_function start_marker end_marker out_var)
  string(FIND "${source}" "${start_marker}" begin)
  if(begin EQUAL -1)
    message(FATAL_ERROR "missing start marker: ${start_marker}")
  endif()
  string(FIND "${source}" "${end_marker}" finish)
  if(finish EQUAL -1 OR finish LESS_EQUAL begin)
    message(FATAL_ERROR "missing/invalid end marker: ${end_marker}")
  endif()
  math(EXPR length "${finish} - ${begin}")
  string(SUBSTRING "${source}" ${begin} ${length} body)
  set(${out_var} "${body}" PARENT_SCOPE)
endfunction()

function(require_marker var_name marker label)
  string(FIND "${${var_name}}" "${marker}" pos)
  if(pos EQUAL -1)
    message(FATAL_ERROR "${label}: missing marker: ${marker}")
  endif()
endfunction()

function(forbid_marker var_name marker label)
  string(FIND "${${var_name}}" "${marker}" pos)
  if(NOT pos EQUAL -1)
    message(FATAL_ERROR "${label}: forbidden marker present: ${marker}")
  endif()
endfunction()

function(require_call_count var_name regex expected label)
  string(REGEX MATCHALL "${regex}" matches "${${var_name}}")
  list(LENGTH matches count)
  if(NOT count EQUAL expected)
    message(FATAL_ERROR
            "${label}: expected ${expected} matches for ${regex}, got ${count}")
  endif()
endfunction()

extract_function(
  "static int flowmq_socket_stage_owned_projection("
  "static int flowmq_socket_process_data_frame("
  stage_projection)
require_marker(stage_projection
               "segmented->segments[i] = projection->segments[i];"
               "segmented projection")
require_marker(stage_projection
               "memset(&projection->segments[i], 0"
               "segmented projection ownership move")
forbid_marker(stage_projection "memcpy(" "segmented projection")

extract_function(
  "static int flowmq_socket_process_owned_stream("
  "static void flowmq_socket_on_state("
  owned_stream)
require_marker(owned_stream
               "flowmq_owned_stream_project_first_data("
               "owned stream projection")
require_marker(owned_stream
               "flowmq_socket_process_data_frame("
               "owned stream DATA projection")
require_marker(owned_stream
               "flowmq_owned_stream_replay_prefix("
               "owned stream explicit copied fallback")

extract_function(
  "static int flowmq_socket_message_copy_payload("
  "static int flowmq_socket_message_coalesce("
  message_copy)
require_marker(message_copy
               "memcpy(out + copied, slice->data, slice->length);"
               "segmented recv caller copy")

extract_function(
  "static int flowmq_socket_message_coalesce("
  "static int flowmq_socket_try_recv("
  coalesce)
require_call_count(coalesce
                   "flowmq_socket_message_copy_payload\\("
                   1
                   "legacy slice coalesce")
forbid_marker(coalesce "memcpy(" "legacy slice coalesce")

extract_function(
  "static int flowmq_socket_try_recv("
  "static int flowmq_socket_try_recv_slice("
  recv_copy)
require_call_count(recv_copy
                   "flowmq_socket_message_copy_payload\\("
                   1
                   "flowmq_recv payload path")
forbid_marker(recv_copy "memcpy(" "flowmq_recv payload path")

extract_function(
  "static int flowmq_socket_try_recv_slice("
  "static int flowmq_socket_try_recv_slicev("
  recv_slice)
require_call_count(recv_slice
                   "flowmq_socket_message_coalesce\\("
                   1
                   "flowmq_recv_slice segmented path")
forbid_marker(recv_slice "memcpy(" "flowmq_recv_slice segmented path")

extract_function(
  "static int flowmq_socket_try_recv_slicev("
  "static int flowmq_socket_resume_receive("
  recv_slicev)
require_marker(recv_slicev
               "if (capacity < required)"
               "flowmq_recv_slicev capacity preflight")
require_marker(recv_slicev
               "segments[i] = message->segmented->segments[i];"
               "flowmq_recv_slicev ownership move")
require_marker(recv_slicev
               "flowmq_socket_receive_consume_credit(message, peer);"
               "flowmq_recv_slicev shared credit")
require_marker(recv_slicev
               "flowmq_socket_receive_commit_pattern(socket, message, peer);"
               "flowmq_recv_slicev shared FSM")
forbid_marker(recv_slicev "memcpy(" "flowmq_recv_slicev payload path")
forbid_marker(recv_slicev
              "flowmq_socket_message_coalesce("
              "flowmq_recv_slicev payload path")

message(STATUS
        "segmented receive contract: projection=0 copies, recv=caller copy, recv_slice=N>1 one coalesce, recv_slicev=ownership move")
