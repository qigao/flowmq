cmake_add_executable(
  flowmq_protocol_roundtrip
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/protocol_roundtrip.c
  LIBS FlowMQ::Protocol
  FOLDER "examples")

cmake_add_executable(
  flowmq_segmented_frame
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/segmented_frame.c
  LIBS FlowMQ::Protocol
  FOLDER "examples")

cmake_add_executable(
  flowmq_security_hello
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/security_hello.c
  LIBS FlowMQ::Protocol
  FOLDER "examples")

cmake_add_executable(
  flowmq_multicore_reqrep
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/multicore_reqrep.c
  LIBS FlowMQ::Transport
  FOLDER "examples")

cmake_add_executable(flowmq_mdp_echo
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/mdp_echo.c
  LIBS FlowMQ::RPC FOLDER "examples")

cmake_add_executable(flowmq_rpc_quote
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/rpc_quote.c
  LIBS FlowMQ::RPC CMeta::CFlow FOLDER "examples")

if(BUILD_TESTS)
  add_test(NAME test_flowmq_rpc_quote COMMAND flowmq_rpc_quote)
  add_test(NAME test_flowmq_rpc_quote_concurrent COMMAND flowmq_rpc_quote --concurrent)
  set_tests_properties(test_flowmq_rpc_quote test_flowmq_rpc_quote_concurrent
    PROPERTIES LABELS "flowmq;flowmq-rpc;example"
    TIMEOUT 15 ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  set(mdp_tcp "tcp://127.0.0.1:0")
  set(mdp_ws "ws://127.0.0.1:0/fmq")
  set(mdp_kcp "kcp://127.0.0.1:0?conv=42")
  add_test(NAME test_flowmq_rpc_echo COMMAND flowmq_mdp_echo --rpc hello "${mdp_ws}" "${mdp_kcp}")
  set_tests_properties(test_flowmq_rpc_echo PROPERTIES LABELS "flowmq;flowmq-rpc;example"
    TIMEOUT 15
    ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  foreach(front IN ITEMS tcp ws kcp)
    foreach(back IN ITEMS tcp ws kcp)
      if(front STREQUAL "tcp" AND back STREQUAL "tcp")
        set(example_test test_flowmq_mdp_echo)
        add_test(NAME ${example_test} COMMAND flowmq_mdp_echo hello)
      else()
        set(example_test test_flowmq_mdp_echo_${front}_${back})
        add_test(NAME ${example_test} COMMAND flowmq_mdp_echo hello "${mdp_${front}}" "${mdp_${back}}")
      endif()
      set_tests_properties(${example_test} PROPERTIES
        LABELS "flowmq;flowmq-mdp;example" TIMEOUT 15
        ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
    endforeach()
  endforeach()
  foreach(workers IN ITEMS 1 4 16)
    set(example_test test_flowmq_multicore_reqrep_${workers})
    add_test(NAME ${example_test} COMMAND flowmq_multicore_reqrep ${workers} 100)
    set_tests_properties(${example_test} PROPERTIES
                         LABELS "flowmq;flowmq-transport;example" TIMEOUT 60)
    if(WIN32)
      set_tests_properties(${example_test} PROPERTIES ENVIRONMENT_MODIFICATION
                           "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
    endif()
  endforeach()
endif()
