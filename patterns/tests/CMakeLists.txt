cmake_add_test(
  test_flowmq_core
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_core.c
  LIBS FlowMQ::Core Rocida::TinyTest
  FOLDER "flowmq/transport/tests")

cmake_add_test(
  test_flowmq_tls_identity_map
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_tls_identity_map.c
  LIBS FlowMQ::Core Rocida::TinyTest
  FOLDER "flowmq/transport/tests")

foreach(test_name IN ITEMS pattern pattern_state flow_control reconnect subscription_set stream_decoder scatter_gather stream_partition saga circuit_breaker)
  cmake_add_test(
    test_flowmq_${test_name}
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_${test_name}.c
    LIBS FlowMQ::Core Rocida::TinyTest
    INCLUDES ${PROJECT_SOURCE_DIR}/patterns/src
    FOLDER "flowmq/transport/tests")
endforeach()

cmake_add_test(
  test_flowmq_transport
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_transport.c
  LIBS FlowMQ::Transport Rocida::TinyTest
  FOLDER "flowmq/transport/tests")

cmake_add_test(
  test_flowmq_socket
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_socket.c
  LIBS FlowMQ::Transport Rocida::TinyTest
  FOLDER "flowmq/transport/tests")

set(FLOWMQ_CORE_TESTS
    test_flowmq_core
    test_flowmq_pattern
    test_flowmq_pattern_state
    test_flowmq_flow_control
    test_flowmq_reconnect
    test_flowmq_subscription_set
    test_flowmq_stream_decoder
    test_flowmq_scatter_gather
    test_flowmq_stream_partition
    test_flowmq_circuit_breaker
    test_flowmq_saga
    test_flowmq_tls_identity_map)
set(FLOWMQ_CNET_TESTS
    test_flowmq_transport
    test_flowmq_socket)
set_tests_properties(${FLOWMQ_CORE_TESTS}
                     PROPERTIES LABELS "flowmq;flowmq-core")
set_tests_properties(${FLOWMQ_CNET_TESTS}
                     PROPERTIES LABELS "flowmq;flowmq-transport")

if(WIN32)
  set_tests_properties(
    ${FLOWMQ_CORE_TESTS} ${FLOWMQ_CNET_TESTS}
    PROPERTIES ENVIRONMENT_MODIFICATION
               "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
endif()
