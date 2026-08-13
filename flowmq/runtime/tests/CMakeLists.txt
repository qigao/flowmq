cmake_add_test(
  test_flowmq_core
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_core.c
  LIBS FlowMQ::Core TurboUtils::TinyTest
  FOLDER "flowmq/core/tests")

cmake_add_test(
  test_flowmq_tls_identity_map
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_tls_identity_map.c
  LIBS FlowMQ::Core TurboUtils::TinyTest
  FOLDER "flowmq/core/tests")

foreach(test_name IN ITEMS pattern reconnect peer_session subscription_set stream_decoder)
  cmake_add_test(
    test_flowmq_${test_name}
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_${test_name}.c
    LIBS flowmq_core_test_support TurboUtils::TinyTest
    FOLDER "flowmq/core/tests")
endforeach()

cmake_add_test(
  test_flowmq_coronet_transport
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_coronet_transport.c
  LIBS FlowMQ::Transport TurboUtils::TinyTest
  FOLDER "flowmq/transport/tests")

cmake_add_test(
  test_flowmq_connect_endpoint
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_connect_endpoint.c
  LIBS FlowMQ::Transport TurboNet::CoroNet TurboUtils::TinyTest
  FOLDER "flowmq/transport/tests")

cmake_add_test(
  test_flowmq_router_endpoint
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_router_endpoint.c
  LIBS FlowMQ::Transport TurboNet::CoroNet TurboUtils::TinyTest
  FOLDER "flowmq/transport/tests")

set(FLOWMQ_CORE_TESTS
    test_flowmq_core
    test_flowmq_pattern
    test_flowmq_reconnect
    test_flowmq_peer_session
    test_flowmq_subscription_set
    test_flowmq_stream_decoder
    test_flowmq_tls_identity_map)
set(FLOWMQ_CORONET_TESTS
    test_flowmq_coronet_transport
    test_flowmq_connect_endpoint
    test_flowmq_router_endpoint)
set_tests_properties(${FLOWMQ_CORE_TESTS}
                     PROPERTIES LABELS "flowmq;flowmq-core")
set_tests_properties(${FLOWMQ_CORONET_TESTS}
                     PROPERTIES LABELS "flowmq;flowmq-transport")

if(WIN32)
  set_tests_properties(
    ${FLOWMQ_CORE_TESTS} ${FLOWMQ_CORONET_TESTS}
    PROPERTIES ENVIRONMENT_MODIFICATION
               "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
endif()
