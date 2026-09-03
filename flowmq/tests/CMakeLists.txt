cmake_add_test(
  test_flowmq_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/protocol/test_flowmq_protocol.c
  LIBS FlowMQ::Protocol Rocida::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/protocol
  FOLDER "flowmq/tests/protocol")

cmake_add_test(
  test_flowmq_esb
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/protocol/test_flowmq_esb.c
  LIBS FlowMQ::Protocol Rocida::TinyTest
  FOLDER "flowmq/tests/protocol")

cmake_add_test(
  test_flowmq_stream_decoder
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/protocol/test_flowmq_stream_decoder.c
  LIBS FlowMQ::Protocol Rocida::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/protocol/stream
  FOLDER "flowmq/tests/protocol")

cmake_add_test(
  test_flowmq_core
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/core/test_flowmq_core.c
  LIBS FlowMQ::Core Rocida::TinyTest
  FOLDER "flowmq/tests/core")

foreach(test_name IN ITEMS pattern pattern_state flow_control reconnect subscription_set)
  cmake_add_test(
    test_flowmq_${test_name}
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/core/test_flowmq_${test_name}.c
    LIBS FlowMQ::Core Rocida::TinyTest
    INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/core/pattern
             ${PROJECT_SOURCE_DIR}/flowmq/src/core/session
    FOLDER "flowmq/tests/core")
endforeach()

cmake_add_test(
  test_flowmq_tls_identity_map
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/security/test_flowmq_tls_identity_map.c
  LIBS FlowMQ::Core Rocida::TinyTest
  FOLDER "flowmq/tests/security")

cmake_add_test(
  test_flowmq_transport
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_flowmq_transport.c
  LIBS FlowMQ::Transport Rocida::TinyTest
  FOLDER "flowmq/tests/transport")

cmake_add_test(
  test_flowmq_socket
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_socket.c
  LIBS FlowMQ::Transport Rocida::TinyTest
  FOLDER "flowmq/tests/runtime")

cmake_add_test(
  test_flowmq_media_provider_schema
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/media_provider/test_flowmq_media_provider_schema.c
          ${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}
  LIBS FlowMQ::FlowMQ Rocida::TinyTest
  INCLUDES ${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}
  FOLDER "flowmq/tests/media_provider")

if(TARGET ${FLOWMQ_MEDIA_PROVIDER_CODEGEN_TARGET})
  add_dependencies(test_flowmq_media_provider_schema
                   ${FLOWMQ_MEDIA_PROVIDER_CODEGEN_TARGET})
endif()

set(FLOWMQ_PROTOCOL_TESTS
    test_flowmq_protocol
    test_flowmq_esb
    test_flowmq_stream_decoder)
set(FLOWMQ_CORE_TESTS
    test_flowmq_core
    test_flowmq_pattern
    test_flowmq_pattern_state
    test_flowmq_flow_control
    test_flowmq_reconnect
    test_flowmq_subscription_set
    test_flowmq_tls_identity_map)
set(FLOWMQ_TRANSPORT_TESTS
    test_flowmq_transport
    test_flowmq_socket)

set_tests_properties(test_flowmq_protocol
                     PROPERTIES LABELS "fmq-release;flowmq-protocol")
set_tests_properties(test_flowmq_esb
                     PROPERTIES LABELS "fmq-release;flowmq-protocol;flowmq-esb")
set_tests_properties(test_flowmq_stream_decoder
                     PROPERTIES LABELS "flowmq;flowmq-protocol")
set_tests_properties(${FLOWMQ_CORE_TESTS}
                     PROPERTIES LABELS "flowmq;flowmq-core")
set_tests_properties(${FLOWMQ_TRANSPORT_TESTS}
                     PROPERTIES LABELS "flowmq;flowmq-transport")
set_tests_properties(test_flowmq_media_provider_schema
                     PROPERTIES LABELS "flowmq;flowmq-application")

if(WIN32)
  set_tests_properties(
    ${FLOWMQ_PROTOCOL_TESTS}
    ${FLOWMQ_CORE_TESTS}
    ${FLOWMQ_TRANSPORT_TESTS}
    test_flowmq_media_provider_schema
    PROPERTIES ENVIRONMENT_MODIFICATION
               "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
endif()
