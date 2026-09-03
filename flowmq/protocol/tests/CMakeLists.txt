cmake_add_test(
  test_flowmq_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_protocol.c
  LIBS FlowMQ::Protocol Rocida::TinyTest
  FOLDER "flowmq/protocol/tests")
target_include_directories(test_flowmq_protocol PRIVATE
                           ${CMAKE_CURRENT_SOURCE_DIR}/../src)

set_tests_properties(test_flowmq_protocol PROPERTIES LABELS "fmq-release;flowmq-protocol")

cmake_add_test(
  test_flowmq_esb
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_esb.c
  LIBS FlowMQ::Protocol Rocida::TinyTest
  FOLDER "flowmq/protocol/tests")

set_tests_properties(test_flowmq_esb PROPERTIES LABELS "fmq-release;flowmq-protocol;flowmq-esb")

if(WIN32)
  set_tests_properties(
    test_flowmq_protocol test_flowmq_esb
    PROPERTIES ENVIRONMENT_MODIFICATION
               "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
endif()
