cmake_add_test(
  test_flowmq_media_provider_schema
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_media_provider_schema.c
  LIBS FlowMQ::FlowMQ TurboUtils::TinyTest
  FOLDER "flowmq/application/tests")

set_tests_properties(
  test_flowmq_media_provider_schema
  PROPERTIES LABELS "flowmq;flowmq-application")

if(WIN32)
  set_tests_properties(
    test_flowmq_media_provider_schema
    PROPERTIES ENVIRONMENT_MODIFICATION
               "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
endif()
