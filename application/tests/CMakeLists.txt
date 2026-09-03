cmake_add_test(
  test_flowmq_media_provider_schema
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_media_provider_schema.c
          ${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}
  LIBS FlowMQ::FlowMQ Rocida::TinyTest
  INCLUDES ${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}
  FOLDER "application/tests")

if(TARGET ${FLOWMQ_MEDIA_PROVIDER_CODEGEN_TARGET})
  add_dependencies(test_flowmq_media_provider_schema
                   ${FLOWMQ_MEDIA_PROVIDER_CODEGEN_TARGET})
endif()

set_tests_properties(
  test_flowmq_media_provider_schema
  PROPERTIES LABELS "flowmq;flowmq-application")

if(WIN32)
  set_tests_properties(
    test_flowmq_media_provider_schema
    PROPERTIES ENVIRONMENT_MODIFICATION
               "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
endif()
