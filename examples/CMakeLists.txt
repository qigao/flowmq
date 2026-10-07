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

if(BUILD_TESTS)
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
