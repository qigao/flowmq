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
  flowmq_router_dealer
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/router_dealer.c
  LIBS FlowMQ::Transport
  FOLDER "examples")

if(WIN32)
  target_link_libraries(flowmq_router_dealer PRIVATE ws2_32)
endif()
