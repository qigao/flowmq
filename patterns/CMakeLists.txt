
add_library(
  flowmq_transport STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_transport.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_cnet_transport.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_cnet_transport.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_socket.c
  ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_transport.h)
cmake_config_target(
  flowmq_transport
  ALIAS FlowMQ::Transport
  FOLDER "flowmq/transport")
set_target_properties(flowmq_transport PROPERTIES EXCLUDE_FROM_ALL TRUE)
target_include_directories(
  flowmq_transport
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/flowmq/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
          ${PROJECT_SOURCE_DIR}/flowmq/protocol/src)
target_link_libraries(flowmq_transport
                      PUBLIC FlowMQ::Core Rocida::Core
                      PRIVATE Rocida::CNet)

set(FLOWMQ_TRANSPORT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_transport.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_cnet_transport.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_socket.c
    PARENT_SCOPE)
set(FLOWMQ_RUNTIME_PUBLIC_HEADERS
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_core.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_socket.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_transport.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_tls_identity_map.h
    PARENT_SCOPE)

add_subdirectory(benchmarks)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
