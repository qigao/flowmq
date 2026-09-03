set(FLOWMQ_TRANSPORT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/flowmq_transport.c
    ${CMAKE_CURRENT_SOURCE_DIR}/cnet/flowmq_cnet_transport.c
    ${CMAKE_CURRENT_SOURCE_DIR}/cnet/flowmq_cnet_transport.h
    ${PROJECT_SOURCE_DIR}/flowmq/src/runtime/flowmq_socket.c)

set(FLOWMQ_TRANSPORT_HEADERS
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_socket.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_transport.h)

add_library(flowmq_transport STATIC
            ${FLOWMQ_TRANSPORT_SOURCES}
            ${FLOWMQ_TRANSPORT_HEADERS})
cmake_config_target(
  flowmq_transport
  ALIAS FlowMQ::Transport
  FOLDER "flowmq/transport")
set_target_properties(flowmq_transport PROPERTIES EXCLUDE_FROM_ALL TRUE)

target_include_directories(
  flowmq_transport
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/flowmq/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cnet
          ${PROJECT_SOURCE_DIR}/flowmq/src/core/pattern
          ${PROJECT_SOURCE_DIR}/flowmq/src/core/session
          ${PROJECT_SOURCE_DIR}/flowmq/src/protocol
          ${PROJECT_SOURCE_DIR}/flowmq/src/protocol/stream)
target_link_libraries(flowmq_transport
                      PUBLIC FlowMQ::Core Salts::Core
                      PRIVATE Salts::CNet)

set(FLOWMQ_TRANSPORT_SOURCES ${FLOWMQ_TRANSPORT_SOURCES} PARENT_SCOPE)
set(FLOWMQ_TRANSPORT_HEADERS ${FLOWMQ_TRANSPORT_HEADERS} PARENT_SCOPE)
