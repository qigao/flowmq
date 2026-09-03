set(FLOWMQ_PROTOCOL_HEADERS
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_export.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_protocol_catalog.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_protocol.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_security.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_esb.h)

set(FLOWMQ_PROTOCOL_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/flowmq_protocol.c
    ${CMAKE_CURRENT_SOURCE_DIR}/flowmq_protocol_catalog.c
    ${CMAKE_CURRENT_SOURCE_DIR}/flowmq_protocol_catalog_schema.h
    ${CMAKE_CURRENT_SOURCE_DIR}/flowmq_security.c
    ${CMAKE_CURRENT_SOURCE_DIR}/flowmq_protocol_control_schema.h
    ${CMAKE_CURRENT_SOURCE_DIR}/flowmq_protocol_internal.h
    ${CMAKE_CURRENT_SOURCE_DIR}/flowmq_esb.c
    ${CMAKE_CURRENT_SOURCE_DIR}/stream/flowmq_stream_decoder.c
    ${CMAKE_CURRENT_SOURCE_DIR}/stream/flowmq_stream_decoder.h)

add_library(flowmq_protocol STATIC
            ${FLOWMQ_PROTOCOL_SOURCES}
            ${FLOWMQ_PROTOCOL_HEADERS})
cmake_config_target(
  flowmq_protocol
  ALIAS FlowMQ::Protocol
  FOLDER "flowmq/protocol")
set_target_properties(flowmq_protocol PROPERTIES EXCLUDE_FROM_ALL TRUE)

target_include_directories(
  flowmq_protocol
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/flowmq/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
          ${CMAKE_CURRENT_SOURCE_DIR}/stream)
target_link_libraries(flowmq_protocol
                      PUBLIC Salts::Core
                      PRIVATE Salts::CMeta)

set(FLOWMQ_PROTOCOL_SOURCES ${FLOWMQ_PROTOCOL_SOURCES} PARENT_SCOPE)
set(FLOWMQ_PROTOCOL_HEADERS ${FLOWMQ_PROTOCOL_HEADERS} PARENT_SCOPE)
