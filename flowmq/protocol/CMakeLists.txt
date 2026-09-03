set(TARGET_NAME flowmq_protocol)

set(FLOWMQ_PROTOCOL_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flowmq_export.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flowmq_protocol_catalog.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flowmq_protocol.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flowmq_security.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flowmq_esb.h)
set(FLOWMQ_PROTOCOL_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_protocol.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_protocol_catalog.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_protocol_catalog_schema.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_security.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_protocol_control_schema.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_protocol_internal.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_esb.c)

add_library(${TARGET_NAME} STATIC
            ${FLOWMQ_PROTOCOL_SOURCES}
            ${FLOWMQ_PROTOCOL_HEADERS})

cmake_config_target(
  ${TARGET_NAME}
  ALIAS FlowMQ::Protocol
  FOLDER "flowmq/protocol")
set_target_properties(${TARGET_NAME} PROPERTIES EXCLUDE_FROM_ALL TRUE)

target_include_directories(
  ${TARGET_NAME}
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(${TARGET_NAME} PUBLIC Rocida::Core PRIVATE Rocida::CMeta)

set(FLOWMQ_PROTOCOL_SOURCES ${FLOWMQ_PROTOCOL_SOURCES} PARENT_SCOPE)
set(FLOWMQ_PROTOCOL_HEADERS ${FLOWMQ_PROTOCOL_HEADERS} PARENT_SCOPE)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
