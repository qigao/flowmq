set(FLOWMQ_CORE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/flowmq_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/pattern/flowmq_pattern.c
    ${CMAKE_CURRENT_SOURCE_DIR}/pattern/flowmq_pattern.h
    ${CMAKE_CURRENT_SOURCE_DIR}/pattern/flowmq_pattern_state.c
    ${CMAKE_CURRENT_SOURCE_DIR}/pattern/flowmq_pattern_state.h
    ${CMAKE_CURRENT_SOURCE_DIR}/pattern/flowmq_subscription_set.c
    ${CMAKE_CURRENT_SOURCE_DIR}/pattern/flowmq_subscription_set.h
    ${CMAKE_CURRENT_SOURCE_DIR}/session/flowmq_flow_control.c
    ${CMAKE_CURRENT_SOURCE_DIR}/session/flowmq_flow_control.h
    ${CMAKE_CURRENT_SOURCE_DIR}/session/flowmq_reconnect.c
    ${CMAKE_CURRENT_SOURCE_DIR}/session/flowmq_reconnect.h
    ${PROJECT_SOURCE_DIR}/flowmq/src/security/flowmq_tls_identity_map.c)

set(FLOWMQ_CORE_HEADERS
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_core.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_tls_identity_map.h)

add_library(flowmq_core STATIC ${FLOWMQ_CORE_SOURCES} ${FLOWMQ_CORE_HEADERS})
cmake_config_target(flowmq_core ALIAS FlowMQ::Core FOLDER "flowmq/core")
set_target_properties(flowmq_core PROPERTIES EXCLUDE_FROM_ALL TRUE)

target_include_directories(
  flowmq_core
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/flowmq/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/pattern
          ${CMAKE_CURRENT_SOURCE_DIR}/session)
target_link_libraries(
  flowmq_core
  PUBLIC FlowMQ::Protocol Rocida::Core
  PRIVATE Rocida::STL)

set(FLOWMQ_CORE_SOURCES ${FLOWMQ_CORE_SOURCES} PARENT_SCOPE)
set(FLOWMQ_CORE_HEADERS ${FLOWMQ_CORE_HEADERS} PARENT_SCOPE)
