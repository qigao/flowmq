add_subdirectory(protocol)

set(FLOWMQ_CORE_SOURCES
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_pattern.c
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_pattern.h
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_reconnect.c
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_reconnect.h
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_subscription_set.c
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_subscription_set.h
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_peer_session.c
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_peer_session.h
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_stream_decoder.c
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_stream_decoder.h
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_scatter_gather.c
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_scatter_gather.h
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_circuit_breaker.c
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_circuit_breaker.h
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_stream_partition.c
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_stream_partition.h
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_saga.c
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_saga.h
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_tls_identity_map.c)

set(FLOWMQ_CORE_PUBLIC_SOURCES
    ${PROJECT_SOURCE_DIR}/patterns/src/flowmq_core.c)

add_library(flowmq_core STATIC
            ${FLOWMQ_CORE_SOURCES}
            ${FLOWMQ_CORE_PUBLIC_SOURCES}
            ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_core.h
            ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_tls_identity_map.h)
cmake_config_target(
  flowmq_core
  ALIAS FlowMQ::Core
  FOLDER "flowmq/core")
set_target_properties(flowmq_core PROPERTIES EXCLUDE_FROM_ALL TRUE)
target_include_directories(
  flowmq_core
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/flowmq/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${PROJECT_SOURCE_DIR}/patterns/src)
target_link_libraries(flowmq_core
                      PUBLIC FlowMQ::Protocol TurboUtils::Core
                      PRIVATE TurboUtils::STL)

if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif() 
set(FLOWMQ_PROTOCOL_SOURCES ${FLOWMQ_PROTOCOL_SOURCES} PARENT_SCOPE)
set(FLOWMQ_PROTOCOL_HEADERS ${FLOWMQ_PROTOCOL_HEADERS} PARENT_SCOPE)
set(FLOWMQ_CORE_SOURCES ${FLOWMQ_CORE_SOURCES} PARENT_SCOPE)
set(FLOWMQ_TRANSPORT_SOURCES ${FLOWMQ_TRANSPORT_SOURCES} PARENT_SCOPE)
set(FLOWMQ_RUNTIME_PUBLIC_HEADERS ${FLOWMQ_RUNTIME_PUBLIC_HEADERS} PARENT_SCOPE)
