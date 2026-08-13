set(FLOWMQ_CORE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_pattern.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_pattern.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_reconnect.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_reconnect.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_subscription_set.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_subscription_set.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_peer_session.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_peer_session.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_stream_decoder.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_stream_decoder.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_tls_identity_map.c)

set(FLOWMQ_CORE_PUBLIC_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_core.c)

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
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(flowmq_core PUBLIC FlowMQ::Protocol TurboUtils::Core)

add_library(
  flowmq_transport STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_coronet_transport.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_coronet_transport.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_connect_endpoint.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_connect_endpoint.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_posted_send.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_posted_send.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_router_endpoint.c
  ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_coronet.h
  ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_connect_endpoint.h
  ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_router_endpoint.h
  ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_send_admission.h)
cmake_config_target(
  flowmq_transport
  ALIAS FlowMQ::Transport
  FOLDER "flowmq/transport")
set_target_properties(flowmq_transport PROPERTIES EXCLUDE_FROM_ALL TRUE)
target_include_directories(
  flowmq_transport
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/flowmq/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(flowmq_transport
                      PUBLIC FlowMQ::Core TurboUtils::Core
                      PRIVATE TurboNet::CoroNet OpenSSL::SSL)

set(FLOWMQ_CORE_SOURCES
    ${FLOWMQ_CORE_SOURCES}
    ${FLOWMQ_CORE_PUBLIC_SOURCES}
    PARENT_SCOPE)
set(FLOWMQ_TRANSPORT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_coronet_transport.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_connect_endpoint.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_posted_send.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_router_endpoint.c
    PARENT_SCOPE)
set(FLOWMQ_RUNTIME_PUBLIC_HEADERS
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_core.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_coronet.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_connect_endpoint.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_router_endpoint.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_send_admission.h
    ${PROJECT_SOURCE_DIR}/flowmq/include/flowmq_tls_identity_map.h
    PARENT_SCOPE)

if(BUILD_TESTS)
  # Internal state-machine tests intentionally use private headers. Compile a
  # static test subject so those private symbols do not become public ABI.
  add_library(flowmq_core_test_support STATIC ${FLOWMQ_CORE_SOURCES} ${FLOWMQ_CORE_PUBLIC_SOURCES})
  cmake_config_target(flowmq_core_test_support FOLDER "flowmq/core/tests")
  target_include_directories(
    flowmq_core_test_support
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
           ${PROJECT_SOURCE_DIR}/flowmq/include)
  target_link_libraries(flowmq_core_test_support PUBLIC FlowMQ::Protocol TurboUtils::Core)
  add_subdirectory(tests)
endif()
