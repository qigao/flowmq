
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
                      PRIVATE TurboUtils::STL TurboNet::CoroNet OpenSSL::SSL)

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
  add_subdirectory(tests)
endif()
