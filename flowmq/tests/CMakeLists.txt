# DataBind FlowMQ transport plans are generated only for qualification. Reuse
# the exact host compiler already resolved by the top-level cross-platform
# build so Android never attempts to execute a target salts-idlc.
set(SaltsUtils_IDLC_EXECUTABLE
    "${FLOWMQ_SALTS_IDLC_EXECUTABLE}"
    CACHE FILEPATH "FlowMQ qualification host salts-idlc" FORCE)
set(SaltsUtils_IDLC_HOST_SALTS_ROOT
    "${FLOWMQ_SALTS_HOST_ROOT_PATH}"
    CACHE PATH "FlowMQ qualification host Salts SDK" FORCE)

salts_idl_target(
  TARGET flowmq_databind_channel_fixture
  IDL "${CMAKE_CURRENT_SOURCE_DIR}/databind/channel.schema"
  ARTIFACT_NAME "flowmq_contract_channel"
  TRANSPORTS FLOWMQ
  BINARY_CODEC
  PROJECTION_CONFIG
    "${CMAKE_CURRENT_SOURCE_DIR}/databind/channel_config.json")

salts_idl_target(
  TARGET flowmq_databind_service_fixture
  IDL "${CMAKE_CURRENT_SOURCE_DIR}/databind/service.schema"
  ARTIFACT_NAME "flowmq_contract_service"
  TRANSPORTS FLOWMQ
  BINARY_CODEC
  PROJECTION_CONFIG
    "${CMAKE_CURRENT_SOURCE_DIR}/databind/service_config.json")

add_executable(
  test_flowmq_databind_projection
  "${CMAKE_CURRENT_SOURCE_DIR}/databind/test_flowmq_databind_projection.c")
target_compile_features(test_flowmq_databind_projection PRIVATE c_std_11)
target_link_libraries(
  test_flowmq_databind_projection
  PRIVATE FlowMQ::FlowMQ
          Salts::DataBind
          ${flowmq_databind_channel_fixture_NATIVE_TARGET}
          ${flowmq_databind_service_fixture_NATIVE_TARGET}
          Salts::TinyTest)
target_include_directories(
  test_flowmq_databind_projection
  PRIVATE "${flowmq_databind_channel_fixture_GENERATED_DIR}"
          "${flowmq_databind_service_fixture_GENERATED_DIR}")
add_dependencies(
  test_flowmq_databind_projection
  flowmq_databind_channel_fixture_idl_codegen
  flowmq_databind_service_fixture_idl_codegen)
add_test(
  NAME test_flowmq_databind_projection
  COMMAND test_flowmq_databind_projection)

cmake_add_test(
  test_flowmq_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/protocol/test_flowmq_protocol.c
  LIBS FlowMQ::Protocol Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/protocol
  FOLDER "flowmq/tests/protocol")

cmake_add_test(
  test_flowmq_esb
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/protocol/test_flowmq_esb.c
  LIBS FlowMQ::Protocol Salts::TinyTest
  FOLDER "flowmq/tests/protocol")

cmake_add_test(
  test_flowmq_stream_decoder
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/protocol/test_flowmq_stream_decoder.c
  LIBS FlowMQ::Protocol Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/protocol/stream
  FOLDER "flowmq/tests/protocol")

cmake_add_test(
  test_flowmq_core
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/core/test_flowmq_core.c
  LIBS FlowMQ::Core Salts::TinyTest
  FOLDER "flowmq/tests/core")

foreach(test_name IN ITEMS pattern pattern_state flow_control reconnect subscription_set)
  cmake_add_test(
    test_flowmq_${test_name}
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/core/test_flowmq_${test_name}.c
    LIBS FlowMQ::Core Salts::TinyTest
    INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/core/pattern
             ${PROJECT_SOURCE_DIR}/flowmq/src/core/session
    FOLDER "flowmq/tests/core")
endforeach()

cmake_add_test(
  test_flowmq_tls_identity_map
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/security/test_flowmq_tls_identity_map.c
  LIBS FlowMQ::Core Salts::TinyTest
  FOLDER "flowmq/tests/security")

cmake_add_test(
  test_flowmq_transport
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_flowmq_transport.c
  LIBS FlowMQ::Transport Salts::TinyTest
  FOLDER "flowmq/tests/transport")

cmake_add_test(
  test_flowmq_listener_external
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_flowmq_listener_external.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/transport/cnet
           ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
  FOLDER "flowmq/tests/transport")
set_tests_properties(test_flowmq_listener_external PROPERTIES TIMEOUT 30)

cmake_add_test(
  test_flowmq_socket
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_socket.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
  FOLDER "flowmq/tests/runtime")

cmake_add_test(
  test_flowmq_destination
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_destination.c
  LIBS FlowMQ::Transport Salts::TinyTest
  FOLDER "flowmq/tests/runtime")

cmake_add_test(
  test_flowmq_peer_pool
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_peer_pool.c
  LIBS FlowMQ::Transport Salts::TinyTest
  FOLDER "flowmq/tests/runtime")

cmake_add_test(
  test_flowmq_socket_pool
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_socket.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
  FOLDER "flowmq/tests/runtime")
target_compile_definitions(test_flowmq_socket_pool PRIVATE FLOWMQ_TEST_PEER_POOL=1)

cmake_add_test(
  test_flowmq_owned_stream
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_owned_stream.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
           ${PROJECT_SOURCE_DIR}/flowmq/src/protocol
           ${PROJECT_SOURCE_DIR}/flowmq/src/protocol/stream
  FOLDER "flowmq/tests/runtime")

add_test(
  NAME test_flowmq_segmented_receive_contract
  COMMAND ${CMAKE_COMMAND}
          "-DFLOWMQ_SOCKET_SOURCE=${PROJECT_SOURCE_DIR}/flowmq/src/runtime/flowmq_socket.c"
          -P "${CMAKE_CURRENT_SOURCE_DIR}/runtime/flowmq_segmented_receive_contract_test.cmake")

cmake_add_test(
  test_flowmq_owner
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_owner.c
  LIBS FlowMQ::Transport Salts::TinyTest
  FOLDER "flowmq/tests/runtime")

cmake_add_test(
  test_flowmq_owner_fault
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_owner_fault.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
  FOLDER "flowmq/tests/runtime")

cmake_add_test(
  test_flowmq_owner_batch
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_owner_batch.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
  FOLDER "flowmq/tests/runtime")

cmake_add_test(
  test_flowmq_owner_fault_pool
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_owner_fault.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
  FOLDER "flowmq/tests/runtime")
target_compile_definitions(test_flowmq_owner_fault_pool PRIVATE FLOWMQ_TEST_PEER_POOL=1)

foreach(test_suffix IN ITEMS listener listener_pool listener_fault listener_fault_pool)
  if(test_suffix MATCHES "fault")
    set(listener_test_source runtime/test_flowmq_owner_fault.c)
  else()
    set(listener_test_source runtime/test_flowmq_owner_listener.c)
  endif()
  cmake_add_test(
    test_flowmq_owner_${test_suffix}
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/${listener_test_source}
    LIBS FlowMQ::Transport Salts::TinyTest
    INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
    DEFS FLOWMQ_TEST_OWNER_LISTENER=1
    FOLDER "flowmq/tests/runtime")
  if(test_suffix MATCHES "pool$")
    target_compile_definitions(test_flowmq_owner_${test_suffix}
                               PRIVATE FLOWMQ_TEST_PEER_POOL=1)
  endif()
  set_tests_properties(test_flowmq_owner_${test_suffix} PROPERTIES TIMEOUT 60)
endforeach()

cmake_add_test(
  test_flowmq_socket_option
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_socket_option.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
           ${PROJECT_SOURCE_DIR}/flowmq/src/core/pattern
  FOLDER "flowmq/tests/runtime")

cmake_add_test(
  test_flowmq_peer_state
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/test_flowmq_peer_state.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
           ${PROJECT_SOURCE_DIR}/flowmq/src/core/session
  FOLDER "flowmq/tests/runtime")

cmake_add_test(
  test_flowmq_media_provider_schema
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/media_provider/test_flowmq_media_provider_schema.c
          ${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}
  LIBS FlowMQ::FlowMQ Salts::TinyTest
  INCLUDES ${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}
  FOLDER "flowmq/tests/media_provider")

if(TARGET ${FLOWMQ_MEDIA_PROVIDER_CODEGEN_TARGET})
  add_dependencies(test_flowmq_media_provider_schema
                   ${FLOWMQ_MEDIA_PROVIDER_CODEGEN_TARGET})
endif()

set(FLOWMQ_PROTOCOL_TESTS
    test_flowmq_protocol
    test_flowmq_esb
    test_flowmq_stream_decoder)
set(FLOWMQ_CORE_TESTS
    test_flowmq_core
    test_flowmq_pattern
    test_flowmq_pattern_state
    test_flowmq_flow_control
    test_flowmq_reconnect
    test_flowmq_subscription_set
    test_flowmq_tls_identity_map)
set(FLOWMQ_TRANSPORT_TESTS
    test_flowmq_transport
    test_flowmq_listener_external
    test_flowmq_socket
    test_flowmq_destination
    test_flowmq_peer_pool
    test_flowmq_socket_pool
    test_flowmq_owned_stream
    test_flowmq_segmented_receive_contract
    test_flowmq_owner
    test_flowmq_owner_fault
    test_flowmq_owner_fault_pool
    test_flowmq_owner_listener
    test_flowmq_owner_listener_pool
    test_flowmq_owner_listener_fault
    test_flowmq_owner_listener_fault_pool
    test_flowmq_owner_batch
    test_flowmq_socket_option
    test_flowmq_peer_state)

# Reuse the installed-package consumers to check the same public C11/C++17
# calls against the product shared library during normal runtime regression.
# Installed target/dependency qualification remains in tests/package.
cmake_add_test(
  test_flowmq_public_c11
  SOURCES ${PROJECT_SOURCE_DIR}/tests/package/main.c
  LIBS FlowMQ::FlowMQ
  FOLDER "flowmq/tests/runtime")
target_compile_features(test_flowmq_public_c11 PRIVATE c_std_11)
cmake_add_test(
  test_flowmq_public_cpp17
  SOURCES ${PROJECT_SOURCE_DIR}/tests/package/consumer_cpp.cpp
  LIBS FlowMQ::FlowMQ
  FOLDER "flowmq/tests/runtime")
target_compile_features(test_flowmq_public_cpp17 PRIVATE cxx_std_17)
list(APPEND FLOWMQ_TRANSPORT_TESTS test_flowmq_public_c11 test_flowmq_public_cpp17)

set_tests_properties(test_flowmq_protocol
                     PROPERTIES LABELS "fmq-release;flowmq-protocol")
set_tests_properties(test_flowmq_esb
                     PROPERTIES LABELS "fmq-release;flowmq-protocol;flowmq-esb")
set_tests_properties(test_flowmq_stream_decoder
                     PROPERTIES LABELS "flowmq;flowmq-protocol")
set_tests_properties(${FLOWMQ_CORE_TESTS}
                     PROPERTIES LABELS "flowmq;flowmq-core")
set_tests_properties(${FLOWMQ_TRANSPORT_TESTS}
                     PROPERTIES LABELS "flowmq;flowmq-transport")
set_tests_properties(test_flowmq_media_provider_schema
                     PROPERTIES LABELS "flowmq;flowmq-application")
set_tests_properties(test_flowmq_databind_projection
                     PROPERTIES LABELS "flowmq;flowmq-application;flowmq-databind")

if(WIN32)
  set_tests_properties(
    ${FLOWMQ_PROTOCOL_TESTS}
    ${FLOWMQ_CORE_TESTS}
    ${FLOWMQ_TRANSPORT_TESTS}
    test_flowmq_media_provider_schema
    test_flowmq_databind_projection
    PROPERTIES ENVIRONMENT_MODIFICATION
               "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
endif()
