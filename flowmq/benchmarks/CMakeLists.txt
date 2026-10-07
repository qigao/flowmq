if(NOT TARGET Salts::TinyTest)
  find_package(Salts CONFIG REQUIRED)
endif()

cmake_add_benchmark(
  bench_flowmq_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/protocol/bench_flowmq_protocol.c
  LIBS FlowMQ::Protocol Salts::TinyTest
  FOLDER "flowmq/benchmarks/protocol")
target_include_directories(
  bench_flowmq_protocol
  PRIVATE ${PROJECT_SOURCE_DIR}/flowmq/src/protocol)

cmake_add_benchmark(
  bench_flowmq_socket_owners
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_socket_owners.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/tests/runtime
  FOLDER "flowmq/benchmarks/runtime")
if(WIN32)
  target_link_libraries(bench_flowmq_socket_owners PRIVATE psapi)
endif()
if(BUILD_TESTS)
  add_test(NAME test_flowmq_socket_owners
           COMMAND bench_flowmq_socket_owners --filter "correctness:")
  set_tests_properties(test_flowmq_socket_owners
                       PROPERTIES LABELS "flowmq;flowmq-transport" TIMEOUT 120)
  if(WIN32)
    set_tests_properties(test_flowmq_socket_owners
                         PROPERTIES ENVIRONMENT_MODIFICATION
                                    "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  endif()
  foreach(transport IN ITEMS tcp tls)
    foreach(payload_bytes IN ITEMS 64 65536)
      set(benchmark_name bench_flowmq_socket_owners_${transport}_${payload_bytes})
      add_test(NAME ${benchmark_name}
               COMMAND bench_flowmq_socket_owners --filter
                       "scaling: ${transport} ${payload_bytes}-byte")
      set_tests_properties(${benchmark_name}
                           PROPERTIES LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 600)
      if(WIN32)
        set_tests_properties(${benchmark_name}
                             PROPERTIES ENVIRONMENT_MODIFICATION
                                        "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
      endif()
    endforeach()
  endforeach()
endif()

cmake_add_benchmark(
  bench_flowmq_socket
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_socket.c
  LIBS FlowMQ::Transport Salts::TinyTest
  FOLDER "flowmq/benchmarks/runtime")
target_include_directories(
  bench_flowmq_socket
  PRIVATE ${PROJECT_SOURCE_DIR}/flowmq/tests/runtime)

if(BUILD_TESTS)
  add_test(NAME bench_flowmq_socket_tcp
           COMMAND bench_flowmq_socket --filter "caller-driven loopback TCP")
  set_tests_properties(bench_flowmq_socket_tcp
                       PROPERTIES LABELS "flowmq;benchmark" RUN_SERIAL TRUE
                                  TIMEOUT 600 ENVIRONMENT "FLOWMQ_BENCH_CI=1")
  if(WIN32)
    set_tests_properties(bench_flowmq_socket_tcp
                         PROPERTIES ENVIRONMENT_MODIFICATION
                                    "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  endif()
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  find_package(Threads REQUIRED)
  cmake_add_benchmark(
    bench_flowmq_owner_parallel
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_owner_parallel.c
    LIBS FlowMQ::Transport Threads::Threads
    FOLDER "flowmq/benchmarks/runtime")
  set_target_properties(
    bench_flowmq_owner_parallel
    PROPERTIES
      C_STANDARD 11
      C_STANDARD_REQUIRED ON
      C_EXTENSIONS OFF)

  cmake_add_benchmark(
    bench_flowmq_owner_shared_wait
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_owner_shared_wait.c
    LIBS FlowMQ::Transport Threads::Threads
    FOLDER "flowmq/benchmarks/runtime")
  set_target_properties(
    bench_flowmq_owner_shared_wait
    PROPERTIES
      C_STANDARD 11
      C_STANDARD_REQUIRED ON
      C_EXTENSIONS OFF)

  cmake_add_benchmark(
    bench_flowmq_reuse_port_multicore
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_reuse_port_multicore.c
    LIBS FlowMQ::Transport Threads::Threads
    FOLDER "flowmq/benchmarks/runtime")
  set_target_properties(
    bench_flowmq_reuse_port_multicore
    PROPERTIES
      C_STANDARD 11
      C_STANDARD_REQUIRED ON
      C_EXTENSIONS OFF)
endif()

cmake_add_benchmark(
  bench_flowmq_retained_fanout
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_retained_fanout.c
  LIBS FlowMQ::Transport
  FOLDER "flowmq/benchmarks/runtime")

cmake_add_benchmark(
  bench_flowmq_pattern_dispatch
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_pattern_dispatch.c
  LIBS FlowMQ::Transport Salts::TinyTest
  FOLDER "flowmq/benchmarks/runtime")

cmake_add_benchmark(
  bench_flowmq_dispatch_policy
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_dispatch_policy.c
  LIBS FlowMQ::Core Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/core/pattern
  FOLDER "flowmq/benchmarks/runtime")

if(FLOWMQ_BUILD_ZMQ_BENCHMARK)
  find_package(ZeroMQ CONFIG REQUIRED)
  string(TOUPPER "${CMAKE_BUILD_TYPE}" _flowmq_zmq_config)
  get_target_property(
    _flowmq_zmq_config_location libzmq
    "IMPORTED_LOCATION_${_flowmq_zmq_config}")
  get_target_property(_flowmq_zmq_generic_location libzmq IMPORTED_LOCATION)
  if(NOT _flowmq_zmq_config_location AND NOT _flowmq_zmq_generic_location)
    message(
      FATAL_ERROR
        "FLOWMQ_BUILD_ZMQ_BENCHMARK requires a ${CMAKE_BUILD_TYPE} libzmq "
        "from the vcpkg manifest; the configured ZeroMQ package has no "
        "matching imported artifact")
  endif()
  target_link_libraries(bench_flowmq_socket PRIVATE libzmq)
  target_compile_definitions(bench_flowmq_socket PRIVATE FLOWMQ_BENCH_WITH_ZMQ=1)
endif()
