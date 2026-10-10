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
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/tests/runtime ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
  FOLDER "flowmq/benchmarks/runtime")

cmake_add_benchmark(
  bench_flowmq_owner_mailbox
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_owner_mailbox.c
  LIBS FlowMQ::Transport Salts::Concurrency Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
  FOLDER "flowmq/benchmarks/runtime")
if(WIN32)
  target_link_libraries(bench_flowmq_owner_mailbox PRIVATE psapi)
endif()
if(BUILD_TESTS)
  add_test(NAME test_flowmq_owner_mailbox
           COMMAND bench_flowmq_owner_mailbox --filter "mailbox correctness:")
  add_test(NAME bench_flowmq_owner_mailbox
           COMMAND bench_flowmq_owner_mailbox --filter "mailbox comparison:")
  set_tests_properties(test_flowmq_owner_mailbox PROPERTIES
    LABELS "flowmq;flowmq-transport" RUN_SERIAL TRUE TIMEOUT 120)
  set_tests_properties(bench_flowmq_owner_mailbox PROPERTIES
    LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 600)
  if(WIN32)
    set_tests_properties(test_flowmq_owner_mailbox bench_flowmq_owner_mailbox PROPERTIES
      ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  endif()
endif()
if(WIN32)
  target_link_libraries(bench_flowmq_socket_owners PRIVATE psapi)
endif()
if(BUILD_TESTS)
  add_test(NAME bench_flowmq_paced_lanes
           COMMAND bench_flowmq_socket_owners --filter "paced lanes:")
  set_tests_properties(bench_flowmq_paced_lanes PROPERTIES
    LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 300)
  add_test(NAME test_flowmq_paced_lanes
           COMMAND bench_flowmq_socket_owners --filter "correctness: paced shared lanes")
  set_tests_properties(test_flowmq_paced_lanes PROPERTIES
    LABELS "flowmq;flowmq-transport" RUN_SERIAL TRUE TIMEOUT 120)
  if(WIN32)
    set_tests_properties(bench_flowmq_paced_lanes test_flowmq_paced_lanes PROPERTIES
      ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  endif()
  add_test(NAME bench_flowmq_shared_lanes
           COMMAND bench_flowmq_socket_owners --filter "shared lanes:")
  set_tests_properties(bench_flowmq_shared_lanes PROPERTIES
    LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 1200)
  add_test(NAME test_flowmq_shared_lanes
           COMMAND bench_flowmq_socket_owners --filter "correctness: shared owner lanes")
  set_tests_properties(test_flowmq_shared_lanes PROPERTIES
    LABELS "flowmq;flowmq-transport" RUN_SERIAL TRUE TIMEOUT 120)
  if(WIN32)
    set_tests_properties(bench_flowmq_shared_lanes test_flowmq_shared_lanes PROPERTIES
      ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  endif()
  add_test(NAME bench_flowmq_lane_scaling
           COMMAND bench_flowmq_socket_owners --filter "lane scaling:")
  set_tests_properties(bench_flowmq_lane_scaling
                       PROPERTIES LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 600)
  if(WIN32)
    set_tests_properties(bench_flowmq_lane_scaling
                         PROPERTIES ENVIRONMENT_MODIFICATION
                                    "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  endif()
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
      set(pool_benchmark_name bench_flowmq_peer_pool_${transport}_${payload_bytes})
      add_test(NAME ${pool_benchmark_name}
               COMMAND bench_flowmq_socket_owners --filter
                       "peer pool: ${transport} ${payload_bytes}-byte")
      set_tests_properties(${pool_benchmark_name}
                           PROPERTIES LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 1200)
      if(WIN32)
        set_tests_properties(${pool_benchmark_name}
                             PROPERTIES ENVIRONMENT_MODIFICATION
                                        "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
      endif()
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
  add_test(NAME test_flowmq_owned_rx_evidence
           COMMAND bench_flowmq_socket --filter "receive evidence:")
  set_tests_properties(test_flowmq_owned_rx_evidence
                       PROPERTIES LABELS "flowmq;flowmq-transport" TIMEOUT 30)
  add_test(NAME bench_flowmq_socket_tcp
           COMMAND bench_flowmq_socket --filter "caller-driven loopback TCP")
  set_tests_properties(bench_flowmq_socket_tcp
                       PROPERTIES LABELS "flowmq;benchmark" RUN_SERIAL TRUE
                                  TIMEOUT 600 ENVIRONMENT "FLOWMQ_BENCH_CI=1")
  if(WIN32)
    set_tests_properties(test_flowmq_owned_rx_evidence bench_flowmq_socket_tcp
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
if(BUILD_TESTS)
  add_test(NAME bench_flowmq_pattern_dispatch COMMAND bench_flowmq_pattern_dispatch)
  set_tests_properties(bench_flowmq_pattern_dispatch PROPERTIES
    LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 600
    ENVIRONMENT "FLOWMQ_BENCH_SMOKE=0")
  if(WIN32)
    set_tests_properties(bench_flowmq_pattern_dispatch PROPERTIES
      ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  endif()
endif()

cmake_add_benchmark(
  bench_flowmq_dispatch_policy
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_dispatch_policy.c
  LIBS FlowMQ::Core Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/core/pattern
  FOLDER "flowmq/benchmarks/runtime")

cmake_add_benchmark(
  bench_flowmq_pattern_shared_listener
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_pattern_dispatch.c
  LIBS FlowMQ::Transport Salts::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
  DEFS FLOWMQ_BENCH_SHARED_LISTENER=1
  FOLDER "flowmq/benchmarks/runtime")
if(BUILD_TESTS)
  add_test(NAME bench_flowmq_pattern_shared_listener
           COMMAND bench_flowmq_pattern_shared_listener)
  set_tests_properties(bench_flowmq_pattern_shared_listener PROPERTIES
    LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 600
    ENVIRONMENT "FLOWMQ_BENCH_SMOKE=0")
  if(WIN32)
    set_tests_properties(bench_flowmq_pattern_shared_listener PROPERTIES
      ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  endif()
endif()

# Reuse the same private transport copies for pattern and PAIR diagnostics.
# Neither the counters nor phase clocks enter the production transport target.
foreach(probe_kind IN ITEMS probe profile)
  set(probe_transport flowmq_transport_batch_${probe_kind})
  add_library(${probe_transport} STATIC ${FLOWMQ_TRANSPORT_SOURCES})
  cmake_config_target(${probe_transport} FOLDER "flowmq/benchmarks/support")
  target_include_directories(${probe_transport}
    PUBLIC $<TARGET_PROPERTY:flowmq_transport,INTERFACE_INCLUDE_DIRECTORIES>
    PRIVATE $<TARGET_PROPERTY:flowmq_transport,INCLUDE_DIRECTORIES>)
  target_link_libraries(${probe_transport}
    PUBLIC $<TARGET_PROPERTY:flowmq_transport,LINK_LIBRARIES>)
  target_compile_definitions(${probe_transport} PRIVATE FLOWMQ_BATCH_PROBE=1)

  set(pattern_probe bench_flowmq_pattern_${probe_kind})
  cmake_add_benchmark(${pattern_probe}
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_pattern_dispatch.c
    LIBS ${probe_transport} Salts::TinyTest
    DEFS FLOWMQ_BATCH_PROBE=1
    INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
    FOLDER "flowmq/benchmarks/runtime")
  if(BUILD_TESTS)
    add_test(NAME ${pattern_probe} COMMAND ${pattern_probe})
    set_tests_properties(${pattern_probe} PROPERTIES
      LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 600
      ENVIRONMENT "FLOWMQ_BENCH_SMOKE=0;FLOWMQ_PATTERN_OMIT_LISTENER=0")
    if(WIN32)
      set_tests_properties(${pattern_probe} PROPERTIES
        ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
    endif()
  endif()
endforeach()
target_compile_definitions(flowmq_transport_batch_profile PRIVATE FLOWMQ_BATCH_PHASE_TIMING=1)
if(BUILD_TESTS)
  add_test(NAME bench_flowmq_pattern_no_listener COMMAND bench_flowmq_pattern_probe)
  set_tests_properties(bench_flowmq_pattern_no_listener PROPERTIES
    LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 600
    ENVIRONMENT "FLOWMQ_BENCH_SMOKE=0;FLOWMQ_PATTERN_OMIT_LISTENER=1")
  if(WIN32)
    set_tests_properties(bench_flowmq_pattern_no_listener PROPERTIES
      ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
  endif()
endif()

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
  target_link_libraries(bench_flowmq_socket_owners PRIVATE libzmq)
  target_compile_definitions(bench_flowmq_socket_owners PRIVATE FLOWMQ_BENCH_WITH_ZMQ=1)
  cmake_add_benchmark(bench_flowmq_pattern_zmq
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_pattern_zmq.c
    LIBS libzmq Salts::Core Salts::TinyTest
    FOLDER "flowmq/benchmarks/runtime")
  if(BUILD_TESTS)
    add_test(NAME test_flowmq_pattern_zmq
             COMMAND bench_flowmq_pattern_zmq --filter "correctness:")
    set_tests_properties(test_flowmq_pattern_zmq PROPERTIES
      LABELS "flowmq;flowmq-transport" RUN_SERIAL TRUE TIMEOUT 120
      ENVIRONMENT "FLOWMQ_PATTERN_ZMQ_SPIN=0")
    add_test(NAME bench_flowmq_pattern_zmq
             COMMAND bench_flowmq_pattern_zmq --filter "performance:")
    set_tests_properties(bench_flowmq_pattern_zmq PROPERTIES
      LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 600
      ENVIRONMENT "FLOWMQ_PATTERN_ZMQ_SPIN=0")
    add_test(NAME test_flowmq_pattern_zmq_spin
             COMMAND bench_flowmq_pattern_zmq --filter "correctness:")
    set_tests_properties(test_flowmq_pattern_zmq_spin PROPERTIES
      LABELS "flowmq;flowmq-transport" RUN_SERIAL TRUE TIMEOUT 120
      ENVIRONMENT "FLOWMQ_PATTERN_ZMQ_SPIN=1")
    add_test(NAME bench_flowmq_pattern_zmq_spin
             COMMAND bench_flowmq_pattern_zmq --filter "performance:")
    set_tests_properties(bench_flowmq_pattern_zmq_spin PROPERTIES
      LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 600
      ENVIRONMENT "FLOWMQ_PATTERN_ZMQ_SPIN=1")
    if(WIN32)
      set_tests_properties(test_flowmq_pattern_zmq bench_flowmq_pattern_zmq
                           test_flowmq_pattern_zmq_spin bench_flowmq_pattern_zmq_spin PROPERTIES
        ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
    endif()
  endif()
  if(BUILD_TESTS)
    add_test(NAME bench_flowmq_lane_zmq
             COMMAND bench_flowmq_socket_owners --filter "lane comparison:")
    set_tests_properties(bench_flowmq_lane_zmq
      PROPERTIES LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 1200)
    if(WIN32)
      set_tests_properties(bench_flowmq_lane_zmq PROPERTIES
        ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
    endif()
  endif()

  foreach(batch_target IN ITEMS bench_flowmq_batch bench_flowmq_batch_probe bench_flowmq_batch_profile)
    if(batch_target STREQUAL "bench_flowmq_batch_probe")
      set(batch_transport flowmq_transport_batch_probe)
    elseif(batch_target STREQUAL "bench_flowmq_batch_profile")
      set(batch_transport flowmq_transport_batch_profile)
    else()
      set(batch_transport FlowMQ::Transport)
    endif()
    cmake_add_benchmark(${batch_target}
      SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_batch.c
      LIBS ${batch_transport} libzmq Salts::TinyTest
      INCLUDES ${PROJECT_SOURCE_DIR}/flowmq/src/runtime
      FOLDER "flowmq/benchmarks/runtime")
    if(WIN32)
      target_link_libraries(${batch_target} PRIVATE psapi)
    endif()
  endforeach()
  target_compile_definitions(bench_flowmq_batch_probe PRIVATE FLOWMQ_BATCH_PROBE=1)
  target_compile_definitions(bench_flowmq_batch_profile
    PRIVATE FLOWMQ_BATCH_PROBE=1 FLOWMQ_BATCH_PHASE_TIMING=1)

  if(BUILD_TESTS)
    foreach(batch_target IN ITEMS bench_flowmq_batch bench_flowmq_batch_probe bench_flowmq_batch_profile)
      add_test(NAME test_${batch_target} COMMAND ${batch_target} --filter "correctness:")
      set_tests_properties(test_${batch_target}
        PROPERTIES LABELS "flowmq;flowmq-transport" RUN_SERIAL TRUE TIMEOUT 120)
      if(WIN32)
        set_tests_properties(test_${batch_target} PROPERTIES
          ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
      endif()
    endforeach()
    add_test(NAME bench_flowmq_batch_sweep
             COMMAND bench_flowmq_batch --filter "performance:")
    add_test(NAME bench_flowmq_batch_diagnostic
             COMMAND bench_flowmq_batch_probe --filter "diagnostic:")
    add_test(NAME bench_flowmq_batch_ablation
             COMMAND bench_flowmq_batch_probe --filter "ablation:")
    add_test(NAME bench_flowmq_batch_phases
             COMMAND bench_flowmq_batch_profile --filter "diagnostic:")
    add_test(NAME bench_flowmq_batch_policy
             COMMAND bench_flowmq_batch_probe --filter "policy:")
    add_test(NAME bench_flowmq_progress_phases
             COMMAND bench_flowmq_batch_profile --filter "progress-phases:")
    add_test(NAME bench_flowmq_progress_ablation
             COMMAND bench_flowmq_batch_probe --filter "progress-ablation:")
    set_tests_properties(bench_flowmq_batch_sweep bench_flowmq_batch_diagnostic
                         bench_flowmq_batch_ablation bench_flowmq_batch_phases bench_flowmq_batch_policy
                         bench_flowmq_progress_phases bench_flowmq_progress_ablation
      PROPERTIES LABELS "flowmq;benchmark" RUN_SERIAL TRUE TIMEOUT 600)
    if(WIN32)
      set_tests_properties(bench_flowmq_batch_sweep bench_flowmq_batch_diagnostic
                           bench_flowmq_batch_ablation bench_flowmq_batch_phases bench_flowmq_batch_policy
                           bench_flowmq_progress_phases bench_flowmq_progress_ablation
        PROPERTIES ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
    endif()
    # The paired cases share fixed sample counts, independent of smoke/CI mode.
    add_test(NAME bench_flowmq_zmq_comparison
             COMMAND bench_flowmq_socket --filter "paired TCP comparison")
    set_tests_properties(bench_flowmq_zmq_comparison
                         PROPERTIES LABELS "flowmq;benchmark" RUN_SERIAL TRUE
                                    TIMEOUT 600
                                    ENVIRONMENT "FLOWMQ_BENCH_CI=0;FLOWMQ_BENCH_SMOKE=0")
    if(WIN32)
      set_tests_properties(bench_flowmq_zmq_comparison
                           PROPERTIES ENVIRONMENT_MODIFICATION
                                      "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
    endif()
  endif()
endif()
