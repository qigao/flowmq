if(NOT TARGET Rocida::TinyTest)
  find_package(Rocida CONFIG REQUIRED)
endif()

cmake_add_benchmark(
  bench_flowmq_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/protocol/bench_flowmq_protocol.c
  LIBS FlowMQ::Protocol Rocida::TinyTest
  FOLDER "flowmq/benchmarks/protocol")
target_include_directories(
  bench_flowmq_protocol
  PRIVATE ${PROJECT_SOURCE_DIR}/flowmq/src/protocol)

cmake_add_benchmark(
  bench_flowmq_socket
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/runtime/bench_flowmq_socket.c
  LIBS FlowMQ::Transport Rocida::TinyTest
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
