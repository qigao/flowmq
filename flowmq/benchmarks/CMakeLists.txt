if(NOT TARGET TurboUtils::TinyTest)
  find_package(TurboUtils CONFIG REQUIRED)
endif()

cmake_add_benchmark(
  bench_flowmq_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_flowmq_protocol.c
  LIBS FlowMQ::Protocol TurboUtils::TinyTest
  FOLDER "flowmq/benchmarks")
