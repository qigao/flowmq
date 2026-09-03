if(NOT TARGET Rocida::TinyTest)
  find_package(Rocida CONFIG REQUIRED)
endif()

cmake_add_benchmark(
  bench_flowmq_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_flowmq_protocol.c
  LIBS FlowMQ::Protocol Rocida::TinyTest
  FOLDER "flowmq/benchmarks")
target_include_directories(bench_flowmq_protocol PRIVATE
                           ${CMAKE_CURRENT_SOURCE_DIR}/../protocol/src)
