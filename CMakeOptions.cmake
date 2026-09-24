include(CMakeDependentOption)

set(CMAKE_COLOR_DIAGNOSTICS ON)

option(BUILD_EXAMPLES "Build standalone FlowMQ examples" ON)
option(BUILD_TESTS "Build standalone FlowMQ tests" OFF)
option(FLOWMQ_BUILD_ZMQ_BENCHMARK
       "Compare the direct socket benchmark with the vcpkg libzmq" OFF)
option(FLOWMQ_EXPERIMENT_ROUTE_REPLY_FACT_SOURCE
       "Experiment: derive reply-peer routing from routing_class" OFF)
cmake_dependent_option(ENABLE_ASAN "Enable AddressSanitizer in Debug builds" ON
                       "CMAKE_BUILD_TYPE STREQUAL Debug" OFF)
