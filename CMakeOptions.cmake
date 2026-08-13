include(CMakeDependentOption)

set(CMAKE_COLOR_DIAGNOSTICS ON)

option(BUILD_EXAMPLES "Build standalone FlowMQ examples" OFF)
option(BUILD_BENCHMARKS "Build standalone FlowMQ benchmarks" OFF)
option(BUILD_TESTS "Build standalone FlowMQ tests" OFF)
cmake_dependent_option(ENABLE_ASAN "Enable AddressSanitizer in Debug builds" ON
                       "CMAKE_BUILD_TYPE STREQUAL Debug" OFF)
