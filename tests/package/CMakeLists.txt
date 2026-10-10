cmake_minimum_required(VERSION 3.20)
foreach(_package_root IN ITEMS FLOWMQ_ROOT SALTS_ROOT SALTS_UTILS_ROOT
                               VCPKG_ROOT VCPKG_CACHE_REPOSITORY_ROOT)
  if(NOT DEFINED ENV{${_package_root}} OR "$ENV{${_package_root}}" STREQUAL "")
    message(FATAL_ERROR "${_package_root} must name the selected package/toolchain root")
  endif()
endforeach()
if(NOT DEFINED VCPKG_TARGET_TRIPLET OR "${VCPKG_TARGET_TRIPLET}" STREQUAL "")
  message(FATAL_ERROR "Select a package user preset and set FLOWMQ_TRIPLET")
endif()
project(flowmq_package_consumer C CXX)

file(TO_CMAKE_PATH "$ENV{FLOWMQ_ROOT}" FLOWMQ_ROOT_PATH)
unset(FlowMQ_DIR CACHE)
unset(FlowMQ_DIR)
find_package(FlowMQ 1.2 CONFIG REQUIRED
             PATHS "${FLOWMQ_ROOT_PATH}" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)

get_target_property(_flowmq_public_links
                    FlowMQ::FlowMQ INTERFACE_LINK_LIBRARIES)
if(NOT _flowmq_public_links)
  set(_flowmq_public_links "")
endif()
foreach(_flowmq_required_dependency IN ITEMS Salts::Core Salts::DataBind)
  if(NOT _flowmq_required_dependency IN_LIST _flowmq_public_links)
    message(FATAL_ERROR
            "installed FlowMQ::FlowMQ is missing canonical public dependency "
            "${_flowmq_required_dependency}: ${_flowmq_public_links}")
  endif()
endforeach()

foreach(_flowmq_forbidden_dependency IN ITEMS
        Salts::TbeSchema
        Salts::DataBindSchema
        Salts::CMeta
        Salts::CFlow
        Salts::CSTL
        Salts::CNet
        Salts::NativeIO
        OpenSSL::SSL
        OpenSSL::Crypto
        GmSSL::GmSSL)
  if(_flowmq_forbidden_dependency IN_LIST _flowmq_public_links)
    message(FATAL_ERROR
            "installed FlowMQ::FlowMQ leaks obsolete/private dependency "
            "${_flowmq_forbidden_dependency}: ${_flowmq_public_links}")
  endif()
endforeach()

add_executable(flowmq_package_consumer main.c)
target_link_libraries(flowmq_package_consumer PRIVATE FlowMQ::FlowMQ)

add_executable(flowmq_package_consumer_cpp consumer_cpp.cpp)
target_compile_features(flowmq_package_consumer_cpp PRIVATE cxx_std_17)
target_link_libraries(flowmq_package_consumer_cpp PRIVATE FlowMQ::FlowMQ)

enable_testing()
add_test(NAME flowmq_package_c11 COMMAND flowmq_package_consumer)
add_test(NAME flowmq_package_cpp17 COMMAND flowmq_package_consumer_cpp)
set_tests_properties(flowmq_package_c11 flowmq_package_cpp17
                     PROPERTIES LABELS "flowmq;installed-sdk" TIMEOUT 30)
