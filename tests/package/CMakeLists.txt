cmake_minimum_required(VERSION 3.20)
project(flowmq_package_consumer C)

if(NOT DEFINED ENV{FLOWMQ_ROOT} OR "$ENV{FLOWMQ_ROOT}" STREQUAL "")
  message(FATAL_ERROR "FLOWMQ_ROOT must name the installed FlowMQ prefix")
endif()

file(TO_CMAKE_PATH "$ENV{FLOWMQ_ROOT}" FLOWMQ_ROOT_PATH)
find_package(FlowMQ 1.1 CONFIG REQUIRED
             PATHS "${FLOWMQ_ROOT_PATH}" NO_DEFAULT_PATH)

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
        Salts::NativeIO)
  if(_flowmq_forbidden_dependency IN_LIST _flowmq_public_links)
    message(FATAL_ERROR
            "installed FlowMQ::FlowMQ leaks obsolete/private dependency "
            "${_flowmq_forbidden_dependency}: ${_flowmq_public_links}")
  endif()
endforeach()

add_executable(flowmq_package_consumer main.c)
target_link_libraries(flowmq_package_consumer PRIVATE FlowMQ::FlowMQ)
