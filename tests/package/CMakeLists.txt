cmake_minimum_required(VERSION 3.20)
project(flowmq_package_consumer C)

if(NOT DEFINED ENV{FLOWMQ_ROOT} OR "$ENV{FLOWMQ_ROOT}" STREQUAL "")
  message(FATAL_ERROR "FLOWMQ_ROOT must name the installed FlowMQ prefix")
endif()

find_package(FlowMQ 1.1 CONFIG REQUIRED
             PATHS "$ENV{FLOWMQ_ROOT}" NO_DEFAULT_PATH)

get_target_property(_flowmq_public_links
                    FlowMQ::FlowMQ INTERFACE_LINK_LIBRARIES)
if(NOT _flowmq_public_links)
  set(_flowmq_public_links "")
endif()
foreach(_flowmq_private_dependency IN ITEMS Salts::CMeta Salts::CFlow)
  string(FIND "${_flowmq_public_links}"
              "${_flowmq_private_dependency}"
              _flowmq_private_dependency_index)
  if(NOT _flowmq_private_dependency_index EQUAL -1)
    message(FATAL_ERROR
            "installed FlowMQ::FlowMQ leaks private dependency "
            "${_flowmq_private_dependency}: ${_flowmq_public_links}")
  endif()
endforeach()

add_executable(flowmq_package_consumer main.c)
target_link_libraries(flowmq_package_consumer PRIVATE FlowMQ::FlowMQ)
