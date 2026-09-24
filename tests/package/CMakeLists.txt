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
string(FIND "${_flowmq_public_links}" "CFlow" _flowmq_cflow_link_index)
if(NOT _flowmq_cflow_link_index EQUAL -1)
  message(FATAL_ERROR
          "installed FlowMQ::FlowMQ must not expose CFlow: ${_flowmq_public_links}")
endif()

add_executable(flowmq_package_consumer main.c)
target_link_libraries(flowmq_package_consumer PRIVATE FlowMQ::FlowMQ)
