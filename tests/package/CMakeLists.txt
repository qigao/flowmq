cmake_minimum_required(VERSION 3.20)
project(flowmq_package_consumer C)

if(NOT DEFINED ENV{FLOWMQ_ROOT} OR "$ENV{FLOWMQ_ROOT}" STREQUAL "")
  message(FATAL_ERROR "FLOWMQ_ROOT must name the installed FlowMQ prefix")
endif()

find_package(FlowMQ 1.1 CONFIG REQUIRED
             PATHS "$ENV{FLOWMQ_ROOT}" NO_DEFAULT_PATH)

add_executable(flowmq_package_consumer main.c)
target_link_libraries(flowmq_package_consumer PRIVATE FlowMQ::FlowMQ)
