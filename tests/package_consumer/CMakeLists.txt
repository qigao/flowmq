cmake_minimum_required(VERSION 3.20)
project(FlowMQPackageConsumer LANGUAGES C)

find_package(FlowMQ CONFIG REQUIRED COMPONENTS FlowMQ)

add_executable(flowmq_package_consumer main.c)
target_link_libraries(flowmq_package_consumer PRIVATE FlowMQ::FlowMQ)
