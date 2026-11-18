find_package(TurboDB CONFIG REQUIRED PATHS "$ENV{TURBODB_ROOT}"
             NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
file(GLOB delivery_sources CONFIGURE_DEPENDS src/*.c)
source_group("Source" FILES ${delivery_sources})
add_library(flowmq_delivery STATIC ${delivery_sources})
cmake_config_target(flowmq_delivery ALIAS FlowMQ::Delivery EXPORT_NAME Delivery FOLDER flowmq)
target_include_directories(flowmq_delivery PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:include>)
target_link_libraries(flowmq_delivery PUBLIC Orm::C)
install(TARGETS flowmq_delivery EXPORT FlowMQDeliveryTargets ARCHIVE DESTINATION lib)
install(FILES include/flowmq_delivery.h DESTINATION include)
install(EXPORT FlowMQDeliveryTargets NAMESPACE FlowMQ:: DESTINATION lib/cmake/FlowMQ)
export(EXPORT FlowMQDeliveryTargets FILE "${PROJECT_BINARY_DIR}/FlowMQDeliveryTargets.cmake"
       NAMESPACE FlowMQ::)
if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
