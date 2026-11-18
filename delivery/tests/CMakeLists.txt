cmake_add_test(test_flowmq_delivery
  SOURCES test_delivery.c
  LIBS FlowMQ::Delivery FlowMQ::FlowMQ Salts::TinyTest
  FOLDER "delivery/tests")
find_file(delivery_sqlite_driver
  NAMES "turbodb_driver_sqlite${CMAKE_SHARED_MODULE_SUFFIX}"
  PATHS "${TurboDB_DRIVER_DIR}" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH REQUIRED)
if(WIN32)
  # The plugin loader searches the application directory for dependent DLLs.
  find_package(unofficial-sqlite3 CONFIG REQUIRED)
  add_custom_command(TARGET test_flowmq_delivery POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      "$<TARGET_FILE:unofficial::sqlite3::sqlite3>" "$<TARGET_FILE_DIR:test_flowmq_delivery>"
    VERBATIM)
endif()
add_executable(flowmq_delivery_crash_node crash_node.c)
target_link_libraries(flowmq_delivery_crash_node PRIVATE FlowMQ::Delivery)
if(APPLE)
  # The child loads TurboDB's @rpath/libdata_bind.dylib through its own
  # executable runpath. It only links the Delivery API directly.
  set_property(TARGET flowmq_delivery_crash_node APPEND PROPERTY BUILD_RPATH
    "$<TARGET_FILE_DIR:Salts::DataBind>")
endif()
add_test(NAME test_flowmq_delivery_crash COMMAND ${CMAKE_COMMAND}
  "-DNODE=$<TARGET_FILE:flowmq_delivery_crash_node>" -P "${CMAKE_CURRENT_SOURCE_DIR}/crash_recovery.cmake")
set_tests_properties(test_flowmq_delivery test_flowmq_delivery_crash PROPERTIES
  LABELS "flowmq;flowmq-delivery"
  ENVIRONMENT "ORM_SQLITE_PLUGIN=${delivery_sqlite_driver}"
  ENVIRONMENT_MODIFICATION "${FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS}")
