cmake_minimum_required(VERSION 3.20)

project(FlowMQCFlowQualification LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

if(NOT DEFINED ENV{FLOWMQ_CFLOW_SALTS_ROOT} OR
   "$ENV{FLOWMQ_CFLOW_SALTS_ROOT}" STREQUAL "")
  message(FATAL_ERROR
          "FLOWMQ_CFLOW_SALTS_ROOT must name the released Salts qualification SDK")
endif()

file(TO_CMAKE_PATH "$ENV{FLOWMQ_CFLOW_SALTS_ROOT}"
     FLOWMQ_CFLOW_SALTS_ROOT_PATH)
if(NOT IS_DIRECTORY "${FLOWMQ_CFLOW_SALTS_ROOT_PATH}")
  message(FATAL_ERROR
          "FLOWMQ_CFLOW_SALTS_ROOT is not a directory: ${FLOWMQ_CFLOW_SALTS_ROOT_PATH}")
endif()

find_package(
  Salts 1.6 CONFIG REQUIRED
  PATHS "${FLOWMQ_CFLOW_SALTS_ROOT_PATH}"
  NO_DEFAULT_PATH
  NO_CMAKE_FIND_ROOT_PATH)

if(NOT TARGET Salts::CFlow)
  message(FATAL_ERROR "released Salts SDK does not export Salts::CFlow")
endif()
if(NOT TARGET Salts::TinyTest)
  message(FATAL_ERROR "released Salts SDK does not export Salts::TinyTest")
endif()

add_executable(
  test_flowmq_cflow_qualification
  test_flowmq_cflow_qualification.c)
target_link_libraries(
  test_flowmq_cflow_qualification
  PRIVATE Salts::CFlow Salts::TinyTest)

enable_testing()
add_test(
  NAME test_flowmq_cflow_qualification
  COMMAND test_flowmq_cflow_qualification)
