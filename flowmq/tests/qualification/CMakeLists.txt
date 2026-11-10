cmake_minimum_required(VERSION 3.20)

project(FlowMQCFlowQualification LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

find_package(CMeta 0.1.4 EXACT CONFIG REQUIRED
  PATHS "$ENV{CMETA_ROOT}" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)

add_executable(
  test_flowmq_cflow_qualification
  test_flowmq_cflow_qualification.c)
target_link_libraries(
  test_flowmq_cflow_qualification
  PRIVATE CMeta::CFlow Salts::TinyTest)

enable_testing()
add_test(
  NAME test_flowmq_cflow_qualification
  COMMAND test_flowmq_cflow_qualification)
