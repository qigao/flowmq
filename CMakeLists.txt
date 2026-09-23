cmake_minimum_required(VERSION 3.20)

if(POLICY CMP0144)
  cmake_policy(SET CMP0144 NEW)
endif()

project(
  FlowMQ
  VERSION 1.1.0
  DESCRIPTION "Pattern-oriented messaging library for C"
  HOMEPAGE_URL "https://github.com/qigao/flowmq"
  LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set_property(GLOBAL PROPERTY PREDEFINED_TARGETS_FOLDER "cmake")

if(MSVC)
  add_compile_options(/experimental:c11atomics)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/CMakeOptions.cmake")
if(BUILD_TESTS)
  enable_testing()
endif()
include(Sanitizers)
include(CmakeUtils)


foreach(_flowmq_dependency_root IN ITEMS SALTS_ROOT SALTS_UTILS_ROOT)
  if(NOT DEFINED ENV{${_flowmq_dependency_root}} OR
     "$ENV{${_flowmq_dependency_root}}" STREQUAL "")
    message(FATAL_ERROR
            "${_flowmq_dependency_root} must name an installed dependency profile")
  endif()
  file(TO_CMAKE_PATH "$ENV{${_flowmq_dependency_root}}"
       FLOWMQ_${_flowmq_dependency_root}_PATH)
  if(NOT IS_DIRECTORY "${FLOWMQ_${_flowmq_dependency_root}_PATH}")
    message(FATAL_ERROR
            "${_flowmq_dependency_root} is not a directory: $ENV{${_flowmq_dependency_root}}")
  endif()
endforeach()

if(CMAKE_CROSSCOMPILING)
  foreach(_flowmq_host_root IN ITEMS SALTS_HOST_ROOT SALTS_UTILS_HOST_ROOT)
    if(NOT DEFINED ENV{${_flowmq_host_root}} OR
       "$ENV{${_flowmq_host_root}}" STREQUAL "")
      message(FATAL_ERROR
              "${_flowmq_host_root} must name a host SDK profile for cross builds")
    endif()
  endforeach()
endif()

foreach(_flowmq_host_root IN ITEMS SALTS_HOST_ROOT SALTS_UTILS_HOST_ROOT)
  if(DEFINED ENV{${_flowmq_host_root}} AND
     NOT "$ENV{${_flowmq_host_root}}" STREQUAL "")
    file(TO_CMAKE_PATH "$ENV{${_flowmq_host_root}}"
         FLOWMQ_${_flowmq_host_root}_PATH)
  elseif(_flowmq_host_root STREQUAL "SALTS_HOST_ROOT")
    set(FLOWMQ_SALTS_HOST_ROOT_PATH "${FLOWMQ_SALTS_ROOT_PATH}")
  else()
    set(FLOWMQ_SALTS_UTILS_HOST_ROOT_PATH "${FLOWMQ_SALTS_UTILS_ROOT_PATH}")
  endif()
  if(NOT IS_DIRECTORY "${FLOWMQ_${_flowmq_host_root}_PATH}")
    message(FATAL_ERROR
            "${_flowmq_host_root} is not a directory: ${FLOWMQ_${_flowmq_host_root}_PATH}")
  endif()
endforeach()

find_package(
  Salts CONFIG REQUIRED
  PATHS "${FLOWMQ_SALTS_ROOT_PATH}"
  NO_DEFAULT_PATH
  NO_CMAKE_FIND_ROOT_PATH)
find_package(
  SaltsUtils CONFIG REQUIRED
  PATHS "${FLOWMQ_SALTS_UTILS_ROOT_PATH}"
  NO_DEFAULT_PATH
  NO_CMAKE_FIND_ROOT_PATH)
find_package(OpenSSL CONFIG REQUIRED)

unset(FLOWMQ_TBE_COMPILER_EXECUTABLE)
unset(FLOWMQ_TBE_COMPILER_EXECUTABLE CACHE)
find_program(FLOWMQ_TBE_COMPILER_EXECUTABLE NAMES tbe_compiler
             PATHS "${FLOWMQ_SALTS_UTILS_HOST_ROOT_PATH}/bin"
             NO_DEFAULT_PATH
             NO_CMAKE_FIND_ROOT_PATH)
if(NOT FLOWMQ_TBE_COMPILER_EXECUTABLE)
  message(FATAL_ERROR
    "FlowMQ media-provider contract requires the host SaltsUtils tbe_compiler")
endif()

set(FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS
    "PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::CNet>"
    "PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::Core>")
if(WIN32)
  list(APPEND FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS
       "PATH=path_list_prepend:$<TARGET_FILE_DIR:OpenSSL::SSL>")
endif()
if(MSVC)
  get_filename_component(FLOWMQ_COMPILER_RUNTIME_DIR "${CMAKE_C_COMPILER}" DIRECTORY)
  list(APPEND FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS
       "PATH=path_list_prepend:${FLOWMQ_COMPILER_RUNTIME_DIR}")
endif()

add_subdirectory(flowmq)

if(BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

# Protocol, Core, and Transport remain build-tree static libraries. The
# installed product compiles the same sources into one shared-library ABI.
add_library(flowmq SHARED
            ${FLOWMQ_PROTOCOL_SOURCES}
            ${FLOWMQ_CORE_SOURCES}
            ${FLOWMQ_TRANSPORT_SOURCES}
            ${FLOWMQ_MEDIA_PROVIDER_SOURCE}
            ${FLOWMQ_PROTOCOL_HEADERS}
            ${FLOWMQ_CORE_HEADERS}
            ${FLOWMQ_TRANSPORT_HEADERS}
            ${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}
            ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/include/flowmq_media_provider.h
            ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/include/flowmq.h)
cmake_config_target(
  flowmq
  ALIAS FlowMQ::FlowMQ
  FOLDER "flowmq"
  EXPORT_NAME FlowMQ)
if(WIN32)
  target_compile_definitions(
    flowmq
    PRIVATE "FLOWMQ_API=__declspec(dllexport)"
    INTERFACE "FLOWMQ_API=__declspec(dllimport)")
endif()
target_include_directories(
  flowmq
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/flowmq/include>
         $<BUILD_INTERFACE:${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/src/protocol
          ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/src/protocol/stream
          ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/src/core/pattern
          ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/src/core/session
          ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/src/transport/cnet)
target_link_libraries(flowmq
                      PUBLIC Salts::Core Salts::TbeSchema
                      PRIVATE Salts::CSTL Salts::CMeta Salts::CNet)
if(TARGET flowmq_media_provider_codegen)
  add_dependencies(flowmq flowmq_media_provider_codegen)
endif()

install(
  TARGETS flowmq
  EXPORT FlowMQTargets
  RUNTIME DESTINATION bin COMPONENT FlowMQRuntime
  LIBRARY DESTINATION lib COMPONENT FlowMQRuntime
  ARCHIVE DESTINATION lib COMPONENT FlowMQDevelopment
  INCLUDES DESTINATION include)
install(FILES ${FLOWMQ_PROTOCOL_HEADERS}
              ${FLOWMQ_CORE_HEADERS}
              ${FLOWMQ_TRANSPORT_HEADERS}
              "${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}"
              "${CMAKE_CURRENT_SOURCE_DIR}/flowmq/include/flowmq_media_provider.h"
              "${CMAKE_CURRENT_SOURCE_DIR}/flowmq/include/flowmq.h"
        DESTINATION include
        COMPONENT FlowMQDevelopment)
install(FILES "${FLOWMQ_MEDIA_PROVIDER_SCHEMA}"
        DESTINATION share/flowmq/schema
        COMPONENT FlowMQDevelopment)

include(CMakePackageConfigHelpers)
configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/FlowMQConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/FlowMQConfig.cmake"
  INSTALL_DESTINATION "lib/cmake/FlowMQ")
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/FlowMQConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

export(
  EXPORT FlowMQTargets
  FILE "${CMAKE_CURRENT_BINARY_DIR}/FlowMQTargets.cmake"
  NAMESPACE FlowMQ::)
install(
  EXPORT FlowMQTargets
  FILE FlowMQTargets.cmake
  NAMESPACE FlowMQ::
  DESTINATION "lib/cmake/FlowMQ"
  COMPONENT FlowMQDevelopment)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/FlowMQConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/FlowMQConfigVersion.cmake"
        DESTINATION "lib/cmake/FlowMQ"
        COMPONENT FlowMQDevelopment)
