cmake_minimum_required(VERSION 3.25)

if(POLICY CMP0144)
  cmake_policy(SET CMP0144 NEW)
endif()

project(
  FlowMQ
  VERSION 1.3.1
  DESCRIPTION "Pattern-oriented messaging library for C"
  HOMEPAGE_URL "https://github.com/qigao/flowmq"
  LANGUAGES C)

if(ANDROID)
  add_link_options("LINKER:-z,max-page-size=16384" "LINKER:-z,common-page-size=16384")
endif()

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
  add_compile_options($<$<COMPILE_LANGUAGE:C>:/experimental:c11atomics>)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/CMakeOptions.cmake")
if(BUILD_TESTS)
  enable_language(CXX)
  enable_testing()
endif()
include(Sanitizers)
include(CmakeUtils)


include(FlowMQDependencies)

# A same-architecture Windows llvm-mingw toolchain also sets CMAKE_CROSSCOMPILING.
if(CMAKE_CROSSCOMPILING AND
   (NOT CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME OR
    NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR))
  foreach(sdk IN ITEMS SALTS_BASE CMETA SALTS_UTILS)
    file(TO_CMAKE_PATH "$ENV{${sdk}_HOST_ROOT}" ${sdk}_HOST_ROOT)
    if(NOT IS_DIRECTORY "${${sdk}_HOST_ROOT}")
      message(FATAL_ERROR "${sdk}_HOST_ROOT must name an installed host SDK")
    endif()
  endforeach()
else()
  foreach(sdk IN ITEMS SALTS_BASE CMETA SALTS_UTILS)
    file(TO_CMAKE_PATH "$ENV{${sdk}_ROOT}" ${sdk}_HOST_ROOT)
  endforeach()
endif()

find_program(FLOWMQ_IDLC_EXECUTABLE NAMES salts-idlc
             PATHS "${SALTS_UTILS_HOST_ROOT}/bin"
             NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH NO_CACHE REQUIRED)
set(SaltsUtils_IDLC_EXECUTABLE "${FLOWMQ_IDLC_EXECUTABLE}")
set(SaltsUtils_IDLC_HOST_SALTS_BASE_ROOT "${SALTS_BASE_HOST_ROOT}")
set(SaltsUtils_IDLC_HOST_CMETA_ROOT "${CMETA_HOST_ROOT}")

set(FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS
    "PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::CNet>"
    "PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::Core>"
    "PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::DataBind>")
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
  set_target_properties(flowmq PROPERTIES PREFIX "")
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
                      PUBLIC Salts::Core Salts::DataBind
                      PRIVATE CMeta::CSTL CMeta::CMeta Salts::CNet Salts::NativeIO)
if(BUILD_TESTS)
  get_target_property(_flowmq_runtime_links flowmq LINK_LIBRARIES)
  foreach(_flowmq_frontend_target IN ITEMS
          Salts::IDL
          Salts::Schema
          CMeta::CFlow)
    if("${_flowmq_frontend_target}" IN_LIST _flowmq_runtime_links)
      message(FATAL_ERROR
              "FlowMQ runtime must not link control-plane target "
              "${_flowmq_frontend_target}")
    endif()
  endforeach()
endif()
add_dependencies(flowmq flowmq_media_provider_codegen)

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
