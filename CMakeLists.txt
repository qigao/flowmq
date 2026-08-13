cmake_minimum_required(VERSION 3.20)

if(POLICY CMP0144)
  cmake_policy(SET CMP0144 NEW)
endif()

project(
  FlowMQ
  VERSION 1.0.0
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

find_package(TurboUtils CONFIG REQUIRED PATHS "${TURBO_UTILS_ROOT}")
find_package(TurboParser CONFIG REQUIRED PATHS "${TURBO_PARSER_ROOT}")
find_package(TurboNet CONFIG REQUIRED PATHS "${TURBO_NET_ROOT}")
find_package(OpenSSL CONFIG REQUIRED)

find_program(FLOWMQ_TBE_COMPILER_EXECUTABLE NAMES tbe_compiler
             HINTS "${TURBO_PARSER_ROOT}/bin")
if(NOT FLOWMQ_TBE_COMPILER_EXECUTABLE)
  message(FATAL_ERROR
    "FlowMQ media-provider contract requires the TurboParser tbe_compiler")
endif()
set(FLOWMQ_MEDIA_PROVIDER_SCHEMA
    "${CMAKE_CURRENT_SOURCE_DIR}/flowmq/application/schema/flowmq_media_provider_v1.schema")
set(FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR
    "${CMAKE_CURRENT_BINARY_DIR}/generated/media_provider")
set(FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER
    "${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}/flowmq_media_provider_v1.h")
set(FLOWMQ_MEDIA_PROVIDER_GENERATED_SOURCE
    "${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}/flowmq_media_provider_v1.c")
file(MAKE_DIRECTORY "${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}")
add_custom_command(
  OUTPUT "${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}"
         "${FLOWMQ_MEDIA_PROVIDER_GENERATED_SOURCE}"
  COMMAND "${CMAKE_COMMAND}" -E env
          "PATH=$<TARGET_FILE_DIR:TurboParser::DataBind>\;$<TARGET_FILE_DIR:TurboParser::Parser>\;$<TARGET_FILE_DIR:TurboUtils::Core>\;$<TARGET_FILE_DIR:OpenSSL::Crypto>\;$ENV{PATH}"
          "${FLOWMQ_TBE_COMPILER_EXECUTABLE}"
          "${FLOWMQ_MEDIA_PROVIDER_SCHEMA}"
          --lang c
          --output "${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}"
          --source-output "${FLOWMQ_MEDIA_PROVIDER_GENERATED_SOURCE}"
  DEPENDS "${FLOWMQ_MEDIA_PROVIDER_SCHEMA}"
  COMMENT "Generating FlowMQ media-provider v1 typed contract"
  VERBATIM)

get_target_property(FLOWMQ_OPENSSL_RELEASE_DLL OpenSSL::SSL IMPORTED_LOCATION_RELEASE)
if(WIN32 AND FLOWMQ_OPENSSL_RELEASE_DLL)
  get_filename_component(FLOWMQ_OPENSSL_RELEASE_RUNTIME_DIR
                         "${FLOWMQ_OPENSSL_RELEASE_DLL}" DIRECTORY)
endif()

set(FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS
    "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboNet::CoroNet>"
    "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboParser::Parser>"
    "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboParser::DataBind>"
    "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboUtils::Core>")
if(WIN32 AND FLOWMQ_OPENSSL_RELEASE_RUNTIME_DIR)
  list(APPEND FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS
       "PATH=path_list_prepend:${FLOWMQ_OPENSSL_RELEASE_RUNTIME_DIR}")
endif()
if(MSVC)
  get_filename_component(FLOWMQ_COMPILER_RUNTIME_DIR "${CMAKE_C_COMPILER}" DIRECTORY)
  list(APPEND FLOWMQ_TEST_ENVIRONMENT_MODIFICATIONS
       "PATH=path_list_prepend:${FLOWMQ_COMPILER_RUNTIME_DIR}")
endif()

add_subdirectory(flowmq/protocol)
add_subdirectory(flowmq/runtime)

# Protocol, Core, and Transport remain build-tree static libraries. The
# installed product compiles the same sources into one shared-library ABI.
add_library(flowmq SHARED
            ${FLOWMQ_PROTOCOL_SOURCES}
            ${FLOWMQ_CORE_SOURCES}
            ${FLOWMQ_TRANSPORT_SOURCES}
            ${FLOWMQ_MEDIA_PROVIDER_GENERATED_SOURCE}
            ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/application/src/flowmq_media_provider.c
            ${FLOWMQ_PROTOCOL_HEADERS}
            ${FLOWMQ_RUNTIME_PUBLIC_HEADERS}
            ${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}
            ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/include/flowmq_media_provider.h
            ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/include/flowmq.h)
cmake_config_target(
  flowmq
  ALIAS FlowMQ::FlowMQ
  FOLDER "flowmq"
  EXPORT_NAME FlowMQ)
target_compile_definitions(flowmq
  PRIVATE SHARED_CXX TBE_GENERATED_BUILD_SHARED)
target_include_directories(
  flowmq
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/flowmq/protocol/include>
         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/flowmq/include>
         $<BUILD_INTERFACE:${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/flowmq/runtime/src)
target_link_libraries(flowmq
                      PUBLIC TurboUtils::Core TurboParser::Parser TurboParser::DataBind
                      PRIVATE TurboNet::CoroNet OpenSSL::SSL)

if(BUILD_TESTS)
  add_subdirectory(flowmq/application/tests)
endif()

install(
  TARGETS flowmq
  EXPORT FlowMQTargets
  RUNTIME DESTINATION bin COMPONENT FlowMQRuntime
  LIBRARY DESTINATION lib COMPONENT FlowMQRuntime
  ARCHIVE DESTINATION lib COMPONENT FlowMQDevelopment
  INCLUDES DESTINATION include)
install(FILES ${FLOWMQ_PROTOCOL_HEADERS}
              ${FLOWMQ_RUNTIME_PUBLIC_HEADERS}
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
