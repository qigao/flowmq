cmake_minimum_required(VERSION 3.20)

function(flowmq_read path out_var)
  if(NOT EXISTS "${CMAKE_CURRENT_LIST_DIR}/../../${path}")
    message(FATAL_ERROR "missing dependency-contract input: ${path}")
  endif()
  file(READ "${CMAKE_CURRENT_LIST_DIR}/../../${path}" text)
  set(${out_var} "${text}" PARENT_SCOPE)
endfunction()

function(flowmq_reject_regex label text regex)
  if("${text}" MATCHES "${regex}")
    message(FATAL_ERROR
      "FlowMQ first-party dependency contract forbids ${label}: ${regex}")
  endif()
endfunction()

flowmq_read("CMakeLists.txt" root_cmake)
flowmq_read("cmake/FlowMQConfig.cmake.in" package_config)
flowmq_read("packaging/nuget/FlowMQ.Native.csproj" nuget_project)
flowmq_read(".github/workflows/vcpkg-cache-contract.yml" cache_workflow)
flowmq_read(".github/workflows/native-sdk-release.yml" sdk_workflow)

foreach(text_var IN ITEMS root_cmake package_config)
  flowmq_reject_regex(
    "a versioned Salts find_package/find_dependency"
    "${${text_var}}"
    "(find_package|find_dependency)\\(Salts[ ]+[0-9]")
  flowmq_reject_regex(
    "a versioned SaltsUtils find_package/find_dependency"
    "${${text_var}}"
    "(find_package|find_dependency)\\(SaltsUtils[ ]+[0-9]")
  flowmq_reject_regex(
    "EXACT on a first-party dependency"
    "${${text_var}}"
    "(Salts|SaltsUtils)[^\\n\\r]*EXACT")
endforeach()

string(FIND "${nuget_project}"
  "<PackageReference Include=\"Salts.Native\" Version=\"*\" />"
  salts_latest_index)
if(salts_latest_index EQUAL -1)
  message(FATAL_ERROR
    "FlowMQ.Native must consume Salts.Native with Version=\"*\"")
endif()

string(FIND "${nuget_project}"
  "<PackageReference Include=\"SaltsUtils.Native\" Version=\"*\" />"
  salts_utils_latest_index)
if(salts_utils_latest_index EQUAL -1)
  message(FATAL_ERROR
    "FlowMQ.Native must consume SaltsUtils.Native with Version=\"*\"")
endif()

foreach(text_var IN ITEMS cache_workflow sdk_workflow)
  flowmq_reject_regex(
    "a fixed Salts.Native PackageReference"
    "${${text_var}}"
    "PackageReference Include=\"Salts[.]Native\" Version=\"\\[[0-9]")
  flowmq_reject_regex(
    "a fixed SaltsUtils.Native PackageReference"
    "${${text_var}}"
    "PackageReference Include=\"SaltsUtils[.]Native\" Version=\"\\[[0-9]")
  flowmq_reject_regex(
    "a pinned SALTS_SDK_VERSION environment variable"
    "${${text_var}}"
    "SALTS_SDK_VERSION:[ ]*[\"']?[0-9]")
  flowmq_reject_regex(
    "a pinned SALTS_UTILS_SDK_VERSION environment variable"
    "${${text_var}}"
    "SALTS_UTILS_SDK_VERSION:[ ]*[\"']?[0-9]")
endforeach()

message(STATUS
  "FlowMQ first-party dependency contract verified: Salts/SaltsUtils are unversioned/latest")
