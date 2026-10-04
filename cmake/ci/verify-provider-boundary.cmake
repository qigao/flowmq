cmake_minimum_required(VERSION 3.20)

get_filename_component(FLOWMQ_ROOT "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)

file(READ "${FLOWMQ_ROOT}/vcpkg.json" FLOWMQ_VCPKG_MANIFEST)
file(READ "${FLOWMQ_ROOT}/CMakeLists.txt" FLOWMQ_TOP_LEVEL_CMAKE)

foreach(_provider_manifest IN ITEMS
        "\"boringssl\""
        "\"openssl\""
        "\"gmssl\"")
  string(FIND "${FLOWMQ_VCPKG_MANIFEST}" "${_provider_manifest}" _provider_manifest_pos)
  if(NOT _provider_manifest_pos EQUAL -1)
    message(FATAL_ERROR
            "FlowMQ must not own a TLS provider in vcpkg.json: ${_provider_manifest}")
  endif()
endforeach()

foreach(_provider_cmake IN ITEMS
        "find_package(OpenSSL"
        "find_package(GmSSL"
        "OpenSSL::SSL"
        "OpenSSL::Crypto"
        "GmSSL::GmSSL")
  string(FIND "${FLOWMQ_TOP_LEVEL_CMAKE}" "${_provider_cmake}" _provider_cmake_pos)
  if(NOT _provider_cmake_pos EQUAL -1)
    message(FATAL_ERROR
            "FlowMQ must consume TLS only through Salts::CNet; found direct provider token: ${_provider_cmake}")
  endif()
endforeach()

message(STATUS "FlowMQ TLS provider boundary: Salts::CNet owns provider selection")
