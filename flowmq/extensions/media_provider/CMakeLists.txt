set(FLOWMQ_MEDIA_PROVIDER_SCHEMA
    "${CMAKE_CURRENT_SOURCE_DIR}/schema/flowmq_media_provider_v1.schema")
set(FLOWMQ_MEDIA_PROVIDER_SOURCE
    "${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_media_provider.c")
set(FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR
    "${CMAKE_CURRENT_BINARY_DIR}/generated/media_provider")
set(FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER
    "${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}/flowmq_media_provider_v1.h")

file(MAKE_DIRECTORY "${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}")

if(CMAKE_HOST_WIN32)
  set(_idlc_library_path PATH)
  set(_idlc_library_dir bin)
elseif(CMAKE_HOST_APPLE)
  set(_idlc_library_path DYLD_LIBRARY_PATH)
  set(_idlc_library_dir lib)
else()
  set(_idlc_library_path LD_LIBRARY_PATH)
  set(_idlc_library_dir lib)
endif()
set(_idlc_environment)
foreach(sdk IN ITEMS SALTS_UTILS CMETA SALTS_BASE)
  list(APPEND _idlc_environment --modify
    "${_idlc_library_path}=path_list_prepend:${${sdk}_HOST_ROOT}/${_idlc_library_dir}")
endforeach()
add_custom_command(
  OUTPUT "${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}"
  COMMAND "${CMAKE_COMMAND}" -E env ${_idlc_environment} --
          "${SaltsUtils_IDLC_EXECUTABLE}" "${FLOWMQ_MEDIA_PROVIDER_SCHEMA}"
          --lang c --binary-codec --output "${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}"
  DEPENDS "${FLOWMQ_MEDIA_PROVIDER_SCHEMA}" "${SaltsUtils_IDLC_EXECUTABLE}"
  COMMENT "Generating FlowMQ media-provider FMP/1 DataBind contract"
  VERBATIM)

set_source_files_properties("${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}"
                            PROPERTIES GENERATED TRUE)

set(FLOWMQ_MEDIA_PROVIDER_CODEGEN_TARGET flowmq_media_provider_codegen)
add_custom_target(${FLOWMQ_MEDIA_PROVIDER_CODEGEN_TARGET}
                  DEPENDS "${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}")

set(FLOWMQ_MEDIA_PROVIDER_SCHEMA ${FLOWMQ_MEDIA_PROVIDER_SCHEMA} PARENT_SCOPE)
set(FLOWMQ_MEDIA_PROVIDER_SOURCE ${FLOWMQ_MEDIA_PROVIDER_SOURCE} PARENT_SCOPE)
set(FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR
    ${FLOWMQ_MEDIA_PROVIDER_GENERATED_DIR}
    PARENT_SCOPE)
set(FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER
    ${FLOWMQ_MEDIA_PROVIDER_GENERATED_HEADER}
    PARENT_SCOPE)
set(FLOWMQ_MEDIA_PROVIDER_CODEGEN_TARGET
    ${FLOWMQ_MEDIA_PROVIDER_CODEGEN_TARGET}
    PARENT_SCOPE)
