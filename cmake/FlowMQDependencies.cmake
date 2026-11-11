# Keep CMake and CI on the same released SDK set.
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
             "${CMAKE_CURRENT_LIST_DIR}/sdk-versions.json")
file(READ "${CMAKE_CURRENT_LIST_DIR}/sdk-versions.json" _flowmq_sdk_versions)
string(JSON FLOWMQ_BASE_VERSION GET "${_flowmq_sdk_versions}" salts-base)
string(JSON FLOWMQ_CMETA_VERSION GET "${_flowmq_sdk_versions}" cmeta)
string(JSON FLOWMQ_CNET_VERSION GET "${_flowmq_sdk_versions}" salts-cnet)
string(JSON FLOWMQ_UTILS_VERSION GET "${_flowmq_sdk_versions}" salts-utils)

find_package(SaltsBase ${FLOWMQ_BASE_VERSION} EXACT CONFIG REQUIRED
             PATHS "$ENV{SALTS_BASE_ROOT}" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
find_package(CMeta ${FLOWMQ_CMETA_VERSION} EXACT CONFIG REQUIRED
             PATHS "$ENV{CMETA_ROOT}" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
# salts-cnet publishes the SaltsCore CMake package.
find_package(SaltsCore ${FLOWMQ_CNET_VERSION} EXACT CONFIG REQUIRED
             PATHS "$ENV{SALTSCORE_ROOT}" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
find_package(SaltsUtils ${FLOWMQ_UTILS_VERSION} EXACT CONFIG REQUIRED
             PATHS "$ENV{SALTS_UTILS_ROOT}" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)

find_package(llhttp CONFIG REQUIRED)
find_package(GmSSL CONFIG REQUIRED)
