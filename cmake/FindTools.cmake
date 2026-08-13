# Find re2c
find_program(RE2C_EXECUTABLE re2c)
if(NOT RE2C_EXECUTABLE)
    message(WARNING "re2c not found - some lexers might not be generated")
endif()

# Find lemon (use built-in or system)
# Check for project-provided lemon target first (prefer direct target over alias)
if(TARGET lemon)
    set(LEMON_EXECUTABLE $<TARGET_FILE:lemon>)
    set(LEMON_DEPENDS lemon)
    message(STATUS "Using project-provided lemon target")
else()
    # Fallback to system lemon only if project target not available
    find_program(LEMON_EXECUTABLE lemon)
    if(NOT LEMON_EXECUTABLE)
        message(WARNING "lemon not found - some parsers might not be generated")
    else()
        message(WARNING "Using system lemon - version mismatch may occur!")
    endif()
    set(LEMON_DEPENDS "")
endif()

# Set path to lemon parser template
set(LEMPAR "${CMAKE_SOURCE_DIR}/tools/lemon/lempar.c" CACHE PATH "Path to lemon parser template")

# Note: These variables are set in the root scope and will be inherited 
# by all subdirectories added via add_subdirectory().

message(STATUS "Tools detection:")
message(STATUS "  re2c: ${RE2C_EXECUTABLE}")
message(STATUS "  lemon: ${LEMON_EXECUTABLE}")
message(STATUS "  lempar: ${LEMPAR}")