cmake_minimum_required(VERSION 3.22)
project(clearParquet)

set(CMAKE_CXX_STANDARD 17)
if (MSVC)
    set(CXX_GROUPED_WARNING_FLAGS "/WX /permissive-")
    set(CXX_SPECIFIC_WARNING_FLAGS "/Zc:strictStrings- /w14650 /wd4068 /w34701 /EHsc")
else()
    set(CXX_GROUPED_WARNING_FLAGS "-Werror -Wall -Wextra -Wno-ignored-qualifiers -Wno-unused-parameter -pedantic -Wshadow -fno-rtti")
    set(CXX_SPECIFIC_WARNING_FLAGS "-Wcast-align -Wcast-qual -Winit-self -Wlogical-op -Wunknown-pragmas -Wwrite-strings -Wno-deprecated-copy -Wno-pessimizing-move -Wno-implicit-fallthrough -Wno-stringop-truncation -Wno-unknown-pragmas -Wsuggest-override -Wcast-qual -Wtautological-compare")
endif()
set(CMAKE_CXX_FLAGS "${CXX_STD_FLAGS} ${CXX_GROUPED_WARNING_FLAGS} ${CXX_SPECIFIC_WARNING_FLAGS}")

if(MSVC)
    set(CMAKE_CXX_FLAGS_RELEASE "/O2")
    set(CMAKE_CXX_FLAGS_DEBUG "/Zi")
else()
    set(CMAKE_CXX_FLAGS_RELEASE "-O3")
    set(CMAKE_CXX_FLAGS_DEBUG "-g")
endif()

message("CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")
message("CMAKE_CXX_FLAGS_RELEASE is ${CMAKE_CXX_FLAGS_RELEASE}")
message("CMAKE_CXX_FLAGS_DEBUG is ${CMAKE_CXX_FLAGS_DEBUG}")
message("CMAKE_BUILD_TYPE is ${CMAKE_BUILD_TYPE}")

include_directories(core)

# Set these variables accordingly
set(ZSTD_BASE "C:/DevPackages/vcpkg/installed/x64-windows/")
set(SNAPPY_BASE "C:/DevPackages/vcpkg/installed/x64-windows/")

# ZSTD
if(MSVC)
    find_library(zstd_location NAMES zstd.lib HINTS ${ZSTD_BASE}/lib)
else()
    find_library(zstd_location NAMES libzstd.a)
endif()
message(STATUS ${zstd_location})
if(NOT zstd_location)
    message("-- Disabling ZSTD options in code.")
else()
    message("-- Enabling ZSTD options in code.")
    add_definitions(-DPARQUET_ZSTD_COMPRESSION=1)
    add_library(zstd STATIC IMPORTED)
    set_target_properties(zstd PROPERTIES IMPORTED_LOCATION ${zstd_location})
endif()

# SNAPPY
if(MSVC)
    find_library(snappy_location NAMES snappy.lib HINTS ${SNAPPY_BASE}/lib)
else()
    find_library(snappy_location NAMES libsnappy.a)
endif()
message(STATUS ${snappy_location})
if(NOT snappy_location)
    message("-- Disabling SNAPPY options in code.")
else()
    message("-- Enabling SNAPPY options in code.")
    add_definitions(-DPARQUET_SNAPPY_COMPRESSION=1)
    add_library(snappy STATIC IMPORTED)
    set_target_properties(snappy PROPERTIES IMPORTED_LOCATION ${snappy_location})
endif()

add_subdirectory(core)

# Clang format fixup
add_custom_target(format COMMAND ${CMAKE_COMMAND} -E env clang-format -i ${CMAKE_SOURCE_DIR}/core/*.hpp)

# Clang tidy check
add_custom_target(tidy COMMAND ${CMAKE_COMMAND} -E env clang-tidy ${CMAKE_SOURCE_DIR}/core/*.hpp -extra-arg=-std=c++17)

add_subdirectory(catch2)

add_subdirectory(test)

