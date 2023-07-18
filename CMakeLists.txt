cmake_minimum_required(VERSION 3.22)
project(clearParquet)

set(CXX_STD_FLAGS "-std=c++17")
set(CXX_GROUPED_WARNING_FLAGS "-Werror -Wall -Wextra -Wno-ignored-qualifiers -Wno-unused-parameter -pedantic")
set(CXX_SPECIFIC_WARNING_FLAGS "-Wcast-align -Wcast-qual -Winit-self -Wlogical-op -Wunknown-pragmas -Wwrite-strings -Wno-deprecated-copy -Wno-pessimizing-move -Wno-implicit-fallthrough -Wno-stringop-truncation -Wno-unknown-pragmas -Wsuggest-override -Wcast-qual -Wtautological-compare")
set(CMAKE_CXX_FLAGS "${CXX_STD_FLAGS} ${CXX_GROUPED_WARNING_FLAGS} ${CXX_SPECIFIC_WARNING_FLAGS}")

set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_DEBUG "-g")

message("CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")
message("CMAKE_CXX_FLAGS_RELEASE is ${CMAKE_CXX_FLAGS_RELEASE}")
message("CMAKE_CXX_FLAGS_DEBUG is ${CMAKE_CXX_FLAGS_DEBUG}")
message("CMAKE_BUILD_TYPE is ${CMAKE_BUILD_TYPE}")

include_directories(core)

# Thrift
find_library(thrift_location NAMES libthrift.a)
message(STATUS ${thrift_location})
add_library(thrift STATIC IMPORTED)
set_target_properties(thrift PROPERTIES IMPORTED_LOCATION ${thrift_location})

# ZSTD
find_library(zstd_location NAMES libzstd.a)
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
find_library(snappy_location NAMES libsnappy.a)
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

# Tests
file(GLOB_RECURSE WRITER_TEST_SOURCES
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/test/writer.cpp)
file(GLOB_RECURSE WRITER_TEST_HEADERS
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/test/writer.hpp)

set(ALL_WRITER_TEST_SOURCES ${WRITER_TEST_HEADERS} ${WRITER_TEST_SOURCES})
set(ALL_WRITER_TEST_LIBRARIES clearParquet thrift)
if(zstd_location)
    set(ALL_WRITER_TEST_LIBRARIES ${ALL_WRITER_TEST_LIBRARIES} zstd)
endif()
if(snappy_location)
    set(ALL_WRITER_TEST_LIBRARIES ${ALL_WRITER_TEST_LIBRARIES} snappy)
endif()
add_executable(writer ${ALL_WRITER_TEST_SOURCES})
target_link_libraries(writer ${ALL_WRITER_TEST_LIBRARIES})
set_target_properties(writer PROPERTIES OUTPUT_NAME writer${BUILD_POSTFIX})
set_target_properties(writer PROPERTIES LINKER_LANGUAGE CXX)

# Clang format fixup
add_custom_target(format COMMAND ${CMAKE_COMMAND} -E env clang-format -i ${CMAKE_SOURCE_DIR}/core/*.hpp)

# Clang tidy check
add_custom_target(tidy COMMAND ${CMAKE_COMMAND} -E env clang-tidy ${CMAKE_SOURCE_DIR}/core/*.hpp -extra-arg=-std=c++17)


# Arrow Tests
#file(GLOB_RECURSE ARROW_WRITER_TEST_SOURCES
#        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
#        ${CMAKE_SOURCE_DIR}/test/arrowWriter.cpp)
#file(GLOB_RECURSE ARROW_WRITER_TEST_HEADERS
#        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
#        ${CMAKE_SOURCE_DIR}/test/arrowWriter.hpp)
#
#set(ALL_ARROW_WRITER_TEST_SOURCES ${ARROW_WRITER_TEST_HEADERS} ${ARROW_WRITER_TEST_SOURCES})
#set(ALL_ARROW_WRITER_TEST_LIBRARIES parquet arrow)
#add_executable(arrowWriter ${ALL_ARROW_WRITER_TEST_SOURCES})
#target_link_libraries(arrowWriter ${ALL_ARROW_WRITER_TEST_LIBRARIES})
#set_target_properties(arrowWriter PROPERTIES OUTPUT_NAME arrowWriter${BUILD_POSTFIX})
#set_target_properties(arrowWriter PROPERTIES LINKER_LANGUAGE CXX)
