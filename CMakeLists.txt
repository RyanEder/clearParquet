cmake_minimum_required(VERSION 3.22)
project(clearParquet)

set(CXX_STD_FLAGS "-std=c++17")
set(CXX_GROUPED_WARNING_FLAGS "-Werror -Wall -Wextra -Wno-ignored-qualifiers -Wno-unused-parameter -pedantic")
set(CXX_SPECIFIC_WARNING_FLAGS "-Wcast-align -Wcast-qual -Winit-self -Wlogical-op -Wunknown-pragmas -Wwrite-strings -Wno-deprecated-copy -Wno-pessimizing-move -Wno-implicit-fallthrough -Wno-stringop-truncation -Wno-unknown-pragmas -Wsuggest-override -Wcast-qual -Wtautological-compare")
set(CMAKE_CXX_FLAGS "${CXX_STD_FLAGS} ${CXX_GROUPED_WARNING_FLAGS} ${CXX_SPECIFIC_WARNING_FLAGS}")

set(CMAKE_CXX_FLAGS_RELEASE "-O3")

message("CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")
message("CMAKE_CXX_FLAGS_RELEASE is ${CMAKE_CXX_FLAGS_RELEASE}")
message("CMAKE_BUILD_TYPE is ${CMAKE_BUILD_TYPE}")

include_directories(core)

# Thrift
find_library(thrift_location NAMES libthrift.a)
message(STATUS ${thrift_location})
add_library(thrift STATIC IMPORTED)
set_target_properties(thrift PROPERTIES IMPORTED_LOCATION ${thrift_location})


add_subdirectory(core)

# Tests
file(GLOB_RECURSE WRITER_TEST_SOURCES
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/test/*.cpp)
file(GLOB_RECURSE WRITER_TEST_HEADERS
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/test/*.hpp)

set(ALL_WRITER_TEST_SOURCES ${WRITER_TEST_HEADERS} ${WRITER_TEST_SOURCES})
set(ALL_WRITER_TEST_LIBRARIES clearParquet thrift)
add_executable(writer ${ALL_WRITER_TEST_SOURCES})
target_link_libraries(writer ${ALL_WRITER_TEST_LIBRARIES})
set_target_properties(writer PROPERTIES OUTPUT_NAME writer${BUILD_POSTFIX})
set_target_properties(writer PROPERTIES LINKER_LANGUAGE CXX)

