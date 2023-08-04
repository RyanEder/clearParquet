# Tests
file(GLOB_RECURSE WRITER_TEST_SOURCES
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/test/writer.cpp)
file(GLOB_RECURSE WRITER_TEST_HEADERS
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/test/writer.hpp)

set(ALL_WRITER_TEST_SOURCES ${WRITER_TEST_HEADERS} ${WRITER_TEST_SOURCES})
set(ALL_WRITER_TEST_LIBRARIES)
add_executable(writer ${ALL_WRITER_TEST_SOURCES})
if(zstd_location)
    set(ALL_WRITER_TEST_LIBRARIES ${ALL_WRITER_TEST_LIBRARIES} zstd)
    if(MSVC)
        target_include_directories(writer PRIVATE ${ZSTD_BASE}/include)
    endif()
endif()
if(snappy_location)
    set(ALL_WRITER_TEST_LIBRARIES ${ALL_WRITER_TEST_LIBRARIES} snappy)
    if(MSVC)
        target_include_directories(writer PRIVATE ${SNAPPY_BASE}/include)
    endif()
endif()

target_link_libraries(writer ${ALL_WRITER_TEST_LIBRARIES})
set_target_properties(writer PROPERTIES OUTPUT_NAME writer${BUILD_POSTFIX})
set_target_properties(writer PROPERTIES LINKER_LANGUAGE CXX)

file(GLOB_RECURSE READER_TEST_SOURCES
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/test/reader.cpp)
file(GLOB_RECURSE READER_TEST_HEADERS
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/test/reader.hpp)

set(ALL_READER_TEST_SOURCES ${READER_TEST_HEADERS} ${READER_TEST_SOURCES})
set(ALL_READER_TEST_LIBRARIES)
add_executable(reader ${ALL_READER_TEST_SOURCES})
if(zstd_location)
    set(ALL_READER_TEST_LIBRARIES ${ALL_READER_TEST_LIBRARIES} zstd)
    if(MSVC)
        target_include_directories(reader PRIVATE ${ZSTD_BASE}/include)
    endif()
endif()
if(snappy_location)
    set(ALL_READER_TEST_LIBRARIES ${ALL_READER_TEST_LIBRARIES} snappy)
    if(MSVC)
        target_include_directories(reader PRIVATE ${SNAPPY_BASE}/include)
    endif()
endif()

target_link_libraries(reader ${ALL_READER_TEST_LIBRARIES})
set_target_properties(reader PROPERTIES OUTPUT_NAME reader${BUILD_POSTFIX})
set_target_properties(reader PROPERTIES LINKER_LANGUAGE CXX)

# Unit tests 
file(GLOB_RECURSE UNITTEST_TEST_SOURCES
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/test/unittest.cpp)
file(GLOB_RECURSE UNITTEST_TEST_HEADERS
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/test/unittest.hpp)

set(ALL_UNITTEST_TEST_SOURCES ${UNITTEST_TEST_HEADERS} ${UNITTEST_TEST_SOURCES})
set(ALL_UNITTEST_TEST_LIBRARIES Catch2::Catch2WithMain)
add_executable(unittest ${ALL_UNITTEST_TEST_SOURCES})
if(zstd_location)
    set(ALL_UNITTEST_TEST_LIBRARIES ${ALL_UNITTEST_TEST_LIBRARIES} zstd)
    if(MSVC)
        target_include_directories(unittest PRIVATE ${ZSTD_BASE}/include)
    endif()
endif()
if(snappy_location)
    set(ALL_UNITTEST_TEST_LIBRARIES ${ALL_UNITTEST_TEST_LIBRARIES} snappy)
    if(MSVC)
        target_include_directories(unittest PRIVATE ${SNAPPY_BASE}/include)
    endif()
endif()

target_link_libraries(unittest ${ALL_UNITTEST_TEST_LIBRARIES})
set_target_properties(unittest PROPERTIES OUTPUT_NAME unittest${BUILD_POSTFIX})
set_target_properties(unittest PROPERTIES LINKER_LANGUAGE CXX)

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
