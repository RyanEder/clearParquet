# clearParquet static lib
file(GLOB_RECURSE CORE_SOURCES
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        *.cpp)
file(GLOB_RECURSE CORE_HEADERS
        RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
        *.hpp)
add_library(clearParquet STATIC ${CORE_HEADERS} ${CORE_SOURCES})
set_target_properties(clearParquet PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set_target_properties(clearParquet PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set_target_properties(clearParquet PROPERTIES OUTPUT_NAME clearParquet${BUILD_POSTFIX})
set_target_properties(clearParquet PROPERTIES LINKER_LANGUAGE CXX)

