include_directories(../third_party/catch)
include_directories(../third_party/tpce-tool/include)
include_directories(../third_party/sqlite/include)
include_directories(../test/include)
include_directories(include)

add_subdirectory(micro)
list(FIND DUCKDB_EXTENSION_NAMES tpch _index)
if(${_index} GREATER -1)
  add_subdirectory(tpch)
endif()

add_extension_definitions()
add_definitions(-DDUCKDB_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}")

add_executable(benchmark_runner benchmark_runner.cpp interpreted_benchmark.cpp
                                ${BENCHMARK_OBJECT_FILES})

target_link_libraries(benchmark_runner duckdb imdb test_helpers)

if(${BUILD_TPCE})
  target_link_libraries(benchmark_runner tpce)
endif()

add_executable(custom_runner custom_runner.cpp ../test/arrow/arrow_test_helper.cpp)
target_link_libraries(custom_runner duckdb)
