cmake_minimum_required(VERSION 3.20)
project(pomai_cache LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(include third_party)

set(PA_OVERRIDE ON)
add_subdirectory(third_party/palloc)

add_library(pomaicache SHARED
  src/engine/engine.cpp
  src/engine/ssd_store.cpp
  src/policy/policies.cpp
  src/server/ai_cache.cpp
  src/pomai_embedded.cc
  src/metrics/info_metrics.cpp
  src/util/time.cpp
  src/ds/bloom_filter.cpp
  src/ds/count_min_sketch.cpp
  src/ds/vector_index.cpp
  src/ds/dep_graph.cpp
  src/ds/compression.cpp
  src/engine/prompt_cache.cpp
  src/bindings/c_api.cc
)

target_link_libraries(pomaicache PUBLIC palloc)

include(CTest)
if(BUILD_TESTING)
  add_library(mini_catch_main third_party/catch2/catch_main.cpp)

  add_executable(test_engine tests/test_engine.cpp)
  target_link_libraries(test_engine PRIVATE pomaicache mini_catch_main)

  add_executable(test_http tests/test_http.cpp)
  target_link_libraries(test_http PRIVATE pomaicache mini_catch_main)

  add_executable(test_ai_cache tests/test_ai_cache.cpp)
  target_link_libraries(test_ai_cache PRIVATE pomaicache mini_catch_main)

  add_test(NAME test_engine COMMAND test_engine)
  add_test(NAME test_http COMMAND test_http)
  add_test(NAME test_ai_cache COMMAND test_ai_cache)

  if(NOT WIN32)
    add_executable(test_integration tests/test_integration.cpp)
    target_link_libraries(test_integration PRIVATE pomaicache mini_catch_main)
    add_test(NAME test_integration COMMAND test_integration)
  endif()
endif()

if(NOT WIN32)
  add_executable(pomai_cache_crash_harness tests/crash_harness.cpp)
  target_link_libraries(pomai_cache_crash_harness PRIVATE pomaicache)
endif()

option(BUILD_PYTHON_BINDINGS "Build Python bindings" ON)

if(BUILD_PYTHON_BINDINGS)
  find_package(pybind11 CONFIG REQUIRED)
  add_library(pomaicache_python MODULE src/bindings/python_bindings.cc)
  target_link_libraries(pomaicache_python PRIVATE pomaicache pybind11::module)
  set_target_properties(pomaicache_python PROPERTIES
    OUTPUT_NAME "pomaicache"
  )
endif()
