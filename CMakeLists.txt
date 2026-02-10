cmake_minimum_required(VERSION 3.20)
project(pomai_cache LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(include third_party)

add_library(pomai_cache_core
  src/engine/engine.cpp
  src/policy/policies.cpp
  src/server/resp.cpp
  src/metrics/info_metrics.cpp
  src/util/time.cpp
)

add_executable(pomai_cache_server src/server/server_main.cpp)
target_link_libraries(pomai_cache_server PRIVATE pomai_cache_core)

add_executable(pomai_cache_cli apps/cli/main.cpp)

add_executable(pomai_cache_bench bench/pomai_cache_bench.cpp)
target_link_libraries(pomai_cache_bench PRIVATE pomai_cache_core)

add_executable(pomai_cache_netbench bench/pomai_cache_netbench.cpp)
target_link_libraries(pomai_cache_netbench PRIVATE pomai_cache_core)

include(CTest)
if(BUILD_TESTING)
  add_library(mini_catch_main third_party/catch2/catch_main.cpp)

  add_executable(test_engine tests/test_engine.cpp)
  target_link_libraries(test_engine PRIVATE pomai_cache_core mini_catch_main)

  add_executable(test_resp tests/test_resp.cpp)
  target_link_libraries(test_resp PRIVATE pomai_cache_core mini_catch_main)

  add_executable(test_integration tests/test_integration.cpp)
  target_link_libraries(test_integration PRIVATE pomai_cache_core mini_catch_main)

  add_test(NAME test_engine COMMAND test_engine)
  add_test(NAME test_resp COMMAND test_resp)
  add_test(NAME test_integration COMMAND test_integration)
endif()
