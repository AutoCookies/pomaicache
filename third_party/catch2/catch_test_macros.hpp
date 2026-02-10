#pragma once

#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace mini_catch {
struct TestCase { std::string name; std::function<void()> fn; };
inline std::vector<TestCase>& registry() { static std::vector<TestCase> r; return r; }
struct Registrar {
  Registrar(std::string n, std::function<void()> fn) { registry().push_back({std::move(n), std::move(fn)}); }
};
inline int run_all() {
  int failed = 0;
  for (const auto& t : registry()) {
    try { t.fn(); std::cerr << "[PASS] " << t.name << "\n"; }
    catch (const std::exception& e) { ++failed; std::cerr << "[FAIL] " << t.name << ": " << e.what() << "\n"; }
  }
  return failed;
}
inline void require(bool cond, const char* expr, const char* file, int line) {
  if (!cond) throw std::runtime_error(std::string(file) + ":" + std::to_string(line) + " REQUIRE(" + expr + ")");
}
inline void check(bool cond, const char* expr, const char* file, int line) {
  if (!cond) throw std::runtime_error(std::string(file) + ":" + std::to_string(line) + " CHECK(" + expr + ")");
}
} // namespace mini_catch

#define MC_CONCAT2(a, b) a##b
#define MC_CONCAT(a, b) MC_CONCAT2(a, b)
#define TEST_CASE(name, tags) \
  static void MC_CONCAT(test_, __LINE__)(); \
  static mini_catch::Registrar MC_CONCAT(reg_, __LINE__)(name, MC_CONCAT(test_, __LINE__)); \
  static void MC_CONCAT(test_, __LINE__)()
#define REQUIRE(x) mini_catch::require((x), #x, __FILE__, __LINE__)
#define CHECK(x) mini_catch::check((x), #x, __FILE__, __LINE__)

#define CHECK_FALSE(x) CHECK(!(x))
#define REQUIRE_FALSE(x) REQUIRE(!(x))
