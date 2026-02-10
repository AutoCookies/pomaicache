#include "pomai_cache/resp.hpp"

#include <catch2/catch_test_macros.hpp>

using namespace pomai_cache;

TEST_CASE("RESP parser handles partial feeds", "[resp]") {
  RespParser p;
  p.feed("*1\r\n$4\r\nPI");
  CHECK_FALSE(p.next_command().has_value());
  p.feed("NG\r\n");
  auto c = p.next_command();
  REQUIRE(c.has_value());
  REQUIRE(c->size() == 1);
  CHECK((*c)[0] == "PING");
}

TEST_CASE("RESP parser flags malformed lengths", "[resp]") {
  RespParser malformed;
  malformed.feed("*1\r\n$-99\r\nBAD\r\n");
  CHECK_FALSE(malformed.next_command().has_value());

  RespParser malformed2;
  malformed2.feed("$3\r\nBAD\r\n");
  auto m = malformed2.next_command();
  REQUIRE(m.has_value());
  CHECK(m->at(0) == "__MALFORMED__");
}

TEST_CASE("RESP parser supports large bulk string within cap", "[resp]") {
  const std::string payload(1024 * 1024, 'a');
  RespParser p;
  p.feed("*1\r\n$" + std::to_string(payload.size()) + "\r\n" + payload +
         "\r\n");
  auto cmd = p.next_command();
  REQUIRE(cmd.has_value());
  REQUIRE(cmd->size() == 1);
  CHECK((*cmd)[0].size() == payload.size());
}
