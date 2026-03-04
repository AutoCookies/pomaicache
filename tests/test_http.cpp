// tests/test_http.cpp
#include <catch2/catch_test_macros.hpp>
#include "pomai_cache/http.hpp"

using namespace pomai_cache;

TEST_CASE("HttpParser: simple GET", "[http]") {
  HttpParser parser;
  parser.feed("GET /key/a HTTP/1.1\r\nHost: localhost\r\n\r\n");
  auto req = parser.next_request();
  REQUIRE(req.has_value());
  CHECK(req->method == "GET");
  CHECK(req->path == "/key/a");
  CHECK(req->headers["Host"] == "localhost");
  CHECK(req->body.empty());
}

TEST_CASE("HttpParser: simple POST with body", "[http]") {
  HttpParser parser;
  parser.feed("POST /key/a HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello");
  auto req = parser.next_request();
  REQUIRE(req.has_value());
  CHECK(req->method == "POST");
  CHECK(req->path == "/key/a");
  CHECK(req->body == "hello");
}

TEST_CASE("HttpParser: query params", "[http]") {
  HttpParser parser;
  parser.feed("GET /ai/sim/get?vec=1,2,3&topk=5 HTTP/1.1\r\n\r\n");
  auto req = parser.next_request();
  REQUIRE(req.has_value());
  CHECK(req->path == "/ai/sim/get");
  CHECK(req->query_params["vec"] == "1,2,3");
  CHECK(req->query_params["topk"] == "5");
}
