#include "pomai_cache/resp.hpp"

#include <cassert>

using namespace pomai_cache;

int main() {
  RespParser p;
  p.feed("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
  auto c = p.next_command();
  assert(c.has_value());
  assert(c->size() == 2);
  assert((*c)[0] == "GET");

  RespParser malformed;
  malformed.feed("$3\r\nBAD\r\n");
  auto m = malformed.next_command();
  assert(m.has_value());
  assert(m->at(0) == "__MALFORMED__");

  RespParser partial;
  partial.feed("*1\r\n$4\r\nPI");
  assert(!partial.next_command().has_value());
  partial.feed("NG\r\n");
  assert(partial.next_command().has_value());
  return 0;
}
