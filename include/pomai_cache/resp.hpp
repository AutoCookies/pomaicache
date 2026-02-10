#pragma once

#include <optional>
#include <string>
#include <vector>

namespace pomai_cache {

class RespParser {
public:
  void feed(const std::string& data);
  std::optional<std::vector<std::string>> next_command();

private:
  bool parse_bulk_string(std::size_t& pos, std::string& out) const;
  std::string buffer_;
};

std::string resp_simple(const std::string& s);
std::string resp_error(const std::string& s);
std::string resp_integer(long long v);
std::string resp_bulk(const std::string& s);
std::string resp_null();
std::string resp_array(const std::vector<std::string>& items);

} // namespace pomai_cache
