#include "pomai_cache/resp.hpp"

#include <cctype>

namespace pomai_cache {

void RespParser::feed(const std::string& data) { buffer_ += data; }

std::optional<std::vector<std::string>> RespParser::next_command() {
  if (buffer_.empty()) return std::nullopt;
  if (buffer_[0] != '*') {
    auto pos = buffer_.find("\r\n");
    if (pos == std::string::npos) return std::nullopt;
    buffer_.erase(0, pos + 2);
    return std::vector<std::string>{"__MALFORMED__"};
  }
  auto crlf = buffer_.find("\r\n");
  if (crlf == std::string::npos) return std::nullopt;
  int argc = 0;
  try { argc = std::stoi(buffer_.substr(1, crlf - 1)); }
  catch (...) {
    buffer_.erase(0, crlf + 2);
    return std::vector<std::string>{"__MALFORMED__"};
  }
  if (argc < 0 || argc > 1024) {
    buffer_.erase(0, crlf + 2);
    return std::vector<std::string>{"__MALFORMED__"};
  }
  std::size_t pos = crlf + 2;
  std::vector<std::string> out;
  for (int i = 0; i < argc; ++i) {
    std::string token;
    if (!parse_bulk_string(pos, token)) return std::nullopt;
    out.push_back(std::move(token));
  }
  buffer_.erase(0, pos);
  return out;
}

bool RespParser::parse_bulk_string(std::size_t& pos, std::string& out) const {
  if (pos >= buffer_.size() || buffer_[pos] != '$') return false;
  auto crlf = buffer_.find("\r\n", pos);
  if (crlf == std::string::npos) return false;
  int len = 0;
  try { len = std::stoi(buffer_.substr(pos + 1, crlf - (pos + 1))); }
  catch (...) { return false; }
  if (len < 0 || len > 8 * 1024 * 1024) return false;
  std::size_t data_start = crlf + 2;
  std::size_t data_end = data_start + static_cast<std::size_t>(len);
  if (data_end + 2 > buffer_.size()) return false;
  if (buffer_.substr(data_end, 2) != "\r\n") return false;
  out = buffer_.substr(data_start, len);
  pos = data_end + 2;
  return true;
}

std::string resp_simple(const std::string& s) { return "+" + s + "\r\n"; }
std::string resp_error(const std::string& s) { return "-ERR " + s + "\r\n"; }
std::string resp_integer(long long v) { return ":" + std::to_string(v) + "\r\n"; }
std::string resp_bulk(const std::string& s) { return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n"; }
std::string resp_null() { return "$-1\r\n"; }
std::string resp_array(const std::vector<std::string>& items) {
  std::string out = "*" + std::to_string(items.size()) + "\r\n";
  for (const auto& i : items) out += i;
  return out;
}

} // namespace pomai_cache
