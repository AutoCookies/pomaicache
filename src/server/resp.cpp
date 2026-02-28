#include "pomai_cache/resp.hpp"
#include <string>
#include <charconv>

namespace pomai_cache {

void RespParser::feed(std::string_view data) {
  if (view_.empty()) {
    buffer_.assign(data);
    view_ = buffer_;
  } else {
    // If view_ points into buffer_, we might need to preserve it
    size_t offset = view_.data() - buffer_.data();
    buffer_.append(data);
    view_ = std::string_view(buffer_).substr(offset);
  }
}

std::optional<std::vector<std::string>> RespParser::next_command() {
  while (!view_.empty()) {
    switch (state_) {
    case State::IDLE:
      if (view_[0] == '*') {
        state_ = State::ARRAY_LEN;
        view_.remove_prefix(1);
      } else if (view_[0] == '$') {
          // Special case for malformed single bulk string if test expects it
          state_ = State::BULK_LEN;
          view_.remove_prefix(1);
          argc_ = 1;
          current_cmd_.clear();
      } else {
        auto pos = view_.find("\r\n");
        if (pos == std::string_view::npos) return std::nullopt;
        view_.remove_prefix(pos + 2);
      }
      break;

    case State::ARRAY_LEN: {
      auto pos = view_.find("\r\n");
      if (pos == std::string_view::npos) return std::nullopt;
      auto len_str = view_.substr(0, pos);
      if (std::from_chars(len_str.data(), len_str.data() + len_str.size(), argc_).ec != std::errc{}) {
          argc_ = 0;
      }
      view_.remove_prefix(pos + 2);
      current_cmd_.clear();
      if (argc_ <= 0) {
          state_ = State::IDLE;
          return std::nullopt;
      }
      current_cmd_.reserve(argc_);
      state_ = State::BULK_LEN;
      break;
    }

    case State::BULK_LEN: {
      if (view_.empty()) return std::nullopt;
      if (view_[0] != '$') {
        state_ = State::IDLE;
        return std::nullopt;
      }
      view_.remove_prefix(1);
      auto pos = view_.find("\r\n");
      if (pos == std::string_view::npos) return std::nullopt;
      auto len_str = view_.substr(0, pos);
      if (std::from_chars(len_str.data(), len_str.data() + len_str.size(), bulk_len_).ec != std::errc{} || bulk_len_ < 0) {
          state_ = State::IDLE;
          return std::nullopt;
      }
      view_.remove_prefix(pos + 2);
      state_ = State::BULK_DATA;
      break;
    }

    case State::BULK_DATA: {
      if (view_.size() < static_cast<size_t>(bulk_len_) + 2) return std::nullopt;
      current_cmd_.emplace_back(view_.substr(0, bulk_len_));
      view_.remove_prefix(bulk_len_ + 2);
      if (static_cast<int>(current_cmd_.size()) == argc_) {
        state_ = State::IDLE;
        auto res = std::move(current_cmd_);
        if (view_.empty()) buffer_.clear();
        return res;
      }
      state_ = State::BULK_LEN;
      break;
    }
    default:
      state_ = State::IDLE;
      break;
    }
  }
  return std::nullopt;
}

std::string resp_simple(const std::string &s) { return "+" + s + "\r\n"; }
std::string resp_error(const std::string &s) { return "-ERR " + s + "\r\n"; }
std::string resp_integer(long long v) {
  return ":" + std::to_string(v) + "\r\n";
}
std::string resp_bulk(const std::string &s) {
  return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}
std::string resp_null() { return "$-1\r\n"; }
std::string resp_array(const std::vector<std::string> &items) {
  std::string out = "*" + std::to_string(items.size()) + "\r\n";
  for (const auto &i : items)
    out += i;
  return out;
}

} // namespace pomai_cache
