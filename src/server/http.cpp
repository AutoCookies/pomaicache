#include "pomai_cache/http.hpp"
#include <charconv>
#include <sstream>

namespace pomai_cache {

void HttpParser::feed(std::string_view data) {
  if (view_.empty()) {
    buffer_.assign(data);
    view_ = buffer_;
  } else {
    size_t offset = view_.data() - buffer_.data();
    buffer_.append(data);
    view_ = std::string_view(buffer_).substr(offset);
  }
}

bool HttpParser::parse_request_line() {
  auto pos = view_.find("\r\n");
  if (pos == std::string_view::npos) return false;
  
  auto line = view_.substr(0, pos);
  view_.remove_prefix(pos + 2);

  auto sp1 = line.find(' ');
  if (sp1 == std::string_view::npos) { state_ = State::ERROR; return false; }
  current_req_.method = std::string(line.substr(0, sp1));
  
  auto sp2 = line.find(' ', sp1 + 1);
  if (sp2 == std::string_view::npos) { state_ = State::ERROR; return false; }
  
  auto full_path = line.substr(sp1 + 1, sp2 - sp1 - 1);
  auto q_pos = full_path.find('?');
  if (q_pos != std::string_view::npos) {
    current_req_.path = std::string(full_path.substr(0, q_pos));
    auto query_str = full_path.substr(q_pos + 1);
    
    // Parse query params (simple parsing)
    size_t start = 0;
    while (start < query_str.size()) {
      auto amp = query_str.find('&', start);
      auto pair = query_str.substr(start, amp == std::string_view::npos ? std::string_view::npos : amp - start);
      auto eq = pair.find('=');
      if (eq != std::string_view::npos) {
        current_req_.query_params[std::string(pair.substr(0, eq))] = std::string(pair.substr(eq + 1));
      } else {
        current_req_.query_params[std::string(pair)] = "";
      }
      if (amp == std::string_view::npos) break;
      start = amp + 1;
    }
  } else {
    current_req_.path = std::string(full_path);
  }
  
  state_ = State::HEADERS;
  return true;
}

bool HttpParser::parse_headers() {
  while (true) {
    auto pos = view_.find("\r\n");
    if (pos == std::string_view::npos) return false;
    
    if (pos == 0) {
      view_.remove_prefix(2);
      auto it = current_req_.headers.find("Content-Length");
      if (it != current_req_.headers.end()) {
        expected_body_len_ = std::stoi(it->second);
        state_ = expected_body_len_ > 0 ? State::BODY : State::COMPLETE;
      } else {
        expected_body_len_ = 0;
        state_ = State::COMPLETE;
      }
      return true;
    }
    
    auto line = view_.substr(0, pos);
    view_.remove_prefix(pos + 2);
    
    auto colon = line.find(':');
    if (colon != std::string_view::npos) {
      auto key = std::string(line.substr(0, colon));
      auto val = line.substr(colon + 1);
      while (!val.empty() && (val[0] == ' ' || val[0] == '\t')) val.remove_prefix(1);
      current_req_.headers[key] = std::string(val);
    }
  }
}

std::optional<HttpRequest> HttpParser::next_request() {
  while (!view_.empty()) {
    switch (state_) {
      case State::REQUEST_LINE:
        if (!parse_request_line()) return std::nullopt;
        break;
      case State::HEADERS:
        if (!parse_headers()) return std::nullopt;
        break;
      case State::BODY:
        if (view_.size() >= static_cast<size_t>(expected_body_len_)) {
          current_req_.body = std::string(view_.substr(0, expected_body_len_));
          view_.remove_prefix(expected_body_len_);
          state_ = State::COMPLETE;
        } else {
          return std::nullopt;
        }
        break;
      case State::COMPLETE: {
        auto req = std::move(current_req_);
        current_req_ = HttpRequest();
        state_ = State::REQUEST_LINE;
        if (view_.empty()) buffer_.clear();
        return req;
      }
      case State::ERROR:
        return std::nullopt;
    }
  }
  if (state_ == State::COMPLETE) {
    auto req = std::move(current_req_);
    current_req_ = HttpRequest();
    state_ = State::REQUEST_LINE;
    if (view_.empty()) buffer_.clear();
    return req;
  }
  return std::nullopt;
}

std::string http_response(int status_code, const std::string& status_text, const std::string& body, const std::string& content_type) {
  std::ostringstream oss;
  oss << "HTTP/1.1 " << status_code << " " << status_text << "\r\n";
  oss << "Content-Length: " << body.size() << "\r\n";
  oss << "Content-Type: " << content_type << "\r\n";
  oss << "Connection: keep-alive\r\n";
  oss << "\r\n";
  oss << body;
  return oss.str();
}

} // namespace pomai_cache
