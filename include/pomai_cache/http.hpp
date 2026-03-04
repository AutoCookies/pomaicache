#pragma once

#include <string_view>
#include <string>
#include <vector>
#include <optional>
#include <unordered_map>

namespace pomai_cache {

struct HttpRequest {
  std::string method;
  std::string path;
  std::unordered_map<std::string, std::string> query_params;
  std::unordered_map<std::string, std::string> headers;
  std::string body;
};

class HttpParser {
public:
  enum class State {
    REQUEST_LINE,
    HEADERS,
    BODY,
    COMPLETE,
    ERROR
  };

  void feed(std::string_view data);
  std::optional<HttpRequest> next_request();

private:
  std::string buffer_;
  std::string_view view_;
  State state_{State::REQUEST_LINE};
  HttpRequest current_req_;
  int expected_body_len_{0};
  
  bool parse_request_line();
  bool parse_headers();
};

std::string http_response(int status_code, const std::string& status_text, const std::string& body, const std::string& content_type = "text/plain");

} // namespace pomai_cache
