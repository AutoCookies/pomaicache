#include <string_view>
#include <string>
#include <vector>
#include <optional>

namespace pomai_cache {

/**
 * Optimized Zero-Copy RESP Parser.
 * Inspired by DragonflyDB's `facade::RedisParser` (src/facade/redis_parser.h).
 */
class RespParser {
public:
  enum class State {
    IDLE,
    ARRAY_LEN,
    BULK_LEN,
    BULK_DATA,
    SIMPLE_STR,
    ERROR_STR,
    INTEGER
  };

  void feed(std::string_view data);
  std::optional<std::vector<std::string>> next_command();

private:
  std::string buffer_;
  std::string_view view_;
  State state_{State::IDLE};
  int argc_{0};
  int bulk_len_{0};
  std::vector<std::string> current_cmd_;
};

std::string resp_simple(const std::string &s);
std::string resp_error(const std::string &s);
std::string resp_integer(long long v);
std::string resp_bulk(const std::string &s);
std::string resp_null();
std::string resp_array(const std::vector<std::string> &items);

} // namespace pomai_cache
