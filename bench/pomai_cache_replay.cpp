#include "pomai_cache/resp.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <netinet/in.h>
#include <numeric>
#include <regex>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
struct TraceOp { std::uint64_t ts_ms{0}; std::string op; std::size_t key_hash{0}; std::size_t value_size{0}; };

bool extract_u64(const std::string &line, const std::string &key, std::uint64_t &out) {
  std::regex re("\\\"" + key + "\\\"\\s*:\\s*([0-9]+)");
  std::smatch m;
  if (!std::regex_search(line, m, re)) return false;
  out = std::stoull(m[1].str());
  return true;
}

bool extract_str(const std::string &line, const std::string &key, std::string &out) {
  std::regex re("\\\"" + key + "\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"");
  std::smatch m;
  if (!std::regex_search(line, m, re)) return false;
  out = m[1].str();
  return true;
}

std::string mkcmd(const TraceOp &op) {
  std::string key = "k" + std::to_string(op.key_hash % 1000);
  if (op.op == "GET") return "*2\r\n$3\r\nGET\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
  if (op.op == "DEL") return "*2\r\n$3\r\nDEL\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
  std::string value(op.value_size > 0 ? op.value_size : 16, 'x');
  return "*3\r\n$3\r\nSET\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}

std::string percentile(const std::vector<double> &v, double p) {
  if (v.empty()) return "0";
  std::vector<double> s = v;
  std::sort(s.begin(), s.end());
  std::size_t idx = static_cast<std::size_t>(std::floor((s.size() - 1) * p));
  std::ostringstream os;
  os << s[idx];
  return os.str();
}

std::string send_cmd(int fd, const std::string &cmd) {
  send(fd, cmd.data(), cmd.size(), 0);
  char buf[4096];
  ssize_t n = recv(fd, buf, sizeof(buf), 0);
  if (n <= 0) return {};
  return std::string(buf, static_cast<std::size_t>(n));
}
} // namespace

int main(int argc, char **argv) {
  std::string trace_path = "traces/mini_hotset.trace";
  std::string out_json = "out/replay_summary.json";
  std::string out_csv = "out/replay_timeseries.csv";
  int port = 6379;
  double scale = 1.0;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--trace" && i + 1 < argc) trace_path = argv[++i];
    else if (a == "--port" && i + 1 < argc) port = std::stoi(argv[++i]);
    else if (a == "--scale" && i + 1 < argc) scale = std::stod(argv[++i]);
    else if (a == "--json" && i + 1 < argc) out_json = argv[++i];
    else if (a == "--csv" && i + 1 < argc) out_csv = argv[++i];
  }

  std::ifstream in(trace_path);
  if (!in.is_open()) {
    std::cerr << "trace file not found\n";
    return 1;
  }
  std::vector<TraceOp> ops;
  for (std::string line; std::getline(in, line);) {
    TraceOp op;
    std::uint64_t v = 0;
    extract_u64(line, "ts_ms", op.ts_ms);
    extract_u64(line, "key_hash", v); op.key_hash = static_cast<std::size_t>(v);
    extract_u64(line, "value_size", v); op.value_size = static_cast<std::size_t>(v);
    extract_str(line, "op", op.op);
    if (!op.op.empty()) ops.push_back(op);
  }

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    std::cerr << "connect failed\n";
    return 2;
  }

  auto before = send_cmd(fd, "*1\r\n$4\r\nINFO\r\n");
  std::vector<double> lats;
  std::vector<std::string> ts_rows;
  std::uint64_t hits = 0;
  std::uint64_t gets = 0;
  const auto replay_start = std::chrono::steady_clock::now();
  std::uint64_t base_ts = ops.empty() ? 0 : ops.front().ts_ms;

  for (std::size_t i = 0; i < ops.size(); ++i) {
    if (i > 0 && scale > 0.0) {
      auto target_ms = static_cast<std::uint64_t>((ops[i].ts_ms - base_ts) / scale);
      auto now_ms = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - replay_start).count());
      if (target_ms > now_ms) std::this_thread::sleep_for(std::chrono::milliseconds(target_ms - now_ms));
    }
    auto st = std::chrono::steady_clock::now();
    auto resp = send_cmd(fd, mkcmd(ops[i]));
    auto en = std::chrono::steady_clock::now();
    double us = std::chrono::duration<double, std::micro>(en - st).count();
    lats.push_back(us);
    if (ops[i].op == "GET") {
      ++gets;
      if (resp.rfind("$-1", 0) != 0) ++hits;
    }
    if (i % 50 == 0) {
      ts_rows.push_back(std::to_string(i) + "," + std::to_string(us));
    }
  }
  auto after = send_cmd(fd, "*1\r\n$4\r\nINFO\r\n");
  close(fd);

  double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - replay_start).count();
  double ops_s = seconds > 0 ? static_cast<double>(ops.size()) / seconds : 0.0;
  double hit_rate = gets > 0 ? static_cast<double>(hits) / static_cast<double>(gets) : 0.0;

  std::ofstream jout(out_json);
  jout << "{\n";
  jout << "  \"trace\": \"" << trace_path << "\",\n";
  jout << "  \"ops\": " << ops.size() << ",\n";
  jout << "  \"ops_per_sec\": " << ops_s << ",\n";
  jout << "  \"p50_us\": " << percentile(lats, 0.50) << ",\n";
  jout << "  \"p95_us\": " << percentile(lats, 0.95) << ",\n";
  jout << "  \"p99_us\": " << percentile(lats, 0.99) << ",\n";
  jout << "  \"p999_us\": " << percentile(lats, 0.999) << ",\n";
  jout << "  \"hit_rate\": " << hit_rate << "\n";
  jout << "}\n";

  std::ofstream csv(out_csv);
  csv << "op_index,latency_us\n";
  for (const auto &r : ts_rows) csv << r << "\n";

  std::cout << "ops/s=" << ops_s << " p50=" << percentile(lats, 0.50) << " p95=" << percentile(lats, 0.95)
            << " p99=" << percentile(lats, 0.99) << " p999=" << percentile(lats, 0.999) << " hit_rate=" << hit_rate << "\n";
  std::cout << "INFO_BEFORE\n" << before << "\nINFO_AFTER\n" << after << "\n";
  return 0;
}
