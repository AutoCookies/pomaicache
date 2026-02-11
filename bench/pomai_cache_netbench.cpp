#include "pomai_cache/resp.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <netinet/in.h>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

struct Options {
  std::string host{"127.0.0.1"};
  int port{6379};
  std::string workload{"mixed"};
  int threads{4};
  int clients{16};
  int pipeline{1};
  int duration_s{10};
  int warmup_s{2};
  int key_size{16};
  int value_size{128};
  int keyspace{10000};
  std::uint64_t seed{1337};
  std::string json_out{"netbench_summary.json"};
};

struct SharedStats {
  std::mutex mu;
  std::vector<double> latencies_us;
  std::uint64_t ops{0};
  std::uint64_t get_ops{0};
  std::uint64_t get_hits{0};
  std::uint64_t set_ops{0};
};

std::string make_cmd(const std::vector<std::string> &args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto &a : args) {
    out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
  }
  return out;
}

std::optional<std::string> read_reply(int fd) {
  std::string out;
  char c = 0;
  while (true) {
    ssize_t r = recv(fd, &c, 1, 0);
    if (r <= 0)
      return std::nullopt;
    out.push_back(c);
    if (out.size() >= 2 && out[out.size() - 2] == '\r' &&
        out[out.size() - 1] == '\n') {
      if (out[0] == '+' || out[0] == '-' || out[0] == ':')
        return out;
      if (out[0] == '$') {
        int len = std::stoi(out.substr(1, out.size() - 3));
        if (len < 0)
          return out;
        std::string payload(len + 2, '\0');
        ssize_t got = recv(fd, payload.data(), payload.size(), MSG_WAITALL);
        if (got <= 0)
          return std::nullopt;
        out += payload;
        return out;
      }
      if (out[0] == '*') {
        int n = std::stoi(out.substr(1, out.size() - 3));
        for (int i = 0; i < n; ++i) {
          auto child = read_reply(fd);
          if (!child)
            return std::nullopt;
          out += *child;
        }
        return out;
      }
    }
  }
}

std::string fixed_key(int k, int key_size) {
  std::string s = "k" + std::to_string(k);
  if (static_cast<int>(s.size()) < key_size)
    s += std::string(
        static_cast<std::size_t>(key_size - static_cast<int>(s.size())), 'x');
  return s;
}

void parse_info(const std::string &info, std::uint64_t &memory_used,
                std::uint64_t &evictions, std::uint64_t &admissions,
                std::uint64_t &ram_hits, std::uint64_t &ssd_hits,
                double &ssd_read_mb, double &ssd_write_mb,
                std::uint64_t &ssd_bytes, double &fragmentation,
                std::uint64_t &index_rebuild_ms) {
  auto value_of = [&](const std::string &k) {
    auto p = info.find(k + ":");
    if (p == std::string::npos)
      return std::uint64_t{0};
    auto e = info.find('\n', p);
    return static_cast<std::uint64_t>(
        std::stoull(info.substr(p + k.size() + 1, e - p - k.size() - 1)));
  };
  memory_used = value_of("memory_used_bytes");
  evictions = value_of("evictions");
  admissions = value_of("admissions_rejected");
  ram_hits = value_of("hits");
  ssd_hits = value_of("ssd_hits");
  ssd_bytes = value_of("ssd_bytes");
  index_rebuild_ms = value_of("ssd_index_rebuild_ms");
  auto value_of_d = [&](const std::string &k) {
    auto p = info.find(k + ":");
    if (p == std::string::npos)
      return 0.0;
    auto e = info.find('\n', p);
    return std::stod(info.substr(p + k.size() + 1, e - p - k.size() - 1));
  };
  ssd_read_mb = value_of_d("ssd_read_mb");
  ssd_write_mb = value_of_d("ssd_write_mb");
  fragmentation = value_of_d("fragmentation_estimate");
}

int connect_server(const Options &opt) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(opt.port);
  inet_pton(AF_INET, opt.host.c_str(), &addr.sin_addr);
  if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    return -1;
  return fd;
}

int main(int argc, char **argv) {
  Options opt;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto take = [&](int &v) {
      if (i + 1 < argc)
        v = std::stoi(argv[++i]);
    };
    if (a == "--port")
      take(opt.port);
    else if (a == "--threads")
      take(opt.threads);
    else if (a == "--clients")
      take(opt.clients);
    else if (a == "--pipeline")
      take(opt.pipeline);
    else if (a == "--duration")
      take(opt.duration_s);
    else if (a == "--warmup")
      take(opt.warmup_s);
    else if (a == "--key-size")
      take(opt.key_size);
    else if (a == "--value-size")
      take(opt.value_size);
    else if (a == "--keyspace")
      take(opt.keyspace);
    else if (a == "--workload" && i + 1 < argc)
      opt.workload = argv[++i];
    else if (a == "--json" && i + 1 < argc)
      opt.json_out = argv[++i];
  }

  SharedStats shared;
  std::atomic<bool> running{true};
  auto end_time = std::chrono::steady_clock::now() +
                  std::chrono::seconds(opt.duration_s + opt.warmup_s);
  auto warmup_end =
      std::chrono::steady_clock::now() + std::chrono::seconds(opt.warmup_s);

  std::vector<std::thread> workers;
  for (int t = 0; t < opt.clients; ++t) {
    workers.emplace_back([&, t] {
      int fd = connect_server(opt);
      if (fd < 0)
        return;
      std::mt19937_64 rng(opt.seed + static_cast<std::uint64_t>(t));
      std::uniform_int_distribution<int> uniform(0, opt.keyspace - 1);
      std::uniform_real_distribution<double> real(0.0, 1.0);
      int value_size = opt.value_size;
      if (opt.workload == "tier_on_large_values")
        value_size = std::max(value_size, 64 * 1024);
      std::string value(static_cast<std::size_t>(value_size), 'v');

      while (std::chrono::steady_clock::now() < end_time) {
        std::vector<std::string> batch;
        std::vector<bool> expect_get;
        batch.reserve(static_cast<std::size_t>(opt.pipeline));
        for (int i = 0; i < opt.pipeline; ++i) {
          int k = uniform(rng);
          if (opt.workload == "hotset" ||
              opt.workload == "tier_on_large_values") {
            const double x = std::pow(real(rng), 2.0);
            k = static_cast<int>(x * std::max(1, opt.keyspace / 10));
          }
          bool do_set = false;
          if (opt.workload == "writeheavy" ||
              opt.workload == "tier_on_pressure_demotion")
            do_set = (real(rng) < 0.8);
          else if (opt.workload == "mixed" ||
                   opt.workload == "tier_off_ram_only")
            do_set = (real(rng) < 0.35);
          else if (opt.workload == "ttlheavy" ||
                   opt.workload == "ttl_storm_with_tier")
            do_set = true;
          else if (opt.workload == "pipeline")
            do_set = (i % 2 == 0);
          std::string key = fixed_key(k, opt.key_size);
          if (do_set) {
            if (opt.workload == "ttlheavy")
              batch.push_back(make_cmd({"SET", key, value, "PX", "200"}));
            else
              batch.push_back(make_cmd({"SET", key, value}));
            expect_get.push_back(false);
          } else {
            batch.push_back(make_cmd({"GET", key}));
            expect_get.push_back(true);
          }
        }

        auto t0 = std::chrono::steady_clock::now();
        for (const auto &cmd : batch)
          send(fd, cmd.data(), cmd.size(), 0);
        for (std::size_t i = 0; i < batch.size(); ++i) {
          auto rep = read_reply(fd);
          if (!rep) {
            close(fd);
            return;
          }
          auto t1 = std::chrono::steady_clock::now();
          if (std::chrono::steady_clock::now() >= warmup_end) {
            std::lock_guard<std::mutex> lk(shared.mu);
            shared.latencies_us.push_back(
                std::chrono::duration<double, std::micro>(t1 - t0).count());
            ++shared.ops;
            if (expect_get[i]) {
              ++shared.get_ops;
              if (rep->rfind("$-1", 0) != 0)
                ++shared.get_hits;
            } else {
              ++shared.set_ops;
            }
          }
        }
      }
      close(fd);
    });
  }

  for (auto &th : workers)
    th.join();

  int infofd = connect_server(opt);
  std::uint64_t mem = 0, evictions = 0, admissions = 0, ram_hits = 0,
                ssd_hits = 0, ssd_bytes = 0, index_rebuild_ms = 0;
  double ssd_read_mb = 0.0, ssd_write_mb = 0.0, fragmentation = 0.0;
  if (infofd >= 0) {
    auto cmd = make_cmd({"INFO"});
    send(infofd, cmd.data(), cmd.size(), 0);
    auto rep = read_reply(infofd);
    if (rep && rep->size() > 0 && (*rep)[0] == '$') {
      auto crlf = rep->find("\r\n");
      int len = std::stoi(rep->substr(1, crlf - 1));
      std::string body = rep->substr(crlf + 2, len);
      parse_info(body, mem, evictions, admissions, ram_hits, ssd_hits,
                 ssd_read_mb, ssd_write_mb, ssd_bytes, fragmentation,
                 index_rebuild_ms);
    }
    close(infofd);
  }

  std::sort(shared.latencies_us.begin(), shared.latencies_us.end());
  auto pct = [&](double p) {
    if (shared.latencies_us.empty())
      return 0.0;
    return shared.latencies_us[static_cast<std::size_t>(
        p * (shared.latencies_us.size() - 1))];
  };
  const double run_secs = static_cast<double>(opt.duration_s);
  const double ops_s =
      run_secs > 0 ? static_cast<double>(shared.ops) / run_secs : 0.0;
  const double hit_rate = shared.get_ops > 0
                              ? static_cast<double>(shared.get_hits) /
                                    static_cast<double>(shared.get_ops)
                              : 0.0;

  std::cout << std::fixed << std::setprecision(2) << "ops/s=" << ops_s
            << " p50_us=" << pct(0.50) << " p95_us=" << pct(0.95)
            << " p99_us=" << pct(0.99) << " p999_us=" << pct(0.999)
            << " hit_rate=" << hit_rate << " ram_hits=" << ram_hits
            << " ssd_hits=" << ssd_hits << " ssd_bytes=" << ssd_bytes
            << " memory_used=" << mem << " evictions=" << evictions
            << " admissions_rejected=" << admissions << "\n";

  std::ofstream out(opt.json_out);
  out << "{\n"
      << "  \"workload\": \"" << opt.workload << "\",\n"
      << "  \"ops_per_sec\": " << ops_s << ",\n"
      << "  \"p50_us\": " << pct(0.50) << ",\n"
      << "  \"p95_us\": " << pct(0.95) << ",\n"
      << "  \"p99_us\": " << pct(0.99) << ",\n"
      << "  \"p999_us\": " << pct(0.999) << ",\n"
      << "  \"hit_rate\": " << hit_rate << ",\n"
      << "  \"ram_hits\": " << ram_hits << ",\n"
      << "  \"ssd_hits\": " << ssd_hits << ",\n"
      << "  \"ssd_bytes\": " << ssd_bytes << ",\n"
      << "  \"ssd_read_mb\": " << ssd_read_mb << ",\n"
      << "  \"ssd_write_mb\": " << ssd_write_mb << ",\n"
      << "  \"ssd_index_rebuild_ms\": " << index_rebuild_ms << ",\n"
      << "  \"fragmentation_estimate\": " << fragmentation << ",\n"
      << "  \"memory_used_bytes\": " << mem << ",\n"
      << "  \"evictions_per_sec\": "
      << (run_secs > 0 ? static_cast<double>(evictions) / run_secs : 0.0)
      << ",\n"
      << "  \"admissions_rejected_per_sec\": "
      << (run_secs > 0 ? static_cast<double>(admissions) / run_secs : 0.0)
      << "\n"
      << "}\n";
  return 0;
}
