#include "pomai_cache/engine.hpp"
#include "pomai_cache/resp.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <netinet/in.h>
#include <optional>
#include <random>
#include <sstream>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>

namespace {
volatile std::sig_atomic_t running = 1;
void on_sigint(int) { running = 0; }

std::string upper(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return s;
}

bool parse_u64(const std::string &s, std::uint64_t &out) {
  try {
    std::size_t idx = 0;
    out = std::stoull(s, &idx);
    return idx == s.size();
  } catch (...) {
    return false;
  }
}

std::string latency_bucket(std::uint64_t us) {
  if (us < 100)
    return "lt100us";
  if (us < 500)
    return "lt500us";
  if (us < 1000)
    return "lt1ms";
  if (us < 5000)
    return "lt5ms";
  return "ge5ms";
}

struct ClientState {
  pomai_cache::RespParser parser;
  std::string out;
};

struct ServerStats {
  std::uint64_t rejected_requests{0};
  std::uint64_t total_request_bytes{0};
  std::uint64_t request_count{0};
};

struct TraceConfig {
  bool enabled{false};
  std::string path{"trace/pomai_cache.trace.jsonl"};
  double sample_rate{0.0};
  std::uint64_t dropped{0};
};

struct SlowEntry {
  std::string cmd;
  std::uint64_t latency_us{0};
  std::uint64_t timestamp_ms{0};
};

} // namespace

int main(int argc, char **argv) {
  int port = 6379;
  std::size_t max_connections = 512;
  std::size_t max_pending_out = 1 << 20;
  std::size_t max_cmds_per_iteration = 64;
  std::size_t memory_limit = 64 * 1024 * 1024;
  std::string policy_mode = "pomai_cost";
  std::string params_path = "config/policy_params.json";

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--port" && i + 1 < argc)
      port = std::stoi(argv[++i]);
    else if (a == "--memory" && i + 1 < argc)
      memory_limit = std::stoull(argv[++i]);
    else if (a == "--policy" && i + 1 < argc)
      policy_mode = argv[++i];
    else if (a == "--params" && i + 1 < argc)
      params_path = argv[++i];
  }

  auto policy = pomai_cache::make_policy_by_name(policy_mode);
  pomai_cache::Engine engine({memory_limit, 256, 1024 * 1024, 128}, std::move(policy));
  std::string reload_err;
  engine.reload_params(params_path, &reload_err);

  TraceConfig trace_cfg;
  std::ofstream trace_stream;
  std::uint64_t rng_seed = 424242;
  std::mt19937_64 rng(rng_seed);
  std::uniform_real_distribution<double> sample_dist(0.0, 1.0);
  std::deque<std::string> trace_ring;
  std::deque<SlowEntry> slowlog;
  constexpr std::size_t max_slowlog = 256;
  constexpr std::size_t max_trace_ring = 512;

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);
  if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    std::cerr << "bind failed\n";
    return 1;
  }
  if (listen(server_fd, 128) < 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  std::signal(SIGINT, on_sigint);
  std::unordered_map<int, ClientState> clients;
  ServerStats stats;
  std::cout << "pomai_cache_server listening on " << port << "\n";

  while (running) {
    engine.tick();
    fd_set readfds, writefds;
    FD_ZERO(&readfds);
    FD_ZERO(&writefds);
    FD_SET(server_fd, &readfds);
    int maxfd = server_fd;
    for (const auto &[fd, st] : clients) {
      FD_SET(fd, &readfds);
      if (!st.out.empty())
        FD_SET(fd, &writefds);
      maxfd = std::max(maxfd, fd);
    }
    timeval tv{0, 20000};
    int n = select(maxfd + 1, &readfds, &writefds, nullptr, &tv);
    if (n < 0)
      continue;

    if (FD_ISSET(server_fd, &readfds)) {
      int cfd = accept(server_fd, nullptr, nullptr);
      if (cfd >= 0) {
        if (clients.size() >= max_connections) {
          const auto msg = pomai_cache::resp_error("connection limit reached");
          send(cfd, msg.data(), msg.size(), 0);
          close(cfd);
          ++stats.rejected_requests;
        } else {
          clients[cfd] = {};
        }
      }
    }

    std::vector<int> to_close;
    for (auto &[fd, st] : clients) {
      if (FD_ISSET(fd, &readfds)) {
        char buf[4096];
        ssize_t r = recv(fd, buf, sizeof(buf), 0);
        if (r <= 0) {
          to_close.push_back(fd);
          continue;
        }
        stats.total_request_bytes += static_cast<std::uint64_t>(r);
        st.parser.feed(std::string(buf, static_cast<std::size_t>(r)));
        std::size_t processed = 0;
        while (processed < max_cmds_per_iteration) {
          auto cmd = st.parser.next_command();
          if (!cmd.has_value())
            break;
          ++processed;
          ++stats.request_count;
          const auto op_start = pomai_cache::Clock::now();
          std::string first_key;
          std::string op_name = cmd->empty() ? "UNKNOWN" : upper((*cmd)[0]);
          bool hit = false;

          if (cmd->size() == 1 && cmd->front() == "__MALFORMED__") {
            ++stats.rejected_requests;
            st.out += pomai_cache::resp_error("malformed RESP");
            break;
          }
          if (cmd->empty()) {
            ++stats.rejected_requests;
            st.out += pomai_cache::resp_error("empty command");
            continue;
          }

          if (op_name == "PING")
            st.out += pomai_cache::resp_simple("PONG");
          else if (op_name == "SET") {
            if (cmd->size() < 3) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("SET key value [EX sec|PX ms] [OWNER name]");
            } else {
              first_key = (*cmd)[1];
              std::optional<std::uint64_t> ttl_ms;
              std::string owner = "default";
              bool valid = true;
              for (std::size_t i = 3; i + 1 < cmd->size(); i += 2) {
                auto opt = upper((*cmd)[i]);
                std::uint64_t ttl_tmp = 0;
                if (opt == "EX") {
                  valid = parse_u64((*cmd)[i + 1], ttl_tmp);
                  ttl_ms = ttl_tmp * 1000;
                } else if (opt == "PX") {
                  valid = parse_u64((*cmd)[i + 1], ttl_tmp);
                  ttl_ms = ttl_tmp;
                } else if (opt == "OWNER") {
                  owner = (*cmd)[i + 1];
                }
                if (!valid)
                  break;
              }
              if (!valid) {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error("invalid numeric argument");
              } else {
                std::vector<std::uint8_t> val((*cmd)[2].begin(), (*cmd)[2].end());
                std::string err;
                if (engine.set((*cmd)[1], val, ttl_ms, owner, &err)) {
                  st.out += pomai_cache::resp_simple("OK");
                  hit = true;
                } else {
                  ++stats.rejected_requests;
                  st.out += pomai_cache::resp_error(err);
                }
              }
            }
          } else if (op_name == "GET") {
            if (cmd->size() != 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("GET key");
            } else {
              first_key = (*cmd)[1];
              auto v = engine.get((*cmd)[1]);
              hit = v.has_value();
              if (!v)
                st.out += pomai_cache::resp_null();
              else
                st.out += pomai_cache::resp_bulk(std::string(v->begin(), v->end()));
            }
          } else if (op_name == "MGET") {
            if (cmd->size() < 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("MGET key [key...]");
            } else {
              first_key = (*cmd)[1];
              std::vector<std::string> keys(cmd->begin() + 1, cmd->end());
              auto vals = engine.mget(keys);
              std::vector<std::string> arr;
              arr.reserve(vals.size());
              for (auto &v : vals) {
                if (v)
                  hit = true;
                arr.push_back(v ? pomai_cache::resp_bulk(std::string(v->begin(), v->end())) : pomai_cache::resp_null());
              }
              st.out += pomai_cache::resp_array(arr);
            }
          } else if (op_name == "DEL") {
            if (cmd->size() < 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("DEL key [key...]");
            } else {
              first_key = (*cmd)[1];
              std::vector<std::string> keys(cmd->begin() + 1, cmd->end());
              st.out += pomai_cache::resp_integer(engine.del(keys));
            }
          } else if (op_name == "EXPIRE") {
            if (cmd->size() != 3) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("EXPIRE key seconds");
            } else {
              first_key = (*cmd)[1];
              std::uint64_t ttl_s = 0;
              if (!parse_u64((*cmd)[2], ttl_s)) {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error("invalid numeric argument");
              } else {
                hit = engine.expire((*cmd)[1], ttl_s);
                st.out += pomai_cache::resp_integer(hit ? 1 : 0);
              }
            }
          } else if (op_name == "TTL") {
            if (cmd->size() != 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("TTL key");
            } else {
              first_key = (*cmd)[1];
              auto t = engine.ttl((*cmd)[1]);
              hit = t.has_value();
              st.out += pomai_cache::resp_integer(t ? *t : -2);
            }
          } else if (op_name == "INFO") {
            std::ostringstream info;
            info << engine.info();
            info << "connected_clients:" << clients.size() << "\n";
            info << "rejected_requests:" << stats.rejected_requests << "\n";
            const double avg_bytes = stats.request_count == 0 ? 0.0 : static_cast<double>(stats.total_request_bytes) / static_cast<double>(stats.request_count);
            info << "avg_request_bytes:" << avg_bytes << "\n";
            info << "trace_enabled:" << (trace_cfg.enabled ? 1 : 0) << "\n";
            info << "trace_sample_rate:" << trace_cfg.sample_rate << "\n";
            info << "trace_dropped:" << trace_cfg.dropped << "\n";
            st.out += pomai_cache::resp_bulk(info.str());
          } else if (op_name == "SLOWLOG") {
            if (cmd->size() >= 2 && upper((*cmd)[1]) == "RESET") {
              slowlog.clear();
              st.out += pomai_cache::resp_simple("OK");
            } else if (cmd->size() >= 2 && upper((*cmd)[1]) == "GET") {
              std::uint64_t nentries = 16;
              if (cmd->size() == 3)
                parse_u64((*cmd)[2], nentries);
              nentries = std::min<std::uint64_t>(nentries, max_slowlog);
              std::vector<std::string> arr;
              for (std::size_t i = 0; i < std::min<std::size_t>(nentries, slowlog.size()); ++i) {
                const auto &e = slowlog[slowlog.size() - 1 - i];
                std::vector<std::string> item{pomai_cache::resp_integer(static_cast<std::int64_t>(e.timestamp_ms)), pomai_cache::resp_integer(static_cast<std::int64_t>(e.latency_us)), pomai_cache::resp_bulk(e.cmd)};
                arr.push_back(pomai_cache::resp_array(item));
              }
              st.out += pomai_cache::resp_array(arr);
            } else {
              st.out += pomai_cache::resp_error("SLOWLOG GET [N]|RESET");
            }
          } else if (op_name == "TRACE" && cmd->size() == 2 && upper((*cmd)[1]) == "STREAM") {
            std::vector<std::string> arr;
            for (const auto &line : trace_ring)
              arr.push_back(pomai_cache::resp_bulk(line));
            st.out += pomai_cache::resp_array(arr);
          } else if (op_name == "DEBUG" && cmd->size() == 3 && upper((*cmd)[1]) == "DUMPSTATS") {
            std::string err;
            if (engine.dump_stats((*cmd)[2], &err))
              st.out += pomai_cache::resp_simple("OK");
            else
              st.out += pomai_cache::resp_error(err);
          } else if (op_name == "CONFIG") {
            if (cmd->size() >= 2 && upper((*cmd)[1]) == "GET") {
              if (cmd->size() == 3 && upper((*cmd)[2]) == "POLICY") {
                std::vector<std::string> arr{pomai_cache::resp_bulk("policy"), pomai_cache::resp_bulk(engine.policy().name())};
                st.out += pomai_cache::resp_array(arr);
              } else {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error("unsupported CONFIG GET");
              }
            } else if (cmd->size() >= 2 && upper((*cmd)[1]) == "SET") {
              if (cmd->size() == 4 && upper((*cmd)[2]) == "POLICY") {
                engine.set_policy(pomai_cache::make_policy_by_name((*cmd)[3]));
                st.out += pomai_cache::resp_simple("OK");
              } else if (cmd->size() == 4 && upper((*cmd)[2]) == "PARAMS") {
                std::string err;
                if (!engine.reload_params((*cmd)[3], &err)) {
                  ++stats.rejected_requests;
                  st.out += pomai_cache::resp_error(err);
                } else {
                  st.out += pomai_cache::resp_simple("OK");
                }
              } else if (cmd->size() == 4 && upper((*cmd)[2]) == "POLICY.CANARY_PCT") {
                std::uint64_t pct = 0;
                if (!parse_u64((*cmd)[3], pct)) {
                  st.out += pomai_cache::resp_error("invalid numeric argument");
                } else {
                  engine.set_canary_pct(pct);
                  st.out += pomai_cache::resp_simple("OK");
                }
              } else if (cmd->size() == 4 && upper((*cmd)[2]) == "TRACE.ENABLED") {
                trace_cfg.enabled = upper((*cmd)[3]) == "YES" || (*cmd)[3] == "1";
                if (trace_cfg.enabled && !trace_stream.is_open()) {
                  trace_stream.open(trace_cfg.path, std::ios::app);
                }
                st.out += pomai_cache::resp_simple("OK");
              } else if (cmd->size() == 4 && upper((*cmd)[2]) == "TRACE.PATH") {
                trace_cfg.path = (*cmd)[3];
                if (trace_stream.is_open())
                  trace_stream.close();
                if (trace_cfg.enabled)
                  trace_stream.open(trace_cfg.path, std::ios::app);
                st.out += pomai_cache::resp_simple("OK");
              } else if (cmd->size() == 4 && upper((*cmd)[2]) == "TRACE.SAMPLE_RATE") {
                try {
                  trace_cfg.sample_rate = std::clamp(std::stod((*cmd)[3]), 0.0, 1.0);
                  st.out += pomai_cache::resp_simple("OK");
                } catch (...) {
                  st.out += pomai_cache::resp_error("invalid sample rate");
                }
              } else {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error("unsupported CONFIG SET");
              }
            } else {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("CONFIG GET|SET");
            }
          } else {
            ++stats.rejected_requests;
            st.out += pomai_cache::resp_error("unknown command");
          }

          const auto latency_us = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(pomai_cache::Clock::now() - op_start).count());
          if (latency_us > 5000) {
            slowlog.push_back({op_name, latency_us, static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(pomai_cache::Clock::now().time_since_epoch()).count())});
            if (slowlog.size() > max_slowlog)
              slowlog.pop_front();
          }

          if (trace_cfg.enabled && sample_dist(rng) <= trace_cfg.sample_rate) {
            if (!trace_stream.is_open())
              trace_stream.open(trace_cfg.path, std::ios::app);
            if (trace_stream.is_open()) {
              const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(pomai_cache::Clock::now().time_since_epoch()).count();
              const std::string owner = cmd->size() > 1 ? "default" : "n/a";
              const std::size_t key_hash = first_key.empty() ? 0 : std::hash<std::string>{}(first_key);
              std::ostringstream line;
              line << "{\"ts_ms\":" << now_ms << ",\"op\":\"" << op_name << "\",\"key_hash\":" << key_hash << ",\"value_size\":";
              if (op_name == "SET" && cmd->size() >= 3)
                line << (*cmd)[2].size();
              else
                line << 0;
              line << ",\"ttl_class\":\"" << (op_name == "SET" && cmd->size() > 3 ? "with_ttl" : "none") << "\",\"owner\":\"" << owner << "\",\"result\":\"" << (hit ? "hit" : "miss") << "\",\"lat_bucket\":\"" << latency_bucket(latency_us) << "\",\"policy_version\":\"" << engine.policy().params().version << "\",\"rng_seed\":" << rng_seed << "}";
              trace_stream << line.str() << "\n";
              trace_ring.push_back(line.str());
              if (trace_ring.size() > max_trace_ring)
                trace_ring.pop_front();
            } else {
              ++trace_cfg.dropped;
            }
          }

          if (st.out.size() > max_pending_out) {
            ++stats.rejected_requests;
            to_close.push_back(fd);
            break;
          }
        }
      }

      if (FD_ISSET(fd, &writefds) && !st.out.empty()) {
        const std::size_t send_bytes = std::min<std::size_t>(st.out.size(), 8192);
        ssize_t w = send(fd, st.out.data(), send_bytes, 0);
        if (w <= 0)
          to_close.push_back(fd);
        else
          st.out.erase(0, static_cast<std::size_t>(w));
      }
    }

    std::sort(to_close.begin(), to_close.end());
    to_close.erase(std::unique(to_close.begin(), to_close.end()), to_close.end());
    for (int fd : to_close) {
      close(fd);
      clients.erase(fd);
    }
  }

  for (auto &[fd, _] : clients)
    close(fd);
  close(server_fd);
  if (trace_stream.is_open())
    trace_stream.close();
  return 0;
}
