#include "pomai_cache/ai_cache.hpp"
#include "pomai_cache/engine.hpp"
#include "pomai_cache/resp.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <csignal>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <optional>
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

struct ClientState {
  pomai_cache::RespParser parser;
  std::string out;
  std::size_t bytes_pending{0};
};

struct ServerStats {
  std::uint64_t rejected_requests{0};
  std::uint64_t total_request_bytes{0};
  std::uint64_t request_count{0};
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
  std::string data_dir = "./data";
  bool ssd_enabled = false;
  std::size_t ssd_value_min_bytes = 32 * 1024;
  std::size_t ssd_max_bytes = 2ULL * 1024 * 1024 * 1024;
  std::size_t promotion_hits = 3;
  double demotion_pressure = 0.90;
  std::size_t ssd_read_mb_s = 256;
  std::size_t ssd_write_mb_s = 256;
  std::string fsync_policy = "never";

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
    else if (a == "--data-dir" && i + 1 < argc)
      data_dir = argv[++i];
    else if (a == "--ssd-enabled")
      ssd_enabled = true;
    else if (a == "--ssd-value-min-bytes" && i + 1 < argc)
      ssd_value_min_bytes = std::stoull(argv[++i]);
    else if (a == "--ssd-max-bytes" && i + 1 < argc)
      ssd_max_bytes = std::stoull(argv[++i]);
    else if (a == "--promotion-hits" && i + 1 < argc)
      promotion_hits = std::stoull(argv[++i]);
    else if (a == "--demotion-pressure" && i + 1 < argc)
      demotion_pressure = std::stod(argv[++i]);
    else if (a == "--ssd-read-mb-s" && i + 1 < argc)
      ssd_read_mb_s = std::stoull(argv[++i]);
    else if (a == "--ssd-write-mb-s" && i + 1 < argc)
      ssd_write_mb_s = std::stoull(argv[++i]);
    else if (a == "--fsync" && i + 1 < argc)
      fsync_policy = argv[++i];
  }

  auto policy = pomai_cache::make_policy_by_name(policy_mode);
  pomai_cache::TierConfig tier_cfg{};
  tier_cfg.ssd_enabled = ssd_enabled;
  tier_cfg.ssd_value_min_bytes = ssd_value_min_bytes;
  tier_cfg.ssd_max_bytes = ssd_max_bytes;
  tier_cfg.ram_max_bytes = memory_limit;
  tier_cfg.promotion_hits = promotion_hits;
  tier_cfg.demotion_pressure = demotion_pressure;
  tier_cfg.ssd_max_read_mb_s = ssd_read_mb_s;
  tier_cfg.ssd_max_write_mb_s = ssd_write_mb_s;
  pomai_cache::FsyncMode fsync_mode = pomai_cache::FsyncMode::EverySec;
  if (upper(fsync_policy) == "NEVER")
    fsync_mode = pomai_cache::FsyncMode::Never;
  else if (upper(fsync_policy) == "ALWAYS")
    fsync_mode = pomai_cache::FsyncMode::Always;
  pomai_cache::EngineConfig engine_cfg{
      memory_limit, 256, 1024 * 1024, 128, 64, data_dir, tier_cfg, fsync_mode};
  pomai_cache::Engine engine(engine_cfg, std::move(policy));
  pomai_cache::AiArtifactCache ai_cache(engine);
  std::string reload_err;
  engine.reload_params(params_path, &reload_err);

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
      if (fd > maxfd)
        maxfd = fd;
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

          const auto c = upper((*cmd)[0]);
          if (c == "PING")
            st.out += pomai_cache::resp_simple("PONG");
          else if (c == "SET") {
            if (cmd->size() < 3) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error(
                  "SET key value [EX sec|PX ms] [OWNER name]");
            } else {
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
                } else if (opt == "OWNER")
                  owner = (*cmd)[i + 1];
                if (!valid)
                  break;
              }
              if (!valid) {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error("invalid numeric argument");
              } else {
                std::vector<std::uint8_t> val((*cmd)[2].begin(),
                                              (*cmd)[2].end());
                std::string err;
                if (engine.set((*cmd)[1], val, ttl_ms, owner, &err))
                  st.out += pomai_cache::resp_simple("OK");
                else {
                  ++stats.rejected_requests;
                  st.out += pomai_cache::resp_error(err);
                }
              }
            }
          } else if (c == "GET") {
            if (cmd->size() != 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("GET key");
            } else {
              auto v = engine.get((*cmd)[1]);
              if (!v)
                st.out += pomai_cache::resp_null();
              else
                st.out +=
                    pomai_cache::resp_bulk(std::string(v->begin(), v->end()));
            }
          } else if (c == "MGET") {
            if (cmd->size() < 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("MGET key [key...]");
            } else {
              std::vector<std::string> keys(cmd->begin() + 1, cmd->end());
              auto vals = engine.mget(keys);
              std::vector<std::string> arr;
              arr.reserve(vals.size());
              for (auto &v : vals)
                arr.push_back(v ? pomai_cache::resp_bulk(
                                      std::string(v->begin(), v->end()))
                                : pomai_cache::resp_null());
              st.out += pomai_cache::resp_array(arr);
            }
          } else if (c == "DEL") {
            if (cmd->size() < 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("DEL key [key...]");
            } else {
              std::vector<std::string> keys(cmd->begin() + 1, cmd->end());
              st.out += pomai_cache::resp_integer(engine.del(keys));
            }
          } else if (c == "EXPIRE") {
            if (cmd->size() != 3) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("EXPIRE key seconds");
            } else {
              std::uint64_t ttl_s = 0;
              if (!parse_u64((*cmd)[2], ttl_s)) {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error("invalid numeric argument");
              } else {
                st.out += pomai_cache::resp_integer(
                    engine.expire((*cmd)[1], ttl_s) ? 1 : 0);
              }
            }
          } else if (c == "TTL") {
            if (cmd->size() != 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("TTL key");
            } else {
              auto t = engine.ttl((*cmd)[1]);
              st.out += pomai_cache::resp_integer(t ? *t : -2);
            }
          } else if (c == "INFO") {
            std::ostringstream info;
            info << engine.info();
            info << "connected_clients:" << clients.size() << "\n";
            info << "rejected_requests:" << stats.rejected_requests << "\n";
            const double avg_bytes =
                stats.request_count == 0
                    ? 0.0
                    : static_cast<double>(stats.total_request_bytes) /
                          static_cast<double>(stats.request_count);
            info << "avg_request_bytes:" << avg_bytes << "\n";
            st.out += pomai_cache::resp_bulk(info.str());
          } else if (c == "CONFIG") {
            if (cmd->size() >= 2 && upper((*cmd)[1]) == "GET") {
              if (cmd->size() == 3 && upper((*cmd)[2]) == "POLICY") {
                std::vector<std::string> arr{
                    pomai_cache::resp_bulk("policy"),
                    pomai_cache::resp_bulk(engine.policy().name())};
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
              } else {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error("unsupported CONFIG SET");
              }
            } else {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("CONFIG GET|SET");
            }
          } else if (c == "AI.PUT") {
            if (cmd->size() != 5) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error(
                  "AI.PUT <type> <key> <meta_json> <payload_bytes>");
            } else {
              std::vector<std::uint8_t> payload((*cmd)[4].begin(),
                                                (*cmd)[4].end());
              std::string err;
              if (!ai_cache.put((*cmd)[1], (*cmd)[2], (*cmd)[3], payload,
                                &err)) {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error(err);
              } else {
                st.out += pomai_cache::resp_simple("OK");
              }
            }
          } else if (c == "AI.GET") {
            if (cmd->size() != 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("AI.GET <key>");
            } else {
              auto v = ai_cache.get((*cmd)[1]);
              if (!v.has_value()) {
                st.out += pomai_cache::resp_null();
              } else {
                std::vector<std::string> arr{
                    pomai_cache::resp_bulk(
                        pomai_cache::AiArtifactCache::meta_to_json(v->meta)),
                    pomai_cache::resp_bulk(
                        std::string(v->payload.begin(), v->payload.end()))};
                st.out += pomai_cache::resp_array(arr);
              }
            }
          } else if (c == "AI.MGET") {
            if (cmd->size() < 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("AI.MGET <key...>");
            } else {
              std::vector<std::string> keys(cmd->begin() + 1, cmd->end());
              auto vals = ai_cache.mget(keys);
              std::vector<std::string> arr;
              for (auto &v : vals) {
                if (!v.has_value()) {
                  arr.push_back(pomai_cache::resp_null());
                } else {
                  std::vector<std::string> pair{
                      pomai_cache::resp_bulk(
                          pomai_cache::AiArtifactCache::meta_to_json(v->meta)),
                      pomai_cache::resp_bulk(
                          std::string(v->payload.begin(), v->payload.end()))};
                  arr.push_back(pomai_cache::resp_array(pair));
                }
              }
              st.out += pomai_cache::resp_array(arr);
            }
          } else if (c == "AI.EMB.PUT") {
            if (cmd->size() != 7) {
              ++stats.rejected_requests;
              st.out +=
                  pomai_cache::resp_error("AI.EMB.PUT <key> <model_id> <dim> "
                                          "<dtype> <ttl_sec> <vector_bytes>");
            } else {
              std::uint64_t dim = 0, ttl_s = 0;
              if (!parse_u64((*cmd)[3], dim) || !parse_u64((*cmd)[5], ttl_s)) {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error("invalid numeric argument");
              } else {
                if ((*cmd)[4] != "float" && (*cmd)[4] != "float16" &&
                    (*cmd)[4] != "int8") {
                  ++stats.rejected_requests;
                  st.out += pomai_cache::resp_error("invalid vector header");
                } else {
                  std::vector<std::uint8_t> payload((*cmd)[6].begin(),
                                                    (*cmd)[6].end());
                  std::ostringstream meta;
                  meta << "{\"artifact_type\":\"embedding\",\"owner\":"
                          "\"vector\",\"schema_version\":\"v1\","
                       << "\"model_id\":\"" << (*cmd)[2] << "\",\"dim\":" << dim
                       << ",\"dtype\":\"" << (*cmd)[4]
                       << "\",\"ttl_deadline\":" << (ttl_s * 1000ULL) << "}";
                  std::string err;
                  if (!ai_cache.put("embedding", (*cmd)[1], meta.str(), payload,
                                    &err)) {
                    ++stats.rejected_requests;
                    st.out += pomai_cache::resp_error(err);
                  } else {
                    st.out += pomai_cache::resp_simple("OK");
                  }
                }
              }
            }
          } else if (c == "AI.EMB.GET") {
            if (cmd->size() != 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("AI.EMB.GET <key>");
            } else {
              auto v = ai_cache.get((*cmd)[1]);
              if (!v.has_value()) {
                st.out += pomai_cache::resp_null();
              } else {
                std::vector<std::string> pair{
                    pomai_cache::resp_bulk(
                        pomai_cache::AiArtifactCache::meta_to_json(v->meta)),
                    pomai_cache::resp_bulk(
                        std::string(v->payload.begin(), v->payload.end()))};
                st.out += pomai_cache::resp_array(pair);
              }
            }
          } else if (c == "AI.INVALIDATE") {
            if (cmd->size() != 3) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error(
                  "AI.INVALIDATE EPOCH|MODEL|PREFIX <value>");
            } else {
              auto mode = upper((*cmd)[1]);
              std::size_t n = 0;
              if (mode == "EPOCH")
                n = ai_cache.invalidate_epoch((*cmd)[2]);
              else if (mode == "MODEL")
                n = ai_cache.invalidate_model((*cmd)[2]);
              else if (mode == "PREFIX")
                n = ai_cache.invalidate_prefix((*cmd)[2]);
              else {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error(
                    "AI.INVALIDATE EPOCH|MODEL|PREFIX <value>");
                n = static_cast<std::size_t>(-1);
              }
              if (n != static_cast<std::size_t>(-1))
                st.out += pomai_cache::resp_integer(static_cast<long long>(n));
            }
          } else if (c == "AI.STATS") {
            st.out += pomai_cache::resp_bulk(ai_cache.stats());
          } else if (c == "AI.TOP") {
            if (cmd->size() < 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("AI.TOP HOT|COSTLY [N]");
            } else {
              std::uint64_t n = 10;
              if (cmd->size() == 3 && !parse_u64((*cmd)[2], n)) {
                ++stats.rejected_requests;
                st.out += pomai_cache::resp_error("invalid numeric argument");
              } else {
                auto mode = upper((*cmd)[1]);
                if (mode == "HOT")
                  st.out += pomai_cache::resp_bulk(
                      ai_cache.top_hot(static_cast<std::size_t>(n)));
                else if (mode == "COSTLY")
                  st.out += pomai_cache::resp_bulk(
                      ai_cache.top_costly(static_cast<std::size_t>(n)));
                else {
                  ++stats.rejected_requests;
                  st.out += pomai_cache::resp_error("AI.TOP HOT|COSTLY [N]");
                }
              }
            }
          } else if (c == "AI.EXPLAIN") {
            if (cmd->size() != 2) {
              ++stats.rejected_requests;
              st.out += pomai_cache::resp_error("AI.EXPLAIN <key>");
            } else {
              st.out += pomai_cache::resp_bulk(ai_cache.explain((*cmd)[1]));
            }
          } else {
            ++stats.rejected_requests;
            st.out += pomai_cache::resp_error("unknown command");
          }
          if (st.out.size() > max_pending_out) {
            ++stats.rejected_requests;
            to_close.push_back(fd);
            break;
          }
        }
      }
      if (FD_ISSET(fd, &writefds) && !st.out.empty()) {
        const std::size_t send_bytes =
            std::min<std::size_t>(st.out.size(), 8192);
        ssize_t w = send(fd, st.out.data(), send_bytes, 0);
        if (w <= 0)
          to_close.push_back(fd);
        else
          st.out.erase(0, static_cast<std::size_t>(w));
      }
    }
    std::sort(to_close.begin(), to_close.end());
    to_close.erase(std::unique(to_close.begin(), to_close.end()),
                   to_close.end());
    for (int fd : to_close) {
      close(fd);
      clients.erase(fd);
    }
  }

  for (auto &[fd, _] : clients)
    close(fd);
  close(server_fd);
  return 0;
}
