#include "pomai_cache/ai_cache.hpp"
#include "pomai_cache/engine.hpp"
#include "pomai_cache/engine_shard.hpp"
#include "pomai_cache/http.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <charconv>
#include <csignal>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <optional>
#include <sstream>
#include <liburing.h>
#include <vector>
#include <thread>

namespace {
volatile std::sig_atomic_t running = 1;
void on_sigint(int) { running = 0; }

std::string upper(std::string_view s) {
  std::string res;
  res.reserve(s.size());
  for (char c : s) res += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  return res;
}

bool parse_u64(std::string_view s, std::uint64_t &out) {
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
  return ec == std::errc{};
}

struct ClientState {
  pomai_cache::HttpParser parser;
  std::string out;
  std::string sending;
  int fd;
  bool is_sending{false};
  char buf[4096];
};

class UringWorker {
public:
  UringWorker(int port, const pomai_cache::EngineConfig& cfg, int id) 
    : port_(port), cfg_(cfg), id_(id) {}

  void run() {
    auto policy = pomai_cache::make_policy_by_name("pomai_cost");
    pomai_cache::EngineShard::InitThreadLocal(id_, cfg_, std::move(policy));
    auto* shard = pomai_cache::EngineShard::tlocal();
    pomai_cache::ShardSet::instance().add_shard(shard);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);
    if (bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
      std::cerr << "Worker " << id_ << " bind failed\n";
      return;
    }
    listen(listen_fd, 128);

    struct io_uring ring;
    io_uring_queue_init(1024, &ring, 0);

    add_accept_sqe(&ring, listen_fd);
    io_uring_submit(&ring);

    std::unordered_map<int, std::unique_ptr<ClientState>> clients;

    while (running) {
      shard->engine().tick();
      
      struct io_uring_cqe *cqe;
      struct __kernel_timespec ts{0, 10000000}; // 10ms
      int ret = io_uring_wait_cqe_timeout(&ring, &cqe, &ts);
      
      if (ret < 0) {
        if (ret == -ETIME) {
           io_uring_submit(&ring);
           continue;
        }
        break;
      }

      int head;
      unsigned count = 0;
      io_uring_for_each_cqe(&ring, head, cqe) {
        count++;
        auto* data = reinterpret_cast<void*>(cqe->user_data);
        uint64_t type = reinterpret_cast<uintptr_t>(data) & 0x7;
        int fd = static_cast<int>(reinterpret_cast<uintptr_t>(data) >> 3);

        if (fd == listen_fd) {
          int cfd = cqe->res;
          if (cfd >= 0) {
            auto client = std::make_unique<ClientState>();
            client->fd = cfd;
            clients[cfd] = std::move(client);
            add_recv_sqe(&ring, cfd, clients[cfd]->buf);
          }
          add_accept_sqe(&ring, listen_fd);
        } else {
          auto it = clients.find(fd);
          if (it == clients.end()) continue;
          auto& st = *it->second;

          if (type == 1) { // RECV
            int r = cqe->res;
            if (r <= 0) {
              close(fd);
              clients.erase(it);
            } else {
              st.parser.feed(std::string_view(st.buf, r));
              while (auto req = st.parser.next_request()) {
                handle_http_request(st, *req);
              }
              if (!st.is_sending && !st.out.empty()) {
                st.is_sending = true;
                st.sending.swap(st.out);
                add_send_sqe(&ring, fd, st.sending);
              }
              add_recv_sqe(&ring, fd, st.buf);
            }
          } else if (type == 2) { // SEND
            st.is_sending = false;
            if (cqe->res > 0) {
              st.sending.erase(0, cqe->res);
            }
            if (!st.sending.empty()) {
              st.is_sending = true;
              add_send_sqe(&ring, fd, st.sending);
            } else if (!st.out.empty()) {
              st.is_sending = true;
              st.sending.swap(st.out);
              add_send_sqe(&ring, fd, st.sending);
            }
          }
        }
      }
      io_uring_cq_advance(&ring, count);
      io_uring_submit(&ring);
    }
    io_uring_queue_exit(&ring);
    close(listen_fd);
  }

private:
  void add_accept_sqe(struct io_uring *ring, int fd) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_accept(sqe, fd, nullptr, nullptr, 0);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(static_cast<uintptr_t>(fd) << 3));
  }

  void add_recv_sqe(struct io_uring *ring, int fd, char* buf) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_recv(sqe, fd, buf, 4096, 0);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>((static_cast<uintptr_t>(fd) << 3) | 1));
  }

  void add_send_sqe(struct io_uring *ring, int fd, const std::string& data) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_send(sqe, fd, data.data(), data.size(), 0);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>((static_cast<uintptr_t>(fd) << 3) | 2));
  }

private:
  std::vector<std::string> split_path(const std::string& path) {
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (start < path.size()) {
      if (path[start] == '/') {
        start++;
        continue;
      }
      auto end = path.find('/', start);
      if (end == std::string::npos) {
        parts.push_back(path.substr(start));
        break;
      }
      parts.push_back(path.substr(start, end - start));
      start = end + 1;
    }
    return parts;
  }

  void handle_http_request(ClientState& st, const pomai_cache::HttpRequest& req) {
    auto parts = split_path(req.path);
    if (parts.empty()) {
      st.out += pomai_cache::http_response(404, "Not Found", "Path missing");
      return;
    }

    std::string base = parts[0];

    // INFO
    if (base == "info" && req.method == "GET") {
      auto shards = pomai_cache::ShardSet::instance().all_shards();
      std::string combined;
      for (auto* s : shards) combined += s->engine().info();
      st.out += pomai_cache::http_response(200, "OK", combined);
      return;
    }

    // CONFIG
    if (base == "config" && req.method == "GET") {
      if (parts.size() >= 2 && parts[1] == "policy") {
        auto shards = pomai_cache::ShardSet::instance().all_shards();
        std::string name = shards.empty() ? "unknown" : shards[0]->engine().policy().name();
        st.out += pomai_cache::http_response(200, "OK", name);
      } else {
        st.out += pomai_cache::http_response(400, "Bad Request", "CONFIG param not supported");
      }
      return;
    }

    // KEY Ops: /key/<x>
    if (base == "key" && parts.size() >= 2) {
      std::string key = parts[1];
      auto* shard = pomai_cache::ShardSet::instance().get_shard(key);
      if (!shard) {
        st.out += pomai_cache::http_response(503, "Service Unavailable", "No shards");
        return;
      }
      auto& engine = shard->engine();

      if (req.method == "GET") {
        auto v = engine.get(key);
        if (v) {
          st.out += pomai_cache::http_response(200, "OK", std::string(v->begin(), v->end()));
        } else {
          st.out += pomai_cache::http_response(404, "Not Found", "Key not found");
        }
      } else if (req.method == "POST") {
        std::vector<uint8_t> val(req.body.begin(), req.body.end());
        std::optional<std::uint64_t> ttl_ms;
        
        auto it = req.query_params.find("ex");
        if (it != req.query_params.end()) {
          std::uint64_t v = 0;
          if (parse_u64(it->second, v)) ttl_ms = v * 1000;
        }
        it = req.query_params.find("px");
        if (it != req.query_params.end()) {
          std::uint64_t v = 0;
          if (parse_u64(it->second, v)) ttl_ms = v;
        }

        std::string set_err;
        if (engine.set(key, val, ttl_ms, "default", &set_err)) {
          std::vector<std::string> jcmd = {"SET", key, req.body};
          if (ttl_ms) { jcmd.push_back("PX"); jcmd.push_back(std::to_string(*ttl_ms)); }
          shard->journal().record(pomai_cache::OpCode::SET, jcmd);
          st.out += pomai_cache::http_response(200, "OK", "OK");
        } else {
          st.out += pomai_cache::http_response(400, "Bad Request", set_err);
        }
      } else if (req.method == "DELETE") {
        int d = engine.del({key});
        st.out += pomai_cache::http_response(200, "OK", std::to_string(d));
      } else {
        st.out += pomai_cache::http_response(405, "Method Not Allowed", "Use GET, POST or DELETE");
      }
      return;
    }

    // AI Operations
    if (base == "ai" && parts.size() >= 2) {
      std::string op = parts[1];
      
      if (op == "stats" && req.method == "GET") {
        auto shards = pomai_cache::ShardSet::instance().all_shards();
        std::string combined;
        for (auto* s : shards) combined += s->ai_cache().stats();
        st.out += pomai_cache::http_response(200, "OK", combined);
        return;
      }
      
      if (op == "cost_report" && req.method == "GET") {
        auto shards = pomai_cache::ShardSet::instance().all_shards();
        std::ostringstream os;
        double total_saved = 0;
        std::uint64_t total_tokens = 0, total_latency = 0, total_hits = 0;
        for (auto* s : shards) {
          auto r = s->ai_cache().cost_report();
          total_saved += r.total_dollar_saved;
          total_tokens += r.total_tokens_saved;
          total_latency += r.total_latency_saved_ms;
          total_hits += r.total_hits;
        }
        os << "total_dollar_saved:" << total_saved << "\n";
        os << "total_tokens_saved:" << total_tokens << "\n";
        os << "total_latency_saved_ms:" << total_latency << "\n";
        os << "total_hits:" << total_hits << "\n";
        st.out += pomai_cache::http_response(200, "OK", os.str());
        return;
      }

      if (op == "budget" && req.method == "POST") {
        auto it = req.query_params.find("value");
        if (it != req.query_params.end()) {
          double budget = std::stod(it->second);
          auto shards = pomai_cache::ShardSet::instance().all_shards();
          for (auto* s : shards) s->ai_cache().set_budget(budget / static_cast<double>(shards.size()));
          st.out += pomai_cache::http_response(200, "OK", "OK");
        } else {
          st.out += pomai_cache::http_response(400, "Bad Request", "Missing value");
        }
        return;
      }

      if (op == "invalidate" && req.method == "POST" && parts.size() >= 4) {
        std::string subcmd = upper(parts[2]);
        std::string arg = parts[3];
        std::size_t total = 0;
        auto shards = pomai_cache::ShardSet::instance().all_shards();
        for (auto* s : shards) {
          if (subcmd == "EPOCH") total += s->ai_cache().invalidate_epoch(arg);
          else if (subcmd == "MODEL") total += s->ai_cache().invalidate_model(arg);
          else if (subcmd == "PREFIX") total += s->ai_cache().invalidate_prefix(arg);
          else if (subcmd == "CASCADE") total += s->ai_cache().invalidate_cascade(arg);
        }
        st.out += pomai_cache::http_response(200, "OK", std::to_string(total));
        return;
      }

      if (op == "sim" && parts.size() >= 3) {
        std::string subcmd = parts[2];
        
        if (subcmd == "put" && req.method == "POST" && parts.size() >= 4) {
          std::string key = parts[3];
          auto* shard = pomai_cache::ShardSet::instance().get_shard(key);
          if (!shard) { st.out += pomai_cache::http_response(503, "Service Unavailable", "no shard"); return; }
          
          auto it = req.query_params.find("vec");
          if (it == req.query_params.end()) { st.out += pomai_cache::http_response(400, "Bad Request", "missing vec"); return; }
          
          std::vector<float> vec;
          std::istringstream vss(it->second);
          float val;
          while (vss >> val) { vec.push_back(val); if (vss.peek() == ',') vss.ignore(); }
          
          std::vector<uint8_t> payload(req.body.begin(), req.body.end());
          
          auto meta_it = req.query_params.find("meta");
          std::string meta_json = meta_it != req.query_params.end() ? meta_it->second : "{\"artifact_type\":\"embedding\",\"owner\":\"vector\",\"schema_version\":\"v1\"}";
          
          std::string err;
          if (shard->ai_cache().sim_put(key, vec, payload, meta_json, &err))
            st.out += pomai_cache::http_response(200, "OK", "OK");
          else
            st.out += pomai_cache::http_response(400, "Bad Request", err);
          return;
        }
        
        if (subcmd == "get" && req.method == "GET") {
          auto it = req.query_params.find("vec");
          if (it == req.query_params.end()) { st.out += pomai_cache::http_response(400, "Bad Request", "missing vec"); return; }
          std::vector<float> query;
          std::istringstream vss(it->second);
          float val;
          while (vss >> val) { query.push_back(val); if (vss.peek() == ',') vss.ignore(); }
          
          std::size_t top_k = 1;
          float threshold = 0.9f;
          auto kt = req.query_params.find("topk");
          if (kt != req.query_params.end()) top_k = std::stoull(kt->second);
          auto tt = req.query_params.find("threshold");
          if (tt != req.query_params.end()) threshold = std::stof(tt->second);
          
          auto shards = pomai_cache::ShardSet::instance().all_shards();
          std::ostringstream arr;
          for (auto* s : shards) {
            auto results = s->ai_cache().sim_get(query, top_k, threshold);
            for (const auto& r : results) {
              arr << "key:" << r.key << " score:" << r.score << "\n";
              arr << "meta:" << pomai_cache::AiArtifactCache::meta_to_json(r.value.meta) << "\n";
              arr << "body:" << std::string(r.value.payload.begin(), r.value.payload.end()) << "\n";
            }
          }
          st.out += pomai_cache::http_response(200, "OK", arr.str());
          return;
        }
      }
      
      if (op == "put" && req.method == "POST" && parts.size() >= 4) {
        std::string type = parts[2];
        std::string key = parts[3];
        
        auto* shard = pomai_cache::ShardSet::instance().get_shard(key);
        if (!shard) { st.out += pomai_cache::http_response(503, "Service Unavailable", "no shard"); return; }
        
        std::vector<uint8_t> payload(req.body.begin(), req.body.end());
        auto it = req.query_params.find("meta");
        std::string meta = it != req.query_params.end() ? it->second : "{}";
        
        std::string err;
        if (shard->ai_cache().put(type, key, meta, payload, &err)) {
          std::vector<std::string> jcmd = {"AI.PUT", type, key, meta, req.body};
          shard->journal().record(pomai_cache::OpCode::AI_PUT, jcmd);
          st.out += pomai_cache::http_response(200, "OK", "OK");
        } else {
          st.out += pomai_cache::http_response(400, "Bad Request", err);
        }
        return;
      }
      
      if (op == "get" && req.method == "GET" && parts.size() >= 3) {
        std::string key = parts[2];
        auto* shard = pomai_cache::ShardSet::instance().get_shard(key);
        if (!shard) { st.out += pomai_cache::http_response(503, "Service Unavailable", "no shard"); return; }
        
        auto v = shard->ai_cache().get(key);
        if (!v) {
          st.out += pomai_cache::http_response(404, "Not Found", "");
        } else {
          std::string resp_body = pomai_cache::AiArtifactCache::meta_to_json(v->meta) + "\n" + std::string(v->payload.begin(), v->payload.end());
          st.out += pomai_cache::http_response(200, "OK", resp_body);
        }
        return;
      }
    }

    st.out += pomai_cache::http_response(400, "Bad Request", "Unknown command");
  }

  int port_;
  pomai_cache::EngineConfig cfg_;
  int id_;
};

} // namespace

int main(int argc, char **argv) {
  int port = 6379;
  std::size_t memory_limit = 128 * 1024 * 1024;
  std::string data_dir = "./data";

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--port" && i + 1 < argc) port = std::stoi(argv[++i]);
    else if (a == "--memory" && i + 1 < argc) memory_limit = std::stoull(argv[++i]);
  }

  pomai_cache::EngineConfig cfg;
  cfg.memory_limit_bytes = memory_limit; // No division because it is single-threaded
  cfg.data_dir = data_dir;

  std::cout << "Starting PomaiCache on single core...\n";

  std::signal(SIGINT, on_sigint);
  
  // Single-threaded so just call run() on main thread
  UringWorker(port, cfg, 0).run();

  return 0;
}
