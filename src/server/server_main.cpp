#include "pomai_cache/ai_cache.hpp"
#include "pomai_cache/engine.hpp"
#include "pomai_cache/engine_shard.hpp"
#include "pomai_cache/resp.hpp"

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
  pomai_cache::RespParser parser;
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
      std::cerr << "Thread " << id_ << " bind failed\n";
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
            std::cout << "DEBUG: Accept on fd " << cfd << "\n";
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
              std::cout << "DEBUG: Received " << r << " bytes from fd " << fd << "\n";
              st.parser.feed(std::string_view(st.buf, r));
              while (auto cmd = st.parser.next_command()) {
                std::cout << "DEBUG: Command " << (cmd->empty() ? "" : std::string((*cmd)[0])) << "\n";
                handle_command(st, *cmd);
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
  void handle_command(ClientState& st, const std::vector<std::string>& cmd) {
    if (cmd.empty()) return;
    std::string c = upper(cmd[0]);
    
    if (c == "PING") {
      st.out += pomai_cache::resp_simple("PONG");
      return;
    }

    // Security (AUTH)
    if (c == "AUTH") {
      if (cmd.size() == 2 && cmd[1] == "pomai_admin") { // Simulating a simple admin password
        st.out += pomai_cache::resp_simple("OK");
      } else {
        st.out += pomai_cache::resp_error("invalid password");
      }
      return;
    }

    // Key-based routing
    if (c == "GET" || c == "SET" || c == "DEL" || c == "EXPIRE" || c == "TTL") {
      if (cmd.size() < 2) {
        st.out += pomai_cache::resp_error("wrong number of arguments");
        return;
      }
      std::string key(cmd[1]);
      auto* shard = pomai_cache::ShardSet::instance().get_shard(key);
      if (!shard) {
        st.out += pomai_cache::resp_error("no shards available");
        return;
      }
      auto& engine = shard->engine();

      if (c == "GET") {
        auto v = engine.get(key);
        st.out += v ? pomai_cache::resp_bulk(std::string(v->begin(), v->end())) : pomai_cache::resp_null();
      } else if (c == "SET") {
        if (cmd.size() < 3) st.out += pomai_cache::resp_error("SET key value");
        else {
          std::vector<uint8_t> val(cmd[2].begin(), cmd[2].end());
          engine.set(key, val, std::nullopt, "default");
          shard->journal().record(pomai_cache::OpCode::SET, cmd);
          st.out += pomai_cache::resp_simple("OK");
        }
      } else if (c == "DEL") {
        st.out += pomai_cache::resp_integer(engine.del({key}));
      } else if (c == "EXPIRE") {
        std::uint64_t ttl_s = 0;
        if (cmd.size() == 3 && parse_u64(cmd[2], ttl_s)) {
          st.out += pomai_cache::resp_integer(engine.expire(key, ttl_s));
        } else st.out += pomai_cache::resp_error("invalid expire");
      } else if (c == "TTL") {
        auto t = engine.ttl(key);
        st.out += pomai_cache::resp_integer(t ? *t : -2);
      }
      return;
    }

    // Multi-key routing (MGET)
    if (c == "MGET") {
      if (cmd.size() < 2) st.out += pomai_cache::resp_error("MGET key [key...]");
      else {
        std::vector<std::string> arr;
        for (size_t i = 1; i < cmd.size(); ++i) {
          std::string key(cmd[i]);
          auto* shard = pomai_cache::ShardSet::instance().get_shard(key);
          if (!shard) arr.push_back(pomai_cache::resp_null());
          else {
            auto v = shard->engine().get(key);
            arr.push_back(v ? pomai_cache::resp_bulk(std::string(v->begin(), v->end())) : pomai_cache::resp_null());
          }
        }
        st.out += pomai_cache::resp_array(arr);
      }
      return;
    }

    // AI commands routing
    if (c.rfind("AI.", 0) == 0) {
      if (cmd.size() < 2) {
        st.out += pomai_cache::resp_error("AI commands require at least a key");
        return;
      }
      // Most AI commands take the key as the second argument (cmd[1] or cmd[2])
      // For now, let's assume cmd[2] is the key for AI.PUT and cmd[1] for AI.GET
      std::string key;
      if (c == "AI.PUT" && cmd.size() >= 3) key = std::string(cmd[2]);
      else if (c == "AI.GET" && cmd.size() >= 2) key = std::string(cmd[1]);
      else if (c == "AI.EXPLAIN" && cmd.size() >= 2) key = std::string(cmd[1]);
      
      if (!key.empty()) {
        auto* shard = pomai_cache::ShardSet::instance().get_shard(key);
        if (shard) {
          pomai_cache::AiArtifactCache ai_cache(shard->engine());
          if (c == "AI.PUT" && cmd.size() == 5) {
            std::vector<uint8_t> payload(cmd[4].begin(), cmd[4].end());
            std::string err;
            if (ai_cache.put(std::string(cmd[1]), key, std::string(cmd[3]), payload, &err)) {
              shard->journal().record(pomai_cache::OpCode::AI_PUT, cmd);
              st.out += pomai_cache::resp_simple("OK");
            } else st.out += pomai_cache::resp_error(err);
          } else if (c == "AI.GET") {
             auto v = ai_cache.get(key);
             if (!v) st.out += pomai_cache::resp_null();
             else {
               std::vector<std::string> arr{
                 pomai_cache::resp_bulk(pomai_cache::AiArtifactCache::meta_to_json(v->meta)),
                 pomai_cache::resp_bulk(std::string(v->payload.begin(), v->payload.end()))
               };
               st.out += pomai_cache::resp_array(arr);
             }
          } else if (c == "AI.EXPLAIN") {
            st.out += pomai_cache::resp_bulk(ai_cache.explain(key));
          }
        } else st.out += pomai_cache::resp_error("no shard for AI key");
      } else st.out += pomai_cache::resp_error("unsupported or malformed AI command in sharded mode");
      return;
    }

    st.out += pomai_cache::resp_error("unknown command");
  }

  int port_;
  pomai_cache::EngineConfig cfg_;
  int id_;
};

} // namespace

int main(int argc, char **argv) {
  int port = 6379;
  int threads = std::thread::hardware_concurrency();
  std::size_t memory_limit = 128 * 1024 * 1024;
  std::string data_dir = "./data";

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--port" && i + 1 < argc) port = std::stoi(argv[++i]);
    else if (a == "--memory" && i + 1 < argc) memory_limit = std::stoull(argv[++i]);
    else if (a == "--threads" && i + 1 < argc) threads = std::stoi(argv[++i]);
  }

  pomai_cache::EngineConfig cfg;
  cfg.memory_limit_bytes = memory_limit / threads;
  cfg.data_dir = data_dir;

  std::cout << "Starting PomaiCache with " << threads << " cores...\n";
  std::vector<std::thread> workers;
  for (int i = 0; i < threads; ++i) {
    workers.emplace_back([port, cfg, i]() {
      UringWorker(port, cfg, i).run();
    });
  }

  std::signal(SIGINT, on_sigint);
  for (auto& t : workers) t.join();

  return 0;
}
