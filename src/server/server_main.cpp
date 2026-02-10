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
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
  return s;
}

struct ClientState {
  pomai_cache::RespParser parser;
  std::string out;
  std::size_t bytes_pending{0};
};

} // namespace

int main(int argc, char** argv) {
  int port = 6379;
  std::size_t max_connections = 512;
  std::size_t max_pending_out = 1 << 20;
  std::size_t memory_limit = 64 * 1024 * 1024;
  std::string policy_mode = "pomai_cost";
  std::string params_path = "config/policy_params.json";

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--port" && i + 1 < argc) port = std::stoi(argv[++i]);
    else if (a == "--memory" && i + 1 < argc) memory_limit = std::stoull(argv[++i]);
    else if (a == "--policy" && i + 1 < argc) policy_mode = argv[++i];
    else if (a == "--params" && i + 1 < argc) params_path = argv[++i];
  }

  auto policy = pomai_cache::make_policy_by_name(policy_mode);
  pomai_cache::Engine engine({memory_limit, 256, 1024 * 1024, 128}, std::move(policy));
  std::string reload_err;
  engine.reload_params(params_path, &reload_err);

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);
  if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    std::cerr << "bind failed\n";
    return 1;
  }
  if (listen(server_fd, 128) < 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  std::signal(SIGINT, on_sigint);
  std::unordered_map<int, ClientState> clients;
  std::cout << "pomai_cache_server listening on " << port << "\n";

  while (running) {
    engine.tick();
    fd_set readfds, writefds;
    FD_ZERO(&readfds);
    FD_ZERO(&writefds);
    FD_SET(server_fd, &readfds);
    int maxfd = server_fd;
    for (const auto& [fd, st] : clients) {
      FD_SET(fd, &readfds);
      if (!st.out.empty()) FD_SET(fd, &writefds);
      if (fd > maxfd) maxfd = fd;
    }
    timeval tv{0, 100000};
    int n = select(maxfd + 1, &readfds, &writefds, nullptr, &tv);
    if (n < 0) continue;

    if (FD_ISSET(server_fd, &readfds)) {
      int cfd = accept(server_fd, nullptr, nullptr);
      if (cfd >= 0) {
        if (clients.size() >= max_connections) {
          const auto msg = pomai_cache::resp_error("connection limit reached");
          send(cfd, msg.data(), msg.size(), 0);
          close(cfd);
        } else {
          clients[cfd] = {};
        }
      }
    }

    std::vector<int> to_close;
    for (auto& [fd, st] : clients) {
      if (FD_ISSET(fd, &readfds)) {
        char buf[4096];
        ssize_t r = recv(fd, buf, sizeof(buf), 0);
        if (r <= 0) {
          to_close.push_back(fd);
          continue;
        }
        st.parser.feed(std::string(buf, static_cast<std::size_t>(r)));
        while (true) {
          auto cmd = st.parser.next_command();
          if (!cmd.has_value()) break;
          if (cmd->size() == 1 && cmd->front() == "__MALFORMED__") {
            st.out += pomai_cache::resp_error("malformed RESP");
            break;
          }
          if (cmd->empty()) {
            st.out += pomai_cache::resp_error("empty command");
            continue;
          }

          const auto c = upper((*cmd)[0]);
          if (c == "PING") st.out += pomai_cache::resp_simple("PONG");
          else if (c == "SET") {
            if (cmd->size() < 3) st.out += pomai_cache::resp_error("SET key value [EX sec] [OWNER name]");
            else {
              std::optional<std::uint64_t> ttl;
              std::string owner = "default";
              for (std::size_t i = 3; i + 1 < cmd->size(); i += 2) {
                auto opt = upper((*cmd)[i]);
                if (opt == "EX") ttl = std::stoull((*cmd)[i + 1]);
                else if (opt == "OWNER") owner = (*cmd)[i + 1];
              }
              std::vector<std::uint8_t> val((*cmd)[2].begin(), (*cmd)[2].end());
              std::string err;
              if (engine.set((*cmd)[1], val, ttl, owner, &err)) st.out += pomai_cache::resp_simple("OK");
              else st.out += pomai_cache::resp_error(err);
            }
          } else if (c == "GET") {
            if (cmd->size() != 2) st.out += pomai_cache::resp_error("GET key");
            else {
              auto v = engine.get((*cmd)[1]);
              if (!v) st.out += pomai_cache::resp_null();
              else st.out += pomai_cache::resp_bulk(std::string(v->begin(), v->end()));
            }
          } else if (c == "MGET") {
            if (cmd->size() < 2) st.out += pomai_cache::resp_error("MGET key [key...]");
            else {
              std::vector<std::string> keys(cmd->begin() + 1, cmd->end());
              auto vals = engine.mget(keys);
              std::vector<std::string> arr;
              arr.reserve(vals.size());
              for (auto& v : vals) arr.push_back(v ? pomai_cache::resp_bulk(std::string(v->begin(), v->end())) : pomai_cache::resp_null());
              st.out += pomai_cache::resp_array(arr);
            }
          } else if (c == "DEL") {
            if (cmd->size() < 2) st.out += pomai_cache::resp_error("DEL key [key...]");
            else {
              std::vector<std::string> keys(cmd->begin() + 1, cmd->end());
              st.out += pomai_cache::resp_integer(engine.del(keys));
            }
          } else if (c == "EXPIRE") {
            if (cmd->size() != 3) st.out += pomai_cache::resp_error("EXPIRE key seconds");
            else st.out += pomai_cache::resp_integer(engine.expire((*cmd)[1], std::stoull((*cmd)[2])) ? 1 : 0);
          } else if (c == "TTL") {
            if (cmd->size() != 2) st.out += pomai_cache::resp_error("TTL key");
            else {
              auto t = engine.ttl((*cmd)[1]);
              st.out += pomai_cache::resp_integer(t ? *t : -2);
            }
          } else if (c == "INFO") {
            st.out += pomai_cache::resp_bulk(engine.info());
          } else if (c == "CONFIG") {
            if (cmd->size() >= 2 && upper((*cmd)[1]) == "GET") {
              if (cmd->size() == 3 && upper((*cmd)[2]) == "POLICY") {
                std::vector<std::string> arr{pomai_cache::resp_bulk("policy"), pomai_cache::resp_bulk(engine.policy().name())};
                st.out += pomai_cache::resp_array(arr);
              } else {
                st.out += pomai_cache::resp_error("unsupported CONFIG GET");
              }
            } else if (cmd->size() >= 2 && upper((*cmd)[1]) == "SET") {
              if (cmd->size() == 4 && upper((*cmd)[2]) == "POLICY") {
                engine.set_policy(pomai_cache::make_policy_by_name((*cmd)[3]));
                st.out += pomai_cache::resp_simple("OK");
              } else if (cmd->size() == 4 && upper((*cmd)[2]) == "PARAMS") {
                std::string err;
                engine.reload_params((*cmd)[3], &err);
                st.out += pomai_cache::resp_simple("OK");
              } else {
                st.out += pomai_cache::resp_error("unsupported CONFIG SET");
              }
            } else {
              st.out += pomai_cache::resp_error("CONFIG GET|SET");
            }
          } else {
            st.out += pomai_cache::resp_error("unknown command");
          }
          if (st.out.size() > max_pending_out) {
            to_close.push_back(fd);
            break;
          }
        }
      }
      if (FD_ISSET(fd, &writefds) && !st.out.empty()) {
        ssize_t w = send(fd, st.out.data(), st.out.size(), 0);
        if (w <= 0) to_close.push_back(fd);
        else st.out.erase(0, static_cast<std::size_t>(w));
      }
    }
    std::sort(to_close.begin(), to_close.end());
    to_close.erase(std::unique(to_close.begin(), to_close.end()), to_close.end());
    for (int fd : to_close) {
      close(fd);
      clients.erase(fd);
    }
  }

  for (auto& [fd, _] : clients) close(fd);
  close(server_fd);
  return 0;
}
