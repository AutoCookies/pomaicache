#include <catch2/catch_test_macros.hpp>

#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <netinet/in.h>
#include <optional>
#include <random>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
std::string cmd(const std::vector<std::string> &args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto &a : args)
    out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
  return out;
}

std::optional<std::string> read_reply(int fd) {
  std::string out;
  char c;
  while (recv(fd, &c, 1, 0) == 1) {
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
        if (recv(fd, payload.data(), payload.size(), MSG_WAITALL) <= 0)
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
  return std::nullopt;
}

int connect_port(int port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    return -1;
  timeval tv{2, 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  return fd;
}

struct ServerProc {
  int port;
  pid_t pid;
};

ServerProc spawn_server() {
  static int attempt = 0;
  int port = 22000 + ((::getpid() + attempt * 137) % 20000);
  ++attempt;
  pid_t pid = fork();
  if (pid == 0) {
    execl("./pomai_cache_server", "./pomai_cache_server", "--port",
          std::to_string(port).c_str(), "--params",
          "../config/policy_params.json", nullptr);
    _exit(1);
  }
  for (int i = 0; i < 50; ++i) {
    int fd = connect_port(port);
    if (fd >= 0) {
      close(fd);
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return {port, pid};
}

void stop_server(const ServerProc &s) {
  kill(s.pid, SIGINT);
  waitpid(s.pid, nullptr, 0);
}
} // namespace

TEST_CASE("integration: RESP core commands and clean shutdown",
          "[integration]") {
  auto s = spawn_server();
  int fd = connect_port(s.port);
  REQUIRE(fd >= 0);

  auto send_cmd = [&](const std::vector<std::string> &args) {
    auto req = cmd(args);
    send(fd, req.data(), req.size(), 0);
    return read_reply(fd);
  };

  REQUIRE(send_cmd({"SET", "a", "1"}).value().rfind("+OK", 0) == 0);
  REQUIRE(send_cmd({"GET", "a"}).value().find("1") != std::string::npos);
  REQUIRE(send_cmd({"MGET", "a", "b"}).has_value());
  REQUIRE(send_cmd({"EXPIRE", "a", "1"}).value().rfind(":1", 0) == 0);
  REQUIRE(send_cmd({"TTL", "a"}).has_value());
  REQUIRE(send_cmd({"INFO"}).value()[0] == '$');
  REQUIRE(send_cmd({"CONFIG", "GET", "POLICY"}).value()[0] == '*');
  REQUIRE(send_cmd({"DEL", "a"}).value().rfind(":1", 0) == 0);

  const std::string bad_req = "*1\r\n$4\r\nNOPE\r\n";
  send(fd, bad_req.data(), bad_req.size(), 0);
  auto bad = read_reply(fd);
  REQUIRE(bad.has_value());
  CHECK(bad->rfind("-ERR", 0) == 0);

  close(fd);
  stop_server(s);
}

TEST_CASE("integration: adversarial caps and churn",
          "[integration][adversarial]") {
  auto s = spawn_server();
  int fd = connect_port(s.port);
  REQUIRE(fd >= 0);

  std::string big(1024 * 1024 + 8, 'x');
  auto req = cmd({"SET", "big", big});
  send(fd, req.data(), req.size(), 0);
  auto rep = read_reply(fd);
  REQUIRE(rep.has_value());
  CHECK(rep->rfind("-ERR", 0) == 0);

  for (int i = 0; i < 500; ++i) {
    auto sreq = cmd({"SET", "churn" + std::to_string(i), "val"});
    send(fd, sreq.data(), sreq.size(), 0);
    REQUIRE(read_reply(fd).has_value());
  }
  auto ireq = cmd({"INFO"});
  send(fd, ireq.data(), ireq.size(), 0);
  auto info = read_reply(fd);
  REQUIRE(info.has_value());
  CHECK(info->find("evictions") != std::string::npos);

  for (int i = 0; i < 128; ++i) {
    auto t = cmd({"SET", "ttl" + std::to_string(i), "v", "PX", "1"});
    send(fd, t.data(), t.size(), 0);
    REQUIRE(read_reply(fd).has_value());
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  send(fd, ireq.data(), ireq.size(), 0);
  auto info2 = read_reply(fd);
  REQUIRE(info2.has_value());
  CHECK(info2->find("expiration_backlog") != std::string::npos);

  close(fd);
  stop_server(s);
}

TEST_CASE("integration: AI artifact commands", "[integration][ai]") {
  auto s = spawn_server();
  int fd = connect_port(s.port);
  REQUIRE(fd >= 0);

  auto send_cmd = [&](const std::vector<std::string> &args) {
    auto req = cmd(args);
    send(fd, req.data(), req.size(), 0);
    return read_reply(fd);
  };

  auto put = send_cmd({"AI.PUT", "embedding", "emb:m:h:3:float", "{\"artifact_type\":\"embedding\",\"owner\":\"vector\",\"schema_version\":\"v1\",\"model_id\":\"m\",\"snapshot_epoch\":\"ep9\"}", "abc"});
  REQUIRE(put.has_value());
  REQUIRE(put->rfind("+OK", 0) == 0);

  auto get = send_cmd({"AI.GET", "emb:m:h:3:float"});
  REQUIRE(get.has_value());
  CHECK(get->rfind("*2", 0) == 0);

  auto stats = send_cmd({"AI.STATS"});
  REQUIRE(stats.has_value());
  CHECK(stats->find("dedup_hits") != std::string::npos);

  auto inv = send_cmd({"AI.INVALIDATE", "EPOCH", "ep9"});
  REQUIRE(inv.has_value());
  CHECK(inv->rfind(":1", 0) == 0);

  auto miss = send_cmd({"AI.GET", "emb:m:h:3:float"});
  REQUIRE(miss.has_value());
  CHECK(miss->rfind("$-1", 0) == 0);

  close(fd);
  stop_server(s);
}
