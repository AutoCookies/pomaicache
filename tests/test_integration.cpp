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
#include <sstream>
#include <cstring>

namespace {

std::optional<std::string> read_reply(int fd) {
  std::string out;
  char buf[4096];
  while (true) {
    int r = recv(fd, buf, 4096, 0);
    if (r <= 0) break;
    out.append(buf, r);
    if (out.find("\r\n\r\n") != std::string::npos) {
      auto pos = out.find("Content-Length: ");
      if (pos != std::string::npos) {
        auto end = out.find("\r\n", pos);
        int len = std::stoi(out.substr(pos + 16, end - pos - 16));
        auto header_end = out.find("\r\n\r\n") + 4;
        if (out.size() >= header_end + len) {
          return out;
        }
      } else {
        return out;
      }
    }
  }
  return out.empty() ? std::nullopt : std::make_optional(out);
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

TEST_CASE("integration: HTTP core commands and clean shutdown",
          "[integration]") {
  auto s = spawn_server();
  int fd = connect_port(s.port);
  REQUIRE(fd >= 0);

  auto req1 = "POST /key/a HTTP/1.1\r\nContent-Length: 1\r\n\r\n1";
  send(fd, req1, strlen(req1), 0);
  REQUIRE(read_reply(fd).value().find("200 OK") != std::string::npos);

  auto req2 = "GET /key/a HTTP/1.1\r\n\r\n";
  send(fd, req2, strlen(req2), 0);
  REQUIRE(read_reply(fd).value().find("1") != std::string::npos);

  auto req3 = "POST /key/a?ex=1 HTTP/1.1\r\nContent-Length: 1\r\n\r\n1";
  send(fd, req3, strlen(req3), 0);
  REQUIRE(read_reply(fd).value().find("200 OK") != std::string::npos);

  auto req4 = "GET /info HTTP/1.1\r\n\r\n";
  send(fd, req4, strlen(req4), 0);
  REQUIRE(read_reply(fd).value().find("200 OK") != std::string::npos);

  auto req5 = "GET /config/policy HTTP/1.1\r\n\r\n";
  send(fd, req5, strlen(req5), 0);
  REQUIRE(read_reply(fd).value().find("200 OK") != std::string::npos);

  auto req6 = "DELETE /key/a HTTP/1.1\r\n\r\n";
  send(fd, req6, strlen(req6), 0);
  REQUIRE(read_reply(fd).value().find("200 OK") != std::string::npos);

  const std::string bad_req = "NOPE /key/a HTTP/1.1\r\n\r\n";
  send(fd, bad_req.data(), bad_req.size(), 0);
  auto bad = read_reply(fd);
  REQUIRE(bad.has_value());
  CHECK(bad->find("405") != std::string::npos);

  close(fd);
  stop_server(s);
}

TEST_CASE("integration: adversarial caps and churn",
          "[integration][adversarial]") {
  auto s = spawn_server();
  int fd = connect_port(s.port);
  REQUIRE(fd >= 0);

  std::string big(1024 * 1024 + 8, 'x');
  std::string req = "POST /key/big HTTP/1.1\r\nContent-Length: " + std::to_string(big.size()) + "\r\n\r\n" + big;
  send(fd, req.data(), req.size(), 0);
  auto rep = read_reply(fd);
  REQUIRE(rep.has_value());
  CHECK(rep->find("400") != std::string::npos);

  for (int i = 0; i < 500; ++i) {
    std::string sreq = "POST /key/churn" + std::to_string(i) + " HTTP/1.1\r\nContent-Length: 3\r\n\r\nval";
    send(fd, sreq.data(), sreq.size(), 0);
    REQUIRE(read_reply(fd).has_value());
  }

  std::string ireq = "GET /info HTTP/1.1\r\n\r\n";
  send(fd, ireq.data(), ireq.size(), 0);
  auto info = read_reply(fd);
  REQUIRE(info.has_value());
  CHECK(info->find("evictions") != std::string::npos);

  for (int i = 0; i < 128; ++i) {
    std::string t = "POST /key/ttl" + std::to_string(i) + "?px=1 HTTP/1.1\r\nContent-Length: 1\r\n\r\nv";
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

  std::string p1 = "POST /ai/put/embedding/emb:m:h:3:float?meta={\"artifact_type\":\"embedding\",\"owner\":\"vector\",\"schema_version\":\"v1\",\"model_id\":\"m\",\"snapshot_epoch\":\"ep9\"} HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc";
  send(fd, p1.data(), p1.size(), 0);
  auto put = read_reply(fd);
  REQUIRE(put.has_value());
  REQUIRE(put->find("200 OK") != std::string::npos);

  std::string g1 = "GET /ai/get/emb:m:h:3:float HTTP/1.1\r\n\r\n";
  send(fd, g1.data(), g1.size(), 0);
  auto get = read_reply(fd);
  REQUIRE(get.has_value());
  CHECK(get->find("200 OK") != std::string::npos);

  std::string s1 = "GET /ai/stats HTTP/1.1\r\n\r\n";
  send(fd, s1.data(), s1.size(), 0);
  auto stats = read_reply(fd);
  REQUIRE(stats.has_value());
  CHECK(stats->find("dedup_hits") != std::string::npos);

  std::string i1 = "POST /ai/invalidate/EPOCH/ep9 HTTP/1.1\r\nContent-Length: 0\r\n\r\n";
  send(fd, i1.data(), i1.size(), 0);
  auto inv = read_reply(fd);
  REQUIRE(inv.has_value());
  CHECK(inv->find("1") != std::string::npos);

  send(fd, g1.data(), g1.size(), 0);
  auto miss = read_reply(fd);
  REQUIRE(miss.has_value());
  CHECK(miss->find("404") != std::string::npos);

  close(fd);
  stop_server(s);
}
