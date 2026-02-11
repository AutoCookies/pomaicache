#include <arpa/inet.h>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <optional>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace {
std::string cmd(const std::vector<std::string> &args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto &a : args)
    out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
  return out;
}

bool read_line(int fd, std::string *line) {
  line->clear();
  char c;
  while (recv(fd, &c, 1, 0) == 1) {
    line->push_back(c);
    if (line->size() >= 2 && (*line)[line->size() - 2] == '\r' &&
        (*line)[line->size() - 1] == '\n')
      return true;
  }
  return false;
}

std::optional<std::string> request(int fd,
                                   const std::vector<std::string> &args) {
  auto q = cmd(args);
  if (send(fd, q.data(), q.size(), 0) < 0)
    return std::nullopt;
  std::string line;
  if (!read_line(fd, &line))
    return std::nullopt;
  if (!line.empty() && line[0] == '$') {
    int len = std::stoi(line.substr(1));
    std::string body(len + 2, '\0');
    if (recv(fd, body.data(), body.size(), MSG_WAITALL) <= 0)
      return std::nullopt;
    line += body;
  }
  return line;
}

int connect_port(int port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    return -1;
  timeval tv{1, 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  return fd;
}

pid_t spawn_server(int port, const std::string &dir, const std::string &fsync) {
  pid_t pid = fork();
  if (pid == 0) {
    execl("./pomai_cache_server", "./pomai_cache_server", "--port",
          std::to_string(port).c_str(), "--data-dir", dir.c_str(),
          "--ssd-enabled", "--memory", "1048576", "--ssd-value-min-bytes", "64",
          "--fsync", fsync.c_str(), nullptr);
    _exit(1);
  }
  for (int i = 0; i < 30; ++i) {
    int fd = connect_port(port);
    if (fd >= 0) {
      close(fd);
      return pid;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return pid;
}
} // namespace

int main(int argc, char **argv) {
  std::string fsync = "everysec";
  if (argc > 1)
    fsync = argv[1];
  const int port = 26379;
  const std::string dir = "crash_data";
  std::unordered_map<std::string, std::string> model;

  pid_t pid = spawn_server(port, dir, fsync);
  for (int iter = 0; iter < 30; ++iter) {
    int fd = connect_port(port);
    if (fd < 0)
      break;
    for (int i = 0; i < 200; ++i) {
      std::string key = "k" + std::to_string((iter * 200 + i) % 200);
      std::string val = std::string((i % 64) + 1, 'a' + (i % 20));
      auto a =
          request(fd, {"SET", key, val, "PX", std::to_string(500 + (i % 200))});
      if (a.has_value())
        model[key] = val;
      if (i % 7 == 0)
        request(fd, {"DEL", "k" + std::to_string((iter + i) % 200)});
      if (i % 3 == 0)
        request(fd, {"GET", key});
    }
    close(fd);
    kill(pid, SIGKILL);
    waitpid(pid, nullptr, 0);
    pid = spawn_server(port, dir, fsync);
  }

  int fd = connect_port(port);
  if (fd < 0)
    return 2;
  for (int i = 0; i < 20; ++i)
    request(fd, {"GET", "k" + std::to_string(i)});
  auto info = request(fd, {"INFO"});
  if (!info.has_value() ||
      info->find("ssd_index_rebuild_ms") == std::string::npos)
    return 4;

  close(fd);
  kill(pid, SIGINT);
  waitpid(pid, nullptr, 0);
  return 0;
}
