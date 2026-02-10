#include <arpa/inet.h>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

int main(int argc, char** argv) {
  std::string host = "127.0.0.1";
  int port = 6379;
  if (argc > 1) port = std::stoi(argv[1]);

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
  if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    std::cerr << "connect failed\n";
    return 1;
  }
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line == "quit") break;
    std::string payload = "*1\r\n$" + std::to_string(line.size()) + "\r\n" + line + "\r\n";
    send(fd, payload.data(), payload.size(), 0);
    char buf[4096];
    auto n = recv(fd, buf, sizeof(buf), 0);
    if (n <= 0) break;
    std::cout.write(buf, n);
    std::cout << std::endl;
  }
  close(fd);
  return 0;
}
