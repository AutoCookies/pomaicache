#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(6379);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Connect failed\n";
        return 1;
    }

    const char* req = "GET /info HTTP/1.1\r\n\r\n";
    send(fd, req, strlen(req), 0);

    char buf[4096];
    int r = recv(fd, buf, 4096, 0);
    if (r > 0) {
        std::cout << "Received " << r << " bytes: " << std::string(buf, r) << "\n";
    } else {
        std::cout << "Receive failed or 0 bytes\n";
    }
    
    close(fd);
    return 0;
}
