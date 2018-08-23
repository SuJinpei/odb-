#pragma once
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>

class TCPConnection {
public:
    TCPConnection();
    TCPConnection(const TCPConnection&) = delete;
    TCPConnection(TCPConnection&& conn);
    TCPConnection& operator=(const TCPConnection& conn) = delete;
    TCPConnection& operator=(TCPConnection&& conn);
    ~TCPConnection();

    std::string getPeerIPStr() const;
    int getPeerPort() const;
    size_t send(const void *buf, size_t len);
    size_t recv(void *buf, size_t len);

    sockaddr_in peer_addr;
    socklen_t addr_len;
    int fd = 0;
};

