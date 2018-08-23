#pragma once
#include <cstdio>
#include <error.h>
#include <string>
#include "TCPCommon.h"

class BaseRequestHandler {
public:
    virtual void handle() {};
    TCPConnection conn;
};

class BaseRequestHandlerFactory {
public:
    virtual BaseRequestHandler* createHandler();
};

class TCPServer {
public:
    TCPServer(std::string ipAddr, unsigned short port, BaseRequestHandlerFactory *pf = new BaseRequestHandlerFactory());
    ~TCPServer();

    TCPConnection accept();
    void serve_num_request(size_t n);

private:
    int sockfd;
    std::string hostIP;

    sockaddr_in srvr_addr;
    unsigned short portNo;
    BaseRequestHandlerFactory *hanlerFactory;
};
