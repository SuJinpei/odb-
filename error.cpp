#include <iostream>

void error(const std::string errmsg, const std::string errmsg2 = "") {
    std::cerr << errmsg << ' ' << errmsg2 << std::endl;
    exit(-1);
}
