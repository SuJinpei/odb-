#include "error.h"
#include "Common.h"
#include <iostream>

void error(const std::string errmsg, const std::string errmsg2) {
    gLog.log<Log::LERROR>(errmsg, errmsg2, "\n");
    // exit(-1);
}
