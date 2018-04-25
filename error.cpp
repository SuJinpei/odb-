#include <iostream>
#include "Common.h"

void error(const std::string errmsg, const std::string errmsg2 = "") {
    gLog.log<Log::ERROR>(errmsg, errmsg2, "\n");
    exit(-1);
}
