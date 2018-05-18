#pragma once
#include "Common.h"
class RandSpeedTester {
public:
    RandSpeedTester(std::string specstr);
    void run();
    void test();
private:
    size_t tnum; // thread number
    size_t randCnt; // random count
};