#include "RandSpeed.h"
#include <sstream>

RandSpeedTester::RandSpeedTester(std::string specstr) {
    std::istringstream iss{ specstr };
    std::string countStr;
    std::string numThreadStr;
    std::getline(iss, countStr, ':');
    std::getline(iss, numThreadStr, ':');
    randCnt = std::stoul(countStr);
    tnum = std::stoul(numThreadStr);
    gLog.log<Log::INFO>("thread num:", tnum, ",random count:", randCnt, "\n");
}

void RandSpeedTester::run() {
    std::vector<std::thread> ths;
    auto tstart = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < tnum; ++i) {
        gLog.log<Log::INFO>("create thread\n");
        ths.push_back(std::thread{ [this] {test(); } });
    }
    for (auto& t : ths) {
        t.join();
    }
    auto tend = std::chrono::high_resolution_clock::now();
    auto telapsed = std::chrono::duration_cast<std::chrono::seconds>(tend - tstart).count();
    gLog.log<Log::INFO>("random generator speed:", (randCnt * tnum) / telapsed, " times/s\n");
}

void RandSpeedTester::test() {
    Random rnd;
    for (size_t i = 0; i < randCnt; ++i) {
        rnd.rand_long(0xffffffff, 0x7fffffff);
    }
}
