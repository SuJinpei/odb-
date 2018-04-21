#include <iostream>
#include "Common.h"

std::mutex mutexIO;
std::mutex mutexLog;

void print(std::ostream&){}

Log gLog{std::cerr};

void Log::setLevel(Log::LogLevel lv) {
    logLevel = lv;
}

