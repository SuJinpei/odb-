#pragma once
#ifdef _WINDOWS
#include <Windows.h>
#ifdef max
#undef max
#undef min
#endif
#endif

#include <sqlext.h>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <mutex>
#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <random>

extern std::mutex mutexIO;
extern std::mutex mutexLog;

void print(std::ostream&);

template<typename T, typename... Args>
void print(std::ostream& os, const T& t, Args const&... args) {
    print(os << t, args...);
}

#ifdef _DEBUG
template<typename... Args>
void debug_log(Args const&... args) {
    std::unique_lock<std::mutex> lckIO{ mutexIO };
    print(std::cout, args...);
}
#else
#define debug_log(...)
#endif

struct ColumnDesc
{
    SQLSMALLINT NameLen;
    SQLSMALLINT Type;
    SQLSMALLINT Decimal;
    SQLSMALLINT Nullable;
    SQLULEN Size = 0;
    SQLLEN OtectLen = 0;
    std::string Name = std::string(128, '\0');

};

struct TableDesc
{
    SQLSMALLINT ColumnNum = 0;
    SQLSMALLINT recordSize = 0;
    std::vector<ColumnDesc> coldesc;

};

class Log {
public:
    enum LogLevel {
        LERROR,
        WARNING,
        INFO,
        DEBUG
    };

    Log(std::ostream& os, const LogLevel lv = LERROR)
        :_buffer{os}, logLevel{lv}{}

    template<Log::LogLevel lv, typename... Args>
        void log(Args const&... args) {
            if (lv <= logLevel) {
                std::unique_lock<std::mutex> lckLog{mutexLog};
                switch(lv) {
                case Log::LERROR:
                    _buffer << "[***ERROR***]\t"; break;
                case Log::WARNING:
                    _buffer << "[***WARNING***]\t"; break;
                default:
                    break;
                }
                print(_buffer, args...);
            }
        }

    void setLevel(Log::LogLevel lv);

private:
    std::ostream& _buffer;
    LogLevel logLevel;
};

extern Log gLog;

class Random {
public:
    Random();
    long rand_long(long lo, long hi);
    double rand_double(double lo, double hi, size_t digit);
    std::string rand_str(size_t len);
    std::string fast_rand_str(size_t len);
private:
    std::string rand_char_seqs;
    static const std::string chars;
//    std::random_device rd;
//    std::mt19937_64 gen;
    drand48_data randbuf;
    unsigned short int seed16v[3];
};
