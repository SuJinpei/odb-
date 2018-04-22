#pragma once

#include <sqlext.h>
#include <iostream>
#include <vector>
#include <mutex>
#include <algorithm>
#include <cstdlib>
#include <ctime>

#ifdef _DEBUG
    #define debug_log(args...) { \
                std::unique_lock<std::mutex> lckIO{mutexIO};\
                print(std::cout, args); \
            }
#else
    #define debug_log(...)
#endif


extern std::mutex mutexIO;
extern std::mutex mutexLog;

void print(std::ostream&);

template<typename T, typename... Args>
void print(std::ostream& os, const T& t, Args const&... args) {
    print(os<<t, args...);
}

struct ColumnDesc
{
    SQLSMALLINT NameLen;
    SQLSMALLINT Type;
    SQLSMALLINT Decimal;
    SQLSMALLINT Nullable;
    SQLULEN Size;
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
        ERROR,
        WARNING,
        INFO,
        DEBUG
    };

    Log(std::ostream& os, const LogLevel lv = ERROR)
        :_buffer{os}, logLevel{lv}{}

    template<Log::LogLevel lv, typename... Args>
        void log(Args const&... args) {
            if (lv <= logLevel) {
                std::unique_lock<std::mutex> lckLog{mutexLog};
                switch(lv) {
                case Log::ERROR:
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
    drand48_data rand_buffer;
};
