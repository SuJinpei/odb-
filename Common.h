#pragma once

#include <sqlext.h>
#include <iostream>
#include <vector>
#include <mutex>

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

    Log(std::ostream& os, LogLevel lv = ERROR)
        :_buffer{os}, logLevel{lv}{}

    template<Log::LogLevel lv, typename... Args>
        void log(Args const&... args) {
            if (lv <= logLevel) {
                std::unique_lock<std::mutex> lckLog{mutexLog};
                print(_buffer, args...);
            }
        }

    void setLevel(Log::LogLevel lv);

private:
    std::ostream& _buffer;
    LogLevel logLevel;
};

extern Log gLog;
