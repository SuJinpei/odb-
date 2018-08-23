#pragma once
#include "Command.h"
#include "Common.h"
#include "DataContainer.h"
#include "Feeder.h"

#if 1
#include "hdfs.h"
#endif

#include <sqlext.h>
#include <queue>
#include <mutex>
#include <thread>
#include <cstring>
#include <condition_variable>
#include <atomic>
#include <sstream>
#include <limits>

void printloadbuf(void *buf, TableDesc& meta, size_t rows);

struct Connection {
    Connection(const DBConfig& dc) {
        check(SQLAllocHandle(SQL_HANDLE_ENV, NULL, &henv), "alloc env handle");
        check(SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_NTS), "SQLSetEnvAttr");
        check(SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc), "Alloc Connection Handle");
        std::cout << "connectting to " << dc.DSN << std::endl;
        check(SQLConnect(hdbc, (SQLCHAR*)dc.DSN.c_str(), SQL_NTS, (SQLCHAR*)dc.UID.c_str(), SQL_NTS, (SQLCHAR*)dc.PWD.c_str(), SQL_NTS), "SQLConnect");
        check(SQLAllocStmt(hdbc, &hstmt), "Alloc Statement Handle");
    }

    ~Connection() {
        if (hstmt) {
            SQLRETURN retcode = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        }
        if (hdbc) {
            SQLRETURN retcode = SQLDisconnect(hdbc);
            retcode = SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        }
    }

    void diagError(const std::string& functionName);

    void check(SQLRETURN ret, const std::string funcName) {
        if (!SQL_SUCCEEDED(ret)) {
            diagError(funcName);
        }
    }

    SQLHANDLE conn() {
        return hdbc;
    }

    SQLHANDLE henv;
    SQLHANDLE hdbc, hstmt;
};

class Loader {
public:
    Loader(const LoaderCmd& command);
    void run();
    void loadData(size_t id);
    void produceData(size_t index);
    void setNumConsumer(size_t n);
    void setNumProducer(size_t n);

private:
    void loadToDB(size_t id);

    void loadToDisk(size_t id);

    void loadToHDFS();
#ifndef _WINDOWS
    void loadToHBase();
#endif

    void initTableMeta(Connection& cnxn);
    FeederFactory *createFeederFactory();

    // monitor
    bool timeOut = false;
    bool go = true;
    std::mutex mutexGo;
    std::condition_variable condGo;

    TableDesc tableMeta;
    std::string loadQuery;
    size_t rowWidth = 0;
    bool isTableMetaInitialized = false;
    bool isUniqueFeeder = false;
    std::mutex mutexTableMeta;

    std::queue<DataContainer> emptyQueue;
    std::queue<DataContainer> fullQueue;

    std::vector<std::unique_ptr<Feeder>> feeders;
    std::unique_ptr<FeederFactory> pFeederFactory;

    std::mutex mEmptyQueue;
    std::mutex mFullQueue;
    std::condition_variable condEmptyQueueNotEmpty;
    std::condition_variable condEmptyQueueNotFull;
    std::condition_variable condFullQueueNotEmpty;
    std::condition_variable condFullQueueNotFull;

    std::queue<DataContainer>::size_type maxSize;

    LoaderCmd cmd;
    std::atomic_size_t producerCnt {0}; 
    std::atomic_size_t consumerCnt {0}; 
    std::atomic_size_t totalLoadedRows {0};
    std::atomic_size_t lid {1};

    std::vector<size_t> statsLoadedRows;

    size_t consumNumer = 1;
    size_t producerNum = 1;

    // statistics
    std::chrono::time_point<std::chrono::high_resolution_clock> tRunStart;
    std::chrono::time_point<std::chrono::high_resolution_clock> tLoadStart;
    std::chrono::time_point<std::chrono::high_resolution_clock> tLoadEnd;
};
