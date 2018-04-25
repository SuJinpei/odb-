#include <iostream>
#include <chrono>
#include <sstream>
#include "error.h"
#include "Loader.h"

#ifdef _TEST
#include "TCPClient.h"
#endif

Loader::Loader(const LoaderCmd& command):cmd{command} {
    maxSize = cmd.parallel * 2;
    isUniqueFeeder = true;
}

void Loader::run() {

    cmd.print();

    tRunStart = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> vps;
    std::vector<std::thread> vcs;

    if (cmd.src == "rand") {
        setNumProducer(cmd.parallel);
        setNumConsumer(cmd.parallel);
    }
    else {
        setNumConsumer(cmd.parallel);
    }

    for (size_t i = 0; i < producerNum; ++i) {
        std::thread tProducer{[&]{
            this->produceData();
        }};
        vps.push_back(std::move(tProducer));
        ++producerCnt;
    }

    for (size_t i = 0, mx = consumNumer; i < mx; ++i) {
        std::thread tLoader {
            [&]{
                this->loadData();
            }
        };
        vcs.push_back(std::move(tLoader));
        ++consumerCnt;
    }

    for(auto& t:vps) {
        t.join();
    }

    for(auto& t:vcs) {
        t.join();
    }
}

void Loader::loadData() {
    Connection cn {cmd.dbcfg};
    bool isParameterBind = false;
    size_t lastRowCnt = 0;
    size_t rowsLoaded = 0;
    SQLRETURN retcode = 0;
    SQLHANDLE hdesc = NULL;

#ifdef _TEST
    TCPClient testcnn{"10.10.10.11", 8800};
#endif

    // intialize table meta
    std::unique_lock<std::mutex> lckTableMeta{mutexTableMeta};

    if (!isTableMetaInitialized) {
        gLog.log<Log::INFO>("first thread initialize table metadata\n");

        initTableMeta(cn);

        // compute rowWidth
        for (SQLSMALLINT i = 0; i < tableMeta.ColumnNum; ++i) {
            rowWidth += tableMeta.coldesc[i].Size;
            debug_log("col", i, ':', tableMeta.coldesc[i].Size, "\n");
            rowWidth += sizeof(SQLLEN);
        }

        // distribute data container
        std::unique_lock<std::mutex> lckEmptyQ{mEmptyQueue};
        for (size_t i = 0; i < maxSize; ++i) {
            emptyQueue.push(DataContainer{rowWidth * cmd.rows, cmd.rows});
        }
        condEmptyQueueNotEmpty.notify_all();
        lckEmptyQ.unlock();

        std::ostringstream oss;
        oss << cmd.loadMethod << " INTO " << cmd.tableName << " VALUES (";
        for (int i = 0; i < tableMeta.ColumnNum; ++i) {
            oss << "?,";
        }
        loadQuery = oss.str();
        loadQuery[loadQuery.size() - 1] = ')';

        isTableMetaInitialized = true;
        tLoadStart = std::chrono::high_resolution_clock::now();
        gLog.log<Log::INFO>("intialize time:",
                std::chrono::duration_cast<std::chrono::milliseconds>(tLoadStart - tRunStart).count(), "\n");
    }

    lckTableMeta.unlock();

    // init batch
    debug_log("initialize batch insert\n");

    std::unique_ptr<SQLSMALLINT> pStatus{new SQLSMALLINT[cmd.rows]};
    SQLULEN processedRow = 0;
    
    debug_log("rowWidth:", rowWidth, "\n");

    if (!SQL_SUCCEEDED(SQLSetStmtAttr(cn.hstmt, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)rowWidth, 0))) {
        cn.diagError("SQLSetStmtAttr");

    }

    if (!SQL_SUCCEEDED(SQLSetStmtAttr(cn.hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)cmd.rows, 0))) {
        cn.diagError("SQLSetStmtAttr");
    }
    lastRowCnt = cmd.rows;

    if (!SQL_SUCCEEDED(SQLSetStmtAttr(cn.hstmt, SQL_ATTR_PARAM_STATUS_PTR, pStatus.get(), 0))) {
        cn.diagError("SQLSetStmtAttr");

    }

    if (!SQL_SUCCEEDED(SQLSetStmtAttr(cn.hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &processedRow, 0))) {
        cn.diagError("SQLSetStmtAttr");

    }

    debug_log("load query:", loadQuery, "\n");
    retcode = SQLPrepare(cn.hstmt, (SQLCHAR*)loadQuery.c_str(), SQL_NTS);
    if (!SQL_SUCCEEDED(retcode)) {
        cn.diagError("SQLPrepare");
    }
    debug_log("prepared stmt\n");

    while (true) {
        std::unique_lock<std::mutex> lckFullQ{mFullQueue};

        condFullQueueNotEmpty.wait(lckFullQ, [this]{return (!fullQueue.empty()) || (producerCnt == 0);});

        debug_log("loader>>size of full queue:", fullQueue.size(), "\n");
        if (fullQueue.empty() && producerCnt == 0) {
            condFullQueueNotEmpty.notify_all();
            break;
        }

        DataContainer c = std::move(fullQueue.front());
        fullQueue.pop();
        condFullQueueNotFull.notify_one();
        lckFullQ.unlock();

        debug_log("=====get loading data rows:", c.rowCnt, "\n");

        if (!isParameterBind) {
            size_t start = 0;
            debug_log("bind parameter\n");
            for (size_t i = 0, max = tableMeta.coldesc.size(); i < max; ++i) {
                if (!SQL_SUCCEEDED(SQLBindParameter(cn.hstmt, (SQLUSMALLINT)(i + 1), SQL_PARAM_INPUT, SQL_C_CHAR, tableMeta.coldesc[i].Type,
                                                    tableMeta.coldesc[i].Size, tableMeta.coldesc[i].Decimal, (SQLPOINTER)(c.buf + start),
                                                    tableMeta.coldesc[i].Size, (SQLLEN*)(c.buf + start + tableMeta.coldesc[i].Size)))) {
                    cn.diagError("SQLBindParameter");
                }
                start += tableMeta.coldesc[i].Size + sizeof(SQLLEN);
            }
            if (!SQL_SUCCEEDED(SQLGetStmtAttr(cn.hstmt, SQL_ATTR_APP_PARAM_DESC, &hdesc, 0, NULL))) {
                cn.diagError("SQLGetStmtAttr");
            }
            isParameterBind = true;
        }
        else { // just change SQL_DESC_DATA_PTR
            size_t start = 0;
            for (size_t i = 0, max = tableMeta.coldesc.size(); i < max; ++i) {
                if (!SQL_SUCCEEDED(SQLSetDescField(hdesc, (SQLSMALLINT)(i+1), SQL_DESC_DATA_PTR, c.buf + start, 0))) {
                    cn.diagError("SQLSetDescField");
                }
                start += tableMeta.coldesc[i].Size + sizeof(SQLLEN);
            }
        }

        if (lastRowCnt != c.rowCnt) {
            if (!SQL_SUCCEEDED(SQLSetStmtAttr(cn.hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)c.rowCnt, 0))) {
                cn.diagError("SQLSetStmtAttr");
            }
            lastRowCnt = c.rowCnt;
        }

        debug_log("doing data loading...\n");
#ifdef _TEST
        testcnn.send(c.buf, c.bufsz);
#else
        if (!SQL_SUCCEEDED(retcode = SQLExecute(cn.hstmt))) {
            debug_log("retcode:", retcode, "\n");
            cn.diagError("SQLExecute");
        }
#endif
        rowsLoaded += lastRowCnt;
        gLog.log<Log::INFO>(lastRowCnt, " rows loaded\n");

        std::unique_lock<std::mutex> lckEmptyQ{mEmptyQueue};
        condEmptyQueueNotFull.wait(lckEmptyQ, [this]{return (emptyQueue.size() < maxSize) || (producerCnt == 0);});

        if (producerCnt == 0) {
            condEmptyQueueNotFull.notify_all();
            break;
        }

        debug_log("loader>>size of empty queue:", emptyQueue.size(), "\n");
        emptyQueue.push(std::move(c));
        condEmptyQueueNotEmpty.notify_one();
    }

    totalLoadedRows += rowsLoaded;
    gLog.log<Log::INFO>("loading thread exit\n");
    if(--consumerCnt == 0) {
        tLoadEnd = std::chrono::high_resolution_clock::now();
        auto tElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(tLoadEnd - tLoadStart).count();
        gLog.log<Log::INFO>("loading time:", tElapsed,
                " ms\nloading speed:", double(totalLoadedRows)/tElapsed * 1000,
                " rows/s, ", double(totalLoadedRows*rowWidth)/tElapsed/1.024/1024, " MB/s\n"); }
}

void Loader::produceData() {
    std::unique_ptr<Feeder> pFeeder {createFeeder()};
    while (!(pFeeder->isWorkDone())) {
        std::unique_lock<std::mutex> lckEmptyQ{mEmptyQueue};
        condEmptyQueueNotEmpty.wait(lckEmptyQ, [this]{return !emptyQueue.empty();});
        debug_log("producer>>size of empty queue:", emptyQueue.size(), "\n");

        DataContainer c{std::move(emptyQueue.front())};
        emptyQueue.pop();
        condEmptyQueueNotFull.notify_one();
        lckEmptyQ.unlock();

        debug_log("filling data...\n");
        if(pFeeder->feedData(c, tableMeta)) {
            std::unique_lock<std::mutex> lckFullQ{mFullQueue};
            condFullQueueNotFull.wait(lckFullQ, [this]{return fullQueue.size() < maxSize;});
            debug_log("producer>>size of full queue:", fullQueue.size(), "\n");

            fullQueue.push(std::move(c));
            condFullQueueNotEmpty.notify_one();
        }
    }
    if(--producerCnt == 0) condFullQueueNotEmpty.notify_all();
}

void Loader::initTableMeta(Connection& cnxn) {
    SQLHANDLE hstmt = cnxn.hstmt;

    std::stringstream ss;
    ss << "SELECT * FROM " << cmd.tableName<< " WHERE 1 = 0";

    /* Prepare dummy select */
    if (!SQL_SUCCEEDED(SQLPrepare(hstmt, (SQLCHAR*)ss.str().c_str(), SQL_NTS))) {
        cnxn.diagError("SQLPrepare");

    }
    debug_log("prepare stmt succeeded\n");

    /* Get number of resulting cols */
    if (!SQL_SUCCEEDED(SQLNumResultCols(hstmt, (SQLSMALLINT *)&tableMeta.ColumnNum))) {
        cnxn.diagError("SQLNumResultCols");

    }
    debug_log("num result col stmt succeeded\n");

    /* Get column attributes and fill td structure */
    for (size_t i = 0; i < (unsigned int)tableMeta.ColumnNum; i++) {
        ColumnDesc cd;
        if (!SQL_SUCCEEDED(SQLDescribeCol(hstmt, (SQLUSMALLINT)(i + 1),
                                               (SQLCHAR*)&cd.Name[0], (SQLSMALLINT)cd.Name.length(), &cd.NameLen,
                                               &cd.Type, &cd.Size,
                                               &cd.Decimal, &cd.Nullable))) {
            cnxn.diagError("SQLDescibeCol");
        }

        tableMeta.coldesc.push_back(cd);
        tableMeta.recordSize += cd.Size;
    }
}

Feeder* Loader::createFeeder() {
    if (cmd.src == "stdin") {
        return new StandardInputFeeder();
    }
    if (cmd.src == "rand") {
        return new RandomFeeder(cmd.maxRows);
    }
    else {
        return new CSVFeeder(cmd.src, ' ');
    }
}

void Loader::setNumConsumer(size_t n) {
    consumNumer = n;
}

void Loader::setNumProducer(size_t n) {
    producerNum = n;
}

void Connection::diagError(const std::string& functionName) {
    debug_log("***ERROR: call diagError\n");
    SQLCHAR State[10];
    SQLINTEGER errCode;
    char errText[1024];
    SQLSMALLINT olen;
    if (henv) {
        SQLSMALLINT oi = 1;
        while (SQL_SUCCEEDED(SQLGetDiagRec(SQL_HANDLE_ENV, henv, oi, State, &errCode, (SQLCHAR*)errText, sizeof(errText), &olen))) {
            gLog.log<Log::ERROR>("[", functionName, "]State:", State, "\nErrorCode:", errCode, "\nErrorMessage:", errText, "\n");
            ++oi;
        }
    }

    if (hdbc) {
        SQLSMALLINT oi = 1;
        while (SQL_SUCCEEDED(SQLGetDiagRec(SQL_HANDLE_DBC, hdbc, oi, State, &errCode, (SQLCHAR*)errText, sizeof(errText), &olen))) {
            gLog.log<Log::ERROR>("[", functionName, "]State:", State, "\nErrorCode:", errCode, "\nErrorMessage:", errText, "\n");
            ++oi;
        }
    }

    if (hstmt) {
        SQLSMALLINT oi = 1;
        while (SQL_SUCCEEDED(SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, oi, State, &errCode, (SQLCHAR*)errText, sizeof(errText), &olen))) {
            gLog.log<Log::ERROR>("[", functionName, "]State:", State, "\nErrorCode:", errCode, "\nErrorMessage:", errText, "\n");
            ++oi;
        }       
    }
    error(functionName + " failed");
}

void LoaderCmd::parse(const std::string& cmdStr) {
    std::istringstream iss{cmdStr};
    std::string opt;
    while (std::getline(iss, opt, ':')) {
        std::istringstream issOpt{opt};
        std::string key;
        std::string val;
        if (std::getline(issOpt, key, '=')) {
            std::getline(issOpt, val, '=');
        }

        if (key == "src") {
            src = val;
        }
        else if (key == "tgt") {
            tableName = val;
        }
        else if (key == "map") {
            mapFile = val;
        }
        else if (key == "max") {
            maxRows = std::stoul(val);
        }
        else if (key == "rows") {
            rows = std::stoul(val);
        }
        else if (key == "parallel") {
            parallel = std::stoul(val);
        }
        else if (key == "loadcmd") {
            if (val == "IN")
                ;// loadMethod = "INSERT";
            else if (val == "UP")
                loadMethod = "UPSERT";
            else if (val == "UL")
                loadMethod = "UPSERT USING LOAD";
            else
                error("unknown loadcmd:", val);
        }
        else {
            error("unsupported load option:", key);
        }
    }
}

void LoaderCmd::print() {
    gLog.log<Log::INFO>("==========Load CMD========\n");
    gLog.log<Log::INFO>("DSN:", dbcfg.DSN, "\n");
    gLog.log<Log::INFO>("UID:", dbcfg.UID, "\n");
    gLog.log<Log::INFO>("PWD:", dbcfg.PWD, "\n");
    gLog.log<Log::INFO>("src:", src, "\n");
    gLog.log<Log::INFO>("tgt:", tableName, "\n");
    gLog.log<Log::INFO>("rows:",rows, "\n");
    gLog.log<Log::INFO>("max:", maxRows, "\n");
    gLog.log<Log::INFO>("parallel:", parallel, "\n");
    gLog.log<Log::INFO>("==========================\n");
}
