#include <iostream>
#include <chrono>
#include <sstream>
#include <condition_variable>
#include <map>
#include <random>
#include <limits>
#include "error.h"
#include "Loader.h"
#include "hbase/hbase.h"

Loader::Loader(const LoaderCmd& command):cmd{command} {
    maxSize = cmd.parallel * 2;
    isUniqueFeeder = true;
}

SQLSMALLINT getCType(SQLSMALLINT sqlType) {
    switch (sqlType) {
    case SQL_CHAR:
    case SQL_VARCHAR:
        return SQL_C_CHAR;
    //case SQL_NUMERIC:
    //    return SQL_C_NUMERIC;
    default:
        return SQL_C_DEFAULT;
    }
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
    else if (cmd.src == "nofile") {
        setNumProducer(1);
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

void Loader::loadToDB() {
    Connection cn {cmd.dbcfg};
    bool isParameterBind = false;
    size_t lastRowCnt = 0;
    size_t rowsLoaded = 0;
    SQLRETURN retcode = 0;
    SQLHANDLE hdesc = NULL;

    // intialize table meta
    std::unique_lock<std::mutex> lckTableMeta{mutexTableMeta};

    if (!isTableMetaInitialized) {
        gLog.log<Log::INFO>("first thread initialize table metadata\n");

        initTableMeta(cn);

        // compute rowWidth
        for (SQLSMALLINT i = 0; i < tableMeta.ColumnNum; ++i) {
            rowWidth += tableMeta.coldesc[i].OtectLen;
            debug_log("col", i, ':', tableMeta.coldesc[i].OtectLen, "\n");
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
    
    gLog.log<Log::INFO>("rowWidth:", rowWidth, "\n");

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

        if (true || !isParameterBind) {
            size_t start = 0;
            debug_log("bind parameter\n");
            for (size_t i = 0, max = tableMeta.coldesc.size(); i < max; ++i) {
                if (!SQL_SUCCEEDED(SQLBindParameter(cn.hstmt, (SQLUSMALLINT)(i + 1), SQL_PARAM_INPUT, getCType(tableMeta.coldesc[i].Type), tableMeta.coldesc[i].Type,
                                                    tableMeta.coldesc[i].Size, tableMeta.coldesc[i].Decimal, (SQLPOINTER)(c.buf + start),
                                                    tableMeta.coldesc[i].OtectLen, (SQLLEN*)(c.buf + start + tableMeta.coldesc[i].OtectLen)))) {
                    cn.diagError("SQLBindParameter");
                }
                start += tableMeta.coldesc[i].OtectLen + sizeof(SQLLEN);
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
        //if (SQL_SUCCESS != (retcode = SQLExecute(cn.hstmt))) {
        //    debug_log("retcode:", retcode, "\n");
        //    cn.diagError("SQLExecute");
        //}

        rowsLoaded += lastRowCnt;
        gLog.log<Log::INFO>(lastRowCnt, " rows loaded\n");

        std::unique_lock<std::mutex> lckEmptyQ{mEmptyQueue};
        condEmptyQueueNotFull.wait(lckEmptyQ, [this]{return (emptyQueue.size() < maxSize) || (producerCnt == 0);});

        if (producerCnt == 0 && fullQueue.size() == 0) {
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

#ifndef _WINDOWS

struct PutContext {
    bool flush_done;
    size_t rmRows;
    std::mutex mutex_flush_done;
    std::condition_variable cond_flush_done; 
    std::mutex mutex_rmRows;
    std::condition_variable cond_rmRows_empty;
};

std::mutex mutex_thread_data;
std::map<hb_client_t, PutContext*> mapThreadData;

static void
put_callback(int32_t err, hb_client_t client,
    hb_mutation_t mutaion, hb_result_t result, void *extra) {
    std::unique_lock<std::mutex> lck{mapThreadData[client]->mutex_rmRows};
    --mapThreadData[client]->rmRows;
    mapThreadData[client]->cond_rmRows_empty.notify_all();
}

static void
client_flush_callback(int32_t err,
  hb_client_t client, void *extra) {
    std::unique_lock<std::mutex> lck{mapThreadData[client]->mutex_flush_done};
    mapThreadData[client]->flush_done = true;
    mapThreadData[client]->cond_flush_done.notify_all();
}

static void
wait_for_flush(hb_client_t client) {
    std::unique_lock<std::mutex> lck{mapThreadData[client]->mutex_flush_done};
    mapThreadData[client]->cond_flush_done.wait(lck,
      [&]{return mapThreadData[client]->flush_done;});
}

static void
wait_for_puts(hb_client_t client) {
    std::unique_lock<std::mutex> lck{mapThreadData[client]->mutex_rmRows};
    mapThreadData[client]->cond_rmRows_empty.wait(lck,
        [&]{return mapThreadData[client]->rmRows == 0;});
}

void Loader::loadToHBase() {
    gLog.log<Log::INFO>("load data to HBase\n");

    size_t rowsLoaded = 0;
    std::string table_name = cmd.tableName.substr(6);

    hb_log_set_level(HBASE_LOG_LEVEL_ERROR); // defaults to INFO

    std::random_device rd{};
    std::mt19937_64 gen{rd()};
    std::uniform_int_distribution<long> keyDist{std::numeric_limits<long>::min(), std::numeric_limits<long>::max()};
    long key;

    // connect hbase
    int32_t retCode = 0;
    hb_connection_t connection = NULL;
    hb_client_t client = NULL;
    std::string zk_ensemble = "localhost:2181";
    const char *zk_root_znode = NULL;

    if ((retCode = hb_connection_create(zk_ensemble.c_str(),
                                        zk_root_znode,
                                        &connection)) != 0) {
        gLog.log<Log::LERROR>("failed to create HBase connection\n");
        exit(-1);
    }

    if ((retCode = hb_client_create(connection, &client)) != 0) {
        gLog.log<Log::LERROR>("failed to create HBase client\n");
        exit(-1);
    }
    std::unique_lock<std::mutex> lckThread{mutex_thread_data};
    PutContext put_context;
    mapThreadData.emplace(client, &put_context);
    lckThread.unlock();

    std::unique_lock<std::mutex> lckTableMeta{mutexTableMeta};
    if (!isTableMetaInitialized) {
        std::ifstream ifs{cmd.mapFile};
        for(std::string temp; std::getline(ifs, temp);) {
            ColumnDesc desc;
            std::istringstream iss{temp};
            std::string temp1;
            std::getline(iss, desc.Name, ':');
            std::getline(iss, temp1, ':');
            if (temp1 == "CRAND") {
                desc.Type = SQL_CHAR;
                std::getline(iss, temp1, ':');
                desc.Size = std::stoul(temp1);
            }
            else if (temp1 == "DRAND") {
                desc.Type = SQL_DATE;
                desc.Size = sizeof(SQL_DATE_STRUCT);
            }
            else if (temp1 == "SEQ" ||
                     temp1 == "IRAND") {
                desc.Size = 12;
                desc.Type = SQL_INTEGER;
            }
            else {
                gLog.log<Log::LERROR>("unsupported type ", temp1, "\n");
                return;
            }
            gLog.log<Log::DEBUG>("***SIZE:", desc.Size, "\n");
            tableMeta.coldesc.push_back(desc);
        }

        // compute rowWidth
        for (SQLSMALLINT i = 0; i < tableMeta.coldesc.size(); ++i) {
            rowWidth += tableMeta.coldesc[i].Size;
            debug_log("col", i, ':', tableMeta.coldesc[i].Size, "\n");
            rowWidth += sizeof(SQLLEN);
        }
	gLog.log<Log::DEBUG>("rowwidth:", rowWidth, "\n");

        // distribute data container
        std::unique_lock<std::mutex> lckEmptyQ{mEmptyQueue};
        for (size_t i = 0; i < maxSize; ++i) {
            emptyQueue.push(DataContainer{rowWidth * cmd.rows, cmd.rows});
        }
        condEmptyQueueNotEmpty.notify_all();
        lckEmptyQ.unlock();

        isTableMetaInitialized = true;
    	tLoadStart = std::chrono::high_resolution_clock::now();
    }

    lckTableMeta.unlock();

    hb_put_t put = NULL;

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

        debug_log("doing data loading...\n");
        
        // tSize num_written_bytes = hdfsWrite(fs, writeFile, c.buf, rowWidth * c.rowCnt);

        mapThreadData[client]->rmRows = c.rowCnt;
        mapThreadData[client]->flush_done = false;

        for (size_t i = 0; i < c.rowCnt; ++i) {
            hb_put_create((byte_t*)&key, sizeof(long), &put);
            hb_mutation_set_table(put, table_name.c_str(), table_name.size());
            hb_mutation_set_durability(put, DURABILITY_SKIP_WAL);
            hb_mutation_set_bufferable(put, false);
            hb_cell_t *cell = (hb_cell_t*)calloc(1, sizeof(hb_cell_t));
            key = keyDist(gen);
            cell->row = (byte_t*)&key;
            cell->row_len = sizeof(long);
            cell->family = (byte_t *)"#1";
            cell->family_len = 2;
            cell->qualifier = (byte_t *)"1";
            cell->qualifier_len = 1;
            cell->value = (byte_t *)(c.buf + i * rowWidth);
            cell->value_len = rowWidth;
            cell->ts = HBASE_LATEST_TIMESTAMP;

            hb_put_add_cell(put, cell);
            hb_mutation_send(client, put, put_callback, NULL);
        }

        rowsLoaded += c.rowCnt;
        gLog.log<Log::INFO>(c.rowCnt, " rows loaded\n");

        std::unique_lock<std::mutex> lckEmptyQ{mEmptyQueue};
        condEmptyQueueNotFull.wait(lckEmptyQ, [this]{return (emptyQueue.size() < maxSize) || (producerCnt == 0);});

        if (producerCnt == 0 && fullQueue.size() == 0) {
            condEmptyQueueNotFull.notify_all();
            break;
        }

        debug_log("loader>>size of empty queue:", emptyQueue.size(), "\n");
        emptyQueue.push(std::move(c));
        condEmptyQueueNotEmpty.notify_one();
    }
    //wait_for_puts(client);
    //hb_client_flush(client, client_flush_callback, NULL);
    //wait_for_flush(client);

    totalLoadedRows += rowsLoaded;
    gLog.log<Log::INFO>("loading thread exit\n");
    if(--consumerCnt == 0) {
        tLoadEnd = std::chrono::high_resolution_clock::now();
        auto tElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(tLoadEnd - tLoadStart).count();
        gLog.log<Log::INFO>("loading time:", tElapsed,
                " ms\nloading speed:", double(totalLoadedRows)/tElapsed * 1000,
                " rows/s, ", double(totalLoadedRows*rowWidth)/tElapsed/1.024/1024, " MB/s\n"); }
}

void Loader::loadToHDFS() {
    gLog.log<Log::INFO>("load data to hdfs\n");

    size_t rowsLoaded = 0;
    hdfsFS fs = hdfsConnect("default", 0);
    std::string writePath = cmd.tableName.substr(5) + std::to_string(cmd.sid + lid++);
    hdfsFile writeFile = hdfsOpenFile(fs, writePath.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
    if (!writeFile) {
        gLog.log<Log::LERROR>("Failed to open %s for writing!\n", writePath);
        exit(-1);
    }

    std::unique_lock<std::mutex> lckTableMeta{mutexTableMeta};
    if (!isTableMetaInitialized) {
        std::ifstream ifs{cmd.mapFile};
        for(std::string temp; std::getline(ifs, temp);) {
            ColumnDesc desc;
            std::istringstream iss{temp};
            std::string temp1;
            std::getline(iss, desc.Name, ':');
            std::getline(iss, temp1, ':');
            if (temp1 == "CRAND") {
                desc.Type = SQL_CHAR;
                std::getline(iss, temp1, ':');
                desc.Size = std::stoul(temp1);
            }
            else if (temp1 == "DRAND") {
                desc.Type = SQL_DATE;
                desc.Size = sizeof(SQL_DATE_STRUCT);
            }
            else if (temp1 == "SEQ" ||
                     temp1 == "IRAND") {
                desc.Size = 12;
                desc.Type = SQL_INTEGER;
            }
            else {
                gLog.log<Log::LERROR>("unsupported type ", temp1, "\n");
                return;
            }
            gLog.log<Log::DEBUG>("***SIZE:", desc.Size, "\n");
            tableMeta.coldesc.push_back(desc);
        }

        // compute rowWidth
        for (SQLSMALLINT i = 0; i < tableMeta.coldesc.size(); ++i) {
            rowWidth += tableMeta.coldesc[i].Size;
            debug_log("col", i, ':', tableMeta.coldesc[i].Size, "\n");
            rowWidth += sizeof(SQLLEN);
        }
	gLog.log<Log::DEBUG>("rowwidth:", rowWidth, "\n");

        // distribute data container
        std::unique_lock<std::mutex> lckEmptyQ{mEmptyQueue};
        for (size_t i = 0; i < maxSize; ++i) {
            emptyQueue.push(DataContainer{rowWidth * cmd.rows, cmd.rows});
        }
        condEmptyQueueNotEmpty.notify_all();
        lckEmptyQ.unlock();

        isTableMetaInitialized = true;
    	tLoadStart = std::chrono::high_resolution_clock::now();
    }

    lckTableMeta.unlock();

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

        debug_log("doing data loading...\n");
        
        tSize num_written_bytes = hdfsWrite(fs, writeFile, c.buf, rowWidth * c.rowCnt);

        rowsLoaded += c.rowCnt;
        gLog.log<Log::INFO>(c.rowCnt, " rows loaded\n");

        std::unique_lock<std::mutex> lckEmptyQ{mEmptyQueue};
        condEmptyQueueNotFull.wait(lckEmptyQ, [this]{return (emptyQueue.size() < maxSize) || (producerCnt == 0);});

        if (producerCnt == 0 && fullQueue.size() == 0) {
            condEmptyQueueNotFull.notify_all();
            break;
        }

        debug_log("loader>>size of empty queue:", emptyQueue.size(), "\n");
        emptyQueue.push(std::move(c));
        condEmptyQueueNotEmpty.notify_one();
    }

    if (hdfsFlush(fs, writeFile)) {
        gLog.log<Log::LERROR>(stderr, "Failed to 'flush'", writePath, "\n");
        exit(-1);
    }

    hdfsCloseFile(fs, writeFile);

    totalLoadedRows += rowsLoaded;
    gLog.log<Log::INFO>("loading thread exit\n");
    if(--consumerCnt == 0) {
        tLoadEnd = std::chrono::high_resolution_clock::now();
        auto tElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(tLoadEnd - tLoadStart).count();
        gLog.log<Log::INFO>("loading time:", tElapsed,
                " ms\nloading speed:", double(totalLoadedRows)/tElapsed * 1000,
                " rows/s, ", double(totalLoadedRows*rowWidth)/tElapsed/1.024/1024, " MB/s\n"); }
}
#endif

void Loader::loadData() {
    if (cmd.tableName.substr(0, 5) == "hdfs.") {
#ifndef _WINDOWS
        loadToHDFS();
#else
        gLog.log<Log::LERROR>("windows don't support this feature\n");
#endif // !_WINDOWS
    }
    else if (cmd.tableName.substr(0,6) == "hbase.") {
        loadToHBase();
    }
    else {
        loadToDB();
    }
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
            gLog.log<Log::DEBUG>("filled data, queue size:", fullQueue.size(), "\n");
        }
    }
    if(--producerCnt == 0) condFullQueueNotEmpty.notify_all();
    gLog.log<Log::DEBUG>("producer exit\n");
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

        if (!SQL_SUCCEEDED(SQLColAttribute(hstmt, (SQLUBIGINT)(i + 1), SQL_DESC_OCTET_LENGTH, NULL, 0, NULL, &cd.OtectLen))) {
            cnxn.diagError("SQLColAttribute");
        }

        tableMeta.coldesc.push_back(cd);
        tableMeta.recordSize += cd.Size;
    }
}

Feeder* Loader::createFeeder() {
    if (cmd.src == "stdin") {
        return new StandardInputFeeder();
    }
    else if (cmd.src == "rand") {
        return new RandomFeeder(cmd.maxRows);
    }
    else if (cmd.src == "nofile") {
        return new MapFeeder(cmd.mapFile, cmd.maxRows);
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
            gLog.log<Log::LERROR>("[", functionName, "]State:", State, "\nErrorCode:", errCode, "\nErrorMessage:", errText, "\n");
            ++oi;
        }
    }

    if (hdbc) {
        SQLSMALLINT oi = 1;
        while (SQL_SUCCEEDED(SQLGetDiagRec(SQL_HANDLE_DBC, hdbc, oi, State, &errCode, (SQLCHAR*)errText, sizeof(errText), &olen))) {
            gLog.log<Log::LERROR>("[", functionName, "]State:", State, "\nErrorCode:", errCode, "\nErrorMessage:", errText, "\n");
            ++oi;
        }
    }

    if (hstmt) {
        SQLSMALLINT oi = 1;
        while (SQL_SUCCEEDED(SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, oi, State, &errCode, (SQLCHAR*)errText, sizeof(errText), &olen))) {
            gLog.log<Log::LERROR>("[", functionName, "]State:", State, "\nErrorCode:", errCode, "\nErrorMessage:", errText, "\n");
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
        else if (key == "sid") {
            sid = std::stoul(val);
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
