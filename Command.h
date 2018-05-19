#pragma once
#include "Common.h"
#include "error.h"
#include <string>
#include <vector>
#include <unordered_map>

struct DBConfig {
    std::string DSN;
    std::string UID;
    std::string PWD;
};

struct Task {
    enum TASKID {COPY, EXTRACT, LOAD, HELP, TEST_RAND_SPEED};
    TASKID taskID;
    std::string taskOptionStr;
};

struct Command {
    // DataSources
    std::vector<DBConfig> dbConfigs;

    // Tasks
    std::vector<Task> tasks;
    
    bool isVerbose() {
        return true;
    }
private:
    std::unordered_map<std::string, std::string> controlOptions;
};

struct LoaderCmd {
    LoaderCmd(const DBConfig& dbc, const std::string& cmdStr) : dbcfg(dbc) { parse(cmdStr); }
    void parse(const std::string& cmdStr);
    void print();

    bool psuedo = false;
    char fieldSep = ',';
    char recordSep = '\n';
    char skipToken = '\\';
    DBConfig dbcfg;
    std::string src;
    std::string tableName;
    std::string mapFile;
    std::string nullString;
    char        escapeChar;
    char        stringQualifier;
    char        padChar;
    char        embededChar;
    int         commit; // auto|end|#rows|x#rs
    bool        norb;
    bool        full;
    bool        truncate;
    bool        show;
    bool        noMark;
    bool        ifempty;
    bool        direct;
    bool        timeOpt;
    bool        xmlord;
    bool        xmldump;
    size_t      bytesPerChar;
    size_t      bytesPerWChar;
    size_t maxErrorCnt = std::numeric_limits<size_t>::max();
    size_t maxRows = std::numeric_limits<size_t>::max();
    size_t rows = 100;
    size_t parallel = 1;
    size_t iobuffSize;
    size_t buffsz;
    size_t fieldtrunc; // {0-4}
    size_t tpar;        // #tables
    size_t maxlen;      // #bytes
    size_t sid = 0;
    std::string pre;    // {@sqlfile} | {sqlcmd}
    std::string post;   // {@sqlfile} | {sqlcmd}
    std::string bad;    // [+]badfile
    std::string loadMethod{ "INSERT" };
    std::string xmltag; // [+]element
};
