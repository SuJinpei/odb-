#include "Command.h"

void LoaderCmd::parse(const std::string& cmdStr) {
    std::istringstream iss{ cmdStr };
    std::string opt;
    while (std::getline(iss, opt, ':')) {
        std::istringstream issOpt{ opt };
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
        else if (key == "psuedo") {
            pseudo = true;
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
    gLog.log<Log::INFO>("rows:", rows, "\n");
    gLog.log<Log::INFO>("max:", maxRows, "\n");
    gLog.log<Log::INFO>("parallel:", parallel, "\n");
    gLog.log<Log::INFO>("==========================\n");
}
