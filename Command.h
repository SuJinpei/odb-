#pragma once
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
