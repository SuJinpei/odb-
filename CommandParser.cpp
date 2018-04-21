#include "CommandParser.h"
#include "Common.h"
#include "error.h"

Command CommandParser::parse() {
    debug_log("start parse...\n");

    DBConfig ds1; // for load and extract job, we only need one dsn
    DBConfig ds2; // for copy job we need two dsn
    Task task;

    while (currentPos < _argc) {
        std::string opt = optionNext();
        if (opt == "-d") {
            ds1.DSN = valueNext();
        }
        else if (opt == "-u") {
            ds1.UID = valueNext();
        }
        else if (opt == "-p") {
            ds1.PWD = valueNext();
        }
        else if (opt == "-l") {
            parseTask(Task::LOAD);
        }
        else if (opt == "-cp") {
            parseTask(Task::COPY);
        }
        else if (opt == "-e") {
            parseTask(Task::EXTRACT);
        }
        else if (opt == "-h") {
            parseTaskWithoutOption(Task::HELP);
        }
        else if (opt == "-v") {
            gLog.setLevel(Log::INFO);
        }
        else if (opt == "-vv") {
            gLog.setLevel(Log::DEBUG);
        }
        else {
            error("unknown option:", opt);
        }
    }

    if (_argc == 1) {
        parseTaskWithoutOption(Task::HELP);
    }

    cmd.dbConfigs.push_back(ds1);

    debug_log("parse end\n");
    return cmd;
}

std::string CommandParser::optionNext() {
    if (currentPos == _argc) error("no more option");
    if (_argv[currentPos][0] != '-') error("expected an option after ", _argv[currentPos-1]);
    return _argv[currentPos++];
}

std::string CommandParser::valueNext() {
    if (currentPos == _argc) error(_argv[currentPos-1], "expected a value");
    return _argv[currentPos++];
}

void CommandParser::parseTask(Task::TASKID taskID) {
    Task t;
    t.taskID = taskID;
    t.taskOptionStr = valueNext();
    cmd.tasks.push_back(t);
}

void CommandParser::parseTaskWithoutOption(Task::TASKID taskID) {
    Task t;
    t.taskID = taskID;
    cmd.tasks.push_back(t);
}
