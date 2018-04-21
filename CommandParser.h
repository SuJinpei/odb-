#pragma once
#include "Command.h"

#include <iostream>

class CommandParser {
public:
    CommandParser(int argc, char* argv[]): _argc(argc), _argv(argv) {}
    Command parse();


private:
    // get option from current position and move position to next
    std::string optionNext();

    // get value from current position and move position to next
    std::string valueNext();

    // parse task
    void parseTask(Task::TASKID taskID);

    void parseTaskWithoutOption(Task::TASKID taskID);

    int currentPos = 1;
    int _argc;
    char **_argv;
    Command cmd;
};
