#include <iostream>
#include "Odb.h"
#include "Loader.h"

void Odb::runCmd(const Command& cmd) {
    for (auto t : cmd.tasks) {
        switch(t.taskID) {
        case Task::COPY:
            debug_log("copy function unsupported yet\n");
            break;
        case Task::EXTRACT:
            debug_log("extract function unsupported yet\n");
            break;
        case Task::HELP:
            std::cout << "please reference to odb manual for help" << std::endl;
            exit(0);
        case Task::LOAD:
            debug_log("doing job load\n");
            debug_log(t.taskOptionStr, "\n");
            Loader l{LoaderCmd{cmd.dbConfigs[0], t.taskOptionStr}};
            l.run();
            break;
        }
    }
}
