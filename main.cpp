#include "Common.h"
#include "CommandParser.h"
#include "Odb.h"
#include <iostream>


int main(int argc, char* argv[]) {
    try {
        CommandParser cmdParser(argc, argv);
        Command cmd = cmdParser.parse();

        debug_log("DSN:", cmd.dbConfigs[0].DSN, "\n");
        debug_log("UID:", cmd.dbConfigs[0].UID, "\n");
        debug_log("PWD:", cmd.dbConfigs[0].PWD, "\n");

        Odb().runCmd(cmd);
    }
    catch(std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    catch(...) {
        std::cerr << "unkown exception" << std::endl;
    }
    return 0;
}
