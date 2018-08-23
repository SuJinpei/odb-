#pragma once
#include "Command.h"

class Odb {
public:
    ~Odb();
    void runCmd(const Command& cmd);
};
