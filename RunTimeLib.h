#pragma once
#include <string>
class RunTimeLib
{
public:
    RunTimeLib(const std::string& libName);
    ~RunTimeLib();

    void* getFunction(const std::string& functionName);

private:
    using LIBHANDLE = void*;
    LIBHANDLE libHandle;
};

