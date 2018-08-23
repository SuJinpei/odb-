#include "RunTimeLib.h"
#include "error.h"
#ifndef _WINDOWS
#include "dlfcn.h"
#endif

#include <sstream>



RunTimeLib::RunTimeLib(const std::string& LibName)
{
#ifndef _WINDOWS
    std::string libFileName = "lib" + LibName + ".so";
    libHandle = dlopen(libFileName.c_str(), RTLD_LAZY);
    if (!libHandle) {
        std::ostringstream oss;
        oss << "Open library " << libFileName << " failed, caused by " << dlerror();
        error(oss.str());
    }
#else
    error("Unsupported Feature on Windows");
#endif
}

RunTimeLib::~RunTimeLib()
{
    if (libHandle) {
#ifndef _WINDOWS
        dlclose(libHandle);
#endif
    }
}

void* RunTimeLib::getFunction(const std::string& functionName) {
#ifndef _WINDOWS
    void *funPtr = dlsym(libHandle, functionName.c_str());
    if (!funPtr) {
        error(dlerror());
    }
    return funPtr;
#else
    return nullptr;
#endif
}
