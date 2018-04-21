#pragma once
#include <cstddef>
#include <utility>

struct DataContainer {
    DataContainer(size_t size, size_t maxr):buf{new char[size]}, bufsz{size}, maxRow{maxr} {
    }

    DataContainer& operator=(const DataContainer& o) = default;

    DataContainer(DataContainer&& o) {
        *this = o;
        o.buf = nullptr;
    }

    DataContainer& operator=(DataContainer&& o) {
        std::swap(*this, o);
        return *this;
    }

    ~DataContainer() {
        if (buf) delete[] buf;
    }

    char *buf = nullptr;
    size_t bufsz = 0;
    size_t rowCnt = 0;
    size_t maxRow = 0;
};

