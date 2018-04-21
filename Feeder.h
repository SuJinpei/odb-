#pragma once
#include "DataContainer.h"
#include "Common.h"
#include <cstring>
#include <iostream>
#include <sstream>
#include <fstream>

class CSV_Stream {
public:
    CSV_Stream(std::istream& is, const char c = ',')
        :source(is), comma(c) {}

    CSV_Stream& operator>>(std::string& s);

    operator bool();

private:
    std::istream &source;
    std::istringstream buffer;
    char comma;
};

class Feeder {
public:
    virtual bool feedData(DataContainer& dc, const TableDesc& tbMeta) {
        size_t start = 0;
        dc.rowCnt = 0;
        for (size_t r = 0; r < dc.maxRow; ++r, ++dc.rowCnt) {
            for (SQLSMALLINT c = 0; c < tbMeta.ColumnNum; ++c) {
                if (!putData(r, c, dc.buf + start, tbMeta)) return dc.rowCnt > 0;
                start += tbMeta.coldesc[c].Size;
                *((SQLLEN*)(dc.buf + start)) = SQL_NTS;
                start += sizeof(SQLLEN);
            }
        }
        return dc.rowCnt > 0;
    }

    virtual bool isWorkDone() const {
        return _isWorkDone;
    }

    virtual bool putData(int r, int c, char *buf, const TableDesc& tbMeta);

    virtual bool getNext(std::string& s) = 0;

protected:
    bool _isWorkDone = false;
};

class StandardInputFeeder: public Feeder {
public:

    bool putData(int r, int c, char *buf, const TableDesc& tbMeta) override;
    bool getNext(std::string& s) override;
};

class CSVFeeder: public Feeder {
public:
    CSVFeeder(const std::string& fileName, const char seperator = ',');

    bool getNext(std::string& s) override;

    bool isWorkDone() const override;

private:
    char sp;
    std::ifstream ifs;
    CSV_Stream icsvs;
};
