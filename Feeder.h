#pragma once
#include "DataContainer.h"
#include "Common.h"
#include <cstring>
#include <iostream>
#include <sstream>
#include <fstream>
#include <memory>

class CSV_Stream {
public:
    CSV_Stream(std::istream& is, const char c = ',')
        :source(is), comma(c) {}

    CSV_Stream& operator>>(std::string& s);

    operator bool() const;

private:
    std::istream &source;
    std::istringstream buffer;
    char comma;
};

class Rand_Stream {
public:
    Rand_Stream(size_t maxRow):maxR{maxRow}{};

    Rand_Stream& operator>>(std::string& s);

    operator bool() const;

    bool fail() { return !isStateGood; }

    void setColDesc(const std::vector<ColumnDesc> vc);

private:
    Random r;
    std::vector<ColumnDesc> cdesc;
    size_t maxR;
    size_t rowsOuted;
    size_t currentColumns = 0;
    bool isStateGood = true;
};

class Feeder {
public:
    virtual bool feedData(DataContainer& dc, const TableDesc& tbMeta) {
        size_t start = 0;
        dc.rowCnt = 0;
        for (size_t r = 0; r < dc.maxRow; ++r, ++dc.rowCnt) {
            if (rowFeeded++ >= _maxRows) {
                _isWorkDone = true;
                break;
            }
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

    virtual bool getNext(std::string& s);

protected:
    bool _isWorkDone = false;
    size_t rowFeeded = 0;
    size_t _maxRows = std::numeric_limits<size_t>::max();
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

class RandomFeeder: public Feeder {
public:
    RandomFeeder(size_t maxRows);

    bool putData(int r, int c, char *buf, const TableDesc& tbMeta) override;

    bool getNext(std::string& s) override;

    bool isWorkDone() const override;

private:
    Rand_Stream rs;
    size_t rowLoaded = 0;
    size_t maxRows;
};


// ###################################################
// class map feeder
// ###################################################

class Filler {
public:
    Filler(const std::string spec)
        :_spec(spec) {}
    virtual bool fill(void *) {
        std::cerr << "unsupported filler\n";
        return false;
    }

protected:
    std::string _spec;
    std::string Name;
};

class ConstLongFiller : public Filler {
public:
    using Filler::Filler;
//    bool fill(void *buf) override;
};

class SeqLongFiller : public Filler {
public:
    SeqLongFiller(const std::string spec);
    bool fill(void *buf) override;

private:
    long seqnum = 0;
};

class IrandFiller : public Filler {
public:
    IrandFiller(const std::string& spec);
    bool fill(void *buf) override;
private:
    long mn, mx;
    Random rnd;
};

class DateRandFiller : public Filler {
public:
    DateRandFiller(const std::string& spec);
    bool fill(void *buff) override;

private:
    Random rnd;
    int maxYear;
    int minYear;
};

class TimeRandFiller : public Filler {
public:
    using Filler::Filler;
    bool fill(void *buff) override;
};

class TimeStampRandFiller : public Filler {
public:
    using Filler::Filler;
    bool fill(void *buff) override;
};

class CharsRandFiller : public Filler {
public:
    CharsRandFiller(const std::string& spec);
    bool fill(void *buff) override;

private:
    long len;
    Random rnd;
};

class NumericRandFiller : public Filler {
public:
    using Filler::Filler;
//    bool fill(void *buff) override;
};

class RandLineFiller : public Filler {
public:
    using Filler::Filler;
//    bool fill(void *buff) override;
};

class RandPortionFiller : public Filler {
public:
    using Filler::Filler;
//    bool fill(void *buff) override;
};

class SelectRandFiller : public Filler {
public:
    using Filler::Filler;
//    bool fill(void *buff) override;
};

class CurrentDateFiller : public Filler {
public:
    using Filler::Filler;
//    bool fill(void *buff) override;

};

class CurrentTimeFiller : public Filler {
public:
    using Filler::Filler;
//    bool fill(void *buff) override;
};

class CurrentTimeStampFiller : public Filler {
public:
    using Filler::Filler;
//    bool fill(void *buff) override;
};


class MapFeeder: public Feeder {
public:
    MapFeeder(std::string fileName, size_t maxRows);
    bool putData(int r, int c, char *buf, const TableDesc& tbMeta) override;

private:
    bool fillData(int c, char *buf);

    std::ifstream fin;
    std::vector<std::unique_ptr<Filler>> fillers;
};

