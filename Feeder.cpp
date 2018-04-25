#include <iostream>
#include "Feeder.h"

bool Feeder::putData(int r, int c, char *buf, const TableDesc& tbMeta) {
    std::string temp;
    if(!getNext(temp)) {
        return false;
    }
    strncpy(buf, temp.c_str(), tbMeta.coldesc[c].Size);
    return true;
}

bool Feeder::getNext(std::string&) {
    return false;
};

bool StandardInputFeeder::putData(int r, int c, char *buf, const TableDesc& tbMeta) {
    std::string temp;
    std::cout << "row:" << r << ",col:" << c << 
        ",please input data for " << tbMeta.coldesc[c].Name 
        << '(' << tbMeta.coldesc[c].Type << ')' <<  std::endl;

    if(!(std::cin >> temp)) {
        _isWorkDone = true;
        return false;
    }

    std::strncpy(buf, temp.c_str(), tbMeta.coldesc[c].Size);
    return true;
}

bool StandardInputFeeder::getNext(std::string& s) {
    std::cin >> s;
    return !!std::cin;
}

CSVFeeder::CSVFeeder(const std::string& fileName, const char seperator):
    sp{seperator}, ifs{fileName}, icsvs{ifs}{
}

bool CSVFeeder::getNext(std::string& s) {
    icsvs >> s;
    return !ifs.fail();
}

bool CSVFeeder::isWorkDone() const {
    return !ifs;
}

CSV_Stream& CSV_Stream::operator>>(std::string& s) {
    while (!std::getline(buffer, s, comma)) {
        if (source.bad() || !source.good()) return *this;
        buffer.clear();

        std::string temp;
        std::getline(source, temp);
        buffer.str(std::move(temp));
    }
    return *this;
}

CSV_Stream::operator bool() const {
    return (buffer.fail() || buffer.bad()) && !source.good();
}

Rand_Stream& Rand_Stream::operator>>(std::string& s) {
    switch(cdesc[currentColumns].Type) {
    case SQL_INTEGER:
        s = std::to_string(r.rand_long(0, 10000000));
        break;
    case SQL_DOUBLE:
        s = std::to_string(r.rand_double(0, 1000, cdesc[currentColumns].Decimal));
        break;
    case SQL_CHAR:
        s = r.fast_rand_str(cdesc[currentColumns].Size -1);
        break;
    default:
        gLog.log<Log::ERROR>("File:",__FILE__,", Line:", __LINE__, ", unsupported SQL type:",
                             cdesc[currentColumns].Type, ",columns:", currentColumns, "\n");
        isStateGood = false;
    }
    if(++currentColumns == cdesc.size()) {
        currentColumns = 0;
        ++rowsOuted;
    }
    return *this;
}

Rand_Stream::operator bool() const {
    return isStateGood && (rowsOuted <= maxR);
}

void Rand_Stream::setColDesc(const std::vector<ColumnDesc> vc) {
    if (cdesc.size() != vc.size())
        cdesc = vc;
}


RandomFeeder::RandomFeeder(size_t maxRows)
    :rs{maxRows} {}

bool RandomFeeder::isWorkDone() const {
    return !rs;
}

bool RandomFeeder::putData(int r, int c, char *buf, const TableDesc& tbMeta) {
    rs.setColDesc(tbMeta.coldesc);
    return Feeder::putData(r, c, buf, tbMeta);
}

bool RandomFeeder::getNext(std::string& s) {
    rs >> s;
    return !(rs.fail());
}
