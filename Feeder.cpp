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

CSV_Stream::operator bool() {
    return (buffer.fail() || buffer.bad()) && !source.good();
}

