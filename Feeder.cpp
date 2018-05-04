#include <iostream>
#include <sstream>
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

//################################################################
// Map Feeder
//################################################################

MapFeeder::MapFeeder(std::string fileName, size_t maxRows)
        :fin{fileName} {
    _maxRows = maxRows;

    if (fin.is_open())
        gLog.log<Log::DEBUG>("map file opened\n");
    else {
        gLog.log<Log::ERROR>("map file open failed\n");
        throw std::runtime_error("open map file");
    }

    for (std::string line; std::getline(fin, line);) {
        gLog.log<Log::DEBUG>("read map line:", line, "\n");
        std::string Name;
        std::string rule;
        std::string leftspec;

        std::istringstream iss{line};
        std::getline(iss, Name, ':');
        std::getline(iss, rule, ':');
        std::getline(iss, leftspec);

        if (rule == "SEQ") {
            fillers.push_back(std::unique_ptr<Filler>{new SeqLongFiller{leftspec}});
        }
        else if (rule == "IRAND") {
            fillers.push_back(std::unique_ptr<Filler>{new IrandFiller{leftspec}});
        }
        else if (rule == "CRAND") {
            fillers.push_back(std::unique_ptr<Filler>{new CharsRandFiller{leftspec}});
        }
        else if (rule == "DRAND") {
            fillers.push_back(std::unique_ptr<Filler>{new DateRandFiller{leftspec}});
        }
        else {
            std::cerr << "unsupported rule:" << rule << std::endl;
        }
    }
}

bool MapFeeder::putData(int , int c, char *buf, const TableDesc& ) {
    return fillData(c, buf);
}

bool MapFeeder::fillData(int c, char *buf) {
    return fillers[c]->fill(buf);
}

SeqLongFiller::SeqLongFiller(const std::string spec)
        :Filler(spec) {
    gLog.log<Log::DEBUG>("SeqLongFiller spec:", spec, "\n");
    seqnum = std::stol(spec);
}

bool SeqLongFiller::fill(void *buf) {
    *((long *)buf) = seqnum++;
    return true;
}

IrandFiller::IrandFiller(const std::string& spec)
    :Filler(spec){
    gLog.log<Log::DEBUG>("IrandFiller spec:", spec, "\n");
    std::istringstream iss{spec};
    std::string minstr, maxstr;
    std::getline(iss, minstr, ':');
    std::getline(iss, maxstr, ':');
    mn = std::stol(minstr);
    mx = std::stol(maxstr);
}

bool IrandFiller::fill(void *buf) {
    *((long*)buf) = rnd.rand_long(mn, mx);
    return true;
}

CharsRandFiller::CharsRandFiller(const std::string& spec)
    : Filler(spec){
    gLog.log<Log::DEBUG>("CRAND:", spec, "\n");
    std::istringstream iss{spec};
    std::string lenstr;
    std::getline(iss, lenstr, ':');
    len = std::stol(lenstr);
};

bool CharsRandFiller::fill(void *buf) {
    strcpy((char*)buf, rnd.fast_rand_str(len).c_str());
    return true;
}

DateRandFiller::DateRandFiller(const std::string& spec)
    : Filler(spec) { 
    gLog.log<Log::DEBUG>("DateRand:", spec, "\n");
    std::istringstream iss{spec};
    std::string maxY, minY;
    std::getline(iss, maxY, ':');
    std::getline(iss, minY, ':');
    maxYear = std::stol(maxY);
    minYear = std::stol(minY);
}

bool DateRandFiller::fill(void *buf) {
    ((SQL_DATE_STRUCT*)buf)->year = rnd.rand_long(minYear, maxYear);
    ((SQL_DATE_STRUCT*)buf)->month = rnd.rand_long(1, 12);
    ((SQL_DATE_STRUCT*)buf)->day = rnd.rand_long(1, 28);
    return true;
}
