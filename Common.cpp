#include <iostream>
#include <iterator>
#include "Common.h"

std::mutex mutexIO;
std::mutex mutexLog;

void print(std::ostream&){}

Log gLog{std::cerr};

void Log::setLevel(Log::LogLevel lv) {
    logLevel = lv;
}

Random::Random()
    : rd{}, gen { rd() }
{
    rand_char_seqs = rand_str(1024*1024);
}

long Random::rand_long(long lo, long hi) {
    std::uniform_int_distribution<long> ldist{ lo, hi };
    return ldist(gen);
}

double Random::rand_double(double lo, double hi) {
    std::uniform_real_distribution<double> dist{ lo, hi };
    return dist(gen);
}

std::string Random::rand_str(size_t len) {
    std::string ret;
    std::generate_n(std::back_inserter(ret), len, [this] {return chars[rand_long(0, chars.size() - 1)]; });
    return ret;
}

std::string Random::fast_rand_str(size_t len) {
    size_t i = rand_long(0, rand_char_seqs.size()/2 -1);
    return rand_char_seqs.substr(i, len);
}

const std::string Random::chars = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()-+[]{}";
