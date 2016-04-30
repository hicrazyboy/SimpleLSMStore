#ifndef BLOOMFILTER_H_
#define BLOOMFILTER_H_

#include <string>
#include <vector>
#include <stdint.h>

#include "format.h"

#define BITS_PER_KEY    10
#define HASH_NUMBER (BITS_PER_KEY*0.69)

uint32_t b_hash(const char* data, size_t n, uint32_t seed);

uint32_t bloom_hash(const slice &key);

void generate_filter(const std::vector<slice> &keys, const size_t n, const size_t bits_per_key, std::string* filter);

bool key_may_match(const slice &key, const std::string* filter); 

#endif //BLOOMFILTER_H_

