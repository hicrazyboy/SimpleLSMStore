
#include <vector>
#include <string>
#include <stdint.h>

#include "bloomfilter.h"
#include "util.h"
#include "format.h"
#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED do { } while (0)
#endif

#define BITS_PER_KEY    10
#define HASH_NUMBER (BITS_PER_KEY*0.69)

uint32_t b_hash(const char* data, size_t n, uint32_t seed) {
 //  Similar to murmur hash
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  uint32_t h = seed ^ (n * m);

//   Pick up four bytes at a time
  while (data + 4 <= limit) {
    uint32_t w = decode_fixed32(data);
    data += 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

//   Pick up remaining bytes
  switch (limit - data) {
    case 3:
      h += data[2] << 16;
      FALLTHROUGH_INTENDED;
    case 2:
      h += data[1] << 8;
      FALLTHROUGH_INTENDED;
    case 1:
      h += data[0];
      h *= m;
      h ^= (h >> r);
      break;
  }
  return h;
}


uint32_t bloom_hash(const slice &key) {
	return b_hash(key.data, key.size, 0xbc9f1d34);
}

//@n: # of  keys.
void generate_filter(const std::vector<slice> &keys, const size_t n, const size_t bits_per_key, std::string* filter) {
	size_t bits = n * bits_per_key;

	if(bits < 64) {
		bits = 64;
	}
	size_t bytes = (bits + 7) / 8;
	bits = bytes * 8;

	filter->resize(bytes, 0);
	char* array = &(*filter)[0];
	for(size_t i = 0; i < n; i++) {
		uint32_t h = bloom_hash(keys[i]);
		const uint32_t delta = (h >> 17) | (h << 15);
		for(size_t j = 0; j < HASH_NUMBER; j++) {
			const uint32_t bitpos = h % bits;
			array[bitpos / 8] |= (1 << (bitpos % 8));
			h += delta;
		}
	}
}

bool key_may_match(const slice &key, const std::string* bloom_filter) {
	size_t k = HASH_NUMBER;
    
	const size_t len = bloom_filter->size();
        if (len < 2) return false;
        
	const char* array = &(*bloom_filter)[0];//bloom_filter->data()
	const size_t bits = (len) * 8;
    
	if(k > 30) {
        //Reserved for potentially new encodings for short bloom filters.
        // Consider it a match.
    	return true;
	}
	uint32_t h = bloom_hash(key);
	const uint32_t delta = (h >> 17) | (h << 15);
	for(size_t j = 0; j < k; j++) {
		const uint32_t bitpos = h % bits;
		if((array[bitpos/8] & (1 << (bitpos % 8))) == 0) {
			return false;
		}
		h += delta;
	}
	return true;
}
