#include "testutil.h"
#include "random.h"
#include "format.h"

#include <string>
#include <stdint.h>

namespace test {

slice RandomString(Random* rnd, int len, std::string* dst) {
	slice s_random;
	dst->resize(len);
	for (int i = 0; i < len; i++) {
		(*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));   // ' ' .. '~'
	}
	s_random.data = const_cast<char *>(dst->data());
	s_random.size = dst->size();
	return s_random;
}

std::string RandomKey(Random* rnd, int len) {
  // Make sure to generate a wide variety of characters so we
  // test the boundary conditions for short-key optimizations.
  static const char kTestChars[] = {
    '\0', '\1', 'a', 'b', 'c', 'd', 'e', '\xfd', '\xfe', '\xff'
  };
  std::string result;
  for (int i = 0; i < len; i++) {
    result += kTestChars[rnd->Uniform(sizeof(kTestChars))];
  }
  return result;
}


extern slice CompressibleString(Random* rnd, double compressed_fraction,
                                int len, std::string* dst) {
  slice s_rst;

  int raw = static_cast<int>(len * compressed_fraction);
  if (raw < 1) raw = 1;
  std::string raw_data;
  RandomString(rnd, raw, &raw_data);

  // Duplicate the random data until we have filled "len" bytes
  dst->clear();
  while (dst->size() < len) {
    dst->append(raw_data);
  }
  dst->resize(len);
  s_rst.data = const_cast<char *>(dst->data());
  s_rst.size = dst->size(); 
  return s_rst;
}

status TrimSpace(slice * s) {
  int start = 0;
  while (start < s->size && isspace(s->data[start])) {
    start++;
  }
  int limit = s->size;
  while (limit > start && isspace(s->data[limit-1])) {
    limit--;
  }
  s->size = limit - start;
  memmove(s->data, s->data + start, limit - start);
  return e_Ok;
}

void AppendWithSpace(std::string* str, slice msg) {
  if (msg.size == 0) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data, msg.size);
}

  RandomGenerator::RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

    status RandomGenerator::Generate(size_t len, slice * s) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
	s->size = len;
	assert(s != NULL);
	memcpy(s->data, data_.data() + pos_ - len, len);	
    return e_Ok;
  }

}
