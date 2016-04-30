// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef TESTUTIL_H_
#define TESTUTIL_H_

#include "format.h"
#include "random.h"

#include <assert.h>
#include <ctype.h>
#include <stdint.h>

static double FLAGS_compression_ratio = 0.5;

namespace test {

// Store in *dst a random string of length "len" and return a Slice that
// references the generated data.
extern slice RandomString(Random* rnd, int len, std::string* dst);

// Return a random key with the specified length that may contain interesting
// characters (e.g. \x00, \xff, etc.).
extern std::string RandomKey(Random* rnd, int len);

// Store in *dst a string of length "len" that will compress to
// "N*compressed_fraction" bytes and return a Slice that references
// the generated data.
extern slice CompressibleString(Random* rnd, double compressed_fraction,
                                int len, std::string* dst);

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
    RandomGenerator();
//    virtual ~RandomGenerator();
    status Generate(size_t len, slice * s);
};

extern status TrimSpace(slice * s);

extern void AppendWithSpace(std::string* str, slice msg);

}
#endif  // TESTUTIL_H_
