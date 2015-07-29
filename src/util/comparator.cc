// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "frontlevel/comparator.h"
#include "frontlevel/slice.h"
#include "port/port.h"

namespace frontlevel {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "frontlevel.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }
};
}  // namespace

static port::OnceType once = FRONTLEVEL_ONCE_INIT;
static const Comparator* bytewise;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}

const Comparator* BytewiseComparator() {
  port::InitOnce(&once, InitModule);
  return bytewise;
}

}  // namespace frontlevel
