// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_FRONTLEVEL_DB_DB_ITER_H_
#define STORAGE_FRONTLEVEL_DB_DB_ITER_H_

#include <stdint.h>
#include "frontlevel/db.h"
#include "db/dbformat.h"

namespace frontlevel {

class DBImpl;

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
extern Iterator* NewDBIterator(
    DBImpl* db,
    const Comparator* user_key_comparator,
    Iterator* internal_iter,
    SequenceNumber sequence);

}  // namespace frontlevel

#endif  // STORAGE_FRONTLEVEL_DB_DB_ITER_H_
