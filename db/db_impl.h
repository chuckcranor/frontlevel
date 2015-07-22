// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_FRONTLEVEL_DB_DB_IMPL_H_
#define STORAGE_FRONTLEVEL_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "frontlevel/db.h"
#include "frontlevel/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace frontlevel {

class MemTable;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname, Backend *backend);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

 private:
  friend class DB;
  struct Writer;

  void MaybeIgnoreError(Status* s) const;

  Status Recover() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Iterator* NewInternalIterator(const ReadOptions&, 
                                SequenceNumber* latest_snapshot);


  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(std::string &logpath)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void  BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Constant after construction
  Env* const env_;
  Backend *backend_;
  const InternalKeyComparator internal_comparator_;
  const Options options_;  // options_.comparator == &internal_comparator_
  bool owns_info_log_;
  const std::string dbname_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;          // Signalled when background work finishes
  MemTable* mem_;
  MemTable* imm_;                // Memtable being compacted
  port::AtomicPointer has_imm_;  // So bg thread can detect non-NULL imm_
  WritableFile* logfile_;
  uint64_t logfile_number_;
  log::Writer* log_;
  uint32_t seed_;                // For sampling.
  uint64_t prev_logfile_number_;
  uint64_t last_sequence_;

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;

  // Has a background compaction been scheduled or is running?
  bool bg_compaction_scheduled_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const Options& src);


}  // namespace frontlevel
#endif  // STORAGE_FRONTLEVEL_DB_DB_IMPL_H_
