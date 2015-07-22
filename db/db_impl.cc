// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>

#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/internalizer_iterator.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/merger.h"
#include "db/tablewalker_internal.h"
#include "db/write_batch_internal.h"

#include "frontlevel/db.h"
#include "frontlevel/env.h"
#include "frontlevel/status.h"

#include "port/port.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace frontlevel {

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  return result;
}

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname,
               Backend *backend)
    : env_(raw_options.env),
      backend_(backend),
      internal_comparator_(raw_options.comparator),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(new MemTable(internal_comparator_)),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      prev_logfile_number_(0),
      last_sequence_(1),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false) {
  mem_->Ref();
  has_imm_.Release_Store(NULL);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (backend_) {
    delete backend_;
  }

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

/*
 * this is only called by DB::Open when opening a DB.   handles 
 * create_if_missing, error_if_exists, and resolving any lingering
 * log files.
 */
Status DBImpl::Recover() {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = SetCurrentFile(env_, dbname_, 1, this->user_comparator()->Name());
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  /* now cross-verify that the comparator matches */
  /* Read "CURRENT" file that contains the comparator name */
  std::string savedcomp;
  s = ReadFileToString(env_, CurrentFileName(dbname_), &savedcomp);
  if (!s.ok()) {
    return s;
  }
  if (savedcomp.empty() || savedcomp[savedcomp.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  savedcomp.resize(savedcomp.size() - 1); 
  if (strcmp(savedcomp.c_str(), this->user_comparator()->Name()) != 0) {
    s = Status::InvalidArgument(savedcomp + 
                                " does not match existing comparator ",
                                this->user_comparator()->Name());
    return s;
  }

  /*
   * what needs to happen here is that we need to load imm log into
   * backend, delete it, load mem log into backend, delete it and then
   * we are good to go.
   */
  std::string logpath;
  logpath = LogFileName(dbname_, kImmLog);
  if (env_->FileExists(logpath)) {
    s = RecoverLogFile(logpath);
    if (s.ok()) {
      s = env_->DeleteFile(logpath);
    }
  }
  logpath = LogFileName(dbname_, kMemLog);
  if (s.ok() && env_->FileExists(logpath)) {
    s = RecoverLogFile(logpath);
    if (s.ok()) {
      s = env_->DeleteFile(logpath);
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(std::string &logpath) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  SequentialFile* file;
  Status status = env_->NewSequentialFile(logpath, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = logpath.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log %s", logpath.c_str());

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  WriteOptions wopt;
  wopt.sync = true;   /* XXXCDC: set sync to be safe? */
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    status = backend_->Write(wopt, &batch);

    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
  }

  delete file;
  return status;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

/*
 * DBImpl::CompactMemTable() runs in the background thread dumping
 * the imm MemTable to the backend.
 */
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  Status s = WriteLevel0Table(imm_);

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    std::string logpath;
    logpath = LogFileName(dbname_, kImmLog);
    s = env_->DeleteFile(logpath);
  } 
  if (!s.ok()) {
    RecordBackgroundError(s);
  }
}

/*
 *
 * WriteLevel0Table:
 *
 * here's where we write our MemTable to the backend.  the
 * control flow is: set imm_ and call MakeRoomForWrite.
 * this triggers the background thread like this:
 *  Schedule->BGWork->BackgroundCall->CompactMemtable->WriteLevel0Table
 * when complete, CompactMemtable clears imm_.
 * 
 * we wrap an iterator into a Tablewalker (that handles getting
 * rid of the dups in the MemTable), and the backend can just 
 * drain that off however it wants to.
 *
 * an alternate path would be to copy the entire MemTable to
 * a frontlevel::WriteBatch and then call backend's Write.
 * you wouldn't need the Load() backend API then, but is 
 * less efficient since at least with LevelDB it would copy
 * the frontlevel::WriteBatch to a leveldb::WriteBatch and 
 * then call leveldb write on it (so there is an extra data 
 * in this case copy).
 */
Status DBImpl::WriteLevel0Table(MemTable* mem) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Level-0 table started");
  Iterator* iter = mem->NewIterator();  /* given to Tablewalker */

  Status s;
  {
    mutex_.Unlock();
    /* iter will get deleted by ti's dtor when it goes out of scope */
    TablewalkerInternal ti(iter, this->user_comparator());
    s = backend_->Load(WriteOptions(), &ti);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table done: %s",
      s.ToString().c_str());

  return s;
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == NULL) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != NULL) {
    CompactMemTable();
    return;
  }
}

namespace {
struct IterState {
  port::Mutex* mu;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = last_sequence_;

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  Iterator *backend_itr = backend_->NewIterator(options);
  Iterator *int_backend_itr = new InternalizingIterator(backend_itr);
  list.push_back(int_backend_itr);

  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  return NewInternalIterator(ReadOptions(), &ignored);
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot);
  return NewDBIterator(
      this, user_comparator(), iter, latest_snapshot);
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot = last_sequence_;

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  mem->Ref();
  if (imm != NULL) imm->Ref();

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = backend_->Get(options, key, value);  /* user key */
    }
    mutex_.Lock();
  }

  mem->Unref();
  if (imm != NULL) imm->Unref();
  return s;
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == NULL);
  uint64_t last_sequence = last_sequence_;
  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    last_sequence_ = last_sequence;
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(TempFileName(dbname_, kMemLog), &lfile);
      if (!s.ok()) {
        break;
      }
      delete log_;       /* close off old log and underlying file */
      delete logfile_;
      s = env_->RenameFile(LogFileName(dbname_, kMemLog), 
                           LogFileName(dbname_, kImmLog));
      assert(s.ok()); /* XXX: improve error recovery? */
      s = env_->RenameFile(TempFileName(dbname_, kMemLog),
                           LogFileName(dbname_, kMemLog));
      assert(s.ok()); /* XXX: improve error recovery? */
      logfile_ = lfile;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                Backend *backend, DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname, backend);
  impl->mutex_.Lock();
  Status s = impl->Recover(); // Handles create_if_missing, error_if_exists
  if (s.ok()) {
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, kMemLog), &lfile);
    if (s.ok()) {
      impl->logfile_ = lfile;  /* memtable created in DBImpl ctor */
      impl->log_ = new log::Writer(lfile);
    }
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace frontlevel

