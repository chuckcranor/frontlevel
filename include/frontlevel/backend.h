#ifndef STORAGE_FRONTLEVEL_INCLUDE_BACKEND_H_
#define STORAGE_FRONTLEVEL_INCLUDE_BACKEND_H_

#include <string>

namespace frontlevel {

class WriteBatch;

/*
 * frontlevel users create their backing store using their favorite
 * key/value system.  then they wrap it up in a Backend object and 
 * pass that to frontlevel when opening the frontlevel database.
 *
 * this is the generic pure virtual class for Backend.  this is 
 * based on a simplified version of the leveldb API.
 */
class Backend {
 public:

  /*
   * frontlevel dtor will call the backend dtor to release the backend.
   */
  virtual ~Backend() {/* base class dtor is a no-op */};
  
  /*
   * set key to value.  the only write option is sync.
   */
  virtual Status Put(const WriteOptions &options,
                     const Slice &key,
                     const Slice &value) = 0;

  /*
   * delete a key from the store.   note that this copies the leveldb
   * semantics: no error if "key" wasn't present in the DB.
   */
  virtual Status Delete(const WriteOptions &options,
                        const Slice& key) = 0;

  /*
   * batch a set of key/value mods to the backend.   if the backend
   * doesn't support batch ops, it can unpack and do them one at a time.
   *
   * error handling: level db WriteBatch builds the batch into a 
   * string buffer ("rep_") that has a seq#, count, and set of records.
   * that entire buffer gets passed down into the write-ahead log 
   * system.   if the write (AddRecord) to the write-ahead log fails,
   * then the Write is failed.   If the write to the write-ahead log
   * is ok, but we've been asked to sync() and the sync fails, then
   * we are in a bad state and disable all future writes (so you are
   * basically hosed in this case).  Once the log is done, then the
   * data is inserted into the memtable.  if that insert fails, the 
   * Write operation fails (but the data is already in the write-ahead 
   * log so what does that mean?).
   *
   * looking at the log write (Writer::AddRecord), for large batches
   * it is going to break the rep_ buffer up into a First, some Middle,
   * and a Last record.  It is going to call EmitPhysicalRecord() for
   * each of those.  if there is an error, it is going to break out
   * of the loop and return the error (meaning that Last record may
   * not be emitted, but the earlier stuff will already be down in the
   * file).  that error will get detected when the log is read
   * via ReportCorruption().
   *
   * so if we have multiple ops and we get a failure part way in,
   * what should we do?  stop on first error, but do the previous
   * changes that went through happen or not?
   * 
   * what about memory in leveldb?
   * Put->WriteBatch::Put(kv)  COPIES  ->Write()
   * Write->InsertInto mem->memtable Add->arena malloc/memcpy
   */
  virtual Status Write(const WriteOptions &options, 
                       WriteBatch *updates) = 0;
                      
  /*
   * read key from store.   rets IsNotFound if not found...
   */
  virtual Status Get(const ReadOptions &options,
                     const Slice &key, std::string *value) = 0;

  /*
   * iterator
   */
  virtual Iterator *NewIterator(const ReadOptions &options) = 0;

  /*
   * Load: load a table into the backend (e.g. memtable to backend).
   */
  virtual Status Load(const WriteOptions &options, Tablewalker *tw) = 0;
};

}  // namespace frontlevel

#endif  // STORAGE_FRONTLEVEL_INCLUDE_BACKEND_H_

