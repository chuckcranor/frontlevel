#ifndef STORAGE_FRONTLEVEL_INCLUDE_LEVELDB_BACKEND_H_
#define STORAGE_FRONTLEVEL_INCLUDE_LEVELDB_BACKEND_H_

#include <string>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "frontlevel/write_batch.h"

namespace frontlevel {

/*
 * lstat2fstat: leveldb Status to frontlevel Status helper fn
 */
static inline Status lstat2fstat(leveldb::Status ls) {
  Status rv;

  /* leveldb Status doesn't give us tests NotSupported or InvalidArg */
  if (ls.ok()) return(rv.OK());
  if (ls.IsNotFound()) return(rv.NotFound(ls.ToString()));
  if (ls.IsCorruption()) return(rv.Corruption(ls.ToString()));
  if (ls.IsIOError()) return(rv.IOError(ls.ToString()));

  return(rv.NotSupported(ls.ToString()));
}

/*
 * LevelDB backend (mainly for testing)
 */
class LevelDBBackend : public Backend {
 private:
  leveldb::DB *dbp_;             /* backing database */
  class Iter;                    /* enable LevelDBBackend::Iter */

  /* private ctor; force users to use public one with dbp_ provided */
  LevelDBBackend() : dbp_(NULL) { };  
  Iterator *NewIter(leveldb::Iterator *itp);

 public:
  /*
   * ctor just inits data structure
   */
  LevelDBBackend(leveldb::DB *dbp) : dbp_(dbp) {
  }

  /*
   * dtor should release all backend resources
   */
  ~LevelDBBackend() {
    if (dbp_ != NULL) {
      delete dbp_;
    }
  }
  
  /*
   * set key to value.  the only write option is sync.
   */
  Status Put(const WriteOptions &options,
                     const Slice &key,
                     const Slice &value) {
    leveldb::WriteOptions wopt;
    leveldb::Slice lkey(key.data(), key.size());
    leveldb::Slice lval(value.data(), value.size());
    leveldb::Status lrv;
    wopt.sync = options.sync;

    lrv = dbp_->Put(wopt, lkey, lval);

    return(lstat2fstat(lrv));
  }

  /*
   * delete a key from the store.   note that this copies the leveldb
   * semantics: no error if "key" wasn't present in the DB.
   */
  Status Delete(const WriteOptions &options, const Slice& key) {
    leveldb::WriteOptions wopt;
    leveldb::Slice lkey(key.data(), key.size());
    leveldb::Status lrv;
    wopt.sync = options.sync;
  
    lrv = dbp_->Delete(wopt, lkey);

    return(lstat2fstat(lrv));
  }

  /*
   * batch a set of key/value mods to the backend.   if the backend
   * doesn't support batch ops, it can unpack and do them one at a time.
   */
  Status Write(const WriteOptions &options, WriteBatch *updates) {
    leveldb::WriteOptions wopt;
    leveldb::WriteBatch lwb;
    leveldb::Status lrv;
    Status rv;
    wopt.sync = options.sync;

    /* Handler copies from a frontlevel WriteBatch to a leveldb one */
    class Handler : public WriteBatch::Handler {
     public:
      leveldb::WriteBatch *lwbp_;
      virtual void Put(const Slice& key, const Slice& value) {
        leveldb::Slice lkey(key.data(), key.size());
        leveldb::Slice lval(value.data(), value.size());
        lwbp_->Put(lkey, lval);
      }
      virtual void Delete(const Slice& key) {
        leveldb::Slice lkey(key.data(), key.size());
        lwbp_->Delete(lkey);
      }
    };

    Handler handler;
    handler.lwbp_ = &lwb;
    rv = updates->Iterate(&handler);

    if (rv.ok()) {
      lrv = dbp_->Write(wopt, &lwb);
      rv = lstat2fstat(lrv);
    }

    return(rv);
  }
                      
  /*
   * read key from store.   rets IsNotFound if not found...
   */
  Status Get(const ReadOptions &options, const Slice &key, 
             std::string *value) {
    leveldb::Slice lkey(key.data(), key.size());
    leveldb::ReadOptions ropt;
    leveldb::Status lrv;

    ropt.verify_checksums = options.verify_checksums;
    ropt.fill_cache = options.fill_cache;

    lrv = dbp_->Get(ropt, lkey, value);

    return(lstat2fstat(lrv));
  }

  /*
   * iterator
   */
  Iterator *NewIterator(const ReadOptions &options) {
    leveldb::ReadOptions ropt;
    leveldb::Iterator *rv;
    Iterator *it;
    
    ropt.verify_checksums = options.verify_checksums;
    ropt.fill_cache = options.fill_cache;

    rv = dbp_->NewIterator(ropt);
    if (rv == NULL)
      return(NULL);
    
    it = NewIter(rv);
    if (it == NULL) {
      delete rv;
    }
    return(it);
  }

  /*
   * load operations from a tablewalker
   */
  Status Load(const WriteOptions &options, Tablewalker *tw) {
    Status rv;
    int cnt;
    Slice k, v;
    bool del;
    leveldb::WriteOptions wopt;
    leveldb::WriteBatch lwb;
    leveldb::Status lrv;
    wopt.sync = options.sync;

    /*
     * download tw into leveldb::WriteBatch lwb.
     *
     * assume we can do the whole thing in one chunk.  could break it
     * up if we had to...
     */
    for (cnt = 0; tw->Valid() ; tw->Next(), cnt++) {
      k = tw->key(del);
      leveldb::Slice lkey(k.data(), k.size());
      if (del) {
        lwb.Delete(lkey);
      } else {
        v = tw->value();
        leveldb::Slice lval(v.data(), v.size());
        lwb.Put(lkey, lval);
      }
    }

    rv = tw->status();
    if (cnt > 0 && rv.ok()) {
      lrv = dbp_->Write(wopt, &lwb);
      rv = lstat2fstat(lrv);
    }

    return(rv);
  }
};

/*
 * LevelDBBackend::Iter: wraper class that translates between 
 * leveldb iterator and frontlevel iterator (easy, since they are
 * the same API).
 */
class LevelDBBackend::Iter : public Iterator {
 private:
  leveldb::Iterator *ldbit_;
 public:
  Iter(leveldb::Iterator *dbit) : ldbit_(dbit) { };
  ~Iter() { if (ldbit_) delete ldbit_; };

  bool Valid() const { return(ldbit_->Valid()); }
  void SeekToFirst() { ldbit_->SeekToFirst(); }
  void SeekToLast() { ldbit_->SeekToLast(); }
  void Seek(const Slice &target) {
    leveldb::Slice ltarg(target.data(), target.size());
    ldbit_->Seek(ltarg);
  }
  void Next() { ldbit_->Next(); }
  void Prev() { ldbit_->Prev(); }
  Slice key() const {
    leveldb::Slice lslice = ldbit_->key();
    return(Slice(lslice.data(), lslice.size()));
  }
  Slice value() const {
    leveldb::Slice lslice = ldbit_->value();
    return(Slice(lslice.data(), lslice.size()));
  }
  Status status() const {
    leveldb::Status rv = ldbit_->status();
    return(lstat2fstat(rv));
  }
};

Iterator *LevelDBBackend::NewIter(leveldb::Iterator *itp) {
  Iter *fl_itp;
  fl_itp = new Iter(itp);
  return(fl_itp);
}


}  // namespace frontlevel

#endif  // STORAGE_FRONTLEVEL_INCLUDE_LEVELDB_BACKEND_H_
