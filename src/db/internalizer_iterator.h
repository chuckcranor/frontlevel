
/*
 * internalizer_iterator.h  convert an iterator to an internal iterator
 */


/*
 * the level db code makes extensive use of iterators: it has two
 * level iterators, merging iterators, iterator wrappers, etc.
 * 
 * in order to make frontlevel work without doing major changes
 * to the iterator code, we need a way to convert an iterator that
 * uses user keys to one that provides internal keys (i.e. keys that
 * have a user key, plus an internal sequence number and type).  
 * we do that here.
 *
 * here's how the iterators stack:
 * 
 *  - top level: DBIter (db_iter.{cc,h})
 *                - takes a single iterator that returns internal keys
 *                - uses internal key seq# and type to handle duplicate
 *                  keys and delete markers (tombstones)
 *  
 *  - the input to the top level DBIter is a merging iterator (merger.{cc,h}).
 *    this iterator merges together MemTable iterators from mem_
 *    and imm_ (if present) with one or more iterators from underlying
 *    tables.   in level db these are sstable iterators, but in frontlevel
 *    it is an iterator from the backend.
 * 
 *    the problem here is that in level db, the sstables use internal
 *    keys so it is easy to merge them with the memtables.  but in 
 *    frontlevel, the backend uses user keys, so they can't be directly
 *    merged.  
 *
 *    the easy way to handle this mismatch is to convert the backend
 *    keys to internal keys (just assign them all seq# of 0 and normal
 *    type value [no tombstones in the backend]).   this class does
 *    this internalization.
 *
 *    so we convert backend keys to internal keys in order to merge
 *    them with the memtables, then the high level DBIter converts
 *    them back to user keys to return them to the caller.
 *
 * InternalizingIterator takes ownership of the parent_ iterator,
 * deleting it at dtor time.
 */ 

#ifndef STORAGE_FRONTLEVEL_DB_INTERNALIZER_ITERATOR_H_
#define STORAGE_FRONTLEVEL_DB_INTERNALIZER_ITERATOR_H_

#include <string>
#include "frontlevel/db.h"
#include "db/dbformat.h"

namespace frontlevel {

class InternalizingIterator : public Iterator {
 public:

  InternalizingIterator(Iterator *parent) : 
    parent_(parent) { 
      GetKey();
  };
  ~InternalizingIterator() { delete parent_; }

  bool Valid() const { return valid_; }

  void SeekToFirst() { parent_->SeekToFirst();   GetKey(); }
  void SeekToLast()  { parent_->SeekToLast();    GetKey(); }
  void Seek(const Slice& target) {   /* target is internal key */
    parent_->Seek(ExtractUserKey(target));
  }
  void Next()        { parent_->Next();          GetKey(); }
  void Prev()        { parent_->Prev();          GetKey(); }
  Slice key() const {
    return(saved_key_);
  }
  Slice value() const   { return(parent_->value()); }
  Status status() const { return(parent_->status()); }

 private:
  void GetKey() {
    valid_ = parent_->Valid();
    if (valid_) {
      /* hardwire seq# and type here */
      ParsedInternalKey ikey(parent_->key(), 0, kTypeValue);
      saved_key_.clear();
      AppendInternalKey(&saved_key_, ikey);  /* result placed in saved_key_ */
    }
  }

  Iterator *parent_;
  bool valid_;
  std::string saved_key_;

  InternalizingIterator();

  // No copying allowed
  InternalizingIterator(const InternalizingIterator&);
  void operator=(const InternalizingIterator&);
};


}  // namespace frontlevel

#endif  // STORAGE_FRONTLEVEL_DB_INTERNALIZER_ITERATOR_H_

