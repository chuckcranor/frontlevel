/*
 * internal table walker.  we want an iterator that returns internal
 * keys as input.  we take over ownership of the input iterator and 
 * delete it when our dtor is called.
 */

#ifndef STORAGE_FRONTLEVEL_DB_TABLEWALKER_INTERNAL_H_
#define STORAGE_FRONTLEVEL_DB_TABLEWALKER_INTERNAL_H_

#include "frontlevel/iterator.h"
#include "frontlevel/tablewalker.h"

namespace frontlevel {

class TablewalkerInternal : public Tablewalker {
 public:

  TablewalkerInternal(Iterator *isrc, const Comparator *usrcmp) :
    isrc_(isrc), usrcmp_(usrcmp) {

    isrc_->SeekToFirst();
    valid_ = isrc_->Valid();
    if (!valid_) return;

    ParsedInternalKey ike;
    valid_ = ParseInternalKey(isrc_->key(), &ike);
    if (valid_) {
      saved_key_.assign(ike.user_key.data(), ike.user_key.size());
      saved_isdel_ = (ike.type == kTypeDeletion);
    } else {
      status_ = Status::Corruption("tablewalker: int key parse failed");
    }
  }

  ~TablewalkerInternal() { if (isrc_) { delete isrc_; } }

  bool Valid()  const   { return valid_; }

  void Next();

  Slice key(bool &is_del) const { 
    assert(valid_); 
    is_del = saved_isdel_;
    return saved_key_; 
  }

  Slice value() const { 
    assert(valid_); 
    return(isrc_->value());  
  }

  Status status() const { 
    return((status_.ok()) ? isrc_->status() : status_);
  }

 private:
  Iterator *isrc_;
  const Comparator *const usrcmp_;

  Status status_;
  bool valid_;
  /* 
   * XXX: could make saved_key_ a Slice pointer to key rather than
   * a copy of key if we know the parent data structure won't move
   * the key on us...
   */
  std::string saved_key_;   /* current user key */
  bool saved_isdel_;
  
  // No copying allowed
  TablewalkerInternal(const TablewalkerInternal&);
  void operator=(const TablewalkerInternal&);
};

}  // namespace frontlevel
#endif  // STORAGE_FRONTLEVEL_DB_TABLEWALKER_INTERNAL_H_
