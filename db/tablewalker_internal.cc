#include "db/dbformat.h"
#include "frontlevel/comparator.h"
#include "frontlevel/env.h"
#include "db/tablewalker_internal.h"

namespace frontlevel {

/*
 * we want to skip over any future entries with the same user key
 * (since they are all older than the current one).
 */
void TablewalkerInternal::Next() {

  ParsedInternalKey ike;

  if (!valid_)
    return;

  while (1) {

    isrc_->Next();
    valid_ = isrc_->Valid();
    if (!valid_) 
      break;

    valid_ = ParseInternalKey(isrc_->key(), &ike);
    if (!valid_) {
      status_ = Status::Corruption("tablewalker: next int key parse failed");
      break;
    }

    if (usrcmp_->Compare(ike.user_key, saved_key_) != 0)
      break;

    /* otherwise older duplicate key, skip it */
  }

  /* save off new key if we got one */
  if (valid_) {
    saved_key_.assign(ike.user_key.data(), ike.user_key.size());
    saved_isdel_ = (ike.type == kTypeDeletion);
  }

}

}  // namespace frontlevel
