/*
 * tablewalker: an iterator wrapper that does a sequential forward scan
 * of keys in a table, removing duplicate entries (e.g. using the 
 * sequence number to provide only * the most recent entry for each 
 * user-level key).
 */

#ifndef STORAGE_FRONTLEVEL_INCLUDE_TABLEWALKER_H_
#define STORAGE_FRONTLEVEL_INCLUDE_TABLEWALKER_H_

#include "frontlevel/slice.h"
#include "frontlevel/status.h"

namespace frontlevel {
class Iterator;    /* forward decl for pointer */

class Tablewalker {
 public:
  Tablewalker() {};
  virtual ~Tablewalker() {};

  virtual bool Valid() const = 0;    /* at valid position */
  virtual void Next() = 0;           /* move to next, update valid */
  /* get key+keytype, must be valid.  using bool since ValueType is internal */
  virtual Slice key(bool &is_deletion) const = 0;
  virtual Slice value() const = 0;   /* get value, must be valid */
  virtual Status status() const = 0; /* get status */

 private:
  // No copying allowed
  Tablewalker(const Tablewalker&);
  void operator=(const Tablewalker&);
};


}  // namespace frontlevel

#endif  // STORAGE_FRONTLEVEL_INCLUDE_TABLEWALKER_H_
