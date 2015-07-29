// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "frontlevel/c.h"

#include <stdlib.h>
#include <unistd.h>
#include "frontlevel/comparator.h"
#include "frontlevel/db.h"
#include "frontlevel/env.h"
#include "frontlevel/iterator.h"
#include "frontlevel/options.h"
#include "frontlevel/status.h"
#include "frontlevel/write_batch.h"

#include "frontlevel/backend.h"

using frontlevel::Backend;
using frontlevel::Comparator;
using frontlevel::DB;
using frontlevel::Env;
using frontlevel::FileLock;
using frontlevel::Iterator;
using frontlevel::kMajorVersion;
using frontlevel::kMinorVersion;
using frontlevel::Logger;
using frontlevel::Options;
using frontlevel::RandomAccessFile;
using frontlevel::ReadOptions;
using frontlevel::SequentialFile;
using frontlevel::Slice;
using frontlevel::Status;
using frontlevel::WritableFile;
using frontlevel::WriteBatch;
using frontlevel::WriteOptions;

extern "C" {

struct frontlevel_t              { DB*               rep; };
struct frontlevel_backend_t      { Backend*          rep; };
struct frontlevel_iterator_t     { Iterator*         rep; };
struct frontlevel_writebatch_t   { WriteBatch        rep; };
struct frontlevel_readoptions_t  { ReadOptions       rep; };
struct frontlevel_writeoptions_t { WriteOptions      rep; };
struct frontlevel_options_t      { Options           rep; };
struct frontlevel_seqfile_t      { SequentialFile*   rep; };
struct frontlevel_randomfile_t   { RandomAccessFile* rep; };
struct frontlevel_writablefile_t { WritableFile*     rep; };
struct frontlevel_logger_t       { Logger*           rep; };
struct frontlevel_filelock_t     { FileLock*         rep; };


struct frontlevel_comparator_t : public Comparator {
  void* state_;
  void (*destructor_)(void*);
  int (*compare_)(
      void*,
      const char* a, size_t alen,
      const char* b, size_t blen);
  const char* (*name_)(void*);

  virtual ~frontlevel_comparator_t() {
    (*destructor_)(state_);
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return (*compare_)(state_, a.data(), a.size(), b.data(), b.size());
  }

  virtual const char* Name() const {
    return (*name_)(state_);
  }

};

struct frontlevel_env_t {
  Env* rep;
  bool is_default;
};

static bool SaveError(char** errptr, const Status& s) {
  assert(errptr != NULL);
  if (s.ok()) {
    return false;
  } else if (*errptr == NULL) {
    *errptr = strdup(s.ToString().c_str());
  } else {
    // TODO(sanjay): Merge with existing error?
    free(*errptr);
    *errptr = strdup(s.ToString().c_str());
  }
  return true;
}

static char* CopyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

frontlevel_t* frontlevel_open(
    const frontlevel_options_t* options,
    const char* name,
    const frontlevel_backend_t *backend,
    char** errptr) {
  DB* db;
  /* open takes over Backend and frees if error, so we can dump it */
  Backend *backing = backend->rep;
  delete backend;
  if (SaveError(errptr, DB::Open(options->rep, std::string(name), 
                                 backend->rep, &db))) {
    return NULL;
  }
  frontlevel_t* result = new frontlevel_t;
  result->rep = db;
  return result;
}

void frontlevel_close(frontlevel_t* db) {
  delete db->rep;
  delete db;
}

void frontlevel_put(
    frontlevel_t* db,
    const frontlevel_writeoptions_t* options,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr) {
  SaveError(errptr,
            db->rep->Put(options->rep, Slice(key, keylen), Slice(val, vallen)));
}

void frontlevel_delete(
    frontlevel_t* db,
    const frontlevel_writeoptions_t* options,
    const char* key, size_t keylen,
    char** errptr) {
  SaveError(errptr, db->rep->Delete(options->rep, Slice(key, keylen)));
}


void frontlevel_write(
    frontlevel_t* db,
    const frontlevel_writeoptions_t* options,
    frontlevel_writebatch_t* batch,
    char** errptr) {
  SaveError(errptr, db->rep->Write(options->rep, &batch->rep));
}

char* frontlevel_get(
    frontlevel_t* db,
    const frontlevel_readoptions_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = NULL;
  std::string tmp;
  Status s = db->rep->Get(options->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

frontlevel_iterator_t* frontlevel_create_iterator(
    frontlevel_t* db,
    const frontlevel_readoptions_t* options) {
  frontlevel_iterator_t* result = new frontlevel_iterator_t;
  result->rep = db->rep->NewIterator(options->rep);
  return result;
}

void frontlevel_destroy_db(
    const frontlevel_options_t* options,
    const char* name,
    char** errptr) {
  SaveError(errptr, DestroyDB(name, options->rep));
}

void frontlevel_iter_destroy(frontlevel_iterator_t* iter) {
  delete iter->rep;
  delete iter;
}

unsigned char frontlevel_iter_valid(const frontlevel_iterator_t* iter) {
  return iter->rep->Valid();
}

void frontlevel_iter_seek_to_first(frontlevel_iterator_t* iter) {
  iter->rep->SeekToFirst();
}

void frontlevel_iter_seek_to_last(frontlevel_iterator_t* iter) {
  iter->rep->SeekToLast();
}

void frontlevel_iter_seek(frontlevel_iterator_t* iter, const char* k, 
                          size_t klen) {
  iter->rep->Seek(Slice(k, klen));
}

void frontlevel_iter_next(frontlevel_iterator_t* iter) {
  iter->rep->Next();
}

void frontlevel_iter_prev(frontlevel_iterator_t* iter) {
  iter->rep->Prev();
}

const char* frontlevel_iter_key(const frontlevel_iterator_t* iter, 
                                size_t* klen) {
  Slice s = iter->rep->key();
  *klen = s.size();
  return s.data();
}

const char* frontlevel_iter_value(const frontlevel_iterator_t* iter, 
                                  size_t* vlen) {
  Slice s = iter->rep->value();
  *vlen = s.size();
  return s.data();
}

void frontlevel_iter_get_error(const frontlevel_iterator_t* iter, 
                               char** errptr) {
  SaveError(errptr, iter->rep->status());
}

frontlevel_writebatch_t* frontlevel_writebatch_create() {
  return new frontlevel_writebatch_t;
}

void frontlevel_writebatch_destroy(frontlevel_writebatch_t* b) {
  delete b;
}

void frontlevel_writebatch_clear(frontlevel_writebatch_t* b) {
  b->rep.Clear();
}

void frontlevel_writebatch_put(
    frontlevel_writebatch_t* b,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep.Put(Slice(key, klen), Slice(val, vlen));
}

void frontlevel_writebatch_delete(
    frontlevel_writebatch_t* b,
    const char* key, size_t klen) {
  b->rep.Delete(Slice(key, klen));
}

void frontlevel_writebatch_iterate(
    frontlevel_writebatch_t* b,
    void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen)) {
  class H : public WriteBatch::Handler {
   public:
    void* state_;
    void (*put_)(void*, const char* k, size_t klen, const char* v, size_t vlen);
    void (*deleted_)(void*, const char* k, size_t klen);
    virtual void Put(const Slice& key, const Slice& value) {
      (*put_)(state_, key.data(), key.size(), value.data(), value.size());
    }
    virtual void Delete(const Slice& key) {
      (*deleted_)(state_, key.data(), key.size());
    }
  };
  H handler;
  handler.state_ = state;
  handler.put_ = put;
  handler.deleted_ = deleted;
  b->rep.Iterate(&handler);
}

frontlevel_options_t* frontlevel_options_create() {
  return new frontlevel_options_t;
}

void frontlevel_options_destroy(frontlevel_options_t* options) {
  delete options;
}

void frontlevel_options_set_comparator(
    frontlevel_options_t* opt,
    frontlevel_comparator_t* cmp) {
  opt->rep.comparator = cmp;
}

void frontlevel_options_set_create_if_missing(
    frontlevel_options_t* opt, unsigned char v) {
  opt->rep.create_if_missing = v;
}

void frontlevel_options_set_error_if_exists(
    frontlevel_options_t* opt, unsigned char v) {
  opt->rep.error_if_exists = v;
}

void frontlevel_options_set_paranoid_checks(
    frontlevel_options_t* opt, unsigned char v) {
  opt->rep.paranoid_checks = v;
}

void frontlevel_options_set_env(frontlevel_options_t* opt, 
                                frontlevel_env_t* env) {
  opt->rep.env = (env ? env->rep : NULL);
}

void frontlevel_options_set_info_log(frontlevel_options_t* opt, 
                                     frontlevel_logger_t* l) {
  opt->rep.info_log = (l ? l->rep : NULL);
}

void frontlevel_options_set_write_buffer_size(frontlevel_options_t* opt, 
                                              size_t s) {
  opt->rep.write_buffer_size = s;
}

frontlevel_comparator_t* frontlevel_comparator_create(
    void* state,
    void (*destructor)(void*),
    int (*compare)(
        void*,
        const char* a, size_t alen,
        const char* b, size_t blen),
    const char* (*name)(void*)) {
  frontlevel_comparator_t* result = new frontlevel_comparator_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->compare_ = compare;
  result->name_ = name;
  return result;
}

void frontlevel_comparator_destroy(frontlevel_comparator_t* cmp) {
  delete cmp;
}

frontlevel_readoptions_t* frontlevel_readoptions_create() {
  return new frontlevel_readoptions_t;
}

void frontlevel_readoptions_destroy(frontlevel_readoptions_t* opt) {
  delete opt;
}

void frontlevel_readoptions_set_verify_checksums(
    frontlevel_readoptions_t* opt,
    unsigned char v) {
  opt->rep.verify_checksums = v;
}

void frontlevel_readoptions_set_fill_cache(
    frontlevel_readoptions_t* opt, unsigned char v) {
  opt->rep.fill_cache = v;
}

frontlevel_writeoptions_t* frontlevel_writeoptions_create() {
  return new frontlevel_writeoptions_t;
}

void frontlevel_writeoptions_destroy(frontlevel_writeoptions_t* opt) {
  delete opt;
}

void frontlevel_writeoptions_set_sync(
    frontlevel_writeoptions_t* opt, unsigned char v) {
  opt->rep.sync = v;
}

frontlevel_env_t* frontlevel_create_default_env() {
  frontlevel_env_t* result = new frontlevel_env_t;
  result->rep = Env::Default();
  result->is_default = true;
  return result;
}

void frontlevel_env_destroy(frontlevel_env_t* env) {
  if (!env->is_default) delete env->rep;
  delete env;
}

void frontlevel_free(void* ptr) {
  free(ptr);
}

int frontlevel_major_version() {
  return kMajorVersion;
}

int frontlevel_minor_version() {
  return kMinorVersion;
}

}  // end extern "C"
