/* Copyright (c) 2011 The LevelDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file. See the AUTHORS file for names of contributors.

  C bindings for leveldb.  May be useful as a stable ABI that can be
  used by programs that keep leveldb in a shared library, or for
  a JNI api.

  Does not support:
  . getters for the option types
  . custom comparators that implement key shortening
  . custom iter, db, env, cache implementations using just the C bindings

  Some conventions:

  (1) We expose just opaque struct pointers and functions to clients.
  This allows us to change internal representations without having to
  recompile clients.

  (2) For simplicity, there is no equivalent to the Slice type.  Instead,
  the caller has to pass the pointer and length as separate
  arguments.

  (3) Errors are represented by a null-terminated c string.  NULL
  means no error.  All operations that can raise an error are passed
  a "char** errptr" as the last argument.  One of the following must
  be true on entry:
     *errptr == NULL
     *errptr points to a malloc()ed null-terminated error message
       (On Windows, *errptr must have been malloc()-ed by this library.)
  On success, a leveldb routine leaves *errptr unchanged.
  On failure, leveldb frees the old value of *errptr and
  set *errptr to a malloc()ed error message.

  (4) Bools have the type unsigned char (0 == false; rest == true)

  (5) All of the pointer arguments must be non-NULL.
*/

#ifndef STORAGE_FRONTLEVEL_INCLUDE_C_H_
#define STORAGE_FRONTLEVEL_INCLUDE_C_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

/* Exported types */

typedef struct frontlevel_t               frontlevel_t;
typedef struct frontlevel_backend_t       frontlevel_backend_t;
typedef struct frontlevel_comparator_t    frontlevel_comparator_t;
typedef struct frontlevel_env_t           frontlevel_env_t;
typedef struct frontlevel_filelock_t      frontlevel_filelock_t;
typedef struct frontlevel_iterator_t      frontlevel_iterator_t;
typedef struct frontlevel_logger_t        frontlevel_logger_t;
typedef struct frontlevel_options_t       frontlevel_options_t;
typedef struct frontlevel_randomfile_t    frontlevel_randomfile_t;
typedef struct frontlevel_readoptions_t   frontlevel_readoptions_t;
typedef struct frontlevel_seqfile_t       frontlevel_seqfile_t;
typedef struct frontlevel_writablefile_t  frontlevel_writablefile_t;
typedef struct frontlevel_writebatch_t    frontlevel_writebatch_t;
typedef struct frontlevel_writeoptions_t  frontlevel_writeoptions_t;

/* DB operations */

extern frontlevel_t* frontlevel_open(
    const frontlevel_options_t* options,
    const char* name,
    const frontlevel_backend_t *backend,
    char** errptr);

extern void frontlevel_close(frontlevel_t* db);

extern void frontlevel_put(
    frontlevel_t* db,
    const frontlevel_writeoptions_t* options,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr);

extern void frontlevel_delete(
    frontlevel_t* db,
    const frontlevel_writeoptions_t* options,
    const char* key, size_t keylen,
    char** errptr);

extern void frontlevel_write(
    frontlevel_t* db,
    const frontlevel_writeoptions_t* options,
    frontlevel_writebatch_t* batch,
    char** errptr);

/* Returns NULL if not found.  A malloc()ed array otherwise.
   Stores the length of the array in *vallen. */
extern char* frontlevel_get(
    frontlevel_t* db,
    const frontlevel_readoptions_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr);

extern frontlevel_iterator_t* frontlevel_create_iterator(
    frontlevel_t* db,
    const frontlevel_readoptions_t* options);

/* Management operations */

extern void frontlevel_destroy_db(
    const frontlevel_options_t* options,
    const char* name,
    char** errptr);

/* Iterator */

extern void frontlevel_iter_destroy(frontlevel_iterator_t*);
extern unsigned char frontlevel_iter_valid(const frontlevel_iterator_t*);
extern void frontlevel_iter_seek_to_first(frontlevel_iterator_t*);
extern void frontlevel_iter_seek_to_last(frontlevel_iterator_t*);
extern void frontlevel_iter_seek(frontlevel_iterator_t*, const char* k, 
                                 size_t klen);
extern void frontlevel_iter_next(frontlevel_iterator_t*);
extern void frontlevel_iter_prev(frontlevel_iterator_t*);
extern const char* frontlevel_iter_key(const frontlevel_iterator_t*, 
                                       size_t* klen);
extern const char* frontlevel_iter_value(const frontlevel_iterator_t*, 
                                         size_t* vlen);
extern void frontlevel_iter_get_error(const frontlevel_iterator_t*, 
                                      char** errptr);

/* Write batch */

extern frontlevel_writebatch_t* frontlevel_writebatch_create();
extern void frontlevel_writebatch_destroy(frontlevel_writebatch_t*);
extern void frontlevel_writebatch_clear(frontlevel_writebatch_t*);
extern void frontlevel_writebatch_put(
    frontlevel_writebatch_t*,
    const char* key, size_t klen,
    const char* val, size_t vlen);
extern void frontlevel_writebatch_delete(
    frontlevel_writebatch_t*,
    const char* key, size_t klen);
extern void frontlevel_writebatch_iterate(
    frontlevel_writebatch_t*,
    void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen));

/* Options */

extern frontlevel_options_t* frontlevel_options_create();
extern void frontlevel_options_destroy(frontlevel_options_t*);
extern void frontlevel_options_set_comparator(
    frontlevel_options_t*,
    frontlevel_comparator_t*);
extern void frontlevel_options_set_create_if_missing(
    frontlevel_options_t*, unsigned char);
extern void frontlevel_options_set_error_if_exists(
    frontlevel_options_t*, unsigned char);
extern void frontlevel_options_set_paranoid_checks(
    frontlevel_options_t*, unsigned char);
extern void frontlevel_options_set_env(frontlevel_options_t*, 
                                       frontlevel_env_t*);
extern void frontlevel_options_set_info_log(frontlevel_options_t*, 
                                            frontlevel_logger_t*);
extern void frontlevel_options_set_write_buffer_size(frontlevel_options_t*, 
                                                     size_t);

/* Comparator */

extern frontlevel_comparator_t* frontlevel_comparator_create(
    void* state,
    void (*destructor)(void*),
    int (*compare)(
        void*,
        const char* a, size_t alen,
        const char* b, size_t blen),
    const char* (*name)(void*));
extern void frontlevel_comparator_destroy(frontlevel_comparator_t*);

/* Read options */

extern frontlevel_readoptions_t* frontlevel_readoptions_create();
extern void frontlevel_readoptions_destroy(frontlevel_readoptions_t*);
extern void frontlevel_readoptions_set_verify_checksums(
    frontlevel_readoptions_t*,
    unsigned char);
extern void frontlevel_readoptions_set_fill_cache(
    frontlevel_readoptions_t*, unsigned char);

/* Write options */

extern frontlevel_writeoptions_t* frontlevel_writeoptions_create();
extern void frontlevel_writeoptions_destroy(frontlevel_writeoptions_t*);
extern void frontlevel_writeoptions_set_sync(
    frontlevel_writeoptions_t*, unsigned char);

/* Env */

extern frontlevel_env_t* frontlevel_create_default_env();
extern void frontlevel_env_destroy(frontlevel_env_t*);

/* Utility */

/* Calls free(ptr).
   REQUIRES: ptr was malloc()-ed and returned by one of the routines
   in this file.  Note that in certain cases (typically on Windows), you
   may need to call this routine instead of free(ptr) to dispose of
   malloc()-ed memory returned by this library. */
extern void frontlevel_free(void* ptr);

/* Return the major version number for this release. */
extern int frontlevel_major_version();

/* Return the minor version number for this release. */
extern int frontlevel_minor_version();

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif  /* STORAGE_FRONTLEVEL_INCLUDE_C_H_ */
