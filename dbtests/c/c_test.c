/* Copyright (c) 2011 The LevelDB Authors. All rights reserved.
   Use of this source code is governed by a BSD-style license that can be
   found in the LICENSE file. See the AUTHORS file for names of contributors. */

#include "frontlevel/c.h"
#include "leveldb/c.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

extern frontlevel_backend_t *frontlevel_new_leveldb_backend(leveldb_t *cldb);

const char* phase = "";
static char dbname[200];
static char ldbname[200];

static void StartPhase(const char* name) {
  fprintf(stderr, "=== Test %s\n", name);
  phase = name;
}

static const char* GetTempDir(void) {
    const char* ret = getenv("TEST_TMPDIR");
    if (ret == NULL || ret[0] == '\0')
        ret = "/tmp";
    return ret;
}

#define CheckNoError(err)                                               \
  if ((err) != NULL) {                                                  \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, (err)); \
    abort();                                                            \
  }

#define CheckCondition(cond)                                            \
  if (!(cond)) {                                                        \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, #cond); \
    abort();                                                            \
  }

static void CheckEqual(const char* expected, const char* v, size_t n) {
  if (expected == NULL && v == NULL) {
    // ok
  } else if (expected != NULL && v != NULL && n == strlen(expected) &&
             memcmp(expected, v, n) == 0) {
    // ok
    return;
  } else {
    fprintf(stderr, "%s: expected '%s', got '%s'\n",
            phase,
            (expected ? expected : "(null)"),
            (v ? v : "(null"));
    abort();
  }
}

static void Free(char** ptr) {
  if (*ptr) {
    free(*ptr);
    *ptr = NULL;
  }
}

static void CheckGet(
    frontlevel_t* db,
    const frontlevel_readoptions_t* options,
    const char* key,
    const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = frontlevel_get(db, options, key, strlen(key), &val_len, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckIter(frontlevel_iterator_t* iter,
                      const char* key, const char* val) {
  size_t len;
  const char* str;
  str = frontlevel_iter_key(iter, &len);
  CheckEqual(key, str, len);
  str = frontlevel_iter_value(iter, &len);
  CheckEqual(val, str, len);
}

// Callback from frontlevel_writebatch_iterate()
static void CheckPut(void* ptr,
                     const char* k, size_t klen,
                     const char* v, size_t vlen) {
  int* state = (int*) ptr;
  CheckCondition(*state < 2);
  switch (*state) {
    case 0:
      CheckEqual("bar", k, klen);
      CheckEqual("b", v, vlen);
      break;
    case 1:
      CheckEqual("box", k, klen);
      CheckEqual("c", v, vlen);
      break;
  }
  (*state)++;
}

// Callback from frontlevel_writebatch_iterate()
static void CheckDel(void* ptr, const char* k, size_t klen) {
  int* state = (int*) ptr;
  CheckCondition(*state == 2);
  CheckEqual("bar", k, klen);
  (*state)++;
}

static void CmpDestroy(void* arg) { }

static int CmpCompare(void* arg, const char* a, size_t alen,
                      const char* b, size_t blen) {
  int n = (alen < blen) ? alen : blen;
  int r = memcmp(a, b, n);
  if (r == 0) {
    if (alen < blen) r = -1;
    else if (alen > blen) r = +1;
  }
  return r;
}

static const char* CmpName(void* arg) {
  return "foo";
}

int main(int argc, char** argv) {
  frontlevel_t* db;
  frontlevel_comparator_t* cmp;
  frontlevel_env_t* env;
  frontlevel_options_t* options;
  frontlevel_readoptions_t* roptions;
  frontlevel_writeoptions_t* woptions;
  frontlevel_backend_t *fback;

  leveldb_t* ldb;
  leveldb_comparator_t* lcmp;
  leveldb_cache_t* lcache;
  leveldb_env_t* lenv;
  leveldb_options_t* loptions;
  leveldb_readoptions_t* lroptions;
  leveldb_writeoptions_t* lwoptions;

  char* err = NULL;
  int run = -1;

  CheckCondition(frontlevel_major_version() >= 1);
  CheckCondition(frontlevel_minor_version() >= 0);
  CheckCondition(leveldb_major_version() >= 1);
  CheckCondition(leveldb_minor_version() >= 1);

  snprintf(dbname, sizeof(dbname),
           "%s/frontlevel_c_test-%d",
           GetTempDir(),
           ((int) geteuid()));
  snprintf(ldbname, sizeof(ldbname),
           "%s/leveldb_c_test-%d",
           GetTempDir(),
           ((int) geteuid()));

  StartPhase("create_backend");
  lcmp = leveldb_comparator_create(NULL, CmpDestroy, CmpCompare, CmpName);
  lenv = leveldb_create_default_env();
  lcache = leveldb_cache_create_lru(100000);

  loptions = leveldb_options_create();
  leveldb_options_set_comparator(loptions, lcmp);
  leveldb_options_set_error_if_exists(loptions, 1);
  leveldb_options_set_cache(loptions, lcache);
  leveldb_options_set_env(loptions, lenv);
  leveldb_options_set_info_log(loptions, NULL);
  leveldb_options_set_write_buffer_size(loptions, 100000);
  leveldb_options_set_paranoid_checks(loptions, 1);
  leveldb_options_set_max_open_files(loptions, 10);
  leveldb_options_set_block_size(loptions, 1024);
  leveldb_options_set_block_restart_interval(loptions, 8);
  leveldb_options_set_compression(loptions, leveldb_no_compression);

  lroptions = leveldb_readoptions_create();
  leveldb_readoptions_set_verify_checksums(lroptions, 1);
  leveldb_readoptions_set_fill_cache(lroptions, 0);

  lwoptions = leveldb_writeoptions_create();
  leveldb_writeoptions_set_sync(lwoptions, 1);

  StartPhase("create_objects");
  cmp = frontlevel_comparator_create(NULL, CmpDestroy, CmpCompare, CmpName);
  env = frontlevel_create_default_env();

  options = frontlevel_options_create();
  frontlevel_options_set_comparator(options, cmp);
  frontlevel_options_set_error_if_exists(options, 1);
  frontlevel_options_set_env(options, env);
  frontlevel_options_set_info_log(options, NULL);
  frontlevel_options_set_write_buffer_size(options, 100000);
  frontlevel_options_set_paranoid_checks(options, 1);

  roptions = frontlevel_readoptions_create();
  frontlevel_readoptions_set_verify_checksums(roptions, 1);
  frontlevel_readoptions_set_fill_cache(roptions, 0);

  woptions = frontlevel_writeoptions_create();
  frontlevel_writeoptions_set_sync(woptions, 1);

  StartPhase("destroy");
  leveldb_destroy_db(loptions, ldbname, &err);
  frontlevel_destroy_db(options, dbname, &err);
  Free(&err);

  StartPhase("open_error");
  leveldb_options_set_create_if_missing(loptions, 1);
  ldb = leveldb_open(loptions, ldbname, &err);
  CheckNoError(err);
  fback = frontlevel_new_leveldb_backend(ldb);  /* consumed by open */
  db = frontlevel_open(options, dbname, fback, &err);
  CheckCondition(err != NULL);
  Free(&err);

  StartPhase("frontlevel_free");
  leveldb_destroy_db(loptions, ldbname, &err);
  ldb = leveldb_open(loptions, ldbname, &err);
  CheckNoError(err);
  fback = frontlevel_new_leveldb_backend(ldb);  /* consumed by open */
  db = frontlevel_open(options, dbname, fback, &err);
  CheckCondition(err != NULL);
  frontlevel_free(err);
  err = NULL;

  StartPhase("open");
  leveldb_destroy_db(loptions, ldbname, &err);
  ldb = leveldb_open(loptions, ldbname, &err);
  CheckNoError(err);
  fback = frontlevel_new_leveldb_backend(ldb);  /* consumed by open */
  frontlevel_options_set_create_if_missing(options, 1);
  db = frontlevel_open(options, dbname, fback, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "foo", NULL);

  StartPhase("put");
  frontlevel_put(db, woptions, "foo", 3, "hello", 5, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "foo", "hello");

  StartPhase("writebatch");
  {
    frontlevel_writebatch_t* wb = frontlevel_writebatch_create();
    frontlevel_writebatch_put(wb, "foo", 3, "a", 1);
    frontlevel_writebatch_clear(wb);
    frontlevel_writebatch_put(wb, "bar", 3, "b", 1);
    frontlevel_writebatch_put(wb, "box", 3, "c", 1);
    frontlevel_writebatch_delete(wb, "bar", 3);
    frontlevel_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", "hello");
    CheckGet(db, roptions, "bar", NULL);
    CheckGet(db, roptions, "box", "c");
    int pos = 0;
    frontlevel_writebatch_iterate(wb, &pos, CheckPut, CheckDel);
    CheckCondition(pos == 3);
    frontlevel_writebatch_destroy(wb);
  }

  StartPhase("iter");
  {
    frontlevel_iterator_t* iter = frontlevel_create_iterator(db, roptions);
    CheckCondition(!frontlevel_iter_valid(iter));
    frontlevel_iter_seek_to_first(iter);
    CheckCondition(frontlevel_iter_valid(iter));
    CheckIter(iter, "box", "c");
    frontlevel_iter_next(iter);
    CheckIter(iter, "foo", "hello");
    frontlevel_iter_prev(iter);
    CheckIter(iter, "box", "c");
    frontlevel_iter_prev(iter);
    CheckCondition(!frontlevel_iter_valid(iter));
    frontlevel_iter_seek_to_last(iter);
    CheckIter(iter, "foo", "hello");
    frontlevel_iter_seek(iter, "b", 1);
    CheckIter(iter, "box", "c");
    frontlevel_iter_get_error(iter, &err);
    CheckNoError(err);
    frontlevel_iter_destroy(iter);
  }

  StartPhase("cleanup");
  frontlevel_close(db);
  frontlevel_options_destroy(options);
  frontlevel_readoptions_destroy(roptions);
  frontlevel_writeoptions_destroy(woptions);
  frontlevel_comparator_destroy(cmp);
  frontlevel_env_destroy(env);

  leveldb_options_destroy(loptions);
  leveldb_readoptions_destroy(lroptions);
  leveldb_writeoptions_destroy(lwoptions);
  leveldb_cache_destroy(lcache);
  leveldb_comparator_destroy(lcmp);
  leveldb_env_destroy(lenv);


  fprintf(stderr, "PASS\n");
  return 0;
}
