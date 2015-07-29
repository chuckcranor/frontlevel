
#include "frontlevel/db.h"
#include "frontlevel/c.h"
#include "frontlevel/leveldb_backend.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * we need to duplicate these so we can get at the leveldb object
 * inside the leveldb_t (which is not defined in the c API file).
 * we need that in order to allocate the leveldb backend.
 *
 * we basically rip the leveldb::DB out of the leveldb_t and put
 * it in the frontlevel::LevelDBBackend.   then we dispose of 
 * the leveldb_t (so the caller should not call close on it).
 *
 * XXX: not the best layering going on here...
 */
struct leveldb_t              { leveldb::DB*               rep; };
struct frontlevel_backend_t   { frontlevel::Backend*       rep; };

frontlevel_backend_t *frontlevel_new_leveldb_backend(leveldb_t *cldb) {
    frontlevel_backend_t *result = new frontlevel_backend_t;
    result->rep = new frontlevel::LevelDBBackend(cldb->rep);
    cldb->rep = NULL;
    delete cldb;
    return(result);
}


#ifdef __cplusplus
} /* end extern "C" */
#endif
