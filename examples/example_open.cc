/*
 * example of opening a frontlevel DB with a LevelDB backend
 */

/* 
 * assuming the compiled leveldb src is in ../../leveldb, you can
 * compile this with:
 * c++ -O -Wall -I../../leveldb/include -I../src/include -o ok ok.cc \
 *	../../leveldb/libleveldb.a ../src/libfrontlevel.a -lpthread
 */

#include <stdio.h>
#include <stdlib.h>

#include <leveldb/db.h>
#include <frontlevel/db.h>
#include <frontlevel/leveldb_backend.h>

int main(int argc, char **argv) {
    leveldb::DB *dbp;
    leveldb::Status lrv;
    leveldb::Options opts;  // ctor inits to default, override below

    printf("hello, first we create the backend db object\n");
    opts.create_if_missing = true;

    dbp = NULL;
    lrv = leveldb::DB::Open(opts, "/tmp/foo", &dbp);
    if (!lrv.ok()) {
        fprintf(stderr, "oops: open: %s\n", lrv.ToString().c_str());
        exit(1);
    }

    printf("now we deposit our backend db in a frontlevel Backend object\n");
    frontlevel::Backend *myback;
    myback = new frontlevel::LevelDBBackend(dbp);
    // myback now owns dbp and will delete it in its dtor

    printf("now we can open the frontlevel DB\n");
    frontlevel::Status frv;
    frontlevel::DB *fdbp;
    frontlevel::Options fopts;

    fopts.create_if_missing = true;
    fdbp = NULL;

    frv = frontlevel::DB::Open(fopts, "/tmp/frontfoo", myback, &fdbp);
    if (!frv.ok()) {
        fprintf(stderr, "oops: FL DB OPEN: %s\n", frv.ToString().c_str());
        exit(1);
    }

    delete dbp;
    exit(0);
}

