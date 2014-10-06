#ifndef __BDB_H__
#define __BDB_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <time.h>

struct expire_key {
    time_t t;
    unsigned int thread_id;
    unsigned int seq;
};

#include <db.h>
#include "rep_common.h"
int start_base(int argc, char *argv[], void *ptr);
int start_mgr(int argc, char *argv[], void *ptr);

int db_get(DB *dbp, DB_TXN *txn, char *_key, unsigned int keylen, char **_data, unsigned int *datalen);
int db_put(DB *dbp, DB_TXN *txn, char *_key, unsigned int keylen, char *_data, unsigned int datalen);
int db_set_expire(
        DB *expire_db,
        DB *index_db,
        DB_TXN *txn,
        char *key,
        unsigned int keylen,
        unsigned int sec,
        unsigned int seq,
        unsigned int tid
        );
int db_close(DB *dbp);

int get_db(DB_ENV *dbenv, SHARED_DATA *shared_data, const char *name, int dbtype, DB **out);
int is_finished(SHARED_DATA *shared_data);

void split_key(char *_key, int keylen, char **table, int *tablelen, char **name, int *namelen);
int expire_key_compare(DB *db, const DBT *a, const DBT *b, size_t *locp);

int txn_begin(DB_ENV *dbenv, DB_TXN **txn, unsigned int flags);
int txn_abort(DB_TXN *txn);
int txn_commit(DB_TXN *txn);

#ifdef __cplusplus
}
#endif

#endif // __BDB_H__
