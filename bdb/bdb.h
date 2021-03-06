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

#define LOG_ERROR(msg, err) log_error(__FILE__, __FUNCTION__, __LINE__, msg, err)

void log_error(const char *file, const char *function, int line, const char *msg, int err);

int start_base(int argc, char *argv[], int flush, void *ptr);
int start_mgr(int argc, char *argv[], int flush, void *ptr);

int db_get(DB *dbp, DB_TXN *txn, char *_key, unsigned int keylen, char **_data, unsigned int *datalen, unsigned int flags);
int db_put(DB *dbp, DB_TXN *txn, char *_key, unsigned int keylen, char *_data, unsigned int datalen, unsigned int flags);
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

int txn_begin(DB_ENV *dbenv, DB_TXN **txn, unsigned int flags);
int txn_abort(DB_TXN *txn);
int txn_commit(DB_TXN *txn);

#ifdef __cplusplus
}
#endif

#endif // __BDB_H__
