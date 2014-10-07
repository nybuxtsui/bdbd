#include <string.h>
#include <stdlib.h>
#include <db.h>
#include <errno.h>
#include "rep_common.h"
#include "bdb.h"
#include "dbmap.h"

struct expire_ctx {
    DB_ENV *dbenv;
    SHARED_DATA *shared_data;
    char *data_buff;
    DB *expire_db, *expire_index_db;
    dbmap_t dbmap;
};

static DB *
must_open_db(struct expire_ctx *ctx, const char *name, int type) {
    DB *db;
    int ret;
    for (;;) {
        if (ctx->shared_data->app_finished == 1) {
            return NULL;
        }
        ret = get_db(ctx->dbenv, ctx->shared_data, name, type, &db);
        if (ret == 0) {
            return db;
        }
        LOG_ERROR("get_db", ret);
        sleep(1);
    }
}

static int
get_target_db(struct expire_ctx *ctx, const char *table, DB **db) {
    int ret;

    *db = dbmap_find(ctx->dbmap, table);
    if (*db == NULL) { // db记录尚未缓存
        char cname[256];
        snprintf(cname, sizeof cname, "%s.db", table);
        cname[sizeof cname - 1] = 0;
        ret = get_db(ctx->dbenv, ctx->shared_data, cname, DB_UNKNOWN, db);
        if (ret) {
            ctx->dbenv->err(ctx->dbenv, ret, "Could not open db.");
            return ret;
        }
        dbmap_add(ctx->dbmap, table, *db);
    }
    return 0;
}

static int
expire_check_one(struct expire_ctx *ctx, DB_TXN *parent_txn, DBT *key, DBT *data) {
    struct expire_key _indexdata;
    int ret, ret2;
    DBT indexdata, delkey;
    DB *target_db;
    char *table, *name;
    int tablelen, namelen;
    DB_TXN *txn;

    data->flags = 0;
    memset(&indexdata, 0, sizeof indexdata);
    memset(&_indexdata, 0, sizeof _indexdata);
    indexdata.flags = DB_DBT_USERMEM;
    indexdata.ulen = sizeof _indexdata;
    indexdata.data = &_indexdata;

    ret = ctx->dbenv->txn_begin(ctx->dbenv, parent_txn, &txn, DB_READ_UNCOMMITTED);
    if (ret) {
        LOG_ERROR("txn_begin", ret);
        return ret;
    }
    ret = ctx->expire_index_db->get(ctx->expire_index_db, txn, data, &indexdata, DB_RMW);
    if (ret == DB_NOTFOUND) {
        Debug("expire_check_one|get|index_not_found");
        goto commit;
    }
    if (ret) {
        LOG_ERROR("get|index", ret);
        goto abort;
    }
    if (memcmp(&_indexdata, key->data, sizeof _indexdata) != 0) {
        Debug("expire_check_one|index_change");
        goto commit;
    }

    ret = ctx->expire_index_db->del(ctx->expire_index_db, txn, data, 0);
    if (ret == DB_NOTFOUND) {
        Debug("expire_check_one|del|index_not_found");
        goto commit;
    }
    if (ret) {
        LOG_ERROR("del|index", ret);
        goto abort;
    }

    split_key(data->data, data->size, &table, &tablelen, &name, &namelen);
    ret = get_target_db(ctx, table, &target_db);
    if (ret) {
        LOG_ERROR("get_target_db", ret);
        goto abort;
    }
    memset(&delkey, 0, sizeof delkey);
    delkey.data = name;
    delkey.size = namelen;
    ret = target_db->del(target_db, txn, &delkey, 0);
    if (ret == 0 || ret == DB_NOTFOUND) {
        goto commit;
    } else {
        LOG_ERROR("del|target", ret);
        if (ret == DB_REP_HANDLE_DEAD) {
            dbmap_del(ctx->dbmap, table);
        }
        goto abort;
    }

abort:
    ret2 = txn->abort(txn);
    if (ret2) {
        LOG_ERROR("abort", ret);
    }
    return ret;
commit:
    ret = txn->commit(txn, 0);
    if (ret) {
        LOG_ERROR("commit", ret);
    }
    return 0;
}

static int
expire_check(struct expire_ctx *ctx) {
    DBT key, data;
    DB_TXN *txn;
    DBC *cur;
    struct expire_key keydata;
    int ret, ret2, count;
    time_t now;

    count = 0;
restart:
    txn = NULL;
    cur = NULL;

    ret = ctx->dbenv->txn_begin(ctx->dbenv, NULL, &txn, DB_READ_UNCOMMITTED);
    if (ret) {
        LOG_ERROR("txn_begin", ret);
        goto end;
    }
    ret = ctx->expire_db->cursor(ctx->expire_db, txn, &cur, DB_READ_UNCOMMITTED);
    if (ret) {
        LOG_ERROR("cursor", ret);
        goto end;
    }

    memset(&key, 0, sizeof key);
    memset(&data, 0, sizeof data);
    memset(&keydata, 0, sizeof keydata);

    key.flags = DB_DBT_USERMEM;
    key.ulen = sizeof keydata;
    key.data = &keydata;
    data.flags = DB_DBT_REALLOC;
    data.data = ctx->data_buff;

    time(&now);
    for (;;) {
        ret = 0;
        ++count;
        if (ctx->shared_data->app_finished == 1) {
            goto end;
        }
        if (count > 1000) {
            ret = DB_NOTFOUND;
            goto end;
        }
        if (count % 10 == 0) {
            goto end;
        }
        ret = cur->get(cur, &key, &data, DB_NEXT);
        if (ret == DB_NOTFOUND) {
            goto end;
        }
        if (ret == 0 && keydata.t > now) {
            ret = DB_NOTFOUND;
            goto end;
        }
        if (ret) {
            LOG_ERROR("get|cursor", ret);
            goto end;
        }

        char buf[1024];
        snprintf(buf,
                sizeof buf,
                "expire_check_one|%u|%d|%d|%.*s",
                (unsigned int)keydata.t,
                keydata.seq,
                keydata.thread_id,
                data.size,
                (char *)data.data);
        buf[sizeof buf - 1] = 0;
        Debug(buf);

        ret = expire_check_one(ctx, txn, &key, &data);
        if (ret) {
            LOG_ERROR("expire_check_one", ret);
            goto end;
        }
        ret = cur->del(cur, 0);
        if (ret) {
            LOG_ERROR("cur|del", ret);
            goto end;
        }
    }

end:
    if (cur) {
        ret2 = cur->close(cur);
        if (ret2) {
            LOG_ERROR("close|cur", ret2);
        }
    }
    if (txn) {
        if (ret == DB_NOTFOUND) {
            ret2 = txn->commit(txn, 0);
            if (ret2) {
                LOG_ERROR("commit", ret);
            }
            return 0;
        } else if (ret) {
            ret2 = txn->abort(txn);
            if (ret2) {
                LOG_ERROR("abort", ret2);
            }
            return -1;
        } else {
            ret2 = txn->commit(txn, 0);
            if (ret2) {
                LOG_ERROR("commit", ret);
            }
            goto restart;
        }
    }
}

int
expire_thread(void *args) {
    struct expire_ctx ctx;
    int ret;

	ctx.dbenv = ((supthr_args *)args)->dbenv;
	ctx.shared_data = ((supthr_args *)args)->shared;
    ctx.expire_db = NULL;
    ctx.expire_index_db = NULL;
    ctx.dbmap = dbmap_create();

    for (;;) {
        sleep(1);
        if (ctx.shared_data->app_finished == 1) {
            break;
        }
        if (!ctx.shared_data->is_master) {
            continue;
        }
        if (ctx.expire_db == NULL) {
            ctx.expire_db = must_open_db(&ctx, "__expire.db", DB_BTREE);
        }
        if (ctx.expire_index_db == NULL) {
            ctx.expire_index_db = must_open_db(&ctx, "__expire.index.db", DB_HASH);
        }
        if (ctx.shared_data->app_finished == 1) {
            break;
        }
        ret = expire_check(&ctx);
        if (ret) {
            if (ctx.expire_db) {
                ret = ctx.expire_db->close(ctx.expire_db, 0);
                if (ret) {
                    LOG_ERROR("close|expire", ret);
                }
                ctx.expire_db = NULL;
            }
            if (ctx.expire_index_db) {
                ret = ctx.expire_index_db->close(ctx.expire_index_db, 0);
                if (ret) {
                    LOG_ERROR("close|index", ret);
                }
                ctx.expire_index_db = NULL;
            }
        }
    }

    if (ctx.data_buff) {
        free(ctx.data_buff);
    }
    dbmap_destroy(ctx.dbmap);
    if (ctx.expire_db) {
        db_close(ctx.expire_db);
    }
    if (ctx.expire_index_db) {
        db_close(ctx.expire_index_db);
    }
    return EXIT_SUCCESS;
}
