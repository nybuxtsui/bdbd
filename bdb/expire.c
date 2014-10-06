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
    DBC *cur;
    char *data_buff;
    DB *expire_db, *expire_index_db;
    dbmap_t dbmap;
};

static DB *
open_expire_db(struct expire_ctx *ctx) {
    DB *db;
    int ret;
    for (;;) {
        if (ctx->shared_data->app_finished == 1) {
            return NULL;
        }
        ret = get_db(ctx->dbenv, ctx->shared_data, "__expire.db", DB_BTREE, &db);
        if (ret == 0) {
            return db;
        }
        ctx->dbenv->err(ctx->dbenv, ret, "Could not open expire db.");
        sleep(1);
    }
}

static DB *
open_expire_index_db(struct expire_ctx *ctx) {
    DB *db;
    int ret;
    for (;;) {
        if (ctx->shared_data->app_finished == 1) {
            return NULL;
        }
        ret = get_db(ctx->dbenv, ctx->shared_data, "__expire.index.db", DB_HASH, &db);
        if (ret == 0) {
            return db;
        }
        ctx->dbenv->err(ctx->dbenv, ret, "Could not open expire index db.");
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
    int ret;
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
        ctx->dbenv->err(ctx->dbenv, ret, "txn_begin");
        return -1;
    }
    ret = ctx->expire_index_db->get(ctx->expire_index_db, txn, data, &indexdata, DB_RMW);
    if (ret == DB_NOTFOUND) {
        Debug("expire_check_one|index_not_found");
        txn->commit(txn, 0);
        return 0;
    }
    if (ret) {
        txn->abort(txn);
        return ret;
    }
    if (memcmp(&_indexdata, key->data, sizeof _indexdata) != 0) {
        Debug("expire_check_one|index_change");
        txn->commit(txn, 0);
        return 0;
    }

    ret = ctx->expire_index_db->del(ctx->expire_index_db, txn, data, 0);
    split_key(data->data, data->size, &table, &tablelen, &name, &namelen);
    ret = get_target_db(ctx, table, &target_db);
    if (ret) {
        txn->abort(txn);
        if (ret == DB_REP_HANDLE_DEAD) {
            dbmap_del(ctx->dbmap, table);
        }
        return ret;
    }
    memset(&delkey, 0, sizeof delkey);
    delkey.data = name;
    delkey.size = namelen;
    ret = target_db->del(target_db, txn, &delkey, 0);
    if (ret == 0 || ret == DB_NOTFOUND) {
        txn->commit(txn, 0);
    } else {
        txn->abort(txn);
    }
    return ret == DB_NOTFOUND ? 0 : ret;
}

static int
expire_check(struct expire_ctx *ctx) {
    DBT key, data;
    DB_TXN *txn;
    struct expire_key keydata;
    int ret, count;
    time_t now;

    ret = ctx->dbenv->txn_begin(ctx->dbenv, NULL, &txn, DB_READ_UNCOMMITTED);
    if (ret) {
        ctx->dbenv->err(ctx->dbenv, ret, "txn_begin");
        return -1;
    }
    ret = ctx->expire_db->cursor(ctx->expire_db, txn, &ctx->cur, DB_READ_UNCOMMITTED);
    if (ret) {
        txn->abort(txn);
        ctx->expire_db->err(ctx->expire_db, ret, "cursor open failed");
        return -1;
    }

    memset(&key, 0, sizeof key);
    memset(&data, 0, sizeof data);
    memset(&keydata, 0, sizeof keydata);

    key.flags = DB_DBT_USERMEM;
    key.ulen = sizeof keydata;
    key.data = &keydata;
    data.flags = DB_DBT_REALLOC;
    data.data = ctx->data_buff;
    count = 0;

    time(&now);
    for (;;) {
        ++count;
        if (count >= 1000 || ctx->shared_data->app_finished == 1) {
            ctx->cur->close(ctx->cur);
            ctx->cur = NULL;
            txn->commit(txn, 0);
            return -1;
        }
        ret = ctx->cur->get(ctx->cur, &key, &data, DB_NEXT);
        if (ret) {
            if (ret != DB_NOTFOUND) { // expire库不为空
                ctx->expire_db->err(ctx->expire_db, ret, "cursor get failed");
            }
            ctx->cur->close(ctx->cur);
            ctx->cur = NULL;
            txn->commit(txn, 0);
            return 0;
        }
        if (keydata.t > now) { // 记录尚未超时
            ctx->cur->close(ctx->cur);
            ctx->cur = NULL;
            txn->commit(txn, 0);
            return 0;
        }

        char buf[1024];
        snprintf(buf,
                sizeof buf,
                "expire_check|%u|%d|%d|%.*s",
                (unsigned int)keydata.t,
                keydata.seq,
                keydata.thread_id,
                data.size,
                (char *)data.data);
        buf[sizeof buf - 1] = 0;
        Debug(buf);

        ret = expire_check_one(ctx, txn, &key, &data);
        if (ret != 0) {
            ctx->cur->close(ctx->cur);
            ctx->cur = NULL;
            txn->abort(txn);
            ctx->dbenv->err(ctx->dbenv, ret, "expire_check_one failed.");
            return -1;
        }
        ret = ctx->cur->del(ctx->cur, 0);
        if (ret != 0) {
            ctx->dbenv->err(ctx->dbenv, ret, "cur->del failed.");
        }
    }
}

int
expire_thread(void *args) {
    struct expire_ctx ctx;
    int ret;

	ctx.dbenv = ((supthr_args *)args)->dbenv;
	ctx.shared_data = ((supthr_args *)args)->shared;
    ctx.cur = NULL;
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
        if (ctx.cur) {
            ctx.cur->close(ctx.cur);
            ctx.cur = NULL;
        }
        if (ctx.expire_db == NULL) {
            ctx.expire_db = open_expire_db(&ctx);
        }
        if (ctx.expire_index_db == NULL) {
            ctx.expire_index_db = open_expire_index_db(&ctx);
        }
        if (ctx.shared_data->app_finished == 1) {
            break;
        }

        ret = expire_check(&ctx);
        if (ret) {
            if (ctx.cur) {
                ctx.cur->close(ctx.cur);
                ctx.cur = NULL;
            }
            if (ctx.expire_db) {
                ctx.expire_db->close(ctx.expire_db, 0);
                ctx.expire_db = NULL;
            }
            if (ctx.expire_index_db) {
                ctx.expire_index_db->close(ctx.expire_index_db, 0);
                ctx.expire_index_db = NULL;
            }
        }
    }

    if (ctx.data_buff) {
        free(ctx.data_buff);
    }
    dbmap_destroy(ctx.dbmap);
    if (ctx.cur) {
        ctx.cur->close(ctx.cur);
    }
    if (ctx.expire_db) {
        db_close(ctx.expire_db);
    }
    if (ctx.expire_index_db) {
        db_close(ctx.expire_index_db);
    }
    return EXIT_SUCCESS;
}
