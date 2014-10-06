#include <string.h>
#include <stdlib.h>
#include <db.h>
#include <errno.h>
#include "rep_common.h"
#include "bdb.h"
#include "dbmap.h"

static DB *
open_expire_db(supthr_args *args) {
    DB *db;
    int ret;
    for (;;) {
        if (args->shared->app_finished == 1) {
            return NULL;
        }
        if (args->shared->is_master == 0) {
            sleep(1);
            continue;
        }
        ret = get_db(args->dbenv, args->shared, "__expire.db", DB_BTREE, &db);
        if (ret != 0) {
            args->dbenv->err(args->dbenv, ret, "Could not open expire db.");
            sleep(1);
            continue;
        }
        return db;
    }
}

static DB *
open_expire_index_db(supthr_args *args) {
    DB *db;
    if ret;
    for (;;) {
        if (args->shared->app_finished == 1) {
            return NULL;
        }
        if (args->shared->is_master == 0) {
            sleep(1);
            continue;
        }
        ret = get_db(args->dbenv, args->shared, "__expire.index.db", DB_HASH, &db);
        if (ret != 0) {
            args->dbenv->err(args->dbenv, ret, "Could not open expire index db.");
            sleep(1);
            continue;
        }
        return db;
    }
}

static void
get_expire_id(const char *key) {
    DB *db;
    int ret;

}

static int
get_target_db(supthr_args *args, dbmap_t dbmap, const char *table, DB **db) {
    int ret;

    *db = dbmap_find(dbmap, table);
    if (*db == NULL) { // db记录尚未缓存
        char cname[256];
        snprintf(cname, sizeof cname, "%s.db", table);
        cname[sizeof cname - 1] = 0;
        ret = get_db(args->dbenv, args->shared, cname, DB_UNKNOWN, db);
        if (ret) {
            args->dbenv->err(args->dbenv, ret, "Could not open db.");
            return ret;
        }
        dbmap_add(dbmap, table, *db);
    }
    return ret;
}

struct expire_ctx {
    DB_ENV *dbenv;
    SHARED_DATA *shared_data;
    DBC *cur;
    char *data_buff;
    DB *expire_db, *expire_index_db;
    dbmap_t dbmap;
};

static int
expire_check_one(expire_ctx *ctx, DBT *key, DBT *value) {
    struct expire_key _indexdata;
    int ret;
    DBT indexdata, delkey;
    DB *target_db;

    memset(&indexdata, 0, sizeof indexdata);
    indexdata.flags = DB_DBT_USERMEM;
    indexdata.data = &_indexdata;
    data->flags = 0;
    ret = expire_index_db->get(expire_index_db, NULL, &data, &indexdata);
    if (ret) {
        return ret;
    }
    if (memcmp(&_indexdata, key->data, sizeof _indexdata) != 0) {
        return 0;
    }
    ret = ctx->expire_index_db->del(ctx->expire_index_db, NULL, &data, 0);
    if (ret && ret != DB_NOTFOUND) {
        return ret;
    }
    split_key((char*)data.data, data.size, &table, &tablelen, &name, &namelen);
    ret = get_target_db(args, dbmap, table, &target_db);
    if (ret) {
        if (ret == DB_REP_HANDLE_DEAD) {
            dbmap_del(ctx->dbmap, table);
        }
        return ret;
    }
    memset(&delkey, 0, sizeof delkey);
    delkey.data = name;
    delkey.size = namelen;
    ret = target_db->del(target_db, NULL, &delkey, 0);
    return ret == DB_NOTFOUND ? 0 : ret;
}

static int
expire_check(expire_ctx *ctx) {
    DBT key, data;
    struct expire_key keydata;
    int ret;
    time_t now;

    memset(&key, 0, sizeof key);
    memset(&data, 0, sizeof data);
    data.flags = DB_DBT_REALLOC;
    data.data = ctx->data_buff;
    key.flags = DB_DBT_USERMEM;
    key.data = &keydata;

    ret = ctx->expire_db->cursor(expire_db, NULL, &cur, DB_READ_UNCOMMITTED);
    if (ret) {
        ctx->expire_db->err(ctx->expire_db, ret, "cursor open failed");
        return -1
    }
    time(&now);
    for (;;) {
        if (ctx->shared_data->app_finished == 1) {
            return -1;
        }
        ret = ctx->cur->get(ctx->cur, &key, &data, DB_NEXT);
        if (ret) {
            if (ret != DB_NOTFOUND) { // expire库不为空
                expire_db->err(expire_db, ret, "cursor get failed");
            }
            return 0;
        }
        if ((struct expire_key *)->t > now) { // 记录尚未超时
            return 0;
        }
        ret = expire_check_one(ctx);
        if (ret != 0) {
            ctx->dbenv->(ctx->dbenv, ret, "expire_check_one failed.");
            return -1;
        }
    }
}

int
expire_thread(void *_args) {
	supthr_args *args;

    struct expire_ctx ctx;

    DB_ENV *dbenv;
    SHARED_DATA *shared_data;
    DBC *cur;
    DBT key, data, delkey;
    int ret, i, expire_key_count, tablelen, namelen;
    time_t now;
    struct expire_key keydata;
    char *data_buff, *table, *name;
    DB *expire_db, *expire_index_db, *target_db;
    dbmap_t dbmap;

	args = (supthr_args *)_args;
	ctx.dbenv = args->dbenv;
	ctx.shared_data = args->shared;
    ctx.cur = NULL;
    ctx.expire_db = NULL;
    ctx.expire_index_db = NULL;
    ctx.dbmap = dbmap_create();

    for (;;) {
        if (ctx.cur) {
            ctx.cur->close(cur);
            ctx.cur = NULL;
        }
        if (ctx.expire_db == NULL) {
            ctx.expire_db = open_expire_db(args);
        }
        if (shared_data->app_finished == 1) {
            break;
        }
        if (ctx.expire_index_db == NULL) {
            ctx.expire_index_db = open_expire_index_db(args);
        }
        if (shared_data->app_finished == 1) {
            break;
        }
        sleep(1);
        if (!shared_data->is_master) {
            continue;
        }

        ret = expire_check(&ctx);
        if (ret) {
            expire_db->close(expire_db, 0);
            expire_db = NULL;
            expire_index_db->close(expire_db, 0);
            expire_index_db = NULL;
        }
    }

    if (ctx.data_buff) {
        free(ctx.data_buff);
    }
    dbmap_destroy(ctx.dbmap);
    db_close(ctx.expire_db);
    db_close(ctx.expire_index_db);

    return EXIT_SUCCESS;
}

