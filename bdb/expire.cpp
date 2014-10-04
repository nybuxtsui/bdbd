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

static void
get_expire_id(const char *key) {
    DB *db;
    int ret;

}

static DB *
get_target_expire_db(supthr_args *args, dbmap_t dbmap, const char *table) {
    DB *db;
    int ret;

    char cname[256];
    snprintf(cname, sizeof cname, "__expire.%s.db", table);
    cname[sizeof cname - 1] = 0;
    db = dbmap_find(dbmap, cname);
    if (db == NULL) {
        ret = get_db(args->dbenv, args->shared, cname, DB_HASH, &db);
        if (ret) {
            args->dbenv->err(args->dbenv, ret, "Could not delete cursor.");
            return NULL;
        }
        dbmap_add(dbmap, table, db);
    }
}

static DB *
get_target_db(supthr_args *args, DBC *cur, dbmap_t dbmap, const char *table) {
    DB *db;
    int ret;

    db = dbmap_find(dbmap, table);
    if (db == NULL) { // db记录尚未缓存
        char cname[256];
        snprintf(cname, sizeof cname, "%s.db", table);
        cname[sizeof cname - 1] = 0;
        ret = get_db(args->dbenv, args->shared, cname, DB_UNKNOWN, &db);
        if (ret) {
            args->dbenv->err(args->dbenv, ret, "Could not open db.");
            if (ret == ENOENT) { // 数据库不存在, 则跳过这条记录
                ret = cur->del(cur, 0);
                if (ret != 0) { // 即使删除失败，也暂时先跳过，等下次循环再来删除
                    args->dbenv->err(args->dbenv, ret, "Could not delete cursor.");
                }
            }
            return NULL;
        }
        dbmap_add(dbmap, table, db);
    }
    return db;
}

extern "C" int
expire_thread(void *_args) {
	supthr_args *args;
    DB_ENV *dbenv;
    SHARED_DATA *shared_data;
    DBC *cur;
    DBT key, data, delkey;
    int ret, i, expire_key_count, tablelen, namelen;
    time_t now;
    struct expire_key keydata;
    char *data_buff, *table, *name;
    DB *expire_db, *target_db, *target_expire_db;
    dbmap_t dbmap;

	args = (supthr_args *)_args;
	dbenv = args->dbenv;
	shared_data = args->shared;

    expire_db = open_expire_db(args);
    dbmap = dbmap_create();
    data_buff = NULL;
    cur = NULL;

    for (;;) {
        if (cur) {
            cur->close(cur);
            cur = NULL;
        }

        sleep(1);
        if (shared_data->app_finished == 1) {
            break;
        }
        if (!shared_data->is_master) {
            continue;
        }

        ret = expire_db->cursor(expire_db, NULL, &cur, DB_READ_UNCOMMITTED);
        if (ret != 0) {
            expire_db->err(expire_db, ret, "Could not open cursor.");
            continue;
        }

        time(&now);

        memset(&key, 0, sizeof key);
        memset(&data, 0, sizeof data);
        data.flags = DB_DBT_REALLOC;
        data.data = data_buff;
        key.flags = DB_DBT_USERMEM;
        key.data = &keydata;

        for (;;) {
            ret = cur->get(cur, &key, &data, DB_NEXT);
            if (ret) {
                if (ret != DB_NOTFOUND) { // expire库不为空
                    expire_db->err(expire_db, ret, "Could not get cursor.");
                }
                break;
            }
            struct expire_key *k = (struct expire_key *)key.data;
            if (k->t > now) { // 记录尚未超时
                break;
            }
            split_key((char*)data.data, data.size, &table, &tablelen, &name, &namelen);
            target_db = get_target_db(args, cur, dbmap, table);
            if (target_db == NULL) {
                break;
            }
            target_expire_db = get_target_expire_db(args, dbmap, table);
            if (target_expire_db) {
            }

            memset(&delkey, 0, sizeof delkey);
            delkey.data = name;
            delkey.size = namelen;

            ret = target_db->del(target_db, NULL, &delkey, 0);
            if (ret == DB_NOTFOUND) {
            } else if (ret) {
                if (ret == DB_REP_HANDLE_DEAD) {
                    dbmap_del(dbmap, table);
                }
                break;
            }
        }
    }

    if (data_buff) {
        free(data_buff);
    }
    dbmap_destroy(dbmap);
    db_close(expire_db);

    return EXIT_SUCCESS;
}

