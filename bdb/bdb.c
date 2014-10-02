#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <db.h>
#include "rep_common.h"
#include "uthash.h"

int
db_close(DB *dbp) {
    int ret;
    if ((ret = dbp->close(dbp, 0)) != 0) {
        dbp->err(dbp, ret, "DB->close");
    }
    return 0;
}

int is_finished(SHARED_DATA *shared_data) {
    return shared_data->app_finished;
}

int
db_get(DB *dbp, char *_key, unsigned int keylen, char **_data, unsigned int *datalen) {
    DBT key, data;
    int ret;

    memset(&key, 0, sizeof key);
    memset(&data, 0, sizeof data);

    key.data = _key;
    key.size = keylen;

    data.flags = DB_DBT_REALLOC;
    data.data = *_data;

    ret = dbp->get(dbp, NULL, &key, &data, 0);
    if (ret == 0) {
        *_data = data.data;
        *datalen = data.size;
    } else if (ret == DB_NOTFOUND) {
        *_data = NULL;
        *datalen = 0;
    }
    return ret;
}

int
db_put(DB *dbp, char *_key, unsigned int keylen, char *_data, unsigned int datalen) {
    DBT key, data;
    int ret;

    memset(&key, 0, sizeof key);
    memset(&data, 0, sizeof data);

    key.data = _key;
    key.size = keylen;

    data.data = _data;
    data.size = datalen;

    ret = dbp->put(dbp, NULL, &key, &data, 0);
    return ret;
}

int
get_db(DB_ENV *dbenv, SHARED_DATA *shared_data, const char *name, int dbtype, unsigned int myflags, DB **out) {
    DB *dbp;
    int ret;
	u_int32_t flags;
	permfail_t *pfinfo;

    *out = NULL;
    if ((ret = db_create(&dbp, dbenv, 0)) != 0)
        return (ret);

    flags = DB_AUTO_COMMIT | DB_READ_UNCOMMITTED | DB_THREAD | myflags;
    /*
     * Open database with DB_CREATE only if this is
     * a master database.  A client database uses
     * polling to attempt to open the database without
     * DB_CREATE until it is successful. 
     *
     * This DB_CREATE polling logic can be simplified
     * under some circumstances.  For example, if the
     * application can be sure a database is already
     * there, it would never need to open it with
     * DB_CREATE.
     */
    if (dbtype != DB_UNKNOWN && shared_data->is_master) {
        flags |= DB_CREATE;
    }
    if ((ret = dbp->open(dbp, NULL, name, NULL, DB_HASH, flags, 0)) != 0) {
        dbenv->err(dbenv, ret, "DB->open");
        if ((ret = dbp->close(dbp, 0)) != 0) {
            dbenv->err(dbenv, ret, "DB->close");
        }
        return ret;
    }
    /* Check this thread's PERM_FAILED indicator. */
    *out = dbp;
    return 0;
}

struct expire_key {
    time_t t;
    unsigned int seq;
};

static int
expire_key_compare_fcn(DB *db, const DBT *a, const DBT *b, size_t *locp) {
    struct expire_key ai, bi;
    memcpy(&ai, a->data, sizeof ai);
    memcpy(&bi, b->data, sizeof bi);
    time_t r = ai.t - bi.t;
    return (r > 0) ? 1 : ((r < 0) ? -1 : 0);
}

#define DEFAULT_TABLE "__table"
void
split_key(char *_key, int keylen, char **table, int *tablelen, char **name, int *namelen) {
    int i;

    *name = NULL;
    *table = NULL;
	for (i = 0; i < keylen; ++i) {
		if (_key[i] == ':') {
            if (i != 0) {
                *table = _key;
                *tablelen = i;
            }
            *namelen = keylen - i - 1;
            if (*namelen != 0) {
                *name = _key + i + 1;
            }
			break;
		}
	}
	if (*table == NULL && *name == NULL) {
		*table = DEFAULT_TABLE;
        *tablelen = (sizeof DEFAULT_TABLE) - 1;
		*name = _key;
        *namelen = keylen;
	}
	if (table == NULL) {
		*table = DEFAULT_TABLE;
        *tablelen = (sizeof DEFAULT_TABLE) - 1;
	}
	if (*name == NULL) {
        *name = "\0";
        *namelen = 1;
	}
}

#define EXPIRE_KEY_MAX 100
int
check_expire(DB_ENV *dbenv, SHARED_DATA *shared_data) {
    DBC *cur;
    DBT key, data;
    int ret, i, expire_key_count;
    time_t now;
    struct expire_key keydata;
    char *data_buff, *expire_key[EXPIRE_KEY_MAX];
    DB *expire_db;
    struct {
        char key[128];
        DB *db;
        UT_hash_handle hh;
    } *dbmap, *dbmap_item, *dbmap_tmp;

    dbmap = NULL;

    // 打开expire数据库
    for (;;) {
        if (shared_data->app_finished == 1) {
            return;
        }
        ret = get_db(dbenv, shared_data, "__expire.db", DB_BTREE, 0, &expire_db);
        if (ret != 0) {
            dbenv->err(dbenv, ret, "Could not open expire db.");
            sleep(1);
        }
        break;
    }
    expire_db->set_dup_compare(expire_db, expire_key_compare_fcn);

    // 初始化realloc缓存
    data_buff = NULL;
    for (i = 0; i < EXPIRE_KEY_MAX; ++i) {
        expire_key[i] = NULL;
    }

    for (;;) {
        sleep(1);
        if (shared_data->app_finished == 1) {
            break;
        }
        ret = expire_db->cursor(expire_db, NULL, &cur, DB_READ_UNCOMMITTED);
        if (ret != 0) {
            expire_db->err(expire_db, ret, "Could not open cursor.");
            continue;
        }

        memset(&key, 0, sizeof key);
        memset(&data, 0, sizeof data);
        data.flags = DB_DBT_REALLOC;
        data.data = data_buff;
        key.flags = DB_DBT_USERMEM;
        key.data = &keydata;

        expire_key_count = 0;
        time(&now);
        while ((ret = cur->get(cur, &key, &data, DB_NEXT)) == 0) {
            struct expire_key *k = (struct expire_key *)key.data;
            if (k->t > now) {
                // expire表中的数据是按照时间排序的
                // t 大于now表示后面不会再有超时的数据了
                break;
            }
            expire_key[expire_key_count] = realloc(expire_key[expire_key_count], data.size);
            memcpy(expire_key[expire_key_count], data.data, data.size);
            ++expire_key_count;
            if (expire_key_count >= EXPIRE_KEY_MAX) {
                // 每批处理EXPIRE_KEY_MAX条记录
                // 避免锁表时间太长
                break;
            }
        }
        cur->close(cur);

        for (i = 0; i < expire_key_count; ++i) {
        }
    }

    if (data_buff) {
        free(data_buff);
    }
    for (i = 0; i < EXPIRE_KEY_MAX; ++i) {
        if (expire_key[i]) {
            free(expire_key[i]);
        }
    }
    HASH_ITER(hh, dbmap, dbmap_item, dbmap_tmp) {
        HASH_DEL(dbmap, dbmap_item);
        db_close(dbmap_item->db);
        free(dbmap_item);
    }
    db_close(expire_db);
}
