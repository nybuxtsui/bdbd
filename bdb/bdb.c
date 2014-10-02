#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <db.h>
#include "rep_common.h"
#include "dbmap.h"

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
get_db(DB_ENV *dbenv,
       SHARED_DATA *shared_data,
       const char *name,
       int dbtype,
       unsigned int myflags,
       void *compare_fcn,
       DB **out) {
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
    if (compare_fcn) {
        dbp->set_dup_compare(dbp, compare_fcn);
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

#define DEFAULT_TABLE "__default"
void
split_key(char *_key, int keylen, char **table, int *tablelen, char **name, int *namelen) {
    int i;

    *name = NULL;
    *table = NULL;
	for (i = 0; i < keylen; ++i) {
		if (_key[i] == ':') {
            if (i != 0) {
                *table = _key;
                _key[i] = '\0';
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

int
expire_thread(void *args) {
	supthr_args *la;
    DB_ENV *dbenv;
    SHARED_DATA *shared_data;
    DBC *cur;
    DBT key, data, delkey;
    int ret, i, expire_key_count, tablelen, namelen;
    time_t now;
    struct expire_key keydata;
    char *data_buff, *table, *name;
    DB *expire_db, *db;
    struct dbmap *dbmap;

	la = (supthr_args *)args;
	dbenv = la->dbenv;
	shared_data = la->shared;

    dbmap = NULL;

    // 打开expire数据库
    for (;;) {
        if (shared_data->app_finished == 1) {
            return EXIT_SUCCESS;
        }
        if (shared_data->is_master == 0) {
            sleep(1);
            continue;
        }
        ret = get_db(dbenv, shared_data, "__expire.db", DB_BTREE, 0, expire_key_compare_fcn, &expire_db);
        if (ret != 0) {
            dbenv->err(dbenv, ret, "Could not open expire db.");
            sleep(1);
            continue;
        }
        break;
    }


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
            if (ret == DB_NOTFOUND) {
                break;
            }
            if (ret != 0) {
                expire_db->err(expire_db, ret, "Could not get cursor.");
                break;
            }
            struct expire_key *k = (struct expire_key *)key.data;
            if (k->t > now) {
                break;
            }
            split_key(data.data, data.size, &table, &tablelen, &name, &namelen);
            db = dbmap_find(dbmap, table);
            if (db == NULL) {
                char cname[128];
                snprintf(cname, sizeof cname, "%s.db", name);
                cname[sizeof cname - 1] = 0;
                ret = get_db(dbenv, shared_data, cname, DB_UNKNOWN, 0, NULL, &db);
                if (ret != 0) {
                    dbenv->err(dbenv, ret, "Could not get db: %s:%.*s", cname, namelen, name);
                    ret = cur->del(cur, 0);
                    if (ret != 0) {
                        dbenv->err(dbenv, ret, "Could not delete cursor.");
                    }
                    break;
                }
                dbmap_add(dbmap, table, db);
            }

            memset(&delkey, 0, sizeof delkey);
            delkey.data = name;
            delkey.size = namelen;

            ret = db->del(db, NULL, &delkey, 0);
            if (ret) {
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
