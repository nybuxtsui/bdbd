#include <errno.h>
#include <string.h>
#include <db.h>
#include "rep_common.h"

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
env_get(DB_ENV *dbenv, SHARED_DATA *shared_data, const char *name, DB **out) {
    DB *dbp;
    int ret;
	u_int32_t flags;
	permfail_t *pfinfo;

    *out = NULL;
    if ((ret = db_create(&dbp, dbenv, 0)) != 0)
        return (ret);

    flags = DB_AUTO_COMMIT | DB_READ_UNCOMMITTED | DB_THREAD;
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
    if (shared_data->is_master)
        flags |= DB_CREATE;
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
