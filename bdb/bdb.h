#include <db.h>
#include "rep_common.h"

int start_base(int argc, char *argv[], void *ptr);
int start_mgr(int argc, char *argv[], void *ptr);

int db_get(DB *dbp, char *_key, unsigned int keylen, char **_data, unsigned int *datalen);
int db_put(DB *dbp, char *_key, unsigned int keylen, char *_data, unsigned int datalen);
int db_close(DB *dbp);

int env_get(DB_ENV *dbenv, SHARED_DATA *shared_data, const char *name, DB **out);
int is_finished(SHARED_DATA *shared_data);
