#include <bdb.h>

typedef void * dbmap_t;
dbmap_t dbmap_create();
DB * dbmap_find(dbmap_t dbmap, const char *table);
void dbmap_add(dbmap_t dbmap, const char *table, DB *db);
void dbmap_del(dbmap_t dbmap, const char *table);
void dbmap_destroy(dbmap_t dbmap);
