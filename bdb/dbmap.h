#ifndef __DBMAP_H__
#define __DBMAP_H__
#include <bdb.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void * dbmap_t;
dbmap_t dbmap_create();
DB * dbmap_find(dbmap_t dbmap, const char *table);
void dbmap_add(dbmap_t dbmap, const char *table, DB *db);
void dbmap_del(dbmap_t dbmap, const char *table);
void dbmap_destroy(dbmap_t dbmap);

#ifdef __cplusplus
}
#endif

#endif
