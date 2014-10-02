#include "dbmap.h"
#include <unordered_map>
#include <string>

typedef std::unordered_map<std::string, DB*> _dbmap;

dbmap_t
dbmap_create() {
    dbmap_t r = dbmap_t(new _dbmap());
    return r;
}

DB * dbmap_find(dbmap_t dbmap, const char *table) {
    auto it = static_cast<_dbmap*>(dbmap)->find(table);
    return (it == static_cast<_dbmap*>(dbmap)->end()) ? NULL : it->second;
}

void dbmap_add(dbmap_t dbmap, const char *table, DB *db) {
    auto r = static_cast<_dbmap*>(dbmap)->insert(_dbmap::value_type(table, db));
    if (!r.second) {
        db_close(r.first->second);
        r.first->second = db;
    }
}

void dbmap_del(dbmap_t dbmap, const char *table) {
    auto it = static_cast<_dbmap*>(dbmap)->find(table);
    if (it != static_cast<_dbmap*>(dbmap)->end()) {
        db_close(it->second);
        static_cast<_dbmap*>(dbmap)->erase(it);
    }
}

void dbmap_destroy(dbmap_t dbmap) {
    for (auto &it : *static_cast<_dbmap*>(dbmap)) {
        db_close(it.second);
    }
    static_cast<_dbmap*>(dbmap)->clear();
    delete(static_cast<_dbmap*>(dbmap));
}
