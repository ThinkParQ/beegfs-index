#include <sqlite3.h>

#include "bf.h"
#include "beegfs_plugin_shared.h"

static int global_init(void *global) {
    (void) global;

    if (sqlite3_initialize() != SQLITE_OK) {
        return 1;
    }

    return 0;
}

static void *ctx_init(void *ptr) {
    sqlite3 *db = (sqlite3 *) ptr;
    if (!db) {
        return NULL;
    }

    beegfs_create_query_views(db);

    return NULL;
}

struct plugin_operations beegfs_query_ops = {
    .type = PLUGIN_QUERY,
    .global_init = global_init,
    .ctx_init = ctx_init,
    .process_dir = NULL,
    .process_file = NULL,
    .ctx_exit = NULL,
    .global_exit = NULL,
};
