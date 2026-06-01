#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

#include "bf.h"
#include "beegfs_plugin_shared.h"

struct beegfs_index_state {
    sqlite3_stmt *entries_stmt;
    sqlite3_stmt *targets_stmt;
    sqlite3_stmt *rst_stmt;
    int dir_fd;
};

static struct beegfs_index_state *beegfs_index_state_create(sqlite3 *db) {
    struct beegfs_index_state *state = calloc(1, sizeof(*state));
    if (!state) {
        return NULL;
    }

    state->dir_fd = -1;

    if (beegfs_plugin_create_tables(db) != 0) {
        free(state);
        return NULL;
    }

    if (beegfs_plugin_prepare_index_statements(db, &state->entries_stmt,
                                               &state->targets_stmt, &state->rst_stmt) != 0) {
        free(state);
        return NULL;
    }

    return state;
}

static void beegfs_index_state_destroy(struct beegfs_index_state *state) {
    if (!state) {
        return;
    }

    if (state->dir_fd >= 0) {
        close(state->dir_fd);
    }

    beegfs_plugin_finalize_index_statements(state->entries_stmt, state->targets_stmt,
                                            state->rst_stmt);
    free(state);
}

static int global_init(void *global) {
    (void) global;
    return sqlite3_initialize() == SQLITE_OK ? 0 : 1;
}

static void *ctx_init(void *ptr) {
    PCS_t *pcs = (PCS_t *) ptr;
    if (!pcs || !pcs->db || !pcs->work) {
        return NULL;
    }

    struct beegfs_index_state *state = beegfs_index_state_create(pcs->db);
    if (state) {
        state->dir_fd = open(pcs->work->name, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (state->dir_fd < 0) {
            beegfs_index_state_destroy(state);
            state = NULL;
        }
    }

    return state;
}

static void process_entry(void *ptr, void *user_data) {
    PCS_t *pcs = (PCS_t *) ptr;
    struct beegfs_index_state *state = (struct beegfs_index_state *) user_data;

    if (!pcs || !state) {
        return;
    }

    if (!pcs->ed || (pcs->ed->type != 'f')) {
        return;
    }

    struct beegfs_entry_metadata metadata;

    if (beegfs_collect_metadata(state->dir_fd, pcs, &metadata) != 0) {
        return;
    }

    int64_t entry_rowid = 0;
    if (beegfs_plugin_insert_metadata(state->entries_stmt, &metadata, &entry_rowid) != 0) {
        return;
    }

    if (metadata.got_stripe_info && (metadata.num_targets > 0)) {
        beegfs_plugin_insert_targets(&metadata, state->targets_stmt, entry_rowid);
    }

    if (metadata.got_stripe_info && (metadata.num_rst_ids > 0)) {
        beegfs_plugin_insert_rst_ids(&metadata, state->rst_stmt, entry_rowid);
    }
}

static void ctx_exit(void *ptr, void *user_data) {
    (void) ptr;
    beegfs_index_state_destroy((struct beegfs_index_state *) user_data);
}

static void global_exit(void *global) {
    (void) global;
    sqlite3_shutdown();
}

struct plugin_operations beegfs_index_ops = {
    .type         = PLUGIN_INDEX,
    .global_init  = global_init,
    .ctx_init     = ctx_init,
    .process_dir  = NULL,
    .process_file = process_entry,
    .ctx_exit     = ctx_exit,
    .global_exit  = global_exit,
};
