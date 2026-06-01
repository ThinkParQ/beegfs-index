#ifndef BEEGFS_PLUGIN_SHARED_H
#define BEEGFS_PLUGIN_SHARED_H

#include <stdint.h>

#include <sqlite3.h>

#include "plugin.h"

#define BEEGFS_PLUGIN_ENTRIES_TABLE "beegfs_entries"
#define BEEGFS_PLUGIN_TARGETS_TABLE "beegfs_stripe_targets"
#define BEEGFS_PLUGIN_RST_TABLE "beegfs_rst_targets"
#define BEEGFS_PLUGIN_FILE_VIEW "beegfs_file_view"
#define BEEGFS_PLUGIN_FILE_TARGETS_VIEW "beegfs_file_targets_view"

/* Must match BEEGFS_IOCTL_MAX_STRIPE_TARGETS in bh_beegfs_ioctl.h */
#define BEEGFS_PLUGIN_MAX_STRIPE_TARGETS 256

struct beegfs_entry_metadata {
    const char *name;
    char type;
    uint64_t inode;

    uint32_t owner_id;
    char parent_entry_id[27];
    char entry_id[27];
    int entry_type;
    int feature_flags;

    uint32_t pattern_type;
    uint32_t chunk_size;
    uint32_t storage_pool_id;
    uint32_t default_num_targets;
    uint16_t num_targets;
    uint16_t stripe_target_ids[BEEGFS_PLUGIN_MAX_STRIPE_TARGETS];

    /* PathInfo */
    uint32_t path_info_flags;
    uint32_t orig_parent_uid;
    char orig_parent_entry_id[27];

    /* File data state (online/offline/tiered) */
    uint8_t  file_data_state;

    /* Remote Storage Target (RST) */
    uint8_t  rst_major_version;
    uint8_t  rst_minor_version;
    uint16_t rst_cool_down_period;
    uint16_t rst_file_policies;
    uint32_t num_rst_ids;
    uint32_t rst_ids[BEEGFS_PLUGIN_MAX_STRIPE_TARGETS];

    int got_info;        /* basic fields (ownerID/entryID/type/flags) are valid */
    int got_stripe_info; /* stripe/PathInfo/RST/session fields are valid */
};

int beegfs_collect_metadata(int dirfd, const PCS_t *pcs, struct beegfs_entry_metadata *metadata);

int beegfs_plugin_create_tables(sqlite3 *db);
int beegfs_plugin_prepare_index_statements(sqlite3 *db,
                                           sqlite3_stmt **entries_stmt,
                                           sqlite3_stmt **targets_stmt,
                                           sqlite3_stmt **rst_stmt);
void beegfs_plugin_finalize_index_statements(sqlite3_stmt *entries_stmt,
                                             sqlite3_stmt *targets_stmt,
                                             sqlite3_stmt *rst_stmt);
int beegfs_plugin_insert_metadata(sqlite3_stmt *entries_stmt,
                                  const struct beegfs_entry_metadata *metadata,
                                  int64_t *entry_rowid);
int beegfs_plugin_insert_targets(const struct beegfs_entry_metadata *metadata,
                                 sqlite3_stmt *targets_stmt,
                                 int64_t entry_rowid);
int beegfs_plugin_insert_rst_ids(const struct beegfs_entry_metadata *metadata,
                                 sqlite3_stmt *rst_stmt,
                                 int64_t entry_rowid);

int beegfs_create_query_views(sqlite3 *db);

#endif
