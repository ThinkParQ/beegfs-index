#include "beegfs_shared.h"

#include <fcntl.h>
#include <inttypes.h>
#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bf.h"
#include "bh_beegfs_ioctl.h"

/* Clamp an untrusted ioctl count to its destination array capacity. The insert
 * loops are bounded by the stored count, so storing an unclamped value would let
 * a malformed count drive an out-of-bounds read of the fixed-size ID arrays. */
#define BEEGFS_CLAMP_COUNT(count, cap) ((count) < (cap) ? (count) : (cap))

int beegfs_collect_metadata(int dirfd, const PCS_t *pcs, struct beegfs_entry_metadata *metadata) {
    if (dirfd < 0 || !pcs || !pcs->work || !metadata) {
        return -1;
    }

    if (!pcs->work->name || (pcs->work->name_len == 0) ||
        (pcs->work->name_len >= PATH_MAX)) {
        return -1;
    }

    memset(metadata, 0, sizeof(*metadata));

    char scratch[PATH_MAX];
    memcpy(scratch, pcs->work->name, pcs->work->name_len + 1);

    const char *bn = basename(scratch);
    if (!bn || !bn[0] || (strlen(bn) >= sizeof(metadata->name))) {
        return -1;
    }
    strncpy(metadata->name, bn, sizeof(metadata->name) - 1);

    metadata->type = (pcs->ed && pcs->ed->type) ? pcs->ed->type : '?';
    metadata->inode = (uint64_t) pcs->work->statuso.st_ino;

    struct BeegfsIoctl_GetEntryInfoV2_Arg arg;
    if (!beegfs_getEntryInfoV2(dirfd, metadata->name, &arg)) {
        return -1;
    }
    switch (arg.getEntryInfoResult) {
        case BEEGFS_OPS_ERR_SUCCESS:
        case BEEGFS_OPS_ERR_INTERRUPTED:
        case BEEGFS_OPS_ERR_COMMUNICATION:
        case BEEGFS_OPS_ERR_COMMTIMEDOUT:
        case BEEGFS_OPS_ERR_UNKNOWNNODE:
        case BEEGFS_OPS_ERR_DYNAMICATTRIBSOUTDATED:
        case BEEGFS_OPS_ERR_WOULDBLOCK:
        case BEEGFS_OPS_ERR_AGAIN:
        case BEEGFS_OPS_ERR_STORAGE_SRV_CRASHED:
        case BEEGFS_OPS_ERR_OUTOFMEM:
        case BEEGFS_OPS_ERR_METAVERSIONMISMATCH:
        case BEEGFS_OPS_ERR_INODELOCKED:
            break;
        default:
            fprintf(stderr, "beegfs plugin: %s: getEntryInfo failed (error %d)\n",
                    metadata->name, (int) arg.getEntryInfoResult);
            return -1;
    }

    if (arg.getEntryInfoResult == BEEGFS_OPS_ERR_SUCCESS && arg.entryID[0] == '\0') {
        fprintf(stderr, "beegfs plugin: %s: getEntryInfo succeeded but returned an empty entry ID\n",
                metadata->name);
        return -1;
    }

    if (arg.entryID[0] != '\0') {
        metadata->got_info        = 1;
        metadata->owner_id        = arg.ownerID;
        metadata->entry_type      = arg.entryType;
        metadata->feature_flags   = arg.featureFlags;
        /* Cap at sizeof - 1; the last byte stays '\0' from the memset above, so
         * the copies are NUL-terminated even if a source string were not. */
        strncpy(metadata->parent_entry_id, arg.parentEntryID, sizeof(metadata->parent_entry_id) - 1);
        strncpy(metadata->entry_id,        arg.entryID,       sizeof(metadata->entry_id) - 1);
    }


    if (!metadata->got_info || arg.getEntryInfoResult != BEEGFS_OPS_ERR_SUCCESS) {
        return 0;
    }

    metadata->got_stripe_info     = 1;
    metadata->pattern_type        = arg.patternType;
    metadata->chunk_size          = arg.chunkSize;
    metadata->storage_pool_id     = arg.storagePoolId;
    metadata->default_num_targets = arg.defaultNumTargets;

    uint16_t n = BEEGFS_CLAMP_COUNT(arg.numTargets, BEEGFS_PLUGIN_MAX_STRIPE_TARGETS);
    metadata->num_targets = n;
    memcpy(metadata->stripe_target_ids, arg.stripeTargetIDs, n * sizeof(uint16_t));

    metadata->path_info_flags      = arg.pathInfoFlags;
    metadata->orig_parent_uid      = arg.origParentUID;
    strncpy(metadata->orig_parent_entry_id, arg.origParentEntryID, sizeof(metadata->orig_parent_entry_id) - 1);

    metadata->file_data_state      = arg.fileDataState;
    metadata->rst_major_version    = arg.rstMajorVersion;
    metadata->rst_minor_version    = arg.rstMinorVersion;
    metadata->rst_cool_down_period = arg.rstCoolDownPeriod;
    metadata->rst_file_policies    = arg.rstFilePolicies;

    uint32_t nr = BEEGFS_CLAMP_COUNT(arg.numRSTIds, BEEGFS_PLUGIN_MAX_RST_IDS);
    metadata->num_rst_ids = nr;
    memcpy(metadata->rst_ids, arg.rstIds, nr * sizeof(uint32_t));

    return 0;
}

int beegfs_plugin_create_tables(sqlite3 *db) {
    static const char SQL[] =
        "CREATE TABLE IF NOT EXISTS " BEEGFS_PLUGIN_ENTRIES_TABLE " ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, type TEXT NOT NULL, inode TEXT NOT NULL, "
        "owner_id INTEGER, parent_entry_id TEXT, entry_id TEXT, entry_type INTEGER, feature_flags INTEGER, "
        "stripe_pattern_type INTEGER, stripe_chunk_size INTEGER, stripe_num_targets INTEGER, "
        "stripe_default_num_targets INTEGER, storage_pool_id INTEGER, path_info_flags INTEGER, "
        "orig_parent_uid INTEGER, orig_parent_entry_id TEXT, file_data_state INTEGER, "
        "rst_major_version INTEGER, rst_minor_version INTEGER, rst_cool_down_period INTEGER, "
        "rst_file_policies INTEGER, num_rst_ids INTEGER);"
        "CREATE INDEX IF NOT EXISTS beegfs_entries_inode_idx ON " BEEGFS_PLUGIN_ENTRIES_TABLE "(inode);"
        "CREATE INDEX IF NOT EXISTS beegfs_entries_entry_id_idx ON " BEEGFS_PLUGIN_ENTRIES_TABLE "(entry_id);"
        "CREATE TABLE IF NOT EXISTS " BEEGFS_PLUGIN_TARGETS_TABLE " ("
        "entry_rowid INTEGER NOT NULL, target_index INTEGER NOT NULL, target_or_group INTEGER NOT NULL, "
        "PRIMARY KEY (entry_rowid, target_index));"
        "CREATE TABLE IF NOT EXISTS " BEEGFS_PLUGIN_RST_TABLE " ("
        "entry_rowid INTEGER NOT NULL, rst_index INTEGER NOT NULL, rst_id INTEGER NOT NULL, "
        "PRIMARY KEY (entry_rowid, rst_index));"
        "CREATE VIEW IF NOT EXISTS " BEEGFS_PLUGIN_FILE_VIEW " AS SELECT "
        "e.id AS beegfs_rowid, e.name, e.type, e.inode, e.owner_id, e.parent_entry_id, e.entry_id, "
        "e.entry_type, e.feature_flags, e.stripe_pattern_type, "
        "CASE e.stripe_pattern_type WHEN 1 THEN 'RAID0' WHEN 2 THEN 'RAID10' WHEN 3 THEN 'BUDDYMIRROR' "
        "WHEN 0 THEN 'INVALID' ELSE 'UNKNOWN' END AS stripe_pattern_name, "
        "e.stripe_chunk_size, e.stripe_num_targets, e.stripe_default_num_targets, e.storage_pool_id, "
        "e.path_info_flags, e.orig_parent_uid, e.orig_parent_entry_id, e.file_data_state, "
        "e.rst_major_version, e.rst_minor_version, e.rst_cool_down_period, e.rst_file_policies, e.num_rst_ids "
        "FROM " BEEGFS_PLUGIN_ENTRIES_TABLE " AS e WHERE e.type == 'f';"
        "CREATE VIEW IF NOT EXISTS " BEEGFS_PLUGIN_FILE_TARGETS_VIEW " AS SELECT "
        "e.id AS beegfs_rowid, e.name, e.inode, t.target_index, t.target_or_group "
        "FROM " BEEGFS_PLUGIN_ENTRIES_TABLE " AS e JOIN " BEEGFS_PLUGIN_TARGETS_TABLE " AS t "
        "ON t.entry_rowid == e.id WHERE e.type == 'f';";

    char *err = NULL;
    if (sqlite3_exec(db, SQL, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "beegfs plugin: failed to create tables: %s\n", err ? err : "(unknown)");
        sqlite3_free(err);
        return -1;
    }

    return 0;
}

int beegfs_plugin_prepare_index_statements(sqlite3 *db,
                                           sqlite3_stmt **entries_stmt,
                                           sqlite3_stmt **targets_stmt,
                                           sqlite3_stmt **rst_stmt) {
    if (!db || !entries_stmt || !targets_stmt || !rst_stmt) {
        return -1;
    }

    *entries_stmt = NULL;
    *targets_stmt = NULL;
    *rst_stmt     = NULL;

    static const char INSERT_ENTRIES[] =
        "INSERT INTO " BEEGFS_PLUGIN_ENTRIES_TABLE " ("
        "name, type, inode, owner_id, parent_entry_id, entry_id, entry_type, feature_flags, "
        "stripe_pattern_type, stripe_chunk_size, stripe_num_targets, stripe_default_num_targets, "
        "storage_pool_id, path_info_flags, orig_parent_uid, orig_parent_entry_id, file_data_state, "
        "rst_major_version, rst_minor_version, rst_cool_down_period, rst_file_policies, num_rst_ids"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    if (sqlite3_prepare_v2(db, INSERT_ENTRIES, -1, entries_stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "beegfs plugin: failed to prepare entries statement: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    static const char INSERT_TARGETS[] =
        "INSERT INTO " BEEGFS_PLUGIN_TARGETS_TABLE " (entry_rowid, target_index, target_or_group) "
        "VALUES (?, ?, ?);";

    if (sqlite3_prepare_v2(db, INSERT_TARGETS, -1, targets_stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "beegfs plugin: failed to prepare targets statement: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(*entries_stmt);
        *entries_stmt = NULL;
        return -1;
    }

    static const char INSERT_RST[] =
        "INSERT INTO " BEEGFS_PLUGIN_RST_TABLE " (entry_rowid, rst_index, rst_id) VALUES (?, ?, ?);";

    if (sqlite3_prepare_v2(db, INSERT_RST, -1, rst_stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "beegfs plugin: failed to prepare RST statement: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(*targets_stmt);
        *targets_stmt = NULL;
        sqlite3_finalize(*entries_stmt);
        *entries_stmt = NULL;
        return -1;
    }

    return 0;
}

void beegfs_plugin_finalize_index_statements(sqlite3_stmt *entries_stmt,
                                             sqlite3_stmt *targets_stmt,
                                             sqlite3_stmt *rst_stmt) {
    sqlite3_finalize(entries_stmt);
    sqlite3_finalize(targets_stmt);
    sqlite3_finalize(rst_stmt);
}

int beegfs_plugin_insert_metadata(sqlite3_stmt *entries_stmt,
                                  const struct beegfs_entry_metadata *metadata,
                                  int64_t *entry_rowid) {
    if (!entries_stmt || !metadata || !entry_rowid) {
        return -1;
    }

    sqlite3_reset(entries_stmt);
    sqlite3_clear_bindings(entries_stmt);

    char type_str[2] = { metadata->type, '\0' };
    char inode_str[21];
    snprintf(inode_str, sizeof(inode_str), "%" PRIu64, metadata->inode);

    int rc = 0;
    rc |= sqlite3_bind_text(entries_stmt, 1, metadata->name, -1, SQLITE_TRANSIENT);
    rc |= sqlite3_bind_text(entries_stmt, 2, type_str, -1, SQLITE_TRANSIENT);
    rc |= sqlite3_bind_text(entries_stmt, 3, inode_str, -1, SQLITE_TRANSIENT);

    if (metadata->got_info) {
        rc |= sqlite3_bind_int64(entries_stmt, 4, (sqlite3_int64) metadata->owner_id);
        rc |= sqlite3_bind_text(entries_stmt,  5, metadata->parent_entry_id, -1, SQLITE_TRANSIENT);
        rc |= sqlite3_bind_text(entries_stmt,  6, metadata->entry_id, -1, SQLITE_TRANSIENT);
        rc |= sqlite3_bind_int(entries_stmt,   7, metadata->entry_type);
        rc |= sqlite3_bind_int(entries_stmt,   8, metadata->feature_flags);
    } else {
        for (int i = 4; i <= 8; i++) {
            rc |= sqlite3_bind_null(entries_stmt, i);
        }
    }

    if (metadata->got_stripe_info) {
        rc |= sqlite3_bind_int64(entries_stmt, 9,  (sqlite3_int64) metadata->pattern_type);
        rc |= sqlite3_bind_int64(entries_stmt, 10, (sqlite3_int64) metadata->chunk_size);
        rc |= sqlite3_bind_int64(entries_stmt, 11, (sqlite3_int64) metadata->num_targets);
        rc |= sqlite3_bind_int64(entries_stmt, 12, (sqlite3_int64) metadata->default_num_targets);
        rc |= sqlite3_bind_int64(entries_stmt, 13, (sqlite3_int64) metadata->storage_pool_id);
        rc |= sqlite3_bind_int64(entries_stmt, 14, (sqlite3_int64) metadata->path_info_flags);
        rc |= sqlite3_bind_int64(entries_stmt, 15, (sqlite3_int64) metadata->orig_parent_uid);
        rc |= sqlite3_bind_text(entries_stmt,  16, metadata->orig_parent_entry_id, -1, SQLITE_TRANSIENT);
        rc |= sqlite3_bind_int(entries_stmt,   17, (int) metadata->file_data_state);
        rc |= sqlite3_bind_int(entries_stmt,   18, (int) metadata->rst_major_version);
        rc |= sqlite3_bind_int(entries_stmt,   19, (int) metadata->rst_minor_version);
        rc |= sqlite3_bind_int(entries_stmt,   20, (int) metadata->rst_cool_down_period);
        rc |= sqlite3_bind_int(entries_stmt,   21, (int) metadata->rst_file_policies);
        rc |= sqlite3_bind_int64(entries_stmt, 22, (sqlite3_int64) metadata->num_rst_ids);
    } else {
        for (int i = 9; i <= 22; i++) {
            rc |= sqlite3_bind_null(entries_stmt, i);
        }
    }

    if (rc != SQLITE_OK) {
        fprintf(stderr, "beegfs plugin: failed to bind entry metadata: %s\n",
                sqlite3_errmsg(sqlite3_db_handle(entries_stmt)));
        return -1;
    }

    if (sqlite3_step(entries_stmt) != SQLITE_DONE) {
        fprintf(stderr, "beegfs plugin: failed to insert entry metadata: %s\n",
                sqlite3_errmsg(sqlite3_db_handle(entries_stmt)));
        return -1;
    }

    *entry_rowid = sqlite3_last_insert_rowid(sqlite3_db_handle(entries_stmt));
    return 0;
}

int beegfs_plugin_insert_targets(const struct beegfs_entry_metadata *metadata,
                                 sqlite3_stmt *targets_stmt,
                                 int64_t entry_rowid) {
    if (!metadata || !targets_stmt) {
        return -1;
    }

    for (uint16_t i = 0; i < metadata->num_targets; i++) {
        sqlite3_reset(targets_stmt);
        sqlite3_clear_bindings(targets_stmt);

        int rc = 0;
        rc |= sqlite3_bind_int64(targets_stmt, 1, (sqlite3_int64) entry_rowid);
        rc |= sqlite3_bind_int(targets_stmt,   2, i);
        rc |= sqlite3_bind_int64(targets_stmt, 3, (sqlite3_int64) metadata->stripe_target_ids[i]);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "beegfs plugin: failed to bind stripe target metadata: %s\n",
                    sqlite3_errmsg(sqlite3_db_handle(targets_stmt)));
            return -1;
        }

        if (sqlite3_step(targets_stmt) != SQLITE_DONE) {
            fprintf(stderr, "beegfs plugin: failed to insert stripe target metadata: %s\n",
                    sqlite3_errmsg(sqlite3_db_handle(targets_stmt)));
            return -1;
        }
    }

    return 0;
}

int beegfs_plugin_insert_rst_ids(const struct beegfs_entry_metadata *metadata,
                                 sqlite3_stmt *rst_stmt,
                                 int64_t entry_rowid) {
    if (!metadata || !rst_stmt) {
        return -1;
    }

    for (uint32_t i = 0; i < metadata->num_rst_ids; i++) {
        sqlite3_reset(rst_stmt);
        sqlite3_clear_bindings(rst_stmt);

        int rc = 0;
        rc |= sqlite3_bind_int64(rst_stmt, 1, (sqlite3_int64) entry_rowid);
        rc |= sqlite3_bind_int64(rst_stmt, 2, (sqlite3_int64) i);
        rc |= sqlite3_bind_int64(rst_stmt, 3, (sqlite3_int64) metadata->rst_ids[i]);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "beegfs plugin: failed to bind RST id: %s\n",
                    sqlite3_errmsg(sqlite3_db_handle(rst_stmt)));
            return -1;
        }

        if (sqlite3_step(rst_stmt) != SQLITE_DONE) {
            fprintf(stderr, "beegfs plugin: failed to insert RST id: %s\n",
                    sqlite3_errmsg(sqlite3_db_handle(rst_stmt)));
            return -1;
        }
    }

    return 0;
}

int beegfs_create_query_views(sqlite3 *db) {
    if (!db) {
        return -1;
    }

    /* Real query traversal attaches each directory db as "tree"; the index
     * plugin's persistent views already exist there, so nothing to do. */
    if (sqlite3_db_filename(db, "tree")) {
        return 0;
    }

    /* gufi_query/gufi_vt type checking uses in-memory dbs with no "tree"
     * attached; provide schema-only stubs so SQL validation succeeds. */
    static const char STUB_SQL[] =
        "CREATE TEMP TABLE IF NOT EXISTS " BEEGFS_PLUGIN_FILE_VIEW " ("
        "beegfs_rowid INTEGER, name TEXT, type TEXT, inode TEXT, owner_id INTEGER, parent_entry_id TEXT, "
        "entry_id TEXT, entry_type INTEGER, feature_flags INTEGER, stripe_pattern_type INTEGER, "
        "stripe_pattern_name TEXT, stripe_chunk_size INTEGER, stripe_num_targets INTEGER, "
        "stripe_default_num_targets INTEGER, storage_pool_id INTEGER, path_info_flags INTEGER, "
        "orig_parent_uid INTEGER, orig_parent_entry_id TEXT, file_data_state INTEGER, "
        "rst_major_version INTEGER, rst_minor_version INTEGER, rst_cool_down_period INTEGER, "
        "rst_file_policies INTEGER, num_rst_ids INTEGER);"
        "CREATE TEMP TABLE IF NOT EXISTS " BEEGFS_PLUGIN_FILE_TARGETS_VIEW " ("
        "beegfs_rowid INTEGER, name TEXT, inode TEXT, target_index INTEGER, target_or_group INTEGER);";

    char *err = NULL;
    if (sqlite3_exec(db, STUB_SQL, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "beegfs plugin: failed to create query stubs: %s\n", err ? err : sqlite3_errmsg(db));
        sqlite3_free(err);
        return -1;
    }

    return 0;
}
