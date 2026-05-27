#include <sqlite3.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <time.h>

#include <pthread.h>

#include "bf.h"
#include "beegfs_plugin_shared.h"

static struct {
    int enabled;
    pthread_mutex_t mutex;
    uint64_t ctx_init_ns;
    uint64_t ctx_init_calls;
    uint64_t ctx_init_max_ns;
} beegfs_query_timing = {
    .enabled = 0,
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .ctx_init_ns = 0,
    .ctx_init_calls = 0,
    .ctx_init_max_ns = 0,
};

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (((uint64_t) ts.tv_sec) * 1000000000ULL) + ((uint64_t) ts.tv_nsec);
}

static inline long double ns_to_s(const uint64_t ns) {
    return ((long double) ns) / 1e9L;
}

static int global_init(void *global) {
    struct input *in = (struct input *) global;
    beegfs_query_timing.enabled = in && in->terse;
    return 0;
}

static void *ctx_init(void *ptr) {
    const uint64_t start = now_ns();

    sqlite3 *db = (sqlite3 *) ptr;
    if (!db) {
        return NULL;
    }

    beegfs_create_query_views(db);

    const uint64_t elapsed = now_ns() - start;
    if (beegfs_query_timing.enabled) {
        pthread_mutex_lock(&beegfs_query_timing.mutex);
        beegfs_query_timing.ctx_init_ns += elapsed;
        beegfs_query_timing.ctx_init_calls++;
        if (elapsed > beegfs_query_timing.ctx_init_max_ns) {
            beegfs_query_timing.ctx_init_max_ns = elapsed;
        }
        pthread_mutex_unlock(&beegfs_query_timing.mutex);
    }

    return NULL;
}

static void global_exit(void *global) {
    (void) global;

    if (!beegfs_query_timing.enabled) {
        return;
    }

    const uint64_t calls = beegfs_query_timing.ctx_init_calls;
    const long double avg_s = calls ?
                              (ns_to_s(beegfs_query_timing.ctx_init_ns) / ((long double) calls)) :
                              0.0L;

    fprintf(stderr, "[beegfs_query_plugin timing] ctx_init=%.6Lfs(calls=%llu avg=%.6Lfs max=%.6Lfs)\n",
            ns_to_s(beegfs_query_timing.ctx_init_ns),
            (unsigned long long) calls,
            avg_s,
            ns_to_s(beegfs_query_timing.ctx_init_max_ns));
}

struct plugin_operations beegfs_query_ops = {
    .type = PLUGIN_QUERY,
    .global_init = global_init,
    .ctx_init = ctx_init,
    .process_dir = NULL,
    .process_file = NULL,
    .ctx_exit = NULL,
    .global_exit = global_exit,
};
