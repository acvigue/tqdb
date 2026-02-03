/**
 * @file tqdb_internal.h
 * @brief Internal definitions for TQDB implementation
 */

#ifndef TQDB_INTERNAL_H
#define TQDB_INTERNAL_H

#include "../tqdb.h"
#include <stdio.h>
#include <string.h>

/* ═══════════════════════════════════════════════════════════════════════════
 * File Format Constants
 * ═══════════════════════════════════════════════════════════════════════════ */

#define TQDB_MAGIC          0x42445154  /* "TQDB" little-endian */
#define TQDB_VERSION        1
#define TQDB_HEADER_SIZE    16

#if TQDB_ENABLE_WAL
/* WAL file format constants */
#define TQDB_WAL_MAGIC      0x4C415754  /* "TWAL" little-endian */
#define TQDB_WAL_VERSION    1
#define TQDB_WAL_HEADER_SIZE 16

/* WAL operation types */
#define TQDB_WAL_OP_ADD     1
#define TQDB_WAL_OP_UPDATE  2
#define TQDB_WAL_OP_DELETE  3
#endif

/* ═══════════════════════════════════════════════════════════════════════════
 * Internal Allocator Helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static inline void* tqdb_alloc(tqdb_t db, size_t size);
static inline void tqdb_dealloc(tqdb_t db, void* ptr);

/* ═══════════════════════════════════════════════════════════════════════════
 * Binary Writer Structure
 * ═══════════════════════════════════════════════════════════════════════════ */

struct tqdb_writer_s {
    FILE* file;
    uint32_t crc;
    bool error;
    uint8_t* buf;
    size_t buf_size;
    size_t buf_pos;
};

/* ═══════════════════════════════════════════════════════════════════════════
 * Binary Reader Structure
 * ═══════════════════════════════════════════════════════════════════════════ */

struct tqdb_reader_s {
    FILE* file;
    uint32_t crc;
    bool error;
    uint8_t* buf;
    size_t buf_size;
    size_t buf_pos;
    size_t buf_filled;
};

#if TQDB_ENABLE_WAL
/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Header Structure
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    uint32_t magic;         /* TQDB_WAL_MAGIC */
    uint16_t version;       /* TQDB_WAL_VERSION */
    uint16_t flags;         /* Reserved */
    uint32_t db_crc;        /* CRC of main DB when WAL started */
    uint32_t entry_count;   /* Number of entries in WAL */
} tqdb_wal_header_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * WAL State Structure
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    char* path;                   /* WAL file path */
    uint32_t entry_count;         /* Current entry count */
    uint32_t file_size;           /* Current file size */
    uint32_t db_crc;              /* CRC of main DB when WAL started */
    size_t max_entries;           /* Checkpoint threshold: entries */
    size_t max_size;              /* Checkpoint threshold: size */
    bool enabled;                 /* WAL enabled flag */
    bool recovery_pending;        /* True if WAL recovery deferred until traits registered */
} tqdb_wal_t;
#endif /* TQDB_ENABLE_WAL */

#if TQDB_ENABLE_CACHE
/* ═══════════════════════════════════════════════════════════════════════════
 * Cache Entry Structure
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    uint32_t id;                /* Entity ID (0 = empty slot) */
    uint8_t type_idx;           /* Entity type index */
    uint8_t op;                 /* Last WAL operation (for deleted tracking) */
    void* entity;               /* Cached entity data (NULL if deleted) */
    uint32_t access_count;      /* LRU counter */
} tqdb_cache_entry_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * Cache Structure
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    tqdb_cache_entry_t* entries;  /* Array of cache entries */
    size_t capacity;              /* Max entries */
    size_t count;                 /* Current entries */
    uint32_t access_counter;      /* Global LRU counter */
    size_t hits;                  /* Cache hit count */
    size_t misses;                /* Cache miss count */
} tqdb_cache_t;
#endif /* TQDB_ENABLE_CACHE */

/* ═══════════════════════════════════════════════════════════════════════════
 * Database Structure
 * ═══════════════════════════════════════════════════════════════════════════ */

struct tqdb_s {
    /* Configuration (copied) */
    char* db_path;
    char* tmp_path;
    char* bak_path;

    /* Allocator */
    tqdb_alloc_t alloc;
    bool owns_alloc;  /* true if using default allocator */

    /* Mutex */
    tqdb_mutex_ops_t* mutex_ops;
    void* mutex;

    /* Scratch buffer */
    uint8_t* scratch;
    size_t scratch_size;
    bool owns_scratch;  /* true if we allocated it */

    /* Entity traits */
    const tqdb_trait_t* traits[TQDB_MAX_ENTITY_TYPES];
    size_t trait_count;

    /* Auto-increment ID counter (per type) */
    uint32_t next_id[TQDB_MAX_ENTITY_TYPES];

#if TQDB_ENABLE_WAL
    /* WAL state */
    tqdb_wal_t wal;
#endif

#if TQDB_ENABLE_CACHE
    /* Cache state */
    tqdb_cache_t* cache;
#endif
};

/* ═══════════════════════════════════════════════════════════════════════════
 * File Header Structure
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint32_t crc;
    uint32_t reserved;
} tqdb_header_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * Internal Functions
 * ═══════════════════════════════════════════════════════════════════════════ */

/* CRC32 (polynomial-based, no lookup table) */
uint32_t tqdb_crc32_update(uint32_t crc, const uint8_t* data, size_t len);
uint32_t tqdb_crc32_finalize(uint32_t crc);

/* Binary I/O */
void tqdb_writer_init(tqdb_writer_t* w, FILE* f, uint8_t* buf, size_t buf_size);
void tqdb_writer_flush(tqdb_writer_t* w);
uint32_t tqdb_writer_crc(tqdb_writer_t* w);

void tqdb_reader_init(tqdb_reader_t* r, FILE* f, uint8_t* buf, size_t buf_size);
uint32_t tqdb_reader_crc(tqdb_reader_t* r);

/* Trait lookup */
const tqdb_trait_t* tqdb_find_trait(tqdb_t db, const char* name);
int tqdb_find_trait_index(tqdb_t db, const char* name);

#if TQDB_ENABLE_WAL
/* WAL internal functions */
tqdb_err_t tqdb_wal_init(tqdb_t db, const char* wal_path, size_t max_entries, size_t max_size);
void tqdb_wal_destroy(tqdb_t db);
tqdb_err_t tqdb_wal_recover(tqdb_t db);
tqdb_err_t tqdb_wal_check_recovery(tqdb_t db);  /* Deferred recovery after traits registered */
tqdb_err_t tqdb_wal_append(tqdb_t db, uint8_t op, uint8_t type_idx,
                           uint32_t id, const void* entity);
tqdb_err_t tqdb_wal_find(tqdb_t db, uint8_t type_idx, uint32_t id,
                         uint8_t* out_op, void* out_entity);
tqdb_err_t tqdb_wal_checkpoint_internal(tqdb_t db);
bool tqdb_wal_should_checkpoint(tqdb_t db);
uint32_t tqdb_wal_compute_db_crc(tqdb_t db);
#endif /* TQDB_ENABLE_WAL */

#if TQDB_ENABLE_CACHE
/* Cache internal functions */
tqdb_err_t tqdb_cache_init(tqdb_t db, size_t capacity);
void tqdb_cache_destroy(tqdb_t db);
tqdb_cache_entry_t* tqdb_cache_get(tqdb_t db, uint8_t type_idx, uint32_t id);
tqdb_err_t tqdb_cache_put(tqdb_t db, uint8_t type_idx, uint32_t id,
                          const void* entity, uint8_t op);
void tqdb_cache_invalidate(tqdb_t db, uint8_t type_idx, uint32_t id);
void tqdb_cache_invalidate_all(tqdb_t db);
#endif /* TQDB_ENABLE_CACHE */

/* Mutex helpers */
static inline bool tqdb_lock(tqdb_t db) {
    if (db->mutex_ops && db->mutex) {
        return db->mutex_ops->lock(db->mutex, 5000);
    }
    return true;
}

static inline void tqdb_unlock(tqdb_t db) {
    if (db->mutex_ops && db->mutex) {
        db->mutex_ops->unlock(db->mutex);
    }
}

/* Allocator helpers */
static inline void* tqdb_alloc(tqdb_t db, size_t size) {
    return db->alloc.malloc(size);
}

static inline void tqdb_dealloc(tqdb_t db, void* ptr) {
    if (ptr) {
        db->alloc.free(ptr);
    }
}

#endif /* TQDB_INTERNAL_H */
