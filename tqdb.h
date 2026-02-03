/**
 * @file tqdb.h
 * @brief TQDB - Portable trait-based file database library
 *
 * A pure C library for embedded file-based databases with configurable
 * entity types. No platform dependencies - uses only standard C library.
 *
 * Features:
 * - Trait-based entity registration (define your own types)
 * - Configurable allocator (bring your own malloc/free)
 * - Optional mutex support for threading
 * - CRC32 integrity checking
 * - Atomic writes with backup/recovery
 *
 * Usage:
 * @code
 * #include "tqdb.h"
 *
 * // Define your entity and trait (see tqdb_trait_t)
 * tqdb_t db;
 * tqdb_config_t cfg = { .db_path = "data.tqdb" };
 * tqdb_open(&cfg, &db);
 * tqdb_register(db, &MY_TRAIT);
 * // ... use tqdb_add, tqdb_get, etc.
 * tqdb_close(db);
 * @endcode
 */

#ifndef TQDB_H
#define TQDB_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ═══════════════════════════════════════════════════════════════════════════
 * Configuration Defaults (override before including this header)
 * ═══════════════════════════════════════════════════════════════════════════ */

#ifndef TQDB_MAX_ENTITY_TYPES
#define TQDB_MAX_ENTITY_TYPES 8
#endif

#ifndef TQDB_MAX_STRING_LEN
#define TQDB_MAX_STRING_LEN 4096
#endif

#ifndef TQDB_DEFAULT_SCRATCH_SIZE
#define TQDB_DEFAULT_SCRATCH_SIZE 8192
#endif

/* WAL defaults */
#ifndef TQDB_WAL_MAX_ENTRIES_DEFAULT
#define TQDB_WAL_MAX_ENTRIES_DEFAULT 100
#endif

#ifndef TQDB_WAL_MAX_SIZE_DEFAULT
#define TQDB_WAL_MAX_SIZE_DEFAULT 65536
#endif

/* Cache defaults */
#ifndef TQDB_CACHE_SIZE_DEFAULT
#define TQDB_CACHE_SIZE_DEFAULT 16
#endif

/* Query system defaults */
#ifndef TQDB_QUERY_MAX_CONDITIONS
#define TQDB_QUERY_MAX_CONDITIONS 8
#endif

/* Feature flags (1 = enabled by default) */
#ifndef TQDB_ENABLE_WAL
#define TQDB_ENABLE_WAL 1
#endif

#ifndef TQDB_ENABLE_CACHE
#define TQDB_ENABLE_CACHE 1
#endif

/* Custom allocator macros (default to stdlib) */
#ifndef TQDB_MALLOC
#include <stdlib.h>
#define TQDB_MALLOC(size) malloc(size)
#endif

#ifndef TQDB_FREE
#include <stdlib.h>
#define TQDB_FREE(ptr) free(ptr)
#endif

#ifndef TQDB_REALLOC
#include <stdlib.h>
#define TQDB_REALLOC(ptr, size) realloc(ptr, size)
#endif

/* ═══════════════════════════════════════════════════════════════════════════
 * Error Codes
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef enum {
    TQDB_OK = 0,              /**< Success */
    TQDB_ERR_INVALID_ARG,     /**< Invalid argument */
    TQDB_ERR_NO_MEM,          /**< Memory allocation failed */
    TQDB_ERR_NOT_FOUND,       /**< Entity not found */
    TQDB_ERR_EXISTS,          /**< Entity already exists */
    TQDB_ERR_IO,              /**< File I/O error */
    TQDB_ERR_CORRUPT,         /**< Database file corrupt */
    TQDB_ERR_FULL,            /**< Max entities reached */
    TQDB_ERR_TIMEOUT,         /**< Mutex timeout */
    TQDB_ERR_NOT_REGISTERED   /**< Entity type not registered */
} tqdb_err_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * Opaque Handle Types
 * ═══════════════════════════════════════════════════════════════════════════ */

/** Database handle */
typedef struct tqdb_s* tqdb_t;

/** Binary writer handle (for trait->write callbacks) */
typedef struct tqdb_writer_s tqdb_writer_t;

/** Binary reader handle (for trait->read callbacks) */
typedef struct tqdb_reader_s tqdb_reader_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * Allocator Interface
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Custom allocator interface.
 * Pass NULL to tqdb_config_t.alloc to use default (stdlib malloc/free).
 */
typedef struct {
    void* (*malloc)(size_t size);
    void  (*free)(void* ptr);
    void* (*realloc)(void* ptr, size_t size);  /**< Optional, can be NULL */
} tqdb_alloc_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * Mutex Interface (Optional)
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Optional mutex operations for thread-safe access.
 * Pass NULL to tqdb_config_t.mutex for single-threaded use.
 */
typedef struct {
    void* (*create)(void);                          /**< Create mutex, return handle */
    void  (*destroy)(void* mutex);                  /**< Destroy mutex */
    bool  (*lock)(void* mutex, uint32_t timeout_ms); /**< Lock with timeout, return success */
    void  (*unlock)(void* mutex);                   /**< Unlock */
} tqdb_mutex_ops_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * Entity Trait
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Entity trait definition.
 *
 * Define one of these for each entity type you want to store.
 * Provides serialization, ID access, and optional lifecycle callbacks.
 */
typedef struct {
    const char* name;         /**< Unique type name (e.g., "Pattern") */
    size_t max_count;         /**< Maximum entities of this type (sanity limit) */
    size_t struct_size;       /**< sizeof(YourEntityType) */

    /* Required callbacks */
    void (*write)(tqdb_writer_t* w, const void* entity);  /**< Serialize entity */
    void (*read)(tqdb_reader_t* r, void* entity);         /**< Deserialize entity */
    uint32_t (*get_id)(const void* entity);               /**< Get entity ID */
    void (*set_id)(void* entity, uint32_t id);            /**< Set entity ID */

    /* Optional callbacks (can be NULL) */
    void (*init)(void* entity);       /**< Initialize before read (e.g., zero memory) */
    void (*destroy)(void* entity);    /**< Cleanup before free (e.g., free internal allocs) */
    void (*skip)(tqdb_reader_t* r);   /**< Skip entity in stream without full read */
} tqdb_trait_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * Database Configuration
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Database configuration.
 * Only db_path is required; all other fields have sensible defaults.
 */
typedef struct {
    const char* db_path;       /**< Required: path to database file */
    const char* tmp_path;      /**< Optional: temp file path (NULL = db_path + ".tmp") */
    const char* bak_path;      /**< Optional: backup file path (NULL = db_path + ".bak") */

    tqdb_alloc_t* alloc;       /**< Optional: custom allocator (NULL = use TQDB_MALLOC/FREE) */
    tqdb_mutex_ops_t* mutex;   /**< Optional: mutex ops (NULL = no locking) */

    uint8_t* scratch_buf;      /**< Optional: user-provided scratch buffer */
    size_t scratch_size;       /**< Scratch size (0 = TQDB_DEFAULT_SCRATCH_SIZE) */

#if TQDB_ENABLE_WAL
    /* WAL options */
    bool enable_wal;           /**< Enable WAL (default: true if wal_path provided or auto-generated) */
    const char* wal_path;      /**< Optional: WAL file path (NULL = db_path + ".wal") */
    size_t wal_max_entries;    /**< Auto-checkpoint after N entries (0 = TQDB_WAL_MAX_ENTRIES_DEFAULT) */
    size_t wal_max_size;       /**< Auto-checkpoint at size bytes (0 = TQDB_WAL_MAX_SIZE_DEFAULT) */
#endif

#if TQDB_ENABLE_CACHE
    /* Cache options */
    bool enable_cache;         /**< Enable read cache (default: false) */
    size_t cache_size;         /**< Max cached entities (0 = TQDB_CACHE_SIZE_DEFAULT if enabled) */
#endif
} tqdb_config_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * Callback Types
 * ═══════════════════════════════════════════════════════════════════════════ */

/** Iterator callback. Return true to continue, false to stop. */
typedef bool (*tqdb_iter_fn)(const void* entity, void* ctx);

/** Modifier callback for batch updates. */
typedef void (*tqdb_modify_fn)(void* entity, void* ctx);

/** Filter callback. Return true to keep, false to delete/skip. */
typedef bool (*tqdb_filter_fn)(const void* entity, void* ctx);

/* ═══════════════════════════════════════════════════════════════════════════
 * Lifecycle Functions
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Open a database.
 *
 * @param config Configuration (db_path required, others optional)
 * @param out_db Output: database handle
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_open(const tqdb_config_t* config, tqdb_t* out_db);

/**
 * Close a database and free resources.
 *
 * @param db Database handle (safe to pass NULL)
 */
void tqdb_close(tqdb_t db);

/* ═══════════════════════════════════════════════════════════════════════════
 * Entity Registration
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Register an entity type.
 * Must be called before using CRUD operations for that type.
 *
 * @param db Database handle
 * @param trait Entity trait definition
 * @return TQDB_OK on success, TQDB_ERR_FULL if max types reached
 */
tqdb_err_t tqdb_register(tqdb_t db, const tqdb_trait_t* trait);

/* ═══════════════════════════════════════════════════════════════════════════
 * CRUD Operations
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Add a new entity.
 * An auto-incrementing ID is assigned via trait->set_id().
 *
 * @param db Database handle
 * @param type Entity type name (must match registered trait)
 * @param entity Pointer to entity data (ID will be set by this function)
 * @return TQDB_OK, TQDB_ERR_FULL if max reached, TQDB_ERR_NOT_REGISTERED
 */
tqdb_err_t tqdb_add(tqdb_t db, const char* type, void* entity);

/**
 * Get an entity by ID.
 *
 * @param db Database handle
 * @param type Entity type name
 * @param id Entity ID
 * @param out Output buffer (must be at least trait->struct_size bytes)
 * @return TQDB_OK, TQDB_ERR_NOT_FOUND if not exists
 */
tqdb_err_t tqdb_get(tqdb_t db, const char* type, uint32_t id, void* out);

/**
 * Update an existing entity.
 *
 * @param db Database handle
 * @param type Entity type name
 * @param id Entity ID
 * @param entity New entity data
 * @return TQDB_OK, TQDB_ERR_NOT_FOUND if not exists
 */
tqdb_err_t tqdb_update(tqdb_t db, const char* type, uint32_t id, const void* entity);

/**
 * Delete an entity by ID.
 *
 * @param db Database handle
 * @param type Entity type name
 * @param id Entity ID
 * @return TQDB_OK, TQDB_ERR_NOT_FOUND if not exists
 */
tqdb_err_t tqdb_delete(tqdb_t db, const char* type, uint32_t id);

/**
 * Check if an entity exists.
 *
 * @param db Database handle
 * @param type Entity type name
 * @param id Entity ID
 * @return true if exists
 */
bool tqdb_exists(tqdb_t db, const char* type, uint32_t id);

/**
 * Count entities of a type.
 *
 * @param db Database handle
 * @param type Entity type name
 * @return Number of entities (0 if type not registered)
 */
size_t tqdb_count(tqdb_t db, const char* type);

/* ═══════════════════════════════════════════════════════════════════════════
 * Iteration & Batch Operations
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Iterate over all entities of a type.
 *
 * @param db Database handle
 * @param type Entity type name
 * @param fn Iterator callback (return false to stop)
 * @param ctx User context passed to callback
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_foreach(tqdb_t db, const char* type, tqdb_iter_fn fn, void* ctx);

/**
 * Modify entities matching a filter.
 *
 * @param db Database handle
 * @param type Entity type name
 * @param filter Filter callback (NULL = all entities)
 * @param filter_ctx Context for filter
 * @param modify Modifier callback
 * @param modify_ctx Context for modifier
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_modify_where(tqdb_t db, const char* type,
                             tqdb_filter_fn filter, void* filter_ctx,
                             tqdb_modify_fn modify, void* modify_ctx);

/**
 * Delete entities matching a filter.
 *
 * @param db Database handle
 * @param type Entity type name
 * @param filter Filter callback (return false to delete)
 * @param ctx Context for filter
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_delete_where(tqdb_t db, const char* type,
                             tqdb_filter_fn filter, void* ctx);

/* ═══════════════════════════════════════════════════════════════════════════
 * Maintenance
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Compact database file (remove deleted space).
 *
 * @param db Database handle
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_vacuum(tqdb_t db);

/**
 * Force pending writes to disk.
 *
 * @param db Database handle
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_flush(tqdb_t db);

#if TQDB_ENABLE_WAL
/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Operations
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Force WAL checkpoint - merge WAL entries into main database.
 * Called automatically when thresholds are reached.
 *
 * @param db Database handle
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_checkpoint(tqdb_t db);

/**
 * Get current WAL statistics.
 *
 * @param db Database handle
 * @param out_entry_count Output: number of entries in WAL (can be NULL)
 * @param out_size Output: current WAL file size in bytes (can be NULL)
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_wal_stats(tqdb_t db, size_t* out_entry_count, size_t* out_size);
#endif /* TQDB_ENABLE_WAL */

#if TQDB_ENABLE_CACHE
/* ═══════════════════════════════════════════════════════════════════════════
 * Cache Operations
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Clear all cached entities.
 *
 * @param db Database handle
 */
void tqdb_cache_clear(tqdb_t db);

/**
 * Get cache hit statistics.
 *
 * @param db Database handle
 * @param out_hits Output: number of cache hits (can be NULL)
 * @param out_misses Output: number of cache misses (can be NULL)
 */
void tqdb_cache_stats(tqdb_t db, size_t* out_hits, size_t* out_misses);
#endif /* TQDB_ENABLE_CACHE */

/* ═══════════════════════════════════════════════════════════════════════════
 * Query System (compile with -DTQDB_ENABLE_QUERY)
 * ═══════════════════════════════════════════════════════════════════════════ */

#ifdef TQDB_ENABLE_QUERY

/** Query handle */
typedef struct tqdb_query_s* tqdb_query_t;

/** Query operators */
typedef enum {
    TQDB_OP_EQ,          /**< field == value */
    TQDB_OP_NE,          /**< field != value */
    TQDB_OP_LT,          /**< field < value */
    TQDB_OP_LE,          /**< field <= value */
    TQDB_OP_GT,          /**< field > value */
    TQDB_OP_GE,          /**< field >= value */
    TQDB_OP_BETWEEN,     /**< min <= field <= max (use tqdb_query_where_between) */
    TQDB_OP_LIKE,        /**< pattern match with * and ? wildcards */
    TQDB_OP_IS_NULL,     /**< field is empty/zero */
    TQDB_OP_NOT_NULL     /**< field is not empty/zero */
} tqdb_query_op_t;

/** Field types for query conditions */
typedef enum {
    TQDB_FIELD_INT32,
    TQDB_FIELD_INT64,
    TQDB_FIELD_FLOAT,
    TQDB_FIELD_DOUBLE,
    TQDB_FIELD_STRING,
    TQDB_FIELD_BOOL,
    TQDB_FIELD_UINT8,
    TQDB_FIELD_UINT16,
    TQDB_FIELD_UINT32
} tqdb_field_type_t;

/** Field definition for queryable entities */
typedef struct {
    const char* name;           /**< Field name for query */
    tqdb_field_type_t type;     /**< Field data type */
    size_t offset;              /**< offsetof(struct, field) */
    size_t size;                /**< sizeof field (for strings: max buffer len) */
} tqdb_field_def_t;

/**
 * Extended trait with field definitions for querying.
 * Cast your tqdb_trait_t pointer to this type if you want to support queries.
 */
typedef struct {
    tqdb_trait_t base;              /**< Standard trait (must be first) */
    const tqdb_field_def_t* fields; /**< Array of field definitions */
    size_t field_count;             /**< Number of fields */
} tqdb_trait_ext_t;

/**
 * Create a new query for an entity type.
 *
 * @param db Database handle
 * @param type Entity type name
 * @return Query handle, or NULL on error
 */
tqdb_query_t tqdb_query_new(tqdb_t db, const char* type);

/**
 * Add a condition with int32 value.
 *
 * @param q Query handle
 * @param field Field name
 * @param op Comparison operator
 * @param value Value to compare
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_query_where_i32(tqdb_query_t q, const char* field,
                                 tqdb_query_op_t op, int32_t value);

/**
 * Add a condition with int64 value.
 */
tqdb_err_t tqdb_query_where_i64(tqdb_query_t q, const char* field,
                                 tqdb_query_op_t op, int64_t value);

/**
 * Add a condition with float value.
 */
tqdb_err_t tqdb_query_where_float(tqdb_query_t q, const char* field,
                                   tqdb_query_op_t op, float value);

/**
 * Add a condition with double value.
 */
tqdb_err_t tqdb_query_where_double(tqdb_query_t q, const char* field,
                                    tqdb_query_op_t op, double value);

/**
 * Add a condition with string value.
 * For TQDB_OP_LIKE, supports * (any chars) and ? (single char) wildcards.
 */
tqdb_err_t tqdb_query_where_str(tqdb_query_t q, const char* field,
                                 tqdb_query_op_t op, const char* value);

/**
 * Add a condition with bool value.
 */
tqdb_err_t tqdb_query_where_bool(tqdb_query_t q, const char* field,
                                  tqdb_query_op_t op, bool value);

/**
 * Add a BETWEEN condition for int32 (min <= field <= max).
 */
tqdb_err_t tqdb_query_where_between_i32(tqdb_query_t q, const char* field,
                                         int32_t min, int32_t max);

/**
 * Add a BETWEEN condition for int64.
 */
tqdb_err_t tqdb_query_where_between_i64(tqdb_query_t q, const char* field,
                                         int64_t min, int64_t max);

/**
 * Add a BETWEEN condition for float.
 */
tqdb_err_t tqdb_query_where_between_float(tqdb_query_t q, const char* field,
                                           float min, float max);

/**
 * Add a BETWEEN condition for double.
 */
tqdb_err_t tqdb_query_where_between_double(tqdb_query_t q, const char* field,
                                            double min, double max);

/**
 * Add IS_NULL or NOT_NULL condition.
 */
tqdb_err_t tqdb_query_where_null(tqdb_query_t q, const char* field,
                                  bool is_null);

/**
 * Set result limit.
 *
 * @param q Query handle
 * @param limit Maximum number of results (0 = no limit)
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_query_limit(tqdb_query_t q, size_t limit);

/**
 * Set result offset (skip first N matches).
 *
 * @param q Query handle
 * @param offset Number of results to skip
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_query_offset(tqdb_query_t q, size_t offset);

/**
 * Execute query and iterate over matching entities.
 *
 * @param q Query handle
 * @param fn Iterator callback (return false to stop)
 * @param ctx User context
 * @return TQDB_OK on success
 */
tqdb_err_t tqdb_query_exec(tqdb_query_t q, tqdb_iter_fn fn, void* ctx);

/**
 * Count matching entities without iterating.
 *
 * @param q Query handle
 * @return Number of matching entities
 */
size_t tqdb_query_count(tqdb_query_t q);

/**
 * Free query resources.
 *
 * @param q Query handle (safe to pass NULL)
 */
void tqdb_query_free(tqdb_query_t q);

#endif /* TQDB_ENABLE_QUERY */

/* ═══════════════════════════════════════════════════════════════════════════
 * Binary Serialization Helpers (for trait callbacks)
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Writer functions */
void tqdb_write_u8(tqdb_writer_t* w, uint8_t v);
void tqdb_write_u16(tqdb_writer_t* w, uint16_t v);
void tqdb_write_u32(tqdb_writer_t* w, uint32_t v);
void tqdb_write_i32(tqdb_writer_t* w, int32_t v);
void tqdb_write_i64(tqdb_writer_t* w, int64_t v);
void tqdb_write_str(tqdb_writer_t* w, const char* s);
void tqdb_write_raw(tqdb_writer_t* w, const void* data, size_t len);
bool tqdb_write_error(tqdb_writer_t* w);

/* Reader functions */
uint8_t  tqdb_read_u8(tqdb_reader_t* r);
uint16_t tqdb_read_u16(tqdb_reader_t* r);
uint32_t tqdb_read_u32(tqdb_reader_t* r);
int32_t  tqdb_read_i32(tqdb_reader_t* r);
int64_t  tqdb_read_i64(tqdb_reader_t* r);
size_t   tqdb_read_str(tqdb_reader_t* r, char* buf, size_t buf_size);
void     tqdb_read_raw(tqdb_reader_t* r, void* data, size_t len);
void     tqdb_read_skip(tqdb_reader_t* r, size_t len);
void     tqdb_read_skip_str(tqdb_reader_t* r);
bool     tqdb_read_error(tqdb_reader_t* r);

#ifdef __cplusplus
}
#endif

#endif /* TQDB_H */
