/**
 * @file tqdb_cache.c
 * @brief LRU cache implementation for TQDB
 *
 * Simple array-based LRU cache for frequently accessed entities.
 * Cache entries store serialized entity data to avoid trait dependency issues.
 */

#include "tqdb_internal.h"

#if TQDB_ENABLE_CACHE

/* Define delete operation constant for cache (may not have WAL enabled) */
#ifndef TQDB_WAL_OP_DELETE
#define TQDB_WAL_OP_DELETE 3
#endif

/* ═══════════════════════════════════════════════════════════════════════════
 * Cache Initialization
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_cache_init(tqdb_t db, size_t capacity) {
    if (!db) return TQDB_ERR_INVALID_ARG;
    if (capacity == 0) capacity = TQDB_CACHE_SIZE_DEFAULT;

    /* Allocate cache structure */
    db->cache = (tqdb_cache_t*)tqdb_alloc(db, sizeof(tqdb_cache_t));
    if (!db->cache) return TQDB_ERR_NO_MEM;

    memset(db->cache, 0, sizeof(tqdb_cache_t));

    /* Allocate entries array */
    db->cache->entries = (tqdb_cache_entry_t*)tqdb_alloc(db,
        capacity * sizeof(tqdb_cache_entry_t));
    if (!db->cache->entries) {
        tqdb_dealloc(db, db->cache);
        db->cache = NULL;
        return TQDB_ERR_NO_MEM;
    }

    memset(db->cache->entries, 0, capacity * sizeof(tqdb_cache_entry_t));
    db->cache->capacity = capacity;
    db->cache->count = 0;
    db->cache->access_counter = 0;
    db->cache->hits = 0;
    db->cache->misses = 0;

    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Cache Destruction
 * ═══════════════════════════════════════════════════════════════════════════ */

void tqdb_cache_destroy(tqdb_t db) {
    if (!db || !db->cache) return;

    /* Free all cached entity data */
    for (size_t i = 0; i < db->cache->capacity; i++) {
        if (db->cache->entries[i].entity) {
            const tqdb_trait_t* trait = db->traits[db->cache->entries[i].type_idx];
            if (trait && trait->destroy) {
                trait->destroy(db->cache->entries[i].entity);
            }
            tqdb_dealloc(db, db->cache->entries[i].entity);
            db->cache->entries[i].entity = NULL;
        }
    }

    tqdb_dealloc(db, db->cache->entries);
    tqdb_dealloc(db, db->cache);
    db->cache = NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Cache Lookup
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_cache_entry_t* tqdb_cache_get(tqdb_t db, uint8_t type_idx, uint32_t id) {
    if (!db || !db->cache || id == 0) return NULL;

    for (size_t i = 0; i < db->cache->capacity; i++) {
        tqdb_cache_entry_t* entry = &db->cache->entries[i];
        if (entry->id != 0 &&
            entry->type_idx == type_idx &&
            entry->id == id) {
            /* Update LRU counter */
            entry->access_count = ++db->cache->access_counter;
            db->cache->hits++;
            return entry;
        }
    }

    db->cache->misses++;
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Find LRU Entry
 * ═══════════════════════════════════════════════════════════════════════════ */

static size_t cache_find_lru_index(tqdb_cache_t* cache) {
    size_t lru_idx = 0;
    uint32_t min_access = UINT32_MAX;

    /* First, try to find an empty slot */
    for (size_t i = 0; i < cache->capacity; i++) {
        if (cache->entries[i].id == 0) {
            return i;
        }
        if (cache->entries[i].access_count < min_access) {
            min_access = cache->entries[i].access_count;
            lru_idx = i;
        }
    }

    return lru_idx;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Cache Insert/Update
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_cache_put(tqdb_t db, uint8_t type_idx, uint32_t id,
                          const void* entity, uint8_t op) {
    if (!db || !db->cache || id == 0) return TQDB_ERR_INVALID_ARG;
    if (type_idx >= db->trait_count) return TQDB_ERR_INVALID_ARG;

    const tqdb_trait_t* trait = db->traits[type_idx];
    if (!trait) return TQDB_ERR_INVALID_ARG;

    /* Check if already in cache */
    tqdb_cache_entry_t* existing = NULL;
    for (size_t i = 0; i < db->cache->capacity; i++) {
        tqdb_cache_entry_t* entry = &db->cache->entries[i];
        if (entry->id != 0 &&
            entry->type_idx == type_idx &&
            entry->id == id) {
            existing = entry;
            break;
        }
    }

    tqdb_cache_entry_t* target;
    if (existing) {
        target = existing;
        /* Free old entity data */
        if (target->entity) {
            if (trait->destroy) trait->destroy(target->entity);
            tqdb_dealloc(db, target->entity);
            target->entity = NULL;
        }
    } else {
        /* Find slot (empty or LRU) */
        size_t idx = cache_find_lru_index(db->cache);
        target = &db->cache->entries[idx];

        /* Evict if occupied */
        if (target->id != 0 && target->entity) {
            const tqdb_trait_t* old_trait = db->traits[target->type_idx];
            if (old_trait && old_trait->destroy) {
                old_trait->destroy(target->entity);
            }
            tqdb_dealloc(db, target->entity);
            target->entity = NULL;
        }

        if (target->id == 0) {
            db->cache->count++;
        }
    }

    /* Set ID */
    target->id = id;
    target->type_idx = type_idx;
    target->op = op;
    target->access_count = ++db->cache->access_counter;

    /* Copy entity data if not a delete */
    if (op != TQDB_WAL_OP_DELETE && entity) {
        target->entity = tqdb_alloc(db, trait->struct_size);
        if (!target->entity) {
            /* Clear entry on allocation failure */
            target->id = 0;
            db->cache->count--;
            return TQDB_ERR_NO_MEM;
        }
        memcpy(target->entity, entity, trait->struct_size);
    } else {
        target->entity = NULL;
    }

    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Cache Invalidation
 * ═══════════════════════════════════════════════════════════════════════════ */

void tqdb_cache_invalidate(tqdb_t db, uint8_t type_idx, uint32_t id) {
    if (!db || !db->cache || id == 0) return;

    for (size_t i = 0; i < db->cache->capacity; i++) {
        tqdb_cache_entry_t* entry = &db->cache->entries[i];
        if (entry->id != 0 &&
            entry->type_idx == type_idx &&
            entry->id == id) {
            /* Free entity data */
            if (entry->entity) {
                const tqdb_trait_t* trait = db->traits[type_idx];
                if (trait && trait->destroy) {
                    trait->destroy(entry->entity);
                }
                tqdb_dealloc(db, entry->entity);
            }
            /* Mark as empty */
            entry->id = 0;
            entry->entity = NULL;
            db->cache->count--;
            return;
        }
    }
}

void tqdb_cache_invalidate_all(tqdb_t db) {
    if (!db || !db->cache) return;

    for (size_t i = 0; i < db->cache->capacity; i++) {
        tqdb_cache_entry_t* entry = &db->cache->entries[i];
        if (entry->id != 0) {
            if (entry->entity) {
                const tqdb_trait_t* trait = db->traits[entry->type_idx];
                if (trait && trait->destroy) {
                    trait->destroy(entry->entity);
                }
                tqdb_dealloc(db, entry->entity);
            }
            entry->id = 0;
            entry->entity = NULL;
        }
    }
    db->cache->count = 0;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Public API
 * ═══════════════════════════════════════════════════════════════════════════ */

void tqdb_cache_clear(tqdb_t db) {
    tqdb_cache_invalidate_all(db);
    if (db && db->cache) {
        db->cache->hits = 0;
        db->cache->misses = 0;
    }
}

void tqdb_cache_stats(tqdb_t db, size_t* out_hits, size_t* out_misses) {
    if (!db || !db->cache) {
        if (out_hits) *out_hits = 0;
        if (out_misses) *out_misses = 0;
        return;
    }

    if (out_hits) *out_hits = db->cache->hits;
    if (out_misses) *out_misses = db->cache->misses;
}

#endif /* TQDB_ENABLE_CACHE */
