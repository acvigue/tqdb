/**
 * @file tqdb_core.c
 * @brief TQDB core database implementation
 */

#include "tqdb_internal.h"

/* ═══════════════════════════════════════════════════════════════════════════
 * Default Allocator
 * ═══════════════════════════════════════════════════════════════════════════ */

static void* default_malloc(size_t size) {
    return TQDB_MALLOC(size);
}

static void default_free(void* ptr) {
    TQDB_FREE(ptr);
}

static void* default_realloc(void* ptr, size_t size) {
    return TQDB_REALLOC(ptr, size);
}

static const tqdb_alloc_t s_default_alloc = {
    .malloc = default_malloc,
    .free = default_free,
    .realloc = default_realloc
};

/* ═══════════════════════════════════════════════════════════════════════════
 * Helper: Duplicate String
 * ═══════════════════════════════════════════════════════════════════════════ */

static char* tqdb_strdup(tqdb_t db, const char* s) {
    if (!s) return NULL;
    size_t len = strlen(s) + 1;
    char* copy = (char*)tqdb_alloc(db, len);
    if (copy) {
        memcpy(copy, s, len);
    }
    return copy;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Trait Lookup
 * ═══════════════════════════════════════════════════════════════════════════ */

const tqdb_trait_t* tqdb_find_trait(tqdb_t db, const char* name) {
    if (!db || !name) return NULL;
    for (size_t i = 0; i < db->trait_count; i++) {
        if (strcmp(db->traits[i]->name, name) == 0) {
            return db->traits[i];
        }
    }
    return NULL;
}

int tqdb_find_trait_index(tqdb_t db, const char* name) {
    if (!db || !name) return -1;
    for (size_t i = 0; i < db->trait_count; i++) {
        if (strcmp(db->traits[i]->name, name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * File Header I/O
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool write_header(FILE* f, const tqdb_header_t* h) {
    return fwrite(&h->magic, 4, 1, f) == 1
        && fwrite(&h->version, 2, 1, f) == 1
        && fwrite(&h->flags, 2, 1, f) == 1
        && fwrite(&h->crc, 4, 1, f) == 1
        && fwrite(&h->reserved, 4, 1, f) == 1;
}

static bool read_header(FILE* f, tqdb_header_t* h) {
    return fread(&h->magic, 4, 1, f) == 1
        && fread(&h->version, 2, 1, f) == 1
        && fread(&h->flags, 2, 1, f) == 1
        && fread(&h->crc, 4, 1, f) == 1
        && fread(&h->reserved, 4, 1, f) == 1;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * File Recovery
 * ═══════════════════════════════════════════════════════════════════════════ */

static FILE* open_for_read(tqdb_t db) {
    FILE* f = fopen(db->db_path, "rb");
    if (!f) {
        /* Try recovering from temp or backup */
        FILE* tmp = fopen(db->tmp_path, "rb");
        if (tmp) {
            fclose(tmp);
            rename(db->tmp_path, db->db_path);
            f = fopen(db->db_path, "rb");
        } else {
            FILE* bak = fopen(db->bak_path, "rb");
            if (bak) {
                fclose(bak);
                rename(db->bak_path, db->db_path);
                f = fopen(db->db_path, "rb");
            }
        }
    } else {
        /* Clean stale temp file */
        remove(db->tmp_path);
    }

    if (!f) return NULL;

    /* Validate header */
    tqdb_header_t hdr;
    if (!read_header(f, &hdr) || hdr.magic != TQDB_MAGIC || hdr.version > TQDB_VERSION) {
        fclose(f);
        return NULL;
    }

    return f;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Stream Context for Modifications
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    /* Entity to add */
    const tqdb_trait_t* add_trait;
    const void* add_entity;

    /* Entity to delete (by type index and id) */
    int delete_type_idx;
    uint32_t delete_id;

    /* Entity to update */
    int update_type_idx;
    uint32_t update_id;
    const void* update_entity;

    /* Filter callback (for delete_where) */
    int filter_type_idx;
    tqdb_filter_fn filter_fn;
    void* filter_ctx;

    /* Modifier callback (for modify_where) */
    int modify_type_idx;
    tqdb_filter_fn modify_filter_fn;
    void* modify_filter_ctx;
    tqdb_modify_fn modify_fn;
    void* modify_ctx;

    /* Results */
    bool found_target;
} stream_ctx_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * Stream Modify (Core Algorithm)
 * ═══════════════════════════════════════════════════════════════════════════ */

static tqdb_err_t stream_modify(tqdb_t db, const stream_ctx_t* ctx) {
    /* Split scratch buffer: first half read, second half write */
    size_t half = db->scratch_size / 2;
    uint8_t* read_buf = db->scratch;
    uint8_t* write_buf = db->scratch + half;

    /* Open source (may not exist) */
    FILE* src = open_for_read(db);

    /* Open destination */
    FILE* dst = fopen(db->tmp_path, "wb");
    if (!dst) {
        if (src) fclose(src);
        return TQDB_ERR_IO;
    }

    /* Write header placeholder */
    tqdb_header_t hdr = { TQDB_MAGIC, TQDB_VERSION, 0, 0, 0 };
    write_header(dst, &hdr);

    tqdb_writer_t w;
    tqdb_writer_init(&w, dst, write_buf, half);

    /* Read source counts */
    uint32_t counts[TQDB_MAX_ENTITY_TYPES] = {0};
    if (src) {
        for (size_t i = 0; i < db->trait_count; i++) {
            uint32_t c;
            if (fread(&c, 4, 1, src) != 1) break;
            if (c <= db->traits[i]->max_count) {
                counts[i] = c;
            }
        }
    }

    /* Calculate new counts */
    uint32_t new_counts[TQDB_MAX_ENTITY_TYPES];
    memcpy(new_counts, counts, sizeof(counts));

    if (ctx->add_trait) {
        int idx = tqdb_find_trait_index(db, ctx->add_trait->name);
        if (idx >= 0) new_counts[idx]++;
    }
    if (ctx->delete_type_idx >= 0 && ctx->delete_id != 0) {
        new_counts[ctx->delete_type_idx]--;
    }
    /* Note: filter/modify deletions counted during streaming */

    /* Write counts placeholder (will rewrite later if filter modifies counts) */
    long counts_pos = ftell(dst);
    for (size_t i = 0; i < db->trait_count; i++) {
        tqdb_write_u32(&w, new_counts[i]);
    }

    /* Initialize reader if source exists */
    tqdb_reader_t r;
    if (src) {
        tqdb_reader_init(&r, src, read_buf, half);
    }

    /* Track if we need to adjust counts due to filtering */
    uint32_t actual_counts[TQDB_MAX_ENTITY_TYPES];
    memcpy(actual_counts, new_counts, sizeof(new_counts));
    bool counts_changed = false;

    /* Stream each entity type */
    for (size_t type_idx = 0; type_idx < db->trait_count; type_idx++) {
        const tqdb_trait_t* trait = db->traits[type_idx];
        uint32_t n = counts[type_idx];
        uint32_t written = 0;

        /* Allocate temp entity on heap */
        void* entity = tqdb_alloc(db, trait->struct_size);
        if (!entity) {
            if (src) fclose(src);
            fclose(dst);
            remove(db->tmp_path);
            return TQDB_ERR_NO_MEM;
        }

        /* Process existing entities */
        if (src) {
            for (uint32_t i = 0; i < n && !tqdb_read_error(&r); i++) {
                /* Initialize and read */
                if (trait->init) trait->init(entity);
                trait->read(&r, entity);
                if (tqdb_read_error(&r)) break;

                uint32_t id = trait->get_id(entity);

                /* Skip deleted entity */
                if (ctx->delete_type_idx == (int)type_idx && ctx->delete_id != 0 &&
                    id == ctx->delete_id) {
                    if (trait->destroy) trait->destroy(entity);
                    continue;
                }

                /* Apply filter (return false = delete) */
                if (ctx->filter_type_idx == (int)type_idx && ctx->filter_fn) {
                    if (!ctx->filter_fn(entity, ctx->filter_ctx)) {
                        if (trait->destroy) trait->destroy(entity);
                        actual_counts[type_idx]--;
                        counts_changed = true;
                        continue;
                    }
                }

                /* Apply update */
                if (ctx->update_type_idx == (int)type_idx && ctx->update_id != 0 &&
                    id == ctx->update_id) {
                    /* Write updated entity instead */
                    trait->write(&w, ctx->update_entity);
                    written++;
                    if (trait->destroy) trait->destroy(entity);
                    continue;
                }

                /* Apply modifier */
                if (ctx->modify_type_idx == (int)type_idx && ctx->modify_fn) {
                    bool should_modify = true;
                    if (ctx->modify_filter_fn) {
                        should_modify = ctx->modify_filter_fn(entity, ctx->modify_filter_ctx);
                    }
                    if (should_modify) {
                        ctx->modify_fn(entity, ctx->modify_ctx);
                    }
                }

                /* Write entity */
                trait->write(&w, entity);
                written++;

                if (trait->destroy) trait->destroy(entity);
            }
        }

        /* Add new entity if this is the right type */
        if (ctx->add_trait == trait && ctx->add_entity) {
            trait->write(&w, ctx->add_entity);
            written++;
        }

        actual_counts[type_idx] = written;
        tqdb_dealloc(db, entity);
    }

    if (src) fclose(src);

    /* Finalize writer */
    tqdb_writer_flush(&w);
    if (tqdb_write_error(&w)) {
        fclose(dst);
        remove(db->tmp_path);
        return TQDB_ERR_IO;
    }

    /* Rewrite counts if changed */
    if (counts_changed) {
        fseek(dst, counts_pos, SEEK_SET);
        for (size_t i = 0; i < db->trait_count; i++) {
            fwrite(&actual_counts[i], 4, 1, dst);
        }
    }

    /* Patch CRC in header */
    uint32_t crc = tqdb_writer_crc(&w);
    fseek(dst, 8, SEEK_SET);  /* Offset of CRC in header */
    fwrite(&crc, 4, 1, dst);

    fflush(dst);
    fclose(dst);

    /* Atomic swap */
    remove(db->bak_path);
    rename(db->db_path, db->bak_path);
    if (rename(db->tmp_path, db->db_path) != 0) {
        rename(db->bak_path, db->db_path);
        return TQDB_ERR_IO;
    }
    remove(db->bak_path);

    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Lifecycle
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_open(const tqdb_config_t* config, tqdb_t* out_db) {
    if (!config || !config->db_path || !out_db) {
        return TQDB_ERR_INVALID_ARG;
    }

    /* Allocate database struct using provided or default allocator */
    tqdb_alloc_t alloc = config->alloc ? *config->alloc : s_default_alloc;

    tqdb_t db = (tqdb_t)alloc.malloc(sizeof(struct tqdb_s));
    if (!db) return TQDB_ERR_NO_MEM;

    memset(db, 0, sizeof(struct tqdb_s));
    db->alloc = alloc;
    db->owns_alloc = (config->alloc == NULL);

    /* Copy paths */
    db->db_path = tqdb_strdup(db, config->db_path);
    if (!db->db_path) {
        tqdb_close(db);
        return TQDB_ERR_NO_MEM;
    }

    /* Generate tmp/bak paths if not provided */
    if (config->tmp_path) {
        db->tmp_path = tqdb_strdup(db, config->tmp_path);
    } else {
        size_t len = strlen(config->db_path) + 5;
        db->tmp_path = (char*)tqdb_alloc(db, len);
        if (db->tmp_path) {
            snprintf(db->tmp_path, len, "%s.tmp", config->db_path);
        }
    }

    if (config->bak_path) {
        db->bak_path = tqdb_strdup(db, config->bak_path);
    } else {
        size_t len = strlen(config->db_path) + 5;
        db->bak_path = (char*)tqdb_alloc(db, len);
        if (db->bak_path) {
            snprintf(db->bak_path, len, "%s.bak", config->db_path);
        }
    }

    if (!db->tmp_path || !db->bak_path) {
        tqdb_close(db);
        return TQDB_ERR_NO_MEM;
    }

    /* Setup mutex */
    if (config->mutex) {
        db->mutex_ops = config->mutex;
        db->mutex = config->mutex->create();
    }

    /* Setup scratch buffer */
    db->scratch_size = config->scratch_size > 0 ? config->scratch_size : TQDB_DEFAULT_SCRATCH_SIZE;
    if (config->scratch_buf) {
        db->scratch = config->scratch_buf;
        db->owns_scratch = false;
    } else {
        db->scratch = (uint8_t*)tqdb_alloc(db, db->scratch_size);
        db->owns_scratch = true;
        if (!db->scratch) {
            tqdb_close(db);
            return TQDB_ERR_NO_MEM;
        }
    }

#if TQDB_ENABLE_WAL
    /* Setup WAL if enabled (default: enabled if wal_path provided or auto-generate) */
    bool wal_enabled = config->enable_wal || config->wal_path != NULL;
    if (wal_enabled) {
        char* wal_path;
        if (config->wal_path) {
            wal_path = tqdb_strdup(db, config->wal_path);
        } else {
            size_t len = strlen(config->db_path) + 5;
            wal_path = (char*)tqdb_alloc(db, len);
            if (wal_path) {
                snprintf(wal_path, len, "%s.wal", config->db_path);
            }
        }
        if (wal_path) {
            tqdb_err_t err = tqdb_wal_init(db, wal_path,
                config->wal_max_entries, config->wal_max_size);
            tqdb_dealloc(db, wal_path);
            if (err != TQDB_OK) {
                tqdb_close(db);
                return err;
            }
            /* Recover any pending WAL entries */
            err = tqdb_wal_recover(db);
            if (err != TQDB_OK) {
                tqdb_close(db);
                return err;
            }
        }
    }
#endif

#if TQDB_ENABLE_CACHE
    /* Setup cache if enabled */
    if (config->enable_cache) {
        tqdb_err_t err = tqdb_cache_init(db, config->cache_size);
        if (err != TQDB_OK) {
            tqdb_close(db);
            return err;
        }
    }
#endif

    *out_db = db;
    return TQDB_OK;
}

void tqdb_close(tqdb_t db) {
    if (!db) return;

#if TQDB_ENABLE_WAL
    /* Checkpoint any pending WAL entries before closing */
    if (db->wal.enabled && db->wal.entry_count > 0) {
        tqdb_wal_checkpoint_internal(db);
    }

    /* Destroy WAL */
    tqdb_wal_destroy(db);
#endif

#if TQDB_ENABLE_CACHE
    /* Destroy cache */
    tqdb_cache_destroy(db);
#endif

    if (db->mutex_ops && db->mutex) {
        db->mutex_ops->destroy(db->mutex);
    }

    if (db->owns_scratch && db->scratch) {
        tqdb_dealloc(db, db->scratch);
    }

    tqdb_dealloc(db, db->db_path);
    tqdb_dealloc(db, db->tmp_path);
    tqdb_dealloc(db, db->bak_path);

    db->alloc.free(db);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Entity Registration
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_register(tqdb_t db, const tqdb_trait_t* trait) {
    if (!db || !trait || !trait->name) return TQDB_ERR_INVALID_ARG;
    if (!trait->write || !trait->read || !trait->get_id || !trait->set_id) {
        return TQDB_ERR_INVALID_ARG;
    }

    if (db->trait_count >= TQDB_MAX_ENTITY_TYPES) {
        return TQDB_ERR_FULL;
    }

    /* Check for duplicate name */
    if (tqdb_find_trait(db, trait->name)) {
        return TQDB_ERR_EXISTS;
    }

    size_t idx = db->trait_count++;
    db->traits[idx] = trait;

    /* Initialize next_id for this type by scanning existing DB */
    db->next_id[idx] = 1;  /* Start at 1, 0 is reserved for "no ID" */

    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * CRUD Operations
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_add(tqdb_t db, const char* type, void* entity) {
    if (!db || !type || !entity) return TQDB_ERR_INVALID_ARG;

#if TQDB_ENABLE_WAL
    /* Check for deferred WAL recovery */
    tqdb_wal_check_recovery(db);
#endif

    const tqdb_trait_t* trait = tqdb_find_trait(db, type);
    if (!trait) return TQDB_ERR_NOT_REGISTERED;

    int type_idx = tqdb_find_trait_index(db, type);

    if (!tqdb_lock(db)) return TQDB_ERR_TIMEOUT;

    /* Auto-generate ID */
    uint32_t new_id = db->next_id[type_idx]++;
    trait->set_id(entity, new_id);

#if TQDB_ENABLE_WAL
    /* If WAL enabled, append to WAL */
    if (db->wal.enabled) {
        tqdb_err_t err = tqdb_wal_append(db, TQDB_WAL_OP_ADD,
                                          (uint8_t)type_idx, new_id, entity);
        tqdb_unlock(db);
        return err;
    }
#endif

    /* Fallback: direct write without WAL */
    stream_ctx_t ctx = {0};
    ctx.add_trait = trait;
    ctx.add_entity = entity;
    ctx.delete_type_idx = -1;
    ctx.update_type_idx = -1;
    ctx.filter_type_idx = -1;
    ctx.modify_type_idx = -1;

    tqdb_err_t err = stream_modify(db, &ctx);

    tqdb_unlock(db);
    return err;
}

tqdb_err_t tqdb_get(tqdb_t db, const char* type, uint32_t id, void* out) {
    if (!db || !type || id == 0 || !out) return TQDB_ERR_INVALID_ARG;

#if TQDB_ENABLE_WAL
    /* Check for deferred WAL recovery */
    tqdb_wal_check_recovery(db);
#endif

    const tqdb_trait_t* trait = tqdb_find_trait(db, type);
    if (!trait) return TQDB_ERR_NOT_REGISTERED;

    int type_idx = tqdb_find_trait_index(db, type);

    if (!tqdb_lock(db)) return TQDB_ERR_TIMEOUT;

#if TQDB_ENABLE_CACHE
    /* 1. Check cache first */
    if (db->cache) {
        tqdb_cache_entry_t* cached = tqdb_cache_get(db, (uint8_t)type_idx, id);
        if (cached) {
            if (cached->op == TQDB_WAL_OP_DELETE) {
                /* Cached as deleted */
                tqdb_unlock(db);
                return TQDB_ERR_NOT_FOUND;
            }
            if (cached->entity) {
                /* Copy from cache */
                if (trait->init) trait->init(out);
                memcpy(out, cached->entity, trait->struct_size);
                tqdb_unlock(db);
                return TQDB_OK;
            }
        }
    }
#endif

#if TQDB_ENABLE_WAL
    /* 2. Check WAL */
    if (db->wal.enabled && db->wal.entry_count > 0) {
        uint8_t wal_op;
        tqdb_err_t wal_result = tqdb_wal_find(db, (uint8_t)type_idx, id, &wal_op, out);
        if (wal_result == TQDB_OK) {
#if TQDB_ENABLE_CACHE
            /* Found in WAL, update cache */
            if (db->cache) {
                tqdb_cache_put(db, (uint8_t)type_idx, id, out, wal_op);
            }
#endif
            tqdb_unlock(db);
            return TQDB_OK;
        }
        /* Check if explicitly deleted in WAL */
        if (wal_result == TQDB_ERR_NOT_FOUND && wal_op == TQDB_WAL_OP_DELETE) {
            tqdb_unlock(db);
            return TQDB_ERR_NOT_FOUND;
        }
    }
#endif

    /* 3. Check main database file */
    FILE* f = open_for_read(db);
    if (!f) {
        tqdb_unlock(db);
        return TQDB_ERR_NOT_FOUND;
    }

    /* Read counts */
    uint32_t counts[TQDB_MAX_ENTITY_TYPES] = {0};
    for (size_t i = 0; i < db->trait_count; i++) {
        fread(&counts[i], 4, 1, f);
    }

    tqdb_reader_t r;
    tqdb_reader_init(&r, f, db->scratch, db->scratch_size);

    tqdb_err_t result = TQDB_ERR_NOT_FOUND;

    /* Skip to target type */
    for (int i = 0; i < type_idx; i++) {
        const tqdb_trait_t* t = db->traits[i];
        for (uint32_t j = 0; j < counts[i] && !tqdb_read_error(&r); j++) {
            if (t->skip) {
                t->skip(&r);
            } else {
                /* Must read and discard */
                void* tmp = tqdb_alloc(db, t->struct_size);
                if (tmp) {
                    if (t->init) t->init(tmp);
                    t->read(&r, tmp);
                    if (t->destroy) t->destroy(tmp);
                    tqdb_dealloc(db, tmp);
                }
            }
        }
    }

    /* Search in target type */
    if (trait->init) trait->init(out);
    for (uint32_t i = 0; i < counts[type_idx] && !tqdb_read_error(&r); i++) {
        trait->read(&r, out);
        if (trait->get_id(out) == id) {
            result = TQDB_OK;
#if TQDB_ENABLE_CACHE
            /* Update cache */
            if (db->cache) {
                tqdb_cache_put(db, (uint8_t)type_idx, id, out, 1 /* ADD */);
            }
#endif
            break;
        }
        /* Reset for next read */
        if (trait->destroy) trait->destroy(out);
        if (trait->init) trait->init(out);
    }

    fclose(f);
    tqdb_unlock(db);
    return result;
}

tqdb_err_t tqdb_update(tqdb_t db, const char* type, uint32_t id, const void* entity) {
    if (!db || !type || id == 0 || !entity) return TQDB_ERR_INVALID_ARG;

#if TQDB_ENABLE_WAL
    /* Check for deferred WAL recovery */
    tqdb_wal_check_recovery(db);
#endif

    const tqdb_trait_t* trait = tqdb_find_trait(db, type);
    if (!trait) return TQDB_ERR_NOT_REGISTERED;

    int type_idx = tqdb_find_trait_index(db, type);

    /* Check existence first */
    if (!tqdb_exists(db, type, id)) {
        return TQDB_ERR_NOT_FOUND;
    }

    if (!tqdb_lock(db)) return TQDB_ERR_TIMEOUT;

#if TQDB_ENABLE_WAL
    /* If WAL enabled, append to WAL */
    if (db->wal.enabled) {
        tqdb_err_t err = tqdb_wal_append(db, TQDB_WAL_OP_UPDATE,
                                          (uint8_t)type_idx, id, entity);
        tqdb_unlock(db);
        return err;
    }
#endif

    /* Fallback: direct write without WAL */
    stream_ctx_t ctx = {0};
    ctx.update_type_idx = type_idx;
    ctx.update_id = id;
    ctx.update_entity = entity;
    ctx.delete_type_idx = -1;
    ctx.filter_type_idx = -1;
    ctx.modify_type_idx = -1;

    tqdb_err_t err = stream_modify(db, &ctx);

    tqdb_unlock(db);
    return err;
}

tqdb_err_t tqdb_delete(tqdb_t db, const char* type, uint32_t id) {
    if (!db || !type || id == 0) return TQDB_ERR_INVALID_ARG;

#if TQDB_ENABLE_WAL
    /* Check for deferred WAL recovery */
    tqdb_wal_check_recovery(db);
#endif

    const tqdb_trait_t* trait = tqdb_find_trait(db, type);
    if (!trait) return TQDB_ERR_NOT_REGISTERED;
    (void)trait;

    int type_idx = tqdb_find_trait_index(db, type);

    if (!tqdb_exists(db, type, id)) {
        return TQDB_ERR_NOT_FOUND;
    }

    if (!tqdb_lock(db)) return TQDB_ERR_TIMEOUT;

#if TQDB_ENABLE_WAL
    /* If WAL enabled, append to WAL */
    if (db->wal.enabled) {
        tqdb_err_t err = tqdb_wal_append(db, TQDB_WAL_OP_DELETE,
                                          (uint8_t)type_idx, id, NULL);
        tqdb_unlock(db);
        return err;
    }
#endif

    /* Fallback: direct write without WAL */
    stream_ctx_t ctx = {0};
    ctx.delete_type_idx = type_idx;
    ctx.delete_id = id;
    ctx.update_type_idx = -1;
    ctx.filter_type_idx = -1;
    ctx.modify_type_idx = -1;

    tqdb_err_t err = stream_modify(db, &ctx);

    tqdb_unlock(db);
    return err;
}

bool tqdb_exists(tqdb_t db, const char* type, uint32_t id) {
    if (!db || !type || id == 0) return false;

#if TQDB_ENABLE_WAL
    /* Check for deferred WAL recovery */
    tqdb_wal_check_recovery(db);
#endif

    const tqdb_trait_t* trait = tqdb_find_trait(db, type);
    if (!trait) return false;

    int type_idx = tqdb_find_trait_index(db, type);

#if TQDB_ENABLE_CACHE
    /* Check cache first for deleted marker */
    if (db->cache) {
        tqdb_cache_entry_t* cached = tqdb_cache_get(db, (uint8_t)type_idx, id);
        if (cached && cached->op == TQDB_WAL_OP_DELETE) {
            return false;
        }
        if (cached && cached->entity) {
            return true;
        }
    }
#endif

#if TQDB_ENABLE_WAL
    /* Check WAL for existence or deletion */
    if (db->wal.enabled && db->wal.entry_count > 0) {
        uint8_t wal_op = 0;
        tqdb_err_t wal_result = tqdb_wal_find(db, (uint8_t)type_idx, id, &wal_op, NULL);
        if (wal_result == TQDB_OK) {
            return true;  /* Found in WAL (add or update) */
        }
        if (wal_op == TQDB_WAL_OP_DELETE) {
            return false;  /* Explicitly deleted in WAL */
        }
    }
#endif

    /* Check main database */
    void* tmp = tqdb_alloc(db, trait->struct_size);
    if (!tmp) return false;

    /* Use internal get that only checks main DB */
    if (!tqdb_lock(db)) {
        tqdb_dealloc(db, tmp);
        return false;
    }

    FILE* f = open_for_read(db);
    if (!f) {
        tqdb_unlock(db);
        tqdb_dealloc(db, tmp);
        return false;
    }

    /* Read counts */
    uint32_t counts[TQDB_MAX_ENTITY_TYPES] = {0};
    for (size_t i = 0; i < db->trait_count; i++) {
        fread(&counts[i], 4, 1, f);
    }

    tqdb_reader_t r;
    tqdb_reader_init(&r, f, db->scratch, db->scratch_size);

    bool found = false;

    /* Skip to target type */
    for (int i = 0; i < type_idx; i++) {
        const tqdb_trait_t* t = db->traits[i];
        for (uint32_t j = 0; j < counts[i] && !tqdb_read_error(&r); j++) {
            if (t->skip) {
                t->skip(&r);
            } else {
                void* skip_tmp = tqdb_alloc(db, t->struct_size);
                if (skip_tmp) {
                    if (t->init) t->init(skip_tmp);
                    t->read(&r, skip_tmp);
                    if (t->destroy) t->destroy(skip_tmp);
                    tqdb_dealloc(db, skip_tmp);
                }
            }
        }
    }

    /* Search in target type */
    if (trait->init) trait->init(tmp);
    for (uint32_t i = 0; i < counts[type_idx] && !tqdb_read_error(&r); i++) {
        trait->read(&r, tmp);
        if (trait->get_id(tmp) == id) {
            found = true;
            break;
        }
        if (trait->destroy) trait->destroy(tmp);
        if (trait->init) trait->init(tmp);
    }

    if (trait->destroy) trait->destroy(tmp);
    tqdb_dealloc(db, tmp);
    fclose(f);
    tqdb_unlock(db);

    return found;
}

size_t tqdb_count(tqdb_t db, const char* type) {
    if (!db || !type) return 0;

#if TQDB_ENABLE_WAL
    /* Check for deferred WAL recovery */
    tqdb_wal_check_recovery(db);
#endif

    int type_idx = tqdb_find_trait_index(db, type);
    if (type_idx < 0) return 0;

    if (!tqdb_lock(db)) return 0;

    /* Get count from main DB file */
    uint32_t count = 0;
    FILE* f = open_for_read(db);
    if (f) {
        fseek(f, TQDB_HEADER_SIZE + type_idx * 4, SEEK_SET);
        fread(&count, 4, 1, f);
        fclose(f);
    }

#if TQDB_ENABLE_WAL
    /* Adjust for WAL entries if WAL is enabled */
    if (db->wal.enabled && db->wal.entry_count > 0 && db->wal.path) {
        FILE* wal = fopen(db->wal.path, "rb");
        if (wal) {
            /* Skip WAL header */
            fseek(wal, TQDB_WAL_HEADER_SIZE, SEEK_SET);

            /* Track IDs we've seen to handle duplicates */
            uint32_t* seen_ids = NULL;
            uint8_t* seen_ops = NULL;
            size_t seen_count = 0;
            size_t seen_capacity = 0;

            for (uint32_t i = 0; i < db->wal.entry_count; i++) {
                uint32_t entry_crc;
                uint8_t op, tidx;
                uint32_t entry_id;

                if (fread(&entry_crc, 4, 1, wal) != 1) break;
                if (fread(&op, 1, 1, wal) != 1) break;
                if (fread(&tidx, 1, 1, wal) != 1) break;
                if (fread(&entry_id, 4, 1, wal) != 1) break;

                uint32_t data_len;
                if (fread(&data_len, 4, 1, wal) != 1) break;

                /* Skip entity data */
                if (data_len > 0) {
                    fseek(wal, data_len, SEEK_CUR);
                }

                /* Only process entries for our type */
                if (tidx != (uint8_t)type_idx) continue;

                /* Check if we've seen this ID before */
                bool found = false;
                size_t found_idx = 0;
                for (size_t j = 0; j < seen_count; j++) {
                    if (seen_ids[j] == entry_id) {
                        found = true;
                        found_idx = j;
                        break;
                    }
                }

                if (found) {
                    /* Update the operation for this ID */
                    seen_ops[found_idx] = op;
                } else {
                    /* Add new ID */
                    if (seen_count >= seen_capacity) {
                        size_t new_cap = seen_capacity == 0 ? 16 : seen_capacity * 2;
                        uint32_t* new_ids = (uint32_t*)tqdb_alloc(db, new_cap * sizeof(uint32_t));
                        uint8_t* new_ops = (uint8_t*)tqdb_alloc(db, new_cap);
                        if (!new_ids || !new_ops) {
                            if (new_ids) tqdb_dealloc(db, new_ids);
                            if (new_ops) tqdb_dealloc(db, new_ops);
                            break;
                        }
                        if (seen_ids) {
                            memcpy(new_ids, seen_ids, seen_count * sizeof(uint32_t));
                            memcpy(new_ops, seen_ops, seen_count);
                            tqdb_dealloc(db, seen_ids);
                            tqdb_dealloc(db, seen_ops);
                        }
                        seen_ids = new_ids;
                        seen_ops = new_ops;
                        seen_capacity = new_cap;
                    }
                    seen_ids[seen_count] = entry_id;
                    seen_ops[seen_count] = op;
                    seen_count++;
                }
            }
            fclose(wal);

            /* Now calculate count adjustment */
            /* For each unique ID in WAL:
             * - ADD that doesn't exist in DB: +1
             * - DELETE that exists in DB: -1
             * - UPDATE: no change
             */
            for (size_t j = 0; j < seen_count; j++) {
                if (seen_ops[j] == TQDB_WAL_OP_ADD) {
                    count++;
                } else if (seen_ops[j] == TQDB_WAL_OP_DELETE) {
                    if (count > 0) count--;
                }
                /* UPDATE doesn't change count */
            }
            if (seen_ids) tqdb_dealloc(db, seen_ids);
            if (seen_ops) tqdb_dealloc(db, seen_ops);
        }
    }
#endif /* TQDB_ENABLE_WAL */

    tqdb_unlock(db);
    return count;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Iteration
 * ═══════════════════════════════════════════════════════════════════════════ */

#if TQDB_ENABLE_WAL
/**
 * WAL entry tracking for foreach iteration
 */
typedef struct {
    uint32_t* ids;          /* Array of IDs in WAL */
    uint8_t* ops;           /* Array of operations */
    void** entities;        /* Array of entity data (NULL for deletes) */
    size_t count;
    size_t capacity;
} wal_id_set_t;

static void wal_id_set_init(wal_id_set_t* set) {
    memset(set, 0, sizeof(wal_id_set_t));
}

static void wal_id_set_destroy(tqdb_t db, wal_id_set_t* set, const tqdb_trait_t* trait) {
    if (set->ids) {
        tqdb_dealloc(db, set->ids);
    }
    if (set->entities) {
        for (size_t i = 0; i < set->count; i++) {
            if (set->entities[i]) {
                if (trait && trait->destroy) trait->destroy(set->entities[i]);
                tqdb_dealloc(db, set->entities[i]);
            }
        }
        tqdb_dealloc(db, set->entities);
    }
    if (set->ops) tqdb_dealloc(db, set->ops);
}

static int wal_id_set_find(wal_id_set_t* set, uint32_t id) {
    for (size_t i = 0; i < set->count; i++) {
        if (set->ids[i] == id) {
            return (int)i;
        }
    }
    return -1;
}

static bool wal_id_set_add(tqdb_t db, wal_id_set_t* set, uint32_t id,
                           uint8_t op, void* entity) {
    int idx = wal_id_set_find(set, id);
    if (idx >= 0) {
        /* Update existing entry */
        set->ops[idx] = op;
        if (set->entities[idx]) {
            tqdb_dealloc(db, set->entities[idx]);
        }
        set->entities[idx] = entity;
        return true;
    }

    /* Add new entry */
    if (set->count >= set->capacity) {
        size_t new_cap = set->capacity == 0 ? 16 : set->capacity * 2;
        uint32_t* new_ids = (uint32_t*)tqdb_alloc(db, new_cap * sizeof(uint32_t));
        uint8_t* new_ops = (uint8_t*)tqdb_alloc(db, new_cap);
        void** new_entities = (void**)tqdb_alloc(db, new_cap * sizeof(void*));

        if (!new_ids || !new_ops || !new_entities) {
            if (new_ids) tqdb_dealloc(db, new_ids);
            if (new_ops) tqdb_dealloc(db, new_ops);
            if (new_entities) tqdb_dealloc(db, new_entities);
            return false;
        }

        memset(new_ids, 0, new_cap * sizeof(uint32_t));
        memset(new_ops, 0, new_cap);
        memset(new_entities, 0, new_cap * sizeof(void*));

        if (set->ids) {
            memcpy(new_ids, set->ids, set->count * sizeof(uint32_t));
            memcpy(new_ops, set->ops, set->count);
            memcpy(new_entities, set->entities, set->count * sizeof(void*));
            tqdb_dealloc(db, set->ids);
            tqdb_dealloc(db, set->ops);
            tqdb_dealloc(db, set->entities);
        }

        set->ids = new_ids;
        set->ops = new_ops;
        set->entities = new_entities;
        set->capacity = new_cap;
    }

    set->ids[set->count] = id;
    set->ops[set->count] = op;
    set->entities[set->count] = entity;
    set->count++;

    return true;
}

/**
 * Load WAL entries for a specific type into the ID set
 */
static tqdb_err_t load_wal_entries(tqdb_t db, int type_idx, const tqdb_trait_t* trait,
                                    wal_id_set_t* set) {
    if (!db->wal.enabled || db->wal.entry_count == 0 || !db->wal.path) {
        return TQDB_OK;
    }

    FILE* wal = fopen(db->wal.path, "rb");
    if (!wal) return TQDB_OK;

    /* Skip WAL header */
    fseek(wal, TQDB_WAL_HEADER_SIZE, SEEK_SET);

    size_t half = db->scratch_size / 2;

    for (uint32_t i = 0; i < db->wal.entry_count; i++) {
        uint32_t entry_crc;
        uint8_t op, tidx;
        uint32_t entry_id;

        if (fread(&entry_crc, 4, 1, wal) != 1) break;
        if (fread(&op, 1, 1, wal) != 1) break;
        if (fread(&tidx, 1, 1, wal) != 1) break;
        if (fread(&entry_id, 4, 1, wal) != 1) break;

        uint32_t data_len;
        if (fread(&data_len, 4, 1, wal) != 1) break;

        /* Only process entries for our type */
        if (tidx != (uint8_t)type_idx) {
            if (data_len > 0) fseek(wal, data_len, SEEK_CUR);
            continue;
        }

        void* entity = NULL;
        if (op != TQDB_WAL_OP_DELETE && data_len > 0) {
            long entity_start = ftell(wal);
            entity = tqdb_alloc(db, trait->struct_size);
            if (entity) {
                tqdb_reader_t r;
                tqdb_reader_init(&r, wal, db->scratch, half);
                if (trait->init) trait->init(entity);
                trait->read(&r, entity);
                if (tqdb_read_error(&r)) {
                    if (trait->destroy) trait->destroy(entity);
                    tqdb_dealloc(db, entity);
                    entity = NULL;
                }
            }
            fseek(wal, entity_start + data_len, SEEK_SET);
        } else if (data_len > 0) {
            fseek(wal, data_len, SEEK_CUR);
        }

        wal_id_set_add(db, set, entry_id, op, entity);
    }

    fclose(wal);
    return TQDB_OK;
}
#endif /* TQDB_ENABLE_WAL */

tqdb_err_t tqdb_foreach(tqdb_t db, const char* type, tqdb_iter_fn fn, void* ctx) {
    if (!db || !type || !fn) return TQDB_ERR_INVALID_ARG;

#if TQDB_ENABLE_WAL
    /* Check for deferred WAL recovery */
    tqdb_wal_check_recovery(db);
#endif

    const tqdb_trait_t* trait = tqdb_find_trait(db, type);
    if (!trait) return TQDB_ERR_NOT_REGISTERED;

    int type_idx = tqdb_find_trait_index(db, type);

    if (!tqdb_lock(db)) return TQDB_ERR_TIMEOUT;

#if TQDB_ENABLE_WAL
    /* Load WAL entries for this type */
    wal_id_set_t wal_set;
    wal_id_set_init(&wal_set);
    load_wal_entries(db, type_idx, trait, &wal_set);
#endif

    /* Open main DB file */
    FILE* f = open_for_read(db);
    uint32_t db_count = 0;

    if (f) {
        /* Read counts */
        uint32_t counts[TQDB_MAX_ENTITY_TYPES] = {0};
        for (size_t i = 0; i < db->trait_count; i++) {
            fread(&counts[i], 4, 1, f);
        }
        db_count = counts[type_idx];

        tqdb_reader_t r;
        tqdb_reader_init(&r, f, db->scratch, db->scratch_size);

        /* Skip to target type */
        for (int i = 0; i < type_idx; i++) {
            const tqdb_trait_t* t = db->traits[i];
            for (uint32_t j = 0; j < counts[i] && !tqdb_read_error(&r); j++) {
                if (t->skip) {
                    t->skip(&r);
                } else {
                    void* tmp = tqdb_alloc(db, t->struct_size);
                    if (tmp) {
                        if (t->init) t->init(tmp);
                        t->read(&r, tmp);
                        if (t->destroy) t->destroy(tmp);
                        tqdb_dealloc(db, tmp);
                    }
                }
            }
        }

        /* Iterate main DB entries */
        void* entity = tqdb_alloc(db, trait->struct_size);
        if (entity) {
            bool stop = false;
            for (uint32_t i = 0; i < db_count && !tqdb_read_error(&r) && !stop; i++) {
                if (trait->init) trait->init(entity);
                trait->read(&r, entity);
                if (tqdb_read_error(&r)) break;

#if TQDB_ENABLE_WAL
                uint32_t entity_id = trait->get_id(entity);
                int wal_idx = wal_id_set_find(&wal_set, entity_id);

                if (wal_idx >= 0) {
                    /* Entity is in WAL - check operation */
                    if (wal_set.ops[wal_idx] == TQDB_WAL_OP_DELETE) {
                        /* Deleted - skip */
                    } else if (wal_set.ops[wal_idx] == TQDB_WAL_OP_UPDATE) {
                        /* Updated - use WAL version */
                        if (wal_set.entities[wal_idx]) {
                            if (!fn(wal_set.entities[wal_idx], ctx)) stop = true;
                        }
                    }
                    /* Mark as processed so we don't iterate it again */
                    wal_set.ids[wal_idx] = 0;
                } else {
                    /* Not in WAL - use main DB version */
                    if (!fn(entity, ctx)) stop = true;
                }
#else
                /* No WAL - just use main DB version */
                if (!fn(entity, ctx)) stop = true;
#endif

                if (trait->destroy) trait->destroy(entity);
            }
            tqdb_dealloc(db, entity);
        }
        fclose(f);
    }

#if TQDB_ENABLE_WAL
    /* Iterate WAL ADD entries (ones not already processed) */
    bool stop = false;
    for (size_t i = 0; i < wal_set.count && !stop; i++) {
        if (wal_set.ids[i] != 0 &&
            wal_set.ops[i] == TQDB_WAL_OP_ADD &&
            wal_set.entities[i] != NULL) {
            if (!fn(wal_set.entities[i], ctx)) stop = true;
        }
    }

    wal_id_set_destroy(db, &wal_set, trait);
#endif

    tqdb_unlock(db);

    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Batch Operations
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_modify_where(tqdb_t db, const char* type,
                             tqdb_filter_fn filter, void* filter_ctx,
                             tqdb_modify_fn modify, void* modify_ctx) {
    if (!db || !type || !modify) return TQDB_ERR_INVALID_ARG;

    int type_idx = tqdb_find_trait_index(db, type);
    if (type_idx < 0) return TQDB_ERR_NOT_REGISTERED;

    if (!tqdb_lock(db)) return TQDB_ERR_TIMEOUT;

    stream_ctx_t ctx = {0};
    ctx.delete_type_idx = -1;
    ctx.update_type_idx = -1;
    ctx.filter_type_idx = -1;
    ctx.modify_type_idx = type_idx;
    ctx.modify_filter_fn = filter;
    ctx.modify_filter_ctx = filter_ctx;
    ctx.modify_fn = modify;
    ctx.modify_ctx = modify_ctx;

    tqdb_err_t err = stream_modify(db, &ctx);

    tqdb_unlock(db);
    return err;
}

tqdb_err_t tqdb_delete_where(tqdb_t db, const char* type,
                             tqdb_filter_fn filter, void* ctx) {
    if (!db || !type || !filter) return TQDB_ERR_INVALID_ARG;

    int type_idx = tqdb_find_trait_index(db, type);
    if (type_idx < 0) return TQDB_ERR_NOT_REGISTERED;

    if (!tqdb_lock(db)) return TQDB_ERR_TIMEOUT;

    stream_ctx_t sctx = {0};
    sctx.delete_type_idx = -1;
    sctx.update_type_idx = -1;
    sctx.filter_type_idx = type_idx;
    sctx.filter_fn = filter;
    sctx.filter_ctx = ctx;
    sctx.modify_type_idx = -1;

    tqdb_err_t err = stream_modify(db, &sctx);

    tqdb_unlock(db);
    return err;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Maintenance
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_vacuum(tqdb_t db) {
    if (!db) return TQDB_ERR_INVALID_ARG;

    /* Vacuum is just a stream_modify with no operations - rewrites the file */
    if (!tqdb_lock(db)) return TQDB_ERR_TIMEOUT;

    stream_ctx_t ctx = {0};
    ctx.delete_type_idx = -1;
    ctx.update_type_idx = -1;
    ctx.filter_type_idx = -1;
    ctx.modify_type_idx = -1;

    tqdb_err_t err = stream_modify(db, &ctx);

    tqdb_unlock(db);
    return err;
}

tqdb_err_t tqdb_flush(tqdb_t db) {
    /* Currently a no-op since we write atomically */
    (void)db;
    return TQDB_OK;
}

#if TQDB_ENABLE_WAL
/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Checkpoint Merge
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * WAL entry for in-memory replay
 */
typedef struct {
    uint8_t op;
    uint8_t type_idx;
    uint32_t id;
    void* entity;  /* NULL for deletes */
} wal_replay_entry_t;

/**
 * Merge WAL entries into main database.
 * Called by tqdb_wal_checkpoint_internal().
 */
tqdb_err_t tqdb_checkpoint_merge(tqdb_t db) {
    if (!db || !db->wal.enabled || !db->wal.path) return TQDB_OK;
    if (db->wal.entry_count == 0) return TQDB_OK;

    /* Read all WAL entries into memory */
    FILE* wal = fopen(db->wal.path, "rb");
    if (!wal) return TQDB_ERR_IO;

    /* Skip WAL header */
    fseek(wal, TQDB_WAL_HEADER_SIZE, SEEK_SET);

    /* Allocate array for WAL entries */
    wal_replay_entry_t* entries = (wal_replay_entry_t*)tqdb_alloc(db,
        db->wal.entry_count * sizeof(wal_replay_entry_t));
    if (!entries) {
        fclose(wal);
        return TQDB_ERR_NO_MEM;
    }
    memset(entries, 0, db->wal.entry_count * sizeof(wal_replay_entry_t));

    size_t half = db->scratch_size / 2;
    uint32_t valid_entries = 0;

    /* Read WAL entries */
    for (uint32_t i = 0; i < db->wal.entry_count && valid_entries < db->wal.entry_count; i++) {
        uint32_t entry_crc;
        uint8_t op, type_idx;
        uint32_t entry_id;

        if (fread(&entry_crc, 4, 1, wal) != 1) break;
        if (fread(&op, 1, 1, wal) != 1) break;
        if (fread(&type_idx, 1, 1, wal) != 1) break;
        if (fread(&entry_id, 4, 1, wal) != 1) break;

        if (type_idx >= db->trait_count) {
            /* Skip invalid entry */
            uint32_t data_len;
            fread(&data_len, 4, 1, wal);
            fseek(wal, data_len, SEEK_CUR);
            continue;
        }

        uint32_t data_len;
        if (fread(&data_len, 4, 1, wal) != 1) break;

        entries[valid_entries].op = op;
        entries[valid_entries].type_idx = type_idx;
        entries[valid_entries].id = entry_id;

        /* Read entity data for non-deletes */
        if (op != TQDB_WAL_OP_DELETE && data_len > 0) {
            const tqdb_trait_t* trait = db->traits[type_idx];
            /* Remember position before entity data */
            long entity_start = ftell(wal);
            entries[valid_entries].entity = tqdb_alloc(db, trait->struct_size);
            if (entries[valid_entries].entity) {
                tqdb_reader_t r;
                tqdb_reader_init(&r, wal, db->scratch, half);
                if (trait->init) trait->init(entries[valid_entries].entity);
                trait->read(&r, entries[valid_entries].entity);
                if (tqdb_read_error(&r)) {
                    if (trait->destroy) trait->destroy(entries[valid_entries].entity);
                    tqdb_dealloc(db, entries[valid_entries].entity);
                    entries[valid_entries].entity = NULL;
                }
            }
            /* Seek to correct position after entity (buffered reader may overshoot) */
            fseek(wal, entity_start + data_len, SEEK_SET);
        } else if (data_len > 0) {
            fseek(wal, data_len, SEEK_CUR);
        }

        valid_entries++;
    }
    fclose(wal);

    /* Deduplicate: keep only the last operation for each ID */
    for (uint32_t i = 0; i < valid_entries; i++) {
        if (entries[i].id == 0) continue;

        for (uint32_t j = i + 1; j < valid_entries; j++) {
            if (entries[j].id != 0 &&
                entries[i].type_idx == entries[j].type_idx &&
                entries[i].id == entries[j].id) {
                /* Newer entry found, invalidate older */
                if (entries[i].entity) {
                    const tqdb_trait_t* trait = db->traits[entries[i].type_idx];
                    if (trait && trait->destroy) trait->destroy(entries[i].entity);
                    tqdb_dealloc(db, entries[i].entity);
                }
                entries[i].id = 0;
                entries[i].entity = NULL;
                break;
            }
        }
    }

    /* Now stream_modify with WAL entries applied */
    size_t read_half = db->scratch_size / 2;
    uint8_t* read_buf = db->scratch;
    uint8_t* write_buf = db->scratch + read_half;

    FILE* src = open_for_read(db);
    FILE* dst = fopen(db->tmp_path, "wb");
    if (!dst) {
        /* Free entries */
        for (uint32_t i = 0; i < valid_entries; i++) {
            if (entries[i].entity) {
                const tqdb_trait_t* trait = db->traits[entries[i].type_idx];
                if (trait && trait->destroy) trait->destroy(entries[i].entity);
                tqdb_dealloc(db, entries[i].entity);
            }
        }
        tqdb_dealloc(db, entries);
        if (src) fclose(src);
        return TQDB_ERR_IO;
    }

    /* Write header placeholder */
    tqdb_header_t hdr = { TQDB_MAGIC, TQDB_VERSION, 0, 0, 0 };
    write_header(dst, &hdr);

    tqdb_writer_t w;
    tqdb_writer_init(&w, dst, write_buf, read_half);

    /* Read source counts */
    uint32_t counts[TQDB_MAX_ENTITY_TYPES] = {0};
    if (src) {
        for (size_t i = 0; i < db->trait_count; i++) {
            uint32_t c;
            if (fread(&c, 4, 1, src) != 1) break;
            if (c <= db->traits[i]->max_count) {
                counts[i] = c;
            }
        }
    }

    /* Calculate new counts based on WAL operations */
    uint32_t new_counts[TQDB_MAX_ENTITY_TYPES];
    memcpy(new_counts, counts, sizeof(counts));

    for (uint32_t i = 0; i < valid_entries; i++) {
        if (entries[i].id == 0) continue;
        if (entries[i].op == TQDB_WAL_OP_ADD) {
            new_counts[entries[i].type_idx]++;
        } else if (entries[i].op == TQDB_WAL_OP_DELETE) {
            if (new_counts[entries[i].type_idx] > 0) {
                new_counts[entries[i].type_idx]--;
            }
        }
        /* UPDATE doesn't change count */
    }

    /* Write counts placeholder */
    long counts_pos = ftell(dst);
    for (size_t i = 0; i < db->trait_count; i++) {
        tqdb_write_u32(&w, new_counts[i]);
    }

    tqdb_reader_t r;
    if (src) {
        tqdb_reader_init(&r, src, read_buf, read_half);
    }

    uint32_t actual_counts[TQDB_MAX_ENTITY_TYPES];
    memset(actual_counts, 0, sizeof(actual_counts));

    /* Stream each entity type */
    for (size_t type_idx = 0; type_idx < db->trait_count; type_idx++) {
        const tqdb_trait_t* trait = db->traits[type_idx];
        uint32_t n = counts[type_idx];

        void* entity = tqdb_alloc(db, trait->struct_size);
        if (!entity) {
            /* Cleanup and fail */
            for (uint32_t i = 0; i < valid_entries; i++) {
                if (entries[i].entity) {
                    const tqdb_trait_t* t = db->traits[entries[i].type_idx];
                    if (t && t->destroy) t->destroy(entries[i].entity);
                    tqdb_dealloc(db, entries[i].entity);
                }
            }
            tqdb_dealloc(db, entries);
            if (src) fclose(src);
            fclose(dst);
            remove(db->tmp_path);
            return TQDB_ERR_NO_MEM;
        }

        /* Process existing entities from main DB */
        if (src) {
            for (uint32_t i = 0; i < n && !tqdb_read_error(&r); i++) {
                if (trait->init) trait->init(entity);
                trait->read(&r, entity);
                if (tqdb_read_error(&r)) break;

                uint32_t entity_id = trait->get_id(entity);
                bool skip = false;

                /* Check if this entity is modified/deleted in WAL */
                for (uint32_t j = 0; j < valid_entries; j++) {
                    if (entries[j].id != 0 &&
                        entries[j].type_idx == type_idx &&
                        entries[j].id == entity_id) {
                        /* Found in WAL */
                        if (entries[j].op == TQDB_WAL_OP_DELETE) {
                            skip = true;  /* Deleted */
                        } else if (entries[j].op == TQDB_WAL_OP_UPDATE) {
                            /* Write updated version instead */
                            if (entries[j].entity) {
                                trait->write(&w, entries[j].entity);
                                actual_counts[type_idx]++;
                            }
                            skip = true;
                            /* Mark as processed */
                            entries[j].id = 0;
                        }
                        break;
                    }
                }

                if (!skip) {
                    /* Write original entity */
                    trait->write(&w, entity);
                    actual_counts[type_idx]++;
                }

                if (trait->destroy) trait->destroy(entity);
            }
        }

        /* Write new entities from WAL (ADD operations) */
        for (uint32_t j = 0; j < valid_entries; j++) {
            if (entries[j].id != 0 &&
                entries[j].type_idx == type_idx &&
                entries[j].op == TQDB_WAL_OP_ADD &&
                entries[j].entity) {
                trait->write(&w, entries[j].entity);
                actual_counts[type_idx]++;
                entries[j].id = 0;  /* Mark as processed */
            }
        }

        tqdb_dealloc(db, entity);
    }

    /* Free WAL entries */
    for (uint32_t i = 0; i < valid_entries; i++) {
        if (entries[i].entity) {
            const tqdb_trait_t* trait = db->traits[entries[i].type_idx];
            if (trait && trait->destroy) trait->destroy(entries[i].entity);
            tqdb_dealloc(db, entries[i].entity);
        }
    }
    tqdb_dealloc(db, entries);

    if (src) fclose(src);

    /* Finalize writer */
    tqdb_writer_flush(&w);
    if (tqdb_write_error(&w)) {
        fclose(dst);
        remove(db->tmp_path);
        return TQDB_ERR_IO;
    }

    /* Rewrite counts */
    fseek(dst, counts_pos, SEEK_SET);
    for (size_t i = 0; i < db->trait_count; i++) {
        fwrite(&actual_counts[i], 4, 1, dst);
    }

    /* Patch CRC in header */
    uint32_t crc = tqdb_writer_crc(&w);
    fseek(dst, 8, SEEK_SET);
    fwrite(&crc, 4, 1, dst);

    fflush(dst);
    fclose(dst);

    /* Atomic swap */
    remove(db->bak_path);
    rename(db->db_path, db->bak_path);
    if (rename(db->tmp_path, db->db_path) != 0) {
        rename(db->bak_path, db->db_path);
        return TQDB_ERR_IO;
    }
    remove(db->bak_path);

#if TQDB_ENABLE_CACHE
    /* Clear cache after checkpoint */
    if (db->cache) {
        tqdb_cache_invalidate_all(db);
    }
#endif

    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Public WAL API
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_checkpoint(tqdb_t db) {
    if (!db) return TQDB_ERR_INVALID_ARG;
    if (!db->wal.enabled) return TQDB_OK;

    if (!tqdb_lock(db)) return TQDB_ERR_TIMEOUT;

    tqdb_err_t err = tqdb_wal_checkpoint_internal(db);

    tqdb_unlock(db);
    return err;
}

tqdb_err_t tqdb_wal_stats(tqdb_t db, size_t* out_entry_count, size_t* out_size) {
    if (!db) return TQDB_ERR_INVALID_ARG;

    if (out_entry_count) {
        *out_entry_count = db->wal.enabled ? db->wal.entry_count : 0;
    }
    if (out_size) {
        *out_size = db->wal.enabled ? db->wal.file_size : 0;
    }

    return TQDB_OK;
}
#endif /* TQDB_ENABLE_WAL */
