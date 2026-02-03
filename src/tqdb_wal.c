/**
 * @file tqdb_wal.c
 * @brief Write-Ahead Logging implementation for TQDB
 */

#include "tqdb_internal.h"

#if TQDB_ENABLE_WAL

/**
 * WAL implementation (only compiled when TQDB_ENABLE_WAL=1)
 *
 * WAL file format:
 *   Header (16 bytes):
 *     magic: u32      = 0x4C415754 ("TWAL")
 *     version: u16    = 1
 *     flags: u16      = 0
 *     db_crc: u32     = CRC of main DB when WAL started
 *     entry_count: u32
 *
 *   Entry format:
 *     entry_crc: u32      (CRC of this entry, excluding this field)
 *     op: u8              (1=ADD, 2=UPDATE, 3=DELETE)
 *     type_idx: u8
 *     id: u32             (entity ID)
 *     data_len: u32       (0 for DELETE)
 *     data: [u8; data_len]
 */

#ifdef _WIN32
#include <io.h>
#define ftruncate _chsize
#define fileno _fileno
#else
#include <unistd.h>
#endif

/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Header I/O
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool wal_write_header(FILE* f, const tqdb_wal_header_t* h) {
    fseek(f, 0, SEEK_SET);
    return fwrite(&h->magic, 4, 1, f) == 1
        && fwrite(&h->version, 2, 1, f) == 1
        && fwrite(&h->flags, 2, 1, f) == 1
        && fwrite(&h->db_crc, 4, 1, f) == 1
        && fwrite(&h->entry_count, 4, 1, f) == 1;
}

static bool wal_read_header(FILE* f, tqdb_wal_header_t* h) {
    fseek(f, 0, SEEK_SET);
    return fread(&h->magic, 4, 1, f) == 1
        && fread(&h->version, 2, 1, f) == 1
        && fread(&h->flags, 2, 1, f) == 1
        && fread(&h->db_crc, 4, 1, f) == 1
        && fread(&h->entry_count, 4, 1, f) == 1;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * WAL File Management
 * ═══════════════════════════════════════════════════════════════════════════ */

static tqdb_err_t wal_create(tqdb_t db) {
    FILE* f = fopen(db->wal.path, "wb");
    if (!f) return TQDB_ERR_IO;

    tqdb_wal_header_t hdr = {
        .magic = TQDB_WAL_MAGIC,
        .version = TQDB_WAL_VERSION,
        .flags = 0,
        .db_crc = db->wal.db_crc,
        .entry_count = 0
    };

    if (!wal_write_header(f, &hdr)) {
        fclose(f);
        remove(db->wal.path);
        return TQDB_ERR_IO;
    }

    fflush(f);
    fclose(f);

    db->wal.entry_count = 0;
    db->wal.file_size = TQDB_WAL_HEADER_SIZE;

    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Initialization
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_wal_init(tqdb_t db, const char* wal_path, size_t max_entries, size_t max_size) {
    if (!db) return TQDB_ERR_INVALID_ARG;

    /* Copy WAL path */
    size_t path_len = strlen(wal_path) + 1;
    db->wal.path = (char*)tqdb_alloc(db, path_len);
    if (!db->wal.path) return TQDB_ERR_NO_MEM;
    memcpy(db->wal.path, wal_path, path_len);

    /* Set thresholds */
    db->wal.max_entries = max_entries > 0 ? max_entries : TQDB_WAL_MAX_ENTRIES_DEFAULT;
    db->wal.max_size = max_size > 0 ? max_size : TQDB_WAL_MAX_SIZE_DEFAULT;
    db->wal.enabled = true;
    db->wal.entry_count = 0;
    db->wal.file_size = 0;
    db->wal.db_crc = 0;
    db->wal.recovery_pending = false;

    return TQDB_OK;
}

void tqdb_wal_destroy(tqdb_t db) {
    if (!db) return;

    if (db->wal.path) {
        tqdb_dealloc(db, db->wal.path);
        db->wal.path = NULL;
    }
    db->wal.enabled = false;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Recovery
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_wal_recover(tqdb_t db) {
    if (!db || !db->wal.enabled || !db->wal.path) return TQDB_OK;

    FILE* f = fopen(db->wal.path, "rb");
    if (!f) {
        /* No WAL file - compute initial DB CRC and create fresh WAL */
        db->wal.db_crc = tqdb_wal_compute_db_crc(db);
        return wal_create(db);
    }

    /* Read WAL header */
    tqdb_wal_header_t hdr;
    if (!wal_read_header(f, &hdr)) {
        fclose(f);
        /* Corrupt header - discard WAL */
        remove(db->wal.path);
        db->wal.db_crc = tqdb_wal_compute_db_crc(db);
        return wal_create(db);
    }

    /* Validate magic and version */
    if (hdr.magic != TQDB_WAL_MAGIC || hdr.version > TQDB_WAL_VERSION) {
        fclose(f);
        remove(db->wal.path);
        db->wal.db_crc = tqdb_wal_compute_db_crc(db);
        return wal_create(db);
    }

    /* Get file size */
    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fclose(f);

    /* Store WAL state */
    db->wal.entry_count = hdr.entry_count;
    db->wal.file_size = (uint32_t)file_size;
    db->wal.db_crc = hdr.db_crc;

    /* If we have pending entries, defer recovery until traits are registered */
    if (hdr.entry_count > 0) {
        db->wal.recovery_pending = true;
    }

    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Deferred Recovery Check
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_wal_check_recovery(tqdb_t db) {
    if (!db || !db->wal.enabled) return TQDB_OK;
    if (!db->wal.recovery_pending) return TQDB_OK;
    if (db->trait_count == 0) return TQDB_OK;  /* Still no traits registered */

    /* Perform deferred recovery - traits are now available */
    db->wal.recovery_pending = false;
    return tqdb_wal_checkpoint_internal(db);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Append Operation
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_wal_append(tqdb_t db, uint8_t op, uint8_t type_idx,
                           uint32_t id, const void* entity) {
    if (!db || !db->wal.enabled || !db->wal.path) return TQDB_OK;
    if (id == 0) return TQDB_ERR_INVALID_ARG;

    const tqdb_trait_t* trait = db->traits[type_idx];
    if (!trait) return TQDB_ERR_INVALID_ARG;

    /* Open WAL for append */
    FILE* f = fopen(db->wal.path, "r+b");
    if (!f) {
        /* WAL doesn't exist, create it */
        db->wal.db_crc = tqdb_wal_compute_db_crc(db);
        tqdb_err_t err = wal_create(db);
        if (err != TQDB_OK) return err;
        f = fopen(db->wal.path, "r+b");
        if (!f) return TQDB_ERR_IO;
    }

    /* Seek to end */
    fseek(f, 0, SEEK_END);
    long entry_start = ftell(f);

    /* Calculate CRC of entry (excluding CRC field itself) */
    uint32_t crc = 0xFFFFFFFF;
    crc = tqdb_crc32_update(crc, &op, 1);
    crc = tqdb_crc32_update(crc, &type_idx, 1);
    crc = tqdb_crc32_update(crc, (uint8_t*)&id, 4);

    /* Serialize entity to get data and data_len */
    uint8_t* entity_data = NULL;
    size_t data_len = 0;
    if (op != TQDB_WAL_OP_DELETE && entity) {
        /* Use scratch buffer to serialize entity */
        size_t half = db->scratch_size / 2;
        uint8_t* write_buf = db->scratch;

        /* Create memory writer */
        FILE* mem = tmpfile();
        if (!mem) {
            fclose(f);
            return TQDB_ERR_IO;
        }

        tqdb_writer_t w;
        tqdb_writer_init(&w, mem, write_buf, half);
        trait->write(&w, entity);
        tqdb_writer_flush(&w);

        data_len = ftell(mem);

        /* Read back serialized data */
        entity_data = (uint8_t*)tqdb_alloc(db, data_len);
        if (!entity_data) {
            fclose(mem);
            fclose(f);
            return TQDB_ERR_NO_MEM;
        }
        fseek(mem, 0, SEEK_SET);
        fread(entity_data, 1, data_len, mem);
        fclose(mem);

        /* Update CRC with entity data */
        uint32_t data_len32 = (uint32_t)data_len;
        crc = tqdb_crc32_update(crc, (uint8_t*)&data_len32, 4);
        crc = tqdb_crc32_update(crc, entity_data, data_len);
    } else {
        uint32_t zero = 0;
        crc = tqdb_crc32_update(crc, (uint8_t*)&zero, 4);
    }
    crc = tqdb_crc32_finalize(crc);

    /* Write entry */
    bool write_ok = true;
    write_ok = write_ok && fwrite(&crc, 4, 1, f) == 1;
    write_ok = write_ok && fwrite(&op, 1, 1, f) == 1;
    write_ok = write_ok && fwrite(&type_idx, 1, 1, f) == 1;
    write_ok = write_ok && fwrite(&id, 4, 1, f) == 1;
    uint32_t data_len32 = (uint32_t)data_len;
    write_ok = write_ok && fwrite(&data_len32, 4, 1, f) == 1;
    if (data_len > 0 && entity_data) {
        write_ok = write_ok && fwrite(entity_data, 1, data_len, f) == data_len;
    }

    if (entity_data) {
        tqdb_dealloc(db, entity_data);
    }

    if (!write_ok) {
        /* Truncate back to entry start */
        ftruncate(fileno(f), entry_start);
        fclose(f);
        return TQDB_ERR_IO;
    }

    /* Update header entry count */
    db->wal.entry_count++;
    db->wal.file_size = (uint32_t)ftell(f);

    fseek(f, 12, SEEK_SET);  /* Offset of entry_count in header */
    fwrite(&db->wal.entry_count, 4, 1, f);

    fflush(f);
    fclose(f);

    /* Update cache if enabled */
#if TQDB_ENABLE_CACHE
    if (db->cache) {
        if (op == TQDB_WAL_OP_DELETE) {
            tqdb_cache_invalidate(db, type_idx, id);
        } else {
            tqdb_cache_put(db, type_idx, id, entity, op);
        }
    }
#endif

    /* Check if we should checkpoint */
    if (tqdb_wal_should_checkpoint(db)) {
        return tqdb_wal_checkpoint_internal(db);
    }

    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Find (for reads)
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_wal_find(tqdb_t db, uint8_t type_idx, uint32_t id,
                         uint8_t* out_op, void* out_entity) {
    if (!db || !db->wal.enabled || !db->wal.path) return TQDB_ERR_NOT_FOUND;
    if (id == 0 || db->wal.entry_count == 0) return TQDB_ERR_NOT_FOUND;

    FILE* f = fopen(db->wal.path, "rb");
    if (!f) return TQDB_ERR_NOT_FOUND;

    /* Skip header */
    fseek(f, TQDB_WAL_HEADER_SIZE, SEEK_SET);

    const tqdb_trait_t* trait = db->traits[type_idx];
    size_t half = db->scratch_size / 2;

    /* Track the last matching entry (most recent wins) */
    uint8_t found_op = 0;
    bool found = false;
    long found_data_pos = 0;
    uint32_t found_data_len = 0;

    /* Scan all entries */
    for (uint32_t i = 0; i < db->wal.entry_count; i++) {
        uint32_t entry_crc;
        uint8_t op, tidx;
        uint32_t entry_id;

        if (fread(&entry_crc, 4, 1, f) != 1) break;
        if (fread(&op, 1, 1, f) != 1) break;
        if (fread(&tidx, 1, 1, f) != 1) break;
        if (fread(&entry_id, 4, 1, f) != 1) break;

        uint32_t data_len;
        if (fread(&data_len, 4, 1, f) != 1) break;

        /* Check if this entry matches */
        if (tidx == type_idx && entry_id == id) {
            found = true;
            found_op = op;
            found_data_pos = ftell(f);
            found_data_len = data_len;
        }

        /* Skip entity data */
        if (data_len > 0) {
            fseek(f, data_len, SEEK_CUR);
        }
    }

    if (!found) {
        fclose(f);
        return TQDB_ERR_NOT_FOUND;
    }

    if (out_op) *out_op = found_op;

    /* If deleted, return not found */
    if (found_op == TQDB_WAL_OP_DELETE) {
        fclose(f);
        return TQDB_ERR_NOT_FOUND;
    }

    /* Read entity data if requested */
    if (out_entity && found_data_len > 0) {
        fseek(f, found_data_pos, SEEK_SET);

        tqdb_reader_t r;
        tqdb_reader_init(&r, f, db->scratch, half);

        if (trait->init) trait->init(out_entity);
        trait->read(&r, out_entity);

        if (tqdb_read_error(&r)) {
            if (trait->destroy) trait->destroy(out_entity);
            fclose(f);
            return TQDB_ERR_CORRUPT;
        }
    }

    fclose(f);
    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Checkpoint
 * ═══════════════════════════════════════════════════════════════════════════ */

bool tqdb_wal_should_checkpoint(tqdb_t db) {
    if (!db || !db->wal.enabled) return false;

    if (db->wal.max_entries > 0 && db->wal.entry_count >= db->wal.max_entries) {
        return true;
    }
    if (db->wal.max_size > 0 && db->wal.file_size >= db->wal.max_size) {
        return true;
    }

    return false;
}

/* Forward declaration - implemented in tqdb_core.c */
extern tqdb_err_t tqdb_checkpoint_merge(tqdb_t db);

tqdb_err_t tqdb_wal_checkpoint_internal(tqdb_t db) {
    if (!db || !db->wal.enabled) return TQDB_OK;
    if (db->wal.entry_count == 0) return TQDB_OK;

    /* Merge WAL into main DB */
    tqdb_err_t err = tqdb_checkpoint_merge(db);
    if (err != TQDB_OK) return err;

    /* Clear WAL */
    db->wal.db_crc = tqdb_wal_compute_db_crc(db);
    return wal_create(db);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Compute DB CRC
 * ═══════════════════════════════════════════════════════════════════════════ */

uint32_t tqdb_wal_compute_db_crc(tqdb_t db) {
    if (!db || !db->db_path) return 0;

    FILE* f = fopen(db->db_path, "rb");
    if (!f) return 0;

    uint32_t crc = 0xFFFFFFFF;
    uint8_t buf[256];
    size_t n;

    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
        crc = tqdb_crc32_update(crc, buf, n);
    }

    fclose(f);
    return tqdb_crc32_finalize(crc);
}

#endif /* TQDB_ENABLE_WAL */
