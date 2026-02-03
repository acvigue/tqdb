/**
 * @file tqdb_binary_io.c
 * @brief Binary reader/writer implementation with CRC tracking
 */

#include "tqdb_internal.h"

/* ═══════════════════════════════════════════════════════════════════════════
 * Writer Implementation
 * ═══════════════════════════════════════════════════════════════════════════ */

void tqdb_writer_init(tqdb_writer_t* w, FILE* f, uint8_t* buf, size_t buf_size) {
    w->file = f;
    w->crc = 0xFFFFFFFF;
    w->error = false;
    w->buf = buf;
    w->buf_size = buf_size;
    w->buf_pos = 0;
}

void tqdb_writer_flush(tqdb_writer_t* w) {
    if (w->error || w->buf_pos == 0) return;
    if (fwrite(w->buf, 1, w->buf_pos, w->file) != w->buf_pos) {
        w->error = true;
    }
    w->buf_pos = 0;
}

uint32_t tqdb_writer_crc(tqdb_writer_t* w) {
    return tqdb_crc32_finalize(w->crc);
}

void tqdb_write_raw(tqdb_writer_t* w, const void* data, size_t len) {
    if (w->error) return;

    const uint8_t* p = (const uint8_t*)data;

    /* Update CRC */
    w->crc = tqdb_crc32_update(w->crc, p, len);

    /* Fits in buffer? */
    if (len <= w->buf_size - w->buf_pos) {
        memcpy(w->buf + w->buf_pos, p, len);
        w->buf_pos += len;
        return;
    }

    /* Flush buffer first */
    tqdb_writer_flush(w);
    if (w->error) return;

    /* Large write: bypass buffer */
    if (len >= w->buf_size) {
        if (fwrite(p, 1, len, w->file) != len) {
            w->error = true;
        }
        return;
    }

    /* Small write: buffer it */
    memcpy(w->buf, p, len);
    w->buf_pos = len;
}

void tqdb_write_u8(tqdb_writer_t* w, uint8_t v) {
    tqdb_write_raw(w, &v, 1);
}

void tqdb_write_u16(tqdb_writer_t* w, uint16_t v) {
    tqdb_write_raw(w, &v, 2);
}

void tqdb_write_u32(tqdb_writer_t* w, uint32_t v) {
    tqdb_write_raw(w, &v, 4);
}

void tqdb_write_i32(tqdb_writer_t* w, int32_t v) {
    tqdb_write_raw(w, &v, 4);
}

void tqdb_write_i64(tqdb_writer_t* w, int64_t v) {
    tqdb_write_raw(w, &v, 8);
}

void tqdb_write_str(tqdb_writer_t* w, const char* s) {
    size_t len = s ? strlen(s) : 0;
    if (len > 0xFFFF) len = 0xFFFF;  /* Cap at 16-bit max */
    tqdb_write_u16(w, (uint16_t)len);
    if (len > 0) {
        tqdb_write_raw(w, s, len);
    }
}

bool tqdb_write_error(tqdb_writer_t* w) {
    return w->error;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Reader Implementation
 * ═══════════════════════════════════════════════════════════════════════════ */

void tqdb_reader_init(tqdb_reader_t* r, FILE* f, uint8_t* buf, size_t buf_size) {
    r->file = f;
    r->crc = 0xFFFFFFFF;
    r->error = false;
    r->buf = buf;
    r->buf_size = buf_size;
    r->buf_pos = 0;
    r->buf_filled = 0;
}

uint32_t tqdb_reader_crc(tqdb_reader_t* r) {
    return tqdb_crc32_finalize(r->crc);
}

void tqdb_read_raw(tqdb_reader_t* r, void* data, size_t len) {
    if (r->error) return;

    uint8_t* p = (uint8_t*)data;
    size_t written = 0;

    while (written < len && !r->error) {
        size_t avail = r->buf_filled - r->buf_pos;
        if (avail > 0) {
            size_t to_copy = len - written;
            if (to_copy > avail) to_copy = avail;
            memcpy(p + written, r->buf + r->buf_pos, to_copy);
            r->buf_pos += to_copy;
            written += to_copy;
        } else {
            r->buf_filled = fread(r->buf, 1, r->buf_size, r->file);
            r->buf_pos = 0;
            if (r->buf_filled == 0) {
                r->error = true;
                return;
            }
        }
    }

    /* Update CRC */
    r->crc = tqdb_crc32_update(r->crc, p, len);
}

uint8_t tqdb_read_u8(tqdb_reader_t* r) {
    uint8_t v = 0;
    tqdb_read_raw(r, &v, 1);
    return v;
}

uint16_t tqdb_read_u16(tqdb_reader_t* r) {
    uint16_t v = 0;
    tqdb_read_raw(r, &v, 2);
    return v;
}

uint32_t tqdb_read_u32(tqdb_reader_t* r) {
    uint32_t v = 0;
    tqdb_read_raw(r, &v, 4);
    return v;
}

int32_t tqdb_read_i32(tqdb_reader_t* r) {
    int32_t v = 0;
    tqdb_read_raw(r, &v, 4);
    return v;
}

int64_t tqdb_read_i64(tqdb_reader_t* r) {
    int64_t v = 0;
    tqdb_read_raw(r, &v, 8);
    return v;
}

size_t tqdb_read_str(tqdb_reader_t* r, char* buf, size_t buf_size) {
    uint16_t len = tqdb_read_u16(r);
    if (r->error || len > TQDB_MAX_STRING_LEN) {
        r->error = true;
        if (buf_size > 0) buf[0] = '\0';
        return 0;
    }

    if (len == 0) {
        if (buf_size > 0) buf[0] = '\0';
        return 0;
    }

    /* Read into buffer, truncating if needed */
    size_t to_read = len;
    if (to_read >= buf_size) to_read = buf_size - 1;

    tqdb_read_raw(r, buf, to_read);
    buf[to_read] = '\0';

    /* Skip remaining if truncated */
    if (len > to_read) {
        tqdb_read_skip(r, len - to_read);
    }

    return to_read;
}

void tqdb_read_skip(tqdb_reader_t* r, size_t len) {
    while (len > 0 && !r->error) {
        size_t avail = r->buf_filled - r->buf_pos;
        if (avail > 0) {
            size_t skip = len;
            if (skip > avail) skip = avail;
            /* Update CRC for skipped bytes */
            r->crc = tqdb_crc32_update(r->crc, r->buf + r->buf_pos, skip);
            r->buf_pos += skip;
            len -= skip;
        } else {
            r->buf_filled = fread(r->buf, 1, r->buf_size, r->file);
            r->buf_pos = 0;
            if (r->buf_filled == 0) {
                r->error = true;
                return;
            }
        }
    }
}

void tqdb_read_skip_str(tqdb_reader_t* r) {
    uint16_t len = tqdb_read_u16(r);
    if (r->error || len > TQDB_MAX_STRING_LEN) {
        r->error = true;
        return;
    }
    if (len > 0) {
        tqdb_read_skip(r, len);
    }
}

bool tqdb_read_error(tqdb_reader_t* r) {
    return r->error;
}
