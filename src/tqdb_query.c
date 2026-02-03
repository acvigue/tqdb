/**
 * @file tqdb_query.c
 * @brief Lightweight query system for TQDB
 *
 * Compile with -DTQDB_ENABLE_QUERY to enable.
 *
 * Features:
 * - Field-based filtering (EQ, NE, LT, LE, GT, GE, BETWEEN, LIKE)
 * - LIKE pattern matching with * (any chars) and ? (single char)
 * - NULL/NOT_NULL checks
 * - Limit and offset for pagination
 */

#ifdef TQDB_ENABLE_QUERY

#include "tqdb_internal.h"
#include <math.h>

/* ═══════════════════════════════════════════════════════════════════════════
 * Query Condition Structure
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    const tqdb_field_def_t* field;  /* Field definition */
    tqdb_query_op_t op;              /* Comparison operator */
    tqdb_field_type_t value_type;    /* Type of value */
    union {
        int32_t i32;
        int64_t i64;
        float f32;
        double f64;
        bool b;
        const char* str;
    } value;
    union {
        int32_t i32;
        int64_t i64;
        float f32;
        double f64;
    } value2;  /* For BETWEEN */
} tqdb_condition_t;

/* ═══════════════════════════════════════════════════════════════════════════
 * Query Structure
 * ═══════════════════════════════════════════════════════════════════════════ */

struct tqdb_query_s {
    tqdb_t db;                       /* Database handle */
    int type_idx;                    /* Entity type index */
    const tqdb_trait_t* trait;       /* Entity trait */
    const tqdb_trait_ext_t* ext;     /* Extended trait (with fields) */

    tqdb_condition_t conditions[TQDB_QUERY_MAX_CONDITIONS];
    size_t condition_count;

    size_t limit;                    /* Result limit (0 = unlimited) */
    size_t offset;                   /* Skip first N results */
};

/* ═══════════════════════════════════════════════════════════════════════════
 * Pattern Matching (LIKE)
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Match string against glob pattern.
 * Supports:
 *   * - matches any sequence of characters (including empty)
 *   ? - matches any single character
 *   \* and \? - literal asterisk/question mark
 */
static bool pattern_match(const char* pattern, const char* str) {
    if (!pattern || !str) return false;

    while (*pattern) {
        if (*pattern == '*') {
            /* Skip consecutive wildcards */
            while (*pattern == '*') pattern++;
            if (*pattern == '\0') return true;  /* Trailing * matches all */

            /* Try matching rest of pattern from each position */
            while (*str) {
                if (pattern_match(pattern, str)) return true;
                str++;
            }
            return pattern_match(pattern, str);  /* Match empty remainder */
        }
        else if (*pattern == '?') {
            if (*str == '\0') return false;  /* ? requires a character */
            pattern++;
            str++;
        }
        else if (*pattern == '\\' && (pattern[1] == '*' || pattern[1] == '?')) {
            /* Escaped wildcard - match literally */
            pattern++;
            if (*pattern != *str) return false;
            pattern++;
            str++;
        }
        else {
            if (*pattern != *str) return false;
            pattern++;
            str++;
        }
    }

    return *str == '\0';
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Field Lookup
 * ═══════════════════════════════════════════════════════════════════════════ */

static const tqdb_field_def_t* find_field(const tqdb_trait_ext_t* ext, const char* name) {
    if (!ext || !ext->fields || !name) return NULL;

    for (size_t i = 0; i < ext->field_count; i++) {
        if (strcmp(ext->fields[i].name, name) == 0) {
            return &ext->fields[i];
        }
    }
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Field Value Access
 * ═══════════════════════════════════════════════════════════════════════════ */

static int64_t get_field_int(const void* entity, const tqdb_field_def_t* field) {
    const uint8_t* ptr = (const uint8_t*)entity + field->offset;

    switch (field->type) {
        case TQDB_FIELD_INT32:
            return *(const int32_t*)ptr;
        case TQDB_FIELD_INT64:
            return *(const int64_t*)ptr;
        case TQDB_FIELD_UINT8:
            return *(const uint8_t*)ptr;
        case TQDB_FIELD_UINT16:
            return *(const uint16_t*)ptr;
        case TQDB_FIELD_UINT32:
            return *(const uint32_t*)ptr;
        case TQDB_FIELD_BOOL:
            return *(const bool*)ptr ? 1 : 0;
        default:
            return 0;
    }
}

static double get_field_float(const void* entity, const tqdb_field_def_t* field) {
    const uint8_t* ptr = (const uint8_t*)entity + field->offset;

    switch (field->type) {
        case TQDB_FIELD_FLOAT:
            return *(const float*)ptr;
        case TQDB_FIELD_DOUBLE:
            return *(const double*)ptr;
        case TQDB_FIELD_INT32:
            return (double)*(const int32_t*)ptr;
        case TQDB_FIELD_INT64:
            return (double)*(const int64_t*)ptr;
        default:
            return 0.0;
    }
}

static const char* get_field_str(const void* entity, const tqdb_field_def_t* field) {
    if (field->type != TQDB_FIELD_STRING) return "";
    return (const char*)((const uint8_t*)entity + field->offset);
}

static bool get_field_bool(const void* entity, const tqdb_field_def_t* field) {
    const uint8_t* ptr = (const uint8_t*)entity + field->offset;

    switch (field->type) {
        case TQDB_FIELD_BOOL:
            return *(const bool*)ptr;
        case TQDB_FIELD_INT32:
            return *(const int32_t*)ptr != 0;
        case TQDB_FIELD_UINT8:
            return *(const uint8_t*)ptr != 0;
        default:
            return false;
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Field Null Check
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool is_field_null(const void* entity, const tqdb_field_def_t* field) {
    const uint8_t* ptr = (const uint8_t*)entity + field->offset;

    switch (field->type) {
        case TQDB_FIELD_STRING:
            return ((const char*)ptr)[0] == '\0';
        case TQDB_FIELD_INT32:
            return *(const int32_t*)ptr == 0;
        case TQDB_FIELD_INT64:
            return *(const int64_t*)ptr == 0;
        case TQDB_FIELD_FLOAT:
            return *(const float*)ptr == 0.0f;
        case TQDB_FIELD_DOUBLE:
            return *(const double*)ptr == 0.0;
        case TQDB_FIELD_BOOL:
            return *(const bool*)ptr == false;
        case TQDB_FIELD_UINT8:
            return *(const uint8_t*)ptr == 0;
        case TQDB_FIELD_UINT16:
            return *(const uint16_t*)ptr == 0;
        case TQDB_FIELD_UINT32:
            return *(const uint32_t*)ptr == 0;
        default:
            return true;
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Condition Evaluation
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool eval_condition(const void* entity, const tqdb_condition_t* cond) {
    if (!cond->field) return true;  /* No field = always match */

    /* Handle null checks */
    if (cond->op == TQDB_OP_IS_NULL) {
        return is_field_null(entity, cond->field);
    }
    if (cond->op == TQDB_OP_NOT_NULL) {
        return !is_field_null(entity, cond->field);
    }

    /* Handle string comparisons */
    if (cond->field->type == TQDB_FIELD_STRING) {
        const char* field_val = get_field_str(entity, cond->field);
        const char* cmp_val = cond->value.str;

        if (cond->op == TQDB_OP_LIKE) {
            return pattern_match(cmp_val, field_val);
        }

        int cmp = strcmp(field_val, cmp_val ? cmp_val : "");
        switch (cond->op) {
            case TQDB_OP_EQ: return cmp == 0;
            case TQDB_OP_NE: return cmp != 0;
            case TQDB_OP_LT: return cmp < 0;
            case TQDB_OP_LE: return cmp <= 0;
            case TQDB_OP_GT: return cmp > 0;
            case TQDB_OP_GE: return cmp >= 0;
            default: return false;
        }
    }

    /* Handle floating point comparisons */
    if (cond->field->type == TQDB_FIELD_FLOAT ||
        cond->field->type == TQDB_FIELD_DOUBLE ||
        cond->value_type == TQDB_FIELD_FLOAT ||
        cond->value_type == TQDB_FIELD_DOUBLE) {

        double field_val = get_field_float(entity, cond->field);
        double cmp_val = (cond->value_type == TQDB_FIELD_DOUBLE) ?
                         cond->value.f64 : (double)cond->value.f32;

        switch (cond->op) {
            case TQDB_OP_EQ: return fabs(field_val - cmp_val) < 1e-9;
            case TQDB_OP_NE: return fabs(field_val - cmp_val) >= 1e-9;
            case TQDB_OP_LT: return field_val < cmp_val;
            case TQDB_OP_LE: return field_val <= cmp_val;
            case TQDB_OP_GT: return field_val > cmp_val;
            case TQDB_OP_GE: return field_val >= cmp_val;
            case TQDB_OP_BETWEEN: {
                double min_val = (cond->value_type == TQDB_FIELD_DOUBLE) ?
                                 cond->value.f64 : (double)cond->value.f32;
                double max_val = (cond->value_type == TQDB_FIELD_DOUBLE) ?
                                 cond->value2.f64 : (double)cond->value2.f32;
                return field_val >= min_val && field_val <= max_val;
            }
            default: return false;
        }
    }

    /* Handle boolean comparisons */
    if (cond->field->type == TQDB_FIELD_BOOL || cond->value_type == TQDB_FIELD_BOOL) {
        bool field_val = get_field_bool(entity, cond->field);
        bool cmp_val = cond->value.b;

        switch (cond->op) {
            case TQDB_OP_EQ: return field_val == cmp_val;
            case TQDB_OP_NE: return field_val != cmp_val;
            default: return false;
        }
    }

    /* Handle integer comparisons */
    int64_t field_val = get_field_int(entity, cond->field);
    int64_t cmp_val = (cond->value_type == TQDB_FIELD_INT64) ?
                      cond->value.i64 : (int64_t)cond->value.i32;

    switch (cond->op) {
        case TQDB_OP_EQ: return field_val == cmp_val;
        case TQDB_OP_NE: return field_val != cmp_val;
        case TQDB_OP_LT: return field_val < cmp_val;
        case TQDB_OP_LE: return field_val <= cmp_val;
        case TQDB_OP_GT: return field_val > cmp_val;
        case TQDB_OP_GE: return field_val >= cmp_val;
        case TQDB_OP_BETWEEN: {
            int64_t min_val = (cond->value_type == TQDB_FIELD_INT64) ?
                              cond->value.i64 : (int64_t)cond->value.i32;
            int64_t max_val = (cond->value_type == TQDB_FIELD_INT64) ?
                              cond->value2.i64 : (int64_t)cond->value2.i32;
            return field_val >= min_val && field_val <= max_val;
        }
        default: return false;
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Entity Matching
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool entity_matches(const void* entity, tqdb_query_t q) {
    /* All conditions must match (AND logic) */
    for (size_t i = 0; i < q->condition_count; i++) {
        if (!eval_condition(entity, &q->conditions[i])) {
            return false;
        }
    }
    return true;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Query Creation
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_query_t tqdb_query_new(tqdb_t db, const char* type) {
    if (!db || !type) return NULL;

    const tqdb_trait_t* trait = tqdb_find_trait(db, type);
    if (!trait) return NULL;

    int type_idx = tqdb_find_trait_index(db, type);
    if (type_idx < 0) return NULL;

    tqdb_query_t q = (tqdb_query_t)tqdb_alloc(db, sizeof(struct tqdb_query_s));
    if (!q) return NULL;

    memset(q, 0, sizeof(struct tqdb_query_s));
    q->db = db;
    q->type_idx = type_idx;
    q->trait = trait;
    q->ext = (const tqdb_trait_ext_t*)trait;  /* May or may not have fields */
    q->limit = 0;
    q->offset = 0;

    return q;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Add Conditions
 * ═══════════════════════════════════════════════════════════════════════════ */

static tqdb_err_t add_condition(tqdb_query_t q, const char* field_name,
                                 tqdb_query_op_t op, tqdb_field_type_t val_type) {
    if (!q) return TQDB_ERR_INVALID_ARG;
    if (q->condition_count >= TQDB_QUERY_MAX_CONDITIONS) return TQDB_ERR_FULL;

    /* Find field definition */
    const tqdb_field_def_t* field = find_field(q->ext, field_name);
    if (!field) return TQDB_ERR_NOT_FOUND;

    tqdb_condition_t* cond = &q->conditions[q->condition_count];
    cond->field = field;
    cond->op = op;
    cond->value_type = val_type;

    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_i32(tqdb_query_t q, const char* field,
                                 tqdb_query_op_t op, int32_t value) {
    tqdb_err_t err = add_condition(q, field, op, TQDB_FIELD_INT32);
    if (err != TQDB_OK) return err;

    q->conditions[q->condition_count].value.i32 = value;
    q->condition_count++;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_i64(tqdb_query_t q, const char* field,
                                 tqdb_query_op_t op, int64_t value) {
    tqdb_err_t err = add_condition(q, field, op, TQDB_FIELD_INT64);
    if (err != TQDB_OK) return err;

    q->conditions[q->condition_count].value.i64 = value;
    q->condition_count++;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_float(tqdb_query_t q, const char* field,
                                   tqdb_query_op_t op, float value) {
    tqdb_err_t err = add_condition(q, field, op, TQDB_FIELD_FLOAT);
    if (err != TQDB_OK) return err;

    q->conditions[q->condition_count].value.f32 = value;
    q->condition_count++;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_double(tqdb_query_t q, const char* field,
                                    tqdb_query_op_t op, double value) {
    tqdb_err_t err = add_condition(q, field, op, TQDB_FIELD_DOUBLE);
    if (err != TQDB_OK) return err;

    q->conditions[q->condition_count].value.f64 = value;
    q->condition_count++;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_str(tqdb_query_t q, const char* field,
                                 tqdb_query_op_t op, const char* value) {
    tqdb_err_t err = add_condition(q, field, op, TQDB_FIELD_STRING);
    if (err != TQDB_OK) return err;

    q->conditions[q->condition_count].value.str = value;
    q->condition_count++;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_bool(tqdb_query_t q, const char* field,
                                  tqdb_query_op_t op, bool value) {
    tqdb_err_t err = add_condition(q, field, op, TQDB_FIELD_BOOL);
    if (err != TQDB_OK) return err;

    q->conditions[q->condition_count].value.b = value;
    q->condition_count++;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_between_i32(tqdb_query_t q, const char* field,
                                         int32_t min, int32_t max) {
    tqdb_err_t err = add_condition(q, field, TQDB_OP_BETWEEN, TQDB_FIELD_INT32);
    if (err != TQDB_OK) return err;

    q->conditions[q->condition_count].value.i32 = min;
    q->conditions[q->condition_count].value2.i32 = max;
    q->condition_count++;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_between_i64(tqdb_query_t q, const char* field,
                                         int64_t min, int64_t max) {
    tqdb_err_t err = add_condition(q, field, TQDB_OP_BETWEEN, TQDB_FIELD_INT64);
    if (err != TQDB_OK) return err;

    q->conditions[q->condition_count].value.i64 = min;
    q->conditions[q->condition_count].value2.i64 = max;
    q->condition_count++;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_between_float(tqdb_query_t q, const char* field,
                                           float min, float max) {
    tqdb_err_t err = add_condition(q, field, TQDB_OP_BETWEEN, TQDB_FIELD_FLOAT);
    if (err != TQDB_OK) return err;

    q->conditions[q->condition_count].value.f32 = min;
    q->conditions[q->condition_count].value2.f32 = max;
    q->condition_count++;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_between_double(tqdb_query_t q, const char* field,
                                            double min, double max) {
    tqdb_err_t err = add_condition(q, field, TQDB_OP_BETWEEN, TQDB_FIELD_DOUBLE);
    if (err != TQDB_OK) return err;

    q->conditions[q->condition_count].value.f64 = min;
    q->conditions[q->condition_count].value2.f64 = max;
    q->condition_count++;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_where_null(tqdb_query_t q, const char* field, bool is_null) {
    tqdb_err_t err = add_condition(q, field,
                                    is_null ? TQDB_OP_IS_NULL : TQDB_OP_NOT_NULL,
                                    TQDB_FIELD_INT32);  /* Type doesn't matter */
    if (err != TQDB_OK) return err;

    q->condition_count++;
    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Limit and Offset
 * ═══════════════════════════════════════════════════════════════════════════ */

tqdb_err_t tqdb_query_limit(tqdb_query_t q, size_t limit) {
    if (!q) return TQDB_ERR_INVALID_ARG;
    q->limit = limit;
    return TQDB_OK;
}

tqdb_err_t tqdb_query_offset(tqdb_query_t q, size_t offset) {
    if (!q) return TQDB_ERR_INVALID_ARG;
    q->offset = offset;
    return TQDB_OK;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Query Execution
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    tqdb_query_t q;
    tqdb_iter_fn user_fn;
    void* user_ctx;
    size_t skipped;
    size_t matched;
} query_exec_ctx_t;

static bool query_iter_callback(const void* entity, void* ctx) {
    query_exec_ctx_t* qctx = (query_exec_ctx_t*)ctx;

    if (!entity_matches(entity, qctx->q)) {
        return true;  /* Continue, doesn't match */
    }

    /* Handle offset */
    if (qctx->skipped < qctx->q->offset) {
        qctx->skipped++;
        return true;
    }

    /* Handle limit */
    if (qctx->q->limit > 0 && qctx->matched >= qctx->q->limit) {
        return false;  /* Stop, limit reached */
    }

    qctx->matched++;

    /* Call user callback */
    if (qctx->user_fn) {
        return qctx->user_fn(entity, qctx->user_ctx);
    }

    return true;
}

tqdb_err_t tqdb_query_exec(tqdb_query_t q, tqdb_iter_fn fn, void* ctx) {
    if (!q) return TQDB_ERR_INVALID_ARG;

    query_exec_ctx_t qctx = {
        .q = q,
        .user_fn = fn,
        .user_ctx = ctx,
        .skipped = 0,
        .matched = 0
    };

    return tqdb_foreach(q->db, q->trait->name, query_iter_callback, &qctx);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Query Count
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool count_callback(const void* entity, void* ctx) {
    (void)entity;
    size_t* count = (size_t*)ctx;
    (*count)++;
    return true;
}

size_t tqdb_query_count(tqdb_query_t q) {
    if (!q) return 0;

    /* Save and clear limit/offset for counting */
    size_t saved_limit = q->limit;
    size_t saved_offset = q->offset;
    q->limit = 0;
    q->offset = 0;

    size_t count = 0;
    tqdb_query_exec(q, count_callback, &count);

    /* Restore limit/offset */
    q->limit = saved_limit;
    q->offset = saved_offset;

    return count;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Query Cleanup
 * ═══════════════════════════════════════════════════════════════════════════ */

void tqdb_query_free(tqdb_query_t q) {
    if (!q) return;
    tqdb_dealloc(q->db, q);
}

#endif /* TQDB_ENABLE_QUERY */
