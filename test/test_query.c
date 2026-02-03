/**
 * @file test_query.c
 * @brief Tests for TQDB query system
 *
 * Compile with: make test-query TQDB_ENABLE_QUERY=1
 */

#include "../tqdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stddef.h>

#define TEST_DB_PATH "test/test_query.tqdb"
#define TEST_WAL_PATH "test/test_query.tqdb.wal"

/* ═══════════════════════════════════════════════════════════════════════════
 * Test Entity: Product with multiple field types
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    uint32_t id;
    char name[64];
    char category[32];
    int32_t price;          /* in cents */
    int32_t quantity;
    float rating;
    bool active;
    uint8_t priority;
} test_product_t;

/* Field definitions for querying */
static const tqdb_field_def_t PRODUCT_FIELDS[] = {
    { "id",       TQDB_FIELD_UINT32, offsetof(test_product_t, id),       sizeof(uint32_t) },
    { "name",     TQDB_FIELD_STRING, offsetof(test_product_t, name),     sizeof(((test_product_t*)0)->name) },
    { "category", TQDB_FIELD_STRING, offsetof(test_product_t, category), sizeof(((test_product_t*)0)->category) },
    { "price",    TQDB_FIELD_INT32,  offsetof(test_product_t, price),    sizeof(int32_t) },
    { "quantity", TQDB_FIELD_INT32,  offsetof(test_product_t, quantity), sizeof(int32_t) },
    { "rating",   TQDB_FIELD_FLOAT,  offsetof(test_product_t, rating),   sizeof(float) },
    { "active",   TQDB_FIELD_BOOL,   offsetof(test_product_t, active),   sizeof(bool) },
    { "priority", TQDB_FIELD_UINT8,  offsetof(test_product_t, priority), sizeof(uint8_t) },
};

/* Serialization callbacks */
static void product_write(tqdb_writer_t* w, const void* entity) {
    const test_product_t* p = (const test_product_t*)entity;
    tqdb_write_u32(w, p->id);
    tqdb_write_str(w, p->name);
    tqdb_write_str(w, p->category);
    tqdb_write_i32(w, p->price);
    tqdb_write_i32(w, p->quantity);
    tqdb_write_raw(w, &p->rating, sizeof(float));
    tqdb_write_u8(w, p->active ? 1 : 0);
    tqdb_write_u8(w, p->priority);
}

static void product_read(tqdb_reader_t* r, void* entity) {
    test_product_t* p = (test_product_t*)entity;
    p->id = tqdb_read_u32(r);
    tqdb_read_str(r, p->name, sizeof(p->name));
    tqdb_read_str(r, p->category, sizeof(p->category));
    p->price = tqdb_read_i32(r);
    p->quantity = tqdb_read_i32(r);
    tqdb_read_raw(r, &p->rating, sizeof(float));
    p->active = tqdb_read_u8(r) != 0;
    p->priority = tqdb_read_u8(r);
}

static uint32_t product_get_id(const void* entity) {
    return ((const test_product_t*)entity)->id;
}

static void product_set_id(void* entity, uint32_t id) {
    ((test_product_t*)entity)->id = id;
}

static void product_init(void* entity) {
    memset(entity, 0, sizeof(test_product_t));
}

/* Extended trait with field definitions */
static const tqdb_trait_ext_t PRODUCT_TRAIT_EXT = {
    .base = {
        .name = "Product",
        .max_count = 1000,
        .struct_size = sizeof(test_product_t),
        .write = product_write,
        .read = product_read,
        .get_id = product_get_id,
        .set_id = product_set_id,
        .init = product_init,
        .destroy = NULL,
        .skip = NULL
    },
    .fields = PRODUCT_FIELDS,
    .field_count = sizeof(PRODUCT_FIELDS) / sizeof(PRODUCT_FIELDS[0])
};

/* ═══════════════════════════════════════════════════════════════════════════
 * Test Helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    tests_run++; \
    printf("  %-40s ", #name); \
    if (test_##name()) { \
        printf("\033[32mPASS\033[0m\n"); \
        tests_passed++; \
    } else { \
        printf("\033[31mFAIL\033[0m\n"); \
    } \
} while(0)

#define ASSERT(cond) do { \
    if (!(cond)) { \
        printf("ASSERT FAILED: %s (line %d)\n", #cond, __LINE__); \
        return false; \
    } \
} while(0)

static void cleanup(void) {
    remove(TEST_DB_PATH);
    remove(TEST_DB_PATH ".tmp");
    remove(TEST_DB_PATH ".bak");
    remove(TEST_WAL_PATH);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test Data Setup
 * ═══════════════════════════════════════════════════════════════════════════ */

static tqdb_t setup_db_with_products(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = {
        .db_path = TEST_DB_PATH,
        .enable_wal = true,
        .wal_path = TEST_WAL_PATH
    };

    if (tqdb_open(&cfg, &db) != TQDB_OK) return NULL;
    if (tqdb_register(db, (const tqdb_trait_t*)&PRODUCT_TRAIT_EXT) != TQDB_OK) {
        tqdb_close(db);
        return NULL;
    }

    /* Add test products - IDs will be auto-assigned */
    test_product_t products[] = {
        { 0, "Apple iPhone 15",   "Electronics", 99900, 50,  4.8f, true,  1 },
        { 0, "Samsung Galaxy",    "Electronics", 89900, 30,  4.5f, true,  2 },
        { 0, "Sony Headphones",   "Electronics", 29900, 100, 4.2f, true,  3 },
        { 0, "Coffee Maker",      "Appliances",  4999,  200, 4.0f, true,  5 },
        { 0, "Toaster",           "Appliances",  2999,  150, 3.8f, true,  6 },
        { 0, "Old Laptop",        "Electronics", 19900, 0,   3.0f, false, 10 },
        { 0, "Vintage Radio",     "Electronics", 5000,  5,   4.9f, false, 8 },
        { 0, "Blender Pro",       "Appliances",  7999,  75,  4.3f, true,  4 },
        { 0, "Test Item Alpha",   "Test",        100,   10,  5.0f, true,  1 },
        { 0, "Test Item Beta",    "Test",        200,   20,  4.5f, false, 2 },
    };

    for (size_t i = 0; i < sizeof(products)/sizeof(products[0]); i++) {
        tqdb_add(db, "Product", &products[i]);
    }

    return db;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Tests: Basic Query Operations
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool test_query_all(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* No conditions = match all */
    size_t count = tqdb_query_count(q);
    ASSERT(count == 10);

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_eq_int(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* price == 29900 (Sony Headphones) */
    tqdb_err_t err = tqdb_query_where_i32(q, "price", TQDB_OP_EQ, 29900);
    ASSERT(err == TQDB_OK);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 1);

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_gt_int(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* price > 50000 (only iPhone and Samsung) */
    tqdb_query_where_i32(q, "price", TQDB_OP_GT, 50000);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 2);

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_lt_int(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* price < 5000 (Coffee Maker, Toaster, Test items) */
    tqdb_query_where_i32(q, "price", TQDB_OP_LT, 5000);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 4);  /* Coffee Maker 4999, Toaster 2999, Test Alpha 100, Test Beta 200 */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_between_int(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* 5000 <= price <= 10000 */
    tqdb_query_where_between_i32(q, "price", 5000, 10000);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 2);  /* Vintage Radio 5000, Blender Pro 7999 */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_eq_string(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* category == "Appliances" */
    tqdb_query_where_str(q, "category", TQDB_OP_EQ, "Appliances");

    size_t count = tqdb_query_count(q);
    ASSERT(count == 3);  /* Coffee Maker, Toaster, Blender Pro */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_like_prefix(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* name starts with "Test" */
    tqdb_query_where_str(q, "name", TQDB_OP_LIKE, "Test*");

    size_t count = tqdb_query_count(q);
    ASSERT(count == 2);  /* Test Item Alpha, Test Item Beta */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_like_contains(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* name contains "Maker" */
    tqdb_query_where_str(q, "name", TQDB_OP_LIKE, "*Maker*");

    size_t count = tqdb_query_count(q);
    ASSERT(count == 1);  /* Coffee Maker */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_like_suffix(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* name ends with "Pro" */
    tqdb_query_where_str(q, "name", TQDB_OP_LIKE, "*Pro");

    size_t count = tqdb_query_count(q);
    ASSERT(count == 1);  /* Blender Pro */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_like_single_char(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* name matches "Test Item ????" (4 char suffix) */
    tqdb_query_where_str(q, "name", TQDB_OP_LIKE, "Test Item ????");

    size_t count = tqdb_query_count(q);
    ASSERT(count == 1);  /* Test Item Beta (Alpha has 5 chars) */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_eq_bool(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* active == true */
    tqdb_query_where_bool(q, "active", TQDB_OP_EQ, true);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 7);  /* 7 active products */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_ne_bool(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* active != true (i.e., inactive) */
    tqdb_query_where_bool(q, "active", TQDB_OP_NE, true);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 3);  /* Old Laptop, Vintage Radio, Test Item Beta */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_gt_float(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* rating > 4.5 */
    tqdb_query_where_float(q, "rating", TQDB_OP_GT, 4.5f);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 3);  /* iPhone 4.8, Vintage Radio 4.9, Test Alpha 5.0 */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_between_float(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* 4.0 <= rating <= 4.5 */
    tqdb_query_where_between_float(q, "rating", 4.0f, 4.5f);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 5);  /* Galaxy 4.5, Headphones 4.2, Coffee 4.0, Blender 4.3, Test Beta 4.5 */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_is_null(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* quantity == 0 (null for int) */
    tqdb_query_where_null(q, "quantity", true);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 1);  /* Old Laptop has 0 quantity */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_not_null(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* quantity != 0 */
    tqdb_query_where_null(q, "quantity", false);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 9);  /* All except Old Laptop */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Tests: Multiple Conditions (AND)
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool test_query_multi_condition(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* active == true AND category == "Electronics" */
    tqdb_query_where_bool(q, "active", TQDB_OP_EQ, true);
    tqdb_query_where_str(q, "category", TQDB_OP_EQ, "Electronics");

    size_t count = tqdb_query_count(q);
    ASSERT(count == 3);  /* iPhone, Galaxy, Headphones */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_three_conditions(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* active AND Electronics AND price > 50000 */
    tqdb_query_where_bool(q, "active", TQDB_OP_EQ, true);
    tqdb_query_where_str(q, "category", TQDB_OP_EQ, "Electronics");
    tqdb_query_where_i32(q, "price", TQDB_OP_GT, 50000);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 2);  /* iPhone, Galaxy */

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Tests: Limit and Offset
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    size_t count;
    uint32_t first_id;
} limit_ctx_t;

static bool limit_callback(const void* entity, void* ctx) {
    limit_ctx_t* lctx = (limit_ctx_t*)ctx;
    const test_product_t* p = (const test_product_t*)entity;
    if (lctx->count == 0) {
        lctx->first_id = p->id;
    }
    lctx->count++;
    return true;
}

static bool test_query_limit_exec(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    tqdb_query_limit(q, 3);

    limit_ctx_t ctx = { 0 };
    tqdb_query_exec(q, limit_callback, &ctx);
    ASSERT(ctx.count == 3);

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_offset(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    /* First, get the 4th product without offset */
    tqdb_query_t q1 = tqdb_query_new(db, "Product");
    tqdb_query_limit(q1, 4);
    limit_ctx_t ctx1 = { 0 };
    tqdb_query_exec(q1, limit_callback, &ctx1);
    tqdb_query_free(q1);

    /* Now query with offset=3, limit=1 should give same as 4th product */
    tqdb_query_t q2 = tqdb_query_new(db, "Product");
    tqdb_query_offset(q2, 3);
    tqdb_query_limit(q2, 1);
    limit_ctx_t ctx2 = { 0 };
    tqdb_query_exec(q2, limit_callback, &ctx2);
    ASSERT(ctx2.count == 1);

    tqdb_query_free(q2);
    tqdb_close(db);
    return true;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Tests: Edge Cases
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool test_query_no_matches(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* No products with price > 1000000 */
    tqdb_query_where_i32(q, "price", TQDB_OP_GT, 1000000);

    size_t count = tqdb_query_count(q);
    ASSERT(count == 0);

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_invalid_field(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "Product");
    ASSERT(q != NULL);

    /* Invalid field name */
    tqdb_err_t err = tqdb_query_where_i32(q, "nonexistent_field", TQDB_OP_EQ, 100);
    ASSERT(err == TQDB_ERR_NOT_FOUND);

    tqdb_query_free(q);
    tqdb_close(db);
    return true;
}

static bool test_query_invalid_type(void) {
    tqdb_t db = setup_db_with_products();
    ASSERT(db != NULL);

    tqdb_query_t q = tqdb_query_new(db, "NonexistentType");
    ASSERT(q == NULL);  /* Should fail */

    tqdb_close(db);
    return true;
}

static bool test_query_null_db(void) {
    tqdb_query_t q = tqdb_query_new(NULL, "Product");
    ASSERT(q == NULL);
    return true;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════════════ */

int main(void) {
    printf("\n");
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("  TQDB Query System Tests\n");
    printf("═══════════════════════════════════════════════════════════════\n\n");

    printf("  --- Basic Operations ---\n\n");

    TEST(query_all);
    TEST(query_eq_int);
    TEST(query_gt_int);
    TEST(query_lt_int);
    TEST(query_between_int);
    TEST(query_eq_string);
    TEST(query_eq_bool);
    TEST(query_ne_bool);
    TEST(query_gt_float);
    TEST(query_between_float);
    TEST(query_is_null);
    TEST(query_not_null);

    printf("\n  --- Pattern Matching (LIKE) ---\n\n");

    TEST(query_like_prefix);
    TEST(query_like_contains);
    TEST(query_like_suffix);
    TEST(query_like_single_char);

    printf("\n  --- Multiple Conditions ---\n\n");

    TEST(query_multi_condition);
    TEST(query_three_conditions);

    printf("\n  --- Limit/Offset ---\n\n");

    TEST(query_limit_exec);
    TEST(query_offset);

    printf("\n  --- Edge Cases ---\n\n");

    TEST(query_no_matches);
    TEST(query_invalid_field);
    TEST(query_invalid_type);
    TEST(query_null_db);

    printf("\n═══════════════════════════════════════════════════════════════\n");
    printf("  Results: %d/%d tests passed\n", tests_passed, tests_run);
    printf("═══════════════════════════════════════════════════════════════\n\n");

    cleanup();

    return (tests_passed == tests_run) ? 0 : 1;
}
