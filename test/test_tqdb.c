/**
 * @file test_tqdb.c
 * @brief TQDB unit tests
 */

#include "../tqdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define TEST_DB_PATH "test/test.tqdb"
#define TEST_WAL_PATH "test/test.tqdb.wal"

/* ═══════════════════════════════════════════════════════════════════════════
 * Test Entity: Simple Item
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    uint32_t id;
    char name[64];
    int32_t value;
    bool active;
} test_item_t;

static void item_write(tqdb_writer_t* w, const void* e) {
    const test_item_t* item = (const test_item_t*)e;
    tqdb_write_u32(w, item->id);
    tqdb_write_str(w, item->name);
    tqdb_write_i32(w, item->value);
    tqdb_write_u8(w, item->active ? 1 : 0);
}

static void item_read(tqdb_reader_t* r, void* e) {
    test_item_t* item = (test_item_t*)e;
    item->id = tqdb_read_u32(r);
    tqdb_read_str(r, item->name, sizeof(item->name));
    item->value = tqdb_read_i32(r);
    item->active = tqdb_read_u8(r) != 0;
}

static uint32_t item_get_id(const void* e) {
    return ((const test_item_t*)e)->id;
}

static void item_set_id(void* e, uint32_t id) {
    ((test_item_t*)e)->id = id;
}

static void item_init(void* e) {
    memset(e, 0, sizeof(test_item_t));
}

static void item_skip(tqdb_reader_t* r) {
    tqdb_read_skip(r, 4);   /* id */
    tqdb_read_skip_str(r);  /* name */
    tqdb_read_skip(r, 4);   /* value */
    tqdb_read_skip(r, 1);   /* active */
}

static const tqdb_trait_t ITEM_TRAIT = {
    .name = "Item",
    .max_count = 1000,
    .struct_size = sizeof(test_item_t),
    .write = item_write,
    .read = item_read,
    .get_id = item_get_id,
    .set_id = item_set_id,
    .init = item_init,
    .destroy = NULL,
    .skip = item_skip
};

/* ═══════════════════════════════════════════════════════════════════════════
 * Test Helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    printf("  %-40s ", #name); \
    fflush(stdout); \
    tests_run++; \
    if (test_##name()) { \
        printf("\033[32mPASS\033[0m\n"); \
        tests_passed++; \
    } else { \
        printf("\033[31mFAIL\033[0m\n"); \
    } \
} while(0)

#define ASSERT(cond) do { if (!(cond)) { printf("ASSERT FAILED: %s\n", #cond); return false; } } while(0)

static void cleanup(void) {
    remove(TEST_DB_PATH);
    remove(TEST_DB_PATH ".tmp");
    remove(TEST_DB_PATH ".bak");
    remove(TEST_WAL_PATH);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Tests
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool test_open_close(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };

    tqdb_err_t err = tqdb_open(&cfg, &db);
    ASSERT(err == TQDB_OK);
    ASSERT(db != NULL);

    tqdb_close(db);
    return true;
}

static bool test_register_trait(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);

    tqdb_err_t err = tqdb_register(db, &ITEM_TRAIT);
    ASSERT(err == TQDB_OK);

    /* Duplicate should fail */
    err = tqdb_register(db, &ITEM_TRAIT);
    ASSERT(err == TQDB_ERR_EXISTS);

    tqdb_close(db);
    return true;
}

static bool test_add_get(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    /* Add an item - ID will be auto-assigned */
    test_item_t item = {
        .id = 0,
        .name = "Test Item",
        .value = 42,
        .active = true
    };

    tqdb_err_t err = tqdb_add(db, "Item", &item);
    ASSERT(err == TQDB_OK);
    ASSERT(item.id == 1);  /* First ID should be 1 */

    /* Get it back */
    test_item_t retrieved;
    err = tqdb_get(db, "Item", 1, &retrieved);
    ASSERT(err == TQDB_OK);
    ASSERT(retrieved.id == 1);
    ASSERT(strcmp(retrieved.name, "Test Item") == 0);
    ASSERT(retrieved.value == 42);
    ASSERT(retrieved.active == true);

    tqdb_close(db);
    return true;
}

static bool test_add_sequential_ids(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    /* Add multiple items - IDs should be auto-assigned sequentially */
    test_item_t item1 = { .id = 0, .name = "Item 1", .value = 1 };
    ASSERT(tqdb_add(db, "Item", &item1) == TQDB_OK);
    ASSERT(item1.id == 1);

    test_item_t item2 = { .id = 0, .name = "Item 2", .value = 2 };
    ASSERT(tqdb_add(db, "Item", &item2) == TQDB_OK);
    ASSERT(item2.id == 2);

    test_item_t item3 = { .id = 0, .name = "Item 3", .value = 3 };
    ASSERT(tqdb_add(db, "Item", &item3) == TQDB_OK);
    ASSERT(item3.id == 3);

    tqdb_close(db);
    return true;
}

static bool test_update(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    test_item_t item = { .id = 0, .name = "Original", .value = 10 };
    tqdb_add(db, "Item", &item);
    uint32_t item_id = item.id;

    /* Update it */
    strcpy(item.name, "Updated");
    item.value = 20;
    tqdb_err_t err = tqdb_update(db, "Item", item_id, &item);
    ASSERT(err == TQDB_OK);

    /* Verify update */
    test_item_t retrieved;
    tqdb_get(db, "Item", item_id, &retrieved);
    ASSERT(strcmp(retrieved.name, "Updated") == 0);
    ASSERT(retrieved.value == 20);

    tqdb_close(db);
    return true;
}

static bool test_delete(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    test_item_t item = { .id = 0, .name = "To Delete", .value = 99 };
    tqdb_add(db, "Item", &item);
    uint32_t item_id = item.id;

    ASSERT(tqdb_exists(db, "Item", item_id) == true);

    tqdb_err_t err = tqdb_delete(db, "Item", item_id);
    ASSERT(err == TQDB_OK);

    ASSERT(tqdb_exists(db, "Item", item_id) == false);

    tqdb_close(db);
    return true;
}

static bool test_count(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    ASSERT(tqdb_count(db, "Item") == 0);

    test_item_t item;
    uint32_t ids[5];
    for (int i = 0; i < 5; i++) {
        item.id = 0;
        snprintf(item.name, sizeof(item.name), "Item %d", i);
        item.value = i;
        tqdb_add(db, "Item", &item);
        ids[i] = item.id;
    }

    ASSERT(tqdb_count(db, "Item") == 5);

    tqdb_delete(db, "Item", ids[2]);
    ASSERT(tqdb_count(db, "Item") == 4);

    tqdb_close(db);
    return true;
}

static int foreach_count;
static bool foreach_callback(const void* entity, void* ctx) {
    (void)entity;
    (void)ctx;
    foreach_count++;
    return true;
}

static bool test_foreach(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    test_item_t item;
    for (int i = 0; i < 10; i++) {
        item.id = 0;
        snprintf(item.name, sizeof(item.name), "Item %d", i);
        item.value = i * 10;
        tqdb_add(db, "Item", &item);
    }

    foreach_count = 0;
    tqdb_err_t err = tqdb_foreach(db, "Item", foreach_callback, NULL);
    ASSERT(err == TQDB_OK);
    ASSERT(foreach_count == 10);

    tqdb_close(db);
    return true;
}

static bool test_persistence(void) {
    cleanup();

    uint32_t id1, id2;

    /* Create and add items */
    {
        tqdb_t db;
        tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
        tqdb_open(&cfg, &db);
        tqdb_register(db, &ITEM_TRAIT);

        test_item_t item = { .id = 0, .name = "Persistent", .value = 123 };
        tqdb_add(db, "Item", &item);
        id1 = item.id;

        item.id = 0;
        strcpy(item.name, "Also Persistent");
        item.value = 456;
        tqdb_add(db, "Item", &item);
        id2 = item.id;

        tqdb_close(db);
    }

    /* Reopen and verify */
    {
        tqdb_t db;
        tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
        tqdb_open(&cfg, &db);
        tqdb_register(db, &ITEM_TRAIT);

        ASSERT(tqdb_count(db, "Item") == 2);

        test_item_t retrieved;
        ASSERT(tqdb_get(db, "Item", id1, &retrieved) == TQDB_OK);
        ASSERT(strcmp(retrieved.name, "Persistent") == 0);
        ASSERT(retrieved.value == 123);

        ASSERT(tqdb_get(db, "Item", id2, &retrieved) == TQDB_OK);
        ASSERT(strcmp(retrieved.name, "Also Persistent") == 0);
        ASSERT(retrieved.value == 456);

        tqdb_close(db);
    }

    return true;
}

static void double_value(void* entity, void* ctx) {
    (void)ctx;
    test_item_t* item = (test_item_t*)entity;
    item->value *= 2;
}

static bool filter_even(const void* entity, void* ctx) {
    (void)ctx;
    const test_item_t* item = (const test_item_t*)entity;
    return (item->value % 2) == 0;
}

static bool test_modify_where(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    test_item_t item;
    uint32_t ids[5];
    for (int i = 0; i < 5; i++) {
        item.id = 0;
        snprintf(item.name, sizeof(item.name), "Item %d", i);
        item.value = i + 1;  /* 1, 2, 3, 4, 5 */
        tqdb_add(db, "Item", &item);
        ids[i] = item.id;
    }

    /* Double all even values */
    tqdb_err_t err = tqdb_modify_where(db, "Item", filter_even, NULL, double_value, NULL);
    ASSERT(err == TQDB_OK);

    /* Verify: 1, 4, 3, 8, 5 */
    test_item_t retrieved;
    tqdb_get(db, "Item", ids[0], &retrieved);
    ASSERT(retrieved.value == 1);  /* odd, unchanged */

    tqdb_get(db, "Item", ids[1], &retrieved);
    ASSERT(retrieved.value == 4);  /* 2 * 2 = 4 */

    tqdb_get(db, "Item", ids[3], &retrieved);
    ASSERT(retrieved.value == 8);  /* 4 * 2 = 8 */

    tqdb_close(db);
    return true;
}

static bool keep_active(const void* entity, void* ctx) {
    (void)ctx;
    return ((const test_item_t*)entity)->active;
}

static bool test_delete_where(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    test_item_t item;
    uint32_t ids[6];
    for (int i = 0; i < 6; i++) {
        item.id = 0;
        snprintf(item.name, sizeof(item.name), "Item %d", i);
        item.value = i;
        item.active = (i % 2) == 0;  /* 0, 2, 4 are active */
        tqdb_add(db, "Item", &item);
        ids[i] = item.id;
    }

    ASSERT(tqdb_count(db, "Item") == 6);

    /* Delete inactive items (filter returns false for inactive) */
    tqdb_err_t err = tqdb_delete_where(db, "Item", keep_active, NULL);
    ASSERT(err == TQDB_OK);

    ASSERT(tqdb_count(db, "Item") == 3);

    /* Only active items should remain */
    ASSERT(tqdb_exists(db, "Item", ids[0]) == true);
    ASSERT(tqdb_exists(db, "Item", ids[1]) == false);
    ASSERT(tqdb_exists(db, "Item", ids[2]) == true);
    ASSERT(tqdb_exists(db, "Item", ids[3]) == false);
    ASSERT(tqdb_exists(db, "Item", ids[4]) == true);
    ASSERT(tqdb_exists(db, "Item", ids[5]) == false);

    tqdb_close(db);
    return true;
}

static bool test_not_found(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    test_item_t item;
    ASSERT(tqdb_get(db, "Item", 999, &item) == TQDB_ERR_NOT_FOUND);
    ASSERT(tqdb_update(db, "Item", 999, &item) == TQDB_ERR_NOT_FOUND);
    ASSERT(tqdb_delete(db, "Item", 999) == TQDB_ERR_NOT_FOUND);
    ASSERT(tqdb_exists(db, "Item", 999) == false);

    tqdb_close(db);
    return true;
}

static bool test_unregistered_type(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = { .db_path = TEST_DB_PATH };
    tqdb_open(&cfg, &db);
    /* Don't register any traits */

    test_item_t item = { .id = 0, .name = "Test" };
    ASSERT(tqdb_add(db, "Item", &item) == TQDB_ERR_NOT_REGISTERED);
    ASSERT(tqdb_get(db, "Item", 1, &item) == TQDB_ERR_NOT_REGISTERED);
    ASSERT(tqdb_count(db, "Item") == 0);

    tqdb_close(db);
    return true;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * WAL Tests
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool test_wal_basic(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = {
        .db_path = TEST_DB_PATH,
        .enable_wal = true,
        .wal_path = TEST_WAL_PATH,
        .wal_max_entries = 100,
        .wal_max_size = 65536
    };

    tqdb_err_t err = tqdb_open(&cfg, &db);
    ASSERT(err == TQDB_OK);

    err = tqdb_register(db, &ITEM_TRAIT);
    ASSERT(err == TQDB_OK);

    /* Add items (should go to WAL) */
    test_item_t item = { .id = 0, .name = "WAL Item 1", .value = 100 };
    err = tqdb_add(db, "Item", &item);
    ASSERT(err == TQDB_OK);
    uint32_t id1 = item.id;

    item.id = 0;
    strcpy(item.name, "WAL Item 2");
    item.value = 200;
    err = tqdb_add(db, "Item", &item);
    ASSERT(err == TQDB_OK);
    uint32_t id2 = item.id;

    /* Verify items are retrievable (from WAL) */
    test_item_t retrieved;
    err = tqdb_get(db, "Item", id1, &retrieved);
    ASSERT(err == TQDB_OK);
    ASSERT(retrieved.value == 100);

    err = tqdb_get(db, "Item", id2, &retrieved);
    ASSERT(err == TQDB_OK);
    ASSERT(retrieved.value == 200);

    /* Check WAL stats */
    size_t entries, size;
    tqdb_wal_stats(db, &entries, &size);
    ASSERT(entries == 2);
    ASSERT(size > 0);

    tqdb_close(db);
    return true;
}

static bool test_wal_persistence(void) {
    cleanup();

    uint32_t id1, id2;

    /* Create DB with WAL and add items */
    {
        tqdb_t db;
        tqdb_config_t cfg = {
            .db_path = TEST_DB_PATH,
            .enable_wal = true,
            .wal_path = TEST_WAL_PATH,
            .wal_max_entries = 100
        };

        tqdb_open(&cfg, &db);
        tqdb_register(db, &ITEM_TRAIT);

        test_item_t item = { .id = 0, .name = "Persist 1", .value = 111 };
        tqdb_add(db, "Item", &item);
        id1 = item.id;

        item.id = 0;
        strcpy(item.name, "Persist 2");
        item.value = 222;
        tqdb_add(db, "Item", &item);
        id2 = item.id;

        /* Close triggers checkpoint */
        tqdb_close(db);
    }

    /* Reopen and verify */
    {
        tqdb_t db;
        tqdb_config_t cfg = {
            .db_path = TEST_DB_PATH,
            .enable_wal = true,
            .wal_path = TEST_WAL_PATH
        };

        tqdb_open(&cfg, &db);
        tqdb_register(db, &ITEM_TRAIT);

        ASSERT(tqdb_count(db, "Item") == 2);

        test_item_t retrieved;
        ASSERT(tqdb_get(db, "Item", id1, &retrieved) == TQDB_OK);
        ASSERT(retrieved.value == 111);

        ASSERT(tqdb_get(db, "Item", id2, &retrieved) == TQDB_OK);
        ASSERT(retrieved.value == 222);

        tqdb_close(db);
    }

    return true;
}

static bool test_wal_update_delete(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = {
        .db_path = TEST_DB_PATH,
        .enable_wal = true,
        .wal_path = TEST_WAL_PATH,
        .wal_max_entries = 100
    };

    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    /* Add initial item */
    test_item_t item = { .id = 0, .name = "Original", .value = 50 };
    tqdb_add(db, "Item", &item);
    uint32_t upd_id = item.id;

    /* Update via WAL */
    strcpy(item.name, "Updated");
    item.value = 100;
    tqdb_err_t err = tqdb_update(db, "Item", upd_id, &item);
    ASSERT(err == TQDB_OK);

    /* Verify update */
    test_item_t retrieved;
    tqdb_get(db, "Item", upd_id, &retrieved);
    ASSERT(strcmp(retrieved.name, "Updated") == 0);
    ASSERT(retrieved.value == 100);

    /* Add another and delete it */
    item.id = 0;
    strcpy(item.name, "To Delete");
    tqdb_add(db, "Item", &item);
    uint32_t del_id = item.id;
    ASSERT(tqdb_exists(db, "Item", del_id) == true);

    err = tqdb_delete(db, "Item", del_id);
    ASSERT(err == TQDB_OK);
    ASSERT(tqdb_exists(db, "Item", del_id) == false);

    tqdb_close(db);
    return true;
}

static bool test_wal_checkpoint(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = {
        .db_path = TEST_DB_PATH,
        .enable_wal = true,
        .wal_path = TEST_WAL_PATH,
        .wal_max_entries = 1000  /* High threshold so we control checkpoint */
    };

    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    /* Add several items */
    test_item_t item;
    uint32_t ids[10];
    for (int i = 0; i < 10; i++) {
        item.id = 0;
        snprintf(item.name, sizeof(item.name), "Item %d", i);
        item.value = i * 10;
        tqdb_add(db, "Item", &item);
        ids[i] = item.id;
    }

    /* Verify WAL has entries */
    size_t entries;
    tqdb_wal_stats(db, &entries, NULL);
    ASSERT(entries == 10);

    /* Manual checkpoint */
    tqdb_err_t err = tqdb_checkpoint(db);
    ASSERT(err == TQDB_OK);

    /* WAL should be empty after checkpoint */
    tqdb_wal_stats(db, &entries, NULL);
    ASSERT(entries == 0);

    /* Items should still be accessible */
    ASSERT(tqdb_count(db, "Item") == 10);

    test_item_t retrieved;
    tqdb_get(db, "Item", ids[5], &retrieved);
    ASSERT(retrieved.value == 50);

    tqdb_close(db);
    return true;
}

static bool test_wal_auto_checkpoint(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = {
        .db_path = TEST_DB_PATH,
        .enable_wal = true,
        .wal_path = TEST_WAL_PATH,
        .wal_max_entries = 5  /* Low threshold for auto-checkpoint */
    };

    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    /* Add items - should trigger checkpoint after 5 */
    test_item_t item;
    for (int i = 0; i < 7; i++) {
        item.id = 0;
        snprintf(item.name, sizeof(item.name), "Item %d", i);
        item.value = i;
        tqdb_add(db, "Item", &item);
    }

    /* WAL should have been checkpointed and have only entries after checkpoint */
    size_t entries;
    tqdb_wal_stats(db, &entries, NULL);
    ASSERT(entries < 7);  /* Some entries were checkpointed */

    /* All items should be accessible */
    ASSERT(tqdb_count(db, "Item") == 7);

    tqdb_close(db);
    return true;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Cache Tests
 * ═══════════════════════════════════════════════════════════════════════════ */

static bool test_cache_basic(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = {
        .db_path = TEST_DB_PATH,
        .enable_cache = true,
        .cache_size = 8
    };

    tqdb_err_t err = tqdb_open(&cfg, &db);
    ASSERT(err == TQDB_OK);

    tqdb_register(db, &ITEM_TRAIT);

    /* Add an item */
    test_item_t item = { .id = 0, .name = "Cached Item", .value = 42 };
    tqdb_add(db, "Item", &item);
    uint32_t item_id = item.id;

    /* First get - cache miss */
    test_item_t retrieved;
    tqdb_get(db, "Item", item_id, &retrieved);
    ASSERT(retrieved.value == 42);

    /* Second get - should be cache hit */
    tqdb_get(db, "Item", item_id, &retrieved);
    ASSERT(retrieved.value == 42);

    /* Check cache stats */
    size_t hits, misses;
    tqdb_cache_stats(db, &hits, &misses);
    ASSERT(hits >= 1);  /* At least one hit */

    tqdb_close(db);
    return true;
}

static bool test_cache_with_wal(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = {
        .db_path = TEST_DB_PATH,
        .enable_wal = true,
        .wal_path = TEST_WAL_PATH,
        .enable_cache = true,
        .cache_size = 16
    };

    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    /* Add items (goes to WAL, should also populate cache) */
    test_item_t item;
    uint32_t ids[5];
    for (int i = 0; i < 5; i++) {
        item.id = 0;
        snprintf(item.name, sizeof(item.name), "Item %d", i);
        item.value = i * 100;
        tqdb_add(db, "Item", &item);
        ids[i] = item.id;
    }

    /* Get items - should be cache hits since WAL append updates cache */
    test_item_t retrieved;
    for (int i = 0; i < 5; i++) {
        tqdb_err_t err = tqdb_get(db, "Item", ids[i], &retrieved);
        ASSERT(err == TQDB_OK);
        ASSERT(retrieved.value == i * 100);
    }

    /* Check stats - should have cache hits */
    size_t hits, misses;
    tqdb_cache_stats(db, &hits, &misses);
    ASSERT(hits >= 5);

    /* Update an item */
    tqdb_get(db, "Item", ids[2], &retrieved);
    retrieved.value = 999;
    tqdb_update(db, "Item", ids[2], &retrieved);

    /* Get updated value - should reflect update in cache */
    tqdb_get(db, "Item", ids[2], &retrieved);
    ASSERT(retrieved.value == 999);

    tqdb_close(db);
    return true;
}

static bool test_cache_clear(void) {
    cleanup();

    tqdb_t db;
    tqdb_config_t cfg = {
        .db_path = TEST_DB_PATH,
        .enable_cache = true,
        .cache_size = 8
    };

    tqdb_open(&cfg, &db);
    tqdb_register(db, &ITEM_TRAIT);

    test_item_t item = { .id = 0, .name = "Item", .value = 1 };
    tqdb_add(db, "Item", &item);
    uint32_t item_id = item.id;

    /* Get to populate cache */
    test_item_t retrieved;
    tqdb_get(db, "Item", item_id, &retrieved);
    tqdb_get(db, "Item", item_id, &retrieved);

    size_t hits_before;
    tqdb_cache_stats(db, &hits_before, NULL);
    ASSERT(hits_before >= 1);

    /* Clear cache */
    tqdb_cache_clear(db);

    /* Stats should be reset */
    size_t hits_after, misses_after;
    tqdb_cache_stats(db, &hits_after, &misses_after);
    ASSERT(hits_after == 0);
    ASSERT(misses_after == 0);

    tqdb_close(db);
    return true;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════════════ */

int main(void) {
    printf("\n");
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("  TQDB Unit Tests\n");
    printf("═══════════════════════════════════════════════════════════════\n\n");

    TEST(open_close);
    TEST(register_trait);
    TEST(add_get);
    TEST(add_sequential_ids);
    TEST(update);
    TEST(delete);
    TEST(count);
    TEST(foreach);
    TEST(persistence);
    TEST(modify_where);
    TEST(delete_where);
    TEST(not_found);
    TEST(unregistered_type);

    printf("\n  --- WAL Tests ---\n\n");

    TEST(wal_basic);
    TEST(wal_persistence);
    TEST(wal_update_delete);
    TEST(wal_checkpoint);
    TEST(wal_auto_checkpoint);

    printf("\n  --- Cache Tests ---\n\n");

    TEST(cache_basic);
    TEST(cache_with_wal);
    TEST(cache_clear);

    printf("\n═══════════════════════════════════════════════════════════════\n");
    printf("  Results: %d/%d tests passed\n", tests_passed, tests_run);
    printf("═══════════════════════════════════════════════════════════════\n\n");

    cleanup();

    return (tests_passed == tests_run) ? 0 : 1;
}
