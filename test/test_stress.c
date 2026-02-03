/**
 * @file test_stress.c
 * @brief TQDB stress test with multiple types, timing, and crash simulation
 */

#include "../tqdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <sys/time.h>

#define TEST_DB_PATH "test/stress.tqdb"
#define TEST_WAL_PATH "test/stress.tqdb.wal"
#define ITEM_COUNT 100

/* ═══════════════════════════════════════════════════════════════════════════
 * Timing Helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static double get_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

static double _t_start;
#define TIME_START() _t_start = get_time_ms()
#define TIME_END(name) printf("  %-35s %8.2f ms\n", name, get_time_ms() - _t_start)

/* ═══════════════════════════════════════════════════════════════════════════
 * Entity Type 1: User
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    uint32_t id;
    char username[32];
    char email[64];
    int32_t age;
    bool active;
    int64_t created_at;
} user_t;

static void user_write(tqdb_writer_t* w, const void* e) {
    const user_t* u = (const user_t*)e;
    tqdb_write_u32(w, u->id);
    tqdb_write_str(w, u->username);
    tqdb_write_str(w, u->email);
    tqdb_write_i32(w, u->age);
    tqdb_write_u8(w, u->active ? 1 : 0);
    tqdb_write_i64(w, u->created_at);
}

static void user_read(tqdb_reader_t* r, void* e) {
    user_t* u = (user_t*)e;
    u->id = tqdb_read_u32(r);
    tqdb_read_str(r, u->username, sizeof(u->username));
    tqdb_read_str(r, u->email, sizeof(u->email));
    u->age = tqdb_read_i32(r);
    u->active = tqdb_read_u8(r) != 0;
    u->created_at = tqdb_read_i64(r);
}

static uint32_t user_get_id(const void* e) { return ((const user_t*)e)->id; }
static void user_set_id(void* e, uint32_t id) { ((user_t*)e)->id = id; }
static void user_init(void* e) { memset(e, 0, sizeof(user_t)); }
static void user_skip(tqdb_reader_t* r) {
    tqdb_read_skip(r, 4);  /* id */
    tqdb_read_skip_str(r); tqdb_read_skip_str(r);
    tqdb_read_skip(r, 4 + 1 + 8);
}

static const tqdb_trait_t USER_TRAIT = {
    .name = "User", .max_count = 10000, .struct_size = sizeof(user_t),
    .write = user_write, .read = user_read, .get_id = user_get_id,
    .set_id = user_set_id, .init = user_init, .skip = user_skip
};

/* ═══════════════════════════════════════════════════════════════════════════
 * Entity Type 2: Product
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    uint32_t id;
    char name[64];
    char description[256];
    int32_t price_cents;
    int32_t stock;
    char category[32];
} product_t;

static void product_write(tqdb_writer_t* w, const void* e) {
    const product_t* p = (const product_t*)e;
    tqdb_write_u32(w, p->id);
    tqdb_write_str(w, p->name);
    tqdb_write_str(w, p->description);
    tqdb_write_i32(w, p->price_cents);
    tqdb_write_i32(w, p->stock);
    tqdb_write_str(w, p->category);
}

static void product_read(tqdb_reader_t* r, void* e) {
    product_t* p = (product_t*)e;
    p->id = tqdb_read_u32(r);
    tqdb_read_str(r, p->name, sizeof(p->name));
    tqdb_read_str(r, p->description, sizeof(p->description));
    p->price_cents = tqdb_read_i32(r);
    p->stock = tqdb_read_i32(r);
    tqdb_read_str(r, p->category, sizeof(p->category));
}

static uint32_t product_get_id(const void* e) { return ((const product_t*)e)->id; }
static void product_set_id(void* e, uint32_t id) { ((product_t*)e)->id = id; }
static void product_init(void* e) { memset(e, 0, sizeof(product_t)); }
static void product_skip(tqdb_reader_t* r) {
    tqdb_read_skip(r, 4);  /* id */
    tqdb_read_skip_str(r); tqdb_read_skip_str(r);
    tqdb_read_skip(r, 4 + 4);
    tqdb_read_skip_str(r);
}

static const tqdb_trait_t PRODUCT_TRAIT = {
    .name = "Product", .max_count = 10000, .struct_size = sizeof(product_t),
    .write = product_write, .read = product_read, .get_id = product_get_id,
    .set_id = product_set_id, .init = product_init, .skip = product_skip
};

/* ═══════════════════════════════════════════════════════════════════════════
 * Entity Type 3: Order
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    uint32_t id;
    uint32_t user_id;
    uint32_t product_id;
    int32_t quantity;
    int32_t total_cents;
    int64_t order_date;
    uint8_t status;  /* 0=pending, 1=shipped, 2=delivered, 3=cancelled */
} order_t;

static void order_write(tqdb_writer_t* w, const void* e) {
    const order_t* o = (const order_t*)e;
    tqdb_write_u32(w, o->id);
    tqdb_write_u32(w, o->user_id);
    tqdb_write_u32(w, o->product_id);
    tqdb_write_i32(w, o->quantity);
    tqdb_write_i32(w, o->total_cents);
    tqdb_write_i64(w, o->order_date);
    tqdb_write_u8(w, o->status);
}

static void order_read(tqdb_reader_t* r, void* e) {
    order_t* o = (order_t*)e;
    o->id = tqdb_read_u32(r);
    o->user_id = tqdb_read_u32(r);
    o->product_id = tqdb_read_u32(r);
    o->quantity = tqdb_read_i32(r);
    o->total_cents = tqdb_read_i32(r);
    o->order_date = tqdb_read_i64(r);
    o->status = tqdb_read_u8(r);
}

static uint32_t order_get_id(const void* e) { return ((const order_t*)e)->id; }
static void order_set_id(void* e, uint32_t id) { ((order_t*)e)->id = id; }
static void order_init(void* e) { memset(e, 0, sizeof(order_t)); }
static void order_skip(tqdb_reader_t* r) {
    tqdb_read_skip(r, 4 + 4 + 4);  /* id, user_id, product_id */
    tqdb_read_skip(r, 4 + 4 + 8 + 1);
}

static const tqdb_trait_t ORDER_TRAIT = {
    .name = "Order", .max_count = 10000, .struct_size = sizeof(order_t),
    .write = order_write, .read = order_read, .get_id = order_get_id,
    .set_id = order_set_id, .init = order_init, .skip = order_skip
};

/* ═══════════════════════════════════════════════════════════════════════════
 * Test Helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static void cleanup(void) {
    remove(TEST_DB_PATH);
    remove(TEST_DB_PATH ".tmp");
    remove(TEST_DB_PATH ".bak");
    remove(TEST_WAL_PATH);
}

static tqdb_t open_db(bool with_wal, bool with_cache) {
    tqdb_t db;
    tqdb_config_t cfg = {
        .db_path = TEST_DB_PATH,
        .enable_wal = with_wal,
        .wal_path = TEST_WAL_PATH,
        .wal_max_entries = 50,
        .wal_max_size = 32768,
        .enable_cache = with_cache,
        .cache_size = 64   /* Must be >= working set to avoid LRU thrashing */
    };
    if (tqdb_open(&cfg, &db) != TQDB_OK) {
        fprintf(stderr, "Failed to open database\n");
        exit(1);
    }
    tqdb_register(db, &USER_TRAIT);
    tqdb_register(db, &PRODUCT_TRAIT);
    tqdb_register(db, &ORDER_TRAIT);
    return db;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: Bulk Insert
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Store IDs for later lookup tests */
static uint32_t* user_ids;
static uint32_t* product_ids;
static uint32_t* order_ids;

static void test_bulk_insert(tqdb_t db, int count) {
    printf("\n--- Bulk Insert (%d items each type) ---\n", count);

    /* Allocate ID arrays */
    user_ids = (uint32_t*)malloc(count * sizeof(uint32_t));
    product_ids = (uint32_t*)malloc(count * sizeof(uint32_t));
    order_ids = (uint32_t*)malloc(count * sizeof(uint32_t));

    /* Insert users */
    {
        TIME_START();
        user_t u;
        for (int i = 0; i < count; i++) {
            u.id = 0;  /* Auto-assign */
            snprintf(u.username, sizeof(u.username), "user_%d", i);
            snprintf(u.email, sizeof(u.email), "user%d@example.com", i);
            u.age = 18 + (i % 60);
            u.active = (i % 3) != 0;
            u.created_at = time(NULL) - (i * 3600);
            tqdb_add(db, "User", &u);
            user_ids[i] = u.id;
        }
        TIME_END("Insert Users:");
    }

    /* Insert products */
    {
        TIME_START();
        product_t p;
        for (int i = 0; i < count; i++) {
            p.id = 0;  /* Auto-assign */
            snprintf(p.name, sizeof(p.name), "Product %d", i);
            snprintf(p.description, sizeof(p.description), "Description for product %d with some extra text", i);
            p.price_cents = 100 + (i * 50);
            p.stock = 10 + (i % 100);
            snprintf(p.category, sizeof(p.category), "Category-%d", i % 10);
            tqdb_add(db, "Product", &p);
            product_ids[i] = p.id;
        }
        TIME_END("Insert Products:");
    }

    /* Insert orders */
    {
        TIME_START();
        order_t o;
        for (int i = 0; i < count; i++) {
            o.id = 0;  /* Auto-assign */
            o.user_id = user_ids[i % count];
            o.product_id = product_ids[(i * 7) % count];
            o.quantity = 1 + (i % 5);
            o.total_cents = o.quantity * (100 + ((i * 7) % count) * 50);
            o.order_date = time(NULL) - (i * 1800);
            o.status = i % 4;
            tqdb_add(db, "Order", &o);
            order_ids[i] = o.id;
        }
        TIME_END("Insert Orders:");
    }

    printf("  Counts: Users=%zu, Products=%zu, Orders=%zu\n",
           tqdb_count(db, "User"), tqdb_count(db, "Product"), tqdb_count(db, "Order"));
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: Random Reads
 * ═══════════════════════════════════════════════════════════════════════════ */

static void test_random_reads(tqdb_t db, int count, int reads) {
    printf("\n--- Random Reads (%d reads) ---\n", reads);

    user_t u;
    product_t p;
    order_t o;

    {
        TIME_START();
        for (int i = 0; i < reads; i++) {
            tqdb_get(db, "User", user_ids[rand() % count], &u);
        }
        TIME_END("Random User reads:");
    }

    {
        TIME_START();
        for (int i = 0; i < reads; i++) {
            tqdb_get(db, "Product", product_ids[rand() % count], &p);
        }
        TIME_END("Random Product reads:");
    }

    {
        TIME_START();
        for (int i = 0; i < reads; i++) {
            tqdb_get(db, "Order", order_ids[rand() % count], &o);
        }
        TIME_END("Random Order reads:");
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: Updates
 * ═══════════════════════════════════════════════════════════════════════════ */

static void test_updates(tqdb_t db, int count, int updates) {
    printf("\n--- Updates (%d updates) ---\n", updates);

    user_t u;
    product_t p;

    {
        TIME_START();
        for (int i = 0; i < updates; i++) {
            int idx = rand() % count;
            if (tqdb_get(db, "User", user_ids[idx], &u) == TQDB_OK) {
                u.age++;
                u.active = !u.active;
                tqdb_update(db, "User", user_ids[idx], &u);
            }
        }
        TIME_END("Update Users:");
    }

    {
        TIME_START();
        for (int i = 0; i < updates; i++) {
            int idx = rand() % count;
            if (tqdb_get(db, "Product", product_ids[idx], &p) == TQDB_OK) {
                p.stock += 10;
                p.price_cents += 100;
                tqdb_update(db, "Product", product_ids[idx], &p);
            }
        }
        TIME_END("Update Products:");
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: Deletes
 * ═══════════════════════════════════════════════════════════════════════════ */

static void test_deletes(tqdb_t db, int count, int deletes) {
    printf("\n--- Deletes (%d deletes) ---\n", deletes);

    {
        TIME_START();
        for (int i = 0; i < deletes; i++) {
            tqdb_delete(db, "User", user_ids[count - 1 - i]);
        }
        TIME_END("Delete Users:");
    }

    {
        TIME_START();
        for (int i = 0; i < deletes; i++) {
            tqdb_delete(db, "Product", product_ids[count - 1 - i]);
        }
        TIME_END("Delete Products:");
    }

    printf("  Counts after delete: Users=%zu, Products=%zu, Orders=%zu\n",
           tqdb_count(db, "User"), tqdb_count(db, "Product"), tqdb_count(db, "Order"));
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: Iteration
 * ═══════════════════════════════════════════════════════════════════════════ */

static int iter_count;
static bool count_iter(const void* e, void* ctx) {
    (void)e; (void)ctx;
    iter_count++;
    return true;
}

static void test_iteration(tqdb_t db) {
    printf("\n--- Iteration ---\n");

    {
        TIME_START();
        iter_count = 0;
        tqdb_foreach(db, "User", count_iter, NULL);
        TIME_END("Iterate all Users:");
        printf("  Iterated %d users\n", iter_count);
    }

    {
        TIME_START();
        iter_count = 0;
        tqdb_foreach(db, "Product", count_iter, NULL);
        TIME_END("Iterate all Products:");
        printf("  Iterated %d products\n", iter_count);
    }

    {
        TIME_START();
        iter_count = 0;
        tqdb_foreach(db, "Order", count_iter, NULL);
        TIME_END("Iterate all Orders:");
        printf("  Iterated %d orders\n", iter_count);
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: WAL Stats
 * ═══════════════════════════════════════════════════════════════════════════ */

static void test_wal_stats(tqdb_t db) {
    size_t entries, size;
    tqdb_wal_stats(db, &entries, &size);
    printf("\n--- WAL Stats ---\n");
    printf("  Entries: %zu, Size: %zu bytes\n", entries, size);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: Cache Stats
 * ═══════════════════════════════════════════════════════════════════════════ */

static void test_cache_stats(tqdb_t db) {
    size_t hits, misses;
    tqdb_cache_stats(db, &hits, &misses);
    printf("\n--- Cache Stats ---\n");
    printf("  Hits: %zu, Misses: %zu", hits, misses);
    if (hits + misses > 0) {
        printf(", Hit rate: %.1f%%", (double)hits / (hits + misses) * 100);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: Checkpoint
 * ═══════════════════════════════════════════════════════════════════════════ */

static void test_checkpoint(tqdb_t db) {
    printf("\n--- Manual Checkpoint ---\n");
    TIME_START();
    tqdb_checkpoint(db);
    TIME_END("Checkpoint:");

    size_t entries, size;
    tqdb_wal_stats(db, &entries, &size);
    printf("  WAL after checkpoint: %zu entries, %zu bytes\n", entries, size);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: Persistence (close and reopen)
 * ═══════════════════════════════════════════════════════════════════════════ */

static void test_persistence(bool with_wal, bool with_cache) {
    printf("\n--- Persistence Test (close/reopen) ---\n");

    size_t users_before, products_before, orders_before;
    {
        tqdb_t db = open_db(with_wal, with_cache);
        users_before = tqdb_count(db, "User");
        products_before = tqdb_count(db, "Product");
        orders_before = tqdb_count(db, "Order");
        printf("  Before close: Users=%zu, Products=%zu, Orders=%zu\n",
               users_before, products_before, orders_before);

        TIME_START();
        tqdb_close(db);
        TIME_END("Close (with checkpoint):");
    }

    {
        TIME_START();
        tqdb_t db = open_db(with_wal, with_cache);
        TIME_END("Reopen:");

        size_t users_after = tqdb_count(db, "User");
        size_t products_after = tqdb_count(db, "Product");
        size_t orders_after = tqdb_count(db, "Order");
        printf("  After reopen: Users=%zu, Products=%zu, Orders=%zu\n",
               users_after, products_after, orders_after);

        if (users_after == users_before && products_after == products_before &&
            orders_after == orders_before) {
            printf("  \033[32mPersistence OK\033[0m\n");
        } else {
            printf("  \033[31mPersistence FAILED\033[0m\n");
        }

        tqdb_close(db);
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: Crash Simulation
 * ═══════════════════════════════════════════════════════════════════════════ */

static void test_crash_simulation(void) {
    printf("\n");
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("  CRASH SIMULATION TESTS\n");
    printf("═══════════════════════════════════════════════════════════════\n");

    /* Test 1: Crash after adding items but before checkpoint */
    printf("\n--- Test 1: Crash before checkpoint ---\n");
    cleanup();
    {
        tqdb_t db = open_db(true, false);

        /* Add some items */
        user_t u;
        for (int i = 0; i < 20; i++) {
            u.id = 0;  /* Auto-assign */
            snprintf(u.username, sizeof(u.username), "crashtest_%d", i);
            snprintf(u.email, sizeof(u.email), "crash%d@test.com", i);
            u.age = 25; u.active = true; u.created_at = time(NULL);
            tqdb_add(db, "User", &u);
        }

        size_t wal_entries;
        tqdb_wal_stats(db, &wal_entries, NULL);
        printf("  Added 20 users, WAL entries: %zu\n", wal_entries);
        printf("  Simulating crash (not closing properly)...\n");

        /* DON'T close - simulate crash by just abandoning the handle */
        /* In real crash, memory would leak, but for test we need to clean up */
        /* The WAL file should still have our data */
    }

    /* Reopen and check recovery */
    {
        tqdb_t db = open_db(true, false);
        size_t count = tqdb_count(db, "User");
        printf("  After recovery: %zu users\n", count);

        /* WAL recovery happens on open, data should be checkpointed */
        if (count == 20) {
            printf("  \033[32mRecovery OK\033[0m\n");
        } else {
            printf("  \033[31mRecovery FAILED (expected 20, got %zu)\033[0m\n", count);
        }

        tqdb_close(db);
    }

    /* Test 2: Crash during checkpoint (partial write) */
    printf("\n--- Test 2: Verify data integrity after normal close ---\n");
    cleanup();
    uint32_t test2_user_ids[30];
    uint32_t test2_product_ids[25];
    {
        tqdb_t db = open_db(true, true);

        /* Add items across all types */
        user_t u;
        for (int i = 0; i < 30; i++) {
            u.id = 0;  /* Auto-assign */
            snprintf(u.username, sizeof(u.username), "user_%d", i);
            snprintf(u.email, sizeof(u.email), "u%d@t.com", i);
            u.age = 20 + i; u.active = true; u.created_at = time(NULL);
            tqdb_add(db, "User", &u);
            test2_user_ids[i] = u.id;
        }

        product_t p;
        for (int i = 0; i < 25; i++) {
            p.id = 0;  /* Auto-assign */
            snprintf(p.name, sizeof(p.name), "Product %d", i);
            snprintf(p.description, sizeof(p.description), "Desc %d", i);
            p.price_cents = 1000 + i * 100;
            p.stock = 50;
            snprintf(p.category, sizeof(p.category), "cat%d", i % 5);
            tqdb_add(db, "Product", &p);
            test2_product_ids[i] = p.id;
        }

        /* Update some items */
        for (int i = 0; i < 10; i++) {
            if (tqdb_get(db, "User", test2_user_ids[i], &u) == TQDB_OK) {
                u.active = false;
                tqdb_update(db, "User", test2_user_ids[i], &u);
            }
        }

        /* Delete some items */
        for (int i = 25; i < 30; i++) {
            tqdb_delete(db, "User", test2_user_ids[i]);
        }

        printf("  Operations: 30 user adds, 25 product adds, 10 updates, 5 deletes\n");

        tqdb_close(db);
    }

    /* Verify */
    {
        tqdb_t db = open_db(true, true);
        size_t users = tqdb_count(db, "User");
        size_t products = tqdb_count(db, "Product");
        printf("  After reopen: Users=%zu (expected 25), Products=%zu (expected 25)\n",
               users, products);

        /* Verify data integrity */
        bool ok = true;
        user_t u;

        /* Check that updated users have active=false */
        for (int i = 0; i < 10 && ok; i++) {
            if (tqdb_get(db, "User", test2_user_ids[i], &u) == TQDB_OK) {
                if (u.active != false) {
                    printf("  User %d should be inactive!\n", i);
                    ok = false;
                }
            }
        }

        /* Check that deleted users don't exist */
        for (int i = 25; i < 30 && ok; i++) {
            if (tqdb_exists(db, "User", test2_user_ids[i])) {
                printf("  User %d should be deleted!\n", i);
                ok = false;
            }
        }

        if (ok && users == 25 && products == 25) {
            printf("  \033[32mData integrity OK\033[0m\n");
        } else {
            printf("  \033[31mData integrity FAILED\033[0m\n");
        }

        tqdb_close(db);
    }

    /* Test 3: Multiple open/close cycles */
    printf("\n--- Test 3: Multiple open/close cycles ---\n");
    cleanup();
    {
        for (int cycle = 0; cycle < 5; cycle++) {
            tqdb_t db = open_db(true, true);

            /* Add items */
            user_t u;
            for (int i = 0; i < 10; i++) {
                u.id = 0;  /* Auto-assign */
                snprintf(u.username, sizeof(u.username), "cycle%d_user%d", cycle, i);
                snprintf(u.email, sizeof(u.email), "c%du%d@t.com", cycle, i);
                u.age = 25; u.active = true; u.created_at = time(NULL);
                tqdb_add(db, "User", &u);
            }

            size_t count = tqdb_count(db, "User");
            printf("  Cycle %d: Added 10, total=%zu\n", cycle + 1, count);

            tqdb_close(db);
        }

        /* Final verification */
        tqdb_t db = open_db(true, true);
        size_t final_count = tqdb_count(db, "User");
        printf("  Final count: %zu (expected 50)\n", final_count);
        if (final_count == 50) {
            printf("  \033[32mMulti-cycle OK\033[0m\n");
        } else {
            printf("  \033[31mMulti-cycle FAILED\033[0m\n");
        }
        tqdb_close(db);
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Test: Compare WAL vs No-WAL Performance
 * ═══════════════════════════════════════════════════════════════════════════ */

static void test_wal_performance_comparison(void) {
    printf("\n");
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("  PERFORMANCE COMPARISON: WAL vs No-WAL\n");
    printf("═══════════════════════════════════════════════════════════════\n");

    int small_count = 50;

    /* Test without WAL */
    printf("\n--- Without WAL ---\n");
    cleanup();
    {
        tqdb_t db = open_db(false, false);
        user_t u;

        TIME_START();
        for (int i = 0; i < small_count; i++) {
            u.id = 0;  /* Auto-assign */
            snprintf(u.username, sizeof(u.username), "user_%d", i);
            snprintf(u.email, sizeof(u.email), "u%d@t.com", i);
            u.age = 25; u.active = true; u.created_at = time(NULL);
            tqdb_add(db, "User", &u);
        }
        TIME_END("Insert 50 users (no WAL):");

        tqdb_close(db);
    }

    /* Test with WAL */
    printf("\n--- With WAL ---\n");
    cleanup();
    {
        tqdb_t db = open_db(true, false);
        user_t u;

        TIME_START();
        for (int i = 0; i < small_count; i++) {
            u.id = 0;  /* Auto-assign */
            snprintf(u.username, sizeof(u.username), "user_%d", i);
            snprintf(u.email, sizeof(u.email), "u%d@t.com", i);
            u.age = 25; u.active = true; u.created_at = time(NULL);
            tqdb_add(db, "User", &u);
        }
        TIME_END("Insert 50 users (with WAL):");

        test_wal_stats(db);
        tqdb_close(db);
    }

    /* Test with WAL + Cache */
    printf("\n--- With WAL + Cache ---\n");
    cleanup();
    {
        tqdb_t db = open_db(true, true);
        user_t u;
        uint32_t perf_ids[50];

        /* Insert */
        TIME_START();
        for (int i = 0; i < small_count; i++) {
            u.id = 0;  /* Auto-assign */
            snprintf(u.username, sizeof(u.username), "user_%d", i);
            snprintf(u.email, sizeof(u.email), "u%d@t.com", i);
            u.age = 25; u.active = true; u.created_at = time(NULL);
            tqdb_add(db, "User", &u);
            perf_ids[i] = u.id;
        }
        TIME_END("Insert 50 users (WAL+Cache):");

        /* Read same items twice (second should be cached) */
        TIME_START();
        for (int i = 0; i < small_count; i++) {
            tqdb_get(db, "User", perf_ids[i], &u);
        }
        TIME_END("First read pass:");

        TIME_START();
        for (int i = 0; i < small_count; i++) {
            tqdb_get(db, "User", perf_ids[i], &u);
        }
        TIME_END("Second read pass (cached):");

        test_cache_stats(db);
        tqdb_close(db);
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════════════ */

int main(void) {
    srand(time(NULL));

    printf("\n");
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("  TQDB Stress Test\n");
    printf("  %d items per type, 3 entity types\n", ITEM_COUNT);
    printf("═══════════════════════════════════════════════════════════════\n");

    cleanup();

    /* Main stress test with WAL and cache */
    {
        tqdb_t db = open_db(true, true);

        test_bulk_insert(db, ITEM_COUNT);
        test_wal_stats(db);
        test_cache_stats(db);

        test_random_reads(db, ITEM_COUNT, ITEM_COUNT * 2);
        test_cache_stats(db);

        test_updates(db, ITEM_COUNT, ITEM_COUNT / 2);
        test_wal_stats(db);

        test_deletes(db, ITEM_COUNT, ITEM_COUNT / 4);

        test_iteration(db);

        test_checkpoint(db);

        tqdb_close(db);
    }

    test_persistence(true, true);

    test_crash_simulation();

    test_wal_performance_comparison();

    /* Free ID arrays */
    free(user_ids);
    free(product_ids);
    free(order_ids);

    cleanup();

    printf("\n");
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("  Stress Test Complete\n");
    printf("═══════════════════════════════════════════════════════════════\n\n");

    return 0;
}
