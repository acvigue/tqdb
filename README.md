# tqdb

**Technically Qualifies as a Database**

A portable, trait-based file database library written in pure C. Designed for embedded systems and resource-constrained environments.

## Features

- **No platform dependencies!**
- **Trait-based entity system** - Define custom types at runtime with serialization callbacks
- **CRUD operations** - Add, get, update, delete, exists, count
- **Write-Ahead Logging (WAL)** - Optional journaling for crash recovery
- **LRU Caching** - Optional in-memory cache for frequently accessed entities
- **Query System** - Optional lightweight field-based filtering with operators (compile-time feature)
- **CRC32 integrity checking** - Detects file corruption
- **Atomic writes** - Backup/recovery mechanism prevents partial writes
- **Configurable allocator** - BYO malloc/free for embedded systems (hello heap_caps_malloc!)
- **Optional mutex support** - Thread-safe access when needed

## Quick Start

```c
#include "tqdb.h"

// 1. Define your entity structure
typedef struct {
    uint32_t id;    // Auto-assigned by tqdb_add()
    char name[64];
    int32_t value;
} my_entity_t;

// 2. Implement serialization callbacks
static void my_write(tqdb_writer_t* w, const void* entity) {
    const my_entity_t* e = entity;
    tqdb_write_u32(w, e->id);
    tqdb_write_str(w, e->name);
    tqdb_write_i32(w, e->value);
}

static void my_read(tqdb_reader_t* r, void* entity) {
    my_entity_t* e = entity;
    e->id = tqdb_read_u32(r);
    tqdb_read_str(r, e->name, sizeof(e->name));
    e->value = tqdb_read_i32(r);
}

// 3. Implement ID getter/setter
static uint32_t my_get_id(const void* entity) {
    return ((const my_entity_t*)entity)->id;
}

static void my_set_id(void* entity, uint32_t id) {
    ((my_entity_t*)entity)->id = id;
}

// 4. Define the trait
static const tqdb_trait_t MY_TRAIT = {
    .name = "my_entity",
    .max_count = 100,
    .struct_size = sizeof(my_entity_t),
    .write = my_write,
    .read = my_read,
    .get_id = my_get_id,
    .set_id = my_set_id,
};

// 5. Use the database
int main(void) {
    tqdb_t db;
    tqdb_config_t cfg = { .db_path = "data.tqdb" };

    tqdb_open(&cfg, &db);
    tqdb_register(db, &MY_TRAIT);

    // Add entity - ID is auto-assigned (starting from 1)
    my_entity_t entity = { .id = 0, .name = "test", .value = 42 };
    tqdb_add(db, "my_entity", &entity);
    printf("Assigned ID: %u\n", entity.id);  // Prints: Assigned ID: 1

    // Retrieve by ID
    my_entity_t retrieved;
    tqdb_get(db, "my_entity", entity.id, &retrieved);

    tqdb_close(db);
    return 0;
}
```

## Building

### Build the library

```bash
make lib
```

This produces `libtqdb.a`.

### Build with query support

```bash
make query
# or
TQDB_ENABLE_QUERY=1 make lib
```

### Debug build

```bash
make debug
```

### Build Flags

| Flag                | Default | Description                          |
| ------------------- | ------- | ------------------------------------ |
| `TQDB_ENABLE_WAL`   | 1       | Enable Write-Ahead Logging           |
| `TQDB_ENABLE_CACHE` | 1       | Enable LRU cache                     |
| `TQDB_ENABLE_QUERY` | 0       | Enable query system (adds ~3KB code) |

Example: minimal build without WAL or cache:

```bash
TQDB_ENABLE_WAL=0 TQDB_ENABLE_CACHE=0 make lib
```

### Code Size (ARM Cortex-M, -Os)

| Configuration            | .text  | .a file |
| ------------------------ | ------ | ------- |
| Core only                | ~8 KB  | 17 KB   |
| + WAL                    | ~14 KB | 31 KB   |
| + WAL + Cache            | ~15 KB | 34 KB   |
| + WAL + Cache + Query    | ~18 KB | 41 KB   |

### RAM Usage

Base overhead (per database instance):

| Component          | Size                |
| ------------------ | ------------------- |
| Scratch buffer     | 8 KB (configurable) |
| Trait pointers     | 64 bytes (8 types)  |
| ID counters        | 32 bytes (8 types)  |
| WAL state          | ~32 bytes           |
| **Base total**     | **~8.2 KB**         |

Optional components:

| Component          | Size                              |
| ------------------ | --------------------------------- |
| Cache (16 entries) | ~1.3 KB + entity data             |
| Query builder      | ~128 bytes per active query       |

## Testing

```bash
# Run core tests
make test

# Run query system tests (requires TQDB_ENABLE_QUERY)
make test-query

# Run stress tests
make test-stress

# Run all tests
make test-all
```

## Configuration

Configuration can be set via preprocessor defines before including `tqdb.h`, or via Kconfig for ESP-IDF projects.

| Option                         | Default | Description                                |
| ------------------------------ | ------- | ------------------------------------------ |
| `TQDB_MAX_ENTITY_TYPES`        | 8       | Maximum registered entity types            |
| `TQDB_DEFAULT_SCRATCH_SIZE`    | 8192    | Serialization buffer size (bytes)          |
| `TQDB_WAL_MAX_ENTRIES_DEFAULT` | 100     | WAL auto-checkpoint entry threshold        |
| `TQDB_WAL_MAX_SIZE_DEFAULT`    | 65536   | WAL auto-checkpoint size threshold (bytes) |
| `TQDB_CACHE_SIZE_DEFAULT`      | 16      | Default LRU cache capacity (entities)      |
| `TQDB_QUERY_MAX_CONDITIONS`    | 8       | Max conditions per query                   |
| `TQDB_ENABLE_QUERY`            | 0       | Enable query system (compile-time)         |

### Custom Allocator

```c
#define TQDB_MALLOC(size)       my_malloc(size)
#define TQDB_FREE(ptr)          my_free(ptr)
#define TQDB_REALLOC(ptr, size) my_realloc(ptr, size)
#include "tqdb.h"
```

## API Overview

### Database Operations

```c
tqdb_err_t tqdb_open(const tqdb_config_t* config, tqdb_t* db);
tqdb_err_t tqdb_close(tqdb_t db);
tqdb_err_t tqdb_register(tqdb_t db, const tqdb_trait_t* trait);
```

### CRUD Operations

```c
tqdb_err_t tqdb_add(tqdb_t db, const char* type, void* entity);     // ID auto-assigned
tqdb_err_t tqdb_get(tqdb_t db, const char* type, uint32_t id, void* entity);
tqdb_err_t tqdb_update(tqdb_t db, const char* type, uint32_t id, const void* entity);
tqdb_err_t tqdb_delete(tqdb_t db, const char* type, uint32_t id);
bool tqdb_exists(tqdb_t db, const char* type, uint32_t id);
size_t tqdb_count(tqdb_t db, const char* type);
```

### Batch Operations

```c
tqdb_err_t tqdb_foreach(tqdb_t db, const char* type, tqdb_foreach_cb cb, void* ctx);
tqdb_err_t tqdb_delete_where(tqdb_t db, const char* type, tqdb_filter_cb filter, void* ctx);
tqdb_err_t tqdb_modify_where(tqdb_t db, const char* type, tqdb_filter_cb filter,
                              tqdb_modify_cb modify, void* ctx);
```

### WAL Operations

```c
tqdb_err_t tqdb_checkpoint(tqdb_t db);  // Force WAL checkpoint
```

### Query System (requires TQDB_ENABLE_QUERY)

```c
tqdb_query_t* tqdb_query_create(tqdb_t db, const char* type);
tqdb_err_t tqdb_query_where(tqdb_query_t* q, const char* field, tqdb_op_t op, ...);
tqdb_err_t tqdb_query_limit(tqdb_query_t* q, size_t limit, size_t offset);
tqdb_err_t tqdb_query_exec(tqdb_query_t* q, void* results, size_t max, size_t* count);
void tqdb_query_free(tqdb_query_t* q);
```

## Error Codes

| Code                      | Description                       |
| ------------------------- | --------------------------------- |
| `TQDB_OK`                 | Success                           |
| `TQDB_ERR_INVALID_ARG`    | Invalid argument                  |
| `TQDB_ERR_NO_MEM`         | Memory allocation failed          |
| `TQDB_ERR_NOT_FOUND`      | Entity not found                  |
| `TQDB_ERR_EXISTS`         | Entity already exists (duplicate) |
| `TQDB_ERR_IO`             | File I/O error                    |
| `TQDB_ERR_CORRUPT`        | Database file corrupt             |
| `TQDB_ERR_FULL`           | Max entities reached              |
| `TQDB_ERR_TIMEOUT`        | Mutex timeout                     |
| `TQDB_ERR_NOT_REGISTERED` | Entity type not registered        |

## File Format

- **Database file**: Magic `TQDB` (0x54514442), version 1, with CRC32 integrity
- **WAL file**: Magic `TWAL` (0x5457414C), version 1, with entry tracking

## License

TODO: Add license
