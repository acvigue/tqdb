# TQDB - Portable File Database Library
# Makefile for non-CMake platforms

CC ?= gcc
AR ?= ar
CFLAGS ?= -Wall -Wextra -Os -std=c99
CFLAGS += -I. -Isrc

# Optional features (set to 1 to enable, 0 to disable)
TQDB_ENABLE_WAL ?= 1
TQDB_ENABLE_CACHE ?= 1
# TQDB_ENABLE_QUERY ?= 0

# Source files (core always included)
SRCS = src/tqdb_core.c src/tqdb_binary_io.c src/tqdb_crc32.c

# Conditionally add WAL module
ifeq ($(TQDB_ENABLE_WAL),1)
SRCS += src/tqdb_wal.c
CFLAGS += -DTQDB_ENABLE_WAL=1
else
CFLAGS += -DTQDB_ENABLE_WAL=0
endif

# Conditionally add cache module
ifeq ($(TQDB_ENABLE_CACHE),1)
SRCS += src/tqdb_cache.c
CFLAGS += -DTQDB_ENABLE_CACHE=1
else
CFLAGS += -DTQDB_ENABLE_CACHE=0
endif

# Conditionally add query module
ifeq ($(TQDB_ENABLE_QUERY),1)
SRCS += src/tqdb_query.c
CFLAGS += -DTQDB_ENABLE_QUERY
endif

OBJS = $(SRCS:.c=.o)

# Library output
LIB = libtqdb.a

# Test programs
TEST_SRC = test/test_tqdb.c
TEST_BIN = test/test_tqdb
TEST_QUERY_SRC = test/test_query.c
TEST_QUERY_BIN = test/test_query
STRESS_SRC = test/test_stress.c
STRESS_BIN = test/test_stress

.PHONY: all clean test test-query test-stress lib

all: lib

lib: $(LIB)

$(LIB): $(OBJS)
	$(AR) rcs $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Test target (basic tests)
test: $(TEST_BIN)
	./$(TEST_BIN)

$(TEST_BIN): $(TEST_SRC) $(LIB)
	@mkdir -p test
	$(CC) $(CFLAGS) -o $@ $< -L. -ltqdb

# Query tests (requires TQDB_ENABLE_QUERY=1)
# Use recursive make to ensure the variable is set at parse time
test-query:
	$(MAKE) clean
	$(MAKE) TQDB_ENABLE_QUERY=1 _test-query-run

_test-query-run: $(TEST_QUERY_BIN)
	./$(TEST_QUERY_BIN)

$(TEST_QUERY_BIN): $(TEST_QUERY_SRC) $(LIB)
	@mkdir -p test
	$(CC) $(CFLAGS) -o $@ $< -L. -ltqdb -lm

# Stress test
test-stress: $(STRESS_BIN)
	./$(STRESS_BIN)

$(STRESS_BIN): $(STRESS_SRC) $(LIB)
	@mkdir -p test
	$(CC) $(CFLAGS) -o $@ $< -L. -ltqdb

# Run all tests
test-all: test
ifeq ($(TQDB_ENABLE_QUERY),1)
	$(MAKE) test-query TQDB_ENABLE_QUERY=1
endif

clean:
	rm -f $(OBJS) $(LIB) $(TEST_BIN) $(TEST_QUERY_BIN) $(STRESS_BIN)
	rm -f src/tqdb_query.o
	rm -f test/*.tqdb test/*.tqdb.*

# Debug build
debug: CFLAGS += -g -O0 -DDEBUG
debug: clean all

# Build with query support
query: TQDB_ENABLE_QUERY=1
query: clean all

# Dependencies
src/tqdb_core.o: src/tqdb_core.c src/tqdb_internal.h tqdb.h
src/tqdb_binary_io.o: src/tqdb_binary_io.c src/tqdb_internal.h tqdb.h
src/tqdb_crc32.o: src/tqdb_crc32.c src/tqdb_internal.h tqdb.h
src/tqdb_wal.o: src/tqdb_wal.c src/tqdb_internal.h tqdb.h
src/tqdb_cache.o: src/tqdb_cache.c src/tqdb_internal.h tqdb.h
src/tqdb_query.o: src/tqdb_query.c src/tqdb_internal.h tqdb.h
