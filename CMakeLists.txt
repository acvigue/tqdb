cmake_minimum_required(VERSION 3.16)

# Base source files (always included)
set(TQDB_SRCS
    "src/tqdb_core.c"
    "src/tqdb_binary_io.c"
    "src/tqdb_crc32.c"
)

# Conditionally add WAL module
if(CONFIG_TQDB_ENABLE_WAL)
    list(APPEND TQDB_SRCS "src/tqdb_wal.c")
endif()

# Conditionally add cache module
if(CONFIG_TQDB_ENABLE_CACHE)
    list(APPEND TQDB_SRCS "src/tqdb_cache.c")
endif()

# Conditionally add query module
if(CONFIG_TQDB_ENABLE_QUERY)
    list(APPEND TQDB_SRCS "src/tqdb_query.c")
endif()

idf_component_register(
    SRCS ${TQDB_SRCS}
    INCLUDE_DIRS "."
    PRIV_INCLUDE_DIRS "src"
)

# Add compile definitions based on Kconfig
if(CONFIG_TQDB_ENABLE_WAL)
    target_compile_definitions(${COMPONENT_LIB} PUBLIC TQDB_ENABLE_WAL=1)
else()
    target_compile_definitions(${COMPONENT_LIB} PUBLIC TQDB_ENABLE_WAL=0)
endif()

if(CONFIG_TQDB_ENABLE_CACHE)
    target_compile_definitions(${COMPONENT_LIB} PUBLIC TQDB_ENABLE_CACHE=1)
else()
    target_compile_definitions(${COMPONENT_LIB} PUBLIC TQDB_ENABLE_CACHE=0)
endif()

if(CONFIG_TQDB_ENABLE_QUERY)
    target_compile_definitions(${COMPONENT_LIB} PUBLIC TQDB_ENABLE_QUERY)
endif()

if(CONFIG_TQDB_QUERY_MAX_CONDITIONS)
    target_compile_definitions(${COMPONENT_LIB} PUBLIC
        TQDB_QUERY_MAX_CONDITIONS=${CONFIG_TQDB_QUERY_MAX_CONDITIONS})
endif()
