set (USE_OPENLDAP OFF)

set (USE_CBLAS OFF)

set (USE_AZURE OFF)
set (USE_AWS OFF)
set (WSSQL_SERVICE OFF)

set (USE_CASSANDRA OFF)
set (USE_NATIVE_LIBRARIES ON)
set (CMAKE_EXPORT_COMPILE_COMMANDS TRUE)

# Plugins (enable one at a time)
set (INCLUDE_PLUGINS OFF)
set (INCLUDE_REMBED OFF)
set (SUPRESS_V8EMBED ON)
set (INCLUDE_MEMCACHED OFF)
set (INCLUDE_REDIS OFF) # hard coded libhiredis.so
set (INCLUDE_SQS ON)
set (INCLUDE_MYSQLEMBED ON)
set (INCLUDE_JAVAEMBED ON)
set (INCLUDE_SQLITE3EMBED ON)
set (INCLUDE_KAFKA ON)
set (INCLUDE_COUCHBASEEMBED OFF) #make[2]: *** No rule to make target `plugins/couchbase/libcouchbase-build/lib/libcouchbase.so', needed by `Debug/libs/libcouchbaseembed.dylib'.  Stop.
set (INCLUDE_SPARK ON)
set (INCLUDE_EXAMPLEPLUGIN ON)

# Additional
option(BUILD_TESTS "Enable libgit2 tests (override libgit2 option)" ON)
set (BUILD_TESTS OFF)
set (SKIP_ECLWATCH ON)
set (USE_OPTIONAL OFF)
