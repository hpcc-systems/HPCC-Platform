if ("${PLUGINS_DONE}" STREQUAL "")
  set (PLUGINS_DONE 1)

# Plugin Options ---
MACRO(SET_PLUGIN_PACKAGE plugin)
    string(TOLOWER "${plugin}" pname)
    if(DEFINED pluginname)
        message(FATAL_ERROR "Cannot enable ${pname}, already declared ${pluginname}")
    else()
        set(pluginname "${pname}")
    endif()
    foreach(p in ${PLUGINS_LIST})
        if(NOT "${p}" STREQUAL "${plugin}" AND ${p})
            message(FATAL_ERROR "Cannot declare multiple plugins in a plugin package (${p}, ${plugin})")
        endif()
    endforeach()
    set(PLUGIN ON)
    set(CLIENTTOOLS OFF)
    set(PLATFORM OFF)
    set(INCLUDE_PLUGINS OFF)
    set(SIGN_MODULES OFF)
    set(USE_OPTIONAL OFF) # Force failure if we can't find the plugin dependencies
ENDMACRO()

set(VCPKG_INCLUDE "(windows | osx | linux)")
set(VCPKG_SUPPRESS "(!windows & !osx & !linux)")

set(PLUGINS_LIST
    COUCHBASEEMBED
    H3
    JAVAEMBED
    KAFKA
    MEMCACHED
    MONGODBEMBED
    MYSQLEMBED
    NLP
    REDIS
    REMBED
    SQLITE3EMBED
    SQS
    V8EMBED
    EXAMPLEPLUGIN
)

foreach(plugin ${PLUGINS_LIST})
    option(${plugin} "Create a package with ONLY the ${plugin} plugin" OFF)
    option(INCLUDE_${plugin} "Include ${plugin} within package for testing" OFF)
    option(SUPPRESS_${plugin} "Suppress ${plugin} from INCLUDE_PLUGINS build" OFF)
    # Plugin Release build for individual package
    set(VCPKG_${plugin} "${VCPKG_SUPPRESS}")
    if(${plugin})
        SET_PLUGIN_PACKAGE("${plugin}")
        set(VCPKG_${plugin} "${VCPKG_INCLUDE}")
    # Development build with all plugins for testing
    # Development build with addition of plugin
    elseif((INCLUDE_PLUGINS OR INCLUDE_${plugin}) AND (NOT SUPPRESS_${plugin}) AND (NOT PLUGIN))
        set(${plugin} ON)
        set(VCPKG_${plugin} "${VCPKG_INCLUDE}")
    endif()
endforeach()

#  vcpkg.json plugins which overlap with options  ---
if (USE_AWS)
    set(VCPKG_SQS "${VCPKG_INCLUDE}")
endif()

if (USE_CASSANDRA)
    set(VCPKG_CASSANDRAEMBED "${VCPKG_INCLUDE}")
endif()

if (USE_LIBMEMCACHED)
    set(VCPKG_MEMCACHED "${VCPKG_INCLUDE}")
endif()

if (USE_MYSQL_REPOSITORY)
    set(VCPKG_MYSQLEMBED "${VCPKG_INCLUDE}")
endif()

#  vcpkg.json options  ---
set(VCPKG_APR "${VCPKG_SUPPRESS}")
if (USE_APR)
    set(VCPKG_APR "${VCPKG_INCLUDE}")
endif()

set(VCPKG_AZURE "${VCPKG_SUPPRESS}")
if (USE_AZURE)
    set(VCPKG_AZURE "${VCPKG_INCLUDE}")
endif()

set(VCPKG_BOOST_REGEX "${VCPKG_SUPPRESS}")
if (USE_BOOST_REGEX)
    set(VCPKG_BOOST_REGEX "${VCPKG_INCLUDE}")
endif()

set(VCPKG_CPPUNIT "${VCPKG_SUPPRESS}")
if (USE_CPPUNIT)
    set(VCPKG_CPPUNIT "${VCPKG_INCLUDE}")
endif()

set(VCPKG_ELASTICSTACK_CLIENT "${VCPKG_SUPPRESS}")
if (USE_ELASTICSTACK_CLIENT)
    set(VCPKG_ELASTICSTACK_CLIENT "${VCPKG_INCLUDE}")
endif()

set(VCPKG_GIT "${VCPKG_SUPPRESS}")
if (USE_GIT)
    set(VCPKG_GIT "${VCPKG_INCLUDE}")
endif()

set(VCPKG_ICU "${VCPKG_SUPPRESS}")
if (USE_ICU)
    set(VCPKG_ICU "${VCPKG_INCLUDE}")
endif()

set(VCPKG_LIBXSLT "${VCPKG_SUPPRESS}")
if (USE_LIBXSLT)
    set(VCPKG_LIBXSLT "${VCPKG_INCLUDE}")
endif()

set(VCPKG_LIBXALAN "${VCPKG_SUPPRESS}")
if (USE_LIBXALAN)
    set(VCPKG_LIBXALAN "${VCPKG_INCLUDE}")
endif()

set(VCPKG_OPENLDAP "${VCPKG_SUPPRESS}")
if (USE_OPENLDAP)
    set(VCPKG_OPENLDAP "${VCPKG_INCLUDE}")
endif()

set(VCPKG_OPENSSL "${VCPKG_SUPPRESS}")
if (USE_OPENSSL)
    set(VCPKG_OPENSSL "${VCPKG_INCLUDE}")
endif()

set(VCPKG_PYTHON3 "${VCPKG_SUPPRESS}")
if (USE_PYTHON3)
    set(VCPKG_PYTHON3 "${VCPKG_INCLUDE}")
endif()

set(VCPKG_TBB "${VCPKG_SUPPRESS}")
if (USE_TBB OR USE_TBBMALLOC OR USE_TBBMALLOC_ROXIE)
    set(VCPKG_TBB "${VCPKG_INCLUDE}")
endif()

set(VCPKG_ZLIB "${VCPKG_SUPPRESS}")
if (USE_ZLIB)
    set(VCPKG_ZLIB "${VCPKG_INCLUDE}")
endif()

configure_file("${HPCC_SOURCE_DIR}/vcpkg.json.in" "${HPCC_SOURCE_DIR}/vcpkg.json")

endif()
