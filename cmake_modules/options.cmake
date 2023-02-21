if ("${OPTIONS_DONE}" STREQUAL "")
    set (OPTIONS_DONE 1)

    # General Options ---
    option(CONTAINERIZED "Build for container images." OFF)
    option(CLIENTTOOLS "Enable the building/inclusion of a Client Tools component." ON)
    option(PLATFORM "Enable the building/inclusion of a Platform component." ON)
    option(DEVEL "Enable the building/inclusion of a Development component." OFF)
    option(CLIENTTOOLS_ONLY "Enable the building of Client Tools only." OFF)
    option(INCLUDE_PLUGINS "Enable the building of platform and all plugins for testing purposes" OFF)
    option(USE_CASSANDRA "Include the Cassandra plugin in the base package" ON)
    option(PLUGIN "Enable building of a plugin" OFF)
    option(USE_SHLIBDEPS "Enable the use of dpkg-shlibdeps on ubuntu packaging" OFF)

    option(SIGN_MODULES "Enable signing of ecl standard library modules" OFF)
    option(USE_CPPUNIT "Enable unit tests (requires cppunit)" OFF)
    option(USE_OPENLDAP "Enable OpenLDAP support (requires OpenLDAP)" ON)
    option(USE_ICU "Enable unicode support (requires ICU)" ON)
    option(USE_BOOST_REGEX "Configure use of boost regex" ON)
    option(CENTOS_6_BOOST "Supply regex library on CentOS 6" OFF)
    # USE_C11_REGEX is only checked if USE_BOOST_REGEX is OFF
    # to disable REGEX altogether turn both off
    option(USE_C11_REGEX "Configure use of c++11 std::regex" ON)
    option(Boost_USE_STATIC_LIBS "Use boost_regex static library for RPM BUILD" OFF)
    option(USE_OPENSSL "Configure use of OpenSSL" ON)
    option(USE_OPENSSLV3 "Configure use of OpenSSL Version 3 or newer" ON)
    option(USE_ZLIB "Configure use of zlib" ON)
    option(USE_AZURE "Configure use of azure" ON)
    option(USE_GIT "Configure use of GIT (Hooks)" ON)
    if (WIN32)
        option(USE_AERON "Include the Aeron message protocol" OFF)
    else()
        option(USE_AERON "Include the Aeron message protocol" ON)
    endif()
    option(USE_LIBARCHIVE "Configure use of libarchive" ON)
    option(USE_URIPARSER "Configure use of uriparser" OFF)
    if (APPLE OR WIN32)
        option(USE_NUMA "Configure use of numa" OFF)
    else()
        option(USE_NUMA "Configure use of numa" ON)
    endif()
    option(USE_AWS "Configure use of aws" ON)
    option(STRIP_RELEASE_SYMBOLS "Strip symbols from release builds" OFF)

    IF (WIN32)
        option(USE_NATIVE_LIBRARIES "Search standard OS locations (otherwise in EXTERNALS_DIRECTORY) for 3rd party libraries" OFF)
    ELSE()
        option(USE_NATIVE_LIBRARIES "Search standard OS locations (otherwise in EXTERNALS_DIRECTORY) for 3rd party libraries" ON)
    ENDIF()

    option(USE_GIT_DESCRIBE "Use git describe to generate build tag" ON)
    option(CHECK_GIT_TAG "Require git tag to match the generated build tag" OFF)
    option(USE_XALAN "Configure use of xalan" OFF)
    option(USE_APR "Configure use of Apache Software Foundation (ASF) Portable Runtime (APR) libraries" ON)
    option(USE_LIBXSLT "Configure use of libxslt" ON)
    option(MAKE_DOCS "Create documentation at build time." OFF)
    option(MAKE_DOCS_ONLY "Create a base build with only docs." OFF)
    option(DOCS_DRUPAL "Create Drupal HTML Docs" OFF)
    option(DOCS_EPUB "Create EPUB Docs" OFF)
    option(DOCS_MOBI "Create Mobi Docs" OFF)
    option(DOCS_AUTO "DOCS automation" OFF)
    option(USE_RESOURCE "Use resource download in ECLWatch" OFF)
    option(GENERATE_COVERAGE_INFO "Generate coverage info for gcov" OFF)
    option(USE_SIGNED_CHAR "Build system with default char type is signed" OFF)
    option(USE_UNSIGNED_CHAR "Build system with default char type is unsigned" OFF)
    option(USE_JAVA "Include support for components that require java" ON)
    option(USE_MYSQL "Enable mysql support" ON)
    option(USE_LIBMEMCACHED "Enable libmemcached support" ON)
    option(USE_PYTHON2 "Enable python2 language support for platform build" OFF)
    option(USE_PYTHON3 "Enable python3 language support for platform build" ON)
    option(USE_OPTIONAL "Automatically disable requested features with missing dependencies" ON)
    option(JLIB_ONLY  "Build JLIB for other projects such as Configurator, Ganglia Monitoring, etc" OFF)
    # Generates code that is more efficient, but will cause problems if target platforms do not support it.
    if (CMAKE_SIZEOF_VOID_P EQUAL 8)
        option(USE_INLINE_TSC "Inline calls to read TSC (time stamp counter)" ON)
    else()
        option(USE_INLINE_TSC "Inline calls to read TSC (time stamp counter)" OFF)
    endif()
    if (APPLE OR WIN32)
        option(USE_TBB "Enable Threading Building Block support" OFF)
    else()
        option(USE_TBB "Enable Threading Building Block support" OFF)
        option(USE_TBBMALLOC "Enable Threading Building Block scalable allocator proxy support" OFF)
        option(USE_TBBMALLOC_ROXIE "Enable Threading Building Block scalable allocator proxy support in Roxie" OFF)
    endif()
    option(LOGGING_SERVICE "Configure use of logging service" ON)
    option(WSSQL_SERVICE "Configure use of ws_sql service" ON)
    option(USE_DIGISIGN "Use digisign" ON)
    option(INCLUDE_EE_PLUGINS "Install EE Plugins in Clienttool" OFF)
    option(INCLUDE_TREEVIEW "Build legacy treeview" OFF)
    option(INCLUDE_CONFIG_MANAGER "Build config manager" ON)
    option(USE_ELASTICSTACK_CLIENT "Configure use of Elastic Stack client" ON)
    option(SKIP_ECLWATCH "Skip building ECL Watch" OFF)
    option(USE_ADDRESS_SANITIZER "Use address sanitizer to spot leaks" OFF)

    # Plugin Options ---
    set(PLUGINS_LIST
        CASSANDRAEMBED
        COUCHBASEEMBED
        ECLBLAS
        H3
        JAVAEMBED
        KAFKA
        MEMCACHED
        MONGODBEMBED
        MYSQLEMBED
        NLP
        REDIS
        REMBED
        SPARK
        SQLITE3EMBED
        SQS
        V8EMBED
        EXAMPLEPLUGIN
    )

    set(VCPKG_INCLUDE "(windows | osx | linux)")
    set(VCPKG_SUPPRESS "(!windows & !osx & !linux)")

    MACRO(SET_PLUGIN_PACKAGE plugin)
        string(TOLOWER "${plugin}" pname)
        if(DEFINED pluginname)
            message(FATAL_ERROR "Cannot enable ${pname}, already declared ${pluginname}")
        else()
            set(pluginname "${pname}")
        endif()
        foreach(p in ${PLUGINS_LIST})
            if(NOT "${p}" STREQUAL "${plugin}" AND ${p})
                message(FATAL_ERROR "Cannot declare multiple plugins in a plugin package")
            endif()
        endforeach()
        set(PLUGIN ON)
        set(CLIENTTOOLS OFF)
        set(PLATFORM OFF)
        set(INCLUDE_PLUGINS OFF)
        set(SIGN_MODULES OFF)
        set(USE_OPTIONAL OFF) # Force failure if we can't find the plugin dependencies
    ENDMACRO()

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

    set(VCPKG_APR "${VCPKG_SUPPRESS}")
    if (USE_APR)
        set(VCPKG_APR "${VCPKG_INCLUDE}")
    endif()

    if (USE_AWS)
        set(VCPKG_SQS "${VCPKG_INCLUDE}")
    endif()

    set(VCPKG_AZURE "${VCPKG_SUPPRESS}")
    if (USE_AZURE)
        set(VCPKG_AZURE "${VCPKG_INCLUDE}")
    endif()

    set(VCPKG_BOOST_REGEX "${VCPKG_SUPPRESS}")
    if (USE_BOOST_REGEX)
        set(VCPKG_BOOST_REGEX "${VCPKG_INCLUDE}")
    endif()

    if (USE_CASSANDRA)
        set(VCPKG_CASSANDRAEMBED "${VCPKG_INCLUDE}")
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

    if (USE_LIBMEMCACHED)
        set(VCPKG_MEMCACHED "${VCPKG_INCLUDE}")
    endif()

    set(VCPKG_LIBXSLT "${VCPKG_SUPPRESS}")
    if (USE_LIBXSLT)
        set(VCPKG_LIBXSLT "${VCPKG_INCLUDE}")
    endif()

    set(VCPKG_LIBXALAN "${VCPKG_SUPPRESS}")
    if (USE_LIBXALAN)
        set(VCPKG_LIBXALAN "${VCPKG_INCLUDE}")
    endif()

    set(VCPKG_NUMA "${VCPKG_SUPPRESS}")
    if (USE_OPENLDAP)
        set(VCPKG_NUMA "${VCPKG_INCLUDE}")
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
