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

# Plugin options
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
    SQLITE3EMBED
    SQS
    V8EMBED
EXAMPLEPLUGIN)

foreach(plugin ${PLUGINS_LIST})
    option(${plugin} "Create a package with ONLY the ${plugin} plugin" OFF)
    option(INCLUDE_${plugin} "Include ${plugin} within package for testing" OFF)
    option(SUPPRESS_${plugin} "Suppress ${plugin} from INCLUDE_PLUGINS build" OFF)
    # Plugin Release build for individual package
    if(${plugin})
        SET_PLUGIN_PACKAGE("${plugin}")
    # Development build with all plugins for testing
    # Development build with addition of plugin
    elseif((INCLUDE_PLUGINS OR INCLUDE_${plugin}) AND (NOT SUPPRESS_${plugin}) AND (NOT PLUGIN))
        set(${plugin} ON)
    endif()
endforeach()
