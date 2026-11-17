project(ws_sql2)

if(WSSQL2_SERVICE)
    find_package(DuckDB CONFIG REQUIRED)
    find_package(OpenSSL REQUIRED)

    include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

    set(SRCS
        ${ESPSCM_GENERATED_DIR}/ws_sql2_esp.cpp
        duckdb-hpcc/extension.cpp
        duckdb-hpcc/materialization.cpp
        duckdb-hpcc/optimizer.cpp
        duckdb-hpcc/typemapping.cpp
        engine.cpp
        metadata.cpp
        plugin.cpp
        service.cpp
    )
    if(USE_CPPUNIT)
        list(APPEND SRCS pushdowntests.cpp)
        include_directories(${HPCC_SOURCE_DIR}/testing/unittests)
    endif()

    include_directories(
        ${HPCC_SOURCE_DIR}/esp/platform
        ${HPCC_SOURCE_DIR}/esp/bindings
        ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
        ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/system/jlib
        ${HPCC_SOURCE_DIR}/system/jhtree
        ${HPCC_SOURCE_DIR}/system/security/shared
        ${HPCC_SOURCE_DIR}/system/security/securesocket
        ${HPCC_SOURCE_DIR}/system/xmllib
        ${HPCC_SOURCE_DIR}/common/thorhelper
        ${HPCC_SOURCE_DIR}/common/fileview2
        ${HPCC_SOURCE_DIR}/common/deftype
        ${HPCC_SOURCE_DIR}/common/workunit
        ${HPCC_SOURCE_DIR}/rtl/include
        ${HPCC_SOURCE_DIR}/rtl/eclrtl
        ${HPCC_SOURCE_DIR}/rtl/nbcd
        ${HPCC_SOURCE_DIR}/esp/services/common
        ${HPCC_SOURCE_DIR}/dali/base
        ${HPCC_SOURCE_DIR}/system/mp
        ${HPCC_SOURCE_DIR}/system/security/shared
        ${ESPSCM_GENERATED_DIR}
        ${DuckDB_INCLUDE_DIRS}
    )

    add_definitions(-D_USRDLL -DWsSql2_API_LOCAL -DESP_SERVICE_WsSQL2)
    HPCC_ADD_LIBRARY(ws_sql2 SHARED ${SRCS})
    add_dependencies(ws_sql2 espscm)
    target_link_libraries(ws_sql2 PRIVATE
        jlib
        jhtree
        xmllib
        esphttp
        dalibase
        deftype
        eclrtl
        nbcd
        thorhelper
        fileview2
        ${DuckDB_LIBRARIES}
        OpenSSL::Crypto
        ${COMMON_ESP_SERVICE_LIBS}
    )
    if("duckdb_static" IN_LIST DuckDB_LIBRARIES AND TARGET duckdb_generated_extension_loader)
        target_link_libraries(ws_sql2 PRIVATE duckdb_generated_extension_loader)
        foreach(extension IN LISTS DuckDB_EXTENSIONS)
            if(TARGET ${extension}_extension)
                target_link_libraries(ws_sql2 PRIVATE ${extension}_extension)
            endif()
        endforeach()
    endif()
    if(USE_CPPUNIT)
        target_link_libraries(ws_sql2 PRIVATE ${CppUnit_LIBRARIES})
    endif()


    install(TARGETS ws_sql2
        RUNTIME DESTINATION ${EXEC_DIR}
        LIBRARY DESTINATION ${LIB_DIR}
        COMPONENT Runtime
        CALC_DEPS
    )
    # Install the shared DuckDB library (a static duckdb_static build has nothing to install).
    if(TARGET duckdb)
        install(FILES
            $<TARGET_FILE:duckdb>
            DESTINATION ${LIB_DIR}
            COMPONENT Runtime
        )
    endif()

endif()
