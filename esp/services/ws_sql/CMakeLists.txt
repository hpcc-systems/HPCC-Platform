################################################################################
#    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
# Component: ws_sql
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_sql
#####################################################

project(ws_sql)

if(WSSQL_SERVICE AND USE_JAVA)
    find_package(Java REQUIRED)
    find_package(ANTLR REQUIRED)

    include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)
    include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

    include(antlr3c.cmake)

    antlr_target(sql2ecl
        GRAMMAR_PREFIX HPCCSQL
        DESTINATION "${CMAKE_CURRENT_BINARY_DIR}"
        GRAMMAR_FILES "${CMAKE_CURRENT_SOURCE_DIR}/SQL2ECL/ANTLR3c/HPCCSQL.g"
        )

    set(SRCS
        ${CMAKE_CURRENT_SOURCE_DIR}/ws_sqlPlugin.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/ws_sqlService.cpp
        ${ESPSCM_GENERATED_DIR}/common_esp.cpp
        ${ESPSCM_GENERATED_DIR}/ws_sql_esp.cpp
        ${ESPSCM_GENERATED_DIR}/ws_workunits_struct_esp.cpp
        ${ESPSCM_GENERATED_DIR}/ws_topology_esp.cpp
        ${HPCC_SOURCE_DIR}/esp/services/ws_ecl/ws_ecl_wuinfo.cpp
        ${HPCC_SOURCE_DIR}/esp/services/ws_workunits/ws_workunitsHelpers.cpp
        SQL2ECL/HPCCFileCache.cpp
        SQL2ECL/HPCCFile.cpp
        SQL2ECL/ECLFunction.cpp
        SQL2ECL/ECLEngine.cpp
        SQL2ECL/SQLColumn.cpp
        SQL2ECL/SQLTable.hpp
        SQL2ECL/SQLExpression.cpp
        SQL2ECL/SQLJoin.cpp
        SQL2ECL/HPCCSQLTreeWalker.cpp
        ${ANTLR_sql2ecl_SOURCES}
        )

    include_directories(
        ##added for build-config.h
        ${CMAKE_BINARY_DIR}
        ##added for build-config.h
        ${CMAKE_BINARY_DIR}/oss
        ${HPCC_WSSQL_SOURCE_DIR}/esp/services/ws_sql/SQL2ECL
        ${HPCC_SOURCE_DIR}/esp/bindings
        ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
        ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
        ${HPCC_SOURCE_DIR}/esp/platform
        ${HPCC_SOURCE_DIR}/esp/services
        ${HPCC_SOURCE_DIR}/esp/services/ws_ecl
        ${HPCC_SOURCE_DIR}/esp/services/ws_workunits
        ${HPCC_SOURCE_DIR}/esp/services/ws_dfu
        ${HPCC_SOURCE_DIR}/system/xmllib
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/system/jlib
        ${HPCC_SOURCE_DIR}/system/security/shared
        ${HPCC_SOURCE_DIR}/system/security/securesocket
        ${HPCC_SOURCE_DIR}/system/security/zcrypt
        ${HPCC_SOURCE_DIR}/system/security/LdapSecurity
        ${HPCC_SOURCE_DIR}/system/mp
        ${HPCC_SOURCE_DIR}/dali/dfu
        ${HPCC_SOURCE_DIR}/dali/base/
        ${HPCC_SOURCE_DIR}/dali/sasha
        ${HPCC_SOURCE_DIR}/dali/ft
        ${HPCC_SOURCE_DIR}/common/workunit
        ${HPCC_SOURCE_DIR}/common/remote
        ${HPCC_SOURCE_DIR}/common/environment
        ${HPCC_SOURCE_DIR}/common/wuwebview
        ${HPCC_SOURCE_DIR}/common/fileview2
        ${HPCC_SOURCE_DIR}/common/dllserver
        ${HPCC_SOURCE_DIR}/common/deftype
        ${HPCC_SOURCE_DIR}/ecl/hql
        ${libantlr3c_includes}
        ${ESPSCM_GENERATED_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/SQL2ECL
        ${HPCC_SOURCE_DIR}/common/thorhelper
        )

    if (CMAKE_COMPILER_IS_CLANG)
      set_source_files_properties(${ANTLR_sql2ecl_SOURCES} PROPERTIES COMPILE_FLAGS -Wno-parentheses-equality)
    endif()
    
    add_definitions(-D_USRDLL)
    HPCC_ADD_LIBRARY(ws_sql SHARED ${SRCS})
    add_dependencies(ws_sql espscm sql2ecl)
    target_link_libraries(
        ws_sql
        ws_workunits
        ws_dfu
        jlib
        xmllib
        esphttp
        dalibase
        libantlr3c
        wuwebview
        )

    SET_DIRECTORY_PROPERTIES(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES "antlr3c")

IF (USE_OPENSSL)
    target_link_libraries ( ws_sql
    	securesocket
    )
ENDIF()
         
    install(TARGETS ws_sql
        RUNTIME DESTINATION ${EXEC_DIR}
        LIBRARY DESTINATION ${LIB_DIR}
        COMPONENT Runtime
        )
endif()
