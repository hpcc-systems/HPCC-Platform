################################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.
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

PROJECT( ws_sql )

# Java and ANTLR needed for generating lexer and parser
FIND_PACKAGE(Java REQUIRED)
FIND_PACKAGE(ANTLR REQUIRED)

#antlr3c must be built in order to build with the generated lexer.c and parser.c
INCLUDE (${CMAKE_MODULE_PATH}buildANTLR3c.cmake)
#INCLUDE (${CMAKE_MODULE_PATH}FindANTLR3c.cmake)

INCLUDE(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)
INCLUDE(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

SET (HPCC_SQL_PARSER_LOCATION ${HPCC_SOURCE_DIR}/esp/services/ws_sql/SQL2ECL)
SET (HPCC_SQL_PARSER_GRAMMAR_PATH ${HPCC_SQL_PARSER_LOCATION}/ANTLR3c)
SET (HPCC_SQL_PARSER_GRAMMAR_NAME HPCCSQL.g)

#sample command
#java -jar antlr-3.4-complete.jar -o /home/hpccuser/GIT/HPCC-Platform/esp/services/ws_sql/SQL2ECL/ -report -traceTreeParser /usr/local/eclipse/workspaces/antlr/mysql/HPCCSQL.g

#The actual java command to generate lexer and parser
SET (ANTLR3c_COMMAND ${Java_JAVA_EXECUTABLE} "-jar" ${ANTLR_BUILDTIME_JAR} ${HPCC_SQL_PARSER_GRAMMAR_PATH}/${HPCC_SQL_PARSER_GRAMMAR_NAME} "-o" ${HPCC_SQL_PARSER_LOCATION})

#The lexer and parser targets
SET (ANTLR_GENERATED_LEXER  ${HPCC_SQL_PARSER_LOCATION}/HPCCSQLLexer.c)
SET (ANTLR_GENERATED_PARSER ${HPCC_SQL_PARSER_LOCATION}/HPCCSQLParser.c)

#Create the ANTLR generated c targeted lexer and parser
ADD_CUSTOM_COMMAND(
        OUTPUT ${ANTLR_GENERATED_LEXER} ${ANTLR_GENERATED_PARSER}
        COMMAND ${ANTLR3c_COMMAND}
)

SET(    SRCS
        ${ESPSCM_GENERATED_DIR}/ws_sql_esp.cpp
        ws_sqlPlugin.cpp
        ws_sqlService.cpp
        ${HPCC_SQL_PARSER_LOCATION}/HPCCFileCache.cpp
        ${HPCC_SQL_PARSER_LOCATION}/HPCCFile.cpp
        ${HPCC_SQL_PARSER_LOCATION}/ECLFunction.cpp
        ${HPCC_SQL_PARSER_LOCATION}/ECLEngine.cpp
        ${HPCC_SQL_PARSER_LOCATION}/SQLColumn.cpp
        ${HPCC_SQL_PARSER_LOCATION}/SQLTable.hpp
        ${HPCC_SQL_PARSER_LOCATION}/SQLExpression.cpp
        ${HPCC_SQL_PARSER_LOCATION}/SQLJoin.cpp
        ${HPCC_SQL_PARSER_LOCATION}/HPCCSQLTreeWalker.cpp
        ${HPCC_SQL_PARSER_LOCATION}/HPCCSQLLexer.c
        ${HPCC_SQL_PARSER_LOCATION}/HPCCSQLParser.c
    )

INCLUDE_DIRECTORIES (
        ##added for build-config.h
        ${CMAKE_BINARY_DIR}
        ##added for build-config.h
        ${CMAKE_BINARY_DIR}/oss

        ./SQL2ECL
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
        ${HPCC_SOURCE_DIR}/system/mp
        ${HPCC_SOURCE_DIR}/dali/dfu
        ${HPCC_SOURCE_DIR}/dali/base/
        ${HPCC_SOURCE_DIR}/common/workunit
        ${HPCC_SOURCE_DIR}/common/remote
        ${HPCC_SOURCE_DIR}/common/environment
        ${HPCC_SOURCE_DIR}/common/workunit
        ${HPCC_SOURCE_DIR}/common/wuwebview
        ${HPCC_SOURCE_DIR}/common/fileview2
        ${HPCC_SOURCE_DIR}/common/dllserver
        ${HPCC_SOURCE_DIR}/common/deftype
        ${HPCC_SOURCE_DIR}/ecl/hql
        ${ANTLR3c_INCLUDE_DIR}
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_sql SHARED ${SRCS} )
INSTALL ( TARGETS ws_sql RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} COMPONENT Runtime)

ADD_DEPENDENCIES (ws_sql espscm)

TARGET_LINK_LIBRARIES (
        ws_sql
        jlib
        xmllib
        esphttp
        securesocket
        ${ANTLR3c_LIBRARIES}
        dalibase
    )
