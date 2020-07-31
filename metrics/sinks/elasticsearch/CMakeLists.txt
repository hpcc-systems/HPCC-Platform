################################################################################
#    HPCC SYSTEMS software Copyright (C) 20120 HPCC Systems.
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

project(hpccmetrics_elasticsearch)

set ( srcs
        ElasticsearchSink.cpp
        )

INCLUDE_DIRECTORIES(
        ../
        ../../../metrics
        ${CMAKE_BINARY_DIR}/generated
        ${HPCC_SOURCE_DIR}/system/jlib
        ${HPCC_SOURCE_DIR}/esp/bindings
        ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
        ${HPCC_SOURCE_DIR}/esp/bindings/http/client
        ${HPCC_SOURCE_DIR}/esp/platform
        ${HPCC_SOURCE_DIR}/system/security/shared
        ${HPCC_SOURCE_DIR}/system/include
)

add_definitions( -DSINKDLL)
HPCC_ADD_LIBRARY( hpccmetrics_elasticsearch SHARED ${srcs} )
TARGET_LINK_LIBRARIES( hpccmetrics_elasticsearch jlib esphttp)
INSTALL ( TARGETS hpccmetrics_elasticsearch RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
