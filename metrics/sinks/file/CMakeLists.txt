################################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
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

project(hpccmetrics_filesink)

set ( srcs
        FileSink.cpp
   )

include_directories(
        ../
        ${HPCC_SOURCE_DIR}/metrics
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/system/jlib
)

add_definitions( -DMETRICSDLL)
HPCC_ADD_LIBRARY( hpccmetrics_filesink SHARED ${srcs} )
TARGET_LINK_LIBRARIES( hpccmetrics_filesink jlib hpccmetrics)
INSTALL ( TARGETS hpccmetrics_filesink RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
