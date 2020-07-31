################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems.
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

project ( metrictest )

SET (SRC_FILES
        metrictest.cpp
#        ../sinks/file/FileSink.cpp
        )

INCLUDE_DIRECTORIES(
        ${CMAKE_BINARY_DIR}
        ${CMAKE_BINARY_DIR}/oss
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/metrics
        ${HPCC_SOURCE_DIR}/metrics/sinks
)

HPCC_ADD_EXECUTABLE ( metrictest ${SRC_FILES} )
TARGET_LINK_LIBRARIES( metrictest dl )
INSTALL ( TARGETS metrictest RUNTIME DESTINATION ${EXEC_DIR} )

#TARGET_LINK_LIBRARIES(envchk configmgr)
