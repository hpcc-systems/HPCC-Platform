################################################################################
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems.
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

project ( envmod )

SET (SRC_FILES
        envmod.cpp
)

INCLUDE_DIRECTORIES(
        ${CMAKE_BINARY_DIR}
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/configuration/configmgr/configmgrlib
        ${HPCC_SOURCE_DIR}/configuration/configmgr/RapidJSON/include
)

HPCC_ADD_EXECUTABLE ( envmod ${SRC_FILES} )

INSTALL ( TARGETS envmod RUNTIME DESTINATION ${EXEC_DIR} )

TARGET_LINK_LIBRARIES(envmod configmgr)
