##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
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
##############################################################################

project ( cfgsupport_addrequiredinstances )

SET(  SRCS
        AddRequiredInstancesSupport.cpp
)

INCLUDE_DIRECTORIES(
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/deployment/deploy
    ${CMAKE_PREFIX_PATH}/mkspecs/linux-g++
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
    ${CMAKE_BINARY_DIR}
    ${CMAKE_BINARY_DIR}/oss
)

ADD_DEFINITIONS( -D_USRDLL -DCFGSUPPORT_DAFILESRV_LIB)
HPCC_ADD_LIBRARY( cfgsupport_addrequiredinstances SHARED ${SRCS} )
TARGET_LINK_LIBRARIES( cfgsupport_addrequiredinstances config2 jlib )

INSTALL ( TARGETS cfgsupport_addrequiredinstances RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
