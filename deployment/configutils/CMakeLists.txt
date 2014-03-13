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


# Component: configutils
#####################################################
# Description:
# ------------
#    Cmake Input File for configutils
#####################################################

project( configutils )

set (    SRCS
         confighelper.cpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/deployment/deploy
         ${CMAKE_BINARY_DIR}/oss
         ${CMAKE_BINARY_DIR}
    )

ADD_DEFINITIONS ( -D_USRDLL -DCONFIGUTILS_EXPORTS )

HPCC_ADD_LIBRARY( configutils SHARED ${SRCS} )
install ( TARGETS configutils RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( configutils
         jlib
         environment
    )
